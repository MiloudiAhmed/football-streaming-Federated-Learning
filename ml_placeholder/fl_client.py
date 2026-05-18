"""
fl_client.py
============
Federated Learning Client.

One client instance handles ONE match (game_id).
It:
  1. Listens to match_events Kafka topic
  2. Filters events for its assigned game_id
  3. Trains a local model on each incoming event
  4. Every 5 events, sends model weights to Kafka (model_updates topic)
  5. Never shares raw data — only model weights

Usage:
  python fl_client.py
"""

import json
import logging
import time
import argparse
import threading
from datetime import datetime
from collections import deque

import torch.optim as optim
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from fl_features import extract_features, build_training_label
from fl_model import (
    create_model, get_weights, set_weights,
    train_step, predict, load_model
)

# ── Silence noisy kafka connection logs ───────────────────────────────────────
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.ERROR)
logging.getLogger("kafka.client").setLevel(logging.ERROR)
logging.getLogger("kafka.consumer").setLevel(logging.ERROR)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS    = "match_events"
TOPIC_MODEL     = "model_updates"
TRAIN_EVERY_N   = 5      # send weights every 5 events
MAX_HISTORY     = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FL-CLIENT] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


class FLClient:
    """
    A single Federated Learning client for one football match.

    Lifecycle:
      1. Start listening to match_events
      2. For each event belonging to this match:
         a. Extract features
         b. Build training label
         c. Train local model one step
         d. Every 5 events → send weights to FL server via Kafka
      3. Stop when match ends
    """

    def __init__(self, game_id: str):
        self.game_id      = game_id
        self.client_id    = f"fl_client_{game_id}"
        self.model        = load_model()
        self.optimizer    = optim.Adam(self.model.parameters(), lr=0.001)
        self.event_buffer = deque(maxlen=MAX_HISTORY)
        self.event_count  = 0
        self.round_num    = 0
        self.total_loss   = 0.0
        self.is_running   = True
        log.info(f"🤖 FL Client initialized for game_id={game_id}")

    def connect_kafka(self):
        """Connect to Kafka with retry logic."""
        for attempt in range(10):
            try:
                self.consumer = KafkaConsumer(
                    TOPIC_EVENTS,
                    TOPIC_MODEL,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="fl-client-worker",   # fixed group — no accumulation
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="earliest",
                    session_timeout_ms=60000,
                    heartbeat_interval_ms=20000,
                    request_timeout_ms=70000,
                    max_poll_interval_ms=300000,
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                )
                log.info("✅ Kafka connected!")
                return
            except NoBrokersAvailable:
                log.warning(f"⏳ Kafka not ready, attempt {attempt+1}/10...")
                time.sleep(3)
        raise ConnectionError("❌ Could not connect to Kafka")

    def process_event(self, event: dict):
        """Train on a single event."""
        self.event_buffer.append(event)
        features = extract_features(event)
        future_events = list(self.event_buffer)
        label = build_training_label(event, future_events)

        loss = train_step(self.model, features, label, self.optimizer)
        self.total_loss  += loss
        self.event_count += 1

        if self.event_count % 10 == 0:
            avg_loss = self.total_loss / self.event_count
            log.info(
                f"  📈 [{self.game_id[:8]}] "
                f"Events: {self.event_count} | "
                f"Avg Loss: {avg_loss:.4f} | "
                f"Round: {self.round_num}"
            )

        if self.event_count % TRAIN_EVERY_N == 0:
            self.send_weights()

    def send_weights(self):
        """Serialize model weights and publish to Kafka."""
        self.round_num += 1
        weights  = get_weights(self.model)
        avg_loss = self.total_loss / max(self.event_count, 1)

        payload = {
            "type":        "CLIENT_UPDATE",
            "client_id":   self.client_id,
            "game_id":     self.game_id,
            "round":       self.round_num,
            "weights":     weights,
            "num_samples": self.event_count,
            "avg_loss":    avg_loss,
            "timestamp":   datetime.utcnow().isoformat(),
        }

        self.producer.send(TOPIC_MODEL, key=self.game_id, value=payload)
        self.producer.flush()
        log.info(
            f"  📤 Sent weights | "
            f"Round {self.round_num} | "
            f"Samples: {self.event_count} | "
            f"Loss: {avg_loss:.4f}"
        )

    def apply_global_update(self, data: dict):
        """Apply aggregated global weights from FL server."""
        if data.get("type") == "GLOBAL_UPDATE":
            weights = data.get("weights", {})
            if weights:
                set_weights(self.model, weights)
                log.info(
                    f"  📥 Applied global model "
                    f"(round {data.get('global_round', '?')})"
                )

    def run(self):
        """Main loop: consume events and train."""
        self.connect_kafka()
        log.info(f"🎯 Listening for game_id={self.game_id}...")

        for message in self.consumer:
            if not self.is_running:
                break

            data  = message.value
            topic = message.topic

            if topic == TOPIC_EVENTS:
                if str(data.get("game_id", "")) == str(self.game_id):
                    self.process_event(data)

            elif topic == TOPIC_MODEL:
                self.apply_global_update(data)

            if (str(data.get("game_id", "")) == str(self.game_id) and
                    data.get("type") == "MATCH_END"):
                log.info(f"🏁 Match {self.game_id} ended. Sending final weights...")
                self.send_weights()
                self.is_running = False
                break

        log.info(
            f"✅ FL Client done for game {self.game_id}. "
            f"Trained {self.event_count} events over {self.round_num} rounds."
        )


# ── Multi-match client manager ─────────────────────────────────────────────────
class FLClientManager:
    """
    Manages multiple FL clients — one per active match.
    Listens to match_status topic to know when matches start/end.
    Spawns a new FLClient thread for each new match.
    """

    def __init__(self):
        self.active_clients: dict = {}
        log.info("🧠 FL Client Manager starting...")

    def run(self):
        """Listen for match start/end and manage client threads."""
        for attempt in range(10):
            try:
                consumer = KafkaConsumer(
                    "match_status",
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="fl-client-manager",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="latest",
                    session_timeout_ms=60000,
                    heartbeat_interval_ms=20000,
                    request_timeout_ms=70000,
                )
                log.info("✅ Manager connected to Kafka!")
                break
            except NoBrokersAvailable:
                log.warning(f"⏳ Attempt {attempt+1}/10...")
                time.sleep(3)

        for message in consumer:
            data     = message.value
            msg_type = data.get("type", "")
            game_id  = str(data.get("game_id", ""))

            if msg_type == "MATCH_START" and game_id not in self.active_clients:
                log.info(f"🟢 New match: {game_id}. Spawning FL client...")
                client = FLClient(game_id)
                thread = threading.Thread(
                    target=client.run,
                    daemon=True,
                    name=f"fl-client-{game_id}"
                )
                thread.start()
                self.active_clients[game_id] = (client, thread)
                log.info(f"📊 Active FL clients: {len(self.active_clients)}")

            elif msg_type == "MATCH_END" and game_id in self.active_clients:
                log.info(f"🔴 Match ended: {game_id}. Stopping FL client...")
                client, thread = self.active_clients.pop(game_id)
                client.is_running = False
                log.info(f"📊 Active FL clients: {len(self.active_clients)}")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--game-id", type=str, default=None,
        help="Specific game_id. If not set, auto-manages all matches."
    )
    args = parser.parse_args()

    if args.game_id:
        client = FLClient(args.game_id)
        client.run()
    else:
        manager = FLClientManager()
        manager.run()