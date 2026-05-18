"""
fl_server.py
============
Federated Learning Server (Aggregator).

What it does:
  1. Listens to model_updates Kafka topic
  2. Collects weight updates from FL clients (one per match)
  3. When 3+ clients have sent updates → runs FedAvg aggregation
  4. Saves the new global model to global_model.pt
  5. Broadcasts the global model back to all clients via Kafka

FedAvg formula:
  global_weight = sum(client_weight * num_samples) / total_samples

Usage:
  python fl_server.py
"""

import json
import logging
import time
import numpy as np
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from fl_model import (
    create_model, get_weights, set_weights,
    save_model, load_model
)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = "localhost:9092"
TOPIC_MODEL        = "model_updates"
MIN_CLIENTS_TO_AGG = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FL-SERVER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


class FLServer:

    def __init__(self):
        self.global_model   = load_model()
        self.client_updates = {}
        self.global_round   = 0
        self.stats = {
            "total_rounds":     0,
            "total_clients":    0,
            "last_aggregation": None,
            "avg_loss_history": [],
        }
        log.info("🖥️  FL Server initialized")
        log.info(f"   Minimum clients for aggregation: {MIN_CLIENTS_TO_AGG}")

    def connect_kafka(self):
        """Connect to Kafka with retry."""
        for attempt in range(10):
            try:
                self.consumer = KafkaConsumer(
                    TOPIC_MODEL,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="fl-server",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="earliest",
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                )
                log.info("✅ Kafka connected!")
                return
            except NoBrokersAvailable:
                log.warning(f"⏳ Attempt {attempt+1}/10...")
                time.sleep(3)
        raise ConnectionError("❌ Could not connect to Kafka")

    def receive_client_update(self, data: dict):
        """Store a client's weight update in the buffer."""
        client_id   = data.get("client_id", "unknown")
        game_id     = data.get("game_id", "")
        num_samples = data.get("num_samples", 0)
        weights     = data.get("weights", {})
        avg_loss    = data.get("avg_loss", 0.0)

        if not weights:
            log.warning(f"⚠️  Empty weights from {client_id}, skipping")
            return

        self.client_updates[client_id] = {
            "weights":     weights,
            "num_samples": num_samples,
            "avg_loss":    avg_loss,
            "game_id":     game_id,
            "received_at": datetime.utcnow().isoformat(),
        }

        log.info(
            f"📥 Received update from {client_id} | "
            f"Samples: {num_samples} | "
            f"Loss: {avg_loss:.4f} | "
            f"Buffer: {len(self.client_updates)}/{MIN_CLIENTS_TO_AGG}"
        )

        if len(self.client_updates) >= MIN_CLIENTS_TO_AGG:
            self.aggregate()

    def aggregate(self):
        """FedAvg aggregation."""
        self.global_round += 1
        log.info(f"\n{'='*50}")
        log.info(f"🔄 Starting FedAvg aggregation — Round {self.global_round}")
        log.info(f"   Clients contributing: {len(self.client_updates)}")

        updates       = list(self.client_updates.values())
        total_samples = sum(u["num_samples"] for u in updates)

        if total_samples == 0:
            log.warning("⚠️  Total samples = 0, skipping aggregation")
            return

        weight_keys = list(updates[0]["weights"].keys())
        aggregated  = {}

        for key in weight_keys:
            weighted_sum = None
            for update in updates:
                client_weight = np.array(update["weights"][key])
                n             = update["num_samples"]
                contribution  = client_weight * (n / total_samples)
                if weighted_sum is None:
                    weighted_sum = contribution
                else:
                    weighted_sum += contribution
            aggregated[key] = weighted_sum.tolist()

        set_weights(self.global_model, aggregated)
        save_model(self.global_model)

        avg_loss = sum(u["avg_loss"] for u in updates) / len(updates)
        self.stats["total_rounds"]     += 1
        self.stats["total_clients"]    += len(updates)
        self.stats["last_aggregation"]  = datetime.utcnow().isoformat()
        self.stats["avg_loss_history"].append(avg_loss)

        log.info(f"   ✅ Aggregation complete!")
        log.info(f"   Total samples used: {total_samples}")
        log.info(f"   Average client loss: {avg_loss:.4f}")
        log.info(f"{'='*50}\n")

        self.broadcast_global_model(aggregated, avg_loss)
        self.client_updates.clear()

    def broadcast_global_model(self, aggregated_weights: dict, avg_loss: float):
        """Send aggregated global model weights back to all FL clients."""
        payload = {
            "type":         "GLOBAL_UPDATE",
            "global_round": self.global_round,
            "weights":      aggregated_weights,
            "avg_loss":     avg_loss,
            "timestamp":    datetime.utcnow().isoformat(),
            "stats":        self.stats,
        }
        self.producer.send(TOPIC_MODEL, key="global", value=payload)
        self.producer.flush()
        log.info(f"📤 Global model broadcast (round {self.global_round})")

    def run(self):
        """Main loop: consume client updates and aggregate."""
        self.connect_kafka()
        log.info("👂 FL Server listening for client updates...")

        for message in self.consumer:
            data     = message.value
            msg_type = data.get("type", "")

            if msg_type == "CLIENT_UPDATE":
                self.receive_client_update(data)
            elif msg_type == "GLOBAL_UPDATE":
                pass  # ignore our own broadcasts


if __name__ == "__main__":
    server = FLServer()
    try:
        server.run()
    except KeyboardInterrupt:
        log.info("\n⛔ FL Server stopped.")
        log.info(f"   Total rounds completed: {server.stats['total_rounds']}")
        log.info(f"   Total client updates:   {server.stats['total_clients']}")