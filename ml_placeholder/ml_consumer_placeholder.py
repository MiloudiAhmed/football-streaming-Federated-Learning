"""
ml_consumer_placeholder.py
===========================
Placeholder for future Federated Learning integration.

This consumer listens to:
  - match_events  → for feature extraction
  - model_updates → for receiving/aggregating FL model updates

Structure is designed so you can drop in actual FL code (e.g. Flower, PySyft)
without changing the Kafka or data pipeline architecture.

Federated Learning integration points are marked with: # FL_HOOK
"""

import json
import logging
import time
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ML] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"


def extract_features(event: dict) -> dict:
    """
    # FL_HOOK
    Extract features from a match event for ML model input.
    Replace this with your actual feature engineering pipeline.

    Example features for a "predict next goal" model:
      - minute, current_score_diff, event_type_encoded, etc.
    """
    return {
        "minute":     event.get("minute", 0),
        "event_type": event.get("type", "other"),
        "score_diff": event.get("home_goals", 0) - event.get("away_goals", 0),
        "player_pos": event.get("player_position", "unknown"),
    }


def handle_model_update(data: dict):
    """
    # FL_HOOK
    Handle incoming model weights from a federated client.
    Replace this with your aggregation logic (FedAvg, etc.)
    """
    log.info(f"📦 Received model update from client: {data.get('client_id', '?')}")
    # TODO: aggregate weights, update global model, broadcast new weights


def run():
    log.info("🤖 ML Consumer (placeholder) starting...")
    log.info("   Listening to: match_events, model_updates")
    log.info("   FL_HOOKs are marked in the code — replace with real FL logic")

    consumer = KafkaConsumer(
        "match_events",
        "model_updates",    # FL_HOOK: subscribe to model update topic
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="ml-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )

    event_count = 0

    for message in consumer:
        data  = message.value
        topic = message.topic

        if topic == "match_events":
            features = extract_features(data)
            event_count += 1

            # FL_HOOK: feed features to local model, accumulate for training
            log.debug(f"Features extracted: {features}")

            # Log every 100 events so you can see it's working
            if event_count % 100 == 0:
                log.info(f"📊 Processed {event_count} events so far")

        elif topic == "model_updates":
            handle_model_update(data)


if __name__ == "__main__":
    run()