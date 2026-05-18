"""
fl_features.py
==============
Converts raw Kafka match events into clean numerical feature vectors
that the PyTorch model can understand.

Raw event example:
  { "minute": 67, "type": "goal", "score_diff": -1, "player_position": "Attacker" }

Feature vector output (8 numbers):
  [0.74, 1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 1.0]
"""

import numpy as np

# ── Encoding maps ──────────────────────────────────────────────────────────────
# Convert string categories to numbers
EVENT_TYPE_MAP = {
    "goal":         1,
    "yellow_card":  2,
    "red_card":     3,
    "substitution": 4,
    "penalty":      5,
    "own_goal":     6,
    "other":        0,
}

POSITION_MAP = {
    "Attacker":   1,
    "Midfielder": 2,
    "Defender":   3,
    "Goalkeeper": 4,
    "Unknown":    0,
}

# Feature vector size — must match fl_model.py INPUT_SIZE
FEATURE_SIZE = 8


def extract_features(event: dict) -> np.ndarray:
    """
    Convert a Kafka match event dict into a numpy feature vector.

    Features:
      [0] minute_normalized    → minute / 90 (0.0 to 1.0)
      [1] event_type           → encoded as int, normalized by max (6)
      [2] score_diff           → home_goals - away_goals, clipped to [-5, 5], normalized
      [3] position_encoded     → player position as int, normalized by max (4)
      [4] is_goal              → 1 if event is a goal, else 0
      [5] is_card              → 1 if event is a card, else 0
      [6] is_substitution      → 1 if event is a substitution, else 0
      [7] late_game            → 1 if minute > 70, else 0 (late game pressure)
    """
    minute     = float(event.get("minute", 0))
    event_type = str(event.get("type", "other")).lower()
    home_goals = int(event.get("home_goals", 0))
    away_goals = int(event.get("away_goals", 0))
    position   = str(event.get("player_position", "Unknown"))

    # Encode and normalize each feature
    minute_norm    = min(minute / 90.0, 1.0)
    type_encoded   = EVENT_TYPE_MAP.get(event_type, 0) / 6.0
    score_diff     = float(np.clip(home_goals - away_goals, -5, 5)) / 5.0
    pos_encoded    = POSITION_MAP.get(position, 0) / 4.0
    is_goal        = 1.0 if "goal" in event_type else 0.0
    is_card        = 1.0 if "card" in event_type else 0.0
    is_sub         = 1.0 if "sub" in event_type else 0.0
    late_game      = 1.0 if minute > 70 else 0.0

    features = np.array([
        minute_norm,
        type_encoded,
        score_diff,
        pos_encoded,
        is_goal,
        is_card,
        is_sub,
        late_game,
    ], dtype=np.float32)

    return features


def build_training_label(event: dict, future_events: list) -> float:
    """
    Build a training label: did a goal happen in the next 10 minutes?

    Returns:
      1.0 if a goal occurred within 10 minutes after this event
      0.0 otherwise

    Used to create supervised training data from historical events.
    """
    current_minute = event.get("minute", 0)
    window_end     = current_minute + 10

    for future_event in future_events:
        if (future_event.get("minute", 0) <= window_end and
                "goal" in str(future_event.get("type", "")).lower()):
            return 1.0

    return 0.0


if __name__ == "__main__":
    # Quick test
    test_event = {
        "minute": 67,
        "type": "goal",
        "home_goals": 1,
        "away_goals": 0,
        "player_position": "Attacker",
    }
    features = extract_features(test_event)
    print(f"Feature vector: {features}")
    print(f"Feature size:   {len(features)} (should be {FEATURE_SIZE})")
