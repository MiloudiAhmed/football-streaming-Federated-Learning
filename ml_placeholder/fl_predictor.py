"""
fl_predictor.py
===============
Loads the global FL model and provides prediction functions
that the FastAPI backend can call.
"""

import logging
import time
from pathlib import Path
from datetime import datetime

from fl_features import extract_features
from fl_model import load_model, predict, MODEL_PATH

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Global model cache ─────────────────────────────────────────────────────────
_model           = None
_model_loaded_at = 0
RELOAD_INTERVAL  = 30


def get_model():
    """Get the global model, reloading from disk if stale."""
    global _model, _model_loaded_at

    now = time.time()
    if _model is None or (now - _model_loaded_at) > RELOAD_INTERVAL:
        _model = load_model(MODEL_PATH)
        _model_loaded_at = now

    return _model


def get_goal_probability(match_state: dict) -> float:
    """
    Predict goal probability combining ML model + rule-based adjustments.

    Strategy:
      - 20% weight on ML model output (not well trained yet)
      - 80% weight on neutral base (0.35)
      - Rule-based adjustments push up/down from there
      - Final clamp between 12% and 85%

    Args:
      match_state: dict with current match info:
        {
          "minute":          67,
          "type":            "yellow_card",
          "home_goals":      1,
          "away_goals":      0,
          "player_position": "Attacker"
        }

    Returns:
      float between 0.12 and 0.85
    """
    try:
        model     = get_model()
        features  = extract_features(match_state)
        base_prob = predict(model, features)

        minute     = match_state.get("minute", 0)
        home_goals = match_state.get("home_goals", 0)
        away_goals = match_state.get("away_goals", 0)
        score_diff = abs(home_goals - away_goals)
        last_type  = str(match_state.get("type", "")).lower()

        # ── Blend model output with neutral base ───────────────────────────
        # Model is not trained well enough yet so we use it lightly
        # Weight: 20% model, 80% neutral starting point
        neutral   = 0.35
        base_prob = (base_prob * 0.2) + (neutral * 0.8)

       # ── Blend model output with neutral base ───────────────────────────
        neutral   = 0.35
        base_prob = (base_prob * 0.2) + (neutral * 0.8)

        # ── Minute factor ──────────────────────────────────────────────────
        if minute > 85:
            base_prob += 0.15
        elif minute > 75:
            base_prob += 0.08
        elif minute > 60:
            base_prob += 0.04
        elif minute < 15:
            base_prob -= 0.12    # very early game → low chance
        elif minute < 30:
            base_prob -= 0.07    # early game → below average

        # ── Score situation ────────────────────────────────────────────────
        if score_diff == 0:
            base_prob += 0.06
        elif score_diff == 1 and minute > 70:
            base_prob += 0.08
        elif score_diff == 2:
            base_prob -= 0.08    # game somewhat decided
        elif score_diff >= 3:
            base_prob -= 0.18    # game completely over → very low

        # ── Last event bonus ───────────────────────────────────────────────
        if "penalty" in last_type:
            base_prob += 0.18
        elif "red" in last_type:
            base_prob += 0.10
        elif "goal" in last_type:
            base_prob += 0.07
        elif "yellow" in last_type:
            base_prob += 0.02
        elif "sub" in last_type and minute > 65:
            base_prob += 0.04

        # ── Clamp to realistic range ───────────────────────────────────────
        return round(float(max(0.08, min(0.85, base_prob))), 3)

    except Exception as e:
        log.error(f"Prediction error: {e}")
        return 0.35


def get_fl_status() -> dict:
    """Return current FL system status for the API."""
    model_exists  = MODEL_PATH.exists()
    model_age_sec = None

    if model_exists:
        model_age_sec = int(time.time() - MODEL_PATH.stat().st_mtime)

    return {
        "model_exists":  model_exists,
        "model_path":    str(MODEL_PATH),
        "model_age_sec": model_age_sec,
        "model_fresh":   model_age_sec < 3600 if model_age_sec else False,
        "status":        "active" if model_exists else "waiting_for_training",
        "last_checked":  datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    # Test different match situations to verify spread
    situations = [
        {"minute": 5,  "type": "substitution", "home_goals": 0, "away_goals": 0, "player_position": "Midfielder"},
        {"minute": 20, "type": "yellow_card",  "home_goals": 0, "away_goals": 1, "player_position": "Defender"},
        {"minute": 45, "type": "goal",         "home_goals": 1, "away_goals": 1, "player_position": "Attacker"},
        {"minute": 60, "type": "substitution", "home_goals": 0, "away_goals": 1, "player_position": "Attacker"},
        {"minute": 72, "type": "yellow_card",  "home_goals": 1, "away_goals": 1, "player_position": "Midfielder"},
        {"minute": 80, "type": "goal",         "home_goals": 2, "away_goals": 1, "player_position": "Attacker"},
        {"minute": 84, "type": "red_card",     "home_goals": 0, "away_goals": 0, "player_position": "Defender"},
        {"minute": 88, "type": "goal",         "home_goals": 2, "away_goals": 3, "player_position": "Attacker"},
        {"minute": 90, "type": "penalty",      "home_goals": 1, "away_goals": 1, "player_position": "Attacker"},
    ]

    print("\n📊 Goal probability across different match situations:\n")
    for s in situations:
        prob = get_goal_probability(s)
        bar  = "█" * int(prob * 20) + "░" * (20 - int(prob * 20))
        print(
            f"  {s['minute']:2d}' "
            f"{s['type']:15s} "
            f"{s['home_goals']}-{s['away_goals']} "
            f"{bar} {prob:.0%}"
        )