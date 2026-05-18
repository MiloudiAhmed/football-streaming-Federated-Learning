"""
fl_api_routes.py
================
New FastAPI routes for the Federated Learning module.

Add to app.py with:
  from fl_api_routes import fl_router
  app.include_router(fl_router)

Endpoints:
  GET /api/ml/status                  → FL system status
  GET /api/ml/prediction/{game_id}    → goal probability for a live match
  GET /api/ml/predictions             → predictions for all active matches
"""

import sys
import logging
from pathlib import Path

from fastapi import APIRouter

# Add ml_placeholder to path so we can import FL modules
ML_PATH = ML_PATH = Path("C:/Users/Administrator/Desktop/football-streaming/ml_placeholder")
sys.path.insert(0, str(ML_PATH))

log = logging.getLogger(__name__)

# Create a router — this gets mounted into the main FastAPI app
fl_router = APIRouter(prefix="/api/ml", tags=["Federated Learning"])


def get_predictor():
    """Lazy import predictor — only loads PyTorch when first called."""
    try:
        from fl_predictor import get_goal_probability, get_fl_status
        return get_goal_probability, get_fl_status
    except ImportError as e:
        log.warning(f"FL predictor not available: {e}")
        return None, None


@fl_router.get("/status")
async def fl_status():
    """
    Returns the current Federated Learning system status.

    Response example:
    {
      "model_exists": true,
      "model_fresh": true,
      "model_age_sec": 45,
      "status": "active"
    }
    """
    _, get_fl_status = get_predictor()

    if get_fl_status is None:
        return {
            "status": "fl_module_not_installed",
            "message": "Install PyTorch: pip install torch numpy",
        }

    return get_fl_status()


@fl_router.get("/prediction/{game_id}")
async def get_prediction(game_id: str):
    """
    Returns goal probability prediction for a specific live match.

    Response example:
    {
      "game_id": "3607577",
      "goal_probability": 0.73,
      "label": "73%",
      "interpretation": "High chance of a goal"
    }
    """
    get_goal_probability, _ = get_predictor()

    if get_goal_probability is None:
        return {
            "game_id":          game_id,
            "goal_probability": 0.5,
            "label":            "N/A",
            "interpretation":   "FL module not installed",
        }

    # Import store from app to get current match state
    try:
        # Get current match state from the in-memory store
        # We import here to avoid circular imports
        import importlib
        app_module = importlib.import_module("app")
        store      = app_module.store

        match = store.active.get(game_id)
        if not match:
            return {"error": "Match not found or not live", "game_id": game_id}

        # Build match state from current store data
        last_event = match.get("last_event", {})
        match_state = {
            "minute":          match.get("minute", 0),
            "type":            last_event.get("type", "other"),
            "home_goals":      match.get("home_goals", 0),
            "away_goals":      match.get("away_goals", 0),
            "player_position": last_event.get("player_position", "Unknown"),
        }

        prob = get_goal_probability(match_state)

        # Human-readable interpretation
        if prob >= 0.7:
            interpretation = "High chance of a goal ⚡"
        elif prob >= 0.4:
            interpretation = "Moderate chance of a goal"
        else:
            interpretation = "Low chance of a goal"

        return {
            "game_id":          game_id,
            "goal_probability": prob,
            "label":            f"{prob:.0%}",
            "interpretation":   interpretation,
            "match_state":      match_state,
        }

    except Exception as e:
        log.error(f"Prediction error for {game_id}: {e}")
        return {
            "game_id":          game_id,
            "goal_probability": 0.5,
            "label":            "50%",
            "interpretation":   "Prediction unavailable",
        }


@fl_router.get("/predictions")
async def get_all_predictions():
    """
    Returns goal probability predictions for ALL active matches.
    Useful for the frontend to show predictions on all match cards at once.
    """
    get_goal_probability, _ = get_predictor()

    try:
        import importlib
        app_module   = importlib.import_module("app")
        store        = app_module.store
        predictions  = {}

        for game_id, match in store.active.items():
            if get_goal_probability:
                last_event  = match.get("last_event", {})
                match_state = {
                    "minute":          match.get("minute", 0),
                    "type":            last_event.get("type", "other"),
                    "home_goals":      match.get("home_goals", 0),
                    "away_goals":      match.get("away_goals", 0),
                    "player_position": last_event.get("player_position", "Unknown"),
                }
                prob = get_goal_probability(match_state)
            else:
                prob = 0.5

            predictions[game_id] = {
                "goal_probability": prob,
                "label":            f"{prob:.0%}",
            }

        return {"predictions": predictions}

    except Exception as e:
        log.error(f"Predictions error: {e}")
        return {"predictions": {}}
