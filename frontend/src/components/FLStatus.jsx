// components/FLStatus.jsx
// =======================
// Shows Federated Learning goal probability predictions.
// Polls /api/ml/predictions every 5 seconds and displays
// a probability badge on each match card.

import { useState, useEffect } from "react";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── FL Status Panel ────────────────────────────────────────────────────────────
export function FLStatusPanel() {
  const [status, setStatus] = useState(null);

  useEffect(() => {
    const fetch_status = async () => {
      try {
        const res  = await fetch(`${API_URL}/api/ml/status`);
        const data = await res.json();
        setStatus(data);
      } catch {
        setStatus(null);
      }
    };

    fetch_status();
    const interval = setInterval(fetch_status, 10000);
    return () => clearInterval(interval);
  }, []);

  if (!status) return null;

  return (
    <div className="fl-status-panel">
      <span className="fl-icon">🧠</span>
      <span className="fl-label">FL Model</span>
      <span className={`fl-dot ${status.model_fresh ? "fl-dot--active" : "fl-dot--idle"}`} />
      <span className="fl-state">
        {status.model_fresh ? "Active" : status.status === "waiting_for_training" ? "Training..." : "Idle"}
      </span>
    </div>
  );
}

// ── Goal Probability Badge ─────────────────────────────────────────────────────
// Drop this inside MatchCard.jsx to show per-match predictions
export function GoalProbBadge({ gameId }) {
  const [prob, setProb]     = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!gameId) return;

    const fetchProb = async () => {
      try {
        const res  = await fetch(`${API_URL}/api/ml/prediction/${gameId}`);
        const data = await res.json();
        if (data.goal_probability !== undefined) {
          setProb(data);
        }
      } catch {
        // silently fail — FL is optional
      } finally {
        setLoading(false);
      }
    };

    fetchProb();
    const interval = setInterval(fetchProb, 5000);
    return () => clearInterval(interval);
  }, [gameId]);

  if (loading || !prob) return null;

  const pct       = Math.round(prob.goal_probability * 100);
  const intensity = pct >= 70 ? "high" : pct >= 40 ? "medium" : "low";

  return (
    <div className={`fl-prob-badge fl-prob-badge--${intensity}`}>
      <span className="fl-prob-icon">⚽</span>
      <span className="fl-prob-label">Goal prob</span>
      <span className="fl-prob-value">{pct}%</span>
      <div className="fl-prob-bar">
        <div
          className="fl-prob-fill"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

// ── All Predictions Hook ───────────────────────────────────────────────────────
// Use this in App.jsx to fetch all predictions at once (more efficient)
export function useAllPredictions() {
  const [predictions, setPredictions] = useState({});

  useEffect(() => {
    const fetch = async () => {
      try {
        const res  = await globalThis.fetch(`${API_URL}/api/ml/predictions`);
        const data = await res.json();
        setPredictions(data.predictions || {});
      } catch {
        // FL is optional, fail silently
      }
    };

    fetch();
    const interval = setInterval(fetch, 5000);
    return () => clearInterval(interval);
  }, []);

  return predictions;
}

export default GoalProbBadge;
