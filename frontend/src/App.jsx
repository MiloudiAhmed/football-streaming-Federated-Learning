// App.jsx
// ========
// Root component. Manages WebSocket connection and top-level state.
// Renders a live football dashboard with multiple match cards.

import { useState, useEffect, useCallback, useRef } from "react";
import MatchCard from "./components/MatchCard";
import Header from "./components/Header";
import { useWebSocket } from "./hooks/useWebSocket";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_URL  = import.meta.env.VITE_WS_URL  || "ws://localhost:8000/ws";

export default function App() {
  const [matches, setMatches]       = useState([]);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [eventFlash, setEventFlash] = useState(null); // for flash animation on new event

  // ── WebSocket connection ───────────────────────────────────────────────────
  const handleMessage = useCallback((data) => {
    if (data.type === "INITIAL_STATE" || data.type === "STATE_UPDATE") {
      setMatches(data.matches || []);
      setLastUpdate(new Date());

      // Flash animation when a notable event occurs
      if (data.event && ["goal", "red_card"].some(t => data.event.type?.includes(t))) {
        setEventFlash(data.event);
        setTimeout(() => setEventFlash(null), 2000);
      }
    }
  }, []);

  const { isConnected, reconnect } = useWebSocket(WS_URL, handleMessage);

  // ── Polling fallback (if WebSocket fails) ─────────────────────────────────
  useEffect(() => {
    if (isConnected) return; // WebSocket handles it

    const fetchMatches = async () => {
      try {
        const res  = await fetch(`${API_URL}/api/matches`);
        const data = await res.json();
        setMatches(data.matches || []);
        setLastUpdate(new Date());
      } catch (err) {
        console.error("Polling error:", err);
      }
    };

    fetchMatches();
    const interval = setInterval(fetchMatches, 3000);
    return () => clearInterval(interval);
  }, [isConnected]);

  // ── Separate live vs finished with STABLE SORTING (FIXED - prevents jumping) ──
  // Sort live matches by started_at to keep them in consistent order
  // This prevents cards from reordering when matches update
  // Include finished_pending so winner animation plays in Live section before moving to Recently Finished
  const liveMatches = matches
    .filter(m => m.status === "live" || m.status === "finished_pending")
    .sort((a, b) => (a.started_at || 0) - (b.started_at || 0));  // stable sort by start time

  // Sort finished matches by finished_at timestamp (most recent first)
  const finishedMatches = matches
    .filter(m => m.status === "finished")
    .sort((a, b) => (b.finished_at || 0) - (a.finished_at || 0));

  return (
    <div className="app">
      <Header
        isConnected={isConnected}
        liveCount={liveMatches.length}
        lastUpdate={lastUpdate}
        onReconnect={reconnect}
      />

      {/* Global goal flash overlay */}
      {eventFlash && (
        <div className={`event-flash event-flash--${eventFlash.type?.includes("goal") ? "goal" : "card"}`}>
          {eventFlash.type?.includes("goal") ? "⚽" : "🟥"}{" "}
          {eventFlash.player_name} — {eventFlash.home_club_name}{" "}
          {eventFlash.home_goals}–{eventFlash.away_goals}{" "}
          {eventFlash.away_club_name}
        </div>
      )}

      <main className="main">
        {/* Live matches - now with stable ordering */}
        {liveMatches.length > 0 && (
          <section className="section">
            <h2 className="section-title">
              <span className="live-dot" /> Live now
              <span className="match-count">{liveMatches.length}</span>
            </h2>
            <div className="match-grid">
              {liveMatches.map(match => (
                <MatchCard key={match.game_id} match={match} isLive={true} />
              ))}
            </div>
          </section>
        )}

        {/* Finished matches - most recent first */}
        {finishedMatches.length > 0 && (
          <section className="section">
            <h2 className="section-title">
              Recently finished
              <span className="match-count">{finishedMatches.length}</span>
            </h2>
            <div className="match-grid">
              {finishedMatches.map(match => (
                <MatchCard key={match.game_id} match={match} isLive={false} />
              ))}
            </div>
          </section>
        )}

        {/* Empty state */}
        {matches.length === 0 && (
          <div className="empty-state">
            <div className="empty-icon">⚽</div>
            <h3>Waiting for matches to start...</h3>
            <p>Make sure the producer is running.</p>
            <code>python backend/producer/producer.py --speed 60</code>
          </div>
        )}
      </main>
    </div>
  );
}