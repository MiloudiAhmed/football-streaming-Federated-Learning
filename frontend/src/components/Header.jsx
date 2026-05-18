// components/Header.jsx
import { FLStatusPanel } from "./FLStatus";

export default function Header({ isConnected, liveCount, lastUpdate, onReconnect }) {
  const timeStr = lastUpdate
    ? lastUpdate.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
    : "—";

  return (
    <header className="app-header">
      <div className="header-brand">
        <span className="header-logo">⚽</span>
        <h1 className="header-title">LiveScore</h1>
        <span className="header-subtitle">Real-time match simulator</span>
      </div>

      <div className="header-status">
        {liveCount > 0 && (
          <span className="header-live-count">
            {liveCount} match{liveCount !== 1 ? "es" : ""} live
          </span>
        )}

         <FLStatusPanel />
         
        <div
          className={`connection-badge ${isConnected ? "connected" : "disconnected"}`}
          title={isConnected ? "WebSocket connected" : "Using polling fallback"}
        >
          
          <span className="connection-dot" />
          {isConnected ? "Live" : "Polling"}
        </div>

        {!isConnected && (
          <button className="reconnect-btn" onClick={onReconnect}>
            Reconnect
          </button>
        )}

        <span className="last-update">Updated {timeStr}</span>
      </div>
    </header>
  );
}
