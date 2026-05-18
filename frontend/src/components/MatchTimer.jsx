// components/MatchTimer.jsx
// =========================
// Shows the current match minute with a pulsing animation.
// Interpolates between server-reported minutes to look smooth.

import { useState, useEffect, useRef } from "react";

export default function MatchTimer({ minute = 0 }) {
  const [display, setDisplay] = useState(minute);
  const intervalRef = useRef(null);

  // When a new minute arrives from the server, tick up visually
  useEffect(() => {
    setDisplay(minute);

    // Clear any existing interval
    if (intervalRef.current) clearInterval(intervalRef.current);

    // Don't go past 90+5
    if (minute >= 95) return;

    // Tick up 1 per real second until next server update arrives
    intervalRef.current = setInterval(() => {
      setDisplay(prev => Math.min(prev + 1, 95));
    }, 1000);

    return () => clearInterval(intervalRef.current);
  }, [minute]);

  // Format: show 90+N for stoppage time
  const format = (m) => {
    if (m > 90) return `90+${m - 90}`;
    return `${m}`;
  };

  return (
    <div className="match-timer">
      <span className="timer-pulse" aria-hidden="true" />
      <span className="timer-text">{format(display)}'</span>
    </div>
  );
}
