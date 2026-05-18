// hooks/useWebSocket.js
// ======================
// Custom hook managing a WebSocket connection with auto-reconnect.

import { useState, useEffect, useRef, useCallback } from "react";

export function useWebSocket(url, onMessage) {
  const [isConnected, setIsConnected] = useState(false);
  const wsRef          = useRef(null);
  const reconnectTimer = useRef(null);
  const retryDelay     = useRef(1000);   // starts at 1s, doubles on failure

  const connect = useCallback(() => {
    // Clean up existing connection
    if (wsRef.current) {
      wsRef.current.onclose = null;   // prevent double-reconnect
      wsRef.current.close();
    }

    console.log(`🔌 Connecting to ${url}...`);
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log("✅ WebSocket connected");
      setIsConnected(true);
      retryDelay.current = 1000;   // reset backoff on success
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type !== "PING") {
          onMessage(data);
        }
      } catch (e) {
        console.error("WS parse error:", e);
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      console.warn(`⚠️ WebSocket closed. Reconnecting in ${retryDelay.current}ms...`);

      // Exponential backoff: 1s → 2s → 4s → max 30s
      reconnectTimer.current = setTimeout(() => {
        retryDelay.current = Math.min(retryDelay.current * 2, 30000);
        connect();
      }, retryDelay.current);
    };

    ws.onerror = (err) => {
      console.error("WS error:", err);
      ws.close();
    };
  }, [url, onMessage]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
      }
    };
  }, [connect]);

  return {
    isConnected,
    reconnect: connect,
  };
}
