"""
app.py  —  FastAPI Backend
==========================
Consumes Kafka topics and serves:
  - WebSocket  ws://localhost:8000/ws
  - REST GET   /api/matches
  - REST GET   /api/matches/{game_id}
  - REST GET   /api/matches/{game_id}/events
  - REST GET   /health
  - FL routes  /api/ml/...
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS    = "match_events"
TOPIC_STATUS    = "match_status"
RECENT_KEEP_SEC = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ── In-memory state store ──────────────────────────────────────────────────────
class MatchStore:
    def __init__(self):
        self.active: Dict[str, dict] = {}
        self.recent: Dict[str, dict] = {}
        self.events: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))

    def start_match(self, data: dict):
        game_id = data["game_id"]
        self.active[game_id] = {
            **data,
            "events":     [],
            "started_at": time.time(),
        }
        log.info(f"🟢 Match started: {data.get('home_club_name')} vs {data.get('away_club_name')}")

    def update_match(self, data: dict):
        game_id = data["game_id"]
        if game_id in self.active:
            self.active[game_id].update({
                "minute":     data.get("minute", 0),
                "home_goals": data.get("home_goals", 0),
                "away_goals": data.get("away_goals", 0),
                "timestamp":  data.get("timestamp"),
            })

    def add_event(self, data: dict):
        game_id = data["game_id"]
        self.events[game_id].appendleft(data)
        if game_id in self.active:
            self.active[game_id]["home_goals"] = data.get("home_goals", 0)
            self.active[game_id]["away_goals"] = data.get("away_goals", 0)
            self.active[game_id]["minute"]     = data.get("minute", 0)
            self.active[game_id]["last_event"] = data

    def end_match(self, data: dict):
        game_id = data["game_id"]
        if game_id in self.active:
            # Step 1 — Update final score, show winner animation
            # Keep in active dict with finished_pending status
            self.active[game_id]["home_goals"] = data.get("home_goals", 0)
            self.active[game_id]["away_goals"] = data.get("away_goals", 0)
            self.active[game_id]["minute"]     = 90
            self.active[game_id]["status"]     = "finished_pending"

            log.info(
                f"🏆 Winner animation: "
                f"{self.active[game_id].get('home_club_name')} "
                f"{self.active[game_id]['home_goals']}-"
                f"{self.active[game_id]['away_goals']} "
                f"{self.active[game_id].get('away_club_name')}"
            )

            # Step 2 — Move to recently finished after 8 seconds
            # Done in a separate thread so Kafka consumer is not blocked
            import threading
            def move_to_finished():
                time.sleep(8)
                if game_id in self.active:
                    match = self.active.pop(game_id)
                    match.update({
                        "status":      "finished",
                        "finished_at": time.time(),
                        "home_goals":  data.get("home_goals", match.get("home_goals", 0)),
                        "away_goals":  data.get("away_goals", match.get("away_goals", 0)),
                    })
                    self.recent[game_id] = match
                    log.info(
                        f"🔴 Moved to finished: "
                        f"{match.get('home_club_name')} "
                        f"{match.get('home_goals')}-"
                        f"{match.get('away_goals')} "
                        f"{match.get('away_club_name')}"
                    )

            threading.Thread(target=move_to_finished, daemon=True).start()

    def cleanup_recent(self):
        now = time.time()
        to_remove = [
            gid for gid, m in self.recent.items()
            if now - m.get("finished_at", now) > RECENT_KEEP_SEC
        ]
        for gid in to_remove:
            del self.recent[gid]

    def get_all_matches(self) -> List[dict]:
        self.cleanup_recent()
        result = []
        for game_id, match in self.active.items():
            # finished_pending stays in Live section but shows as finished
            # so winner animation plays while card is still visible
            display_status = match.get("status", "live")
            result.append({
                **match,
                "events": list(self.events[game_id])[:10],
                "status": display_status,
            })
        for game_id, match in self.recent.items():
            result.append({
                **match,
                "events": list(self.events[game_id])[:10],
                "status": "finished",
            })
        return result


store   = MatchStore()


class ConnectionManager:
    def __init__(self):
        self.connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self.connections.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.connections:
            try:
                await ws.send_text(json.dumps(data, default=str))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.connections.remove(ws)


manager = ConnectionManager()


def kafka_consumer_thread(loop: asyncio.AbstractEventLoop):
    log.info("🔄 Starting Kafka consumer thread...")
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                TOPIC_EVENTS,
                TOPIC_STATUS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="football-api-consumer",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            log.info("✅ Kafka consumer connected!")
            break
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka not ready, attempt {attempt+1}/15...")
            time.sleep(3)
    else:
        log.error("❌ Could not connect Kafka consumer.")
        return

    for message in consumer:
        data  = message.value
        topic = message.topic
        try:
            if topic == TOPIC_STATUS:
                msg_type = data.get("type", "")
                if msg_type == "MATCH_START":
                    store.start_match(data)
                elif msg_type == "LIVE_UPDATE":
                    store.update_match(data)
                elif msg_type == "MATCH_END":
                    store.end_match(data)
            elif topic == TOPIC_EVENTS:
                store.add_event(data)

            all_matches = store.get_all_matches()
            asyncio.run_coroutine_threadsafe(
                manager.broadcast({
                    "type":    "STATE_UPDATE",
                    "matches": all_matches,
                    "event":   data,
                }),
                loop,
            )
        except Exception as e:
            log.error(f"Error processing message: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import threading
    loop   = asyncio.get_event_loop()
    thread = threading.Thread(
        target=kafka_consumer_thread,
        args=(loop,),
        daemon=True,
        name="kafka-consumer",
    )
    thread.start()
    log.info("🚀 Football Streaming API started!")
    yield
    log.info("🛑 Shutting down...")


app = FastAPI(
    title="Football Streaming API",
    description="Real-time football match simulation via Kafka",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {
        "status":           "ok",
        "active_matches":   len(store.active),
        "finished_matches": len(store.recent),
    }


@app.get("/api/matches")
async def get_all_matches():
    return {"matches": store.get_all_matches()}


@app.get("/api/matches/{game_id}")
async def get_match(game_id: str):
    match = store.active.get(game_id) or store.recent.get(game_id)
    if not match:
        return {"error": "Match not found"}
    return {**match, "events": list(store.events[game_id])}


@app.get("/api/matches/{game_id}/events")
async def get_events(game_id: str, limit: int = 20):
    return {
        "game_id": game_id,
        "events":  list(store.events[game_id])[:limit],
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_text(json.dumps({
            "type":    "INITIAL_STATE",
            "matches": store.get_all_matches(),
        }, default=str))
        while True:
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"type": "PING"}))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        log.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# ── FL routes ──────────────────────────────────────────────────────────────────
try:
    from fl_api_routes import fl_router
    app.include_router(fl_router)
    log.info("✅ FL routes loaded: /api/ml/status, /api/ml/prediction/{id}")
except ImportError:
    log.warning("⚠️  FL routes not available")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)