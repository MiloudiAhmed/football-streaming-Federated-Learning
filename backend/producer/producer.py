"""
producer.py
===========
The heart of the simulation engine.

What it does:
  1. Loads enriched match + event data
  2. Schedules concurrent matches with staggered kickoffs
  3. As soon as one match ends, a new one starts immediately
  4. Publishes to Kafka topics:
       - match_status  → START / LIVE_UPDATE / END events
       - match_events  → individual goals, cards, substitutions

Usage:
  python producer.py [--speed 60] [--matches-per-batch 16] [--loop]
"""

import json
import time
import threading
import random
import argparse
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS    = "match_events"
TOPIC_STATUS    = "match_status"
DATA_DIR        = Path(__file__).parent.parent.parent / "data" / "processed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ── Kafka producer setup ───────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
            )
            log.info("✅ Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka not ready, attempt {attempt+1}/10. Retrying in 3s...")
            time.sleep(3)
    raise ConnectionError("❌ Could not connect to Kafka after 10 attempts.")


# ── Data loading ───────────────────────────────────────────────────────────────
def load_data():
    matches_path = DATA_DIR / "enriched_matches.json"
    events_path  = DATA_DIR / "enriched_events.json"

    if not matches_path.exists():
        raise FileNotFoundError(
            f"❌ {matches_path} not found.\n"
            "Run: python scripts/data_enricher.py first!"
        )

    with open(matches_path) as f:
        matches = json.load(f)
    with open(events_path) as f:
        events = json.load(f)

    events_by_game = defaultdict(list)
    for event in events:
        events_by_game[event["game_id"]].append(event)

    for game_id in events_by_game:
        events_by_game[game_id].sort(key=lambda e: e["minute"])

    log.info(f"📦 Loaded {len(matches):,} matches, {len(events):,} events")
    return matches, events_by_game


# ── Match simulator ────────────────────────────────────────────────────────────
class MatchSimulator(threading.Thread):
    """
    Simulates a single football match in its own thread.

    Lifecycle:
      1. Publish START event
      2. Walk through events minute by minute
      3. Publish LIVE_UPDATE every 5 simulated minutes
      4. Wait 3 seconds after last minute (so frontend shows final score)
      5. Publish END event
    """

    def __init__(
        self,
        match: dict,
        events: list,
        producer: KafkaProducer,
        speed_factor: float = 60,
    ):
        super().__init__(daemon=True)
        self.match          = match
        self.events         = events
        self.producer       = producer
        self.speed_factor   = speed_factor
        self.home_goals     = 0
        self.away_goals     = 0
        self.current_minute = 0
        self.is_running     = True

    def publish(self, topic: str, data: dict):
        game_id = self.match["game_id"]
        self.producer.send(topic, key=game_id, value=data)

    def run(self):
        match   = self.match
        game_id = match["game_id"]
        home    = match["home_club_name"]
        away    = match["away_club_name"]

        log.info(f"🟢 STARTING: {home} vs {away} (game_id={game_id})")

        # ── START event ──────────────────────────────────────────────────────
        self.publish(TOPIC_STATUS, {
            "type":                "MATCH_START",
            "game_id":             game_id,
            "timestamp":           datetime.utcnow().isoformat(),
            "home_club_name":      home,
            "away_club_name":      away,
            "home_club_id":        match["home_club_id"],
            "away_club_id":        match["away_club_id"],
            "competition_name":    match.get("competition_name", ""),
            "competition_country": match.get("competition_country", ""),
            "stadium":             match.get("home_stadium", ""),
            "season":              match.get("season", ""),
            "home_goals":          0,
            "away_goals":          0,
            "minute":              0,
            "status":              "live",
        })

        seconds_per_minute = 60.0 / self.speed_factor
        event_idx = 0

        # ── Simulate minute by minute ────────────────────────────────────────
        for minute in range(1, 96):
            self.current_minute = minute

            while event_idx < len(self.events) and self.events[event_idx]["minute"] <= minute:
                evt = self.events[event_idx]
                self._fire_event(evt, minute)
                event_idx += 1

            if minute % 5 == 0 or minute == 1:
                self.publish(TOPIC_STATUS, {
                    "type":           "LIVE_UPDATE",
                    "game_id":        game_id,
                    "timestamp":      datetime.utcnow().isoformat(),
                    "minute":         minute,
                    "home_goals":     self.home_goals,
                    "away_goals":     self.away_goals,
                    "home_club_name": home,
                    "away_club_name": away,
                    "status":         "live",
                })

            time.sleep(seconds_per_minute)

        # ── Wait so frontend can display final score before match ends ───────
        # This prevents the card moving to "finished" before showing last goal
        time.sleep(3)

        # ── END event ────────────────────────────────────────────────────────
        log.info(f"🔴 FINISHED: {home} {self.home_goals}-{self.away_goals} {away}")
        self.publish(TOPIC_STATUS, {
            "type":           "MATCH_END",
            "game_id":        game_id,
            "timestamp":      datetime.utcnow().isoformat(),
            "minute":         90,
            "home_goals":     self.home_goals,
            "away_goals":     self.away_goals,
            "home_club_name": home,
            "away_club_name": away,
            "status":         "finished",
        })
        self.producer.flush()

    def _fire_event(self, event: dict, current_minute: int):
        game_id    = self.match["game_id"]
        event_type = event["type"]

        if "goal" in event_type:
            scorer_club = event.get("club_id", "")
            if scorer_club == self.match["home_club_id"]:
                self.home_goals += 1
            else:
                self.away_goals += 1

        description = event.get("description", "")
        if not description:
            description = self._build_description(event)

        payload = {
            **event,
            "type":           event_type,
            "game_id":        game_id,
            "timestamp":      datetime.utcnow().isoformat(),
            "minute":         current_minute,
            "home_goals":     self.home_goals,
            "away_goals":     self.away_goals,
            "description":    description,
            "home_club_name": self.match["home_club_name"],
            "away_club_name": self.match["away_club_name"],
        }

        self.publish(TOPIC_EVENTS, payload)

        log.info(
            f"  ⚡ [{game_id[:8]}] {current_minute}' "
            f"{event_type.upper()}: {event.get('player_name', '?')} "
            f"({self.match['home_club_name']} "
            f"{self.home_goals}-{self.away_goals} "
            f"{self.match['away_club_name']})"
        )

    def _build_description(self, event: dict) -> str:
        t    = event.get("type", "").lower()
        name = event.get("player_name", "Unknown Player")
        club = event.get("club_name", "")
        inn  = event.get("player_in_name", "")

        if "goal" in t:
            return f"{name} scores for {club}!"
        elif t == "yellow_card":
            return f"{name} receives a yellow card."
        elif t == "red_card":
            return f"{name} is sent off with a red card!"
        elif t == "substitution" and inn:
            return f"{inn} comes on for {name}."
        elif t == "substitution":
            return f"Substitution: {name} off."
        return f"{t.replace('_', ' ').title()} – {name}"


# ── Match scheduler (FIXED VERSION) ────────────────────────────────────────────
class MatchScheduler:
    """
    Schedules matches continuously with infinite match pool.
    Each worker thread runs forever, cycling through matches.
    Staggered kickoffs prevent all matches from ending simultaneously.
    """

    def __init__(
        self,
        matches: list,
        events_by_game: dict,
        producer: KafkaProducer,
        speed_factor: float = 60,
        matches_per_batch: int = 16,  # CHANGED: default to 16 matches
        loop: bool = True,
    ):
        self.matches           = matches
        self.events_by_game    = events_by_game
        self.producer          = producer
        self.speed_factor      = speed_factor
        self.matches_per_batch = matches_per_batch
        self.loop              = loop

    def run(self):
        # Filter to well-known clubs only
        playable = [
            m for m in self.matches
            if not m["home_club_name"].startswith("Club ")
            and not m["away_club_name"].startswith("Club ")
            and m["game_id"] in self.events_by_game
        ]

        if not playable:
            log.warning("⚠️  No playable matches found!")
            return

        log.info(f"🎮 {len(playable)} playable matches loaded.")
        random.shuffle(playable)

        # Infinite pool — keeps cycling through matches forever
        match_pool = list(playable)
        pool_index = [0]  # use list so closure can modify it
        pool_lock  = threading.Lock()

        def get_next_match():
            with pool_lock:
                if pool_index[0] >= len(match_pool):
                    if self.loop:
                        random.shuffle(match_pool)
                        pool_index[0] = 0
                        log.info("🔄 Loop enabled — restarting match pool")
                    else:
                        return None
                match = match_pool[pool_index[0]]
                pool_index[0] += 1
                return match

        def worker(initial_delay=0):
            """
            Each worker thread runs forever:
            sleep delay → run match → get next → run match → repeat
            """
            time.sleep(initial_delay)
            while True:
                match = get_next_match()
                if not match:
                    log.info("🏁 No more matches and loop disabled. Worker stopping.")
                    break
                    
                events = self.events_by_game.get(match["game_id"], [])
                log.info(
                    f"🟢 Starting: {match['home_club_name']} vs "
                    f"{match['away_club_name']}"
                )
                sim = MatchSimulator(
                    match=match,
                    events=events,
                    producer=self.producer,
                    speed_factor=self.speed_factor,
                )
                sim.run()
                log.info(
                    f"🔴 Done: {match['home_club_name']} vs "
                    f"{match['away_club_name']}. Getting next..."
                )

        log.info(f"\n{'='*60}")
        log.info(f"🏟️  Starting {self.matches_per_batch} permanent worker threads")
        log.info(f"{'='*60}")

        threads = []
        for i in range(self.matches_per_batch):
            # Stagger kickoffs: each worker starts 8 sim-minutes apart
            delay = i * (8 * 60 / self.speed_factor)
            log.info(f"  Worker {i+1} starts in {delay:.1f}s")
            t = threading.Thread(
                target=worker,
                args=(delay,),
                daemon=True,
                name=f"match-worker-{i+1}"
            )
            t.start()
            threads.append(t)

        # Keep main thread alive forever
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("⛔ Scheduler stopped.")


# ── CLI entry point ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Football streaming producer")
    parser.add_argument("--speed",   type=float, default=60, help="Speed factor (default: 60)")
    parser.add_argument("--matches", type=int,   default=16, help="Number of concurrent matches (default: 16)")
    parser.add_argument("--loop",    action="store_true", default=True, help="Loop matches indefinitely")
    parser.add_argument("--no-loop", action="store_false", dest="loop", help="Don't loop matches")
    args = parser.parse_args()

    log.info(f"🚀 Football Streaming Producer starting...")
    log.info(f"   Speed factor:   {args.speed}x")
    log.info(f"   Matches/batch:  {args.matches}")
    log.info(f"   Loop:           {args.loop}")

    producer = create_producer()
    matches, events_by_game = load_data()

    scheduler = MatchScheduler(
        matches=matches,
        events_by_game=events_by_game,
        producer=producer,
        speed_factor=args.speed,
        matches_per_batch=args.matches,
        loop=args.loop,
    )

    try:
        scheduler.run()
    except KeyboardInterrupt:
        log.info("\n⛔ Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        log.info("👋 Producer shutdown complete.")


if __name__ == "__main__":
    main()