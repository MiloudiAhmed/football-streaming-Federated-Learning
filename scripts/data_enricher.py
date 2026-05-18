"""
data_enricher.py
================
Loads all Kaggle CSVs and joins them into a single enriched structure.

Output:
  - data/processed/enriched_matches.json
  - data/processed/enriched_events.json

Fix applied:
  - Kaggle dataset uses "Goals", "Cards", "Substitutions" (capitalized/plural)
  - "Cards" is split into "yellow_card" or "red_card" using the description field
"""

import pandas as pd
import json
from pathlib import Path
from collections import Counter

# ── Path configuration ──────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
OUT_DIR  = Path(__file__).parent.parent / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_csv(filename: str) -> pd.DataFrame:
    """Load a CSV file and return a DataFrame. Print shape for sanity check."""
    path = DATA_DIR / filename
    if not path.exists():
        print(f"⚠️  Warning: {filename} not found. Creating empty DataFrame.")
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    print(f"  ✅ Loaded {filename}: {df.shape[0]:,} rows, {df.shape[1]} cols")
    return df


def build_lookup(df: pd.DataFrame, key_col: str, val_col: str) -> dict:
    """Create a fast lookup dict: {key → value} from a DataFrame."""
    if df.empty or key_col not in df.columns or val_col not in df.columns:
        return {}
    return dict(zip(df[key_col].astype(str), df[val_col].fillna("Unknown")))


def normalize_event_type(raw_type: str, description: str) -> str:
    """
    Convert Kaggle event types to our internal format.

    Kaggle uses:  "Goals", "Cards", "Substitutions", "Shootout"
    We want:      "goal",  "yellow_card"/"red_card", "substitution", "penalty"

    For "Cards" we look at the description to determine yellow vs red.
    """
    t = str(raw_type).strip().lower()
    d = str(description).strip().lower()

    if t in ("goals", "goal"):
        return "goal"

    elif t in ("cards", "card"):
        # Try to determine yellow vs red from description
        if any(word in d for word in ("red", "rouge", "rojo", "rosso")):
            return "red_card"
        elif any(word in d for word in ("yellow", "jaune", "amarillo", "giallo")):
            return "yellow_card"
        else:
            # Default to yellow — red cards are much rarer
            # The description field in this dataset often contains
            # "Yellow Card" or "Red Card" in English
            return "yellow_card"

    elif t in ("substitutions", "substitution", "sub", "subs"):
        return "substitution"

    elif t in ("shootout", "penalty", "penalties", "pen"):
        return "penalty"

    elif t == "own_goal" or "own goal" in d:
        return "own_goal"

    else:
        # Keep original but lowercase — log it so we know about it
        return t


def enrich_data():
    print("\n🔄 Loading raw CSV files...")

    # ── Load all tables ──────────────────────────────────────────────────────
    games        = load_csv("games.csv")
    game_events  = load_csv("game_events.csv")
    players      = load_csv("players.csv")
    clubs        = load_csv("clubs.csv")
    competitions = load_csv("competitions.csv")
    lineups      = load_csv("game_lineups.csv")
    appearances  = load_csv("appearances.csv")

    # ── Build lookup dictionaries ────────────────────────────────────────────
    print("\n🔗 Building lookup tables...")

    player_names     = build_lookup(players, "player_id", "name")
    club_names       = build_lookup(clubs,   "club_id",   "name")
    club_stadiums    = build_lookup(clubs,   "club_id",   "stadium_name")
    comp_names       = build_lookup(competitions, "competition_id", "name")
    comp_countries   = build_lookup(competitions, "competition_id", "country_name")
    player_positions = build_lookup(players, "player_id", "position")

    print(f"  Players: {len(player_names):,}")
    print(f"  Clubs:   {len(club_names):,}")
    print(f"  Comps:   {len(comp_names):,}")

    # ── Enrich: matches ──────────────────────────────────────────────────────
    print("\n⚽ Enriching matches...")

    if games.empty:
        print("  ⚠️  No games data found!")
        return

    games.columns = games.columns.str.lower().str.strip()

    enriched_matches = []
    for _, row in games.iterrows():
        game_id  = str(row.get("game_id", row.get("id", "")))
        home_id  = str(row.get("home_club_id", ""))
        away_id  = str(row.get("away_club_id", ""))
        comp_id  = str(row.get("competition_id", ""))
        date_str = str(row.get("date", ""))
        season   = str(row.get("season", ""))

        home_goals = row.get("home_club_goals", row.get("home_goals", 0))
        away_goals = row.get("away_club_goals", row.get("away_goals", 0))

        try:
            home_goals = int(float(home_goals)) if pd.notna(home_goals) else 0
            away_goals = int(float(away_goals)) if pd.notna(away_goals) else 0
        except (ValueError, TypeError):
            home_goals, away_goals = 0, 0

        enriched_matches.append({
            "game_id":             game_id,
            "date":                date_str,
            "season":              season,
            "home_club_id":        home_id,
            "away_club_id":        away_id,
            "home_club_name":      club_names.get(home_id, f"Club {home_id}"),
            "away_club_name":      club_names.get(away_id, f"Club {away_id}"),
            "home_stadium":        club_stadiums.get(home_id, "Unknown Stadium"),
            "competition_id":      comp_id,
            "competition_name":    comp_names.get(comp_id, "Unknown Competition"),
            "competition_country": comp_countries.get(comp_id, ""),
            "final_home_goals":    home_goals,
            "final_away_goals":    away_goals,
        })

    print(f"  ✅ Enriched {len(enriched_matches):,} matches")

    # ── Enrich: game events ──────────────────────────────────────────────────
    print("\n🎯 Enriching events...")

    if game_events.empty:
        print("  ⚠️  No events data found!")
        enriched_events = []
    else:
        game_events.columns = game_events.columns.str.lower().str.strip()

        # ── Print raw type distribution so we can see what the dataset has ──
        print("\n  📋 Raw event types found in dataset:")
        raw_types = game_events["type"].value_counts()
        for t, count in raw_types.items():
            print(f"     {str(t):30s}: {count:,}")

        match_index = {m["game_id"]: m for m in enriched_matches}

        enriched_events = []
        for _, row in game_events.iterrows():
            game_id     = str(row.get("game_id", ""))
            player_id   = str(row.get("player_id", ""))
            player_in   = str(row.get("player_in_id", row.get("player_assist_id", "")))
            club_id     = str(row.get("club_id", ""))
            description = str(row.get("description", "")).strip()

            # ── Minute parsing (handles "90+2" style) ───────────────────────
            minute_raw = row.get("minute", 0)
            try:
                parts  = str(minute_raw).split("+")
                minute = int(float(parts[0])) if pd.notna(minute_raw) else 0
                if len(parts) > 1:
                    minute += int(parts[1])
            except (ValueError, TypeError):
                minute = 0

            # ── Normalize event type ─────────────────────────────────────────
            raw_type   = str(row.get("type", "other"))
            event_type = normalize_event_type(raw_type, description)

            # ── Player name for substitution (player coming IN) ──────────────
            player_in_name = ""
            if player_in not in ("nan", "", "None"):
                player_in_name = player_names.get(player_in, "")

            # ── Build description if missing ─────────────────────────────────
            if not description or description == "nan":
                description = build_description(
                    event_type,
                    player_names.get(player_id, "Unknown Player"),
                    club_names.get(club_id, ""),
                    player_in_name,
                )

            enriched_events.append({
                "event_id":         str(row.get("game_event_id", row.get("id", ""))),
                "game_id":          game_id,
                "minute":           minute,
                "type":             event_type,
                "player_id":        player_id,
                "player_name":      player_names.get(player_id, "Unknown Player"),
                "player_position":  player_positions.get(player_id, ""),
                "player_in_name":   player_in_name,
                "club_id":          club_id,
                "club_name":        club_names.get(club_id, "Unknown Team"),
                "description":      description,
                "home_club_name":   match_index.get(game_id, {}).get("home_club_name", ""),
                "away_club_name":   match_index.get(game_id, {}).get("away_club_name", ""),
                "competition_name": match_index.get(game_id, {}).get("competition_name", ""),
            })

        print(f"\n  ✅ Enriched {len(enriched_events):,} events")

    # ── Write output ─────────────────────────────────────────────────────────
    print("\n💾 Saving processed data...")

    with open(OUT_DIR / "enriched_matches.json", "w") as f:
        json.dump(enriched_matches, f, indent=2, default=str)
    print(f"  ✅ enriched_matches.json")

    with open(OUT_DIR / "enriched_events.json", "w") as f:
        json.dump(enriched_events, f, indent=2, default=str)
    print(f"  ✅ enriched_events.json")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n📊 Normalized event type distribution:")
    types = Counter(e["type"] for e in enriched_events)
    for t, count in types.most_common(15):
        print(f"    {t:25s}: {count:,}")

    print("\n🎉 Enrichment complete! Ready for streaming.\n")
    return enriched_matches, enriched_events


def build_description(event_type: str, player_name: str, club_name: str, player_in: str) -> str:
    """Build a human-readable description when the CSV has none."""
    if event_type == "goal":
        return f"{player_name} scores for {club_name}!"
    elif event_type == "yellow_card":
        return f"{player_name} receives a yellow card."
    elif event_type == "red_card":
        return f"{player_name} is sent off with a red card!"
    elif event_type == "substitution":
        return f"{player_in} comes on for {player_name}." if player_in else f"{player_name} is substituted off."
    elif event_type == "own_goal":
        return f"{player_name} scores an own goal!"
    elif event_type == "penalty":
        return f"{player_name} scores from the penalty spot!"
    return f"{event_type.replace('_', ' ').title()} — {player_name}"


if __name__ == "__main__":
    enrich_data()