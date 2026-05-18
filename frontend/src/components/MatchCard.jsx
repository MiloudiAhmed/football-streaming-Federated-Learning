// components/MatchCard.jsx
import { useState, useEffect, useRef } from "react";
import EventBadge from "./EventBadge";
import MatchTimer from "./MatchTimer";
import { GoalProbBadge } from "./FLStatus";

export default function MatchCard({ match, isLive }) {
  const prevGoals  = useRef({ home: 0, away: 0 });
  const [scoreAnim, setScoreAnim] = useState(null);
  const [isNew, setIsNew]         = useState(true);
  const [isLeaving, setIsLeaving] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => setIsNew(false), 600);
    return () => clearTimeout(t);
  }, []);

  useEffect(() => {
    const h = match.home_goals ?? 0;
    const a = match.away_goals ?? 0;

    if (h > prevGoals.current.home) {
      setScoreAnim("home");
      setTimeout(() => setScoreAnim(null), 1200);
    } else if (a > prevGoals.current.away) {
      setScoreAnim("away");
      setTimeout(() => setScoreAnim(null), 1200);
    }

    prevGoals.current = { home: h, away: a };
  }, [match.home_goals, match.away_goals]);

  useEffect(() => {
    if (!isLive) {
      const t = setTimeout(() => setIsLeaving(true), 200);
      return () => clearTimeout(t);
    }
  }, [isLive]);

  const {
    game_id,
    home_club_name      = "Home",
    away_club_name      = "Away",
    home_goals          = 0,
    away_goals          = 0,
    minute              = 0,
    competition_name    = "",
    competition_country = "",
    events              = [],
    status,
    last_event,
  } = match;

  const progress = Math.min((minute / 90) * 100, 100);
  const isFinished = status === "finished" || status === "finished_pending";
  const homeWon = isFinished && home_goals > away_goals;
  const awayWon = isFinished && away_goals > home_goals;
  const isDraw  = isFinished && home_goals === away_goals;

  // Short name for default view
  const shortName = (name) => {
    const shortened = name
      .replace("Football Club", "FC")
      .replace("Fussball Club", "FC")
      .replace("Futbolniy Klub", "FK")
      .replace("Fodbold Club", "FC")
      .replace("Association", "Ass.")
      .replace("Sporting Club", "SC")
      .replace("Athletic Club", "Athletic")
      .replace("Sport Club", "SC")
      .replace("de Fútbol S.A.D.", "")
      .replace("S.A.D.", "")
      .replace("S.L.", "")
      .replace("F.C.", "FC")
      .replace("A.F.C.", "AFC")
      .replace("A.C.", "AC")
      .trim();

    return shortened.length > 20 ? shortened.slice(0, 18) + "…" : shortened;
  };

  return (
    <article
      className={[
        "match-card",
        isLive    ? "match-card--live"    : "match-card--finished",
        isNew     ? "match-card--entering" : "",
        isLeaving ? "match-card--leaving"  : "",
        scoreAnim ? "match-card--goal"     : "",
        isHovered ? "match-card--expanded" : "",
      ].filter(Boolean).join(" ")}
      aria-label={`${home_club_name} vs ${away_club_name}`}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* ── Card header ── */}
      <header className="card-header">
        <span className="competition-name">
          {competition_country && `${competition_country} · `}
          {competition_name || "League Match"}
        </span>
        {isLive && <span className="live-badge">LIVE</span>}
        {(status === "finished" || status === "finished_pending") && <span className="finished-badge">FT</span>}
      </header>

      {/* ── Scoreboard ── */}
      <div className="scoreboard">
        <div className={`team team--home ${scoreAnim === "home" ? "team--scored" : ""} ${homeWon ? "team--winner" : awayWon ? "team--loser" : ""}`}>
          {/* Show full name on hover, short name otherwise */}
          <span className="team-name team-name--short">
            {isHovered ? home_club_name : shortName(home_club_name)}
          </span>
          <span className="team-score">{home_goals}</span>
          {homeWon && <span className="winner-trophy">🏆</span>}
        </div>

        <div className="score-divider">
          {isLive
            ? <MatchTimer minute={minute} />
            : <span className="vs-text">–</span>
          }
        </div>

        <div className={`team team--away ${scoreAnim === "away" ? "team--scored" : ""} ${awayWon ? "team--winner" : homeWon ? "team--loser" : ""}`}>
          <span className="team-score">{away_goals}</span>
          <span className="team-name team-name--short">
            {isHovered ? away_club_name : shortName(away_club_name)}
          </span>
          {awayWon && <span className="winner-trophy">🏆</span>}
        </div>
      </div>

      {/* ── Progress bar ── */}
      {isLive && (
        <div className="progress-track" title={`${minute}'`}>
          <div
            className="progress-bar"
            style={{ width: `${progress}%` }}
            role="progressbar"
            aria-valuenow={minute}
            aria-valuemax={90}
          />
        </div>
      )}

      {/* ── Last event preview ── */}
      {last_event && (
        <div className="last-event">
          <EventBadge type={last_event.type} size="sm" />
          <span className="last-event-text">
            {last_event.minute}' {last_event.player_name}
          </span>
        </div>
      )}

      {/* ── Event log ── */}
      {events.length > 0 && (
        <ul className="event-log" aria-label="Match events">
          {events.slice(0, 5).map((evt, i) => (
            <li key={evt.event_id || i} className="event-row">
              <span className="event-minute">{evt.minute}'</span>
              <EventBadge type={evt.type} />
              <span className="event-desc">
                {evt.player_name}
                {evt.player_in_name && ` ↔ ${evt.player_in_name}`}
              </span>
            </li>
          ))}
        </ul>
      )}

      {/* ── FL Goal Probability ── */}
      {isLive && <GoalProbBadge gameId={game_id} />}

    </article>
  );
}