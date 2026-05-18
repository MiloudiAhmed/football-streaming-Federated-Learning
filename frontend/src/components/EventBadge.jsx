// components/EventBadge.jsx
// =========================
// Colored badge for goal / yellow card / red card / substitution

export default function EventBadge({ type = "", size = "md" }) {
  const t = type.toLowerCase().trim();

  let emoji = "•";
  let className = "badge badge--other";

  if (t.includes("goal") || t === "goals") {
    emoji = "⚽";
    className = "badge badge--goal";
  } else if (
    t === "yellow_card" || t === "yellowcard" ||
    t === "yellow card" || t === "yellow" ||
    t === "yellowcards" || t === "card_yellow"
  ) {
    emoji = "🟨";
    className = "badge badge--yellow";
  } else if (
    t === "red_card" || t === "redcard" ||
    t === "red card" || t === "red" ||
    t === "redcards" || t === "card_red"
  ) {
    emoji = "🟥";
    className = "badge badge--red";
  } else if (
    t === "substitution" || t === "sub" ||
    t === "subst" || t === "subs" ||
    t === "substitutions"
  ) {
    emoji = "🔄";
    className = "badge badge--sub";
  } else if (t === "own_goal" || t === "own goal" || t === "owngoal") {
    emoji = "⚽";
    className = "badge badge--own-goal";
  } else if (t === "penalty" || t === "pen" || t === "penalties") {
    emoji = "🎯";
    className = "badge badge--goal";
  } else if (t === "var" || t === "var_goal" || t === "var_card") {
    emoji = "📺";
    className = "badge badge--other";
  } else {
    // Log unknown types in the browser console so you can add them
    console.log("Unknown event type:", type);
    emoji = "•";
    className = "badge badge--other";
  }

  return (
    <span className={`${className} badge--${size}`} aria-label={type}>
      {emoji}
    </span>
  );
}