"""Remedial video ("тебе было сложно") — nightly assembly.

When a user scores at or below the weak-topic threshold on a single-topic game
(Wo-Frage / Artikel / Adjektiv sprint), the submit endpoint logs a weak-topic
event. This job runs once a night (off-peak): per user it picks the WEAKEST
topic, and — if a curated pool video exists and the caps pass — drops one
`format='video'` card into «Работа над ошибками» so it surfaces first next day.

No YouTube search here: videos come from the pool (filled by the /addvideo admin
command). Empty pool for a topic → simply no card.
"""
from __future__ import annotations

import logging
import os


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or str(default)).strip() or str(default))
    except Exception:
        return default


def materialize_remedial_video_for_user(user_id: int, topic_key: str, *, force: bool = False) -> bool:
    """Try to create one remedial video card for (user, weakest topic). Returns True
    if a card was created. Honors the global 7-day cap, the per-topic 90-day cap, and
    pool availability. `force=True` (admin /remedialtest) bypasses the caps so the card
    can be re-created on demand for testing."""
    from backend.database import (
        user_has_recent_remedial_video,
        topic_recently_sent_to_user,
        list_active_video_recommendations_for_focus,
        insert_remedial_video_card,
        record_video_send,
    )
    from backend.grammar_video_catalog import get_topic

    topic_key = str(topic_key or "").strip()
    if not topic_key:
        return False

    cooldown_days = _env_int("REMEDIAL_VIDEO_COOLDOWN_DAYS", 7)
    topic_cooldown_days = _env_int("REMEDIAL_VIDEO_TOPIC_COOLDOWN_DAYS", 90)

    if not force:
        # Global cap: at most one remedial video per user per week.
        if user_has_recent_remedial_video(user_id=int(user_id), days=cooldown_days):
            return False
        # Per-topic cap: don't repeat the same topic for a long while.
        if topic_recently_sent_to_user(user_id=int(user_id), topic_key=topic_key, days=topic_cooldown_days):
            return False

    # A fixed grammar topic has only a handful of curated clips — offer ALL of them as a
    # choice (different presenters/explanations), no per-video rotation. Empty pool → skip.
    videos = list_active_video_recommendations_for_focus(
        source_lang="ru", target_lang="de", skill_id=topic_key, limit=8,
    )
    if not videos:
        return False

    topic = get_topic(topic_key) or {}
    mistake_id = insert_remedial_video_card(
        user_id=int(user_id), topic_key=topic_key, videos=videos,
        topic_ru=str(topic.get("ru") or ""), topic_de=str(topic.get("de") or ""),
    )
    if not mistake_id:
        return False
    # Record the topic send (one marker is enough for the 90-day per-topic cap).
    record_video_send(user_id=int(user_id), video_id=str(videos[0].get("video_id") or ""), topic_key=topic_key)
    logging.info("remedial video card queued user_id=%s topic=%s videos=%d",
                 user_id, topic_key, len(videos))
    return True


def run_remedial_video_materialization() -> dict:
    """Nightly: read unprocessed weak-topic events, pick each user's weakest topic,
    try to queue a card, then mark ALL of that user's pending events processed."""
    from backend.database import (
        ensure_weak_topic_events_schema,
        fetch_pending_weak_topic_events,
        mark_weak_topic_events_processed,
    )
    ensure_weak_topic_events_schema()
    # Only act on events older than N hours so a startup catch-up (after a daytime
    # restart/redeploy) never hands out a video the same day the user struggled.
    min_age_hours = _env_int("REMEDIAL_VIDEO_MIN_AGE_HOURS", 8)
    events = fetch_pending_weak_topic_events(min_age_hours=min_age_hours)
    if not events:
        return {"users": 0, "cards_created": 0, "events": 0}

    by_user: dict[int, list[dict]] = {}
    for e in events:
        by_user.setdefault(int(e["user_id"]), []).append(e)

    created = 0
    processed_ids: list[int] = []
    for uid, evs in by_user.items():
        weakest = min(evs, key=lambda x: int(x.get("pct", 100)))
        try:
            if materialize_remedial_video_for_user(int(uid), str(weakest["topic_key"])):
                created += 1
        except Exception:
            logging.warning("remedial video materialize failed user_id=%s", uid, exc_info=True)
        processed_ids.extend(int(e["id"]) for e in evs)

    mark_weak_topic_events_processed(processed_ids)
    stats = {"users": len(by_user), "cards_created": created, "events": len(events)}
    logging.info("remedial video materialization: %s", stats)
    return stats
