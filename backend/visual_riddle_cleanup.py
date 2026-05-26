"""
Worker-safe visual-riddle R2 cleanup.

Two-pass cleanup:
  Pass 1 — exhausted: templates dispatched at least once (use_count >= 1) and
    idle for VISUAL_RIDDLE_R2_CLEANUP_IDLE_DAYS days (default 30).
  Pass 2 — orphaned: templates never dispatched (use_count = 0) but older than
    VISUAL_RIDDLE_R2_CLEANUP_ORPHAN_AGE_DAYS days (default 90).

No import of backend_server — depends only on database and r2_storage.
"""
import logging
import os

from backend.database import (
    clear_visual_riddle_template_r2_ref,
    list_exhausted_visual_riddle_r2_objects,
    list_orphaned_visual_riddle_r2_objects,
)
from backend.r2_storage import r2_delete_object


def _delete_batch(candidates: list[dict]) -> tuple[int, int, int, int]:
    deleted = missing = cleared = failed = 0
    for item in candidates:
        template_id = int(item.get("template_id") or 0)
        object_key = str(item.get("object_key") or "").strip()
        if not template_id or not object_key:
            continue
        try:
            removed = bool(r2_delete_object(object_key))
            if removed:
                deleted += 1
            else:
                missing += 1
            if bool(clear_visual_riddle_template_r2_ref(template_id)):
                cleared += 1
        except Exception:
            failed += 1
            logging.exception(
                "Failed to delete visual-riddle R2 object template_id=%s object_key=%s",
                template_id,
                object_key,
            )
    return deleted, missing, cleared, failed


def run_visual_riddle_r2_cleanup() -> None:
    enabled = (os.getenv("VISUAL_RIDDLE_R2_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("visual-riddle R2 cleanup disabled by VISUAL_RIDDLE_R2_CLEANUP_ENABLED")
        return

    idle_days = max(1, int((os.getenv("VISUAL_RIDDLE_R2_CLEANUP_IDLE_DAYS") or "30").strip() or "30"))
    orphan_age_days = max(7, int((os.getenv("VISUAL_RIDDLE_R2_CLEANUP_ORPHAN_AGE_DAYS") or "90").strip() or "90"))
    batch_limit = max(1, min(2000, int((os.getenv("VISUAL_RIDDLE_R2_CLEANUP_BATCH_LIMIT") or "200").strip() or "200")))

    try:
        exhausted = list_exhausted_visual_riddle_r2_objects(limit=batch_limit, idle_days=idle_days)
        d1, m1, c1, f1 = _delete_batch(exhausted)

        orphaned = list_orphaned_visual_riddle_r2_objects(limit=batch_limit, min_age_days=orphan_age_days)
        d2, m2, c2, f2 = _delete_batch(orphaned)

        logging.info(
            "visual-riddle R2 cleanup finished: "
            "exhausted(idle_days=%s candidates=%s deleted=%s missing=%s cleared=%s failed=%s) "
            "orphaned(min_age_days=%s candidates=%s deleted=%s missing=%s cleared=%s failed=%s)",
            idle_days, len(exhausted), d1, m1, c1, f1,
            orphan_age_days, len(orphaned), d2, m2, c2, f2,
        )
    except Exception:
        logging.exception("visual-riddle R2 cleanup failed")
        raise
