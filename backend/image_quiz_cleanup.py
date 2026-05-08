"""
Worker-safe image-quiz R2 cleanup.

Two-pass cleanup:
  Pass 1 — exhausted: templates shown to at least one user (use_count >= 1) and
    idle for IMAGE_QUIZ_R2_CLEANUP_IDLE_DAYS days (default 14).
  Pass 2 — orphaned: templates never dispatched (use_count = 0) but older than
    IMAGE_QUIZ_R2_CLEANUP_ORPHAN_AGE_DAYS days (default 30).  These accumulate
    when images are generated for users who never returned.

No import of backend_server — depends only on database and r2_storage.
"""
import logging
import os

from backend.database import (
    clear_image_quiz_template_r2_ref,
    list_exhausted_image_quiz_r2_objects,
    list_orphaned_image_quiz_r2_objects,
)
from backend.r2_storage import r2_delete_object


def _delete_batch(candidates: list[dict]) -> tuple[int, int, int, int]:
    """Delete a batch of R2 objects and clear their DB refs. Returns (deleted, missing, cleared, failed)."""
    deleted_objects = 0
    missing_objects = 0
    cleared_rows = 0
    failed_objects = 0
    for item in candidates:
        template_id = int(item.get("template_id") or 0)
        object_key = str(item.get("object_key") or "").strip()
        if not template_id or not object_key:
            continue
        try:
            removed = bool(r2_delete_object(object_key))
            if removed:
                deleted_objects += 1
            else:
                missing_objects += 1
            if bool(clear_image_quiz_template_r2_ref(template_id)):
                cleared_rows += 1
        except Exception:
            failed_objects += 1
            logging.exception(
                "❌ Failed to delete image-quiz R2 object: "
                "template_id=%s object_key=%s",
                template_id,
                object_key,
            )
    return deleted_objects, missing_objects, cleared_rows, failed_objects


def run_image_quiz_r2_cleanup() -> None:
    enabled = (os.getenv("IMAGE_QUIZ_R2_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Image-quiz R2 cleanup disabled by IMAGE_QUIZ_R2_CLEANUP_ENABLED")
        return

    idle_days = max(1, int((os.getenv("IMAGE_QUIZ_R2_CLEANUP_IDLE_DAYS") or "14").strip() or "14"))
    orphan_age_days = max(7, int((os.getenv("IMAGE_QUIZ_R2_CLEANUP_ORPHAN_AGE_DAYS") or "180").strip() or "180"))
    batch_limit = max(1, min(2000, int((os.getenv("IMAGE_QUIZ_R2_CLEANUP_BATCH_LIMIT") or "200").strip() or "200")))

    try:
        # Pass 1: shown images idle for idle_days
        exhausted = list_exhausted_image_quiz_r2_objects(limit=batch_limit, idle_days=idle_days)
        d1, m1, c1, f1 = _delete_batch(exhausted)

        # Pass 2: orphaned images (use_count=0, never dispatched) older than orphan_age_days
        orphaned = list_orphaned_image_quiz_r2_objects(limit=batch_limit, min_age_days=orphan_age_days)
        d2, m2, c2, f2 = _delete_batch(orphaned)

        logging.info(
            "✅ Image-quiz R2 cleanup finished: "
            "exhausted(idle_days=%s candidates=%s deleted=%s missing=%s cleared=%s failed=%s) "
            "orphaned(min_age_days=%s candidates=%s deleted=%s missing=%s cleared=%s failed=%s)",
            idle_days, len(exhausted), d1, m1, c1, f1,
            orphan_age_days, len(orphaned), d2, m2, c2, f2,
        )
    except Exception:
        logging.exception("❌ Image-quiz R2 cleanup failed")
        raise
