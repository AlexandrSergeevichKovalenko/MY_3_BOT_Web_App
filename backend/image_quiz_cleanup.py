"""
Worker-safe image-quiz R2 cleanup.

Deletes R2 objects for image-quiz templates that have already been shown to at
least one user and have not been used for IMAGE_QUIZ_R2_CLEANUP_IDLE_DAYS days.
Templates that were never shown (use_count = 0) are never touched — they were
paid for and may still be dispatched to future users.

No import of backend_server — depends only on database and r2_storage.
"""
import logging
import os

from backend.database import (
    clear_image_quiz_template_r2_ref,
    list_exhausted_image_quiz_r2_objects,
)
from backend.r2_storage import r2_delete_object


def run_image_quiz_r2_cleanup() -> None:
    enabled = (os.getenv("IMAGE_QUIZ_R2_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Image-quiz R2 cleanup disabled by IMAGE_QUIZ_R2_CLEANUP_ENABLED")
        return

    idle_days = max(1, int((os.getenv("IMAGE_QUIZ_R2_CLEANUP_IDLE_DAYS") or "14").strip() or "14"))
    batch_limit = max(1, min(2000, int((os.getenv("IMAGE_QUIZ_R2_CLEANUP_BATCH_LIMIT") or "200").strip() or "200")))

    deleted_objects = 0
    missing_objects = 0
    cleared_rows = 0
    failed_objects = 0

    try:
        candidates = list_exhausted_image_quiz_r2_objects(
            limit=batch_limit,
            idle_days=idle_days,
        )
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
                    "❌ Failed to delete exhausted image-quiz R2 object: "
                    "template_id=%s object_key=%s",
                    template_id,
                    object_key,
                )

        logging.info(
            "✅ Image-quiz R2 cleanup finished: idle_days=%s candidates=%s "
            "deleted_objects=%s missing_objects=%s cleared_rows=%s failed_objects=%s",
            idle_days,
            len(candidates),
            deleted_objects,
            missing_objects,
            cleared_rows,
            failed_objects,
        )
    except Exception:
        logging.exception("❌ Image-quiz R2 cleanup failed")
        raise
