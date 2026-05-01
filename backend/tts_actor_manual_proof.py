import json
import time
from uuid import uuid4

from backend.background_jobs import run_tts_generation_actor
from backend.database import create_tts_object_pending, get_tts_object_meta
from backend.job_queue import claim_tts_generation_in_flight, get_redis_client
from backend.r2_storage import r2_exists
from backend.tts_generation import _tts_object_key


def main() -> None:
    ts_ms = int(time.time() * 1000)
    proof_suffix = uuid4().hex[:10]
    cache_key = f"ttsproofactor{ts_ms}{proof_suffix}"
    object_key = _tts_object_key("de", "de-DE-Neural2-C", cache_key)
    request_id = f"tts_actor_proof_{proof_suffix}"
    correlation_id = f"tts_actor_proof_{proof_suffix}"
    normalized_text = f"Hallo das ist ein Test {proof_suffix}."

    create_tts_object_pending(
        cache_key=cache_key,
        language="de-DE",
        voice="de-DE-Neural2-C",
        speed=0.95,
        source_text=normalized_text,
        object_key=object_key,
    )
    claimed = bool(claim_tts_generation_in_flight(cache_key))
    if not claimed:
        print(json.dumps({"ok": False, "error": "duplicate_in_flight", "cache_key": cache_key}))
        return

    message = run_tts_generation_actor.send(
        cache_key=cache_key,
        user_id=0,
        language="de-DE",
        tts_lang_short="de",
        voice="de-DE-Neural2-C",
        speaking_rate=0.95,
        normalized_text=normalized_text,
        object_key=object_key,
        had_existing_meta=False,
        correlation_id=correlation_id,
        request_id=request_id,
        enqueue_ts_ms=ts_ms,
    )

    final_meta = None
    for _ in range(36):
        meta = get_tts_object_meta(cache_key, touch_hit=False) or {}
        status = str(meta.get("status") or "").strip().lower()
        if status in {"ready", "failed"}:
            final_meta = meta
            break
        time.sleep(5)
    if final_meta is None:
        final_meta = get_tts_object_meta(cache_key, touch_hit=False) or {}

    redis_claim_released = True
    client = get_redis_client()
    if client is not None:
        redis_claim_released = not bool(client.exists(f"tts:in_flight:{cache_key}"))

    final_status = str(final_meta.get("status") or "").strip() or "missing"
    r2_object_written = False
    if object_key and final_status == "ready":
        try:
            r2_object_written = bool(r2_exists(object_key))
        except Exception:
            r2_object_written = False

    print(
        json.dumps(
            {
                "ok": True,
                "cache_key": cache_key,
                "object_key": object_key,
                "request_id": request_id,
                "correlation_id": correlation_id,
                "message_id": str(getattr(message, "message_id", "") or ""),
                "actor_job_enqueued": True,
                "final_status": final_status,
                "db_meta": final_meta,
                "r2_object_written": r2_object_written,
                "redis_claim_released": redis_claim_released,
            }
        )
    )


if __name__ == "__main__":
    main()
