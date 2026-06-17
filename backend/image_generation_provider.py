import base64
import hashlib
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


load_dotenv(dotenv_path=Path(__file__).parent / ".env")

IMAGE_GENERATION_PROVIDER = str(
    os.getenv("IMAGE_GENERATION_PROVIDER") or "openai_gpt_image_1"
).strip().lower() or "openai_gpt_image_1"
IMAGE_GENERATION_MODEL = str(
    os.getenv("IMAGE_GENERATION_MODEL") or "gpt-image-1"
).strip() or "gpt-image-1"
IMAGE_GENERATION_SIZE = str(
    os.getenv("IMAGE_GENERATION_SIZE") or "1024x1024"
).strip() or "1024x1024"
IMAGE_GENERATION_QUALITY = str(
    os.getenv("IMAGE_GENERATION_QUALITY") or "medium"
).strip().lower() or "medium"
IMAGE_GENERATION_OUTPUT_FORMAT = str(
    os.getenv("IMAGE_GENERATION_OUTPUT_FORMAT") or "png"
).strip().lower() or "png"
IMAGE_GENERATION_MODERATION = str(
    os.getenv("IMAGE_GENERATION_MODERATION") or "auto"
).strip().lower() or "auto"
IMAGE_GENERATION_TIMEOUT_SECONDS = max(
    10.0,
    float(str(os.getenv("IMAGE_GENERATION_TIMEOUT_SECONDS") or "90").strip() or "90"),
)


def _model_dump(value):
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            return None
    if hasattr(value, "dict"):
        try:
            return value.dict()
        except Exception:
            return None
    return None


def _mime_type_for_output_format(output_format: str) -> str:
    normalized = str(output_format or "png").strip().lower() or "png"
    if normalized == "jpeg":
        return "image/jpeg"
    if normalized == "webp":
        return "image/webp"
    return "image/png"


def get_image_generation_provider_name() -> str:
    return IMAGE_GENERATION_PROVIDER


def _log_image_billing_event(*, action_type: str, user_id: int, quality: str) -> None:
    """Record one gpt-image-1 image in the cost ledger (best-effort, background) so
    image generation shows up in the per-activity cost breakdown — it used to be a
    blind spot. Priced per-image by quality via seeded 'gpt-image-1_<quality>' SKUs."""
    try:
        import threading
        import time as _time

        def _worker():
            try:
                from backend.database import log_billing_event
                tn = str(action_type or "image_generation").strip() or "image_generation"
                uid = int(user_id) if user_id and int(user_id) > 0 else None
                q = str(quality or "medium").strip().lower() or "medium"
                seed = f"img:{tn}:{_time.time_ns()}"
                log_billing_event(
                    idempotency_key=f"req:{seed}", user_id=uid, action_type=tn,
                    provider="openai", units_type="requests", units_value=1.0,
                    status="estimated", metadata={"model": IMAGE_GENERATION_MODEL, "kind": "image"},
                )
                log_billing_event(
                    idempotency_key=f"img:{seed}", user_id=uid, action_type=tn,
                    provider="openai", units_type="images", units_value=1.0,
                    price_provider="openai", price_sku=f"{IMAGE_GENERATION_MODEL}_{q}", price_unit="images",
                    status="estimated", metadata={"model": IMAGE_GENERATION_MODEL, "quality": q},
                )
            except Exception:
                logging.debug("image billing log failed task=%s", action_type, exc_info=True)

        threading.Thread(target=_worker, daemon=True).start()
    except Exception:
        pass


def generate_image_bytes(
    *,
    prompt: str,
    template_id: int,
    user_id: int,
    action_type: str = "image_generation",
) -> dict:
    normalized_prompt = str(prompt or "").strip()
    if not normalized_prompt:
        raise ValueError("image_prompt is required")
    if IMAGE_GENERATION_PROVIDER != "openai_gpt_image_1":
        raise RuntimeError(f"Unsupported image generation provider: {IMAGE_GENERATION_PROVIDER}")

    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    # SYNTHETIC_LOAD_MODE -> deterministic proxy (no network). Default OFF -> OpenAI(...).
    from backend.synthetic_load import build_sync_openai_client
    client = build_sync_openai_client(
        api_key=api_key,
        timeout=IMAGE_GENERATION_TIMEOUT_SECONDS,
    )
    response = client.images.generate(
        model=IMAGE_GENERATION_MODEL,
        prompt=normalized_prompt,
        size=IMAGE_GENERATION_SIZE,
        quality=IMAGE_GENERATION_QUALITY,
        output_format=IMAGE_GENERATION_OUTPUT_FORMAT,
        moderation=IMAGE_GENERATION_MODERATION,
        user=f"image_quiz:{int(user_id)}:{int(template_id)}",
    )
    data_items = list(getattr(response, "data", None) or [])
    if not data_items:
        raise RuntimeError("Image provider returned no image data")
    first_item = data_items[0]
    image_b64 = str(getattr(first_item, "b64_json", "") or "").strip()
    if not image_b64:
        raise RuntimeError("Image provider returned empty b64_json")
    try:
        image_bytes = base64.b64decode(image_b64)
    except Exception as exc:
        raise RuntimeError("Failed to decode generated image bytes") from exc
    if not image_bytes:
        raise RuntimeError("Generated image payload is empty")

    provider_meta = {
        "model": IMAGE_GENERATION_MODEL,
        "size": IMAGE_GENERATION_SIZE,
        "quality": IMAGE_GENERATION_QUALITY,
        "output_format": IMAGE_GENERATION_OUTPUT_FORMAT,
        "moderation": IMAGE_GENERATION_MODERATION,
        "created": getattr(response, "created", None),
        "usage": _model_dump(getattr(response, "usage", None)),
        "revised_prompt": str(getattr(first_item, "revised_prompt", "") or "").strip() or None,
        "prompt_sha1": hashlib.sha1(normalized_prompt.encode("utf-8")).hexdigest(),
    }
    _log_image_billing_event(action_type=action_type, user_id=int(user_id), quality=IMAGE_GENERATION_QUALITY)
    return {
        "provider_name": IMAGE_GENERATION_PROVIDER,
        "mime_type": _mime_type_for_output_format(IMAGE_GENERATION_OUTPUT_FORMAT),
        "data": image_bytes,
        "provider_meta": provider_meta,
    }
