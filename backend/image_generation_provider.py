import base64
import hashlib
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


def generate_image_bytes(
    *,
    prompt: str,
    template_id: int,
    user_id: int,
) -> dict:
    normalized_prompt = str(prompt or "").strip()
    if not normalized_prompt:
        raise ValueError("image_prompt is required")
    if IMAGE_GENERATION_PROVIDER != "openai_gpt_image_1":
        raise RuntimeError(f"Unsupported image generation provider: {IMAGE_GENERATION_PROVIDER}")

    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    client = OpenAI(
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
    return {
        "provider_name": IMAGE_GENERATION_PROVIDER,
        "mime_type": _mime_type_for_output_format(IMAGE_GENERATION_OUTPUT_FORMAT),
        "data": image_bytes,
        "provider_meta": provider_meta,
    }
