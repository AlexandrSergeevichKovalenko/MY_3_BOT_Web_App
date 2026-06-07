"""Synthetic load-testing provider layer (Phase 1 infrastructure).

When SYNTHETIC_LOAD_MODE is enabled, paid external providers (OpenAI, Google TTS,
YouTube transcript) are replaced with deterministic in-process fakes so realistic
load tests can exercise the *real* infrastructure (PostgreSQL, Redis, PgBouncer,
Dramatiq, billing-event writes, caches, feature limits, telemetry) without
spending any external API budget.

Design:
  * One env flag (`SYNTHETIC_LOAD_MODE`) gates everything; default OFF means the
    real providers are used and production behaviour is byte-for-byte unchanged.
  * OpenAI is intercepted at the client object via build_async_openai_client() /
    build_sync_openai_client(); a path-dispatching proxy returns schema-valid fake
    responses for chat/responses/images/embeddings/audio and harmless stubs for
    anything else — so no call ever reaches the network.
  * YouTube and Google TTS are intercepted at their single fetch/synthesize
    boundaries via helper guards.

NOTHING here mocks the database, Redis, billing, caches, or queues — only paid
external APIs are faked.
"""

from __future__ import annotations

import hashlib
import json
import os
from types import SimpleNamespace
from typing import Any


def is_synthetic_load_mode() -> bool:
    """True only when SYNTHETIC_LOAD_MODE is explicitly enabled. Default False."""
    return (os.getenv("SYNTHETIC_LOAD_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}


# --------------------------------------------------------------------------- #
# Deterministic fake payload builders
# --------------------------------------------------------------------------- #

def _deterministic_seed(*parts: Any) -> int:
    raw = "|".join(str(p) for p in parts).encode("utf-8", "ignore")
    return int.from_bytes(hashlib.sha256(raw).digest()[:4], "big")


def _last_user_text(messages: Any) -> str:
    if not isinstance(messages, (list, tuple)):
        return ""
    for msg in reversed(list(messages)):
        if isinstance(msg, dict) and msg.get("role") == "user":
            content = msg.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):  # vision-style content parts
                for part in content:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        return part["text"]
    return ""


def fake_chat_completion(model: str = "synthetic-gpt", messages: Any = None, **kwargs: Any) -> SimpleNamespace:
    """Schema-valid fake of an OpenAI ChatCompletion object (deterministic)."""
    user_text = _last_user_text(messages)
    seed = _deterministic_seed(model, user_text)
    content = json.dumps(
        {"synthetic": True, "model": model, "echo": user_text[:200], "seed": seed},
        ensure_ascii=False,
    )
    prompt_tokens = max(1, len(user_text) // 4)
    completion_tokens = max(1, len(content) // 4)
    message = SimpleNamespace(
        role="assistant",
        content=content,
        tool_calls=None,
        function_call=None,
        parsed=None,
        refusal=None,
    )
    choice = SimpleNamespace(index=0, message=message, finish_reason="stop", logprobs=None)
    usage = SimpleNamespace(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
    )
    completion = SimpleNamespace(
        id=f"synthetic-{seed:08x}",
        object="chat.completion",
        created=0,
        model=model,
        choices=[choice],
        usage=usage,
        system_fingerprint="synthetic",
    )
    completion.model_dump = lambda: {  # type: ignore[attr-defined]
        "id": completion.id,
        "model": model,
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": content}}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                  "total_tokens": prompt_tokens + completion_tokens},
    }
    return completion


def fake_image_response(prompt: str = "", **kwargs: Any) -> SimpleNamespace:
    seed = _deterministic_seed("image", prompt)
    # 1x1 transparent PNG, base64 — valid, tiny, deterministic.
    b64 = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )
    return SimpleNamespace(created=0, data=[SimpleNamespace(b64_json=b64, url=None, revised_prompt=prompt)])


def fake_embeddings_response(**kwargs: Any) -> SimpleNamespace:
    vec = [0.0] * 8
    usage = SimpleNamespace(prompt_tokens=1, total_tokens=1)
    return SimpleNamespace(object="list", data=[SimpleNamespace(index=0, embedding=vec)], usage=usage)


def fake_audio_response(**kwargs: Any) -> SimpleNamespace:
    payload = fake_tts_mp3_bytes()
    return SimpleNamespace(content=payload, read=lambda: payload)


def fake_tts_mp3_bytes(text: str = "", **kwargs: Any) -> bytes:
    """Tiny deterministic fake audio payload (NOT a decodable MP3 — placeholder)."""
    seed = _deterministic_seed("tts", text)
    return b"SYNTHETIC_TTS\x00" + seed.to_bytes(4, "big")


def fake_youtube_transcript(video_id: str = "", lang: str | None = None, **kwargs: Any) -> dict:
    """Fixed transcript fixture matching _build_youtube_transcript_result() shape."""
    language = (lang or "de").strip() or "de"
    items = [
        {"text": "Synthetisches Transkript Segment eins.", "start": 0.0, "duration": 2.0},
        {"text": "Synthetisches Transkript Segment zwei.", "start": 2.0, "duration": 2.0},
        {"text": "Synthetisches Transkript Segment drei.", "start": 4.0, "duration": 2.0},
    ]
    return {
        "success": True,
        "source": "synthetic",
        "ip_country": "DE",
        "language": language,
        "is_generated": True,
        "items": items,
    }


# --------------------------------------------------------------------------- #
# OpenAI client proxy (path-dispatch) — covers async and sync clients
# --------------------------------------------------------------------------- #

def _dispatch_openai(path: list[str], args: tuple, kwargs: dict) -> Any:
    key = ".".join(path)
    if key.endswith("chat.completions.create") or key.endswith("chat.completions.parse") \
            or key.endswith("responses.create") or key.endswith("responses.parse"):
        return fake_chat_completion(model=str(kwargs.get("model") or "synthetic-gpt"),
                                    messages=kwargs.get("messages"))
    if key.endswith("images.generate") or key.endswith("images.create") or key.endswith("images.edit"):
        return fake_image_response(prompt=str(kwargs.get("prompt") or ""))
    if key.endswith("embeddings.create"):
        return fake_embeddings_response()
    if key.endswith("audio.speech.create"):
        return fake_audio_response()
    # Unknown surface (e.g. beta.threads/runs/assistants): harmless stub, no network.
    return SimpleNamespace(id="synthetic", status="completed", data=[], object="synthetic")


class _SyntheticNode:
    """Attribute-chain proxy: client.<a>.<b>.<terminal>(**kw) -> fake payload.

    For async clients the terminal returns a coroutine (awaitable); for sync
    clients it returns the value directly. No call reaches the network.
    """

    __slots__ = ("_path", "_is_async")

    def __init__(self, path: list[str], is_async: bool):
        self._path = path
        self._is_async = is_async

    def __getattr__(self, name: str) -> "_SyntheticNode":
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return _SyntheticNode(self._path + [name], self._is_async)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            async def _run() -> Any:
                return _dispatch_openai(self._path, args, kwargs)
            return _run()
        return _dispatch_openai(self._path, args, kwargs)


def build_async_openai_client(*args: Any, **kwargs: Any):
    """Return the real AsyncOpenAI client, or a synthetic proxy in load mode."""
    if is_synthetic_load_mode():
        return _SyntheticNode([], is_async=True)
    from openai import AsyncOpenAI
    return AsyncOpenAI(*args, **kwargs)


def build_sync_openai_client(*args: Any, **kwargs: Any):
    """Return the real OpenAI client, or a synthetic proxy in load mode."""
    if is_synthetic_load_mode():
        return _SyntheticNode([], is_async=False)
    from openai import OpenAI
    return OpenAI(*args, **kwargs)


# --------------------------------------------------------------------------- #
# Boundary guards used by the YouTube / TTS providers
# --------------------------------------------------------------------------- #

def synthetic_youtube_transcript_or_none(video_id: str, lang: str | None = None) -> dict | None:
    """Return a fixed transcript fixture in load mode, else None (use real path)."""
    if is_synthetic_load_mode():
        return fake_youtube_transcript(video_id=video_id, lang=lang)
    return None


def synthetic_tts_mp3_or_none(text: str = "") -> bytes | None:
    """Return a tiny fake audio payload in load mode, else None (use real path)."""
    if is_synthetic_load_mode():
        return fake_tts_mp3_bytes(text=text)
    return None
