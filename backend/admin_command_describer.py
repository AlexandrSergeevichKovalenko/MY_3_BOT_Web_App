# -*- coding: utf-8 -*-
"""LLM generation of Russian descriptions for admin bot commands.

Given a command's name, docstring and source, produce a catalog-shaped entry
({desc, args, example, topic_id}) in Russian so it can be shown in the
«🛠 Команды админа» palette. Used by the /describe_new Telegram flow.
"""
from __future__ import annotations

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


def _model() -> str:
    return (os.getenv("ADMIN_CMD_DESC_MODEL") or "gpt-4.1-mini").strip() or "gpt-4.1-mini"


_SYSTEM = (
    "Ты документируешь команды Telegram-бота для внутренней админ-панели. "
    "По имени команды, её docstring и исходному коду верни СТРОГО JSON-объект с полями: "
    "desc — одно предложение по-русски, что команда делает (без префикса '/'); "
    "args — краткое описание аргументов по-русски или строка 'нет аргументов'; "
    "example — пример вызова, начинается со слэша; "
    "topic_id — id темы из предложенного списка, наиболее подходящей по смыслу. "
    "Пиши по-русски, кратко и по делу. Только JSON, без пояснений."
)

_USER_TMPL = (
    "Команда: /{cmd}\n\n"
    "Docstring:\n{doc}\n\n"
    "Исходный код (фрагмент):\n{src}\n\n"
    "Доступные темы (topic_id — название):\n{topics}\n\n"
    "{variant}"
    "Верни JSON: {{\"desc\": \"...\", \"args\": \"...\", \"example\": \"/{cmd} ...\", \"topic_id\": \"...\"}}"
)


def generate_description(cmd_slug: str, docstring: str, source: str,
                         topics: list[tuple[str, str]], *, variant_hint: str = "") -> dict:
    """Return {desc, args, example, topic_id}. Raises on hard failure so the caller can
    show an error. topic_id is coerced to one of the provided topic ids ('misc' fallback)."""
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    slug = str(cmd_slug or "").lstrip("/").strip()
    valid_ids = {tid for tid, _ in topics} or {"misc"}
    topics_txt = "\n".join(f"{tid} — {title}" for tid, title in topics) or "misc — Прочее"
    variant = ""
    if variant_hint:
        variant = f"Дай ДРУГУЮ формулировку, отличную от прошлой. {variant_hint}\n\n"
    payload = {
        "model": _model(),
        "temperature": 0.6 if variant_hint else 0.3,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": _USER_TMPL.format(
                cmd=slug, doc=(docstring or "—")[:1500], src=(source or "—")[:3500],
                topics=topics_txt, variant=variant,
            )},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post("https://api.openai.com/v1/chat/completions",
                         headers=headers, json=payload, timeout=90)
    if not resp.ok:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:200]}")
    raw = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content") or ""
    data = json.loads(raw)
    desc = str(data.get("desc") or "").strip()
    args = str(data.get("args") or "нет аргументов").strip() or "нет аргументов"
    example = str(data.get("example") or f"/{slug}").strip() or f"/{slug}"
    if not example.startswith("/"):
        example = "/" + example.lstrip("/")
    topic_id = str(data.get("topic_id") or "misc").strip()
    if topic_id not in valid_ids:
        topic_id = "misc"
    if not desc:
        raise ValueError("empty desc from LLM")
    return {"desc": desc[:400], "args": args[:400], "example": example[:200], "topic_id": topic_id}
