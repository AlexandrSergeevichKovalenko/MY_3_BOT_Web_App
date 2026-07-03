"""
«Начни день с коротких новостей» — nightly generator for the daily shared news rubric.

Runs ONCE per day in the background (heavy path): pick a fresh German learner-news
video (DW «Langsam gesprochene Nachrichten» / «Nachrichten leicht») that actually has
German subtitles and fits the length cap, fetch its transcript, then a single LLM call
builds the RU summary + 12–18 interesting phrases (translation + usage) + exactly 4
multiple-choice comprehension questions. The result lands in bt_3_world_news_daily.

The morning broadcast never calls anything here — it just reads the prepared row, so the
user-facing path stays instant and scales O(1) regardless of user count.

Resilience (not a dumb fallback): the selector walks several fresh candidates newest-first
and keeps the first one whose transcript really loads and validates. If none pass, prepare_*
raises and the morning path cleanly degrades to the plain reminder — we never send a broken
card and never reuse a stale day.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

# ── Config (env-overridable) ────────────────────────────────────────────────────

def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name) or "").strip() or default)
    except Exception:
        return default


def _search_queries() -> list[str]:
    raw = (os.getenv("WORLD_NEWS_SEARCH_QUERIES") or "").strip()
    if raw:
        return [q.strip() for q in raw.split("|") if q.strip()]
    # STRICTLY news, learner-oriented, reliable German subtitles, newest-first.
    return [
        "Langsam gesprochene Nachrichten",      # DW — slow news for learners
        "tagesschau in Einfacher Sprache",      # ARD — simple-language news
        "nachrichtenleicht",                    # Deutschlandfunk — easy news
        "DW Nachrichten",
    ]


def _channel_ids() -> list[str]:
    raw = (os.getenv("WORLD_NEWS_CHANNEL_IDS") or "").strip()
    return [c.strip() for c in raw.split(",") if c.strip()]


def _news_channel_allow() -> list[str]:
    """Lowercased channel-title substrings that count as REAL news sources. Anything else
    (entertainment, docs, vlogs) is rejected — this is a strict news rubric. Env-overridable
    via WORLD_NEWS_ALLOWED_CHANNELS (|-separated). Set to '*' to disable the filter."""
    raw = (os.getenv("WORLD_NEWS_ALLOWED_CHANNELS") or "").strip()
    if raw:
        return [c.strip().lower() for c in raw.split("|") if c.strip()]
    return [
        "deutsche welle", "dw deutsch", "dw nachrichten", "learn german with dw",
        "tagesschau", "zdfheute", "zdf heute", "heute journal",
        "nachrichtenleicht", "deutschlandfunk",
    ]


def _is_allowed_news_channel(channel_title: str) -> bool:
    allow = _news_channel_allow()
    if allow == ["*"]:
        return True
    ct = str(channel_title or "").strip().lower()
    if not ct:
        return False
    return any(sub in ct for sub in allow)


WORLD_NEWS_MAX_SECONDS = _env_int("WORLD_NEWS_MAX_SECONDS", 900)   # ≤ 15 min (DW «Langsam
# gesprochene Nachrichten» — the most reliable learner-news source with real German subtitles —
# runs ~9–10 min, so a 6-min cap silently excluded it. Env-overridable if you want it shorter.
WORLD_NEWS_MIN_SECONDS = _env_int("WORLD_NEWS_MIN_SECONDS", 40)
WORLD_NEWS_CANDIDATES = _env_int("WORLD_NEWS_CANDIDATES", 12)
WORLD_NEWS_MIN_TRANSCRIPT_CHARS = _env_int("WORLD_NEWS_MIN_TRANSCRIPT_CHARS", 300)
WORLD_NEWS_MAX_TRANSCRIPT_CHARS = _env_int("WORLD_NEWS_MAX_TRANSCRIPT_CHARS", 8000)


def _model() -> str:
    return (
        os.getenv("WORLD_NEWS_MODEL")
        or os.getenv("OPENAI_MODEL")
        or "gpt-4.1-2025-04-14"
    ).strip()


def _youtube_api_key() -> str:
    return (
        os.getenv("YOUTUBE_API_KEY")
        or os.getenv("YOUTUBE_DATA_API_KEY")
        or ""
    ).strip()


# ── YouTube Data API helpers (self-contained; order=date for newest) ────────────

def _iso8601_duration_to_seconds(value: str) -> int:
    m = re.match(r"^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", str(value or "").strip())
    if not m:
        return 0
    days, hours, minutes, seconds = (int(x) if x else 0 for x in m.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _yt_api_search_recent(query: str, *, channel_id: str | None = None, max_results: int = 10) -> list[dict]:
    api_key = _youtube_api_key()
    if not api_key:
        return []
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "order": "date",          # newest first
        "maxResults": max_results,
        "relevanceLanguage": "de",
        "regionCode": "DE",
        "key": api_key,
    }
    if channel_id:
        params["channelId"] = channel_id
    try:
        resp = requests.get("https://www.googleapis.com/youtube/v3/search", params=params, timeout=12)
        if resp.status_code >= 400:
            logger.info("world_news: YT search HTTP %s for query=%r", resp.status_code, query)
            return []
        out = []
        for item in (resp.json().get("items") or []):
            vid = ((item.get("id") or {}).get("videoId") or "").strip()
            snip = item.get("snippet") or {}
            if not vid:
                continue
            out.append({
                "video_id": vid,
                "title": (snip.get("title") or "").strip(),
                "channel_title": (snip.get("channelTitle") or "").strip(),
                "published_at": (snip.get("publishedAt") or "").strip(),
            })
        return out
    except Exception:
        logger.warning("world_news: YT search failed for query=%r", query, exc_info=True)
        return []


def _yt_api_video_details(video_ids: list[str]) -> dict[str, dict]:
    api_key = _youtube_api_key()
    if not api_key or not video_ids:
        return {}
    params = {
        "part": "contentDetails,snippet",
        "id": ",".join(video_ids[:50]),
        "key": api_key,
    }
    try:
        resp = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params, timeout=12)
        if resp.status_code >= 400:
            return {}
        details: dict[str, dict] = {}
        for item in (resp.json().get("items") or []):
            vid = (item.get("id") or "").strip()
            if not vid:
                continue
            content = item.get("contentDetails") or {}
            snip = item.get("snippet") or {}
            details[vid] = {
                "duration_seconds": _iso8601_duration_to_seconds(content.get("duration")),
                "title": (snip.get("title") or "").strip(),
                "channel_title": (snip.get("channelTitle") or "").strip(),
                "published_at": (snip.get("publishedAt") or "").strip(),
            }
        return details
    except Exception:
        logger.warning("world_news: YT videos.list failed", exc_info=True)
        return {}


def _extract_video_id(url_or_id: str) -> str:
    s = str(url_or_id or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", s):
        return s
    m = re.search(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})", s)
    return m.group(1) if m else ""


def _fetch_transcript(video_id: str) -> dict | None:
    """Lazy-import the production transcript pipeline from the web tier (proxy + fallbacks)."""
    try:
        from backend.backend_server import _fetch_youtube_transcript
    except Exception:
        logger.warning("world_news: cannot import _fetch_youtube_transcript", exc_info=True)
        return None
    try:
        data = _fetch_youtube_transcript(video_id, lang="de", allow_proxy=True)
    except Exception:
        return None
    items = data.get("items") if isinstance(data, dict) else None
    if not items:
        return None
    return data


def _transcript_to_text(items: list) -> str:
    import html
    parts = []
    for it in items or []:
        txt = str((it or {}).get("text") or "").strip()
        if txt:
            parts.append(txt)
    # Caption sources sometimes carry literal HTML entities (&nbsp;, &amp;, …); decode them so
    # the LLM (and the phrases it extracts) never sees "&nbsp;".
    text = html.unescape(" ".join(parts))
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ── Candidate selection ─────────────────────────────────────────────────────────

def _gather_candidates() -> list[dict]:
    """Newest-first, de-duplicated candidate videos across configured queries/channels."""
    seen: set[str] = set()
    candidates: list[dict] = []
    channels = _channel_ids() or [None]
    for query in _search_queries():
        for channel_id in channels:
            for row in _yt_api_search_recent(query, channel_id=channel_id, max_results=10):
                vid = row["video_id"]
                if vid in seen:
                    continue
                seen.add(vid)
                candidates.append(row)
    # Sort newest-first by published_at (ISO 8601 sorts lexicographically).
    candidates.sort(key=lambda r: r.get("published_at") or "", reverse=True)
    return candidates[: max(1, WORLD_NEWS_CANDIDATES)]


def _pick_video_with_transcript(*, manual_url: str | None = None) -> tuple[dict | None, dict]:
    """Return ({video_id, video_url, title, channel_title, duration_seconds, lang, text, items}
    or None, diag). `diag` counts why candidates were rejected (dur / no-captions / too-short)
    so a failure is diagnosable from the error message instead of a generic 'not found'."""
    diag: dict = {"candidates": 0, "dur_skipped": 0, "no_transcript": 0, "short_transcript": 0,
                  "channel_rejected": 0, "manual": bool(manual_url), "has_yt_key": bool(_youtube_api_key())}
    if manual_url:
        vid = _extract_video_id(manual_url)
        if not vid:
            diag["reason"] = "bad_url"
            return None, diag
        details = _yt_api_video_details([vid]).get(vid, {})
        data = _fetch_transcript(vid)
        if not data:
            diag["no_transcript"] = 1
            diag["reason"] = "manual_no_transcript"
            return None, diag
        text = _transcript_to_text(data.get("items") or [])
        if len(text) < WORLD_NEWS_MIN_TRANSCRIPT_CHARS:
            diag["short_transcript"] = 1
            diag["reason"] = "manual_transcript_too_short"
            return None, diag
        return {
            "video_id": vid,
            "video_url": f"https://www.youtube.com/watch?v={vid}",
            "title": details.get("title") or "",
            "channel_title": details.get("channel_title") or "",
            "duration_seconds": details.get("duration_seconds") or 0,
            "lang": data.get("language") or "de",
            "text": text[:WORLD_NEWS_MAX_TRANSCRIPT_CHARS],
            "items": data.get("items") or [],
        }, diag

    candidates = _gather_candidates()
    diag["candidates"] = len(candidates)
    if not candidates:
        diag["reason"] = "no_candidates" if diag["has_yt_key"] else "no_youtube_api_key"
        logger.warning("world_news: no candidates from YouTube search (diag=%s)", diag)
        return None, diag
    details_map = _yt_api_video_details([c["video_id"] for c in candidates])
    for cand in candidates:
        vid = cand["video_id"]
        det = details_map.get(vid, {})
        # STRICT news filter: only accept real news channels (reject entertainment/docs/vlogs).
        channel_title = det.get("channel_title") or cand.get("channel_title") or ""
        if not _is_allowed_news_channel(channel_title):
            diag["channel_rejected"] += 1
            continue
        dur = det.get("duration_seconds") or 0
        if dur and not (WORLD_NEWS_MIN_SECONDS <= dur <= WORLD_NEWS_MAX_SECONDS):
            diag["dur_skipped"] += 1
            continue
        data = _fetch_transcript(vid)
        if not data:
            diag["no_transcript"] += 1
            continue
        text = _transcript_to_text(data.get("items") or [])
        if len(text) < WORLD_NEWS_MIN_TRANSCRIPT_CHARS:
            diag["short_transcript"] += 1
            continue
        return {
            "video_id": vid,
            "video_url": f"https://www.youtube.com/watch?v={vid}",
            "title": det.get("title") or cand.get("title") or "",
            "channel_title": det.get("channel_title") or cand.get("channel_title") or "",
            "duration_seconds": dur,
            "lang": data.get("language") or "de",
            "text": text[:WORLD_NEWS_MAX_TRANSCRIPT_CHARS],
            "items": data.get("items") or [],
        }, diag
    diag["reason"] = "all_candidates_rejected"
    logger.warning("world_news: no candidate passed duration+transcript checks (diag=%s)", diag)
    return None, diag


# ── LLM pack (summary + phrases + 4 MC questions) ───────────────────────────────

_LLM_SYSTEM = """\
Du bist ein erfahrener Deutschlehrer und Redakteur einer täglichen Kurznachrichten-Rubrik
für Deutschlernende (Niveau B1–B2). Der Nutzer sieht ein kurzes deutsches Nachrichtenvideo
und darunter deine Aufbereitung.

Du bekommst das Transkript des Videos. Erstelle daraus ein JSON-Paket mit:

1) "summary_points": 2–4 sehr kurze THESEN auf RUSSISCH — je EIN Fakt pro Zeile, wie
   Schlagzeilen. KEINE Verbindungswörter ("кроме того", "но", "также"), KEINE Wertung,
   kein Wasser. Nur die nackten Fakten, jede These 3–9 Wörter. Beispiel:
   ["правительство Германии планирует изменить закон о налогах и больничных",
    "новые атаки России на Украину", "крупный штраф Google в Европе"].

2) "phrases": 12–18 wirklich nützliche, im Transkript tatsächlich vorkommende Wörter und
   Wendungen (bevorzugt Wortgruppen/Kollokationen, nicht triviale Wörter wie "und", "sein").
   Jedes Element:
     - "de": das Wort/die Wendung, korrekt geschrieben; bei Nomen MIT Artikel und, wenn sinnvoll,
       Pluralform (z. B. "die Regierung, -en"); bei Verben der Infinitiv.
     - "translation_ru": knappe russische Übersetzung.
     - "usage_ru": ein sehr kurzer russischer Hinweis, WIE/WANN man es benutzt (1 Satz),
       ggf. mit Rektion/Kasus.

3) "quiz": GENAU 4 Multiple-Choice-Fragen auf DEUTSCH zum Inhalt des Videos, die echtes
   Verständnis prüfen (Details, Zahlen, Daten, Zusammenhänge — nicht nur Vokabeln).
   Jede Frage:
     - "question_de": klar formulierte Frage auf Deutsch.
     - "options": GENAU 4 plausible Antworten auf Deutsch; die falschen sind knifflige,
       aber eindeutig falsche Distraktoren (ähnliche Zahlen/Namen/Bedingungen).
     - "correct_index": Index (0–3) der einzig richtigen Antwort.
     - "explanation_ru": 1 kurzer russischer Satz, warum die Antwort richtig ist.

Antworte NUR mit validem JSON, ohne Erklärungen drumherum."""

_LLM_USER_TMPL = """\
Videotitel: {title}

Transkript:
{transcript}

Erzeuge das JSON exakt in diesem Format:
{{
  "summary_points": ["…", "…", "…"],
  "phrases": [
    {{"de": "…", "translation_ru": "…", "usage_ru": "…"}}
  ],
  "quiz": [
    {{"question_de": "…", "options": ["…","…","…","…"], "correct_index": 0, "explanation_ru": "…"}}
  ]
}}"""


def _call_llm(title: str, transcript: str) -> dict:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    payload = {
        "model": _model(),
        "temperature": 0.5,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _LLM_SYSTEM},
            {"role": "user", "content": _LLM_USER_TMPL.format(title=title or "—", transcript=transcript)},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers, json=payload, timeout=120,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:300]}")
    raw = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content") or ""
    return json.loads(raw)


def _validate_and_normalize_pack(data: dict) -> dict:
    # Summary as 2–4 terse thesis lines (stored newline-joined). Fall back to splitting a
    # legacy paragraph summary_ru into sentences if the model still returns one.
    raw_points = (data or {}).get("summary_points")
    points: list[str] = []
    if isinstance(raw_points, list):
        points = [str(x).strip() for x in raw_points if str(x).strip()]
    if not points:
        legacy = str((data or {}).get("summary_ru") or "").strip()
        points = [s.strip() for s in re.split(r"(?<=[.!?])\s+", legacy) if s.strip()]
    summary_ru = "\n".join(points[:4])

    raw_phrases = (data or {}).get("phrases")
    if not isinstance(raw_phrases, list):
        raise ValueError("phrases missing")
    phrases = []
    for p in raw_phrases:
        if not isinstance(p, dict):
            continue
        de = str(p.get("de") or "").strip()
        tr = str(p.get("translation_ru") or "").strip()
        if not de or not tr:
            continue
        phrases.append({
            "de": de,
            "translation_ru": tr,
            "usage_ru": str(p.get("usage_ru") or "").strip(),
        })
    if len(phrases) < 6:
        raise ValueError(f"need >=6 valid phrases, got {len(phrases)}")
    phrases = phrases[:18]

    raw_quiz = (data or {}).get("quiz")
    if not isinstance(raw_quiz, list) or len(raw_quiz) < 4:
        raise ValueError("need >=4 quiz questions")
    quiz = []
    for q in raw_quiz[:4]:
        if not isinstance(q, dict):
            raise ValueError("bad quiz item")
        question = str(q.get("question_de") or "").strip()
        options = q.get("options")
        if not question or not isinstance(options, list) or len(options) != 4:
            raise ValueError("quiz question needs exactly 4 options")
        options = [str(o or "").strip() for o in options]
        if any(not o for o in options):
            raise ValueError("empty quiz option")
        try:
            ci = int(q.get("correct_index"))
        except Exception:
            raise ValueError("bad correct_index")
        if not (0 <= ci <= 3):
            raise ValueError("correct_index out of range")
        quiz.append({
            "question_de": question,
            "options": options,
            "correct_index": ci,
            "explanation_ru": str(q.get("explanation_ru") or "").strip(),
        })
    if len(quiz) != 4:
        raise ValueError("quiz must have exactly 4 questions")

    return {"summary_ru": summary_ru, "phrases": phrases, "quiz": quiz}


# ── Orchestrator ────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def prepare_world_news(
    news_date: str | None = None,
    *,
    manual_url: str | None = None,
    status: str = "ready",
) -> dict:
    """Pick a video, build the pack, persist to bt_3_world_news_daily. Returns the stored
    entry dict. Raises on any failure so callers can degrade cleanly."""
    from backend.database import upsert_world_news_daily, get_world_news_for_date

    date_str = (news_date or _today_str()).strip()

    picked, diag = _pick_video_with_transcript(manual_url=manual_url)
    if not picked:
        raise RuntimeError(f"world_news: no suitable video with transcript found — diag={diag}")

    pack = _validate_and_normalize_pack(_call_llm(picked["title"], picked["text"]))

    upsert_world_news_daily(
        news_date=date_str,
        video_id=picked["video_id"],
        video_url=picked["video_url"],
        video_title=picked["title"],
        channel_title=picked["channel_title"],
        duration_seconds=picked["duration_seconds"],
        transcript_lang=picked["lang"],
        summary_ru=pack["summary_ru"],
        phrases=pack["phrases"],
        quiz=pack["quiz"],
        status=status,
    )
    logger.info(
        "world_news: prepared %s video=%s phrases=%d quiz=%d",
        date_str, picked["video_id"], len(pack["phrases"]), len(pack["quiz"]),
    )
    entry = get_world_news_for_date(date_str)
    if not entry:
        raise RuntimeError("world_news: persisted entry not found after upsert")
    return entry
