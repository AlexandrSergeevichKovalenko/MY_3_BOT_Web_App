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
import random
import re
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

# YouTube search.list costs 100 quota units/call (10k/day default). Two guards keep us from
# burning it: a short-lived candidate cache so repeated «переформировать» clicks reuse one
# search sweep, and a quota flag so a 429/403 surfaces as a clear reason instead of "no videos".
_QUOTA_EXCEEDED = False
# Свип кандидатов кэшируется ОТДЕЛЬНО по рубрикам ('news' / 'standup'): иначе вечерняя
# подготовка стендапа получила бы новостной список каналов из тёплого кэша.
_CAND_CACHE: dict = {}

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
    # STRICTLY news, learner-oriented, reliable German subtitles, newest-first. Kept LEAN on
    # purpose: search.list costs 100 quota units/call, so every extra query 100×-multiplies the
    # daily burn. These 4 cover the only sources whose German subtitles reliably fetch.
    return [
        "Langsam gesprochene Nachrichten",      # DW — slow news for learners
        "tagesschau in Einfacher Sprache",      # ARD — simple-language news
        "nachrichtenleicht",                    # Deutschlandfunk — easy news
        "DW Nachrichten",
    ]


def _channel_ids() -> list[str]:
    """Curated German-news channels we pull recent uploads from (via their uploads playlist,
    1 quota unit/call — vs 100 for search.list). Env-overridable with WORLD_NEWS_CHANNEL_IDS
    (comma-separated UC… ids)."""
    raw = (os.getenv("WORLD_NEWS_CHANNEL_IDS") or "").strip()
    if raw:
        return [c.strip() for c in raw.split(",") if c.strip()]
    return [
        "UC5NOEUbkLheQcaaRldYW5GA",  # tagesschau
        "UCeqKIgPQfNInOswGRWt48kQ",  # ZDFheute Nachrichten
        "UCMIgOXM2JEQ2Pv2d0_PVfcg",  # DW Deutsch
        "UCxUWIEL-USsiPak0Qy6_vVg",  # Deutsch lernen mit der DW (learner-oriented)
        "UCkCab7liRnZSZsN8YqzhuuA",  # Deutschlandfunk
    ]


def _uploads_playlist_id(channel_id: str) -> str:
    """A channel's 'uploads' playlist is its channel id with the UC prefix swapped to UU."""
    cid = str(channel_id or "").strip()
    if cid.startswith("UU"):
        return cid
    if cid.startswith("UC") and len(cid) > 2:
        return "UU" + cid[2:]
    return ""


def _news_channel_allow() -> list[str]:
    """Lowercased channel-title substrings that count as REAL news sources. Anything else
    (entertainment, docs, vlogs) is rejected — this is a strict news rubric. Env-overridable
    via WORLD_NEWS_ALLOWED_CHANNELS (|-separated). Set to '*' to disable the filter."""
    raw = (os.getenv("WORLD_NEWS_ALLOWED_CHANNELS") or "").strip()
    if raw:
        return [c.strip().lower() for c in raw.split("|") if c.strip()]
    return [
        "deutsche welle", "dw deutsch", "dw nachrichten", "learn german with dw",
        "deutsch lernen mit der dw",
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
# Preferred length window for listening practice: 5–7 min. The picker doesn't *restrict* to this
# band (that could leave a morning with no news) — it *prioritises* it, trying videos inside the
# window first, then longer ones (up to the hard cap — DW «Langsam gesprochene Nachrichten» runs
# ~9–10 min and is our best source), then 2–5 min, and sub-2-min clips only as a last resort. So
# a fresh 1-min clip no longer beats a 6-min one just for being newer.
WORLD_NEWS_PREF_MIN_SECONDS = _env_int("WORLD_NEWS_PREF_MIN_SECONDS", 300)
WORLD_NEWS_PREF_MAX_SECONDS = _env_int("WORLD_NEWS_PREF_MAX_SECONDS", 420)
WORLD_NEWS_CANDIDATES = _env_int("WORLD_NEWS_CANDIDATES", 20)
WORLD_NEWS_MIN_TRANSCRIPT_CHARS = _env_int("WORLD_NEWS_MIN_TRANSCRIPT_CHARS", 300)
WORLD_NEWS_MAX_TRANSCRIPT_CHARS = _env_int("WORLD_NEWS_MAX_TRANSCRIPT_CHARS", 8000)
# Wall-clock budget for the whole candidate sweep. Each transcript fetch has retries but NO hard
# socket timeout, so a blocked IP (see the 2026-07-10 datacenter-block incident) could make the
# loop crawl for many minutes. Cap it: once the budget is spent we stop trying more candidates and
# return (None, diag) so the caller alerts fast instead of the worker thread lingering. The bot
# also wraps prepare_world_news in its own wait_for as a second line of defence.
WORLD_NEWS_PICK_BUDGET_SEC = _env_int("WORLD_NEWS_PICK_BUDGET_SEC", 210)


def _model(profile=None) -> str:
    """Модель для разбора. У стендапа своя переменная: разбор сленга и переносных значений
    заметно тяжелее пересказа новости, и владелец должен иметь возможность поставить сюда
    модель посильнее, не выкатывая код."""
    if profile is not None and getattr(profile, "key", "") == "standup":
        standup_model = (os.getenv("STANDUP_MODEL") or "").strip()
        if standup_model:
            return standup_model
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
            if resp.status_code in (429, 403):
                global _QUOTA_EXCEEDED
                _QUOTA_EXCEEDED = True
                logger.warning("world_news: YT search quota/rate-limited (HTTP %s) query=%r", resp.status_code, query)
            else:
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


def _yt_api_playlist_recent(playlist_id: str, *, max_results: int = 10, pages: int = 1) -> list[dict]:
    """Uploads from a channel's uploads playlist. Costs 1 quota unit/call (vs 100 for
    search.list). Candidates are marked trusted (curated channel) so they bypass the channel
    allow-filter downstream.

    `pages` — сколько страниц по `max_results` пройти. Новостям хватает одной (нужна
    свежесть), стендапу нужен весь архив: ролики вечнозелёные, и выбирать приходится из
    сотен, вычитая уже показанное.
    """
    api_key = _youtube_api_key()
    if not api_key or not playlist_id:
        return []
    out = []
    page_token = ""
    for _ in range(max(1, int(pages or 1))):
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": max_results,
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token
        try:
            resp = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params, timeout=12)
            if resp.status_code >= 400:
                if resp.status_code in (429, 403):
                    global _QUOTA_EXCEEDED
                    _QUOTA_EXCEEDED = True
                    logger.warning("world_news: YT playlistItems quota/rate-limited (HTTP %s) pl=%s", resp.status_code, playlist_id)
                else:
                    logger.info("world_news: YT playlistItems HTTP %s for pl=%s", resp.status_code, playlist_id)
                break
            payload = resp.json()
            for item in (payload.get("items") or []):
                snip = item.get("snippet") or {}
                vid = ((snip.get("resourceId") or {}).get("videoId") or "").strip()
                if not vid:
                    continue
                out.append({
                    "video_id": vid,
                    "title": (snip.get("title") or "").strip(),
                    "channel_title": (snip.get("videoOwnerChannelTitle") or snip.get("channelTitle") or "").strip(),
                    "published_at": (snip.get("publishedAt") or "").strip(),
                    "trusted": True,
                })
            page_token = (payload.get("nextPageToken") or "").strip()
            if not page_token:
                break
        except Exception:
            logger.warning("world_news: YT playlistItems failed for pl=%s", playlist_id, exc_info=True)
            break
    return out


def _yt_api_video_details(video_ids: list[str]) -> dict[str, dict]:
    """Длительность, канал и наличие ручных субтитров для списка роликов.

    videos.list принимает максимум 50 id за вызов, поэтому список идёт пачками: у стендапа
    кандидатов сотни, и обрезка до первых 50 оставила бы остальных без длительности — их
    погнало бы качать субтитры вслепую. Один вызов стоит 1 единицу квоты.
    """
    api_key = _youtube_api_key()
    if not api_key or not video_ids:
        return {}
    details: dict[str, dict] = {}
    for start in range(0, len(video_ids), 50):
        chunk = video_ids[start:start + 50]
        if not chunk:
            continue
        _yt_api_video_details_chunk(chunk, api_key, details)
    return details


def _yt_api_video_details_chunk(video_ids: list[str], api_key: str, details: dict) -> None:
    params = {
        "part": "contentDetails,snippet",
        "id": ",".join(video_ids),
        "key": api_key,
    }
    try:
        resp = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params, timeout=12)
        if resp.status_code >= 400:
            return
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
                # YouTube помечает здесь ТОЛЬКО субтитры, положенные автором руками
                # ("true"); машинная расшифровка в этот флаг не попадает. Для стендапа это
                # ровно та разница, по которой владелец 20.08.2026 велел ставить ролики с
                # ручными субтитрами первыми: под стендап машина пишет без знаков препинания
                # и угадывает слова на слух, а человек читает субтитры и заучивает их.
                "has_manual_captions": str(content.get("caption") or "").strip().lower() == "true",
            }
    except Exception:
        logger.warning("world_news: YT videos.list failed", exc_info=True)


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

def _gather_candidates(profile=None) -> list[dict]:
    """Newest-first, de-duplicated candidate videos.

    PRIMARY: recent uploads from curated German-news channels via their uploads playlists
    (playlistItems.list = 1 quota unit/call). This is both ~100× cheaper than search.list and
    a cleaner pool (no English DW News / re-uploaders to filter out).
    FALLBACK: keyword search (search.list = 100 units) only if the playlists yield nothing and
    we weren't rate-limited — keeps the rubric alive if the channel set is misconfigured.

    Cached for WORLD_NEWS_CANDIDATE_TTL_SEC (default 6h) so repeated «переформировать» clicks
    re-pick from ONE sweep instead of re-hitting the API. Rotation/exclusion still runs on the
    cached pool, so re-forms still yield a different video without re-fetching.

    Профиль рубрики (backend/daily_video_rubrics.py) задаёт каналы и стратегию обхода:
    новостям нужна свежесть (одна страница последних загрузок), стендапу — глубина архива
    (ролики вечнозелёные, важно не повториться). Кэш держится отдельно по рубрикам, иначе
    вечерняя подготовка стендапа отдавала бы новостной свип."""
    global _QUOTA_EXCEEDED
    from backend.daily_video_rubrics import NEWS_PROFILE, profile_channel_ids
    profile = profile or NEWS_PROFILE
    ttl = _env_int("WORLD_NEWS_CANDIDATE_TTL_SEC", 6 * 3600)
    now = time.time()
    slot = _CAND_CACHE.setdefault(profile.key, {"ts": 0.0, "items": []})
    if slot["items"] and (now - slot["ts"]) < ttl:
        return list(slot["items"])

    _QUOTA_EXCEEDED = False
    seen: set[str] = set()
    candidates: list[dict] = []
    archive = profile.pick_strategy == "archive"
    per_channel = 50 if archive else _env_int("WORLD_NEWS_PER_CHANNEL", 8)
    pages = profile.archive_pages if archive else 1
    for cid in profile_channel_ids(profile):
        pl = _uploads_playlist_id(cid)
        if not pl:
            continue
        for row in _yt_api_playlist_recent(pl, max_results=per_channel, pages=pages):
            vid = row["video_id"]
            if vid in seen:
                continue
            seen.add(vid)
            candidates.append(row)

    # Fallback to keyword search only if the cheap path produced nothing (and not because we
    # were rate-limited — in that case searching would just burn 100-unit calls for nothing).
    if not candidates and not _QUOTA_EXCEEDED:
        logger.info("world_news: no playlist candidates — falling back to keyword search")
        for query in _search_queries():
            for row in _yt_api_search_recent(query, max_results=10):
                vid = row["video_id"]
                if vid in seen:
                    continue
                seen.add(vid)
                candidates.append(row)

    # Sort newest-first by published_at (ISO 8601 sorts lexicographically).
    candidates.sort(key=lambda r: r.get("published_at") or "", reverse=True)
    # Новостям хватает верхушки списка — там самое свежее. У стендапа свежесть значения не
    # имеет, а обрезка до 20 роликов убила бы весь смысл обхода архива: показанное
    # вычитается, и через пару месяцев верхушка кончилась бы, хотя в архиве сотни роликов.
    # Потолок архивного свипа. Измерено 20.08.2026: при потолке 1000 в пул попадали
    # 204 годных ролика вместо 646 — список отсортирован по свежести, а свежие загрузки
    # у стендап-каналов это в основном Shorts, и они съедали весь потолок. Потолок 4000
    # покрывает весь обойдённый архив; по квоте это ~180 единиц из 10 000 в сутки и
    # только раз в 6 часов (свип кэшируется).
    cap = _env_int("STANDUP_CANDIDATES", 4000) if archive else max(1, WORLD_NEWS_CANDIDATES)
    candidates = candidates[: max(1, cap)]
    if candidates:  # only cache a real sweep — never cache an empty (quota-exhausted) result
        slot["items"] = candidates
        slot["ts"] = now
    return candidates


def _length_priority(dur: int, profile=None) -> tuple[int, int]:
    """Sort key (lower = tried first) that prefers the 5–7 min window for listening practice.

    Tiers: 0 = inside [PREF_MIN, PREF_MAX] (ideal) · 1 = longer than the window (fuller news, still
    good) · 2 = duration unknown (rare; try before the short clips) · 3 = 2–5 min · 4 = under 2 min
    (last resort). Ties within a tier keep the caller's newest-first order (Python sort is stable),
    so the freshest video wins among equally-good lengths. This only REORDERS the transcript walk —
    it never adds transcript fetches, so the quota/budget behaviour is unchanged."""
    pref_min = profile.pref_min_seconds if profile else WORLD_NEWS_PREF_MIN_SECONDS
    pref_max = profile.pref_max_seconds if profile else WORLD_NEWS_PREF_MAX_SECONDS
    if dur <= 0:
        return (2, 0)
    if pref_min <= dur <= pref_max:
        return (0, 0)
    if dur > pref_max:
        return (1, dur - pref_max)   # longer — prefer closer to the window
    if dur >= 120:
        return (3, pref_min - dur)   # 2–5 min — prefer closer to the window
    return (4, pref_min - dur)       # < 2 min — last resort


def _pick_video_with_transcript(*, profile=None, manual_url: str | None = None,
                                exclude_video_ids: set[str] | None = None) -> tuple[dict | None, dict]:
    """Return ({video_id, video_url, title, channel_title, duration_seconds, lang, text, items}
    or None, diag). `diag` counts why candidates were rejected (dur / no-captions / too-short)
    so a failure is diagnosable from the error message instead of a generic 'not found'.
    `exclude_video_ids` skips those videos (used by «переформировать» to pick a DIFFERENT one)."""
    from backend.daily_video_rubrics import NEWS_PROFILE
    profile = profile or NEWS_PROFILE
    exclude = {str(v).strip() for v in (exclude_video_ids or set()) if str(v).strip()}
    diag: dict = {"rubric": profile.key, "candidates": 0, "dur_skipped": 0, "no_transcript": 0,
                  "short_transcript": 0, "channel_rejected": 0, "excluded": 0,
                  "manual": bool(manual_url), "has_yt_key": bool(_youtube_api_key())}
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
            "is_generated": data.get("is_generated"),
            "has_manual_captions": bool(details.get("has_manual_captions")),
        }, diag

    candidates = _gather_candidates(profile)
    diag["candidates"] = len(candidates)
    diag["quota_exceeded"] = _QUOTA_EXCEEDED
    if not candidates:
        if _QUOTA_EXCEEDED:
            diag["reason"] = "youtube_quota_exceeded"
        else:
            diag["reason"] = "no_candidates" if diag["has_yt_key"] else "no_youtube_api_key"
        logger.warning("world_news: no candidates from YouTube search (diag=%s)", diag)
        return None, diag
    details_map = _yt_api_video_details([c["video_id"] for c in candidates])
    # Reorder the (newest-first) pool so 5–7 min videos are tried FIRST, then longer, then shorter —
    # a stable sort keeps recency as the tiebreak. Duration comes from videos.list metadata we
    # already fetched, so this costs no extra transcript fetches (those still stop at the first
    # valid candidate below). See _length_priority for the tiering.
    if profile.pick_strategy == "archive":
        # У вечнозелёного архива нет «свежести», по которой можно было бы разбивать ничьи,
        # и без перемешивания рубрика месяцами шла бы по одному каналу — тому, чьи ролики
        # оказались первыми в свипе. Перемешиваем ДО сортировки: устойчивая сортировка
        # сохранит случайный порядок внутри одинаковых по приоритету роликов.
        random.shuffle(candidates)
    candidates.sort(key=lambda c: (
        # Решение владельца 20.08.2026: ролики с субтитрами, положенными руками, идут
        # первыми; машинная расшифровка — второй эшелон. Для новостей флаг выключен, и
        # ключ вырождается в прежний порядок (все нули) — их поведение не меняется.
        0 if (not profile.prefer_manual_captions
              or details_map.get(c["video_id"], {}).get("has_manual_captions")) else 1,
        _length_priority(
            (details_map.get(c["video_id"], {}).get("duration_seconds") or 0), profile
        ),
    ))
    _budget_started = time.monotonic()
    for cand in candidates:
        if time.monotonic() - _budget_started > WORLD_NEWS_PICK_BUDGET_SEC:
            diag["budget_exhausted"] = True
            diag["reason"] = "pick_budget_exhausted"
            logger.warning(
                "world_news: candidate sweep exceeded %ds budget (diag=%s) — likely transcript "
                "fetch is blocked/slow (proxy?)", WORLD_NEWS_PICK_BUDGET_SEC, diag,
            )
            return None, diag
        vid = cand["video_id"]
        if vid in exclude:
            diag["excluded"] += 1
            continue
        det = details_map.get(vid, {})
        # STRICT news filter: only accept real news channels (reject entertainment/docs/vlogs).
        # Playlist candidates are pre-trusted (curated channel) and skip this check.
        channel_title = det.get("channel_title") or cand.get("channel_title") or ""
        if not cand.get("trusted") and not _is_allowed_news_channel(channel_title):
            diag["channel_rejected"] += 1
            continue
        dur = det.get("duration_seconds") or 0
        if dur and not (profile.min_seconds <= dur <= profile.max_seconds):
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
            "is_generated": data.get("is_generated"),
            # Идёт в реестр показанного и в отчёт: так владелец видит числом, сколько
            # роликов рубрика взяла с ручными субтитрами, а сколько — с машинными.
            "has_manual_captions": bool(det.get("has_manual_captions")),
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

   GRUNDREGEL ZUR FORM (Entscheidung des Eigentümers, 21.08.2026):
   Eine WORTGRUPPE wird NIEMALS in eine Wörterbuchform umgeschrieben — sie bleibt genau so,
   wie sie im Transkript steht, mitsamt Kasus. Beim Umschreiben entstehen grammatisch
   falsche Formen, und die lernt der Nutzer dann auswendig.
   Nur ein EINZELNES Nomen bekommt Artikel und ggf. Plural ("die Regierung, -en"), nur ein
   EINZELNES Verb den Infinitiv.
   Weil die Wortgruppe also in ihrer Kasusform stehen bleibt, MUSS die Karte diese Form
   erklären, statt sie zu verschweigen: die Übersetzung steht in DERSELBEN Form, und
   "form_ru" benennt die Form auf Russisch.
   Beispiel, wie es RICHTIG aussieht:
     de: "einen hohen genetischen Anteil" · form_ru: "винительный падеж"
     translation_ru: "высокУЮ генетическУЮ составляющУЮ"   ← nicht "высокая составляющая"
   Jedes Element:
     - "de": das Wort/die Wendung, korrekt geschrieben, nach der Grundregel oben.
     - "form_ru": in welcher grammatischen Form "de" dasteht — kurz und auf RUSSISCH, in
       MENSCHLICHER Sprache: «винительный падеж», «дательный падеж», «инфинитив»,
       «множественное число», «словарная форма». Du liest die Form im Transkript ab
       (Rektion des Verbs, Artikelform) — rate NICHT.
     - "translation_ru": knappe russische Übersetzung in DERSELBEN grammatischen Form wie
       "de". Steht das Deutsche im Akkusativ, steht auch das Russische im Akkusativ.
     - "quote_de": der Satz aus dem TRANSKRIPT, in dem die Einheit vorkommt — WÖRTLICH
       kopiert, 4–20 Wörter. NICHT umformulieren, NICHT ausdenken. Ohne Satz ist eine
       Wortgruppe für den Nutzer nicht zu verstehen, egal in welchem Kasus.
     - "quote_ru": Übersetzung genau dieses Satzes ins Russische.
     - "usage_ru": ein sehr kurzer russischer Hinweis, WIE/WANN man es benutzt (1 Satz),
       ggf. mit Rektion/Kasus.

3) "quiz": GENAU 4 KNIFFLIGE Multiple-Choice-Fragen auf DEUTSCH, die PRÄZISES Hörverständnis
   prüfen — NICHT den groben Sinn. Jede Frage muss sich an EINEM konkreten Detail aus dem
   Transkript festmachen, das man nur bei aufmerksamem Zuhören mitbekommt:
     • Zahlen, Beträge, Prozente, Mengen, Jahre, Daten, Uhrzeiten, Fristen, Reihenfolgen;
     • Eigennamen (Personen, Orte, Organisationen, Parteien) und wer GENAU was gesagt/getan hat;
     • exakte Bedingungen und Zusammenhänge (Ursache→Folge, "nur wenn…", "ab wann…", "trotz…").
   Wenn im Transkript Zahlen, Beträge oder Daten vorkommen, MÜSSEN mindestens 2 der 4 Fragen
   genau darauf zielen. VERMEIDE reine Gist-, Meinungs- oder "Wie wird X beschrieben"-Fragen und
   alles, was man ohne das Video allein aus Weltwissen erraten kann.
   Jede Frage:
     - "question_de": klar und eindeutig, auf EIN präzises Detail zugespitzt.
     - "options": GENAU 4 Antworten; die 3 Distraktoren sind absichtlich MINIMAL verschieden
       (Nachbar-Zahlen wie 2019 statt 2018, 14 % statt 40 %, 300 Mio. statt 300 Mrd.,
       vertauschte Namen/Rollen, fast identische Bedingung) — so dass NUR wer genau hingehört
       hat die richtige erkennt. Alle Optionen etwa gleich lang und gleich plausibel, KEINE
       offensichtlich absurde Antwort.
     - "correct_index": Index (0–3) der einzig richtigen, im Transkript belegten Antwort.
     - "explanation_ru": 1 kurzer russischer Satz mit dem konkreten Beleg (Zahl/Datum/Name/Bedingung).

Antworte NUR mit validem JSON, ohne Erklärungen drumherum."""

_LLM_USER_TMPL = """\
Videotitel: {title}

Transkript:
{transcript}

Erzeuge das JSON exakt in diesem Format:
{{
  "summary_points": ["…", "…", "…"],
  "phrases": [
    {{"de": "…", "form_ru": "…", "translation_ru": "…", "quote_de": "…", "quote_ru": "…",
      "usage_ru": "…"}}
  ],
  "quiz": [
    {{"question_de": "…", "options": ["…","…","…","…"], "correct_index": 0, "explanation_ru": "…"}}
  ]
}}"""


def _call_llm(title: str, transcript: str, profile=None) -> dict:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    # Задание берётся из профиля рубрики. У новостей его нет — там работает исходный
    # новостной промпт этого модуля, и поведение рубрики не меняется.
    system = profile.llm_system if (profile and profile.llm_system) else _LLM_SYSTEM
    user_tmpl = profile.llm_user_tmpl if (profile and profile.llm_user_tmpl) else _LLM_USER_TMPL
    payload = {
        "model": _model(profile),
        "temperature": 0.5,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_tmpl.format(title=title or "—", transcript=transcript)},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers, json=payload, timeout=120,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:300]}")
    resp_json = resp.json()
    try:
        from backend.openai_usage_logging import log_openai_raw_usage
        # Расход считается ОТДЕЛЬНО по рубрикам — иначе стендап растворился бы в новостях,
        # и владелец не увидел бы, во что ему обходится каждая из них.
        action = "pool_standup" if (profile and profile.key == "standup") else "pool_world_news"
        log_openai_raw_usage(action_type=action, model=str(payload.get("model") or ""),
                             usage=resp_json.get("usage"), user_id=None)
    except Exception:
        pass
    raw = (resp_json.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    return json.loads(raw)


def _quote_fingerprint(text: str) -> str:
    """Отпечаток строки для сверки цитаты с субтитрами: только буквы и цифры в нижнем
    регистре. Знаки препинания и пробелы выкидываются, потому что в субтитрах они стоят
    иначе, чем их перепишет модель, а нам важно совпадение СЛОВ, а не вёрстки."""
    return re.sub(r"[^0-9a-zäöüß]+", "", str(text or "").lower())


def _validate_and_normalize_pack(data: dict, profile=None, transcript_text: str = "") -> dict:
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

    needs_quote = bool(profile and profile.requires_quote)
    min_phrases = profile.min_phrases if profile else 6
    max_phrases = profile.max_phrases if profile else 18
    transcript_fp = _quote_fingerprint(transcript_text) if needs_quote else ""
    dropped_no_quote = 0
    dropped_thin = 0

    phrases = []
    for p in raw_phrases:
        if not isinstance(p, dict):
            continue
        de = str(p.get("de") or "").strip()
        tr = str(p.get("translation_ru") or "").strip()
        if not de or not tr:
            continue
        item = {
            "de": de,
            "translation_ru": tr,
            "usage_ru": str(p.get("usage_ru") or "").strip(),
        }
        if needs_quote:
            # Карточка обязана быть СОГЛАСОВАНА САМА С СОБОЙ и объяснять то, что показывает.
            # Повод (случай владельца, 21.08.2026): карточка новостей показывала
            # «einen hohen genetischen Anteil» и переводила «высокая генетическая
            # составляющая» — немецкое в винительном, русское в именительном. Обороты к
            # словарной форме приводить запрещено (модель переписала бы живую речь в
            # грамматически неверную), поэтому форма не скрывается, а НАЗЫВАЕТСЯ.
            # С 21.08.2026 это критично вдвойне: корректор больше не правит текст при
            # сохранении, и показанное уезжает человеку в словарь дословно.
            # Неполная карточка не «показывается частично», а выбрасывается и считается.
            quote_de = str(p.get("quote_de") or "").strip()
            quote_ru = str(p.get("quote_ru") or "").strip()
            form = str(p.get("form_ru") or "").strip()
            # Помету регистра («сленг», «грубое») спрашиваем только у стендапа: в новостях
            # речь нейтральная, и требовать её там значило бы принуждать модель выдумывать.
            register = str(p.get("register_ru") or "").strip()
            needs_register = bool(profile and profile.requires_register)
            if (not quote_de or not quote_ru or not form or not item["usage_ru"]
                    or (needs_register and not register)):
                dropped_thin += 1
                continue
            # СТРАЖ ПРОТИВ ВЫДУМКИ: цитата обязана дословно найтись в субтитрах ролика.
            # Модель, которой велено «скопировать строку из транскрипта», иногда
            # пересказывает её своими словами — и тогда мы показали бы человеку фразу,
            # которой в ролике не звучало. Такая карточка не показывается.
            if _quote_fingerprint(quote_de) not in transcript_fp:
                dropped_no_quote += 1
                continue
            item["form_ru"] = form
            item["quote_de"] = quote_de
            item["quote_ru"] = quote_ru
            if register:
                item["register_ru"] = register
            # Обычное значение заполняется, только когда оно вправду другое: модели прямо
            # запрещено выдумывать второе значение там, где его нет.
            item["literal_ru"] = str(p.get("literal_ru") or "").strip()
        phrases.append(item)

    if needs_quote and (dropped_no_quote or dropped_thin):
        logger.info(
            "daily_video[%s]: карточек отброшено — цитаты нет в субтитрах: %d, неполных: %d "
            "(осталось %d)",
            getattr(profile, "key", "?"), dropped_no_quote, dropped_thin, len(phrases),
        )
    if len(phrases) < min_phrases:
        raise ValueError(
            f"need >={min_phrases} valid phrases, got {len(phrases)} "
            f"(quote_not_in_transcript={dropped_no_quote}, incomplete={dropped_thin})"
        )
    phrases = phrases[:max_phrases]

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
        # The model reliably emits the correct answer at index 0 (its worked example
        # shows correct_index: 0), so without shuffling the right option is always on
        # top. Shuffle the options and recompute the index via the permutation so
        # duplicate option texts can't misplace it.
        order = list(range(4))
        random.shuffle(order)
        options = [options[j] for j in order]
        ci = order.index(ci)
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
    rubric: str | None = None,
    manual_url: str | None = None,
    status: str = "ready",
    exclude_video_ids: set[str] | None = None,
) -> dict:
    """Pick a video, build the pack, persist to bt_3_world_news_daily. Returns the stored
    entry dict. Raises on any failure so callers can degrade cleanly. `exclude_video_ids`
    forces a DIFFERENT video (used by «переформировать»).

    Auto-pick (no manual_url) also excludes videos used by the rubric in the last
    WORLD_NEWS_ROTATE_DAYS days (default 14) so it stops re-selecting the same freshest video
    every day. If that rotation exclusion leaves nothing pickable, it retries WITHOUT it —
    variety is preferred, but never at the cost of having no news at all."""
    from backend.database import (
        upsert_world_news_daily, get_world_news_for_date, get_recent_world_news_video_ids,
        upsert_youtube_transcript_cache, get_shown_daily_video_ids, record_daily_video_shown,
    )
    from backend.daily_video_rubrics import get_profile, rubric_for_date

    date_str = (news_date or _today_str()).strip()
    rubric_key = (rubric or "").strip().lower() or rubric_for_date(date_str)
    profile = get_profile(rubric_key)
    base_exclude = {str(v).strip() for v in (exclude_video_ids or set()) if str(v).strip()}
    # Стендап вечнозелёный: повторить показанное — хуже, чем не показать ничего, потому что
    # человек решит, что рубрика сломалась. Поэтому у архивной стратегии повтор запрещён, а
    # пустой выбор поднимается ошибкой — вечерняя подготовка на неё уже умеет звать владельца.
    # У новостей поведение прежнее: разнообразие желательно, но никогда ценой пустого утра.
    allow_repeat_when_empty = profile.pick_strategy != "archive"

    picked, diag = None, {}
    if not manual_url:
        try:
            shown = get_shown_daily_video_ids(profile.key)
        except Exception:
            # Пустой ответ от сбоя базы неотличим от честного «мы ещё ничего не показывали»,
            # и молча приняв его, рубрика пошла бы по второму кругу. Поэтому падаем.
            logger.exception("daily_video[%s]: не удалось прочитать реестр показанного", profile.key)
            raise
        if profile.pick_strategy == "newest":
            # У новостей к вечному реестру добавляется прежнее окно в 14 дней по самой
            # таблице дня — поведение рубрики не меняется.
            rotate_days = _env_int("WORLD_NEWS_ROTATE_DAYS", 14)
            try:
                shown = shown | get_recent_world_news_video_ids(rotate_days)
            except Exception:
                logger.warning("world_news: recent-video lookup failed — оставляем вечный реестр",
                               exc_info=True)
        diag["shown_excluded"] = len(shown)
        rotated_exclude = base_exclude | shown
        if rotated_exclude:
            picked, diag = _pick_video_with_transcript(
                profile=profile, exclude_video_ids=rotated_exclude
            )
            if not picked and not allow_repeat_when_empty:
                raise RuntimeError(
                    f"daily_video[{profile.key}]: непоказанных роликов не осталось — "
                    f"нужно пополнить набор каналов. diag={diag}"
                )
            if not picked:
                logger.info(
                    "world_news: rotation exclude (%d shown) left nothing pickable — retrying without it (diag=%s)",
                    len(shown), diag,
                )
    if not picked:
        picked, diag = _pick_video_with_transcript(
            profile=profile, manual_url=manual_url, exclude_video_ids=base_exclude
        )
    if not picked:
        raise RuntimeError(f"world_news: no suitable video with transcript found — diag={diag}")

    pack = _validate_and_normalize_pack(
        _call_llm(picked["title"], picked["text"], profile), profile, picked["text"]
    )

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
        rubric=profile.key,
    )
    # Вечный реестр показанного — единственная память рубрики о том, что уже было: ночная
    # чистка стирает саму строку дня. Пишется СРАЗУ после сохранения пакета, а не в момент
    # утренней рассылки: если владелец переформирует день, израсходованный ролик всё равно
    # не должен вернуться завтра.
    record_daily_video_shown(
        video_id=picked["video_id"],
        rubric=profile.key,
        shown_on=date_str,
        video_title=picked["title"],
        channel_title=picked["channel_title"],
        had_manual_captions=picked.get("has_manual_captions"),
    )
    # Warm the shared transcript library so EVERY user — not just the library admin — gets the
    # German subtitles for this curated video. Without this, non-admin users (free AND non-admin
    # Pro) hit the library gate in the /youtube_transcript endpoint and see «Субтитры недоступны»,
    # because only the admin's own view live-fetches and caches the transcript. We store the German
    # `items` only; the RU translations layer stays on its own on-demand, Pro-gated path (the
    # cache upsert COALESCEs translations, so we never clobber any that get added later).
    try:
        upsert_youtube_transcript_cache(
            picked["video_id"],
            picked["items"],
            picked["lang"],
            picked.get("is_generated"),
        )
    except Exception:
        logger.warning(
            "world_news: transcript-cache warm failed video=%s", picked.get("video_id"), exc_info=True,
        )
    logger.info(
        "world_news: prepared %s video=%s phrases=%d quiz=%d",
        date_str, picked["video_id"], len(pack["phrases"]), len(pack["quiz"]),
    )
    entry = get_world_news_for_date(date_str)
    if not entry:
        raise RuntimeError("world_news: persisted entry not found after upsert")
    return entry
