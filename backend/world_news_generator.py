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
# Остатка квоты не хватило на холодный обход — мы решили НЕ ходить в сеть. Это не то же
# самое, что _QUOTA_EXCEEDED (там YouTube ответил отказом): здесь мы не потратили ничего.
_QUOTA_LOW = False
# Свип кандидатов кэшируется ОТДЕЛЬНО по рубрикам ('news' / 'standup'): иначе вечерняя
# подготовка стендапа получила бы новостной список каналов из тёплого кэша.
_CAND_CACHE: dict = {}

# ── Config (env-overridable) ────────────────────────────────────────────────────

def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


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


def _quota_spent(units: float) -> None:
    """Сообщить о потраченных единицах квоты YouTube в общий суточный счётчик.

    До 21.08.2026 рубрика ходила в YouTube МИМО счётчика: не сообщала о тратах и не
    спрашивала разрешения. Из-за этого счётчик показывал меньше, чем потрачено на самом
    деле, и остальные части приложения принимали решения по заниженному числу. Повод —
    21.08.2026: суточная квота кончилась, и `/standup` не смог подобрать ролик.

    Сбой самого счётчика не должен ронять подготовку выпуска — но и молчать о нём нельзя,
    иначе мы снова считаем вслепую.
    """
    try:
        from backend.backend_server import _youtube_quota_local_add
        _youtube_quota_local_add(float(units))
    except Exception:
        logger.warning("daily_video: не удалось учесть %s единиц квоты YouTube", units,
                       exc_info=True)


def _quota_allows(estimated_units: float) -> bool:
    """Хватит ли остатка квоты на запланированную трату (с неприкосновенным запасом).

    Спрашиваем ДО обхода: при пустом кошельке прежний код всё равно делал ~170 запросов,
    получал ~170 отказов и только потом говорил «ничего не нашлось» — медленно и невнятно.
    Если счётчик недоступен, идём в сеть: отказать в подготовке выпуска из-за сбоя
    счётчика — хуже, чем потратить единицы (YouTube сам вернёт 403, и мы это увидим).
    """
    try:
        from backend.backend_server import youtube_live_search_allowed
        return bool(youtube_live_search_allowed(float(estimated_units)))
    except Exception:
        logger.warning("daily_video: счётчик квоты недоступен — идём в сеть", exc_info=True)
        return True


def _quota_remaining_text() -> str:
    """Остаток квоты для человеческого сообщения владельцу. Пустая строка — если неизвестен."""
    try:
        from backend.backend_server import youtube_daily_quota_remaining
        left = youtube_daily_quota_remaining()
        return "" if left is None else str(int(left))
    except Exception:
        return ""


def _yt_refusal_reason(resp) -> str:
    """Почему YouTube отказал. В теле ответа он называет причину словом, и слова эти
    означают РАЗНОЕ:

      quotaExceeded / dailyLimitExceeded — суточные единицы кончились. До сброса (полночь
          по тихоокеанскому времени, это ~09:00 по Вене) сделать ничего нельзя.
      rateLimitExceeded / userRateLimitExceeded — единицы есть, но мы частим. Проходит за
          секунды.

    До 21.08.2026 код валил обе причины в одну кучу и сообщал «дневная квота исчерпана».
    Из-за этого сотня быстрых запросов подряд выглядела как суточный простой, и рубрику
    считали мёртвой до утра, хотя достаточно было подождать минуту.
    """
    try:
        payload = resp.json() or {}
    except Exception:
        return ""
    err = payload.get("error") or {}
    errors = err.get("errors") or []
    if errors and isinstance(errors[0], dict):
        return str(errors[0].get("reason") or "").strip()
    return str(err.get("status") or "").strip()


_RATE_LIMIT_REASONS = {"ratelimitexceeded", "userratelimitexceeded", "backenderror",
                       "servicelimitexceeded"}
_DAILY_QUOTA_REASONS = {"quotaexceeded", "dailylimitexceeded"}


def _yt_get(url: str, params: dict, *, cost: float, what: str) -> dict | None:
    """Запрос к YouTube Data API с честным разбором отказа.

    Возвращает разобранный ответ или None. При «мы частим» — короткая пауза и повтор:
    это проходит за секунды, и сдаваться тут значит терять выпуск на ровном месте
    (21.08.2026: сотня быстрых запросов подряд придушила ключ, и `/standup` сдался,
    хотя суточные единицы были целы). При «единицы кончились» повторять бессмысленно —
    ставим флаг и выходим сразу, до сброса всё равно ничего не изменится.
    """
    attempts = max(1, _env_int("YOUTUBE_RATE_LIMIT_RETRIES", 3))
    pause = max(1, _env_int("YOUTUBE_RATE_LIMIT_PAUSE_SEC", 4))
    global _QUOTA_EXCEEDED
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=12)
        except Exception:
            logger.warning("daily_video: сетевой сбой на %s", what, exc_info=True)
            return None
        _quota_spent(cost)
        if resp.status_code < 400:
            try:
                return resp.json()
            except Exception:
                logger.warning("daily_video: не разобрать ответ %s", what, exc_info=True)
                return None
        reason = _yt_refusal_reason(resp).lower()
        if reason in _DAILY_QUOTA_REASONS:
            _QUOTA_EXCEEDED = True
            logger.warning("daily_video: суточные единицы YouTube кончились (%s, %s)", what, reason)
            return None
        if resp.status_code == 429 or reason in _RATE_LIMIT_REASONS:
            if attempt + 1 < attempts:
                logger.info("daily_video: YouTube просит не частить (%s) — пауза %ds и повтор",
                            what, pause)
                time.sleep(pause)
                continue
            # Повторы кончились. Это НЕ суточный простой: единицы целы, просто мы частим.
            logger.warning("daily_video: YouTube придушил по частоте на %s — повторы исчерпаны", what)
            return None
        logger.info("daily_video: YouTube HTTP %s на %s (%s)", resp.status_code, what, reason or "—")
        return None
    return None


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
    payload = _yt_get("https://www.googleapis.com/youtube/v3/search", params,
                      cost=100, what=f"search {query!r}")  # search.list — 100 единиц
    if not payload:
        return []
    out = []
    for item in (payload.get("items") or []):
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
        payload = _yt_get("https://www.googleapis.com/youtube/v3/playlistItems", params,
                          cost=1, what=f"playlistItems {playlist_id}")  # 1 единица за страницу
        if not payload:
            break
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
        # statistics добавлен 21.08.2026: по числу просмотров полка стендапов решает,
        # какой ролик ставить раньше. Часть запроса, а не отдельный вызов — цена та же,
        # одна единица за пачку до 50 роликов.
        "part": "contentDetails,snippet,statistics",
        "id": ",".join(video_ids),
        "key": api_key,
    }
    payload = _yt_get("https://www.googleapis.com/youtube/v3/videos", params,
                      cost=1, what="videos.list")  # 1 единица за пачку до 50 роликов
    if not payload:
        return
    for item in (payload.get("items") or []):
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
            # YouTube помечает здесь ТОЛЬКО субтитры, положенные автором руками ("true");
            # машинная расшифровка в этот флаг не попадает. По нему рубрика ставит ролики
            # с ручными субтитрами первыми (решение владельца 20.08.2026).
            "has_manual_captions": str(content.get("caption") or "").strip().lower() == "true",
            "view_count": _as_int_or_none((item.get("statistics") or {}).get("viewCount")),
        }


def _as_int_or_none(value):
    """Число просмотров или None. Отсутствие статистики — это «не знаем», а не ноль:
    ноль означал бы «ролик никто не смотрел», и он уехал бы в конец очереди незаслуженно."""
    try:
        return int(str(value).strip())
    except Exception:
        return None


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

    global _QUOTA_LOW
    _QUOTA_EXCEEDED = False
    _QUOTA_LOW = False
    seen: set[str] = set()
    candidates: list[dict] = []
    archive = profile.pick_strategy == "archive"
    per_channel = 50 if archive else _env_int("WORLD_NEWS_PER_CHANNEL", 8)
    pages = profile.archive_pages if archive else 1

    # СПРАШИВАЕМ РАЗРЕШЕНИЕ ДО ТРАТЫ (21.08.2026). Холодный обход архива стоит примерно
    # столько: по одной единице за страницу списка роликов (каналы × страницы) плюс по
    # одной за пачку из 50 роликов в справке. При пустом кошельке прежний код всё равно
    # делал все эти запросы, получал столько же отказов и лишь потом говорил «ничего не
    # нашлось». Теперь при нехватке остатка в сеть не идём НИ РАЗУ, а причина уходит
    # наверх, чтобы владелец увидел число и понял, когда автоподбор вернётся.
    # Ручная выдача по ссылке сюда не заходит: она стоит одну единицу и обязана работать
    # всегда, даже когда квота исчерпана, — рубрика не должна умирать полностью.
    if archive:
        channels_count = len(profile_channel_ids(profile))
        # Страницы списка роликов: по одной единице за каждую. Справка о роликах: одна
        # единица за пачку до 50 — считаем по тому же потолку, что применяется ниже.
        pages_cost = channels_count * max(1, pages)
        details_cost = -(-min(_env_int("STANDUP_CANDIDATES", 4000),
                              channels_count * max(1, pages) * 50) // 50)
        est_units = pages_cost + details_cost
        if not _quota_allows(est_units):
            left = _quota_remaining_text()
            logger.warning(
                "daily_video[%s]: обход архива пропущен — остатка квоты не хватает "
                "(нужно ~%s единиц, осталось %s)", profile.key, est_units, left or "неизвестно",
            )
            _QUOTA_LOW = True
            _CAND_CACHE.setdefault(profile.key, {"ts": 0.0, "items": []})
            return []
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
        if _QUOTA_LOW:
            diag["reason"] = "youtube_quota_low"
            diag["quota_left"] = _quota_remaining_text()
        elif _QUOTA_EXCEEDED:
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
    # Считаем пул ПРЯМО ЗДЕСЬ: справка о роликах уже получена и оплачена, значит «сколько
    # подходит по длине и сколько с ручными субтитрами» достаётся даром. Эти числа уходят
    # в снимок пула, из которого потом собирается еженедельный отчёт — без второго обхода.
    _in_range = 0
    _manual = 0
    for _c in candidates:
        _d = details_map.get(_c["video_id"]) or {}
        _dur = int(_d.get("duration_seconds") or 0)
        if _dur and profile.min_seconds <= _dur <= profile.max_seconds:
            _in_range += 1
            if _d.get("has_manual_captions"):
                _manual += 1
    diag["pool_scanned"] = len(candidates)
    diag["pool_in_range"] = _in_range
    diag["pool_manual_captions"] = _manual

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
   ["Правительство Германии меняет закон о налогах и больничных",
    "Новые атаки России на Украину", "Крупный штраф Google в Европе"].
   SCHREIBWEISE: Eigennamen behalten ihren GROSSBUCHSTABEN — Länder, Städte, Bundesländer,
   Parteien, Organisationen, Personen (Германия, Саксония-Анхальт, Мекленбург-Передняя
   Померания, АдГ, Бундестаг). Abkürzungen so, wie sie im Russischen üblich sind: AfD → АдГ,
   nicht «афд». «Knapp» heisst OHNE Wasser — NICHT ohne Grossbuchstaben. Jede These beginnt
   mit einem Grossbuchstaben.
   ACHTUNG: NUR DER ERSTE BUCHSTABE ist gross, der Rest klein. Richtig: «Тереза»,
   «Германия». FALSCH: «ТЕРЕЗА», «ГЕРМАНИЯ» — Wörter komplett in Grossbuchstaben sind
   ein Fehler.


   EIGENNAMEN: die Spracherkennung verstümmelt Namen von Ämtern, Parteien und Personen
   («Bafer» statt BAFA, «Rentenpflege» statt «Renten-, Pflege-»). Schreib den Namen
   RICHTIG, so wie die Einrichtung wirklich heisst. Bist du dir nicht sicher, wie sie
   heisst — benutz den Namen GAR NICHT, weder in den Thesen noch in den Fragen. Einen
   falschen Namen liest der Nutzer als richtigen.

2) "phrases": 12–18 SPRACHEINHEITEN aus dem Transkript.

   WAS EINE SPRACHEINHEIT IST: ein Wort oder eine feste Wendung, die dem Lernenden in
   einer ANDEREN Nachricht und in einem ANDEREN Gespräch wiederbegegnet.
     RICHTIG: "der Schwarzmarkt", "unter strengen Auflagen", "unter Druck stehen",
       "Tür und Tor öffnen (für etwas)", "einen langen Atem brauchen", "in Kraft treten".
     FALSCH — und das ist der häufigste Fehler:
       • ZAHLEN AUS DIESER MELDUNG: "rund 300 Aussteller", "bis zu 30.000 Besuchern",
         "21,5 Milliarden Euro". Morgen stehen dort andere Zahlen; gelernt wird nichts.
         Brauchst du das Wort, nimm es NACKT: "der Aussteller", "der Besucher".
       • AMTS- UND TITELBEZEICHNUNGEN: "der Drogenbeauftragte der Bundesregierung",
         "das Bundesamt für Wirtschaft" — Namen von Stellen, keine Sprache.
       • GANZE SÄTZE UND SATZTEILE mit Subjekt und konjugiertem Verb: "Opfer fordern ihre
         Rechte", "eine wirksame Kontrolle muss es geben".
       • ZWEI EINHEITEN MIT KOMMA ZUSAMMENGEKLEBT: "ein neuer Markt, der Graumarkt" —
         nimm EINE davon: "der Graumarkt".
       • DIE WENDUNG MITSAMT IHREM OBJEKT: aus «hat Tür und Tor für weniger Jugendschutz
         geöffnet» gehört "Tür und Tor öffnen (für etwas)" auf die Karte — NICHT der ganze
         Satzteil mit Objekt und Partizip. Die Wendung wird HERAUSGELÖST, nicht mitkopiert.

   FORM DER EINHEIT:
     • einzelnes Nomen → mit Artikel, richtig gross geschrieben: "der Schwarzmarkt";
     • einzelnes Verb → Infinitiv: "zurückdrängen"; reflexiv MIT "sich";
     • feste Wendung → so, wie man sie nachschlägt, mit Platzhalter statt konkretem
       Objekt: "Tür und Tor öffnen (für etwas)", "unter strengen Auflagen".
     Eine Einheit endet NIE auf einem Artikel oder einer Konjunktion — endet sie so, hast
     du mitten im Satz abgeschnitten.

   ZUR SPRACHERKENNUNG: das Transkript ist maschinell und enthält Tippfehler
   («Jugendchutz» statt «Jugendschutz», «streg» statt «streng») und zerbrochene Sätze.
   Wähle eine Einheit NUR aus einer Zeile, die sauber ist. Ist die einzige Stelle
   verstümmelt, nimm die Einheit NICHT — der Lernende darf falsch geschriebenes Deutsch
   nicht zu sehen bekommen.

   Jedes Element:
     - "de": die Einheit nach den Regeln oben, korrekt geschrieben.

   ВОЗВРАТНОСТЬ ЧИТАЕТСЯ ИЗ ЦИТАТЫ, А НЕ ДОДУМЫВАЕТСЯ. Не всякому немецкому глаголу нужно
   «sich», и подставлять его «для словарной формы» — значит выдумывать грамматику.
   Смотри, какое дополнение стоит в цитате:
     • «habe ich DICH unter den Tisch gesoffen» → дополнение не возвратное, значит единица
       «jemanden unter den Tisch saufen» (перепить кого-то). Ставить «sich» здесь НЕЛЬЗЯ:
       «sich unter den Tisch saufen» значит другое — напиться до бесчувствия самому.
     • «um SICH ein Bild zu machen» → возвратное, значит «sich ein Bild machen».
   Нет в цитате ни «sich», ни личного дополнения — не добавляй ничего от себя.
     - "form_ru": in welcher Form "de" dasteht — ЗАКРЫТЫЙ СПИСОК, пиши ТОЛЬКО одно из
       этих значений, дословно: «словарная форма» · «устойчивое выражение» · «инфинитив» ·
       «именительный падеж» · «винительный падеж» · «дательный падеж» ·
       «родительный падеж» · «множественное число» · «повелительная форма».
       Ничего своего не сочиняй и НЕ добавляй немецких слов в помету: «инфинитив с sich» —
       так нельзя, человек не знает, что такое sich, и подпись ему ничего не объясняет.
       Существительное в словарном виде падежа НЕ имеет — это «словарная форма»,
       а возвратный глагол в словарном виде — тоже просто «словарная форма».
     - "translation_ru": knappe russische Übersetzung in DERSELBEN Form wie "de".
     - "de_in_text": die Einheit GENAU SO, wie sie in "quote_de" steht — nur die Einheit,
       nicht der ganze Satz. MUSS wörtlich im Zitat vorkommen.
     - "quote_de": der Satz aus dem TRANSKRIPT, in dem die Einheit vorkommt — wörtlich
       kopiert, 4–20 Wörter, sauber (siehe oben), und er MUSS die Einheit zeigen.
     - "quote_ru": Übersetzung genau dieses Satzes.
     - "usage_ru": ein kurzer russischer Hinweis, WIE/WANN man es benutzt (1 Satz),
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
    {{"de": "…", "form_ru": "…", "translation_ru": "…", "de_in_text": "…",
      "quote_de": "…", "quote_ru": "…", "usage_ru": "…"}}
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
    # Разбор стендапа втрое тяжелее новостного: у каждой единицы семь полей вместо трёх
    # (помета регистра, форма, значение здесь, обычное значение, цитата, её перевод,
    # употребление), и единиц бывает до восемнадцати. Замер 21.08.2026: на 120 секундах
    # запрос не успевал, и вечерняя подготовка падала по таймауту — причём уже ПОСЛЕ того,
    # как ролик выбран и субтитры получены, то есть вся дорогая работа шла впустую.
    timeout_sec = _env_int("DAILY_VIDEO_LLM_TIMEOUT_SEC", 240)
    attempts = max(1, _env_int("DAILY_VIDEO_LLM_RETRIES", 2))
    resp = None
    for attempt in range(attempts):
        started = time.monotonic()
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers, json=payload, timeout=timeout_sec,
            )
        except requests.Timeout:
            spent = int(time.monotonic() - started)
            if attempt + 1 < attempts:
                logger.warning("daily_video: модель молчала %ds — повтор (%d/%d)",
                               spent, attempt + 2, attempts)
                continue
            raise RuntimeError(
                f"модель не ответила за {timeout_sec}s ({attempts} попытки) — "
                "ролик и субтитры уже получены, повтори подготовку"
            )
        # Сколько модель думала на самом деле — иначе следующий таймаут снова придётся
        # разбирать вслепую.
        logger.info("daily_video: модель ответила за %ds", int(time.monotonic() - started))
        break
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


# Отделяемые приставки: в живой речи глагол разрывается («ausrasten» → «da rasten alle
# aus»), поэтому искать единицу в цитате целиком нельзя — так выбрасываются ПРАВИЛЬНЫЕ
# карточки. Эта грабля в репозитории уже известна по заданиям с отделяемыми глаголами.
_SEPARABLE_PREFIXES = (
    "zusammen", "zurück", "vorbei", "durch", "nieder", "gegen", "unter", "über",
    "hinter", "voran", "vorau", "fort", "fest", "statt", "weiter",
    "aus", "auf", "ein", "mit", "nach", "vor", "weg", "her", "hin", "los", "ab",
    "an", "bei", "um", "zu",
)

# Служебные слова: по ним искать бессмысленно, они есть в любой строке.
_FUNCTION_WORDS = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer",
    "sich", "und", "oder", "nicht", "am", "im", "in", "an", "auf", "mit", "es", "zu",
    "etwas", "jemand", "jemandem", "jemanden", "man", "ist", "sein", "haben", "werden",
}


def _unit_roots(de: str) -> list[str]:
    """Корни, по которым единицу можно узнать в цитате, даже если она там изменена.

    Из «ausrasten» получаются «ausrast» и «rast»: второй найдётся в «da rasten alle aus».
    Из «nichts am Hut haben» — «nichts», «hut», «hab». Служебные слова отбрасываются:
    искать «am» в немецкой строке бессмысленно, оно есть везде.
    """
    text = re.sub(r"\(.*?\)", " ", str(de or ""))          # пояснения в скобках не ищем
    words = re.findall(r"[A-Za-zÄÖÜäöüß]+", text)
    content = [w for w in words if w.lower() not in _FUNCTION_WORDS and len(w) >= 3]
    if not content:
        content = words
    roots: list[str] = []
    for word in content:
        low = word.lower()
        variants = [low]
        for prefix in _SEPARABLE_PREFIXES:
            if low.startswith(prefix) and len(low) > len(prefix) + 2:
                variants.append(low[len(prefix):])
                break
        for variant in variants:
            root = re.sub(r"(en|n|e)$", "", variant)        # инфинитивное окончание
            if len(root) >= 3:
                roots.append(root)
    return roots


def _quote_shows_the_unit(de: str, quote: str) -> bool:
    """Показывает ли цитата разбираемое слово.

    Повод (первый живой стендап, 21.08.2026): карточка «ausrasten» получила цитату про то,
    как все будут громко смеяться, — слова там не было вовсе. Страж проверял, что цитата
    есть в СУБТИТРАХ, но не проверял, что она показывает саму единицу. А ведь ради этого
    цитата и нужна: человек должен увидеть, как это ГОВОРЯТ.
    """
    roots = _unit_roots(de)
    if not roots:
        return True          # опознавать нечего — не выбрасываем по формальному признаку
    fp = _quote_fingerprint(quote)
    return any(root in fp for root in roots)


# Слова, на которые единица кончаться НЕ МОЖЕТ: артикли и союзы. Кончается — значит
# фразу отрезали посреди предложения и оставили висящий хвост: «die Koalition auffordern,
# die» (случай владельца 22.08.2026 — на экране это выглядит как поломка, и это она и есть).
#
# Список ЗАКРЫТЫЙ и состоит из служебных слов — никакого разбора грамматики здесь нет, а
# значит нет и догадок. Глаголы в конце не запрещены: «nichts am Hut haben» законно.
_DANGLING_TAIL_WORDS = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer",
    "und", "oder", "aber", "dass", "weil", "wenn", "als", "sondern", "damit",
}


def _headword_ends_dangling(de: str) -> bool:
    text = str(de or "").strip()
    if not text:
        return False
    if text.endswith(","):
        return True
    words = re.findall(r"[A-Za-zÄÖÜäöüß]+", text)
    return bool(words) and words[-1].lower() in _DANGLING_TAIL_WORDS


def _card_passes_source_guards(card: dict, transcript_text: str, profile=None) -> tuple[bool, str]:
    """Проходит ли карточка проверки. Возвращает (прошла, первая претензия).

    Проверки живут в ОДНОМ месте — backend/daily_video_quality.py. Раньше они были
    рассыпаны по генератору, судье и тестам, каждая со своим счётчиком, и одно правило
    жило в трёх местах, устаревая в двух из них.

    Через это обязана пройти не только свежая карточка от модели, но и ИСПРАВЛЕННАЯ
    судьёй — иначе его правка стала бы дырой в той самой защите, ради которой он поставлен.
    """
    from backend.daily_video_quality import check_card

    problems = check_card(
        card, transcript=transcript_text,
        requires_register=bool(profile is not None and getattr(profile, "requires_register", False)),
    )
    if problems:
        return False, problems[0][1]
    return True, ""


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
    dropped_quote_off_topic = 0
    dropped_neutral = 0
    dropped_form_not_in_quote = 0

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
            # ВТОРОЙ СТРАЖ ЦИТАТЫ: строка обязана ПОКАЗЫВАТЬ разбираемое слово, а не просто
            # существовать в субтитрах. Карточка «ausrasten» с цитатой про смех (21.08.2026)
            # обманывает: человек читает «вот как это звучит», а слова там нет.
            if not _quote_shows_the_unit(de, quote_de):
                dropped_quote_off_topic += 1
                continue
            # ФОРМА ИЗ ТЕКСТА (решение владельца 22.08.2026). На карточке человек видит
            # ДВЕ формы: словарную — чтобы сохранить и выучить, и ту, что стоит в ролике, —
            # чтобы узнать выученное в живой речи. Без второй он услышит «da rasten alle
            # aus» и не свяжет это с «ausrasten».
            # Проверяемо насквозь: форма из текста обязана дословно найтись в цитате,
            # цитата — в субтитрах. Выдумать негде ни на одном шаге.
            de_in_text = str(p.get("de_in_text") or "").strip()
            if not de_in_text:
                dropped_thin += 1
                continue
            if _quote_fingerprint(de_in_text) not in _quote_fingerprint(quote_de):
                dropped_form_not_in_quote += 1
                continue
            item["de_in_text"] = de_in_text
            # Нейтральное бытовое слово в рубрике сленга — это добор до количества, ровно
            # тот мусор, которого владелец просил избегать. `die Kommentarspalte` с пометой
            # «нейтральное» (21.08.2026) человек и так знает, а место карточки занял.
            if needs_register and register.strip().lower().startswith("нейтральн"):
                dropped_neutral += 1
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

    if needs_quote and (dropped_no_quote or dropped_thin or dropped_quote_off_topic
                        or dropped_neutral or dropped_form_not_in_quote):
        logger.info(
            "daily_video[%s]: карточек отброшено — цитаты нет в субтитрах: %d, цитата не "
            "показывает слово: %d, формы нет в цитате: %d, нейтральных: %d, "
            "неполных: %d (осталось %d)",
            getattr(profile, "key", "?"), dropped_no_quote, dropped_quote_off_topic,
            dropped_form_not_in_quote, dropped_neutral, dropped_thin, len(phrases),
        )
    if len(phrases) < min_phrases:
        raise ValueError(
            f"need >={min_phrases} valid phrases, got {len(phrases)} "
            f"(quote_not_in_transcript={dropped_no_quote}, "
            f"quote_off_topic={dropped_quote_off_topic}, "
            f"form_not_in_quote={dropped_form_not_in_quote}, neutral={dropped_neutral}, "
            f"incomplete={dropped_thin})"
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

    # Тест не спрашивает то, что уже разобрано карточкой. Повод (22.08.2026): карточка
    # объясняла «Digger», а второй вопрос теста спрашивал, что это слово значит — ответ
    # человек прочитал строкой выше, и вопрос перестал что-либо проверять.
    # Запрет стоял в задании модели и НЕ сработал, поэтому ловим механически: если
    # формулировка вопроса содержит саму единицу с карточки, пакет бракуется и модель
    # переспрашивается. Латать вопрос своими руками нельзя — придумывать за неё нечего.
    if needs_quote:
        # Бракуем ТОЛЬКО вопрос, который спрашивает ЗНАЧЕНИЕ уже разобранной единицы —
        # человек прочитал ответ строкой выше, и вопрос ничего не проверяет.
        #
        # Первая версия этой проверки была слишком жадной: она бракевала любой вопрос, где
        # встречались слова карточки. 22.08.2026 карточка «vollständig gelöscht» и законный
        # вопрос «Wann wurde das Feuer vollständig gelöscht?» (КОГДА потушили, а не что это
        # значит) забраковали весь пакет трижды подряд — выпуск не собрался вовсе. Защита
        # оказалась вреднее дефекта, от которого стерегла.
        #
        # Признак вопроса-определения: единица взята в кавычки ИЛИ рядом стоит слово,
        # которым по-немецки спрашивают значение.
        meaning_markers = ("gemeint", "bedeutet", "bedeutung", "versteht man", "heißt",
                           "heisst", "rolle spielt", "meint ")
        for q in quiz:
            question = str(q["question_de"])
            q_low = question.lower()
            q_fp = _quote_fingerprint(question)
            asks_meaning = any(m in q_low for m in meaning_markers)
            for p in phrases:
                key = _quote_fingerprint(p["de"])
                if len(key) < 4 or key not in q_fp:
                    continue
                quoted = any(f"{mark}{p['de']}{close}" in question
                             for mark, close in (("«", "»"), ("'", "'"), ('"', '"')))
                if quoted or asks_meaning:
                    raise ValueError(
                        f"quiz question asks the meaning of a card unit: {p['de']!r}"
                    )

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
        upsert_daily_video_pool_snapshot,
    )
    from backend.daily_video_rubrics import get_profile, rubric_for_date

    date_str = (news_date or _today_str()).strip()
    # Что уже стояло на этот день. Если сейчас подставим ДРУГОЙ ролик, а прежний людям не
    # уходил, — он возвращается на полку: тратится ролик тогда, когда он дошёл до человека,
    # а не когда его подставили в черновик (повод — полка, опустевшая за вечер 22.08.2026).
    try:
        _previous = get_world_news_for_date(date_str)
    except Exception:
        logger.warning("daily_video: не удалось прочитать прежнюю запись на %s", date_str,
                       exc_info=True)
        _previous = None
    rubric_key = (rubric or "").strip().lower() or rubric_for_date(date_str)
    profile = get_profile(rubric_key)
    base_exclude = {str(v).strip() for v in (exclude_video_ids or set()) if str(v).strip()}
    # Стендап вечнозелёный: повторить показанное — хуже, чем не показать ничего, потому что
    # человек решит, что рубрика сломалась. Поэтому у архивной стратегии повтор запрещён, а
    # пустой выбор поднимается ошибкой — вечерняя подготовка на неё уже умеет звать владельца.
    # У новостей поведение прежнее: разнообразие желательно, но никогда ценой пустого утра.
    allow_repeat_when_empty = profile.pick_strategy != "archive"

    picked, diag = None, {}
    # ── ПОЛКА ───────────────────────────────────────────────────────────────────
    # Стендап берёт готовое: ролик уже отобран, и субтитры к нему уже скачаны. В момент
    # выпуска мы не ходим ни в YouTube, ни за субтитрами — обе эти дороги 20–21.08.2026
    # оказались перекрыты ровно тогда, когда рубрика была нужна.
    if not manual_url and profile.uses_shelf:
        from backend.database import take_next_from_standup_shelf
        shelf_item = take_next_from_standup_shelf(base_exclude)
        if not shelf_item:
            # Полка пуста. Пробуем пополнить ОДИН раз и берём снова: это тот же источник,
            # просто добираем его сейчас, а не по расписанию. Если и после этого пусто —
            # честно падаем, а не подсовываем повтор.
            logger.warning("daily_video[%s]: полка пуста — пробую пополнить на месте", profile.key)
            try:
                from backend.standup_shelf import refill_standup_shelf
                # Аварийное пополнение: добрать столько, чтобы выпуск состоялся И полка
                # не осталась на нуле до ночи. Бюджет считается от потолка подготовки:
                # он 600 секунд, из них ~250 нужно модели на разбор, значит пополнению
                # можно отдать 200 и оно спокойно возьмёт несколько роликов.
                # 22.08.2026 здесь стояло «два ролика, 70 секунд» — от тех времён, когда
                # потолок был 300. Потолок я поднял, а это забыл, и полка, кончившаяся
                # днём, не пополнилась вовсе.
                refill_standup_shelf(
                    max_add=_env_int("STANDUP_EMERGENCY_REFILL_MAX", 6),
                    budget_sec=_env_int("STANDUP_EMERGENCY_REFILL_BUDGET_SEC", 200),
                )
            except Exception:
                logger.exception("daily_video[%s]: пополнение полки не удалось", profile.key)
            shelf_item = take_next_from_standup_shelf(base_exclude)
        if not shelf_item:
            raise RuntimeError(
                f"daily_video[{profile.key}]: полка пуста и пополнить её не удалось — "
                "нужен ролик вручную или пополнение набора каналов"
            )
        text = _transcript_to_text(shelf_item["transcript"])
        if len(text) < WORLD_NEWS_MIN_TRANSCRIPT_CHARS:
            # На полку кладутся только ролики с проверенными субтитрами, так что сюда мы
            # попасть не должны. Если попали — это порча данных, и её надо видеть.
            raise RuntimeError(
                f"daily_video[{profile.key}]: у ролика {shelf_item['video_id']} с полки "
                f"субтитры короче порога ({len(text)} симв.) — полка испорчена"
            )
        picked = {
            "video_id": shelf_item["video_id"],
            "video_url": f"https://www.youtube.com/watch?v={shelf_item['video_id']}",
            "title": shelf_item["video_title"],
            "channel_title": shelf_item["channel_title"],
            "duration_seconds": shelf_item["duration_seconds"],
            "lang": shelf_item["transcript_lang"],
            "text": text[:WORLD_NEWS_MAX_TRANSCRIPT_CHARS],
            "items": shelf_item["transcript"],
            "is_generated": shelf_item["transcript_is_generated"],
            "has_manual_captions": shelf_item["has_manual_captions"],
        }
        diag = {"rubric": profile.key, "source": "shelf"}

    if not picked and not manual_url:
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

    # Модель иногда возвращает пакет с изъяном — например, в одном вопросе теста три
    # варианта вместо четырёх (случай 21.08.2026). Раньше это убивало ВЕСЬ пакет вместе
    # с хорошим разбором слов, и выпуск срывался целиком.
    #
    # Чинить кривой ответ своими руками нельзя: обрезать лишние варианты значит рискнуть
    # выкинуть правильный, а дописать недостающие — выдумать. Поэтому мы не латаем ответ,
    # а ПЕРЕСПРАШИВАЕМ модель. Ролик и субтитры уже получены, повтор стоит только запроса.
    pack_attempts = max(1, _env_int("DAILY_VIDEO_PACK_RETRIES", 3))
    pack = None
    last_err = None
    for attempt in range(pack_attempts):
        try:
            pack = _validate_and_normalize_pack(
                _call_llm(picked["title"], picked["text"], profile), profile, picked["text"]
            )
            break
        except ValueError as exc:
            last_err = exc
            logger.warning(
                "daily_video[%s]: модель вернула негодный пакет (попытка %d из %d): %s",
                profile.key, attempt + 1, pack_attempts, exc,
            )
    if pack is None:
        raise RuntimeError(
            f"daily_video[{profile.key}]: модель {pack_attempts} раза подряд вернула "
            f"негодный разбор — последняя причина: {last_err}"
        )

    # ── АРТИКЛЬ БЕРЁТСЯ ИЗ СПРАВОЧНИКА, А НЕ У МОДЕЛИ ─────────────────────────
    # Правило ноль: ответ берётся из источника, модель только читает. Род существительного
    # — ровно тот случай, где источник есть и давно построен (article_authority: свой
    # справочник + Wiktionary). Спрашивать род у модели, когда справочник под рукой, значит
    # предпочесть догадку знанию.
    # Идёт ДО судьи: пусть он видит уже выверенные артикли и не тратит проход на них.
    # Справочник промолчал — карточка остаётся как есть: неизвестность честнее выдумки.
    article_fixes = []
    try:
        from backend.daily_video_quality import (
            correct_article_from_reference, correct_spelling_from_reference, word_not_german,
        )
        checked = []
        for card in pack["phrases"]:
            # Справочник прямо говорит, что такого слова нет («Abschiebu») — карточке не
            # место на экране. Помеченную живую речь сюда не пускают: справочники знают
            # сленг плохо, и владелец 22.08.2026 решил верить карточке, а не справочнику.
            junk = word_not_german(card, allow_network=True)
            if junk:
                article_fixes.append(f"выброшена: {junk}")
                continue
            fixed, what_art = correct_article_from_reference(card, allow_network=True)
            fixed, what_spell = correct_spelling_from_reference(fixed, allow_network=True)
            for what in (what_art, what_spell):
                if what:
                    article_fixes.append(f"«{card.get('de')}»: {what}")
            checked.append(fixed)
        pack["phrases"] = checked
    except Exception:
        # Недоступный справочник не повод рушить выпуск, но и молчать нельзя: значит
        # артикли в этом выпуске держатся на слове модели.
        logger.warning("daily_video[%s]: сверка артиклей со справочником не отработала",
                       profile.key, exc_info=True)
    if article_fixes:
        logger.info("daily_video[%s]: справочник вмешался %d раз(а)",
                    profile.key, len(article_fixes))

    # ── СУДЬЯ ПРИЁМКИ ──────────────────────────────────────────────────────────
    # Идёт ПО КАРТОЧКАМ и правит их поштучно. Ролик, субтитры и тест не трогает вовсе:
    # выбрасывать готовый выпуск из-за одного слова со строчной буквы — это выбрасывать
    # выбранный ролик, скачанные субтитры и десяток хороших карточек, чтобы получить новую
    # лотерею (решение владельца 22.08.2026).
    judge_report = {}
    if _env_flag("DAILY_VIDEO_JUDGE_ENABLED", True):
        try:
            from backend.daily_video_judge import judge_and_repair_cards
            pack["phrases"], judge_report = judge_and_repair_cards(
                pack["phrases"], profile=profile, transcript=picked["text"]
            )
        except Exception:
            # Судья — заслон, а не поставщик содержания. Его падение не должно ронять
            # выпуск, но и молчать нельзя: непроверенный пакет обязан быть виден как
            # непроверенный, иначе мы решим, что проверка была.
            logger.exception("daily_video[%s]: судья приёмки не отработал", profile.key)
            judge_report = {"failed": True}
    if article_fixes:
        judge_report = dict(judge_report or {})
        judge_report["article_fixes"] = article_fixes
        judge_report.setdefault("reasons", []).extend(article_fixes)
    if len(pack["phrases"]) < profile.min_phrases:
        # После правки карточек осталось меньше порога — значит ролик и вправду беден на
        # годный материал. Вот ЭТО повод взять другой, а не одна помарка.
        raise RuntimeError(
            f"daily_video[{profile.key}]: после приёмки осталось {len(pack['phrases'])} "
            f"карточек при пороге {profile.min_phrases} — ролик беден на годный материал"
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
        judge_report=judge_report,
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
    # Ролик израсходован — помечаем на полке, чтобы он не вышел вторым кругом. С полки не
    # удаляем: так видно, что рубрика уже показывала, даже когда реестр почистят.
    if diag.get("source") == "shelf":
        from backend.database import mark_standup_shelf_used
        mark_standup_shelf_used(picked["video_id"], date_str)
    # Прежний ролик этого дня возвращается на полку, если он был другим и НЕ уходил людям.
    if (_previous and _previous.get("video_id")
            and _previous["video_id"] != picked["video_id"]
            and str(_previous.get("status") or "") != "sent"):
        try:
            from backend.database import release_standup_shelf_video
            if release_standup_shelf_video(_previous["video_id"]):
                logger.info("daily_video[%s]: ролик %s возвращён на полку — людям он не уходил",
                            profile.key, _previous["video_id"])
        except Exception:
            logger.warning("daily_video: не удалось вернуть ролик %s на полку",
                           _previous.get("video_id"), exc_info=True)
    # Снимок пула — из того, что обход и так увидел. Отчёт владельцу собирается потом из
    # него и из реестра показанного, не тратя ни единицы квоты. При ручной выдаче по ссылке
    # обхода не было, и снимка нет — тогда прежний остаётся нетронутым, а не обнуляется.
    if diag.get("pool_in_range") is not None:
        try:
            upsert_daily_video_pool_snapshot(
                rubric=profile.key,
                scanned=int(diag.get("pool_scanned") or 0),
                in_range=int(diag.get("pool_in_range") or 0),
                manual_captions=int(diag.get("pool_manual_captions") or 0),
                measured_on=date_str,
            )
        except Exception:
            # Снимок — материал для отчёта, а не для выпуска: его потеря не повод рушить
            # уже собранный разбор. Но и молчать нельзя, иначе отчёт тихо застареет.
            logger.warning("daily_video[%s]: снимок пула не записан", profile.key, exc_info=True)
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
