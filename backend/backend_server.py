# import os
# from flask import Flask, request, jsonify
# from flask_cors import CORS
# from dotenv import load_dotenv
# from livekit.api import AccessToken, VideoGrants

# # Загружаем переменные окружения (LIVEKIT_API_KEY и LIVEKIT_API_SECRET) из файла .env
# load_dotenv()

# # --- Настройка Flask-сервера ---
# app = Flask(__name__)
# # CORS - это механизм безопасности браузера. Эта строка разрешает вашему фронтенду (на localhost:5173)
# # делать запросы к этому бэкенду (на localhost:5001).
# CORS(app) 

# # --- Получение ключей LiveKit из окружения ---
# LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY")
# LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET")

# # Проверка, что ключи существуют
# if not LIVEKIT_API_KEY or not LIVEKIT_API_SECRET:
#     raise RuntimeError("LIVEKIT_API_KEY и LIVEKIT_API_SECRET должны быть установлены в .env файле")

# # --- Главная и единственная точка доступа (API Endpoint) ---
# @app.route("/token", methods=['GET'])
# def get_token():
#     user_id = request.args.get('user_id')
#     username = request.args.get('username')

#     if not username or not user_id:
#         return jsonify({"error": "Нужны и user_id, и username"}), 400

#     #user_id = username

#     # Создаем права доступа
#     grant = VideoGrants(
#         room_join=True,
#         room="sales-assistant-room",
#     )

#     # Создаем токен с помощью правильной "цепочки" методов
#     access_token = AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET) \
#         .with_identity(user_id) \
#         .with_name(username) \
#         .with_grants(grant) # <--- ИСПРАВЛЕНО ЗДЕСЬ

#     # Возвращаем готовый токен
#     return jsonify({"token": access_token.to_jwt()})


# # --- Запуск сервера ---
# if __name__ == '__main__':
#     # Запускаем сервер на порту 5001, доступный для всех устройств в вашей сети
#     # debug=True автоматически перезагружает сервер при изменениях в коде
#     app.run(host="0.0.0.0", port=5001, debug=True)


import subprocess
import hashlib
import io
from urllib.parse import urlparse
from youtube_transcript_api import YouTubeTranscriptApi
import os
import hmac
import hashlib
import json
import asyncio
import logging
import requests
import tempfile
import base64
import time
import random
import http.cookiejar
import re
import html
from zoneinfo import ZoneInfo
from datetime import timedelta, date
from calendar import monthrange
import importlib.metadata as importlib_metadata
import youtube_transcript_api as yta
from youtube_transcript_api.proxies import GenericProxyConfig, WebshareProxyConfig
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4
from urllib.parse import parse_qsl, urlparse, urlencode
from flask import Flask, request, jsonify, send_from_directory, send_file, g, redirect
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException
from backend.database import get_db_connection_context
from backend.translation_workflow import _extract_correct_translation
from livekit.api import AccessToken, VideoGrants
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
try:
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None
try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None
try:
    import pyttsx3
except Exception:  # pragma: no cover - optional dependency
    pyttsx3 = None
from backend.utils import prepare_google_creds_for_tts
from pydub import AudioSegment
try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:  # pragma: no cover - optional in some deploys
    BackgroundScheduler = None
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional in some deploys
    matplotlib = None
    plt = None

from backend.openai_manager import (
    run_check_translation,
    run_check_translation_multilang,
    run_dictionary_lookup,
    run_dictionary_lookup_de,
    run_dictionary_lookup_multilang,
    run_dictionary_collocations,
    run_dictionary_collocations_multilang,
    run_translate_subtitles_ru,
    run_translate_subtitles_multilang,
    run_translation_explanation,
    run_translation_explanation_multilang,
    run_audio_sentence_grammar_explain_multilang,
    run_feel_word,
    run_feel_word_multilang,
    run_enrich_word,
    run_enrich_word_multilang,
    run_theory_generation,
    run_theory_practice_sentences,
    run_theory_check_feedback,
    run_beginner_topic,
    run_tts_chunk_de,
    get_last_llm_usage,
)
from backend.database import (
    is_telegram_user_allowed,
    ensure_webapp_tables,
    get_pending_daily_sentences,
    get_webapp_dictionary_entries,
    get_webapp_translation_history,
    get_latest_daily_sentences,
    save_webapp_dictionary_query,
    save_webapp_translation,
    get_dictionary_cache,
    upsert_dictionary_cache,
    get_youtube_transcript_cache,
    upsert_youtube_transcript_cache,
    upsert_youtube_translations,
    get_flashcard_set,
    create_dictionary_folder,
    get_dictionary_folders,
    get_or_create_dictionary_folder,
    update_webapp_dictionary_entry,
    get_dictionary_entry_by_id,
    get_tts_chunk_cache,
    upsert_tts_chunk_cache,
    get_tts_audio_cache,
    upsert_tts_audio_cache,
    get_next_due_srs_card,
    get_next_new_srs_candidate,
    count_due_srs_cards,
    count_new_cards_introduced_today,
    count_available_new_srs_cards,
    ensure_new_srs_state,
    get_card_srs_state,
    upsert_card_srs_state,
    get_dictionary_entry_for_user,
    insert_card_review_log,
    record_telegram_system_message,
    get_pending_telegram_system_messages,
    mark_telegram_system_message_deleted,
    get_user_language_profile,
    upsert_user_language_profile,
    get_daily_plan,
    create_daily_plan,
    update_daily_plan_item_status,
    update_daily_plan_item_payload,
    update_daily_plan_item_timer,
    get_best_video_recommendation_for_focus,
    upsert_video_recommendation,
    get_video_recommendation_by_id,
    vote_video_recommendation,
    consume_today_regenerate_limit,
    get_lowest_mastery_skill,
    get_top_error_topic_for_skill,
    get_skill_by_id,
    get_skill_progress_report,
    get_top_weak_topic,
    get_weak_topic_sentences,
    get_recent_mistake_examples_for_topic,
    list_default_topics_for_user,
    add_default_topic_for_user,
    upsert_billing_price_snapshot,
    get_effective_billing_price_snapshot,
    log_billing_event,
    upsert_billing_fixed_cost,
    get_user_billing_summary,
    get_today_reminder_settings,
    upsert_today_reminder_settings,
    list_today_reminder_users,
    get_audio_grammar_settings,
    upsert_audio_grammar_settings,
    update_translation_audio_grammar_opt_in,
    has_youtube_proxy_subtitles_access,
    upsert_youtube_proxy_subtitles_access,
    list_youtube_proxy_subtitles_access,
    get_weekly_goals,
    upsert_weekly_goals,
    get_weekly_plan_progress,
    get_plan_progress,
    start_agent_voice_session,
    finish_agent_voice_session,
    start_reader_session,
    finish_reader_session,
    upsert_reader_library_document,
    list_reader_library_documents,
    get_reader_library_document,
    update_reader_library_state,
    rename_reader_library_document,
    archive_reader_library_document,
    delete_reader_library_document,
    SUPPORTED_LEARNING_LANGUAGES,
    SUPPORTED_NATIVE_LANGUAGES,
)
from backend.srs import schedule_review, MATURE_INTERVAL_DAYS
from backend.translation_workflow import (
    build_user_daily_summary,
    check_user_translation_webapp,
    finish_translation_webapp,
    get_daily_translation_history,
    start_translation_session_webapp,
    start_story_session_webapp,
    submit_story_translation_webapp,
    get_story_history_webapp,
    get_active_session_type,
)
from backend.analytics import (
    fetch_user_summary,
    fetch_user_timeseries,
    fetch_comparison_leaderboard,
    get_period_bounds,
    get_all_time_bounds,
    _normalize_granularity,
    _normalize_period,
)

load_dotenv()
try:
    yta_version = importlib_metadata.version("youtube-transcript-api")
except Exception:
    yta_version = getattr(yta, "__version__", "unknown")
logging.info("✅ youtube_transcript_api version: %s", yta_version)
if os.getenv("YOUTUBE_TRANSCRIPT_PROXY") or os.getenv("YOUTUBE_TRANSCRIPT_PROXY_AU") or os.getenv("YOUTUBE_TRANSCRIPT_PROXY_DE"):
    logging.info("✅ YouTube transcript proxy configured")
else:
    logging.info("⚠️ YouTube transcript proxy not configured")
if os.getenv("YOUTUBE_COOKIES_BASE64") or os.getenv("YOUTUBE_COOKIES_PATH"):
    logging.info("✅ YouTube cookies configured for yt-dlp")
else:
    logging.info("⚠️ YouTube cookies not configured")

# Do NOT set global proxy env vars: it can affect Telegram API traffic.
# Proxy usage is scoped to YouTube transcript requests only.
os.environ.setdefault("NO_PROXY", "api.telegram.org,telegram.org")

# YouTube transcript cache/throttle (in-memory)
_yt_transcript_cache = {}
_yt_transcript_errors = {}
_YT_CACHE_TTL = 24 * 60 * 60  # 24 hours
_YT_ERROR_TTL = 10 * 60  # 10 minutes
_YT_MAX_RETRIES_PER_VIDEO = 5
_YT_MAX_PROXY_ATTEMPTS = 5
_YT_RETRY_SLEEP_MIN = 2.0
_YT_RETRY_SLEEP_MAX = 3.0
_YT_REQUEST_JITTER_MIN = 1.9
_YT_REQUEST_JITTER_MAX = 1.9
_YT_OEMBED_CACHE: dict[str, dict] = {}

_audio_scheduler = None
_audio_scheduler_lock = None
TELEGRAM_Deutsch_BOT_TOKEN = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
MOBILE_AUTH_SECRET = (os.getenv("MOBILE_AUTH_SECRET") or TELEGRAM_Deutsch_BOT_TOKEN or "").strip()
MOBILE_AUTH_TTL_SECONDS = int(os.getenv("MOBILE_AUTH_TTL_SECONDS", "2592000"))
TELEGRAM_LOGIN_TTL_SECONDS = int(os.getenv("TELEGRAM_LOGIN_TTL_SECONDS", "86400"))
NEW_PER_DAY = int(os.getenv("SRS_NEW_PER_DAY", "20"))
TELEGRAM_BOT_USERNAME = (os.getenv("TELEGRAM_BOT_USERNAME") or "").strip().lstrip("@")
TODAY_PLAN_DEFAULT_TZ = (os.getenv("TODAY_PLAN_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
YOUTUBE_API_KEY = (os.getenv("YOUTUBE_API_KEY") or "").strip()
THEORY_PACKAGE_TTL_MINUTES = max(1, int((os.getenv("THEORY_PACKAGE_TTL_MINUTES") or "720").strip()))
BILLING_CURRENCY_DEFAULT = (os.getenv("BILLING_CURRENCY") or "USD").strip().upper() or "USD"
BILLING_ALLOCATION_DEFAULT = (os.getenv("BILLING_ALLOCATION_METHOD_DEFAULT") or "weighted").strip().lower() or "weighted"

_TODAY_PREFERRED_CHANNELS = [
    "UCthmoIZKvuR1-KuwednkjHg",
    "UCHLkEhIoBRu2JTqYJlqlgbw",
    "UCeVQK7ZPXDOAyjY0NYqmX-Q",
    "UCuVbK_d3wh3M8TYUk5aFCiQ",
    "UCsxqCqZHE6guBCdSUEWpPsg",
    "UCm-E8MXdNquzETSsNxgoWig",
    "UCjdRXC3Wh2hDq8Utx7RIaMw",
    "UC9rwo-ES6aDKxD2qqkL6seA",
    "UCVx6RFaEAg46xfbsDjb440A",
    "UCvs8dBa7v3ti1QDaXE7dtKw",
    "UCE2vOZZIluHMtt2sAXhRhcw",
]

WEBAPP_TOPICS = [
    "🧩 ЗАГАДОЧНАЯ ИСТОРИЯ",
    "💼 Business",
    "🏥 Medicine",
    "🎨 Hobbies",
    "✈️ Travel",
    "🔬 Science",
    "💻 Technology",
    "🖼️ Art",
    "🎓 Education",
    "🍽️ Food",
    "⚽ Sports",
    "🌿 Nature",
    "🎵 Music",
    "📚 Literature",
    "🧠 Psychology",
    "🏛️ History",
    "📰 News",
]

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET")
TELEGRAM_GROUP_CHAT_ID = os.getenv("TELEGRAM_GROUP_CHAT_ID") or os.getenv("BOT_GROUP_CHAT_ID_Deutsch")

if not LIVEKIT_API_KEY or not LIVEKIT_API_SECRET:
    raise RuntimeError("LIVEKIT_API_KEY и LIVEKIT_API_SECRET должны быть установлены")

if not TELEGRAM_Deutsch_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_Deutsch_BOT_TOKEN должен быть установлен")

ensure_webapp_tables()

# === Путь к собранному фронту (frontend/dist) ===
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"

_ACCESS_PUBLIC_WEBAPP_PATHS = {"/api/webapp/topics"}
_ACCESS_PROTECTED_EXACT_PATHS = {"/api/message"}
_LEGACY_API_PREFIXES = (
    "/webapp/",
    "/web/",
    "/user/",
    "/telegram/",
    "/cards/",
    "/mobile/",
    "/admin/",
)
_LEGACY_API_EXACT_PATHS = {"/token", "/message"}


def _is_webapp_allowlist_bypass_enabled() -> bool:
    return str(os.getenv("WEBAPP_DISABLE_ALLOWLIST") or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_webapp_user_allowed(user_id: int) -> bool:
    if _is_webapp_allowlist_bypass_enabled():
        return True
    return is_telegram_user_allowed(int(user_id))


@app.before_request
def enforce_webapp_access():
    path = request.path or ""
    # Backward compatibility for legacy frontend/API paths without /api prefix.
    if path in _LEGACY_API_EXACT_PATHS or path.startswith(_LEGACY_API_PREFIXES):
        return redirect(f"/api{path}", code=307)

    is_protected = path.startswith("/api/webapp/") or path in _ACCESS_PROTECTED_EXACT_PATHS
    if not is_protected or path in _ACCESS_PUBLIC_WEBAPP_PATHS:
        return None

    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    if not _is_webapp_user_allowed(int(user_id)):
        return jsonify({"error": "Доступ к WebApp закрыт. Ожидайте одобрения администратора."}), 403

    g.telegram_user_id = int(user_id)
    g.telegram_user = user_data
    g.telegram_init_data = parsed
    return None


# === Раздача фронта ===
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    # если запросили конкретный файл (например assets/...), отдаём его
    file_path = FRONTEND_DIST / path
    if path != "" and file_path.exists():
        return send_from_directory(FRONTEND_DIST, path)

    # иначе отдаём index.html (SPA-логика)
    return send_from_directory(FRONTEND_DIST, "index.html")


# === API для токена (как ждёт фронт: /api/token) ===
@app.route("/api/token", methods=["GET"])
def get_token_api():
    user_id = request.args.get("user_id")
    username = request.args.get("username")

    if not username or not user_id:
        return jsonify({"error": "Нужны и user_id, и username"}), 400

    grant = VideoGrants(
        room_join=True,
        room="sales-assistant-room",
    )

    access_token = (
        AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        .with_identity(user_id)
        .with_name(username)
        .with_grants(grant)
    )

    return jsonify({"token": access_token.to_jwt()})


@app.route("/api/mobile/auth/exchange", methods=["POST"])
def exchange_mobile_access_token():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    user_id = int(user_id)
    if not is_telegram_user_allowed(user_id):
        return jsonify({"error": "Доступ закрыт. Ожидайте одобрения администратора."}), 403

    username = _extract_display_name(user_data)
    try:
        token = _issue_mobile_access_token(user_id=user_id, username=username)
    except Exception as exc:
        return jsonify({"error": f"Ошибка выпуска mobile token: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "access_token": token,
            "expires_in": MOBILE_AUTH_TTL_SECONDS,
            "user": {"id": user_id, "username": username},
        }
    )


@app.route("/api/web/auth/config", methods=["GET"])
def web_auth_config():
    return jsonify(
        {
            "ok": True,
            "telegram_bot_username": TELEGRAM_BOT_USERNAME,
            "telegram_login_enabled": bool(TELEGRAM_BOT_USERNAME),
        }
    )


@app.route("/api/web/auth/telegram", methods=["POST"])
def web_auth_telegram():
    payload = request.get_json(silent=True) or {}
    if not _telegram_login_hash_is_valid(payload):
        return jsonify({"error": "Telegram login hash invalid"}), 401

    auth_date_raw = str(payload.get("auth_date") or "").strip()
    try:
        auth_date = int(auth_date_raw)
    except Exception:
        return jsonify({"error": "auth_date invalid"}), 400
    if int(time.time()) - auth_date > max(60, TELEGRAM_LOGIN_TTL_SECONDS):
        return jsonify({"error": "Telegram login expired"}), 401

    user_id_raw = str(payload.get("id") or "").strip()
    if not user_id_raw.isdigit():
        return jsonify({"error": "user_id invalid"}), 400
    user_id = int(user_id_raw)

    if not is_telegram_user_allowed(user_id):
        return jsonify({"error": "Доступ к WebApp закрыт. Ожидайте одобрения администратора."}), 403

    user_data = {
        "id": user_id,
        "first_name": str(payload.get("first_name") or "").strip(),
        "last_name": str(payload.get("last_name") or "").strip(),
        "username": str(payload.get("username") or "").strip(),
        "photo_url": str(payload.get("photo_url") or "").strip(),
    }
    init_data = _build_signed_init_data_for_user(user_data, auth_date=int(time.time()))
    if not init_data:
        return jsonify({"error": "Не удалось выпустить initData"}), 500
    return jsonify(
        {
            "ok": True,
            "initData": init_data,
            "user": user_data,
            "chat_type": "browser",
        }
    )


@app.route("/api/user/language-profile", methods=["GET", "POST"])
def user_language_profile():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    if request.method == "GET":
        profile = get_user_language_profile(user_id=user_id)
        return jsonify(
            {
                "ok": True,
                "profile": profile,
                "supported": {
                    "learning_language": sorted(SUPPORTED_LEARNING_LANGUAGES),
                    "native_language": sorted(SUPPORTED_NATIVE_LANGUAGES),
                },
            }
        )

    payload = request.get_json(silent=True) or {}
    learning_language = str(payload.get("learning_language") or "").strip().lower()
    native_language = str(payload.get("native_language") or "").strip().lower()

    # Allow POST read-mode to avoid query-string issues with large initData in WebView.
    if not learning_language and not native_language:
        profile = get_user_language_profile(user_id=user_id)
        return jsonify(
            {
                "ok": True,
                "profile": profile,
                "supported": {
                    "learning_language": sorted(SUPPORTED_LEARNING_LANGUAGES),
                    "native_language": sorted(SUPPORTED_NATIVE_LANGUAGES),
                },
            }
        )

    if not learning_language or not native_language:
        return jsonify({"error": "learning_language и native_language обязательны"}), 400
    try:
        old_profile = get_user_language_profile(user_id=user_id)
        old_pair = (
            str(old_profile.get("native_language") or "ru").strip().lower(),
            str(old_profile.get("learning_language") or "de").strip().lower(),
        )
        profile = upsert_user_language_profile(
            user_id=user_id,
            learning_language=learning_language,
            native_language=native_language,
        )
        new_pair = (
            str(profile.get("native_language") or "ru").strip().lower(),
            str(profile.get("learning_language") or "de").strip().lower(),
        )
        reset_sessions = old_pair != new_pair
        if reset_sessions:
            # Switching language pair must start from a clean translation screen.
            # Unfinished sentences remain untranslated in DB (counted in analytics),
            # while active sessions are closed to avoid cross-pair leakage in UI.
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE bt_3_user_progress
                        SET end_time = NOW(), completed = TRUE
                        WHERE user_id = %s AND completed = FALSE;
                        """,
                        (int(user_id),),
                    )
    except ValueError as exc:
        return jsonify(
            {
                "error": str(exc),
                "supported": {
                    "learning_language": sorted(SUPPORTED_LEARNING_LANGUAGES),
                    "native_language": sorted(SUPPORTED_NATIVE_LANGUAGES),
                },
            }
        ), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения language profile: {exc}"}), 500
    return jsonify({"ok": True, "profile": profile, "reset_sessions": reset_sessions})


def _build_telegram_data_check_string(init_data: str) -> str:
    pairs = parse_qsl(init_data, keep_blank_values=True)
    data = {key: value for key, value in pairs if key != "hash"}
    sorted_pairs = [f"{key}={data[key]}" for key in sorted(data.keys())]
    return "\n".join(sorted_pairs)


def _telegram_hash_is_valid(init_data: str) -> bool:
    data_check_string = _build_telegram_data_check_string(init_data)
    secret_key = hmac.new(
        b"WebAppData",
        TELEGRAM_Deutsch_BOT_TOKEN.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    received_hash = dict(parse_qsl(init_data, keep_blank_values=True)).get("hash")
    return hmac.compare_digest(calculated_hash, received_hash or "")


def _parse_telegram_init_data(init_data: str) -> dict:
    data = dict(parse_qsl(init_data, keep_blank_values=True))
    user_payload = data.get("user")
    user_data = json.loads(user_payload) if user_payload else None
    return {
        "user": user_data,
        "auth_date": data.get("auth_date"),
        "query_id": data.get("query_id"),
        "chat_type": data.get("chat_type"),
        "chat_instance": data.get("chat_instance"),
    }


def _telegram_login_hash_is_valid(payload: dict) -> bool:
    received_hash = str(payload.get("hash") or "").strip()
    if not received_hash:
        return False
    data = {}
    for key, value in payload.items():
        if key == "hash":
            continue
        if value is None:
            continue
        data[str(key)] = str(value)
    data_check_string = "\n".join(f"{key}={data[key]}" for key in sorted(data.keys()))
    secret_key = hashlib.sha256(TELEGRAM_Deutsch_BOT_TOKEN.encode("utf-8")).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(calculated_hash, received_hash)


def _build_signed_init_data_for_user(user_data: dict, auth_date: int | None = None) -> str:
    if not user_data:
        return ""
    compact_user = {
        "id": int(user_data.get("id")),
        "first_name": str(user_data.get("first_name") or "").strip(),
        "last_name": str(user_data.get("last_name") or "").strip(),
        "username": str(user_data.get("username") or "").strip(),
    }
    photo_url = str(user_data.get("photo_url") or "").strip()
    if photo_url:
        compact_user["photo_url"] = photo_url
    payload = {
        "auth_date": str(int(auth_date or time.time())),
        "user": json.dumps(compact_user, ensure_ascii=False, separators=(",", ":")),
    }
    data_check_string = "\n".join(f"{key}={payload[key]}" for key in sorted(payload.keys()))
    secret_key = hmac.new(
        b"WebAppData",
        TELEGRAM_Deutsch_BOT_TOKEN.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    payload["hash"] = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(payload)


def _extract_webapp_user_from_init_data(init_data: str) -> tuple[int | None, str | None]:
    if not init_data or not _telegram_hash_is_valid(init_data):
        return None, None
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return None, None
    return int(user_id), _extract_display_name(user_data)


def _get_authenticated_user_from_request_init_data() -> tuple[int | None, str | None, str | None]:
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData") or request.args.get("initData") or request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        return None, None, "initData обязателен"
    user_id, username = _extract_webapp_user_from_init_data(init_data)
    if not user_id:
        return None, None, "initData не прошёл проверку или user_id отсутствует"
    if not _is_webapp_user_allowed(int(user_id)):
        return None, None, "Доступ к WebApp закрыт. Ожидайте одобрения администратора."
    return int(user_id), username, None


def _get_user_language_pair(user_id: int) -> tuple[str, str, dict]:
    profile = get_user_language_profile(user_id=int(user_id))
    native_language = str(profile.get("native_language") or "ru").strip().lower() or "ru"
    learning_language = str(profile.get("learning_language") or "de").strip().lower() or "de"
    return native_language, learning_language, profile


def _build_language_pair_payload(source_lang: str, target_lang: str) -> dict:
    return {
        "source_lang": source_lang,
        "target_lang": target_lang,
        "code": f"{source_lang}-{target_lang}",
    }


def _is_legacy_ru_de_pair(source_lang: str, target_lang: str) -> bool:
    return source_lang == "ru" and target_lang == "de"


def _get_local_today_date(tz_name: str = TODAY_PLAN_DEFAULT_TZ) -> date:
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return datetime.utcnow().date()


def _safe_plan_date(raw: str | None, tz_name: str = TODAY_PLAN_DEFAULT_TZ) -> date:
    if not raw:
        return _get_local_today_date(tz_name)
    try:
        return datetime.fromisoformat(str(raw)).date()
    except Exception:
        return _get_local_today_date(tz_name)


def _today_cards_item_payload(
    *,
    due_count: int,
    has_new_candidates: bool,
) -> tuple[dict | None, int]:
    if due_count > 0:
        limit = max(10, min(20, int(due_count)))
        minutes = 10 if limit <= 15 else 15
        return (
            {
                "task_type": "cards",
                "title": "Карточки: повторение",
                "estimated_minutes": minutes,
                "payload": {"mode": "fsrs_due", "limit": limit, "due_total": int(due_count)},
                "status": "todo",
            },
            minutes,
        )
    if has_new_candidates:
        return (
            {
                "task_type": "cards",
                "title": "Карточки: новые слова",
                "estimated_minutes": 10,
                "payload": {"mode": "cards_new", "limit": 10},
                "status": "todo",
            },
            10,
        )
    return None, 0


def _compute_srs_queue_info(
    *,
    user_id: int,
    now_utc: datetime,
    source_lang: str,
    target_lang: str,
) -> dict:
    due_count = count_due_srs_cards(
        user_id=user_id,
        now_utc=now_utc,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    introduced_today = count_new_cards_introduced_today(
        user_id=user_id,
        now_utc=now_utc,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    available_new_total = count_available_new_srs_cards(
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    new_remaining_today = max(NEW_PER_DAY - introduced_today, 0)
    new_remaining_today = min(new_remaining_today, max(0, int(available_new_total)))
    return {
        "due_count": int(due_count),
        "new_remaining_today": int(new_remaining_today),
        "available_new_total": int(available_new_total),
    }


def _build_next_srs_payload(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    now_utc: datetime,
) -> dict:
    due_payload = get_next_due_srs_card(
        user_id=user_id,
        now_utc=now_utc,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    card_payload = None
    srs_payload = None
    queue_info = _compute_srs_queue_info(
        user_id=user_id,
        now_utc=now_utc,
        source_lang=source_lang,
        target_lang=target_lang,
    )

    if due_payload:
        card_payload = due_payload.get("card")
        srs_payload = due_payload.get("srs")
    else:
        if int(queue_info.get("new_remaining_today") or 0) > 0:
            candidate = get_next_new_srs_candidate(
                user_id=user_id,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            if candidate:
                state = ensure_new_srs_state(user_id=user_id, card_id=int(candidate["id"]), now_utc=now_utc)
                card_payload = candidate
                srs_payload = {
                    "status": state.get("status") or "new",
                    "due_at": state.get("due_at"),
                    "interval_days": int(state.get("interval_days") or 0),
                    "stability": float(state.get("stability") or 0.0),
                    "difficulty": float(state.get("difficulty") or 0.0),
                }

    if not card_payload:
        return {
            "card": None,
            "srs": None,
            "queue_info": {
                "due_count": int(queue_info.get("due_count") or 0),
                "new_remaining_today": int(queue_info.get("new_remaining_today") or 0),
            },
        }

    due_at = srs_payload.get("due_at")
    due_iso = due_at.isoformat() if hasattr(due_at, "isoformat") else None
    interval_days = int(srs_payload.get("interval_days") or 0)
    srs_response = {
        "status": srs_payload.get("status") or "new",
        "due_at": due_iso,
        "interval_days": interval_days,
        "stability": float(srs_payload.get("stability") or 0.0),
        "difficulty": float(srs_payload.get("difficulty") or 0.0),
        "is_mature": interval_days >= MATURE_INTERVAL_DAYS,
    }
    card_response = _decorate_dictionary_item(
        card_payload if isinstance(card_payload, dict) else {},
        source_lang=source_lang,
        target_lang=target_lang,
        direction=f"{source_lang}-{target_lang}",
    )
    return {
        "card": card_response,
        "srs": srs_response,
        "queue_info": {
            "due_count": int(queue_info.get("due_count") or 0),
            "new_remaining_today": int(queue_info.get("new_remaining_today") or 0),
        },
    }


def _build_video_search_queries(
    main_category: str | None,
    sub_category: str | None,
    *,
    skill_title: str | None = None,
    examples: list[str] | None = None,
    target_lang: str = "de",
) -> list[str]:
    target = _normalize_short_lang_code(target_lang, fallback="de")
    sub = str(sub_category or "").strip()
    main = str(main_category or "").strip()
    skill = str(skill_title or "").strip()

    def _translate_term_for_video_search(raw: str) -> str:
        cleaned = " ".join(str(raw or "").strip().split())
        if not cleaned:
            return ""
        if target == "de" and re.search(r"[äöüßÄÖÜ]", cleaned):
            return cleaned
        if target == "ru" and re.search(r"[А-Яа-яЁё]", cleaned):
            return cleaned
        source_guess = _detect_reader_language(cleaned, fallback="en")
        source_guess = _normalize_short_lang_code(source_guess, fallback="en")
        if source_guess == target:
            return cleaned
        try:
            translated = asyncio.run(
                run_translate_subtitles_multilang(
                    lines=[cleaned],
                    source_lang=source_guess,
                    target_lang=target,
                )
            )
            candidate = str((translated or [""])[0] or "").strip()
            if candidate:
                return candidate
        except Exception:
            logging.debug("Video query translation failed for '%s' -> %s", cleaned, target, exc_info=True)
        return cleaned

    sub_local = _translate_term_for_video_search(sub)
    main_local = _translate_term_for_video_search(main)
    skill_local = _translate_term_for_video_search(skill)

    phrase_map = {
        "de": {
            "lang_word": "deutsch",
            "grammar_explained": "grammatik erklaert",
            "grammar_practice": "grammatik uebung",
            "learn": "deutsch lernen",
            "baseline": "deutsch grammatik b1 b2",
        },
        "en": {
            "lang_word": "english",
            "grammar_explained": "grammar explained",
            "grammar_practice": "grammar practice",
            "learn": "learn english",
            "baseline": "english grammar b1 b2",
        },
        "es": {
            "lang_word": "espanol",
            "grammar_explained": "gramatica explicada",
            "grammar_practice": "ejercicios de gramatica",
            "learn": "aprender espanol",
            "baseline": "gramatica espanol b1 b2",
        },
        "it": {
            "lang_word": "italiano",
            "grammar_explained": "grammatica spiegata",
            "grammar_practice": "esercizi di grammatica",
            "learn": "imparare italiano",
            "baseline": "grammatica italiano b1 b2",
        },
        "ru": {
            "lang_word": "русский",
            "grammar_explained": "грамматика объяснение",
            "grammar_practice": "грамматика упражнения",
            "learn": "учить русский",
            "baseline": "русская грамматика b1 b2",
        },
    }
    phrases = phrase_map.get(target, phrase_map["de"])

    sample_items = []
    if isinstance(examples, list):
        for item in examples[:3]:
            text = " ".join(str(item or "").strip().split())
            if text:
                sample_items.append(_translate_term_for_video_search(text[:80]))

    queries: list[str] = []
    if sub_local:
        queries.extend(
            [
                f"{sub_local} {phrases['lang_word']} {phrases['grammar_explained']}",
                f"{sub_local} {phrases['lang_word']} {phrases['grammar_practice']}",
                f"{sub_local} {phrases['learn']} b1 b2",
            ]
        )
    if main_local:
        queries.extend(
            [
                f"{main_local} {phrases['lang_word']} {phrases['grammar_explained']}",
                f"{main_local} {phrases['learn']}",
            ]
        )
    if skill_local:
        queries.extend(
            [
                f"{skill_local} {phrases['lang_word']} {phrases['grammar_explained']}",
                f"{skill_local} {phrases['learn']}",
            ]
        )
    for sample in sample_items:
        queries.append(f"{sample} {phrases['lang_word']} {phrases['grammar_explained']}")
    queries.append(phrases["baseline"])

    unique: list[str] = []
    seen: set[str] = set()
    for query in queries:
        norm = " ".join(query.strip().lower().split())
        if not norm or norm in seen:
            continue
        seen.add(norm)
        unique.append(query.strip())
    return unique


def _youtube_search_videos(
    query: str,
    *,
    max_results: int = 5,
    target_lang: str = "de",
    billing_user_id: int | None = None,
    billing_source_lang: str | None = None,
    billing_target_lang: str | None = None,
) -> list[dict]:
    if not YOUTUBE_API_KEY or not query:
        logging.info("YT search skipped: query='%s' key_present=%s", query, bool(YOUTUBE_API_KEY))
        return []
    collected: list[dict] = []
    preferred_hits = 0
    fallback_hits = 0
    try:
        base_url = "https://www.googleapis.com/youtube/v3/search"
        common_params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": max_results,
            "key": YOUTUBE_API_KEY,
        }
        for channel_id in _TODAY_PREFERRED_CHANNELS:
            params = {**common_params, "channelId": channel_id}
            resp = requests.get(base_url, params=params, timeout=12)
            _billing_log_youtube_quota_usage(
                user_id=billing_user_id,
                source_lang=billing_source_lang,
                target_lang=billing_target_lang or target_lang,
                action_type="youtube_api_search",
                endpoint="search",
                quota_units=100.0,
                metadata={"query": query[:120], "scope": "preferred_channel"},
            )
            if resp.status_code >= 400:
                continue
            data = resp.json() if resp.content else {}
            for item in data.get("items", []):
                video_id = ((item.get("id") or {}).get("videoId") or "").strip()
                title = ((item.get("snippet") or {}).get("title") or "").strip()
                if not video_id:
                    continue
                collected.append({"video_id": video_id, "title": title})
                preferred_hits += 1
        if not collected:
            lang_code = _normalize_short_lang_code(target_lang, fallback="de")
            region_map = {
                "de": "DE",
                "en": "US",
                "es": "ES",
                "it": "IT",
                "ru": "RU",
            }
            fallback_params = {
                **common_params,
                "relevanceLanguage": lang_code,
                "regionCode": region_map.get(lang_code, "DE"),
            }
            resp = requests.get(base_url, params=fallback_params, timeout=12)
            _billing_log_youtube_quota_usage(
                user_id=billing_user_id,
                source_lang=billing_source_lang,
                target_lang=billing_target_lang or target_lang,
                action_type="youtube_api_search",
                endpoint="search",
                quota_units=100.0,
                metadata={"query": query[:120], "scope": "fallback"},
            )
            if resp.status_code < 400:
                data = resp.json() if resp.content else {}
                for item in data.get("items", []):
                    video_id = ((item.get("id") or {}).get("videoId") or "").strip()
                    title = ((item.get("snippet") or {}).get("title") or "").strip()
                    if not video_id:
                        continue
                    collected.append({"video_id": video_id, "title": title})
                    fallback_hits += 1
    except Exception:
        logging.exception("YT search failed: query='%s'", query)
        return []

    unique = {}
    for row in collected:
        vid = row.get("video_id")
        if vid and vid not in unique:
            unique[vid] = {"video_id": vid, "title": row.get("title") or ""}
    logging.info(
        "YT search query='%s' preferred_hits=%s fallback_hits=%s unique=%s",
        query,
        preferred_hits,
        fallback_hits,
        len(unique),
    )
    return list(unique.values())


def _youtube_fill_view_counts(
    videos: list[dict],
    *,
    billing_user_id: int | None = None,
    billing_source_lang: str | None = None,
    billing_target_lang: str | None = None,
) -> list[dict]:
    if not videos or not YOUTUBE_API_KEY:
        return videos
    try:
        ids = ",".join([str(v.get("video_id") or "").strip() for v in videos if v.get("video_id")])
        if not ids:
            return videos
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "part": "statistics,snippet",
                "id": ids,
                "key": YOUTUBE_API_KEY,
            },
            timeout=12,
        )
        _billing_log_youtube_quota_usage(
            user_id=billing_user_id,
            source_lang=billing_source_lang,
            target_lang=billing_target_lang,
            action_type="youtube_api_videos_lookup",
            endpoint="videos",
            quota_units=1.0,
            metadata={"videos_count": len(videos)},
        )
        if resp.status_code >= 400:
            return videos
        stats = resp.json() if resp.content else {}
        by_id = {}
        for item in stats.get("items", []):
            vid = (item.get("id") or "").strip()
            if not vid:
                continue
            by_id[vid] = {
                "views": int(((item.get("statistics") or {}).get("viewCount") or 0)),
                "title": ((item.get("snippet") or {}).get("title") or "").strip(),
            }
        enriched = []
        for video in videos:
            vid = str(video.get("video_id") or "").strip()
            row = dict(video)
            row["views"] = int((by_id.get(vid) or {}).get("views") or 0)
            if not row.get("title"):
                row["title"] = (by_id.get(vid) or {}).get("title") or ""
            enriched.append(row)
        return enriched
    except Exception:
        return videos


def _pick_today_recommended_video(
    main_category: str | None,
    sub_category: str | None,
    *,
    skill_title: str | None = None,
    examples: list[str] | None = None,
    target_lang: str = "de",
    billing_user_id: int | None = None,
    billing_source_lang: str | None = None,
    billing_target_lang: str | None = None,
) -> dict | None:
    queries = _build_video_search_queries(
        main_category,
        sub_category,
        skill_title=skill_title,
        examples=examples,
        target_lang=target_lang,
    )
    logging.info("YT recommendation queries: %s", queries)
    for query in queries:
        videos = _youtube_search_videos(
            query,
            max_results=5,
            target_lang=target_lang,
            billing_user_id=billing_user_id,
            billing_source_lang=billing_source_lang,
            billing_target_lang=billing_target_lang,
        )
        if not videos:
            continue
        videos = _youtube_fill_view_counts(
            videos,
            billing_user_id=billing_user_id,
            billing_source_lang=billing_source_lang,
            billing_target_lang=billing_target_lang,
        )
        videos.sort(key=lambda x: int(x.get("views") or 0), reverse=True)
        best = videos[0] if videos else None
        if not best or not best.get("video_id"):
            continue
        video_id = str(best.get("video_id")).strip()
        return {
            "video_id": video_id,
            "video_url": f"https://www.youtube.com/watch?v={video_id}",
            "title": str(best.get("title") or "").strip(),
            "query": query,
            "views": int(best.get("views") or 0),
        }
    return None


def _format_theory_mistake_examples(examples: list[dict] | None) -> list[str]:
    rows: list[str] = []
    for item in examples or []:
        if not isinstance(item, dict):
            continue
        source_sentence = str(item.get("source_sentence") or "").strip()
        user_translation = str(item.get("user_translation") or "").strip()
        feedback = str(item.get("feedback") or "").strip()
        if source_sentence and user_translation:
            rows.append(f"RU: {source_sentence} | USER: {user_translation}")
        elif source_sentence:
            rows.append(f"RU: {source_sentence}")
        elif user_translation:
            rows.append(f"USER: {user_translation}")
        if feedback:
            rows.append(f"FEEDBACK: {feedback[:220]}")
        if len(rows) >= 8:
            break
    return rows[:8]


def _normalize_theory_sentences(payload: dict) -> list[str]:
    raw = payload.get("sentences")
    if isinstance(raw, list):
        cleaned = [str(item or "").strip() for item in raw if str(item or "").strip()]
        if len(cleaned) >= 5:
            return cleaned[:5]
    return []


def _default_theory_resources(target_lang: str) -> list[dict]:
    lang = _normalize_short_lang_code(target_lang, fallback="de")
    if lang == "de":
        return [
            {
                "title": "Duden Grammatik",
                "url": "https://www.duden.de/sprachwissen/grammatik",
                "type": "article",
                "why": "Справочник по грамматике немецкого языка.",
            },
            {
                "title": "Canoonet",
                "url": "https://www.canoonet.eu/",
                "type": "article",
                "why": "Подробные правила и формы немецкой грамматики.",
            },
            {
                "title": "Deutsch lernen (DW)",
                "url": "https://www.youtube.com/@dwlearngerman",
                "type": "video",
                "why": "Известный учебный канал с объяснениями по темам.",
            },
        ]
    if lang == "en":
        return [
            {
                "title": "Cambridge Grammar",
                "url": "https://dictionary.cambridge.org/grammar/",
                "type": "article",
                "why": "Авторитетные объяснения английской грамматики.",
            },
            {
                "title": "BBC Learning English",
                "url": "https://www.youtube.com/@bbclearningenglish",
                "type": "video",
                "why": "Популярный канал с практичной грамматикой.",
            },
        ]
    if lang == "es":
        return [
            {
                "title": "RAE - Diccionario Panhispánico de Dudas",
                "url": "https://www.rae.es/dpd/",
                "type": "article",
                "why": "Официальный источник по нормам испанского.",
            },
            {
                "title": "Butterfly Spanish",
                "url": "https://www.youtube.com/@ButterflySpanish",
                "type": "video",
                "why": "Понятные объяснения испанской грамматики.",
            },
        ]
    if lang == "it":
        return [
            {
                "title": "Treccani - Grammatica italiana",
                "url": "https://www.treccani.it/enciclopedia/grammatica-italiana/",
                "type": "article",
                "why": "Надежные материалы по итальянской грамматике.",
            },
            {
                "title": "Learn Italian with Lucrezia",
                "url": "https://www.youtube.com/@lucreziaoddone",
                "type": "video",
                "why": "Популярный канал с объяснениями по грамматике.",
            },
        ]
    return [
        {
            "title": "Wikipedia - Grammar",
            "url": "https://en.wikipedia.org/wiki/Grammar",
            "type": "article",
            "why": "Базовое введение в грамматику.",
        }
    ]


def _normalize_theory_resources(theory: dict, target_lang: str) -> list[dict]:
    raw = theory.get("resources") if isinstance(theory, dict) else None
    resources: list[dict] = []
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            title = str(entry.get("title") or "").strip()
            url = str(entry.get("url") or "").strip()
            kind = str(entry.get("type") or "").strip().lower()
            why = str(entry.get("why") or "").strip()
            if not title or not url:
                continue
            try:
                parsed = urlparse(url)
            except Exception:
                continue
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                continue
            if kind not in {"article", "video"}:
                kind = "article"
            resources.append(
                {
                    "title": title[:160],
                    "url": url,
                    "type": kind,
                    "why": why[:220],
                }
            )
            if len(resources) >= 3:
                break
    if resources:
        return resources
    return _default_theory_resources(target_lang)[:3]


def _billing_env_float(name: str) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return 0.0
    try:
        value = float(raw)
    except Exception:
        return 0.0
    return max(0.0, value)


def _billing_month_bounds(anchor_date: date | None = None) -> tuple[date, date]:
    base = anchor_date or datetime.utcnow().date()
    start = base.replace(day=1)
    end = date(base.year, base.month, monthrange(base.year, base.month)[1])
    return start, end


def _sync_billing_fixed_costs_from_env(anchor_date: date | None = None) -> dict:
    month_start, month_end = _billing_month_bounds(anchor_date)
    currency = (os.getenv("BILLING_CURRENCY") or BILLING_CURRENCY_DEFAULT or "USD").strip().upper() or "USD"
    allocation = (os.getenv("BILLING_ALLOCATION_METHOD_DEFAULT") or BILLING_ALLOCATION_DEFAULT or "weighted").strip().lower()
    if allocation not in {"equal", "weighted"}:
        allocation = "weighted"
    spec = [
        ("railway", "railway", "BILLING_FIXED_RAILWAY_USD_MONTH"),
        ("static_ips", "network", "BILLING_FIXED_STATIC_IPS_USD_MONTH"),
        ("proxy_subscription", "proxy", "BILLING_FIXED_PROXY_BASE_USD_MONTH"),
        ("subscriptions", "subscriptions", "BILLING_FIXED_SUBSCRIPTIONS_USD_MONTH"),
        ("other", "infra", "BILLING_FIXED_OTHER_USD_MONTH"),
    ]
    upserted: list[dict] = []
    for category, provider, env_name in spec:
        amount = _billing_env_float(env_name)
        if amount <= 0:
            continue
        item = upsert_billing_fixed_cost(
            category=category,
            provider=provider,
            amount=amount,
            currency=currency,
            period_start=month_start,
            period_end=month_end,
            allocation_method_default=allocation,
            metadata={"source": "env", "env_var": env_name},
        )
        if item:
            upserted.append(item)
    return {
        "currency": currency,
        "allocation_method_default": allocation,
        "period_start": month_start.isoformat(),
        "period_end": month_end.isoformat(),
        "upserted_count": len(upserted),
        "upserted": upserted,
    }


def _openai_model_key_to_model(model_key: str) -> str:
    key = str(model_key or "").strip().lower()
    if not key:
        return ""
    parts = [p for p in key.split("_") if p]
    if len(parts) >= 3 and parts[0] == "gpt" and parts[1].isdigit() and parts[2].isdigit():
        suffix = "-".join(parts[3:]) if len(parts) > 3 else ""
        base = f"gpt-{parts[1]}.{parts[2]}"
        return f"{base}-{suffix}" if suffix else base
    return "-".join(parts)


def _openai_model_to_key(model_name: str) -> str:
    value = str(model_name or "").strip().lower()
    if not value:
        return ""
    value = value.replace(".", "_").replace("-", "_")
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _upsert_price_snapshot_if_changed(
    *,
    provider: str,
    sku: str,
    unit: str,
    price_per_unit: float,
    currency: str,
    source: str,
    raw_payload: dict | None = None,
) -> tuple[str, dict]:
    now_utc = datetime.now(timezone.utc)
    existing = get_effective_billing_price_snapshot(
        provider=provider,
        sku=sku,
        unit=unit,
        currency=currency,
        as_of=now_utc,
    )
    if existing:
        existing_price = float(existing.get("price_per_unit") or 0.0)
        if abs(existing_price - price_per_unit) <= 1e-12:
            return (
                "unchanged",
                {
                    "sku": sku,
                    "unit": unit,
                    "price_per_unit": price_per_unit,
                    "snapshot_id": int(existing.get("id") or 0),
                },
            )
    item = upsert_billing_price_snapshot(
        provider=provider,
        sku=sku,
        unit=unit,
        price_per_unit=price_per_unit,
        currency=currency,
        valid_from=now_utc,
        source=source,
        raw_payload=raw_payload,
    )
    if not item:
        return ("error", {"sku": sku, "unit": unit, "price_per_unit": price_per_unit, "error": "upsert_failed"})
    return (
        "created",
        {
            "sku": sku,
            "unit": unit,
            "price_per_unit": price_per_unit,
            "snapshot_id": int(item.get("id") or 0),
        },
    )


def _sync_openai_price_snapshots_from_env() -> dict:
    currency = (os.getenv("BILLING_CURRENCY") or BILLING_CURRENCY_DEFAULT or "USD").strip().upper() or "USD"
    pattern = re.compile(r"^OPENAI_PRICE_(.+)_(INPUT|OUTPUT)_PER_1M$")
    created: list[dict] = []
    skipped: list[dict] = []
    errors: list[dict] = []
    considered = 0
    for env_name, raw_value in sorted(os.environ.items()):
        match = pattern.match(str(env_name or "").strip())
        if not match:
            continue
        considered += 1
        model_key = match.group(1)
        side = match.group(2).lower()
        value_raw = str(raw_value or "").strip()
        try:
            per_1m = float(value_raw)
        except Exception:
            errors.append({"env": env_name, "error": "not_a_number"})
            continue
        if not per_1m or per_1m <= 0:
            errors.append({"env": env_name, "error": "must_be_positive"})
            continue
        model_name = _openai_model_key_to_model(model_key)
        if not model_name:
            errors.append({"env": env_name, "error": "invalid_model_key"})
            continue
        sku = f"{model_name}_{'input' if side == 'input' else 'output'}"
        unit = "tokens_in" if side == "input" else "tokens_out"
        price_per_unit = per_1m / 1_000_000.0
        status, payload = _upsert_price_snapshot_if_changed(
            provider="openai",
            sku=sku,
            unit=unit,
            price_per_unit=price_per_unit,
            currency=currency,
            source="env",
            raw_payload={"env_var": env_name, "price_per_1m": per_1m},
        )
        if status == "created":
            created.append({"env": env_name, **payload})
        elif status == "unchanged":
            skipped.append({"env": env_name, **payload, "reason": "unchanged"})
        else:
            errors.append({"env": env_name, **payload})
    return {
        "currency": currency,
        "considered": considered,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "errors_count": len(errors),
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }


def _extract_openai_public_prices_from_text(text: str, model: str) -> dict[str, float]:
    if not text or not model:
        return {}
    normalized_text = str(text)
    model_lc = model.lower()
    idx = normalized_text.lower().find(model_lc)
    if idx < 0:
        model_alt = model_lc.replace("-", " ")
        idx = normalized_text.lower().find(model_alt)
    if idx < 0:
        return {}
    start = max(0, idx - 500)
    end = min(len(normalized_text), idx + 2500)
    chunk = normalized_text[start:end]
    lower_chunk = chunk.lower()
    result: dict[str, float] = {}

    in_match = re.search(r"(?:input|prompt)[^$0-9]{0,40}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:/|per)\s*1m", lower_chunk)
    out_match = re.search(r"(?:output|completion)[^$0-9]{0,40}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:/|per)\s*1m", lower_chunk)
    if in_match:
        try:
            result["input_per_1m"] = float(in_match.group(1))
        except Exception:
            pass
    if out_match:
        try:
            result["output_per_1m"] = float(out_match.group(1))
        except Exception:
            pass
    return result


def _sync_openai_price_snapshots_from_public() -> dict:
    currency = (os.getenv("BILLING_CURRENCY") or BILLING_CURRENCY_DEFAULT or "USD").strip().upper() or "USD"
    models_raw = str(os.getenv("OPENAI_PUBLIC_PRICING_MODELS") or "gpt-4.1-2025-04-14,gpt-4.1-mini").strip()
    models = [m.strip() for m in models_raw.split(",") if m.strip()]
    url = str(os.getenv("OPENAI_PUBLIC_PRICING_URL") or "https://openai.com/api/pricing").strip()
    timeout_sec = max(3, min(25, int(str(os.getenv("OPENAI_PUBLIC_PRICING_TIMEOUT_SEC") or "12").strip() or "12")))

    created: list[dict] = []
    skipped: list[dict] = []
    errors: list[dict] = []
    considered = 0
    fetched_text = ""
    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        fetched_text = response.text or ""
    except Exception as exc:
        return {
            "currency": currency,
            "source_url": url,
            "considered": 0,
            "created_count": 0,
            "skipped_count": 0,
            "errors_count": 1,
            "created": [],
            "skipped": [],
            "errors": [{"source_url": url, "error": f"fetch_failed: {exc}"}],
        }

    for model in models:
        pricing = _extract_openai_public_prices_from_text(fetched_text, model)
        for side in ("input", "output"):
            considered += 1
            key_name = f"{side}_per_1m"
            value = pricing.get(key_name)
            if value is None or value <= 0:
                errors.append({"model": model, "side": side, "error": "price_not_found"})
                continue
            sku = f"{model}_{side}"
            unit = "tokens_in" if side == "input" else "tokens_out"
            price_per_unit = float(value) / 1_000_000.0
            status, payload = _upsert_price_snapshot_if_changed(
                provider="openai",
                sku=sku,
                unit=unit,
                price_per_unit=price_per_unit,
                currency=currency,
                source="public_openai",
                raw_payload={"source_url": url, "price_per_1m": value},
            )
            if status == "created":
                created.append({"model": model, "side": side, **payload})
            elif status == "unchanged":
                skipped.append({"model": model, "side": side, **payload, "reason": "unchanged"})
            else:
                errors.append({"model": model, "side": side, **payload})
    return {
        "currency": currency,
        "source_url": url,
        "considered": considered,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "errors_count": len(errors),
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }


def _sync_aux_price_snapshots_from_env() -> dict:
    currency = (os.getenv("BILLING_CURRENCY") or BILLING_CURRENCY_DEFAULT or "USD").strip().upper() or "USD"
    specs = [
        ("WHISPER_PRICE_PER_MINUTE_USD", "openai", "whisper_input", "audio_minutes", "per_minute"),
        ("AGENT_TTS_PRICE_PER_MINUTE_USD", "agent_tts", "agent_tts_output", "audio_minutes", "per_minute"),
        ("LIVEKIT_PRICE_PER_MINUTE_USD", "livekit", "room_minute", "audio_minutes", "per_minute"),
        ("GOOGLE_TTS_PRICE_PER_1M_CHARS_USD", "google_tts", "google_tts_chars", "chars", "per_1m_chars"),
        ("YOUTUBE_API_PRICE_PER_1000_UNITS_USD", "youtube_api", "youtube_api_quota", "youtube_quota_units", "per_1000_units"),
    ]
    created: list[dict] = []
    skipped: list[dict] = []
    errors: list[dict] = []
    considered = 0
    for env_name, provider, sku, unit, mode in specs:
        raw = str(os.getenv(env_name) or "").strip()
        if not raw:
            continue
        considered += 1
        try:
            val = float(raw)
        except Exception:
            errors.append({"env": env_name, "error": "not_a_number"})
            continue
        if val <= 0:
            errors.append({"env": env_name, "error": "must_be_positive"})
            continue
        if mode == "per_1m_chars":
            price_per_unit = val / 1_000_000.0
        elif mode == "per_1000_units":
            price_per_unit = val / 1000.0
        else:
            price_per_unit = val
        status, payload = _upsert_price_snapshot_if_changed(
            provider=provider,
            sku=sku,
            unit=unit,
            price_per_unit=price_per_unit,
            currency=currency,
            source="env",
            raw_payload={"env_var": env_name, "raw_value": val, "mode": mode},
        )
        if status == "created":
            created.append({"env": env_name, **payload})
        elif status == "unchanged":
            skipped.append({"env": env_name, **payload, "reason": "unchanged"})
        else:
            errors.append({"env": env_name, **payload})
    return {
        "currency": currency,
        "considered": considered,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "errors_count": len(errors),
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }


def _sync_openai_price_snapshots_public_then_env() -> dict:
    public_enabled = str(os.getenv("BILLING_OPENAI_PUBLIC_PRICING_ENABLED") or "1").strip().lower() in {"1", "true", "yes", "on"}
    public_result = {"enabled": False}
    if public_enabled:
        public_result = _sync_openai_price_snapshots_from_public()
        public_result["enabled"] = True
    env_result = _sync_openai_price_snapshots_from_env()
    aux_env_result = _sync_aux_price_snapshots_from_env()
    return {
        "public": public_result,
        "env": env_result,
        "aux_env": aux_env_result,
        "summary": {
            "created_count": int((public_result.get("created_count") or 0) + (env_result.get("created_count") or 0) + (aux_env_result.get("created_count") or 0)),
            "skipped_count": int((public_result.get("skipped_count") or 0) + (env_result.get("skipped_count") or 0) + (aux_env_result.get("skipped_count") or 0)),
            "errors_count": int((public_result.get("errors_count") or 0) + (env_result.get("errors_count") or 0) + (aux_env_result.get("errors_count") or 0)),
        },
    }


def _billing_log_event_safe(
    *,
    user_id: int | None,
    action_type: str,
    provider: str,
    units_type: str,
    units_value: float,
    source_lang: str | None = None,
    target_lang: str | None = None,
    idempotency_seed: str | None = None,
    status: str = "estimated",
    metadata: dict | None = None,
) -> None:
    try:
        seed = str(idempotency_seed or "").strip() or f"{user_id}:{action_type}:{provider}:{units_type}:{units_value}:{time.time_ns()}"
        digest = hashlib.sha1(seed.encode("utf-8", "ignore")).hexdigest()[:28]
        key = f"ev_{action_type}_{digest}"
        log_billing_event(
            idempotency_key=key,
            user_id=int(user_id) if user_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
            action_type=action_type,
            provider=provider,
            units_type=units_type,
            units_value=float(units_value or 0.0),
            currency=BILLING_CURRENCY_DEFAULT,
            status=status,
            metadata=metadata or {},
        )
    except Exception as exc:
        logging.debug("billing event skipped: %s", exc)


def _billing_log_openai_usage(
    *,
    user_id: int | None,
    action_type: str,
    source_lang: str | None,
    target_lang: str | None,
    usage: dict | None,
    seed: str,
    metadata: dict | None = None,
) -> None:
    if not isinstance(usage, dict):
        return
    model = str(usage.get("model") or "unknown").strip() or "unknown"
    prompt_tokens = max(0, int(usage.get("prompt_tokens") or 0))
    completion_tokens = max(0, int(usage.get("completion_tokens") or 0))
    extra = metadata if isinstance(metadata, dict) else {}
    base_meta = {
        "model": model,
        "task_name": usage.get("task_name"),
        "total_tokens": max(0, int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))),
        **extra,
    }
    if prompt_tokens > 0:
        try:
            sku_in = f"{model}_input"
            snapshot_in = get_effective_billing_price_snapshot(
                provider="openai",
                sku=sku_in,
                unit="tokens_in",
                currency=BILLING_CURRENCY_DEFAULT,
            )
            meta_in = dict(base_meta)
            meta_in["price_sku"] = sku_in
            if snapshot_in:
                meta_in["pricing_state"] = "priced"
            else:
                meta_in["pricing_state"] = "missing_snapshot"
            log_billing_event(
                idempotency_key=f"tok_in_{hashlib.sha1((seed + ':in').encode('utf-8', 'ignore')).hexdigest()[:30]}",
                user_id=int(user_id) if user_id is not None else None,
                source_lang=source_lang,
                target_lang=target_lang,
                action_type=action_type,
                provider="openai",
                units_type="tokens_in",
                units_value=float(prompt_tokens),
                price_provider="openai" if snapshot_in else None,
                price_sku=sku_in if snapshot_in else None,
                price_unit="tokens_in" if snapshot_in else None,
                currency=BILLING_CURRENCY_DEFAULT,
                status="estimated",
                metadata=meta_in,
            )
        except Exception as exc:
            logging.debug("billing tokens_in skipped: %s", exc)
    if completion_tokens > 0:
        try:
            sku_out = f"{model}_output"
            snapshot_out = get_effective_billing_price_snapshot(
                provider="openai",
                sku=sku_out,
                unit="tokens_out",
                currency=BILLING_CURRENCY_DEFAULT,
            )
            meta_out = dict(base_meta)
            meta_out["price_sku"] = sku_out
            if snapshot_out:
                meta_out["pricing_state"] = "priced"
            else:
                meta_out["pricing_state"] = "missing_snapshot"
            log_billing_event(
                idempotency_key=f"tok_out_{hashlib.sha1((seed + ':out').encode('utf-8', 'ignore')).hexdigest()[:29]}",
                user_id=int(user_id) if user_id is not None else None,
                source_lang=source_lang,
                target_lang=target_lang,
                action_type=action_type,
                provider="openai",
                units_type="tokens_out",
                units_value=float(completion_tokens),
                price_provider="openai" if snapshot_out else None,
                price_sku=sku_out if snapshot_out else None,
                price_unit="tokens_out" if snapshot_out else None,
                currency=BILLING_CURRENCY_DEFAULT,
                status="estimated",
                metadata=meta_out,
            )
        except Exception as exc:
            logging.debug("billing tokens_out skipped: %s", exc)


def _billing_log_youtube_quota_usage(
    *,
    user_id: int | None,
    source_lang: str | None,
    target_lang: str | None,
    action_type: str,
    endpoint: str,
    quota_units: float,
    metadata: dict | None = None,
) -> None:
    units = max(0.0, float(quota_units or 0.0))
    if units <= 0:
        return
    meta = metadata if isinstance(metadata, dict) else {}
    snapshot = get_effective_billing_price_snapshot(
        provider="youtube_api",
        sku="youtube_api_quota",
        unit="youtube_quota_units",
        currency=BILLING_CURRENCY_DEFAULT,
    )
    seed = f"yt_quota:{user_id}:{action_type}:{endpoint}:{units}:{time.time_ns()}"
    try:
        log_billing_event(
            idempotency_key=f"ytq_{hashlib.sha1(seed.encode('utf-8', 'ignore')).hexdigest()[:32]}",
            user_id=int(user_id) if user_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
            action_type=action_type,
            provider="youtube_api",
            units_type="youtube_quota_units",
            units_value=units,
            price_provider="youtube_api" if snapshot else None,
            price_sku="youtube_api_quota" if snapshot else None,
            price_unit="youtube_quota_units" if snapshot else None,
            currency=BILLING_CURRENCY_DEFAULT,
            status="estimated",
            metadata={
                "endpoint": endpoint,
                "pricing_state": "priced" if snapshot else "missing_snapshot",
                **meta,
            },
        )
    except Exception as exc:
        logging.debug("billing youtube quota skipped: %s", exc)


def _compute_livekit_session_cost(session_minutes: float, *, currency: str) -> tuple[float, dict]:
    minutes = max(0.0, float(session_minutes or 0.0))
    free_minutes = max(0.0, _billing_env_float("LIVEKIT_FREE_MINUTES_MONTH"))
    price_per_minute = max(0.0, _billing_env_float("LIVEKIT_PRICE_PER_MINUTE_USD"))
    if minutes <= 0 or price_per_minute <= 0:
        return 0.0, {
            "free_minutes_month": free_minutes,
            "price_per_minute": price_per_minute,
            "consumed_before": 0.0,
            "charged_minutes": 0.0,
            "currency": currency,
        }
    month_start, month_end = _billing_month_bounds()
    consumed_before = 0.0
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(units_value), 0)
                    FROM bt_3_billing_events
                    WHERE provider = 'livekit'
                      AND action_type = 'livekit_room_minutes'
                      AND units_type = 'audio_minutes'
                      AND currency = %s
                      AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                    """,
                    (currency, month_start, month_end),
                )
                consumed_before = float((cursor.fetchone() or [0])[0] or 0.0)
    except Exception as exc:
        logging.debug("LiveKit consumed_before query failed: %s", exc)
    chargeable_before = max(0.0, consumed_before - free_minutes)
    chargeable_after = max(0.0, consumed_before + minutes - free_minutes)
    charged_minutes = max(0.0, chargeable_after - chargeable_before)
    return charged_minutes * price_per_minute, {
        "free_minutes_month": free_minutes,
        "price_per_minute": price_per_minute,
        "consumed_before": consumed_before,
        "charged_minutes": charged_minutes,
        "currency": currency,
    }


def _build_today_plan_for_user(
    *,
    user_id: int,
    plan_date: date,
    source_lang: str,
    target_lang: str,
) -> dict:
    now_utc = datetime.now(timezone.utc)
    due_count = 0
    has_new_candidates = False
    weakest_skill = None
    weak_topic = None
    weak_sentences: list[str] = []

    try:
        due_count = count_due_srs_cards(
            user_id=user_id,
            now_utc=now_utc,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception:
        due_count = 0

    if due_count <= 0:
        try:
            has_new_candidates = bool(
                get_next_new_srs_candidate(
                    user_id=user_id,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
        except Exception:
            has_new_candidates = False

    try:
        weakest_skill = get_lowest_mastery_skill(
            user_id=user_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception:
        weakest_skill = None

    if weakest_skill and weakest_skill.get("skill_id"):
        try:
            weak_topic = get_top_error_topic_for_skill(
                user_id=user_id,
                skill_id=str(weakest_skill.get("skill_id") or ""),
                lookback_days=7,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            weak_topic = None

    if not weak_topic:
        try:
            weak_topic = get_top_weak_topic(
                user_id=user_id,
                lookback_days=7,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            weak_topic = None

    if weak_topic:
        try:
            weak_sentences = get_weak_topic_sentences(
                user_id=user_id,
                main_category=str(weak_topic.get("main_category") or ""),
                sub_category=str(weak_topic.get("sub_category") or ""),
                lookback_days=7,
                limit=7,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            weak_sentences = []

    items: list[dict] = []
    total_minutes = 0

    cards_item, cards_minutes = _today_cards_item_payload(
        due_count=due_count,
        has_new_candidates=has_new_candidates,
    )
    if cards_item:
        items.append(cards_item)
        total_minutes += cards_minutes

    weak_title = "Перевод: 7 предложений по ошибкам"
    if weakest_skill and weakest_skill.get("skill_title"):
        weak_title = f"Перевод: 7 предложений ({weakest_skill.get('skill_title')})"
    elif weak_topic and weak_topic.get("sub_category"):
        weak_title = f"Перевод: 7 предложений ({weak_topic.get('sub_category')})"
    items.append(
        {
            "task_type": "translation",
            "title": weak_title,
            "estimated_minutes": 10,
            "payload": {
                "mode": "weakest_topic",
                "lookback_days": 7,
                "sentences": 7,
                "focus_source": "mastery" if weakest_skill else "mistakes",
                "skill_id": weakest_skill.get("skill_id") if weakest_skill else None,
                "skill_title": weakest_skill.get("skill_title") if weakest_skill else None,
                "skill_category": weakest_skill.get("skill_category") if weakest_skill else None,
                "skill_mastery": weakest_skill.get("mastery") if weakest_skill else None,
                "main_category": weak_topic.get("main_category") if weak_topic else None,
                "sub_category": weak_topic.get("sub_category") if weak_topic else None,
                "examples": weak_sentences[:7],
                "level": "c1",
                "start_action": "translations",
            },
            "status": "todo",
        }
    )
    total_minutes += 10

    theory_title = "Теория"
    if weakest_skill and weakest_skill.get("skill_title"):
        theory_title = f"Теория ({weakest_skill.get('skill_title')})"
    elif weak_topic and weak_topic.get("sub_category"):
        theory_title = f"Теория ({weak_topic.get('sub_category')})"
    items.append(
        {
            "task_type": "theory",
            "title": theory_title,
            "estimated_minutes": 12,
            "payload": {
                "mode": "theory_focus",
                "focus_source": "mastery" if weakest_skill else "mistakes",
                "skill_id": weakest_skill.get("skill_id") if weakest_skill else None,
                "skill_title": weakest_skill.get("skill_title") if weakest_skill else None,
                "main_category": weak_topic.get("main_category") if weak_topic else None,
                "sub_category": weak_topic.get("sub_category") if weak_topic else None,
                "examples": weak_sentences[:5],
                "start_action": "theory",
            },
            "status": "todo",
        }
    )
    total_minutes += 12

    include_video = str(os.getenv("TODAY_PLAN_INCLUDE_VIDEO") or "0").strip().lower() in {"1", "true", "yes", "on"}
    if include_video:
        recommended_video = _pick_today_recommended_video(
            weak_topic.get("main_category") if weak_topic else None,
            weak_topic.get("sub_category") if weak_topic else None,
            skill_title=weakest_skill.get("skill_title") if weakest_skill else None,
            examples=weak_sentences[:5],
            target_lang=target_lang,
            billing_user_id=int(user_id),
            billing_source_lang=source_lang,
            billing_target_lang=target_lang,
        )
        items.append(
            {
                "task_type": "video",
                "title": "Видео: 5 минут",
                "estimated_minutes": 5,
                "payload": {
                    "mode": "short_video",
                    "source": "youtube_library",
                    "duration_sec": 300,
                    "focus": "same_as_weak_topic",
                    "focus_source": "mastery" if weakest_skill else "mistakes",
                    "skill_id": weakest_skill.get("skill_id") if weakest_skill else None,
                    "skill_title": weakest_skill.get("skill_title") if weakest_skill else None,
                    "main_category": weak_topic.get("main_category") if weak_topic else None,
                    "sub_category": weak_topic.get("sub_category") if weak_topic else None,
                    "start_action": "youtube",
                    "video_id": (recommended_video or {}).get("video_id"),
                    "video_url": (recommended_video or {}).get("video_url"),
                    "video_title": (recommended_video or {}).get("title"),
                    "video_query": (recommended_video or {}).get("query"),
                },
                "status": "todo",
            }
        )
        total_minutes += 5

    for idx, item in enumerate(items):
        item["order_index"] = idx

    return create_daily_plan(
        user_id=user_id,
        plan_date=plan_date,
        total_minutes=total_minutes,
        items=items,
    )


def _get_or_create_today_plan(
    *,
    user_id: int,
    plan_date: date,
    source_lang: str,
    target_lang: str,
) -> dict:
    existing = get_daily_plan(user_id=user_id, plan_date=plan_date)
    if existing:
        items = existing.get("items") or []
        has_theory_item = any(str(item.get("task_type") or "").lower() == "theory" for item in items)
        has_non_todo = any(str(item.get("status") or "").lower() in {"doing", "done"} for item in items)
        if not has_theory_item and not has_non_todo:
            return _build_today_plan_for_user(
                user_id=user_id,
                plan_date=plan_date,
                source_lang=source_lang,
                target_lang=target_lang,
            )

        include_video = str(os.getenv("TODAY_PLAN_INCLUDE_VIDEO") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if include_video:
            has_video_item = any(str(item.get("task_type") or "").lower() == "video" for item in items)
            video_missing_link = any(
                str(item.get("task_type") or "").lower() == "video"
                and isinstance(item.get("payload"), dict)
                and not (item.get("payload") or {}).get("video_id")
                and not (item.get("payload") or {}).get("video_url")
                for item in items
            )
            if has_video_item and video_missing_link and not has_non_todo:
                return _build_today_plan_for_user(
                    user_id=user_id,
                    plan_date=plan_date,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
        return existing
    return _build_today_plan_for_user(
        user_id=user_id,
        plan_date=plan_date,
        source_lang=source_lang,
        target_lang=target_lang,
    )


def _format_today_plan_response(plan: dict | None) -> dict:
    if not plan:
        return {"date": None, "total_minutes": 0, "items": []}
    return {
        "date": plan.get("plan_date"),
        "total_minutes": int(plan.get("total_minutes") or 0),
        "items": [
            {
                "id": int(item.get("id")),
                "task_type": item.get("task_type"),
                "title": item.get("title"),
                "estimated_minutes": int(item.get("estimated_minutes") or 0),
                "status": item.get("status") or "todo",
                "payload": item.get("payload") if isinstance(item.get("payload"), dict) else {},
            }
            for item in (plan.get("items") or [])
        ],
    }


def _normalize_short_lang_code(value: str | None, fallback: str = "ru") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return fallback
    raw = raw.replace("_", "-")
    if "-" in raw:
        raw = raw.split("-", 1)[0]
    return raw or fallback


def _language_label(code: str) -> str:
    normalized = _normalize_short_lang_code(code, fallback="unknown")
    labels = {
        "de": "Deutsch",
        "en": "English",
        "es": "Espanol",
        "it": "Italiano",
        "ru": "Russkiy",
    }
    return labels.get(normalized, normalized.upper())


def _detect_reader_language(text: str, fallback: str = "de") -> str:
    sample = str(text or "")[:6000]
    if not sample.strip():
        return _normalize_short_lang_code(fallback, fallback="de")

    cyrillic_count = len(re.findall(r"[А-Яа-яЁё]", sample))
    latin_count = len(re.findall(r"[A-Za-zÀ-ÿ]", sample))
    if cyrillic_count > max(16, latin_count * 0.7):
        return "ru"

    tokens = re.findall(r"[A-Za-zÀ-ÿ']{2,}", sample.lower())
    if not tokens:
        return _normalize_short_lang_code(fallback, fallback="de")

    stopwords = {
        "de": {"der", "die", "das", "und", "ist", "nicht", "ich", "du", "wir", "sie", "mit", "auf", "zu", "von", "ein", "eine", "im", "den"},
        "en": {"the", "and", "is", "are", "i", "you", "we", "they", "to", "of", "in", "that", "it", "with", "for", "on", "this"},
        "es": {"el", "la", "los", "las", "y", "es", "de", "que", "en", "un", "una", "con", "por", "para", "como", "yo"},
        "it": {"il", "la", "gli", "le", "e", "che", "di", "in", "un", "una", "con", "per", "come", "io", "tu", "noi"},
        "ru": {"и", "в", "на", "что", "это", "как", "я", "ты", "мы", "вы", "он", "она", "они"},
    }
    scores: dict[str, int] = {lang: 0 for lang in stopwords}
    for token in tokens:
        for lang, vocab in stopwords.items():
            if token in vocab:
                scores[lang] += 1

    best_lang = max(scores, key=scores.get)
    best_score = scores.get(best_lang, 0)
    if best_score < 2:
        return _normalize_short_lang_code(fallback, fallback="de")
    return best_lang


def _normalize_reader_text(raw_text: str, max_chars: int = 150000) -> str:
    text = str(raw_text or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\u00a0]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()[:max_chars]


def _extract_text_from_html(html_content: str) -> str:
    content = str(html_content or "")
    content = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", content)
    content = re.sub(r"(?i)<br\s*/?>", "\n", content)
    content = re.sub(r"(?i)</(p|div|article|section|h1|h2|h3|h4|h5|h6|li|blockquote)>", "\n", content)
    content = re.sub(r"(?s)<[^>]+>", " ", content)
    content = html.unescape(content)
    return _normalize_reader_text(content)


def _extract_text_from_pdf_bytes(data: bytes) -> str:
    text, _pages = _extract_pdf_content_from_bytes(data)
    return text


def _extract_pdf_content_from_bytes(data: bytes) -> tuple[str, list[dict]]:
    if not data:
        return "", []
    if PdfReader is None:
        raise RuntimeError("PDF extraction is unavailable: install pypdf")
    reader = PdfReader(BytesIO(data))
    chunks: list[str] = []
    pages: list[dict] = []
    for idx, page in enumerate(reader.pages):
        if idx >= 250:
            break
        try:
            page_text = page.extract_text() or ""
        except Exception:
            page_text = ""
        normalized = _normalize_reader_text(page_text, max_chars=50000)
        if not normalized:
            continue
        chunks.append(normalized)
        pages.append({"page_number": idx + 1, "text": normalized})
    return _normalize_reader_text("\n\n".join(chunks)), pages


def _fetch_reader_text_from_url(raw_url: str) -> tuple[str, str, list[dict]]:
    parsed = urlparse(str(raw_url or "").strip())
    if not parsed.scheme:
        raw_url = f"https://{str(raw_url or '').strip()}"
        parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https links are supported")

    response = requests.get(
        raw_url,
        timeout=20,
        allow_redirects=True,
        headers={"User-Agent": "DeutschFlow-Reader/1.0"},
    )
    response.raise_for_status()
    content_type = str(response.headers.get("Content-Type") or "").lower()
    final_url = response.url or raw_url
    is_pdf = "application/pdf" in content_type or final_url.lower().split("?", 1)[0].endswith(".pdf")
    if is_pdf:
        text, pages = _extract_pdf_content_from_bytes(response.content)
        return text, "pdf", pages
    return _extract_text_from_html(response.text), "html", []


def _resolve_offline_espeak_voice(lang_hint: str | None, text: str) -> str:
    lang = _normalize_short_lang_code(lang_hint, fallback="")
    if not lang:
        lang = _detect_reader_language(text, fallback="de")
    # espeak/espeak-ng voice ids
    voice_map = {
        "de": "de",
        "ru": "ru",
        "en": "en",
        "es": "es",
        "it": "it",
        "fr": "fr",
        "pt": "pt",
    }
    return voice_map.get(lang, "de")


def _synthesize_offline_audio_wav(text: str, language: str | None = None) -> bytes:
    cleaned = _normalize_reader_text(text, max_chars=300000)
    if not cleaned:
        raise ValueError("Нет текста для аудио")
    if len(cleaned) > 180000:
        raise ValueError("Слишком длинный фрагмент для офлайн-конвертации. Выберите меньший диапазон страниц.")
    errors: list[str] = []
    voice = _resolve_offline_espeak_voice(language, cleaned)

    # Primary path: pyttsx3 (offline python engine).
    if pyttsx3 is not None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            wav_path = tmp.name
        try:
            engine = pyttsx3.init()
            engine.setProperty("rate", 165)
            try:
                voices = engine.getProperty("voices") or []
                normalized_voice = _normalize_short_lang_code(language, fallback="")
                if normalized_voice:
                    matched_voice = None
                    for voice_info in voices:
                        voice_id = str(getattr(voice_info, "id", "") or "").lower()
                        voice_name = str(getattr(voice_info, "name", "") or "").lower()
                        langs_raw = getattr(voice_info, "languages", None)
                        langs: list[str] = []
                        if isinstance(langs_raw, (list, tuple)):
                            for raw_item in langs_raw:
                                try:
                                    decoded = raw_item.decode("utf-8", errors="ignore") if isinstance(raw_item, (bytes, bytearray)) else str(raw_item)
                                except Exception:
                                    decoded = str(raw_item)
                                langs.append(decoded.lower())
                        if (
                            normalized_voice in voice_id
                            or normalized_voice in voice_name
                            or any(normalized_voice in item for item in langs)
                        ):
                            matched_voice = getattr(voice_info, "id", None)
                            break
                    if matched_voice:
                        engine.setProperty("voice", matched_voice)
            except Exception:
                # Best-effort voice selection; ignore if current runtime doesn't expose voices.
                pass
            engine.save_to_file(cleaned, wav_path)
            engine.runAndWait()
            with open(wav_path, "rb") as fh:
                payload = fh.read()
            if payload:
                return payload
            errors.append("pyttsx3 returned empty audio payload")
        except Exception as exc:
            errors.append(f"pyttsx3 failed: {exc}")
        finally:
            try:
                os.remove(wav_path)
            except Exception:
                pass
    else:
        errors.append("pyttsx3 module is not installed")

    # Fallback path: espeak-ng/espeak CLI (still fully offline/local).
    for cli_name in ("espeak-ng", "espeak"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as tmp_text:
            text_path = tmp_text.name
            tmp_text.write(cleaned)
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_wav:
            wav_path = tmp_wav.name
        try:
            proc = subprocess.run(
                [cli_name, "-v", voice, "-w", wav_path, "-s", "165", "-f", text_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                stderr = (proc.stderr or proc.stdout or "").strip()
                errors.append(f"{cli_name} exited with code {proc.returncode}: {stderr}")
                continue
            with open(wav_path, "rb") as fh:
                payload = fh.read()
            if payload:
                return payload
            errors.append(f"{cli_name} returned empty audio payload")
        except FileNotFoundError:
            errors.append(f"{cli_name} binary is not installed")
        except Exception as exc:
            errors.append(f"{cli_name} failed: {exc}")
        finally:
            try:
                os.remove(text_path)
            except Exception:
                pass
            try:
                os.remove(wav_path)
            except Exception:
                pass

    raise RuntimeError(
        "Offline speech engine is unavailable. "
        "Install pyttsx3 with espeak support (or espeak-ng/espeak binary). "
        f"Voice: {voice}. Details: {'; '.join(errors)}"
    )


def _infer_reader_title(
    *,
    input_text: str,
    input_url: str,
    source_type: str,
) -> str:
    if input_url:
        try:
            parsed = urlparse(input_url)
            path = str(parsed.path or "").strip("/")
            if path:
                leaf = path.split("/")[-1]
                leaf = re.sub(r"\.[A-Za-z0-9]{1,6}$", "", leaf)
                leaf = re.sub(r"[-_]+", " ", leaf).strip()
                if leaf:
                    return leaf[:120]
            host = str(parsed.netloc or "").strip()
            if host:
                return host[:120]
        except Exception:
            pass
    first_line = str(input_text or "").strip().splitlines()[0] if str(input_text or "").strip() else ""
    first_line = re.sub(r"\s+", " ", first_line).strip()
    if first_line:
        return first_line[:120]
    fallback = "PDF document" if source_type == "pdf" else "Text document"
    return fallback


def _youtube_translation_key(target_lang: str, idx: int | str) -> str:
    return f"{_normalize_short_lang_code(target_lang)}:{idx}"


def _extract_youtube_translations_for_target(
    translations_map: dict | None,
    target_lang: str,
) -> dict[str, str]:
    result: dict[str, str] = {}
    if not isinstance(translations_map, dict):
        return result

    normalized_target = _normalize_short_lang_code(target_lang)
    prefix = f"{normalized_target}:"
    for key, value in translations_map.items():
        key_str = str(key)
        if key_str.startswith(prefix):
            idx = key_str[len(prefix):]
            if idx:
                result[idx] = str(value or "")

    # Backward compatibility for old RU-only cache format: key is index.
    if normalized_target == "ru":
        for key, value in translations_map.items():
            key_str = str(key)
            if key_str.isdigit() and key_str not in result:
                result[key_str] = str(value or "")
    return result


def _decorate_dictionary_item(
    item: dict | None,
    source_lang: str,
    target_lang: str,
    direction: str,
) -> dict:
    data = dict(item or {})
    if data.get("source_text") is not None and data.get("target_text") is not None:
        source_text = data.get("source_text") or ""
        target_text = data.get("target_text") or ""
    else:
        reverse_direction_code = f"{target_lang}-{source_lang}"
        is_reverse = direction == reverse_direction_code or direction == "de-ru"
        if is_reverse:
            source_text = data.get("word_de") or data.get("translation_de") or ""
            target_text = data.get("translation_ru") or data.get("word_ru") or ""
        else:
            source_text = data.get("word_ru") or data.get("translation_ru") or ""
            target_text = data.get("translation_de") or data.get("word_de") or ""

    data.setdefault("source_text", source_text)
    data.setdefault("target_text", target_text)
    data.setdefault("source_lang", source_lang)
    data.setdefault("target_lang", target_lang)
    return data


def _resolve_entry_texts_for_pair(
    entry: dict | None,
    response_json: dict | None,
    source_lang: str,
    target_lang: str,
    source_text_hint: str | None = None,
    target_text_hint: str | None = None,
) -> tuple[str, str]:
    response_json = response_json if isinstance(response_json, dict) else {}
    entry = entry if isinstance(entry, dict) else {}

    source_text = str(source_text_hint or "").strip()
    target_text = str(target_text_hint or "").strip()

    if not source_text:
        source_text = str(
            response_json.get("source_text")
            or entry.get("word_ru")
            or response_json.get("word_ru")
            or entry.get("translation_ru")
            or response_json.get("translation_ru")
            or entry.get("word_de")
            or response_json.get("word_de")
            or ""
        ).strip()
    if not target_text:
        target_text = str(
            response_json.get("target_text")
            or entry.get("translation_de")
            or response_json.get("translation_de")
            or entry.get("word_de")
            or response_json.get("word_de")
            or entry.get("translation_ru")
            or response_json.get("translation_ru")
            or ""
        ).strip()

    # Final fallback based on pair direction if generic fields are missing.
    if not source_text and source_lang == "de":
        source_text = str(entry.get("word_de") or response_json.get("word_de") or "").strip()
    if not target_text and target_lang == "de":
        target_text = str(entry.get("translation_de") or response_json.get("translation_de") or "").strip()

    return source_text, target_text


def _build_multilang_dictionary_result(
    raw: dict,
    query_word: str,
    source_lang: str,
    target_lang: str,
) -> tuple[dict, str, str, str]:
    detected = str(raw.get("detected_language") or "source").strip().lower()
    direction = f"{source_lang}-{target_lang}" if detected != "target" else f"{target_lang}-{source_lang}"
    word_source = str(raw.get("word_source") or "").strip()
    word_target = str(raw.get("word_target") or "").strip()
    forms = raw.get("forms") if isinstance(raw.get("forms"), dict) else {}
    examples = raw.get("usage_examples") if isinstance(raw.get("usage_examples"), list) else []

    if detected == "target":
        source_value = word_source
        target_value = word_target or query_word
    else:
        source_value = word_source or query_word
        target_value = word_target

    if not target_value:
        target_value = query_word
    if not source_value:
        source_value = query_word

    result = {
        "word_ru": source_value,
        "translation_de": target_value,
        "word_de": target_value,
        "translation_ru": source_value,
        "source_text": source_value,
        "target_text": target_value,
        "part_of_speech": raw.get("part_of_speech"),
        "article": raw.get("article"),
        "forms": forms,
        "usage_examples": examples,
        "raw_text": raw.get("raw_text"),
    }
    return result, detected, source_value, target_value


def _force_translate_text(
    text: str,
    source_lang: str,
    target_lang: str,
) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    try:
        translated = asyncio.run(
            run_translate_subtitles_multilang(
                lines=[cleaned],
                source_lang=source_lang,
                target_lang=target_lang,
            )
        )
        if isinstance(translated, list) and translated:
            return str(translated[0] or "").strip()
    except Exception:
        return ""
    return ""


def _mobile_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _mobile_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _issue_mobile_access_token(user_id: int, username: str | None = None, ttl_seconds: int | None = None) -> str:
    ttl = int(ttl_seconds if ttl_seconds is not None else MOBILE_AUTH_TTL_SECONDS)
    payload = {
        "uid": int(user_id),
        "usr": (username or "").strip(),
        "exp": int(time.time()) + max(60, ttl),
    }
    payload_raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if not MOBILE_AUTH_SECRET:
        raise RuntimeError("MOBILE_AUTH_SECRET не задан")
    sig = hmac.new(MOBILE_AUTH_SECRET.encode("utf-8"), payload_raw, hashlib.sha256).hexdigest()
    return f"{_mobile_b64encode(payload_raw)}.{sig}"


def _verify_mobile_access_token(token: str) -> dict | None:
    token = (token or "").strip()
    if not token or "." not in token:
        return None
    if not MOBILE_AUTH_SECRET:
        return None

    payload_part, sig_part = token.rsplit(".", 1)
    try:
        payload_raw = _mobile_b64decode(payload_part)
    except Exception:
        return None

    expected = hmac.new(MOBILE_AUTH_SECRET.encode("utf-8"), payload_raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig_part):
        return None

    try:
        payload = json.loads(payload_raw.decode("utf-8"))
    except Exception:
        return None

    uid = payload.get("uid")
    exp = payload.get("exp")
    if not isinstance(uid, int):
        return None
    if not isinstance(exp, int) or exp <= int(time.time()):
        return None
    return payload


def _extract_mobile_token() -> str:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    payload = request.get_json(silent=True) or {}
    return (payload.get("access_token") or "").strip()


def _get_mobile_authenticated_user() -> tuple[int | None, str | None, object | None]:
    token = _extract_mobile_token()
    data = _verify_mobile_access_token(token)
    if not data:
        return None, None, (jsonify({"error": "Неверный или просроченный access token"}), 401)
    user_id = int(data.get("uid"))
    username = (data.get("usr") or "").strip() or None
    if not is_telegram_user_allowed(user_id):
        return None, None, (jsonify({"error": "Доступ закрыт. Ожидайте одобрения администратора."}), 403)
    return user_id, username, None


def _extract_display_name(user_data: dict | None) -> str | None:
    if not isinstance(user_data, dict):
        return None
    username = (user_data.get("username") or "").strip()
    if username:
        return username
    first_name = (user_data.get("first_name") or "").strip()
    last_name = (user_data.get("last_name") or "").strip()
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()
    return full_name or None


def _build_webapp_deeplink(path: str = "review") -> str:
    clean_path = (path or "review").strip().lstrip("/")
    if TELEGRAM_BOT_USERNAME:
        return f"https://t.me/{TELEGRAM_BOT_USERNAME}?startapp={clean_path}"
    # Fallback: if bot username is unknown, return generic root.
    return "https://t.me/"


def _normalize_sentence_text(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return ""
    if cleaned[0].isdigit():
        cleaned = cleaned.lstrip("0123456789").lstrip(".)- ").strip()
    return cleaned


def _dedupe_sentences(items: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for item in items:
        normalized = _normalize_sentence_text(item.get("sentence", ""))
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        result.append({**item, "sentence": normalized})
    return result


def _send_group_message(
    text: str,
    reply_markup: dict | None = None,
    disable_web_page_preview: bool = True,
) -> None:
    if not TELEGRAM_GROUP_CHAT_ID:
        raise RuntimeError("TELEGRAM_GROUP_CHAT_ID должен быть установлен")
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_GROUP_CHAT_ID,
        "text": text,
        "disable_web_page_preview": bool(disable_web_page_preview),
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    response = requests.post(
        url,
        json=payload,
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(TELEGRAM_GROUP_CHAT_ID),
                message_id=int(message_id),
                message_type="text",
            )
    except Exception:
        logging.debug("Failed to track group system message", exc_info=True)


def _send_private_message(
    user_id: int,
    text: str,
    reply_markup: dict | None = None,
    disable_web_page_preview: bool = True,
) -> None:
    payload = {
        "chat_id": int(user_id),
        "text": text,
        "disable_web_page_preview": bool(disable_web_page_preview),
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        json=payload,
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(user_id),
                message_id=int(message_id),
                message_type="text",
            )
    except Exception:
        logging.debug("Failed to track private system message", exc_info=True)


def _send_private_photo(user_id: int, image_bytes: bytes, filename: str, caption: str | None = None) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendPhoto"
    data = {"chat_id": int(user_id)}
    if caption:
        data["caption"] = caption
    files = {"photo": (filename, image_bytes, "image/png")}
    response = requests.post(url, data=data, files=files, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(user_id),
                message_id=int(message_id),
                message_type="photo",
            )
    except Exception:
        logging.debug("Failed to track private photo message", exc_info=True)


def _send_group_photo(image_bytes: bytes, filename: str, caption: str | None = None) -> None:
    if not TELEGRAM_GROUP_CHAT_ID:
        raise RuntimeError("TELEGRAM_GROUP_CHAT_ID должен быть установлен")
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendPhoto"
    data = {"chat_id": TELEGRAM_GROUP_CHAT_ID}
    if caption:
        data["caption"] = caption
    files = {"photo": (filename, image_bytes, "image/png")}
    response = requests.post(url, data=data, files=files, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(TELEGRAM_GROUP_CHAT_ID),
                message_id=int(message_id),
                message_type="photo",
            )
    except Exception:
        logging.debug("Failed to track group photo message", exc_info=True)


def _is_user_member_of_group_chat(user_id: int) -> bool:
    if not TELEGRAM_GROUP_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/getChatMember"
    try:
        response = requests.post(
            url,
            json={"chat_id": TELEGRAM_GROUP_CHAT_ID, "user_id": int(user_id)},
            timeout=15,
        )
        if response.status_code >= 400:
            return False
        payload = response.json() if response.content else {}
        status = str((payload.get("result") or {}).get("status") or "").strip().lower()
        return status in {"creator", "administrator", "member", "restricted"}
    except Exception:
        return False


def _send_private_message_chunks(user_id: int, text: str, limit: int = 3800) -> None:
    parts: list[str] = []
    buf = ""
    for line in text.splitlines():
        chunk = (buf + "\n" + line) if buf else line
        if len(chunk) > limit:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf = chunk
    if buf:
        parts.append(buf)
    for part in parts:
        _send_private_message(user_id, part)


def _build_private_analytics_chart_png(
    user_id: int,
    start_date: date,
    end_date: date,
    username: str,
) -> bytes | None:
    if plt is None:
        return None

    series = fetch_user_timeseries(
        user_id=user_id,
        start_date=start_date,
        end_date=end_date,
        granularity="day",
    )
    if not series:
        return None

    labels: list[str] = []
    avg_scores: list[float] = []
    success_rates: list[float] = []
    totals: list[int] = []
    for row in series:
        period_start = (row.get("period_start") or "")[:10]
        labels.append(period_start[5:] if len(period_start) >= 10 else period_start)
        avg_scores.append(float(row.get("avg_score") or 0.0))
        success_rates.append(float(row.get("success_rate") or 0.0))
        totals.append(int(row.get("total_translations") or 0))

    fig, ax = plt.subplots(figsize=(10, 5), dpi=140)
    x = list(range(len(labels)))
    ax.bar(x, totals, color="#dbeafe", alpha=0.9, label="Переводы (шт)")
    ax.plot(x, avg_scores, color="#2563eb", marker="o", linewidth=2, label="Средний балл")
    ax.plot(x, success_rates, color="#059669", marker="o", linewidth=2, label="Успешность %")

    ax.set_title(f"Аналитика за неделю: {username}")
    ax.set_xlabel("День")
    ax.set_ylabel("Значения")
    ax.set_ylim(0, 100)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.legend(loc="upper left")
    fig.tight_layout()

    buff = BytesIO()
    fig.savefig(buff, format="png")
    plt.close(fig)
    buff.seek(0)
    return buff.read()


def _send_group_audio(audio_bytes: bytes, filename: str, caption: str | None = None) -> None:
    if not TELEGRAM_GROUP_CHAT_ID:
        raise RuntimeError("TELEGRAM_GROUP_CHAT_ID должен быть установлен")
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendAudio"
    data = {"chat_id": TELEGRAM_GROUP_CHAT_ID}
    if caption:
        data["caption"] = caption
    files = {"audio": (filename, audio_bytes, "audio/mpeg")}
    response = requests.post(url, data=data, files=files, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(TELEGRAM_GROUP_CHAT_ID),
                message_id=int(message_id),
                message_type="audio",
            )
    except Exception:
        logging.debug("Failed to track group audio message", exc_info=True)


def _delete_telegram_message(chat_id: int, message_id: int) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/deleteMessage"
    response = requests.post(
        url,
        json={
            "chat_id": int(chat_id),
            "message_id": int(message_id),
        },
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}
    if not payload.get("ok", False):
        raise RuntimeError(f"Telegram delete failed: {payload}")


def _acquire_audio_scheduler_lock() -> bool:
    """
    Ensure only one worker starts the scheduler (gunicorn may spawn multiple).
    """
    global _audio_scheduler_lock
    try:
        import fcntl
        lock_path = "/tmp/audio_scheduler.lock"
        lock_file = open(lock_path, "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _audio_scheduler_lock = lock_file
        return True
    except Exception:
        return False


def _chunk_sentence_simple(sentence: str, max_len: int = 60) -> list[str]:
    if not sentence:
        return []
    parts = [part.strip() for part in re.split(r"[;,]", sentence) if part.strip()]
    chunks: list[str] = []
    for part in parts:
        if len(part) <= max_len:
            chunks.append(part)
            continue
        words = part.split()
        buf: list[str] = []
        size = 0
        for word in words:
            extra = len(word) + (1 if buf else 0)
            if size + extra > max_len and buf:
                chunks.append(" ".join(buf))
                buf = [word]
                size = len(word)
            else:
                buf.append(word)
                size += extra
        if buf:
            chunks.append(" ".join(buf))
    return chunks or [sentence.strip()]


def _build_practice_text(sentences: list[str]) -> str:
    parts: list[str] = []
    for sentence in sentences:
        cleaned = (sentence or "").strip()
        if not cleaned:
            continue
        chunks = _chunk_sentence_simple(cleaned)
        for chunk in chunks:
            parts.append(chunk)
        parts.append(cleaned)
    return ". ".join(part.strip().rstrip(".") for part in parts if part).strip()


_TTS_CACHE: dict[str, AudioSegment] = {}
_CHAIN_CACHE: dict[str, AudioSegment] = {}
_SILENCE_CACHE: dict[int, AudioSegment] = {}
_AUDIO_GRAMMAR_EXPL_CACHE: dict[str, str] = {}

_TTS_VOICES = {
    "de": "de-DE-Neural2-C",
    "ru": "ru-RU-Wavenet-B",
    "en": "en-US-Wavenet-D",
    "es": "es-ES-Standard-A",
    "it": "it-IT-Standard-A",
}
_TTS_LANG_CODES = {
    "de": "de-DE",
    "ru": "ru-RU",
    "en": "en-US",
    "es": "es-ES",
    "it": "it-IT",
}

_TTS_SPEED_DEFAULT = 0.9
_PAUSE_BETWEEN_REPEATS_MS = 500
_PAUSE_BETWEEN_STEPS_MS = 900
_PAUSE_BETWEEN_MISTAKES_MS = 1700
_CHAIN_INTER_CHUNK_MS = 240
_MAX_CHUNKS = 10


def safe_filename(username: str | None, user_id: int, date_str: str) -> str:
    base = (username or "").strip() or str(user_id)
    base = re.sub(r"[^a-zA-Z0-9_-]", "_", base)
    base = base[:50] if len(base) > 50 else base
    return f"{base}_mistakes_{date_str}.mp3"


def _normalize_utterance_text(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _audio_grammar_cache_key(target_lang: str, source_lang: str, sentence: str) -> str:
    raw = "|".join(
        [
            _normalize_short_lang_code(target_lang, fallback="de"),
            _normalize_short_lang_code(source_lang, fallback="ru"),
            _normalize_utterance_text(sentence),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _generate_audio_grammar_explanation(
    *,
    sentence: str,
    source_lang: str,
    target_lang: str,
) -> str:
    text = _normalize_utterance_text(sentence)
    if not text:
        return ""
    key = _audio_grammar_cache_key(target_lang, source_lang, text)
    cached = _AUDIO_GRAMMAR_EXPL_CACHE.get(key)
    if cached:
        return cached
    try:
        content = asyncio.run(
            run_audio_sentence_grammar_explain_multilang(
                sentence=text,
                language=_normalize_short_lang_code(target_lang, fallback="de"),
                explanation_language=_normalize_short_lang_code(source_lang, fallback="ru"),
            )
        )
    except Exception as exc:
        logging.warning("Audio grammar explanation generation failed: %s", exc)
        return ""
    cleaned = _normalize_utterance_text(content)
    if cleaned:
        _AUDIO_GRAMMAR_EXPL_CACHE[key] = cleaned
    return cleaned


def _tts_cache_key(lang: str, voice: str, speed: float, text: str) -> str:
    normalized = _normalize_utterance_text(text)
    raw = f"{lang}|{voice}|{speed}|{normalized}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_silence(ms: int) -> AudioSegment:
    if ms <= 0:
        return AudioSegment.silent(duration=0)
    cached = _SILENCE_CACHE.get(ms)
    if cached is not None:
        return cached
    segment = AudioSegment.silent(duration=ms)
    _SILENCE_CACHE[ms] = segment
    return segment


def get_or_create_tts_clip(lang: str, text: str, speed: float = _TTS_SPEED_DEFAULT) -> AudioSegment:
    voice = _TTS_VOICES.get(lang, _TTS_VOICES["de"])
    key = _tts_cache_key(lang, voice, speed, text)
    if key in _TTS_CACHE:
        return _TTS_CACHE[key]

    cached_db = get_tts_audio_cache(key)
    if cached_db:
        audio = AudioSegment.from_file(io.BytesIO(cached_db), format="mp3")
        _TTS_CACHE[key] = audio
        return audio

    cache_dir = "/tmp/tts_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{key}.mp3")
    if os.path.exists(cache_path):
        audio = AudioSegment.from_file(cache_path, format="mp3")
        _TTS_CACHE[key] = audio
        try:
            with open(cache_path, "rb") as cached_file:
                upsert_tts_audio_cache(
                    cache_key=key,
                    language=lang,
                    voice=voice,
                    speed=speed,
                    source_text=_normalize_utterance_text(text),
                    audio_mp3=cached_file.read(),
                )
        except Exception as exc:
            logging.warning("Failed to sync /tmp TTS cache to DB: %s", exc)
        return audio
    language = _TTS_LANG_CODES.get(lang, "en-US")
    audio_bytes = _synthesize_mp3(text, language=language, voice=voice, speed=speed)
    audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format="mp3")
    audio.export(cache_path, format="mp3", bitrate="192k")
    try:
        upsert_tts_audio_cache(
            cache_key=key,
            language=lang,
            voice=voice,
            speed=speed,
            source_text=_normalize_utterance_text(text),
            audio_mp3=audio_bytes,
        )
    except Exception as exc:
        logging.warning("Failed to persist TTS audio cache: %s", exc)
    _TTS_CACHE[key] = audio
    return audio


def _chain_cache_key(chunks: list[str], lang: str, speed: float) -> str:
    normalized = "|".join(_normalize_utterance_text(c) for c in chunks)
    raw = f"{lang}|{speed}|{normalized}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_or_create_chain_clip(chunks: list[str], lang: str, speed: float = _TTS_SPEED_DEFAULT) -> AudioSegment:
    key = _chain_cache_key(chunks, lang, speed)
    if key in _CHAIN_CACHE:
        return _CHAIN_CACHE[key]
    clip = AudioSegment.silent(duration=0)
    for idx, chunk in enumerate(chunks):
        clip += get_or_create_tts_clip(lang, chunk, speed)
        if idx < len(chunks) - 1:
            clip += _get_silence(_CHAIN_INTER_CHUNK_MS)
    _CHAIN_CACHE[key] = clip
    return clip


def _merge_smallest_chunks(chunks: list[str], max_chunks: int) -> list[str]:
    items = chunks[:]
    while len(items) > max_chunks:
        sizes = [len(x) for x in items]
        idx = sizes.index(min(sizes))
        if idx == 0:
            items[0:2] = [f"{items[0]} {items[1]}".strip()]
        elif idx == len(items) - 1:
            items[-2:] = [f"{items[-2]} {items[-1]}".strip()]
        else:
            left = len(items[idx - 1])
            right = len(items[idx + 1])
            if left <= right:
                items[idx - 1: idx + 1] = [f"{items[idx - 1]} {items[idx]}".strip()]
            else:
                items[idx: idx + 2] = [f"{items[idx]} {items[idx + 1]}".strip()]
    return items


def chunk_sentence_llm_de(de_sentence: str) -> list[str]:
    cleaned = (de_sentence or "").strip()
    if not cleaned:
        return []

    normalized = _normalize_utterance_text(cleaned)
    cache_key = hashlib.sha256(f"de|{normalized}".encode("utf-8")).hexdigest()
    cached_chunks = get_tts_chunk_cache(cache_key)
    if cached_chunks:
        return cached_chunks

    try:
        result = asyncio.run(run_tts_chunk_de(cleaned))
    except Exception as exc:
        logging.warning("Chunking failed: %s", exc)
        return _chunk_sentence_simple(cleaned)
    chunks = []
    try:
        for item in result.get("chunks", []):
            text = (item.get("text") or "").strip()
            if text:
                chunks.append(text)
    except Exception:
        chunks = []
    if not chunks:
        return _chunk_sentence_simple(cleaned)
    if len(chunks) > _MAX_CHUNKS:
        chunks = _merge_smallest_chunks(chunks, _MAX_CHUNKS)
    try:
        upsert_tts_chunk_cache(
            cache_key=cache_key,
            language="de",
            source_text=normalized,
            chunks=chunks,
        )
    except Exception as exc:
        logging.warning("Failed to persist chunk cache: %s", exc)
    return chunks


def _split_sentence_by_connectors(sentence: str, connectors: list[str]) -> list[str]:
    if not sentence:
        return []
    escaped = [re.escape(item.strip()) for item in connectors if item and item.strip()]
    if not escaped:
        return [sentence]
    pattern = r"\s+(?=(?:" + "|".join(escaped) + r")\b)"
    parts = [part.strip() for part in re.split(pattern, sentence, flags=re.IGNORECASE) if part and part.strip()]
    return parts or [sentence]


def _chunk_sentence_rules_for_language(sentence: str, language: str) -> list[str]:
    cleaned = _normalize_utterance_text(sentence)
    if not cleaned:
        return []

    lang = _normalize_short_lang_code(language, fallback="de")
    connectors_map = {
        "de": ["weil", "dass", "wenn", "obwohl", "damit", "während", "bevor", "nachdem"],
        "en": ["because", "that", "when", "if", "although", "while", "before", "after"],
        "es": ["porque", "que", "cuando", "si", "aunque", "mientras", "antes", "después"],
        "it": ["perché", "che", "quando", "se", "anche", "mentre", "prima", "dopo"],
        "ru": ["потому что", "что", "когда", "если", "хотя", "пока", "перед", "после"],
    }
    connectors = connectors_map.get(lang, connectors_map["de"])

    primary_parts = [part.strip() for part in re.split(r"(?<=[\.\!\?\:\;])\s+", cleaned) if part.strip()]
    if not primary_parts:
        primary_parts = [cleaned]
    chunks: list[str] = []
    for part in primary_parts:
        comma_parts = [p.strip() for p in re.split(r",\s*", part) if p.strip()]
        for comma_part in comma_parts:
            split_parts = _split_sentence_by_connectors(comma_part, connectors)
            for item in split_parts:
                item = item.strip()
                if item:
                    chunks.append(item)
    if not chunks:
        chunks = [cleaned]
    if len(chunks) > _MAX_CHUNKS:
        chunks = _merge_smallest_chunks(chunks, _MAX_CHUNKS)
    return chunks


def chunk_sentence_for_language(sentence: str, language: str) -> list[str]:
    lang = _normalize_short_lang_code(language, fallback="de")
    if lang == "de":
        chunks = chunk_sentence_llm_de(sentence)
        if chunks:
            return chunks
    return _chunk_sentence_rules_for_language(sentence, lang)


def build_target_script(chunks: list[str], target_lang: str = "de") -> list[dict]:
    if not chunks:
        return []
    script = []
    lang = _normalize_short_lang_code(target_lang, fallback="de")
    for i in range(len(chunks)):
        chunk = chunks[i]
        for _ in range(2):
            script.append(
                {
                    "kind": "utterance",
                    "lang": lang,
                    "text": chunk,
                    "speed": _TTS_SPEED_DEFAULT,
                    "pause_ms_after": _PAUSE_BETWEEN_REPEATS_MS,
                }
            )
        if i > 0:
            chain = chunks[: i + 1]
            for _ in range(2):
                script.append(
                    {
                        "kind": "chain",
                        "lang": lang,
                        "chunks": chain,
                        "speed": _TTS_SPEED_DEFAULT,
                        "pause_ms_after": _PAUSE_BETWEEN_REPEATS_MS,
                    }
                )
        script[-1]["pause_ms_after"] = _PAUSE_BETWEEN_STEPS_MS
    return script


def build_source_script(source_text: str, source_lang: str = "ru") -> list[dict]:
    cleaned = (source_text or "").strip()
    if not cleaned:
        return []
    return [
        {
            "kind": "utterance",
            "lang": _normalize_short_lang_code(source_lang, fallback="ru"),
            "text": cleaned,
            "speed": _TTS_SPEED_DEFAULT,
            "pause_ms_after": 600,
        }
    ]


def build_de_script(chunks: list[str]) -> list[dict]:
    return build_target_script(chunks, target_lang="de")


def build_ru_script(ru_text: str) -> list[dict]:
    return build_source_script(ru_text, source_lang="ru")


def build_full_script(
    mistakes: list[dict],
    source_lang: str = "ru",
    target_lang: str = "de",
) -> list[dict]:
    script = []
    src = _normalize_short_lang_code(source_lang, fallback="ru")
    tgt = _normalize_short_lang_code(target_lang, fallback="de")
    for item in mistakes:
        source_text = item.get("source_text") or item.get("ru_original") or ""
        target_text = item.get("target_text") or item.get("de_correct") or ""
        explanation_text = item.get("explanation_text") or ""
        script.extend(build_source_script(source_text, source_lang=src))
        chunks = chunk_sentence_for_language(target_text, tgt)
        if not chunks:
            chunks = [target_text.strip()] if target_text.strip() else []
        script.extend(build_target_script(chunks, target_lang=tgt))
        if explanation_text:
            script.extend(
                build_explanation_mixed_script(
                    explanation_text=explanation_text,
                    source_lang=src,
                    target_lang=tgt,
                )
            )
        if script:
            script[-1]["pause_ms_after"] = _PAUSE_BETWEEN_MISTAKES_MS
    return script


def _guess_fragment_lang_for_audio(
    fragment: str,
    source_lang: str,
    target_lang: str,
    *,
    prefer_target: bool = False,
) -> str:
    text = _normalize_utterance_text(fragment)
    src = _normalize_short_lang_code(source_lang, fallback="ru")
    tgt = _normalize_short_lang_code(target_lang, fallback="de")
    if not text:
        return src
    if src == tgt:
        return src

    cyr_count = len(re.findall(r"[А-Яа-яЁё]", text))
    latin_count = len(re.findall(r"[A-Za-zÀ-ÿ]", text))

    if cyr_count > 0 and latin_count == 0:
        if "ru" == src or "ru" == tgt:
            return "ru"
        return src
    if latin_count > 0 and cyr_count == 0:
        if "ru" == src and tgt != "ru":
            return tgt
        if "ru" == tgt and src != "ru":
            return src

    detected = _normalize_short_lang_code(_detect_reader_language(text, fallback=src), fallback=src)
    if detected == tgt:
        return tgt
    if detected == src:
        return src
    return tgt if prefer_target else src


def _split_explanation_fragments_with_lang(
    text: str,
    source_lang: str,
    target_lang: str,
) -> list[tuple[str, str]]:
    raw = str(text or "").strip()
    if not raw:
        return []
    src = _normalize_short_lang_code(source_lang, fallback="ru")
    tgt = _normalize_short_lang_code(target_lang, fallback="de")

    def _tag_to_lang(tag_name: str) -> str | None:
        tag = str(tag_name or "").strip().lower()
        if tag in {"target", "tgt"}:
            return tgt
        if tag in {"source", "src", "native", "explanation"}:
            return src
        if tag in {"de", "en", "ru", "es", "it"}:
            return _normalize_short_lang_code(tag, fallback=src)
        return None

    tagged_pattern = re.compile(
        r"\[(TARGET|TGT|SOURCE|SRC|NATIVE|EXPLANATION|DE|EN|RU|ES|IT)\](.*?)\[/\1\]",
        re.IGNORECASE | re.DOTALL,
    )
    has_explicit_tags = bool(tagged_pattern.search(raw))
    segments: list[tuple[str, str]] = []
    cursor = 0

    for match in tagged_pattern.finditer(raw):
        before = raw[cursor:match.start()].strip()
        if before:
            segments.extend(_split_explanation_fragments_with_lang(before, src, tgt))
        tag_lang = _tag_to_lang(match.group(1))
        tagged_text = _normalize_utterance_text(match.group(2))
        if tagged_text:
            segments.append((tagged_text, tag_lang or src))
        cursor = match.end()

    if has_explicit_tags:
        tail = raw[cursor:].strip()
        if tail:
            segments.extend(_split_explanation_fragments_with_lang(tail, src, tgt))
        return segments

    quote_pattern = r"(\"[^\"]+\"|“[^”]+”|«[^»]+»)"
    parts = re.split(quote_pattern, raw)
    for part in parts:
        piece = str(part or "").strip()
        if not piece:
            continue
        quoted = (
            (piece.startswith('"') and piece.endswith('"'))
            or (piece.startswith("“") and piece.endswith("”"))
            or (piece.startswith("«") and piece.endswith("»"))
        )
        if quoted:
            piece = piece[1:-1].strip()
        if not piece:
            continue
        lang = _guess_fragment_lang_for_audio(
            piece,
            source_lang=src,
            target_lang=tgt,
            prefer_target=quoted,
        )
        segments.append((piece, lang))
    return segments


def _split_mixed_fragment_by_script(
    text: str,
    source_lang: str,
    target_lang: str,
    default_lang: str,
) -> list[tuple[str, str]]:
    raw = str(text or "")
    if not raw.strip():
        return []

    src = _normalize_short_lang_code(source_lang, fallback="ru")
    tgt = _normalize_short_lang_code(target_lang, fallback="de")
    fallback = _normalize_short_lang_code(default_lang, fallback=src)
    has_cyr = bool(re.search(r"[А-Яа-яЁё]", raw))
    has_latin = bool(re.search(r"[A-Za-zÀ-ÿ]", raw))
    if not (has_cyr and has_latin):
        cleaned = _normalize_utterance_text(raw)
        return [(cleaned, fallback)] if cleaned else []

    non_ru_lang = tgt if src == "ru" else src if tgt == "ru" else tgt
    token_pattern = re.compile(
        r"[A-Za-zÀ-ÿ]+(?:[’'`\-][A-Za-zÀ-ÿ]+)*"
        r"|[А-Яа-яЁё]+(?:[’'`\-][А-Яа-яЁё]+)*"
        r"|\d+"
        r"|[^\w\s]+"
        r"|\s+",
        re.UNICODE,
    )
    tokens = token_pattern.findall(raw)
    if not tokens:
        cleaned = _normalize_utterance_text(raw)
        return [(cleaned, fallback)] if cleaned else []

    result: list[tuple[str, str]] = []
    buf: list[str] = []
    current_lang = fallback

    def _flush() -> None:
        nonlocal buf
        if not buf:
            return
        segment = _normalize_utterance_text("".join(buf))
        buf = []
        if segment:
            result.append((segment, current_lang))

    for token in tokens:
        token_lang = current_lang
        if re.search(r"[А-Яа-яЁё]", token):
            token_lang = "ru" if "ru" in {src, tgt} else src
        elif re.search(r"[A-Za-zÀ-ÿ]", token):
            token_lang = non_ru_lang
        elif token.strip():
            # Punctuation/numbers follow current segment language.
            token_lang = current_lang
        if token_lang != current_lang and token.strip():
            _flush()
            current_lang = token_lang
        buf.append(token)

    _flush()
    return result


def build_explanation_mixed_script(
    explanation_text: str,
    source_lang: str,
    target_lang: str,
) -> list[dict]:
    segments = _split_explanation_fragments_with_lang(
        text=explanation_text,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    if not segments:
        return []
    script: list[dict] = []
    mixed_segments: list[tuple[str, str]] = []
    for text, lang in segments[:40]:
        mixed_segments.extend(
            _split_mixed_fragment_by_script(
                text=text,
                source_lang=source_lang,
                target_lang=target_lang,
                default_lang=lang,
            )
        )
    for text, lang in mixed_segments[:80]:
        script.extend(build_source_script(text, source_lang=lang))
    if script:
        script[-1]["pause_ms_after"] = 800
    return script


def render_script_to_audio(script: list[dict]) -> bytes:
    combined = AudioSegment.silent(duration=0)
    for step in script:
        kind = step.get("kind")
        lang = step.get("lang", "de")
        speed = float(step.get("speed", _TTS_SPEED_DEFAULT))
        pause_ms = int(step.get("pause_ms_after", 0))
        if kind == "chain":
            chunks = step.get("chunks") or []
            if not chunks:
                continue
            clip = get_or_create_chain_clip(chunks, lang, speed)
        else:
            text = step.get("text") or ""
            if not text:
                continue
            clip = get_or_create_tts_clip(lang, text, speed)
        combined += clip
        if pause_ms:
            combined += _get_silence(pause_ms)
    buf = io.BytesIO()
    combined.export(buf, format="mp3", bitrate="192k")
    return buf.getvalue()


def _test_build_de_script() -> None:
    chunks = [
        "Gestern wurde der Geschäftsführung vorgeschlagen,",
        "eine wichtige Besprechung abzuhalten,",
        "um neue Strategien zur Entwicklung zu besprechen.",
    ]
    script = build_de_script(chunks)
    assert script[0]["text"] == chunks[0]
    assert script[1]["text"] == chunks[0]
    assert script[2]["chunks"] == chunks[:1]
    assert script[3]["chunks"] == chunks[:1]
    assert script[4]["text"] == chunks[1]
    assert script[6]["chunks"] == chunks[:2]
    assert script[-1]["chunks"] == chunks[:3]


def _synthesize_mp3(
    text: str,
    language: str = "de-DE",
    voice: str = "de-DE-Neural2-C",
    speed: float = 0.9,
) -> bytes:
    try:
        from google.cloud import texttospeech
    except Exception as exc:
        raise RuntimeError(f"Google TTS не установлен: {exc}") from exc
    key_path = prepare_google_creds_for_tts()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    tts_client = texttospeech.TextToSpeechClient()
    input_text = texttospeech.SynthesisInput(text=text)
    voice_params = texttospeech.VoiceSelectionParams(language_code=language, name=voice)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=speed,
    )
    response = tts_client.synthesize_speech(
        input=input_text,
        voice=voice_params,
        audio_config=audio_config,
    )
    return response.audio_content


_de_nlp = None


def _get_de_nlp():
    global _de_nlp
    if _de_nlp is None:
        if spacy is None:
            raise RuntimeError("spaCy не установлен")
        _de_nlp = spacy.load("de_core_news_sm")
    return _de_nlp


def _normalize_german_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    try:
        nlp = _get_de_nlp()
        doc = nlp(cleaned)
    except Exception as exc:
        logging.warning("German normalization skipped: %s", exc)
        return cleaned
    lemmas = []
    for token in doc:
        if token.is_space:
            continue
        if token.pos_ in ("PUNCT", "SYM"):
            continue
        lemma = token.lemma_ if token.lemma_ else token.text
        if token.pos_ in ("NOUN", "PROPN"):
            lemma = lemma.capitalize()
        else:
            lemma = lemma.lower()
        lemmas.append(lemma)
    return " ".join(lemmas).strip()


def _normalize_lookup_text(text: str, lang_code: str) -> str:
    cleaned = _normalize_sentence_text(text)
    if not cleaned:
        return ""
    lang = (lang_code or "").strip().lower()
    if lang == "de":
        return _normalize_german_text(cleaned)
    # For non-German pairs we keep normalization lightweight to avoid
    # language-specific distortions (lemmatization/stemming side-effects).
    return cleaned



ALLOWED_COUNTRIES = {"DE", "AT"}
DEFAULT_LANG_ORDER = ("de", "en", "ru")


def _get_yta_special_exceptions() -> tuple[type[BaseException], ...]:
    """
    Newer youtube-transcript-api versions define these exception types.
    Resolve dynamically to avoid hard import failures.
    """
    names = ("RequestBlocked", "AgeRestricted", "VideoUnplayable")
    excs: list[type[BaseException]] = []
    for name in names:
        exc = getattr(yta, name, None)
        if isinstance(exc, type) and issubclass(exc, BaseException):
            excs.append(exc)
    return tuple(excs)


def _check_ip_country(proxy_url: str, timeout: int = 10) -> str | None:
    """
    Проверяем страну IP через тот же proxy_url (важно: именно через прокси).
    Возвращает 'DE'/'AT'/... или None.
    """
    try:
        r = requests.get(
            "https://ipinfo.io/country",
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
        )
        return r.text.strip()
    except Exception:
        return None


def _fetch_with_yta(video_id: str, lang: str | None, proxy_config=None) -> list[dict]:
    """
    Use instance.fetch(...) with the new youtube-transcript-api API.
    """
    yta = YouTubeTranscriptApi(proxy_config=proxy_config)
    fetch_fn = getattr(yta, "fetch", None)
    if not callable(fetch_fn):
        raise RuntimeError("YouTubeTranscriptApi.fetch is not available")
    special_excs = _get_yta_special_exceptions()
    try:
        if lang:
            fetched = yta.fetch(video_id=video_id, languages=[lang])
        else:
            fetched = yta.fetch(video_id=video_id)
        return fetched.to_raw_data()
    except Exception as exc:
        if special_excs and isinstance(exc, special_excs):
            logging.warning("YouTubeTranscriptApi special error for %s: %s", video_id, exc)
        raise


def _parse_vtt_timestamp(ts: str) -> float:
    # "00:01:23.456" или "01:23.456"
    parts = ts.replace(",", ".").split(":")
    if len(parts) == 3:
        h, m, s = parts
        return float(h) * 3600 + float(m) * 60 + float(s)
    if len(parts) == 2:
        m, s = parts
        return float(m) * 60 + float(s)
    return float(parts[0])


def _parse_vtt_text(vtt: str) -> list[dict]:
    """
    Парсим WEBVTT в список {start, duration, text}.
    Это проще, чем тащить внешние парсеры.
    """
    lines = [ln.strip("\n") for ln in vtt.splitlines()]
    out: list[dict] = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.upper().startswith("WEBVTT"):
            i += 1
            continue

        # пропускаем индекс (иногда VTT имеет числовые блоки)
        if line.isdigit():
            i += 1
            continue

        # ожидаем таймкоды
        if "-->" in line:
            try:
                left, right = [p.strip() for p in line.split("-->")[:2]]
                start = _parse_vtt_timestamp(left.split()[0])
                end = _parse_vtt_timestamp(right.split()[0])
                duration = max(0.0, end - start)
            except Exception:
                start, duration = None, None

            i += 1
            text_lines = []
            while i < len(lines) and lines[i].strip():
                t = lines[i].strip()
                # убираем теги типа <c>, <v>, и прочую VTT-разметку
                t = re.sub(r"<[^>]+>", "", t).strip()
                if t:
                    text_lines.append(t)
                i += 1

            text = " ".join(text_lines).strip()
            if text and start is not None and duration is not None:
                out.append({"start": start, "duration": duration, "text": text})
            i += 1
            continue

        i += 1

    return out


def _fetch_with_ytdlp(video_id: str, proxy_url: str | None, lang: str | None) -> tuple[list[dict], str | None, bool | None]:
    """
    Fallback через yt-dlp:
    - пробуем и обычные сабы (--write-subs), и авто (--write-auto-subs)
    - пробуем lang_order
    - возвращаем items + language_code + is_generated
    """
    lang_order = list(DEFAULT_LANG_ORDER)
    if lang:
        lang_order = [lang] + [x for x in lang_order if x != lang]

    base_url = f"https://www.youtube.com/watch?v={video_id}"

    cookies_path = (os.getenv("YOUTUBE_COOKIES_PATH") or "").strip() or None
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    accept_lang = "en-US,en;q=0.9,ru;q=0.8,de;q=0.7"

    def _run_ytdlp_python(tmpdir_path: str, mode_flag: str, code: str) -> bool:
        try:
            from yt_dlp import YoutubeDL
        except Exception:
            return False

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "outtmpl": "%(id)s.%(ext)s",
            "user_agent": user_agent,
            "http_headers": {"Accept-Language": accept_lang},
        }
        if mode_flag == "--write-subs":
            ydl_opts["writesubtitles"] = True
        else:
            ydl_opts["writeautomaticsub"] = True
        ydl_opts["subtitleslangs"] = [code]
        ydl_opts["convertsubtitles"] = "vtt"
        if proxy_url:
            ydl_opts["proxy"] = proxy_url
        if cookies_path:
            ydl_opts["cookiefile"] = cookies_path

        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([base_url])
            return True
        except Exception:
            return False

    with tempfile.TemporaryDirectory() as tmpdir:
        cli_missing = False
        for is_generated, mode_flag in ((False, "--write-subs"), (True, "--write-auto-subs")):
            for code in lang_order:
                if not cli_missing:
                    cmd = [
                        "yt-dlp",
                        "--skip-download",
                        "--no-warnings",
                        "--quiet",
                        "--user-agent", user_agent,
                        "--add-headers", f"Accept-Language:{accept_lang}",
                        mode_flag,
                        "--sub-langs", code,
                        "--convert-subs", "vtt",
                        "-o", "%(id)s.%(ext)s",
                        base_url,
                    ]
                    if proxy_url:
                        cmd += ["--proxy", proxy_url]
                    if cookies_path:
                        cmd += ["--cookies", cookies_path]

                    try:
                        subprocess.run(cmd, cwd=tmpdir, check=True)
                    except FileNotFoundError:
                        cli_missing = True
                    except Exception:
                        pass

                if cli_missing:
                    ran = _run_ytdlp_python(tmpdir, mode_flag, code)
                    if not ran:
                        continue

                # ищем .vtt
                for fn in os.listdir(tmpdir):
                    if fn.endswith(".vtt"):
                        path = os.path.join(tmpdir, fn)
                        with open(path, "r", encoding="utf-8", errors="ignore") as f:
                            vtt = f.read()
                        items = _parse_vtt_text(vtt)
                        if items:
                            return items, code, is_generated

    raise RuntimeError("yt-dlp fallback failed to obtain VTT subtitles")


def _fetch_youtube_transcript(
    video_id: str,
    lang: str | None = None,
    *,
    allow_proxy: bool = True,
) -> dict:
    """
    Production pipeline:
    1) Webshare rotation + DE/AT filter (only if allow_proxy=True)
    2) Generic proxy + DE/AT filter (only if allow_proxy=True)
    3) Direct
    4) yt-dlp fallback (proxy only when allow_proxy=True)
    """

    errors: list[str] = []

    webshare_proxy = (
        (os.getenv("WEBSHARE_PROXY_URL") or "").strip()
        or (os.getenv("YOUTUBE_TRANSCRIPT_WEBSHARE_PROXY_URL") or "").strip()
        or None
    )
    generic_proxy = (
        (os.getenv("YOUTUBE_TRANSCRIPT_PROXY") or "").strip()
        or (os.getenv("YOUTUBE_TRANSCRIPT_PROXY_DE") or "").strip()
        or (os.getenv("YOUTUBE_TRANSCRIPT_PROXY_AU") or "").strip()
        or None
    )

    # ----------------------------
    # 1) Webshare rotation (DE/AT)
    # ----------------------------
    if allow_proxy and webshare_proxy:
        p = urlparse(webshare_proxy)
        ws_user = p.username or ""
        ws_pass = p.password or ""

        max_attempts = 5
        delay_seconds = 10

        for attempt in range(1, max_attempts + 1):
            country = _check_ip_country(webshare_proxy)
            if country not in ALLOWED_COUNTRIES:
                errors.append(f"webshare attempt {attempt}: rejected country {country}")
                time.sleep(delay_seconds)
                continue

            try:
                proxy_config = WebshareProxyConfig(
                    proxy_username=ws_user,
                    proxy_password=ws_pass,
                    filter_ip_locations=["de", "at"],
                )
                subs = _fetch_with_yta(video_id, lang, proxy_config=proxy_config)
                return {
                    "success": True,
                    "source": "webshare",
                    "ip_country": country,
                    "language": lang,
                    "is_generated": None,
                    "items": subs,
                }
            except Exception as e:
                errors.append(f"webshare attempt {attempt}: {e}")
                time.sleep(delay_seconds)

    # ----------------------------
    # 2) Generic proxy (DE/AT)
    # ----------------------------
    if allow_proxy and generic_proxy:
        country = _check_ip_country(generic_proxy)
        if country in ALLOWED_COUNTRIES:
            try:
                proxy_config = GenericProxyConfig(http_url=generic_proxy, https_url=generic_proxy)
                subs = _fetch_with_yta(video_id, lang, proxy_config=proxy_config)
                return {
                    "success": True,
                    "source": "generic",
                    "ip_country": country,
                    "language": lang,
                    "is_generated": None,
                    "items": subs,
                }
            except Exception as e:
                errors.append(f"generic: {e}")
        else:
            errors.append(f"generic rejected country {country}")

    # ----------------------------
    # 3) Direct
    # ----------------------------
    try:
        if lang:
            subs = _fetch_with_yta(video_id, lang, proxy_config=None)
            return {
                "success": True,
                "source": "direct",
                "ip_country": None,
                "language": lang,
                "is_generated": None,
                "items": subs,
            }
        for code in DEFAULT_LANG_ORDER:
            try:
                subs = _fetch_with_yta(video_id, code, proxy_config=None)
                return {
                    "success": True,
                    "source": "direct",
                    "ip_country": None,
                    "language": code,
                    "is_generated": None,
                    "items": subs,
                }
            except Exception:
                continue
        raise RuntimeError("direct: no transcripts for default languages")
    except Exception as e:
        errors.append(f"direct: {e}")

    # ----------------------------
    # 4) yt-dlp fallback
    # ----------------------------
    try:
        proxy_for_ytdlp = (webshare_proxy or generic_proxy) if allow_proxy else None
        items, detected_lang, is_generated = _fetch_with_ytdlp(video_id, proxy_for_ytdlp, lang)
        return {
            "success": True,
            "source": "yt-dlp",
            "ip_country": _check_ip_country(proxy_for_ytdlp) if proxy_for_ytdlp else None,
            "language": detected_lang,
            "is_generated": is_generated,
            "items": items,
        }
    except Exception as e:
        errors.append(f"yt-dlp: {e}")

    raise RuntimeError("; ".join(errors) if errors else "Не удалось получить субтитры")





# def _fetch_youtube_transcript(video_id: str, lang: str | None = None) -> dict:
#     def _sleep_jitter(min_s: float, max_s: float) -> None:
#         delay = random.uniform(min_s, max_s)
#         time.sleep(delay)

#     def _normalize_items(raw) -> list[dict]:
#         if isinstance(raw, list):
#             return raw
#         if isinstance(raw, dict):
#             snippets = raw.get("snippets")
#             if isinstance(snippets, list):
#                 return [
#                     {
#                         "start": item.get("start"),
#                         "duration": item.get("duration"),
#                         "text": item.get("text"),
#                     }
#                     for item in snippets
#                     if isinstance(item, dict)
#                 ]
#         return []

#     def _parse_proxy_url(proxy_url: str) -> dict:
#         parsed = urlparse(proxy_url)
#         return {
#             "username": parsed.username or "",
#             "password": parsed.password or "",
#         }

#     def _build_yta_kwargs(proxy_variant: dict) -> tuple[dict, list[str], dict | None, str | None, str]:
#         kwargs: dict = {}
#         cleanup_paths: list[str] = []
#         requests_proxies: dict | None = None
#         single_proxy: str | None = None
#         proxy = (proxy_variant.get("proxy_url") or "").strip() or None
#         variant_type = (proxy_variant.get("type") or "").strip()
#         mode = variant_type or "direct"

#         if variant_type == "webshare" and proxy:
#             parsed_auth = _parse_proxy_url(proxy)
#             filter_locations = proxy_variant.get("geo") or ["de", "at"]
#             kwargs["proxy_config"] = WebshareProxyConfig(
#                 proxy_username=parsed_auth["username"],
#                 proxy_password=parsed_auth["password"],
#                 filter_ip_locations=[c.strip().lower() for c in filter_locations if str(c).strip()] or None,
#             )
#             requests_proxies = {"http": proxy, "https": proxy}
#             kwargs["proxies"] = requests_proxies
#             single_proxy = proxy
#         elif variant_type == "webshare":
#             webshare_user = (os.getenv("WEBSHARE_PROXY_USERNAME") or "").strip()
#             webshare_pass = (os.getenv("WEBSHARE_PROXY_PASSWORD") or "").strip()
#             webshare_countries = (os.getenv("WEBSHARE_PROXY_COUNTRIES") or "").strip()
#             filter_locations = [c.strip().lower() for c in webshare_countries.split(",") if c.strip()] or ["de", "at"]
#             kwargs["proxy_config"] = WebshareProxyConfig(
#                 proxy_username=webshare_user,
#                 proxy_password=webshare_pass,
#                 filter_ip_locations=filter_locations,
#             )
#         elif variant_type == "generic" and proxy:
#             kwargs["proxy_config"] = GenericProxyConfig(http_url=proxy, https_url=proxy)
#             requests_proxies = {"http": proxy, "https": proxy}
#             kwargs["proxies"] = requests_proxies
#             single_proxy = proxy
#         elif proxy:
#             kwargs["proxy_config"] = GenericProxyConfig(http_url=proxy, https_url=proxy)
#             requests_proxies = {"http": proxy, "https": proxy}
#             kwargs["proxies"] = requests_proxies
#             single_proxy = proxy

#         if proxy:
#             single_proxy = proxy

#         cookies_path = os.getenv("YOUTUBE_COOKIES_PATH") or ""
#         cookies_b64 = os.getenv("YOUTUBE_COOKIES_BASE64") or ""
#         if cookies_b64 and not cookies_path:
#             try:
#                 tmp_cookie_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
#                 tmp_cookie_file.write(base64.b64decode(cookies_b64))
#                 tmp_cookie_file.flush()
#                 cookies_path = tmp_cookie_file.name
#                 cleanup_paths.append(cookies_path)
#             except Exception:
#                 pass
#         if cookies_path:
#             try:
#                 jar = http.cookiejar.MozillaCookieJar()
#                 jar.load(cookies_path, ignore_discard=True, ignore_expires=True)
#                 kwargs["cookies"] = jar
#             except Exception:
#                 logging.warning("Failed to load cookies for youtube_transcript_api")

#         return kwargs, cleanup_paths, requests_proxies, single_proxy, mode

#     def _build_proxy_variants() -> list[dict]:
#         variants = [
#             {"name": "DIRECT", "type": "direct", "proxy_url": None},
#         ]

#         webshare_proxy_url = (
#             (os.getenv("WEBSHARE_PROXY_URL") or "").strip()
#             or (os.getenv("YOUTUBE_TRANSCRIPT_WEBSHARE_PROXY_URL") or "").strip()
#         )
#         webshare_user = (os.getenv("WEBSHARE_PROXY_USERNAME") or "").strip()
#         webshare_pass = (os.getenv("WEBSHARE_PROXY_PASSWORD") or "").strip()
#         webshare_countries = [c.strip().lower() for c in (os.getenv("WEBSHARE_PROXY_COUNTRIES") or "").split(",") if c.strip()]
#         if webshare_proxy_url or (webshare_user and webshare_pass):
#             variants.append(
#                 {
#                     "name": "WEBSHARE",
#                     "type": "webshare",
#                     "proxy_url": webshare_proxy_url or None,
#                     "geo": webshare_countries or ["de", "at"],
#                 }
#             )

#         generic_keys = (
#             ("CHEAP_AU", "YOUTUBE_TRANSCRIPT_PROXY_AU"),
#             ("CHEAP_DE", "YOUTUBE_TRANSCRIPT_PROXY_DE"),
#             ("CHEAP_1", "YOUTUBE_TRANSCRIPT_PROXY"),
#         )
#         for name, key in generic_keys:
#             proxy_url = (os.getenv(key) or "").strip()
#             if proxy_url:
#                 variants.append({"name": name, "type": "generic", "proxy_url": proxy_url})
#         return variants

#     def _parse_vtt_text(text: str) -> list[dict]:
#         items = []
#         lines = [line.strip() for line in text.splitlines()]
#         buffer = []
#         start = None
#         for line in lines:
#             if "-->" in line:
#                 if buffer and start is not None:
#                     items.append({"start": start, "text": " ".join(buffer).strip()})
#                     buffer = []
#                 try:
#                     ts = line.split("-->")[0].strip()
#                     parts = ts.split(":")
#                     seconds = float(parts[-1].replace(",", "."))
#                     minutes = int(parts[-2]) if len(parts) >= 2 else 0
#                     hours = int(parts[-3]) if len(parts) >= 3 else 0
#                     start = hours * 3600 + minutes * 60 + seconds
#                 except Exception:
#                     start = None
#                 continue
#             if not line:
#                 if buffer and start is not None:
#                     items.append({"start": start, "text": " ".join(buffer).strip()})
#                 buffer = []
#                 start = None
#                 continue
#             if line.lower() == "webvtt":
#                 continue
#             if line.isdigit():
#                 continue
#             buffer.append(line)
#         if buffer and start is not None:
#             items.append({"start": start, "text": " ".join(buffer).strip()})
#         return items

#     api_error = None
#     instance_error = None
#     legacy_error = None
#     yta_kwargs = {}
#     cleanup_paths: list[str] = []
#     requests_proxies: dict | None = None
#     single_proxy: str | None = None
#     proxy_variants = _build_proxy_variants()[:_YT_MAX_PROXY_ATTEMPTS]
#     allowed_countries = {"DE", "AT"}
#     mode = "direct"
#     candidate_label = "DIRECT"

#     for variant_index, proxy_variant in enumerate(proxy_variants):
#         candidate_label = proxy_variant.get("name") or "UNKNOWN"
#         variant_type = proxy_variant.get("type") or "direct"
#         mode = variant_type
#         try:
#             yta_kwargs, cleanup_paths, requests_proxies, single_proxy, mode = _build_yta_kwargs(proxy_variant)
#             proxy_url = (proxy_variant.get("proxy_url") or "").strip() or None
#             logging.info(
#                 "YouTube transcript attempt %s/%s: mode=%s proxy=%s",
#                 variant_index + 1,
#                 len(proxy_variants),
#                 mode,
#                 candidate_label,
#             )
#             if variant_type == "webshare":
#                 max_attempts = 10
#                 delay_seconds = 30
#                 if proxy_url:
#                     country_ok = False
#                     for _ in range(max_attempts):
#                         country = requests.get(
#                             "https://ipinfo.io/country",
#                             proxies={"http": proxy_url, "https": proxy_url},
#                             timeout=10,
#                         ).text.strip()
#                         if country in allowed_countries:
#                             country_ok = True
#                             break
#                         time.sleep(delay_seconds)
#                     if not country_ok:
#                         raise RuntimeError("Webshare: no suitable DE/AT IP found")
#             elif variant_type == "generic" and proxy_url:
#                 country = requests.get(
#                     "https://ipinfo.io/country",
#                     proxies={"http": proxy_url, "https": proxy_url},
#                     timeout=10,
#                 ).text.strip()
#                 if country not in allowed_countries:
#                     raise RuntimeError(f"Rejected country {country}")

#             try:
#                 from youtube_transcript_api import (
#                     YouTubeTranscriptApi,
#                     TranscriptsDisabled,
#                     NoTranscriptFound,
#                     VideoUnavailable,
#                 )
#             except Exception as exc:
#                 raise RuntimeError("youtube_transcript_api не установлен") from exc
#             transcripts = None
#             if hasattr(YouTubeTranscriptApi, "list_transcripts"):
#                 try:
#                     transcripts = YouTubeTranscriptApi.list_transcripts(video_id, **yta_kwargs)
#                 except TypeError:
#                     transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
#             preferred = None
#             lang_order = ["de", "en", "ru"]
#             if lang:
#                 lang_order = [lang] + [code for code in lang_order if code != lang]
#             if transcripts is not None:
#                 for code in lang_order:
#                     try:
#                         preferred = transcripts.find_transcript([code])
#                         break
#                     except Exception:
#                         continue
#                 if preferred is None:
#                     preferred = transcripts.find_transcript([t.language_code for t in transcripts])
#                 transcript = preferred.fetch()
#                 logging.info(
#                     "YouTube transcript success: mode=%s proxy=%s source=list_api",
#                     mode,
#                     candidate_label,
#                 )
#                 return {
#                     "items": transcript,
#                     "language": preferred.language_code,
#                     "is_generated": preferred.is_generated,
#                     "source": "list_api",
#                 }
#         except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as exc:
#             api_error = f"Субтитры недоступны: {exc}"
#         except Exception as exc:
#             api_error = f"Не удалось загрузить субтитры: {exc}"

#         try:
#             from youtube_transcript_api import YouTubeTranscriptApi
#             _sleep_jitter(_YT_REQUEST_JITTER_MIN, _YT_REQUEST_JITTER_MAX)

#             # Static API (some versions expose get_transcript with proxy/cookies support)
#             get_transcript_fn = getattr(YouTubeTranscriptApi, "get_transcript", None)
#             if callable(get_transcript_fn):
#                 lang_order = ["de", "en", "ru"]
#                 if lang:
#                     lang_order = [lang] + [code for code in lang_order if code != lang]
#                 for code in lang_order:
#                     try:
#                         try:
#                             raw_items = get_transcript_fn(video_id, languages=[code], **yta_kwargs)
#                         except TypeError:
#                             raw_items = get_transcript_fn(video_id, languages=[code])
#                         items = _normalize_items(raw_items)
#                         if items:
#                             logging.info(
#                                 "YouTube transcript success: mode=%s proxy=%s source=static_api",
#                                 mode,
#                                 candidate_label,
#                             )
#                             return {
#                                 "items": items,
#                                 "language": code,
#                                 "is_generated": None,
#                                 "source": "static_api",
#                             }
#                     except Exception:
#                         continue

#             # Exact fallback matching user's local script (instance list + fetch)
#             try:
#                 _sleep_jitter(_YT_REQUEST_JITTER_MIN, _YT_REQUEST_JITTER_MAX)
#                 if "proxy_config" in yta_kwargs:
#                     yta_plain = YouTubeTranscriptApi(proxy_config=yta_kwargs["proxy_config"])
#                 else:
#                     yta_plain = YouTubeTranscriptApi()

#                 list_obj = None
#                 try:
#                     list_obj = yta_plain.list(video_id=video_id)
#                 except Exception:
#                     list_obj = None

#                 if list_obj is not None:
#                     lang_order = ["de", "en", "ru"]
#                     if lang and lang == "de":
#                         lang_order = [lang] + [code for code in lang_order if code != lang]
#                     for code in lang_order:
#                         try:
#                             preferred = list_obj.find_transcript([code])
#                             items = _normalize_items(preferred.fetch())
#                             if items:
#                                 logging.info(
#                                     "YouTube transcript success: mode=%s proxy=%s source=legacy_list",
#                                     mode,
#                                     candidate_label,
#                                 )
#                                 return {
#                                     "items": items,
#                                     "language": preferred.language_code,
#                                     "is_generated": preferred.is_generated,
#                                     "source": "legacy_list",
#                                 }
#                         except Exception:
#                             pass
#                     for code in lang_order:
#                         try:
#                             preferred = list_obj.find_generated_transcript([code])
#                             items = _normalize_items(preferred.fetch())
#                             if items:
#                                 logging.info(
#                                     "YouTube transcript success: mode=%s proxy=%s source=legacy_list_generated",
#                                     mode,
#                                     candidate_label,
#                                 )
#                                 return {
#                                     "items": items,
#                                     "language": preferred.language_code,
#                                     "is_generated": preferred.is_generated,
#                                     "source": "legacy_list_generated",
#                                 }
#                         except Exception:
#                             pass

#                 if lang:
#                     try:
#                         raw_items = yta_plain.fetch(video_id=video_id, languages=[lang])
#                         items = _normalize_items(raw_items)
#                         if items:
#                             logging.info(
#                                 "YouTube transcript success: mode=%s proxy=%s source=legacy_instance",
#                                 mode,
#                                 candidate_label,
#                             )
#                             return {
#                                 "items": items,
#                                 "language": lang,
#                                 "is_generated": None,
#                                 "source": "legacy_instance",
#                             }
#                     except Exception:
#                         pass
#                     if lang != "de":
#                         try:
#                             raw_items = yta_plain.fetch(video_id=video_id, languages=["de"])
#                             items = _normalize_items(raw_items)
#                             if items:
#                                 logging.info(
#                                     "YouTube transcript success: mode=%s proxy=%s source=legacy_instance",
#                                     mode,
#                                     candidate_label,
#                                 )
#                                 return {
#                                     "items": items,
#                                     "language": "de",
#                                     "is_generated": None,
#                                     "source": "legacy_instance",
#                                 }
#                         except Exception:
#                             pass
#                 raw_items = yta_plain.fetch(video_id=video_id)
#                 items = _normalize_items(raw_items)
#                 if items:
#                     logging.info(
#                         "YouTube transcript success: mode=%s proxy=%s source=legacy_instance",
#                         mode,
#                         candidate_label,
#                     )
#                     return {
#                         "items": items,
#                         "language": None,
#                         "is_generated": None,
#                         "source": "legacy_instance",
#                     }
#                 legacy_error = "Legacy instance API: empty response"
#             except Exception as exc:
#                 legacy_error = f"Legacy instance API: {exc}"

#             try:
#                 _sleep_jitter(_YT_REQUEST_JITTER_MIN, _YT_REQUEST_JITTER_MAX)
#                 yta = YouTubeTranscriptApi(**yta_kwargs)
#             except TypeError:
#                 yta = YouTubeTranscriptApi()
#             fetch_fn = getattr(yta, "fetch", None)
#             if not callable(fetch_fn):
#                 raise RuntimeError("YouTubeTranscriptApi.fetch не поддерживается этой версией")

#             lang_order = ["de", "en", "ru"]
#             if lang and lang == "de":
#                 lang_order = [lang] + [code for code in lang_order if code != lang]
#             for code in lang_order:
#                 try:
#                     try:
#                         raw_items = yta.fetch(video_id=video_id, languages=[code], **yta_kwargs)
#                     except TypeError:
#                         raw_items = yta.fetch(video_id=video_id, languages=[code])
#                     items = _normalize_items(raw_items)
#                     if items:
#                         logging.info(
#                             "YouTube transcript success: mode=%s proxy=%s source=instance_api",
#                             mode,
#                             candidate_label,
#                         )
#                         return {
#                             "items": items,
#                             "language": code,
#                             "is_generated": None,
#                             "source": "instance_api",
#                         }
#                 except Exception:
#                     continue
#             if lang and lang != "de":
#                 try:
#                     try:
#                         raw_items = yta.fetch(video_id=video_id, languages=["de"], **yta_kwargs)
#                     except TypeError:
#                         raw_items = yta.fetch(video_id=video_id, languages=["de"])
#                     items = _normalize_items(raw_items)
#                     if items:
#                         logging.info(
#                             "YouTube transcript success: mode=%s proxy=%s source=instance_api",
#                             mode,
#                             candidate_label,
#                         )
#                         return {
#                             "items": items,
#                             "language": "de",
#                             "is_generated": None,
#                             "source": "instance_api",
#                         }
#                 except Exception:
#                     pass

#             try:
#                 raw_items = yta.fetch(video_id=video_id, **yta_kwargs)
#             except TypeError:
#                 raw_items = yta.fetch(video_id=video_id)
#             items = _normalize_items(raw_items)
#             if not items:
#                 raise RuntimeError("Пустой ответ от YouTubeTranscriptApi.fetch")
#             logging.info(
#                 "YouTube transcript success: mode=%s proxy=%s source=instance_api",
#                 mode,
#                 candidate_label,
#             )
#             return {
#                 "items": items,
#                 "language": None,
#                 "is_generated": None,
#                 "source": "instance_api",
#             }
#         except Exception as exc:
#             instance_error = f"Instance API: {exc}"
#         finally:
#             for path in cleanup_paths:
#                 try:
#                     os.unlink(path)
#                 except Exception:
#                     pass
#         if variant_index < len(proxy_variants) - 1:
#             _sleep_jitter(_YT_RETRY_SLEEP_MIN, _YT_RETRY_SLEEP_MAX)

#     try:
#         try:
#             from yt_dlp import YoutubeDL
#         except Exception as exc:
#             raise RuntimeError("yt-dlp не установлен") from exc
#         _sleep_jitter(_YT_REQUEST_JITTER_MIN, _YT_REQUEST_JITTER_MAX)
#         ydl_opts = {
#             "skip_download": True,
#             "quiet": True,
#             "no_warnings": True,
#             "geo_bypass": True,
#             "noplaylist": True,
#             "format": "best",
#             "ignore_no_formats_error": True,
#             "allow_unplayable_formats": True,
#             "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
#             "http_headers": {
#                 "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
#                 "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,de;q=0.7",
#             },
#         }
#         if single_proxy:
#             ydl_opts["proxy"] = single_proxy
#         cookies_path = os.getenv("YOUTUBE_COOKIES_PATH") or ""
#         cookies_b64 = os.getenv("YOUTUBE_COOKIES_BASE64") or ""
#         tmp_cookie_file = None
#         try:
#             if cookies_b64 and not cookies_path:
#                 tmp_cookie_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
#                 tmp_cookie_file.write(base64.b64decode(cookies_b64))
#                 tmp_cookie_file.flush()
#                 cookies_path = tmp_cookie_file.name
#             if cookies_path:
#                 ydl_opts["cookiefile"] = cookies_path
#             with YoutubeDL(ydl_opts) as ydl:
#                 info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
#         finally:
#             if tmp_cookie_file is not None:
#                 try:
#                     tmp_cookie_file.close()
#                     os.unlink(tmp_cookie_file.name)
#                 except Exception:
#                     pass
#         language_order = ["de", "en", "ru"]
#         sources = [
#             ("subtitles", info.get("subtitles") or {}, False),
#             ("automatic_captions", info.get("automatic_captions") or {}, True),
#         ]
#         for source_name, source_map, is_generated in sources:
#             for lang in language_order:
#                 formats = source_map.get(lang) or []
#                 vtt_item = next((item for item in formats if item.get("ext") == "vtt"), None)
#                 if not vtt_item:
#                     continue
#                 url = vtt_item.get("url")
#                 if not url:
#                     continue
#                 _sleep_jitter(_YT_REQUEST_JITTER_MIN, _YT_REQUEST_JITTER_MAX)
#                 response = requests.get(url, timeout=20, proxies=requests_proxies)
#                 if response.status_code >= 400:
#                     continue
#                 items = _parse_vtt_text(response.text)
#                 if not items:
#                     continue
#                 logging.info(
#                     "YouTube transcript success: mode=%s proxy=%s source=yt_dlp",
#                     mode,
#                     candidate_label,
#                 )
#                 return {
#                     "items": items,
#                     "language": lang,
#                     "is_generated": is_generated,
#                     "source": "yt_dlp",
#                 }
#         raise RuntimeError("yt-dlp не нашёл .vtt в subtitles/automatic_captions")
#     except Exception as exc:
#         fallback_error = f"Fallback yt-dlp: {exc}"
#         if api_error or legacy_error or instance_error:
#             errors = "; ".join([e for e in (api_error, legacy_error, instance_error, fallback_error) if e])
#             raise RuntimeError(errors) from exc
#         raise RuntimeError(fallback_error) from exc


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if isinstance(error, HTTPException):
        # Preserve Flask/Werkzeug HTTP status codes (e.g. 404/405) without
        # converting them into noisy 500 errors.
        return error
    logging.exception("Unhandled exception: %s", error)
    if request.path.startswith("/api/"):
        return jsonify({"error": "Internal Server Error"}), 500
    raise error


@app.route("/api/telegram/validate", methods=["POST"])
def validate_telegram_init_data():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    return jsonify({"ok": True, **_parse_telegram_init_data(init_data)})


@app.route("/api/webapp/bootstrap", methods=["POST"])
def bootstrap_webapp_session():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    session_id = str(uuid4())
    parsed = _parse_telegram_init_data(init_data)
    return jsonify({"ok": True, "session_id": session_id, **parsed})


@app.route("/api/message", methods=["POST"])
def process_webapp_message():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    original_text = payload.get("original_text")
    user_translation = payload.get("user_translation")
    session_id = payload.get("session_id")
    translations = payload.get("translations") or []

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not translations and (not original_text or not user_translation):
        return jsonify({"error": "translations или original_text и user_translation обязательны"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    if translations:
        try:
            results = asyncio.run(
                check_user_translation_webapp(
                    user_id,
                    username,
                    translations,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка обработки запроса: {exc}"}), 500
        return jsonify(
            {
                "ok": True,
                "results": results,
                "language_pair": _build_language_pair_payload(source_lang, target_lang),
            }
        )

    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            result = asyncio.run(run_check_translation(original_text, user_translation))
        else:
            result = asyncio.run(
                run_check_translation_multilang(
                    original_text=original_text,
                    user_translation=user_translation,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
    except Exception as exc:
        return jsonify({"error": f"Ошибка обработки запроса: {exc}"}), 500

    save_webapp_translation(
        user_id=user_id,
        username=username,
        session_id=session_id,
        original_text=original_text,
        user_translation=user_translation,
        result=result,
        source_lang=source_lang,
        target_lang=target_lang,
    )

    return jsonify(
        {
            "ok": True,
            "result": result,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/history", methods=["POST"])
def get_webapp_history():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = payload.get("limit", 20)

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    history = get_webapp_translation_history(user_id=user_id, limit=int(limit))
    return jsonify({"ok": True, "items": history})


@app.route("/api/webapp/history/daily", methods=["POST"])
def get_webapp_daily_history():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = payload.get("limit", 50)

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    history = get_daily_translation_history(
        user_id=user_id,
        limit=int(limit),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    return jsonify(
        {
            "ok": True,
            "items": history,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


def _parse_iso_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


@app.route("/api/webapp/analytics/summary", methods=["POST"])
def get_webapp_analytics_summary():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    period = payload.get("period", "week")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        period = _normalize_period(period)
        if period == "all":
            bounds = get_all_time_bounds(user_id)
        else:
            bounds = get_period_bounds(period)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    summary = fetch_user_summary(
        user_id,
        bounds.start_date,
        bounds.end_date,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    return jsonify(
        {
            "ok": True,
            "period": {
                "period": period,
                "start_date": bounds.start_date.isoformat(),
                "end_date": bounds.end_date.isoformat(),
            },
            "summary": summary,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/analytics/timeseries", methods=["POST"])
def get_webapp_analytics_timeseries():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    period = payload.get("period", "week")
    granularity = payload.get("granularity")
    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    start_date = _parse_iso_date(start_date_raw)
    end_date = _parse_iso_date(end_date_raw)
    try:
        period = _normalize_period(period)
        if not start_date or not end_date:
            if period == "all":
                bounds = get_all_time_bounds(user_id)
            else:
                bounds = get_period_bounds(period)
            start_date = bounds.start_date
            end_date = bounds.end_date
        granularity = _normalize_granularity(granularity)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    points = fetch_user_timeseries(
        user_id,
        start_date,
        end_date,
        granularity,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    return jsonify(
        {
            "ok": True,
            "period": {
                "period": period,
                "granularity": granularity,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "points": points,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/analytics/compare", methods=["POST"])
def get_webapp_analytics_compare():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    period = payload.get("period", "week")
    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")
    limit = int(payload.get("limit", 10))

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    start_date = _parse_iso_date(start_date_raw)
    end_date = _parse_iso_date(end_date_raw)
    try:
        period = _normalize_period(period)
        if not start_date or not end_date:
            if period == "all":
                bounds = get_all_time_bounds(None)
            else:
                bounds = get_period_bounds(period)
            start_date = bounds.start_date
            end_date = bounds.end_date
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    leaderboard = fetch_comparison_leaderboard(
        start_date,
        end_date,
        limit=limit,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    user_rank = None
    for index, item in enumerate(leaderboard, start=1):
        if int(item.get("user_id")) == int(user_id):
            user_rank = index
            break

    return jsonify(
        {
            "ok": True,
            "period": {
                "period": period,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "items": leaderboard,
            "self": {"user_id": user_id, "rank": user_rank},
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )

@app.route("/api/economics/summary", methods=["GET"])
def get_economics_summary():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    period = str(request.args.get("period") or "month").strip().lower()
    if period == "half_year":
        period = "half-year"
    if period not in {"week", "month", "quarter", "half-year", "year", "all"}:
        return jsonify({"error": "period must be one of: week, month, quarter, half-year, year, all"}), 400
    allocation = str(request.args.get("allocation") or BILLING_ALLOCATION_DEFAULT or "weighted").strip().lower()
    if allocation not in {"equal", "weighted"}:
        return jsonify({"error": "allocation must be one of: equal, weighted"}), 400
    sync_fixed = str(request.args.get("sync_fixed") or "1").strip().lower() in {"1", "true", "yes", "on"}

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    sync_result = None
    if sync_fixed:
        try:
            sync_result = _sync_billing_fixed_costs_from_env()
        except Exception as exc:
            logging.warning("billing fixed cost env sync failed: %s", exc)
            sync_result = {"error": str(exc)}

    try:
        summary = get_user_billing_summary(
            user_id=int(user_id),
            period=period,
            allocation_method=allocation,
            source_lang=source_lang,
            target_lang=target_lang,
            currency=BILLING_CURRENCY_DEFAULT,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка расчёта экономики: {exc}"}), 500

    try:
        livekit_free_minutes_month = max(0.0, _billing_env_float("LIVEKIT_FREE_MINUTES_MONTH"))
        livekit_price_per_minute = max(0.0, _billing_env_float("LIVEKIT_PRICE_PER_MINUTE_USD"))
        month_start, month_end = _billing_month_bounds()
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(units_value), 0)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'livekit_room_minutes'
                      AND provider = 'livekit'
                      AND units_type = 'audio_minutes'
                      AND currency = %s
                      AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                    """,
                    (int(user_id), BILLING_CURRENCY_DEFAULT, month_start, month_end),
                )
                user_month_minutes = float((cursor.fetchone() or [0])[0] or 0.0)
        livekit_color = "green"
        livekit_ratio = None
        if livekit_free_minutes_month > 0:
            livekit_ratio = user_month_minutes / livekit_free_minutes_month
            if livekit_ratio >= 1.0:
                livekit_color = "red"
            elif livekit_ratio >= 0.8:
                livekit_color = "yellow"
            else:
                livekit_color = "green"
        else:
            # No free tier configured: any usage is effectively paid.
            livekit_color = "red" if user_month_minutes > 0 else "green"
        summary["livekit_status"] = {
            "color": livekit_color,
            "free_minutes_month": round(livekit_free_minutes_month, 3),
            "user_month_minutes": round(user_month_minutes, 3),
            "ratio_to_free_tier": round(livekit_ratio, 4) if livekit_ratio is not None else None,
            "price_per_minute": round(livekit_price_per_minute, 8),
            "month_start": month_start.isoformat(),
            "month_end": month_end.isoformat(),
        }
    except Exception as exc:
        logging.warning("LiveKit status compute failed: %s", exc)

    return jsonify(
        {
            "ok": True,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            "summary": summary,
            "fixed_cost_sync": sync_result,
        }
    )


@app.route("/api/admin/economics/fixed-costs/sync", methods=["POST"])
def sync_economics_fixed_costs():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401
    try:
        result = _sync_billing_fixed_costs_from_env()
    except Exception as exc:
        return jsonify({"error": f"Не удалось синхронизировать fixed costs: {exc}"}), 500
    return jsonify({"ok": True, "result": result})


@app.route("/api/admin/economics/price-snapshot", methods=["POST"])
def upsert_economics_price_snapshot():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    provider = str(payload.get("provider") or "").strip().lower()
    sku = str(payload.get("sku") or "").strip()
    unit = str(payload.get("unit") or "").strip().lower()
    if not provider or not sku or not unit:
        return jsonify({"error": "provider, sku и unit обязательны"}), 400
    try:
        price_per_unit = float(payload.get("price_per_unit"))
    except Exception:
        return jsonify({"error": "price_per_unit должен быть числом"}), 400
    valid_from_raw = str(payload.get("valid_from") or "").strip()
    valid_from = None
    if valid_from_raw:
        try:
            valid_from = datetime.fromisoformat(valid_from_raw.replace("Z", "+00:00"))
        except Exception:
            return jsonify({"error": "valid_from должен быть в ISO-формате"}), 400
    item = upsert_billing_price_snapshot(
        provider=provider,
        sku=sku,
        unit=unit,
        price_per_unit=price_per_unit,
        currency=str(payload.get("currency") or BILLING_CURRENCY_DEFAULT),
        valid_from=valid_from,
        source=str(payload.get("source") or "manual"),
        raw_payload=payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else None,
    )
    if not item:
        return jsonify({"error": "Не удалось сохранить price snapshot"}), 500
    return jsonify({"ok": True, "snapshot": item})


@app.route("/api/admin/economics/price-snapshots/active", methods=["GET"])
def list_active_economics_price_snapshots():
    token = request.args.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401
    provider_filter = str(request.args.get("provider") or "").strip().lower()
    currency = str(request.args.get("currency") or BILLING_CURRENCY_DEFAULT).strip().upper() or BILLING_CURRENCY_DEFAULT
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                provider_sql = " AND provider = %s" if provider_filter else ""
                params = [currency]
                if provider_filter:
                    params.append(provider_filter)
                cursor.execute(
                    f"""
                    SELECT DISTINCT ON (provider, sku, unit, currency)
                        id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at
                    FROM bt_3_billing_price_snapshots
                    WHERE currency = %s
                      {provider_sql}
                    ORDER BY provider, sku, unit, currency, valid_from DESC;
                    """,
                    params,
                )
                rows = cursor.fetchall() or []
    except Exception as exc:
        return jsonify({"error": f"Ошибка чтения активных price snapshots: {exc}"}), 500
    items = [
        {
            "id": int(row[0]),
            "provider": str(row[1] or ""),
            "sku": str(row[2] or ""),
            "unit": str(row[3] or ""),
            "price_per_unit": float(row[4] or 0.0),
            "currency": str(row[5] or currency),
            "price_per_1m": float(row[4] or 0.0) * 1_000_000 if str(row[3] or "").startswith("tokens_") else None,
            "valid_from": row[6].isoformat() if row[6] else None,
            "source": str(row[7] or "manual"),
            "raw_payload": row[8] if isinstance(row[8], dict) else None,
            "created_at": row[9].isoformat() if row[9] else None,
        }
        for row in rows
    ]
    return jsonify({"ok": True, "currency": currency, "count": len(items), "items": items})


@app.route("/api/admin/economics/event", methods=["POST"])
def log_economics_event():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    action_type = str(payload.get("action_type") or "").strip()
    provider = str(payload.get("provider") or "").strip().lower()
    units_type = str(payload.get("units_type") or "").strip().lower()
    if not idempotency_key or not action_type or not provider or not units_type:
        return jsonify({"error": "idempotency_key, action_type, provider, units_type обязательны"}), 400
    try:
        units_value = float(payload.get("units_value", 0))
    except Exception:
        return jsonify({"error": "units_value должен быть числом"}), 400
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    item = log_billing_event(
        idempotency_key=idempotency_key,
        user_id=int(payload["user_id"]) if payload.get("user_id") is not None else None,
        source_lang=str(payload.get("source_lang") or "").strip().lower() or None,
        target_lang=str(payload.get("target_lang") or "").strip().lower() or None,
        action_type=action_type,
        provider=provider,
        units_type=units_type,
        units_value=units_value,
        currency=str(payload.get("currency") or BILLING_CURRENCY_DEFAULT),
        status=str(payload.get("status") or "estimated"),
        metadata=metadata,
        price_snapshot_id=int(payload["price_snapshot_id"]) if payload.get("price_snapshot_id") is not None else None,
        price_provider=str(payload.get("price_provider") or "").strip().lower() or None,
        price_sku=str(payload.get("price_sku") or "").strip() or None,
        price_unit=str(payload.get("price_unit") or "").strip().lower() or None,
        cost_amount=float(payload["cost_amount"]) if payload.get("cost_amount") is not None else None,
    )
    if not item:
        return jsonify({"error": "Не удалось записать billing event"}), 500
    return jsonify({"ok": True, "event": item})


@app.route("/api/admin/economics/price-snapshots/sync-env", methods=["POST"])
def sync_economics_price_snapshots_from_env():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401
    try:
        result = _sync_openai_price_snapshots_public_then_env()
    except Exception as exc:
        return jsonify({"error": f"Не удалось синхронизировать price snapshots: {exc}"}), 500
    return jsonify({"ok": True, "result": result})


@app.route("/api/webapp/dictionary", methods=["POST"])
def lookup_webapp_dictionary():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    word_ru = (payload.get("word") or "").strip()
    lookup_lang = _normalize_short_lang_code(payload.get("lookup_lang"), fallback="")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not word_ru:
        return jsonify({"error": "word обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        # Unified multilang lookup for all language pairs (including legacy RU<->DE)
        # to keep inline popup translation stable in reader/overlay modes.
        query_source_lang = source_lang
        query_target_lang = target_lang
        if lookup_lang and lookup_lang == target_lang:
            query_source_lang = target_lang
            query_target_lang = source_lang
        elif lookup_lang and lookup_lang == source_lang:
            query_source_lang = source_lang
            query_target_lang = target_lang
        elif _is_legacy_ru_de_pair(source_lang, target_lang):
            # Legacy fallback without explicit hint: infer by alphabet.
            is_ru = any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in word_ru)
            query_source_lang = "ru" if is_ru else "de"
            query_target_lang = "de" if is_ru else "ru"

        raw = asyncio.run(
            run_dictionary_lookup_multilang(
                word=word_ru,
                source_lang=query_source_lang,
                target_lang=query_target_lang,
            )
        )
        usage_main = get_last_llm_usage(reset=True)
        result, detected, source_value, target_value = _build_multilang_dictionary_result(
            raw=raw,
            query_word=word_ru,
            source_lang=query_source_lang,
            target_lang=query_target_lang,
        )
        if lookup_lang and lookup_lang in {query_source_lang, query_target_lang}:
            # Hard-fix direction from explicit UI language hint to avoid
            # wrong reversals when model mis-detects the query language.
            direction = f"{query_source_lang}-{query_target_lang}"
        else:
            direction = f"{query_source_lang}-{query_target_lang}" if detected != "target" else f"{query_target_lang}-{query_source_lang}"

        # Fallback for cases where model returns identical source/target.
        if not source_value or source_value.casefold() == target_value.casefold():
            reverse_raw = asyncio.run(
                run_dictionary_lookup_multilang(
                    word=word_ru,
                    source_lang=query_target_lang,
                    target_lang=query_source_lang,
                )
            )
            usage_reverse = get_last_llm_usage(reset=True)
            _billing_log_openai_usage(
                user_id=int(user_id),
                action_type="dictionary_lookup_fallback",
                source_lang=source_lang,
                target_lang=target_lang,
                usage=usage_reverse,
                seed=f"dict_lookup_fallback:{user_id}:{word_ru}:{direction}:{time.time_ns()}",
                metadata={"word": word_ru, "direction": direction},
            )
            reverse_target = str(reverse_raw.get("word_target") or "").strip()
            if reverse_target and reverse_target.casefold() != target_value.casefold():
                result["word_ru"] = reverse_target
                result["translation_ru"] = reverse_target
                result["source_text"] = reverse_target
            else:
                forced = _force_translate_text(
                    text=word_ru,
                    source_lang=query_source_lang,
                    target_lang=query_target_lang,
                )
                if forced and forced.casefold() != target_value.casefold():
                    result["word_ru"] = forced
                    result["translation_ru"] = forced
                    result["source_text"] = forced
        # If explicit lookup language is provided and selected word is from
        # query_source_lang, ensure target side is truly translated.
        if lookup_lang and lookup_lang == query_source_lang:
            current_target = str(result.get("target_text") or "").strip()
            current_source = str(result.get("source_text") or word_ru).strip()
            if not current_target or current_target.casefold() == current_source.casefold():
                forced_target = _force_translate_text(
                    text=word_ru,
                    source_lang=query_source_lang,
                    target_lang=query_target_lang,
                )
                if forced_target and forced_target.casefold() != current_source.casefold():
                    result["target_text"] = forced_target
                    result["translation_de"] = forced_target
                    result["word_de"] = forced_target
                    target_value = forced_target
    except Exception as exc:
        return jsonify({"error": f"Ошибка запроса словаря: {exc}"}), 500

    result = _decorate_dictionary_item(
        result if isinstance(result, dict) else {},
        source_lang=source_lang,
        target_lang=target_lang,
        direction=direction,
    )
    _billing_log_event_safe(
        user_id=int(user_id),
        action_type="dictionary_lookup",
        provider="openai",
        units_type="requests",
        units_value=1.0,
        source_lang=source_lang,
        target_lang=target_lang,
        idempotency_seed=f"dict_lookup:{user_id}:{source_lang}:{target_lang}:{word_ru.lower()}:{direction}:{time.time_ns()}",
        status="estimated",
        metadata={
            "word": word_ru,
            "direction": direction,
            "lookup_lang": lookup_lang or None,
        },
    )
    _billing_log_openai_usage(
        user_id=int(user_id),
        action_type="dictionary_lookup",
        source_lang=source_lang,
        target_lang=target_lang,
        usage=locals().get("usage_main"),
        seed=f"dict_lookup_tokens:{user_id}:{word_ru}:{direction}:{time.time_ns()}",
        metadata={
            "word": word_ru,
            "direction": direction,
            "lookup_lang": lookup_lang or None,
        },
    )
    return jsonify(
        {
            "ok": True,
            "item": result,
            "direction": direction,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/mobile/dictionary/lookup", methods=["POST"])
def lookup_mobile_dictionary():
    user_id, _username, error = _get_mobile_authenticated_user()
    if error:
        return error

    payload = request.get_json(silent=True) or {}
    word = (payload.get("word") or "").strip()
    if not word:
        return jsonify({"error": "word обязателен"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            is_ru = any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in word)
            if is_ru:
                cached = get_dictionary_cache(word)
                if cached:
                    cached_item = _decorate_dictionary_item(
                        cached if isinstance(cached, dict) else {},
                        source_lang=source_lang,
                        target_lang=target_lang,
                        direction="ru-de",
                    )
                    return jsonify(
                        {
                            "ok": True,
                            "item": cached_item,
                            "direction": "ru-de",
                            "user_id": user_id,
                            "language_pair": _build_language_pair_payload(source_lang, target_lang),
                        }
                    )
                result = asyncio.run(run_dictionary_lookup(word))
            else:
                result = asyncio.run(run_dictionary_lookup_de(word))
            if result and is_ru:
                upsert_dictionary_cache(word, result)
            direction = "ru-de" if is_ru else "de-ru"
        else:
            raw = asyncio.run(
                run_dictionary_lookup_multilang(
                    word=word,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
            result, detected, source_value, target_value = _build_multilang_dictionary_result(
                raw=raw,
                query_word=word,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            direction = f"{source_lang}-{target_lang}" if detected != "target" else f"{target_lang}-{source_lang}"

            if not source_value or source_value.casefold() == target_value.casefold():
                reverse_raw = asyncio.run(
                    run_dictionary_lookup_multilang(
                        word=word,
                        source_lang=target_lang,
                        target_lang=source_lang,
                    )
                )
                reverse_target = str(reverse_raw.get("word_target") or "").strip()
                if reverse_target and reverse_target.casefold() != target_value.casefold():
                    result["word_ru"] = reverse_target
                    result["translation_ru"] = reverse_target
                    result["source_text"] = reverse_target
                else:
                    forced = _force_translate_text(
                        text=word,
                        source_lang=target_lang,
                        target_lang=source_lang,
                    )
                    if forced and forced.casefold() != target_value.casefold():
                        result["word_ru"] = forced
                        result["translation_ru"] = forced
                        result["source_text"] = forced
    except Exception as exc:
        return jsonify({"error": f"Ошибка запроса словаря: {exc}"}), 500

    result = _decorate_dictionary_item(
        result if isinstance(result, dict) else {},
        source_lang=source_lang,
        target_lang=target_lang,
        direction=direction,
    )
    return jsonify(
        {
            "ok": True,
            "item": result,
            "direction": direction,
            "user_id": user_id,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/mobile/dashboard", methods=["GET"])
def get_mobile_dashboard():
    user_id, _username, error = _get_mobile_authenticated_user()
    if error:
        return error

    now_utc = datetime.now(timezone.utc)
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        queue_info = _compute_srs_queue_info(
            user_id=user_id,
            now_utc=now_utc,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка чтения SRS-статистики: {exc}"}), 500

    word_of_day = None
    try:
        latest_items = get_webapp_dictionary_entries(
            user_id=user_id,
            limit=1,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        if latest_items:
            item = latest_items[0] or {}
            response_json = item.get("response_json") if isinstance(item.get("response_json"), dict) else {}
            source_word = (
                item.get("word_de")
                or item.get("word_ru")
                or response_json.get("word_de")
                or response_json.get("word_ru")
            )
            target_translation = (
                item.get("translation_ru")
                or item.get("translation_de")
                or response_json.get("translation_ru")
                or response_json.get("translation_de")
            )
            word_of_day = {
                "entry_id": item.get("id"),
                "source": source_word,
                "target": target_translation,
                "created_at": item.get("created_at"),
            }
    except Exception:
        word_of_day = None

    return jsonify(
        {
            "ok": True,
            "queue_info": {
                "due_count": int(queue_info.get("due_count") or 0),
                "new_remaining_today": int(queue_info.get("new_remaining_today") or 0),
            },
            "word_of_day": word_of_day,
            "deep_links": {
                "review": _build_webapp_deeplink("review"),
                "webapp": _build_webapp_deeplink("webapp"),
            },
        }
    )


@app.route("/api/today", methods=["GET"])
def get_today_plan():
    user_id, username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    requested_date = request.args.get("date")
    plan_date = _safe_plan_date(requested_date, TODAY_PLAN_DEFAULT_TZ)
    auto_opt_in = (os.getenv("TODAY_REMINDER_AUTO_OPT_IN") or "1").strip().lower() in {"1", "true", "yes", "on"}
    if auto_opt_in:
        try:
            current = get_today_reminder_settings(int(user_id))
            if not current.get("updated_at"):
                upsert_today_reminder_settings(
                    int(user_id),
                    enabled=True,
                    timezone_name=TODAY_PLAN_DEFAULT_TZ,
                    reminder_hour=7,
                    reminder_minute=0,
                )
        except Exception as exc:
            logging.warning("Today reminder auto-opt-in skipped for user %s: %s", user_id, exc)

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        plan = _get_or_create_today_plan(
            user_id=int(user_id),
            plan_date=plan_date,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка формирования плана: {exc}"}), 500

    response_payload = _format_today_plan_response(plan)
    response_payload.update(
        {
            "ok": True,
            "user_id": int(user_id),
            "username": username,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )
    return jsonify(response_payload)


@app.route("/api/today/regenerate", methods=["POST"])
def regenerate_today_plan():
    user_id, username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    requested_date = request.args.get("date")
    if not requested_date and isinstance(payload, dict):
        requested_date = payload.get("date")
    plan_date = _safe_plan_date(requested_date, TODAY_PLAN_DEFAULT_TZ)
    try:
        limit_result = consume_today_regenerate_limit(
            user_id=int(user_id),
            limit_date=plan_date,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка проверки лимита пересборки: {exc}"}), 500
    if not bool((limit_result or {}).get("allowed")):
        return jsonify(
            {
                "error": "План можно пересобрать только один раз в день.",
                "ok": False,
                "limit_reached": True,
                "date": plan_date.isoformat(),
                "consumed_at": (limit_result or {}).get("consumed_at"),
            }
        ), 429

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        plan = _build_today_plan_for_user(
            user_id=int(user_id),
            plan_date=plan_date,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка пересборки плана: {exc}"}), 500

    response_payload = _format_today_plan_response(plan)
    response_payload.update(
        {
            "ok": True,
            "regenerated": True,
            "user_id": int(user_id),
            "username": username,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )
    return jsonify(response_payload)


@app.route("/api/today/items/<int:item_id>/start", methods=["POST"])
def start_today_item(item_id: int):
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    try:
        item = update_daily_plan_item_status(
            user_id=int(user_id),
            item_id=int(item_id),
            status="doing",
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка обновления статуса: {exc}"}), 500

    if not item:
        return jsonify({"error": "Задача не найдена"}), 404
    return jsonify({"ok": True, "item": item})


@app.route("/api/today/items/<int:item_id>/translation/start", methods=["POST"])
def start_today_translation_item(item_id: int):
    user_id, username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    requested_level = str(payload.get("level") or "").strip().lower()
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    today_date = _get_local_today_date(TODAY_PLAN_DEFAULT_TZ)
    plan = get_daily_plan(user_id=int(user_id), plan_date=today_date)
    if not plan:
        return jsonify({"error": "План на сегодня не найден"}), 404

    item = None
    for entry in (plan.get("items") or []):
        if int(entry.get("id") or 0) == int(item_id):
            item = entry
            break
    if not item:
        return jsonify({"error": "Задача не найдена"}), 404
    if str(item.get("task_type") or "").strip().lower() != "translation":
        return jsonify({"error": "Эта задача не является переводом"}), 400

    task_payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    level = requested_level or str(task_payload.get("level") or "c1").strip().lower() or "c1"
    skill_title = str(task_payload.get("skill_title") or "").strip()
    sub_category = str(task_payload.get("sub_category") or "").strip()
    main_category = str(task_payload.get("main_category") or "").strip()
    topic_label = sub_category or skill_title or main_category or "Weak skill practice"
    if skill_title and sub_category:
        topic_label = f"{skill_title}: {sub_category}"
    elif skill_title and not sub_category:
        topic_label = skill_title

    try:
        session = asyncio.run(
            start_translation_session_webapp(
                user_id=int(user_id),
                username=username,
                topic=topic_label,
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка запуска переводов по задаче дня: {exc}"}), 500

    updated_item = update_daily_plan_item_status(
        user_id=int(user_id),
        item_id=int(item_id),
        status="doing",
    )
    return jsonify(
        {
            "ok": True,
            "item": updated_item or item,
            "practice": {
                "task_type": "translation",
                "mode": str(task_payload.get("mode") or "weakest_topic"),
                "topic_label": topic_label,
                "level": level,
                "sentences": int(task_payload.get("sentences") or 7),
                "main_category": main_category or None,
                "sub_category": sub_category or None,
                "skill_title": skill_title or None,
            },
            **session,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/today/items/<int:item_id>/timer", methods=["POST"])
def update_today_item_timer(item_id: int):
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "sync").strip().lower() or "sync"
    elapsed_raw = payload.get("elapsed_seconds")
    running_raw = payload.get("running")

    elapsed_seconds = None
    if elapsed_raw is not None and str(elapsed_raw).strip() != "":
        try:
            elapsed_seconds = max(0, int(elapsed_raw))
        except Exception:
            return jsonify({"error": "elapsed_seconds должен быть числом"}), 400

    running = None if running_raw is None else bool(running_raw)

    try:
        item = update_daily_plan_item_timer(
            user_id=int(user_id),
            item_id=int(item_id),
            action=action,
            elapsed_seconds=elapsed_seconds,
            running=running,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка обновления таймера задачи: {exc}"}), 500

    if not item:
        return jsonify({"error": "Задача не найдена"}), 404
    return jsonify({"ok": True, "item": item})


@app.route("/api/today/video/recommend", methods=["POST"])
def recommend_today_video():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    lookback_days_raw = payload.get("lookback_days", 7)
    try:
        lookback_days = max(1, min(30, int(lookback_days_raw)))
    except Exception:
        lookback_days = 7

    skill_id = str(payload.get("skill_id") or "").strip()
    skill_title = str(payload.get("skill_title") or "").strip()
    main_category = str(payload.get("main_category") or "").strip()
    sub_category = str(payload.get("sub_category") or "").strip()
    examples_payload = payload.get("examples") if isinstance(payload.get("examples"), list) else []
    resolved_skill = None

    if not skill_id:
        try:
            weakest_skill = get_lowest_mastery_skill(
                user_id=int(user_id),
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            weakest_skill = None
        if weakest_skill:
            skill_id = str(weakest_skill.get("skill_id") or "").strip()
            skill_title = str(weakest_skill.get("skill_title") or "").strip()
            resolved_skill = weakest_skill

    if skill_id and (not main_category or not sub_category):
        try:
            focus_topic = get_top_error_topic_for_skill(
                user_id=int(user_id),
                skill_id=skill_id,
                lookback_days=lookback_days,
                source_lang=source_lang,
                target_lang=target_lang,
            ) or {}
            if not main_category:
                main_category = str(focus_topic.get("main_category") or "").strip()
            if not sub_category:
                sub_category = str(focus_topic.get("sub_category") or "").strip()
        except Exception:
            pass

    if not main_category and not sub_category:
        try:
            weak_topic = get_top_weak_topic(
                user_id=int(user_id),
                lookback_days=lookback_days,
                source_lang=source_lang,
                target_lang=target_lang,
            ) or {}
            main_category = str(weak_topic.get("main_category") or "").strip()
            sub_category = str(weak_topic.get("sub_category") or "").strip()
        except Exception:
            pass

    recommended_video = None
    recommendation_row = None
    cache_hit = False

    try:
        recommendation_row = get_best_video_recommendation_for_focus(
            source_lang=source_lang,
            target_lang=target_lang,
            skill_id=skill_id or None,
            main_category=main_category or None,
            sub_category=sub_category or None,
        )
    except Exception:
        recommendation_row = None

    if recommendation_row and recommendation_row.get("video_id"):
        cache_hit = True
        recommended_video = {
            "video_id": recommendation_row.get("video_id"),
            "video_url": recommendation_row.get("video_url") or f"https://www.youtube.com/watch?v={recommendation_row.get('video_id')}",
            "title": recommendation_row.get("video_title"),
            "query": recommendation_row.get("search_query"),
            "recommendation_id": recommendation_row.get("id"),
            "like_count": int(recommendation_row.get("like_count") or 0),
            "dislike_count": int(recommendation_row.get("dislike_count") or 0),
            "score": int(recommendation_row.get("score") or 0),
        }

    if not recommended_video:
        recommended_video = _pick_today_recommended_video(
            main_category or None,
            sub_category or None,
            skill_title=skill_title or None,
            examples=examples_payload[:5],
            target_lang=target_lang,
            billing_user_id=int(user_id),
            billing_source_lang=source_lang,
            billing_target_lang=target_lang,
        )
        if not recommended_video:
            logging.warning(
                "YT recommendation not found: user_id=%s skill_id=%s skill_title='%s' main='%s' sub='%s'",
                int(user_id),
                skill_id,
                skill_title,
                main_category,
                sub_category,
            )

        if recommended_video and recommended_video.get("video_id"):
            try:
                recommendation_row = upsert_video_recommendation(
                    source_lang=source_lang,
                    target_lang=target_lang,
                    skill_id=skill_id or None,
                    main_category=main_category or None,
                    sub_category=sub_category or None,
                    search_query=(recommended_video or {}).get("query"),
                    video_id=str((recommended_video or {}).get("video_id") or "").strip(),
                    video_url=(recommended_video or {}).get("video_url"),
                    video_title=(recommended_video or {}).get("title"),
                )
            except Exception:
                recommendation_row = None
            if recommendation_row:
                recommended_video["recommendation_id"] = recommendation_row.get("id")
                recommended_video["like_count"] = int(recommendation_row.get("like_count") or 0)
                recommended_video["dislike_count"] = int(recommendation_row.get("dislike_count") or 0)
                recommended_video["score"] = int(recommendation_row.get("score") or 0)

    updated_item = None
    item_id_raw = payload.get("item_id")
    if item_id_raw is not None and str(item_id_raw).strip() != "" and recommended_video:
        try:
            item_id = int(item_id_raw)
        except Exception:
            return jsonify({"error": "item_id должен быть числом"}), 400
        payload_updates = {
            "video_id": (recommended_video or {}).get("video_id"),
            "video_url": (recommended_video or {}).get("video_url"),
            "video_title": (recommended_video or {}).get("title"),
            "video_query": (recommended_video or {}).get("query"),
            "recommendation_id": (recommended_video or {}).get("recommendation_id"),
            "video_likes": int((recommended_video or {}).get("like_count") or 0),
            "video_dislikes": int((recommended_video or {}).get("dislike_count") or 0),
            "video_score": int((recommended_video or {}).get("score") or 0),
        }
        try:
            updated_item = update_daily_plan_item_payload(
                user_id=int(user_id),
                item_id=item_id,
                payload_updates=payload_updates,
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка сохранения рекомендованного видео: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "focus": {
                "skill_id": skill_id or None,
                "skill_title": skill_title or None,
                "main_category": main_category or None,
                "sub_category": sub_category or None,
            },
            "video": recommended_video,
            "updated_item": updated_item,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            "resolved_from_mastery": bool(resolved_skill),
            "cache_hit": cache_hit,
        }
    )


@app.route("/api/today/video/feedback", methods=["POST"])
def today_video_feedback():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    recommendation_id_raw = payload.get("recommendation_id")
    item_id_raw = payload.get("item_id")
    vote_raw = str(payload.get("vote") or "").strip().lower()

    if vote_raw in {"like", "up", "+1", "1", "thumbs_up"}:
        vote_value = 1
    elif vote_raw in {"dislike", "down", "-1", "thumbs_down"}:
        vote_value = -1
    else:
        return jsonify({"error": "vote должен быть like или dislike"}), 400

    try:
        recommendation_id = int(recommendation_id_raw)
    except Exception:
        return jsonify({"error": "recommendation_id должен быть числом"}), 400

    recommendation = get_video_recommendation_by_id(recommendation_id)
    if not recommendation:
        return jsonify({"error": "Рекомендация не найдена"}), 404

    try:
        updated_recommendation = vote_video_recommendation(
            user_id=int(user_id),
            recommendation_id=recommendation_id,
            vote=vote_value,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения оценки видео: {exc}"}), 500

    updated_item = None
    if item_id_raw is not None and str(item_id_raw).strip() != "":
        try:
            item_id = int(item_id_raw)
        except Exception:
            return jsonify({"error": "item_id должен быть числом"}), 400
        payload_updates = {
            "recommendation_id": recommendation_id,
            "video_likes": int((updated_recommendation or {}).get("like_count") or 0),
            "video_dislikes": int((updated_recommendation or {}).get("dislike_count") or 0),
            "video_score": int((updated_recommendation or {}).get("score") or 0),
            "video_user_vote": int(vote_value),
        }
        try:
            updated_item = update_daily_plan_item_payload(
                user_id=int(user_id),
                item_id=item_id,
                payload_updates=payload_updates,
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка обновления задачи после оценки: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "recommendation": updated_recommendation,
            "updated_item": updated_item,
        }
    )


@app.route("/api/today/theory/prepare", methods=["POST"])
def prepare_today_theory():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    force_refresh = bool(payload.get("force_refresh"))

    item_id_raw = payload.get("item_id")
    item_payload = payload.get("item_payload") if isinstance(payload.get("item_payload"), dict) else {}
    cached_package = item_payload.get("theory_package") if isinstance(item_payload.get("theory_package"), dict) else None
    if not force_refresh and isinstance(cached_package, dict):
        generated_at_raw = str(cached_package.get("generated_at") or "").strip()
        try:
            generated_at_dt = datetime.fromisoformat(generated_at_raw.replace("Z", "+00:00"))
            if generated_at_dt.tzinfo is None:
                generated_at_dt = generated_at_dt.replace(tzinfo=timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - generated_at_dt).total_seconds()
            if age_seconds >= 0 and age_seconds <= (THEORY_PACKAGE_TTL_MINUTES * 60):
                return jsonify({"ok": True, "package": cached_package, "updated_item": None, "cached": True})
        except Exception:
            pass

    lookback_days_raw = payload.get("lookback_days") or item_payload.get("lookback_days") or 14
    try:
        lookback_days = max(1, min(30, int(lookback_days_raw)))
    except Exception:
        lookback_days = 14

    skill_id = str(item_payload.get("skill_id") or payload.get("skill_id") or "").strip()
    skill_name = str(item_payload.get("skill_title") or payload.get("skill_title") or "").strip()
    main_category = str(item_payload.get("main_category") or payload.get("main_category") or "").strip()
    sub_category = str(item_payload.get("sub_category") or payload.get("sub_category") or "").strip()
    example_strings = [str(item).strip() for item in (item_payload.get("examples") or payload.get("examples") or []) if str(item).strip()]

    weakest_skill = None
    if not skill_id:
        try:
            weakest_skill = get_lowest_mastery_skill(
                user_id=int(user_id),
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            weakest_skill = None
        if weakest_skill:
            skill_id = str(weakest_skill.get("skill_id") or "").strip()
            if not skill_name:
                skill_name = str(weakest_skill.get("skill_title") or "").strip()

    if skill_id and (not main_category or not sub_category):
        try:
            top_topic = get_top_error_topic_for_skill(
                user_id=int(user_id),
                skill_id=skill_id,
                lookback_days=lookback_days,
                source_lang=source_lang,
                target_lang=target_lang,
            ) or {}
            if not main_category:
                main_category = str(top_topic.get("main_category") or "").strip()
            if not sub_category:
                sub_category = str(top_topic.get("sub_category") or "").strip()
        except Exception:
            pass

    if not main_category and not sub_category:
        try:
            weak_topic = get_top_weak_topic(
                user_id=int(user_id),
                lookback_days=lookback_days,
                source_lang=source_lang,
                target_lang=target_lang,
            ) or {}
            main_category = str(weak_topic.get("main_category") or "").strip()
            sub_category = str(weak_topic.get("sub_category") or "").strip()
        except Exception:
            pass

    detailed_examples: list[dict] = []
    if main_category and sub_category:
        try:
            detailed_examples = get_recent_mistake_examples_for_topic(
                user_id=int(user_id),
                main_category=main_category,
                sub_category=sub_category,
                lookback_days=lookback_days,
                limit=5,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except Exception:
            detailed_examples = []

    formatted_examples = _format_theory_mistake_examples(detailed_examples)
    if not formatted_examples and example_strings:
        formatted_examples = example_strings[:5]

    is_beginner = False
    if not skill_id and not main_category and not sub_category and not formatted_examples:
        is_beginner = True
        excluded_topics = [str(item.get("topic_name") or "").strip() for item in list_default_topics_for_user(
            user_id=int(user_id),
            target_language=target_lang,
            limit=100,
        ) if str(item.get("topic_name") or "").strip()]
        beginner_payload = {
            "target_language": target_lang,
            "native_language": source_lang,
            "excluded_topics": excluded_topics,
        }
        beginner_topic = {}
        try:
            beginner_topic = asyncio.run(run_beginner_topic(beginner_payload))
        except Exception as exc:
            logging.warning("BEGINNER_TOPIC failed: %s", exc)
            beginner_topic = {}
        topic_name = str(beginner_topic.get("topic_name") or "").strip() or "Present tense and basic word order"
        main_category = str(beginner_topic.get("error_category") or "").strip() or "Other mistake"
        sub_category = str(beginner_topic.get("error_subcategory") or "").strip() or "Unclassified mistake"
        skill_id = str(beginner_topic.get("skill_id") or "").strip()
        skill_name = str(beginner_topic.get("develops_skill") or "").strip() or topic_name
        formatted_examples = [
            str(beginner_topic.get("why_important") or "").strip() or "Beginner starting point without historical mistakes."
        ]
        try:
            add_default_topic_for_user(
                user_id=int(user_id),
                target_language=target_lang,
                topic_name=topic_name,
                error_category=main_category,
                error_subcategory=sub_category,
                skill_id=skill_id or None,
            )
        except Exception as exc:
            logging.warning("Saving default topic failed: %s", exc)

    if not skill_name:
        skill_name = sub_category or main_category or "Grammar basics"

    theory_payload = {
        "target_language": target_lang,
        "native_language": source_lang,
        "skill_name": skill_name,
        "error_category": main_category or "Other mistake",
        "error_subcategory": sub_category or "Unclassified mistake",
        "user_mistake_examples": formatted_examples[:8],
    }
    practice_payload = {
        "target_language": target_lang,
        "native_language": source_lang,
        "skill_name": skill_name,
        "error_category": main_category or "Other mistake",
        "error_subcategory": sub_category or "Unclassified mistake",
    }

    try:
        theory = asyncio.run(run_theory_generation(theory_payload))
    except Exception as exc:
        return jsonify({"error": f"Ошибка генерации теории: {exc}"}), 500
    usage_theory = get_last_llm_usage(reset=True)
    if not isinstance(theory, dict):
        theory = {}
    theory["resources"] = _normalize_theory_resources(theory, target_lang)

    try:
        practice_raw = asyncio.run(run_theory_practice_sentences(practice_payload))
    except Exception as exc:
        return jsonify({"error": f"Ошибка генерации практики: {exc}"}), 500
    usage_practice = get_last_llm_usage(reset=True)

    practice_sentences = _normalize_theory_sentences(practice_raw)
    if len(practice_sentences) < 5:
        practice_sentences = [
            "Я живу в большом городе.",
            "Мы часто ходим в парк по выходным.",
            "Она хочет учить язык каждый день.",
            "Они купили новую книгу вчера.",
            "Ты можешь прийти сегодня вечером?"
        ]

    package = {
        "focus": {
            "skill_id": skill_id or None,
            "skill_name": skill_name,
            "error_category": main_category or "Other mistake",
            "error_subcategory": sub_category or "Unclassified mistake",
            "is_beginner": bool(is_beginner),
        },
        "theory": theory,
        "practice_sentences": practice_sentences[:5],
        "examples_used": formatted_examples[:8],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    _billing_log_event_safe(
        user_id=int(user_id),
        action_type="theory_package_prepare",
        provider="openai",
        units_type="requests",
        units_value=2.0,
        source_lang=source_lang,
        target_lang=target_lang,
        idempotency_seed=(
            f"theory_prepare:{user_id}:{skill_id or ''}:{main_category or ''}:"
            f"{sub_category or ''}:{package['generated_at']}"
        ),
        status="estimated",
        metadata={
            "skill_id": skill_id or None,
            "skill_name": skill_name,
            "is_beginner": bool(is_beginner),
        },
    )
    _billing_log_openai_usage(
        user_id=int(user_id),
        action_type="theory_generation",
        source_lang=source_lang,
        target_lang=target_lang,
        usage=usage_theory,
        seed=f"theory_generation:{user_id}:{skill_id or ''}:{main_category or ''}:{sub_category or ''}:{time.time_ns()}",
        metadata={"skill_id": skill_id or None, "skill_name": skill_name},
    )
    _billing_log_openai_usage(
        user_id=int(user_id),
        action_type="theory_practice_sentences",
        source_lang=source_lang,
        target_lang=target_lang,
        usage=usage_practice,
        seed=f"theory_practice:{user_id}:{skill_id or ''}:{main_category or ''}:{sub_category or ''}:{time.time_ns()}",
        metadata={"skill_id": skill_id or None, "skill_name": skill_name},
    )

    updated_item = None
    if item_id_raw is not None and str(item_id_raw).strip() != "":
        try:
            item_id = int(item_id_raw)
        except Exception:
            return jsonify({"error": "item_id должен быть числом"}), 400
        try:
            updated_item = update_daily_plan_item_payload(
                user_id=int(user_id),
                item_id=item_id,
                payload_updates={
                    "skill_id": package["focus"]["skill_id"],
                    "skill_title": package["focus"]["skill_name"],
                    "main_category": package["focus"]["error_category"],
                    "sub_category": package["focus"]["error_subcategory"],
                    "theory_package": package,
                },
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка сохранения пакета теории: {exc}"}), 500

    return jsonify({"ok": True, "package": package, "updated_item": updated_item})


@app.route("/api/today/theory/check", methods=["POST"])
def check_today_theory():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    item_id_raw = payload.get("item_id")
    focus = payload.get("focus") if isinstance(payload.get("focus"), dict) else {}
    native_sentences = payload.get("native_sentences") if isinstance(payload.get("native_sentences"), list) else []
    translations = payload.get("translations") if isinstance(payload.get("translations"), list) else []
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    paired = []
    for idx in range(5):
        native_sentence = str(native_sentences[idx] if idx < len(native_sentences) else "").strip()
        learner_translation = str(translations[idx] if idx < len(translations) else "").strip()
        if not native_sentence:
            continue
        paired.append(
            {
                "native_sentence": native_sentence,
                "learner_translation": learner_translation,
            }
        )
    if len(paired) < 1:
        return jsonify({"error": "Нет предложений для проверки"}), 400

    check_payload = {
        "target_language": target_lang,
        "native_language": source_lang,
        "skill_name": str(focus.get("skill_name") or "Grammar basics"),
        "error_category": str(focus.get("error_category") or "Other mistake"),
        "error_subcategory": str(focus.get("error_subcategory") or "Unclassified mistake"),
        "pairs": paired,
    }
    try:
        feedback = asyncio.run(run_theory_check_feedback(check_payload))
    except Exception as exc:
        return jsonify({"error": f"Ошибка проверки теории: {exc}"}), 500
    usage_check = get_last_llm_usage(reset=True)
    _billing_log_event_safe(
        user_id=int(user_id),
        action_type="theory_check_feedback",
        provider="openai",
        units_type="requests",
        units_value=1.0,
        source_lang=source_lang,
        target_lang=target_lang,
        idempotency_seed=f"theory_check:{user_id}:{len(paired)}:{str(item_id_raw or '')}:{time.time_ns()}",
        status="estimated",
        metadata={
            "pairs_count": len(paired),
            "item_id": int(item_id_raw) if str(item_id_raw or "").strip().isdigit() else None,
        },
    )
    _billing_log_openai_usage(
        user_id=int(user_id),
        action_type="theory_check_feedback",
        source_lang=source_lang,
        target_lang=target_lang,
        usage=usage_check,
        seed=f"theory_check_tokens:{user_id}:{len(paired)}:{time.time_ns()}",
        metadata={"pairs_count": len(paired)},
    )

    updated_item = None
    if item_id_raw is not None and str(item_id_raw).strip() != "":
        try:
            item_id = int(item_id_raw)
        except Exception:
            return jsonify({"error": "item_id должен быть числом"}), 400
        try:
            updated_item = update_daily_plan_item_payload(
                user_id=int(user_id),
                item_id=item_id,
                payload_updates={
                    "theory_last_check": {
                        "items": (feedback or {}).get("items") if isinstance(feedback, dict) else [],
                        "summary_good": (feedback or {}).get("summary_good") if isinstance(feedback, dict) else "",
                        "summary_improve": (feedback or {}).get("summary_improve") if isinstance(feedback, dict) else "",
                        "memory_secret": (feedback or {}).get("memory_secret") if isinstance(feedback, dict) else "",
                        "checked_at": datetime.now(timezone.utc).isoformat(),
                    }
                },
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка сохранения проверки теории: {exc}"}), 500

    return jsonify({"ok": True, "feedback": feedback if isinstance(feedback, dict) else {}, "updated_item": updated_item})


@app.route("/api/today/items/<int:item_id>/complete", methods=["POST"])
def complete_today_item(item_id: int):
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    try:
        item = update_daily_plan_item_status(
            user_id=int(user_id),
            item_id=int(item_id),
            status="done",
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка обновления статуса: {exc}"}), 500

    if not item:
        return jsonify({"error": "Задача не найдена"}), 404
    return jsonify({"ok": True, "item": item})


@app.route("/api/today/reminders", methods=["GET", "POST"])
def today_reminders_settings():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    if request.method == "GET":
        settings = get_today_reminder_settings(int(user_id))
        return jsonify({"ok": True, "settings": settings})

    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get("enabled"))
    timezone_name = (payload.get("timezone") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    reminder_hour = int(payload.get("reminder_hour", 7))
    reminder_minute = int(payload.get("reminder_minute", 0))
    try:
        settings = upsert_today_reminder_settings(
            int(user_id),
            enabled=enabled,
            timezone_name=timezone_name,
            reminder_hour=reminder_hour,
            reminder_minute=reminder_minute,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения настроек reminder: {exc}"}), 500
    return jsonify({"ok": True, "settings": settings})


@app.route("/api/today/reminders/test", methods=["POST"])
def today_reminders_test_send():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    requested_date = request.args.get("date")
    if not requested_date and isinstance(payload, dict):
        requested_date = payload.get("date")
    plan_date = _safe_plan_date(requested_date, TODAY_PLAN_DEFAULT_TZ)

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        plan = _get_or_create_today_plan(
            user_id=int(user_id),
            plan_date=plan_date,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        _send_today_plan_private_message(user_id=int(user_id), plan=plan)
    except Exception as exc:
        return jsonify({"error": f"Не удалось отправить тест в личку: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "sent": True,
            "date": plan_date.isoformat(),
            "user_id": int(user_id),
        }
    )


@app.route("/api/audio/grammar-settings", methods=["GET", "POST"])
def audio_grammar_settings():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    if request.method == "GET":
        settings = get_audio_grammar_settings(int(user_id))
        return jsonify({"ok": True, "settings": settings})

    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get("enabled"))
    try:
        settings = upsert_audio_grammar_settings(int(user_id), enabled=enabled)
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения audio grammar settings: {exc}"}), 500
    return jsonify({"ok": True, "settings": settings})


@app.route("/api/audio/grammar-optin", methods=["POST"])
def audio_grammar_optin():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    translation_id_raw = payload.get("translation_id")
    if translation_id_raw is None:
        return jsonify({"error": "translation_id обязателен"}), 400
    try:
        translation_id = int(translation_id_raw)
    except Exception:
        return jsonify({"error": "translation_id должен быть числом"}), 400
    enabled = bool(payload.get("enabled"))
    try:
        result = update_translation_audio_grammar_opt_in(
            int(user_id),
            translation_id=translation_id,
            enabled=enabled,
        )
    except ValueError:
        return jsonify({"error": "Перевод не найден"}), 404
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения opt-in: {exc}"}), 500

    return jsonify({"ok": True, "item": result})


@app.route("/api/assistant/session/start", methods=["POST"])
def start_assistant_session():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        session = start_agent_voice_session(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка старта голосовой сессии: {exc}"}), 500
    return jsonify({"ok": True, "session": session})


@app.route("/api/assistant/session/complete", methods=["POST"])
def complete_assistant_session():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status
    payload = request.get_json(silent=True) or {}
    session_id_raw = payload.get("session_id")
    session_id = None
    if session_id_raw is not None and str(session_id_raw).strip() != "":
        try:
            session_id = int(session_id_raw)
        except Exception:
            return jsonify({"error": "session_id должен быть числом"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        session = finish_agent_voice_session(
            user_id=int(user_id),
            session_id=session_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка завершения голосовой сессии: {exc}"}), 500
    if isinstance(session, dict):
        duration_seconds = max(0, int(session.get("duration_seconds") or 0))
        session_minutes = duration_seconds / 60.0
        resolved_session_id = int(session.get("session_id") or (session_id or 0))
        if session_minutes > 0 and resolved_session_id > 0:
            whisper_snapshot = get_effective_billing_price_snapshot(
                provider="openai",
                sku="whisper_input",
                unit="audio_minutes",
                currency=BILLING_CURRENCY_DEFAULT,
            )
            try:
                log_billing_event(
                    idempotency_key=f"voice_stt_whisper_priced_{resolved_session_id}_{int(user_id)}",
                    user_id=int(user_id),
                    source_lang=source_lang,
                    target_lang=target_lang,
                    action_type="voice_stt_whisper",
                    provider="openai",
                    units_type="audio_minutes",
                    units_value=session_minutes,
                    price_provider="openai" if whisper_snapshot else None,
                    price_sku="whisper_input" if whisper_snapshot else None,
                    price_unit="audio_minutes" if whisper_snapshot else None,
                    currency=BILLING_CURRENCY_DEFAULT,
                    status="estimated",
                    metadata={
                        "session_id": resolved_session_id,
                        "duration_seconds": duration_seconds,
                        "pricing_state": "priced" if whisper_snapshot else "missing_snapshot",
                    },
                )
            except Exception:
                pass
            tts_snapshot = get_effective_billing_price_snapshot(
                provider="agent_tts",
                sku="agent_tts_output",
                unit="audio_minutes",
                currency=BILLING_CURRENCY_DEFAULT,
            )
            try:
                log_billing_event(
                    idempotency_key=f"voice_tts_agent_priced_{resolved_session_id}_{int(user_id)}",
                    user_id=int(user_id),
                    source_lang=source_lang,
                    target_lang=target_lang,
                    action_type="voice_tts_agent",
                    provider="agent_tts",
                    units_type="audio_minutes",
                    units_value=session_minutes,
                    price_provider="agent_tts" if tts_snapshot else None,
                    price_sku="agent_tts_output" if tts_snapshot else None,
                    price_unit="audio_minutes" if tts_snapshot else None,
                    currency=BILLING_CURRENCY_DEFAULT,
                    status="estimated",
                    metadata={
                        "session_id": resolved_session_id,
                        "duration_seconds": duration_seconds,
                        "pricing_state": "priced" if tts_snapshot else "missing_snapshot",
                    },
                )
            except Exception:
                pass
            livekit_cost, livekit_meta = _compute_livekit_session_cost(
                session_minutes,
                currency=BILLING_CURRENCY_DEFAULT,
            )
            try:
                log_billing_event(
                    idempotency_key=f"livekit_room_minutes_{resolved_session_id}",
                    user_id=int(user_id),
                    source_lang=source_lang,
                    target_lang=target_lang,
                    action_type="livekit_room_minutes",
                    provider="livekit",
                    units_type="audio_minutes",
                    units_value=session_minutes,
                    cost_amount=livekit_cost,
                    currency=BILLING_CURRENCY_DEFAULT,
                    status="final",
                    metadata={
                        "session_id": resolved_session_id,
                        "duration_seconds": duration_seconds,
                        **(livekit_meta if isinstance(livekit_meta, dict) else {}),
                    },
                )
            except Exception:
                pass
    return jsonify({"ok": True, "session": session})


@app.route("/api/reader/session/start", methods=["POST"])
def start_reader_session_route():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        session = start_reader_session(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка старта сессии чтения: {exc}"}), 500
    return jsonify({"ok": True, "session": session})


@app.route("/api/reader/session/complete", methods=["POST"])
def complete_reader_session_route():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status
    payload = request.get_json(silent=True) or {}
    session_id_raw = payload.get("session_id")
    session_id = None
    if session_id_raw is not None and str(session_id_raw).strip() != "":
        try:
            session_id = int(session_id_raw)
        except Exception:
            return jsonify({"error": "session_id должен быть числом"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        session = finish_reader_session(
            user_id=int(user_id),
            session_id=session_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка завершения сессии чтения: {exc}"}), 500
    return jsonify({"ok": True, "session": session})


@app.route("/api/progress/skills", methods=["GET"])
def get_skill_progress():
    user_id, username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    raw_period = str(request.args.get("period") or "7d").strip().lower()
    lookback_days = 7
    if raw_period.endswith("d"):
        try:
            lookback_days = int(raw_period[:-1] or "7")
        except Exception:
            lookback_days = 7

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        report = get_skill_progress_report(
            user_id=int(user_id),
            lookback_days=lookback_days,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка построения отчёта по навыкам: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "user_id": int(user_id),
            "username": username,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            **report,
        }
    )


@app.route("/api/progress/weekly-plan", methods=["GET", "POST"])
def weekly_plan_progress():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        try:
            translations_goal = max(0, int(payload.get("translations_goal", 0)))
            learned_words_goal = max(0, int(payload.get("learned_words_goal", 0)))
            agent_minutes_goal = max(0, int(payload.get("agent_minutes_goal", 0)))
            reading_minutes_goal = max(0, int(payload.get("reading_minutes_goal", 0)))
        except Exception:
            return jsonify({"error": "Значения плана должны быть целыми числами"}), 400
        try:
            upsert_weekly_goals(
                user_id=int(user_id),
                source_lang=source_lang,
                target_lang=target_lang,
                translations_goal=translations_goal,
                learned_words_goal=learned_words_goal,
                agent_minutes_goal=agent_minutes_goal,
                reading_minutes_goal=reading_minutes_goal,
            )
        except Exception as exc:
            return jsonify({"error": f"Ошибка сохранения недельного плана: {exc}"}), 500

    try:
        goals = get_weekly_goals(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
        ) or {
            "translations_goal": 0,
            "learned_words_goal": 0,
            "agent_minutes_goal": 0,
            "reading_minutes_goal": 0,
        }
        progress = get_weekly_plan_progress(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
            mature_interval_days=MATURE_INTERVAL_DAYS,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка расчёта недельного прогресса: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            "week": {
                "start_date": progress.get("week_start"),
                "end_date": progress.get("week_end"),
                "as_of_date": progress.get("as_of_date"),
                "days_elapsed": int(progress.get("days_elapsed") or 1),
                "days_total": int(progress.get("days_total") or 7),
            },
            "plan": {
                "translations_goal": int(goals.get("translations_goal") or 0),
                "learned_words_goal": int(goals.get("learned_words_goal") or 0),
                "agent_minutes_goal": int(goals.get("agent_minutes_goal") or 0),
                "reading_minutes_goal": int(goals.get("reading_minutes_goal") or 0),
            },
            "metrics": progress.get("metrics") or {},
        }
    )


@app.route("/api/progress/plan-analytics", methods=["GET"])
def get_plan_analytics():
    user_id, _username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status
    raw_period = str(request.args.get("period") or "week").strip().lower()
    if raw_period == "half_year":
        raw_period = "half-year"
    if raw_period not in {"week", "month", "quarter", "half-year", "year"}:
        return jsonify({"error": "period must be one of: week, month, quarter, half-year, year"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        progress = get_plan_progress(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
            mature_interval_days=MATURE_INTERVAL_DAYS,
            period=raw_period,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка расчёта аналитики плана: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "period": raw_period,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            "range": {
                "start_date": progress.get("start_date"),
                "end_date": progress.get("end_date"),
                "as_of_date": progress.get("as_of_date"),
                "days_elapsed": int(progress.get("days_elapsed") or 1),
                "days_total": int(progress.get("days_total") or 1),
            },
            "metrics": progress.get("metrics") or {},
        }
    )


@app.route("/api/progress/skills/<string:skill_id>/practice/start", methods=["POST"])
def start_skill_practice(skill_id: str):
    user_id, username, error = _get_authenticated_user_from_request_init_data()
    if error:
        status = 401 if "прошёл проверку" in error else 403 if "Доступ" in error else 400
        return jsonify({"error": error}), status

    payload = request.get_json(silent=True) or {}
    level = str(payload.get("level") or "b1").strip().lower() or "b1"
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    skill = get_skill_by_id(skill_id, language_code=target_lang)
    if not skill or not bool(skill.get("is_active")):
        return jsonify({"error": "Навык не найден"}), 404

    focus_topic = get_top_error_topic_for_skill(
        user_id=int(user_id),
        skill_id=str(skill.get("skill_id") or skill_id),
        lookback_days=14,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    main_category = (focus_topic or {}).get("main_category")
    sub_category = (focus_topic or {}).get("sub_category")
    examples: list[str] = []
    if main_category and sub_category:
        examples = get_weak_topic_sentences(
            user_id=int(user_id),
            main_category=str(main_category),
            sub_category=str(sub_category),
            lookback_days=14,
            limit=5,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    topic_label = str(skill.get("title") or skill.get("skill_id") or "Skill practice")
    if sub_category:
        topic_label = f"{topic_label}: {sub_category}"
    elif main_category:
        topic_label = f"{topic_label}: {main_category}"

    try:
        session = asyncio.run(
            start_translation_session_webapp(
                user_id=int(user_id),
                username=username,
                topic=topic_label,
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка запуска прокачки навыка: {exc}"}), 500

    recommended_video = _pick_today_recommended_video(
        str(main_category or ""),
        str(sub_category or ""),
        target_lang=target_lang,
        billing_user_id=int(user_id),
        billing_source_lang=source_lang,
        billing_target_lang=target_lang,
    )
    return jsonify(
        {
            "ok": True,
            "skill": {
                "skill_id": skill.get("skill_id"),
                "title": skill.get("title"),
                "category": skill.get("category"),
            },
            "practice": {
                "task_type": "translation",
                "topic_label": topic_label,
                "level": level,
                "main_category": main_category,
                "sub_category": sub_category,
                "examples": examples[:5],
                "video_url": (recommended_video or {}).get("video_url"),
                "video_id": (recommended_video or {}).get("video_id"),
                "video_title": (recommended_video or {}).get("title"),
            },
            **session,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/dictionary/collocations", methods=["POST"])
def get_webapp_dictionary_collocations():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    word = (payload.get("word") or "").strip()
    direction = (payload.get("direction") or "").strip() or None
    translation = (payload.get("translation") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not word:
        return jsonify({"error": "word обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            if not direction:
                is_ru = any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in word)
                direction = "ru-de" if is_ru else "de-ru"
            result = asyncio.run(run_dictionary_collocations(direction, word, translation))
        else:
            if not direction:
                direction = f"{source_lang}-{target_lang}"
            reverse_direction = f"{target_lang}-{source_lang}"
            is_reverse = direction == reverse_direction or direction == "de-ru"
            if is_reverse:
                word_source = translation or word
                word_target = word
            else:
                word_source = word
                word_target = translation
            result = asyncio.run(
                run_dictionary_collocations_multilang(
                    source_lang=source_lang,
                    target_lang=target_lang,
                    word_source=word_source,
                    word_target=word_target,
                )
            )
    except Exception as exc:
        return jsonify({"error": f"Ошибка генерации связок: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "items": result.get("items", []),
            "direction": direction,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


def _wrap_text(text: str, max_chars: int = 80) -> list[str]:
    if not text:
        return []
    words = text.split()
    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


@app.route("/api/webapp/dictionary/export/pdf", methods=["POST"])
def export_webapp_dictionary_pdf():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    folder_mode = payload.get("folder_mode", "all")
    folder_id = payload.get("folder_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        items = get_webapp_dictionary_entries(
            user_id=user_id,
            limit=500,
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения словаря: {exc}"}), 500

    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib import colors

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    # Use Unicode-capable font for Cyrillic/Umlauts
    font_name = "DejaVuSans"
    font_bold = "DejaVuSans-Bold"
    font_paths = [
        os.path.join(os.path.dirname(__file__), "assets", "fonts", "DejaVuSans.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    font_bold_paths = [
        os.path.join(os.path.dirname(__file__), "assets", "fonts", "DejaVuSans-Bold.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont(font_name, fp))
            except Exception:
                font_name = "Helvetica"
            break
    else:
        font_name = "Helvetica"
    for fp in font_bold_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont(font_bold, fp))
            except Exception:
                font_bold = "Helvetica-Bold"
            break
    else:
        font_bold = "Helvetica-Bold"
    width, height = A4
    x = 40
    y = height - 40
    line_height = 14
    title_size = 16
    word_size = 16
    normal_size = 11

    pdf.setFont(font_name, title_size)
    title = "Словарь"
    pdf.drawString(x, y, title)
    y -= 2 * line_height

    pdf.setFont(font_name, normal_size)
    for item in items:
        response_json = item.get("response_json") or {}
        word = item.get("word_ru") or response_json.get("word_ru") or item.get("word_de") or response_json.get("word_de") or "—"
        translation = (
            item.get("translation_de")
            or response_json.get("translation_de")
            or item.get("translation_ru")
            or response_json.get("translation_ru")
            or "—"
        )
        created_at = item.get("created_at") or ""
        examples = response_json.get("usage_examples") or []
        if isinstance(examples, str):
            examples = [examples]

        lines = [
            f"Перевод: {translation}",
            f"Дата: {created_at}",
        ]
        if examples:
            lines.append("Примеры:")
            for example in examples[:3]:
                lines.extend([f"- {line}" for line in _wrap_text(example, 90)])

        # Word line: larger, bold, colored
        if y < 80:
            pdf.showPage()
            pdf.setFont(font_name, normal_size)
            pdf.setFillColor(colors.black)
            y = height - 40
        pdf.setFont(font_bold, word_size)
        pdf.setFillColorRGB(0.12, 0.25, 0.65)
        pdf.drawString(x, y, word)
        pdf.setFillColor(colors.black)
        y -= line_height * 1.4

        for line in lines:
            if y < 60:
                pdf.showPage()
                pdf.setFont(font_name, normal_size)
                pdf.setFillColor(colors.black)
                y = height - 40
            pdf.setFont(font_name, normal_size)
            pdf.drawString(x, y, line)
            y -= line_height

        y -= line_height

    pdf.save()
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name="dictionary.pdf",
        mimetype="application/pdf",
    )


@app.route("/api/webapp/dictionary/save", methods=["POST"])
def save_webapp_dictionary_entry():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    word_ru = (payload.get("word_ru") or "").strip()
    word_de = (payload.get("word_de") or "").strip()
    translation_de = (payload.get("translation_de") or "").strip()
    translation_ru = (payload.get("translation_ru") or "").strip()
    source_text = (payload.get("source_text") or "").strip()
    target_text = (payload.get("target_text") or "").strip()
    response_json = payload.get("response_json") or {}
    folder_id = payload.get("folder_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not word_ru and not word_de and not source_text:
        return jsonify({"error": "word_ru или word_de или source_text обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    if source_text and not word_ru and source_lang != "de":
        word_ru = source_text
    if source_text and not word_de and source_lang == "de":
        word_de = source_text
    if target_text and not translation_de and target_lang == "de":
        translation_de = target_text
    if target_text and not translation_ru and target_lang != "de":
        translation_ru = target_text

    if folder_id is None:
        try:
            default_folder = get_or_create_dictionary_folder(
                user_id=user_id,
                name="GENERAL",
                color="#7d8590",
                icon="📁",
            )
            folder_id = default_folder.get("id")
        except Exception:
            folder_id = None

    try:
        resolved_word_ru = word_ru or response_json.get("word_ru")
        resolved_word_de = word_de or response_json.get("word_de")
        if isinstance(response_json, dict):
            response_json = dict(response_json)
            response_json.setdefault("source_text", source_text or word_ru or word_de or "")
            response_json.setdefault("target_text", target_text or translation_de or translation_ru or word_de or "")
            response_json.setdefault("source_lang", source_lang)
            response_json.setdefault("target_lang", target_lang)
            response_json.setdefault(
                "language_pair",
                _build_language_pair_payload(source_lang, target_lang),
            )
        save_webapp_dictionary_query(
            user_id=user_id,
            word_ru=resolved_word_ru if resolved_word_ru else None,
            translation_de=translation_de or response_json.get("translation_de"),
            word_de=resolved_word_de if resolved_word_de else None,
            translation_ru=translation_ru or response_json.get("translation_ru"),
            response_json=response_json,
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения словаря: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/mobile/dictionary/save", methods=["POST"])
def save_mobile_dictionary_entry():
    user_id, _username, error = _get_mobile_authenticated_user()
    if error:
        return error

    payload = request.get_json(silent=True) or {}
    word_ru = (payload.get("word_ru") or "").strip()
    word_de = (payload.get("word_de") or "").strip()
    translation_de = (payload.get("translation_de") or "").strip()
    translation_ru = (payload.get("translation_ru") or "").strip()
    source_text = (payload.get("source_text") or "").strip()
    target_text = (payload.get("target_text") or "").strip()
    response_json = payload.get("response_json") or {}
    folder_id = payload.get("folder_id")
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    if not word_ru and not word_de and not source_text and not isinstance(response_json, dict):
        return jsonify({"error": "word_ru/word_de/source_text или response_json обязателен"}), 400
    if source_text and not word_ru and source_lang != "de":
        word_ru = source_text
    if source_text and not word_de and source_lang == "de":
        word_de = source_text
    if target_text and not translation_de and target_lang == "de":
        translation_de = target_text
    if target_text and not translation_ru and target_lang != "de":
        translation_ru = target_text

    if folder_id is None:
        try:
            default_folder = get_or_create_dictionary_folder(
                user_id=user_id,
                name="GENERAL",
                color="#7d8590",
                icon="📁",
            )
            folder_id = default_folder.get("id")
        except Exception:
            folder_id = None

    try:
        resolved_word_ru = word_ru or (response_json.get("word_ru") if isinstance(response_json, dict) else None)
        resolved_word_de = word_de or (response_json.get("word_de") if isinstance(response_json, dict) else None)
        resolved_translation_de = translation_de or (response_json.get("translation_de") if isinstance(response_json, dict) else None)
        resolved_translation_ru = translation_ru or (response_json.get("translation_ru") if isinstance(response_json, dict) else None)
        if isinstance(response_json, dict):
            response_json = dict(response_json)
            response_json.setdefault("source_text", source_text or word_ru or word_de or "")
            response_json.setdefault("target_text", target_text or translation_de or translation_ru or word_de or "")
            response_json.setdefault("source_lang", source_lang)
            response_json.setdefault("target_lang", target_lang)
            response_json.setdefault(
                "language_pair",
                _build_language_pair_payload(source_lang, target_lang),
            )

        save_webapp_dictionary_query(
            user_id=user_id,
            word_ru=resolved_word_ru if resolved_word_ru else None,
            translation_de=resolved_translation_de,
            word_de=resolved_word_de if resolved_word_de else None,
            translation_ru=resolved_translation_ru,
            response_json=response_json if isinstance(response_json, dict) else {},
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения словаря: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "user_id": user_id,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/dictionary/cards", methods=["POST"])
def get_webapp_dictionary_cards():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = payload.get("limit", 100)
    folder_mode = payload.get("folder_mode", "all")
    folder_id = payload.get("folder_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        items = get_webapp_dictionary_entries(
            user_id=user_id,
            limit=int(limit),
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения словаря: {exc}"}), 500

    direction = f"{source_lang}-{target_lang}"
    decorated_items = [
        _decorate_dictionary_item(
            item if isinstance(item, dict) else {},
            source_lang=source_lang,
            target_lang=target_lang,
            direction=direction,
        )
        for item in items
    ]
    return jsonify(
        {
            "ok": True,
            "items": decorated_items,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/dictionary/folders", methods=["POST"])
def list_webapp_dictionary_folders():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        items = get_dictionary_folders(user_id=user_id)
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения папок: {exc}"}), 500

    return jsonify({"ok": True, "items": items})


@app.route("/api/webapp/dictionary/folders/create", methods=["POST"])
def create_webapp_dictionary_folder():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    name = (payload.get("name") or "").strip()
    color = (payload.get("color") or "").strip()
    icon = (payload.get("icon") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not name:
        return jsonify({"error": "name обязателен"}), 400
    if not color:
        return jsonify({"error": "color обязателен"}), 400
    if not icon:
        return jsonify({"error": "icon обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        folder = create_dictionary_folder(user_id=user_id, name=name, color=color, icon=icon)
    except Exception as exc:
        return jsonify({"error": f"Ошибка создания папки: {exc}"}), 500

    return jsonify({"ok": True, "item": folder})


@app.route("/api/webapp/flashcards/set", methods=["POST"])
def get_webapp_flashcard_set():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    set_size = int(payload.get("set_size", 15))
    wrong_size = int(payload.get("wrong_size", 5))
    folder_mode = payload.get("folder_mode", "all")
    folder_id = payload.get("folder_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        items = get_flashcard_set(
            user_id=user_id,
            set_size=set_size,
            wrong_size=wrong_size,
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения карточек: {exc}"}), 500

    direction = f"{source_lang}-{target_lang}"
    decorated_items = [
        _decorate_dictionary_item(
            item if isinstance(item, dict) else {},
            source_lang=source_lang,
            target_lang=target_lang,
            direction=direction,
        )
        for item in items
    ]
    return jsonify(
        {
            "ok": True,
            "items": decorated_items,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/flashcards/answer", methods=["POST"])
def record_webapp_flashcard_answer():
    # Legacy endpoint is intentionally left as a no-op.
    # Training answers must be sent to /api/cards/review so FSRS stays the single source of truth.
    return jsonify(
        {
            "ok": True,
            "deprecated": True,
            "message": "Use /api/cards/review for training answers.",
        }
    )


@app.route("/api/cards/next", methods=["GET"])
def get_next_srs_card():
    init_data = request.args.get("initData") or request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    user_id, _username = _extract_webapp_user_from_init_data(init_data)
    if not user_id:
        return jsonify({"error": "initData не прошёл проверку или user_id отсутствует"}), 401
    if not _is_webapp_user_allowed(int(user_id)):
        return jsonify({"error": "Доступ закрыт. Ожидайте одобрения администратора."}), 403

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    now_utc = datetime.now(timezone.utc)

    payload_next = _build_next_srs_payload(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
        now_utc=now_utc,
    )

    return jsonify(
        {
            "ok": True,
            **payload_next,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/cards/review", methods=["POST"])
def review_srs_card():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    card_id = payload.get("card_id")
    rating_raw = payload.get("rating")
    response_ms = payload.get("response_ms")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not card_id:
        return jsonify({"error": "card_id обязателен"}), 400
    if rating_raw is None:
        return jsonify({"error": "rating обязателен"}), 400

    user_id, _username = _extract_webapp_user_from_init_data(init_data)
    if not user_id:
        return jsonify({"error": "initData не прошёл проверку или user_id отсутствует"}), 401
    if not _is_webapp_user_allowed(int(user_id)):
        return jsonify({"error": "Доступ закрыт. Ожидайте одобрения администратора."}), 403
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    reviewed_at = datetime.now(timezone.utc)
    card_id = int(card_id)

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                card = get_dictionary_entry_for_user(user_id=user_id, card_id=card_id, cursor=cursor)
                if not card:
                    return jsonify({"error": "Карточка не найдена"}), 404

                current_state = get_card_srs_state(user_id=user_id, card_id=card_id, cursor=cursor)
                if not current_state:
                    current_state = {
                        "status": "new",
                        "due_at": reviewed_at,
                        "last_review_at": None,
                        "interval_days": 0,
                        "reps": 0,
                        "lapses": 0,
                        "stability": 0.0,
                        "difficulty": 0.0,
                    }

                before_due = current_state.get("due_at")
                before_stability = float(current_state.get("stability") or 0.0)
                before_difficulty = float(current_state.get("difficulty") or 0.0)

                scheduled, canonical_rating = schedule_review(
                    current_state=current_state,
                    rating=rating_raw,
                    reviewed_at=reviewed_at,
                )

                persisted = upsert_card_srs_state(
                    user_id=user_id,
                    card_id=card_id,
                    status=scheduled.status,
                    due_at=scheduled.due_at,
                    last_review_at=scheduled.last_review_at,
                    interval_days=scheduled.interval_days,
                    reps=scheduled.reps,
                    lapses=scheduled.lapses,
                    stability=scheduled.stability,
                    difficulty=scheduled.difficulty,
                    cursor=cursor,
                )

                insert_card_review_log(
                    user_id=user_id,
                    card_id=card_id,
                    reviewed_at=reviewed_at,
                    rating=canonical_rating,
                    response_ms=int(response_ms) if response_ms is not None else None,
                    scheduled_due_before=before_due,
                    scheduled_due_after=scheduled.due_at,
                    stability_before=before_stability,
                    difficulty_before=before_difficulty,
                    stability_after=scheduled.stability,
                    difficulty_after=scheduled.difficulty,
                    interval_days_after=scheduled.interval_days,
                    cursor=cursor,
                )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка review: {exc}"}), 500

    interval_days = int(persisted.get("interval_days") or 0)
    due_at = persisted.get("due_at")
    payload_next = _build_next_srs_payload(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
        now_utc=datetime.now(timezone.utc),
    )
    return jsonify(
        {
            "ok": True,
            "next_due_at": due_at.isoformat() if hasattr(due_at, "isoformat") else None,
            "interval_days": interval_days,
            "status": persisted.get("status") or "new",
            "stability": float(persisted.get("stability") or 0.0),
            "difficulty": float(persisted.get("difficulty") or 0.0),
            "is_mature": interval_days >= MATURE_INTERVAL_DAYS,
            "message": "Review saved",
            "next": payload_next,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/flashcards/feel", methods=["POST"])
def get_flashcard_feel():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    entry_id = payload.get("entry_id")
    word_ru = (payload.get("word_ru") or "").strip()
    word_de = (payload.get("word_de") or "").strip()
    source_text_hint = (payload.get("source_text") or "").strip()
    target_text_hint = (payload.get("target_text") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not entry_id:
        return jsonify({"error": "entry_id обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    response_json = None
    entry = None
    try:
        entry = get_dictionary_entry_by_id(int(entry_id))
        if entry:
            response_json = entry.get("response_json")
            if isinstance(response_json, str):
                try:
                    response_json = json.loads(response_json)
                except Exception:
                    response_json = None
    except Exception:
        response_json = None

    if response_json and response_json.get("feel_explanation"):
        return jsonify({"ok": True, "feel_explanation": response_json.get("feel_explanation")})

    if not word_ru:
        word_ru = (
            (response_json or {}).get("word_ru")
            or (entry or {}).get("word_ru")
            or ""
        ).strip()

    if not word_de:
        word_de = (
            (response_json or {}).get("word_de")
            or (entry or {}).get("word_de")
            or (response_json or {}).get("translation_de")
            or (entry or {}).get("translation_de")
            or ""
        ).strip()

    source_text, target_text = _resolve_entry_texts_for_pair(
        entry=entry,
        response_json=response_json if isinstance(response_json, dict) else {},
        source_lang=source_lang,
        target_lang=target_lang,
        source_text_hint=source_text_hint or word_ru,
        target_text_hint=target_text_hint or word_de,
    )
    if not source_text:
        return jsonify({"error": "Нужно передать source_text/word_ru/word_de"}), 400

    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            feel_text = asyncio.run(run_feel_word(source_text, target_text))
        else:
            feel_text = asyncio.run(
                run_feel_word_multilang(
                    source_text=source_text,
                    target_text=target_text,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
    except Exception as exc:
        return jsonify({"error": f"Ошибка feel: {exc}"}), 500

    if not response_json:
        response_json = {}
    response_json["feel_explanation"] = feel_text
    try:
        update_webapp_dictionary_entry(int(entry_id), response_json)
    except Exception:
        pass

    return jsonify(
        {
            "ok": True,
            "feel_explanation": feel_text,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/flashcards/feel/feedback", methods=["POST"])
def set_flashcard_feel_feedback():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    entry_id = payload.get("entry_id")
    liked = payload.get("liked")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not entry_id:
        return jsonify({"error": "entry_id обязателен"}), 400
    if liked is None:
        return jsonify({"error": "liked обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    try:
        entry = get_dictionary_entry_by_id(int(entry_id))
    except Exception:
        entry = None

    if not entry:
        return jsonify({"error": "entry не найден"}), 404

    response_json = entry.get("response_json")
    if isinstance(response_json, str):
        try:
            response_json = json.loads(response_json)
        except Exception:
            response_json = {}
    if not isinstance(response_json, dict):
        response_json = {}

    if bool(liked):
        response_json["feel_feedback"] = "like"
    else:
        response_json.pop("feel_explanation", None)
        response_json["feel_feedback"] = "dislike"

    try:
        update_webapp_dictionary_entry(int(entry_id), response_json)
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения feedback: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "liked": bool(liked),
            "kept": bool(liked),
        }
    )


@app.route("/api/webapp/tts", methods=["POST"])
def webapp_tts():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    text = (payload.get("text") or "").strip()
    language = (payload.get("language") or "de-DE").strip()
    voice = (payload.get("voice") or "de-DE-Neural2-C").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not text:
        return jsonify({"error": "text обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    speaking_rate = 0.95
    normalized = _normalize_utterance_text(text)
    cache_key = _tts_cache_key(language, voice, speaking_rate, normalized)

    try:
        cached_audio = get_tts_audio_cache(cache_key)
        if cached_audio:
            return send_file(
                BytesIO(cached_audio),
                mimetype="audio/mpeg",
                as_attachment=False,
                download_name="tts.mp3",
            )

        try:
            from google.cloud import texttospeech
        except Exception:
            return jsonify({"error": "Google TTS не установлен на сервере"}), 500
        key_path = prepare_google_creds_for_tts()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        tts_client = texttospeech.TextToSpeechClient()
        input_data = texttospeech.SynthesisInput(text=normalized)
        voice_params = texttospeech.VoiceSelectionParams(language_code=language, name=voice)
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            speaking_rate=speaking_rate,
        )
        response = tts_client.synthesize_speech(
            input=input_data,
            voice=voice_params,
            audio_config=audio_config,
        )
        try:
            upsert_tts_audio_cache(
                cache_key=cache_key,
                language=language,
                voice=voice,
                speed=speaking_rate,
                source_text=normalized,
                audio_mp3=response.audio_content,
            )
        except Exception as exc:
            logging.warning("Failed to persist webapp TTS cache: %s", exc)
        return send_file(
            BytesIO(response.audio_content),
            mimetype="audio/mpeg",
            as_attachment=False,
            download_name="tts.mp3",
        )
    except Exception as exc:
        return jsonify({"error": f"TTS error: {exc}"}), 500


@app.route("/api/webapp/flashcards/enrich", methods=["POST"])
def enrich_flashcard_entry():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    entry_id = payload.get("entry_id")
    word_ru = (payload.get("word_ru") or "").strip()
    word_de = (payload.get("word_de") or "").strip()
    source_text_hint = (payload.get("source_text") or "").strip()
    target_text_hint = (payload.get("target_text") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not entry_id:
        return jsonify({"error": "entry_id обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    entry = get_dictionary_entry_by_id(int(entry_id))
    response_json = entry.get("response_json") if entry else None
    if isinstance(response_json, str):
        try:
            response_json = json.loads(response_json)
        except Exception:
            response_json = None

    source_text, target_text = _resolve_entry_texts_for_pair(
        entry=entry,
        response_json=response_json if isinstance(response_json, dict) else {},
        source_lang=source_lang,
        target_lang=target_lang,
        source_text_hint=source_text_hint or word_ru,
        target_text_hint=target_text_hint or word_de,
    )

    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            enrich = asyncio.run(run_enrich_word(source_text, target_text))
        else:
            enrich = asyncio.run(
                run_enrich_word_multilang(
                    source_text=source_text,
                    target_text=target_text,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            )
    except Exception as exc:
        return jsonify({"error": f"Ошибка enrich: {exc}"}), 500

    if not response_json:
        response_json = {}
    if isinstance(enrich, dict):
        enrich_data = dict(enrich)
        prefixes = enrich_data.get("prefixes")
        if isinstance(prefixes, list):
            normalized_prefixes = []
            for item in prefixes:
                if not isinstance(item, dict):
                    continue
                normalized = dict(item)
                if normalized.get("translation_target") and not normalized.get("translation_de"):
                    normalized["translation_de"] = normalized.get("translation_target")
                if normalized.get("translation_source") and not normalized.get("translation_ru"):
                    normalized["translation_ru"] = normalized.get("translation_source")
                if normalized.get("example_target") and not normalized.get("example_de"):
                    normalized["example_de"] = normalized.get("example_target")
                normalized_prefixes.append(normalized)
            enrich_data["prefixes"] = normalized_prefixes

        response_json.update(enrich_data)
        response_json["source_text"] = source_text
        response_json["target_text"] = target_text
        response_json["source_lang"] = source_lang
        response_json["target_lang"] = target_lang

    try:
        update_webapp_dictionary_entry(int(entry_id), response_json)
    except Exception:
        pass

    return jsonify(
        {
            "ok": True,
            "response_json": response_json,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/youtube/transcript", methods=["POST"])
def get_youtube_transcript():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    video_id = (payload.get("videoId") or "").strip()
    lang = (payload.get("lang") or "").strip() or None

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not video_id:
        return jsonify({"error": "videoId обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    proxy_allowed = has_youtube_proxy_subtitles_access(int(user_id))
    subtitle_target_lang = _normalize_short_lang_code(source_lang, fallback="ru")

    now = time.time()

    cached = _yt_transcript_cache.get(video_id)
    if cached and now - cached.get("ts", 0) < _YT_CACHE_TTL:
        data = cached.get("data") or {}
        visible_translations = _extract_youtube_translations_for_target(
            data.get("translations") or {},
            subtitle_target_lang,
        )
        return jsonify(
            {
                "ok": True,
                "items": data.get("items", []),
                "language": data.get("language"),
                "is_generated": data.get("is_generated"),
                "translations": visible_translations,
                "translation_lang": subtitle_target_lang,
                "source": data.get("source"),
                "cached": True,
                "proxy_allowed": bool(proxy_allowed),
            }
        )
    cached_db = None
    try:
        cached_db = get_youtube_transcript_cache(video_id)
    except Exception:
        cached_db = None
    if cached_db and cached_db.get("items"):
        data = {
            "items": cached_db.get("items", []),
            "language": cached_db.get("language"),
            "is_generated": cached_db.get("is_generated"),
            "translations": cached_db.get("translations") or {},
        }
        _yt_transcript_cache[video_id] = {"ts": now, "data": data}
        visible_translations = _extract_youtube_translations_for_target(
            data.get("translations") or {},
            subtitle_target_lang,
        )
        return jsonify(
            {
                "ok": True,
                "items": data.get("items", []),
                "language": data.get("language"),
                "is_generated": data.get("is_generated"),
                "translations": visible_translations,
                "translation_lang": subtitle_target_lang,
                "source": data.get("source"),
                "cached_db": True,
                "proxy_allowed": bool(proxy_allowed),
            }
        )

    err = _yt_transcript_errors.get(video_id)
    if err and now - err.get("ts", 0) < _YT_ERROR_TTL:
        return jsonify({"error": err.get("error", "Субтитры временно недоступны")}), 429

    try:
        data = _fetch_youtube_transcript(video_id, lang=lang, allow_proxy=proxy_allowed)
    except Exception as exc:
        logging.warning("YouTube transcript error for %s: %s", video_id, exc)
        _yt_transcript_errors[video_id] = {"ts": now, "error": str(exc)}
        return jsonify({"error": f"Не удалось получить субтитры: {exc}"}), 404

    _billing_log_event_safe(
        user_id=int(user_id),
        action_type="youtube_transcript_fetch",
        provider="youtube_proxy",
        units_type="requests",
        units_value=1.0,
        source_lang=source_lang,
        target_lang=target_lang,
        idempotency_seed=f"yt_fetch:{user_id}:{video_id}:{int(now)}",
        status="estimated",
        metadata={
            "video_id": video_id,
            "requested_lang": lang,
            "detected_language": data.get("language"),
            "source": data.get("source"),
            "proxy_allowed": bool(proxy_allowed),
        },
    )

    try:
        upsert_youtube_transcript_cache(
            video_id,
            data.get("items", []),
            data.get("language"),
            data.get("is_generated"),
            data.get("translations"),
        )
    except Exception:
        pass
    _yt_transcript_cache[video_id] = {"ts": now, "data": data}

    return jsonify(
        {
            "ok": True,
            "items": data.get("items", []),
            "language": data.get("language"),
            "is_generated": data.get("is_generated"),
            "translations": _extract_youtube_translations_for_target(
                data.get("translations") or {},
                subtitle_target_lang,
            ),
            "translation_lang": subtitle_target_lang,
            "source": data.get("source"),
            "proxy_allowed": bool(proxy_allowed),
        }
    )


@app.route("/api/webapp/youtube/catalog", methods=["POST"])
def get_youtube_catalog():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = int(payload.get("limit", 60))

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    learning_code = _normalize_short_lang_code(target_lang, fallback="de")

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id,
                           language,
                           is_generated,
                           updated_at,
                           jsonb_array_length(items) AS items_count
                    FROM bt_3_youtube_transcripts
                    ORDER BY updated_at DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
    except Exception as exc:
        return jsonify({"error": f"Ошибка каталога: {exc}"}), 500

    items = []
    for video_id, language, is_generated, updated_at, items_count in rows:
        language_code = _normalize_short_lang_code(language, fallback="")
        if learning_code and language_code and language_code != learning_code:
            continue
        if learning_code and not language_code:
            continue
        oembed = _get_youtube_oembed(video_id)
        title = oembed.get("title") or video_id
        author = oembed.get("author_name") or ""
        thumbnail = oembed.get("thumbnail_url") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
        items.append(
            {
                "video_id": video_id,
                "title": title,
                "author": author,
                "thumbnail": thumbnail,
                "language": language,
                "is_generated": is_generated,
                "items_count": items_count,
                "updated_at": updated_at.isoformat() if updated_at else None,
            }
        )

    return jsonify({"ok": True, "items": items})


@app.route("/api/webapp/youtube/manual", methods=["POST"])
def save_manual_youtube_transcript():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    video_id = (payload.get("videoId") or "").strip()
    items = payload.get("items") or []
    language = (payload.get("language") or "").strip() or None

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not video_id:
        return jsonify({"error": "videoId обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items обязателен"}), 400

    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = (item.get("text") or "").strip()
        if not text:
            continue
        start = item.get("start")
        duration = item.get("duration")
        normalized.append(
            {
                "text": text,
                "start": float(start) if start is not None else None,
                "duration": float(duration) if duration is not None else None,
            }
        )
    if not normalized:
        return jsonify({"error": "items пустой"}), 400

    try:
        upsert_youtube_transcript_cache(
            video_id,
            normalized,
            language,
            False,
            {},
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения: {exc}"}), 500

    _yt_transcript_cache[video_id] = {"ts": time.time(), "data": {
        "items": normalized,
        "language": language,
        "is_generated": False,
        "translations": {},
        "source": "manual",
    }}

    return jsonify({"ok": True})


@app.route("/api/webapp/youtube/translate", methods=["POST"])
def translate_youtube_subtitles():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    video_id = (payload.get("videoId") or "").strip()
    start_index = payload.get("start_index")
    lines = payload.get("lines") or []

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not video_id:
        return jsonify({"error": "videoId обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    if not isinstance(lines, list):
        return jsonify({"error": "lines должен быть массивом"}), 400
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    subtitle_target_lang = _normalize_short_lang_code(source_lang, fallback="ru")

    try:
        start_index = int(start_index) if start_index is not None else 0
    except Exception:
        start_index = 0

    lines = [str(line) for line in lines][:50]
    if not lines:
        return jsonify({"ok": True, "translations": []})

    translations_map = {}
    try:
        cached_db = get_youtube_transcript_cache(video_id) or {}
        translations_map = cached_db.get("translations") or {}
    except Exception:
        translations_map = {}

    results = []
    missing_lines = []
    missing_indices = []
    for offset, line in enumerate(lines):
        line_clean = str(line).strip()
        idx_int = start_index + offset
        idx = str(idx_int)
        lang_key = _youtube_translation_key(subtitle_target_lang, idx)
        cached = translations_map.get(lang_key)
        if cached is None and subtitle_target_lang == "ru":
            cached = translations_map.get(idx)
        if cached:
            results.append(cached)
        elif not line_clean:
            results.append("")
        else:
            results.append("")
            missing_lines.append(line_clean)
            missing_indices.append(idx_int)

    if missing_lines:
        detected_source_lang = "de"
        try:
            cached_db = get_youtube_transcript_cache(video_id) or {}
            detected_source_lang = _normalize_short_lang_code(cached_db.get("language"), fallback="de")
        except Exception:
            detected_source_lang = "de"
        try:
            if subtitle_target_lang == "ru" and detected_source_lang == "de":
                translated = asyncio.run(run_translate_subtitles_ru(missing_lines))
            else:
                translated = asyncio.run(
                    run_translate_subtitles_multilang(
                        lines=missing_lines,
                        source_lang=detected_source_lang,
                        target_lang=subtitle_target_lang,
                    )
                )
        except Exception as exc:
            return jsonify({"error": f"translation error: {exc}"}), 500
        usage_subtitles = get_last_llm_usage(reset=True)
        _billing_log_openai_usage(
            user_id=int(user_id),
            action_type="youtube_subtitles_translate",
            source_lang=source_lang,
            target_lang=target_lang,
            usage=usage_subtitles,
            seed=f"yt_subtitles:{user_id}:{video_id}:{start_index}:{len(missing_lines)}:{time.time_ns()}",
            metadata={
                "video_id": video_id,
                "source_subtitle_lang": detected_source_lang,
                "target_subtitle_lang": subtitle_target_lang,
                "lines_count": len(missing_lines),
            },
        )
        update_map = {}
        for idx_int, text in zip(missing_indices, translated):
            idx = str(idx_int)
            update_map[_youtube_translation_key(subtitle_target_lang, idx)] = text
            if subtitle_target_lang == "ru":
                update_map[idx] = text
        translations_map.update(update_map)
        try:
            upsert_youtube_translations(video_id, update_map)
        except Exception:
            pass
        for idx_int in missing_indices:
            pos = idx_int - start_index
            idx = str(idx_int)
            if 0 <= pos < len(results):
                results[pos] = update_map.get(
                    _youtube_translation_key(subtitle_target_lang, idx),
                    "",
                )
        cached = _yt_transcript_cache.get(video_id)
        if cached and cached.get("data"):
            cached["data"]["translations"] = translations_map

    return jsonify({"ok": True, "translations": results, "translation_lang": subtitle_target_lang})


@app.route("/api/webapp/reader/ingest", methods=["POST"])
def ingest_reader_content():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    input_text = str(payload.get("text") or "").strip()
    input_url = str(payload.get("url") or "").strip()
    file_name = str(payload.get("file_name") or "").strip()
    file_mime = str(payload.get("file_mime") or "").strip().lower()
    file_content_b64 = str(payload.get("file_content_base64") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not input_text and not input_url and not file_content_b64:
        return jsonify({"error": "Нужно передать text, url или файл"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    normalized_text = ""
    content_pages: list[dict] = []
    source_type = "text"
    resolved_url = None

    try:
        if file_content_b64:
            try:
                raw_bytes = base64.b64decode(file_content_b64, validate=True)
            except Exception as exc:
                raise ValueError(f"Некорректный файл: {exc}") from exc
            lower_name = file_name.lower()
            is_pdf = (
                file_mime == "application/pdf"
                or lower_name.endswith(".pdf")
            )
            if is_pdf:
                normalized_text, content_pages = _extract_pdf_content_from_bytes(raw_bytes)
                source_type = "pdf"
            else:
                decoded_text = raw_bytes.decode("utf-8", errors="ignore")
                normalized_text = _normalize_reader_text(decoded_text)
                source_type = "file"
        elif input_text:
            normalized_text = _normalize_reader_text(input_text)
            source_type = "text"
        else:
            normalized_text, source_type, content_pages = _fetch_reader_text_from_url(input_url)
            resolved_url = input_url
    except requests.RequestException as exc:
        return jsonify({"error": f"Не удалось загрузить ссылку: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Ошибка обработки читалки: {exc}"}), 500

    if not normalized_text:
        return jsonify({"error": "Не удалось извлечь текст"}), 422

    title = _infer_reader_title(
        input_text=normalized_text,
        input_url=resolved_url or input_url or file_name,
        source_type=source_type,
    )
    try:
        document = upsert_reader_library_document(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
            title=title,
            source_type=source_type,
            source_url=resolved_url or input_url or None,
            content_text=normalized_text,
            content_pages=content_pages,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения в библиотеку: {exc}"}), 500

    detected_lang = _detect_reader_language(normalized_text, fallback=target_lang)
    return jsonify(
        {
            "ok": True,
            "text": normalized_text,
            "source_type": source_type,
            "source_url": resolved_url,
            "title": title,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
            "detected_language": detected_lang,
            "detected_language_label": _language_label(detected_lang),
            "document": document,
        }
    )


@app.route("/api/webapp/reader/library", methods=["POST"])
def reader_library_list():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = int(payload.get("limit", 100))
    include_archived = bool(payload.get("include_archived"))

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        items = list_reader_library_documents(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
            limit=limit,
            include_archived=include_archived,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка загрузки библиотеки: {exc}"}), 500
    return jsonify(
        {
            "ok": True,
            "items": items,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/reader/library/open", methods=["POST"])
def reader_library_open():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        doc = get_reader_library_document(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка открытия книги: {exc}"}), 500
    if not doc:
        return jsonify({"error": "Книга не найдена"}), 404
    detected_lang = _detect_reader_language(str(doc.get("content_text") or ""), fallback=target_lang)
    return jsonify(
        {
            "ok": True,
            "document": doc,
            "text": str(doc.get("content_text") or ""),
            "title": str(doc.get("title") or "Untitled"),
            "source_type": str(doc.get("source_type") or "text"),
            "source_url": doc.get("source_url"),
            "detected_language": detected_lang,
            "detected_language_label": _language_label(detected_lang),
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/reader/library/state", methods=["POST"])
def reader_library_state():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")
    progress_percent = payload.get("progress_percent")
    bookmark_percent = payload.get("bookmark_percent")
    reading_mode = payload.get("reading_mode")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        progress_val = None if progress_percent is None else float(progress_percent)
        bookmark_val = None if bookmark_percent is None else float(bookmark_percent)
    except Exception:
        return jsonify({"error": "progress_percent/bookmark_percent должны быть числами"}), 400

    try:
        doc = update_reader_library_state(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
            progress_percent=progress_val,
            bookmark_percent=bookmark_val,
            reading_mode=str(reading_mode or "").strip().lower() or None,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка обновления прогресса чтения: {exc}"}), 500
    if not doc:
        return jsonify({"error": "Книга не найдена"}), 404
    return jsonify({"ok": True, "document": doc})


@app.route("/api/webapp/reader/library/rename", methods=["POST"])
def reader_library_rename():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")
    title = str(payload.get("title") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not title:
        return jsonify({"error": "title обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        doc = rename_reader_library_document(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
            title=title,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка переименования книги: {exc}"}), 500
    if not doc:
        return jsonify({"error": "Книга не найдена"}), 404
    return jsonify({"ok": True, "document": doc})


@app.route("/api/webapp/reader/library/archive", methods=["POST"])
def reader_library_archive():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")
    archived = bool(payload.get("archived", True))

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        doc = archive_reader_library_document(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
            archived=archived,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка архивации книги: {exc}"}), 500
    if not doc:
        return jsonify({"error": "Книга не найдена"}), 404
    return jsonify({"ok": True, "document": doc})


@app.route("/api/webapp/reader/library/delete", methods=["POST"])
def reader_library_delete():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        deleted = delete_reader_library_document(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка удаления книги: {exc}"}), 500
    if not deleted:
        return jsonify({"error": "Книга не найдена"}), 404
    return jsonify({"ok": True, "deleted": True})


@app.route("/api/webapp/reader/audio", methods=["POST"])
def reader_audio_export():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    document_id = payload.get("document_id")
    page_from = payload.get("page_from")
    page_to = payload.get("page_to")
    requested_language = str(payload.get("language") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if document_id is None:
        return jsonify({"error": "document_id обязателен"}), 400
    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401
    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        document = get_reader_library_document(
            user_id=int(user_id),
            document_id=int(document_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка загрузки документа: {exc}"}), 500
    if not document:
        return jsonify({"error": "Книга не найдена"}), 404

    pages = document.get("content_pages") if isinstance(document.get("content_pages"), list) else []
    if pages:
        try:
            start_page = int(page_from) if page_from is not None else 1
            end_page = int(page_to) if page_to is not None else len(pages)
        except Exception:
            return jsonify({"error": "page_from/page_to должны быть числами"}), 400
        start_page = max(1, start_page)
        end_page = max(start_page, min(len(pages), end_page))
        selected = []
        for page in pages:
            try:
                page_no = int(page.get("page_number") or 0)
            except Exception:
                page_no = 0
            if start_page <= page_no <= end_page:
                selected.append(str(page.get("text") or "").strip())
        text_to_read = "\n\n".join([chunk for chunk in selected if chunk])
        filename_suffix = f"p{start_page}-{end_page}"
    else:
        text_to_read = str(document.get("content_text") or "").strip()
        filename_suffix = "full"

    if not text_to_read:
        return jsonify({"error": "В выбранном диапазоне нет текста"}), 422
    language_for_tts = _normalize_short_lang_code(
        requested_language or _detect_reader_language(text_to_read, fallback=target_lang),
        fallback=_normalize_short_lang_code(target_lang, fallback="de"),
    )
    safe_title = re.sub(r"[^A-Za-z0-9._-]+", "_", str(document.get("title") or "reader"))[:60]
    try:
        google_voice = _TTS_VOICES.get(language_for_tts, _TTS_VOICES["de"])
        google_lang_code = _TTS_LANG_CODES.get(language_for_tts, "de-DE")
        mp3_bytes = _synthesize_mp3(
            text_to_read,
            language=google_lang_code,
            voice=google_voice,
            speed=_TTS_SPEED_DEFAULT,
        )
        _billing_log_event_safe(
            user_id=int(user_id),
            action_type="reader_audio_tts",
            provider="google_tts",
            units_type="chars",
            units_value=float(len(text_to_read)),
            source_lang=source_lang,
            target_lang=target_lang,
            idempotency_seed=f"reader_audio:{user_id}:{document_id}:{filename_suffix}:{len(text_to_read)}:{time.time_ns()}",
            status="estimated",
            metadata={
                "document_id": int(document_id),
                "language": language_for_tts,
                "voice": google_voice,
                "format": "mp3",
            },
        )
        return send_file(
            BytesIO(mp3_bytes),
            mimetype="audio/mpeg",
            as_attachment=True,
            download_name=f"{safe_title}_{filename_suffix}.mp3",
        )
    except Exception as google_exc:
        logging.warning("Reader audio Google TTS failed, fallback to offline: %s", google_exc)
        try:
            wav_bytes = _synthesize_offline_audio_wav(text_to_read, language=language_for_tts)
            _billing_log_event_safe(
                user_id=int(user_id),
                action_type="reader_audio_tts_fallback",
                provider="offline_tts",
                units_type="chars",
                units_value=float(len(text_to_read)),
                source_lang=source_lang,
                target_lang=target_lang,
                idempotency_seed=f"reader_audio_fallback:{user_id}:{document_id}:{filename_suffix}:{len(text_to_read)}:{time.time_ns()}",
                status="final",
                metadata={
                    "document_id": int(document_id),
                    "language": language_for_tts,
                    "format": "wav",
                },
            )
            return send_file(
                BytesIO(wav_bytes),
                mimetype="audio/wav",
                as_attachment=True,
                download_name=f"{safe_title}_{filename_suffix}.wav",
            )
        except Exception as offline_exc:
            return jsonify(
                {
                    "error": (
                        f"TTS недоступен. Google: {google_exc}. "
                        f"Offline: {offline_exc}"
                    )
                }
            ), 500


@app.route("/api/webapp/normalize/de", methods=["POST"])
@app.route("/api/webapp/normalize/<lang_code>", methods=["POST"])
def normalize_lookup_text(lang_code: str = "de"):
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    text = (payload.get("text") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not text:
        return jsonify({"error": "text обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        normalized = _normalize_lookup_text(text, lang_code)
    except Exception as exc:
        return jsonify({"error": f"Не удалось нормализовать текст: {exc}"}), 500

    return jsonify({"ok": True, "normalized": normalized})


@app.route("/api/webapp/sentences", methods=["POST"])
def get_webapp_sentences():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = payload.get("limit", 7)

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    sentences = get_pending_daily_sentences(
        user_id=user_id,
        limit=int(limit),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    deduped = _dedupe_sentences(sentences)
    return jsonify(
        {
            "ok": True,
            "items": deduped,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/topics", methods=["GET"])
def get_webapp_topics():
    return jsonify({"ok": True, "items": WEBAPP_TOPICS})


@app.route("/api/webapp/start", methods=["POST"])
def start_webapp_translation():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    topic = (payload.get("topic") or "Random sentences").strip()
    level = (payload.get("level") or "c1").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        result = asyncio.run(
            start_translation_session_webapp(
                user_id=user_id,
                username=username,
                topic=topic if topic else "Random sentences",
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка запуска сессии: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            **result,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/story/history", methods=["POST"])
def get_webapp_story_history():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    limit = int(payload.get("limit", 10))

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    items = get_story_history_webapp(user_id=user_id, limit=limit)
    return jsonify(
        {
            "ok": True,
            "items": items,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/story/start", methods=["POST"])
def start_webapp_story():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    mode = (payload.get("mode") or "new").strip().lower()
    story_type = (payload.get("story_type") or "знаменитая личность").strip()
    difficulty = (payload.get("difficulty") or "средний").strip()
    story_id = payload.get("story_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        result = asyncio.run(
            start_story_session_webapp(
                user_id=user_id,
                username=username,
                mode=mode,
                story_type=story_type,
                difficulty=difficulty,
                story_id=int(story_id) if story_id else None,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка запуска истории: {exc}"}), 500

    if isinstance(result, dict) and result.get("error"):
        return jsonify({"error": result["error"]}), 400

    return jsonify(
        {
            "ok": True,
            **result,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/story/submit", methods=["POST"])
def submit_webapp_story():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    translations = payload.get("translations") or []
    guess = (payload.get("guess") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not translations:
        return jsonify({"error": "translations обязательны"}), 400
    if not guess:
        return jsonify({"error": "guess обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400
    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))

    try:
        result = asyncio.run(
            submit_story_translation_webapp(
                user_id=user_id,
                username=username,
                translations=translations,
                guess=guess,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка истории: {exc}"}), 500

    if isinstance(result, dict) and result.get("error"):
        return jsonify({"error": result["error"]}), 400

    return jsonify(
        {
            "ok": True,
            **result,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/session", methods=["POST"])
def get_webapp_session():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    info = get_active_session_type(user_id=user_id)
    return jsonify({"ok": True, **info})


@app.route("/api/webapp/submit-group", methods=["POST"])
def submit_webapp_group_message():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    translations = payload.get("translations") or []

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not translations:
        return jsonify({"error": "translations обязательны"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    user_name = user_data.get("first_name") or "User"
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    latest_sentences = get_latest_daily_sentences(user_id=user_id, limit=50)
    sentence_map = {
        item["id_for_mistake_table"]: item["sentence"]
        for item in latest_sentences
    }

    lines = [f"WebApp submission от {user_name}" + (f" (@{username})" if username else "")]
    for idx, entry in enumerate(translations, start=1):
        sentence_id = entry.get("id_for_mistake_table")
        translation = (entry.get("translation") or "").strip()
        if not translation:
            continue
        original = sentence_map.get(sentence_id, "—")
        lines.append(f"{idx}. {original}")
        lines.append(f"DE: {translation}")

    if len(lines) == 1:
        return jsonify({"error": "Нет заполненных переводов"}), 400

    summary = build_user_daily_summary(user_id=user_id, username=username)
    if summary:
        lines.append("")
        lines.append(summary)

    try:
        _send_group_message("\n".join(lines))
    except Exception as exc:
        return jsonify({"error": f"Ошибка отправки в группу: {exc}"}), 500

    return jsonify({"ok": True})


def _dispatch_daily_audio(target_date: date) -> dict:
    def _pair_code(source_lang: str | None, target_lang: str | None) -> str:
        src = str(source_lang or "ru").strip().upper() or "RU"
        tgt = str(target_lang or "de").strip().upper() or "DE"
        return f"{src}->{tgt}"

    story_sessions: dict[int, set[str]] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, session_id
                FROM bt_3_story_sessions
                WHERE created_at::date = %s;
                """,
                (target_date,),
            )
            for user_id, session_id in cursor.fetchall():
                story_sessions.setdefault(int(user_id), set()).add(str(session_id))

    story_session_ids = {sid for sids in story_sessions.values() for sid in sids}

    daily_rows = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if story_session_ids:
                cursor.execute(
                    """
                    SELECT
                        t.user_id,
                        t.username,
                        t.id AS translation_id,
                        t.feedback,
                        t.user_translation,
                        COALESCE(t.audio_grammar_opt_in, FALSE) AS audio_grammar_opt_in,
                        ds.sentence,
                        ds.unique_id,
                        t.session_id,
                        COALESCE(t.source_lang, 'ru') AS source_lang,
                        COALESCE(t.target_lang, 'de') AS target_lang
                    FROM bt_3_translations t
                    JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                    WHERE ds.date = %s
                      AND t.score < 85
                      AND t.session_id NOT IN %s
                    ORDER BY t.user_id, ds.unique_id;
                    """,
                    (target_date, tuple(story_session_ids)),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        t.user_id,
                        t.username,
                        t.id AS translation_id,
                        t.feedback,
                        t.user_translation,
                        COALESCE(t.audio_grammar_opt_in, FALSE) AS audio_grammar_opt_in,
                        ds.sentence,
                        ds.unique_id,
                        t.session_id,
                        COALESCE(t.source_lang, 'ru') AS source_lang,
                        COALESCE(t.target_lang, 'de') AS target_lang
                    FROM bt_3_translations t
                    JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                    WHERE ds.date = %s
                      AND t.score < 85
                    ORDER BY t.user_id, ds.unique_id;
                    """,
                    (target_date,),
                )
            daily_rows = cursor.fetchall()

    daily_by_user_pair: dict[tuple[int, str, str], list[dict]] = {}
    daily_names: dict[int, str] = {}
    for user_id, username, _translation_id, feedback, user_translation, audio_grammar_opt_in, _sentence, _unique_id, _session_id, source_lang, target_lang in daily_rows:
        user_id = int(user_id)
        src = str(source_lang or "ru").strip().lower() or "ru"
        tgt = str(target_lang or "de").strip().lower() or "de"
        display = (username or "").strip()
        if display:
            daily_names[user_id] = display
        correct = _extract_correct_translation(feedback)
        ru_original = (_sentence or "").strip()
        de_correct = (correct or user_translation or "").strip()
        if not de_correct:
            continue
        daily_by_user_pair.setdefault((user_id, src, tgt), []).append(
            {
                "source_text": ru_original,
                "target_text": de_correct,
                "audio_grammar_opt_in": bool(audio_grammar_opt_in),
            }
        )

    story_by_user_pair: dict[tuple[int, str, str], list[dict]] = {}
    story_names: dict[int, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for user_id, session_ids in story_sessions.items():
                for session_id in session_ids:
                    cursor.execute(
                        """
                        SELECT
                            t.user_id,
                            t.username,
                            t.id AS translation_id,
                            t.feedback,
                            t.user_translation,
                            COALESCE(t.audio_grammar_opt_in, FALSE) AS audio_grammar_opt_in,
                            ds.sentence,
                            ds.unique_id,
                            COALESCE(t.source_lang, 'ru') AS source_lang,
                            COALESCE(t.target_lang, 'de') AS target_lang
                        FROM bt_3_translations t
                        JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                        WHERE t.user_id = %s
                          AND t.session_id = %s
                          AND t.timestamp::date = %s
                        ORDER BY ds.unique_id;
                        """,
                        (user_id, session_id, target_date),
                    )
                    rows = cursor.fetchall()
                    for uid, username, _translation_id, feedback, user_translation, audio_grammar_opt_in, _sentence, _unique_id, source_lang, target_lang in rows:
                        uid = int(uid)
                        src = str(source_lang or "ru").strip().lower() or "ru"
                        tgt = str(target_lang or "de").strip().lower() or "de"
                        display = (username or "").strip()
                        if display:
                            story_names[uid] = display
                        correct = _extract_correct_translation(feedback)
                        ru_original = (_sentence or "").strip()
                        de_correct = (correct or user_translation or "").strip()
                        if not de_correct:
                            continue
                        story_by_user_pair.setdefault((uid, src, tgt), []).append(
                            {
                                "source_text": ru_original,
                                "target_text": de_correct,
                                "audio_grammar_opt_in": bool(audio_grammar_opt_in),
                            }
                        )

    sent_daily = 0
    sent_story = 0
    errors: list[str] = []

    for (user_id, source_lang, target_lang), mistakes in daily_by_user_pair.items():
        if not mistakes:
            continue
        try:
            enriched: list[dict] = []
            for item in mistakes:
                enriched_item = dict(item)
                if bool(item.get("audio_grammar_opt_in")):
                    explanation_text = _generate_audio_grammar_explanation(
                        sentence=str(item.get("target_text") or ""),
                        source_lang=source_lang,
                        target_lang=target_lang,
                    )
                    if explanation_text:
                        enriched_item["explanation_text"] = explanation_text
                enriched.append(enriched_item)
            mistakes = enriched
            script = build_full_script(mistakes, source_lang=source_lang, target_lang=target_lang)
            audio = render_script_to_audio(script)
            name = daily_names.get(user_id) or f"user_{user_id}"
            pair_label = _pair_code(source_lang, target_lang)
            filename = safe_filename(f"{name}_{pair_label}", user_id, target_date.isoformat())
            caption = f"Ошибки за {target_date.isoformat()} — {name} ({pair_label})"
            _send_group_audio(audio, filename, caption)
            sent_daily += 1
        except Exception as exc:
            errors.append(f"daily user {user_id} {source_lang}->{target_lang}: {exc}")

    for (user_id, source_lang, target_lang), mistakes in story_by_user_pair.items():
        if not mistakes:
            continue
        try:
            enriched: list[dict] = []
            for item in mistakes:
                enriched_item = dict(item)
                if bool(item.get("audio_grammar_opt_in")):
                    explanation_text = _generate_audio_grammar_explanation(
                        sentence=str(item.get("target_text") or ""),
                        source_lang=source_lang,
                        target_lang=target_lang,
                    )
                    if explanation_text:
                        enriched_item["explanation_text"] = explanation_text
                enriched.append(enriched_item)
            mistakes = enriched
            script = build_full_script(mistakes, source_lang=source_lang, target_lang=target_lang)
            audio = render_script_to_audio(script)
            name = story_names.get(user_id) or f"user_{user_id}"
            pair_label = _pair_code(source_lang, target_lang)
            filename = safe_filename(f"{name}_{pair_label}", user_id, target_date.isoformat())
            caption = f"История за {target_date.isoformat()} — {name} ({pair_label})"
            _send_group_audio(audio, filename, caption)
            sent_story += 1
        except Exception as exc:
            errors.append(f"story user {user_id} {source_lang}->{target_lang}: {exc}")

    return {
        "ok": True,
        "date": target_date.isoformat(),
        "sent_daily": sent_daily,
        "sent_story": sent_story,
        "errors": errors,
    }


def _run_audio_scheduler_job() -> None:
    mode = (os.getenv("AUDIO_SCHEDULER_DATE_MODE") or "today").strip().lower()
    tz_name = (os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
    try:
        now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        now = datetime.utcnow()
    target_date = now.date()
    if mode == "yesterday":
        target_date = target_date - timedelta(days=1)
    try:
        result = _dispatch_daily_audio(target_date)
        logging.info("✅ Audio scheduler finished: %s", result)
    except Exception:
        logging.exception("❌ Audio scheduler failed")


def _dispatch_private_analytics(target_date: date) -> dict:
    users: dict[int, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COALESCE(NULLIF(MAX(username), ''), '')
                FROM bt_3_user_progress
                WHERE start_time::date = %s
                GROUP BY user_id;
                """,
                (target_date,),
            )
            for user_id, username in cursor.fetchall():
                users[int(user_id)] = (username or "").strip()

            cursor.execute(
                """
                SELECT t.user_id, COALESCE(NULLIF(MAX(t.username), ''), '')
                FROM bt_3_translations t
                JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                WHERE ds.date = %s
                GROUP BY t.user_id;
                """,
                (target_date,),
            )
            for user_id, username in cursor.fetchall():
                users.setdefault(int(user_id), (username or "").strip())

    if not users:
        return {"ok": True, "date": target_date.isoformat(), "sent": 0, "errors": []}

    bounds = get_period_bounds("week", today=target_date)
    sent = 0
    errors: list[str] = []
    for user_id, username in users.items():
        if not is_telegram_user_allowed(user_id):
            continue
        try:
            summary = fetch_user_summary(
                user_id=user_id,
                start_date=bounds.start_date,
                end_date=bounds.end_date,
            )
            total = int(summary.get("total_translations") or 0)
            if total <= 0:
                continue
            name = username or f"user_{user_id}"
            text = (
                f"📊 Твоя аналитика за неделю ({bounds.start_date} — {bounds.end_date})\n"
                f"👤 {name}\n"
                f"✅ Успешных переводов: {summary.get('successful_translations', 0)} / {total}\n"
                f"🎯 Средний балл: {summary.get('avg_score', 0)}\n"
                f"📈 Успешность: {summary.get('success_rate', 0)}%\n"
                f"⏱️ Среднее время: {summary.get('avg_time_min', 0)} мин\n"
                f"🔥 Final score: {summary.get('final_score', 0)}\n\n"
                f"Открыть графики и детали:\n{_build_webapp_deeplink('review')}"
            )
            _send_private_message(user_id, text)

            chart_png = _build_private_analytics_chart_png(
                user_id=user_id,
                start_date=bounds.start_date,
                end_date=bounds.end_date,
                username=name,
            )
            if chart_png:
                _send_private_photo(
                    user_id=user_id,
                    image_bytes=chart_png,
                    filename=f"analytics_{user_id}_{target_date.isoformat()}.png",
                    caption=f"📈 График прогресса за неделю\n{bounds.start_date} — {bounds.end_date}",
                )
            sent += 1
        except Exception as exc:
            errors.append(f"user {user_id}: {exc}")

    return {"ok": True, "date": target_date.isoformat(), "sent": sent, "errors": errors}


def _build_plan_goals_chart_png(
    *,
    username: str,
    source_lang: str,
    target_lang: str,
    start_date: str,
    end_date: str,
    period_label: str,
    metrics: dict | None,
) -> bytes | None:
    if plt is None:
        return None
    metrics = metrics or {}
    ordered = [
        ("Переводы", metrics.get("translations") or {}, "#60a5fa"),
        ("Выученные слова", metrics.get("learned_words") or {}, "#34d399"),
        ("Минуты с агентом", metrics.get("agent_minutes") or {}, "#fbbf24"),
        ("Чтение (мин)", metrics.get("reading_minutes") or {}, "#22d3ee"),
    ]
    labels = [item[0] for item in ordered]
    plan_values = [float(item[1].get("goal") or 0.0) for item in ordered]
    actual_values = [float(item[1].get("actual") or 0.0) for item in ordered]
    forecast_values = [float(item[1].get("forecast") or 0.0) for item in ordered]
    if max(plan_values + actual_values + forecast_values + [0.0]) <= 0:
        return None

    fig, ax = plt.subplots(figsize=(9, 5), dpi=140)
    x = list(range(len(labels)))
    width = 0.24
    ax.bar([i - width for i in x], plan_values, width=width, label="План", color="#93c5fd", alpha=0.9)
    ax.bar(x, actual_values, width=width, label="Факт", color="#34d399", alpha=0.9)
    ax.bar([i + width for i in x], forecast_values, width=width, label="Прогноз", color="#f59e0b", alpha=0.9)

    ax.set_title(f"Личные цели ({period_label}): {username} ({source_lang}->{target_lang})\n{start_date} — {end_date}")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=0)
    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.legend(loc="upper left")
    fig.tight_layout()

    buff = BytesIO()
    fig.savefig(buff, format="png")
    plt.close(fig)
    buff.seek(0)
    return buff.read()


def _collect_active_users_for_plan_reminders(target_date: date) -> dict[int, str]:
    active_since = target_date - timedelta(days=30)
    users: dict[int, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COALESCE(NULLIF(MAX(username), ''), '')
                FROM bt_3_user_progress
                WHERE start_time::date >= %s
                GROUP BY user_id;
                """,
                (active_since,),
            )
            rows = cursor.fetchall()
            for user_id, username in rows:
                users[int(user_id)] = str(username or "").strip()
    return users


def _dispatch_plan_period_progress(
    *,
    target_date: date,
    period: str,
) -> dict:
    normalized_period = str(period or "week").strip().lower()
    if normalized_period == "half_year":
        normalized_period = "half-year"
    bounds = get_period_bounds(normalized_period, today=target_date)
    users: dict[tuple[int, str, str], str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH latest_name AS (
                    SELECT DISTINCT ON (user_id)
                        user_id,
                        username
                    FROM bt_3_user_progress
                    ORDER BY user_id, start_time DESC
                )
                SELECT
                    g.user_id,
                    g.source_lang,
                    g.target_lang,
                    COALESCE(NULLIF(n.username, ''), '') AS username
                FROM bt_3_weekly_goals g
                LEFT JOIN latest_name n ON n.user_id = g.user_id
                WHERE g.week_start BETWEEN %s AND %s
                  AND (
                    COALESCE(g.translations_goal, 0) > 0
                    OR COALESCE(g.learned_words_goal, 0) > 0
                    OR COALESCE(g.agent_minutes_goal, 0) > 0
                    OR COALESCE(g.reading_minutes_goal, 0) > 0
                  );
                """,
                (bounds.start_date, bounds.end_date),
            )
            rows = cursor.fetchall()
    for user_id, source_lang, target_lang, username in rows:
        users[(int(user_id), str(source_lang or "ru"), str(target_lang or "de"))] = str(username or "").strip()

    period_label_map = {
        "week": "неделя",
        "month": "месяц",
        "quarter": "квартал",
        "half-year": "полугодие",
        "year": "год",
    }
    period_label = period_label_map.get(normalized_period, normalized_period)
    sent_group = 0
    sent_private = 0
    reminders_sent = 0
    errors: list[str] = []
    for (user_id, source_lang, target_lang), username in users.items():
        if not is_telegram_user_allowed(user_id):
            continue
        try:
            progress = get_plan_progress(
                user_id=user_id,
                source_lang=source_lang,
                target_lang=target_lang,
                mature_interval_days=MATURE_INTERVAL_DAYS,
                period=normalized_period,
                as_of_date=target_date,
            )
            metrics = progress.get("metrics") if isinstance(progress, dict) else {}
            title_name = username or f"user_{user_id}"
            chart_png = _build_plan_goals_chart_png(
                username=title_name,
                source_lang=source_lang,
                target_lang=target_lang,
                start_date=str(progress.get("start_date") or bounds.start_date),
                end_date=str(progress.get("end_date") or bounds.end_date),
                period_label=period_label,
                metrics=metrics if isinstance(metrics, dict) else {},
            )
            if not chart_png:
                continue

            m_trans = (metrics or {}).get("translations") or {}
            m_words = (metrics or {}).get("learned_words") or {}
            m_agent = (metrics or {}).get("agent_minutes") or {}
            m_reading = (metrics or {}).get("reading_minutes") or {}
            caption = (
                f"🎯 Личные цели ({period_label}): {title_name} ({source_lang}->{target_lang})\n"
                f"{progress.get('start_date')} — {progress.get('end_date')}\n"
                f"Переводы: {m_trans.get('actual', 0)} / {m_trans.get('goal', 0)} (прогноз {m_trans.get('forecast', 0)})\n"
                f"Слова: {m_words.get('actual', 0)} / {m_words.get('goal', 0)} (прогноз {m_words.get('forecast', 0)})\n"
                f"Агент (мин): {m_agent.get('actual', 0)} / {m_agent.get('goal', 0)} (прогноз {m_agent.get('forecast', 0)})\n"
                f"Чтение (мин): {m_reading.get('actual', 0)} / {m_reading.get('goal', 0)} (прогноз {m_reading.get('forecast', 0)})"
            )

            if _is_user_member_of_group_chat(user_id):
                _send_group_photo(
                    image_bytes=chart_png,
                    filename=f"plan_{normalized_period}_{user_id}_{source_lang}_{target_lang}_{target_date.isoformat()}.png",
                    caption=caption,
                )
                sent_group += 1
            else:
                _send_private_photo(
                    user_id=user_id,
                    image_bytes=chart_png,
                    filename=f"plan_{normalized_period}_{user_id}_{source_lang}_{target_lang}_{target_date.isoformat()}.png",
                    caption=caption,
                )
                sent_private += 1
        except Exception as exc:
            errors.append(f"user {user_id} {source_lang}->{target_lang}: {exc}")

    if normalized_period == "week" and target_date.weekday() == 0:
        active_users = _collect_active_users_for_plan_reminders(target_date=target_date)
        for user_id, username in active_users.items():
            if not is_telegram_user_allowed(user_id):
                continue
            try:
                source_lang, target_lang, _profile = _get_user_language_pair(user_id)
                goals = get_weekly_goals(
                    user_id=user_id,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    week_start=bounds.start_date,
                ) or {}
                has_plan = (
                    int(goals.get("translations_goal") or 0) > 0
                    or int(goals.get("learned_words_goal") or 0) > 0
                    or int(goals.get("agent_minutes_goal") or 0) > 0
                    or int(goals.get("reading_minutes_goal") or 0) > 0
                )
                if has_plan:
                    continue
                name = username or f"user_{user_id}"
                reminder = (
                    f"🗓 Новый понедельник, {name}.\n"
                    "Поставь личный план на неделю: переводы, выученные слова, минуты с агентом и чтение."
                )
                plan_button_url = _build_webapp_deeplink("webapp")
                reply_markup = {
                    "inline_keyboard": [[{"text": "Войти и поставить план", "url": plan_button_url}]],
                }
                if _is_user_member_of_group_chat(user_id):
                    _send_group_message(reminder, reply_markup=reply_markup)
                else:
                    _send_private_message(user_id=user_id, text=reminder, reply_markup=reply_markup)
                reminders_sent += 1
            except Exception as exc:
                errors.append(f"reminder user {user_id}: {exc}")

    return {
        "ok": True,
        "period": normalized_period,
        "date": target_date.isoformat(),
        "start_date": bounds.start_date.isoformat(),
        "end_date": bounds.end_date.isoformat(),
        "sent_group": sent_group,
        "sent_private": sent_private,
        "reminders_sent": reminders_sent,
        "errors": errors,
    }


def _resolve_previous_week_bounds(anchor_date: date) -> tuple[date, date]:
    current_week_start = anchor_date - timedelta(days=anchor_date.weekday())
    week_end = current_week_start - timedelta(days=1)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


def _collect_weekly_badges_rows(week_start: date, week_end: date) -> list[dict]:
    period_start_dt = datetime.combine(week_start, datetime.min.time(), tzinfo=timezone.utc)
    period_end_exclusive = datetime.combine(week_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH week_users AS (
                    SELECT user_id
                    FROM (
                        SELECT DISTINCT p.user_id
                        FROM bt_3_user_progress p
                        WHERE p.start_time::date BETWEEN %s AND %s
                        UNION
                        SELECT DISTINCT t.user_id
                        FROM bt_3_translations t
                        JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                        WHERE ds.date BETWEEN %s AND %s
                        UNION
                        SELECT DISTINCT a.user_id
                        FROM bt_3_agent_voice_sessions a
                        WHERE a.started_at < %s
                          AND COALESCE(a.ended_at, %s) > %s
                        UNION
                        SELECT DISTINCT r.user_id
                        FROM bt_3_reader_sessions r
                        WHERE r.started_at < %s
                          AND COALESCE(r.ended_at, %s) > %s
                    ) x
                ),
                latest_name AS (
                    SELECT DISTINCT ON (p.user_id)
                        p.user_id,
                        p.username
                    FROM bt_3_user_progress p
                    ORDER BY p.user_id, p.start_time DESC
                ),
                translation_time AS (
                    SELECT
                        p.user_id,
                        COALESCE(SUM(EXTRACT(EPOCH FROM (p.end_time - p.start_time)) / 60.0), 0.0) AS translation_minutes
                    FROM bt_3_user_progress p
                    WHERE p.completed = TRUE
                      AND p.start_time::date BETWEEN %s AND %s
                    GROUP BY p.user_id
                ),
                agent_time AS (
                    SELECT
                        a.user_id,
                        COALESCE(
                            SUM(
                                GREATEST(
                                    0,
                                    EXTRACT(
                                        EPOCH FROM (
                                            LEAST(COALESCE(a.ended_at, %s), %s)
                                            - GREATEST(a.started_at, %s)
                                        )
                                    )
                                )
                            ) / 60.0,
                            0.0
                        ) AS agent_minutes
                    FROM bt_3_agent_voice_sessions a
                    WHERE a.started_at < %s
                      AND COALESCE(a.ended_at, %s) > %s
                    GROUP BY a.user_id
                ),
                reader_time AS (
                    SELECT
                        r.user_id,
                        COALESCE(
                            SUM(
                                GREATEST(
                                    0,
                                    EXTRACT(
                                        EPOCH FROM (
                                            LEAST(COALESCE(r.ended_at, %s), %s)
                                            - GREATEST(r.started_at, %s)
                                        )
                                    )
                                )
                            ) / 60.0,
                            0.0
                        ) AS reader_minutes
                    FROM bt_3_reader_sessions r
                    WHERE r.started_at < %s
                      AND COALESCE(r.ended_at, %s) > %s
                    GROUP BY r.user_id
                ),
                score_base AS (
                    SELECT
                        t.user_id,
                        t.id_for_mistake_table,
                        t.score,
                        t.timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.user_id, t.id_for_mistake_table
                            ORDER BY t.timestamp DESC
                        ) AS rn
                    FROM bt_3_translations t
                    JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                    WHERE ds.date BETWEEN %s AND %s
                ),
                score_agg AS (
                    SELECT
                        user_id,
                        COUNT(*) AS translations_count,
                        AVG(score) AS avg_score
                    FROM score_base
                    WHERE rn = 1
                    GROUP BY user_id
                ),
                plan_days AS (
                    SELECT
                        p.user_id,
                        p.plan_date,
                        COUNT(*) AS total_items,
                        SUM(CASE WHEN LOWER(COALESCE(i.status, 'todo')) = 'done' THEN 1 ELSE 0 END) AS done_items
                    FROM bt_3_daily_plans p
                    JOIN bt_3_daily_plan_items i ON i.plan_id = p.id
                    WHERE p.plan_date BETWEEN %s AND %s
                    GROUP BY p.user_id, p.plan_date
                ),
                plan_agg AS (
                    SELECT
                        user_id,
                        SUM(CASE WHEN total_items >= 3 AND done_items >= total_items THEN 1 ELSE 0 END) AS full_days_3plus
                    FROM plan_days
                    GROUP BY user_id
                )
                SELECT
                    u.user_id,
                    COALESCE(NULLIF(n.username, ''), NULLIF(au.username, ''), '') AS username,
                    COALESCE(t.translation_minutes, 0.0) AS translation_minutes,
                    COALESCE(a.agent_minutes, 0.0) AS agent_minutes,
                    COALESCE(r.reader_minutes, 0.0) AS reader_minutes,
                    COALESCE(s.avg_score, 0.0) AS avg_score,
                    COALESCE(s.translations_count, 0) AS translations_count,
                    COALESCE(p.full_days_3plus, 0) AS full_days_3plus
                FROM week_users u
                LEFT JOIN latest_name n ON n.user_id = u.user_id
                LEFT JOIN bt_3_allowed_users au ON au.user_id = u.user_id
                LEFT JOIN translation_time t ON t.user_id = u.user_id
                LEFT JOIN agent_time a ON a.user_id = u.user_id
                LEFT JOIN reader_time r ON r.user_id = u.user_id
                LEFT JOIN score_agg s ON s.user_id = u.user_id
                LEFT JOIN plan_agg p ON p.user_id = u.user_id
                ORDER BY u.user_id ASC;
                """,
                (
                    week_start,
                    week_end,
                    week_start,
                    week_end,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    week_start,
                    week_end,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    period_end_exclusive,
                    period_end_exclusive,
                    period_start_dt,
                    week_start,
                    week_end,
                    week_start,
                    week_end,
                ),
            )
            rows = cursor.fetchall()

    normalized: list[dict] = []
    for row in rows:
        user_id = int(row[0] or 0)
        username = str(row[1] or "").strip()
        translation_minutes = max(0.0, float(row[2] or 0.0))
        agent_minutes = max(0.0, float(row[3] or 0.0))
        reader_minutes = max(0.0, float(row[4] or 0.0))
        avg_score = max(0.0, float(row[5] or 0.0))
        translations_count = max(0, int(row[6] or 0))
        full_days_3plus = max(0, int(row[7] or 0))
        total_minutes = translation_minutes + agent_minutes + reader_minutes
        if total_minutes <= 0 and translations_count <= 0 and full_days_3plus <= 0:
            continue
        normalized.append(
            {
                "user_id": user_id,
                "username": username,
                "translation_minutes": round(translation_minutes, 1),
                "agent_minutes": round(agent_minutes, 1),
                "reader_minutes": round(reader_minutes, 1),
                "total_minutes": round(total_minutes, 1),
                "avg_score": round(avg_score, 1),
                "translations_count": translations_count,
                "full_days_3plus": full_days_3plus,
            }
        )

    normalized.sort(
        key=lambda item: (
            float(item.get("total_minutes") or 0.0),
            float(item.get("avg_score") or 0.0),
            int(item.get("translations_count") or 0),
        ),
        reverse=True,
    )
    for idx, item in enumerate(normalized, start=1):
        item["rank"] = idx
    return normalized


def _weekly_badges_payload(rows: list[dict]) -> dict:
    if not rows:
        return {"leaderboard": [], "champion": None, "score_master": None, "streak_monsters": [], "growth_note": None}
    champion = rows[0]
    score_candidates = [
        item for item in rows if int(item.get("translations_count") or 0) >= 5 and float(item.get("avg_score") or 0.0) > 0.0
    ]
    score_master = max(score_candidates, key=lambda item: float(item.get("avg_score") or 0.0), default=None)
    streak_monsters = [item for item in rows if int(item.get("full_days_3plus") or 0) >= 5]
    growth_note = None
    if len(rows) >= 3:
        growth_note = "🌱 Зона роста недели: кому-то немного не хватило темпа. Поддержим друг друга без шейминга."
    return {
        "leaderboard": rows[:10],
        "champion": champion,
        "score_master": score_master,
        "streak_monsters": streak_monsters[:5],
        "growth_note": growth_note,
    }


def _format_weekly_user_label(row: dict | None) -> str:
    if not isinstance(row, dict):
        return "участник"
    username = str(row.get("username") or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    return f"user_{int(row.get('user_id') or 0)}"


def _format_weekly_badges_message(week_start: date, week_end: date, payload: dict) -> str:
    leaderboard = payload.get("leaderboard") if isinstance(payload.get("leaderboard"), list) else []
    champion = payload.get("champion") if isinstance(payload.get("champion"), dict) else None
    score_master = payload.get("score_master") if isinstance(payload.get("score_master"), dict) else None
    streak_monsters = payload.get("streak_monsters") if isinstance(payload.get("streak_monsters"), list) else []
    growth_note = str(payload.get("growth_note") or "").strip()

    lines = [
        f"🏁 Weekly leaderboard ({week_start.isoformat()} — {week_end.isoformat()})",
        "Минуты = переводы + агент + читалка",
        "",
    ]
    for item in leaderboard[:5]:
        lines.append(
            f"{int(item.get('rank') or 0)}. {_format_weekly_user_label(item)}"
            f" — {float(item.get('total_minutes') or 0.0):.1f} мин"
            f" | score {float(item.get('avg_score') or 0.0):.1f}"
        )
    if not leaderboard:
        lines.append("Активных данных за неделю пока нет.")

    lines.append("")
    if champion:
        lines.append(
            f"🥇 Champion der Woche: {_format_weekly_user_label(champion)}"
            f" ({float(champion.get('total_minutes') or 0.0):.1f} мин)"
        )
    if score_master:
        lines.append(
            f"🎯 Präzisionsmeister: {_format_weekly_user_label(score_master)}"
            f" ({float(score_master.get('avg_score') or 0.0):.1f}/100, {int(score_master.get('translations_count') or 0)} переводов)"
        )
    if streak_monsters:
        labels = ", ".join(_format_weekly_user_label(item) for item in streak_monsters[:3])
        lines.append(f"🔥 Streak-Monster: {labels}")
    if growth_note:
        lines.append(growth_note)
    return "\n".join(lines)


def _dispatch_weekly_group_badges(
    *,
    target_date: date,
    tz_name: str = TODAY_PLAN_DEFAULT_TZ,
    include_current_week: bool = False,
) -> dict:
    if not TELEGRAM_GROUP_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_GROUP_CHAT_ID должен быть установлен"}
    if include_current_week:
        week_start = target_date - timedelta(days=target_date.weekday())
        week_end = week_start + timedelta(days=6)
    else:
        week_start, week_end = _resolve_previous_week_bounds(target_date)

    try:
        rows = _collect_weekly_badges_rows(week_start=week_start, week_end=week_end)
        badges_payload = _weekly_badges_payload(rows)
        text = _format_weekly_badges_message(week_start=week_start, week_end=week_end, payload=badges_payload)
        _send_group_message(
            text,
            reply_markup={
                "inline_keyboard": [[{"text": "Открыть приложение", "url": _build_webapp_deeplink("webapp")}]],
            },
        )
        return {
            "ok": True,
            "date": target_date.isoformat(),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "participants": len(rows),
            "top_count": len(badges_payload.get("leaderboard") or []),
            "sent_group": 1,
            "tz": tz_name,
        }
    except Exception as exc:
        logging.exception("❌ Weekly group badges dispatch failed")
        return {
            "ok": False,
            "date": target_date.isoformat(),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "error": str(exc),
        }


def _daily_plan_completion_snapshot(user_id: int, target_date: date) -> dict:
    plan = get_daily_plan(user_id=int(user_id), plan_date=target_date)
    items = (plan or {}).get("items") or []
    total = len(items)
    done = sum(1 for item in items if str(item.get("status") or "").strip().lower() == "done")
    is_complete = total > 0 and done >= total
    return {
        "has_plan": bool(plan),
        "total_items": int(total),
        "done_items": int(done),
        "is_complete": bool(is_complete),
    }


def _compute_daily_plan_streak(user_id: int, anchor_date: date) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH completed_days AS (
                    SELECT p.plan_date
                    FROM bt_3_daily_plans p
                    JOIN bt_3_daily_plan_items i ON i.plan_id = p.id
                    WHERE p.user_id = %s
                      AND p.plan_date <= %s
                    GROUP BY p.plan_date
                    HAVING COUNT(*) > 0
                       AND BOOL_AND(LOWER(COALESCE(i.status, 'todo')) = 'done')
                ),
                ordered AS (
                    SELECT
                        plan_date,
                        (%s::date - plan_date) AS delta_days,
                        ROW_NUMBER() OVER (ORDER BY plan_date DESC) AS rn
                    FROM completed_days
                )
                SELECT COUNT(*)
                FROM ordered
                WHERE delta_days = (rn - 1);
                """,
                (int(user_id), anchor_date, anchor_date),
            )
            row = cursor.fetchone()
    return max(0, int((row or [0])[0] or 0))


def _streak_badge(streak_days: int) -> str:
    streak = max(0, int(streak_days or 0))
    if streak >= 20:
        return "⚡ Неостановимый"
    if streak >= 7:
        return "🐉 Пылающий дракон"
    if streak >= 3:
        return "🔥 Огонек в профиле"
    return ""


def _dispatch_today_evening_reminders(target_date: date, tz_name: str = TODAY_PLAN_DEFAULT_TZ) -> dict:
    users = list_today_reminder_users(limit=5000, offset=0)
    if not users:
        return {"ok": True, "date": target_date.isoformat(), "reminded": 0, "celebrated": 0, "errors": []}

    reminded = 0
    celebrated = 0
    errors: list[str] = []
    plan_url = _build_webapp_deeplink("today")
    plan_button = {"inline_keyboard": [[{"text": "Открыть план на сегодня", "url": plan_url}]]}

    for user in users:
        user_id = int(user.get("user_id") or 0)
        if not user_id or not is_telegram_user_allowed(user_id):
            continue
        username = str(user.get("username") or "").strip() or f"user_{user_id}"
        try:
            snapshot = _daily_plan_completion_snapshot(user_id, target_date)
            if not snapshot.get("has_plan"):
                continue
            if not snapshot.get("is_complete"):
                text = (
                    f"⏰ {username}, мягкое напоминание: сегодня выполнено "
                    f"{snapshot.get('done_items', 0)}/{snapshot.get('total_items', 0)} задач.\n"
                    "Если будет возможность, закрой дневной план до конца."
                )
                if _is_user_member_of_group_chat(user_id):
                    _send_group_message(text, reply_markup=plan_button)
                else:
                    _send_private_message(user_id=user_id, text=text, reply_markup=plan_button)
                reminded += 1
                continue

            streak = _compute_daily_plan_streak(user_id, target_date)
            badge = _streak_badge(streak)
            text = (
                f"✅ {username}, отличный день: план выполнен полностью.\n"
                f"Серия: {streak} дн."
            )
            if badge:
                text += f"\n{badge}"
            if _is_user_member_of_group_chat(user_id):
                _send_group_message(text, reply_markup=plan_button)
            else:
                _send_private_message(user_id=user_id, text=text, reply_markup=plan_button)
            celebrated += 1
        except Exception as exc:
            errors.append(f"user {user_id}: {exc}")

    return {
        "ok": True,
        "date": target_date.isoformat(),
        "reminded": int(reminded),
        "celebrated": int(celebrated),
        "errors": errors,
    }


def _run_private_analytics_scheduler_job() -> None:
    mode = (os.getenv("ANALYTICS_SCHEDULER_DATE_MODE") or "today").strip().lower()
    tz_name = (os.getenv("ANALYTICS_SCHEDULER_TZ") or "UTC").strip()
    try:
        now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        now = datetime.utcnow()
    target_date = now.date()
    if mode == "yesterday":
        target_date = target_date - timedelta(days=1)
    try:
        result = _dispatch_private_analytics(target_date)
        logging.info("✅ Private analytics scheduler finished: %s", result)
    except Exception:
        logging.exception("❌ Private analytics scheduler failed")


def _run_weekly_goals_scheduler_job() -> None:
    tz_name = (os.getenv("WEEKLY_GOALS_SCHEDULER_TZ") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    target_date = _get_local_today_date(tz_name)
    try:
        result_week = _dispatch_plan_period_progress(target_date=target_date, period="week")
        logging.info("✅ Weekly goals scheduler finished (week): %s", result_week)
        weekly_badges_enabled = (os.getenv("WEEKLY_BADGES_GROUP_ENABLED") or "1").strip().lower()
        if weekly_badges_enabled in ("1", "true", "yes", "on") and target_date.weekday() == 0:
            badges_result = _dispatch_weekly_group_badges(
                target_date=target_date,
                tz_name=tz_name,
                include_current_week=False,
            )
            logging.info("✅ Weekly badges scheduler finished: %s", badges_result)
        if target_date.day == 1:
            prev_day = target_date - timedelta(days=1)
            result_month = _dispatch_plan_period_progress(target_date=prev_day, period="month")
            logging.info("✅ Weekly goals scheduler finished (month): %s", result_month)
            if target_date.month in {1, 4, 7, 10}:
                result_quarter = _dispatch_plan_period_progress(target_date=prev_day, period="quarter")
                logging.info("✅ Weekly goals scheduler finished (quarter): %s", result_quarter)
            if target_date.month in {1, 7}:
                result_half = _dispatch_plan_period_progress(target_date=prev_day, period="half-year")
                logging.info("✅ Weekly goals scheduler finished (half-year): %s", result_half)
            if target_date.month == 1:
                result_year = _dispatch_plan_period_progress(target_date=prev_day, period="year")
                logging.info("✅ Weekly goals scheduler finished (year): %s", result_year)
    except Exception:
        logging.exception("❌ Weekly goals scheduler failed")


def _format_today_plan_message(plan: dict) -> str:
    total = int(plan.get("total_minutes") or 0)
    lines = ["Твои задачи на сегодня готовы ✅", f"Всего {total} минут:"]
    for idx, item in enumerate(plan.get("items") or [], start=1):
        title = str(item.get("title") or "Задача").strip()
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        if item.get("task_type") == "cards":
            limit = payload.get("limit")
            if limit:
                lines.append(f"{idx}. Карточки: {limit} повторений")
            else:
                lines.append(f"{idx}. {title}")
            continue
        if item.get("task_type") == "translation":
            sentences = int(payload.get("sentences") or 7)
            sub = str(payload.get("sub_category") or "").strip()
            if sub:
                lines.append(f"{idx}. Перевод: {sentences} предложений ({sub})")
            else:
                lines.append(f"{idx}. Перевод: {sentences} предложений")
            continue
        if item.get("task_type") == "theory":
            sub = str(payload.get("sub_category") or "").strip()
            skill = str(payload.get("skill_title") or "").strip()
            label = sub or skill
            if label:
                lines.append(f"{idx}. Теория: {label}")
            else:
                lines.append(f"{idx}. Теория")
            continue
        if item.get("task_type") == "video":
            lines.append(f"{idx}. Видео: 5 минут")
            continue
        lines.append(f"{idx}. {title}")
    lines.append("")
    lines.append("Нажми и начинай 👇")
    return "\n".join(lines)


def _format_today_group_announcement(user_label: str, plan: dict) -> str:
    total = int(plan.get("total_minutes") or 0)
    lines = [f"📌 Для {user_label} готов дневной план", f"Всего: {total} минут"]
    for idx, item in enumerate(plan.get("items") or [], start=1):
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        task_type = str(item.get("task_type") or "").lower()
        if task_type == "cards":
            limit = int(payload.get("limit") or 0)
            if limit > 0:
                lines.append(f"{idx}. Карточки: {limit} повторений")
            else:
                lines.append(f"{idx}. Карточки")
            continue
        if task_type == "translation":
            sentences = int(payload.get("sentences") or 7)
            sub = str(payload.get("sub_category") or "").strip()
            if sub:
                lines.append(f"{idx}. Перевод: {sentences} предложений ({sub})")
            else:
                lines.append(f"{idx}. Перевод: {sentences} предложений")
            continue
        if task_type == "theory":
            sub = str(payload.get("sub_category") or "").strip()
            skill = str(payload.get("skill_title") or "").strip()
            label = sub or skill
            if label:
                lines.append(f"{idx}. Теория: {label}")
            else:
                lines.append(f"{idx}. Теория")
            continue
        if task_type == "video":
            sec = int(payload.get("duration_sec") or 300)
            minutes = max(1, round(sec / 60))
            lines.append(f"{idx}. Видео: {minutes} минут")
            continue
        lines.append(f"{idx}. {str(item.get('title') or 'Задача')}")
    return "\n".join(lines)


def _send_today_plan_private_message(user_id: int, plan: dict) -> None:
    button_url = _build_webapp_deeplink("today")
    reply_markup = {
        "inline_keyboard": [[{"text": "Открыть план", "url": button_url}]],
    }
    _send_private_message(
        user_id=int(user_id),
        text=_format_today_plan_message(plan),
        reply_markup=reply_markup,
    )


def _dispatch_today_plans(target_date: date, tz_name: str = TODAY_PLAN_DEFAULT_TZ) -> dict:
    users = list_today_reminder_users(limit=5000, offset=0)
    if not users:
        return {"ok": True, "date": target_date.isoformat(), "sent_private": 0, "sent_group_fallback": 0, "errors": []}

    group_on_private_fail = str(os.getenv("TODAY_PLAN_GROUP_ON_PRIVATE_FAIL") or "1").strip().lower() in {"1", "true", "yes", "on"}
    sent_private = 0
    sent_group_fallback = 0
    errors: list[str] = []
    for row in users:
        user_id = int(row.get("user_id"))
        if not is_telegram_user_allowed(user_id):
            continue
        try:
            source_lang, target_lang, _profile = _get_user_language_pair(user_id)
            plan = _get_or_create_today_plan(
                user_id=user_id,
                plan_date=target_date,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            try:
                _send_today_plan_private_message(user_id=user_id, plan=plan)
                sent_private += 1
            except Exception as private_exc:
                if not group_on_private_fail:
                    raise
                username = str(row.get("username") or "").strip()
                user_label = f"@{username}" if username else f"user_{user_id}"
                fallback_text = _format_today_group_announcement(user_label, plan)
                fallback_text += "\n\n⚠️ Личное сообщение не доставлено."
                _send_group_message(fallback_text)
                sent_group_fallback += 1
                logging.warning("Today private send failed for user %s: %s", user_id, private_exc)
                errors.append(f"user {user_id}: private_send_failed: {private_exc}")
        except Exception as exc:
            errors.append(f"user {user_id}: {exc}")
    return {
        "ok": True,
        "date": target_date.isoformat(),
        "sent_private": sent_private,
        "sent_group_fallback": sent_group_fallback,
        "errors": errors,
    }


def _run_today_plan_scheduler_job() -> None:
    tz_name = (os.getenv("TODAY_PLAN_TZ") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    target_date = _get_local_today_date(tz_name)
    try:
        result = _dispatch_today_plans(target_date=target_date, tz_name=tz_name)
        logging.info("✅ Today plan scheduler finished: %s", result)
    except Exception:
        logging.exception("❌ Today plan scheduler failed")


def _run_today_evening_reminders_scheduler_job() -> None:
    tz_name = (os.getenv("TODAY_PLAN_TZ") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    target_date = _get_local_today_date(tz_name)
    try:
        result = _dispatch_today_evening_reminders(target_date=target_date, tz_name=tz_name)
        logging.info("✅ Today evening reminders scheduler finished: %s", result)
    except Exception:
        logging.exception("❌ Today evening reminders scheduler failed")


def _run_system_message_cleanup_job() -> None:
    enabled = (os.getenv("SYSTEM_MESSAGE_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ System message cleanup disabled by SYSTEM_MESSAGE_CLEANUP_ENABLED")
        return
    tz_name = (os.getenv("SYSTEM_MESSAGE_CLEANUP_TZ") or os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
    max_days_back = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK") or "2").strip())
    try:
        now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        now = datetime.utcnow()
        tz_name = "UTC"
    target_date = now.date()
    try:
        pending = get_pending_telegram_system_messages(
            target_date=target_date,
            tz_name=tz_name,
            max_days_back=max_days_back,
            limit=10000,
        )
    except Exception:
        logging.exception("❌ System message cleanup failed while reading pending list")
        return

    deleted = 0
    failed = 0
    for item in pending:
        row_id = int(item.get("id"))
        chat_id = int(item.get("chat_id"))
        message_id = int(item.get("message_id"))
        try:
            _delete_telegram_message(chat_id=chat_id, message_id=message_id)
            mark_telegram_system_message_deleted(row_id)
            deleted += 1
        except Exception as exc:
            failed += 1
            try:
                mark_telegram_system_message_deleted(row_id, delete_error=str(exc))
            except Exception:
                logging.debug("Failed to store delete error for row %s", row_id, exc_info=True)
    logging.info(
        "✅ System message cleanup finished: date=%s tz=%s pending=%s deleted=%s failed=%s",
        target_date.isoformat(),
        tz_name,
        len(pending),
        deleted,
        failed,
    )


def _get_youtube_oembed(video_id: str) -> dict:
    now = time.time()
    cached = _YT_OEMBED_CACHE.get(video_id)
    if cached and now - cached.get("ts", 0) < 7 * 24 * 60 * 60:
        return cached.get("data") or {}
    url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    data = {}
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code < 400:
            data = resp.json() or {}
    except Exception:
        data = {}
    _YT_OEMBED_CACHE[video_id] = {"ts": now, "data": data}
    return data


def _run_transcript_storage_report_job() -> None:
    user_id = int((os.getenv("TRANSCRIPT_REPORT_USER_ID") or "117649764").strip())
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pg_total_relation_size('bt_3_youtube_transcripts') AS bytes,
                           pg_size_pretty(pg_total_relation_size('bt_3_youtube_transcripts')::bigint) AS pretty,
                           COUNT(*) AS rows
                    FROM bt_3_youtube_transcripts;
                    """
                )
                row = cursor.fetchone() or (0, "0 bytes", 0)
        size_bytes, size_pretty, rows = row

        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id,
                           language,
                           is_generated,
                           updated_at,
                           pg_column_size(t.*) AS row_bytes,
                           pg_size_pretty(pg_column_size(t.*)::bigint) AS row_pretty,
                           pg_column_size(items) AS items_bytes,
                           pg_column_size(translations) AS translations_bytes
                    FROM bt_3_youtube_transcripts t
                    ORDER BY pg_column_size(t.*) DESC
                    LIMIT 20;
                    """
                )
                top_rows = cursor.fetchall()

        header = (
            "📊 Отчёт по кэшу субтитров\n"
            "Таблица: bt_3_youtube_transcripts\n"
            "Поля: video_id, items, language, is_generated, translations, updated_at\n"
            f"Записей всего: {rows}\n"
            f"Общий объём: {size_pretty} ({size_bytes} bytes)\n"
            "\n"
            "Топ-20 самых тяжёлых записей:"
        )

        lines = [header]
        for video_id, language, is_generated, updated_at, row_bytes, row_pretty, items_bytes, translations_bytes in top_rows:
            lines.append(
                f"- {video_id} | https://youtu.be/{video_id} | lang={language or '—'} | gen={is_generated} | "
                f"updated={updated_at:%Y-%m-%d} | row={row_pretty} ({row_bytes}) | "
                f"items={items_bytes}B | translations={translations_bytes}B"
            )

        _send_private_message_chunks(user_id, "\n".join(lines))
        logging.info("✅ Transcript storage report sent to %s", user_id)
    except Exception:
        logging.exception("❌ Transcript storage report failed")


def _start_audio_scheduler() -> None:
    global _audio_scheduler
    if BackgroundScheduler is None:
        logging.warning("⚠️ APScheduler not installed; audio scheduler disabled")
        return
    enabled = (os.getenv("AUDIO_SCHEDULER_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Audio scheduler disabled by AUDIO_SCHEDULER_ENABLED")
        return
    if not _acquire_audio_scheduler_lock():
        logging.info("ℹ️ Audio scheduler lock not acquired (another worker)")
        return
    tz_name = (os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
    hour = int((os.getenv("AUDIO_SCHEDULER_HOUR") or "13").strip())
    minute = int((os.getenv("AUDIO_SCHEDULER_MINUTE") or "0").strip())
    _audio_scheduler = BackgroundScheduler(timezone=tz_name)
    _audio_scheduler.add_job(
        _run_audio_scheduler_job,
        "cron",
        hour=hour,
        minute=minute,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    analytics_enabled = (os.getenv("ANALYTICS_PRIVATE_SCHEDULER_ENABLED") or "1").strip().lower()
    if analytics_enabled in ("1", "true", "yes", "on"):
        analytics_hour = int((os.getenv("ANALYTICS_PRIVATE_SCHEDULER_HOUR") or "19").strip())
        analytics_minute = int((os.getenv("ANALYTICS_PRIVATE_SCHEDULER_MINUTE") or "30").strip())
        _audio_scheduler.add_job(
            _run_private_analytics_scheduler_job,
            "cron",
            hour=analytics_hour,
            minute=analytics_minute,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    today_plan_enabled = (os.getenv("TODAY_PLAN_SCHEDULER_ENABLED") or "1").strip().lower()
    if today_plan_enabled in ("1", "true", "yes", "on"):
        today_hour = int((os.getenv("TODAY_PLAN_SCHEDULER_HOUR") or "7").strip())
        today_minute = int((os.getenv("TODAY_PLAN_SCHEDULER_MINUTE") or "0").strip())
        _audio_scheduler.add_job(
            _run_today_plan_scheduler_job,
            "cron",
            hour=today_hour,
            minute=today_minute,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    today_evening_enabled = (os.getenv("TODAY_EVENING_REMINDER_ENABLED") or "1").strip().lower()
    if today_evening_enabled in ("1", "true", "yes", "on"):
        today_evening_hour = int((os.getenv("TODAY_EVENING_REMINDER_HOUR") or "18").strip())
        today_evening_minute = int((os.getenv("TODAY_EVENING_REMINDER_MINUTE") or "0").strip())
        _audio_scheduler.add_job(
            _run_today_evening_reminders_scheduler_job,
            "cron",
            hour=today_evening_hour,
            minute=today_evening_minute,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    weekly_goals_enabled = (os.getenv("WEEKLY_GOALS_SCHEDULER_ENABLED") or "1").strip().lower()
    if weekly_goals_enabled in ("1", "true", "yes", "on"):
        weekly_goals_hour = int((os.getenv("WEEKLY_GOALS_SCHEDULER_HOUR") or "6").strip())
        weekly_goals_minute = int((os.getenv("WEEKLY_GOALS_SCHEDULER_MINUTE") or "45").strip())
        _audio_scheduler.add_job(
            _run_weekly_goals_scheduler_job,
            "cron",
            hour=weekly_goals_hour,
            minute=weekly_goals_minute,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    now = datetime.now(ZoneInfo(tz_name))
    first_report = now.replace(hour=17, minute=0, second=0, microsecond=0)
    if first_report <= now:
        first_report = now + timedelta(minutes=1)
    _audio_scheduler.add_job(
        _run_transcript_storage_report_job,
        "interval",
        days=14,
        start_date=first_report,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    cleanup_enabled = (os.getenv("SYSTEM_MESSAGE_CLEANUP_ENABLED") or "1").strip().lower()
    if cleanup_enabled in ("1", "true", "yes", "on"):
        cleanup_hour = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_HOUR") or "23").strip())
        cleanup_minute = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MINUTE") or "59").strip())
        _audio_scheduler.add_job(
            _run_system_message_cleanup_job,
            "cron",
            hour=cleanup_hour,
            minute=cleanup_minute,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    _audio_scheduler.start()
    logging.info("✅ Audio scheduler started: %02d:%02d %s", hour, minute, tz_name)


@app.route("/api/admin/send-daily-audio", methods=["POST"])
def send_daily_audio_to_group():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    date_str = (payload.get("date") or "").strip()
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = (datetime.utcnow().date() - timedelta(days=1))

    result = _dispatch_daily_audio(target_date)
    return jsonify(result)


@app.route("/api/admin/send-private-analytics", methods=["POST"])
def send_private_analytics_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    date_str = (payload.get("date") or "").strip()
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = datetime.utcnow().date()

    result = _dispatch_private_analytics(target_date)
    return jsonify(result)


@app.route("/api/admin/send-weekly-goals", methods=["POST"])
def send_weekly_goals_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    date_str = (payload.get("date") or "").strip()
    tz_name = (payload.get("tz") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    period = str(payload.get("period") or "week").strip().lower()
    if period == "half_year":
        period = "half-year"
    if period not in {"week", "month", "quarter", "half-year", "year"}:
        return jsonify({"error": "period must be one of: week, month, quarter, half-year, year"}), 400
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = _get_local_today_date(tz_name)

    result = _dispatch_plan_period_progress(target_date=target_date, period=period)
    return jsonify(result)


@app.route("/api/admin/send-today-plans", methods=["POST"])
def send_today_plans_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    date_str = (payload.get("date") or "").strip()
    tz_name = (payload.get("tz") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = _get_local_today_date(tz_name)

    result = _dispatch_today_plans(target_date=target_date, tz_name=tz_name)
    return jsonify(result)


@app.route("/api/admin/send-today-evening-reminders", methods=["POST"])
def send_today_evening_reminders_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    date_str = (payload.get("date") or "").strip()
    tz_name = (payload.get("tz") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = _get_local_today_date(tz_name)

    result = _dispatch_today_evening_reminders(target_date=target_date, tz_name=tz_name)
    return jsonify(result)


@app.route("/api/admin/send-weekly-badges", methods=["POST"])
def send_weekly_badges_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    tz_name = (payload.get("tz") or TODAY_PLAN_DEFAULT_TZ).strip() or TODAY_PLAN_DEFAULT_TZ
    date_str = (payload.get("date") or "").strip()
    include_current_week = str(payload.get("include_current_week") or "0").strip().lower() in {"1", "true", "yes", "on"}
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Неверный формат даты, нужен YYYY-MM-DD"}), 400
    else:
        target_date = _get_local_today_date(tz_name)

    result = _dispatch_weekly_group_badges(
        target_date=target_date,
        tz_name=tz_name,
        include_current_week=include_current_week,
    )
    return jsonify(result)


@app.route("/api/admin/youtube-proxy-subtitles-access", methods=["GET"])
def admin_list_youtube_proxy_subtitles_access():
    token = request.args.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    try:
        limit = int(request.args.get("limit", 200))
    except Exception:
        limit = 200
    try:
        offset = int(request.args.get("offset", 0))
    except Exception:
        offset = 0
    enabled_only_raw = str(request.args.get("enabled_only", "0")).strip().lower()
    enabled_only = enabled_only_raw in {"1", "true", "yes", "on"}
    items = list_youtube_proxy_subtitles_access(
        limit=limit,
        offset=offset,
        enabled_only=enabled_only,
    )
    return jsonify(
        {
            "ok": True,
            "items": items,
            "count": len(items),
            "enabled_only": bool(enabled_only),
        }
    )


@app.route("/api/admin/youtube-proxy-subtitles-access/grant", methods=["POST"])
def admin_grant_youtube_proxy_subtitles_access():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    user_id_raw = payload.get("user_id")
    if user_id_raw is None or str(user_id_raw).strip() == "":
        return jsonify({"error": "user_id обязателен"}), 400
    try:
        managed_user_id = int(user_id_raw)
    except Exception:
        return jsonify({"error": "user_id должен быть числом"}), 400

    granted_by_raw = payload.get("granted_by")
    granted_by = None
    if granted_by_raw is not None and str(granted_by_raw).strip() != "":
        try:
            granted_by = int(granted_by_raw)
        except Exception:
            return jsonify({"error": "granted_by должен быть числом"}), 400
    note = (payload.get("note") or "").strip() or None
    item = upsert_youtube_proxy_subtitles_access(
        user_id=managed_user_id,
        enabled=True,
        granted_by=granted_by,
        note=note,
    )
    return jsonify({"ok": True, "item": item})


@app.route("/api/admin/youtube-proxy-subtitles-access/revoke", methods=["POST"])
def admin_revoke_youtube_proxy_subtitles_access():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    user_id_raw = payload.get("user_id")
    if user_id_raw is None or str(user_id_raw).strip() == "":
        return jsonify({"error": "user_id обязателен"}), 400
    try:
        managed_user_id = int(user_id_raw)
    except Exception:
        return jsonify({"error": "user_id должен быть числом"}), 400

    granted_by_raw = payload.get("granted_by")
    granted_by = None
    if granted_by_raw is not None and str(granted_by_raw).strip() != "":
        try:
            granted_by = int(granted_by_raw)
        except Exception:
            return jsonify({"error": "granted_by должен быть числом"}), 400
    note = (payload.get("note") or "").strip() or None
    item = upsert_youtube_proxy_subtitles_access(
        user_id=managed_user_id,
        enabled=False,
        granted_by=granted_by,
        note=note,
    )
    return jsonify({"ok": True, "item": item})


@app.route("/api/admin/youtube-debug", methods=["POST"])
def youtube_debug():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401

    video_id = (payload.get("video_id") or "").strip()
    lang = (payload.get("lang") or "").strip() or None
    if not video_id:
        return jsonify({"error": "video_id обязателен"}), 400

    results: dict[str, str] = {}
    proxy = (os.getenv("YOUTUBE_TRANSCRIPT_PROXY") or "").strip()
    if proxy:
        try:
            resp = requests.get(
                f"https://www.youtube.com/watch?v={video_id}",
                timeout=15,
                proxies={"http": proxy, "https": proxy},
            )
            results["proxy_http_status"] = str(resp.status_code)
        except Exception as exc:
            results["proxy_http_error"] = str(exc)
    else:
        results["proxy_http_status"] = "proxy_not_set"

    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        yta = YouTubeTranscriptApi()
        try:
            _ = yta.list(video_id=video_id)
            results["legacy_list"] = "ok"
        except Exception as exc:
            results["legacy_list"] = str(exc)
        try:
            if lang:
                items = yta.fetch(video_id=video_id, languages=[lang])
            else:
                items = yta.fetch(video_id=video_id)
            results["legacy_fetch"] = f"ok:{len(_normalize_items(items))}"
        except Exception as exc:
            results["legacy_fetch"] = str(exc)
    except Exception as exc:
        results["legacy_init"] = str(exc)

    try:
        data = _fetch_youtube_transcript(video_id, lang=lang)
        results["pipeline"] = f"ok:{data.get('source')}"
    except Exception as exc:
        results["pipeline"] = str(exc)

    return jsonify({"ok": True, "results": results})


@app.route("/api/admin/cleanup-system-messages", methods=["POST"])
def cleanup_system_messages_now():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.headers.get("X-Admin-Token")
    required_token = os.getenv("AUDIO_DISPATCH_TOKEN") or ""
    if not required_token:
        return jsonify({"error": "AUDIO_DISPATCH_TOKEN не задан"}), 500
    if token != required_token:
        return jsonify({"error": "Неверный токен"}), 401
    _run_system_message_cleanup_job()
    return jsonify({"ok": True})


@app.route("/api/webapp/explain", methods=["POST"])
def explain_webapp_translation():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    original_text = (payload.get("original_text") or "").strip()
    user_translation = (payload.get("user_translation") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not original_text or not user_translation:
        return jsonify({"error": "original_text и user_translation обязательны"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
    try:
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            explanation = asyncio.run(run_translation_explanation(original_text, user_translation))
        else:
            explanation = asyncio.run(
                run_translation_explanation_multilang(
                    original_text=original_text,
                    user_translation=user_translation,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    explanation_lang=source_lang,
                )
            )
    except Exception as exc:
        return jsonify({"error": f"Ошибка объяснения: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "explanation": explanation,
            "language_pair": _build_language_pair_payload(source_lang, target_lang),
        }
    )


@app.route("/api/webapp/finish", methods=["POST"])
def finish_webapp_translation():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")
    user_name = user_data.get("first_name") or "User"
    username = _extract_display_name(user_data)

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    result = finish_translation_webapp(user_id)
    summary = build_user_daily_summary(user_id=user_id, username=username or user_name)
    group_warning = None
    if summary:
        try:
            _send_group_message(summary)
        except Exception as exc:
            # Group delivery is optional for WebApp flow; do not fail finish.
            group_warning = f"Не удалось отправить сводку в группу: {exc}"
    response_payload = {"ok": True, **result}
    if group_warning:
        response_payload["group_warning"] = group_warning
    return jsonify(response_payload)


try:
    if str(os.getenv("BILLING_OPENAI_SNAPSHOT_SYNC_ON_STARTUP") or "1").strip().lower() in {"1", "true", "yes", "on"}:
        snapshot_sync_result = _sync_openai_price_snapshots_public_then_env()
        logging.info(
            "Billing OpenAI snapshot sync: created=%s skipped=%s errors=%s",
            (snapshot_sync_result.get("summary") or {}).get("created_count"),
            (snapshot_sync_result.get("summary") or {}).get("skipped_count"),
            (snapshot_sync_result.get("summary") or {}).get("errors_count"),
        )
except Exception as exc:
    logging.warning("Billing OpenAI snapshot sync failed: %s", exc)

_start_audio_scheduler()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
