import os
import openai
from openai import OpenAI
import logging
import json
import psycopg2
import datetime
import calendar
import time as pytime
from datetime import datetime, time, date, timedelta
from zoneinfo import ZoneInfo
from telegram import Update, Poll
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, TypeHandler, Defaults, PollAnswerHandler, ContextTypes, ApplicationHandlerStop, ExtBot, ChatMemberHandler
from telegram.request import HTTPXRequest
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import CallbackQueryHandler
import hashlib
import hmac
import base64
import re
import html
import random
import requests
import aiohttp
from telegram.ext import CallbackContext
import textwrap
from googleapiclient.discovery import build
from telegram.error import TelegramError
from telegram.helpers import escape_markdown
from telegram.error import TimedOut, BadRequest, RetryAfter, Forbidden
import tempfile
import sys
import livekit.api # Нужен для LiveKit комнат
from typing import Any, Callable
from backend.analytics import fetch_user_summary, get_period_bounds
from backend.backend_server import (
    GoogleTTSBudgetBlockedError,
    _build_tts_prewarm_quota_control_text,
    _build_video_search_queries,
    _get_user_language_pair,
    _is_youtube_short_like,
    _parse_iso8601_duration_to_seconds,
    _video_conflicts_with_target_language,
    _youtube_search_videos_manual,
    _youtube_fill_view_counts,
    _sanitize_focus_topic,
    get_or_create_tts_clip,
    chunk_sentence_llm_de,
    schedule_user_paid_subscription_cancel_at_period_end,
)
import os
from pathlib import Path
from dotenv import load_dotenv
from pydub import AudioSegment
import io
from datetime import datetime
import logging
import sys
from backend.openai_manager import (
    llm_execute,
    run_dictionary_lookup,
    run_dictionary_lookup_de,
    run_dictionary_lookup_multilang,
    run_check_story_guess_semantic,
    run_feel_word,
    run_feel_word_multilang,
    run_dictionary_collocations,
    run_dictionary_collocations_multilang,
    run_generate_word_quiz,
    run_language_learning_private_question,
    run_quiz_followup_question,
    run_translate_subtitles_ru,
    run_translate_subtitles_multilang,
)
from backend.image_quiz_utils import (
    build_image_quiz_feedback_alert,
    build_image_quiz_feedback_payload,
    normalize_image_quiz_option_text,
)
from backend.database import (
    init_db,
    build_translation_session_minutes_sql,
    get_random_dictionary_entry,
    get_random_dictionary_entry_for_quiz_type,
    list_low_accuracy_telegram_quiz_entries,
    record_quiz_word,
    record_telegram_quiz_delivery,
    record_telegram_quiz_attempt,
    ensure_webapp_tables,
    get_admin_telegram_ids,
    is_telegram_user_allowed,
    allow_telegram_user,
    revoke_telegram_user,
    schedule_telegram_user_removal,
    cancel_telegram_user_removal,
    get_user_removal_request,
    list_user_removal_queue,
    list_due_user_removals_for_admin_confirmation,
    mark_user_removal_admin_notified,
    update_user_removal_billing_cancel_snapshot,
    purge_telegram_user_personal_data,
    list_allowed_telegram_users,
    create_access_request,
    resolve_access_request,
    resolve_latest_pending_access_request_for_user,
    get_access_request_by_id,
    list_pending_access_requests,
    update_webapp_dictionary_entry,
    apply_flashcard_feel_feedback,
    create_flashcard_feel_feedback_token,
    get_dictionary_cache,
    upsert_dictionary_cache,
    save_webapp_dictionary_query,
    create_support_message,
    get_or_create_dictionary_folder,
    record_telegram_system_message,
    get_pending_telegram_system_messages,
    mark_telegram_system_message_deleted,
    update_telegram_system_message_type,
    has_admin_scheduler_run,
    mark_admin_scheduler_run,
    get_google_translate_monthly_budget_status,
    get_google_tts_monthly_budget_status,
    set_provider_budget_extra_limit,
    set_provider_budget_block_state,
    get_tts_prewarm_settings,
    upsert_tts_prewarm_settings,
    confirm_webapp_group_participation,
    get_webapp_scope_state,
    list_known_webapp_group_chats,
    list_webapp_group_contexts,
    upsert_webapp_group_context,
    get_telegram_quiz_next_mode,
    set_telegram_quiz_next_mode,
    upsert_active_quiz,
    get_active_quiz,
    delete_active_quiz,
    store_prepared_telegram_quiz,
    count_prepared_telegram_quizzes,
    count_available_image_quiz_templates,
    claim_prepared_telegram_quiz,
    claim_next_ready_image_quiz_template,
    create_image_quiz_dispatch,
    mark_image_quiz_dispatch_sent,
    mark_image_quiz_dispatch_failed,
    mark_image_quiz_template_failed,
    get_image_quiz_dispatch,
    get_image_quiz_template,
    record_image_quiz_answer,
    mark_image_quiz_answer_feedback_sent,
    list_top_weak_topics,
)
from backend.job_queue import (
    can_enqueue_background_jobs,
    enqueue_image_quiz_template_refresh_job,
)
from user_analytics import prepare_aggregate_data_by_period_and_draw_analytic_for_user, aggregate_data_for_charts, create_analytics_figure_async
from load_data_from_db import load_data_for_analytics 
from users_comparison_analytics import create_comparison_report_async
from dateutil.relativedelta import relativedelta 
from datetime import date, timedelta
from backend.config_mistakes_data import VALID_CATEGORIES, VALID_SUBCATEGORIES, VALID_CATEGORIES_lower, VALID_SUBCATEGORIES_lower

application = None

QUIZ_SCHEDULE_HOURS = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
QUIZ_SCHEDULE_MINUTES = [0]
QUIZ_SCHEDULE_TZ_NAME = (os.getenv("QUIZ_SCHEDULE_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
QUIZ_IMAGE_SLOT_TIMES = {(9, 0), (12, 0), (18, 0)}
QUIZ_FEEDBACK_TTL_SECONDS = 120
QUIZ_CACHE_TTL_SECONDS = 60 * 60 * 24
QUIZ_FREEFORM_OPTION = "keine korrekte Antworten"
QUIZ_HIDE_CORRECT_PROBABILITY = 0.3
QUIZ_QUESTION_TTL_SECONDS = 60 * 30
QUIZ_QUESTION_REPLY_MAX_CHARS = 3200
QUIZ_REPEAT_ACCURACY_THRESHOLD = min(
    1.0,
    max(0.0, float(os.getenv("QUIZ_REPEAT_ACCURACY_THRESHOLD", "0.5"))),
)
QUIZ_REPEAT_CANDIDATE_LIMIT = max(1, int(os.getenv("QUIZ_REPEAT_CANDIDATE_LIMIT", "8")))
QUIZ_PREPARED_TARGET_PER_TYPE = max(2, int((os.getenv("QUIZ_PREPARED_TARGET_PER_TYPE") or "8").strip() or "8"))
QUIZ_PREPARED_STARTUP_DELAY_SECONDS = max(10, int((os.getenv("QUIZ_PREPARED_STARTUP_DELAY_SECONDS") or "45").strip() or "45"))
QUIZ_PREPARED_HOURLY_TOPUP_MINUTE = max(0, min(59, int((os.getenv("QUIZ_PREPARED_HOURLY_TOPUP_MINUTE") or "35").strip() or "35")))
IMAGE_QUIZ_READY_TARGET_PER_USER = max(1, int((os.getenv("IMAGE_QUIZ_READY_TARGET_PER_USER") or "1").strip() or "1"))
IMAGE_QUIZ_TOPUP_LOOKBACK_DAYS = max(1, int((os.getenv("IMAGE_QUIZ_TOPUP_LOOKBACK_DAYS") or "30").strip() or "30"))
IMAGE_QUIZ_TOPUP_ACTIVE_USERS_LIMIT = max(1, int((os.getenv("IMAGE_QUIZ_TOPUP_ACTIVE_USERS_LIMIT") or "32").strip() or "32"))
FLASHCARD_REMINDER_TIMES = [(7, 0), (16, 30)]
active_quizzes = {}
pending_quiz_freeform = {}
quiz_ru_translation_cache = {}
pending_quiz_feel_requests = {}
pending_quiz_question_requests = {}
pending_quiz_question_input = {}
pending_quiz_question_save_requests = {}
pending_quiz_phrase_requests = {}
pending_dictionary_cards = {}
pending_dictionary_save_options = {}
pending_dictionary_lookup_requests = {}
pending_dictionary_lookup_inflight = set()
pending_feel_requests_inflight = set()
pending_tts_listen_requests_inflight = set()
pending_quiz_phrase_requests_inflight = set()
scheduled_quiz_delivery_suppress_until = {}
recent_message_activity_logged = {}
SYNTHETIC_TELEGRAM_USER_ID_MIN = max(
    1,
    int((os.getenv("SYNTHETIC_TELEGRAM_USER_ID_MIN") or "9100000001").strip() or "9100000001"),
)
MESSAGE_ACTIVITY_LOG_MIN_INTERVAL_SECONDS = max(
    300,
    int((os.getenv("MESSAGE_ACTIVITY_LOG_MIN_INTERVAL_SECONDS") or "21600").strip() or "21600"),
)
BOT_DEBUG_LOG_ALL_MESSAGES = (os.getenv("BOT_DEBUG_LOG_ALL_MESSAGES") or "0").strip().lower() in {"1", "true", "yes", "on"}

QUIZ_DELIVERY_SUPPRESS_SECONDS = max(
    900,
    int((os.getenv("QUIZ_DELIVERY_SUPPRESS_SECONDS") or "21600").strip()),
)


def _purge_expired_quiz_delivery_suppressions() -> None:
    now_ts = pytime.time()
    expired = [chat_id for chat_id, until_ts in scheduled_quiz_delivery_suppress_until.items() if float(until_ts or 0) <= now_ts]
    for chat_id in expired:
        scheduled_quiz_delivery_suppress_until.pop(chat_id, None)


def _suppress_quiz_delivery_target(chat_id: int, *, seconds: int | None = None) -> None:
    ttl = max(60, int(seconds if seconds is not None else QUIZ_DELIVERY_SUPPRESS_SECONDS))
    scheduled_quiz_delivery_suppress_until[int(chat_id)] = pytime.time() + ttl


def _is_quiz_delivery_target_suppressed(chat_id: int) -> bool:
    _purge_expired_quiz_delivery_suppressions()
    until_ts = scheduled_quiz_delivery_suppress_until.get(int(chat_id))
    return bool(until_ts and float(until_ts) > pytime.time())


def _is_permanent_quiz_delivery_error(exc: Exception) -> bool:
    if isinstance(exc, Forbidden):
        return True
    if not isinstance(exc, BadRequest):
        return False
    message = str(exc or "").strip().lower()
    permanent_fragments = (
        "chat not found",
        "bot was kicked",
        "user is deactivated",
        "need administrator rights",
        "have no rights",
        "polls can't be sent",
        "poll can't be stopped",
        "not enough rights",
        "group chat was upgraded",
    )
    return any(fragment in message for fragment in permanent_fragments)


def _get_quiz_schedule_now() -> datetime:
    try:
        tz = ZoneInfo(QUIZ_SCHEDULE_TZ_NAME)
    except Exception:
        tz = ZoneInfo("Europe/Vienna")
    return datetime.now(tz)


def _is_image_quiz_slot(slot_dt: datetime) -> bool:
    return (int(slot_dt.hour), int(slot_dt.minute)) in QUIZ_IMAGE_SLOT_TIMES


def _format_quiz_delivery_slot(slot_dt: datetime) -> str:
    return f"{int(slot_dt.hour):02d}:{int(slot_dt.minute):02d}"


def _is_synthetic_telegram_user_id(user_id: int) -> bool:
    try:
        candidate = int(user_id)
    except Exception:
        return False
    return candidate >= SYNTHETIC_TELEGRAM_USER_ID_MIN


def _should_persist_message_activity(user_id: int) -> bool:
    now_ts = pytime.time()
    safe_user_id = int(user_id)
    stale_before = now_ts - (MESSAGE_ACTIVITY_LOG_MIN_INTERVAL_SECONDS * 2)
    expired = [item for item, ts in recent_message_activity_logged.items() if float(ts or 0.0) < stale_before]
    for item in expired:
        recent_message_activity_logged.pop(item, None)
    last_logged_at = float(recent_message_activity_logged.get(safe_user_id) or 0.0)
    if last_logged_at > 0 and (now_ts - last_logged_at) < MESSAGE_ACTIVITY_LOG_MIN_INTERVAL_SECONDS:
        return False
    recent_message_activity_logged[safe_user_id] = now_ts
    return True


def _persist_message_activity_touch(user_id: int, username: str) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO bt_3_messages (user_id, username, message)
            VALUES(%s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                timestamp = NOW();
            """,
            (int(user_id), str(username or "").strip(), "user_message"),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
pending_language_tutor_input = {}
pending_tts_budget_custom = {}
TTS_BUDGET_CUSTOM_TTL_SECONDS = 60 * 5
LANGUAGE_TUTOR_INPUT_TTL_SECONDS = 60 * 20
LANGUAGE_TUTOR_BUTTON_TEXT = "💬 Спросить у GPT"
TTS_PREWARM_QUOTA_MIN = max(50, min(10000, int((os.getenv("TTS_PREWARM_PER_USER_CHAR_LIMIT_MIN") or "200").strip() or "200")))
TTS_PREWARM_QUOTA_MAX = max(
    TTS_PREWARM_QUOTA_MIN,
    min(20000, int((os.getenv("TTS_PREWARM_PER_USER_CHAR_LIMIT_MAX") or "3000").strip() or "3000")),
)
MOBILE_AUTH_TTL_SECONDS = int(os.getenv("MOBILE_AUTH_TTL_SECONDS", "2592000"))
SYSTEM_MESSAGE_CLEANUP_TZ = (os.getenv("SYSTEM_MESSAGE_CLEANUP_TZ") or os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
SYSTEM_MESSAGE_CLEANUP_HOUR = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_HOUR") or "23").strip())
SYSTEM_MESSAGE_CLEANUP_MINUTE = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MINUTE") or "59").strip())
SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK") or "2").strip())
USER_REMOVAL_REVIEW_TZ = (os.getenv("USER_REMOVAL_REVIEW_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
USER_REMOVAL_REVIEW_MINUTE = max(0, min(59, int((os.getenv("USER_REMOVAL_REVIEW_MINUTE") or "17").strip() or "17")))
USER_REMOVAL_WEEKLY_REPORT_TZ = (os.getenv("USER_REMOVAL_WEEKLY_REPORT_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
USER_REMOVAL_WEEKLY_REPORT_DAY = (os.getenv("USER_REMOVAL_WEEKLY_REPORT_DAY") or "sun").strip().lower() or "sun"
USER_REMOVAL_WEEKLY_REPORT_HOUR = max(0, min(23, int((os.getenv("USER_REMOVAL_WEEKLY_REPORT_HOUR") or "9").strip() or "9")))
USER_REMOVAL_WEEKLY_REPORT_MINUTE = max(0, min(59, int((os.getenv("USER_REMOVAL_WEEKLY_REPORT_MINUTE") or "12").strip() or "12")))
SYSTEM_MESSAGE_CLEANUP_EXCLUDE_TYPES = [
    item.strip().lower()
    for item in (os.getenv("SYSTEM_MESSAGE_CLEANUP_EXCLUDE_TYPES") or "feel_word").split(",")
    if item.strip()
]
ENABLE_LEGACY_REPLY_KEYBOARD = (os.getenv("ENABLE_LEGACY_REPLY_KEYBOARD") or "0").strip().lower() in {"1", "true", "yes", "on"}
DICTIONARY_CARD_THEME = (os.getenv("DICTIONARY_CARD_THEME") or "classic").strip().lower()


# === Логирование ===
# Настраиваем логгер глобально
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)  # вывод в stdout
    ]
)

load_dotenv(dotenv_path=Path(__file__).parent/".env") # Загружаем переменные из .env
# Ты кладёшь GOOGLE_APPLICATION_CREDENTIALS=/path/... в .env.
# load_dotenv() загружает .env и делает вид, что это переменные окружения.
# os.getenv(...) читает эти значения.
# Ты вручную регистрируешь это в переменных окружения процесса
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

success=load_dotenv(dotenv_path=Path(__file__).parent/".env")

# Никогда не используем прокси для Telegram API
os.environ.setdefault("NO_PROXY", "api.telegram.org,telegram.org")


def _normalize_runtime_service_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _should_start_bot_scheduler() -> bool:
    override = str(os.getenv("BOT_SCHEDULER_ENABLED") or "").strip().lower()
    if override:
        return override in {"1", "true", "yes", "on"}
    railway_service_name = str(os.getenv("RAILWAY_SERVICE_NAME") or "").strip()
    if not railway_service_name:
        return True
    allowed_raw = str(os.getenv("PRIMARY_TELEGRAM_BOT_SERVICE_NAMES") or "MY_3_BOT").strip()
    allowed_names = {
        _normalize_runtime_service_name(item)
        for item in allowed_raw.split(",")
        if str(item).strip()
    }
    normalized_service_name = _normalize_runtime_service_name(railway_service_name)
    return normalized_service_name in allowed_names


def _should_run_primary_telegram_bot_process() -> bool:
    override = str(os.getenv("BOT_POLLING_ENABLED") or "").strip().lower()
    if override:
        return override in {"1", "true", "yes", "on"}
    railway_service_name = str(os.getenv("RAILWAY_SERVICE_NAME") or "").strip()
    if not railway_service_name:
        return True
    allowed_raw = str(os.getenv("PRIMARY_TELEGRAM_BOT_SERVICE_NAMES") or "MY_3_BOT").strip()
    allowed_names = {
        _normalize_runtime_service_name(item)
        for item in allowed_raw.split(",")
        if str(item).strip()
    }
    normalized_service_name = _normalize_runtime_service_name(railway_service_name)
    return normalized_service_name in allowed_names


# Buttons in Telegramm
TOPICS = [
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
    "📰 News"
]


# Получи ключ на https://console.cloud.google.com/apis/credentials
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Ваш API-ключ для CLAUDE 3.7
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")
if CLAUDE_API_KEY:
    logging.info("✅ CLAUDE_API_KEY успешно загружен!")
else:   
    logging.error("❌ Ошибка: CLAUDE_API_KEY не задан. Проверь переменные окружения!")

# Ваш API-ключ для mediastack
API_KEY_NEWS = os.getenv("API_KEY_NEWS")

# ✅ Проверяем, что категория и подкатегория соответствуют утверждённым значениям
# VALID_CATEGORIES = [
#     'Nouns', 'Cases', 'Verbs', 'Tenses', 'Adjectives', 'Adverbs', 
#     'Conjunctions', 'Prepositions', 'Moods', 'Word Order', 'Other mistake'
# ]

# VALID_SUBCATEGORIES = {
#     'Nouns': ['Gendered Articles', 'Pluralization', 'Compound Nouns', 'Declension Errors'],
#     'Cases': ['Nominative', 'Accusative', 'Dative', 'Genitive', 'Akkusativ + Preposition', 'Dative + Preposition', 'Genitive + Preposition'],
#     'Verbs': ['Placement', 'Conjugation', 'Weak Verbs', 'Strong Verbs', 'Mixed Verbs', 'Separable Verbs', 'Reflexive Verbs', 'Auxiliary Verbs', 'Modal Verbs', 'Verb Placement in Subordinate Clause'],
#     'Tenses': ['Present', 'Past', 'Simple Past', 'Present Perfect', 'Past Perfect', 'Future', 'Future 1', 'Future 2', 'Plusquamperfekt Passive', 'Futur 1 Passive', 'Futur 2 Passive'],
#     'Adjectives': ['Endings', 'Weak Declension', 'Strong Declension', 'Mixed Declension', 'Placement', 'Comparative', 'Superlative', 'Incorrect Adjective Case Agreement'],
#     'Adverbs': ['Placement', 'Multiple Adverbs', 'Incorrect Adverb Usage'],
#     'Conjunctions': ['Coordinating', 'Subordinating', 'Incorrect Use of Conjunctions'],
#     'Prepositions': ['Accusative', 'Dative', 'Genitive', 'Two-way', 'Incorrect Preposition Usage'],
#     'Moods': ['Indicative', 'Declarative', 'Interrogative', 'Imperative', 'Subjunctive 1', 'Subjunctive 2'],
#     'Word Order': ['Standard', 'Inverted', 'Verb-Second Rule', 'Position of Negation', 'Incorrect Order in Subordinate Clause', 'Incorrect Order with Modal Verb'],
#     'Other mistake': ['Unclassified mistake']
# }


# ✅ Нормализуем VALID_CATEGORIES и VALID_SUBCATEGORIES к нижнему регистру для того чтобы пройти нормально проверку в функции log_translation_mistake
# VALID_CATEGORIES_lower = [cat.lower() for cat in VALID_CATEGORIES]
# VALID_SUBCATEGORIES_lower = {k.lower(): [v.lower() for v in values] for k, values in VALID_SUBCATEGORIES.items()}

# === Подключение к базе данных PostgreSQL ===
DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY")

if DATABASE_URL:
    logging.info("✅ DATABASE_URL успешно загружен!")
else:   
    logging.error("❌ Ошибка: DATABASE_URL не задан. Проверь переменные окружения!")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# Проверка подключения
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute("SELECT version();")
db_version = cursor.fetchone()

print(f"✅ База данных подключена! Версия: {db_version}")

cursor.close()
conn.close()


async def _track_telegram_message_async(message, message_type: str = "text") -> None:
    try:
        if not message:
            return
        chat_id = int(message.chat_id)
        message_id = int(message.message_id)
        await asyncio.to_thread(
            record_telegram_system_message,
            chat_id,
            message_id,
            message_type,
        )
    except Exception:
        logging.debug("Failed to track telegram system message", exc_info=True)


def _install_tracked_send_wrappers(app: Application) -> None:
    # Backward-compatible no-op: tracking is implemented in TrackingExtBot below.
    return


class TrackingExtBot(ExtBot):
    async def _track_single(self, msg, message_type: str):
        await _track_telegram_message_async(msg, message_type)
        return msg

    @staticmethod
    def _strip_internal_kwargs(kwargs):
        kwargs.pop("language_tutor_button", None)
        return kwargs

    async def send_message(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_message(*args, **kwargs)
        return await self._track_single(msg, "text")

    async def send_photo(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_photo(*args, **kwargs)
        return await self._track_single(msg, "photo")

    async def send_audio(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_audio(*args, **kwargs)
        return await self._track_single(msg, "audio")

    async def send_voice(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_voice(*args, **kwargs)
        return await self._track_single(msg, "voice")

    async def send_document(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_document(*args, **kwargs)
        return await self._track_single(msg, "document")

    async def send_video(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_video(*args, **kwargs)
        return await self._track_single(msg, "video")

    async def send_video_note(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_video_note(*args, **kwargs)
        return await self._track_single(msg, "video_note")

    async def send_animation(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_animation(*args, **kwargs)
        return await self._track_single(msg, "animation")

    async def send_sticker(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_sticker(*args, **kwargs)
        return await self._track_single(msg, "sticker")

    async def copy_message(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg_id = await super().copy_message(*args, **kwargs)
        # copy_message returns MessageId, not Message.
        return msg_id

    async def forward_message(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().forward_message(*args, **kwargs)
        return await self._track_single(msg, "forward")

    async def send_media_group(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        messages = await super().send_media_group(*args, **kwargs)
        if messages:
            for msg in messages:
                await _track_telegram_message_async(msg, "media_group")
        return messages

    async def send_poll(self, *args, **kwargs):
        kwargs = self._strip_internal_kwargs(kwargs)
        msg = await super().send_poll(*args, **kwargs)
        return await self._track_single(msg, "poll")


async def cleanup_system_messages(context: CallbackContext) -> None:
    try:
        now = datetime.now(ZoneInfo(SYSTEM_MESSAGE_CLEANUP_TZ))
    except Exception:
        now = datetime.utcnow()
    target_date = now.date()

    pending = await asyncio.to_thread(
        get_pending_telegram_system_messages,
        target_date,
        SYSTEM_MESSAGE_CLEANUP_TZ,
        SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK,
        10000,
        SYSTEM_MESSAGE_CLEANUP_EXCLUDE_TYPES,
    )
    deleted = 0
    failed = 0
    for item in pending:
        row_id = int(item["id"])
        chat_id = int(item["chat_id"])
        message_id = int(item["message_id"])
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            await asyncio.to_thread(mark_telegram_system_message_deleted, row_id)
            deleted += 1
        except Exception as exc:
            failed += 1
            await asyncio.to_thread(mark_telegram_system_message_deleted, row_id, str(exc))
    logging.info(
        "✅ bot_3 cleanup_system_messages finished: date=%s pending=%s deleted=%s failed=%s",
        target_date.isoformat(),
        len(pending),
        deleted,
        failed,
    )

# # === Настройки бота ===
TELEGRAM_Deutsch_BOT_TOKEN = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")

if TELEGRAM_Deutsch_BOT_TOKEN:
    logging.info("✅ TELEGRAM_Deutsch_BOT_TOKEN успешно загружен!")
else:
    logging.error("❌ TELEGRAM_Deutsch_BOT_TOKEN не загружен! Проверьте переменные окружения.")

# ID группы (берём из ENV, чтобы не держать хардкод в коде)
_group_chat_id_raw = (
    os.getenv("BOT_GROUP_CHAT_ID_Deutsch")
    or os.getenv("GROUP_CHAT_ID")
    or "-1002607222537"
)
try:
    BOT_GROUP_CHAT_ID_Deutsch = int(str(_group_chat_id_raw).strip())
    logging.info("✅ GROUP_CHAT_ID успешно загружен: %s", BOT_GROUP_CHAT_ID_Deutsch)
except Exception:
    BOT_GROUP_CHAT_ID_Deutsch = -1002607222537
    logging.error(
        "❌ Некорректный GROUP_CHAT_ID=%r. Используем fallback: %s",
        _group_chat_id_raw,
        BOT_GROUP_CHAT_ID_Deutsch,
    )

DELIVERY_ROUTE_DEBUG_ENABLED = str(os.getenv("DELIVERY_ROUTE_DEBUG") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# # === Настройка DeepSeek API ===
# api_key_deepseek = os.getenv("DeepSeek_API_Key")

# if api_key_deepseek:
#     logging.info("✅ DeepSeek_API_Key успешно загружен!")
# else:
#     logging.error("❌ Ошибка: DeepSeek_API_Key не задан. Проверь переменные окружения!")


# LiveKit конфигурация
# Они нужны, чтобы  приложение имело право создавать комнаты и генерировать токены доступа.
# LIVEKIT_URL: Это WebSocket-адрес вашего сервера LiveKit. Именно по этому адресу будут подключаться и ваш агент (agent.py), и браузер пользователя (client.html).
# CLIENT_HOST: Это доменное имя, где размещен ваш client.html. Используется для построения финальной ссылки-приглашения.
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET")
LIVEKIT_URL = "wss://implemrntingvoicetobot-vhsnc86g.livekit.cloud"
#CLIENT_HOST = os.getenv("CLIENT_HOST")

if LIVEKIT_API_KEY and LIVEKIT_API_SECRET:
    logging.info("✅ LiveKit API keys и CLIENT_HOST загружены!")
else:
    logging.error("❌ LiveKit API keys или CLIENT_HOST не загружены!")

WEB_APP_URL = os.getenv("WEB_APP_URL")

if WEB_APP_URL:
    logging.info("✅ WEB_APP_URL задан (ссылка на фронтенд будет стабильной).")
    logging.info(f"WEB_APP_URL env = {os.getenv('WEB_APP_URL')!r}")
else:
    logging.warning("⚠️ WEB_APP_URL не задан: локально можно использовать ngrok/localhost.")


print("🚀 Все переменные окружения Railway:")
for key, value in os.environ.items():
    print(f"{key}: {value[:10]}...")  # Выводим первые 10 символов для безопасности




# Функция для получения новостей на немецком
async def send_german_news(context: CallbackContext):
    url = f"http://api.mediastack.com/v1/news?access_key={API_KEY_NEWS}&languages=de&technology&countries=de,au&limit=2" # Ограничим до 3 новостей
    #url = f"http://api.mediastack.com/v1/news?access_key={API_KEY_NEWS}&languages=de&countries=at&limit=3" for Austria

    response = requests.get(url)

    try:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_german_news")
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для news", exc_info=True)
        targets = []

    if not targets:
        logging.info("ℹ️ send_german_news: нет targets для рассылки")
        return

    if response.status_code == 200:
        data = response.json()

        if "data" in data and len(data["data"]) > 0:
            print("📢 Nachrichten auf Deutsch:")
            for i, article in enumerate(data["data"], start=1):  # Ограничим до 3 новостей in API request
                title = article.get("title", "Без заголовка")
                source = article.get("source", "Неизвестный источник")
                url = article.get("url", "#")

                message = f"📰 {i}. *{title}*\n\n📌 {source}\n\n[Читать полностью]({url})"
                for target_chat_id in targets:
                    try:
                        await context.bot.send_message(
                            chat_id=int(target_chat_id),
                            text=message,
                            parse_mode="Markdown",
                            disable_web_page_preview=False  # Чтобы загружались превью страниц
                        )
                    except Exception as exc:
                        logging.warning("⚠️ Не удалось отправить news в chat_id=%s: %s", target_chat_id, exc)
        else:
            for target_chat_id in targets:
                try:
                    await context.bot.send_message(chat_id=int(target_chat_id), text="❌ Нет свежих новостей на сегодня!")
                except Exception as exc:
                    logging.warning("⚠️ Не удалось отправить empty news в chat_id=%s: %s", target_chat_id, exc)
    else:
        for target_chat_id in targets:
            try:
                await context.bot.send_message(
                    chat_id=int(target_chat_id),
                    text=f"❌ Ошибка: {response.status_code} - {response.text}",
                )
            except Exception as exc:
                logging.warning("⚠️ Не удалось отправить error news в chat_id=%s: %s", target_chat_id, exc)



# Используем контекстный менеджер для того чтобы Автоматически разрывает соединение закрывая курсор и соединения
def initialise_database():
    with get_db_connection() as connection:
        with connection.cursor() as curr:

            # Table with user translations with 80 or more points
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_successful_translations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                sentence_id BIGINT,
                score INT NOT NULL,
                attempt INT NOT NULL,
                date TIMESTAMP
                );
            """)  

            # ✅ Таблица с оригинальными предложениями
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_sentences (
                        id SERIAL PRIMARY KEY,
                        sentence TEXT NOT NULL
                        
                );
            """)

            # ✅ Таблица для переводов пользователей
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translations (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        id_for_mistake_table INT,
                        session_id BIGINT,
                        username TEXT,
                        sentence_id INT NOT NULL,
                        user_translation TEXT,
                        score INT,
                        feedback TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # ✅ Новая таблица для всех сообщений пользователей (чтобы учитывать ленивых)
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_messages (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        username TEXT NOT NULL,
                        message TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # ✅ Таблица для ошибок пользователя при разговоре с агентом (Расширенная версия)
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_conversation_errors (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        session_id TEXT,          -- Важливо: ID сесії для звіту після дзвінка
                        
                        -- Основні дані про помилку
                        sentence_with_error TEXT NOT NULL,
                        corrected_sentence TEXT NOT NULL,
                        
                        -- Деталізація (як у bt_3_detailed_mistakes)
                        error_type TEXT,          -- Головна категорія (напр. Grammar)
                        error_subtype TEXT,       -- Підкатегорія (напр. Present Simple)
                        explanation_ru TEXT,      -- Пояснення російською
                        explanation_en TEXT,      -- Пояснення англійською (опціонально)
                        
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            curr.execute("""
                CREATE INDEX IF NOT EXISTS idx_voice_errors_user
                ON bt_3_conversation_errors (user_id);
            """)

            curr.execute("""
                CREATE INDEX IF NOT EXISTS idx_voice_errors_session
                ON bt_3_conversation_errors (session_id);
            """)

            # ✅ Таблица для закладок пользователей (когда пользователь говорит агенту в процессе голосового звонка в комнате что он хочет сохранить эту фразу или слово)
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_bookmarks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                session_id TEXT,
                phrase TEXT NOT NULL,
                context_note TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # ✅ Таблица daily_sentences
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_daily_sentences (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL DEFAULT CURRENT_DATE,
                        sentence TEXT NOT NULL,
                        unique_id INT NOT NULL,
                        user_id BIGINT,
                        session_id BIGINT,
                        id_for_mistake_table INT
                );
            """)

            # ✅ Таблица user_progress
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_progress (
                    session_id BIGINT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    active_seconds BIGINT,
                    active_started_at TIMESTAMPTZ,
                    active_running BOOLEAN,
                    completed BOOLEAN DEFAULT FALSE,
                    CONSTRAINT unique_user_session_bt_3 UNIQUE (user_id, start_time)
                );
            """)

            # ✅ Таблица для хранения ошибок перевода
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_errors (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        category TEXT NOT NULL CHECK (category IN ('Грамматика', 'Лексика', 'Падежи', 'Орфография', 'Синтаксис')),  
                        error_description TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # ✅ Таблица для хранения запасных предложений в случае отсутствия связи Или ошибки на стороне Open AI API
            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_spare_sentences (
                    id SERIAL PRIMARY KEY,
                    sentence TEXT NOT NULL
                );
                         
            """)

            curr.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_attempts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    id_for_mistake_table INT NOT NULL,
                    attempt INT DEFAULT 1,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT unique_attempt UNIQUE (user_id, id_for_mistake_table)
                         
                );
            """)

            # таблица для хранения id assistant API Open AI
            curr.execute("""
                CREATE TABLE IF NOT EXISTS assistants(
                    task_name TEXT PRIMARY KEY,
                    assistant_id TEXT NOT NULL
                    );
            """)


            # ✅ Таблица для хранения ошибок
            curr.execute("""
                    CREATE TABLE IF NOT EXISTS bt_3_detailed_mistakes (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        sentence TEXT NOT NULL,
                        added_data TIMESTAMP,
                        main_category TEXT CHECK (main_category IN (
                            -- 🔹 Nouns
                            'Nouns', 'Cases', 'Verbs', 'Tenses', 'Adjectives', 'Adverbs', 
                            'Conjunctions', 'Prepositions', 'Moods', 'Word Order', 'Other mistake'
                        )),  
                        sub_category TEXT CHECK (sub_category IN (
                            -- 🔹 Nouns
                            'Gendered Articles', 'Pluralization', 'Compound Nouns', 'Declension Errors',
                            
                            -- 🔹 Cases
                            'Nominative', 'Accusative', 'Dative', 'Genitive',
                            'Akkusativ + Preposition', 'Dative + Preposition', 'Genitive + Preposition',
                            
                            -- 🔹 Verbs
                            'Placement', 'Conjugation', 'Weak Verbs', 'Strong Verbs', 'Mixed Verbs', 
                            'Separable Verbs', 'Reflexive Verbs', 'Auxiliary Verbs', 'Modal Verbs',
                            'Verb Placement in Subordinate Clause',
                            
                            -- 🔹 Tenses
                            'Present', 'Past', 'Simple Past', 'Present Perfect', 
                            'Past Perfect', 'Future', 'Future 1', 'Future 2',
                            'Plusquamperfekt Passive', 'Futur 1 Passive', 'Futur 2 Passive',

                            -- 🔹 Adjectives
                            'Endings', 'Weak Declension', 'Strong Declension', 'Mixed Declension', 
                            'Placement', 'Comparative', 'Superlative', 'Incorrect Adjective Case Agreement',

                            -- 🔹 Adverbs
                            'Placement', 'Multiple Adverbs', 'Incorrect Adverb Usage',

                            -- 🔹 Conjunctions
                            'Coordinating', 'Subordinating', 'Incorrect Use of Conjunctions',

                            -- 🔹 Prepositions
                            'Accusative', 'Dative', 'Genitive', 'Two-way',
                            'Incorrect Preposition Usage',

                            -- 🔹 Moods
                            'Indicative', 'Declarative', 'Interrogative', 'Imperative',
                            'Subjunctive 1', 'Subjunctive 2',

                            -- 🔹 Word Order
                            'Standard', 'Inverted', 'Verb-Second Rule', 'Position of Negation',
                            'Incorrect Order in Subordinate Clause', 'Incorrect Order with Modal Verb',

                            -- 🔹 Other
                            'Unclassified mistake' -- Для ошибок, которые не попали в категории
                        )),
                        
                        mistake_count INT DEFAULT 1, -- Количество раз, когда ошибка была зафиксирована
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Время первой фиксации ошибки
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Время последнего появления ошибки
                        error_count_week INT DEFAULT 0, -- Количество ошибок за последнюю неделю
                        sentence_id INT,
                        correct_translation TEXT NOT NULL,
                        score INT,
                        attempt INT DEFAULT 1, 

                        -- ✅ Уникальный ключ для предотвращения дубликатов
                        CONSTRAINT for_mistakes_table_bt_3 UNIQUE (user_id, sentence, main_category, sub_category)
                    );

            """)
                         
    connection.commit()

    print("✅ Таблицы проверены и готовы к использованию.")

initialise_database()

async def log_all_messages(update: Update, context: CallbackContext):
    """Логируем ВСЕ текстовые сообщения для отладки."""
    if not BOT_DEBUG_LOG_ALL_MESSAGES:
        return
    try:
        if update.message and update.message.text:
            logging.info(f"📩 Бот получил сообщение: {update.message.text}")
        else:
            logging.warning("⚠️ update.message отсутствует или пустое.")
    except Exception as e:
        logging.error(f"❌ Ошибка логирования сообщения: {e}")
    

# Функция для добавления в словарь всех id Сообщений которые потом я буду удалять, Это служебные сообщения вспомогательные
def add_service_msg_id(context, message_id):
    context_id = id(context)
    logging.info(f"DEBUG: context_id={context_id} в add_service_msg_id, добавляем message_id={message_id}")
    if "service_message_ids" not in context.user_data:
        logging.info(f"📝 Создаём service_message_ids для user_id={context._user_id}")
        context.user_data["service_message_ids"] = []
    context.user_data["service_message_ids"].append(message_id)
    logging.info(f"DEBUG: Добавлен message_id: {message_id}, текущий список: {context.user_data['service_message_ids']}")


#Имитация набора текста с typing-индикатором
async def simulate_typing(context, chat_id, duration=3):
    """Эмулирует набор текста в чате."""
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    await asyncio.sleep(duration)  # Имитация задержки перед отправкой текста



# Buttons in Telegram
async def send_main_menu(update: Update, context: CallbackContext):
    """Принудительно обновляет главное меню с кнопками."""
    if not ENABLE_LEGACY_REPLY_KEYBOARD:
        # Legacy reply-keyboard flow is disabled: users should use WebApp.
        bot_username = context.bot.username
        if not bot_username:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
        webapp_url = get_webapp_deeplink(bot_username=bot_username)
        await update.message.reply_text(
            "✅ Используйте mini app для переводов, аналитики, словаря и карточек.\n"
            f"Открыть: {webapp_url}",
            disable_web_page_preview=True,
            reply_markup=_build_private_language_tutor_reply_keyboard()
            if update.effective_chat and update.effective_chat.type == "private"
            else None,
        )
        return

    keyboard = [
        ["📌 Выбрать тему"],  # ❗ Убедись, что текст здесь правильный
        ["🚀 Начать перевод", "✅ Завершить перевод"],
        ["📜 Проверить перевод", "🟡 Посмотреть свою статистику"],
        ["🎙 Начать урок", "👥 Групповой звонок"],
        ["💬 Перейти в личку"],
        [LANGUAGE_TUTOR_BUTTON_TEXT],
    ]
    
    # создаем в словаре клю service_message_ids Список для хранения всех id Сообщений, Для того чтобы потом можно было их удалить после выполнения перевода
    context.user_data.setdefault("service_message_ids", [])

    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    chat_type = update.effective_chat.type if update.effective_chat else "private"
    if chat_type in ("group", "supergroup"):
        # ReplyKeyboard в группе больше не используем.
        # await update.message.reply_text("⏳ Обновляем меню...", reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True))
        # await update.message.reply_text("Используйте кнопки:", reply_markup=reply_markup)
        bot_username = context.bot.username
        if not bot_username:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
        webapp_url = get_webapp_deeplink(bot_username=bot_username)
        await update.message.reply_text(
            "✅ В группе используем Web App.\n"
            f"Открыть приложение: {webapp_url}",
            disable_web_page_preview=True,
        )
        return

    # 1️⃣ Удаляем старую клавиатуру
    await update.message.reply_text("⏳ Обновляем меню...", reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True))

    # 2️⃣ Отправляем новое меню (только в личке)
    #await update.message.reply_text("Используйте кнопки:", reply_markup=reply_markup)

async def debug_message_handler(update: Update, context: CallbackContext):
    print(f"🔹 Получено сообщение (DEBUG): {update.message.text}")

import requests

def get_ngrok_url():
    """Возвращает текущий публичный URL ngrok (https)."""
    try:
        # /api/tunnels — Ngrok по этому адресу отдает список всех активных туннелей в формате JSON.
        response = requests.get("http://127.0.0.1:4040/api/tunnels")
        data = response.json()
        https_tunnel = next(
            (tunnel for tunnel in data["tunnels"] if tunnel["public_url"].startswith("https")), 
            None
        )
        if https_tunnel:
            print(f"🌍 Найден ngrok URL: {https_tunnel['public_url']}")
            return https_tunnel["public_url"]
        else:
            print("⚠️ HTTPS туннель не найден.")
            return None
    except Exception as e:
        print(f"❌ Ошибка при получении ngrok URL: {e}")
        return None
    
def get_public_web_url():
    url = os.getenv("WEB_APP_URL")
    if url:
        cleaned_url = url.rstrip("/")  # чтобы не было двойных //
        if cleaned_url.endswith("/webapp"):
            return cleaned_url[: -len("/webapp")]
        return cleaned_url

    # 2) Локально (по желанию): fallback
    ngrok_url = get_ngrok_url()
    if ngrok_url:
        return ngrok_url.rstrip("/")
    
    return "http://localhost:8000"  # Локальный fallback (если нужно)

def get_webapp_url():
    base_url = get_public_web_url()
    if base_url.endswith("/webapp"):
        return base_url
    return f"{base_url}/webapp"


def get_webapp_deeplink(path: str = "review", bot_username: str | None = None) -> str:
    resolved_username = (bot_username or os.getenv("TELEGRAM_BOT_USERNAME") or "").lstrip("@")
    if resolved_username:
        return f"https://t.me/{resolved_username}?startapp={path}"
    return f"{get_webapp_url()}/{path}"


def _mobile_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _issue_mobile_access_token(user_id: int, username: str | None = None) -> str:
    payload = {
        "uid": int(user_id),
        "usr": (username or "").strip(),
        "exp": int(datetime.utcnow().timestamp()) + max(60, MOBILE_AUTH_TTL_SECONDS),
    }
    payload_raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    secret = (os.getenv("MOBILE_AUTH_SECRET") or TELEGRAM_Deutsch_BOT_TOKEN or "").strip()
    if not secret:
        raise RuntimeError("MOBILE_AUTH_SECRET/TELEGRAM_Deutsch_BOT_TOKEN не задан")
    sig = hmac.new(secret.encode("utf-8"), payload_raw, hashlib.sha256).hexdigest()
    return f"{_mobile_b64encode(payload_raw)}.{sig}"


def _is_admin_user(user_id: int | None) -> bool:
    if not user_id:
        return False
    return int(user_id) in get_admin_telegram_ids()


def _format_admin_datetime(value: str | None) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return "-"
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
        return parsed.astimezone(ZoneInfo(USER_REMOVAL_REVIEW_TZ)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return raw_value


def _build_user_removal_confirmation_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🗑 Подтвердить удаление", callback_data=f"userpurge:confirm:{int(user_id)}"),
            InlineKeyboardButton("✋ Не удалять", callback_data=f"userpurge:cancel:{int(user_id)}"),
        ]
    ])


def _build_user_removal_admin_text(removal: dict[str, Any]) -> str:
    user_id = int(removal.get("user_id") or 0)
    username = str(removal.get("username") or "unknown")
    revoked_at = _format_admin_datetime(removal.get("revoked_at"))
    grace_until = _format_admin_datetime(removal.get("grace_until"))
    reason = str(removal.get("reason") or "not specified")
    return (
        "🗑 Подтверждение purge пользователя\n"
        f"User ID: {user_id}\n"
        f"User: {username}\n"
        f"Доступ отозван: {revoked_at}\n"
        f"Grace period истёк: {grace_until}\n"
        f"Причина: {reason}\n\n"
        "После подтверждения бот удалит личные учебные данные пользователя "
        "(переводы, ошибки, словарь, reader, SRS, настройки и служебное состояние).\n"
        "Платёжные и финансовые записи в этом шаге не удаляются."
    )


def _format_subscription_cancel_result(result: dict[str, Any] | None) -> str:
    payload = result or {}
    stripe_status = str(payload.get("stripe_status") or "unknown")
    scheduled_count = len(payload.get("scheduled_ids") or [])
    already_scheduled_count = len(payload.get("already_scheduled_ids") or [])
    failed_count = len(payload.get("failed_ids") or [])
    effective_access_until = _format_admin_datetime(payload.get("effective_access_until"))
    if stripe_status in {"no_local_subscription", "no_active_subscription"}:
        return "Подписка: активной Stripe-подписки не найдено."
    if stripe_status == "scheduled_for_period_end":
        return (
            f"Подписка: автоотмена поставлена на конец оплаченного периода, "
            f"scheduled={scheduled_count}, доступ оплачен до {effective_access_until}."
        )
    if stripe_status == "already_scheduled_for_period_end":
        return (
            f"Подписка: автоотмена уже была поставлена ранее, "
            f"scheduled={already_scheduled_count}, доступ оплачен до {effective_access_until}."
        )
    if stripe_status == "stripe_unavailable":
        return "Подписка: Stripe недоступен в этом процессе, проверьте отмену отдельно."
    if stripe_status in {"partial_failure", "schedule_failed", "list_failed"}:
        return (
            f"Подписка: не удалось гарантированно поставить автоотмену "
            f"(scheduled={scheduled_count}, already={already_scheduled_count}, failed={failed_count}, status={stripe_status})."
        )
    return f"Подписка: статус {stripe_status}."


def _format_subscription_cancel_digest_marker(result: dict[str, Any] | None) -> str:
    payload = result or {}
    stripe_status = str(payload.get("stripe_status") or "unknown")
    effective_access_until_raw = str(payload.get("effective_access_until") or "").strip()
    effective_access_until = _format_admin_datetime(effective_access_until_raw) if effective_access_until_raw else "-"
    if stripe_status in {"scheduled_for_period_end", "already_scheduled_for_period_end"}:
        return f"billing=active_until:{effective_access_until}"
    if stripe_status in {"no_local_subscription", "no_active_subscription"}:
        return "billing=no_active_sub"
    if stripe_status == "stripe_unavailable":
        return "billing=stripe_unavailable"
    if stripe_status in {"partial_failure", "schedule_failed", "list_failed"}:
        return f"billing=issue:{stripe_status}"
    return f"billing={stripe_status}"


def _build_pending_purges_report_text(items: list[dict[str, Any]], *, title: str) -> str:
    if not items:
        return f"{title}\n\nОчередь удаления сейчас пуста."
    counts = Counter(str(item.get("status") or "unknown") for item in items)
    lines = [
        title,
        "",
        f"Всего в очереди: {len(items)}",
        f"Ожидают подтверждения: {counts.get('awaiting_admin_confirmation', 0)}",
        f"Ждут grace period: {counts.get('scheduled', 0)}",
        "",
    ]
    for item in items:
        user_id = int(item.get("user_id") or 0)
        username = str(item.get("username") or "unknown")
        status = str(item.get("status") or "unknown")
        grace_until = _format_admin_datetime(item.get("grace_until"))
        revoked_at = _format_admin_datetime(item.get("revoked_at"))
        reason = str(item.get("reason") or "not specified")
        billing_text = _format_subscription_cancel_digest_marker(item.get("billing_cancel_snapshot"))
        lines.append(
            f"- {user_id} ({username}) | status={status} | revoked={revoked_at} | grace_until={grace_until} | reason={reason} | {billing_text}"
        )
    return "\n".join(lines)


async def _notify_admins_due_user_removals(context: CallbackContext) -> None:
    admin_ids = get_admin_telegram_ids()
    if not admin_ids:
        logging.warning("⚠️ Нет admin ID в окружении, некуда отправить подтверждение purge.")
        return
    due_items = await asyncio.to_thread(list_due_user_removals_for_admin_confirmation, 20)
    if not due_items:
        return
    for removal in due_items:
        user_id = int(removal.get("user_id") or 0)
        if user_id <= 0:
            continue
        text = _build_user_removal_admin_text(removal)
        keyboard = _build_user_removal_confirmation_keyboard(user_id)
        sent_refs: list[dict[str, Any]] = []
        for admin_id in admin_ids:
            try:
                sent_message = await context.bot.send_message(
                    chat_id=int(admin_id),
                    text=text,
                    reply_markup=keyboard,
                )
                sent_refs.append({
                    "chat_id": int(admin_id),
                    "message_id": int(sent_message.message_id),
                })
            except Exception as exc:
                logging.warning("Не удалось отправить purge-confirmation администратору %s: %s", admin_id, exc)
        if sent_refs:
            await asyncio.to_thread(
                mark_user_removal_admin_notified,
                user_id=user_id,
                notification_message_refs=sent_refs,
            )
            logging.info("✅ Отправлено подтверждение purge user_id=%s admins=%s", user_id, len(sent_refs))
        else:
            logging.warning("⚠️ Не удалось отправить ни одного purge-confirmation для user_id=%s", user_id)


async def send_weekly_user_removal_digest(context: CallbackContext) -> None:
    if not context or not context.bot:
        return
    admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
    if not admin_ids:
        logging.warning("⚠️ Нет admin ID для weekly user removal digest.")
        return
    tz_name = USER_REMOVAL_WEEKLY_REPORT_TZ
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception:
        tz_name = "UTC"
        now_local = datetime.now(timezone.utc)
    run_period = f"{now_local.isocalendar().year}-W{int(now_local.isocalendar().week):02d}"
    items = await asyncio.to_thread(
        list_user_removal_queue,
        statuses=("scheduled", "awaiting_admin_confirmation"),
        limit=200,
    )
    message_text = _build_pending_purges_report_text(
        items,
        title=f"🗓 Еженедельный digest по очереди удаления\nWeek: {run_period}\nTimezone: {tz_name}",
    )
    for admin_id in admin_ids:
        already_sent = await asyncio.to_thread(
            has_admin_scheduler_run,
            job_key="weekly_user_removal_digest",
            run_period=run_period,
            target_chat_id=int(admin_id),
        )
        if already_sent:
            continue
        try:
            await context.bot.send_message(chat_id=int(admin_id), text=message_text)
            await asyncio.to_thread(
                mark_admin_scheduler_run,
                job_key="weekly_user_removal_digest",
                run_period=run_period,
                target_chat_id=int(admin_id),
                metadata={"tz": tz_name, "items": len(items)},
            )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить weekly user removal digest admin_id=%s: %s", admin_id, exc)


_SUPPORT_USER_ID_RE = re.compile(r"Support User ID:\s*(\d+)", re.IGNORECASE)
_SUPPORT_MESSAGE_ID_RE = re.compile(r"Support Message ID:\s*(\d+)", re.IGNORECASE)


def _extract_support_target_from_reply_text(text: str) -> tuple[int | None, int | None]:
    raw = str(text or "")
    user_match = _SUPPORT_USER_ID_RE.search(raw)
    message_match = _SUPPORT_MESSAGE_ID_RE.search(raw)
    user_id = int(user_match.group(1)) if user_match else None
    support_message_id = int(message_match.group(1)) if message_match else None
    return user_id, support_message_id


async def _try_handle_admin_support_reply(update: Update, context: CallbackContext, text: str) -> bool:
    message = update.message
    if not message or not message.from_user:
        return False
    if not _is_admin_user(message.from_user.id):
        return False
    if not (update.effective_chat and update.effective_chat.type == "private"):
        return False
    if not message.reply_to_message:
        return False

    reply_source_text = (
        (message.reply_to_message.text or "")
        or (message.reply_to_message.caption or "")
    ).strip()
    target_user_id, source_support_message_id = _extract_support_target_from_reply_text(reply_source_text)
    if not target_user_id:
        return False

    try:
        await asyncio.to_thread(
            create_support_message,
            user_id=int(target_user_id),
            from_role="admin",
            message_text=str(text or "").strip(),
            admin_telegram_id=int(message.from_user.id),
            reply_to_id=int(source_support_message_id) if source_support_message_id else None,
            is_read_by_user=False,
        )
    except Exception as exc:
        await message.reply_text(f"❌ Не удалось сохранить ответ техподдержки: {exc}")
        return True

    bot_username = context.bot.username
    if not bot_username:
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username
    webapp_url = get_webapp_deeplink("webapp", bot_username=bot_username)
    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛟 Открыть техподдержку", url=webapp_url)]]
    )
    try:
        await context.bot.send_message(
            chat_id=int(target_user_id),
            text=(
                "🛟 У вас новое сообщение от администратора в разделе «Техподдержка».\n"
                "Откройте WebApp и перейдите в этот раздел."
            ),
            reply_markup=reply_markup,
        )
    except Exception as exc:
        logging.warning("Не удалось отправить уведомление пользователю %s: %s", target_user_id, exc)

    await message.reply_text("✅ Ответ отправлен в WebApp пользователя.")
    return True


def _display_user_name(user) -> str:
    username = (getattr(user, "username", None) or "").strip()
    if username:
        return f"@{username}"
    first = (getattr(user, "first_name", None) or "").strip()
    last = (getattr(user, "last_name", None) or "").strip()
    full = " ".join(part for part in (first, last) if part).strip()
    return full or "unknown"


def _language_tutor_default_refusal() -> str:
    return (
        "Я отвечаю только на вопросы по изучению языка.\n\n"
        "Примеры подходящих вопросов:\n"
        "• В чём разница между `weil` и `denn`?\n"
        "• Почему здесь Akkusativ, а не Dativ?\n"
        "• Как естественно перевести эту фразу на немецкий?"
    )


def _language_tutor_pair_for_user(user_id: int) -> tuple[str, str]:
    try:
        source_lang, target_lang, _profile = _get_user_language_pair(int(user_id))
        return str(source_lang or "ru").strip().lower() or "ru", str(target_lang or "de").strip().lower() or "de"
    except Exception:
        return "ru", "de"


def _build_private_language_tutor_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[LANGUAGE_TUTOR_BUTTON_TEXT]],
        resize_keyboard=True,
        is_persistent=True,
    )


def _build_language_tutor_continue_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❓ Задать вопрос", callback_data="langgpt:continue")]]
    )


async def _open_language_tutor_prompt(
    message,
    *,
    user_id: int,
    continue_from_last: bool = False,
) -> None:
    pending_language_tutor_input[int(user_id)] = {
        "started_at": pytime.time(),
        "continue_from_last": bool(continue_from_last),
    }
    prompt_suffix = (
        "\n\nЯ учту контекст прошлого ответа."
        if continue_from_last
        else ""
    )
    await message.reply_text(
        "💬 Напишите одним сообщением ваш вопрос по языку.\n\n"
        "Можно спрашивать про грамматику, перевод, слова, артикли, времена, оттенки смысла и естественные формулировки.\n\n"
        "Нельзя: вопросы не про язык.\n"
        f"Для отмены напишите `cancel`.{prompt_suffix}",
        parse_mode="Markdown",
        reply_markup=_build_private_language_tutor_reply_keyboard(),
    )


def _extract_lookup_primary_target_text(lookup: dict) -> str:
    if not isinstance(lookup, dict):
        return ""
    direct = str(lookup.get("word_target") or "").strip()
    if direct:
        return direct
    translations = lookup.get("translations") if isinstance(lookup.get("translations"), list) else []
    for item in translations:
        if not isinstance(item, dict):
            continue
        candidate = str(item.get("value") or "").strip()
        if candidate:
            return candidate
    return ""


def _resolve_dictionary_speech_payload(payload: dict, *, user_id: int) -> tuple[str, str]:
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if (not source_lang or not target_lang) and "-" in str(payload.get("direction") or ""):
        source_lang, target_lang = [x.strip().lower() for x in str(payload.get("direction")).split("-", 1)]

    lookup = payload.get("lookup") if isinstance(payload.get("lookup"), dict) else {}
    source_text = str(
        lookup.get("word_source")
        or payload.get("source_text")
        or payload.get("original_query")
        or ""
    ).strip()
    target_text = _extract_lookup_primary_target_text(lookup)
    chosen_lang = ""
    chosen_text = ""
    if source_lang == "de" and source_text:
        chosen_lang, chosen_text = source_lang, source_text
    elif target_lang == "de" and target_text:
        chosen_lang, chosen_text = target_lang, target_text
    else:
        _profile_source_lang, profile_target_lang = _language_tutor_pair_for_user(int(user_id))
        if source_lang == profile_target_lang and source_text:
            chosen_lang, chosen_text = source_lang, source_text
        elif target_lang == profile_target_lang and target_text:
            chosen_lang, chosen_text = target_lang, target_text
        elif target_text and target_lang:
            chosen_lang, chosen_text = target_lang, target_text
        elif source_text and source_lang:
            chosen_lang, chosen_text = source_lang, source_text

    return chosen_lang, chosen_text


def _resolve_quiz_speech_payload(payload: dict) -> tuple[str, str]:
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    source_text = str(payload.get("source_text") or "").strip()
    target_text = str(payload.get("target_text") or "").strip()
    if source_lang == "de" and source_text:
        return source_lang, source_text
    if target_lang == "de" and target_text:
        return target_lang, target_text
    if source_text and source_lang:
        return source_lang, source_text
    if target_text and target_lang:
        return target_lang, target_text
    return "", ""


def _resolve_quiz_followup_focus_payload(payload: dict, *, user_id: int) -> dict:
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    source_text = str(payload.get("source_text") or "").strip()
    target_text = str(payload.get("target_text") or "").strip()

    _profile_source_lang, profile_target_lang = _language_tutor_pair_for_user(int(user_id))

    studied_language = ""
    studied_text = ""
    translation_language = ""
    translation_text = ""

    if source_lang == profile_target_lang and source_text:
        studied_language, studied_text = source_lang, source_text
        translation_language, translation_text = target_lang, target_text
    elif target_lang == profile_target_lang and target_text:
        studied_language, studied_text = target_lang, target_text
        translation_language, translation_text = source_lang, source_text
    elif source_lang == "de" and source_text:
        studied_language, studied_text = source_lang, source_text
        translation_language, translation_text = target_lang, target_text
    elif target_lang == "de" and target_text:
        studied_language, studied_text = target_lang, target_text
        translation_language, translation_text = source_lang, source_text
    elif source_text and source_lang:
        studied_language, studied_text = source_lang, source_text
        translation_language, translation_text = target_lang, target_text
    elif target_text and target_lang:
        studied_language, studied_text = target_lang, target_text
        translation_language, translation_text = source_lang, source_text

    return {
        "studied_language": studied_language,
        "studied_text": studied_text,
        "translation_language": translation_language,
        "translation_text": translation_text,
    }


async def _synthesize_telegram_tts_voice(lang: str, text: str) -> tuple[io.BytesIO, str]:
    normalized_lang = str(lang or "").strip().lower() or "de"
    normalized_text = re.sub(r"\s+", " ", str(text or "").strip())
    if not normalized_text:
        raise ValueError("empty_tts_text")
    preview = normalized_text[:120] + ("..." if len(normalized_text) > 120 else "")
    logging.info(
        "🔊 Bot TTS request: lang=%s chars=%s text_preview=%s",
        normalized_lang,
        len(normalized_text),
        preview,
    )
    audio_segment = await asyncio.to_thread(get_or_create_tts_clip, normalized_lang, normalized_text, 0.95)
    voice_bytes = await asyncio.to_thread(_audiosegment_to_ogg_opus_bytes, audio_segment)
    voice_buffer = io.BytesIO(voice_bytes)
    voice_buffer.name = f"listen_{normalized_lang}.ogg"
    voice_buffer.seek(0)
    return voice_buffer, normalized_text


def _audiosegment_to_mp3_bytes(audio_segment: AudioSegment) -> bytes:
    buf = io.BytesIO()
    audio_segment.export(buf, format="mp3", bitrate="192k")
    buf.seek(0)
    return buf.read()


def _audiosegment_to_ogg_opus_bytes(audio_segment: AudioSegment) -> bytes:
    buf = io.BytesIO()
    audio_segment.export(buf, format="ogg", codec="libopus", bitrate="64k")
    buf.seek(0)
    return buf.read()


async def handle_language_tutor_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query or not query.from_user:
        return
    await query.answer()
    if not (query.message and query.message.chat and query.message.chat.type == "private"):
        if query.message:
            await query.message.reply_text("Эта кнопка работает только в личке с ботом.")
        return
    if not is_telegram_user_allowed(int(query.from_user.id)):
        await query.message.reply_text("Сначала получите доступ к боту, потом сможете задавать вопросы.")
        return

    continue_from_last = str(query.data or "").strip() == "langgpt:continue"
    await _open_language_tutor_prompt(
        query.message,
        user_id=int(query.from_user.id),
        continue_from_last=continue_from_last,
    )


async def _notify_admins_access_request(context: CallbackContext, user) -> None:
    admin_ids = get_admin_telegram_ids()
    if not admin_ids:
        logging.warning("⚠️ Нет admin ID в окружении, некуда отправить запрос доступа.")
        return

    user_id = int(user.id)
    username = _display_user_name(user)
    request_id = create_access_request(
        user_id=user_id,
        username=username,
        requested_via="bot",
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"access:approve:{request_id}:{user_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"access:reject:{request_id}:{user_id}"),
            InlineKeyboardButton("⏸ Отложить", callback_data=f"access:defer:{request_id}:{user_id}"),
        ],
        [InlineKeyboardButton("📋 Pending заявки", callback_data="access:pending:list")],
    ])
    text = (
        "🔐 Новый запрос доступа к боту\n"
        f"Request ID: {request_id}\n"
        f"User ID: {user_id}\n"
        f"User: {username}\n\n"
        f"Одобрить: /allow {user_id}\n"
        f"Отклонить: /deny {user_id}"
    )
    for admin_id in admin_ids:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, reply_markup=keyboard)
        except Exception as exc:
            logging.warning(f"Не удалось отправить запрос администратору {admin_id}: {exc}")


def _request_access_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📨 Запросить доступ", callback_data="access:request")]]
    )


_ACCESS_DENIED_TEXT = (
    "⛔️ Доступ к боту закрыт.\n"
    "Нажмите кнопку ниже для отправки запроса администратору."
)


def _is_group_chat_type(chat_type: str | None) -> bool:
    normalized = str(chat_type or "").strip().lower()
    return normalized in {"group", "supergroup"}


def _open_private_chat_keyboard(bot_username: str | None) -> InlineKeyboardMarkup | None:
    username = str(bot_username or "").strip().lstrip("@")
    if not username:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬 Открыть личный чат", url=f"https://t.me/{username}?start=access")]]
    )


async def _send_access_denied_private(context: CallbackContext, user_id: int) -> bool:
    try:
        await context.bot.send_message(
            chat_id=int(user_id),
            text=_ACCESS_DENIED_TEXT,
            reply_markup=_request_access_keyboard(),
        )
        return True
    except (Forbidden, BadRequest) as exc:
        logging.info("Не удалось отправить access-denied в личку user_id=%s: %s", user_id, exc)
        return False
    except TelegramError as exc:
        logging.warning("Ошибка Telegram при отправке access-denied user_id=%s: %s", user_id, exc)
        return False


def _should_attempt_access_denied_private(context: CallbackContext, cooldown_seconds: int = 43200) -> bool:
    now_ts = int(datetime.now().timestamp())
    last_sent = int(context.user_data.get("last_access_denied_private_attempt_at", 0) or 0)
    if last_sent and (now_ts - last_sent) < max(60, int(cooldown_seconds or 43200)):
        return False
    context.user_data["last_access_denied_private_attempt_at"] = now_ts
    return True


async def _send_pending_requests_to_admin(context: CallbackContext, chat_id: int, limit: int = 20) -> None:
    items = list_pending_access_requests(limit=limit)
    if not items:
        await context.bot.send_message(chat_id=chat_id, text="✅ Pending-заявок сейчас нет.")
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📋 Pending заявки: {len(items)} шт. (показаны последние {min(limit, len(items))})",
    )

    for item in items:
        request_id = int(item["id"])
        user_id = int(item["user_id"])
        username = item.get("username") or "unknown"
        created_at = item.get("created_at") or "-"
        text = (
            f"Request #{request_id}\n"
            f"User ID: {user_id}\n"
            f"User: {username}\n"
            f"Создана: {created_at}"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Одобрить", callback_data=f"access:approve:{request_id}:{user_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"access:reject:{request_id}:{user_id}"),
                InlineKeyboardButton("⏸ Отложить", callback_data=f"access:defer:{request_id}:{user_id}"),
            ]
        ])
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)


async def handle_pending_access_list(update: Update, context: CallbackContext):
    query = update.callback_query
    admin = update.effective_user
    if not query or not admin:
        return
    if not _is_admin_user(admin.id):
        await query.answer("Команда доступна только администратору.", show_alert=True)
        return

    await query.answer("Загружаю pending-заявки…", show_alert=False)
    await _send_pending_requests_to_admin(context, chat_id=query.message.chat_id if query.message else admin.id)


def _command_name_from_text(text: str) -> str:
    command = text.split()[0].split("@")[0]
    return command.lower()


def _build_group_enroll_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Подтвердить участие", callback_data="groupenroll:confirm")]]
    )


def _message_has_group_enrollment_button(message) -> bool:
    if not message:
        return False
    reply_markup = getattr(message, "reply_markup", None)
    inline_keyboard = getattr(reply_markup, "inline_keyboard", None)
    if not isinstance(inline_keyboard, list):
        return False
    for row in inline_keyboard:
        if not isinstance(row, list):
            continue
        for button in row:
            callback_data = str(getattr(button, "callback_data", "") or "").strip()
            if callback_data == "groupenroll:confirm":
                return True
    return False


def _chat_has_group_enrollment_pin(chat) -> bool:
    pinned_message = getattr(chat, "pinned_message", None)
    return _message_has_group_enrollment_button(pinned_message)


def _is_active_member_status(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"member", "administrator", "creator", "restricted"}


async def _send_group_enrollment_prompt(
    context: CallbackContext,
    *,
    chat_id: int,
    chat_title: str | None = None,
) -> None:
    group_title = str(chat_title or "").strip()
    prefix = f"Группа: {group_title}\n\n" if group_title else ""
    text = (
        "🏁 Режим соревнования в Mini App\n\n"
        f"{prefix}"
        "Чтобы участвовать в рейтинге и групповой статистике, нажмите кнопку ниже.\n"
        "Кто не подтвердит участие, останется в группе, но в соревновании учитываться не будет.\n"
        "Если вы уже подтверждали участие раньше, повторно нажимать кнопку не нужно."
    )
    sent_message = await context.bot.send_message(
        chat_id=int(chat_id),
        text=text,
        reply_markup=_build_group_enroll_keyboard(),
    )
    try:
        await context.bot.pin_chat_message(
            chat_id=int(chat_id),
            message_id=int(sent_message.message_id),
            disable_notification=True,
        )
    except BadRequest as exc:
        error_text = str(exc or "").lower()
        logging.info("Не удалось закрепить enrollment сообщение в chat_id=%s: %s", chat_id, exc)
        if (
            "not enough rights" in error_text
            or "chat_admin_required" in error_text
            or "manage pinned messages" in error_text
        ):
            try:
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text=(
                        "ℹ️ Я отправил кнопку подтверждения, но не смог закрепить сообщение автоматически.\n"
                        "Сделайте бота администратором с правом «Закреплять сообщения» "
                        "или закрепите это сообщение вручную."
                    ),
                )
            except Exception as notify_exc:
                logging.info("Не удалось отправить подсказку про закрепление в chat_id=%s: %s", chat_id, notify_exc)
    except Exception as exc:
        logging.warning("Ошибка автозакрепления enrollment сообщения в chat_id=%s: %s", chat_id, exc)


async def _ensure_group_enrollment_prompt_for_chat(
    context: CallbackContext,
    *,
    chat_id: int,
    chat_title: str | None = None,
    source: str = "startup_backfill",
    force_send: bool = False,
) -> bool:
    if not context or not context.bot:
        return False
    safe_chat_id = int(chat_id)
    safe_source = str(source or "startup_backfill").strip()[:64] or "startup_backfill"
    run_period = datetime.now().strftime("%Y-%m-%d")

    try:
        chat = await context.bot.get_chat(safe_chat_id)
    except Exception as exc:
        logging.info("Skip enrollment ensure: chat недоступен chat_id=%s (%s)", safe_chat_id, exc)
        return False

    if not force_send and _chat_has_group_enrollment_pin(chat):
        return False

    if not force_send:
        already_processed = await asyncio.to_thread(
            has_admin_scheduler_run,
            job_key="group_enrollment_prompt",
            run_period=run_period,
            target_chat_id=safe_chat_id,
        )
        if already_processed:
            return False

    resolved_title = (
        str(chat_title or "").strip()
        or str(getattr(chat, "title", "") or "").strip()
        or None
    )
    await _send_group_enrollment_prompt(
        context,
        chat_id=safe_chat_id,
        chat_title=resolved_title,
    )
    await asyncio.to_thread(
        mark_admin_scheduler_run,
        job_key="group_enrollment_prompt",
        run_period=run_period,
        target_chat_id=safe_chat_id,
        metadata={"source": safe_source},
    )
    return True


async def backfill_group_enrollment_prompts(context: CallbackContext) -> None:
    if not context or not context.bot:
        return
    try:
        limit = int((os.getenv("GROUP_ENROLL_BACKFILL_LIMIT") or "300").strip() or "300")
    except Exception:
        limit = 300
    known_groups = await asyncio.to_thread(list_known_webapp_group_chats, max(1, limit))
    if not known_groups:
        logging.info("group enrollment backfill: known groups not found")
        return

    updated = 0
    skipped = 0
    for item in known_groups:
        chat_id = item.get("chat_id")
        if chat_id is None:
            skipped += 1
            continue
        try:
            changed = await _ensure_group_enrollment_prompt_for_chat(
                context,
                chat_id=int(chat_id),
                chat_title=item.get("chat_title"),
                source="startup_backfill",
                force_send=False,
            )
            if changed:
                updated += 1
            else:
                skipped += 1
        except Exception as exc:
            skipped += 1
            logging.info("group enrollment backfill skip chat_id=%s: %s", chat_id, exc)
        await asyncio.sleep(0.05)

    logging.info(
        "group enrollment backfill completed: total=%s updated=%s skipped=%s",
        len(known_groups),
        updated,
        skipped,
    )


async def _register_group_context_from_update(update: Update) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return

    chat_type = str(getattr(chat, "type", "") or "").strip().lower()
    if chat_type not in {"group", "supergroup"}:
        return

    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        return

    chat_title = str(getattr(chat, "title", "") or "").strip() or None
    try:
        await asyncio.to_thread(
            upsert_webapp_group_context,
            user_id=int(user.id),
            chat_id=int(chat_id),
            chat_type=chat_type,
            chat_title=chat_title,
        )
    except Exception as exc:
        logging.warning(
            "⚠️ Не удалось обновить group context user_id=%s chat_id=%s: %s",
            user.id,
            chat_id,
            exc,
        )


async def handle_group_enrollment_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    actor = update.effective_user
    message = query.message if query else None
    chat = message.chat if message else None
    if not query or not actor or not chat:
        return

    chat_type = str(getattr(chat, "type", "") or "").strip().lower()
    if chat_type not in {"group", "supergroup"}:
        await query.answer("Эта кнопка работает только в группе.", show_alert=True)
        return

    try:
        result = await asyncio.to_thread(
            confirm_webapp_group_participation,
            user_id=int(actor.id),
            chat_id=int(chat.id),
            chat_type=chat_type,
            chat_title=str(getattr(chat, "title", "") or "").strip() or None,
            source="telegram_group_callback",
        )
    except Exception as exc:
        logging.warning(
            "⚠️ Не удалось подтвердить участие user_id=%s chat_id=%s: %s",
            actor.id,
            chat.id,
            exc,
        )
        await query.answer("Не удалось подтвердить участие. Попробуйте ещё раз.", show_alert=True)
        return

    if bool(result.get("was_confirmed_before")):
        await query.answer("Вы уже участвуете в соревновании этой группы.", show_alert=False)
        return
    await query.answer("Готово! Вы участвуете в рейтинге этой группы.", show_alert=False)


async def handle_bot_group_membership(update: Update, context: CallbackContext) -> None:
    membership_update = getattr(update, "my_chat_member", None)
    if not membership_update:
        return

    chat = membership_update.chat
    chat_type = str(getattr(chat, "type", "") or "").strip().lower()
    if chat_type not in {"group", "supergroup"}:
        return

    old_status = str(getattr(membership_update.old_chat_member, "status", "") or "").strip().lower()
    new_status = str(getattr(membership_update.new_chat_member, "status", "") or "").strip().lower()
    if not _is_active_member_status(new_status):
        return
    if _is_active_member_status(old_status):
        return

    try:
        await _ensure_group_enrollment_prompt_for_chat(
            context,
            chat_id=int(chat.id),
            chat_title=str(getattr(chat, "title", "") or "").strip() or None,
            source="my_chat_member",
            force_send=False,
        )
    except Exception as exc:
        logging.warning("⚠️ Не удалось отправить enrollment prompt в chat_id=%s: %s", getattr(chat, "id", None), exc)


async def track_group_member_context(update: Update, context: CallbackContext) -> None:
    membership_update = getattr(update, "chat_member", None)
    if not membership_update:
        return

    chat = membership_update.chat
    chat_type = str(getattr(chat, "type", "") or "").strip().lower()
    if chat_type not in {"group", "supergroup"}:
        return

    new_member = getattr(membership_update, "new_chat_member", None)
    if not new_member:
        return

    new_status = str(getattr(new_member, "status", "") or "").strip().lower()
    if new_status not in {"member", "administrator", "creator", "restricted"}:
        return

    target_user = getattr(new_member, "user", None)
    if not target_user:
        return

    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        return

    chat_title = str(getattr(chat, "title", "") or "").strip() or None
    try:
        await asyncio.to_thread(
            upsert_webapp_group_context,
            user_id=int(target_user.id),
            chat_id=int(chat_id),
            chat_type=chat_type,
            chat_title=chat_title,
        )
    except Exception as exc:
        logging.warning(
            "⚠️ Не удалось зафиксировать join context user_id=%s chat_id=%s: %s",
            getattr(target_user, "id", None),
            chat_id,
            exc,
        )


async def enforce_user_access(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return

    # Poll answers must be processed to deliver private quiz feedback.
    if getattr(update, "poll_answer", None):
        return

    await _register_group_context_from_update(update)

    if is_telegram_user_allowed(int(user.id)):
        return

    message = update.effective_message
    text = (message.text or "").strip() if message and getattr(message, "text", None) else ""
    allowed_commands_for_new_user = {"/start", "/request_access"}
    if text.startswith("/"):
        if _command_name_from_text(text) in allowed_commands_for_new_user:
            return

    if update.callback_query and (update.callback_query.data or "") == "access:request":
        return

    chat = update.effective_chat
    is_group_chat = _is_group_chat_type(getattr(chat, "type", None))

    if is_group_chat:
        trigger_kind = "callback" if update.callback_query else "message" if message else "other"
        trigger_preview = (
            str((update.callback_query.data or "")).strip()
            if update.callback_query
            else text
        )
        if len(trigger_preview) > 120:
            trigger_preview = f"{trigger_preview[:117]}..."
        chat_title = str(getattr(chat, "title", "") or "").strip() or None
        username = str(getattr(user, "username", "") or "").strip() or None
        private_attempt_allowed = _should_attempt_access_denied_private(context)
        logging.info(
            "🚫 access_guard blocked user_id=%s username=%s chat_id=%s chat_title=%r trigger_kind=%s trigger=%r private_attempt=%s",
            int(user.id),
            username,
            getattr(chat, "id", None),
            chat_title,
            trigger_kind,
            trigger_preview,
            private_attempt_allowed,
        )
        delivered_private = False
        if private_attempt_allowed:
            delivered_private = await _send_access_denied_private(context, int(user.id))
        if update.callback_query:
            try:
                if delivered_private:
                    await update.callback_query.answer(
                        "Я отправил инструкцию в личные сообщения.",
                        show_alert=True,
                    )
                else:
                    await update.callback_query.answer(
                        "Не могу написать в личку. Откройте бота в личке и нажмите /start.",
                        show_alert=True,
                    )
            except Exception:
                pass
    elif update.callback_query:
        try:
            await update.callback_query.answer("Доступ закрыт. Ожидайте одобрения администратора.", show_alert=True)
        except Exception:
            pass
    elif message:
        await message.reply_text(
            _ACCESS_DENIED_TEXT,
            reply_markup=_request_access_keyboard(),
        )

    raise ApplicationHandlerStop


async def request_access(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or not update.effective_message:
        return

    if is_telegram_user_allowed(int(user.id)):
        await update.effective_message.reply_text("✅ У вас уже есть доступ.")
        return

    now_ts = int(datetime.now().timestamp())
    last_sent = int(context.user_data.get("last_access_request_at", 0))
    if now_ts - last_sent < 60:
        await update.effective_message.reply_text("⏳ Запрос уже отправлен недавно. Подождите минуту.")
        return

    context.user_data["last_access_request_at"] = now_ts
    await _notify_admins_access_request(context, user)

    chat = update.effective_chat
    is_group_chat = _is_group_chat_type(getattr(chat, "type", None))
    confirmation_text = f"📨 Запрос отправлен администратору.\nВаш ID: {user.id}\nОжидайте подтверждения."

    if is_group_chat:
        try:
            await context.bot.send_message(chat_id=int(user.id), text=confirmation_text)
        except (Forbidden, BadRequest):
            await update.effective_message.reply_text(
                "ℹ️ Я не могу написать вам в личку.\n"
                "Откройте чат с ботом, нажмите /start и повторите запрос доступа.",
                reply_markup=_open_private_chat_keyboard(getattr(context.bot, "username", None)),
            )
        except TelegramError as exc:
            logging.warning("Ошибка Telegram при отправке подтверждения запроса user_id=%s: %s", user.id, exc)
            await update.effective_message.reply_text(
                "⚠️ Запрос отправлен администратору, но не удалось отправить подтверждение в личку."
            )
    else:
        await update.effective_message.reply_text(confirmation_text)


async def request_access_from_button(update: Update, context: CallbackContext):
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return

    if is_telegram_user_allowed(int(user.id)):
        await query.answer("У вас уже есть доступ.", show_alert=True)
        return

    now_ts = int(datetime.now().timestamp())
    last_sent = int(context.user_data.get("last_access_request_at", 0))
    if now_ts - last_sent < 60:
        await query.answer("Запрос уже отправлен недавно. Подождите минуту.", show_alert=True)
        return

    context.user_data["last_access_request_at"] = now_ts
    await _notify_admins_access_request(context, user)
    await query.answer("Запрос отправлен администратору.", show_alert=True)

    chat = query.message.chat if query.message else None
    is_group_chat = _is_group_chat_type(getattr(chat, "type", None))
    confirmation_text = "📨 Запрос отправлен администратору. Ожидайте подтверждения."

    if is_group_chat:
        try:
            await context.bot.send_message(chat_id=int(user.id), text=confirmation_text)
        except (Forbidden, BadRequest):
            if query.message:
                await query.message.reply_text(
                    "ℹ️ Я не могу написать вам в личку.\n"
                    "Откройте чат с ботом, нажмите /start и повторите запрос доступа.",
                    reply_markup=_open_private_chat_keyboard(getattr(context.bot, "username", None)),
                )
        except TelegramError as exc:
            logging.warning("Ошибка Telegram при отправке подтверждения из кнопки user_id=%s: %s", user.id, exc)
    elif query.message:
        await query.message.reply_text(confirmation_text)


async def allow_user_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    if not sender or not update.effective_message:
        return
    if not _is_admin_user(sender.id):
        await update.effective_message.reply_text("⛔️ Команда доступна только администратору.")
        return
    if not context.args:
        await update.effective_message.reply_text("Использование: /allow <telegram_user_id> [username]")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ user_id должен быть числом.")
        return

    username_hint = " ".join(context.args[1:]).strip() or None
    allow_telegram_user(
        user_id=target_id,
        username=username_hint,
        added_by=int(sender.id),
        note="approved via bot command",
    )
    resolve_latest_pending_access_request_for_user(
        user_id=target_id,
        status="approved",
        reviewed_by=int(sender.id),
        review_note="approved via /allow",
    )
    cancel_telegram_user_removal(
        user_id=target_id,
        canceled_by=int(sender.id),
        note="access restored via /allow",
    )
    await update.effective_message.reply_text(f"✅ Доступ выдан пользователю {target_id}.")
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text="✅ Ваша заявка одобрена. Доступ к боту и WebApp открыт.",
        )
    except Exception as exc:
        logging.info(f"Не удалось отправить уведомление пользователю {target_id}: {exc}")


async def deny_user_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    if not sender or not update.effective_message:
        return
    if not _is_admin_user(sender.id):
        await update.effective_message.reply_text("⛔️ Команда доступна только администратору.")
        return
    if not context.args:
        await update.effective_message.reply_text("Использование: /deny <telegram_user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ user_id должен быть числом.")
        return
    username_hint = " ".join(context.args[1:]).strip() or None

    resolved = resolve_latest_pending_access_request_for_user(
        user_id=target_id,
        status="rejected",
        reviewed_by=int(sender.id),
        review_note="rejected via /deny",
    )
    removed = revoke_telegram_user(target_id)
    scheduled_removal = None
    billing_cancel_result = None
    if removed:
        billing_cancel_result = await asyncio.to_thread(schedule_user_paid_subscription_cancel_at_period_end, target_id)
        scheduled_removal = schedule_telegram_user_removal(
            user_id=target_id,
            username=(resolved or {}).get("username") or username_hint,
            scheduled_by=int(sender.id),
            reason="revoked via /deny",
        )
        await asyncio.to_thread(
            update_user_removal_billing_cancel_snapshot,
            user_id=target_id,
            billing_cancel_snapshot=billing_cancel_result,
        )
    if resolved and scheduled_removal:
        grace_until = _format_admin_datetime(scheduled_removal.get("grace_until"))
        await update.effective_message.reply_text(
            f"🚫 Заявка отклонена для пользователя {target_id}.\n"
            f"Purge поставлен в очередь. После {grace_until} бот запросит подтверждение у администратора.\n"
            f"{_format_subscription_cancel_result(billing_cancel_result)}"
        )
    elif resolved:
        await update.effective_message.reply_text(f"🚫 Заявка отклонена для пользователя {target_id}.")
    elif removed and scheduled_removal:
        grace_until = _format_admin_datetime(scheduled_removal.get("grace_until"))
        await update.effective_message.reply_text(
            f"🚫 Доступ отозван у пользователя {target_id}.\n"
            f"Purge поставлен в очередь. После {grace_until} бот запросит подтверждение у администратора.\n"
            f"{_format_subscription_cancel_result(billing_cancel_result)}"
        )
    else:
        await update.effective_message.reply_text(f"ℹ️ Нет pending-заявки и пользователя {target_id} нет в whitelist.")
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "❌ Заявка отклонена администратором."
                if not scheduled_removal else
                "🚫 Доступ к боту закрыт. Личные учебные данные поставлены в очередь на удаление, "
                "но будут удалены только после отдельного подтверждения администратора после grace period."
            ),
        )
    except Exception as exc:
        logging.info(f"Не удалось отправить уведомление пользователю {target_id}: {exc}")


async def handle_access_request_action(update: Update, context: CallbackContext):
    query = update.callback_query
    admin = update.effective_user
    if not query or not admin:
        return
    if not _is_admin_user(admin.id):
        await query.answer("Команда доступна только администратору.", show_alert=True)
        return

    match = re.match(r"^access:(approve|reject|defer):(\d+):(\d+)$", query.data or "")
    if not match:
        await query.answer("Некорректные данные кнопки.", show_alert=True)
        return

    action, request_id_raw, user_id_raw = match.groups()
    request_id = int(request_id_raw)
    target_id = int(user_id_raw)
    if action == "defer":
        await query.answer("Заявка отложена.", show_alert=False)
        if query.message:
            await query.message.reply_text(f"⏸ Заявка #{request_id} отложена. Пользователь {target_id}.")
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="⏸ Ваша заявка пока отложена. Ожидайте решения администратора.",
            )
        except Exception as exc:
            logging.info(f"Не удалось отправить уведомление пользователю {target_id}: {exc}")
        return

    decision = "approved" if action == "approve" else "rejected"

    request_row = resolve_access_request(
        request_id=request_id,
        status=decision,
        reviewed_by=int(admin.id),
        review_note=f"{decision} via inline button",
    )
    if not request_row:
        existing = get_access_request_by_id(request_id)
        existing_status = existing["status"] if existing else "unknown"
        await query.answer(f"Заявка уже обработана ({existing_status}).", show_alert=True)
        return

    username = request_row.get("username")
    if decision == "approved":
        allow_telegram_user(
            user_id=target_id,
            username=username,
            added_by=int(admin.id),
            note="approved via inline button",
        )
        cancel_telegram_user_removal(
            user_id=target_id,
            canceled_by=int(admin.id),
            note="access restored via inline button",
        )
        admin_text = f"✅ Заявка #{request_id} одобрена. Пользователь {target_id} получил доступ."
        user_text = "✅ Ваша заявка одобрена. Доступ к боту и WebApp открыт."
    else:
        removed = revoke_telegram_user(target_id)
        scheduled_removal = None
        billing_cancel_result = None
        if removed:
            billing_cancel_result = await asyncio.to_thread(schedule_user_paid_subscription_cancel_at_period_end, target_id)
            scheduled_removal = schedule_telegram_user_removal(
                user_id=target_id,
                username=username,
                scheduled_by=int(admin.id),
                reason="revoked via access inline button",
            )
            await asyncio.to_thread(
                update_user_removal_billing_cancel_snapshot,
                user_id=target_id,
                billing_cancel_snapshot=billing_cancel_result,
            )
        if scheduled_removal:
            grace_until = _format_admin_datetime(scheduled_removal.get("grace_until"))
            admin_text = (
                f"❌ Заявка #{request_id} отклонена. Пользователь {target_id}. "
                f"Purge будет вынесен на подтверждение после {grace_until}. "
                f"{_format_subscription_cancel_result(billing_cancel_result)}"
            )
            user_text = (
                "🚫 Доступ к боту закрыт. Личные учебные данные поставлены в очередь на удаление, "
                "но будут удалены только после отдельного подтверждения администратора после grace period."
            )
        else:
            admin_text = f"❌ Заявка #{request_id} отклонена. Пользователь {target_id}."
            user_text = "❌ Ваша заявка отклонена администратором."

    await query.answer("Готово.")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    if query.message:
        await query.message.reply_text(admin_text)

    try:
        await context.bot.send_message(chat_id=target_id, text=user_text)
    except Exception as exc:
        logging.info(f"Не удалось отправить уведомление пользователю {target_id}: {exc}")


async def handle_user_removal_action(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    admin = update.effective_user
    if not query or not admin:
        return
    if not _is_admin_user(admin.id):
        await query.answer("Команда доступна только администратору.", show_alert=True)
        return

    match = re.match(r"^userpurge:(confirm|cancel):(\d+)$", query.data or "")
    if not match:
        await query.answer("Некорректные данные кнопки.", show_alert=True)
        return

    action, user_id_raw = match.groups()
    target_id = int(user_id_raw)
    current = await asyncio.to_thread(get_user_removal_request, target_id)
    if not current:
        await query.answer("Очередь удаления для пользователя не найдена.", show_alert=True)
        return
    current_status = str(current.get("status") or "").strip().lower()
    if current_status in {"purged", "canceled"}:
        await query.answer(f"Запрос уже обработан ({current_status}).", show_alert=True)
        return

    if action == "cancel":
        result = await asyncio.to_thread(
            cancel_telegram_user_removal,
            user_id=target_id,
            canceled_by=int(admin.id),
            note="purge canceled via inline button",
        )
        admin_text = (
            f"✋ Purge отменён для пользователя {target_id}. "
            "Доступ остаётся отозванным, данные пока сохраняются."
        )
    else:
        result = await asyncio.to_thread(
            purge_telegram_user_personal_data,
            user_id=target_id,
            approved_by=int(admin.id),
            note="purge approved via inline button",
        )
        summary = (result or {}).get("purge_summary") or {}
        total_deleted_rows = int(summary.get("total_deleted_rows") or 0)
        admin_text = (
            f"🗑 Purge подтверждён для пользователя {target_id}. "
            f"Удалено строк: {total_deleted_rows}."
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="🗑 По решению администратора ваши личные учебные данные были удалены из приложения.",
            )
        except Exception as exc:
            logging.info("Не удалось отправить уведомление о purge пользователю %s: %s", target_id, exc)

    await query.answer("Готово.")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    if query.message:
        await query.message.reply_text(admin_text)


async def allowed_users_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    if not sender or not update.effective_message:
        return
    if not _is_admin_user(sender.id):
        await update.effective_message.reply_text("⛔️ Команда доступна только администратору.")
        return

    items = list_allowed_telegram_users(limit=50)
    if not items:
        await update.effective_message.reply_text("Список разрешённых пользователей пуст.")
        return

    lines = ["✅ Разрешённые пользователи:"]
    for item in items:
        row = f"- {item['user_id']}"
        if item.get("username"):
            row += f" ({item['username']})"
        lines.append(row)
    await update.effective_message.reply_text("\n".join(lines))


async def pending_requests_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    if not sender or not update.effective_message:
        return
    if not _is_admin_user(sender.id):
        await update.effective_message.reply_text("⛔️ Команда доступна только администратору.")
        return
    await _send_pending_requests_to_admin(context, chat_id=update.effective_message.chat_id)


async def pending_purges_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    if not sender or not update.effective_message:
        return
    if not _is_admin_user(sender.id):
        await update.effective_message.reply_text("⛔️ Команда доступна только администратору.")
        return
    items = await asyncio.to_thread(
        list_user_removal_queue,
        statuses=("scheduled", "awaiting_admin_confirmation"),
        limit=200,
    )
    text = _build_pending_purges_report_text(items, title="🧾 Очередь удаления пользователей")
    await update.effective_message.reply_text(text)


async def mobile_token_command(update: Update, context: CallbackContext):
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    if not is_telegram_user_allowed(int(user.id)):
        await message.reply_text("⛔️ Доступ закрыт. Сначала получите доступ к боту.")
        return

    username = (user.username or f"{user.first_name or ''} {user.last_name or ''}".strip() or str(user.id))
    try:
        token = _issue_mobile_access_token(user_id=int(user.id), username=username)
    except Exception as exc:
        logging.exception(f"❌ Не удалось выдать mobile token: {exc}")
        await message.reply_text("❌ Не удалось выдать mobile token. Проверьте настройки сервера.")
        return

    base_url = get_public_web_url()
    backend_check_ok = False
    backend_check_note = ""
    try:
        test_url = f"{base_url.rstrip('/')}/api/mobile/dictionary/lookup"
        resp = requests.post(
            test_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json={"word": "Haus"},
            timeout=12,
        )
        if resp.status_code == 200:
            backend_check_ok = True
            backend_check_note = "✅ backend проверил токен: OK"
        else:
            try:
                payload = resp.json()
                err_text = payload.get("error") or str(payload)
            except Exception:
                err_text = resp.text[:200]
            backend_check_note = (
                f"❌ backend проверил токен: FAIL (HTTP {resp.status_code})\n"
                f"Причина: {err_text}"
            )
    except Exception as exc:
        backend_check_note = f"❌ backend-проверка недоступна: {exc}"

    ttl_days = max(1, MOBILE_AUTH_TTL_SECONDS // 86400)
    text = (
        "📱 Mobile access token для iOS Share Extension\n\n"
        f"base_url:\n`{base_url}`\n\n"
        f"access_token:\n`{token}`\n\n"
        f"Срок действия: ~{ttl_days} дн.\n"
        "Вставьте эти значения в экран настройки iOS-приложения.\n\n"
        f"{backend_check_note}"
    )
    await message.reply_text(text, parse_mode="Markdown")


def _parse_budget_units(raw: str | None) -> int | None:
    cleaned = re.sub(r"[^\d]", "", str(raw or ""))
    if not cleaned:
        return None
    try:
        value = int(cleaned)
    except Exception:
        return None
    return value if value > 0 else None


def _format_google_tts_budget_status_text(status: dict) -> str:
    used_units = int(round(float(status.get("used_units") or 0.0)))
    base_limit = int(status.get("base_limit_units") or 0)
    extra_limit = int(status.get("extra_limit_units") or 0)
    effective_limit = int(status.get("effective_limit_units") or 0)
    remaining = int(round(float(status.get("remaining_units") or 0.0)))
    usage_ratio = float(status.get("usage_ratio") or 0.0) * 100.0
    is_blocked = bool(status.get("is_blocked"))
    block_reason = str(status.get("block_reason") or "").strip()
    period_month = str(status.get("period_month") or "—")
    notified = status.get("notified_thresholds") if isinstance(status.get("notified_thresholds"), dict) else {}
    notified_list = ", ".join(f"{key}%" for key in sorted(notified.keys(), key=lambda item: int(item))) or "нет"

    lines = [
        "🔊 Google TTS budget status",
        f"Month: {period_month}",
        f"Used: {used_units} chars",
        f"Base limit: {base_limit} chars",
        f"Extra limit: {extra_limit} chars",
        f"Effective limit: {effective_limit} chars",
        f"Remaining: {remaining} chars",
        f"Usage: {usage_ratio:.1f}%",
        f"Blocked: {'yes' if is_blocked else 'no'}",
        f"Threshold alerts sent: {notified_list}",
    ]
    if block_reason:
        lines.append(f"Block reason: {block_reason}")
    lines.extend(["", *_format_budget_command_help_lines()])
    return "\n".join(lines)


def _format_google_translate_budget_status_text(status: dict) -> str:
    used_units = int(round(float(status.get("used_units") or 0.0)))
    base_limit = int(status.get("base_limit_units") or 0)
    extra_limit = int(status.get("extra_limit_units") or 0)
    effective_limit = int(status.get("effective_limit_units") or 0)
    remaining = int(round(float(status.get("remaining_units") or 0.0)))
    usage_ratio = float(status.get("usage_ratio") or 0.0) * 100.0
    is_blocked = bool(status.get("is_blocked"))
    block_reason = str(status.get("block_reason") or "").strip()
    period_month = str(status.get("period_month") or "—")

    lines = [
        "🌐 Google Translate budget status",
        f"Month: {period_month}",
        f"Used: {used_units} chars",
        f"Base limit: {base_limit} chars",
        f"Extra limit: {extra_limit} chars",
        f"Effective limit: {effective_limit} chars",
        f"Remaining: {remaining} chars",
        f"Usage: {usage_ratio:.1f}%",
        f"Blocked: {'yes' if is_blocked else 'no'}",
    ]
    if block_reason:
        lines.append(f"Block reason: {block_reason}")
    return "\n".join(lines)


def _resolve_budget_report_period_start(now_local: datetime) -> date:
    current_month_start = date(int(now_local.year), int(now_local.month), 1)
    return current_month_start - timedelta(days=1)


def _format_budget_command_help_lines() -> list[str]:
    return [
        "Commands:",
        "/budgets",
        "/budgets sendnow",
        "/budgets add 200000",
        "/budgets block",
        "/budgets unblock",
        "/budgets translate_add 200000",
        "/budgets translate_block",
        "/budgets translate_unblock",
        "",
        "Alias: /ttsbudget",
    ]


async def _format_all_translation_budget_status_text(*, period_month: date | None = None, tz_name: str = "Europe/Vienna") -> str:
    tts_status = await asyncio.to_thread(
        get_google_tts_monthly_budget_status,
        period_month=period_month,
        tz=tz_name,
    )
    google_translate_status = await asyncio.to_thread(
        get_google_translate_monthly_budget_status,
        period_month=period_month,
        tz=tz_name,
    )
    parts: list[str] = []
    if tts_status:
        parts.append(_format_google_tts_budget_status_text(tts_status))
    else:
        parts.append("❌ Не удалось получить статус бюджета Google TTS.")
    if google_translate_status:
        parts.append(_format_google_translate_budget_status_text(google_translate_status))
    else:
        parts.append("❌ Не удалось получить статус бюджета Google Translate.")
    return "\n\n".join(parts)


async def send_monthly_budget_report(context: CallbackContext):
    if not context or not context.bot:
        return
    tz_name = (os.getenv("BUDGET_REPORT_SCHEDULER_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception:
        tz_name = "UTC"
        now_local = datetime.now(timezone.utc)
    report_period_month = _resolve_budget_report_period_start(now_local)
    run_period = report_period_month.strftime("%Y-%m")
    admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
    if not admin_ids:
        logging.warning("⚠️ Нет admin ID для monthly budget report.")
        return
    summary_text = await _format_all_translation_budget_status_text(
        period_month=report_period_month,
        tz_name=tz_name,
    )
    message_text = (
        "🗓 Ежемесячный budget report\n"
        f"Period: {run_period}\n"
        f"Timezone: {tz_name}\n\n"
        f"{summary_text}"
    )
    for admin_id in admin_ids:
        already_sent = await asyncio.to_thread(
            has_admin_scheduler_run,
            job_key="monthly_budget_report",
            run_period=run_period,
            target_chat_id=int(admin_id),
        )
        if already_sent:
            continue
        try:
            await context.bot.send_message(
                chat_id=int(admin_id),
                text=message_text,
                reply_markup=_build_tts_budget_keyboard(),
            )
            await asyncio.to_thread(
                mark_admin_scheduler_run,
                job_key="monthly_budget_report",
                run_period=run_period,
                target_chat_id=int(admin_id),
                metadata={"tz": tz_name, "source": "scheduler"},
            )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить monthly budget report admin_id=%s: %s", admin_id, exc)


async def send_monthly_budget_report_now(context: CallbackContext, admin_chat_id: int) -> bool:
    if not context or not context.bot:
        return False
    tz_name = (os.getenv("BUDGET_REPORT_SCHEDULER_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception:
        tz_name = "UTC"
        now_local = datetime.now(timezone.utc)
    report_period_month = _resolve_budget_report_period_start(now_local)
    run_period = report_period_month.strftime("%Y-%m")
    summary_text = await _format_all_translation_budget_status_text(
        period_month=report_period_month,
        tz_name=tz_name,
    )
    message_text = (
        "🧪 Budget report (manual send)\n"
        f"Period: {run_period}\n"
        f"Timezone: {tz_name}\n\n"
        f"{summary_text}"
    )
    try:
        await context.bot.send_message(
            chat_id=int(admin_chat_id),
            text=message_text,
            reply_markup=_build_tts_budget_keyboard(),
        )
        return True
    except Exception as exc:
        logging.warning("⚠️ Не удалось отправить manual budget report admin_id=%s: %s", admin_chat_id, exc)
        return False


def _build_tts_budget_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 Status All", callback_data="ttsbudget:status"),
                InlineKeyboardButton("🔄 Refresh", callback_data="ttsbudget:refresh"),
            ],
            [
                InlineKeyboardButton("➕ TTS +200k", callback_data="ttsbudget:add:200000"),
                InlineKeyboardButton("➕ TTS +500k", callback_data="ttsbudget:add:500000"),
            ],
            [InlineKeyboardButton("➕ TTS custom", callback_data="ttsbudget:addcustom")],
            [
                InlineKeyboardButton("⛔ TTS Block", callback_data="ttsbudget:block"),
                InlineKeyboardButton("✅ TTS Unblock", callback_data="ttsbudget:unblock"),
            ],
            [
                InlineKeyboardButton("➕ Translate +200k", callback_data="ttsbudget:translateadd:200000"),
                InlineKeyboardButton("➕ Translate +500k", callback_data="ttsbudget:translateadd:500000"),
            ],
            [InlineKeyboardButton("➕ Translate custom", callback_data="ttsbudget:translateaddcustom")],
            [
                InlineKeyboardButton("⛔ Translate Block", callback_data="ttsbudget:translateblock"),
                InlineKeyboardButton("✅ Translate Unblock", callback_data="ttsbudget:translateunblock"),
            ],
        ]
    )


def _clamp_tts_prewarm_quota_limit(value: int) -> int:
    return max(int(TTS_PREWARM_QUOTA_MIN), min(int(TTS_PREWARM_QUOTA_MAX), int(value or 0)))


def _get_current_tts_prewarm_quota_limit() -> int:
    settings = get_tts_prewarm_settings()
    return _clamp_tts_prewarm_quota_limit(int(settings.get("per_user_char_limit") or 0))


def _build_tts_prewarm_quota_keyboard(current_limit: int | None = None) -> InlineKeyboardMarkup:
    limit_value = _clamp_tts_prewarm_quota_limit(
        int(current_limit if current_limit is not None else _get_current_tts_prewarm_quota_limit())
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("-400", callback_data="ttsprewarmquota:delta:-400"),
                InlineKeyboardButton("-300", callback_data="ttsprewarmquota:delta:-300"),
                InlineKeyboardButton("-200", callback_data="ttsprewarmquota:delta:-200"),
                InlineKeyboardButton("-100", callback_data="ttsprewarmquota:delta:-100"),
            ],
            [
                InlineKeyboardButton(f"Current {limit_value}", callback_data="ttsprewarmquota:refresh"),
            ],
            [
                InlineKeyboardButton("+100", callback_data="ttsprewarmquota:delta:100"),
                InlineKeyboardButton("+200", callback_data="ttsprewarmquota:delta:200"),
                InlineKeyboardButton("+300", callback_data="ttsprewarmquota:delta:300"),
                InlineKeyboardButton("+400", callback_data="ttsprewarmquota:delta:400"),
            ],
            [
                InlineKeyboardButton("Refresh", callback_data="ttsprewarmquota:refresh"),
            ],
        ]
    )


async def tts_prewarm_quota_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    message = update.effective_message
    if not sender or not message:
        return
    if not _is_admin_user(sender.id):
        await message.reply_text("⛔️ Команда доступна только администратору.")
        return

    current_limit = await asyncio.to_thread(_get_current_tts_prewarm_quota_limit)
    text = await asyncio.to_thread(_build_tts_prewarm_quota_control_text)
    await message.reply_text(text, reply_markup=_build_tts_prewarm_quota_keyboard(current_limit))


async def _execute_tts_budget_action(
    *,
    action: str,
    admin_user_id: int,
    delta_units: int | None = None,
) -> str:
    normalized_action = str(action or "status").strip().lower()
    provider = "google_tts"
    provider_label = "Google TTS"
    status_loader = get_google_tts_monthly_budget_status

    if normalized_action.startswith("translate_"):
        provider = "google_translate"
        provider_label = "Google Translate"
        status_loader = get_google_translate_monthly_budget_status
        normalized_action = normalized_action[len("translate_"):]

    if normalized_action == "status":
        return await _format_all_translation_budget_status_text()

    if normalized_action == "sendnow":
        return "Используйте команду /budgets sendnow в личке с ботом."

    if normalized_action == "add":
        if not delta_units or int(delta_units) <= 0:
            return "❌ Укажите корректное количество символов."
        current = await asyncio.to_thread(status_loader)
        if not current:
            return "❌ Не удалось получить текущий статус бюджета."
        current_extra = int(current.get("extra_limit_units") or 0)
        new_extra = current_extra + int(delta_units)
        updated = await asyncio.to_thread(
            set_provider_budget_extra_limit,
            provider=provider,
            extra_limit_units=new_extra,
            metadata={
                "last_manual_add_by": int(admin_user_id),
                "last_manual_add_delta": int(delta_units),
            },
        )
        if not updated:
            return f"❌ Не удалось увеличить лимит {provider_label}."
        await asyncio.to_thread(
            set_provider_budget_block_state,
            provider=provider,
            is_blocked=False,
            block_reason=None,
        )
        return (
            "✅ Лимит увеличен.\n\n"
            f"Провайдер: {provider_label}\n"
            f"Добавлено: {int(delta_units)} chars\n"
            f"Новый extra limit: {int(new_extra)} chars\n\n"
            f"{await _format_all_translation_budget_status_text()}"
        )

    if normalized_action == "block":
        updated = await asyncio.to_thread(
            set_provider_budget_block_state,
            provider=provider,
            is_blocked=True,
            block_reason=f"Manual block by admin {int(admin_user_id)}",
        )
        if not updated:
            return f"❌ Не удалось вручную заблокировать {provider_label}."
        return await _format_all_translation_budget_status_text()

    if normalized_action == "unblock":
        updated = await asyncio.to_thread(
            set_provider_budget_block_state,
            provider=provider,
            is_blocked=False,
            block_reason=None,
        )
        if not updated:
            return f"❌ Не удалось снять блокировку {provider_label}."
        return await _format_all_translation_budget_status_text()

    return (
        "Использование:\n"
        "/budgets status\n"
        "/budgets sendnow\n"
        "/budgets add 200000\n"
        "/budgets block\n"
        "/budgets unblock\n"
        "/budgets translate_add 200000\n"
        "/budgets translate_block\n"
        "/budgets translate_unblock\n\n"
        "Alias: /ttsbudget"
    )


async def tts_budget_command(update: Update, context: CallbackContext):
    sender = update.effective_user
    message = update.effective_message
    if not sender or not message:
        return
    if not _is_admin_user(sender.id):
        await message.reply_text("⛔️ Команда доступна только администратору.")
        return

    pending_tts_budget_custom.pop(int(sender.id), None)
    args = context.args or []
    subcommand = str(args[0] if args else "status").strip().lower()
    if subcommand == "sendnow":
        sent = await send_monthly_budget_report_now(context, int(sender.id))
        if sent:
            await message.reply_text("✅ Budget report отправлен в эту личку.", reply_markup=_build_tts_budget_keyboard())
        else:
            await message.reply_text("❌ Не удалось отправить budget report в личку.")
        return
    delta_units = _parse_budget_units(args[1]) if subcommand in {"add", "translate_add"} and len(args) >= 2 else None
    if subcommand in {"add", "translate_add"} and delta_units is None:
        await message.reply_text("❌ Укажите корректное количество символов. Пример: /budgets add 200000")
        return
    response_text = await _execute_tts_budget_action(
        action=subcommand,
        admin_user_id=int(sender.id),
        delta_units=delta_units,
    )
    await message.reply_text(response_text, reply_markup=_build_tts_budget_keyboard())


async def budgets_command(update: Update, context: CallbackContext):
    await tts_budget_command(update, context)


async def handle_tts_budget_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    admin = update.effective_user
    if not query or not admin:
        return
    if not _is_admin_user(admin.id):
        await query.answer("Команда доступна только администратору.", show_alert=True)
        return

    pending_tts_budget_custom.pop(int(admin.id), None)
    match = re.match(r"^ttsbudget:(status|refresh|block|unblock|add|addcustom|translateblock|translateunblock|translateadd|translateaddcustom)(?::(\d+))?$", query.data or "")
    if not match:
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    action = str(match.group(1) or "status").strip().lower()
    if action == "refresh":
        action = "status"
    if action == "translateblock":
        action = "translate_block"
    if action == "translateunblock":
        action = "translate_unblock"
    if action == "translateadd":
        action = "translate_add"
    if action == "addcustom":
        pending_tts_budget_custom[int(admin.id)] = {
            "provider": "google_tts",
            "started_at": pytime.time(),
        }
        await query.answer("Жду число в сообщении", show_alert=False)
        if query.message:
            await query.message.reply_text(
                "Введите, на сколько символов увеличить лимит Google TTS.\n"
                "Пример: `200000`\n\n"
                "Чтобы отменить, отправьте `cancel`.",
                parse_mode="Markdown",
            )
        return
    if action == "translateaddcustom":
        pending_tts_budget_custom[int(admin.id)] = {
            "provider": "google_translate",
            "started_at": pytime.time(),
        }
        await query.answer("Жду число в сообщении", show_alert=False)
        if query.message:
            await query.message.reply_text(
                "Введите, на сколько символов увеличить лимит Google Translate.\n"
                "Пример: `200000`\n\n"
                "Чтобы отменить, отправьте `cancel`.",
                parse_mode="Markdown",
            )
        return
    delta_units = _parse_budget_units(match.group(2)) if action in {"add", "translate_add"} else None
    await query.answer("Обновляю…", show_alert=False)
    response_text = await _execute_tts_budget_action(
        action=action,
        admin_user_id=int(admin.id),
        delta_units=delta_units,
    )
    if query.message:
        try:
            await query.message.edit_text(response_text, reply_markup=_build_tts_budget_keyboard())
        except Exception:
            await query.message.reply_text(response_text, reply_markup=_build_tts_budget_keyboard())


async def handle_tts_prewarm_quota_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    admin = update.effective_user
    if not query or not admin:
        return
    if not _is_admin_user(admin.id):
        await query.answer("Команда доступна только администратору.", show_alert=True)
        return

    match = re.match(r"^ttsprewarmquota:(refresh|delta)(?::(-?\d+))?$", query.data or "")
    if not match:
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    action = str(match.group(1) or "refresh").strip().lower()
    delta_value = int(match.group(2) or 0) if action == "delta" else 0
    await query.answer("Обновляю…", show_alert=False)

    if action == "delta":
        current_settings = await asyncio.to_thread(get_tts_prewarm_settings)
        current_limit = _clamp_tts_prewarm_quota_limit(int(current_settings.get("per_user_char_limit") or 0))
        target_limit = _clamp_tts_prewarm_quota_limit(current_limit + delta_value)
        await asyncio.to_thread(
            upsert_tts_prewarm_settings,
            per_user_char_limit=target_limit,
            updated_by=int(admin.id),
        )
    current_limit = await asyncio.to_thread(_get_current_tts_prewarm_quota_limit)
    text = await asyncio.to_thread(_build_tts_prewarm_quota_control_text)
    reply_markup = _build_tts_prewarm_quota_keyboard(current_limit)
    if query.message:
        try:
            await query.message.edit_text(text, reply_markup=reply_markup)
        except Exception:
            await query.message.reply_text(text, reply_markup=reply_markup)


async def handle_button_click(update: Update, context: CallbackContext):
    """Обрабатывает нажатия на кнопки главного меню."""
    if not ENABLE_LEGACY_REPLY_KEYBOARD:
        return
    
    print("🛠 handle_button_click() вызван!")  # Логируем сам вызов функции

    if not update.message:
        print("❌ Ошибка: update.message отсутствует!")
        return
    
    text = update.message.text.strip()
    print(f"📥 Получено сообщение: {text}")

    # Добавляем message_id пользовательского сообщения в список сервисных сообщений
    add_service_msg_id(context, update.message.message_id)
    logging.info(f"📩 Добавлен message_id пользовательского сообщения: {update.message.message_id}")
    
    if text == "📌 Выбрать тему":
        await choose_topic(update, context)
    elif text == "🚀 Начать перевод":
        await letsgo(update, context)
    elif text == "✅ Завершить перевод":
        await done(update, context)
    elif text == "🟡 Посмотреть свою статистику":
        await user_stats(update, context)
    elif text == "📜 Проверить перевод":
        logging.info(f"📌 Пользователь {update.message.from_user.id} нажал кнопку '📜 Проверить перевод'. Запускаем проверку.")
        await check_translation_from_text(update, context)  # ✅ Теперь сразу запускаем проверку переводов
    elif text == "💬 Перейти в личку":
        bot_username = context.bot.username
        if not bot_username:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
        if bot_username:
            private_url = f"https://t.me/{bot_username}?start=from_group"
            await update.message.reply_text(
                f"Перейдите в личку: {private_url}",
                disable_web_page_preview=True,
            )
        else:
            await update.message.reply_text("Не удалось получить имя бота.")
    elif text == LANGUAGE_TUTOR_BUTTON_TEXT:
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text("Вопрос для GPT отправьте в личку с ботом.")
            return
        await _open_language_tutor_prompt(
            update.message,
            user_id=int(update.effective_user.id),
            continue_from_last=False,
        )
    
    elif text == "🎙 Начать урок":
        #frontend_url = "https://83df2cddf824.ngrok-free.app"
        frontend_url = await asyncio.to_thread(get_public_web_url)
        message_text = (
            "Your Room for conversation is ready\n\n"
            f'Press <a href="{frontend_url}">the link</a>, to connect the room'
        )
 
        await update.message.reply_text(
        text=message_text,
        parse_mode='HTML'
        )


        #await start_lesson(update, context)
    #elif text == "👥 Групповой звонок":
        #await group_call(update, context)


# 🔹 **Функция, которая запускает проверку переводов**
async def check_translation_from_text(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id

    # Проверяем, есть ли накопленные переводы
    if "pending_translations" not in context.user_data or not context.user_data["pending_translations"]:
        logging.info(f"❌ Пользователь {user_id} нажал '📜 Проверить перевод', но у него нет сохранённых переводов!")
        msg_1 = await update.message.reply_text("❌ У вас нет непроверенных переводов! Сначала отправьте перевод, затем нажмите '📜 Проверить перевод'.")
        logging.info(f"📩 Отправлено сообщение об отсутствии переводов с ID={msg_1.message_id}")
        add_service_msg_id(context, msg_1.message_id)
        return

    logging.info(f"📌 Пользователь {user_id} нажал кнопку '📜 Проверить перевод'. Запускаем проверку переводов.")

    # ✅ Формируем переводы в нужном формате (чтобы избежать ошибки "неверный формат")
    formatted_translations = []
    for t in context.user_data["pending_translations"]:
        match = re.match(r"^(\d+)\.\s*(.+)", t)  # Извлекаем номер и перевод
        if match:
            formatted_translations.append(f"{match.group(1)}. {match.group(2)}")

    # Если нет отформатированных переводов, выдаём ошибку
    if not formatted_translations:
        msg_2 = await update.message.reply_text("❌ Ошибка: Нет переводов для проверки!")
        logging.info(f"📩 Отправлено сообщение об отсутствии переводов for translation с ID={msg_2.message_id}")
        add_service_msg_id(context, msg_2.message_id)
        return

    # ✅ Формируем команду "/translate" с нужным форматом
    translation_text = "/translate\n" + "\n".join(formatted_translations)

    # ✅ Очищаем список ожидающих переводов (чтобы повторно не сохранялись)
    #context.user_data["pending_translations"] = []

    # ✅ Логируем перед передачей в `check_user_translation()`
    logging.info(f"📜 Передаём в check_user_translation():\n{translation_text}")

    # ✅ Отправляем текст в `check_user_translation()`
    await check_user_translation(update, context, translation_text)

    

async def start(update: Update, context: CallbackContext):
    """Запуск бота и отправка главного меню."""
    user = update.effective_user
    if user and not is_telegram_user_allowed(int(user.id)):
        await _notify_admins_access_request(context, user)
        message = update.effective_message
        chat = update.effective_chat
        is_group_chat = _is_group_chat_type(getattr(chat, "type", None))
        if is_group_chat:
            delivered_private = await _send_access_denied_private(context, int(user.id))
            if message and not delivered_private:
                await message.reply_text(
                    "ℹ️ Не могу написать вам в личные сообщения.\n"
                    "Откройте чат с ботом и нажмите /start, затем «📨 Запросить доступ».",
                    reply_markup=_open_private_chat_keyboard(getattr(context.bot, "username", None)),
                )
        elif message:
            await message.reply_text(
                "⛔️ Доступ к боту пока не выдан.\n"
                "Нажмите кнопку ниже или дождитесь подтверждения администратора.",
                reply_markup=_request_access_keyboard(),
            )
        return

    context.user_data.setdefault("service_message_ids", [])  # Инициализируем список
    await send_main_menu(update, context)

async def log_message(update: Update, context: CallbackContext):
    """логируются (сохраняются) все сообщения пользователей в базе данных"""
    if not update.message: #Если update.message отсутствует, значит, пользователь отправил что-то другое (например, фото, видео, стикер).
        return #В таком случае мы не логируем это и просто выходим из функции
    
    user = update.message.from_user # Данные о пользователе содержит ID и имя пользователя.
    message_text = update.message.text.strip() if update.message else "" #сам текст сообщения.

    if not message_text:
        return
    if not user:
        return
    safe_user_id = int(user.id)
    if _is_synthetic_telegram_user_id(safe_user_id):
        return
    if not _should_persist_message_activity(safe_user_id):
        return

    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    try:
        await asyncio.to_thread(_persist_message_activity_touch, safe_user_id, username)
    except Exception:
        logging.warning("⚠️ Ошибка при записи bt_3_messages activity for user_id=%s", safe_user_id, exc_info=True)

# утреннее приветствие членом группы
async def send_morning_reminder(context:CallbackContext):
    time_now= datetime.now().time()
    # Формируем утреннее сообщение
    message = (
        f"🌅 {'Доброе утро' if time(2, 0) < time_now < time(10, 0) else ('Добрый день' if time(10, 1) < time_now < time(17, 0) else 'Добрый вечер')}!\n\n"
        "Чтобы начать обучение, откройте приложение через кнопку WEB APP.\n\n"
        "Что доступно в приложении:\n"
        "🔹 Переводы и проверка предложений.\n"
        "🔹 Карточки (FSRS, Quiz, Blocks, Дополни предложение).\n"
        "🔹 Видео по слабым темам, словарь и читалка.\n"
        "🔹 Практика с AI-учителем и прокачка навыков.\n"
        "🔹 Дневной план и персональная аналитика прогресса.\n\n"
        "🎯 Рекомендация: откройте раздел «Задачи на день» и выполняйте план по шагам.\n"
    )

    # формируем список команд
    commands = (
        "📜 **Доступные команды:**\n"
        "📌 Выбрать тему - Выбрать тему для перевода\n"
        "🚀 Начать перевод - Получить предложение для перевода после выбора темы.\n"
        "📜 Проверить перевод - После отправки предложений, проверить перевод\n"
        "✅ Завершить перевод - Завершить перевод и зафиксировать время.\n"
        "/stats - Узнать свою статистику\n"
    )

    bot_username = context.bot.username
    if not bot_username:
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username
    webapp_url = get_webapp_deeplink("webapp", bot_username=bot_username)

    group_reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚀 Открыть Web App", url=webapp_url)]]
    )
    private_reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚀 Открыть приложение", url=webapp_url)]]
    )

    try:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_morning_reminder")
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для morning reminder", exc_info=True)
        targets = []

    if not targets:
        logging.info("ℹ️ morning reminder: нет targets для рассылки")
        return

    for target_chat_id in targets:
        try:
            if int(target_chat_id) < 0:
                await context.bot.send_message(
                    chat_id=int(target_chat_id),
                    text=message,
                    reply_markup=group_reply_markup,
                )
                continue
            private_text = (
                "ℹ️ Вы сейчас не состоите в группе, поэтому отправляю напоминание в личку.\n\n"
                f"{message}"
            )
            await context.bot.send_message(
                chat_id=int(target_chat_id),
                text=private_text,
                reply_markup=private_reply_markup,
            )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить morning reminder в chat_id=%s: %s", target_chat_id, exc)
    #await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text= commands)

async def send_flashcard_reminder(context: CallbackContext):
    base_url = get_public_web_url()
    bot_username = context.bot.username
    if not bot_username:
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username
    review_url = get_webapp_deeplink("review", bot_username=bot_username)
    message = (
        "📌 Пора повторить слова!\n"
        f'Перейти к тренировке: <a href="{review_url}">Открыть карточки</a>'
    )
    try:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_flashcard_reminder")
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для flashcard reminder", exc_info=True)
        targets = []

    if not targets:
        logging.info("ℹ️ flashcard reminder: нет targets для рассылки")
        return

    for target_chat_id in targets:
        try:
            await context.bot.send_message(
                chat_id=int(target_chat_id),
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить flashcard reminder в chat_id=%s: %s", target_chat_id, exc)



async def letsgo(update: Update, context: CallbackContext):
    user = update.message.from_user
    user_id = user.id
    chat_id = update.message.chat_id  # ✅ Исправленный атрибут
    username = user.username or user.first_name

    context.user_data.setdefault("service_message_ids", [])

     # ✅ Если словаря `start_times` нет — создаём его (это может быть в начале запуска бота, Когда ещё нет словаря)
    if "start_times" not in context.user_data:
        context.user_data["start_times"] = {}
    
    # ✅ Запоминаем время старта **для конкретного пользователя**
    context.user_data["start_times"][user_id] = datetime.now()

    # # ✅ Отправляем сообщение с таймером
    # timer_message = await update.message.reply_text(f"⏳ Время перевода: 0 мин 0 сек")

    # # ✅ Запускаем `start_timer()` с правильными аргументами
    # asyncio.create_task(start_timer(chat_id, context, timer_message.message_id, user_id))


    # 🔹 Проверяем, выбрал ли пользователь тему
    chosen_topic = context.user_data.get("chosen_topic")
    if not chosen_topic:
        msg_1 = await update.message.reply_text(
            "❌ Вы не выбрали тему! Сначала выберите тему используя кнопку '📌 Выбрать тему'"
        )
        logging.info(f"📩 Отправлено сообщение об ошибке темы с ID={msg_1.message_id}")
        add_service_msg_id(context, msg_1.message_id)
        return  # ⛔ Прерываем выполнение функции, если тема не выбрана

    conn = get_db_connection()
    cursor = conn.cursor()

    # Проверяем, не запустил ли уже пользователь перевод (но только за СЕГОДНЯ!)
    cursor.execute("""
        SELECT user_id FROM bt_3_user_progress
        WHERE user_id = %s AND start_time::date = CURRENT_DATE AND completed = FALSE;
        """, (user_id, ))
    active_session = cursor.fetchone()

    if active_session is not None:
        logging.info(f"⏳ Пользователь {username} ({user_id}) уже начал перевод сегодня.")
        #await update.message.reply_animation("https://media.giphy.com/media/3o7aD2saalBwwftBIY/giphy.gif")
        msg_2 = await update.message.reply_text("❌ Вы уже начали перевод! Завершите его перед повторным запуском нажав на кнопку '✅ Завершить перевод'")
        logging.info(f"📩 Отправлено сообщение об активной сессии с ID={msg_2.message_id}")
        add_service_msg_id(context, msg_2.message_id)
        cursor.close()
        conn.close()
        return

    # ✅ **Автоматически завершаем вчерашние сессии**
    cursor.execute("""
        UPDATE bt_3_user_progress
        SET end_time = NOW(), completed = TRUE
        WHERE user_id = %s AND start_time::date < CURRENT_DATE AND completed = FALSE;
    """, (user_id,))

    # 🔹 Генерируем session_id на основе user_id + текущего времени
    session_id = int(hashlib.md5(f"{user_id}{datetime.now()}".encode()).hexdigest(), 16) % (10 ** 12)

    # ✅ **Создаём новую запись в `user_progress`, НЕ ЗАТИРАЯ старые сессии и получаем `session_id`****
    cursor.execute("""
        INSERT INTO bt_3_user_progress (session_id, user_id, username, start_time, completed) 
        VALUES (%s, %s, %s, NOW(), FALSE);
    """, (session_id, user_id, username))
    
    conn.commit()


    # ✅ **Выдаём новые предложения**
    sentences = [s.strip() for s in await get_original_sentences(user_id, context) if s.strip()]

    if not sentences:
        msg_3 = await update.message.reply_text("❌ Ошибка: не удалось получить предложения. Попробуйте позже.")
        logging.info(f"📩 Отправлено сообщение: ❌ Ошибка: не удалось получить предложения. Попробуйте позже с ID={msg_3.message_id}")
        add_service_msg_id(context, msg_3.message_id)       
        cursor.close()
        conn.close()
        return

    # Определяем стартовый индекс (если пользователь делал /getmore)
    cursor.execute("""
        SELECT COUNT(*) FROM bt_3_daily_sentences WHERE date = CURRENT_DATE AND user_id = %s;
    """, (user_id,))
    last_index = cursor.fetchone()[0]

    # Добавляем логирование, чтобы видеть, были ли исправления
    original_sentences = sentences
    sentences = correct_numbering(sentences)

    for before, after in zip(original_sentences, sentences):
        if before != after:
            logging.info(f"⚠️ Исправлена нумерация: '{before}' → '{after}'")

    # Записываем bсе предложения в базу
    tasks = []

    for i, sentence in enumerate(sentences, start=last_index+1):
        # ✅ Проверяем, есть ли уже предложение с таким текстом
        cursor.execute("""
            SELECT id_for_mistake_table
            FROM bt_3_daily_sentences
            WHERE sentence = %s
            LIMIT 1;
        """, (sentence, ))
        result = cursor.fetchone()

        if result:
            id_for_mistake_table = result[0]
            logging.info(f"✅ Найден существующий id_for_mistake_table = {id_for_mistake_table} для текста: '{sentence}'")
        else:
            # ✅ Если текста нет — получаем максимальный ID и создаём новый
            cursor.execute("""
                SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences;
            """)
            result = cursor.fetchone()
            max_id = result[0] if result and result[0] is not None else 0
            id_for_mistake_table = max_id + 1
            logging.info(f"✅ Присваиваем новый id_for_mistake_table = {id_for_mistake_table} для текста: '{sentence}'")

        # ✅ Вставляем предложение в таблицу с id_for_mistake_table
        cursor.execute("""
            INSERT INTO bt_3_daily_sentences (date, sentence, unique_id, user_id, session_id, id_for_mistake_table)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s);
        """, (sentence, i, user_id, session_id, id_for_mistake_table))
        
        tasks.append(f"{i}. {sentence}")

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"🚀 Пользователь {username} ({user_id}) начал перевод. Записано {len(tasks)} предложений.")

    # 🔹 **Создаём пустой список для переводов пользователя**
    context.user_data["pending_translations"] = []


    # ✅ Отправляем либо ссылку на личку (в группе), либо предложения (в личке)
    task_text = "\n".join(tasks)
    print(f"Sentences before sending to the user: {task_text}")

    chat_type = update.effective_chat.type if update.effective_chat else "private"
    if chat_type in ("group", "supergroup"):
        bot_username = context.bot.username
        if not bot_username:
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username

        bot_link = f"https://t.me/{bot_username}"
        message_text = (
            f"🚀 {user.first_name}, перевод начат.\n\n"
            "Чтобы получить предложения и открыть Web App, перейдите в личку с ботом."
        )
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Открыть личку с ботом", url=bot_link)]]
        )

        msg_4 = await context.bot.send_message(
            chat_id=update.message.chat_id,
            text=message_text,
            reply_markup=reply_markup,
        )
        logging.info(f"📩 Отправлено сообщение с ссылкой в личку ID={msg_4.message_id}")
        add_service_msg_id(context, msg_4.message_id)
    else:
        text = (
            f"🚀 {user.first_name}, Вы начали перевод! Время пошло.\n\n"
            "✏️ Отправьте ваши переводы в формате:\n1. Mein Name ist Konchita.\n\n"
        )

        msg_4 = await context.bot.send_message(chat_id=update.message.chat_id, text=text)
        logging.info(f"📩 Отправлено сообщение о начале перевода с ID={msg_4.message_id}")
        add_service_msg_id(context, msg_4.message_id)

        msg_5 = await update.message.reply_text(
            f"{user.first_name}, Ваши предложения:\n{task_text}\n\n"
            #"После того как вы отправите все переводы, нажмите **'📜 Проверить перевод'**, чтобы проверить их.\n"
            #"Когда все переводы будут проверены, нажмите **'✅ Завершить перевод'**, чтобы зафиксировать время!"
        )
        logging.info(f"📩 Отправлено сообщение с предложениями с ID={msg_5.message_id}")
        add_service_msg_id(context, msg_5.message_id)



# 🔹 **Функция, которая запоминает переводы, но не проверяет их**
async def handle_user_message(update: Update, context: CallbackContext):
    # ✅ Проверяем, содержит ли update.message данные
    if update.message is None or update.message.text is None:
        logging.warning("⚠️ update.message отсутствует или пустое.")
        return  # ⛔ Прерываем выполнение, если сообщение отсутствует

    user_id = update.message.from_user.id
    text = update.message.text.strip()

    if _is_admin_user(user_id):
        if await _try_handle_admin_support_reply(update, context, text):
            return
        pending_budget = pending_tts_budget_custom.get(int(user_id))
        if pending_budget and update.effective_chat and update.effective_chat.type == "private":
            started_at = float((pending_budget or {}).get("started_at") or 0.0)
            provider = str((pending_budget or {}).get("provider") or "google_tts").strip().lower()
            if started_at > 0 and (pytime.time() - started_at) > TTS_BUDGET_CUSTOM_TTL_SECONDS:
                pending_tts_budget_custom.pop(int(user_id), None)
                await update.message.reply_text(
                    "Ожидание custom лимита истекло. Нажмите `➕ Add custom` ещё раз.",
                    parse_mode="Markdown",
                    reply_markup=_build_tts_budget_keyboard(),
                )
                return
            lowered = str(text or "").strip().lower()
            if lowered in {"cancel", "/cancel", "отмена"}:
                pending_tts_budget_custom.pop(int(user_id), None)
                await update.message.reply_text(
                    "Операция добавления custom limit отменена.",
                    reply_markup=_build_tts_budget_keyboard(),
                )
                return
            delta_units = _parse_budget_units(text)
            if not delta_units:
                await update.message.reply_text(
                    "❌ Нужна только цифра. Пример: `200000`\n"
                    "Или отправьте `cancel`, чтобы отменить.",
                    parse_mode="Markdown",
                )
                return
            pending_tts_budget_custom.pop(int(user_id), None)
            response_text = await _execute_tts_budget_action(
                action="translate_add" if provider == "google_translate" else "add",
                admin_user_id=int(user_id),
                delta_units=delta_units,
            )
            await update.message.reply_text(response_text, reply_markup=_build_tts_budget_keyboard())
            return

    pending = pending_quiz_freeform.get(user_id)
    if pending:
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text("Ответ на этот квиз отправьте в личку с ботом.")
            return

        correct_text = pending.get("correct_text") or ""
        is_correct = False
        if correct_text:
            is_correct = _quiz_text_matches(text, correct_text)
            if not is_correct:
                is_correct = await _quiz_text_matches_semantic(text, correct_text)
        try:
            quiz_data = pending.get("quiz_data") or {}
            await asyncio.to_thread(
                record_telegram_quiz_attempt,
                str(pending.get("poll_id") or ""),
                chat_id=int(quiz_data.get("chat_id") or 0),
                user_id=int(user_id),
                word_ru=(quiz_data.get("word_ru") or ""),
                quiz_type=(quiz_data.get("quiz_type") or ""),
                selected_option_index=None,
                selected_text=text,
                is_correct=bool(is_correct),
            )
        except Exception:
            logging.warning("⚠️ Не удалось записать freeform quiz attempt user_id=%s", user_id, exc_info=True)
        await _send_quiz_result_private(
            context=context,
            user_id=user_id,
            quiz_data=pending.get("quiz_data") or {},
            is_correct=is_correct,
            selected_text=text,
        )
        pending_quiz_freeform.pop(user_id, None)
        return

    pending_question = pending_quiz_question_input.get(user_id)
    if pending_question:
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text("Вопрос по квизу отправьте в личку с ботом.")
            return

        request_key = str((pending_question or {}).get("request_key") or "").strip()
        request_payload = pending_quiz_question_requests.get(request_key)
        if not request_key or not request_payload or _is_quiz_question_payload_expired(request_payload):
            pending_quiz_question_input.pop(user_id, None)
            pending_quiz_question_requests.pop(request_key, None)
            await update.message.reply_text("Этот контекст вопроса уже устарел. Нажмите кнопку под результатом квиза ещё раз.")
            return

        lowered = str(text or "").strip().lower()
        if lowered in {"cancel", "/cancel", "отмена"}:
            pending_quiz_question_input.pop(user_id, None)
            await update.message.reply_text("Ок, отменил вопрос.")
            return

        await update.message.reply_text("⏳ Думаю над ответом...")
        try:
            focus_payload = _resolve_quiz_followup_focus_payload(request_payload, user_id=int(user_id))
            llm_payload = {
                "source_language": str(request_payload.get("source_lang") or "").strip().lower(),
                "target_language": str(request_payload.get("target_lang") or "").strip().lower(),
                "source_text": str(request_payload.get("source_text") or "").strip(),
                "target_text": str(request_payload.get("target_text") or "").strip(),
                "studied_language": str(focus_payload.get("studied_language") or "").strip().lower(),
                "studied_text": str(focus_payload.get("studied_text") or "").strip(),
                "translation_language": str(focus_payload.get("translation_language") or "").strip().lower(),
                "translation_text": str(focus_payload.get("translation_text") or "").strip(),
                "learner_question": text,
            }
            llm_response = await run_quiz_followup_question(llm_payload)
            normalized = _normalize_quiz_question_llm_response(
                llm_response,
                source_lang=llm_payload["source_language"],
                target_lang=llm_payload["target_language"],
            )
            save_key = None
            if normalized["save_source_text"] and normalized["save_target_text"]:
                save_key = _store_pending_quiz_question_save_request(
                    user_id=int(user_id),
                    request_key=request_key,
                    source_text=normalized["save_source_text"],
                    target_text=normalized["save_target_text"],
                    source_lang=normalized["source_lang"],
                    target_lang=normalized["target_lang"],
                )
            reply_text = _build_quiz_question_reply_message(normalized)
            await update.message.reply_text(
                reply_text,
                disable_web_page_preview=True,
                reply_markup=_build_quiz_question_answer_keyboard(request_key=request_key, save_key=save_key),
            )
        except Exception:
            logging.exception("❌ Ошибка ответа на quiz follow-up user_id=%s", user_id)
            await update.message.reply_text("Не удалось подготовить ответ. Попробуйте чуть позже.")
        finally:
            pending_quiz_question_input.pop(user_id, None)
        return

    pending_language_question = pending_language_tutor_input.get(user_id)
    if pending_language_question:
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text("Вопрос для GPT отправьте в личку с ботом.")
            return

        started_at = float((pending_language_question or {}).get("started_at") or 0.0)
        if started_at > 0 and (pytime.time() - started_at) > LANGUAGE_TUTOR_INPUT_TTL_SECONDS:
            pending_language_tutor_input.pop(user_id, None)
            await update.message.reply_text(
                "Окно для вопроса истекло. Нажмите кнопку `Спросить у GPT` ещё раз.",
                parse_mode="Markdown",
                reply_markup=_build_private_language_tutor_reply_keyboard(),
            )
            return

        if str(text or "").strip() == LANGUAGE_TUTOR_BUTTON_TEXT:
            await _open_language_tutor_prompt(
                update.message,
                user_id=int(user_id),
                continue_from_last=bool((pending_language_question or {}).get("continue_from_last")),
            )
            return

        lowered = str(text or "").strip().lower()
        if lowered in {"cancel", "/cancel", "отмена"}:
            pending_language_tutor_input.pop(user_id, None)
            await update.message.reply_text(
                "Ок, отменил вопрос.",
                reply_markup=_build_private_language_tutor_reply_keyboard(),
            )
            return

        await update.message.reply_text(
            "⏳ Думаю над ответом...",
            reply_markup=_build_private_language_tutor_reply_keyboard(),
        )
        try:
            source_lang, target_lang = _language_tutor_pair_for_user(int(user_id))
            llm_payload = {
                "learner_question": text,
                "source_language": source_lang,
                "target_language": target_lang,
            }
            continue_from_last = bool((pending_language_question or {}).get("continue_from_last"))
            last_exchange = context.user_data.get("language_tutor_last_exchange")
            if continue_from_last and isinstance(last_exchange, dict):
                prev_question = str(last_exchange.get("question") or "").strip()
                prev_answer = str(last_exchange.get("answer") or "").strip()
                if prev_question and prev_answer:
                    llm_payload["conversation_context"] = {
                        "previous_question": prev_question,
                        "previous_answer": prev_answer,
                    }
            llm_response = await run_language_learning_private_question(llm_payload)
            normalized_tutor = _normalize_language_tutor_llm_response(
                llm_response,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            is_language_question = bool(normalized_tutor.get("is_language_question"))
            answer = str(normalized_tutor.get("answer") or "").strip()
            suggested_rephrase = str(normalized_tutor.get("suggested_rephrase") or "").strip()
            if not answer:
                answer = _language_tutor_default_refusal() if not is_language_question else "Не удалось подготовить ответ. Попробуйте переформулировать вопрос."
            if not is_language_question and suggested_rephrase:
                answer = f"{answer}\n\nНапример:\n{suggested_rephrase}"
            context.user_data["language_tutor_last_exchange"] = {
                "question": str(text or "").strip(),
                "answer": answer,
                "is_language_question": bool(is_language_question),
                "updated_at": pytime.time(),
            }
            save_key = None
            if normalized_tutor["save_source_text"] and normalized_tutor["save_target_text"]:
                save_key = _store_pending_quiz_question_save_request(
                    user_id=int(user_id),
                    request_key=f"langgpt:{int(user_id)}",
                    source_text=normalized_tutor["save_source_text"],
                    target_text=normalized_tutor["save_target_text"],
                    source_lang=normalized_tutor["source_lang"],
                    target_lang=normalized_tutor["target_lang"],
                    continue_callback_data="langgpt:continue",
                    continue_button_text="❓ Задать вопрос",
                )
            await update.message.reply_text(
                _truncate_telegram_reply_text(answer, max_chars=3000),
                disable_web_page_preview=True,
                reply_markup=_build_language_tutor_answer_keyboard(save_key=save_key),
            )
        except Exception:
            logging.exception("❌ Ошибка language tutor answer user_id=%s", user_id)
            await update.message.reply_text(
                "Не удалось подготовить ответ. Попробуйте чуть позже.",
                reply_markup=_build_private_language_tutor_reply_keyboard(),
            )
        finally:
            pending_language_tutor_input.pop(user_id, None)
        return

    # Проверяем, является ли сообщение переводом (поддержка многострочных сообщений)
    pattern = re.compile(r"^(\d+)\.\s*([^\d\n]+(?:\n[^\d\n]+)*)", re.MULTILINE)
    translations = pattern.findall(text)

    if translations:
        if "pending_translations" not in context.user_data:
            context.user_data["pending_translations"] = []

        for num, trans in translations:
            full_translation = f"{num}. {trans.strip()}"
            context.user_data["pending_translations"].append(full_translation)
            logging.info(f"📝 Добавлен перевод: {full_translation}")

        msg = await update.message.reply_text(
            "✅ Ваш перевод сохранён.\n\n"
            "Когда будете готовы, нажмите:\n"
            "📜 Проверить перевод.\n\n"
            "✅ Завершить перевод чтобы зафиксировать время.\n"
            )
        add_service_msg_id(context, msg.message_id)
    else:
        if update.effective_chat and update.effective_chat.type == "private" and text == LANGUAGE_TUTOR_BUTTON_TEXT:
            await _open_language_tutor_prompt(
                update.message,
                user_id=int(user_id),
                continue_from_last=False,
            )
            return
        if _is_menu_button_text(text):
            await handle_button_click(update, context)
            return
        if update.effective_chat and update.effective_chat.type == "private" and _is_dictionary_lookup_candidate(text):
            await _handle_private_dictionary_lookup(update, context, text)
            return
        await handle_button_click(update, context)


def _is_menu_button_text(text: str) -> bool:
    if not ENABLE_LEGACY_REPLY_KEYBOARD:
        return False
    return text in {
        "📌 Выбрать тему",
        "🚀 Начать перевод",
        "✅ Завершить перевод",
        "🟡 Посмотреть свою статистику",
        "📜 Проверить перевод",
        "💬 Перейти в личку",
        "🎙 Начать урок",
        LANGUAGE_TUTOR_BUTTON_TEXT,
    }


def _is_dictionary_lookup_candidate(text: str) -> bool:
    if not text or text.startswith("/") or len(text) > 120:
        return False
    # Allow numbers in dictionary/phrase lookup (e.g. "Top 10", "B2 level", "5 минут").
    if re.search(r"[^0-9A-Za-zА-Яа-яЁёÄÖÜäöüßẞ'\-\s.,!?;:()\"]", text):
        return False
    normalized = re.sub(r"[.,!?;:()\"]", " ", text)
    words = [part for part in re.split(r"\s+", normalized.strip()) if part]
    return 1 <= len(words) <= 15


def _dictionary_language_pairs() -> list[tuple[str, str]]:
    return [
        ("ru", "en"),
        ("en", "ru"),
        ("ru", "de"),
        ("de", "ru"),
        ("ru", "es"),
        ("es", "ru"),
        ("ru", "it"),
        ("it", "ru"),
    ]


def _build_dictionary_pair_keyboard(request_key: str) -> InlineKeyboardMarkup:
    flag_by_lang = {
        "ru": "🇷🇺",
        "de": "🇩🇪",
        "en": "🇬🇧",
        "it": "🇮🇹",
        "es": "🇪🇸",
    }
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for source_lang, target_lang in _dictionary_language_pairs():
        source_flag = flag_by_lang.get(source_lang, "🏳️")
        target_flag = flag_by_lang.get(target_lang, "🏳️")
        label = f"{source_flag} {source_lang.upper()} -> {target_flag} {target_lang.upper()}"
        row.append(
            InlineKeyboardButton(
                label,
                callback_data=f"dictpair:{request_key}:{source_lang}-{target_lang}",
            )
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def _store_pending_dictionary_lookup_request(user_id: int, text: str) -> str:
    key = hashlib.sha1(
        f"{user_id}:{text}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    pending_dictionary_lookup_requests[key] = {
        "user_id": int(user_id),
        "text": (text or "").strip(),
    }
    if len(pending_dictionary_lookup_requests) > 500:
        oldest_key = next(iter(pending_dictionary_lookup_requests))
        pending_dictionary_lookup_requests.pop(oldest_key, None)
    return key


def _build_dictionary_pair_selection_text(source_text: str) -> str:
    return (
        f"Запрос: {source_text.strip()}\n\n"
        "Выберите языковую пару для перевода:"
    )


def _build_dictionary_mode_selection_text(source_text: str, source_lang: str, target_lang: str) -> str:
    return (
        f"Запрос: {source_text.strip()}\n"
        f"Пара: {source_lang.upper()} -> {target_lang.upper()}\n\n"
        "Выберите формат ответа:\n"
        "• Быстрый перевод: короткий ответ и кнопки быстрого сохранения\n"
        "• Расширенный перевод: полный разбор, как сейчас"
    )


def _build_dictionary_mode_keyboard(request_key: str, source_lang: str, target_lang: str) -> InlineKeyboardMarkup:
    pair = f"{source_lang}-{target_lang}"
    rows = [
        [InlineKeyboardButton("⚡ Быстрый перевод", callback_data=f"dictmode:{request_key}:{pair}:quick")],
        [InlineKeyboardButton("🧠 Расширенный перевод", callback_data=f"dictmode:{request_key}:{pair}:full")],
    ]
    return InlineKeyboardMarkup(rows)


def _extract_lookup_input_from_pair_message_text(message_text: str) -> str:
    text = str(message_text or "").strip()
    if not text:
        return ""
    first_line = text.splitlines()[0] if text.splitlines() else ""
    match = re.match(r"^\s*Запрос:\s*(.+?)\s*$", first_line)
    if match:
        return str(match.group(1) or "").strip()
    return ""


def _extract_card_key_from_reply_markup(reply_markup) -> str:
    if not reply_markup:
        return ""
    rows = getattr(reply_markup, "inline_keyboard", None) or []
    for row in rows:
        for button in row or []:
            callback_data = str(getattr(button, "callback_data", "") or "").strip()
            if callback_data.startswith("dictfeel:"):
                return callback_data.split(":", 1)[1].strip()
    return ""


def _extract_dictionary_question_key_from_reply_markup(reply_markup) -> str:
    if not reply_markup:
        return ""
    rows = getattr(reply_markup, "inline_keyboard", None) or []
    for row in rows:
        for button in row or []:
            callback_data = str(getattr(button, "callback_data", "") or "").strip()
            if callback_data.startswith("quizask:"):
                return callback_data.split(":", 1)[1].strip()
    return ""


def _parse_dictionary_options_from_message_text(message_text: str) -> tuple[str, str, list[dict]]:
    text = str(message_text or "")
    lines = text.splitlines()
    options_by_idx: dict[int, dict] = {}
    source_lang = ""
    target_lang = ""
    i = 0
    while i < len(lines):
        line = str(lines[i] or "").strip()
        first_match = re.match(r"^(\d+)\.\s*([A-Za-z]{2}):\s*(.+)$", line)
        if not first_match:
            i += 1
            continue
        idx = int(first_match.group(1))
        src_lang = first_match.group(2).strip().lower()
        source_value = first_match.group(3).strip()
        tgt_lang = ""
        target_value = ""
        if i + 1 < len(lines):
            second_line = str(lines[i + 1] or "").strip()
            second_match = re.match(r"^([A-Za-z]{2}):\s*(.+)$", second_line)
            if second_match:
                tgt_lang = second_match.group(1).strip().lower()
                target_value = second_match.group(2).strip()
        if source_value and target_value:
            options_by_idx[idx] = {"source": source_value, "target": target_value}
            if not source_lang:
                source_lang = src_lang
            if not target_lang:
                target_lang = tgt_lang
            i += 2
            continue
        i += 1

    ordered = [options_by_idx[key] for key in sorted(options_by_idx.keys())][:3]
    return source_lang, target_lang, ordered


def _rebuild_dictionary_save_options_payload_from_message(query, option_key: str) -> dict | None:
    message = getattr(query, "message", None)
    if not message:
        return None
    text = str(getattr(message, "text", "") or getattr(message, "caption", "") or "")
    source_lang, target_lang, options = _parse_dictionary_options_from_message_text(text)
    if not options:
        return None
    if not source_lang or not target_lang:
        return None
    user_id = int(getattr(query.from_user, "id", 0) or 0)
    if user_id <= 0:
        return None
    reply_markup = getattr(message, "reply_markup", None)
    card_key = _extract_card_key_from_reply_markup(reply_markup)
    question_request_key = _extract_dictionary_question_key_from_reply_markup(reply_markup)
    lookup = {
        "word_source": str(options[0].get("source") or "").strip(),
        "word_target": str(options[0].get("target") or "").strip(),
        "source_lang": source_lang,
        "target_lang": target_lang,
    }
    payload = {
        "user_id": user_id,
        "card_key": card_key,
        "direction": f"{source_lang}-{target_lang}",
        "source_lang": source_lang,
        "target_lang": target_lang,
        "lookup": lookup,
        "options": options,
        "selected": [],
        "question_request_key": question_request_key,
    }
    pending_dictionary_save_options[option_key] = payload
    if card_key and card_key not in pending_dictionary_cards:
        pending_dictionary_cards[card_key] = {
            "user_id": user_id,
            "direction": f"{source_lang}-{target_lang}",
            "source_lang": source_lang,
            "target_lang": target_lang,
            "source_text": str(options[0].get("source") or "").strip(),
            "lookup": lookup,
            "saved": False,
            "original_query": str(options[0].get("source") or "").strip(),
            "question_request_key": question_request_key,
        }
    return payload


def _detect_dictionary_direction(text: str) -> str | None:
    normalized = re.sub(r"[.,!?;:()\"]", " ", text)
    has_cyr = bool(re.search(r"[А-Яа-яЁё]", normalized))
    has_lat = bool(re.search(r"[A-Za-zÄÖÜäöüßẞ]", normalized))
    if has_cyr and not has_lat:
        return "ru-de"
    if has_lat and not has_cyr:
        return "de-ru"
    return None


def _is_sentence_like_lookup(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    if "\n" in cleaned:
        return True
    tokens = re.findall(r"\S+", cleaned)
    if len(tokens) >= 4:
        return True
    if len(tokens) >= 2 and any(ch in cleaned for ch in ".!?;:"):
        return True
    return False


def _has_cyrillic_chars(text: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", str(text or "")))


def _has_latin_chars(text: str) -> bool:
    return bool(re.search(r"[A-Za-zÄÖÜäöüßẞÀ-ÿ]", str(text or "")))


def _normalize_dictionary_compare_key(text: str) -> str:
    normalized = str(text or "").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip(" \t\r\n.,!?;:()[]{}\"'«»")
    return normalized.casefold()


def _looks_like_target_language(text: str, target_lang: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    lang = str(target_lang or "").strip().lower()
    if lang == "ru":
        return _has_cyrillic_chars(cleaned)
    if lang in {"de", "en", "es", "it"}:
        return _has_latin_chars(cleaned)
    return True


def _extract_translation_candidates_for_target(lookup: dict, target_lang: str) -> list[str]:
    if not isinstance(lookup, dict):
        return []
    candidates: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        text = str(value or "").strip()
        if not text:
            return
        key = text.casefold()
        if key in seen:
            return
        seen.add(key)
        candidates.append(text)

    translations = lookup.get("translations")
    if isinstance(translations, list):
        ordered = sorted(
            [item for item in translations if isinstance(item, dict)],
            key=lambda item: (0 if bool(item.get("is_primary")) else 1),
        )
        for item in ordered:
            _push(item.get("value") or "")

    meanings = lookup.get("meanings")
    if isinstance(meanings, dict):
        primary = meanings.get("primary") if isinstance(meanings.get("primary"), dict) else None
        if primary:
            _push(primary.get("value") or "")
        secondary = meanings.get("secondary")
        if isinstance(secondary, list):
            for item in secondary:
                if isinstance(item, dict):
                    _push(item.get("value") or "")

    return [item for item in candidates if _looks_like_target_language(item, target_lang)]


async def _ensure_lookup_target_language(
    lookup: dict,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
) -> dict:
    result = dict(lookup or {})
    source_text = str(result.get("word_source") or lookup_input or "").strip()
    current_target = str(result.get("word_target") or "").strip()
    if _looks_like_target_language(current_target, target_lang):
        return result

    for candidate in _extract_translation_candidates_for_target(result, target_lang):
        if source_text and candidate.casefold() == source_text.casefold():
            continue
        result["word_target"] = candidate
        if target_lang == "ru":
            result["translation_ru"] = candidate
        elif target_lang == "de":
            result["translation_de"] = candidate
        return result

    # Last-resort language correction: ask translation helper for one clean target variant.
    try:
        translated_lines = await run_translate_subtitles_multilang(
            lines=[source_text or lookup_input or ""],
            source_lang=source_lang,
            target_lang=target_lang,
        )
        forced_target = str(translated_lines[0] or "").strip() if isinstance(translated_lines, list) and translated_lines else ""
    except Exception:
        forced_target = ""
    if forced_target and _looks_like_target_language(forced_target, target_lang):
        result["word_target"] = forced_target
        if target_lang == "ru":
            result["translation_ru"] = forced_target
        elif target_lang == "de":
            result["translation_de"] = forced_target
        if not isinstance(result.get("translations"), list):
            result["translations"] = []
        existing = result.get("translations") if isinstance(result.get("translations"), list) else []
        result["translations"] = [
            {"value": forced_target, "context": "base_fallback", "is_primary": True},
            *[
                item for item in existing
                if isinstance(item, dict) and str(item.get("value") or "").strip().casefold() != forced_target.casefold()
            ],
        ]
    return result


async def _coerce_sentence_lookup_payload(
    payload: dict,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
) -> dict:
    normalized_input = str(lookup_input or "").strip()
    if not _is_sentence_like_lookup(normalized_input):
        return payload if isinstance(payload, dict) else {}

    incoming_payload = payload if isinstance(payload, dict) else {}
    model_source_text = str(incoming_payload.get("word_source") or "").strip()
    correction_applied = bool(incoming_payload.get("correction_applied"))
    corrected_form = str(incoming_payload.get("corrected_form") or "").strip()
    if not corrected_form and model_source_text:
        if _normalize_dictionary_compare_key(model_source_text) != _normalize_dictionary_compare_key(normalized_input):
            corrected_form = model_source_text
            correction_applied = True

    forced_target = ""
    try:
        translated_lines = await run_translate_subtitles_multilang(
            lines=[normalized_input],
            source_lang=source_lang,
            target_lang=target_lang,
        )
        if isinstance(translated_lines, list) and translated_lines:
            forced_target = str(translated_lines[0] or "").strip()
    except Exception:
        logging.debug("Sentence translation fallback failed", exc_info=True)

    if not forced_target or forced_target.casefold() == normalized_input.casefold():
        return payload if isinstance(payload, dict) else {}

    result = dict(incoming_payload or {})
    effective_source = corrected_form or model_source_text or normalized_input
    result["word_source"] = effective_source
    result["word_target"] = forced_target
    result["part_of_speech"] = "phrase"
    result["correction_applied"] = bool(correction_applied)
    result["corrected_form"] = corrected_form or None

    if source_lang == "ru" and target_lang == "de":
        result["word_ru"] = effective_source
        result["translation_de"] = forced_target
    elif source_lang == "de" and target_lang == "ru":
        result["word_de"] = effective_source
        result["translation_ru"] = forced_target

    dedup_values: set[str] = {forced_target.casefold()}
    merged_translations = [{"value": forced_target, "context": "full_sentence", "is_primary": True}]
    existing = result.get("translations")
    if isinstance(existing, list):
        for item in existing:
            if not isinstance(item, dict):
                continue
            value = str(item.get("value") or "").strip()
            if not value:
                continue
            key = value.casefold()
            if key in dedup_values:
                continue
            dedup_values.add(key)
            merged_translations.append(
                {
                    "value": value,
                    "context": str(item.get("context") or "").strip(),
                    "is_primary": False,
                }
            )
            if len(merged_translations) >= 4:
                break
    result["translations"] = merged_translations

    result["meanings"] = {
        "primary": {
            "value": forced_target,
            "priority": 1,
            "context": "full_sentence",
            "example_source": effective_source,
            "example_target": forced_target,
        },
        "secondary": [],
    }

    usage_examples = [{"source": effective_source, "target": forced_target}]
    existing_examples = result.get("usage_examples")
    if isinstance(existing_examples, list):
        for item in existing_examples:
            if not isinstance(item, dict):
                continue
            src = str(item.get("source") or "").strip()
            tgt = str(item.get("target") or "").strip()
            if not src or not tgt:
                continue
            if src.casefold() == effective_source.casefold() and tgt.casefold() == forced_target.casefold():
                continue
            usage_examples.append({"source": src, "target": tgt})
            if len(usage_examples) >= 3:
                break
    result["usage_examples"] = usage_examples

    return result


async def _detect_latin_source_language(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return "de"
    if re.search(r"[ÄÖÜäöüßẞ]", cleaned):
        return "de"
    candidates = ["de", "en", "es", "it"]
    best_lang = "de"
    best_score = -1
    for lang in candidates:
        try:
            raw = await run_dictionary_lookup_multilang(
                word=cleaned,
                source_lang=lang,
                target_lang="ru",
            )
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        detected = str(raw.get("detected_language") or "").strip().lower()
        word_source = str(raw.get("word_source") or "").strip()
        word_target = str(raw.get("word_target") or "").strip()
        score = 0
        if detected == "source":
            score += 2
        if word_target and word_target.lower() != cleaned.lower():
            score += 2
        if word_source and word_source.lower() == cleaned.lower():
            score += 1
        if score > best_score:
            best_score = score
            best_lang = lang
    return best_lang


def _format_forms_block(forms: dict | None) -> list[str]:
    if not isinstance(forms, dict):
        return []
    items = [
        ("Plural", forms.get("plural")),
        ("Genitiv", forms.get("genitive") or forms.get("genitiv")),
        ("Praesens (er/sie/es)", forms.get("present_3sg") or forms.get("praesens_3sg")),
        ("Prateritum", forms.get("praeteritum")),
        ("Partizip II", forms.get("partizip2") or forms.get("perfekt")),
        ("Komparativ", forms.get("comparative") or forms.get("komparativ")),
        ("Superlativ", forms.get("superlative") or forms.get("superlativ")),
        ("Konjunktiv I", forms.get("konjunktiv1")),
        ("Konjunktiv II", forms.get("konjunktiv2")),
    ]
    lines = []
    for label, value in items:
        value_text = str(value).strip() if value is not None else ""
        if value_text:
            lines.append(f"- {label}: {value_text}")
    return lines


def _format_prefixes_block(prefixes: list | None) -> list[str]:
    if not isinstance(prefixes, list):
        return []
    lines = []
    for item in prefixes[:3]:
        if not isinstance(item, dict):
            continue
        variant = (item.get("variant") or "").strip()
        target = (item.get("translation_de") or item.get("translation_ru") or "").strip()
        if not variant and not target:
            continue
        head = variant if variant else "Вариант"
        body = f": {target}" if target else ""
        lines.append(f"- {head}{body}")
    return lines


def _format_examples_block(examples: list | None) -> list[str]:
    if not isinstance(examples, list):
        return []
    result = []
    for idx, ex in enumerate(examples[:3], start=1):
        if isinstance(ex, dict):
            source = str(ex.get("source") or "").strip()
            target = str(ex.get("target") or "").strip()
            if source and target:
                result.append(f"{idx}. {source}")
                result.append(f"   ↳ {target}")
                continue
            cleaned_obj = str(ex.get("text") or source or target).strip()
            if cleaned_obj:
                result.append(f"{idx}. {cleaned_obj}")
                continue
        elif isinstance(ex, str):
            cleaned = ex.strip()
            if cleaned:
                result.append(f"{idx}. {cleaned}")
    return result


def _dictionary_lang_flag(lang: str) -> str:
    return {
        "ru": "🇷🇺",
        "de": "🇩🇪",
        "en": "🇬🇧",
        "it": "🇮🇹",
        "es": "🇪🇸",
    }.get((lang or "").strip().lower(), "🏳️")


def _normalize_meaning_item(item: dict, fallback_priority: int) -> dict:
    if not isinstance(item, dict):
        return {}
    value = str(item.get("value") or item.get("translation") or "").strip()
    if not value:
        return {}
    context = str(item.get("context") or item.get("note") or "").strip()
    try:
        priority = int(item.get("priority") or fallback_priority)
    except Exception:
        priority = fallback_priority
    ex_source = str(item.get("example_source") or "").strip()
    ex_target = str(item.get("example_target") or "").strip()
    example_obj = item.get("example")
    if isinstance(example_obj, dict):
        ex_source = ex_source or str(example_obj.get("source") or example_obj.get("text") or "").strip()
        ex_target = ex_target or str(example_obj.get("target") or "").strip()
    return {
        "value": value,
        "context": context,
        "priority": priority,
        "example_source": ex_source,
        "example_target": ex_target,
    }


def _extract_lookup_meanings(lookup: dict) -> list[dict]:
    meanings: list[dict] = []
    if not isinstance(lookup, dict):
        return meanings

    raw_meanings = lookup.get("meanings")
    if isinstance(raw_meanings, dict):
        primary = _normalize_meaning_item(raw_meanings.get("primary") or {}, fallback_priority=1)
        if primary:
            meanings.append(primary)
        secondary_raw = raw_meanings.get("secondary")
        if isinstance(secondary_raw, list):
            for idx, item in enumerate(secondary_raw[:2], start=2):
                normalized = _normalize_meaning_item(item, fallback_priority=idx)
                if normalized:
                    meanings.append(normalized)

    if not meanings:
        translations = lookup.get("translations") if isinstance(lookup.get("translations"), list) else []
        ordered: list[dict] = []
        for item in translations:
            if not isinstance(item, dict):
                continue
            value = str(item.get("value") or "").strip()
            if not value:
                continue
            context = str(item.get("context") or "").strip()
            is_primary = bool(item.get("is_primary"))
            ordered.append({"value": value, "context": context, "is_primary": is_primary})
        ordered.sort(key=lambda x: (0 if x.get("is_primary") else 1))
        for idx, item in enumerate(ordered[:3], start=1):
            meanings.append(
                {
                    "value": item.get("value") or "",
                    "context": item.get("context") or "",
                    "priority": idx,
                    "example_source": "",
                    "example_target": "",
                }
            )

    if not meanings:
        fallback_value = str(lookup.get("word_target") or "").strip()
        if fallback_value:
            meanings.append(
                {
                    "value": fallback_value,
                    "context": "",
                    "priority": 1,
                    "example_source": "",
                    "example_target": "",
                }
            )

    usage_examples = lookup.get("usage_examples") if isinstance(lookup.get("usage_examples"), list) else []
    ex_pairs: list[tuple[str, str]] = []
    for item in usage_examples:
        if isinstance(item, dict):
            src = str(item.get("source") or "").strip()
            tgt = str(item.get("target") or "").strip()
            if src or tgt:
                ex_pairs.append((src, tgt))
        elif isinstance(item, str):
            cleaned = item.strip()
            if cleaned:
                ex_pairs.append((cleaned, ""))

    ex_idx = 0
    seen_values: set[str] = set()
    unique_meanings: list[dict] = []
    for idx, meaning in enumerate(meanings[:3], start=1):
        value = str(meaning.get("value") or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen_values:
            continue
        seen_values.add(key)
        source_ex = str(meaning.get("example_source") or "").strip()
        target_ex = str(meaning.get("example_target") or "").strip()
        if (not source_ex or not target_ex) and ex_idx < len(ex_pairs):
            src, tgt = ex_pairs[ex_idx]
            ex_idx += 1
            source_ex = source_ex or src
            target_ex = target_ex or tgt
        unique_meanings.append(
            {
                "value": value,
                "context": str(meaning.get("context") or "").strip(),
                "priority": idx,
                "example_source": source_ex,
                "example_target": target_ex,
            }
        )
    return unique_meanings


def _apply_article_for_display(value: str, lookup: dict, target_lang: str) -> str:
    text = (value or "").strip()
    if not text or not isinstance(lookup, dict):
        return text
    part_of_speech = str(lookup.get("part_of_speech") or "").strip().lower()
    article = str(lookup.get("article") or "").strip()
    if part_of_speech != "noun" or not article:
        return text
    if (target_lang or "").strip().lower() not in {"de", "es", "it", "en"}:
        return text
    lowered = text.lower()
    article_lower = article.lower()
    if lowered == article_lower or lowered.startswith(f"{article_lower} "):
        return text
    return f"{article} {text}".strip()


def _extract_learning_notes(lookup: dict) -> dict:
    if not isinstance(lookup, dict):
        return {
            "etymology_note": "",
            "usage_note": "",
            "real_life_usage": "",
            "register_note": "",
            "memory_tip": "",
            "expression_note": "",
            "part_of_speech_note": "",
        }
    return {
        "etymology_note": str(lookup.get("etymology_note") or "").strip(),
        "usage_note": str(lookup.get("usage_note") or "").strip(),
        "real_life_usage": str(lookup.get("real_life_usage") or "").strip(),
        "register_note": str(lookup.get("register_note") or "").strip(),
        "memory_tip": str(lookup.get("memory_tip") or "").strip(),
        "expression_note": str(lookup.get("expression_note") or "").strip(),
        "part_of_speech_note": str(lookup.get("part_of_speech_note") or "").strip(),
    }


def _extract_translation_variants(lookup: dict, target_lang: str) -> list[str]:
    if not isinstance(lookup, dict):
        return []
    variants: list[str] = []
    seen: set[str] = set()
    translations = lookup.get("translations") if isinstance(lookup.get("translations"), list) else []
    ordered = sorted(
        [item for item in translations if isinstance(item, dict)],
        key=lambda item: (0 if bool(item.get("is_primary")) else 1),
    )
    for item in ordered[:3]:
        value = _apply_article_for_display(str(item.get("value") or "").strip(), lookup, target_lang)
        key = _normalize_dictionary_compare_key(value)
        if not value or not key or key in seen:
            continue
        seen.add(key)
        variants.append(value)
    return variants


def _format_government_patterns_block(patterns: list | None) -> list[str]:
    if not isinstance(patterns, list):
        return []
    lines: list[str] = []
    for item in patterns[:3]:
        if not isinstance(item, dict):
            continue
        pattern = str(item.get("pattern") or "").strip()
        preposition = str(item.get("preposition") or "").strip()
        case_name = str(item.get("case") or "").strip()
        example_source = str(item.get("example_source") or "").strip()
        example_target = str(item.get("example_target") or "").strip()
        if not pattern:
            pieces = []
            if preposition:
                pieces.append(preposition)
            if case_name:
                pieces.append(case_name)
            pattern = " + ".join(pieces)
        if not pattern:
            continue
        head = pattern
        if preposition and preposition.casefold() not in pattern.casefold():
            head = f"{head} ({preposition})"
        if case_name and case_name.casefold() not in head.casefold():
            head = f"{head} + {case_name}"
        lines.append(f"- {head}")
        if example_source:
            lines.append(f"  Пример: {example_source}")
        if example_target:
            lines.append(f"  ↳ {example_target}")
    return lines


def _format_common_collocations(collocations: list | None) -> str:
    if not isinstance(collocations, list):
        return ""
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in collocations[:4]:
        if isinstance(item, dict):
            value = str(item.get("source") or item.get("value") or "").strip()
        else:
            value = str(item or "").strip()
        key = _normalize_dictionary_compare_key(value)
        if not value or not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(value)
    return "; ".join(cleaned[:3])


def _build_dictionary_card_text(
    source_lang: str,
    target_lang: str,
    source_text: str,
    lookup: dict,
    *,
    original_query: str = "",
) -> str:
    def _esc(value: str) -> str:
        return html.escape(str(value or "").strip())

    source_text = source_text.strip()
    translation = (lookup.get("word_target") or "").strip()
    meanings = _extract_lookup_meanings(lookup)
    translation_variants = _extract_translation_variants(lookup, target_lang)
    notes = _extract_learning_notes(lookup)
    part_of_speech = (lookup.get("part_of_speech") or "").strip()
    article = (lookup.get("article") or "").strip()
    corrected_form = str(lookup.get("corrected_form") or "").strip()
    correction_applied = bool(lookup.get("correction_applied"))
    original_request = str(original_query or lookup.get("original_query") or source_text).strip()
    pronunciation = lookup.get("pronunciation") if isinstance(lookup.get("pronunciation"), dict) else {}
    ipa = str(pronunciation.get("ipa") or "").strip()
    stress = str(pronunciation.get("stress") or "").strip()
    forms = _format_forms_block(lookup.get("forms"))
    is_separable = lookup.get("is_separable")
    collocations = _format_common_collocations(lookup.get("common_collocations"))
    government_lines = _format_government_patterns_block(lookup.get("government_patterns"))
    show_corrected = False
    if corrected_form and original_request:
        show_corrected = _normalize_dictionary_compare_key(corrected_form) != _normalize_dictionary_compare_key(original_request)
    elif correction_applied and source_text and original_request:
        show_corrected = _normalize_dictionary_compare_key(source_text) != _normalize_dictionary_compare_key(original_request)
        if show_corrected and not corrected_form:
            corrected_form = source_text

    lines: list[str] = []
    lines.append("🔹 <b>Запрос</b>")
    lines.append(f"• <code>{_esc(original_request or source_text or '—')}</code>")
    if show_corrected:
        lines.append(f"• Исправленная форма: <code>{_esc(corrected_form)}</code>")

    if meanings:
        primary = meanings[0]
        primary_value = _apply_article_for_display(str(translation or primary.get("value") or "—"), lookup, target_lang)
        lines.append("")
        lines.append("🎯 <b>Основные значения</b>")
        if translation_variants:
            lines.append(f"• <b>{_esc('; '.join(translation_variants))}</b>")
        else:
            lines.append(f"• <b>{_esc(primary_value)}</b>")
        primary_context = str(primary.get("context") or "").strip()
        primary_source = str(primary.get("example_source") or "").strip()
        primary_target = str(primary.get("example_target") or "").strip()
        if primary_context:
            lines.append(f"• Контекст: {_esc(primary_context)}")
        if primary_source:
            lines.append(f"• Пример: {_esc(primary_source)}")
        if primary_target:
            lines.append(f"  ↳ {_esc(primary_target)}")

        secondary = meanings[1:3]
        if secondary:
            lines.append("")
            lines.append("🧩 <b>Дополнительные значения</b>")
            for idx, item in enumerate(secondary, start=1):
                sec_value = _apply_article_for_display(str(item.get("value") or "—"), lookup, target_lang)
                lines.append(f"{idx}. <b>{_esc(sec_value)}</b>")
                item_context = str(item.get("context") or "").strip()
                item_source = str(item.get("example_source") or "").strip()
                item_target = str(item.get("example_target") or "").strip()
                if item_context:
                    lines.append(f"   • Контекст: {_esc(item_context)}")
                if item_source:
                    lines.append(f"   • Пример: {_esc(item_source)}")
                if item_target:
                    lines.append(f"     ↳ {_esc(item_target)}")
    else:
        lines.append("")
        lines.append("🎯 <b>Основные значения</b>")
        lines.append(f"• <b>{_esc(_apply_article_for_display(translation or '—', lookup, target_lang))}</b>")

    lines.append("")
    lines.append("🗂 <b>Лингвистическая информация</b>")
    lines.append(f"• Часть речи: {_esc(part_of_speech or '—')}")
    if article:
        lines.append(f"• Род / артикль: <b>{_esc(article)}</b>")
    if notes["part_of_speech_note"]:
        lines.append(f"• Особенность: {_esc(notes['part_of_speech_note'])}")
    if ipa or stress:
        pron = f"{ipa or '—'}{f' | ударение: {stress}' if stress else ''}"
        lines.append(f"• Произношение: {_esc(pron)}")
    if isinstance(is_separable, bool):
        lines.append(f"• Отделяемость: {_esc('отделяемый' if is_separable else 'неотделяемый')}")
    if notes["expression_note"]:
        lines.append(f"• Тип выражения: {_esc(notes['expression_note'])}")
    if collocations:
        lines.append(f"• Частые сочетания: {_esc(collocations)}")
    if government_lines:
        lines.append("• Управление / конструкции:")
        for gov_line in government_lines:
            lines.append(f"   {_esc(gov_line)}")
    if forms:
        lines.append("• Формы:")
        for form_line in forms:
            cleaned = form_line.replace("- ", "", 1)
            lines.append(f"   - {_esc(cleaned)}")

    note_lines = []
    if notes["etymology_note"]:
        note_lines.append(f"• Происхождение: {_esc(notes['etymology_note'])}")
    if notes["memory_tip"]:
        note_lines.append(f"• Фишка запоминания: {_esc(notes['memory_tip'])}")
    if note_lines:
        lines.append("")
        lines.append("💡 <b>Как почувствовать и запомнить</b>")
        lines.extend(note_lines)

    return "\n".join(lines)


def _store_pending_dictionary_card(
    user_id: int,
    source_lang: str,
    target_lang: str,
    source_text: str,
    lookup: dict,
    original_query: str = "",
) -> str:
    direction = f"{source_lang}-{target_lang}"
    key = hashlib.sha1(
        f"{user_id}:{direction}:{source_text}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    pending_dictionary_cards[key] = {
        "user_id": user_id,
        "direction": direction,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "source_text": source_text,
        "lookup": lookup,
        "original_query": (original_query or source_text or "").strip(),
        "saved": False,
    }
    if len(pending_dictionary_cards) > 500:
        oldest_key = next(iter(pending_dictionary_cards))
        pending_dictionary_cards.pop(oldest_key, None)
    return key


def _store_pending_dictionary_save_options(
    user_id: int,
    card_key: str,
    options: list[dict],
    lookup: dict,
    source_lang: str,
    target_lang: str,
    question_request_key: str = "",
) -> str:
    direction = f"{source_lang}-{target_lang}"
    key = hashlib.sha1(
        f"{user_id}:{card_key}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    pending_dictionary_save_options[key] = {
        "user_id": user_id,
        "card_key": card_key,
        "direction": direction,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "lookup": lookup,
        "options": options[:3],
        "selected": [],
        "question_request_key": str(question_request_key or "").strip(),
    }
    if len(pending_dictionary_save_options) > 500:
        oldest_key = next(iter(pending_dictionary_save_options))
        pending_dictionary_save_options.pop(oldest_key, None)
    return key


def _resolve_default_dictionary_option(payload: dict) -> dict:
    lookup = payload.get("lookup") or {}
    source_text = (payload.get("source_text") or lookup.get("word_source") or "").strip()
    source = (lookup.get("word_source") or source_text).strip()
    target = (lookup.get("word_target") or "").strip()
    return {"source": source, "target": target}


def _build_save_variant_keyboard(
    option_key: str,
    options: list[dict],
    selected: list[int] | None = None,
    feel_card_key: str | None = None,
    speak_card_key: str | None = None,
    question_request_key: str | None = None,
) -> InlineKeyboardMarkup:
    selected_set = set(int(item) for item in (selected or []) if isinstance(item, int) or str(item).isdigit())
    rows = []
    for idx, _opt in enumerate(options[:3], start=1):
        mark = "✅" if (idx - 1) in selected_set else "☐"
        rows.append([InlineKeyboardButton(f"{mark} Вариант {idx}", callback_data=f"dictseltoggle:{option_key}:{idx-1}")])
    rows.append([InlineKeyboardButton("✅ Сохранить выбранные", callback_data=f"dictsaveconfirm:{option_key}")])
    rows.append([InlineKeyboardButton("☑️ Выбрать все", callback_data=f"dictselall:{option_key}")])
    if speak_card_key:
        rows.append([InlineKeyboardButton("🔊 Прослушать", callback_data=f"dictspeak:{speak_card_key}")])
    if feel_card_key:
        rows.append([InlineKeyboardButton("📌 Почувствовать слово", callback_data=f"dictfeel:{feel_card_key}")])
    if question_request_key:
        rows.append([InlineKeyboardButton("❓ Задать свой вопрос", callback_data=f"quizask:{question_request_key}")])
    return InlineKeyboardMarkup(rows)


def _build_save_variants_text(source_lang: str, target_lang: str, options: list[dict]) -> str:
    def _esc(value: str) -> str:
        return html.escape(str(value or "").strip())

    lines = ["📌 <b>Варианты для сохранения</b>", ""]
    for idx, opt in enumerate(options[:3], start=1):
        source = (opt.get("source") or "").strip() or "—"
        target = (opt.get("target") or "").strip() or "—"
        lines.append(f"{idx}. <b>{source_lang.upper()}:</b> {_esc(source)}")
        lines.append(f"   <b>{target_lang.upper()}:</b> {_esc(target)}")
        lines.append("")
    return "\n".join(lines)


def _build_quick_dictionary_result_text(
    source_lang: str,
    target_lang: str,
    source_text: str,
    options: list[dict],
    *,
    original_query: str = "",
) -> str:
    def _esc(value: str) -> str:
        return html.escape(str(value or "").strip())

    query_text = str(original_query or source_text).strip() or source_text
    lines = [
        "⚡ <b>Быстрый перевод</b>",
        f"🌐 <b>{source_lang.upper()} → {target_lang.upper()}</b>",
        "",
        f"• <b>Запрос:</b> <code>{_esc(query_text)}</code>",
        "",
        "📌 <b>Варианты для сохранения</b>",
        "",
    ]
    for idx, opt in enumerate(options[:2], start=1):
        source = (opt.get("source") or "").strip() or source_text or "—"
        target = (opt.get("target") or "").strip() or "—"
        lines.append(f"{idx}. <b>{source_lang.upper()}:</b> {_esc(source)}")
        lines.append(f"   <b>{target_lang.upper()}:</b> {_esc(target)}")
        lines.append("")
    return "\n".join(lines).strip()


def _build_quick_dictionary_save_keyboard(option_key: str, options: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if options:
        rows.append([InlineKeyboardButton("✅ Сохранить 1", callback_data=f"dictquicksave:{option_key}:0")])
    if len(options) >= 2:
        rows.append([InlineKeyboardButton("✅ Сохранить 2", callback_data=f"dictquicksave:{option_key}:1")])
        rows.append([InlineKeyboardButton("✅ Сохранить оба", callback_data=f"dictquicksave:{option_key}:all")])
    return InlineKeyboardMarkup(rows)


def _format_flashcard_feel_html_for_bot(feel_text: str) -> str:
    compact = str(feel_text or "").strip()
    if len(compact) > 2800:
        compact = compact[:2797].rstrip() + "..."
    if not compact:
        return html.escape(compact)

    def _decorate_line(raw_line: str) -> str:
        line = str(raw_line or "").strip()
        if not line:
            return ""
        escaped = html.escape(line)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
        escaped = re.sub(r"\*(.+?)\*", r"<b>\1</b>", escaped)

        heading_probe = re.sub(r"<[^>]+>", "", escaped)
        heading_probe = re.sub(r"[*_`#:\-–—\s]+", " ", heading_probe).strip().casefold()
        if not heading_probe:
            return escaped

        if ("основ" in heading_probe) or ("main explanation" in heading_probe):
            return f"🧠 {escaped}"
        if ("похож" in heading_probe) or ("similar" in heading_probe):
            return f"🧩 {escaped}"
        if ("внутрен" in heading_probe) or ("inner logic" in heading_probe):
            return f"🔍 {escaped}"
        if ("пример" in heading_probe) or ("example" in heading_probe):
            return f"📝 {escaped}"
        return escaped

    return "\n".join(_decorate_line(line) for line in compact.splitlines())


def _build_dictionary_feel_private_message(
    *,
    source_text: str,
    target_text: str,
    source_lang: str,
    target_lang: str,
    feel_text: str,
) -> str:
    source_code = str(source_lang or "").strip().upper() or "SRC"
    target_code = str(target_lang or "").strip().upper() or "TGT"
    source_safe = html.escape(str(source_text or "").strip())
    target_safe = html.escape(str(target_text or "").strip())
    formatted_feel = _format_flashcard_feel_html_for_bot(feel_text)
    lines = [
        "🧠 <b>Feel the Word</b>",
        f"🌐 <b>{source_code} → {target_code}</b>",
        "",
        "✨ <b>Слово и перевод</b>",
        f"• {source_safe}",
        f"• → {target_safe}",
        "",
        "━━━━━━━━━━━━",
        "📚 <b>Разбор</b>",
        formatted_feel,
        "",
        "━━━━━━━━━━━━",
        "👍👎 <i>Оцени ответ кнопкой ниже:</i>",
    ]
    return "\n".join(lines)


def _render_dictionary_card_png(
    source_lang: str,
    target_lang: str,
    source_text: str,
    lookup: dict,
    options: list[dict],
) -> str | None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return None

    try:
        base_dir = Path(__file__).resolve().parent
        font_regular_path = base_dir / "backend" / "assets" / "fonts" / "DejaVuSans.ttf"
        font_bold_path = base_dir / "backend" / "assets" / "fonts" / "DejaVuSans-Bold.ttf"

        def _font(size: int, bold: bool = False):
            candidate = font_bold_path if bold else font_regular_path
            if candidate.exists():
                try:
                    return ImageFont.truetype(str(candidate), size=size)
                except Exception:
                    pass
            return ImageFont.load_default()

        title_font = _font(56, bold=True)
        section_font = _font(40, bold=True)
        body_font = _font(34, bold=False)
        body_bold_font = _font(38, bold=True)
        small_font = _font(30, bold=False)

        theme = DICTIONARY_CARD_THEME if DICTIONARY_CARD_THEME in {"classic", "minimal"} else "classic"
        width = 1080
        outer_pad = 34
        section_gap = 22
        block_pad_x = 34
        block_pad_y = 24
        content_width = width - (outer_pad * 2)
        text_width = content_width - (block_pad_x * 2)

        if not isinstance(lookup, dict):
            lookup = {}

        meanings = _extract_lookup_meanings(lookup)
        primary = meanings[0] if meanings else {}
        secondary = meanings[1:3] if len(meanings) > 1 else []
        notes = _extract_learning_notes(lookup)
        part_of_speech = str(lookup.get("part_of_speech") or "").strip() or "—"
        article = str(lookup.get("article") or "").strip()
        pronunciation = lookup.get("pronunciation") if isinstance(lookup.get("pronunciation"), dict) else {}
        ipa = str(pronunciation.get("ipa") or "").strip()
        stress = str(pronunciation.get("stress") or "").strip()
        forms = _format_forms_block(lookup.get("forms"))
        primary_value = _apply_article_for_display(
            str(lookup.get("word_target") or primary.get("value") or "—").strip(),
            lookup,
            target_lang,
        )

        def _wrap(draw_ctx, text: str, font, max_width: int) -> list[str]:
            cleaned = (text or "").strip()
            if not cleaned:
                return []
            words = cleaned.split()
            if not words:
                return []
            lines_local: list[str] = []
            current = words[0]
            for word in words[1:]:
                candidate = f"{current} {word}"
                if draw_ctx.textlength(candidate, font=font) <= max_width:
                    current = candidate
                else:
                    lines_local.append(current)
                    current = word
            lines_local.append(current)
            return lines_local

        blocks: list[dict] = []
        blocks.append(
            {
                "title": "Запрос",
                "tone": "query",
                "rows": [("headline", source_text or "—")],
            }
        )
        primary_rows: list[tuple[str, str]] = [("headline", primary_value)]
        if str(primary.get("context") or "").strip():
            primary_rows.append(("muted", f"Контекст: {str(primary.get('context') or '').strip()}"))
        if str(primary.get("example_source") or "").strip():
            primary_rows.append(("body", f"Пример: {str(primary.get('example_source') or '').strip()}"))
        if str(primary.get("example_target") or "").strip():
            primary_rows.append(("small", f"Перевод примера: {str(primary.get('example_target') or '').strip()}"))
        blocks.append({"title": "Основной перевод", "tone": "primary", "rows": primary_rows})

        secondary_rows: list[tuple[str, str]] = []
        if secondary:
            for idx, item in enumerate(secondary, start=1):
                secondary_rows.append(
                    (
                        "headline",
                        f"{idx}) {_apply_article_for_display(str(item.get('value') or '—').strip(), lookup, target_lang)}",
                    )
                )
                if str(item.get("example_source") or "").strip():
                    secondary_rows.append(("body", f"Пример: {str(item.get('example_source') or '').strip()}"))
                if str(item.get("example_target") or "").strip():
                    secondary_rows.append(("small", f"Перевод примера: {str(item.get('example_target') or '').strip()}"))
                if str(item.get("context") or "").strip():
                    secondary_rows.append(("muted", f"Контекст: {str(item.get('context') or '').strip()}"))
        else:
            secondary_rows = [("body", "Дополнительные значения не найдены")]
        blocks.append({"title": "Дополнительные значения", "tone": "secondary", "rows": secondary_rows})

        save_rows: list[tuple[str, str]] = []
        for idx, item in enumerate((options or [])[:3], start=1):
            src = str((item or {}).get("source") or "").strip() or "—"
            tgt = str((item or {}).get("target") or "").strip() or "—"
            save_rows.append(("body", f"{idx}) {src}"))
            save_rows.append(("small", f"   {tgt}"))
        if save_rows:
            blocks.append({"title": "Варианты для сохранения", "tone": "save", "rows": save_rows})

        tip_rows: list[tuple[str, str]] = []
        if notes["etymology_note"]:
            tip_rows.append(("body", f"Происхождение: {notes['etymology_note']}"))
        if notes["memory_tip"]:
            tip_rows.append(("body", f"Фишка запоминания: {notes['memory_tip']}"))
        if not tip_rows:
            tip_rows = [("body", "Свяжите слово с личной ситуацией и повторите в своей фразе 3 раза.")]
        blocks.append({"title": "Как запомнить", "tone": "tips", "rows": tip_rows})

        meta_rows = [("body", f"Часть речи: {part_of_speech}")]
        if article:
            meta_rows.append(("body", f"Артикль: {article}"))
        if ipa or stress:
            meta_rows.append(("small", f"Произношение: {ipa or '—'}{f' | ударение: {stress}' if stress else ''}"))
        for line in forms[:3]:
            meta_rows.append(("small", line.replace("- ", "")))
        blocks.append({"title": "Лингвистика", "tone": "meta", "rows": meta_rows})

        if theme == "minimal":
            bg_top = (245, 248, 252)
            bg_bottom = (232, 238, 248)
            header_fill = (255, 255, 255)
            header_outline = (184, 198, 222)
            title_color = (24, 42, 70)
            subtitle_color = (76, 101, 140)
            body_color = (26, 36, 53)
            small_color = (66, 82, 110)
            tone_fills = {
                "query": (255, 255, 255),
                "primary": (239, 247, 255),
                "secondary": (246, 241, 255),
                "save": (239, 250, 244),
                "tips": (255, 248, 236),
                "meta": (247, 248, 252),
            }
            tone_outlines = {
                "query": (186, 201, 225),
                "primary": (140, 184, 225),
                "secondary": (184, 163, 225),
                "save": (151, 194, 163),
                "tips": (219, 184, 130),
                "meta": (188, 198, 216),
            }
        else:
            bg_top = (9, 24, 64)
            bg_bottom = (8, 16, 46)
            header_fill = (15, 31, 78)
            header_outline = (92, 128, 210)
            title_color = (255, 229, 142)
            subtitle_color = (170, 214, 255)
            body_color = (244, 248, 255)
            small_color = (210, 224, 255)
            tone_fills = {
                "query": (18, 39, 95),
                "primary": (25, 55, 120),
                "secondary": (39, 47, 111),
                "save": (20, 67, 88),
                "tips": (76, 57, 26),
                "meta": (20, 34, 83),
            }
            tone_outlines = {
                "query": (108, 140, 220),
                "primary": (118, 181, 246),
                "secondary": (152, 132, 221),
                "save": (94, 174, 150),
                "tips": (214, 174, 113),
                "meta": (117, 145, 210),
            }

        draw_probe = ImageDraw.Draw(Image.new("RGB", (10, 10)))
        header_height = 130
        line_height = {"headline": 46, "body": 40, "small": 36, "muted": 36}
        total_height = outer_pad + header_height + section_gap
        block_heights: list[int] = []
        wrapped_cache: list[list[tuple[str, str]]] = []
        for block in blocks:
            wrapped_rows: list[tuple[str, str]] = []
            for style, raw_text in block["rows"]:
                font = body_bold_font if style == "headline" else (small_font if style in {"small", "muted"} else body_font)
                for ln in _wrap(draw_probe, raw_text, font, text_width):
                    wrapped_rows.append((style, ln))
            if not wrapped_rows:
                wrapped_rows = [("body", "—")]
            wrapped_cache.append(wrapped_rows)
            block_height = block_pad_y * 2 + 48
            block_height += sum(line_height.get(st, 40) for st, _ in wrapped_rows)
            block_height += 8
            block_heights.append(block_height)
            total_height += block_height + section_gap

        height = total_height + outer_pad
        image = Image.new("RGB", (width, height), color=bg_bottom)
        draw = ImageDraw.Draw(image)
        draw.rectangle([(0, 0), (width, int(height * 0.44))], fill=bg_top)
        draw.rectangle([(0, int(height * 0.44)), (width, height)], fill=bg_bottom)

        header_x0 = outer_pad
        header_y0 = outer_pad
        header_x1 = width - outer_pad
        header_y1 = header_y0 + header_height
        draw.rounded_rectangle(
            [(header_x0, header_y0), (header_x1, header_y1)],
            radius=28,
            fill=header_fill,
            outline=header_outline,
            width=3,
        )
        draw.text((header_x0 + 30, header_y0 + 30), "Словарная карточка", fill=title_color, font=title_font)

        y = header_y1 + section_gap
        for idx, block in enumerate(blocks):
            block_h = block_heights[idx]
            b_x0 = outer_pad
            b_y0 = y
            b_x1 = width - outer_pad
            b_y1 = y + block_h
            tone = str(block.get("tone") or "meta")
            draw.rounded_rectangle(
                [(b_x0, b_y0), (b_x1, b_y1)],
                radius=24,
                fill=tone_fills.get(tone, tone_fills["meta"]),
                outline=tone_outlines.get(tone, tone_outlines["meta"]),
                width=3,
            )
            draw.text((b_x0 + block_pad_x, b_y0 + block_pad_y - 1), str(block.get("title") or ""), fill=subtitle_color, font=section_font)
            row_y = b_y0 + block_pad_y + 50
            for style, text in wrapped_cache[idx]:
                if style == "headline":
                    font = body_bold_font
                    color = body_color
                elif style == "small":
                    font = small_font
                    color = small_color
                elif style == "muted":
                    font = small_font
                    color = subtitle_color
                else:
                    font = body_font
                    color = body_color
                draw.text((b_x0 + block_pad_x, row_y), text, fill=color, font=font)
                row_y += line_height.get(style, 40)
            y += block_h + section_gap

        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            image.save(tmp.name, format="PNG", optimize=True)
            return tmp.name
    except Exception:
        logging.debug("Dictionary PNG card render failed", exc_info=True)
        return None


async def _generate_dictionary_save_options(payload: dict) -> list[dict]:
    lookup = payload.get("lookup") or {}
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if (not source_lang or not target_lang) and "-" in str(payload.get("direction") or ""):
        source_lang, target_lang = [x.strip().lower() for x in str(payload.get("direction")).split("-", 1)]
    source_text = (payload.get("source_text") or "").strip()
    default_option = _resolve_default_dictionary_option(payload)
    base_translation = (default_option.get("target") or "").strip()

    direction = f"{source_lang}-{target_lang}"
    if direction in {"ru-de", "de-ru"}:
        generated = await run_dictionary_collocations(direction, source_text, base_translation)
    else:
        generated = await run_dictionary_collocations_multilang(
            source_lang=source_lang,
            target_lang=target_lang,
            word_source=source_text,
            word_target=base_translation,
        )
    items = generated.get("items") if isinstance(generated, dict) else []

    unique: set[tuple[str, str]] = set()
    options: list[dict] = []

    def _add_option(source_value: str, target_value: str, is_original: bool = False) -> None:
        s = source_value.strip()
        t = target_value.strip()
        if not s or not t:
            return
        key = (s.lower(), t.lower())
        if key in unique:
            return
        unique.add(key)
        options.append({"source": s, "target": t, "is_original": bool(is_original)})

    original_query = str(payload.get("original_query") or "").strip()
    if original_query and _normalize_dictionary_compare_key(original_query) == _normalize_dictionary_compare_key(source_text):
        _add_option(original_query, default_option.get("target") or "", is_original=True)

    _add_option(default_option.get("source") or "", default_option.get("target") or "", is_original=False)

    predefined = lookup.get("save_worthy_options") if isinstance(lookup, dict) else None
    if isinstance(predefined, list):
        for item in predefined:
            if not isinstance(item, dict):
                continue
            _add_option(
                item.get("source") or item.get("word_source") or "",
                item.get("target") or item.get("word_target") or "",
                is_original=False,
            )
            if len(options) >= 3:
                return options[:3]

    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            _add_option(
                item.get("source") or item.get("word_source") or "",
                item.get("target") or item.get("word_target") or "",
                is_original=False,
            )
            if len(options) >= 3:
                break

    return options[:3]


async def _run_dictionary_lookup_for_pair(lookup_input: str, source_lang: str, target_lang: str) -> dict:
    source_lang = (source_lang or "").strip().lower()
    target_lang = (target_lang or "").strip().lower()
    direction = f"{source_lang}-{target_lang}"

    async def _run_multilang_fallback() -> dict:
        raw_fallback = await run_dictionary_lookup_multilang(
            word=lookup_input,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        if not isinstance(raw_fallback, dict):
            raw_fallback = {}
        return {
            **raw_fallback,
            "word_source": str(raw_fallback.get("word_source") or lookup_input).strip(),
            "word_target": str(raw_fallback.get("word_target") or "").strip(),
            "translations": (
                raw_fallback.get("translations")
                if isinstance(raw_fallback.get("translations"), list)
                else (
                    [{"value": str(raw_fallback.get("word_target") or "").strip(), "context": "base", "is_primary": True}]
                    if str(raw_fallback.get("word_target") or "").strip()
                    else []
                )
            ),
            "meanings": (
                raw_fallback.get("meanings")
                if isinstance(raw_fallback.get("meanings"), dict)
                else {"primary": {}, "secondary": []}
            ),
            "etymology_note": str(raw_fallback.get("etymology_note") or "").strip() or None,
            "usage_note": str(raw_fallback.get("usage_note") or "").strip() or None,
            "memory_tip": str(raw_fallback.get("memory_tip") or "").strip() or None,
            "source_lang": source_lang,
            "target_lang": target_lang,
        }

    if direction == "ru-de":
        try:
            raw = await run_dictionary_lookup(lookup_input)
        except Exception:
            logging.warning("Legacy dictionary_assistant failed for ru-de; fallback to multilang", exc_info=True)
            return await _coerce_sentence_lookup_payload(
                await _run_multilang_fallback(),
                lookup_input,
                source_lang,
                target_lang,
            )
        translations = raw.get("translations") if isinstance(raw, dict) else []
        if not isinstance(translations, list):
            translations = []
        target_value = str((raw or {}).get("translation_de") or "").strip()
        if target_value and not translations:
            translations = [{"value": target_value, "context": "base", "is_primary": True}]
        result = {
            **(raw if isinstance(raw, dict) else {}),
            "word_source": str((raw or {}).get("word_ru") or lookup_input).strip(),
            "word_target": target_value,
            "translations": translations,
            "meanings": (raw.get("meanings") if isinstance(raw, dict) and isinstance(raw.get("meanings"), dict) else {"primary": {}, "secondary": []}),
            "etymology_note": str((raw or {}).get("etymology_note") or "").strip() or None,
            "usage_note": str((raw or {}).get("usage_note") or "").strip() or None,
            "memory_tip": str((raw or {}).get("memory_tip") or "").strip() or None,
            "source_lang": source_lang,
            "target_lang": target_lang,
        }
        result = await _ensure_lookup_target_language(result, lookup_input, source_lang, target_lang)
        return await _coerce_sentence_lookup_payload(result, lookup_input, source_lang, target_lang)
    if direction == "de-ru":
        try:
            raw = await run_dictionary_lookup_de(lookup_input)
        except Exception:
            logging.warning("Legacy dictionary_assistant_de failed for de-ru; fallback to multilang", exc_info=True)
            return await _coerce_sentence_lookup_payload(
                await _run_multilang_fallback(),
                lookup_input,
                source_lang,
                target_lang,
            )
        translations = raw.get("translations") if isinstance(raw, dict) else []
        if not isinstance(translations, list):
            translations = []
        target_value = str((raw or {}).get("translation_ru") or "").strip()
        if target_value and not translations:
            translations = [{"value": target_value, "context": "base", "is_primary": True}]
        result = {
            **(raw if isinstance(raw, dict) else {}),
            "word_source": str((raw or {}).get("word_de") or lookup_input).strip(),
            "word_target": target_value,
            "translations": translations,
            "meanings": (raw.get("meanings") if isinstance(raw, dict) and isinstance(raw.get("meanings"), dict) else {"primary": {}, "secondary": []}),
            "etymology_note": str((raw or {}).get("etymology_note") or "").strip() or None,
            "usage_note": str((raw or {}).get("usage_note") or "").strip() or None,
            "memory_tip": str((raw or {}).get("memory_tip") or "").strip() or None,
            "source_lang": source_lang,
            "target_lang": target_lang,
        }
        result = await _ensure_lookup_target_language(result, lookup_input, source_lang, target_lang)
        return await _coerce_sentence_lookup_payload(result, lookup_input, source_lang, target_lang)
    raw = await run_dictionary_lookup_multilang(
        word=lookup_input,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    if not isinstance(raw, dict):
        raw = {}
    result = {
        **raw,
        "word_source": str(raw.get("word_source") or lookup_input).strip(),
        "word_target": str(raw.get("word_target") or "").strip(),
        "translations": (
            raw.get("translations")
            if isinstance(raw.get("translations"), list)
            else (
                [{"value": str(raw.get("word_target") or "").strip(), "context": "base", "is_primary": True}]
                if str(raw.get("word_target") or "").strip()
                else []
            )
        ),
        "meanings": (
            raw.get("meanings")
            if isinstance(raw.get("meanings"), dict)
            else {"primary": {}, "secondary": []}
        ),
        "etymology_note": str(raw.get("etymology_note") or "").strip() or None,
        "usage_note": str(raw.get("usage_note") or "").strip() or None,
        "memory_tip": str(raw.get("memory_tip") or "").strip() or None,
        "source_lang": source_lang,
        "target_lang": target_lang,
    }
    result = await _ensure_lookup_target_language(result, lookup_input, source_lang, target_lang)
    return await _coerce_sentence_lookup_payload(result, lookup_input, source_lang, target_lang)


async def _send_dictionary_lookup_result(
    message,
    context: CallbackContext,
    user_id: int,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
) -> None:
    lookup_input = (lookup_input or "").strip()
    try:
        prepared = await _prepare_dictionary_lookup_response(
            user_id=int(user_id),
            lookup_input=lookup_input,
            source_lang=source_lang,
            target_lang=target_lang,
            max_options=3,
        )
    except Exception as exc:
        logging.exception(
            "❌ Ошибка словарного поиска для '%s' (%s->%s, %s): %r",
            lookup_input,
            source_lang,
            target_lang,
            type(exc).__name__,
            exc,
        )
        await message.reply_text("Не удалось получить перевод. Попробуйте снова через несколько секунд.")
        return

    card_text = _build_dictionary_card_text(
        source_lang,
        target_lang,
        prepared["source_text"],
        prepared["lookup"],
        original_query=lookup_input,
    )
    variants_text = _build_save_variants_text(source_lang, target_lang, prepared["options"])
    full_text = f"{card_text}\n\n{variants_text}"
    keyboard = _build_save_variant_keyboard(
        prepared["option_key"],
        prepared["options"],
        selected=[],
        feel_card_key=prepared["card_key"],
        speak_card_key=prepared["card_key"],
        question_request_key=prepared["question_request_key"],
    )
    msg = await message.reply_text(
        full_text,
        reply_markup=keyboard,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    add_service_msg_id(context, msg.message_id)


async def _prepare_dictionary_lookup_response(
    *,
    user_id: int,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
    max_options: int = 3,
) -> dict:
    lookup = await _run_dictionary_lookup_for_pair(lookup_input, source_lang, target_lang)
    if not isinstance(lookup, dict):
        raise ValueError("lookup result is not a dict")

    lookup["original_query"] = str(lookup.get("original_query") or lookup_input).strip()
    source_text = (lookup.get("word_source") or lookup_input).strip()
    card_key = _store_pending_dictionary_card(
        int(user_id),
        source_lang,
        target_lang,
        source_text,
        lookup,
        original_query=lookup_input,
    )
    question_request_key = _store_pending_quiz_question_request(
        user_id=int(user_id),
        source_text=source_text,
        target_text=str(lookup.get("word_target") or "").strip(),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    card_payload = pending_dictionary_cards.get(card_key)
    if isinstance(card_payload, dict):
        card_payload["question_request_key"] = question_request_key
        pending_dictionary_cards[card_key] = card_payload

    try:
        options = await _generate_dictionary_save_options(
            {
                "source_lang": source_lang,
                "target_lang": target_lang,
                "source_text": source_text,
                "lookup": lookup,
                "original_query": lookup_input,
            }
        )
    except Exception as exc:
        logging.exception(f"❌ Ошибка генерации вариантов сохранения: {exc}")
        options = []

    if not options:
        options = [_resolve_default_dictionary_option({"source_text": source_text, "lookup": lookup})]

    trimmed_options = [item for item in (options or []) if isinstance(item, dict)]
    trimmed_options = trimmed_options[: max(1, int(max_options or 1))]
    option_key = _store_pending_dictionary_save_options(
        user_id=int(user_id),
        card_key=card_key,
        options=trimmed_options,
        lookup=lookup,
        source_lang=source_lang,
        target_lang=target_lang,
        question_request_key=question_request_key,
    )
    return {
        "lookup": lookup,
        "source_text": source_text,
        "card_key": card_key,
        "question_request_key": question_request_key,
        "options": trimmed_options,
        "option_key": option_key,
    }


async def _send_dictionary_lookup_quick_result(
    message,
    context: CallbackContext,
    user_id: int,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
) -> None:
    lookup_input = (lookup_input or "").strip()
    try:
        prepared = await _prepare_dictionary_lookup_response(
            user_id=int(user_id),
            lookup_input=lookup_input,
            source_lang=source_lang,
            target_lang=target_lang,
            max_options=2,
        )
    except Exception as exc:
        logging.exception(
            "❌ Ошибка быстрого словарного поиска для '%s' (%s->%s, %s): %r",
            lookup_input,
            source_lang,
            target_lang,
            type(exc).__name__,
            exc,
        )
        await message.reply_text("Не удалось получить быстрый перевод. Попробуйте снова через несколько секунд.")
        return

    quick_text = _build_quick_dictionary_result_text(
        source_lang,
        target_lang,
        prepared["source_text"],
        prepared["options"],
        original_query=lookup_input,
    )
    keyboard = _build_quick_dictionary_save_keyboard(
        prepared["option_key"],
        prepared["options"],
    )
    msg = await message.reply_text(
        quick_text,
        reply_markup=keyboard,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    add_service_msg_id(context, msg.message_id)


async def _handle_private_dictionary_lookup(update: Update, context: CallbackContext, text: str) -> None:
    if not update.message or not update.message.from_user:
        return
    lookup_input = (text or "").strip()
    if not lookup_input:
        await update.message.reply_text("Пустой запрос. Отправьте слово или короткую фразу.")
        return
    request_key = _store_pending_dictionary_lookup_request(int(update.message.from_user.id), lookup_input)
    keyboard = _build_dictionary_pair_keyboard(request_key)
    msg = await update.message.reply_text(
        _build_dictionary_pair_selection_text(lookup_input),
        reply_markup=keyboard,
    )
    add_service_msg_id(context, msg.message_id)


async def _process_dictionary_pair_selection(
    *,
    context: CallbackContext,
    message,
    request_key: str,
    user_id: int,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
    response_mode: str = "full",
) -> None:
    try:
        normalized_mode = str(response_mode or "full").strip().lower()
        if normalized_mode == "quick":
            await _send_dictionary_lookup_quick_result(
                message=message,
                context=context,
                user_id=user_id,
                lookup_input=lookup_input,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        else:
            await _send_dictionary_lookup_result(
                message=message,
                context=context,
                user_id=user_id,
                lookup_input=lookup_input,
                source_lang=source_lang,
                target_lang=target_lang,
            )
    except Exception as exc:
        logging.exception(
            "❌ Dictionary pair processing failed user_id=%s key=%s pair=%s-%s mode=%s: %s",
            user_id,
            request_key,
            source_lang,
            target_lang,
            response_mode,
            exc,
        )
        try:
            await message.reply_text("Не удалось выполнить перевод. Попробуйте снова.")
        except Exception:
            logging.debug("Failed to send dictionary pair error message", exc_info=True)
    finally:
        pending_dictionary_lookup_requests.pop(request_key, None)
        pending_dictionary_lookup_inflight.discard(request_key)


async def handle_dictionary_pair_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    if not query.message:
        await query.answer("Сообщение недоступно. Отправьте запрос снова.", show_alert=True)
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неверный формат языковой пары.", show_alert=True)
        return
    request_key = parts[1].strip()
    pair = parts[2].strip().lower()
    if "-" not in pair:
        await query.answer("Неверный формат языковой пары.", show_alert=True)
        return
    source_lang, target_lang = [chunk.strip() for chunk in pair.split("-", 1)]
    if (source_lang, target_lang) not in _dictionary_language_pairs():
        await query.answer("Эта языковая пара не поддерживается.", show_alert=True)
        return

    payload = pending_dictionary_lookup_requests.get(request_key)
    user = query.from_user
    if not user:
        await query.answer("Пользователь не найден. Повторите запрос.", show_alert=True)
        return

    if not payload:
        reconstructed_lookup_input = _extract_lookup_input_from_pair_message_text(
            str(getattr(query.message, "text", "") or getattr(query.message, "caption", "") or "")
        )
        if not reconstructed_lookup_input:
            await query.answer("Запрос устарел. Отправьте слово ещё раз.", show_alert=True)
            return
        payload = {
            "user_id": int(user.id),
            "text": reconstructed_lookup_input,
        }
        pending_dictionary_lookup_requests[request_key] = payload

    if int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Выбор доступен только автору запроса.", show_alert=True)
        return

    lookup_input = str(payload.get("text") or "").strip()
    if not lookup_input:
        await query.answer("Запрос пустой. Отправьте слово снова.", show_alert=True)
        return

    await query.answer(f"Пара выбрана ({source_lang.upper()} -> {target_lang.upper()})")
    try:
        await query.edit_message_text(
            _build_dictionary_mode_selection_text(lookup_input, source_lang, target_lang),
            reply_markup=_build_dictionary_mode_keyboard(request_key, source_lang, target_lang),
        )
    except Exception:
        pass


async def handle_dictionary_mode_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    if not query.message:
        await query.answer("Сообщение недоступно. Отправьте запрос заново.", show_alert=True)
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("Неверный формат режима.", show_alert=True)
        return

    request_key = parts[1].strip()
    pair = parts[2].strip().lower()
    response_mode = parts[3].strip().lower()
    if response_mode not in {"quick", "full"}:
        await query.answer("Неизвестный режим ответа.", show_alert=True)
        return
    if "-" not in pair:
        await query.answer("Неверный формат языковой пары.", show_alert=True)
        return
    source_lang, target_lang = [chunk.strip() for chunk in pair.split("-", 1)]
    if (source_lang, target_lang) not in _dictionary_language_pairs():
        await query.answer("Эта языковая пара не поддерживается.", show_alert=True)
        return

    payload = pending_dictionary_lookup_requests.get(request_key)
    user = query.from_user
    if not user:
        await query.answer("Пользователь не найден. Повторите запрос.", show_alert=True)
        return
    if not payload:
        await query.answer("Запрос устарел. Отправьте слово снова.", show_alert=True)
        return
    if int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Выбор доступен только автору запроса.", show_alert=True)
        return

    lookup_input = str(payload.get("text") or "").strip()
    if not lookup_input:
        await query.answer("Запрос пустой. Отправьте слово снова.", show_alert=True)
        return

    if request_key in pending_dictionary_lookup_inflight:
        await query.answer("Этот запрос уже обрабатывается. Подождите немного.", show_alert=True)
        return
    pending_dictionary_lookup_inflight.add(request_key)

    mode_label = "быстрый" if response_mode == "quick" else "расширенный"
    await query.answer(f"Запустил {mode_label} перевод")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    context.application.create_task(
        _process_dictionary_pair_selection(
            context=context,
            message=query.message,
            request_key=request_key,
            user_id=int(user.id),
            lookup_input=lookup_input,
            source_lang=source_lang,
            target_lang=target_lang,
            response_mode=response_mode,
        )
    )


async def handle_dictionary_feel_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    card_key = parts[1].strip()
    payload = pending_dictionary_cards.get(card_key)
    if not payload:
        await query.answer("Карточка устарела. Запросите перевод заново.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору карточки.", show_alert=True)
        return
    if payload.get("feel_used"):
        await query.answer("Разбор уже отправлен для этой карточки.", show_alert=True)
        return

    inflight_key = f"dictfeel:{int(user.id)}:{card_key}"
    if inflight_key in pending_feel_requests_inflight:
        await query.answer("Разбор уже готовится, подождите.", show_alert=True)
        return
    pending_feel_requests_inflight.add(inflight_key)

    try:
        source_lang = str(payload.get("source_lang") or "").strip().lower()
        target_lang = str(payload.get("target_lang") or "").strip().lower()
        if (not source_lang or not target_lang) and "-" in str(payload.get("direction") or ""):
            source_lang, target_lang = [x.strip().lower() for x in str(payload.get("direction")).split("-", 1)]

        lookup = payload.get("lookup") if isinstance(payload.get("lookup"), dict) else {}
        source_text = str(
            lookup.get("word_source")
            or payload.get("source_text")
            or payload.get("original_query")
            or ""
        ).strip()
        target_text = str(lookup.get("word_target") or "").strip()
        if not target_text:
            translations = lookup.get("translations") if isinstance(lookup.get("translations"), list) else []
            for item in translations:
                if not isinstance(item, dict):
                    continue
                candidate = str(item.get("value") or "").strip()
                if candidate:
                    target_text = candidate
                    break

        if not source_text:
            await query.answer("Не удалось определить исходное слово.", show_alert=True)
            return

        try:
            await query.answer("Готовлю разбор…")
        except Exception:
            pass

        if source_lang == "ru" and target_lang == "de":
            feel_text = await run_feel_word(source_text, target_text)
        elif source_lang == "de" and target_lang == "ru":
            feel_text = await run_feel_word(target_text or source_text, source_text)
        else:
            feel_text = await run_feel_word_multilang(
                source_text=source_text,
                target_text=target_text,
                source_lang=source_lang or "auto",
                target_lang=target_lang or "auto",
            )

        feel_text = str(feel_text or "").strip()
        if not feel_text:
            await query.answer("Разбор пустой. Попробуйте снова.", show_alert=True)
            return

        token = await asyncio.to_thread(
            create_flashcard_feel_feedback_token,
            user_id=int(user.id),
            entry_id=0,
            feel_explanation=feel_text,
        )
        reply_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("👍 Like", callback_data=f"feelfb:{token}:like"),
            InlineKeyboardButton("👎 Dislike", callback_data=f"feelfb:{token}:dislike"),
        ]])
        text = _build_dictionary_feel_private_message(
            source_text=source_text,
            target_text=target_text or "—",
            source_lang=source_lang,
            target_lang=target_lang,
            feel_text=feel_text,
        )
        msg = await query.message.reply_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
        add_service_msg_id(context, msg.message_id)
        payload["feel_used"] = True
        pending_dictionary_cards[card_key] = payload
        try:
            await asyncio.to_thread(
                update_telegram_system_message_type,
                chat_id=int(query.message.chat_id),
                message_id=int(msg.message_id),
                message_type="feel_word",
            )
        except Exception:
            logging.debug("Failed to mark dictfeel message as preserved", exc_info=True)
    except Exception as exc:
        logging.exception("❌ Ошибка dictfeel для user_id=%s card_key=%s: %s", int(user.id), card_key, exc)
        await query.answer("Не удалось сделать разбор. Попробуйте чуть позже.", show_alert=True)
    finally:
        pending_feel_requests_inflight.discard(inflight_key)


async def handle_dictionary_speak_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    card_key = parts[1].strip()
    payload = pending_dictionary_cards.get(card_key)
    if not payload:
        await query.answer("Карточка устарела. Запросите перевод заново.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору карточки.", show_alert=True)
        return

    inflight_key = f"dictspeak:{int(user.id)}:{card_key}"
    if inflight_key in pending_tts_listen_requests_inflight:
        await query.answer("Озвучка уже готовится, подождите.", show_alert=True)
        return
    pending_tts_listen_requests_inflight.add(inflight_key)

    try:
        if _is_synthetic_telegram_user_id(int(user.id)):
            await query.answer("Озвучка отключена для load-test user.", show_alert=True)
            return
        speak_lang, speak_text = _resolve_dictionary_speech_payload(payload, user_id=int(user.id))
        if not speak_lang or not speak_text:
            await query.answer("Не удалось определить фразу для озвучки.", show_alert=True)
            return

        try:
            await query.answer("Готовлю озвучку…")
        except Exception:
            pass

        voice_buffer, _normalized_text = await _synthesize_telegram_tts_voice(speak_lang, speak_text)
        await context.bot.send_voice(
            chat_id=int(query.message.chat_id),
            voice=voice_buffer,
            reply_to_message_id=int(query.message.message_id),
            language_tutor_button=False,
        )
    except Exception as exc:
        logging.exception("❌ Ошибка dictspeak для user_id=%s card_key=%s: %s", int(user.id), card_key, exc)
        await query.answer("Не удалось подготовить озвучку. Попробуйте позже.", show_alert=True)
    finally:
        pending_tts_listen_requests_inflight.discard(inflight_key)


async def handle_quiz_feel_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    key = parts[1].strip()
    payload = pending_quiz_feel_requests.get(key)
    if not payload:
        await query.answer("Кнопка устарела. Пройдите квиз ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору результата.", show_alert=True)
        return
    if payload.get("feel_used"):
        await query.answer("Разбор уже отправлен для этого результата.", show_alert=True)
        return

    inflight_key = f"quizfeel:{int(user.id)}:{key}"
    if inflight_key in pending_feel_requests_inflight:
        await query.answer("Разбор уже готовится, подождите.", show_alert=True)
        return
    pending_feel_requests_inflight.add(inflight_key)

    try:
        source_text = str(payload.get("source_text") or "").strip()
        target_text = str(payload.get("target_text") or "").strip()
        source_lang = str(payload.get("source_lang") or "").strip().lower()
        target_lang = str(payload.get("target_lang") or "").strip().lower()
        if not source_text:
            await query.answer("Не удалось определить фразу для разбора.", show_alert=True)
            return

        try:
            await query.answer("Готовлю разбор…")
        except Exception:
            pass

        if source_lang == "ru" and target_lang == "de":
            feel_text = await run_feel_word(source_text, target_text)
        elif source_lang == "de" and target_lang == "ru":
            feel_text = await run_feel_word(target_text or source_text, source_text)
        else:
            feel_text = await run_feel_word_multilang(
                source_text=source_text,
                target_text=target_text,
                source_lang=source_lang or "auto",
                target_lang=target_lang or "auto",
            )

        feel_text = str(feel_text or "").strip()
        if not feel_text:
            await query.answer("Разбор пустой. Попробуйте снова.", show_alert=True)
            return

        token = await asyncio.to_thread(
            create_flashcard_feel_feedback_token,
            user_id=int(user.id),
            entry_id=0,
            feel_explanation=feel_text,
        )
        reply_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("👍 Like", callback_data=f"feelfb:{token}:like"),
            InlineKeyboardButton("👎 Dislike", callback_data=f"feelfb:{token}:dislike"),
        ]])
        text = _build_dictionary_feel_private_message(
            source_text=source_text,
            target_text=target_text or "—",
            source_lang=source_lang,
            target_lang=target_lang,
            feel_text=feel_text,
        )
        msg = await query.message.reply_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
        add_service_msg_id(context, msg.message_id)
        payload["feel_used"] = True
        pending_quiz_feel_requests[key] = payload
        try:
            await asyncio.to_thread(
                update_telegram_system_message_type,
                chat_id=int(query.message.chat_id),
                message_id=int(msg.message_id),
                message_type="feel_word",
            )
        except Exception:
            logging.debug("Failed to mark quizfeel message as preserved", exc_info=True)
    except Exception as exc:
        logging.exception("❌ Ошибка quizfeel для user_id=%s key=%s: %s", int(user.id), key, exc)
        await query.answer("Не удалось сделать разбор. Попробуйте позже.", show_alert=True)
    finally:
        pending_feel_requests_inflight.discard(inflight_key)


async def handle_quiz_speak_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    key = parts[1].strip()
    payload = pending_quiz_feel_requests.get(key)
    if not payload:
        await query.answer("Кнопка устарела. Пройдите квиз ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору результата.", show_alert=True)
        return

    inflight_key = f"quizspeak:{int(user.id)}:{key}"
    if inflight_key in pending_tts_listen_requests_inflight:
        await query.answer("Озвучка уже готовится, подождите.", show_alert=True)
        return
    pending_tts_listen_requests_inflight.add(inflight_key)

    try:
        if _is_synthetic_telegram_user_id(int(user.id)):
            await query.answer("Озвучка отключена для load-test user.", show_alert=True)
            return
        speak_lang, speak_text = _resolve_quiz_speech_payload(payload)
        if not speak_lang or not speak_text:
            await query.answer("Не удалось определить фразу для озвучки.", show_alert=True)
            return

        try:
            await query.answer("Готовлю озвучку…")
        except Exception:
            pass

        voice_buffer, _normalized_text = await _synthesize_telegram_tts_voice(speak_lang, speak_text)
        await context.bot.send_voice(
            chat_id=int(query.message.chat_id),
            voice=voice_buffer,
            reply_to_message_id=int(query.message.message_id),
            language_tutor_button=False,
        )
    except Exception as exc:
        logging.exception("❌ Ошибка quizspeak для user_id=%s key=%s: %s", int(user.id), key, exc)
        await query.answer("Не удалось подготовить озвучку. Попробуйте позже.", show_alert=True)
    finally:
        pending_tts_listen_requests_inflight.discard(inflight_key)


async def handle_quiz_phrase_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    key = parts[1].strip()
    payload = pending_quiz_phrase_requests.get(key)
    if not payload or _is_quiz_question_payload_expired(payload):
        pending_quiz_phrase_requests.pop(key, None)
        await query.answer("Кнопка устарела. Пройдите квиз ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору результата.", show_alert=True)
        return

    inflight_key = f"quizphrase:{int(user.id)}:{key}"
    if inflight_key in pending_quiz_phrase_requests_inflight:
        await query.answer("Сочетание уже готовится, подождите.", show_alert=True)
        return
    pending_quiz_phrase_requests_inflight.add(inflight_key)

    try:
        try:
            await query.answer("Подбираю сочетание…")
        except Exception:
            pass

        suggestion = await _generate_quiz_phrase_suggestion(payload)
        source_text = str(suggestion.get("source_text") or "").strip()
        target_text = str(suggestion.get("target_text") or "").strip()
        source_lang = str(suggestion.get("source_lang") or payload.get("source_lang") or "").strip().lower()
        target_lang = str(suggestion.get("target_lang") or payload.get("target_lang") or "").strip().lower()
        if not source_text or not target_text or not source_lang or not target_lang:
            await query.answer("Не удалось подготовить сочетание.", show_alert=True)
            return

        save_key = _store_pending_quiz_question_save_request(
            user_id=int(user.id),
            request_key=key,
            source_text=source_text,
            target_text=target_text,
            source_lang=source_lang,
            target_lang=target_lang,
            hide_continue_after_save=True,
        )
        lines = [
            "🧩 Короткое сочетание с правильным вариантом",
            f"{source_lang.upper()}: {source_text}",
            f"{target_lang.upper()}: {target_text}",
            "",
            "Если хотите, сохраните это сочетание в словарь.",
        ]
        await query.message.reply_text(
            "\n".join(lines),
            reply_markup=_build_save_only_keyboard(save_key=save_key),
        )
    except Exception as exc:
        logging.exception("❌ Ошибка quizphrase для user_id=%s key=%s: %s", int(user.id), key, exc)
        await query.answer("Не удалось подготовить сочетание. Попробуйте позже.", show_alert=True)
    finally:
        pending_quiz_phrase_requests_inflight.discard(inflight_key)


async def handle_quiz_ask_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    request_key = parts[1].strip()
    payload = pending_quiz_question_requests.get(request_key)
    if not payload or _is_quiz_question_payload_expired(payload):
        pending_quiz_question_requests.pop(request_key, None)
        await query.answer("Кнопка устарела. Пройдите квиз ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору результата.", show_alert=True)
        return

    pending_quiz_question_input[int(user.id)] = {
        "request_key": request_key,
        "started_at": pytime.time(),
    }
    try:
        await query.answer("Жду ваш вопрос")
    except Exception:
        pass
    await query.message.reply_text(
        "❓ Напишите одним сообщением ваш вопрос по этой фразе.\n\n"
        "Например:\n"
        "• когда так говорят\n"
        "• в чём разница с похожим вариантом\n"
        "• дайте ещё 2 примера\n"
        "• почему здесь именно такая форма\n\n"
        "Если передумали, нажмите кнопку ниже или напишите: отмена",
        reply_markup=_build_quiz_question_prompt_keyboard(),
    )


async def handle_quiz_question_cancel_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    user = query.from_user
    if not user:
        await query.answer("Не удалось определить пользователя.", show_alert=True)
        return

    pending_question = pending_quiz_question_input.get(int(user.id))
    if not pending_question:
        await query.answer("Активного вопроса уже нет.", show_alert=False)
        return

    pending_quiz_question_input.pop(int(user.id), None)
    try:
        await query.answer("Вопрос отменён")
    except Exception:
        pass
    await query.message.reply_text("Ок, отменил вопрос.")


async def handle_quiz_question_save_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    save_key = parts[1].strip()
    payload = pending_quiz_question_save_requests.get(save_key)
    if not payload or _is_quiz_question_payload_expired(payload):
        pending_quiz_question_save_requests.pop(save_key, None)
        await query.answer("Эта кнопка уже устарела. Задайте вопрос заново.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Кнопка доступна только автору ответа.", show_alert=True)
        return
    if payload.get("saved"):
        await query.answer("Эта фраза уже сохранена.")
        return

    source_text = str(payload.get("source_text") or "").strip()
    target_text = str(payload.get("target_text") or "").strip()
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if not source_text or not target_text or not source_lang or not target_lang:
        await query.answer("Не удалось определить языковую пару для сохранения.", show_alert=True)
        return

    save_payload = {
        "source_lang": source_lang,
        "target_lang": target_lang,
        "direction": f"{source_lang}-{target_lang}",
        "lookup": {
            "word_source": source_text,
            "word_target": target_text,
        },
    }
    chosen = {
        "source": source_text,
        "target": target_text,
    }
    save_ok, save_msg = _save_dictionary_option_for_user(
        payload=save_payload,
        chosen=chosen,
        user_id=int(user.id),
    )
    if not save_ok:
        await query.answer(save_msg or "Ошибка сохранения. Попробуйте позже.", show_alert=True)
        return

    payload["saved"] = True
    pending_quiz_question_save_requests[save_key] = payload
    try:
        if query.message:
            if bool(payload.get("hide_continue_after_save")):
                await query.message.edit_reply_markup(reply_markup=None)
            else:
                continue_callback_data = str(payload.get("continue_callback_data") or "").strip() or "langgpt:continue"
                continue_button_text = str(payload.get("continue_button_text") or "").strip() or "❓ Задать вопрос"
                await query.message.edit_reply_markup(
                    reply_markup=_build_followup_answer_keyboard(
                        continue_callback_data=continue_callback_data,
                        continue_button_text=continue_button_text,
                        save_key=None,
                    )
                )
    except Exception:
        logging.debug("Failed to update quiz question save keyboard", exc_info=True)
    try:
        await query.answer("✅ Сохранено")
    except Exception:
        pass
    await query.message.reply_text(f"✅ Сохранена фраза: {source_text} -> {target_text}")


async def handle_flashcard_feel_feedback_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неверный формат кнопки.", show_alert=True)
        return

    token = parts[1].strip()
    action = parts[2].strip().lower()
    if action not in {"like", "dislike"}:
        await query.answer("Неизвестное действие.", show_alert=True)
        return

    user = query.from_user
    if not user:
        await query.answer("Пользователь не определён.", show_alert=True)
        return

    try:
        result = await asyncio.to_thread(
            apply_flashcard_feel_feedback,
            token=token,
            user_id=int(user.id),
            liked=(action == "like"),
        )
    except Exception as exc:
        logging.exception(f"❌ Ошибка сохранения feel feedback: {exc}")
        await query.answer("Не удалось сохранить feedback.", show_alert=True)
        return

    if not result:
        await query.answer("Эта оценка уже недоступна.", show_alert=True)
        return

    already_processed = bool(result.get("already_processed"))
    resolved_action = str(result.get("action") or action).strip().lower()
    if already_processed:
        text = "Уже сохранено ранее."
    elif resolved_action == "like":
        text = "✅ Сохранили. Это объяснение останется в базе."
    else:
        text = "🗑 Удалили. Это объяснение больше не будет храниться."

    try:
        await query.answer("Готово")
    except Exception:
        pass
    try:
        if query.message:
            await query.message.edit_reply_markup(reply_markup=None)
            await query.message.reply_text(text)
    except Exception:
        logging.debug("Failed to update feel feedback message", exc_info=True)


async def handle_dictionary_save_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = query.data or ""
    key = data.replace("dictsave:", "", 1).strip()
    payload = pending_dictionary_cards.get(key)
    if not payload and query.message and query.from_user:
        text = str(getattr(query.message, "text", "") or getattr(query.message, "caption", "") or "")
        source_lang, target_lang, options = _parse_dictionary_options_from_message_text(text)
        if source_lang and target_lang and options:
            lookup = {
                "word_source": str(options[0].get("source") or "").strip(),
                "word_target": str(options[0].get("target") or "").strip(),
                "source_lang": source_lang,
                "target_lang": target_lang,
            }
            payload = {
                "user_id": int(query.from_user.id),
                "direction": f"{source_lang}-{target_lang}",
                "source_lang": source_lang,
                "target_lang": target_lang,
                "source_text": str(options[0].get("source") or "").strip(),
                "lookup": lookup,
                "saved": False,
                "original_query": str(options[0].get("source") or "").strip(),
            }
            pending_dictionary_cards[key] = payload
    if not payload:
        await query.answer("Эта карточка уже недоступна. Запросите перевод ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Сохранение доступно только автору карточки.", show_alert=True)
        return

    if payload.get("saved"):
        await query.answer("Слово уже сохранено.")
        return

    lookup = payload.get("lookup") or {}
    if not isinstance(lookup, dict):
        await query.answer("Не удалось сохранить: данные карточки повреждены.", show_alert=True)
        return

    try:
        options = await _generate_dictionary_save_options(payload)
        if not options:
            options = [_resolve_default_dictionary_option(payload)]
    except Exception as exc:
        logging.exception(f"❌ Ошибка генерации вариантов сохранения: {exc}")
        await query.answer("Ошибка подготовки вариантов. Попробуйте позже.", show_alert=True)
        return

    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if (not source_lang or not target_lang) and "-" in str(payload.get("direction") or ""):
        source_lang, target_lang = [x.strip().lower() for x in str(payload.get("direction")).split("-", 1)]

    option_key = _store_pending_dictionary_save_options(
        user_id=int(user.id),
        card_key=key,
        options=options,
        lookup=lookup,
        source_lang=source_lang,
        target_lang=target_lang,
        question_request_key=str(payload.get("question_request_key") or "").strip(),
    )
    variants_text = _build_save_variants_text(source_lang, target_lang, options)
    keyboard = _build_save_variant_keyboard(
        option_key,
        options,
        selected=[],
        feel_card_key=key,
        speak_card_key=key,
        question_request_key=str(payload.get("question_request_key") or "").strip() or None,
    )
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("Выберите вариант")
    await query.message.reply_text(variants_text, reply_markup=keyboard)


async def handle_dictionary_save_option_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неверный формат выбора.", show_alert=True)
        return
    option_key = parts[1].strip()
    try:
        option_idx = int(parts[2].strip())
    except ValueError:
        await query.answer("Неверный индекс варианта.", show_alert=True)
        return

    payload = pending_dictionary_save_options.get(option_key)
    if not payload:
        payload = _rebuild_dictionary_save_options_payload_from_message(query, option_key)
    if not payload:
        await query.answer("Варианты устарели. Нажмите сохранить ещё раз.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Сохранение доступно только автору карточки.", show_alert=True)
        return

    options = payload.get("options") or []
    if option_idx < 0 or option_idx >= len(options):
        await query.answer("Выбранный вариант не найден.", show_alert=True)
        return

    chosen = options[option_idx]
    save_ok, save_msg = _save_dictionary_option_for_user(payload=payload, chosen=chosen, user_id=int(user.id))
    if not save_ok:
        await query.answer(save_msg or "Ошибка сохранения. Попробуйте позже.", show_alert=True)
        return

    card_key = payload.get("card_key")
    card_payload = pending_dictionary_cards.get(card_key or "")
    if isinstance(card_payload, dict):
        card_payload["saved"] = True
        pending_dictionary_cards[card_key] = card_payload

    pending_dictionary_save_options.pop(option_key, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("✅ Сохранено")
    source = (chosen.get("source") or "").strip()
    target = (chosen.get("target") or "").strip()
    await query.message.reply_text(f"✅ Сохранён вариант: {source} -> {target}")


def _save_dictionary_option_for_user(payload: dict, chosen: dict, user_id: int) -> tuple[bool, str]:
    source = (chosen.get("source") or "").strip()
    target = (chosen.get("target") or "").strip()
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if (not source_lang or not target_lang) and "-" in str(payload.get("direction") or ""):
        source_lang, target_lang = [x.strip().lower() for x in str(payload.get("direction")).split("-", 1)]
    lookup = payload.get("lookup") or {}

    if not source or not target:
        return False, "Вариант неполный, выберите другой."

    response_json = dict(lookup) if isinstance(lookup, dict) else {}
    response_json["word_source"] = source
    response_json["word_target"] = target
    response_json["source_text"] = source
    response_json["target_text"] = target
    response_json["source_lang"] = source_lang
    response_json["target_lang"] = target_lang
    response_json["language_pair"] = {
        "source_lang": source_lang,
        "target_lang": target_lang,
    }

    # Canonical language-aware mapping.
    # For RU<->DE, we additionally keep legacy columns in RU->DE orientation
    # so existing consumers (quiz/statistics/admin views) stay consistent.
    if source_lang == "ru" and target_lang == "de":
        word_ru = source
        word_de = target
        translation_de = target
        translation_ru = source
    elif source_lang == "de" and target_lang == "ru":
        word_ru = target
        word_de = source
        translation_de = source
        translation_ru = target
    else:
        word_ru = source if source_lang == "ru" else (target if target_lang == "ru" else None)
        word_de = source if source_lang == "de" else (target if target_lang == "de" else None)
        translation_de = target if target_lang == "de" else (source if source_lang == "de" else None)
        translation_ru = target if target_lang == "ru" else (source if source_lang == "ru" else None)

    if word_ru:
        response_json["word_ru"] = word_ru
    if word_de:
        response_json["word_de"] = word_de
    if translation_de:
        response_json["translation_de"] = translation_de
    if translation_ru:
        response_json["translation_ru"] = translation_ru

    try:
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

        save_webapp_dictionary_query(
            user_id=user_id,
            word_ru=word_ru,
            translation_de=translation_de,
            word_de=word_de,
            translation_ru=translation_ru,
            response_json=response_json,
            folder_id=int(folder_id) if folder_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
            origin_process="bot_private_save",
            origin_meta={
                "flow": "dictionary_select",
                "source": "private_bot",
            },
        )
    except Exception as exc:
        logging.exception(f"❌ Ошибка сохранения выбранного варианта user_id={user_id}: {exc}")
        return False, "Ошибка сохранения. Попробуйте позже."
    return True, "ok"


async def handle_dictionary_select_toggle_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неверный формат выбора.", show_alert=True)
        return
    option_key = parts[1].strip()
    try:
        option_idx = int(parts[2].strip())
    except ValueError:
        await query.answer("Неверный индекс варианта.", show_alert=True)
        return

    payload = pending_dictionary_save_options.get(option_key)
    if not payload:
        payload = _rebuild_dictionary_save_options_payload_from_message(query, option_key)
    if not payload:
        await query.answer("Варианты устарели. Запросите снова.", show_alert=True)
        return
    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Доступно только автору карточки.", show_alert=True)
        return

    options = payload.get("options") or []
    if option_idx < 0 or option_idx >= len(options):
        await query.answer("Выбранный вариант не найден.", show_alert=True)
        return

    selected = payload.get("selected") or []
    selected_set = set(int(item) for item in selected if isinstance(item, int) or str(item).isdigit())
    if option_idx in selected_set:
        selected_set.remove(option_idx)
    else:
        selected_set.add(option_idx)
    payload["selected"] = sorted(selected_set)
    pending_dictionary_save_options[option_key] = payload
    keyboard = _build_save_variant_keyboard(
        option_key,
        options,
        selected=payload["selected"],
        feel_card_key=str(payload.get("card_key") or "").strip() or None,
        speak_card_key=str(payload.get("card_key") or "").strip() or None,
        question_request_key=str(payload.get("question_request_key") or "").strip() or None,
    )
    try:
        await query.edit_message_reply_markup(reply_markup=keyboard)
    except Exception:
        pass
    await query.answer("Выбор обновлён")


async def handle_dictionary_select_all_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат.", show_alert=True)
        return
    option_key = parts[1].strip()
    payload = pending_dictionary_save_options.get(option_key)
    if not payload:
        payload = _rebuild_dictionary_save_options_payload_from_message(query, option_key)
    if not payload:
        await query.answer("Варианты устарели. Запросите снова.", show_alert=True)
        return
    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Доступно только автору карточки.", show_alert=True)
        return
    options = payload.get("options") or []
    payload["selected"] = list(range(len(options)))
    pending_dictionary_save_options[option_key] = payload
    keyboard = _build_save_variant_keyboard(
        option_key,
        options,
        selected=payload["selected"],
        feel_card_key=str(payload.get("card_key") or "").strip() or None,
        speak_card_key=str(payload.get("card_key") or "").strip() or None,
        question_request_key=str(payload.get("question_request_key") or "").strip() or None,
    )
    try:
        await query.edit_message_reply_markup(reply_markup=keyboard)
    except Exception:
        pass
    await query.answer("Выбраны все варианты")


async def handle_dictionary_save_confirm_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 2:
        await query.answer("Неверный формат.", show_alert=True)
        return
    option_key = parts[1].strip()
    payload = pending_dictionary_save_options.get(option_key)
    if not payload:
        payload = _rebuild_dictionary_save_options_payload_from_message(query, option_key)
    if not payload:
        await query.answer("Варианты устарели. Запросите снова.", show_alert=True)
        return
    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Доступно только автору карточки.", show_alert=True)
        return
    options = payload.get("options") or []
    selected = payload.get("selected") or []
    selected_idxs = sorted(set(int(item) for item in selected if isinstance(item, int) or str(item).isdigit()))
    if not selected_idxs:
        await query.answer("Выберите минимум один вариант.", show_alert=True)
        return

    saved_lines: list[str] = []
    for idx in selected_idxs:
        if idx < 0 or idx >= len(options):
            continue
        chosen = options[idx]
        save_ok, save_msg = _save_dictionary_option_for_user(payload=payload, chosen=chosen, user_id=int(user.id))
        if save_ok:
            source = (chosen.get("source") or "").strip()
            target = (chosen.get("target") or "").strip()
            saved_lines.append(f"• {source} -> {target}")
        else:
            logging.warning("Dictionary multi-save skipped idx=%s: %s", idx, save_msg)

    if not saved_lines:
        await query.answer("Не удалось сохранить выбранные варианты.", show_alert=True)
        return

    card_key = payload.get("card_key")
    card_payload = pending_dictionary_cards.get(card_key or "")
    if isinstance(card_payload, dict):
        card_payload["saved"] = True
        pending_dictionary_cards[card_key] = card_payload
    pending_dictionary_save_options.pop(option_key, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("✅ Сохранено")
    await query.message.reply_text("✅ Сохранены варианты:\n" + "\n".join(saved_lines))


async def handle_dictionary_quick_save_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if not query:
        return
    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неверный формат сохранения.", show_alert=True)
        return

    option_key = parts[1].strip()
    selector = parts[2].strip().lower()
    payload = pending_dictionary_save_options.get(option_key)
    if not payload:
        await query.answer("Варианты устарели. Запросите перевод снова.", show_alert=True)
        return

    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Сохранение доступно только автору карточки.", show_alert=True)
        return

    options = payload.get("options") or []
    if not options:
        await query.answer("Нет вариантов для сохранения.", show_alert=True)
        return

    if selector == "all":
        selected_idxs = list(range(min(2, len(options))))
    else:
        try:
            selected_idx = int(selector)
        except ValueError:
            await query.answer("Неверный вариант для сохранения.", show_alert=True)
            return
        if selected_idx < 0 or selected_idx >= len(options):
            await query.answer("Выбранный вариант не найден.", show_alert=True)
            return
        selected_idxs = [selected_idx]

    saved_lines: list[str] = []
    for idx in selected_idxs:
        chosen = options[idx]
        save_ok, save_msg = _save_dictionary_option_for_user(payload=payload, chosen=chosen, user_id=int(user.id))
        if not save_ok:
            logging.warning("Quick dictionary save skipped idx=%s: %s", idx, save_msg)
            continue
        source = (chosen.get("source") or "").strip()
        target = (chosen.get("target") or "").strip()
        saved_lines.append(f"• {source} -> {target}")

    if not saved_lines:
        await query.answer("Не удалось сохранить выбранные варианты.", show_alert=True)
        return

    card_key = payload.get("card_key")
    card_payload = pending_dictionary_cards.get(card_key or "")
    if isinstance(card_payload, dict):
        card_payload["saved"] = True
        pending_dictionary_cards[card_key] = card_payload
    pending_dictionary_save_options.pop(option_key, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("✅ Сохранено")
    if len(saved_lines) == 1:
        await query.message.reply_text("✅ Сохранён вариант:\n" + saved_lines[0])
    else:
        await query.message.reply_text("✅ Сохранены варианты:\n" + "\n".join(saved_lines))


async def delete_message_with_retry(bot, chat_id, message_id, retries=3, delay=2):
    for attempt in range(retries):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            print(f"DEBUG: Успешно удалено сообщение {message_id}")
            return
        except TimedOut as e:
            print(f"❌ Таймаут при удалении сообщения {message_id} (попытка {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
        except BadRequest as e:
            print(f"❌ Ошибка Telegram при удалении сообщения {message_id}: {e}")
            return  # Сообщение не существует или уже удалено
        except Exception as e:
            print(f"❌ Неизвестная ошибка при удалении сообщения {message_id}: {e}")
            return
    print(f"❌ Не удалось удалить сообщение {message_id} после {retries} попыток")


async def done(update: Update, context: CallbackContext):
    user = update.message.from_user
    user_id = user.id
    context_id = id(context)
    logging.info(f"DEBUG: context_id={context_id} в done")


    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔹 Проверяем, есть ли у пользователя активная сессия
    cursor.execute("""
        SELECT session_id
        FROM bt_3_user_progress 
        WHERE user_id = %s AND completed = FALSE
        ORDER BY start_time DESC
        LIMIT 1;""", 
        (user_id,))
    session = cursor.fetchone()

    if not session:
        msg_1 = await update.message.reply_text("❌ У вас нет активных сессий! Используйте кнопки: '📌 Выбрать тему' -> '🚀 Начать перевод' чтобы начать.")
        logging.info(f"📩 Отправлено сообщение об отсутствии сессии с ID={msg_1.message_id}")
        add_service_msg_id(context, msg_1.message_id)
        cursor.close()
        conn.close()
        return
    session_id = session[0]   # ID текущей сессии

    message_ids = context.user_data.get("service_message_ids", [])

    # 📊 Получаем общее количество предложений
    cursor.execute("""
        SELECT COUNT(*) 
        FROM bt_3_daily_sentences 
        WHERE user_id = %s AND session_id = %s;
        """, (user_id, session_id))
    
    total_sentences = cursor.fetchone()[0]
    logging.info(f"🔄 Ожидаем записи всех переводов пользователя {user_id}. Всего предложений: {total_sentences}")

    # Получаем количество отправленных переводов (из pending_translations)
    pending_translations_count = len(context.user_data.get("pending_translations", []))
    logging.info(f"📤 Пользователь отправил переводов: {pending_translations_count}")

    # Даем время для завершения асинхронных задач (например, записи переводов из check_translation_from_text)
    logging.info("⏳ Даем время для завершения записи переводов в базу...")
    await asyncio.sleep(5)  # Задержка 5 секунд перед первой проверкой

    # Получаем количество записанных переводов в базе
    cursor.execute("""
        SELECT COUNT(*) 
        FROM bt_3_translations 
        WHERE user_id = %s AND session_id = %s;
        """, (user_id, session_id))
    translated_count = cursor.fetchone()[0]
    logging.info(f"📬 Уже записано переводов: {translated_count}/{pending_translations_count}")


    # Проверяем, если отправленных переводов больше, чем предложений в сессии
    if pending_translations_count > total_sentences:
        logging.warning(f"⚠️ pending_translations_count ({pending_translations_count}) больше total_sentences ({total_sentences})")
        pending_translations_count = min(pending_translations_count, total_sentences)

    #await asyncio.sleep(10)


    # Ожидаем, пока все отправленные переводы не запишутся в базу
    max_attempts = 40  # Максимум 30 попыток (30 * 5 секунд = 150 секунд)
    attempt = 0
    start_time = datetime.now()

    logging.info(f"🚩 START while-loop: translated_count={translated_count}, pending_translations_count={pending_translations_count}")

    while translated_count < pending_translations_count and attempt < max_attempts:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM bt_3_translations 
            WHERE user_id = %s AND session_id = %s;
            """, (user_id, session_id))
        translated_count = cursor.fetchone()[0]
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"⌛ Проверяем запись переводов: {translated_count}/{pending_translations_count}. Прошло {elapsed_time:.1f} сек, попытка {attempt + 1}")

        if translated_count >= pending_translations_count:
            logging.info(f"✅ Все отправленные переводы записаны: {translated_count}/{pending_translations_count}")
            break

        await asyncio.sleep(5)  # Ждем 5 секунд
        attempt += 1

    # Логируем, если не все переводы записаны
    if translated_count < pending_translations_count and attempt >= max_attempts:
        logging.warning(f"⚠️ Не все переводы записаны после {max_attempts} попыток: {translated_count}/{pending_translations_count}")


    # Завершаем сессию
    cursor.execute("""
        UPDATE bt_3_user_progress
        SET end_time = NOW(), completed = TRUE
        WHERE user_id = %s AND session_id = %s AND completed = FALSE;
        """, (user_id, session_id))
    conn.commit()

    # Сбрасываем pending_translations
    context.user_data["pending_translations"] = []
    logging.info(f"DEBUG: Сброшены pending_translations для user_id={user_id}")

    # Отправляем итоговое сообщение пользователю
    if translated_count == 0:
        completion_msg = await update.message.reply_text(
            f"😔 Вы не перевели ни одного предложения из {total_sentences} в этой сессии.\n"
            f"Попробуйте начать новую сессию с помощью кнопок '📌 Выбрать тему' -> '🚀 Начать перевод'.",
            parse_mode="Markdown"
        )
    elif translated_count < total_sentences:
        completion_msg = await update.message.reply_text(
            f"⚠️ *Вы перевели {translated_count} из {total_sentences} предложений!*\n"
            f"Перевод завершён, но не все предложения переведены. Это повлияет на ваш итоговый балл.",
            parse_mode="Markdown"
        )
    else:
        completion_msg = await update.message.reply_text(
            f"🎉 *Вы успешно завершили перевод!*\n"
            f"Все {total_sentences} предложений этой сессии переведены! 🚀",
            parse_mode="Markdown"
        )
    

    # Deletion messages from the chat
    for message_id in message_ids:
        try:
            await delete_message_with_retry(context.bot, update.effective_chat.id, message_id)
        except TelegramError as e:
            logging.warning(f"⚠️ Не удалось удалить сервисное сообщение {message_id}: {e}")
    
    # Сбрасываем список
    logging.debug(f"DEBUG: Сбрасываем service_message_ids. Было: {context.user_data.get('service_message_ids', '[] (ключ отсутствовал или пуст)')}")
    context.user_data["service_message_ids"] = []

    cursor.close()
    conn.close()
    

def correct_numbering(sentences):
    """!?! Но это выражение требует фиксированный длины шаблона внутри скобок(?<=^\d+\.), Поэтому не подходит.Исправляет нумерацию, удаляя только вторую некорректную цифру.
    (?<=^\d+\.) — Найди совпадение, но только если перед ним есть число с точкой в начале строки
    Это называется lookbehind assertion. Например, 29. будет найдено, но не заменено.
    \s*\d+\.\s* — теперь заменяется только вторая цифра."""
    corrected_sentences = []
    for sentence in sentences:
        # Удаляем только **второе** число, оставляя первое
        cleaned_sentence = re.sub(r"^(\d+)\.\s*\d+\.\s*", r"\1. ", sentence).strip()
        corrected_sentences.append(cleaned_sentence)
    return corrected_sentences


# Создаёт кнопки с темами (Business, Medicine, Hobbies и т. д.).
async def choose_topic(update: Update, context: CallbackContext):
    print("🔹 Функция choose_topic() вызвана!")  # 👈 Логируем вызов
    global TOPICS
    
    context.user_data.setdefault("service_message_ids", [])
    message_ids = context.user_data["service_message_ids"]
    #message_ids = context.user_data.get("service_message_ids", [])
    print(f"DEBUG: message_ids in choose_topic function: {message_ids}")
    
    buttons = []
    row = []
    for i, topic in enumerate(TOPICS, 1):
        row.append(InlineKeyboardButton(topic, callback_data=topic))
        if i % 2 == 0:
            buttons.append(row)
            row = []
    if row:  # если остались кнопки, которые не кратны 3 (например 10 тем — 9 + 1)
        buttons.append(row)

    reply_markup = InlineKeyboardMarkup(buttons)

    if update.callback_query:
        msg = await update.callback_query.message.edit_text("📌 Выберите тему для предложений:", reply_markup=reply_markup)
        add_service_msg_id(context, msg.message_id)
    else:
        msg_1 = await update.message.reply_text("📌 Выберите тему для предложений:", reply_markup=reply_markup) #Отправляем сообщение пользователю с прикреплёнными кнопками.
        add_service_msg_id(context, msg_1.message_id)



# Когда пользователь нажимает на кнопку, Telegram отправляет callback-запрос, который мы обработаем в topic_selected().
async def topic_selected(update: Update, context: CallbackContext):
    """Handles the button click event when the user selects a topic."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press: Подтверждаем нажатие кнопки (иначе кнопка будет висеть)

    if not query.data:
        logging.error("❌ Ошибка: callback_data отсутствует!")
        return

    chosen_topic = query.data  # Get the selected topic: # Получаем данные (какую кнопку нажали)
    logging.info(f"✅ Пользователь выбрал тему: {chosen_topic}")

    context.user_data["chosen_topic"] = chosen_topic  # Store it in user data: # Сохраняем выбранную тему в памяти пользователя
    msg_1 = await query.message.reply_text(f"✅ Вы выбрали тему: {chosen_topic}.\nТеперь нажмите '🚀 Начать перевод'.")
    add_service_msg_id(context, msg_1.message_id)



# === Функция для генерации новых предложений с помощью GPT-4 ===
async def generate_sentences(user_id, num_sentances, context: CallbackContext = None):
    #client_deepseek = OpenAI(api_key = api_key_deepseek,base_url="https://api.deepseek.com")
    
    task_name = f"generate_sentences"
    system_instruction_key = f"generate_sentences"

    chosen_topic = context.user_data.get("chosen_topic", "Random sentences")  # Default: General topic


    # if chosen_topic != "Random sentences":
    user_message = f"""
    Number of sentences: {num_sentances}. Topic: "{chosen_topic}".
    """

    #Генерация с помощью GPT     
    for attempt in range(5): # Пробуем до 5 раз при ошибке
        try:
            sentences = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            # response = await client.chat.completions.create(
            #     model = "gpt-4-turbo",
            #     messages = [{"role": "user", "content": prompt}]
            # )
            # sentences = response.choices[0].message.content.split("\n")
            filtered_sentences = [s.strip() for s in sentences.split("\n") if s.strip()] # ✅ Фильтруем пустые строки
            
            if filtered_sentences:
                return filtered_sentences
            
        except openai.RateLimitError:
            wait_time = (attempt +1) * 2 # Задержка: 2, 4, 6 сек...
            print(f"⚠️ OpenAI API Rate Limit. Ждем {wait_time} сек...")
            await asyncio.sleep(wait_time)
    
    print("❌ Ошибка: не удалось получить ответ от OpenAI. Используем запасные предложения.")


    # # Генерация с помощью DeepSeek API
    # for attempt in range(5): # Пробуем до 5 раз при ошибке
    #     try:
    #         response = await client_deepseek.chat.completions.create(
    #             model = "deepseek-chat",
    #             messages = [{"role": "user", "content": prompt}], stream=False
    #         )
    #         sentences = response.choices[0].message.content.split("\n")
    #         filtered_sentences = [s.strip() for s in sentences if s.strip()] # ✅ Фильтруем пустые строки
    #         if filtered_sentences:
    #             return filtered_sentences
    #     except openai.RateLimitError:
    #         wait_time = (attempt +1) * 2 # Задержка: 2, 4, 6 сек...
    #         print(f"⚠️ OpenAI API Rate Limit. Ждем {wait_time} сек...")
    #         await asyncio.sleep(wait_time)
    
    # print("❌ Ошибка: не удалось получить ответ от OpenAI. Используем запасные предложения.")


    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sentence FROM bt_3_spare_sentences ORDER BY RANDOM() LIMIT 7;""")
    spare_rows = cursor.fetchall()

    cursor.close()
    conn.close()

    if spare_rows:
        return [row[0].strip() for row in spare_rows if row[0].strip()]
    else:
        print("❌ Ошибка: даже запасные предложения отсутствуют.")
        return ["Запасное предложение 1", "Запасное предложение 2"]


async def recheck_score_only(original_text, user_translation):

    task_name = "recheck_translation"
    system_instruction_key = "recheck_translation"

    user_message = f"""
    Original sentence (Russian): "{original_text}"  
    User's translation (German): "{user_translation}"
    """ 
    
    #Генерация с помощью GPT     
    for attempt in range(3): # Пробуем до 3 раз при ошибке
        try:
            text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            
            print(f"🔁 Ответ на перепроверку оценки:\n{text}")
            if "score" in text.lower():
                reassessed_score = text.lower().split("score:")[-1].split("/")[0].strip()
                try:
                    reassessed_score = int(reassessed_score)
                    print(f"🔁 GPT повторно оценил на: {reassessed_score}/100")
                    return str(reassessed_score)
                except ValueError:
                    print(f"⚠️ Не удалось привести reassessed_score к числу: {reassessed_score}")
                    continue

        except Exception as e:
            print(f"❌ Ошибка при перепроверке score: {e}")
            continue
        
    return "0" # fallback, если GPT не ответил


async def check_translation(original_text, user_translation, update: Update | None, context: CallbackContext | None, sentence_number):

    task_name = f"check_translation"
    system_instruction_key = f"check_translation"

    # Initialize variables with default values at the beginning of the function
    score = None  # Default score
    categories = []
    subcategories = []
    #correct_translation = "there is no information."  # Default translation
    correct_translation = None
    
    # ✅ Показываем сообщение о начале проверки
    has_telegram = bool(update and context and getattr(update, "message", None))
    user_id_label = update.message.from_user.id if has_telegram else "webapp"
    sent_message = None
    message = None

    if has_telegram:
        message = await context.bot.send_message(chat_id=update.message.chat_id, text="⏳ Посмотрим на что ты способен...")
        await simulate_typing(context, update.message.chat_id, duration=3)

    taxonomy_lines = []
    if VALID_CATEGORIES:
        taxonomy_lines.append(
            "allowed_categories: " + ", ".join([str(item).strip() for item in VALID_CATEGORIES if str(item).strip()])
        )
    if VALID_SUBCATEGORIES:
        compact = []
        for cat, values in VALID_SUBCATEGORIES.items():
            normalized_values = [str(value).strip() for value in (values or []) if str(value).strip()]
            if normalized_values:
                compact.append(f"{str(cat).strip()}: {', '.join(normalized_values)}")
        if compact:
            taxonomy_lines.append("allowed_subcategories:")
            taxonomy_lines.extend([f"- {row}" for row in compact])
    taxonomy_hint = ("\n" + "\n".join(taxonomy_lines)) if taxonomy_lines else ""

    user_message = f"""

    **Original sentence (Russian):** "{original_text}"
    **User's translation (German):** "{user_translation}"{taxonomy_hint}

    """

    for attempt in range(3):
        try:
            logging.info(f" GPT started working on {original_text} sentence. Passing data to GPT model")
            start_time = asyncio.get_running_loop().time()
            
            collected_text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=2.0,
            )
            logging.info(f"We got a reply from GPT model for sentence {original_text}")
                

            # ✅ Логируем полный ответ для анализа
            print(f"🔎 FULL RESPONSE:\n{collected_text}")


            # ✅ Парсим результат
            score_str = collected_text.split("Score: ")[-1].split("/")[0].strip() if "Score:" in collected_text else None
            
            #my offer to split by ", " because it is a string and take all list
            # ✅ Ограничиваем строку до конца строки с помощью split("\n")[0]
            categories = collected_text.split("Mistake Categories: ")[-1].split("\n")[0].split(",") if "Mistake Categories:" in collected_text else []
            subcategories = collected_text.split("Subcategories: ")[-1].split("\n")[0].split(",") if "Subcategories:" in collected_text else []

            #severity = collected_text.split("Severity: ")[-1].split("\n")[0].strip() if "Severity:" in collected_text and len(collected_text.split("Severity: ")[-1].split("\n")) > 0 else None
            
            #correct_translation = collected_text.split("Correct Translation: ")[-1].strip() if "Correct Translation:" in collected_text else None

            match = re.search(r'Correct Translation:\s*(.+?)(?:\n|\Z)', collected_text)
            if match:
                correct_translation = match.group(1).strip()
            
            # ✅ Логируем До обработки
            print(f"🔎 RAW CATEGORIES BEFORE HANDLING in check_translation function (User {user_id_label}): {', '.join(categories)}")
            print(f"🔎 RAW SUBCATEGORIES BEFORE HANDLING in check_translation function (User {user_id_label}): {', '.join(subcategories)}")
            
            # my offer for category: i would reduce all unneccessary symbols not only ** except from words and commas (what do you think!?)
            categories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", cat).strip() for cat in categories if cat.strip()]
            # my offer for subcategory: i would reduce all unneccessary symbols not only ** except from words and commas (what do you think!?)
            subcategories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", subcat).strip() for subcat in subcategories if subcat.strip()]

            # ✅ Преобразуем строки в списки: my offer
            categories = [cat.strip() for cat in categories if cat.strip()]
            subcategories = [subcat.strip() for subcat in subcategories if subcat.strip()]

            # ✅ Логируем
            print(f"🔎 RAW CATEGORIES AFTER HANDLING in check_translation function (User {user_id_label}): {', '.join(categories)}")
            print(f"🔎 RAW SUBCATEGORIES AFRET HANDLING (User {user_id_label}): {', '.join(subcategories)}")

            
            if not categories:
                print(f"⚠️ Категории отсутствуют в ответе GPT")
            if not subcategories:
                print(f"⚠️ Подкатегории отсутствуют в ответе GPT")

            if score_str and correct_translation:
                try:
                    score_int = int(score_str)
                except ValueError:
                    print(f"⚠️ Не удалось привести score_str к числу: {score_str}")
                    print(f"⚠️ GPT вернул некорректный формат оценки. Запрашиваем повторную оценку...")
                    reassessed_score = await recheck_score_only(original_text, user_translation)
                    print(f"🔁 GPT повторно оценил на: {reassessed_score}/100")
                    score = reassessed_score
                    break  # завершаем цикл успешно

                if score_int == 0:
                    print(f"⚠️ GPT поставил 0. Запрашиваем повторную оценку...")
                    reassessed_score = await recheck_score_only(original_text, user_translation)
                    print(f"🔁 GPT повторно оценил на: {reassessed_score}/100")
                    score = reassessed_score
                    break

                score = score_str
                print(f"✅ Успешно получены все обязательные данные на попытке {attempt + 1}")
                break
            
            else:
                missing_fields = []
                if not score_str:
                    missing_fields.append("Score")
                #if not severity:
                #    missing_fields.append("Severity")
                if not correct_translation:
                    missing_fields.append("Correct Translation")
                print(f"⚠️ Не получены обязательные поля: {', '.join(missing_fields)}. Повторяем запрос...")
                raise ValueError(f"Missing required fields: "
                     f"{'Score' if not score_str else ''} "
                     f"{'Correct Translation' if not correct_translation else ''}")


        except openai.RateLimitError:
            wait_time = (attempt + 1) * 5
            print(f"⚠️ OpenAI API перегружен. Ждём {wait_time} сек...")
            await asyncio.sleep(wait_time)

        except Exception as e:
            logging.error(f"❌ Ошибка: {e}")
            print(f"❌ Ошибка в цикле обработки: {e}")
            await asyncio.sleep(5)

    if not score or not str(score).isdigit():
        reassessed_score = await recheck_score_only(original_text, user_translation)
        score = reassessed_score if reassessed_score else "0"

    if not correct_translation:
        correct_translation = "—"


    # ✅ Убираем лишние пробелы для ровного форматирования
    result_text = f"""
🟢 *Sentence number:* {sentence_number}\n
✅ *Score:* {score}/100\n
🔵 *Original Sentence:* {original_text}\n
🟡 *User Translation:* {user_translation}\n
🟣 *Correct Translation:* {correct_translation}\n
"""

    # ✅ Если балл > 75 → стилистическая ошибка
    if score and score.isdigit() and int(score) > 75:
        result_text += "\n✅ Перевод на высоком уровне."

    # ✅ Отправляем текст в Telegram с поддержкой HTML
    if has_telegram:
        sent_message = await context.bot.send_message(
            chat_id=update.message.chat_id,
            text=escape_html_with_bold(result_text),
            parse_mode="HTML"
        )

    if has_telegram and sent_message:
        message_id = sent_message.message_id

        # ✅ Сохраняем данные в context.user_data
        if len(context.user_data) >= 10:
            oldest_key = next(iter(context.user_data))
            del context.user_data[oldest_key]  # Удаляем самые старые данные

        context.user_data[f"translation_{message_id}"] = {
            "original_text": original_text,
            "user_translation": user_translation
        }

        # ✅ Удаляем сообщение с индикатором "Генерация ответа"
        if message:
            await message.delete()

        # ✅ Добавляем инлайн-кнопку после отправки сообщения
        keyboard = [[InlineKeyboardButton("❓ Explain me GPT", callback_data=f"explain:{message_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # ✅ Задержка в 1,5 секунды для предотвращения блокировки
        await asyncio.sleep(1.5)

        # ✅ Редактируем сообщение, добавляем кнопку
        await sent_message.edit_text(
            text=escape_html_with_bold(result_text),
            reply_markup=reply_markup,
            parse_mode="HTML"
            )                        

        # ✅ Логируем успешную проверку
        logging.info(f"✅ Перевод проверен для пользователя {update.message.from_user.id}")

    return result_text, categories, subcategories, score, correct_translation


async def handle_explain_request(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()  # Подтверждаем получение запроса

    # ✅ Логируем факт вызова функции
    logging.info("🔹 handle_explain_request вызвана!")

    try:
        logging.info(f"🔹 Callback data: {query.data}")

        # ✅ Получаем `message_id` из callback_data
        message_id = int(query.data.split(":")[1])
        logging.info(f"✅ Извлечённый message_id: {message_id}")
        
        # Логируем сообщение, к которому пытаемся прикрепить комментарий
        chat_id = update.callback_query.message.chat_id

        # ✅ Логируем статус бота в чате
        member = await context.bot.get_chat_member(
            chat_id=chat_id,
            user_id=context.bot.id
        )
        if member.status in ['administrator', 'creator']:
            can_send_messages = True
        elif hasattr(member, 'can_send_messages'):
            can_send_messages = member.can_send_messages
        else:
            can_send_messages = False

        print(f"👮 Bot status: {member.status}, can_send_messages: {can_send_messages}")
        if not can_send_messages:
            logging.error("❌ Бот не имеет прав отправлять сообщения в этот чат!")
            await query.message.reply_text("❌ У бота нет прав отправлять сообщения в этот чат!")
            return


        #✅ Ищем в сохранённых данных
        data = context.user_data.get(f"translation_{message_id}")
        if not data:
            logging.error(f"❌ Данные для message_id {message_id} не найдены в context.user_data!")
            msg = await query.message.reply_text("❌ Данные перевода не найдены!")
            add_service_msg_id(context, msg.message_id)
            return       

        # ✅ Получаем текст оригинала и перевода
        original_text = data["original_text"]
        user_translation = data["user_translation"]
        # ✅ Запускаем объяснение с помощью Claude
        explanation = await check_translation_with_claude(original_text, user_translation, update, context)
        if not explanation:
            logging.error("❌ Не удалось получить объяснение от Claude!")
            msg_1 = await query.message.reply_text("❌ Не удалось получить объяснение!")
            add_service_msg_id(context, msg_1.message_id)
            return          
      
        # ✅ Логируем попытку отправки комментария
        print(f"📩 Sending reply to message with message_id: {message_id} in chat ID: {chat_id}")
        escaped_explanation = escape_html_with_bold(explanation)

        print(f"explanation from handle_explain_request_function before escape_html_with_bold: {explanation}")
        print(f"explanation from handle_explain_request_function after escape_html_with_bold: : {escaped_explanation}")

        # ✅ Отправляем ответ как комментарий к сообщению
        await context.bot.send_message(
            chat_id=chat_id,
            text=escaped_explanation,
            parse_mode="HTML",
            reply_to_message_id=message_id  # 🔥 ПРИКРЕПЛЯЕМСЯ К СООБЩЕНИЮ
            )
        
        # ✅ Удаляем данные после успешной обработки
        del context.user_data[f"translation_{message_id}"]
        print(f"✅ Удалены данные для message_id {message_id}")

    except TelegramError as e:
            if 'message to reply not found' in str(e).lower():
                print(f"⚠️ Message ID {message_id} not found — возможно, сообщение удалено!")
                await query.message.reply_text("❌ Сообщение не найдено, возможно, оно было удалено!")
            else:
                logging.error(f"❌ Telegram Error: {e}")
                await query.message.reply_text(f"❌ Ошибка Telegram: {e}")
    except Exception as e:
        logging.error(f"❌ Ошибка в handle_explain_request: {e}")
        await query.message.reply_text(f"❌ Произошла ошибка: {e}. Попробуйте повторить запрос.")




#✅ Explain with Claude
async def check_translation_with_claude(original_text, user_translation, update, context):
    task_name = f"check_translation_with_claude"
    system_instruction_key = f"check_translation_with_claude"

    if update.callback_query:
        user = update.callback_query.from_user
        chat_id = update.callback_query.message.chat_id
    else:
        logging.error("❌ Нет callback_query в update!")
        return None, None
    #this client is for Claude
    #client = AsyncAnthropic(api_key=CLAUDE_API_KEY)

    user_message = f"""
Analyze the translation and return output in this strict format.

For each error (Error 1/2/3), include all fields in ONE line:
Error N: User fragment: "<exact wrong fragment from user's German translation>"; Issue: <what is wrong>; Correct fragment: "<how this fragment should be translated>"; Rule: <short grammar/lexical rule and why>.

Keep Error lines concise but informative.
Do not omit the user fragment and corrected fragment.

Then provide:
Correct Translation: ...
Grammar Explanation:
Alternative Sentence Construction: ...
Synonyms:
Original Word: ...
Possible Synonyms: ...

**Original sentence (Russian):** "{original_text}"
**User's translation (German):** "{user_translation}"
"""
    #available_models = await client.models.list()
    # logging.info(f"📢 Available models: {available_models}")
    # print(f"📢 Available models: {available_models}")
    
    #model_name = "claude-3-7-sonnet-20250219"  
    
    for attempt in range(3):
        try:
            #it is correct working with Claude model
            # response = await client.messages.create(
            #     model=model_name,
            #     messages=[{"role": "user", "content": prompt}],
            #     max_tokens=500,
            #     temperature=0.2
            # )
            response = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            logging.info(f"📥 FULL RESPONSE BODY: {response}")

            if response:
                cloud_response = response
                #this is for the claude model
                #cloud_response = response.content[0].text
                break
            else:
                logging.warning("⚠️ Claude returned an empty response.")
                print("❌ Ошибка: Claude вернул пустой ответ. We will try one more time in 5 seconds")
                await asyncio.sleep(5)
        
        except Exception as e:
            logging.error(f"❌ API Error from Claude: {e}")
            # Если ошибка действительно критическая — можно добавить проверку и выйти из цикла
            if "authentication" in str(e).lower() or "invalid token" in str(e).lower():
                logging.error("🚨 Критическая ошибка — завершаем цикл")
                break
            else:
                logging.warning("⚠️ Попробуем снова через 5 секунд...")
                await asyncio.sleep(5)

    else:
        print("❌ Ошибка: Пустой ответ от Claude после 3 попыток")
        return "❌ Ошибка: Не удалось обработать ответ от Claude."
    
    list_of_errors_pattern = re.findall(
        r'(Error)\s*(\d+)\:*\s*(.+?)(?=\nError\s*\d+\s*:|\nCorrect Translation:|\Z)',
        cloud_response,
        flags=re.DOTALL | re.IGNORECASE,
    )

    correct_translation = re.findall(r'(Correct Translation)\:\s*(.+?)(?:\n|$)', cloud_response, flags=re.DOTALL)

    grammar_explanation_pattern = re.findall(r'(Grammar Explanation)\s*\:*\s*\n*(.+?)(?=\n[A-Z][a-zA-Z\s]+:|\Z)',cloud_response,flags=re.DOTALL | re.IGNORECASE)

    altern_sentence_pattern = re.findall(r'(Alternative Construction|Alternative Sentence Construction)\:*\s*(.+?)(?=Synonyms|$)', cloud_response, flags=re.DOTALL | re.IGNORECASE)
    #(?:\n[A-Z][a-zA-Z\s]*\:|\Z) — захватываем до: или до новой строки с новым заголовком (\n + заглавная буква + слово + :) или до конца строки (\Z).
    synonyms_pattern = re.findall(r'Synonyms\:*\n([\s\S]*?)(?=\Z)',cloud_response,flags=re.DOTALL | re.IGNORECASE)

    if not list_of_errors_pattern and not correct_translation:
        logging.error("❌ Claude вернул некорректный формат ответа!")
        return "❌ Ошибка: Не удалось обработать ответ от Claude."
    
    # Собираем результат в список
    result_list = ["📥 *Detailed grammar explanation*:\n", f"🟢*Original russian sentence*:\n{original_text}\n", f"🟣*User translation*:\n{user_translation}\n"]

    # Добавляем ошибки
    for line in list_of_errors_pattern:
        result_list.append(f"🔴*{line[0]} {line[1]}*: {line[2].strip()}\n")

    # Добавляем корректный перевод
    for item in correct_translation:
        result_list.append(f"✅*{item[0]}*:\n➡️ {item[1]}\n")

    # Добавляем объяснения грамматики
    for k in grammar_explanation_pattern:
        result_list.append(f"🟡*{k[0]}*:")  # Добавляем заголовок
        grammar_parts = k[1].split("\n")  # Разбиваем текст по строкам
        for part in grammar_parts:
            clean_part = part.strip()
            if clean_part and clean_part not in ["-", ":"]:
                result_list.append(f"🔥{clean_part}")
    #result_list.append("\n")    

    # Добавляем альтернативные варианты
    for a in altern_sentence_pattern:
        result_list.append(f"\n🔵*{a[0]}*:\n {a[1].strip()}\n")  # Убираем лишние пробелы

    # Добавляем синонимы
    if synonyms_pattern:
        result_list.append("➡️ *Synonyms*:")
        #count = 0
        for s in synonyms_pattern:
            synonym_parts = s.split("\n")
            for part in synonym_parts:
                clean_part = part.strip()
                if not clean_part:
                    continue
                # if count > 0 and count % 2 == 0:
                #     result_list.append(f"{'-'*33}")
                result_list.append(f"🔄 {clean_part}")
                #count += 1

    # результат
    result_line_for_output = "\n".join(result_list)

    return result_line_for_output



async def log_translation_mistake(user_id, original_text, user_translation, categories, subcategories, score, correct_translation):
    global VALID_CATEGORIES, VALID_SUBCATEGORIES, VALID_CATEGORIES_lower, VALID_SUBCATEGORIES_lower
    #client = anthropic.Client(api_key=CLAUDE_API_KEY)

    # ✅ Логируем нормализованные значения
    if categories:
        print(f"🔎 LIST OF CATEGORIES FROM log_translation_function: {', '.join(categories)}")

    if subcategories:
        print(f"🔎 LIST OF SUBCATEGORIES log_translation_function: {', '.join(subcategories)}")


    # ✅ Перебираем все сочетания категорий и подкатегорий
    valid_combinations = []
    for cat in categories:
        cat_lower =cat.lower() # Приводим к нижнему регистру для соответствия VALID_SUBCATEGORIES
        for subcat in subcategories:
            subcat_lower = subcat.lower() # Приводим к нижнему регистру для соответствия VALID_SUBCATEGORIES
            if cat_lower in VALID_SUBCATEGORIES_lower and subcat_lower in VALID_SUBCATEGORIES_lower[cat_lower]:
                # ✅ Добавляем НОРМАЛИЗОВАННЫЕ значения для последующей обработки
                valid_combinations.append((cat_lower, subcat_lower))


    # ✅ Если есть хотя бы одно совпадение → логируем ВСЕ совпадения
    if valid_combinations:
        print(f"✅ Найдены следующие валидные комбинации ошибок выведенные в формате lower:")
        for main_category_lower, sub_category_lower in valid_combinations:
            print(f"➡️ {main_category_lower} - {sub_category_lower}")

    else:
        # ❗ Если не удалось классифицировать → помечаем как неклассифицированную ошибку
        print(f"⚠️ Ошибка классификации — помечаем как неклассифицированную.")
        valid_combinations.append(("Other mistake", "Unclassified mistake"))


    # ✅ Извлекаем уровень серьёзности ошибки (по умолчанию ставим 3)
    #severity = int(severity) if severity else 3

    # ✅ Проверка на идеальный перевод
    score = int(score) if score else 0


    # ✅ Если нет ошибок — не записываем в базу
    if len(valid_combinations) == 0:
        print(f"✅ Нет categories and subcategories соответствующих названию ошибок в базе данных — пропускаем запись в базу.")
        return

    # ✅ Убираем дубли из valid_combinations (чтобы не логировать одно и то же)
    valid_combinations = list(set(valid_combinations))


    # ✅ Логирование финальных данных для каждой комбинации
    for main_category, sub_category in valid_combinations:
        # ✅ Восстанавливаем оригинальные значения перед записью в базу данных
        main_category = next((cat for cat in VALID_CATEGORIES if cat.lower() == main_category), main_category)
        sub_category = next((subcat for subcat in VALID_SUBCATEGORIES.get(main_category, []) if subcat.lower() == sub_category), sub_category)
        
        if main_category == "Other mistake" and sub_category == "Unclassified mistake":
            print(f"⚠️ Ошибка '{main_category} - {sub_category}' добавлена в базу как неклассифицированная.")
        else:
            print(f"✅ Классифицировано: '{main_category} - {sub_category}'")

        print(f"🔍 Перед записью в БД: main_category = {main_category} | sub_category = {sub_category}")

        if not isinstance(user_id, int):
            print(f"❌ Ошибка типа данных: user_id = {type(user_id)}")
            return

        if not isinstance(main_category, str) or not isinstance(sub_category, str):
            print(f"❌ Ошибка типа данных: main_category = {type(main_category)}, sub_category = {type(sub_category)}")
            return


        # ✅ Запись в базу данных
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # ✅ Получаем id_for_mistake_table
                    cursor.execute("""
                    SELECT id_for_mistake_table 
                    FROM bt_3_daily_sentences
                    WHERE sentence=%s
                    LIMIT 1;
                """, (original_text, )
                    )
                    #sentence_id В нашем случае это идентификатор id_for_mistake_table Из таблицы bt_3_daily_sentences (для одинаковых предложений он одинаков) Для разных он разный.
                    # это нужно чтобы правильно Помечать предложения особенно одинаковые предложения и потом их правильно удалять из базы данных на основании этого идентификатора
                    result = cursor.fetchone()
                    sentence_id = result[0] if result else None

                    if sentence_id:
                        logging.info(f"✅ sentence_id для предложения '{original_text}': {sentence_id}")
                    else:
                        logging.warning(f"⚠️ sentence_id не найдено для предложения '{original_text}'")
                    
                    # ✅ Вставляем в таблицу ошибок с использованием общего идентификатора
                    #score = EXCLUDED.score означает:
                    # "Обновить поле score в существующей строке, установив его в то значение score, которое мы только что пытались вставить in VALUES".
                    cursor.execute("""
                        INSERT INTO bt_3_detailed_mistakes (
                            user_id, sentence, added_data, main_category, sub_category, mistake_count, sentence_id, correct_translation, score
                        ) VALUES (%s, %s, NOW(), %s, %s, 1, %s, %s, %s)
                        ON CONFLICT (user_id, sentence, main_category, sub_category)
                        DO UPDATE SET
                            mistake_count = bt_3_detailed_mistakes.mistake_count + 1,
                            attempt = bt_3_detailed_mistakes.attempt + 1,
                            last_seen = NOW(),
                            score = EXCLUDED.score;
                    """, (user_id, original_text, main_category, sub_category, sentence_id, correct_translation, score) # получить его из таблицы bt_daily_sentences Выше уже мы к этой таблице обращаемся и получаем из неё что-то добавить ещё session_id
                    )
                    
                    conn.commit()
                    print(f"✅ Ошибка '{main_category} - {sub_category}' успешно записана в базу.")
                
                except Exception as e:
                    print(f"❌ Ошибка при записи в БД: {e}")
                    logging.error(f"❌ Ошибка при записи в БД: {e}")

    # ✅ Логирование успешного завершения обработки
    print(f"✅ Все ошибки успешно обработаны!")


async def check_user_translation(update: Update, context: CallbackContext, translation_text=None):
    
    if update.message is None or update.message.text is None:
        logging.warning("⚠️ update.message отсутствует в check_user_translation().")
        return
    
    if "pending_translations" in context.user_data and context.user_data["pending_translations"]:
        translation_text = "\n".join(context.user_data["pending_translations"])
        #context.user_data["pending_translations"] = []
    
    # Убираем команду "/translate", оставляя только переводы
    # message_text = update.message.text.strip()
    # translation_text = message_text.replace("/translate", "").strip()

    # Разбираем входной текст на номера предложений и переводы
    pattern = re.compile(r"(\d+)\.\s*([^\d\n]+(?:\n[^\d\n]+)*)", re.MULTILINE)
    translations = pattern.findall(translation_text)
    
    print(f"✅ Извлечено {len(translations)} переводов: {translations}")

    if not translations:
        msg_2 = await update.message.reply_text("❌ Ошибка: Формат перевода неверен. Должно быть: 1. <перевод>")
        add_service_msg_id(context, msg_2.message_id)
        return

    # Получаем ID пользователя
    user_id = update.message.from_user.id
    username = update.message.from_user.first_name

    # Подключаемся к базе данных
    conn = get_db_connection()
    cursor = conn.cursor()

    # Получаем разрешённые номера предложений
    cursor.execute("""
        SELECT unique_id FROM bt_3_daily_sentences WHERE date = CURRENT_DATE AND user_id = %s
    """, (user_id,))
    
    allowed_sentences = {row[0] for row in cursor.fetchall()}  # Собираем в set() для быстрого поиска

    # Проверяем каждое предложение
    results = []  # Храним результаты для Telegram

    for number_str, user_translation in translations:
        try:
            sentence_number = int(number_str)

            # Проверяем, принадлежит ли это предложение пользователю
            if sentence_number not in allowed_sentences:
                results.append(f"❌ Ошибка: Предложение {sentence_number} вам не принадлежит!")
                continue

            # Получаем оригинальный текст предложения
            cursor.execute("""
                SELECT id, sentence, session_id, id_for_mistake_table FROM bt_3_daily_sentences 
                WHERE date = CURRENT_DATE AND unique_id = %s AND user_id = %s;
            """, (sentence_number, user_id))

            row = cursor.fetchone()

            if not row:
                results.append(f"❌ Ошибка: Предложение {sentence_number} не найдено.")
                continue

            sentence_id, original_text, session_id, id_for_mistake_table  = row

            # Проверяем, отправлял ли этот пользователь перевод этого предложения
            cursor.execute("""
                SELECT id FROM bt_3_translations 
                WHERE user_id = %s AND sentence_id = %s AND timestamp::date = CURRENT_DATE;
            """, (user_id, sentence_id))

            existing_translation = cursor.fetchone()
            if existing_translation:
                results.append(f"⚠️ Вы уже переводили предложение {sentence_number}. Только первый перевод учитывается!")
                continue

            logging.info(f"📌 Проверяем перевод №{sentence_number}: {user_translation}")

            # Проверяем перевод через GPT
            MAX_FEEDBACK_LENGTH = 1000  # Ограничим длину комментария GPT

            try:
                feedback, categories, subcategories, score, correct_translation = await check_translation(original_text, user_translation, update, context, sentence_number)

            except Exception as e:
                print(f"⚠️ Ошибка при проверке перевода №{sentence_number}: {e}")
                logging.error(f"⚠️ Ошибка при проверке перевода №{sentence_number}: {e}", exc_info=True)
                feedback = "⚠️ Ошибка: не удалось проверить перевод."

            score = int(score) if score else 50

            # Обрезаем, если слишком длинный
            if len(feedback) > MAX_FEEDBACK_LENGTH:
                feedback = feedback[:MAX_FEEDBACK_LENGTH] + "...\n⚠️ Ответ GPT был сокращён."
            
            # ✅ Добавляем результат для последующей отправки    
            results.append(f"📜 **Предложение {sentence_number}**\n🎯 Оценка: {feedback}")

            # ✅ Сохраняем перевод в базу данных с защитой от ошибок
            cursor.execute("""
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id, user_translation, score, feedback)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (user_id, id_for_mistake_table, session_id, username, sentence_id, user_translation, score, feedback))

            conn.commit()

            # Проверяем: реально ли это предложение есть в базе ошибок?
            cursor.execute("""
                SELECT COUNT(*) FROM bt_3_detailed_mistakes
                WHERE sentence_id = %s AND user_id = %s;
            """, (id_for_mistake_table, user_id))

            was_in_mistakes = cursor.fetchone()[0] > 0

            # === КЛЮЧЕВАЯ ЛОГИКА ===

            if was_in_mistakes:
                if score >= 85:
                    # Получаем текущую максимальную попытку
                    cursor.execute("""
                        SELECT attempt 
                        FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id = %s;
                    """, (id_for_mistake_table, user_id))
                    
                    result = cursor.fetchone()
                    total_attempts = (result[0] or 0) + 1 # +1 — текущая попытка, если успешная

                    # Переносим в успешные
                    cursor.execute("""
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES (%s, %s, %s, %s, NOW());
                    """, (user_id, id_for_mistake_table, score, total_attempts))

                    # Удаляем из ошибок
                    cursor.execute("""
                        DELETE FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                    """, (id_for_mistake_table, user_id))

                    cursor.execute("""
                        DELETE FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id= %s;
                    """,(id_for_mistake_table, user_id))

                    conn.commit()
                    logging.info(f"✅ Перевод №{sentence_number} перемещён в успешные и удалён из ошибок.")
                else:
                    logging.info(f"⚠️ Перевод №{sentence_number} пока не набрал 85, остаётся в ошибках.")

                    # Если мы не набрали 85 Баллов то необходимо увел attempt 
                    cursor.execute("""
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table, timestamp)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET 
                            attempt = bt_3_attempts.attempt + 1,
                            timestamp= NOW();
                    """, (user_id, id_for_mistake_table))
                    
                    conn.commit()

                continue  # не идём дальше

            # Новый перевод (не был в ошибках)
            if not was_in_mistakes:
                if score >= 80:
                    cursor.execute("""
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES(%s, %s, %s, %s, NOW());
                    """, (user_id, id_for_mistake_table, score, 1))
                    conn.commit()
                    logging.info(f"✅ Новый успешный перевод №{sentence_number}, {score}/100")
                    continue
                else:
                    # Добавляем в ошибки
                    try:
                        # Если перевод не набрал 80 С первого раза Мы должны увеличить счётчик attempt С 0 До 1 (по умолчанию стоит 1 Если мы вносим в таблицу предложения)
                        cursor.execute("""
                            INSERT INTO bt_3_attempts (user_id, id_for_mistake_table)
                            VALUES (%s, %s)
                            ON CONFLICT (user_id, id_for_mistake_table)
                            DO UPDATE SET attempt = bt_3_attempts.attempt + 1;
                        """, (user_id, id_for_mistake_table))
                        conn.commit()
                        logging.info(f"✅ Записана попытка в bt_3_attempts: id_for_mistake_table={id_for_mistake_table}, score={score}")

                        await log_translation_mistake(
                            user_id, original_text, user_translation,
                            categories, subcategories, score, correct_translation
                        )
                        logging.info(f"🟥 Добавлен в ошибки: №{sentence_number}, score={score}")

                    except Exception as e:
                        logging.error(f"❌ Ошибка при записи ошибки: {e}")

        except Exception as e:
            logging.error(f"❌ Ошибка обработки предложения {number_str}: {e}")
            
    cursor.close()
    conn.close()


async def check_user_translation_webapp(user_id: int, username: str | None, translations: list[dict]) -> list[dict]:
    if not translations:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT unique_id, id_for_mistake_table, id, sentence, session_id
            FROM bt_3_daily_sentences
            WHERE date = CURRENT_DATE AND user_id = %s;
        """, (user_id,))
        allowed_rows = cursor.fetchall()
        allowed_by_mistake_id = {
            row[1]: {
                "unique_id": row[0],
                "sentence_id": row[2],
                "sentence": row[3],
                "session_id": row[4],
            }
            for row in allowed_rows
        }

        results = []

        for entry in translations:
            sentence_id_for_mistake = entry.get("id_for_mistake_table")
            user_translation = (entry.get("translation") or "").strip()
            if not sentence_id_for_mistake or not user_translation:
                continue

            if sentence_id_for_mistake not in allowed_by_mistake_id:
                results.append({
                    "sentence_number": None,
                    "error": "Предложение не принадлежит пользователю или не найдено.",
                })
                continue

            sentence_info = allowed_by_mistake_id[sentence_id_for_mistake]
            sentence_number = sentence_info["unique_id"]
            original_text = sentence_info["sentence"]
            session_id = sentence_info["session_id"]
            sentence_pk_id = sentence_info["sentence_id"]

            cursor.execute("""
                SELECT id FROM bt_3_translations
                WHERE user_id = %s AND sentence_id = %s AND timestamp::date = CURRENT_DATE;
            """, (user_id, sentence_pk_id))

            existing_translation = cursor.fetchone()
            if existing_translation:
                results.append({
                    "sentence_number": sentence_number,
                    "error": "Вы уже переводили это предложение.",
                })
                continue

            try:
                feedback, categories, subcategories, score, correct_translation = await check_translation(
                    original_text,
                    user_translation,
                    None,
                    None,
                    sentence_number,
                )
            except Exception as exc:
                logging.error(f"⚠️ Ошибка при проверке перевода №{sentence_number}: {exc}", exc_info=True)
                results.append({
                    "sentence_number": sentence_number,
                    "error": "Ошибка: не удалось проверить перевод.",
                })
                continue

            score_value = int(score) if score and str(score).isdigit() else 0

            cursor.execute("""
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id, user_translation, score, feedback)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (user_id, sentence_id_for_mistake, session_id, username, sentence_pk_id, user_translation, score_value, feedback))

            conn.commit()

            cursor.execute("""
                SELECT COUNT(*) FROM bt_3_detailed_mistakes
                WHERE sentence_id = %s AND user_id = %s;
            """, (sentence_id_for_mistake, user_id))

            was_in_mistakes = cursor.fetchone()[0] > 0

            if was_in_mistakes:
                if score_value >= 85:
                    cursor.execute("""
                        SELECT attempt
                        FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id = %s;
                    """, (sentence_id_for_mistake, user_id))

                    result = cursor.fetchone()
                    total_attempts = (result[0] or 0) + 1

                    cursor.execute("""
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES (%s, %s, %s, %s, NOW());
                    """, (user_id, sentence_id_for_mistake, score_value, total_attempts))

                    cursor.execute("""
                        DELETE FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                    """, (sentence_id_for_mistake, user_id))

                    cursor.execute("""
                        DELETE FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id= %s;
                    """, (sentence_id_for_mistake, user_id))

                    conn.commit()
                else:
                    cursor.execute("""
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table, timestamp)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET
                            attempt = bt_3_attempts.attempt + 1,
                            timestamp= NOW();
                    """, (sentence_id_for_mistake, user_id))

                    conn.commit()
            else:
                if score_value >= 80:
                    cursor.execute("""
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES(%s, %s, %s, %s, NOW());
                    """, (user_id, sentence_id_for_mistake, score_value, 1))
                    conn.commit()
                else:
                    cursor.execute("""
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET attempt = bt_3_attempts.attempt + 1;
                    """, (user_id, sentence_id_for_mistake))
                    conn.commit()

                    await log_translation_mistake(
                        user_id,
                        original_text,
                        user_translation,
                        categories,
                        subcategories,
                        score_value,
                        correct_translation,
                    )

            results.append({
                "sentence_number": sentence_number,
                "score": score_value,
                "original_text": original_text,
                "user_translation": user_translation,
                "correct_translation": correct_translation,
                "feedback": feedback,
            })

        results.sort(key=lambda item: item.get("sentence_number") or 0)
        return results

    finally:
        cursor.close()
        conn.close()



async def get_original_sentences(user_id, context: CallbackContext):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
    
        # Выполняем SQL-запрос: выбираем 1 случайных предложений из базы данных в которую мы предварительно поместили предложение
        cursor.execute("SELECT sentence FROM bt_3_sentences ORDER BY RANDOM() LIMIT 1;")
        rows = [row[0] for row in cursor.fetchall()]   # Возвращаем список предложений
        print(f"📌 Найдено в базе данных: {rows}") # ✅ Логируем результат

        # ✅ Загружаем все предложения из базы ошибок
        cursor.execute("""
            SELECT sentence, sentence_id
            FROM bt_3_detailed_mistakes
            WHERE user_id = %s
            ORDER BY mistake_count DESC, last_seen ASC; 
        """, (user_id, ))
        
        # ✅ Используем set() для удаления дубликатов по sentence_id
        already_given_sentence_ids = set()
        unique_sentences = set()
        mistake_sentences = []

        for sentence, sentence_id in cursor.fetchall():
            if sentence_id and sentence_id not in already_given_sentence_ids:
                if sentence_id not in unique_sentences:
                    unique_sentences.add(sentence_id)
                    mistake_sentences.append(sentence)
                    already_given_sentence_ids.add(sentence_id)

                    # ✅ Ограничиваем до нужного количества предложений (например, 5)
                    if len(mistake_sentences) == 5:
                        break


        print(f"✅ Уникальные предложения из базы ошибок: {len(mistake_sentences)} / 5")

        # 🔹 3. Определяем, сколько предложений не хватает до 7
        num_sentences = 7 - len(rows) - len(mistake_sentences)

        print(f"📌 Найдено: {len(rows)} в базе данных + {len(mistake_sentences)} повторение ошибок. Генерируем ещё {num_sentences} предложений.")
        gpt_sentences = []
        
        # 📌 3. Остальные предложений генерируем через GPT
        if num_sentences > 0:
            print("⚠️ Генерируем дополнительные предложения через GPT-4...")
            gpt_sentences = await generate_sentences(user_id, num_sentences, context)
            #print(f"🚀 Сгенерированные GPT предложения: {gpt_sentences}") # ✅ Логируем результат
            
        
        def normalize_sentences(items):
            normalized = []
            seen = set()
            for item in items:
                if not item:
                    continue
                text = str(item).strip()
                if not text:
                    continue
                for line in text.split("\n"):
                    candidate = re.sub(r"^\s*\d+\.\s*", "", line).strip()
                    if not candidate:
                        continue
                    if candidate in seen:
                        continue
                    seen.add(candidate)
                    normalized.append(candidate)
            return normalized

        # ✅ Проверяем финальный список предложений
        final_sentences = normalize_sentences(rows + mistake_sentences + gpt_sentences)
        print(f"✅ Финальный список предложений: {final_sentences}")

        attempts = 0
        while len(final_sentences) < 7 and attempts < 3:
            needed = 7 - len(final_sentences)
            extra_sentences = await generate_sentences(user_id, needed, context)
            final_sentences = normalize_sentences(final_sentences + extra_sentences)
            attempts += 1
        
        if not final_sentences:
            print("❌ Ошибка: Не удалось получить предложения!")
            return []  # Вернём пустой список в случае ошибки
        
        return final_sentences
    
    finally: # Закрываем курсор и соединение **в конце**, независимо от того, какая ветка выполнялась
        cursor.close()
        conn.close()



# Указываем ID нужных каналов
PREFERRED_CHANNELS = [
    "UCthmoIZKvuR1-KuwednkjHg",  # Deutsch mit Yehor
    "UCHLkEhIoBRu2JTqYJlqlgbw",  # Deutsch mit Rieke
    "UCeVQK7ZPXDOAyjY0NYqmX-Q", # Benjamin - Der Deutschlehrer
    "UCuVbK_d3wh3M8TYUk5aFCiQ",   # Lera
    "UCsxqCqZHE6guBCdSUEWpPsg",
    "UCm-E8MXdNquzETSsNxgoWig",
    "UCjdRXC3Wh2hDq8Utx7RIaMw",
    "UC9rwo-ES6aDKxD2qqkL6seA",
    "UCVx6RFaEAg46xfbsDjb440A",
    "UCvs8dBa7v3ti1QDaXE7dtKw",
    "UCE2vOZZIluHMtt2sAXhRhcw"
]

def search_youtube_videous(
    topic,
    max_results=5,
    *,
    main_category=None,
    sub_category=None,
    target_lang="de",
):
    skill_title = str(topic or "").strip()
    main_category, sub_category = _sanitize_focus_topic(main_category, sub_category)

    query_stages = [
        _build_video_search_queries(
            main_category,
            sub_category,
            skill_title=skill_title,
            target_lang=target_lang,
        ),
        _build_video_search_queries(
            main_category,
            None,
            skill_title=skill_title,
            target_lang=target_lang,
        ),
        _build_video_search_queries(
            None,
            sub_category,
            skill_title=skill_title,
            target_lang=target_lang,
        ),
    ]
    if skill_title:
        query_stages.append([skill_title])

    queries = []
    seen_queries = set()
    for stage in query_stages:
        for query in stage:
            normalized = " ".join(str(query or "").strip().lower().split())
            if not normalized or normalized in seen_queries:
                continue
            seen_queries.add(normalized)
            queries.append(str(query).strip())

    print(
        f"ℹ️ YouTube recommendation search: topic='{skill_title}' "
        f"main='{main_category}' sub='{sub_category}' queries={queries}"
    )

    def _filter_weekly_recommendation_videos(videos):
        filtered = []
        for item in videos or []:
            video = dict(item or {})
            if _is_youtube_short_like(video):
                continue
            if _video_conflicts_with_target_language(video, target_lang=target_lang, native_lang=""):
                continue
            duration_seconds = int(video.get("duration_seconds") or 0)
            if duration_seconds <= 0:
                duration_seconds = _parse_iso8601_duration_to_seconds(video.get("duration"))
                if duration_seconds > 0:
                    video["duration_seconds"] = duration_seconds
            if 0 < duration_seconds < 120:
                continue
            filtered.append(video)
        return filtered

    collected_videos = {}
    for query in queries:
        try:
            videos, provider_name = _youtube_search_videos_manual(
                query,
                max_results=max_results,
                target_lang=target_lang,
            )
            if not videos:
                continue
            videos = _youtube_fill_view_counts(videos, billing_target_lang=target_lang)
            filtered_videos = _filter_weekly_recommendation_videos(videos)
            if filtered_videos:
                videos = filtered_videos
            else:
                print(
                    f"ℹ️ Weekly YouTube search fallback keeps raw provider results: "
                    f"query='{query}' provider='{provider_name}' raw={len(videos)}"
                )
            for video in videos:
                video_id = str(video.get("video_id") or "").strip()
                if not video_id:
                    continue
                existing = collected_videos.get(video_id)
                candidate = {
                    "video_id": video_id,
                    "title": str(video.get("title") or "").strip(),
                    "views": int(video.get("views") or 0),
                    "query": query,
                    "provider": provider_name,
                }
                if not existing or candidate["views"] > int(existing.get("views") or 0):
                    collected_videos[video_id] = candidate
        except Exception as search_error:
            print(f"⚠️ Ошибка поиска YouTube по запросу '{query}': {search_error}")

    if not collected_videos:
        return []

    top_videos = sorted(
        collected_videos.values(),
        key=lambda item: int(item.get("views") or 0),
        reverse=True,
    )[:2]

    preferred_videos = [
        f'<a href="{html.escape("https://www.youtube.com/watch?v=" + video["video_id"])}">▶️ {escape_html_with_bold(video["title"])}</a>'
        for video in top_videos
    ]

    print(f"preferred_videos after escape_html_with_bold: {preferred_videos}")
    return preferred_videos


_WEEKLY_RECOMMENDATION_EXCLUDED_SUBCATEGORY_KEYS = {
    "unclassified mistake",
    "unclassified mistakes",
    "неизвестно",
    "unknown",
    "unknown mistake",
    "unknown mistakes",
}
_WEEKLY_RECOMMENDATION_EXCLUDED_CATEGORY_KEYS = {
    "неизвестно",
    "unknown",
    "unknown mistake",
    "unknown mistakes",
}


def _normalize_weekly_recommendation_label(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _exclude_weekly_recommendation_topic(main_category, sub_category) -> bool:
    main = _normalize_weekly_recommendation_label(main_category)
    sub = _normalize_weekly_recommendation_label(sub_category)
    if sub in _WEEKLY_RECOMMENDATION_EXCLUDED_SUBCATEGORY_KEYS:
        return True
    if main in _WEEKLY_RECOMMENDATION_EXCLUDED_CATEGORY_KEYS:
        return True
    if main in {"other mistake", "other mistakes"} and not sub:
        return True
    return False


def _get_weekly_recommendation_topics(
    user_id: int,
    *,
    lookback_days: int = 7,
    target_lang: str = "de",
) -> list[dict]:
    try:
        rows = list_top_weak_topics(
            user_id=int(user_id),
            lookback_days=max(1, int(lookback_days or 7)),
            source_lang="ru",
            target_lang=target_lang,
            limit=10,
        )
    except Exception:
        rows = []

    topics: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        main_category = str(row.get("main_category") or "").strip()
        sub_category = str(row.get("sub_category") or "").strip()
        if _exclude_weekly_recommendation_topic(main_category, sub_category):
            continue
        identity = (main_category.lower(), sub_category.lower())
        if identity in seen:
            continue
        seen.add(identity)
        topics.append(
            {
                "main_category": main_category,
                "sub_category": sub_category,
                "mistakes": int(row.get("mistakes") or 0),
            }
        )
    return topics


#📌 this function will filter and rate mistakes
async def rate_mistakes(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            
            # we calculate amount of translated sentences of the user in a week 
            cursor.execute("""
                SELECT COUNT(sentence_id) 
                FROM bt_3_translations 
                WHERE user_id = %s AND timestamp >= NOW() - INTERVAL '6 days'; 
            """, (user_id,))
            total_sentences = cursor.fetchone()
            total_sentences = total_sentences[0] if isinstance(total_sentences, tuple) else total_sentences or 0

            # ✅ 2. Select and calculate all mistakes KPI within a week
            cursor.execute("""
                SELECT COUNT(*) AS mistakes_week
                FROM bt_3_detailed_mistakes
                WHERE user_id = %s
                AND added_data >= NOW() - INTERVAL '6 days'
            """, (user_id,))
            mistakes_row = cursor.fetchone()
            mistakes_week = mistakes_row[0] if isinstance(mistakes_row, tuple) else mistakes_row or 0

            cursor.execute("""
                SELECT
                    COALESCE(NULLIF(TRIM(main_category), ''), 'неизвестно') AS main_category,
                    COALESCE(NULLIF(TRIM(sub_category), ''), 'неизвестно') AS sub_category,
                    COUNT(*) AS mistakes_count
                FROM bt_3_detailed_mistakes
                WHERE user_id = %s
                  AND added_data >= NOW() - INTERVAL '6 days'
                GROUP BY 1, 2
                ORDER BY mistakes_count DESC, main_category ASC, sub_category ASC;
            """, (user_id,))
            grouped_rows = cursor.fetchall() or []

            filtered_rows = []
            for row in grouped_rows:
                if not row:
                    continue
                main_category = str(row[0] or "").strip() or "неизвестно"
                sub_category = str(row[1] or "").strip() or "неизвестно"
                mistakes_count = int(row[2] or 0)
                if _exclude_weekly_recommendation_topic(main_category, sub_category):
                    continue
                filtered_rows.append((main_category, sub_category, mistakes_count))

            top_mistake_category = "неизвестно"
            number_of_top_category_mistakes = 0
            top_mistake_subcategory_1 = ""
            top_mistake_subcategory_2 = ""

            if filtered_rows:
                category_totals = {}
                for main_category, _, mistakes_count in filtered_rows:
                    category_totals[main_category] = category_totals.get(main_category, 0) + mistakes_count

                top_mistake_category, number_of_top_category_mistakes = sorted(
                    category_totals.items(),
                    key=lambda item: (-int(item[1]), str(item[0]).lower()),
                )[0]

                top_subcategories = [
                    (sub_category, mistakes_count)
                    for main_category, sub_category, mistakes_count in filtered_rows
                    if main_category == top_mistake_category
                ]
                top_subcategories.sort(key=lambda item: (-int(item[1]), str(item[0]).lower()))

                if top_subcategories:
                    top_mistake_subcategory_1 = str(top_subcategories[0][0] or "").strip()
                if len(top_subcategories) > 1:
                    top_mistake_subcategory_2 = str(top_subcategories[1][0] or "").strip()


    return total_sentences, mistakes_week, top_mistake_category, number_of_top_category_mistakes, top_mistake_subcategory_1, top_mistake_subcategory_2


# ✅ Функция для проверки статуса ссылки
async def check_url(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return True
                else:
                    print(f"⚠️ Ошибка ссылки {url} - Статус: {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Ошибка при проверке ссылки {url}: {e}")
        return False

# Полностью рабочая функция однако не получается экранировать чтобы оставить жирным текст в ** текст**.
# def escape_markdown_v2(text):
#     # Экранируем только спецсимволы Markdown
#     if not isinstance(text, str):
#         text = str(text)
#     escape_chars = r'_*[]()~`>#+-=|{}.,!:'
#     return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

def escape_html_with_bold(text):
    if not isinstance(text, str):
        text = str(text)
    
    # Сначала заменим *text* на <b>text</b>
    bold_pattern = r'\*(.*?)\*'
    text = re.sub(bold_pattern, r'<b>\1</b>', text)
    
    # Теперь экранируем весь остальной текст кроме наших тэгов
    def escape_except_tags(part):
        if part.startswith('<b>') and part.endswith('</b>'):
            # Внутри <b>...</b> тоже нужно экранировать
            inner = html.escape(part[3:-4])
            return f"<b>{inner}</b>"
        else:
            return html.escape(part)
    
    # Разбиваем текст на куски: либо <b>...</b> либо обычный текст
    #re.split(r'(<b>.*?</b>)', text) работает так:
    #Разбивает текст вокруг кусков <b>...</b>,И сохраняет сами <b>...</b> в список благодаря скобкам () в регулярке.
    parts = re.split(r'(<b>.*?</b>)', text)
    escaped_parts = [escape_except_tags(part) for part in parts]
    return ''.join(escaped_parts)



def _log_delivery_route(job_name: str, user_id: int, target_chat_id: int, reason: str) -> None:
    if not DELIVERY_ROUTE_DEBUG_ENABLED:
        return
    logging.info(
        "📬 Delivery route: job=%s user_id=%s target_chat_id=%s reason=%s",
        str(job_name or "unknown"),
        int(user_id),
        int(target_chat_id),
        str(reason or "unknown"),
    )


async def _resolve_user_delivery_chat_id(
    context: CallbackContext,
    user_id: int,
    *,
    job_name: str = "unknown",
) -> int:
    safe_user_id = int(user_id)
    candidate_chat_ids: list[int] = []
    route_reason = "fallback_private"

    try:
        scope_state = await asyncio.to_thread(get_webapp_scope_state, safe_user_id)
    except Exception:
        scope_state = {}
    if bool(scope_state.get("has_state")) and str(scope_state.get("scope_kind") or "").strip().lower() == "personal":
        _log_delivery_route(job_name, safe_user_id, safe_user_id, "scope_personal")
        return safe_user_id
    if str(scope_state.get("scope_kind") or "").strip().lower() == "group":
        try:
            scope_chat_id = int(scope_state.get("scope_chat_id"))
            candidate_chat_ids.append(scope_chat_id)
        except Exception:
            pass

    for only_confirmed in (True, False):
        try:
            contexts = await asyncio.to_thread(
                list_webapp_group_contexts,
                safe_user_id,
                20,
                only_confirmed=only_confirmed,
            )
        except Exception:
            contexts = []
        for item in contexts:
            try:
                chat_id = int(item.get("chat_id"))
            except Exception:
                continue
            if chat_id not in candidate_chat_ids:
                candidate_chat_ids.append(chat_id)

    for chat_id in candidate_chat_ids:
        try:
            member = await context.bot.get_chat_member(chat_id=int(chat_id), user_id=safe_user_id)
            status = str(getattr(member, "status", "") or "").strip().lower()
            if status in {"creator", "administrator", "member", "restricted"}:
                route_reason = f"group_membership:{chat_id}"
                _log_delivery_route(job_name, safe_user_id, int(chat_id), route_reason)
                return int(chat_id)
        except Exception:
            continue

    _log_delivery_route(job_name, safe_user_id, safe_user_id, route_reason)
    return safe_user_id


async def _collect_scheduler_candidate_user_ids(
    *,
    lookback_days: int = 30,
    include_allowed: bool = True,
    include_admins: bool = True,
) -> list[int]:
    safe_days = max(1, int(lookback_days or 30))
    user_ids: set[int] = set()
    skipped_synthetic = 0

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT user_id
                FROM bt_3_messages
                WHERE timestamp >= NOW() - (%s || ' days')::interval
                  AND user_id IS NOT NULL
                UNION
                SELECT DISTINCT user_id
                FROM bt_3_translations
                WHERE timestamp >= NOW() - (%s || ' days')::interval
                  AND user_id IS NOT NULL
                UNION
                SELECT DISTINCT user_id
                FROM bt_3_user_progress
                WHERE start_time >= NOW() - (%s || ' days')::interval
                  AND user_id IS NOT NULL;
                """,
                (safe_days, safe_days, safe_days),
            )
            for row in cursor.fetchall() or []:
                try:
                    candidate = int(row[0])
                except Exception:
                    continue
                if candidate <= 0:
                    continue
                if _is_synthetic_telegram_user_id(candidate):
                    skipped_synthetic += 1
                    continue
                if candidate > 0:
                    user_ids.add(candidate)

    if include_allowed:
        try:
            allowed_rows = await asyncio.to_thread(list_allowed_telegram_users, 2000)
        except Exception:
            allowed_rows = []
        for row in allowed_rows:
            try:
                candidate = int((row or {}).get("user_id") or 0)
            except Exception:
                continue
            if candidate <= 0:
                continue
            if _is_synthetic_telegram_user_id(candidate):
                skipped_synthetic += 1
                continue
            if candidate > 0:
                user_ids.add(candidate)

    if include_admins:
        for admin_id in get_admin_telegram_ids():
            try:
                candidate = int(admin_id)
            except Exception:
                continue
            if candidate > 0:
                user_ids.add(candidate)

    if skipped_synthetic > 0:
        logging.info(
            "ℹ️ scheduler candidate collection skipped %s synthetic load-test user id(s)",
            skipped_synthetic,
        )
    return sorted(user_ids)


async def _build_user_delivery_map(
    context: CallbackContext,
    user_ids: list[int] | set[int] | tuple[int, ...],
    *,
    job_name: str = "unknown",
) -> dict[int, int]:
    mapping: dict[int, int] = {}
    prepared: list[int] = []
    for raw in user_ids:
        try:
            user_id = int(raw)
        except Exception:
            continue
        if user_id > 0 and user_id not in mapping:
            prepared.append(user_id)

    for user_id in prepared:
        mapping[user_id] = await _resolve_user_delivery_chat_id(
            context,
            user_id,
            job_name=job_name,
        )
    return mapping


async def _collect_scheduler_delivery_targets(
    context: CallbackContext,
    *,
    lookback_days: int = 30,
    job_name: str = "unknown",
) -> list[int]:
    user_ids = await _collect_scheduler_candidate_user_ids(
        lookback_days=lookback_days,
        include_allowed=True,
        include_admins=True,
    )
    delivery_map = await _build_user_delivery_map(context, user_ids, job_name=job_name)
    targets: list[int] = []
    seen: set[int] = set()
    for _, chat_id in sorted(delivery_map.items()):
        try:
            target = int(chat_id)
        except Exception:
            continue
        if target in seen:
            continue
        seen.add(target)
        targets.append(target)
    return targets


async def _send_analytics_message_with_fallback(
    context: CallbackContext,
    *,
    user_id: int,
    target_chat_id: int,
    text: str,
) -> None:
    safe_user_id = int(user_id)
    safe_target_chat_id = int(target_chat_id)
    try:
        await context.bot.send_message(
            chat_id=safe_target_chat_id,
            text=text,
            parse_mode="HTML",
        )
        return
    except Exception as primary_exc:
        if safe_target_chat_id == safe_user_id:
            logging.warning(
                "⚠️ Не удалось отправить weekly analytics в личку user_id=%s: %s",
                safe_user_id,
                primary_exc,
            )
            return

        logging.warning(
            "⚠️ Не удалось отправить weekly analytics в группу chat_id=%s для user_id=%s: %s. Пробуем личку.",
            safe_target_chat_id,
            safe_user_id,
            primary_exc,
        )

    try:
        await context.bot.send_message(
            chat_id=safe_user_id,
            text=text,
            parse_mode="HTML",
        )
    except Exception as fallback_exc:
        logging.warning(
            "⚠️ Не удалось отправить weekly analytics fallback в личку user_id=%s: %s",
            safe_user_id,
            fallback_exc,
        )


# 📌📌📌📌📌
async def send_me_analytics_and_recommend_me(context: CallbackContext):
    task_name = "send_me_analytics_and_recommend_me"
    system_instruction_key = "send_me_analytics_and_recommend_me"
    skipped_synthetic_users = 0

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT user_id
                FROM bt_3_translations
                WHERE timestamp >= NOW() - INTERVAL '6 days'
                  AND user_id IS NOT NULL;
                """
            )
            user_ids = cursor.fetchall()

    if not user_ids:
        print("❌ Нет активных пользователей за последнюю неделю.")
        return

    for user_id, in user_ids:
        safe_user_id = int(user_id)
        if _is_synthetic_telegram_user_id(safe_user_id):
            skipped_synthetic_users += 1
            continue
        delivery_chat_id = await _resolve_user_delivery_chat_id(
            context,
            safe_user_id,
            job_name="send_me_analytics_and_recommend_me",
        )

        total_sentences, mistakes_week, top_mistake_category, number_of_top_category_mistakes, top_mistake_subcategory_1, top_mistake_subcategory_2 = await rate_mistakes(safe_user_id)
        if total_sentences:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT DISTINCT username FROM bt_3_translations WHERE user_id = %s;
                        """,
                        (safe_user_id,),
                    )
                    result = cursor.fetchone()
                    username = result[0] if result else "Unknown User"

            ranked_topics = _get_weekly_recommendation_topics(
                safe_user_id,
                lookback_days=7,
                target_lang="de",
            )
            if not ranked_topics and (top_mistake_category or top_mistake_subcategory_1):
                ranked_topics = [
                    {
                        "main_category": str(top_mistake_category or "").strip(),
                        "sub_category": str(top_mistake_subcategory_1 or "").strip(),
                        "mistakes": int(number_of_top_category_mistakes or 0),
                    }
                ]

            video_data = []
            selected_video_topic = None
            for topic_row in ranked_topics:
                candidate_main_category = str(topic_row.get("main_category") or "").strip()
                candidate_sub_category = str(topic_row.get("sub_category") or "").strip()
                user_message = f"""
                - **Категория ошибки:** {candidate_main_category}
                - **Первая подкатегория:** {candidate_sub_category}
                - **Вторая подкатегория:** 
                """
                topic = candidate_sub_category or candidate_main_category or "Deutsch Grammatik"

                for attempt in range(5):
                    try:
                        topic = await llm_execute(
                            task_name=task_name,
                            system_instruction_key=system_instruction_key,
                            user_message=user_message,
                            poll_interval_seconds=1.0,
                        )
                        print(f"📌 Определена тема: {topic}")
                        break
                    except openai.RateLimitError:
                        wait_time = (attempt + 1) * 5
                        print(f"⚠️ OpenAI API перегружен. Ждём {wait_time} сек...")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        print(f"⚠️ Ошибка OpenAI: {e}")
                        continue

                candidate_video_data = search_youtube_videous(
                    topic,
                    main_category=candidate_main_category,
                    sub_category=candidate_sub_category,
                    target_lang="de",
                )
                if isinstance(candidate_video_data, list) and candidate_video_data:
                    video_data = candidate_video_data
                    selected_video_topic = topic_row
                    break

            if not isinstance(video_data, list):
                print(f"❌ ОШИБКА: search_youtube_videous вернула {type(video_data)} вместо списка!")
                video_data = []
            if not video_data:
                print("❌ Видео не найдено. Список пуст.")
            else:
                print(f"✅ Найдено {len(video_data)} видео:")
                for video in video_data:
                    print(f"▶️ {video}")

            valid_links = video_data or ["❌ Не удалось найти видео на YouTube по этой теме. Попробуйте позже."]
            rounded_value = round(mistakes_week / total_sentences, 2)
            recommendations = (
                f"🧔 *{username}*,\nВы *перевели* за неделю: {total_sentences} предложений;\n"
                f"📌 *В них допущено* {mistakes_week} ошибок;\n"
                f"🚨 *Количество ошибок на одно предложение:* {rounded_value} штук;\n"
                f"🔴 *Больше всего ошибок:* {number_of_top_category_mistakes} штук в категории:\n {top_mistake_category or 'неизвестно'}\n"
            )
            if top_mistake_subcategory_1:
                recommendations += f"📜 *Основные ошибки в подкатегории:*\n {top_mistake_subcategory_1}\n\n"
            if top_mistake_subcategory_2:
                recommendations += f"📜 *Вторые по частоте ошибки в подкатегории:*\n {top_mistake_subcategory_2}\n\n"
            if selected_video_topic:
                selected_video_main = str(selected_video_topic.get("main_category") or "").strip()
                selected_video_sub = str(selected_video_topic.get("sub_category") or "").strip()
                if (
                    selected_video_main and
                    (
                        selected_video_main != str(top_mistake_category or "").strip()
                        or selected_video_sub != str(top_mistake_subcategory_1 or "").strip()
                    )
                ):
                    recommendations += f"🎯 *Видео подобрано по теме:*\n {selected_video_sub or selected_video_main}\n\n"

            recommendations += "🟢 *Рекомендую посмотреть:*\n\n"
            recommendations = escape_html_with_bold(recommendations)
            recommendations += "\n\n".join(valid_links)

            print("DEBUG:", recommendations)
            await _send_analytics_message_with_fallback(
                context,
                user_id=safe_user_id,
                target_chat_id=delivery_chat_id,
                text=recommendations,
            )
            await asyncio.sleep(5)
            continue

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT username FROM bt_3_translations WHERE user_id = %s;
                    """,
                    (safe_user_id,),
                )
                result = cursor.fetchone()
                username = result[0] if result else f"User {safe_user_id}"

        no_activity_text = escape_html_with_bold(
            f"⚠️ Пользователь {username} не перевёл ни одного предложения на этой неделе."
        )
        await _send_analytics_message_with_fallback(
            context,
            user_id=safe_user_id,
            target_chat_id=delivery_chat_id,
            text=no_activity_text,
        )

    if skipped_synthetic_users > 0:
        logging.info(
            "ℹ️ send_me_analytics_and_recommend_me skipped %s synthetic load-test user id(s)",
            skipped_synthetic_users,
        )


async def force_finalize_sessions(context: CallbackContext = None):
    """Legacy/manual helper for force-closing today's open sessions."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE bt_3_user_progress 
        SET end_time = NOW(), completed = TRUE
        WHERE completed = FALSE AND start_time::date = CURRENT_DATE;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    if context is None:
        return

    try:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=14, job_name="force_finalize_sessions")
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для force finalize notice", exc_info=True)
        targets = []

    for target_chat_id in targets:
        try:
            await context.bot.send_message(
                chat_id=int(target_chat_id),
                text="🔔 Все незавершённые сессии за сегодня автоматически закрыты!",
            )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить finalize notice в chat_id=%s: %s", target_chat_id, exc)



#SQL Запрос проверено
async def send_weekly_summary(context: CallbackContext):

    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔹 Собираем статистику за неделю
    cursor.execute(f"""
    SELECT 
        t.user_id,
        t.username,
        COUNT(DISTINCT t.sentence_id) AS всего_переводов,
        COALESCE(AVG(t.score), 0) AS средняя_оценка,
        COALESCE(p.avg_time, 0) AS среднее_время_сессии_в_минутах, -- ✅ Среднее время сессии
        COALESCE(p.total_time, 0) AS общее_время_в_минутах, -- ✅ Теперь есть и общее время
        (SELECT COUNT(*) 
        FROM bt_3_daily_sentences 
        WHERE date >= CURRENT_DATE - INTERVAL '6 days' 
        AND user_id = t.user_id) 
        - COUNT(DISTINCT t.sentence_id) AS пропущено_за_неделю,
        COALESCE(AVG(t.score), 0) 
            - (COALESCE(p.avg_time, 0) * 1) -- ✅ Среднее время в штрафе
            - ((SELECT COUNT(*) 
                FROM bt_3_daily_sentences 
                WHERE date >= CURRENT_DATE - INTERVAL '6 days' 
                AND user_id = t.user_id) 
            - COUNT(DISTINCT t.sentence_id)) * 20
            AS итоговый_балл
    FROM bt_3_translations t
    LEFT JOIN (
        SELECT user_id, 
            AVG({build_translation_session_minutes_sql('p')}) AS avg_time, -- ✅ Среднее время сессии
            SUM({build_translation_session_minutes_sql('p')}) AS total_time -- ✅ Общее время
        FROM bt_3_user_progress 
        WHERE completed = TRUE 
        AND start_time >= CURRENT_DATE - INTERVAL '6 days'
        GROUP BY user_id
    ) p ON t.user_id = p.user_id
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '6 days'
    GROUP BY t.user_id, t.username, p.avg_time, p.total_time
    ORDER BY итоговый_балл DESC;

    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    if not rows:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_weekly_summary_empty")
        for target_chat_id in targets:
            try:
                await context.bot.send_message(
                    chat_id=int(target_chat_id),
                    text="📊 Неделя прошла, но никто не перевел ни одного предложения!",
                )
            except Exception as exc:
                logging.warning("⚠️ Не удалось отправить weekly summary (empty) в chat_id=%s: %s", target_chat_id, exc)
        return

    user_ids = [int(row[0]) for row in rows]
    delivery_map = await _build_user_delivery_map(context, user_ids, job_name="send_weekly_summary")
    grouped_rows: dict[int, list[tuple]] = {}
    for row in rows:
        user_id = int(row[0])
        target_chat_id = int(delivery_map.get(user_id, user_id))
        grouped_rows.setdefault(target_chat_id, []).append(row)

    medals = ["🥇", "🥈", "🥉"]
    for target_chat_id, chat_rows in grouped_rows.items():
        sorted_rows = sorted(chat_rows, key=lambda item: float(item[7] or 0), reverse=True)
        summary = "🏆 Итоги недели:\n\n"
        for i, row in enumerate(sorted_rows):
            _, username, count, avg_score, avg_minutes, total_minutes, missed, final_score = row
            medal = medals[i] if i < len(medals) else "💩"
            summary += (
                f"{medal} {username}\n"
                f"📜 Переведено: {count}\n"
                f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
                f"⏱ Время среднее: {avg_minutes:.1f} мин\n"
                f"⏱ Время общее: {total_minutes:.1f} мин\n"
                f"🚨 Пропущено: {missed}\n"
                f"🏆 Итоговый балл: {final_score:.1f}\n\n"
            )
        try:
            await context.bot.send_message(chat_id=int(target_chat_id), text=summary)
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить weekly summary в chat_id=%s: %s", target_chat_id, exc)



async def user_stats(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    username = update.message.from_user.first_name

    today_bounds = get_period_bounds("day")
    week_bounds = get_period_bounds("week")
    month_bounds = get_period_bounds("month")
    quarter_bounds = get_period_bounds("quarter")
    all_time_start = date(1970, 1, 1)
    all_time_end = date.today()

    today_summary = fetch_user_summary(user_id, today_bounds.start_date, today_bounds.end_date)
    week_summary = fetch_user_summary(user_id, week_bounds.start_date, week_bounds.end_date)
    month_summary = fetch_user_summary(user_id, month_bounds.start_date, month_bounds.end_date)
    quarter_summary = fetch_user_summary(user_id, quarter_bounds.start_date, quarter_bounds.end_date)
    all_time_summary = fetch_user_summary(user_id, all_time_start, all_time_end)

    if today_summary and today_summary.get("total_translations", 0) > 0:
        today_text = (
            f"📅 Сегодняшняя статистика ({username})\n"
            f"🔹 Переведено: {today_summary['total_translations']}\n"
            f"🎯 Средняя оценка: {today_summary['avg_score']:.1f}/100\n"
            f"⏱ Среднее время сессии: {today_summary.get('avg_session_time_min', 0):.1f} мин\n"
            f"🚨 Пропущено: {today_summary['missed_sentences']}\n"
            f"🏆 Итоговый балл: {today_summary['final_score']:.1f}\n"
        )
    else:
        today_text = f"📅 **Сегодняшняя статистика ({username})**\n❌ Нет данных (вы ещё не переводили)."

    if week_summary and week_summary.get("total_translations", 0) > 0:
        weekly_text = (
            f"\n📆 Статистика за неделю\n"
            f"🔹 Переведено: {week_summary['total_translations']}\n"
            f"🎯 Средняя оценка: {week_summary['avg_score']:.1f}/100\n"
            f"⏱ Среднее время сессии: {week_summary.get('avg_session_time_min', 0):.1f} мин\n"
            f"⏱ Общее время за неделю: {week_summary['total_time_min']:.1f} мин\n"
            f"🚨 Пропущено за неделю: {week_summary['missed_sentences']}\n"
            f"🏆 Итоговый балл: {week_summary['final_score']:.1f}\n"
        )
    else:
        weekly_text = "\n📆 Статистика за неделю\n❌ Нет данных."

    if month_summary and month_summary.get("total_translations", 0) > 0:
        month_text = (
            f"\n🗓 Статистика за месяц\n"
            f"🔹 Переведено: {month_summary['total_translations']}\n"
            f"🎯 Средняя оценка: {month_summary['avg_score']:.1f}/100\n"
            f"⏱ Среднее время сессии: {month_summary.get('avg_session_time_min', 0):.1f} мин\n"
            f"⏱ Общее время за месяц: {month_summary['total_time_min']:.1f} мин\n"
            f"🚨 Пропущено за месяц: {month_summary['missed_sentences']}\n"
            f"🏆 Итоговый балл: {month_summary['final_score']:.1f}\n"
        )
    else:
        month_text = "\n🗓 Статистика за месяц\n❌ Нет данных."

    if quarter_summary and quarter_summary.get("total_translations", 0) > 0:
        quarter_text = (
            f"\n📊 Статистика за квартал\n"
            f"🔹 Переведено: {quarter_summary['total_translations']}\n"
            f"🎯 Средняя оценка: {quarter_summary['avg_score']:.1f}/100\n"
            f"⏱ Среднее время сессии: {quarter_summary.get('avg_session_time_min', 0):.1f} мин\n"
            f"⏱ Общее время за квартал: {quarter_summary['total_time_min']:.1f} мин\n"
            f"🚨 Пропущено за квартал: {quarter_summary['missed_sentences']}\n"
            f"🏆 Итоговый балл: {quarter_summary['final_score']:.1f}\n"
        )
    else:
        quarter_text = "\n📊 Статистика за квартал\n❌ Нет данных."

    if all_time_summary and all_time_summary.get("total_translations", 0) > 0:
        all_time_text = (
            f"\n🏁 Статистика за весь период\n"
            f"🔹 Переведено: {all_time_summary['total_translations']}\n"
            f"🎯 Средняя оценка: {all_time_summary['avg_score']:.1f}/100\n"
            f"⏱ Среднее время сессии: {all_time_summary.get('avg_session_time_min', 0):.1f} мин\n"
            f"⏱ Общее время: {all_time_summary['total_time_min']:.1f} мин\n"
            f"🚨 Пропущено: {all_time_summary['missed_sentences']}\n"
            f"🏆 Итоговый балл: {all_time_summary['final_score']:.1f}\n"
        )
    else:
        all_time_text = "\n🏁 Статистика за весь период\n❌ Нет данных."

    await update.message.reply_text(
        today_text + weekly_text + month_text + quarter_text + all_time_text
    )



async def send_daily_summary(context: CallbackContext):

    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔹 Собираем активных пользователей (кто перевёл хотя бы одно предложение)
    cursor.execute("""
        SELECT DISTINCT user_id, username 
        FROM bt_3_translations
        WHERE timestamp::date = CURRENT_DATE;
    """)
    active_users = {row[0]: row[1] for row in cursor.fetchall()}

    # 🔹 Собираем всех, кто хоть что-то писал в чат
    cursor.execute("""
        SELECT DISTINCT user_id, username
        FROM bt_3_messages
        WHERE timestamp >= date_trunc('month', CURRENT_DATE);
    """)
    all_users = {row[0]: row[1] for row in cursor.fetchall()}
    for user_id, username in all_users.items():
        print(f"User ID from rows: {user_id}, uswername: {username}")

    # 🔹 Собираем статистику за день
    cursor.execute(f"""
       SELECT 
            ds.user_id, 
            COUNT(DISTINCT ds.id) AS total_sentences,
            COUNT(DISTINCT t.id) AS translated,
            (COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) AS missed,
            COALESCE(p.avg_time, 0) AS avg_time_minutes, 
            COALESCE(p.total_time, 0) AS total_time_minutes, 
            COALESCE(AVG(t.score), 0) AS avg_score,
            COALESCE(AVG(t.score), 0) 
            - (COALESCE(p.avg_time, 0) * 1) 
            - ((COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) * 20) AS final_score
        FROM bt_3_daily_sentences ds
        LEFT JOIN bt_3_translations t ON ds.user_id = t.user_id AND ds.id = t.sentence_id
        LEFT JOIN (
            SELECT user_id, 
                AVG({build_translation_session_minutes_sql('p')}) AS avg_time, 
                SUM({build_translation_session_minutes_sql('p')}) AS total_time
            FROM bt_3_user_progress
            WHERE completed = true
        		AND start_time::date = CURRENT_DATE -- ✅ Теперь только за день
            GROUP BY user_id
        ) p ON ds.user_id = p.user_id
        WHERE ds.date = CURRENT_DATE
        GROUP BY ds.user_id, p.avg_time, p.total_time
        ORDER BY final_score DESC;
    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    candidate_user_ids: set[int] = {int(uid) for uid in all_users.keys()}
    candidate_user_ids.update({int(row[0]) for row in rows if row and row[0] is not None})
    if not candidate_user_ids:
        candidate_user_ids.update(await _collect_scheduler_candidate_user_ids(lookback_days=30))

    delivery_map = await _build_user_delivery_map(context, candidate_user_ids, job_name="send_daily_summary")
    chat_usernames: dict[int, dict[int, str]] = {}
    for user_id, username in all_users.items():
        target_chat_id = int(delivery_map.get(int(user_id), int(user_id)))
        chat_usernames.setdefault(target_chat_id, {})[int(user_id)] = username

    chat_rows: dict[int, list[tuple]] = {}
    for row in rows:
        user_id = int(row[0])
        target_chat_id = int(delivery_map.get(user_id, user_id))
        chat_rows.setdefault(target_chat_id, []).append(row)

    all_targets: list[int] = []
    seen_targets: set[int] = set()
    for chat_id in list(chat_usernames.keys()) + list(chat_rows.keys()):
        if chat_id in seen_targets:
            continue
        seen_targets.add(chat_id)
        all_targets.append(chat_id)

    if not all_targets:
        return

    medals = ["🥇", "🥈", "🥉"]
    for target_chat_id in all_targets:
        current_rows = sorted(
            chat_rows.get(target_chat_id, []),
            key=lambda item: float(item[7] or 0),
            reverse=True,
        )
        summary = "📊 Итоги дня:\n\n"
        if not current_rows:
            summary += "📊 Сегодня никто не перевёл ни одного предложения!\n"
        else:
            for i, (user_id, total_sentences, translated, missed, avg_minutes, total_time_minutes, avg_score, final_score) in enumerate(current_rows):
                username = all_users.get(int(user_id), 'Неизвестный пользователь')
                medal = medals[i] if i < len(medals) else "💩"
                summary += (
                    f"{medal} {username}\n"
                    f"📜 Всего предложений: {total_sentences}\n"
                    f"✅ Переведено: {translated}\n"
                    f"🚨 Не переведено: {missed}\n"
                    f"⏱ Время среднее: {avg_minutes:.1f} мин\n"
                    f"⏱ Время общее: {total_time_minutes:.1f} мин\n"
                    f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
                    f"🏆 Итоговый балл: {final_score:.1f}\n\n"
                )

        lazy_user_map = {
            uid: uname
            for uid, uname in chat_usernames.get(target_chat_id, {}).items()
            if uid not in active_users
        }
        if lazy_user_map:
            summary += "\n🦥 Ленивцы (писали в чат, но не переводили):\n"
            for username in lazy_user_map.values():
                summary += f"👤 {username}: ничего не перевёл!\n"

        try:
            await context.bot.send_message(chat_id=int(target_chat_id), text=summary)
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить daily summary в chat_id=%s: %s", target_chat_id, exc)



async def send_progress_report(context: CallbackContext):
    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔹 Получаем всех пользователей, которые писали в чат **за месяц**
    cursor.execute("""
        SELECT DISTINCT user_id, username 
        FROM bt_3_messages
        WHERE timestamp >= date_trunc('month', CURRENT_DATE);
    """)
    all_users = {int(row[0]): row[1] for row in cursor.fetchall()}

    # 🔹 Получаем всех, кто перевёл хотя бы одно предложение **за сегодня**
    cursor.execute("""
        SELECT DISTINCT user_id FROM bt_3_translations WHERE timestamp::date = CURRENT_DATE;
    """)
    active_users = {row[0] for row in cursor.fetchall()}

    # 🔹 Собираем статистику по пользователям **за сегодня**(checked)
    cursor.execute(f"""
        SELECT 
        ds.user_id,
        COUNT(DISTINCT ds.id) AS всего_предложений,
        COUNT(DISTINCT t.id) AS переведено,
        (COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) AS пропущено,
        COALESCE(p.avg_time, 0) AS среднее_время_сессии_в_минутах, -- ✅ Среднее время за день
        COALESCE(p.total_time, 0) AS общее_время_за_день, -- ✅ Общее время за день
        COALESCE(AVG(t.score), 0) AS средняя_оценка,
        COALESCE(AVG(t.score), 0) 
            - (COALESCE(p.avg_time, 0) * 1) -- ✅ Используем среднее время в расчётах
            - ((COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) * 20) AS итоговый_балл
    FROM bt_3_daily_sentences ds
    LEFT JOIN bt_3_translations t ON ds.user_id = t.user_id AND ds.id = t.sentence_id
    LEFT JOIN (
        SELECT user_id, 
            AVG({build_translation_session_minutes_sql('p')}) AS avg_time, -- ✅ Среднее время сессии за день
            SUM({build_translation_session_minutes_sql('p')}) AS total_time -- ✅ Общее время за день
        FROM bt_3_user_progress
        WHERE completed = TRUE 
            AND start_time::date = CURRENT_DATE -- ✅ Теперь только за день
        GROUP BY user_id
    ) p ON ds.user_id = p.user_id
    WHERE ds.date = CURRENT_DATE
    GROUP BY ds.user_id, p.avg_time, p.total_time
    ORDER BY итоговый_балл DESC;
    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    candidate_user_ids: set[int] = {int(uid) for uid in all_users.keys()}
    candidate_user_ids.update({int(row[0]) for row in rows if row and row[0] is not None})
    if not candidate_user_ids:
        candidate_user_ids.update(await _collect_scheduler_candidate_user_ids(lookback_days=30))

    delivery_map = await _build_user_delivery_map(context, candidate_user_ids, job_name="send_progress_report")
    chat_usernames: dict[int, dict[int, str]] = {}
    for user_id, username in all_users.items():
        target_chat_id = int(delivery_map.get(int(user_id), int(user_id)))
        chat_usernames.setdefault(target_chat_id, {})[int(user_id)] = username

    chat_rows: dict[int, list[tuple]] = {}
    for row in rows:
        user_id = int(row[0])
        target_chat_id = int(delivery_map.get(user_id, user_id))
        chat_rows.setdefault(target_chat_id, []).append(row)

    all_targets: list[int] = []
    seen_targets: set[int] = set()
    for chat_id in list(chat_usernames.keys()) + list(chat_rows.keys()):
        if chat_id in seen_targets:
            continue
        seen_targets.add(chat_id)
        all_targets.append(chat_id)

    if not all_targets:
        return

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for target_chat_id in all_targets:
        progress_report = f"📊 Промежуточные итоги перевода:\n🕒 Время отчёта:\n{current_time}\n\n"
        current_rows = sorted(
            chat_rows.get(target_chat_id, []),
            key=lambda item: float(item[7] or 0),
            reverse=True,
        )

        if not current_rows:
            progress_report += "📊 Сегодня никто не перевёл ни одного предложения!\n"
        else:
            for user_id, total, translated, missed, avg_minutes, total_minutes, avg_score, final_score in current_rows:
                progress_report += (
                    f"👤 {all_users.get(int(user_id), 'Неизвестный пользователь')}\n"
                    f"📜 Переведено: {translated}/{total}\n"
                    f"🚨 Не переведено: {missed}\n"
                    f"⏱ Время среднее: {avg_minutes:.1f} мин\n"
                    f"⏱ Время общ.: {total_minutes:.1f} мин\n"
                    f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
                    f"🏆 Итоговый балл: {final_score:.1f}\n\n"
                )

        lazy_user_map = {
            uid: uname
            for uid, uname in chat_usernames.get(target_chat_id, {}).items()
            if uid not in active_users
        }
        if lazy_user_map:
            progress_report += "\n🦥 Ленивцы (писали в чат, но не переводили):\n"
            for username in lazy_user_map.values():
                progress_report += f"👤 {username}: ничего не перевёл!\n"

        try:
            await context.bot.send_message(chat_id=int(target_chat_id), text=progress_report)
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить progress report в chat_id=%s: %s", target_chat_id, exc)


async def error_handler(update, context):
    logging.error(f"❌ Ошибка в обработчике Telegram: {context.error}")


# Глобальная переменная
GOOGLE_CREDS_FILE_PATH = None

# ✅ # ✅ Загружаем переменные окружения из .env-файла (только при локальной разработке)
# Это загрузит все переменные из file with name .env which was created by me в os.environ

def prepare_google_creds_file():
    global GOOGLE_CREDS_FILE_PATH
    global success
    print("✅ .env loaded?", success)
    print("🧪 Функция prepare_google_creds_file вызвана")

    # ✅ 1. Попробовать использовать путь к локальному .json-файлу
    direct_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"📢 direct_path (print): {direct_path}")
    logging.info(f"direct_path: {direct_path}")

    if direct_path:
        print("🌐 Переменная найдена:", direct_path)
        print("🧱 Существует ли файл?", Path(direct_path).exists())
        GOOGLE_CREDS_FILE_PATH = direct_path
        return GOOGLE_CREDS_FILE_PATH
    
    # ✅ 2. Попробовать использовать GOOGLE_CREDS_JSON (из Railway)
    if GOOGLE_CREDS_FILE_PATH and Path(GOOGLE_CREDS_FILE_PATH).exists():
        return GOOGLE_CREDS_FILE_PATH
    
    raw_creds = os.getenv("GOOGLE_CREDS_JSON")
    if not raw_creds:
        raise RuntimeError("❌ Не найдены переменные GOOGLE_APPLICATION_CREDENTIALS или GOOGLE_CREDS_JSON.")

    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as temp_key_file:
        temp_key_file.write(raw_creds)
        temp_key_file.flush()
        # Когда создаё временный файл через tempfile.NamedTemporaryFile, Python возвращает объект этого файла. 
        # У него есть атрибут .name, который содержит полный путь к этому файлу в файловой системе
        GOOGLE_CREDS_FILE_PATH = temp_key_file.name
        print(f"🧪 Сгенерирован временный ключ: {GOOGLE_CREDS_FILE_PATH}")

    return GOOGLE_CREDS_FILE_PATH



async def mistakes_to_voice(username, sentence_pairs):
    german_voice = "de-DE-Neural2-C"

    audio_segments = []

    async def synthesize_cached(lang: str, text: str) -> AudioSegment:
        return await asyncio.to_thread(get_or_create_tts_clip, lang, text, 0.9)

    async def split_german_sentence(sentence: str) -> list[str]:
        if not sentence:
            return []
        chunks = await asyncio.to_thread(chunk_sentence_llm_de, sentence)
        return [str(chunk).strip() for chunk in (chunks or []) if str(chunk).strip()] or [sentence]

    pause_short = AudioSegment.silent(duration=500)
    pause_long = AudioSegment.silent(duration=900)
    pause_between_sentences = AudioSegment.silent(duration=1700)

    for russian, german in sentence_pairs:
        print(f"🎤 Синтезируем: {russian} -> {german}")
        # Русский (один раз)
        ru_audio = await synthesize_cached("ru", russian)
        # Немецкий: строим по кускам
        chunks = await split_german_sentence(german)
        chunk_segments = []
        for idx, chunk in enumerate(chunks):
            # повторяем кусок
            chunk_audio = await synthesize_cached("de", chunk)
            chunk_segments.extend([chunk_audio, pause_short, chunk_audio, pause_short])

            if idx > 0:
                combined_text = " ".join(chunks[: idx + 1])
                combined_audio = await synthesize_cached("de", combined_text)
                chunk_segments.extend([combined_audio, pause_long])

        # финальная фраза полностью
        full_audio = await synthesize_cached("de", german)

        # Объединяем
        combined = ru_audio + pause_long + sum(chunk_segments, AudioSegment.silent(duration=0)) + full_audio + pause_between_sentences
        audio_segments.append(combined)

    final_audio = sum(audio_segments)

    output_path = f"{username}.mp3"

    final_audio.export(output_path, format="mp3")
    print(f"🔊 Сохранён итоговый файл: {output_path}")


async def get_yesterdays_mistakes_for_audio_message(context: CallbackContext):
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:

            # take all users who made at least one mistake from bt_3_detailed_mistakes table
            cursor.execute("""
                SELECT DISTINCT user_id FROM bt_3_detailed_mistakes
                WHERE added_data >= NOW() - INTERVAL '6 days';
            """)
            user_ids = [i[0] for i in cursor.fetchall() if i[0] is not None]
            print(user_ids)
            for user_id in user_ids:
                safe_user_id = int(user_id)
                original_by_id = {}

                cursor.execute("""
                SELECT username FROM bt_3_user_progress
                WHERE user_id = %s;
                """, (user_id,))
                row = cursor.fetchone()
                username = row[0] if row and row[0] else f"useer_{user_id}"

                ## Шаг 1 — Собираем оригинальные предложения по user_id
                # ✅ Загружаем все предложения из базы ошибок
                cursor.execute("""
                    SELECT sentence, correct_translation
                    FROM bt_3_detailed_mistakes
                    WHERE user_id = %s
                    ORDER BY mistake_count DESC, last_seen ASC; 
                """, (user_id, ))
                
                # ✅ Используем set() для удаления дубликатов по sentence_id
                already_given_sentence_translation = set()
                unique_sentences = set()
                mistake_sentences = []
                result_for_audio = []
                
                rows = cursor.fetchall()
                max_to_collect = min(len(rows), 5)

                for sentence, correct_translation in rows:
                    if sentence and correct_translation and correct_translation not in already_given_sentence_translation and sentence not in mistake_sentences:
                        if correct_translation not in unique_sentences:
                            unique_sentences.add(correct_translation)
                            mistake_sentences.append(sentence)
                            already_given_sentence_translation.add(correct_translation)
                            original_by_id[correct_translation] = sentence

                            # ✅ Ограничиваем до нужного количества предложений (например, 5)
                            
                            if len(mistake_sentences) == max_to_collect:
                                break

                sentence_pairs = [(origin_sentence, correct_transl) for correct_transl, origin_sentence in original_by_id.items()]
                try:
                    await mistakes_to_voice(username, sentence_pairs)
                except Exception as e:
                    print(f"❌ Ошибка синтеза речи для {username}: {e}")
                    continue
                audio_path = Path(f"{username}.mp3")
                print(f"📦 Размер файла: {audio_path.stat().st_size / 1024 / 1024:.2f} MB ")

                target_chat_id = await _resolve_user_delivery_chat_id(
                    context,
                    safe_user_id,
                    job_name="get_yesterdays_mistakes_for_audio_message",
                )

                if audio_path.exists():
                    try:
                        start = asyncio.get_running_loop().time()
                        with audio_path.open("rb") as audio_file:
                            await context.bot.send_audio(
                                chat_id=int(target_chat_id),
                                audio=audio_file,
                                caption=f"🎧 Ошибки пользователя @{username} за вчерашний день."
                            )
                        print(f"⏱ Отправка заняла {asyncio.get_running_loop().time() - start:.2f} секунд")
                        await asyncio.sleep(5)
                    except Exception as e:
                        print(f"❌ Ошибка при отправке аудиофайла для @{username}: {e}")
                        if int(target_chat_id) != safe_user_id:
                            try:
                                with audio_path.open("rb") as audio_file:
                                    await context.bot.send_audio(
                                        chat_id=safe_user_id,
                                        audio=audio_file,
                                        caption=f"🎧 Ошибки пользователя @{username} за вчерашний день."
                                    )
                            except Exception as fallback_error:
                                print(f"❌ Ошибка fallback отправки аудио в личку @{username}: {fallback_error}")

                    try:    
                        audio_path.unlink()
                    except FileNotFoundError:
                        print(f"⚠️ Файл уже был удалён: {audio_path}")
                
                else:
                    await _send_analytics_message_with_fallback(
                        context,
                        user_id=safe_user_id,
                        target_chat_id=int(target_chat_id),
                        text=escape_html_with_bold(f"❌ Для пользователя @{username} не найден аудиофайл."),
                    )
                    await asyncio.sleep(5)


# import atexit

# def cleanup_creds_file():
#     global GOOGLE_CREDS_FILE_PATH
#     if GOOGLE_CREDS_FILE_PATH and os.path.exists(GOOGLE_CREDS_FILE_PATH):
#         os.remove(GOOGLE_CREDS_FILE_PATH)
#         print(f"🧹 Удалён временный ключ: {GOOGLE_CREDS_FILE_PATH}")

# atexit.register(cleanup_creds_file)


# --- Функции LiveKit Room, перенесенные сюда ---
# Они были в agent.py, но логичнее управлять созданием ссылок из Telegram-бота.
# Назначение: Безопасно создать уникальную комнату на сервере LiveKit и 
# сгенерировать персональный токен доступа для конкретного пользователя.
# async def create_livekit_room(user_id, username, is_group=False):
#     """Создаёт комнату LiveKit и возвращает ссылку."""
#     try:
#         # Использование async with для корректного управления сессией LiveKitAPI
#         # async with livekit.api.LiveKitAPI(...) as livekit_api:: Создается клиент для общения с API LiveKit. 
#         # async with гарантирует, что соединение с API будет корректно закрыто.
#         async with livekit.api.LiveKitAPI(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, LIVEKIT_URL) as livekit_api:
#             room_name = f"{'group-' if is_group else ''}sales-mentor-{user_id}-{int(datetime.now().timestamp())}"
            
#             # Генерация токена с использованием цепочки методов .with_
#             # .with_identity(str(user_id)): В токен "зашивается" идентификатор пользователя. Именно это значение потом получит ваш агент в on_user_joined как participant.identity. Так агент понимает, КТО именно подключился.
#             # .with_name(username): Задается отображаемое имя пользователя.
#             # .with_grants(...): Выдаются права (permissions) пользователю внутри комнаты. room_join: True — разрешает войти, can_publish: True — разрешает транслировать свое аудио/видео.
#             # .to_jwt(): Все эти данные подписываются вашим секретным ключом и превращаются в длинную, безопасную строку — JSON Web Token (JWT).
#             token_participant = livekit.api.AccessToken(
#                 api_key=LIVEKIT_API_KEY,
#                 api_secret=LIVEKIT_API_SECRET
#             ).with_identity(str(user_id)) \
#              .with_name(username) \
#              .with_grants(livekit.api.VideoGrants( # VideoGrants теперь без identity и name
#                  room=room_name, # room_name должен быть здесь, как и в оригинале
#                  room_join=True,
#                  can_publish=True,
#                  can_subscribe=True
#              )).to_jwt()
#             # Формируется финальная ссылка. Имя комнаты и уникальный токен передаются как параметры в URL. 
#             # JavaScript на странице client.html прочитает их из адреса и использует для подключения к звонку.
#             client_url = f"{CLIENT_HOST}/client.html?room_name={room_name}&token={token_participant}"
#             logging.info(f"✅ Создана ссылка LiveKit: {client_url}")
#             return client_url, room_name
#     except Exception as e:
#         logging.error(f"❌ Ошибка при создании ссылки LiveKit комнаты: {e}", exc_info=True)
#         return None, None

# async def start_lesson(update: Update, context: CallbackContext):
#     """Обработчик кнопки 'Начать урок'."""
#     user = update.message.from_user
#     user_id = user.id
#     username = user.username or user.first_name
#     # Используем новое имя функции
#     client_url, room_name = await create_livekit_room(user_id, username, is_group=False) 
#     link_text = "Join your *personal room*"

#     if client_url:
#         formatted_message = (
#             f"You Room for conversation is ready\n"
#             f'<a href="{html.escape(client_url)}">{escape_html_with_bold(link_text)}</a>'
#         )
#         msg = await context.bot.send_message(
#             chat_id = update.message.chat_id,
#             text=formatted_message,
#             parse_mode="HTML",
#             reply_to_message_id=update.message.message_id
#         )
#         add_service_msg_id(context, msg.message_id)
#         logging.info(f"📩 Отправлена ссылка LiveKit пользователю {user_id}")
#     else:
#         msg = await update.message.reply_text("❌ Помилка створення кімнати. Спробуйте пізніше.")
#         add_service_msg_id(context, msg.message_id)

# async def group_call(update: Update, context: CallbackContext):
#     """Обработчик кнопки 'Групповой звонок'."""
#     user = update.message.from_user
#     user_id = user.id
#     username = user.username or user.first_name
#     # Используем новое имя функции
#     client_url, room_name = await create_livekit_room(user_id, username, is_group=True)
#     link_text = "Join your *group room*"
    
#     if client_url:
#         formatted_message = (
#             f"The Group Room is reasdy\n"
#             f'<a href="{html.escape(client_url)}">{escape_html_with_bold(link_text)}</a>'
#         )
#         msg = await context.bot.send_message(
#             chat_id=update.message.chat_id,
#             text = formatted_message,
#             parse_mode="HTML",
#             reply_to_message_id=update.message.message_id
#         )
#         add_service_msg_id(context, msg.message_id)
#         logging.info(f"📩 Отправлена групповая ссылка LiveKit пользователю {user_id}")
#     else:
#         msg = await update.message.reply_text("❌ Помилка створення кімнати. Спробуйте пізніше.")
#         add_service_msg_id(context, msg.message_id)


def get_date_range(period: str) -> tuple[date, date]:
    end_date = date.today()
    start_date = end_date

    if period == "day":
        start_date = end_date
    elif period == "week":
        start_date = end_date - timedelta(days=end_date.weekday())
        end_date = start_date + timedelta(days=6)
    elif period == "month":
        start_date = end_date.replace(day=1)
        last_day = calendar.monthrange(end_date.year, end_date.month)[1]
        end_date = end_date.replace(day=last_day)
    elif period == "quarter":
        quarter_index = (end_date.month - 1) // 3
        start_month = quarter_index * 3 + 1
        end_month = start_month + 2
        start_date = date(end_date.year, start_month, 1)
        last_day = calendar.monthrange(end_date.year, end_month)[1]
        end_date = date(end_date.year, end_month, last_day)
    elif period == "half_year":
        if end_date.month <= 6:
            start_date = date(end_date.year, 1, 1)
            end_date = date(end_date.year, 6, 30)
        else:
            start_date = date(end_date.year, 7, 1)
            end_date = date(end_date.year, 12, 31)
    elif period == "year":
        start_date = date(end_date.year, 1, 1)
        end_date = date(end_date.year, 12, 31)

    return start_date, end_date


async def send_user_analytics_bar_charts(context: CallbackContext, period="day"): # 'update' parameter removed
    start_date, end_date = get_date_range(period)
    targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_user_analytics_bar_charts")
    if not targets:
        logging.info("ℹ️ user analytics bar charts: нет targets для рассылки")
        return

    # Send one message before starting the process
    for target_chat_id in targets:
        try:
            await context.bot.send_message(chat_id=int(target_chat_id), text="🚀 Starting to prepare analytical reports for all active users...")
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить старт user analytics в chat_id=%s: %s", target_chat_id, exc)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as curr: # FIXED: added ()
                # RECOMMENDATION: Get users who have actually translated something
                curr.execute("""
                    SELECT DISTINCT user_id, username
                    FROM bt_3_translations;
                """)
                all_users = curr.fetchall()
        
        if not all_users:
            for target_chat_id in targets:
                try:
                    await context.bot.send_message(chat_id=int(target_chat_id), text="No active users found for analysis today.")
                except Exception as exc:
                    logging.warning("⚠️ Не удалось отправить empty user analytics в chat_id=%s: %s", target_chat_id, exc)
            return

        delivery_map = await _build_user_delivery_map(
            context,
            [int(user_id) for user_id, _ in all_users],
            job_name="send_user_analytics_bar_charts",
        )

        for user_id, username in all_users:
            target_chat_id = int(delivery_map.get(int(user_id), int(user_id)))
            try:
                # IMPORTANT: Make sure this function accepts user_id and uses it
                full_user_data = await prepare_aggregate_data_by_period_and_draw_analytic_for_user(user_id, start_date, end_date)

                if not full_user_data.empty:
                    daily_data = await aggregate_data_for_charts(full_user_data, period="day")
                    weekly_data = await aggregate_data_for_charts(full_user_data, period="week")

                    print(f"Data for {username} prepared. Drawing plots...")
                    image_path = await create_analytics_figure_async(daily_data, weekly_data, user_id)
                    
                    # FIXED: send_photo method name
                    with open(image_path, "rb") as photo_file:
                        await context.bot.send_photo(
                            chat_id=target_chat_id,
                            photo=photo_file,
                            caption=f"📊 Analytics for user: {username}",
                        )
                    os.remove(image_path)
                else:
                    print(f"⚠️ No data found for analysis for user {username} ({user_id}).")

            except Exception as e:
                logging.error(f"Error creating individual report for {username} ({user_id}): {e}")
                # Report the error, but continue the loop for other users
                await _send_analytics_message_with_fallback(
                    context,
                    user_id=int(user_id),
                    target_chat_id=target_chat_id,
                    text=escape_html_with_bold(f"❌ Failed to create a report for {username}."),
                )

    except Exception as e:
        logging.error(f"Critical error during send_user_analytics_bar_charts execution: {e}")
        for target_chat_id in targets:
            try:
                await context.bot.send_message(chat_id=int(target_chat_id), text="❌ A general error occurred while creating reports.")
            except Exception as exc:
                logging.warning("⚠️ Не удалось отправить critical user analytics в chat_id=%s: %s", target_chat_id, exc)


async def send_users_comparison_bar_chart(context: CallbackContext, period):
    start_date, end_date = get_date_range(period)
    targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_users_comparison_bar_chart")
    if not targets:
        logging.info("ℹ️ users comparison analytics: нет targets для рассылки")
        return
    # relativedelta(months=3) создаёт объект, который представляет собой интервал в "3 календарных месяца".
    # Когда вы вычитаете этот объект из даты, он корректно отсчитывает месяцы назад.

    for target_chat_id in targets:
        try:
            await context.bot.send_message(chat_id=int(target_chat_id), text="Starting preparation of Comparison analytics for all users..." )
        except Exception as exc:
            logging.warning("⚠️ Не удалось отправить старт comparison analytics в chat_id=%s: %s", target_chat_id, exc)

    try:
        image_path = await create_comparison_report_async(period=period, start_date=start_date, end_date=end_date)
        if image_path:
            for target_chat_id in targets:
                try:
                    with open(image_path, "rb") as photo_file:
                        await context.bot.send_photo(
                            chat_id=int(target_chat_id),
                            photo=photo_file,
                            caption=f"Users Comparison Analytics for the last {period}",
                        )
                except Exception as exc:
                    logging.warning("⚠️ Не удалось отправить comparison analytics в chat_id=%s: %s", target_chat_id, exc)
            os.remove(image_path)
        else:
            print(f"⚠️ No path found for comparison analysis for users.")
    
    except Exception as e:
        logging.error(f"Critical error during users_comparison analytics execution: {e}")


def _coerce_response_json(response_json: object) -> dict:
    if isinstance(response_json, dict):
        return response_json
    if isinstance(response_json, str):
        try:
            return json.loads(response_json)
        except json.JSONDecodeError:
            return {}
    return {}


def _build_quiz_fallback(word_ru: str, translation_de: str | None, article: str | None) -> dict:
    if article and translation_de:
        correct_option = f"{article} {translation_de}"
        options = [
            correct_option,
            *(f"{other} {translation_de}" for other in ["der", "die", "das"] if other != article),
        ]
        if len(options) < 4:
            options.append(translation_de)
        question = f"Какой артикль и перевод правильны для «{word_ru}»?"
        correct_option_id = options.index(correct_option)
    elif translation_de:
        options = [
            translation_de,
            f"der {translation_de}",
            f"die {translation_de}",
            f"das {translation_de}",
        ]
        question = f"Как переводится слово «{word_ru}» на немецкий?"
        correct_option_id = 0
    else:
        options = [
            word_ru,
            f"{word_ru} (DE)",
            f"die {word_ru}",
            f"das {word_ru}",
        ]
        question = f"Выберите правильный немецкий вариант для «{word_ru}»."
        correct_option_id = 0

    return {
        "question": question,
        "options": options[:4],
        "correct_option_id": correct_option_id,
        "quiz_type": "fallback",
    }


def _normalize_quiz_payload(payload: dict, fallback: dict) -> dict:
    question = payload.get("question")
    options = payload.get("options")
    correct_option_id = payload.get("correct_option_id")

    if not isinstance(question, str) or not isinstance(options, list):
        return fallback

    correct_text = None
    if isinstance(correct_option_id, int) and 0 <= correct_option_id < len(options):
        correct_text = str(options[correct_option_id]).strip()

    cleaned_options = []
    seen = set()
    for option in options:
        text = str(option).strip()
        if text and text not in seen:
            cleaned_options.append(text)
            seen.add(text)

    if len(cleaned_options) < 2:
        return fallback

    if not correct_text:
        return fallback
    if correct_text not in cleaned_options:
        return fallback
    correct_option_id = cleaned_options.index(correct_text)

    if len(cleaned_options) > 10:
        cleaned_options = cleaned_options[:10]
        if correct_text not in cleaned_options:
            return fallback
        correct_option_id = cleaned_options.index(correct_text)

    if any(_contains_cyrillic_text(option) or not _contains_latin_text(option) for option in cleaned_options):
        return fallback

    if len(cleaned_options) < 4:
        for option in fallback["options"]:
            if option not in cleaned_options:
                cleaned_options.append(option)
            if len(cleaned_options) >= 4:
                break

    if correct_option_id is None:
        return fallback

    return {
        "question": question.strip(),
        "options": cleaned_options,
        "correct_option_id": correct_option_id,
        "quiz_type": payload.get("quiz_type", "generated"),
    }


def _normalize_quiz_text(value: str) -> str:
    lowered = value.lower()
    cleaned = re.sub(r"[^a-zäöüßà-ÿ0-9\s'\-]", " ", lowered)
    cleaned = cleaned.replace("-", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


_GERMAN_ARTICLE_TOKENS = {
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
}


def _quiz_text_matches(user_text: str, correct_text: str) -> bool:
    user_norm = _normalize_quiz_text(user_text or "")
    correct_norm = _normalize_quiz_text(correct_text or "")
    if not user_norm or not correct_norm:
        return False
    if user_norm == correct_norm:
        return True

    user_tokens = user_norm.split()
    correct_tokens = correct_norm.split()
    if not user_tokens or not correct_tokens:
        return False

    user_article = user_tokens[0] if user_tokens[0] in _GERMAN_ARTICLE_TOKENS else ""
    correct_article = correct_tokens[0] if correct_tokens[0] in _GERMAN_ARTICLE_TOKENS else ""
    allow_article_strip = not (user_article and correct_article and user_article != correct_article)

    def _build_variants(tokens: list[str], strip_article: bool) -> set[str]:
        variants: set[str] = {" ".join(tokens)}
        if strip_article and tokens and tokens[0] in _GERMAN_ARTICLE_TOKENS:
            no_article = tokens[1:]
            if no_article:
                variants.add(" ".join(no_article))
        return {item for item in variants if item}

    user_variants = _build_variants(user_tokens, strip_article=allow_article_strip)
    correct_variants = _build_variants(correct_tokens, strip_article=allow_article_strip)
    return bool(user_variants & correct_variants)


async def _quiz_text_matches_semantic(user_text: str, correct_text: str) -> bool:
    canonical = _normalize_quiz_option_for_private_message(correct_text or "")
    guess = _normalize_quiz_option_for_private_message(user_text or "")
    if not canonical or not guess:
        return False
    # Keep LLM fallback bounded to short lexical answers.
    if len(canonical.split()) > 5 or len(guess.split()) > 5:
        return False
    if len(canonical) > 80 or len(guess) > 80:
        return False

    try:
        result = await asyncio.wait_for(
            run_check_story_guess_semantic(
                canonical_answer=canonical,
                aliases=[],
                user_guess=guess,
            ),
            timeout=8.0,
        )
    except Exception as exc:
        logging.warning("⚠️ Semantic check failed for quiz freeform: %s", exc)
        return False

    return bool((result or {}).get("is_correct"))


def _normalize_quiz_option_for_private_message(option: str) -> str:
    text = (option or "").strip()
    if not text:
        return ""
    text = text.replace("[", "").replace("]", "")

    # Collapse sequences like "B e i s p i e l" -> "Beispiel" without touching normal words.
    def _collapse(match: re.Match) -> str:
        return match.group(0).replace(" ", "")

    text = re.sub(r"\b(?:[A-Za-zÄÖÜäöüß]\s+){2,}[A-Za-zÄÖÜäöüß]\b", _collapse, text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


async def _translate_quiz_text_to_ru(de_text: str, fallback_ru: str | None = None) -> str:
    normalized = (de_text or "").strip()
    if not normalized:
        return (fallback_ru or "—").strip()

    cached = quiz_ru_translation_cache.get(normalized)
    if cached:
        return cached

    translated = ""
    try:
        items = await asyncio.wait_for(run_translate_subtitles_ru([normalized]), timeout=8)
        if items and isinstance(items, list):
            translated = (items[0] or "").strip()
    except asyncio.TimeoutError:
        logging.warning("⚠️ Таймаут перевода квиз-текста на русский, используем fallback.")
    except Exception as exc:
        logging.warning(f"⚠️ Не удалось перевести квиз-текст на русский: {exc}")

    if not translated:
        translated = (fallback_ru or "").strip()
    if translated:
        quiz_ru_translation_cache[normalized] = translated
    return translated or "—"


def _is_quiz_question_payload_expired(payload: dict | None) -> bool:
    started_at = float((payload or {}).get("started_at") or 0.0)
    if started_at <= 0:
        return True
    return (pytime.time() - started_at) > QUIZ_QUESTION_TTL_SECONDS


def _truncate_telegram_reply_text(text: str, max_chars: int = QUIZ_QUESTION_REPLY_MAX_CHARS) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= max_chars:
        return normalized
    shortened = normalized[: max_chars - 3].rstrip()
    split_idx = max(shortened.rfind("\n"), shortened.rfind(". "), shortened.rfind("! "), shortened.rfind("? "))
    if split_idx >= max_chars // 2:
        shortened = shortened[:split_idx].rstrip()
    return shortened.rstrip() + "..."


def _text_matches_language_side(text: str, lang: str) -> bool:
    normalized = str(text or "").strip()
    code = str(lang or "").strip().lower()
    if not normalized or not code:
        return False
    if code == "de":
        return bool(re.search(r"[A-Za-zÄÖÜäöüß]", normalized))
    if code == "ru":
        return bool(re.search(r"[А-Яа-яЁё]", normalized))
    return True


def _store_pending_quiz_question_request(
    *,
    user_id: int,
    source_text: str,
    target_text: str,
    source_lang: str,
    target_lang: str,
) -> str:
    key = hashlib.sha1(
        f"quizq:{user_id}:{source_lang}:{target_lang}:{source_text}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    pending_quiz_question_requests[key] = {
        "user_id": int(user_id),
        "source_text": str(source_text or "").strip(),
        "target_text": str(target_text or "").strip(),
        "source_lang": str(source_lang or "").strip().lower(),
        "target_lang": str(target_lang or "").strip().lower(),
        "started_at": pytime.time(),
    }
    if len(pending_quiz_question_requests) > 500:
        oldest_key = next(iter(pending_quiz_question_requests))
        pending_quiz_question_requests.pop(oldest_key, None)
    return key


def _store_pending_quiz_phrase_request(
    *,
    user_id: int,
    source_text: str,
    target_text: str,
    source_lang: str,
    target_lang: str,
) -> str:
    key = hashlib.sha1(
        f"quizphrase:{user_id}:{source_lang}:{target_lang}:{source_text}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    pending_quiz_phrase_requests[key] = {
        "user_id": int(user_id),
        "source_text": str(source_text or "").strip(),
        "target_text": str(target_text or "").strip(),
        "source_lang": str(source_lang or "").strip().lower(),
        "target_lang": str(target_lang or "").strip().lower(),
        "started_at": pytime.time(),
    }
    if len(pending_quiz_phrase_requests) > 500:
        oldest_key = next(iter(pending_quiz_phrase_requests))
        pending_quiz_phrase_requests.pop(oldest_key, None)
    return key


def _store_pending_quiz_question_save_request(
    *,
    user_id: int,
    request_key: str,
    source_text: str,
    target_text: str,
    source_lang: str,
    target_lang: str,
    continue_callback_data: str | None = None,
    continue_button_text: str | None = None,
    hide_continue_after_save: bool = False,
) -> str:
    save_key = hashlib.sha1(
        f"quizqsave:{user_id}:{request_key}:{source_text}:{datetime.utcnow().isoformat()}".encode("utf-8")
    ).hexdigest()[:20]
    resolved_continue_callback = str(continue_callback_data or "").strip() or (
        f"quizask:{str(request_key or '').strip()}"
        if str(request_key or "").strip()
        else "langgpt:continue"
    )
    resolved_continue_button_text = str(continue_button_text or "").strip() or "❓ Ещё вопрос"
    pending_quiz_question_save_requests[save_key] = {
        "user_id": int(user_id),
        "request_key": str(request_key or "").strip(),
        "source_text": str(source_text or "").strip(),
        "target_text": str(target_text or "").strip(),
        "source_lang": str(source_lang or "").strip().lower(),
        "target_lang": str(target_lang or "").strip().lower(),
        "continue_callback_data": resolved_continue_callback,
        "continue_button_text": resolved_continue_button_text,
        "hide_continue_after_save": bool(hide_continue_after_save),
        "started_at": pytime.time(),
        "saved": False,
    }
    if len(pending_quiz_question_save_requests) > 500:
        oldest_key = next(iter(pending_quiz_question_save_requests))
        pending_quiz_question_save_requests.pop(oldest_key, None)
    return save_key


def _build_followup_answer_keyboard(
    *,
    continue_callback_data: str,
    continue_button_text: str,
    save_key: str | None = None,
) -> InlineKeyboardMarkup:
    rows = []
    if save_key:
        rows.append([InlineKeyboardButton("💾 Сохранить эту фразу", callback_data=f"quizqsave:{save_key}")])
    rows.append([InlineKeyboardButton(str(continue_button_text or "❓ Ещё вопрос"), callback_data=str(continue_callback_data or "langgpt:continue"))])
    return InlineKeyboardMarkup(rows)


def _build_save_only_keyboard(*, save_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 Сохранить это сочетание", callback_data=f"quizqsave:{save_key}")]
    ])


def _build_quiz_result_keyboard(
    *,
    feel_key: str | None = None,
    question_key: str | None = None,
    speak_key: str | None = None,
    phrase_key: str | None = None,
) -> InlineKeyboardMarkup | None:
    rows = []
    if speak_key:
        rows.append([InlineKeyboardButton("🔊 Прослушать", callback_data=f"quizspeak:{speak_key}")])
    if feel_key:
        rows.append([InlineKeyboardButton("📌 Почувствовать слово", callback_data=f"quizfeel:{feel_key}")])
    if phrase_key:
        rows.append([InlineKeyboardButton("🧩 Дать сочетание", callback_data=f"quizphrase:{phrase_key}")])
    if question_key:
        rows.append([InlineKeyboardButton("❓ Задать свой вопрос", callback_data=f"quizask:{question_key}")])
    if not rows:
        return None
    return InlineKeyboardMarkup(rows)


def _build_quiz_question_answer_keyboard(*, request_key: str, save_key: str | None = None) -> InlineKeyboardMarkup:
    return _build_followup_answer_keyboard(
        continue_callback_data=f"quizask:{request_key}",
        continue_button_text="❓ Ещё вопрос",
        save_key=save_key,
    )


def _build_language_tutor_answer_keyboard(*, save_key: str | None = None) -> InlineKeyboardMarkup:
    return _build_followup_answer_keyboard(
        continue_callback_data="langgpt:continue",
        continue_button_text="❓ Задать вопрос",
        save_key=save_key,
    )


def _build_quiz_question_prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отмена", callback_data="quizaskcancel")]
    ])


def _normalize_quiz_question_llm_response(
    raw_payload: dict,
    *,
    source_lang: str,
    target_lang: str,
) -> dict:
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    reply_text = _truncate_telegram_reply_text(str(payload.get("reply_text") or "").strip())
    save_source_text = str(payload.get("save_source_text") or "").strip()
    save_target_text = str(payload.get("save_target_text") or "").strip()
    if not reply_text:
        reply_text = "Не удалось подготовить ответ. Попробуйте переформулировать вопрос чуть короче."
    if len(save_source_text) > 180:
        save_source_text = save_source_text[:177].rstrip() + "..."
    if len(save_target_text) > 220:
        save_target_text = save_target_text[:217].rstrip() + "..."
    if save_source_text and not _text_matches_language_side(save_source_text, source_lang):
        save_source_text = ""
        save_target_text = ""
    if save_target_text and not _text_matches_language_side(save_target_text, target_lang):
        save_source_text = ""
        save_target_text = ""
    if not save_source_text or not save_target_text:
        save_source_text = ""
        save_target_text = ""
    return {
        "reply_text": reply_text,
        "save_source_text": save_source_text,
        "save_target_text": save_target_text,
        "source_lang": str(source_lang or "").strip().lower(),
        "target_lang": str(target_lang or "").strip().lower(),
    }


def _build_quiz_question_reply_message(normalized: dict) -> str:
    reply_text = str((normalized or {}).get("reply_text") or "").strip()
    save_source_text = str((normalized or {}).get("save_source_text") or "").strip()
    save_target_text = str((normalized or {}).get("save_target_text") or "").strip()
    if not save_source_text or not save_target_text:
        return _truncate_telegram_reply_text(reply_text)
    combined = (
        f"{reply_text}\n\n"
        f"💾 Фраза для сохранения: {save_source_text}\n"
        f"Перевод: {save_target_text}"
    )
    return _truncate_telegram_reply_text(combined)


async def _generate_quiz_phrase_suggestion(payload: dict) -> dict:
    source_text = str(payload.get("source_text") or "").strip()
    target_text = str(payload.get("target_text") or "").strip()
    source_lang = str(payload.get("source_lang") or "").strip().lower()
    target_lang = str(payload.get("target_lang") or "").strip().lower()
    if not source_text or not target_text or not source_lang or not target_lang:
        raise ValueError("missing_quiz_phrase_payload_fields")

    direction = f"{source_lang}-{target_lang}"
    if direction in {"ru-de", "de-ru"}:
        generated = await run_dictionary_collocations(direction, source_text, target_text)
    else:
        generated = await run_dictionary_collocations_multilang(
            source_lang=source_lang,
            target_lang=target_lang,
            word_source=source_text,
            word_target=target_text,
        )
    items = generated.get("items") if isinstance(generated, dict) else []
    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    base_key = (
        _normalize_dictionary_compare_key(source_text),
        _normalize_dictionary_compare_key(target_text),
    )
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        source_value = str(item.get("source") or "").strip()
        target_value = str(item.get("target") or "").strip()
        if not source_value or not target_value:
            continue
        compare_key = (
            _normalize_dictionary_compare_key(source_value),
            _normalize_dictionary_compare_key(target_value),
        )
        if compare_key in seen:
            continue
        seen.add(compare_key)
        if compare_key == base_key:
            continue
        candidates.append(
            {
                "source": source_value,
                "target": target_value,
            }
        )

    def _candidate_rank(item: dict[str, str]) -> tuple[int, int, int, str]:
        source_value = str(item.get("source") or "").strip()
        source_word_count = len([part for part in re.split(r"\s+", source_value) if part])
        char_count = len(source_value)
        contains_base = 0 if _normalize_dictionary_compare_key(source_text) in _normalize_dictionary_compare_key(source_value) else 1
        return (
            contains_base,
            abs(source_word_count - 3),
            abs(char_count - max(12, len(source_text))),
            source_value.lower(),
        )

    best = min(candidates, key=_candidate_rank) if candidates else {
        "source": source_text,
        "target": target_text,
    }
    return {
        "source_text": str(best.get("source") or "").strip() or source_text,
        "target_text": str(best.get("target") or "").strip() or target_text,
        "source_lang": source_lang,
        "target_lang": target_lang,
    }


def _normalize_language_tutor_llm_response(
    raw_payload: dict,
    *,
    source_lang: str,
    target_lang: str,
) -> dict:
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    is_language_question = bool(payload.get("is_language_question"))
    answer = _truncate_telegram_reply_text(str(payload.get("answer") or "").strip(), max_chars=3000)
    suggested_rephrase = str(payload.get("suggested_rephrase") or "").strip()
    save_source_text = str(payload.get("save_source_text") or "").strip()
    save_target_text = str(payload.get("save_target_text") or "").strip()
    if len(save_source_text) > 180:
        save_source_text = save_source_text[:177].rstrip() + "..."
    if len(save_target_text) > 220:
        save_target_text = save_target_text[:217].rstrip() + "..."
    if save_source_text and not _text_matches_language_side(save_source_text, source_lang):
        save_source_text = ""
        save_target_text = ""
    if save_target_text and not _text_matches_language_side(save_target_text, target_lang):
        save_source_text = ""
        save_target_text = ""
    if not save_source_text or not save_target_text:
        save_source_text = ""
        save_target_text = ""
    return {
        "is_language_question": is_language_question,
        "answer": answer,
        "suggested_rephrase": suggested_rephrase,
        "save_source_text": save_source_text,
        "save_target_text": save_target_text,
        "source_lang": str(source_lang or "").strip().lower(),
        "target_lang": str(target_lang or "").strip().lower(),
    }


async def _send_quiz_result_private(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    quiz_data: dict,
    is_correct: bool,
    selected_text: str | None = None,
) -> bool:
    options = quiz_data.get("options") or []
    correct_option_id = quiz_data.get("correct_option_id")
    correct_text = (quiz_data.get("correct_text") or "").strip()

    correct_option_text = ""
    if isinstance(correct_option_id, int) and 0 <= correct_option_id < len(options):
        indexed_text = str(options[correct_option_id]).strip()
        if indexed_text and indexed_text != QUIZ_FREEFORM_OPTION:
            correct_option_text = indexed_text
    if not correct_option_text:
        correct_option_text = correct_text

    quiz_type = str(quiz_data.get("quiz_type") or "").strip().lower()
    if quiz_type == "anagram":
        de_text = _normalize_quiz_option_for_private_message(correct_text)
    else:
        de_text = _normalize_quiz_option_for_private_message(correct_option_text)
        if not de_text:
            de_text = _normalize_quiz_option_for_private_message(correct_text)

    fallback_ru = quiz_data.get("word_ru") or ""
    ru_text = await _translate_quiz_text_to_ru(de_text, fallback_ru=fallback_ru)

    selected_display = _normalize_quiz_option_for_private_message(selected_text or "")
    status_line = "✅ Верно" if is_correct else "❌ Неверно"
    lines = [
        "🧠 Результат квиза",
        f"Статус: {status_line}",
    ]
    if selected_display:
        lines.append(f"Ваш ответ (DE): {selected_display}")
    lines.extend([
        f"Правильный вариант (DE): {de_text or '—'}",
        f"Перевод (RU): {ru_text or '—'}",
    ])
    explanation_text = _truncate_telegram_reply_text(str(quiz_data.get("explanation") or "").strip(), max_chars=220)
    if explanation_text:
        lines.append(f"Пояснение: {explanation_text}")

    reply_markup = None
    fallback_reply_markup = None
    question_key = None
    phrase_key = None
    feel_source_text = (de_text or correct_text or "").strip()
    feel_target_text = (ru_text or fallback_ru or "").strip()
    if feel_source_text:
        feel_key = hashlib.sha1(
            f"{user_id}:{feel_source_text}:{datetime.utcnow().isoformat()}".encode("utf-8")
        ).hexdigest()[:20]
        pending_quiz_feel_requests[feel_key] = {
            "user_id": int(user_id),
            "source_text": feel_source_text,
            "target_text": feel_target_text,
            "source_lang": "de",
            "target_lang": "ru",
        }
        if len(pending_quiz_feel_requests) > 500:
            oldest_key = next(iter(pending_quiz_feel_requests))
            pending_quiz_feel_requests.pop(oldest_key, None)
        fallback_reply_markup = _build_quiz_result_keyboard(
            feel_key=feel_key,
            question_key=None,
            speak_key=feel_key,
            phrase_key=None,
        )
        reply_markup = fallback_reply_markup
        try:
            phrase_key = _store_pending_quiz_phrase_request(
                user_id=int(user_id),
                source_text=feel_source_text,
                target_text=feel_target_text,
                source_lang="de",
                target_lang="ru",
            )
            question_key = _store_pending_quiz_question_request(
                user_id=int(user_id),
                source_text=feel_source_text,
                target_text=feel_target_text,
                source_lang="de",
                target_lang="ru",
            )
            reply_markup = _build_quiz_result_keyboard(
                feel_key=feel_key,
                question_key=question_key,
                speak_key=feel_key,
                phrase_key=phrase_key,
            )
        except Exception:
            logging.exception("❌ Не удалось подготовить кнопку quiz follow-up user_id=%s", user_id)
            reply_markup = fallback_reply_markup

    max_attempts = 2
    send_variants = []
    if reply_markup is not None:
        send_variants.append(("full_markup", reply_markup))
    if fallback_reply_markup is not None and fallback_reply_markup is not reply_markup:
        send_variants.append(("feel_only_markup", fallback_reply_markup))
    send_variants.append(("no_markup", None))
    for attempt in range(max_attempts):
        for variant_name, variant_markup in send_variants:
            try:
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="\n".join(lines),
                    reply_markup=variant_markup,
                )
                return True
            except RetryAfter as exc:
                delay = max(1, int(getattr(exc, "retry_after", 1)))
                logging.warning(
                    "⚠️ RetryAfter for quiz private result user_id=%s variant=%s sleep=%ss",
                    user_id,
                    variant_name,
                    delay,
                )
                await asyncio.sleep(delay)
                break
            except TimedOut:
                if attempt + 1 < max_attempts:
                    await asyncio.sleep(1.0)
                    break
                logging.warning("⚠️ Timeout sending quiz private result user_id=%s variant=%s", user_id, variant_name)
            except BadRequest as exc:
                logging.warning(
                    "⚠️ BadRequest sending quiz private result user_id=%s variant=%s: %s",
                    user_id,
                    variant_name,
                    exc,
                    exc_info=True,
                )
                continue
            except Exception as exc:
                logging.warning(
                    "⚠️ Не удалось отправить результат квиза в личку user_id=%s variant=%s: %s",
                    user_id,
                    variant_name,
                    exc,
                    exc_info=True,
                )
                if variant_markup is not None:
                    continue
                return False
    return False


def _apply_quiz_freeform_option(quiz: dict) -> dict:
    options = [str(option).strip() for option in quiz.get("options", []) if str(option).strip()]
    if not options:
        return quiz

    correct_option_id = quiz.get("correct_option_id")
    if not isinstance(correct_option_id, int) or not (0 <= correct_option_id < len(options)):
        return quiz

    correct_option_text = options[correct_option_id]
    # Keep semantic correct_text (e.g. unscrumbled anagram word) if generator provided it.
    correct_text = str(quiz.get("correct_text") or "").strip() or correct_option_text
    if QUIZ_FREEFORM_OPTION not in options:
        options.append(QUIZ_FREEFORM_OPTION)

    hide_correct = random.random() < QUIZ_HIDE_CORRECT_PROBABILITY
    if hide_correct:
        options = [option for option in options if option != correct_option_text]
        if QUIZ_FREEFORM_OPTION not in options:
            options.append(QUIZ_FREEFORM_OPTION)

    if len(options) > 10:
        trimmed = []
        for option in options:
            if option == QUIZ_FREEFORM_OPTION:
                continue
            trimmed.append(option)
            if len(trimmed) >= 9:
                break
        if QUIZ_FREEFORM_OPTION not in trimmed:
            trimmed.append(QUIZ_FREEFORM_OPTION)
        options = trimmed

    if hide_correct:
        correct_option_id = options.index(QUIZ_FREEFORM_OPTION)
    else:
        if correct_option_text not in options:
            return quiz
        correct_option_id = options.index(correct_option_text)

    quiz = dict(quiz)
    quiz["options"] = options
    quiz["correct_option_id"] = correct_option_id
    quiz["correct_text"] = correct_text
    quiz["hide_correct"] = hide_correct
    return quiz


def _shuffle_quiz_options(quiz: dict) -> dict | None:
    options = [str(option).strip() for option in quiz.get("options", []) if str(option).strip()]
    correct_option_id = quiz.get("correct_option_id")
    if not options or not isinstance(correct_option_id, int):
        return None
    if not (0 <= correct_option_id < len(options)):
        return None

    flagged_options = [
        {"text": option, "is_correct": idx == correct_option_id}
        for idx, option in enumerate(options)
    ]
    rng = random.SystemRandom()
    rng.shuffle(flagged_options)
    shuffled_options = [item["text"] for item in flagged_options]
    new_correct_id = next((idx for idx, item in enumerate(flagged_options) if item["is_correct"]), None)
    if new_correct_id is None:
        return None

    shuffled = dict(quiz)
    shuffled["options"] = shuffled_options
    shuffled["correct_option_id"] = new_correct_id
    return shuffled


def _is_ru_de_quiz_entry(entry: dict | None) -> bool:
    if not isinstance(entry, dict):
        return False

    response_json = _coerce_response_json(entry.get("response_json"))
    source_lang = str(
        entry.get("source_lang")
        or response_json.get("source_lang")
        or ((response_json.get("language_pair") or {}).get("source_lang") if isinstance(response_json.get("language_pair"), dict) else "")
        or ""
    ).strip().lower()
    target_lang = str(
        entry.get("target_lang")
        or response_json.get("target_lang")
        or ((response_json.get("language_pair") or {}).get("target_lang") if isinstance(response_json.get("language_pair"), dict) else "")
        or ""
    ).strip().lower()

    return source_lang == "ru" and target_lang == "de"


def _extract_german_word(entry: dict, *, require_single_token: bool = False) -> str | None:
    response_json = _coerce_response_json(entry.get("response_json"))
    candidate = (
        (entry.get("translation_de") or "").strip()
        or (entry.get("word_de") or "").strip()
        or (response_json.get("word_de") or "").strip()
        or (response_json.get("translation_de") or "").strip()
    )
    if not candidate:
        return None
    candidate = re.sub(r"\([^)]*\)", "", candidate)
    candidate = candidate.split("/")[0].strip()
    tokens = [t for t in re.split(r"\s+", candidate) if t]
    if not tokens:
        return None
    articles = {"der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer"}
    tokens = [t for t in tokens if t.lower() not in articles]
    if not tokens:
        return None
    if require_single_token and len(tokens) != 1:
        return None
    if require_single_token and tokens[0].lower() == "sich":
        return None
    if tokens[0].lower() == "sich" and len(tokens) > 1:
        return tokens[1]
    return tokens[0]


def _scramble_word(word: str) -> str | None:
    letters = [ch for ch in word if re.match(r"[A-Za-zÄÖÜäöüß]", ch)]
    if len(letters) < 4:
        return None
    original = "".join(letters).lower()
    for _ in range(10):
        random.shuffle(letters)
        scrambled = "".join(letters)
        if scrambled.lower() != original:
            return scrambled
    return None


def _scramble_word_preserve_ends(word: str) -> str | None:
    if not word:
        return None
    if len(word) < 4:
        return None
    first = word[0]
    last = word[-1]
    middle = list(word[1:-1])
    original = "".join(middle).lower()
    for _ in range(20):
        random.shuffle(middle)
        scrambled_middle = "".join(middle)
        if scrambled_middle.lower() != original:
            return f"{first}{scrambled_middle}{last}"
    return None


def _to_letters_only_word(word: str) -> str:
    return "".join(ch for ch in (word or "") if re.match(r"[A-Za-zÄÖÜäöüß]", ch))


_ANAGRAM_MUTATION_ALPHABET = list("abcdefghijklmnopqrstuvwxyzäöüß")
_SEPARABLE_PREFIXES = (
    "zurück", "zurueck", "weiter", "weg", "fest",
    "ab", "an", "auf", "aus", "bei", "ein", "mit", "nach", "vor", "zu", "hin", "her", "los",
)
_ANAGRAM_EXCLUDED_WORDS = {
    # explicit user-requested exclusions
    "um", "mit", "für", "fuer", "der", "die", "das", "ein", "eine", "sich", "nicht", "auch", "schon",
    # common function words/articles/prepositions/pronouns
    "den", "dem", "des", "einen", "einem", "einer", "und", "oder", "aber", "doch", "nur", "sehr",
    "etwas", "haben", "sein", "bin", "bist", "ist", "sind", "seid", "war", "waren", "gewesen",
    "ich", "du", "er", "sie", "es", "wir", "ihr", "mich", "dich", "ihn", "ihm", "uns", "euch", "ihnen",
    "zu", "in", "an", "auf", "bei", "für", "von", "nach", "aus", "über", "unter", "ohne", "durch",
    "gegen", "bis", "ab", "seit", "vor", "hinter", "zwischen", "am", "im", "vom", "zum", "zur",
}


def _is_valid_anagram_target(raw_word: str) -> bool:
    word = str(raw_word or "").strip()
    if not word:
        return False
    if " " in word:
        return False
    if not re.fullmatch(r"[A-Za-zÄÖÜäöüß]+", word):
        return False
    normalized = word.lower()
    if len(normalized) < 4:
        return False
    if normalized in _ANAGRAM_EXCLUDED_WORDS:
        return False
    return True


def _is_valid_anagram_ru_hint(raw_hint: str) -> bool:
    hint = str(raw_hint or "").strip()
    if not hint:
        return False
    if len(hint) > 48:
        return False
    if "\n" in hint:
        return False
    if any(ch in hint for ch in [",", ";", "/", "(", ")", ":", ".", "!", "?", "\"", "«", "»"]):
        return False
    tokens = [token for token in hint.split() if token]
    if len(tokens) != 1:
        return False
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁё-]+", tokens[0]):
        return False
    return True


def _letters_signature(value: str) -> str:
    return "".join(sorted(_to_letters_only_word(value).lower()))


def _middle_signature(value: str) -> str:
    normalized = _to_letters_only_word(value).lower()
    if len(normalized) < 3:
        return ""
    return "".join(sorted(normalized[1:-1]))


def _is_anagram_option_shape_valid(option: str, correct_word: str) -> bool:
    option_clean = _to_letters_only_word(option)
    correct_clean = _to_letters_only_word(correct_word)
    if not option_clean or not correct_clean:
        return False
    if len(option_clean) != len(correct_clean):
        return False
    if option_clean[0].lower() != correct_clean[0].lower():
        return False
    if option_clean[-1].lower() != correct_clean[-1].lower():
        return False
    return True


def _build_impossible_anagram_distractors(correct_word: str, count: int = 3) -> list[str]:
    base = _to_letters_only_word(correct_word)
    if len(base) < 4:
        return []

    first = base[0]
    last = base[-1]
    middle = list(base[1:-1])
    target_middle_signature = "".join(sorted("".join(middle).lower()))
    variants: list[str] = []
    seen: set[str] = set()

    for _ in range(320):
        if len(variants) >= count:
            break
        working = middle[:]
        random.shuffle(working)
        middle_len = len(working)
        mutate_count = 1 if middle_len <= 3 else random.choice([1, 1, 2])
        mutate_positions = random.sample(range(middle_len), k=min(mutate_count, middle_len))
        for pos in mutate_positions:
            current = working[pos].lower()
            replacement_pool = [ch for ch in _ANAGRAM_MUTATION_ALPHABET if ch != current]
            if replacement_pool:
                working[pos] = random.choice(replacement_pool)
        candidate = f"{first}{''.join(working)}{last}"
        lower_candidate = candidate.lower()
        if lower_candidate in seen:
            continue
        if _middle_signature(candidate) == target_middle_signature:
            continue
        seen.add(lower_candidate)
        variants.append(candidate)

    return variants


def _validate_anagram_pool(options_raw: list[str], correct_option_id: int, correct_word: str) -> bool:
    if len(options_raw) != 4:
        return False
    if not (0 <= correct_option_id < len(options_raw)):
        return False
    normalized = [_to_letters_only_word(item) for item in options_raw]
    if any(not item for item in normalized):
        return False
    lowered = [item.lower() for item in normalized]
    if len(set(lowered)) != 4:
        return False
    if any(not _is_anagram_option_shape_valid(item, correct_word) for item in normalized):
        return False

    correct_option = normalized[correct_option_id]
    if _letters_signature(correct_option) != _letters_signature(correct_word):
        return False
    if correct_option.lower() == _to_letters_only_word(correct_word).lower():
        return False

    for idx, option in enumerate(normalized):
        if idx == correct_option_id:
            continue
        if _letters_signature(option) == _letters_signature(correct_word):
            return False
    return True


def _format_anagram_option(word: str) -> str:
    if not word:
        return word
    def _tile_for_char(ch: str) -> str:
        upper = (ch or "").upper()
        if "A" <= upper <= "Z":
            return chr(0x1F1E6 + (ord(upper) - ord("A")))
        return f"▫️{upper}"

    base = _to_letters_only_word(word)
    if not base:
        return word
    return " ".join(_tile_for_char(ch) for ch in base)


def _short_ru_prompt(word_ru: str) -> str | None:
    cleaned = (word_ru or "").strip()
    if not cleaned:
        return None
    if len(cleaned) > 50:
        return None
    tokens = [t for t in cleaned.split() if t]
    if len(tokens) > 3:
        return None
    return cleaned


def _build_anagram_question(word_ru: str, correct_word: str, response_json: dict) -> str:
    usage_examples = response_json.get("usage_examples") if response_json else []
    if usage_examples:
        example = next(
            (
                ex if isinstance(ex, str) else str(ex.get("source") or ex.get("text") or "").strip()
                for ex in usage_examples
                if (isinstance(ex, str) and ex.strip()) or (isinstance(ex, dict) and (ex.get("source") or ex.get("text")))
            ),
            "",
        )
        if example:
            pattern = re.compile(rf"\\b{re.escape(correct_word)}\\b", re.IGNORECASE)
            scrambled = _scramble_word_preserve_ends(correct_word) or correct_word
            if pattern.search(example):
                sentence = pattern.sub(scrambled, example)
                return f"Какой глагол подходит по смыслу?\n{sentence}"
    short_ru = _short_ru_prompt(word_ru)
    if short_ru:
        return f"Выберите правильный немецкий вариант для «{short_ru}». Буквы перемешаны."
    return "Выберите правильный немецкий вариант. Буквы перемешаны."


def _pick_anagram_distractors(correct_word: str, count: int = 3) -> list[str]:
    distractors = []
    attempts = 0
    while len(distractors) < count and attempts < 30:
        attempts += 1
        entry = get_random_dictionary_entry(cooldown_days=0)
        if not entry:
            break
        candidate = _extract_german_word(entry)
        if not candidate:
            continue
        if candidate.lower() == correct_word.lower():
            continue
        scrambled = _scramble_word_preserve_ends(candidate)
        if not scrambled:
            continue
        if scrambled.lower() == correct_word.lower():
            continue
        if scrambled in distractors:
            continue
        distractors.append(scrambled)
    return distractors


async def generate_anagram_quiz(entry: dict) -> dict | None:
    word_ru = (entry.get("word_ru") or "").strip()
    if not _is_valid_anagram_ru_hint(word_ru):
        # Keep prompt/answer aligned to one lexical unit only.
        return None

    correct_word = _extract_german_word(entry, require_single_token=True)
    if not correct_word:
        return None
    if not _is_valid_anagram_target(correct_word):
        return None

    correct_word = _to_letters_only_word(correct_word)
    if not _is_valid_anagram_target(correct_word):
        return None

    correct_scrambled = _scramble_word_preserve_ends(correct_word)
    if not correct_scrambled:
        return None
    if _letters_signature(correct_scrambled) != _letters_signature(correct_word):
        return None

    distractors = _build_impossible_anagram_distractors(correct_word, count=3)
    if len(distractors) < 3:
        return None

    options_raw = [correct_scrambled] + distractors
    options_raw = list(dict.fromkeys(options_raw))[:4]
    if len(options_raw) != 4:
        return None
    random.shuffle(options_raw)
    correct_option_id = options_raw.index(correct_scrambled)
    if not _validate_anagram_pool(options_raw, correct_option_id, correct_word):
        return None
    options = [_format_anagram_option(item) for item in options_raw]

    question = (
        f"Какое немецкое слово соответствует подсказке «{word_ru}»?\n"
        "Выберите правильный вариант: первая и последняя буквы сохранены, внутренние буквы перемешаны."
    )
    return {
        "question": question,
        "options": options,
        "correct_option_id": correct_option_id,
        "quiz_type": "anagram",
        "correct_text": correct_word,
        "word_ru": word_ru,
    }


def _build_word_order_options(sentence: str) -> list[str]:
    tokens = [t for t in sentence.split() if t]
    if len(tokens) < 4:
        return []
    correct = " ".join(tokens)
    options = [correct]
    candidates = []

    swapped = tokens[:]
    swapped[0], swapped[1] = swapped[1], swapped[0]
    candidates.append(" ".join(swapped))

    moved = tokens[:]
    moved.append(moved.pop(0))
    candidates.append(" ".join(moved))

    if len(tokens) > 4:
        middle = tokens[1:-1]
        middle = list(reversed(middle))
        reversed_mid = [tokens[0]] + middle + [tokens[-1]]
        candidates.append(" ".join(reversed_mid))

    for cand in candidates:
        if cand != correct and cand not in options:
            options.append(cand)

    return options


def _contains_cyrillic_text(value: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", str(value or "")))


def _contains_latin_text(value: str) -> bool:
    return bool(re.search(r"[A-Za-zÄÖÜäöüß]", str(value or "")))


def _normalize_word_order_example(
    sentence_raw: str,
    hint_raw: str,
) -> tuple[str | None, str | None]:
    sentence_text = str(sentence_raw or "").strip()
    hint_text = str(hint_raw or "").strip()

    if not sentence_text:
        return None, None

    sentence_has_cyr = _contains_cyrillic_text(sentence_text)
    hint_has_cyr = _contains_cyrillic_text(hint_text)
    hint_has_latin = _contains_latin_text(hint_text)

    de_sentence = sentence_text
    ru_hint = hint_text

    # Legacy records can contain swapped source/target usage examples.
    if sentence_has_cyr and hint_has_latin and not hint_has_cyr:
        de_sentence = hint_text
        ru_hint = sentence_text

    if _contains_cyrillic_text(de_sentence) or not _contains_latin_text(de_sentence):
        return None, None

    # Keep only example-local RU hint; do not fallback to entry-level fields.
    if not ru_hint or not _contains_cyrillic_text(ru_hint):
        return None, None

    return de_sentence, ru_hint


def _build_word_order_question(hint_text: str) -> str:
    hint = str(hint_text or "").strip()
    base = "Выберите правильный порядок слов в немецком предложении."
    if hint:
        return f"{base}\nПодсказка: «{hint}»."
    return base


async def generate_word_order_quiz(entry: dict) -> dict | None:
    response_json = _coerce_response_json(entry.get("response_json"))
    usage_examples_raw = response_json.get("usage_examples") if response_json else []
    usage_examples: list[dict] = []
    if isinstance(usage_examples_raw, list):
        for item in usage_examples_raw:
            if isinstance(item, str):
                sentence = item.strip()
                if sentence:
                    usage_examples.append({"sentence": sentence, "hint": ""})
                continue
            if not isinstance(item, dict):
                continue
            sentence = str(item.get("source") or item.get("text") or "").strip()
            if not sentence:
                continue
            hint = str(item.get("target") or item.get("translation") or "").strip()
            usage_examples.append({"sentence": sentence, "hint": hint})
    if not usage_examples:
        return None

    candidates = usage_examples[:]
    random.shuffle(candidates)
    for selected_example in candidates:
        sentence_raw = str(selected_example.get("sentence") or "").strip()
        hint_raw = str(selected_example.get("hint") or "").strip()
        sentence, hint_text = _normalize_word_order_example(sentence_raw, hint_raw)
        if not sentence or not hint_text:
            continue

        options = _build_word_order_options(sentence)
        if len(options) < 2:
            continue
        if len(options) > 4:
            options = options[:4]
        correct_option_id = options.index(sentence)
        question = _build_word_order_question(hint_text)
        return {
            "question": question,
            "options": options,
            "correct_option_id": correct_option_id,
            "quiz_type": "word_order",
            "word_ru": hint_text,
        }

    return None


def _extract_prefix_candidates_from_text(
    raw_text: str,
    *,
    allow_joined_prefix_variant: bool = False,
) -> list[str]:
    cleaned = str(raw_text or "").strip()
    if not cleaned:
        return []

    tokens = re.findall(r"[A-Za-zÄÖÜäöüß]+", cleaned)
    variants: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if not _is_valid_prefix_quiz_verb(token):
            continue
        normalized = token.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        variants.append(normalized)
    if not allow_joined_prefix_variant:
        return variants
    for idx in range(len(tokens) - 1):
        prefix_token = tokens[idx]
        stem_token = tokens[idx + 1]
        prefix = prefix_token.lower()
        if prefix not in _SEPARABLE_PREFIXES:
            continue
        if not re.fullmatch(r"[A-Za-zÄÖÜäöüß]+", stem_token):
            continue
        if stem_token[:1].isupper():
            continue
        combined = f"{prefix}{stem_token.lower()}"
        if not _is_valid_prefix_quiz_verb(combined):
            continue
        if combined in seen:
            continue
        seen.add(combined)
        variants.append(combined)
    return variants


def _is_valid_prefix_quiz_verb(raw_word: str) -> bool:
    token = str(raw_word or "").strip()
    if not token or " " in token:
        return False
    if not re.fullmatch(r"[A-Za-zÄÖÜäöüß]+", token):
        return False
    if token[:1].isupper():
        return False
    word = token.lower()
    if not word:
        return False
    if len(word) < 5 or len(word) > 28:
        return False
    if not (word.endswith("en") or word.endswith("eln") or word.endswith("ern")):
        return False
    for prefix in _SEPARABLE_PREFIXES:
        if word.startswith(prefix) and len(word) - len(prefix) >= 3:
            return True
    return False


def _is_prefix_candidate_source_text(raw_text: str) -> bool:
    text = re.sub(r"\s+", " ", str(raw_text or "").strip())
    if not text:
        return False
    if bool(re.search(r"[.!?;:,()\"«»]", text)):
        return False
    tokens = [token for token in text.split(" ") if token]
    return len(tokens) <= 2


def _response_json_part_of_speech_is_verb(response_json: dict) -> bool:
    part_of_speech = str((response_json or {}).get("part_of_speech") or "").strip().lower()
    return "verb" in part_of_speech


def _extract_prefix_variants(response_json: dict) -> list[str]:
    prefixes = response_json.get("prefixes") if response_json else []
    variants: list[str] = []
    seen: set[str] = set()
    if isinstance(prefixes, list):
        for item in prefixes:
            if not isinstance(item, dict):
                continue
            raw_variant = str(item.get("variant") or "").strip()
            if not raw_variant:
                continue
            for candidate in _extract_prefix_candidates_from_text(
                raw_variant,
                allow_joined_prefix_variant=True,
            ):
                if candidate in seen:
                    continue
                if not _is_valid_prefix_quiz_verb(candidate):
                    continue
                seen.add(candidate)
                variants.append(candidate)
    return variants


def _extract_prefix_correct_word(entry: dict, response_json: dict) -> str | None:
    explicit_variants = _extract_prefix_variants(response_json)
    if explicit_variants:
        response_word = str(response_json.get("word_de") or response_json.get("translation_de") or "").strip()
        response_word_letters = _to_letters_only_word(response_word).lower()
        if response_word_letters and response_word_letters in explicit_variants:
            return response_word_letters
        return explicit_variants[0]

    if not _response_json_part_of_speech_is_verb(response_json):
        return None

    primary = _extract_german_word({
        "translation_de": entry.get("translation_de"),
        "word_de": entry.get("word_de"),
        "response_json": response_json,
    })
    if primary:
        normalized_primary = _to_letters_only_word(primary).lower()
        if _is_valid_prefix_quiz_verb(normalized_primary):
            return normalized_primary

    fields = [
        entry.get("translation_de"),
        entry.get("word_de"),
        response_json.get("word_de"),
        response_json.get("translation_de"),
        response_json.get("target_text"),
    ]
    for raw_field in fields:
        field_text = str(raw_field or "").strip()
        if not _is_prefix_candidate_source_text(field_text):
            continue
        for candidate in _extract_prefix_candidates_from_text(field_text):
            if _is_valid_prefix_quiz_verb(candidate):
                return candidate
    return None


def _build_prefix_distractors(correct_word: str, count: int = 3) -> list[str]:
    correct = _to_letters_only_word(correct_word).lower()
    if not correct:
        return []

    distractors: list[str] = []
    seen: set[str] = {correct}
    attempts = 0
    while len(distractors) < count and attempts < 140:
        attempts += 1
        entry = get_random_dictionary_entry(cooldown_days=0, source_lang="ru", target_lang="de")
        if not entry:
            break
        entry_response = _coerce_response_json(entry.get("response_json"))
        raw_candidates = [
            entry.get("translation_de"),
            entry.get("word_de"),
            entry_response.get("word_de"),
            entry_response.get("translation_de"),
            entry_response.get("target_text"),
        ]
        candidate_added = False
        for raw in raw_candidates:
            for candidate in _extract_prefix_candidates_from_text(str(raw or "")):
                if not _is_valid_prefix_quiz_verb(candidate):
                    continue
                if candidate in seen:
                    continue
                seen.add(candidate)
                distractors.append(candidate)
                candidate_added = True
                break
            if candidate_added:
                break
    return distractors


def _is_sentence_like_quiz_hint(value: str) -> bool:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return False
    tokens = [token for token in cleaned.split(" ") if token]
    if len(tokens) >= 5:
        return True
    return bool(re.search(r"[,.!?;:«»\"()]", cleaned))


def _extract_prefix_context_options(response_json: dict, correct_word: str) -> list[str] | None:
    raw_options = response_json.get("options") if isinstance(response_json, dict) else None
    if not isinstance(raw_options, list):
        return None
    normalized_options: list[str] = []
    seen: set[str] = set()
    for raw_option in raw_options:
        option = _to_letters_only_word(str(raw_option or "")).lower()
        if not option or option in seen:
            continue
        if not _is_valid_prefix_quiz_verb(option):
            return None
        seen.add(option)
        normalized_options.append(option)
    if len(normalized_options) != 4:
        return None
    if correct_word.lower() not in normalized_options:
        return None
    return normalized_options


def _extract_prefix_quiz_context(
    response_json: dict,
    fallback_ru: str,
    correct_word: str,
) -> dict | None:
    if not isinstance(response_json, dict):
        return None
    sentence_with_gap = re.sub(r"\s+", " ", str(response_json.get("sentence_with_gap") or "").strip())
    translation_ru = re.sub(r"\s+", " ", str(response_json.get("translation_ru") or fallback_ru or "").strip())
    correct_full_sentence = re.sub(r"\s+", " ", str(response_json.get("correct_full_sentence") or "").strip())
    payload_correct_word = _to_letters_only_word(
        str(response_json.get("correct_word") or response_json.get("correct_infinitive") or "").strip()
    ).lower()

    if payload_correct_word and payload_correct_word != correct_word.lower():
        return None
    if sentence_with_gap.count("___") != 1:
        return None
    if not correct_full_sentence:
        return None
    if not translation_ru or not _contains_cyrillic_text(translation_ru):
        return None
    if _contains_cyrillic_text(sentence_with_gap) or not _contains_latin_text(sentence_with_gap):
        return None

    return {
        "sentence_with_gap": sentence_with_gap,
        "translation_ru": translation_ru,
        "correct_full_sentence": correct_full_sentence,
    }


async def generate_prefix_quiz(entry: dict) -> dict | None:
    word_ru = (entry.get("word_ru") or "").strip()
    response_json = _coerce_response_json(entry.get("response_json"))
    if not word_ru:
        return None

    correct_word = _extract_prefix_correct_word(entry, response_json)
    if not correct_word:
        return None

    context = _extract_prefix_quiz_context(response_json, word_ru, correct_word)
    options = _extract_prefix_context_options(response_json, correct_word) or [correct_word]
    if len(options) < 4:
        variants = _extract_prefix_variants(response_json)
        for variant in variants:
            if variant.lower() == correct_word.lower():
                continue
            options.append(variant)
            if len(options) >= 4:
                break

    if len(options) < 4:
        fallback_distractors = _build_prefix_distractors(correct_word, count=4 - len(options))
        for candidate in fallback_distractors:
            if candidate.lower() == correct_word.lower():
                continue
            if candidate in options:
                continue
            options.append(candidate)
            if len(options) >= 4:
                break

    options = list(dict.fromkeys([opt for opt in options if opt]))
    if len(options) < 4:
        return None

    if not context and _is_sentence_like_quiz_hint(word_ru):
        # Long sentence prompts with one-word options are misleading. Skip this
        # entry so scheduler can select a contextual quiz or another item.
        return None

    random.shuffle(options)
    correct_option_id = options.index(correct_word)
    if context:
        question = (
            "Выберите глагол, который правильно заполняет пропуск.\n"
            f"RU: «{context['translation_ru']}»\n"
            f"DE: {context['sentence_with_gap']}"
        )
    else:
        question = (
            f"Выберите правильный немецкий глагол с приставкой для «{word_ru}»."
        )
    return {
        "question": question,
        "options": options,
        "correct_option_id": correct_option_id,
        "quiz_type": "prefix",
        "word_ru": word_ru,
        "correct_text": correct_word,
    }


async def generate_word_quiz(entry: dict) -> dict | None:
    word_ru = (entry.get("word_ru") or "").strip()
    translation_de = (entry.get("translation_de") or "").strip()
    response_json = _coerce_response_json(entry.get("response_json"))
    entry_id = entry.get("id")

    if not word_ru:
        return None

    if not response_json:
        cached = get_dictionary_cache(word_ru)
        if cached:
            response_json = cached
        else:
            try:
                lookup = await run_dictionary_lookup(word_ru)
            except Exception as exc:
                logging.warning(f"⚠️ Dictionary lookup failed for quiz: {exc}")
                lookup = None
            if lookup:
                if translation_de:
                    lookup["translation_de"] = translation_de
                response_json = lookup
                upsert_dictionary_cache(word_ru, response_json)
                update_webapp_dictionary_entry(entry_id, response_json, translation_de or lookup.get("translation_de"))

    article = response_json.get("article") if response_json else None
    part_of_speech = response_json.get("part_of_speech") if response_json else None
    usage_examples = response_json.get("usage_examples") if response_json else []
    usage_examples = usage_examples or []

    fallback = _build_quiz_fallback(word_ru, translation_de, article)

    prompt_payload = {
        "word_ru": word_ru,
        "translation_de": translation_de or response_json.get("translation_de"),
        "article": article,
        "part_of_speech": part_of_speech,
        "usage_examples": usage_examples,
    }

    payload = await run_generate_word_quiz(prompt_payload)
    if not payload:
        return _apply_quiz_freeform_option(fallback)

    quiz = _normalize_quiz_payload(payload, fallback)
    quiz = _apply_quiz_freeform_option(quiz)
    return quiz


async def cleanup_quiz_cache(context: CallbackContext) -> None:
    poll_id = context.job.data.get("poll_id")
    if poll_id in active_quizzes:
        active_quizzes.pop(poll_id, None)
    try:
        await asyncio.to_thread(delete_active_quiz, str(poll_id))
    except Exception:
        logging.debug("Failed to delete active quiz from DB", exc_info=True)


def _toggle_quiz_delivery_mode(mode: str | None) -> str:
    return "new" if str(mode or "").strip().lower() == "repeat" else "repeat"


def _get_quiz_generator_catalog() -> list[tuple[str, Callable[..., Any]]]:
    return [
        ("word_order", generate_word_order_quiz),
        ("prefix", generate_prefix_quiz),
        ("anagram", generate_anagram_quiz),
        ("word", generate_word_quiz),
    ]


def _get_scheduled_quiz_generators(context: CallbackContext) -> list[tuple[str, Callable[..., Any]]]:
    generator_order = _get_quiz_generator_catalog()
    rotation_idx = int(context.application.bot_data.get("quiz_rotation_idx", 0)) % len(generator_order)
    context.application.bot_data["quiz_rotation_idx"] = rotation_idx + 1
    return generator_order[rotation_idx:] + generator_order[:rotation_idx]


def _prioritize_quiz_generators(
    ordered_generators: list[tuple[str, Callable[..., Any]]],
    preferred_quiz_type: str | None,
) -> list[tuple[str, Callable[..., Any]]]:
    preferred = str(preferred_quiz_type or "").strip().lower()
    if not preferred:
        return list(ordered_generators)
    prioritized = [item for item in ordered_generators if item[0] == preferred]
    prioritized.extend(item for item in ordered_generators if item[0] != preferred)
    return prioritized or list(ordered_generators)


async def _generate_quiz_from_entry(
    entry: dict,
    ordered_generators: list[tuple[str, Callable[..., Any]]],
) -> tuple[dict | None, str | None]:
    if not entry or not _is_ru_de_quiz_entry(entry):
        return None, None
    for quiz_type, generator in ordered_generators:
        try:
            quiz = await generator(entry)
        except Exception as exc:
            logging.warning("⚠️ Генератор квиза '%s' упал для word=%s: %s", quiz_type, entry.get("word_ru"), exc)
            quiz = None
        if not quiz:
            continue
        if not quiz.get("quiz_type"):
            quiz["quiz_type"] = quiz_type
        if not quiz.get("word_ru"):
            quiz["word_ru"] = entry.get("word_ru")
        return quiz, quiz_type
    return None, None


async def _select_new_scheduled_quiz(
    ordered_generators: list[tuple[str, Callable[..., Any]]],
) -> dict | None:
    max_entries_per_generator = 4
    for quiz_type, generator in ordered_generators:
        for _ in range(max_entries_per_generator):
            entry = get_random_dictionary_entry_for_quiz_type(
                quiz_type,
                cooldown_days=5,
                source_lang="ru",
                target_lang="de",
            )
            if not entry:
                entry = get_random_dictionary_entry(
                    cooldown_days=5,
                    source_lang="ru",
                    target_lang="de",
                )
            if not entry:
                break
            if not _is_ru_de_quiz_entry(entry):
                logging.warning(
                    "⚠️ Пропускаем quiz entry с неверной языковой парой: source=%s target=%s word=%s",
                    entry.get("source_lang"),
                    entry.get("target_lang"),
                    entry.get("word_ru"),
                )
                continue
            try:
                quiz = await generator(entry)
            except Exception as exc:
                logging.warning("⚠️ Генератор квиза '%s' упал: %s", quiz_type, exc)
                quiz = None
            if not quiz:
                continue
            if not quiz.get("quiz_type"):
                quiz["quiz_type"] = quiz_type
            if not quiz.get("word_ru"):
                quiz["word_ru"] = entry.get("word_ru")
            return {
                "entry": entry,
                "quiz": quiz,
                "resolved_quiz_type": quiz_type,
                "used_mode": "new",
            }
        logging.info("ℹ️ Генератор квиза '%s' вернул пустой результат, пробуем следующий.", quiz_type)
    return None


async def _select_prepared_scheduled_quiz(
    ordered_generators: list[tuple[str, Callable[..., Any]]],
) -> dict | None:
    preferred_types = [quiz_type for quiz_type, _ in ordered_generators]
    try:
        prepared = await asyncio.to_thread(
            claim_prepared_telegram_quiz,
            preferred_types,
            source_lang="ru",
            target_lang="de",
        )
    except Exception:
        logging.warning("⚠️ Не удалось получить prepared scheduled quiz", exc_info=True)
        return None
    if not prepared:
        return None
    quiz = dict(prepared.get("payload") or {})
    resolved_quiz_type = str(prepared.get("quiz_type") or quiz.get("quiz_type") or "generated")
    if not quiz.get("quiz_type"):
        quiz["quiz_type"] = resolved_quiz_type
    if not quiz.get("word_ru") and prepared.get("word_ru"):
        quiz["word_ru"] = prepared.get("word_ru")
    return {
        "entry": {"word_ru": quiz.get("word_ru") or prepared.get("word_ru")},
        "quiz": quiz,
        "resolved_quiz_type": resolved_quiz_type,
        "used_mode": "new_prepared",
    }


async def _select_repeat_scheduled_quiz(
    target_chat_id: int,
    ordered_generators: list[tuple[str, Callable[..., Any]]],
) -> dict | None:
    try:
        candidates = await asyncio.to_thread(
            list_low_accuracy_telegram_quiz_entries,
            int(target_chat_id),
            source_lang="ru",
            target_lang="de",
            accuracy_threshold=QUIZ_REPEAT_ACCURACY_THRESHOLD,
            limit=QUIZ_REPEAT_CANDIDATE_LIMIT,
        )
    except Exception:
        logging.warning("⚠️ Не удалось получить repeat-candidates для chat_id=%s", target_chat_id, exc_info=True)
        return None
    for entry in candidates:
        prioritized_generators = _prioritize_quiz_generators(
            ordered_generators,
            str(entry.get("preferred_quiz_type") or ""),
        )
        quiz, resolved_quiz_type = await _generate_quiz_from_entry(entry, prioritized_generators)
        if not quiz:
            continue
        return {
            "entry": entry,
            "quiz": quiz,
            "resolved_quiz_type": resolved_quiz_type or str(entry.get("preferred_quiz_type") or ""),
            "used_mode": "repeat",
        }
    return None


async def _select_scheduled_quiz_for_target(
    target_chat_id: int,
    ordered_generators: list[tuple[str, Callable[..., Any]]],
) -> dict | None:
    desired_mode = "new"
    try:
        desired_mode = await asyncio.to_thread(get_telegram_quiz_next_mode, int(target_chat_id))
    except Exception:
        logging.warning("⚠️ Не удалось получить состояние чередования quiz для chat_id=%s", target_chat_id, exc_info=True)
    if desired_mode == "repeat":
        selection = await _select_repeat_scheduled_quiz(int(target_chat_id), ordered_generators)
        if not selection:
            selection = await _select_prepared_scheduled_quiz(ordered_generators)
        if not selection:
            selection = await _select_new_scheduled_quiz(ordered_generators)
    else:
        selection = await _select_prepared_scheduled_quiz(ordered_generators)
        if not selection:
            selection = await _select_new_scheduled_quiz(ordered_generators)
        if not selection:
            selection = await _select_repeat_scheduled_quiz(int(target_chat_id), ordered_generators)
    if not selection:
        return None
    selection["desired_mode"] = desired_mode
    return selection


async def prepare_scheduled_quiz_pool(context: CallbackContext, target_per_type: int | None = None) -> None:
    desired_per_type = max(1, int(target_per_type or QUIZ_PREPARED_TARGET_PER_TYPE))
    generator_catalog = _get_quiz_generator_catalog()
    summary: list[str] = []
    for quiz_type, generator in generator_catalog:
        try:
            ready_before = await asyncio.to_thread(
                count_prepared_telegram_quizzes,
                quiz_type,
                source_lang="ru",
                target_lang="de",
            )
        except Exception:
            logging.warning("⚠️ Не удалось посчитать prepared quizzes для type=%s", quiz_type, exc_info=True)
            continue
        missing = max(0, desired_per_type - int(ready_before or 0))
        if missing <= 0:
            summary.append(f"{quiz_type}:ok({ready_before})")
            continue
        added = 0
        attempts = 0
        max_attempts = max(6, missing * 6)
        seen_words: set[str] = set()
        while added < missing and attempts < max_attempts:
            attempts += 1
            entry = get_random_dictionary_entry_for_quiz_type(
                quiz_type,
                cooldown_days=5,
                source_lang="ru",
                target_lang="de",
            )
            if not entry:
                entry = get_random_dictionary_entry(
                    cooldown_days=5,
                    source_lang="ru",
                    target_lang="de",
                )
            if not entry or not _is_ru_de_quiz_entry(entry):
                continue
            word_key = str(entry.get("word_ru") or "").strip().lower()
            if word_key and word_key in seen_words:
                continue
            try:
                quiz = await generator(entry)
            except Exception as exc:
                logging.warning("⚠️ Prepared quiz generator '%s' failed: %s", quiz_type, exc)
                quiz = None
            if not quiz:
                continue
            if not quiz.get("quiz_type"):
                quiz["quiz_type"] = quiz_type
            if not quiz.get("word_ru"):
                quiz["word_ru"] = entry.get("word_ru")
            try:
                await asyncio.to_thread(
                    store_prepared_telegram_quiz,
                    quiz_type,
                    quiz,
                    word_ru=(quiz.get("word_ru") or entry.get("word_ru") or None),
                    source_lang="ru",
                    target_lang="de",
                )
            except Exception:
                logging.warning("⚠️ Не удалось сохранить prepared scheduled quiz type=%s", quiz_type, exc_info=True)
                continue
            if word_key:
                seen_words.add(word_key)
            added += 1
        summary.append(f"{quiz_type}:+{added}/{missing}")
    logging.info("✅ prepared scheduled quiz pool updated: %s", ", ".join(summary))


async def prepare_image_quiz_pool(context: CallbackContext, target_ready_per_user: int | None = None) -> None:
    desired_ready = max(1, int(target_ready_per_user or IMAGE_QUIZ_READY_TARGET_PER_USER))
    if not can_enqueue_background_jobs():
        logging.info("ℹ️ image quiz pool topup skipped: background jobs unavailable")
        return
    candidate_user_ids = await _collect_scheduler_candidate_user_ids(
        lookback_days=IMAGE_QUIZ_TOPUP_LOOKBACK_DAYS,
        include_allowed=True,
        include_admins=True,
    )
    if not candidate_user_ids:
        logging.info("ℹ️ image quiz pool topup skipped: no active users")
        return

    queued_users = 0
    already_ready_users = 0
    failed_users = 0
    total_requested = 0
    for user_id in sorted(candidate_user_ids)[:IMAGE_QUIZ_TOPUP_ACTIVE_USERS_LIMIT]:
        try:
            ready_count = await asyncio.to_thread(
                count_available_image_quiz_templates,
                user_id=int(user_id),
                source_lang="ru",
                target_lang="de",
            )
        except Exception:
            failed_users += 1
            logging.warning("⚠️ Не удалось посчитать image quiz templates для user_id=%s", int(user_id), exc_info=True)
            continue

        missing = max(0, desired_ready - int(ready_count or 0))
        if missing <= 0:
            already_ready_users += 1
            continue

        try:
            await asyncio.to_thread(
                enqueue_image_quiz_template_refresh_job,
                user_id=int(user_id),
                source_lang="ru",
                target_lang="de",
                requested_count=missing,
                request_id=f"image_quiz_topup:{int(user_id)}:{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            )
            queued_users += 1
            total_requested += missing
        except Exception:
            failed_users += 1
            logging.warning("⚠️ Не удалось enqueue image quiz refresh для user_id=%s missing=%s", int(user_id), missing, exc_info=True)

    logging.info(
        "✅ image quiz pool topup queued: candidates=%s queued_users=%s requested=%s already_ready=%s failed=%s desired_ready=%s",
        min(len(candidate_user_ids), IMAGE_QUIZ_TOPUP_ACTIVE_USERS_LIMIT),
        queued_users,
        total_requested,
        already_ready_users,
        failed_users,
        desired_ready,
    )


async def delete_temporary_message(context: CallbackContext) -> None:
    chat_id = context.job.data.get("chat_id")
    message_id = context.job.data.get("message_id")
    if chat_id and message_id:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)


async def _collect_quiz_delivery_targets(context: CallbackContext) -> list[int]:
    try:
        targets = await _collect_scheduler_delivery_targets(context, lookback_days=30, job_name="send_scheduled_quiz")
        filtered_targets = [int(chat_id) for chat_id in targets if not _is_quiz_delivery_target_suppressed(int(chat_id))]
        skipped = len(targets) - len(filtered_targets)
        if skipped > 0:
            logging.info("ℹ️ scheduled quiz: suppressed %s permanently failing target(s)", skipped)
        return filtered_targets
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для scheduled quiz", exc_info=True)
        return []


async def _collect_quiz_delivery_user_targets(context: CallbackContext) -> list[dict]:
    try:
        user_ids = await _collect_scheduler_candidate_user_ids(
            lookback_days=30,
            include_allowed=True,
            include_admins=True,
        )
        delivery_map = await _build_user_delivery_map(context, user_ids, job_name="send_scheduled_quiz")
        grouped: dict[int, list[int]] = {}
        skipped = 0
        for user_id, chat_id in sorted(delivery_map.items(), key=lambda item: (int(item[1]), int(item[0]))):
            safe_chat_id = int(chat_id)
            if _is_quiz_delivery_target_suppressed(safe_chat_id):
                skipped += 1
                continue
            grouped.setdefault(safe_chat_id, []).append(int(user_id))
        if skipped > 0:
            logging.info("ℹ️ scheduled quiz: suppressed %s user target(s) before chat grouping", skipped)
        return [
            {
                "chat_id": int(chat_id),
                "user_ids": [int(user_id) for user_id in user_ids_for_chat],
            }
            for chat_id, user_ids_for_chat in sorted(grouped.items(), key=lambda item: int(item[0]))
        ]
    except Exception:
        logging.warning("⚠️ Не удалось собрать user targets для scheduled quiz", exc_info=True)
        return []


def _build_image_quiz_button_label(option_index: int) -> str:
    labels = ["1", "2", "3", "4"]
    if 0 <= option_index < len(labels):
        return labels[option_index]
    return str(option_index + 1)


def _format_image_quiz_option_text(option_text: str, *, max_chars: int = 84) -> str:
    compact = normalize_image_quiz_option_text(option_text)
    if not compact:
        return "—"
    return _truncate_telegram_reply_text(compact, max_chars=max_chars)


def _build_image_quiz_keyboard(dispatch_id: int, answer_options: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for idx, _option in enumerate(answer_options[:4]):
        current_row.append(
            InlineKeyboardButton(
                _build_image_quiz_button_label(idx),
                callback_data=f"iq:{int(dispatch_id)}:{int(idx)}",
            )
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(rows)


def _build_image_quiz_caption(template: dict, answer_options: list[str]) -> str:
    question = str(template.get("question_de") or "").strip()
    lines = [question or "Was passt zum Bild?"]
    normalized_options = [
        _format_image_quiz_option_text(option)
        for option in (answer_options or [])[:4]
        if str(option or "").strip()
    ]
    if normalized_options:
        lines.append("")
        lines.extend(
            f"{_build_image_quiz_button_label(idx)}. {option_text}"
            for idx, option_text in enumerate(normalized_options)
        )
    return _truncate_telegram_reply_text("\n".join(lines), max_chars=900)


async def _send_image_quiz_for_target(
    context: CallbackContext,
    *,
    target_chat_id: int,
    candidate_user_ids: list[int],
    delivery_slot: str,
    delivery_date_local: date,
) -> bool:
    chosen_user_id: int | None = None
    chosen_template: dict | None = None
    for user_id in candidate_user_ids:
        try:
            template = await asyncio.to_thread(
                claim_next_ready_image_quiz_template,
                user_id=int(user_id),
                source_lang="ru",
                target_lang="de",
            )
        except Exception:
            logging.warning(
                "⚠️ Не удалось claim ready image quiz template user_id=%s chat_id=%s",
                int(user_id),
                int(target_chat_id),
                exc_info=True,
            )
            continue
        if template:
            chosen_user_id = int(user_id)
            chosen_template = template
            break

    if chosen_user_id is None or not chosen_template:
        logging.info(
            "ℹ️ No ready image quiz template for chat_id=%s candidate_user_ids=%s slot=%s",
            int(target_chat_id),
            [int(user_id) for user_id in candidate_user_ids[:8]],
            delivery_slot,
        )
        return False

    quiz_data = build_image_quiz_feedback_payload(chosen_template)
    image_url = str(chosen_template.get("image_url") or "").strip()
    if not quiz_data or not image_url:
        try:
            await asyncio.to_thread(
                mark_image_quiz_template_failed,
                int(chosen_template["id"]),
                last_error="image_quiz_ready_template_invalid",
                visual_status="rejected",
                provider_name="telegram_bot_send",
            )
        except Exception:
            logging.warning(
                "⚠️ Не удалось отметить invalid ready image quiz template template_id=%s",
                chosen_template.get("id"),
                exc_info=True,
            )
        logging.warning(
            "⚠️ Ready image quiz template is incomplete template_id=%s user_id=%s chat_id=%s",
            chosen_template.get("id"),
            chosen_user_id,
            int(target_chat_id),
        )
        return False
    answer_options = list(quiz_data["options"])

    delivery_scope = "group" if int(target_chat_id) < 0 else "private"
    dispatch = None
    try:
        dispatch = await asyncio.to_thread(
            create_image_quiz_dispatch,
            template_id=int(chosen_template["id"]),
            target_user_id=int(chosen_user_id),
            chat_id=int(target_chat_id),
            delivery_scope=delivery_scope,
            delivery_slot=delivery_slot,
            delivery_date_local=delivery_date_local,
            status="claimed",
        )
    except Exception:
        logging.warning(
            "⚠️ Не удалось создать image quiz dispatch template_id=%s user_id=%s chat_id=%s slot=%s date=%s",
            chosen_template.get("id"),
            chosen_user_id,
            int(target_chat_id),
            delivery_slot,
            delivery_date_local,
            exc_info=True,
        )
        return False

    if not dispatch or not dispatch.get("id"):
        logging.warning(
            "⚠️ Image quiz dispatch was not created template_id=%s user_id=%s chat_id=%s",
            chosen_template.get("id"),
            chosen_user_id,
            int(target_chat_id),
        )
        return False

    dispatch_id = int(dispatch["id"])
    try:
        photo_message = await context.bot.send_photo(
            chat_id=int(target_chat_id),
            photo=image_url,
            caption=_build_image_quiz_caption(chosen_template, answer_options),
            reply_markup=_build_image_quiz_keyboard(dispatch_id, answer_options),
        )
    except Exception as exc:
        try:
            await asyncio.to_thread(mark_image_quiz_dispatch_failed, dispatch_id)
        except Exception:
            logging.warning("⚠️ Не удалось отметить failed image quiz dispatch id=%s", dispatch_id, exc_info=True)
        logging.warning(
            "⚠️ Не удалось отправить image quiz template_id=%s dispatch_id=%s user_id=%s chat_id=%s: %s",
            chosen_template.get("id"),
            dispatch_id,
            chosen_user_id,
            int(target_chat_id),
            exc,
        )
        return False

    try:
        await asyncio.to_thread(
            mark_image_quiz_dispatch_sent,
            dispatch_id,
            message_id=int(photo_message.message_id),
        )
    except Exception:
        logging.warning(
            "⚠️ Не удалось отметить sent image quiz dispatch id=%s template_id=%s chat_id=%s",
            dispatch_id,
            chosen_template.get("id"),
            int(target_chat_id),
            exc_info=True,
        )

    logging.info(
        "✅ Image quiz sent: chat_id=%s user_id=%s dispatch_id=%s template_id=%s slot=%s",
        int(target_chat_id),
        int(chosen_user_id),
        dispatch_id,
        int(chosen_template["id"]),
        delivery_slot,
    )
    return True


async def send_scheduled_quiz(context: CallbackContext) -> None:
    ordered = _get_scheduled_quiz_generators(context)
    delivery_targets = await _collect_quiz_delivery_user_targets(context)
    if not delivery_targets:
        logging.info("ℹ️ scheduled quiz: нет targets для рассылки")
        return
    slot_now = _get_quiz_schedule_now()
    delivery_slot = _format_quiz_delivery_slot(slot_now)
    delivery_date_local = slot_now.date()
    image_slot = _is_image_quiz_slot(slot_now)
    sent_count = 0
    for target in delivery_targets:
        target_chat_id = int(target.get("chat_id") or 0)
        target_user_ids = [int(user_id) for user_id in (target.get("user_ids") or []) if int(user_id) > 0]
        if image_slot:
            image_sent = await _send_image_quiz_for_target(
                context,
                target_chat_id=int(target_chat_id),
                candidate_user_ids=target_user_ids,
                delivery_slot=delivery_slot,
                delivery_date_local=delivery_date_local,
            )
            if image_sent:
                sent_count += 1
                continue
            logging.info(
                "ℹ️ Falling back to poll quiz for chat_id=%s slot=%s because image quiz was unavailable",
                int(target_chat_id),
                delivery_slot,
            )
        selection = await _select_scheduled_quiz_for_target(int(target_chat_id), ordered)
        if not selection:
            logging.warning("⚠️ Не удалось подобрать scheduled quiz для chat_id=%s", target_chat_id)
            continue
        quiz = selection.get("quiz") or {}
        chosen_entry = selection.get("entry") or {}
        desired_mode = str(selection.get("desired_mode") or "new")
        used_mode = str(selection.get("used_mode") or "new")
        resolved_quiz_type = str(selection.get("resolved_quiz_type") or quiz.get("quiz_type") or "generated")
        shuffled_quiz = _shuffle_quiz_options(quiz)
        if shuffled_quiz:
            quiz = shuffled_quiz
        try:
            poll_message = await context.bot.send_poll(
                chat_id=int(target_chat_id),
                question=quiz["question"],
                options=quiz["options"],
                type=Poll.QUIZ,
                correct_option_id=quiz["correct_option_id"],
                is_anonymous=False,
                allows_multiple_answers=False,
            )
        except Exception as exc:
            if _is_permanent_quiz_delivery_error(exc):
                _suppress_quiz_delivery_target(int(target_chat_id))
                logging.warning(
                    "⚠️ suppressing scheduled quiz target chat_id=%s for %ss after permanent sendPoll failure: %s",
                    int(target_chat_id),
                    QUIZ_DELIVERY_SUPPRESS_SECONDS,
                    exc,
                )
            logging.warning(
                "⚠️ Не удалось отправить quiz в chat_id=%s: %s",
                target_chat_id,
                exc,
            )
            continue

        sent_count += 1
        active_quizzes[poll_message.poll.id] = {
            "chat_id": int(target_chat_id),
            "correct_option_id": quiz["correct_option_id"],
            "correct_text": quiz.get("correct_text"),
            "options": quiz["options"],
            "freeform_option": QUIZ_FREEFORM_OPTION,
            "message_id": poll_message.message_id,
            "quiz_type": quiz.get("quiz_type", resolved_quiz_type),
            "word_ru": quiz.get("word_ru"),
        }
        try:
            await asyncio.to_thread(
                upsert_active_quiz,
                str(poll_message.poll.id),
                chat_id=int(target_chat_id),
                message_id=int(poll_message.message_id),
                correct_option_id=int(quiz["correct_option_id"]),
                options=[str(option) for option in (quiz.get("options") or [])],
                correct_text=(quiz.get("correct_text") or ""),
                freeform_option=QUIZ_FREEFORM_OPTION,
                quiz_type=(quiz.get("quiz_type") or resolved_quiz_type or "generated"),
                word_ru=(quiz.get("word_ru") or ""),
            )
        except Exception:
            logging.warning("⚠️ Не удалось сохранить активный квиз в БД", exc_info=True)
        try:
            await asyncio.to_thread(
                record_telegram_quiz_delivery,
                int(target_chat_id),
                poll_id=str(poll_message.poll.id),
                word_ru=(quiz.get("word_ru") or chosen_entry.get("word_ru") or ""),
                quiz_type=(quiz.get("quiz_type") or resolved_quiz_type or "generated"),
                delivery_mode=used_mode,
            )
        except Exception:
            logging.warning("⚠️ Не удалось записать историю Telegram quiz delivery", exc_info=True)
        try:
            await asyncio.to_thread(
                set_telegram_quiz_next_mode,
                int(target_chat_id),
                _toggle_quiz_delivery_mode(desired_mode),
            )
        except Exception:
            logging.warning("⚠️ Не удалось обновить состояние чередования Telegram quiz", exc_info=True)

        context.job_queue.run_once(
            cleanup_quiz_cache,
            when=QUIZ_CACHE_TTL_SECONDS,
            data={"poll_id": poll_message.poll.id},
        )
        try:
            await asyncio.to_thread(record_quiz_word, (chosen_entry or {}).get("word_ru"))
        except Exception:
            logging.warning("⚠️ Не удалось записать global quiz history", exc_info=True)
        logging.info(
            "✅ Quiz sent: chat_id=%s mode_slot=%s mode_used=%s type=%s options=%s correct_option_id=%s word_ru=%s",
            int(target_chat_id),
            desired_mode,
            used_mode,
            quiz.get("quiz_type", resolved_quiz_type or "generated"),
            len(quiz.get("options", [])),
            quiz.get("correct_option_id"),
            (chosen_entry or {}).get("word_ru"),
        )
    if sent_count <= 0:
        logging.warning("⚠️ Scheduled quiz run finished without successful deliveries.")
    else:
        logging.info("✅ Scheduled quiz run completed with successful deliveries: %s", sent_count)


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    poll_answer = update.poll_answer
    if not poll_answer:
        logging.warning("⚠️ handle_poll_answer called without poll_answer payload")
        return
    logging.info(
        "🗳️ Poll answer received: poll_id=%s user_id=%s option_ids=%s",
        poll_answer.poll_id,
        getattr(getattr(poll_answer, "user", None), "id", None),
        list(getattr(poll_answer, "option_ids", []) or []),
    )
    quiz_data = active_quizzes.get(poll_answer.poll_id)
    quiz_source = "memory" if quiz_data else ""
    if not quiz_data:
        try:
            quiz_data = await asyncio.to_thread(get_active_quiz, str(poll_answer.poll_id))
            if quiz_data:
                active_quizzes[poll_answer.poll_id] = dict(quiz_data)
                quiz_source = "db"
        except Exception:
            logging.warning("⚠️ Не удалось получить active quiz из БД", exc_info=True)
    if not quiz_data:
        logging.warning(
            "⚠️ Poll answer ignored: active quiz not found poll_id=%s user_id=%s",
            poll_answer.poll_id,
            getattr(getattr(poll_answer, "user", None), "id", None),
        )
        return
    if not poll_answer.option_ids:
        logging.warning(
            "⚠️ Poll answer ignored: empty option_ids poll_id=%s user_id=%s quiz_source=%s",
            poll_answer.poll_id,
            getattr(getattr(poll_answer, "user", None), "id", None),
            quiz_source or "unknown",
        )
        return
    logging.info(
        "🗳️ Poll answer resolved: poll_id=%s user_id=%s quiz_source=%s",
        poll_answer.poll_id,
        getattr(getattr(poll_answer, "user", None), "id", None),
        quiz_source or "memory",
    )

    selected_index = poll_answer.option_ids[0]
    options = quiz_data.get("options") or []
    freeform_option = quiz_data.get("freeform_option")
    selected_text = options[selected_index] if 0 <= selected_index < len(options) else ""

    if freeform_option and selected_text == freeform_option:
        pending_quiz_freeform[poll_answer.user.id] = {
            "poll_id": poll_answer.poll_id,
            "correct_text": quiz_data.get("correct_text") or "",
            "quiz_data": dict(quiz_data),
        }
        try:
            await context.bot.send_message(
                chat_id=poll_answer.user.id,
                text="✍️ Вы выбрали вариант без готового ответа. Напишите ваш вариант одним сообщением здесь, в личке.",
            )
        except Exception as exc:
            logging.warning(f"⚠️ Не удалось отправить freeform-инструкцию в личку user_id={poll_answer.user.id}: {exc}")
            await context.bot.send_message(
                chat_id=quiz_data["chat_id"],
                text=f"{poll_answer.user.first_name}, откройте личку с ботом (/start), чтобы получить результат квиза.",
                reply_to_message_id=quiz_data["message_id"],
            )
        return

    is_correct = selected_index == quiz_data["correct_option_id"]
    try:
        await asyncio.to_thread(
            record_telegram_quiz_attempt,
            str(poll_answer.poll_id),
            chat_id=int(quiz_data.get("chat_id") or 0),
            user_id=int(poll_answer.user.id),
            word_ru=(quiz_data.get("word_ru") or ""),
            quiz_type=(quiz_data.get("quiz_type") or ""),
            selected_option_index=int(selected_index),
            selected_text=selected_text,
            is_correct=bool(is_correct),
        )
    except Exception:
        logging.warning("⚠️ Не удалось записать quiz attempt user_id=%s poll_id=%s", poll_answer.user.id, poll_answer.poll_id, exc_info=True)
    logging.info(
        "📨 Sending quiz private result: poll_id=%s user_id=%s is_correct=%s selected_index=%s",
        poll_answer.poll_id,
        poll_answer.user.id,
        bool(is_correct),
        int(selected_index),
    )
    sent_private = await _send_quiz_result_private(
        context=context,
        user_id=poll_answer.user.id,
        quiz_data=quiz_data,
        is_correct=is_correct,
        selected_text=selected_text,
    )
    logging.info(
        "📨 Quiz private result completed: poll_id=%s user_id=%s sent_private=%s",
        poll_answer.poll_id,
        poll_answer.user.id,
        bool(sent_private),
    )
    if not sent_private:
        await context.bot.send_message(
            chat_id=quiz_data["chat_id"],
            text=f"{poll_answer.user.first_name}, откройте личку с ботом (/start), чтобы получать результаты квизов приватно.",
            reply_to_message_id=quiz_data["message_id"],
        )


async def handle_image_quiz_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    raw_data = str(query.data or "").strip()
    match = re.match(r"^iq:(\d+):(\d+)$", raw_data)
    if not match:
        await query.answer("Quiz unavailable")
        return

    dispatch_id = int(match.group(1))
    selected_option_index = int(match.group(2))
    try:
        dispatch = await asyncio.to_thread(get_image_quiz_dispatch, dispatch_id)
    except Exception:
        logging.warning("⚠️ Не удалось загрузить image quiz dispatch id=%s", dispatch_id, exc_info=True)
        dispatch = None
    if not dispatch or str(dispatch.get("status") or "").strip().lower() != "sent":
        await query.answer("Quiz unavailable")
        return

    try:
        template = await asyncio.to_thread(get_image_quiz_template, int(dispatch.get("template_id") or 0))
    except Exception:
        logging.warning(
            "⚠️ Не удалось загрузить image quiz template для dispatch id=%s template_id=%s",
            dispatch_id,
            dispatch.get("template_id"),
            exc_info=True,
        )
        template = None
    if not template:
        await query.answer("Quiz unavailable")
        return

    quiz_data = build_image_quiz_feedback_payload(template)
    if not quiz_data:
        await query.answer("Quiz unavailable")
        return
    answer_options = list(quiz_data["options"])
    correct_option_index = int(quiz_data["correct_option_id"])
    if selected_option_index < 0 or selected_option_index >= len(answer_options):
        await query.answer("Quiz unavailable")
        return

    selected_text = answer_options[selected_option_index]
    is_correct = int(selected_option_index) == correct_option_index
    try:
        answer = await asyncio.to_thread(
            record_image_quiz_answer,
            dispatch_id=int(dispatch_id),
            user_id=int(user.id),
            selected_option_index=int(selected_option_index),
            selected_text=selected_text,
            is_correct=bool(is_correct),
        )
    except Exception:
        logging.warning(
            "⚠️ Не удалось записать image quiz answer dispatch_id=%s user_id=%s",
            dispatch_id,
            int(user.id),
            exc_info=True,
        )
        await query.answer("Quiz unavailable")
        return

    if not answer:
        await query.answer("Quiz unavailable")
        return

    answer_created = bool(answer.get("created"))
    feedback_sent_at = answer.get("feedback_sent_at")
    stored_selected_text = str(answer.get("selected_text") or selected_text).strip() or selected_text
    stored_is_correct = bool(answer.get("is_correct")) if answer.get("is_correct") is not None else bool(is_correct)

    if (not answer_created) and feedback_sent_at:
        await query.answer(
            build_image_quiz_feedback_alert(
                is_correct=stored_is_correct,
                correct_text=quiz_data.get("correct_text"),
                answer_accepted=False,
            ),
            show_alert=True,
        )
        return

    try:
        await query.answer(
            build_image_quiz_feedback_alert(
                is_correct=stored_is_correct,
                correct_text=quiz_data.get("correct_text"),
                answer_accepted=answer_created,
            ),
            show_alert=True,
        )
    except Exception:
        logging.warning(
            "⚠️ Не удалось показать image quiz alert dispatch_id=%s user_id=%s",
            dispatch_id,
            int(user.id),
            exc_info=True,
        )

    sent_private = await _send_quiz_result_private(
        context=context,
        user_id=int(user.id),
        quiz_data=quiz_data,
        is_correct=stored_is_correct,
        selected_text=stored_selected_text,
    )

    if sent_private:
        try:
            await asyncio.to_thread(
                mark_image_quiz_answer_feedback_sent,
                int(dispatch_id),
                int(user.id),
            )
        except Exception:
            logging.warning(
                "⚠️ Не удалось отметить feedback sent для image quiz dispatch_id=%s user_id=%s",
                dispatch_id,
                int(user.id),
                exc_info=True,
            )
        return

    try:
        if int(dispatch.get("chat_id") or 0) != int(user.id):
            await context.bot.send_message(
                chat_id=int(dispatch.get("chat_id") or 0),
                text=f"{user.first_name}, откройте личку с ботом (/start), чтобы получить результат image quiz.",
                reply_to_message_id=int(dispatch.get("message_id") or 0) or None,
            )
    except Exception:
        logging.warning(
            "⚠️ Не удалось отправить image quiz fallback в чат dispatch_id=%s user_id=%s",
            dispatch_id,
            int(user.id),
            exc_info=True,
        )



def main():
    global application

    bot_polling_enabled = _should_run_primary_telegram_bot_process()
    logging.info(
        "Bot runtime process: polling_enabled=%s railway_service=%s",
        bot_polling_enabled,
        str(os.getenv("RAILWAY_SERVICE_NAME") or "").strip() or "-",
    )
    if not bot_polling_enabled:
        logging.info("Non-primary bot service detected; keeping process idle to avoid duplicate Telegram polling")
        while True:
            pytime.sleep(3600)
    
    # Инициализация базы данных from database.py 
    init_db()
    ensure_webapp_tables()

    #defaults = Defaults(timeout=60)  # увеличили таймаут до 60 секунд
    telegram_request = HTTPXRequest(
        connection_pool_size=max(32, int((os.getenv("TELEGRAM_HTTP_POOL_SIZE") or "64").strip())),
        pool_timeout=max(5.0, float((os.getenv("TELEGRAM_HTTP_POOL_TIMEOUT") or "30").strip())),
        read_timeout=max(10.0, float((os.getenv("TELEGRAM_HTTP_READ_TIMEOUT") or "60").strip())),
        write_timeout=max(10.0, float((os.getenv("TELEGRAM_HTTP_WRITE_TIMEOUT") or "60").strip())),
        connect_timeout=max(5.0, float((os.getenv("TELEGRAM_HTTP_CONNECT_TIMEOUT") or "20").strip())),
    )
    tracking_bot = TrackingExtBot(token=TELEGRAM_Deutsch_BOT_TOKEN, request=telegram_request)
    application = Application.builder().bot(tracking_bot).build()
    application.bot.request.timeout = 60

    # 🔹 Добавляем обработчики команд (исправленный порядок)
    application.add_handler(ChatMemberHandler(handle_bot_group_membership, chat_member_types=ChatMemberHandler.MY_CHAT_MEMBER), group=-4)
    application.add_handler(ChatMemberHandler(track_group_member_context, chat_member_types=ChatMemberHandler.CHAT_MEMBER), group=-3)
    application.add_handler(TypeHandler(Update, enforce_user_access, block=True), group=-2)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("request_access", request_access))
    application.add_handler(CommandHandler("allow", allow_user_command))
    application.add_handler(CommandHandler("deny", deny_user_command))
    application.add_handler(CommandHandler("allowed", allowed_users_command))
    application.add_handler(CommandHandler("pending", pending_requests_command))
    application.add_handler(CommandHandler("pending_purges", pending_purges_command))
    application.add_handler(CommandHandler("mobile_token", mobile_token_command))
    application.add_handler(CommandHandler("budgets", budgets_command))
    application.add_handler(CommandHandler("ttsbudget", tts_budget_command))
    application.add_handler(CommandHandler("ttsprewarmquota", tts_prewarm_quota_command))
    application.add_handler(CallbackQueryHandler(request_access_from_button, pattern=r"^access:request$"))
    # 🔥 Логирование всех сообщений (группа -1, не блокирует цепочку)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, log_message, block=False), group=-1)

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message, block=False), group=1)  # ✅ Сохраняем переводы
    # Legacy ReplyKeyboard-based flow is disabled by default; keep handler for rollback via env.
    if ENABLE_LEGACY_REPLY_KEYBOARD:
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_button_click, block=False), group=1)  # ✅ Обрабатываем кнопки 
    application.add_handler(CallbackQueryHandler(handle_explain_request, pattern=r"^explain:"))
    application.add_handler(CallbackQueryHandler(handle_pending_access_list, pattern=r"^access:pending:list$"))
    application.add_handler(CallbackQueryHandler(handle_access_request_action, pattern=r"^access:(approve|reject|defer):"))
    application.add_handler(CallbackQueryHandler(handle_user_removal_action, pattern=r"^userpurge:(confirm|cancel):"))
    application.add_handler(CallbackQueryHandler(handle_tts_budget_callback, pattern=r"^ttsbudget:"))
    application.add_handler(CallbackQueryHandler(handle_tts_prewarm_quota_callback, pattern=r"^ttsprewarmquota:"))
    application.add_handler(CallbackQueryHandler(handle_flashcard_feel_feedback_callback, pattern=r"^feelfb:"))
    application.add_handler(CallbackQueryHandler(handle_quiz_question_cancel_callback, pattern=r"^quizaskcancel$"))
    application.add_handler(CallbackQueryHandler(handle_quiz_ask_callback, pattern=r"^quizask:"))
    application.add_handler(CallbackQueryHandler(handle_quiz_question_save_callback, pattern=r"^quizqsave:"))
    application.add_handler(CallbackQueryHandler(handle_quiz_phrase_callback, pattern=r"^quizphrase:"))
    application.add_handler(CallbackQueryHandler(handle_quiz_speak_callback, pattern=r"^quizspeak:"))
    application.add_handler(CallbackQueryHandler(handle_quiz_feel_callback, pattern=r"^quizfeel:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_speak_callback, pattern=r"^dictspeak:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_feel_callback, pattern=r"^dictfeel:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_pair_callback, pattern=r"^dictpair:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_mode_callback, pattern=r"^dictmode:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_select_toggle_callback, pattern=r"^dictseltoggle:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_select_all_callback, pattern=r"^dictselall:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_confirm_callback, pattern=r"^dictsaveconfirm:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_option_callback, pattern=r"^dictsaveopt:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_quick_save_callback, pattern=r"^dictquicksave:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_callback, pattern=r"^dictsave:"))
    application.add_handler(CallbackQueryHandler(handle_group_enrollment_callback, pattern=r"^groupenroll:confirm$"))
    application.add_handler(CallbackQueryHandler(handle_language_tutor_callback, pattern=r"^langgpt:(ask|continue)$"))
    application.add_handler(CallbackQueryHandler(handle_image_quiz_callback, pattern=r"^iq:\d+:\d+$"))

    application.add_handler(CommandHandler("translate", check_user_translation))  # ✅ Проверка переводов


    application.add_handler(CallbackQueryHandler(topic_selected)) #Он ждет любые нажатия на inline-кнопки.
    if BOT_DEBUG_LOG_ALL_MESSAGES:
        application.add_handler(MessageHandler(filters.TEXT, log_all_messages, block=False), group=2)
    application.add_handler(PollAnswerHandler(handle_poll_answer))

    application.add_error_handler(error_handler)
    bot_scheduler_enabled = _should_start_bot_scheduler()
    logging.info(
        "Bot scheduler runtime: enabled=%s railway_service=%s",
        bot_scheduler_enabled,
        str(os.getenv("RAILWAY_SERVICE_NAME") or "").strip() or "-",
    )
    if application.job_queue and bot_scheduler_enabled:
        application.job_queue.run_once(backfill_group_enrollment_prompts, when=20)
        application.job_queue.run_once(prepare_scheduled_quiz_pool, when=QUIZ_PREPARED_STARTUP_DELAY_SECONDS)
        application.job_queue.run_once(prepare_image_quiz_pool, when=QUIZ_PREPARED_STARTUP_DELAY_SECONDS + 20)
    elif application.job_queue:
        logging.info("Skipping bot startup run_once jobs in this process")
    
    # --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ LIVEKIT КНОПОК ---
    #application.add_handler(MessageHandler(filters.Regex(r'🎙 Начать урок'), start_lesson)) # Теперь обрабатываем текст кнопки
    #application.add_handler(MessageHandler(filters.Regex(r'👥 Групповой звонок'), group_call)) # Теперь обрабатываем текст кнопки
    
    # 1) Создаём loop и делаем его текущим для MainThread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 2) Прогрев ассистента
    # try:
    #     loop.run_until_complete(
    #         get_or_create_openai_resources("sales_assistant_instructions", "sales_assistant")
    #     )
    #     logging.info("✅ Sales Assistant Assistant ID подтвержден/создан при старте бота.")
    # except Exception as e:
    #     logging.critical(f"❌ Не удалось инициализировать Sales Assistant: {e}", exc_info=True)


    # 3) APScheduler → вкидываем корутину в тот же loop
    def submit_async(async_func, context=None, *args, **kwargs):
        if context is None:
            context = CallbackContext(application=application)

        fut = asyncio.run_coroutine_threadsafe(
            async_func(context, *args, **kwargs),
            loop
        )
        fut.add_done_callback(lambda f: f.exception() and logging.exception("❌ APScheduler job crashed"))

    # def run_async_job(async_func, context=None, *args, **kwargs):
    #     if context is None:
    #         context = CallbackContext(application=application)   # Создаем `context`, если его нет

    #     try:
    #         loop = asyncio.get_running_loop() # ✅ Берем уже работающий event loop
    #     except RuntimeError:
    #         loop = asyncio.new_event_loop()  # ❌ В потоке `apscheduler` нет loop — создаем новый
    #         asyncio.set_event_loop(loop)
    #     loop.run_until_complete(async_func(context, *args, **kwargs)) # ✅ Теперь event loop всегда работает

    # --- ЗАДАЧИ SCHEDULER ИСПОЛЬЗУЮТ НОВУЮ СТРУКТУРУ ---
    # Мы можем гарантировать, что Sales Assistant создан при запуске бота:
    # try:
    #     # Используем get_or_create_openai_resources из openai_manager.py
    #     # Обратите внимание: task_name и system_instruction (ключ) одинаковы.
        
    #     # await — это “подожди внутри уже работающего асинхронного мира”.
    #     # asyncio.run() — это “создай маленький асинхронный мир, выполни там задачу до конца, вернись обратно в обычный Python”.
    #     await get_or_create_openai_resources("sales_assistant_instructions", "sales_assistant")

    #     logging.info("✅ Sales Assistant Assistant ID подтвержден/создан при старте бота.")
    # except Exception as e:
    #     logging.critical(
    #         f"❌ Критическая ошибка: Не удалось инициализировать Sales Assistant при запуске: {e}", 
    #         exc_info=True
    #     )
    #     # Если это критично, можно здесь sys.exit(1)


    # ✅ Добавляем задачу в `scheduler` ДЛЯ УТРА
    scheduler = BackgroundScheduler() if bot_scheduler_enabled else None

    if scheduler is not None:
        print("📌 Добавляем задачу в scheduler...")
        scheduler.add_job(lambda: submit_async(send_morning_reminder,CallbackContext(application=application)),"cron", hour=5, minute=5)
        scheduler.add_job(lambda: submit_async(send_morning_reminder,CallbackContext(application=application)),"cron", hour=15, minute=30)
        scheduler.add_job(
            lambda: submit_async(backfill_group_enrollment_prompts, CallbackContext(application=application)),
            "cron",
            hour=4,
            minute=20,
        )
        for hour, minute in FLASHCARD_REMINDER_TIMES:
            scheduler.add_job(
                lambda: submit_async(send_flashcard_reminder, CallbackContext(application=application)),
                "cron",
                hour=hour,
                minute=minute,
            )

        for hour in QUIZ_SCHEDULE_HOURS:
            for minute in QUIZ_SCHEDULE_MINUTES:
                scheduler.add_job(
                    lambda: submit_async(send_scheduled_quiz, CallbackContext(application=application)),
                    "cron",
                    hour=hour,
                    minute=minute,
                )
        scheduler.add_job(
            lambda: submit_async(
                prepare_scheduled_quiz_pool,
                CallbackContext(application=application),
                QUIZ_PREPARED_TARGET_PER_TYPE,
            ),
            "cron",
            minute=QUIZ_PREPARED_HOURLY_TOPUP_MINUTE,
        )
        scheduler.add_job(
            lambda: submit_async(
                prepare_image_quiz_pool,
                CallbackContext(application=application),
                IMAGE_QUIZ_READY_TARGET_PER_USER,
            ),
            "cron",
            minute=QUIZ_PREPARED_HOURLY_TOPUP_MINUTE,
        )

    # scheduler.add_job(
    #     lambda: submit_async(send_german_news, CallbackContext(application=application)), 
    #     "cron",
    #     hour=4,
    #     minute=1,
    #     #day_of_week = "mon,tue,thu,fri,sat"
    #     day_of_week = "mon, fri"
    # )
    
        scheduler.add_job(lambda: submit_async(send_me_analytics_and_recommend_me, CallbackContext(application=application)), "cron", day_of_week="fri", hour=15, minute=15)
        scheduler.add_job(lambda: submit_async(send_me_analytics_and_recommend_me, CallbackContext(application=application)), "cron", day_of_week="mon", hour=6, minute=5) 
    #scheduler.add_job(lambda: run_async_job(send_me_analytics_and_recommend_me, CallbackContext(application=application)), "cron", day_of_week="sun", hour=7, minute=7)
    
    # Legacy auto-close disabled.
    # Session auto-close now lives in backend/backend_server.py and runs via
    # TRANSLATION_SESSIONS_AUTO_CLOSE_* on TODAY_PLAN_TZ at 23:59 by default.
    
        scheduler.add_job(lambda: submit_async(send_daily_summary), "cron", hour=20, minute=52)
        scheduler.add_job(lambda: submit_async(send_weekly_summary), "cron", day_of_week="sun", hour=20, minute=55)

        for hour in [7,12,16]:
            scheduler.add_job(lambda: submit_async(send_progress_report), "cron", hour=hour, minute=5)

    #scheduler.add_job(lambda: submit_async(get_yesterdays_mistakes_for_audio_message, CallbackContext(application=application)), "cron", hour=4, minute=15)

    # scheduler.add_job(lambda: submit_async(send_user_analytics_bar_charts, CallbackContext(application=application), period="day"), "cron", hour= 22, minute=39, day_of_week = "sun")

    # планировщик по отправке аналитике:
    # scheduler.add_job(lambda: submit_async(send_users_comparison_bar_chart, CallbackContext(application=application), period="day"), "cron", hour=22, minute=40, day_of_week="sun")
    
    # scheduler.add_job(lambda: submit_async(send_users_comparison_bar_chart, CallbackContext(application=application), period="week"), "cron", day="last", hour= 22, minute=2)

    # scheduler.add_job(lambda: submit_async(send_users_comparison_bar_chart, CallbackContext(application=application), period="month"), "cron", day="last", month="3,6,9,12", hour= 7, minute=2)

    # scheduler.add_job(lambda: submit_async(send_users_comparison_bar_chart, CallbackContext(application=application), period="half_year"), "cron", day="last", month="6,12", hour= 10, minute=2)

    # scheduler.add_job(lambda: submit_async(send_users_comparison_bar_chart, CallbackContext(application=application), period="quarter"), "cron", day="last", month="12", hour= 23, minute=2)
        try:
            user_removal_review_timezone = ZoneInfo(USER_REMOVAL_REVIEW_TZ)
        except Exception:
            user_removal_review_timezone = ZoneInfo("UTC")
        scheduler.add_job(
            lambda: submit_async(cleanup_system_messages, CallbackContext(application=application)),
            "cron",
            hour=SYSTEM_MESSAGE_CLEANUP_HOUR,
            minute=SYSTEM_MESSAGE_CLEANUP_MINUTE,
        )
        scheduler.add_job(
            lambda: submit_async(_notify_admins_due_user_removals, CallbackContext(application=application)),
            "cron",
            minute=USER_REMOVAL_REVIEW_MINUTE,
            timezone=user_removal_review_timezone,
        )
        try:
            user_removal_weekly_timezone = ZoneInfo(USER_REMOVAL_WEEKLY_REPORT_TZ)
        except Exception:
            user_removal_weekly_timezone = ZoneInfo("UTC")
        scheduler.add_job(
            lambda: submit_async(send_weekly_user_removal_digest, CallbackContext(application=application)),
            "cron",
            day_of_week=USER_REMOVAL_WEEKLY_REPORT_DAY,
            hour=USER_REMOVAL_WEEKLY_REPORT_HOUR,
            minute=USER_REMOVAL_WEEKLY_REPORT_MINUTE,
            timezone=user_removal_weekly_timezone,
        )
        budget_report_enabled = (os.getenv("BUDGET_REPORT_SCHEDULER_ENABLED") or "1").strip().lower() in {"1", "true", "yes", "on"}
        if budget_report_enabled:
            budget_report_day = max(1, min(28, int((os.getenv("BUDGET_REPORT_SCHEDULER_DAY") or "1").strip())))
            budget_report_hour = max(0, min(23, int((os.getenv("BUDGET_REPORT_SCHEDULER_HOUR") or "9").strip())))
            budget_report_minute = max(0, min(59, int((os.getenv("BUDGET_REPORT_SCHEDULER_MINUTE") or "0").strip())))
            budget_report_tz = (os.getenv("BUDGET_REPORT_SCHEDULER_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
            try:
                budget_report_timezone = ZoneInfo(budget_report_tz)
            except Exception:
                budget_report_timezone = ZoneInfo("UTC")
            scheduler.add_job(
                lambda: submit_async(send_monthly_budget_report, CallbackContext(application=application)),
                "cron",
                day=budget_report_day,
                hour=budget_report_hour,
                minute=budget_report_minute,
                timezone=budget_report_timezone,
            )

        scheduler.start()
    else:
        logging.info("Skipping APScheduler startup in this bot process")
    print("🚀 Бот запущен! Ожидаем сообщения...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)





if __name__ == "__main__":
    main()
