import os
import openai
from openai import OpenAI
import logging
import json
import psycopg2
import datetime
import calendar
from datetime import datetime, time, date, timedelta
from zoneinfo import ZoneInfo
from telegram import Update, Poll
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, TypeHandler, Defaults, PollAnswerHandler, ContextTypes, ApplicationHandlerStop, ExtBot
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
from telegram.error import TimedOut, BadRequest, RetryAfter
import tempfile
import sys
import livekit.api # Нужен для LiveKit комнат
from google.cloud import texttospeech
from backend.analytics import fetch_user_summary, get_period_bounds
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
    run_dictionary_collocations,
    run_dictionary_collocations_multilang,
    run_generate_word_quiz,
    run_translate_subtitles_ru,
    run_translate_subtitles_multilang,
)
from backend.database import (
    init_db,
    get_random_dictionary_entry,
    get_random_dictionary_entry_for_quiz_type,
    record_quiz_word,
    ensure_webapp_tables,
    get_admin_telegram_ids,
    is_telegram_user_allowed,
    allow_telegram_user,
    revoke_telegram_user,
    list_allowed_telegram_users,
    create_access_request,
    resolve_access_request,
    resolve_latest_pending_access_request_for_user,
    get_access_request_by_id,
    list_pending_access_requests,
    update_webapp_dictionary_entry,
    apply_flashcard_feel_feedback,
    get_dictionary_cache,
    upsert_dictionary_cache,
    save_webapp_dictionary_query,
    create_support_message,
    get_or_create_dictionary_folder,
    record_telegram_system_message,
    get_pending_telegram_system_messages,
    mark_telegram_system_message_deleted,
    upsert_active_quiz,
    get_active_quiz,
    delete_active_quiz,
)
from user_analytics import prepare_aggregate_data_by_period_and_draw_analytic_for_user, aggregate_data_for_charts, create_analytics_figure_async
from load_data_from_db import load_data_for_analytics 
from users_comparison_analytics import create_comparison_report_async
from dateutil.relativedelta import relativedelta 
from datetime import date, timedelta
from backend.config_mistakes_data import VALID_CATEGORIES, VALID_SUBCATEGORIES, VALID_CATEGORIES_lower, VALID_SUBCATEGORIES_lower

application = None

QUIZ_SCHEDULE_HOURS = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
QUIZ_SCHEDULE_MINUTES = [0, 30]
QUIZ_FEEDBACK_TTL_SECONDS = 120
QUIZ_CACHE_TTL_SECONDS = 60 * 60 * 24
QUIZ_FREEFORM_OPTION = "keine korrekte Antworten"
QUIZ_HIDE_CORRECT_PROBABILITY = 0.3
FLASHCARD_REMINDER_TIMES = [(7, 0), (16, 30)]
active_quizzes = {}
pending_quiz_freeform = {}
quiz_ru_translation_cache = {}
pending_dictionary_cards = {}
pending_dictionary_save_options = {}
pending_dictionary_lookup_requests = {}
MOBILE_AUTH_TTL_SECONDS = int(os.getenv("MOBILE_AUTH_TTL_SECONDS", "2592000"))
SYSTEM_MESSAGE_CLEANUP_TZ = (os.getenv("SYSTEM_MESSAGE_CLEANUP_TZ") or os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
SYSTEM_MESSAGE_CLEANUP_HOUR = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_HOUR") or "23").strip())
SYSTEM_MESSAGE_CLEANUP_MINUTE = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MINUTE") or "59").strip())
SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK") or "2").strip())
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

    async def send_message(self, *args, **kwargs):
        msg = await super().send_message(*args, **kwargs)
        return await self._track_single(msg, "text")

    async def send_photo(self, *args, **kwargs):
        msg = await super().send_photo(*args, **kwargs)
        return await self._track_single(msg, "photo")

    async def send_audio(self, *args, **kwargs):
        msg = await super().send_audio(*args, **kwargs)
        return await self._track_single(msg, "audio")

    async def send_voice(self, *args, **kwargs):
        msg = await super().send_voice(*args, **kwargs)
        return await self._track_single(msg, "voice")

    async def send_document(self, *args, **kwargs):
        msg = await super().send_document(*args, **kwargs)
        return await self._track_single(msg, "document")

    async def send_video(self, *args, **kwargs):
        msg = await super().send_video(*args, **kwargs)
        return await self._track_single(msg, "video")

    async def send_video_note(self, *args, **kwargs):
        msg = await super().send_video_note(*args, **kwargs)
        return await self._track_single(msg, "video_note")

    async def send_animation(self, *args, **kwargs):
        msg = await super().send_animation(*args, **kwargs)
        return await self._track_single(msg, "animation")

    async def send_sticker(self, *args, **kwargs):
        msg = await super().send_sticker(*args, **kwargs)
        return await self._track_single(msg, "sticker")

    async def copy_message(self, *args, **kwargs):
        msg_id = await super().copy_message(*args, **kwargs)
        # copy_message returns MessageId, not Message.
        return msg_id

    async def forward_message(self, *args, **kwargs):
        msg = await super().forward_message(*args, **kwargs)
        return await self._track_single(msg, "forward")

    async def send_media_group(self, *args, **kwargs):
        messages = await super().send_media_group(*args, **kwargs)
        if messages:
            for msg in messages:
                await _track_telegram_message_async(msg, "media_group")
        return messages

    async def send_poll(self, *args, **kwargs):
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

# ID группы
BOT_GROUP_CHAT_ID_Deutsch = -1002607222537

if BOT_GROUP_CHAT_ID_Deutsch:
    logging.info("✅ GROUP_CHAT_ID успешно загружен!")
else:
    logging.error("❌ GROUP_CHAT_ID не загружен! Проверьте переменные окружения.")

BOT_GROUP_CHAT_ID_Deutsch = int(BOT_GROUP_CHAT_ID_Deutsch)

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

    if response.status_code == 200:
        data = response.json()

        if "data" in data and len(data["data"]) > 0:
            print("📢 Nachrichten auf Deutsch:")
            for i, article in enumerate(data["data"], start=1):  # Ограничим до 3 новостей in API request
                title = article.get("title", "Без заголовка")
                source = article.get("source", "Неизвестный источник")
                url = article.get("url", "#")

                message = f"📰 {i}. *{title}*\n\n📌 {source}\n\n[Читать полностью]({url})"
                await context.bot.send_message(
                    chat_id=BOT_GROUP_CHAT_ID_Deutsch,
                    text=message,
                    parse_mode="Markdown",
                    disable_web_page_preview=False  # Чтобы загружались превью страниц
                )
        else:
            await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text="❌ Нет свежих новостей на сегодня!")
    else:
        await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text=f"❌ Ошибка: {response.status_code} - {response.text}")



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
        )
        return

    keyboard = [
        ["📌 Выбрать тему"],  # ❗ Убедись, что текст здесь правильный
        ["🚀 Начать перевод", "✅ Завершить перевод"],
        ["📜 Проверить перевод", "🟡 Посмотреть свою статистику"],
        ["🎙 Начать урок", "👥 Групповой звонок"],
        ["💬 Перейти в личку"]
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


async def enforce_user_access(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return

    # Poll answers must be processed to deliver private quiz feedback.
    if getattr(update, "poll_answer", None):
        return

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

    if update.callback_query:
        try:
            await update.callback_query.answer("Доступ закрыт. Ожидайте одобрения администратора.", show_alert=True)
        except Exception:
            pass
    elif message:
        await message.reply_text(
            "⛔️ Доступ к боту закрыт.\n"
            "Нажмите кнопку ниже для отправки запроса администратору.",
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
    await update.effective_message.reply_text(
        f"📨 Запрос отправлен администратору.\nВаш ID: {user.id}\nОжидайте подтверждения."
    )


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
    if query.message:
        await query.message.reply_text(
            "📨 Запрос отправлен администратору. Ожидайте подтверждения."
        )


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

    resolved = resolve_latest_pending_access_request_for_user(
        user_id=target_id,
        status="rejected",
        reviewed_by=int(sender.id),
        review_note="rejected via /deny",
    )
    removed = revoke_telegram_user(target_id)
    if resolved:
        await update.effective_message.reply_text(f"🚫 Заявка отклонена для пользователя {target_id}.")
    elif removed:
        await update.effective_message.reply_text(f"🚫 Доступ отозван у пользователя {target_id}.")
    else:
        await update.effective_message.reply_text(f"ℹ️ Нет pending-заявки и пользователя {target_id} нет в whitelist.")
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text="❌ Заявка отклонена администратором.",
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
        admin_text = f"✅ Заявка #{request_id} одобрена. Пользователь {target_id} получил доступ."
        user_text = "✅ Ваша заявка одобрена. Доступ к боту и WebApp открыт."
    else:
        revoke_telegram_user(target_id)
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
        if update.effective_message:
            await update.effective_message.reply_text(
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
        print("⚠️ Пустое сообщение — пропускаем логирование.")
        return
    
    username = user.username or f"{user.first_name or ''} {user.last_name or ''}".strip()
    # Логируем данные для диагностики
    print(f"📥 Получено сообщение от {username} ({user.id}): {message_text}")

    conn = get_db_connection()
    cursor = conn.cursor()
    try: 
        cursor.execute("""
            INSERT INTO bt_3_messages (user_id, username, message)
            VALUES(%s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET timestamp = NOW();
            """,
            (user.id, username, 'user_message')
        )

        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка при записи в базу: {e}")
    finally:
        cursor.close()
        conn.close()

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
        targets = await _collect_quiz_delivery_targets(context)
    except Exception:
        logging.warning("⚠️ Не удалось собрать targets для morning reminder, отправляем только в группу", exc_info=True)
        targets = [int(BOT_GROUP_CHAT_ID_Deutsch)]

    for target_chat_id in targets:
        try:
            if int(target_chat_id) == int(BOT_GROUP_CHAT_ID_Deutsch):
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
    await context.bot.send_message(
        chat_id=BOT_GROUP_CHAT_ID_Deutsch,
        text=message,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )



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

    pending = pending_quiz_freeform.get(user_id)
    if pending:
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text("Ответ на этот квиз отправьте в личку с ботом.")
            return

        correct_text = pending.get("correct_text") or ""
        is_correct = False
        if correct_text:
            is_correct = _normalize_quiz_text(text) == _normalize_quiz_text(correct_text)
        await _send_quiz_result_private(
            context=context,
            user_id=user_id,
            quiz_data=pending.get("quiz_data") or {},
            is_correct=is_correct,
            selected_text=text,
        )
        pending_quiz_freeform.pop(user_id, None)
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


async def _coerce_sentence_lookup_payload(
    payload: dict,
    lookup_input: str,
    source_lang: str,
    target_lang: str,
) -> dict:
    normalized_input = str(lookup_input or "").strip()
    if not _is_sentence_like_lookup(normalized_input):
        return payload if isinstance(payload, dict) else {}

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

    result = dict(payload or {})
    result["word_source"] = normalized_input
    result["word_target"] = forced_target
    result["part_of_speech"] = "phrase"

    if source_lang == "ru" and target_lang == "de":
        result["word_ru"] = normalized_input
        result["translation_de"] = forced_target
    elif source_lang == "de" and target_lang == "ru":
        result["word_de"] = normalized_input
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
            "example_source": normalized_input,
            "example_target": forced_target,
        },
        "secondary": [],
    }

    usage_examples = [{"source": normalized_input, "target": forced_target}]
    existing_examples = result.get("usage_examples")
    if isinstance(existing_examples, list):
        for item in existing_examples:
            if not isinstance(item, dict):
                continue
            src = str(item.get("source") or "").strip()
            tgt = str(item.get("target") or "").strip()
            if not src or not tgt:
                continue
            if src.casefold() == normalized_input.casefold() and tgt.casefold() == forced_target.casefold():
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
        ("Prateritum", forms.get("praeteritum")),
        ("Perfekt", forms.get("perfekt")),
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
        return {"etymology_note": "", "usage_note": "", "memory_tip": ""}
    return {
        "etymology_note": str(lookup.get("etymology_note") or "").strip(),
        "usage_note": str(lookup.get("usage_note") or "").strip(),
        "memory_tip": str(lookup.get("memory_tip") or "").strip(),
    }


def _build_dictionary_card_text(source_lang: str, target_lang: str, source_text: str, lookup: dict) -> str:
    def _esc(value: str) -> str:
        return html.escape(str(value or "").strip())

    source_text = source_text.strip()
    translation = (lookup.get("word_target") or "").strip()
    meanings = _extract_lookup_meanings(lookup)
    notes = _extract_learning_notes(lookup)
    part_of_speech = (lookup.get("part_of_speech") or "").strip()
    article = (lookup.get("article") or "").strip()
    pronunciation = lookup.get("pronunciation") if isinstance(lookup.get("pronunciation"), dict) else {}
    ipa = str(pronunciation.get("ipa") or "").strip()
    stress = str(pronunciation.get("stress") or "").strip()
    forms = _format_forms_block(lookup.get("forms"))

    lines: list[str] = []
    lines.append("🔹 <b>Запрос</b>")
    lines.append(f"• <code>{_esc(source_text or '—')}</code>")

    if meanings:
        primary = meanings[0]
        primary_value = _apply_article_for_display(str(primary.get("value") or translation or "—"), lookup, target_lang)
        lines.append("")
        lines.append("🎯 <b>Основной перевод</b>")
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
        lines.append("🎯 <b>Основной перевод</b>")
        lines.append(f"• <b>{_esc(_apply_article_for_display(translation or '—', lookup, target_lang))}</b>")

    lines.append("")
    lines.append("🗂 <b>Грамматика и произношение</b>")
    lines.append(f"• Часть речи: {_esc(part_of_speech or '—')}")
    if article:
        lines.append(f"• Артикль: <b>{_esc(article)}</b>")
    if ipa or stress:
        pron = f"{ipa or '—'}{f' | ударение: {stress}' if stress else ''}"
        lines.append(f"• Произношение: {_esc(pron)}")
    if forms:
        lines.append("• Формы:")
        for form_line in forms:
            cleaned = form_line.replace("- ", "", 1)
            lines.append(f"   - {_esc(cleaned)}")

    note_lines = []
    if notes["etymology_note"]:
        note_lines.append(f"• Происхождение: {_esc(notes['etymology_note'])}")
    if notes["usage_note"]:
        note_lines.append(f"• Где используется: {_esc(notes['usage_note'])}")
    if notes["memory_tip"]:
        note_lines.append(f"• Фишка запоминания: {_esc(notes['memory_tip'])}")
    if note_lines:
        lines.append("")
        lines.append("💡 <b>Как запомнить</b>")
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


def _build_save_variant_keyboard(option_key: str, options: list[dict], selected: list[int] | None = None) -> InlineKeyboardMarkup:
    selected_set = set(int(item) for item in (selected or []) if isinstance(item, int) or str(item).isdigit())
    rows = []
    for idx, _opt in enumerate(options[:3], start=1):
        mark = "✅" if (idx - 1) in selected_set else "☐"
        rows.append([InlineKeyboardButton(f"{mark} Вариант {idx}", callback_data=f"dictseltoggle:{option_key}:{idx-1}")])
    rows.append([InlineKeyboardButton("✅ Сохранить выбранные", callback_data=f"dictsaveconfirm:{option_key}")])
    rows.append([InlineKeyboardButton("☑️ Выбрать все", callback_data=f"dictselall:{option_key}")])
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
            str(primary.get("value") or lookup.get("word_target") or "—").strip(),
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
        if notes["usage_note"]:
            tip_rows.append(("body", f"Контекст использования: {notes['usage_note']}"))
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
    if original_query:
        _add_option(original_query, default_option.get("target") or "", is_original=True)

    _add_option(default_option.get("source") or "", default_option.get("target") or "", is_original=False)

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
    if direction == "ru-de":
        raw = await run_dictionary_lookup(lookup_input)
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
        return await _coerce_sentence_lookup_payload(result, lookup_input, source_lang, target_lang)
    if direction == "de-ru":
        raw = await run_dictionary_lookup_de(lookup_input)
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
        lookup = await _run_dictionary_lookup_for_pair(lookup_input, source_lang, target_lang)
    except Exception as exc:
        logging.exception(f"❌ Ошибка словарного поиска для '{lookup_input}': {exc}")
        await message.reply_text("Не удалось получить перевод. Попробуйте снова через несколько секунд.")
        return

    if not isinstance(lookup, dict):
        await message.reply_text("Не удалось разобрать ответ словаря. Попробуйте ещё раз.")
        return

    source_text = (lookup.get("word_source") or lookup_input).strip()
    card_key = _store_pending_dictionary_card(
        user_id,
        source_lang,
        target_lang,
        source_text,
        lookup,
        original_query=lookup_input,
    )

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

    option_key = _store_pending_dictionary_save_options(
        user_id=int(user_id),
        card_key=card_key,
        options=options,
        lookup=lookup,
        source_lang=source_lang,
        target_lang=target_lang,
    )

    card_text = _build_dictionary_card_text(source_lang, target_lang, source_text, lookup)
    variants_text = _build_save_variants_text(source_lang, target_lang, options)
    full_text = f"{card_text}\n\n{variants_text}"
    keyboard = _build_save_variant_keyboard(option_key, options, selected=[])
    msg = await message.reply_text(
        full_text,
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
    if not payload:
        await query.answer("Запрос устарел. Отправьте слово ещё раз.", show_alert=True)
        return
    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Выбор доступен только автору запроса.", show_alert=True)
        return

    lookup_input = str(payload.get("text") or "").strip()
    if not lookup_input:
        await query.answer("Запрос пустой. Отправьте слово снова.", show_alert=True)
        return

    await query.answer(f"Перевожу ({source_lang.upper()} -> {target_lang.upper()})")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    await _send_dictionary_lookup_result(
        message=query.message,
        context=context,
        user_id=int(user.id),
        lookup_input=lookup_input,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    pending_dictionary_lookup_requests.pop(request_key, None)


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
    )
    variants_text = _build_save_variants_text(source_lang, target_lang, options)
    keyboard = _build_save_variant_keyboard(option_key, options, selected=[])
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
    keyboard = _build_save_variant_keyboard(option_key, options, selected=payload["selected"])
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
        await query.answer("Варианты устарели. Запросите снова.", show_alert=True)
        return
    user = query.from_user
    if not user or int(payload.get("user_id", 0)) != int(user.id):
        await query.answer("Доступно только автору карточки.", show_alert=True)
        return
    options = payload.get("options") or []
    payload["selected"] = list(range(len(options)))
    pending_dictionary_save_options[option_key] = payload
    keyboard = _build_save_variant_keyboard(option_key, options, selected=payload["selected"])
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

    user_message = f"""

    **Original sentence (Russian):** "{original_text}"
    **User's translation (German):** "{user_translation}"

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
            categories = collected_text.split("Mistake Categories: ")[-1].split("\n")[0].split(", ") if "Mistake Categories:" in collected_text else []
            subcategories = collected_text.split("Subcategories: ")[-1].split("\n")[0].split(", ") if "Subcategories:" in collected_text else []

            #severity = collected_text.split("Severity: ")[-1].split("\n")[0].strip() if "Severity:" in collected_text and len(collected_text.split("Severity: ")[-1].split("\n")) > 0 else None
            
            #correct_translation = collected_text.split("Correct Translation: ")[-1].strip() if "Correct Translation:" in collected_text else None

            match = re.search(r'Correct Translation:\s*(.+?)(?:\n|\Z)', collected_text)
            if match:
                correct_translation = match.group(1).strip()
            
            # ✅ Логируем До обработки
            print(f"🔎 RAW CATEGORIES BEFORE HANDLING in check_translation function (User {user_id_label}): {', '.join(categories)}")
            print(f"🔎 RAW SUBCATEGORIES BEFORE HANDLING in check_translation function (User {user_id_label}): {', '.join(subcategories)}")
            
            # my offer for category: i would reduce all unneccessary symbols not only ** except from words and commas (what do you think!?)
            categories = [re.sub(r"[^0-9a-zA-Z\s,+\-–]", "", cat).strip() for cat in categories if cat.strip()]
            # my offer for subcategory: i would reduce all unneccessary symbols not only ** except from words and commas (what do you think!?)
            subcategories = [re.sub(r"[^0-9a-zA-Z\s,+\-–]", "", subcat).strip() for subcat in subcategories if subcat.strip()]

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

def search_youtube_videous(topic, max_results=5):
    query=topic
    if not YOUTUBE_API_KEY:
        print("❌ Ошибка: YOUTUBE_API_KEY не задан!")
        return []
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        
        # Поиск по приоритетным каналам
        video_data = []
        for channal_id in PREFERRED_CHANNELS:

            request = youtube.search().list(
                part="snippet",
                q=query,
                type="video",
                maxResults=max_results,
                channelId=channal_id
            )
            response = request.execute()

            for item in response.get("items", []):
                title = item["snippet"]["title"]
                #title = title.replace('{', '{{').replace('}', '}}') # Экранирование фигурных скобок
                #title = title.replace('%', '%%') # Экранирование символов % 
                video_id = item["id"].get("videoId", "") # Безопасное извлечение videoId
                #video_url = f"https://www.youtube.com/watch?v={video_id}"
                if video_id:
                    video_data.append({'title': title, 'video_id': video_id})     

        # Если не найдено видео на приоритетных каналах, ищем по всем каналам
        if not video_data:
            print("❌ Видео на приоритетных каналах не найдено — ищем по всем каналам.")
            request = youtube.search().list(
                part="snippet",
                q=query,
                type="video",
                maxResults=max_results,
                relevanceLanguage="de",
                regionCode="DE"
            )
            responce = request.execute()

            for item in responce.get("items", []):
                title = item["snippet"]["title"]
                #title = title.replace('{', '{{').replace('}', '}}') # Экранирование фигурных скобок
                #title = title.replace('%', '%%') # Экранирование символов % 
                video_id = item["id"].get("videoId", "") # Безопасное извлечение videoId
                #video_url = f"https://www.youtube.com/watch?v={video_id}"
                if video_id:
                    video_data.append({'title': title, 'video_id': video_id})
                                  
        if not video_data:
            return ["❌ Видео не найдено. Попробуйте позже."]
        
        # ✅ Теперь получаем количество просмотров для всех найденных видео
        video_ids =  ",".join([video['video_id'] for video in video_data if video['video_id']])
        if video_ids:
            stats_request = youtube.videos().list(
                part = "statistics",
                id=video_ids
            )
            stats_response = stats_request.execute()

            for item in stats_response.get("items", []):
                video_id = item["id"]
                view_count = int(item["statistics"].get("viewCount", 0))
                for video in video_data:
                    if video['video_id'] == video_id:
                        video["views"] = view_count

        # ✅ Подставляем значение по умолчанию (если данных о просмотрах нет)
        for video in video_data:
            video.setdefault("views", 0)

        # ✅ Сортируем по количеству просмотров (по убыванию)
        sorted_videos = sorted(video_data, key=lambda x: x["views"], reverse=True)

        # ✅ Возвращаем только 2 самых популярных видео
        top_videos = sorted_videos[:2]

        # ✅ Формируем ссылки в Telegram-формате
        preferred_videos = [
            f'<a href="{html.escape("https://www.youtube.com/watch?v=" + video["video_id"])}">▶️ {escape_html_with_bold(video["title"])}</a>'
            for video in top_videos
        ]

        print(f"preferred_videos after escape_html_with_bold: {preferred_videos}")
        return preferred_videos
    
    except Exception as e:
        print(f"❌ Ошибка при поиске видео в YouTube: {e}")
        return []


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
                WITH user_mistakes AS (
                    SELECT COUNT(*) AS mistakes_week
                    FROM bt_3_detailed_mistakes
                    WHERE user_id = %s
                    AND added_data >= NOW() - INTERVAL '6 days'
                ),
                top_category AS (
                    SELECT main_category
                    FROM bt_3_detailed_mistakes
                    WHERE user_id = %s
                    AND added_data >= NOW() - INTERVAL '6 days'
                    GROUP BY main_category
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ),
                number_of_topcategory_mist AS (
                    SELECT main_category, COUNT(*) AS number_of_top_category_mistakes
                    FROM bt_3_detailed_mistakes
                    WHERE user_id = %s
                    AND added_data >= NOW() - INTERVAL '6 days'
                    AND main_category = (SELECT main_category FROM top_category)
                    GROUP BY main_category
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ),
                top_two_subcategories AS (
                    SELECT sub_category, 
                        COUNT(*) AS count,
                        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS subcategory_rank
                    FROM bt_3_detailed_mistakes 
                    WHERE user_id = %s
                    AND added_data >= NOW() - INTERVAL '6 days'
                    AND main_category = (SELECT main_category FROM top_category)
                    GROUP BY sub_category
                    ORDER BY COUNT(*) DESC
                    LIMIT 2
                )
                -- ✅ FINAL QUERY WITH LEFT JOIN TO AVOID EMPTY RESULTS
                SELECT 
                    COALESCE((SELECT mistakes_week FROM user_mistakes), 0) AS mistakes_week,
                    COALESCE(ntc.main_category, 'неизвестно') AS top_mistake_category,
                    COALESCE(ntc.number_of_top_category_mistakes, 0) AS number_of_top_category_mistakes,
                    COALESCE(MAX(CASE WHEN tts.subcategory_rank = 1 THEN tts.sub_category END), 'неизвестно') AS top_subcategory_1,
                    COALESCE(MAX(CASE WHEN tts.subcategory_rank = 2 THEN tts.sub_category END), 'неизвестно') AS top_subcategory_2
                FROM number_of_topcategory_mist ntc
                LEFT JOIN top_two_subcategories tts ON TRUE
                GROUP BY ntc.main_category, ntc.number_of_top_category_mistakes;
            """, (user_id, user_id, user_id, user_id))

            # ✅ ОБРАБАТЫВАЕМ СЛУЧАЙ, КОГДА ВОЗВРАЩАЕТСЯ МЕНЬШЕ ДАННЫХ
            result = cursor.fetchone()
            if result is not None:
                # Распаковываем все значения с защитой от отсутствия данных
                mistakes_week, top_mistake_category, number_of_top_category_mistakes, top_mistake_subcategory_1, top_mistake_subcategory_2 = result
            else:
                # Если нет данных — возвращаем пустые значения
                mistakes_week, top_mistake_category, number_of_top_category_mistakes, top_mistake_subcategory_1, top_mistake_subcategory_2 = 0, 'неизвестно', 0, 'неизвестно', 'неизвестно'


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



# 📌📌📌📌📌
async def send_me_analytics_and_recommend_me(context: CallbackContext):
    #client = openai.AsyncOpenAI(api_key=openai.api_key)
    task_name = f"send_me_analytics_and_recommend_me"
    system_instruction_key = f"send_me_analytics_and_recommend_me"

    #get all user_id's from _DB to itterate over them and send them recommendations
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT DISTINCT user_id FROM bt_3_detailed_mistakes;
            """)
            user_ids = cursor.fetchall()
    if not user_ids:
        print("❌ Нет пользователей с ошибками за последнюю неделю.")
        return

    for user_id, in user_ids:
        total_sentences, mistakes_week, top_mistake_category, number_of_top_category_mistakes, top_mistake_subcategory_1, top_mistake_subcategory_2 = await rate_mistakes(user_id)
        if total_sentences:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT DISTINCT username FROM bt_3_translations WHERE user_id = %s;""",
                        (user_id, ))

                    result = cursor.fetchone()
                    username = result[0] if result else "Unknown User"

            # ✅ Запрашиваем тему у OpenAI
            user_message = f"""
            - **Категория ошибки:** {top_mistake_category}  
            - **Первая подкатегория:** {top_mistake_subcategory_1}  
            - **Вторая подкатегория:** {top_mistake_subcategory_2}  
            """

            for attempt in range(5):
                try:
                    topic = await llm_execute(
                        task_name=task_name,
                        system_instruction_key=system_instruction_key,
                        user_message=user_message,
                        poll_interval_seconds=1.0,
                    )

                    # response = await client.chat.completions.create(
                    # model="gpt-4-turbo",
                    # messages=[{"role": "user", "content": prompt}]
                    # )
                    # topic = response.choices[0].message.content.strip()

                    print(f"📌 Определена тема: {topic}")
                    break
                except openai.RateLimitError:
                    wait_time = (attempt + 1 )*5
                    print(f"⚠️ OpenAI API перегружен. Ждём {wait_time} сек...")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    print(f"⚠️ Ошибка OpenAI: {e}")
                    continue
                
            # ✅ Ищем видео на YouTube только по конкретным каналам
            video_data = search_youtube_videous(topic)

            # ✅ Добавляем логирование для диагностики
            if not isinstance(video_data, list):
                print(f"❌ ОШИБКА: search_youtube_videous вернула {type(video_data)} вместо списка!")
            if not video_data:
                print("❌ Видео не найдено. Список пуст.")
            else:
                print(f"✅ Найдено {len(video_data)} видео:")
                for video in video_data:
                    print(f"▶️ {video}")
            
            # ✅ Формируем список ссылок только если элемент является словарём
            # ✅ Нет необходимости преобразовывать снова — список уже готов
            valid_links = video_data

            
            if not valid_links:
                valid_links = ["❌ Не удалось найти видео на YouTube по этой теме. Попробуйте позже."]

            rounded_value = round(mistakes_week/total_sentences, 2)
            # ✅ Формируем сообщение для пользователя
            recommendations = (
                f"🧔 *{username}*,\nВы *перевели* за неделю: {total_sentences} предложений;\n"
                f"📌 *В них допущено* {mistakes_week} ошибок;\n"
                f"🚨 *Количество ошибок на одно предложение:* {rounded_value} штук;\n"
                f"🔴 *Больше всего ошибок:* {number_of_top_category_mistakes} штук в категории:\n {top_mistake_category or 'неизвестно'}\n"
            )
            if top_mistake_subcategory_1:
                recommendations += (f"📜 *Основные ошибки в подкатегории:*\n {top_mistake_subcategory_1}\n\n")
            if top_mistake_subcategory_2:
                recommendations += (f"📜 *Вторые по частоте ошибки в подкатегории:*\n {top_mistake_subcategory_2}\n\n")
            
            # ✅ Добавляем строку с рекомендацией → ЭТО ВАЖНО!
            recommendations += (f"🟢 *Рекомендую посмотреть:*\n\n")
            recommendations = escape_html_with_bold(recommendations)


            # ✅ Добавляем рабочие ссылки
            recommendations += "\n\n".join(valid_links)
            
            #Debugging...
            print("DEBUG: ", recommendations)


            # ✅ Отправляем сообщение пользователю
            await context.bot.send_message(
                chat_id=BOT_GROUP_CHAT_ID_Deutsch, 
                text=recommendations,
                parse_mode = "HTML"
                )
            await asyncio.sleep(5)

        else:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT DISTINCT username FROM bt_3_translations WHERE user_id = %s;
                    """, (user_id, ))
                    result = cursor.fetchone()
                    username = result[0] if result else f"User {user_id}"
            
            await context.bot.send_message(
                chat_id=BOT_GROUP_CHAT_ID_Deutsch,
                text=escape_html_with_bold(f"⚠️ Пользователь {username} не перевёл ни одного предложения на этой неделе."),
                parse_mode="HTML"
            )


async def force_finalize_sessions(context: CallbackContext = None):
    """Завершает ВСЕ незавершённые сессии только за сегодняшний день в 23:59."""
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

    msg = await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text="🔔 Все незавершённые сессии за сегодня автоматически закрыты!")
    #add_service_msg_id(context, msg.message_id)



#SQL Запрос проверено
async def send_weekly_summary(context: CallbackContext):

    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔹 Собираем статистику за неделю
    cursor.execute("""
        SELECT 
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
            AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS avg_time, -- ✅ Среднее время сессии
            SUM(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS total_time -- ✅ Общее время
        FROM bt_3_user_progress 
        WHERE completed = TRUE 
        AND start_time >= CURRENT_DATE - INTERVAL '6 days'
        GROUP BY user_id
    ) p ON t.user_id = p.user_id
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '6 days'
    GROUP BY t.username, t.user_id, p.avg_time, p.total_time
    ORDER BY итоговый_балл DESC;

    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    if not rows:
        await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text="📊 Неделя прошла, но никто не перевел ни одного предложения!")
        return

    summary = "🏆 Итоги недели:\n\n"

    medals = ["🥇", "🥈", "🥉"]
    for i, (username, count, avg_score, avg_minutes, total_minutes, missed, final_score) in enumerate(rows):
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

    await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text=summary)



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
    cursor.execute("""
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
                AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS avg_time, 
                SUM(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS total_time
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

    # 🔹 Формируем итоговый отчёт
    if not rows:
        await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text="📊 Сегодня никто не перевёл ни одного предложения!")
        return

    summary = "📊 Итоги дня:\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (user_id, total_sentences, translated, missed, avg_minutes, total_time_minutes, avg_score, final_score) in enumerate(rows):
        username = all_users.get(int(user_id), 'Неизвестный пользователь')  # ✅ Берём имя пользователя из словаря
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


    # 🚨 **Добавляем блок про ленивых**
    lazy_users = {uid: uname for uid, uname in all_users.items() if uid not in active_users}
    if lazy_users:
        summary += "\n🦥 Ленивцы (писали в чат, но не переводили):\n"
        for username in lazy_users.values():
            summary += f"👤 {username}: ничего не перевёл!\n"

    await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text=summary)



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
    cursor.execute("""
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
            AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS avg_time, -- ✅ Среднее время сессии за день
            SUM(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS total_time -- ✅ Общее время за день
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

    # 🔹 Формируем отчёт
    if not rows:
        await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text="📊 Сегодня никто не перевёл ни одного предложения!")
        return
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    progress_report = f"📊 Промежуточные итоги перевода:\n🕒 Время отчёта:\n{current_time}\n\n"

    for user_id, total, translated, missed, avg_minutes, total_minutes, avg_score, final_score in rows:
        progress_report += (
            f"👤 {all_users.get(int(user_id), 'Неизвестный пользователь')}\n"
            f"📜 Переведено: {translated}/{total}\n"
            f"🚨 Не переведено: {missed}\n"
            f"⏱ Время среднее: {avg_minutes:.1f} мин\n"
            f"⏱ Время общ.: {total_minutes:.1f} мин\n"
            f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
            f"🏆 Итоговый балл: {final_score:.1f}\n\n"
        )

    # 🚨 **Добавляем блок про ленивых (учитываем всех, кто писал в чат за месяц)**
    lazy_users = {uid: uname for uid, uname in all_users.items() if uid not in active_users}
    if lazy_users:
        progress_report += "\n🦥 Ленивцы (писали в чат, но не переводили):\n"
        for username in lazy_users.values():
            progress_report += f"👤 {username}: ничего не перевёл!\n"

    await context.bot.send_message(chat_id=BOT_GROUP_CHAT_ID_Deutsch, text=progress_report)


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
    #global GOOGLE_CREDS_FILE_PATH
    key_path = prepare_google_creds_file()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    client = texttospeech.TextToSpeechClient()
    german_voice = "de-DE-Neural2-C"

    audio_segments = []

    def synthesize(text, language_code, voice_name):
        input_data = texttospeech.SynthesisInput(text = text)

        voice = texttospeech.VoiceSelectionParams(
            language_code = language_code, name=voice_name
        )

        config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            speaking_rate=0.9 # 90% скорости
        )

        response = client.synthesize_speech(
            input=input_data, voice=voice, audio_config=config 
        )

        return AudioSegment.from_file_using_temporary_files(io.BytesIO(response.audio_content))

    async def split_german_sentence(sentence: str) -> list[str]:
        if not sentence:
            return []
        system_instruction_key = "tts_chunk_de"
        task_name = "tts_chunk_de"

        try:
            text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=sentence,
                poll_interval_seconds=1.0,
            )
        except Exception:
            text = ""

        try:
            chunks = json.loads(text)
        except Exception:
            chunks = []
        chunks = [str(c).strip() for c in chunks if str(c).strip()]
        if not chunks:
            # fallback: split by commas / semicolons
            chunks = [c.strip() for c in re.split(r"[;,]", sentence) if c.strip()]
        return chunks or [sentence]

    pause_short = AudioSegment.silent(duration=500)
    pause_long = AudioSegment.silent(duration=900)
    pause_between_sentences = AudioSegment.silent(duration=1700)

    for russian, german in sentence_pairs:
        print(f"🎤 Синтезируем: {russian} -> {german}")
        # Русский (один раз)
        ru_audio = synthesize(russian, "ru-RU", "ru-RU-Wavenet-C")
        # Немецкий: строим по кускам
        chunks = await split_german_sentence(german)
        chunk_segments = []
        for idx, chunk in enumerate(chunks):
            # повторяем кусок
            chunk_audio = synthesize(chunk, "de-DE", german_voice)
            chunk_segments.extend([chunk_audio, pause_short, chunk_audio, pause_short])

            if idx > 0:
                combined_text = " ".join(chunks[: idx + 1])
                combined_audio = synthesize(combined_text, "de-DE", german_voice)
                chunk_segments.extend([combined_audio, pause_long])

        # финальная фраза полностью
        full_audio = synthesize(german, "de-DE", german_voice)

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

                if audio_path.exists():
                    try:
                        start = asyncio.get_running_loop().time()
                        with audio_path.open("rb") as audio_file:
                            await context.bot.send_audio(
                                chat_id=BOT_GROUP_CHAT_ID_Deutsch, 
                                audio=audio_file,
                                caption=f"🎧 Ошибки пользователя @{username} за вчерашний день."
                            )
                        print(f"⏱ Отправка заняла {asyncio.get_running_loop().time() - start:.2f} секунд")
                        await asyncio.sleep(5)
                    except Exception as e:
                        print(f"❌ Ошибка при отправке аудиофайла для @{username}: {e}")

                    try:    
                        audio_path.unlink()
                    except FileNotFoundError:
                        print(f"⚠️ Файл уже был удалён: {audio_path}")
                
                else:
                    await context.bot.send_message(
                        chat_id=BOT_GROUP_CHAT_ID_Deutsch,
                        text=f"❌ Для пользователя @{username} не найден аудиофайл."
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
    chat_id = BOT_GROUP_CHAT_ID_Deutsch

    start_date, end_date = get_date_range(period)

    # Send one message before starting the process
    await context.bot.send_message(chat_id=chat_id, text="🚀 Starting to prepare analytical reports for all active users...")

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
            await context.bot.send_message(chat_id=chat_id, text="No active users found for analysis today.")
            return

        for user_id, username in all_users:
            try:
                # IMPORTANT: Make sure this function accepts user_id and uses it
                full_user_data = await prepare_aggregate_data_by_period_and_draw_analytic_for_user(user_id, start_date, end_date)

                if not full_user_data.empty:
                    daily_data = await aggregate_data_for_charts(full_user_data, period="day")
                    weekly_data = await aggregate_data_for_charts(full_user_data, period="week")

                    print(f"Data for {username} prepared. Drawing plots...")
                    image_path = await create_analytics_figure_async(daily_data, weekly_data, user_id)
                    
                    # FIXED: send_photo method name
                    await context.bot.send_photo(chat_id=chat_id, photo=open(image_path, 'rb'), caption=f"📊 Analytics for user: {username}")
                    os.remove(image_path)
                else:
                    print(f"⚠️ No data found for analysis for user {username} ({user_id}).")

            except Exception as e:
                logging.error(f"Error creating individual report for {username} ({user_id}): {e}")
                # Report the error, but continue the loop for other users
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to create a report for {username}.")

    except Exception as e:
        logging.error(f"Critical error during send_user_analytics_bar_charts execution: {e}")
        # FIXED: added await
        await context.bot.send_message(chat_id=chat_id, text="❌ A general error occurred while creating reports.")


async def send_users_comparison_bar_chart(context: CallbackContext, period):
    chat_id = BOT_GROUP_CHAT_ID_Deutsch

    start_date, end_date = get_date_range(period)
    # relativedelta(months=3) создаёт объект, который представляет собой интервал в "3 календарных месяца".
    # Когда вы вычитаете этот объект из даты, он корректно отсчитывает месяцы назад.

    await context.bot.send_message(chat_id=chat_id, text="Starting preparation of Comparison analytics for all users..." )

    try:
        image_path = await create_comparison_report_async(period=period, start_date=start_date, end_date=end_date)
        if image_path:
            await context.bot.send_photo(chat_id=chat_id, photo=open(image_path, "rb"), caption=f"Users Comparison Analytics for the last {period}")
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

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            await context.bot.send_message(
                chat_id=int(user_id),
                text="\n".join(lines),
            )
            return True
        except RetryAfter as exc:
            delay = max(1, int(getattr(exc, "retry_after", 1)))
            logging.warning("⚠️ RetryAfter for quiz private result user_id=%s, sleep=%ss", user_id, delay)
            await asyncio.sleep(delay)
        except TimedOut:
            if attempt + 1 < max_attempts:
                await asyncio.sleep(1.0)
                continue
            logging.warning("⚠️ Timeout sending quiz private result user_id=%s", user_id)
        except Exception as exc:
            logging.warning(f"⚠️ Не удалось отправить результат квиза в личку user_id={user_id}: {exc}")
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

    correct_text = options[correct_option_id]
    indexed = list(enumerate(options))
    random.shuffle(indexed)
    shuffled_options = [item[1] for item in indexed]
    new_correct_id = shuffled_options.index(correct_text)

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


def _extract_german_word(entry: dict) -> str | None:
    response_json = _coerce_response_json(entry.get("response_json"))
    candidate = (
        (entry.get("translation_de") or "").strip()
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


def _build_same_word_anagram_distractors(correct_word: str, count: int = 3) -> list[str]:
    base = _to_letters_only_word(correct_word)
    if len(base) < 4:
        return []

    first = base[0]
    last = base[-1]
    middle = list(base[1:-1])
    original = base.lower()
    variants: list[str] = []
    seen: set[str] = set()

    # Repeated letters can drastically reduce unique permutations, so use bounded retries.
    for _ in range(120):
        shuffled = middle[:]
        random.shuffle(shuffled)
        candidate = f"{first}{''.join(shuffled)}{last}"
        lower_candidate = candidate.lower()
        if lower_candidate == original or lower_candidate in seen:
            continue
        seen.add(lower_candidate)
        variants.append(candidate)
        if len(variants) >= count:
            break

    return variants


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
    if not word_ru:
        return None

    correct_word = _extract_german_word(entry)
    if not correct_word:
        return None

    correct_word = _to_letters_only_word(correct_word)
    if len(correct_word) < 4:
        return None

    distractors = _build_same_word_anagram_distractors(correct_word, count=3)
    if len(distractors) < 3:
        fallback_distractors = _pick_anagram_distractors(correct_word, count=3 - len(distractors))
        for candidate in fallback_distractors:
            cleaned = _to_letters_only_word(candidate)
            if not cleaned:
                continue
            if cleaned.lower() == correct_word.lower():
                continue
            if cleaned in distractors:
                continue
            distractors.append(cleaned)
            if len(distractors) >= 3:
                break

    if len(distractors) < 2:
        return None

    options_raw = [correct_word] + distractors
    options_raw = list(dict.fromkeys(options_raw))[:4]
    if len(options_raw) < 2:
        return None
    random.shuffle(options_raw)
    correct_option_id = options_raw.index(correct_word)
    options = [_format_anagram_option(item) for item in options_raw]

    question = (
        f"Выберите правильное немецкое слово для «{word_ru}».\n"
        "Во всех вариантах первая и последняя буквы совпадают."
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

    selected_example = random.choice(usage_examples)
    sentence = str(selected_example.get("sentence") or "").strip()
    hint_text = str(selected_example.get("hint") or "").strip()
    if not hint_text:
        hint_text = str(
            response_json.get("translation_ru")
            or entry.get("translation_ru")
            or entry.get("word_ru")
            or response_json.get("word_ru")
            or ""
        ).strip()
    options = _build_word_order_options(sentence)
    if len(options) < 2:
        return None
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


def _extract_prefix_variants(response_json: dict) -> list[str]:
    prefixes = response_json.get("prefixes") if response_json else []
    variants = []
    if isinstance(prefixes, list):
        for item in prefixes:
            if not isinstance(item, dict):
                continue
            variant = (item.get("variant") or "").strip()
            if variant:
                variants.append(variant)
    return variants


def _build_prefix_distractors(correct_word: str, count: int = 3) -> list[str]:
    prefixes = ["ab", "an", "auf", "aus", "bei", "ein", "mit", "nach", "vor", "zu", "um", "ver", "be", "ent"]
    lower = (correct_word or "").lower()
    if not lower:
        return []

    base = lower
    for pref in sorted(prefixes, key=len, reverse=True):
        if lower.startswith(pref) and len(lower) > len(pref) + 2:
            base = lower[len(pref):]
            break

    variants: list[str] = []
    for pref in prefixes:
        candidate = f"{pref}{base}"
        if candidate == lower:
            continue
        if candidate not in variants and len(candidate) >= 4:
            variants.append(candidate)
        if len(variants) >= count:
            break
    return variants


async def generate_prefix_quiz(entry: dict) -> dict | None:
    word_ru = (entry.get("word_ru") or "").strip()
    response_json = _coerce_response_json(entry.get("response_json"))
    if not word_ru or not response_json:
        return None

    correct_word = _extract_german_word({
        "translation_de": entry.get("translation_de"),
        "word_de": entry.get("word_de"),
        "response_json": response_json,
    })
    if not correct_word:
        return None

    variants = _extract_prefix_variants(response_json)
    if not variants:
        variants = _build_prefix_distractors(correct_word)
    if not variants:
        return None

    options = [correct_word]
    for variant in variants:
        if variant.lower() == correct_word.lower():
            continue
        options.append(variant)
        if len(options) >= 4:
            break

    options = list(dict.fromkeys([opt for opt in options if opt]))
    if len(options) < 2:
        return None

    random.shuffle(options)
    correct_option_id = options.index(correct_word)
    question = (
        f"Выберите правильный немецкий глагол с приставкой для «{word_ru}»."
    )
    return {
        "question": question,
        "options": options,
        "correct_option_id": correct_option_id,
        "quiz_type": "prefix",
        "word_ru": word_ru,
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


async def delete_temporary_message(context: CallbackContext) -> None:
    chat_id = context.job.data.get("chat_id")
    message_id = context.job.data.get("message_id")
    if chat_id and message_id:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)


async def _is_user_member_of_quiz_group(context: CallbackContext, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(
            chat_id=BOT_GROUP_CHAT_ID_Deutsch,
            user_id=int(user_id),
        )
        status = str(getattr(member, "status", "") or "").strip().lower()
        return status in {"creator", "administrator", "member", "restricted"}
    except Exception:
        return False


async def _collect_quiz_delivery_targets(context: CallbackContext) -> list[int]:
    targets: list[int] = [int(BOT_GROUP_CHAT_ID_Deutsch)]
    try:
        allowed_rows = await asyncio.to_thread(list_allowed_telegram_users, 500)
    except Exception:
        logging.warning("⚠️ Не удалось получить список allowed users для quiz fallback", exc_info=True)
        allowed_rows = []

    candidate_user_ids: set[int] = set()
    for row in allowed_rows:
        try:
            user_id = int((row or {}).get("user_id") or 0)
        except Exception:
            user_id = 0
        if user_id > 0:
            candidate_user_ids.add(user_id)
    candidate_user_ids.update({int(user_id) for user_id in get_admin_telegram_ids() if int(user_id) > 0})

    for user_id in sorted(candidate_user_ids):
        if await _is_user_member_of_quiz_group(context, user_id):
            continue
        targets.append(int(user_id))
    return targets


async def send_scheduled_quiz(context: CallbackContext) -> None:
    generator_order = [
        ("word_order", generate_word_order_quiz),
        ("prefix", generate_prefix_quiz),
        ("anagram", generate_anagram_quiz),
        ("word", generate_word_quiz),
    ]
    rotation_idx = int(context.application.bot_data.get("quiz_rotation_idx", 0)) % len(generator_order)
    context.application.bot_data["quiz_rotation_idx"] = rotation_idx + 1

    ordered = generator_order[rotation_idx:] + generator_order[:rotation_idx]
    quiz = None
    chosen_entry = None
    max_entries_per_generator = 4
    for quiz_type, generator in ordered:
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
                logging.warning(f"⚠️ Генератор квиза '{quiz_type}' упал: {exc}")
                quiz = None
            if quiz:
                if not quiz.get("quiz_type"):
                    quiz["quiz_type"] = quiz_type
                if not quiz.get("word_ru"):
                    quiz["word_ru"] = entry.get("word_ru")
                chosen_entry = entry
                break
        if quiz:
            break
        logging.info(f"ℹ️ Генератор квиза '{quiz_type}' вернул пустой результат, пробуем следующий.")

    if not quiz:
        logging.warning("⚠️ Не удалось сгенерировать квиз.")
        return

    shuffled_quiz = _shuffle_quiz_options(quiz)
    if shuffled_quiz:
        quiz = shuffled_quiz

    delivery_targets = await _collect_quiz_delivery_targets(context)
    sent_count = 0
    for target_chat_id in delivery_targets:
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
            "quiz_type": quiz.get("quiz_type", "generated"),
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
                quiz_type=(quiz.get("quiz_type") or "generated"),
                word_ru=(quiz.get("word_ru") or ""),
            )
        except Exception:
            logging.warning("⚠️ Не удалось сохранить активный квиз в БД", exc_info=True)

        context.job_queue.run_once(
            cleanup_quiz_cache,
            when=QUIZ_CACHE_TTL_SECONDS,
            data={"poll_id": poll_message.poll.id},
        )

    if sent_count > 0:
        record_quiz_word((chosen_entry or {}).get("word_ru"))
        logging.info(
            "✅ Quiz sent to %s chats: type=%s options=%s correct_option_id=%s word_ru=%s",
            sent_count,
            quiz.get("quiz_type", "generated"),
            len(quiz.get("options", [])),
            quiz.get("correct_option_id"),
            (chosen_entry or {}).get("word_ru"),
        )
    else:
        logging.warning("⚠️ Квиз сгенерирован, но не был отправлен ни в один чат.")


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    poll_answer = update.poll_answer
    quiz_data = active_quizzes.get(poll_answer.poll_id)
    if not quiz_data:
        try:
            quiz_data = await asyncio.to_thread(get_active_quiz, str(poll_answer.poll_id))
            if quiz_data:
                active_quizzes[poll_answer.poll_id] = dict(quiz_data)
        except Exception:
            logging.warning("⚠️ Не удалось получить active quiz из БД", exc_info=True)
    if not quiz_data or not poll_answer.option_ids:
        return

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
    sent_private = await _send_quiz_result_private(
        context=context,
        user_id=poll_answer.user.id,
        quiz_data=quiz_data,
        is_correct=is_correct,
        selected_text=selected_text,
    )
    if not sent_private:
        await context.bot.send_message(
            chat_id=quiz_data["chat_id"],
            text=f"{poll_answer.user.first_name}, откройте личку с ботом (/start), чтобы получать результаты квизов приватно.",
            reply_to_message_id=quiz_data["message_id"],
        )



def main():
    global application
    
    # Инициализация базы данных from database.py 
    init_db()
    ensure_webapp_tables()

    #defaults = Defaults(timeout=60)  # увеличили таймаут до 60 секунд
    tracking_bot = TrackingExtBot(token=TELEGRAM_Deutsch_BOT_TOKEN)
    application = Application.builder().bot(tracking_bot).build()
    application.bot.request.timeout = 60

    # 🔹 Добавляем обработчики команд (исправленный порядок)
    application.add_handler(TypeHandler(Update, enforce_user_access, block=True), group=-2)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("request_access", request_access))
    application.add_handler(CommandHandler("allow", allow_user_command))
    application.add_handler(CommandHandler("deny", deny_user_command))
    application.add_handler(CommandHandler("allowed", allowed_users_command))
    application.add_handler(CommandHandler("pending", pending_requests_command))
    application.add_handler(CommandHandler("mobile_token", mobile_token_command))
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
    application.add_handler(CallbackQueryHandler(handle_flashcard_feel_feedback_callback, pattern=r"^feelfb:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_pair_callback, pattern=r"^dictpair:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_select_toggle_callback, pattern=r"^dictseltoggle:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_select_all_callback, pattern=r"^dictselall:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_confirm_callback, pattern=r"^dictsaveconfirm:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_option_callback, pattern=r"^dictsaveopt:"))
    application.add_handler(CallbackQueryHandler(handle_dictionary_save_callback, pattern=r"^dictsave:"))

    application.add_handler(CommandHandler("translate", check_user_translation))  # ✅ Проверка переводов
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, check_translation_from_text, block=False), group=1)  # ✅ Проверяем переводы


    application.add_handler(CallbackQueryHandler(topic_selected)) #Он ждет любые нажатия на inline-кнопки.
    application.add_handler(MessageHandler(filters.TEXT, log_all_messages, block=False), group=2)  # 👈 Добавляем в main()
    application.add_handler(PollAnswerHandler(handle_poll_answer))

    application.add_error_handler(error_handler)
    
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


    scheduler = BackgroundScheduler()

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
    print("📌 Добавляем задачу в scheduler...")
    scheduler.add_job(lambda: submit_async(send_morning_reminder,CallbackContext(application=application)),"cron", hour=5, minute=5)
    scheduler.add_job(lambda: submit_async(send_morning_reminder,CallbackContext(application=application)),"cron", hour=15, minute=30)
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
    
    scheduler.add_job(lambda: submit_async(force_finalize_sessions, CallbackContext(application=application)), "cron", hour=21, minute=59)
    
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
    scheduler.add_job(
        lambda: submit_async(cleanup_system_messages, CallbackContext(application=application)),
        "cron",
        hour=SYSTEM_MESSAGE_CLEANUP_HOUR,
        minute=SYSTEM_MESSAGE_CLEANUP_MINUTE,
    )
    
    scheduler.start()
    print("🚀 Бот запущен! Ожидаем сообщения...")
    application.run_polling()





if __name__ == "__main__":
    main()
