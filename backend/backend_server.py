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


import os
import hmac
import hashlib
import json
import asyncio
import logging
import requests
import tempfile
from datetime import datetime
from io import BytesIO
from uuid import uuid4
from urllib.parse import parse_qsl
from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from livekit.api import AccessToken, VideoGrants
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
try:
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None
from backend.utils import prepare_google_creds_for_tts

from backend.openai_manager import (
    run_check_translation,
    run_dictionary_lookup,
    run_dictionary_lookup_de,
    run_dictionary_collocations,
    run_translation_explanation,
    run_feel_word,
    run_enrich_word,
)
from backend.database import (
    ensure_webapp_tables,
    get_pending_daily_sentences,
    get_webapp_dictionary_entries,
    get_webapp_translation_history,
    get_latest_daily_sentences,
    save_webapp_dictionary_query,
    save_webapp_translation,
    get_dictionary_cache,
    upsert_dictionary_cache,
    record_flashcard_answer,
    get_flashcard_set,
    create_dictionary_folder,
    get_dictionary_folders,
    update_webapp_dictionary_entry,
    get_dictionary_entry_by_id,
)
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
TELEGRAM_Deutsch_BOT_TOKEN = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
TELEGRAM_GROUP_CHAT_ID = os.getenv("TELEGRAM_GROUP_CHAT_ID") or os.getenv("BOT_GROUP_CHAT_ID_Deutsch")

if not LIVEKIT_API_KEY or not LIVEKIT_API_SECRET:
    raise RuntimeError("LIVEKIT_API_KEY и LIVEKIT_API_SECRET должны быть установлены")

if not TELEGRAM_Deutsch_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_Deutsch_BOT_TOKEN должен быть установлен")

ensure_webapp_tables()

# === Путь к собранному фронту (frontend/dist) ===
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"


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


def _send_group_message(text: str) -> None:
    if not TELEGRAM_GROUP_CHAT_ID:
        raise RuntimeError("TELEGRAM_GROUP_CHAT_ID должен быть установлен")
    url = f"https://api.telegram.org/bot{TELEGRAM_Deutsch_BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_GROUP_CHAT_ID,
            "text": text,
        },
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")


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


def _fetch_youtube_transcript(video_id: str) -> dict:
    def _parse_vtt(path: Path) -> list[dict]:
        items = []
        if not path.exists():
            return items
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = [line.strip() for line in text.splitlines()]
        buffer = []
        start = None
        for line in lines:
            if "-->" in line:
                if buffer and start is not None:
                    items.append({"start": start, "text": " ".join(buffer).strip()})
                    buffer = []
                try:
                    ts = line.split("-->")[0].strip()
                    parts = ts.split(":")
                    seconds = float(parts[-1].replace(",", "."))
                    minutes = int(parts[-2]) if len(parts) >= 2 else 0
                    hours = int(parts[-3]) if len(parts) >= 3 else 0
                    start = hours * 3600 + minutes * 60 + seconds
                except Exception:
                    start = None
                continue
            if not line:
                if buffer and start is not None:
                    items.append({"start": start, "text": " ".join(buffer).strip()})
                buffer = []
                start = None
                continue
            if line.lower() == "webvtt":
                continue
            if line.isdigit():
                continue
            buffer.append(line)
        if buffer and start is not None:
            items.append({"start": start, "text": " ".join(buffer).strip()})
        return items

    api_error = None
    try:
        try:
            from youtube_transcript_api import (
                YouTubeTranscriptApi,
                TranscriptsDisabled,
                NoTranscriptFound,
                VideoUnavailable,
            )
        except Exception as exc:
            raise RuntimeError("youtube_transcript_api не установлен") from exc
        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
        preferred = None
        for lang in ("de", "en", "ru"):
            try:
                preferred = transcripts.find_transcript([lang])
                break
            except Exception:
                continue
        if preferred is None:
            preferred = transcripts.find_transcript([t.language_code for t in transcripts])
        transcript = preferred.fetch()
        return {
            "items": transcript,
            "language": preferred.language_code,
            "is_generated": preferred.is_generated,
        }
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as exc:
        api_error = f"Субтитры недоступны: {exc}"
    except Exception as exc:
        api_error = f"Не удалось загрузить субтитры: {exc}"

    try:
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise RuntimeError("yt-dlp не установлен") from exc
        with tempfile.TemporaryDirectory() as tmpdir:
            template = str(Path(tmpdir) / "%(id)s.%(ext)s")
            ydl_opts = {
                "skip_download": True,
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": ["de", "en", "ru"],
                "subtitlesformat": "vtt",
                "outtmpl": template,
                "quiet": True,
                "no_warnings": True,
            }
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                subtitles = info.get("requested_subtitles") or {}
                language_order = ["de", "en", "ru"]
                candidate = None
                for lang in language_order:
                    if lang in subtitles and subtitles[lang].get("ext") == "vtt":
                        candidate = lang
                        break
                if candidate is None and subtitles:
                    candidate = next(iter(subtitles.keys()))
                vtt_files = list(Path(tmpdir).glob("*.vtt"))
                if not vtt_files and candidate:
                    vtt_files = list(Path(tmpdir).glob(f"*{candidate}*.vtt"))
                if not vtt_files:
                    raise RuntimeError("yt-dlp не вернул .vtt файл")
                vtt_files.sort(key=lambda p: p.stat().st_size, reverse=True)
                items = _parse_vtt(vtt_files[0])
                if not items:
                    raise RuntimeError("yt-dlp вернул пустые субтитры")
                return {
                    "items": items,
                    "language": candidate,
                    "is_generated": True,
                }
    except Exception as exc:
        fallback_error = f"Fallback yt-dlp: {exc}"
        if api_error:
            raise RuntimeError(f"{api_error}; {fallback_error}") from exc
        raise RuntimeError(fallback_error) from exc


@app.errorhandler(Exception)
def handle_unexpected_error(error):
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
    username = user_data.get("username")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    if translations:
        try:
            results = asyncio.run(check_user_translation_webapp(user_id, username, translations))
        except Exception as exc:
            return jsonify({"error": f"Ошибка обработки запроса: {exc}"}), 500
        return jsonify({"ok": True, "results": results})

    try:
        result = asyncio.run(run_check_translation(original_text, user_translation))
    except Exception as exc:
        return jsonify({"error": f"Ошибка обработки запроса: {exc}"}), 500

    save_webapp_translation(
        user_id=user_id,
        username=username,
        session_id=session_id,
        original_text=original_text,
        user_translation=user_translation,
        result=result,
    )

    return jsonify({"ok": True, "result": result})


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

    history = get_daily_translation_history(user_id=user_id, limit=int(limit))
    return jsonify({"ok": True, "items": history})


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

    try:
        period = _normalize_period(period)
        if period == "all":
            bounds = get_all_time_bounds(user_id)
        else:
            bounds = get_period_bounds(period)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    summary = fetch_user_summary(user_id, bounds.start_date, bounds.end_date)
    return jsonify(
        {
            "ok": True,
            "period": {
                "period": period,
                "start_date": bounds.start_date.isoformat(),
                "end_date": bounds.end_date.isoformat(),
            },
            "summary": summary,
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

    points = fetch_user_timeseries(user_id, start_date, end_date, granularity)
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

    leaderboard = fetch_comparison_leaderboard(start_date, end_date, limit=limit)
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
        }
    )

@app.route("/api/webapp/dictionary", methods=["POST"])
def lookup_webapp_dictionary():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    word_ru = (payload.get("word") or "").strip()

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

    try:
        is_ru = any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in word_ru)
        if is_ru:
            cached = get_dictionary_cache(word_ru)
            if cached:
                return jsonify({"ok": True, "item": cached, "direction": "ru-de"})
            result = asyncio.run(run_dictionary_lookup(word_ru))
        else:
            result = asyncio.run(run_dictionary_lookup_de(word_ru))
    except Exception as exc:
        return jsonify({"error": f"Ошибка запроса словаря: {exc}"}), 500

    if result and is_ru:
        upsert_dictionary_cache(word_ru, result)

    return jsonify({"ok": True, "item": result, "direction": "ru-de" if is_ru else "de-ru"})


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

    try:
        if not direction:
            is_ru = any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in word)
            direction = "ru-de" if is_ru else "de-ru"
        result = asyncio.run(run_dictionary_collocations(direction, word, translation))
    except Exception as exc:
        return jsonify({"error": f"Ошибка генерации связок: {exc}"}), 500

    return jsonify({"ok": True, "items": result.get("items", []), "direction": direction})


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

    try:
        items = get_webapp_dictionary_entries(
            user_id=user_id,
            limit=500,
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения словаря: {exc}"}), 500

    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    # Use Unicode-capable font for Cyrillic/Umlauts
    font_name = "DejaVuSans"
    font_paths = [
        os.path.join(os.path.dirname(__file__), "assets", "fonts", "DejaVuSans.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
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
    width, height = A4
    x = 40
    y = height - 40
    line_height = 14

    pdf.setFont(font_name, 14)
    title = "Словарь"
    pdf.drawString(x, y, title)
    y -= 2 * line_height

    pdf.setFont(font_name, 11)
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
            f"Слово: {word}",
            f"Перевод: {translation}",
            f"Дата: {created_at}",
        ]
        if examples:
            lines.append("Примеры:")
            for example in examples[:3]:
                lines.extend([f"- {line}" for line in _wrap_text(example, 90)])

        for line in lines:
            if y < 60:
                pdf.showPage()
                pdf.setFont("Helvetica", 11)
                y = height - 40
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
    response_json = payload.get("response_json") or {}
    folder_id = payload.get("folder_id")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not word_ru and not word_de:
        return jsonify({"error": "word_ru или word_de обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        resolved_word_ru = word_ru or response_json.get("word_ru")
        resolved_word_de = word_de or response_json.get("word_de")
        save_webapp_dictionary_query(
            user_id=user_id,
            word_ru=resolved_word_ru if resolved_word_ru else None,
            translation_de=translation_de or response_json.get("translation_de"),
            word_de=resolved_word_de if resolved_word_de else None,
            translation_ru=translation_ru or response_json.get("translation_ru"),
            response_json=response_json,
            folder_id=int(folder_id) if folder_id is not None else None,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения словаря: {exc}"}), 500

    return jsonify({"ok": True})


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

    try:
        items = get_webapp_dictionary_entries(
            user_id=user_id,
            limit=int(limit),
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения словаря: {exc}"}), 500

    return jsonify({"ok": True, "items": items})


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

    try:
        items = get_flashcard_set(
            user_id=user_id,
            set_size=set_size,
            wrong_size=wrong_size,
            folder_mode=folder_mode,
            folder_id=int(folder_id) if folder_id is not None else None,
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка получения карточек: {exc}"}), 500

    return jsonify({"ok": True, "items": items})


@app.route("/api/webapp/flashcards/answer", methods=["POST"])
def record_webapp_flashcard_answer():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    entry_id = payload.get("entry_id")
    is_correct = payload.get("is_correct")

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if entry_id is None or is_correct is None:
        return jsonify({"error": "entry_id и is_correct обязательны"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    parsed = _parse_telegram_init_data(init_data)
    user_data = parsed.get("user") or {}
    user_id = user_data.get("id")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        record_flashcard_answer(user_id=user_id, entry_id=int(entry_id), is_correct=bool(is_correct))
    except Exception as exc:
        return jsonify({"error": f"Ошибка сохранения ответа: {exc}"}), 500

    return jsonify({"ok": True})


@app.route("/api/webapp/flashcards/feel", methods=["POST"])
def get_flashcard_feel():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    entry_id = payload.get("entry_id")
    word_ru = (payload.get("word_ru") or "").strip()
    word_de = (payload.get("word_de") or "").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not entry_id:
        return jsonify({"error": "entry_id обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    response_json = None
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
        return jsonify({"error": "word_ru обязателен"}), 400

    try:
        feel_text = asyncio.run(run_feel_word(word_ru, word_de))
    except Exception as exc:
        return jsonify({"error": f"Ошибка feel: {exc}"}), 500

    if not response_json:
        response_json = {}
    response_json["feel_explanation"] = feel_text
    try:
        update_webapp_dictionary_entry(int(entry_id), response_json)
    except Exception:
        pass

    return jsonify({"ok": True, "feel_explanation": feel_text})


@app.route("/api/webapp/tts", methods=["POST"])
def webapp_tts():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    text = (payload.get("text") or "").strip()
    language = (payload.get("language") or "de-DE").strip()
    voice = (payload.get("voice") or "de-DE-Wavenet-B").strip()

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not text:
        return jsonify({"error": "text обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    try:
        try:
            from google.cloud import texttospeech
        except Exception:
            return jsonify({"error": "Google TTS не установлен на сервере"}), 500
        key_path = prepare_google_creds_for_tts()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        tts_client = texttospeech.TextToSpeechClient()
        input_data = texttospeech.SynthesisInput(text=text)
        voice_params = texttospeech.VoiceSelectionParams(language_code=language, name=voice)
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            speaking_rate=0.95,
        )
        response = tts_client.synthesize_speech(
            input=input_data,
            voice=voice_params,
            audio_config=audio_config,
        )
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

    if not init_data:
        return jsonify({"error": "initData обязателен"}), 400
    if not entry_id:
        return jsonify({"error": "entry_id обязателен"}), 400

    if not _telegram_hash_is_valid(init_data):
        return jsonify({"error": "initData не прошёл проверку"}), 401

    entry = get_dictionary_entry_by_id(int(entry_id))
    response_json = entry.get("response_json") if entry else None
    if isinstance(response_json, str):
        try:
            response_json = json.loads(response_json)
        except Exception:
            response_json = None

    if not word_ru and entry:
        word_ru = (entry.get("word_ru") or "").strip()
    if not word_de and entry:
        word_de = (entry.get("word_de") or "").strip()

    try:
        enrich = asyncio.run(run_enrich_word(word_ru, word_de))
    except Exception as exc:
        return jsonify({"error": f"Ошибка enrich: {exc}"}), 500

    if not response_json:
        response_json = {}
    if isinstance(enrich, dict):
        response_json.update(enrich)

    try:
        update_webapp_dictionary_entry(int(entry_id), response_json)
    except Exception:
        pass

    return jsonify({"ok": True, "response_json": response_json})


@app.route("/api/webapp/youtube/transcript", methods=["POST"])
def get_youtube_transcript():
    payload = request.get_json(silent=True) or {}
    init_data = payload.get("initData")
    video_id = (payload.get("videoId") or "").strip()

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

    try:
        data = _fetch_youtube_transcript(video_id)
    except Exception as exc:
        logging.warning("YouTube transcript error for %s: %s", video_id, exc)
        return jsonify({"error": f"Не удалось получить субтитры: {exc}"}), 404

    return jsonify(
        {
            "ok": True,
            "items": data.get("items", []),
            "language": data.get("language"),
            "is_generated": data.get("is_generated"),
        }
    )


@app.route("/api/webapp/normalize/de", methods=["POST"])
def normalize_german():
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
        normalized = _normalize_german_text(text)
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

    sentences = get_pending_daily_sentences(user_id=user_id, limit=int(limit))
    deduped = _dedupe_sentences(sentences)
    return jsonify({"ok": True, "items": deduped})


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
    username = user_data.get("username")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        result = asyncio.run(
            start_translation_session_webapp(
                user_id=user_id,
                username=username,
                topic=topic if topic else "Random sentences",
                level=level,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка запуска сессии: {exc}"}), 500

    return jsonify({"ok": True, **result})


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

    items = get_story_history_webapp(user_id=user_id, limit=limit)
    return jsonify({"ok": True, "items": items})


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
    username = user_data.get("username")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

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

    return jsonify({"ok": True, **result})


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
    username = user_data.get("username")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    try:
        result = asyncio.run(
            submit_story_translation_webapp(
                user_id=user_id,
                username=username,
                translations=translations,
                guess=guess,
            )
        )
    except Exception as exc:
        return jsonify({"error": f"Ошибка истории: {exc}"}), 500

    if isinstance(result, dict) and result.get("error"):
        return jsonify({"error": result["error"]}), 400

    return jsonify({"ok": True, **result})


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
    username = user_data.get("username")

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

    try:
        explanation = asyncio.run(run_translation_explanation(original_text, user_translation))
    except Exception as exc:
        return jsonify({"error": f"Ошибка объяснения: {exc}"}), 500

    return jsonify({"ok": True, "explanation": explanation})


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
    username = user_data.get("username")

    if not user_id:
        return jsonify({"error": "user_id отсутствует в initData"}), 400

    result = finish_translation_webapp(user_id)
    summary = build_user_daily_summary(user_id=user_id, username=username or user_name)
    if summary:
        try:
            _send_group_message(summary)
        except Exception as exc:
            return jsonify({"error": f"Ошибка отправки в группу: {exc}"}), 500
    return jsonify({"ok": True, **result})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
