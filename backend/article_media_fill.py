"""Медиа для банка артиклей: мнемоники, озвучка, картинки.

Раньше всё это жило в `bot_3.py`, и потому считалось прямо в процессе бота. В образ
воркера (`Dockerfile.jobs` копирует только `backend/`) `bot_3.py` не попадает, так что
перенести работу в фон было нельзя, не переехав сюда. Функции здесь синхронные: их
зовут из рабочего потока dramatiq, где своего цикла событий нет и он не нужен.

`bot_3.py` теперь зовёт эти же функции — реализация одна, расхождению взяться неоткуда.
"""

from __future__ import annotations

import asyncio
import logging
import re

_UMLAUT = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
                         "Ä": "ae", "Ö": "oe", "Ü": "ue"})


def artikel_image_key(word: str) -> str:
    base = str(word).translate(_UMLAUT).lower()
    slug = re.sub(r"[^a-z0-9]+", "-", base).strip("-")[:48] or "w"
    return f"artikel/img/{slug}.jpg"


def backfill_artikel_audio(items: list[dict], limit: int = 30) -> dict:
    """Озвучка «<артикль> <слово>» → MP3 → R2 для слов без клипа, ключ оседает в банке.
    Дешёвый Standard-голос на бакете google_tts_standard, чтобы не конкурировать с
    Читалкой и SRS за премиум-тир. Как только бакет исчерпан — останавливаемся и
    говорим об этом (stopped='blocked'), а не молотим в пустоту."""
    from backend.backend_server import synthesize_and_store_artikel_audio
    from backend.tts_generation import GoogleTTSBudgetBlockedError

    made = 0
    stopped: str | None = None
    for it in (items or [])[:limit]:
        word = str(it.get("word") or "").strip()
        article = str(it.get("article") or "").strip().lower()
        if not word or article not in ("der", "die", "das"):
            continue
        try:
            if synthesize_and_store_artikel_audio(word=word, article=article):
                made += 1
        except GoogleTTSBudgetBlockedError:
            logging.info("artikel_audio backfill: standard bucket blocked at made=%s — stopping", made)
            stopped = "blocked"
            break
        except Exception:
            logging.warning("artikel_audio backfill failed word=%s", word, exc_info=True)
    return {"requested": len(items or []), "made": made, "stopped": stopped}


def backfill_artikel_images(items: list[dict], limit: int = 15) -> dict:
    """Для ещё не оценённых слов: модель решает «предметное ли» и даёт английский
    запрос → бесплатное фото со стока → R2. Абстрактное просто помечается проверенным.

    Без PIXABAY_API_KEY ничего не помечаем и возвращаем skipped='no_key': пометить
    «проверено» без ключа значило бы навсегда похоронить эти слова без картинок."""
    from backend.stock_image import fetch_stock_image_bytes, stock_image_enabled

    if not stock_image_enabled():
        logging.info("artikel_images: PIXABAY_API_KEY not set — skipping image backfill")
        return {"requested": 0, "made": 0, "skipped": "no_key"}
    from backend.database import mark_article_noun_image
    from backend.openai_manager import run_article_image_meta
    from backend.r2_storage import r2_put_bytes

    batch = (items or [])[:limit]
    if not batch:
        return {"requested": 0, "made": 0}
    # Мета от модели пачками по 20: на большом limit один огромный промпт склонен
    # обрезаться, и ответ приходит рваным.
    metas: list = []
    for i in range(0, len(batch), 20):
        chunk = batch[i:i + 20]
        metas.extend(asyncio.run(run_article_image_meta(items=[
            {"word": x.get("word"), "article": x.get("article"), "ru": x.get("meaning_ru", "")}
            for x in chunk])) or [])
    meta_by_word = {str(m.get("word") or "").lower(): m for m in (metas or [])}
    made = concrete = 0
    for it in batch:
        word = str(it.get("word") or "").strip()
        article = str(it.get("article") or "").strip().lower()
        if not word:
            continue
        m = meta_by_word.get(word.lower()) or {}
        query = str(m.get("image_query") or "").strip()
        key = ""
        if bool(m.get("is_concrete")) and query:
            concrete += 1
            img = fetch_stock_image_bytes(query)
            if img:
                k = artikel_image_key(word)
                try:
                    r2_put_bytes(k, img, content_type="image/jpeg")
                    key = k
                    made += 1
                except Exception:
                    logging.warning("artikel_images: R2 put failed word=%s", word, exc_info=True)
        mark_article_noun_image(word=word, article=article, image_object_key=key)
    logging.info("artikel_images: requested=%s meta=%s concrete=%s made=%s",
                 len(batch), len(metas or []), concrete, made)
    return {"requested": len(batch), "meta": len(metas or []), "concrete": concrete, "made": made}


def fill_theme_media(theme_key: str, *, cap: int = 60, include_audio: bool = True) -> dict:
    """Мнемоники + картинки (+ озвучка, если include_audio) для cap ещё не обработанных
    слов темы. Хелперы `*_without_*` / `*_for_image` отдают только незакрытые слова,
    поэтому повторный прогон идёт дальше по теме, а не переделывает сделанное.

    Ночной прогрев зовёт с include_audio=False: озвучку по всем темам ровно размазывает
    отдельный бюджетный свип. Админская команда оставляет озвучку включённой, чтобы
    добить одну тему целиком."""
    from backend.article_learn import _generate_and_cache_mnemonics
    from backend.database import (
        get_article_nouns_for_image,
        get_article_nouns_without_audio,
        get_article_nouns_without_mnemonic,
    )

    made = {"mnemonics": 0, "audio": 0, "images": 0}
    todo_m = get_article_nouns_without_mnemonic(theme_key, cap)
    mitems = [{"w": t["word"], "a": t["article"], "ru": t.get("meaning_ru", "")} for t in todo_m]
    for i in range(0, len(mitems), 20):
        try:
            made["mnemonics"] += len(_generate_and_cache_mnemonics(mitems[i:i + 20]))
        except Exception:
            logging.warning("artikel_media: mnemonic chunk failed theme=%s", theme_key, exc_info=True)
    if include_audio:
        todo_a = get_article_nouns_without_audio(theme_key, cap)
        audio_res = backfill_artikel_audio(todo_a, limit=cap)
        made["audio"] = int(audio_res.get("made") or 0)
        if audio_res.get("stopped"):
            made["audio_stopped"] = audio_res["stopped"]
    todo_i = get_article_nouns_for_image(theme_key, cap)
    image_res = backfill_artikel_images(todo_i, limit=cap)
    made["images"] = int(image_res.get("made") or 0)
    if image_res.get("skipped"):
        made["images_skipped"] = image_res["skipped"]
    return made
