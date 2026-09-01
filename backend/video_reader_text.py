"""Текст ролика для читалки: субтитры → то, что можно читать глазами.

Зачем это появилось (владелец, 01.09.2026): «мы часто занимаемся с учителем, учитель
даёт видео, мы это видео смотрим, а потом на уроке разбираем. Вот это бы помогло очень
сильно — не просто посмотреть, а ещё и прочитать текст. Ну и всегда иметь его под рукой:
не всегда удобно смотреть видео, да и читать иногда проще, чем смотреть».

Что здесь происходит и чего здесь НЕ происходит
───────────────────────────────────────────────
Субтитры YouTube — это не текст. Это лента реплик без точек и запятых, с [musik], с
«ähm», с оговорками, с повторами одной мысли и со склейками распознавалки. Читать её
глазами невозможно, а учить по ней немецкий — вредно.

Мы приводим её в вид текста. РЕЖИМ ЗАВИСИТ ОТ ОБЪЁМА, и порог измеряется в СИМВОЛАХ,
а не в минутах — решение владельца 01.09.2026, и вот почему оно правильное. Замер по
живой базе (62 ролика с субтитрами) в тот же день:

    48 566 символов — 66 минут  (медленный лектор, 733 симв/мин)
    90 278 символов — 29 минут  (скороговорка,    3 089 симв/мин)

29-минутный ролик даёт ВДВОЕ больше текста, чем часовой. По минутам мы обработали бы его
как короткий и заплатили бы за него втрое. Медианный темп речи у наших роликов — около
800 символов в минуту, значит час обычной речи это ≈50 000 символов. Здесь и порог.
Под дословный режим по тому замеру попадали 58 роликов из 62.

  • ДО порога — «дословно»: те же слова, тот же порядок, ничего не выброшено и ничего
    не добавлено. Модель расставляет знаки препинания и абзацы, убирает мусор
    распознавалки и оговорки. Учить обороты по такому тексту можно: они из ролика.
  • ВЫШЕ порога — «пересказ»: модель рассказывает своими словами, но ОБЯЗАНА держаться
    лексики оригинала — слова, обороты, выражения берутся из ролика. Двухчасовую лекцию
    дословно не осилит ни модель, ни читатель.

Чего здесь нет и не будет: механической починки немецкого нашими руками. Мы не
дописываем окончания, не «исправляем» грамматику регулярками и не склеиваем слова
своей арифметикой. Всё, что попадает человеку на экран, пришло либо из субтитров,
либо из ответа модели, прошедшего проверку ниже.

Проверка (страж), и почему она не «на всякий случай»
────────────────────────────────────────────────────
Главный способ для модели «справиться» с длинным куском — тихо проглотить его конец.
Наружу это выглядит как нормальный абзац: человек не знает, что у ролика было
продолжение. Поэтому каждый кусок после обработки сверяется по объёму с исходным, и
кусок, не прошедший проверку, переспрашивается ОДИН раз. Не помогло — сборка честно
падает, и человек видит «не удалось собрать текст». Сырые субтитры вместо текста мы
не подсовываем: это была бы ровно та подмена, которую запрещает правило ноль.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)

# ── Пороги. Все — в символах исходных субтитров ───────────────────────────────

# Граница «дословно / пересказ». Обоснование и замер — в шапке модуля.
VERBATIM_CHAR_LIMIT = 50_000

# Объём, к которому сжимается ролик длиннее порога. Тот же порядок, что и у самого
# длинного «дословного» ролика: читатель получает сопоставимый по величине текст.
CONDENSED_TARGET_CHARS = 45_000

# Кусок, которым мы кормим модель. Меньше кусок — меньше требований в одном ответе и
# меньше соблазна проглотить конец; больше кусок — реже теряется связность на швах.
CHUNK_CHARS = 4_000

# Границы, в которых объём ответа считается честным. Дословный режим не имеет права
# усохнуть больше чем на пятую часть: усох — значит содержание проглочено.
VERBATIM_MIN_RATIO = 0.80
VERBATIM_MAX_RATIO = 1.25
CONDENSED_MIN_RATIO = 0.55
CONDENSED_MAX_RATIO = 1.45


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name) or "").strip() or default)
    except Exception:
        return default


# ── Задания модели ────────────────────────────────────────────────────────────

_CLEAN_SYSTEM = """\
Du bekommst ein Stück AUTOMATISCH ERKANNTER Untertitel eines Videos. Deine Aufgabe: daraus
LESBAREN Fliesstext machen — und sonst nichts.

DU DARFST:
  • Satzzeichen setzen: Punkte, Kommas, Fragezeichen, Gedankenstriche;
  • Gross- und Kleinschreibung korrigieren (Substantive gross, Satzanfang gross);
  • Absätze bilden: eine Leerzeile zwischen Sinnabschnitten;
  • Erkennungsmüll entfernen: [Musik], [Applaus], (unverständlich), «ähm», «äh», «also ja»
    als reine Verzögerung, doppelt gesprochene Wörter, abgebrochene Ansätze
    («ich habe — ich habe gesagt» → «ich habe gesagt»);
  • offensichtlich falsch erkannte Wortgrenzen zusammenfügen, wenn das Wort dadurch
    eindeutig wird;
  • ein VERHÖRTES Wort wiederherstellen, wenn der Zusammenhang es EINDEUTIG macht:
    «weitere werben mit sagen» → «weitere Verben mit sagen», «unser erstes wert»
    → «unser erstes Verb». Der Sprecher hat das richtige Wort gesagt, die Maschine
    hat es falsch gehört — und der Nutzer würde den Fehler auswendig lernen.
    Ist es NICHT eindeutig, bleibt das Wort unverändert stehen. Im Zweifel: stehen
    lassen. Das gilt nur für Erkennungsfehler, NIE für Stil.

DU DARFST NICHT:
  • Inhalt weglassen, kürzen oder zusammenfassen — JEDER Gedanke bleibt;
  • Wörter durch Synonyme ersetzen oder Sätze umformulieren;
  • etwas hinzufügen, was im Stück nicht gesagt wurde — keine Überleitungen, keine
    Erklärungen, keine Überschriften, keine Zusammenfassung, kein Vorwort;
  • Zahlen, Namen oder Fakten ändern. Bist du bei einem Namen unsicher, LASS IHN
    GENAU SO, wie er dasteht — rate nicht.

Der Nutzer LERNT an diesem Text Deutsch. Er wird die Wendungen daraus auswendig lernen.
Deshalb bleiben es SEINE Wörter aus dem Video, nicht deine.

Antworte NUR mit validem JSON: {"text": "<der lesbare Text>"}"""

_CONDENSE_SYSTEM = """\
Du bekommst ein Stück AUTOMATISCH ERKANNTER Untertitel eines sehr langen Videos. Es ist
zu lang, um es vollständig zu lesen. Deine Aufgabe: dieses Stück NACHERZÄHLEN — kürzer,
in ganzen Sätzen, aber MIT DEM WORTMATERIAL DES ORIGINALS.

SO NACHERZÄHLEN:
  • Reihenfolge des Originals beibehalten — nichts umstellen;
  • jeden eigenständigen Gedanken behalten; weg dürfen nur Wiederholungen, Füllsel,
    Abschweifungen, Werbung und Begrüssungsfloskeln;
  • die WÖRTER, WENDUNGEN und AUSDRÜCKE des Originals übernehmen, wo es nur geht:
    der Nutzer lernt daran genau diese Sprache. Ein Synonym nur dann, wenn der Satz
    ohne es nicht steht;
  • Zahlen, Daten, Eigennamen und Fakten unverändert übernehmen. Unsicher bei einem
    Namen — dann nenne ihn gar nicht, statt ihn zu raten;
  • Absätze bilden: eine Leerzeile zwischen Sinnabschnitten.

NICHT ERLAUBT: eigene Wertung, eigene Erklärungen, Überschriften, Vorwort, «In diesem
Video geht es um…», Aufzählungszeichen. Es ist ein Text zum LESEN, kein Protokoll.

Ziellänge dieses Stücks: etwa {target} Zeichen.

Antworte NUR mit validem JSON: {{"text": "<die Nacherzählung>"}}"""


# ── Обращение к модели ────────────────────────────────────────────────────────

def _model_for(mode: str) -> str:
    """Модель под режим.

    Чистка — работа, где все слова уже лежат в тексте: модель расставляет знаки и
    убирает мусор, ничего не сочиняя. Это тянет gpt-4.1-mini, и он в пять раз дешевле.
    Пересказ длинного ролика — работа настоящая, там нужен полный gpt-4.1; по замеру
    01.09.2026 в этот режим попадали 4 ролика из 62, так что цена ограничена.
    Обе переопределяются через LLM_TASK_MODEL_VIDEO_READER_TEXT_CLEAN / _CONDENSE.
    """
    from backend.openai_manager import _get_task_gateway_model

    task = "video_reader_text_condense" if mode == "condensed" else "video_reader_text_clean"
    return _get_task_gateway_model(task)


def ask_model_for_text(system: str, user: str, *, model: str, what: str,
                       user_id: int | None = None) -> str:
    """Один запрос к модели, ответ — кусок текста. Здесь и только здесь живут ключ,
    таймаут, повтор по таймауту и запись расхода.

    Сбой НЕ глушится и НЕ подменяется пустой строкой: пустой ответ от ошибки
    неотличим от честного «модель промолчала», а нам эти два мира путать нельзя.
    """
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    payload = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    timeout_sec = _env_int("VIDEO_READER_TEXT_TIMEOUT_SEC", 240)
    attempts = max(1, _env_int("VIDEO_READER_TEXT_RETRIES", 2))
    resp = None
    for attempt in range(attempts):
        started = time.monotonic()
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers, json=payload, timeout=timeout_sec,
            )
        except requests.Timeout:
            if attempt + 1 < attempts:
                logger.warning("текст ролика: модель молчала на «%s» — повтор", what)
                continue
            raise RuntimeError(f"модель не ответила на «{what}» за {timeout_sec}s")
        logger.info("текст ролика: «%s» — %ds", what, int(time.monotonic() - started))
        break
    if resp is None or not resp.ok:
        code = getattr(resp, "status_code", "нет ответа")
        body = (getattr(resp, "text", "") or "")[:200]
        raise RuntimeError(f"OpenAI HTTP {code} на «{what}»: {body}")
    resp_json = resp.json()
    try:
        from backend.openai_usage_logging import log_openai_raw_usage
        log_openai_raw_usage(
            action_type="video_reader_text",
            model=model,
            usage=resp_json.get("usage"),
            user_id=user_id,
        )
    except Exception:
        logger.debug("текст ролика: расход не записан", exc_info=True)
    raw = (resp_json.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    parsed = json.loads(raw)
    return str((parsed or {}).get("text") or "")


# ── Резка субтитров на куски ──────────────────────────────────────────────────

def transcript_to_text(items) -> str:
    """Склейка реплик субтитров в одну строку — ровно как их отдал YouTube."""
    parts = []
    for item in (items or []):
        piece = str((item or {}).get("text") or "").strip()
        if piece:
            parts.append(piece)
    return " ".join(parts)


def split_into_chunks(items, *, chunk_chars: int = CHUNK_CHARS) -> list[str]:
    """Резать на куски по границам реплик, а не по символам.

    Резать посреди реплики нельзя: обрубленное предложение модель не восстановит,
    а начало следующего куска она примет за начало мысли. Поэтому реплики копятся,
    пока кусок не дорос до нужного размера, и только тогда он закрывается.
    """
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for item in (items or []):
        piece = str((item or {}).get("text") or "").strip()
        if not piece:
            continue
        if current and current_len + len(piece) + 1 > chunk_chars:
            chunks.append(" ".join(current))
            current = []
            current_len = 0
        current.append(piece)
        current_len += len(piece) + 1
    if current:
        chunks.append(" ".join(current))
    return chunks


# ── Страж ─────────────────────────────────────────────────────────────────────

_CYRILLIC_RE = re.compile(r"[а-яёА-ЯЁ]")


def check_chunk_answer(answer: str, *, source_chars: int, target_chars: int,
                       mode: str) -> str:
    """Проверить один кусок. Возвращает ПУСТУЮ строку, если кусок годен, иначе —
    причину человеческими словами (она уходит в лог и в повторный запрос).

    Проверяем две вещи, и обе — про подмену, а не про красоту:

    1. ОБЪЁМ. Молча проглоченный конец куска выглядит наружу как нормальный абзац:
       человек не узнает, что у ролика было продолжение. Единственный дешёвый признак
       этого — усохший объём.
    2. ЯЗЫК. Модель, не справившись, иногда отвечает по-русски или объясняет, почему
       не может. Такой ответ нельзя показывать как немецкий текст ролика.
    """
    text = str(answer or "").strip()
    if not text:
        return "модель вернула пустой текст"
    letters = sum(1 for ch in text if ch.isalpha())
    if letters:
        cyrillic = len(_CYRILLIC_RE.findall(text))
        if cyrillic / letters > 0.02:
            return "ответ пришёл не по-немецки"
    if mode == "verbatim":
        low = int(source_chars * VERBATIM_MIN_RATIO)
        high = int(source_chars * VERBATIM_MAX_RATIO)
        if len(text) < low:
            return f"текст усох: {len(text)} символов вместо ≥{low}"
        if len(text) > high:
            return f"текст разбух: {len(text)} символов вместо ≤{high}"
        return ""
    low = int(target_chars * CONDENSED_MIN_RATIO)
    high = int(target_chars * CONDENSED_MAX_RATIO)
    if len(text) < low:
        return f"пересказ усох: {len(text)} символов вместо ≥{low}"
    if len(text) > high:
        return f"пересказ разбух: {len(text)} символов вместо ≤{high}"
    return ""


# ── Сборка ────────────────────────────────────────────────────────────────────

def pick_mode(source_chars: int) -> str:
    """«dословно» или «пересказ» — решает объём в символах, не длина ролика."""
    return "verbatim" if source_chars <= VERBATIM_CHAR_LIMIT else "condensed"


def build_reader_text(*, items, on_progress=None, user_id: int | None = None,
                      ask=None) -> dict:
    """Собрать текст ролика из субтитров.

    `on_progress(done, total)` — чтобы человек на экране видел, сколько осталось.
    `ask` — как обращаться к модели; подменяется в тестах, в бою берётся здешний.

    Возвращает {"text", "mode", "source_chars", "result_chars", "model", "chunks"}.
    Ошибка ЛЮБОГО куска роняет всю сборку: полтекста ролика — это не текст ролика,
    а обрезанный текст, о котором читатель не знает.
    """
    ask = ask or ask_model_for_text
    source_text = transcript_to_text(items)
    source_chars = len(source_text)
    if source_chars < 200:
        raise ValueError("субтитры слишком короткие, собирать нечего")

    mode = pick_mode(source_chars)
    model = _model_for(mode)
    chunks = split_into_chunks(items)
    total = len(chunks)
    # Сжатие раскладывается на куски пропорционально: каждый кусок ужимается в одно
    # и то же число раз, поэтому ни начало, ни конец ролика не пропадают.
    ratio = (CONDENSED_TARGET_CHARS / source_chars) if mode == "condensed" else 1.0

    out_parts: list[str] = []
    for index, chunk in enumerate(chunks):
        target_chars = max(400, int(len(chunk) * ratio))
        if mode == "verbatim":
            system = _CLEAN_SYSTEM
        else:
            system = _CONDENSE_SYSTEM.format(target=target_chars)
        what = f"кусок {index + 1}/{total}"
        answer = ask(system, chunk, model=model, what=what, user_id=user_id)
        problem = check_chunk_answer(
            answer, source_chars=len(chunk), target_chars=target_chars, mode=mode,
        )
        if problem:
            # Один переспрос — и в нём прямо сказано, что было не так. Это не «второй
            # шанс на удачу», а другой запрос: модель видит свою ошибку.
            logger.warning("текст ролика: %s не прошёл (%s) — переспрашиваю", what, problem)
            answer = ask(
                system + f"\n\nDein vorheriger Versuch war unbrauchbar: {problem}. "
                         f"Diesmal MUSS der ganze Abschnitt verarbeitet werden, "
                         f"vom ersten bis zum letzten Satz.",
                chunk, model=model, what=what + " (повтор)", user_id=user_id,
            )
            problem = check_chunk_answer(
                answer, source_chars=len(chunk), target_chars=target_chars, mode=mode,
            )
            if problem:
                raise RuntimeError(f"{what}: {problem}")
        out_parts.append(str(answer).strip())
        if callable(on_progress):
            try:
                on_progress(index + 1, total)
            except Exception:
                logger.debug("текст ролика: прогресс не записан", exc_info=True)

    text = "\n\n".join(part for part in out_parts if part)
    return {
        "text": text,
        "mode": mode,
        "source_chars": source_chars,
        "result_chars": len(text),
        "model": model,
        "chunks": total,
    }
