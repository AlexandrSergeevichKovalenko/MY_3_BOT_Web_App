"""Пересказ ролика для читалки: субтитры → свой текст с лексикой оригинала.

Зачем это появилось (владелец, 01.09.2026): «мы часто занимаемся с учителем, учитель
даёт видео, мы это видео смотрим, а потом на уроке разбираем. Вот это бы помогло очень
сильно — не просто посмотреть, а ещё и прочитать текст. Ну и всегда иметь его под рукой:
не всегда удобно смотреть видео, да и читать иногда проще, чем смотреть».

⚠️ ЭТО ПЕРЕСКАЗ, А НЕ РАСШИФРОВКА. Решение владельца 02.09.2026
───────────────────────────────────────────────────────────────
Первая версия на коротких роликах чистила субтитры и отдавала их почти дословно.
Владелец остановил это сам: «есть же какое-то право, охраняющее субтитры от копирования
и распространения. Лучше текст обрабатывать, оставляя исходные оригинальные фразы и
слова, но сам текст подвергать обработке и не давать его в оригинальном виде».

Замер, который это подтвердил (01.09.2026, ролик ru2ROHh_Ru4): у «вычищенного» текста
72% двенадцатисловных окон совпадали с субтитрами буквально, а самый длинный дословный
кусок был 119 слов подряд. Это была раздача чужих субтитров, а не наш текст.

Поэтому дословного режима БОЛЬШЕ НЕТ. Любой ролик становится пересказом:

  • лексика, устойчивые обороты, словосочетания и грамматические конструкции —
    ИЗ ОРИГИНАЛА: ровно ради них человек и читает этот текст;
  • предложения — СВОИ: устная речь переложена в письменную, повторы и оговорки убраны,
    порядок мыслей сохранён;
  • длинных дословных кусков быть не должно, и это проверяется механически, а не
    «на честном слове» — см. verbatim_overlap ниже.

Объём и порог считаются в СИМВОЛАХ, а не в минутах — решение владельца 01.09.2026,
и вот почему оно правильное. Замер по живой базе (62 ролика с субтитрами):

    48 566 символов — 66 минут  (медленный лектор, 733 симв/мин)
    90 278 символов — 29 минут  (скороговорка,    3 089 симв/мин)

29-минутный ролик даёт ВДВОЕ больше текста, чем часовой. По минутам мы обработали бы его
как короткий и заплатили бы за него втрое. Медианный темп речи — около 800 символов в
минуту, значит час обычной речи это ≈50 000 символов. Здесь и порог:

  • ДО порога — пересказ близко к оригиналу: объём почти тот же, теряются только
    устные повторы, оговорки и мусор распознавалки;
  • ВЫШЕ порога — пересказ со сжатием до читаемого объёма: двухчасовую лекцию целиком
    не осилит ни модель, ни читатель.

Чего здесь нет и не будет: механической починки немецкого нашими руками. Мы не
дописываем окончания, не «исправляем» грамматику регулярками и не склеиваем слова
своей арифметикой. Всё, что попадает человеку на экран, пришло из ответа модели,
прошедшего проверки ниже.

Три стража, и ни один не «на всякий случай»
───────────────────────────────────────────
1. ОБЪЁМ. Главный способ для модели «справиться» с длинным куском — тихо проглотить его
   конец. Наружу это выглядит как нормальный абзац: человек не узнает, что у ролика было
   продолжение. Единственный дешёвый признак — усохший объём.
2. ЯЗЫК. Модель, не справившись, иногда отвечает по-русски или объясняет, почему не
   может. Показать это как немецкий текст ролика нельзя.
3. ДОСЛОВНОСТЬ. Пересказ, оказавшийся копией, — это ровно то, что владелец запретил
   02.09.2026, и на словах в задании этого не удержать. Меряем долей совпадающих
   двенадцатисловных окон и самым длинным дословным куском.

Кусок, не прошедший проверку, переспрашивается ОДИН раз — и в переспросе прямо сказано,
что было не так. Не помогло — сборка честно падает, и человек видит «не удалось собрать
текст». Сырые субтитры вместо текста мы не подсовываем НИКОГДА: это была бы и подмена
из правила ноль, и ровно та раздача чужого, от которой мы уходим.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)

# ── Пороги объёма. Все — в символах исходных субтитров ────────────────────────

# Граница «пересказ близко к оригиналу / пересказ со сжатием». Обоснование — в шапке.
VERBATIM_CHAR_LIMIT = 50_000

# Доля объёма, которую держит пересказ короткого ролика. Не единица: устная речь
# короче на письме — повторы, «ähm», оговорки и переспросы в текст не попадают.
# Замер 02.09.2026 (три ролика): при задании 0.75 модель отдавала 0.92, 0.98 и 1.00 —
# пересказ с той же лексикой почти не жмётся, и это правильно (владелец просил
# «максимально приближен к оригиналу по сути»). Задание подтянуто к наблюдаемому.
CLOSE_TARGET_RATIO = 0.85

# Объём, к которому сжимается ролик длиннее порога.
CONDENSED_TARGET_CHARS = 45_000

# Кусок, которым мы кормим модель. Меньше кусок — меньше требований в одном ответе и
# меньше соблазна проглотить конец; больше кусок — реже теряется связность на швах.
CHUNK_CHARS = 4_000

# Границы, в которых объём ответа считается честным (доля от ЦЕЛЕВОГО объёма куска).
TARGET_MIN_RATIO = 0.55
TARGET_MAX_RATIO = 1.45

# ── Пороги дословности ────────────────────────────────────────────────────────
# ┌─ ЗАМЕРЕНО 02.09.2026 на ТРЁХ живых роликах, 9 кусков. НЕ ТРОГАТЬ НА ГЛАЗОК. ──┐
# │ Старый «вычищенный» текст (ru2ROHh_Ru4): 72% окон совпадают дословно,          │
# │ длиннейший дословный кусок 119 слов. Это была раздача субтитров.               │
# │ Новый пересказ, те же и ещё два ролика (KEyufCK38ZE, Q7CLz772shY):             │
# │   доля совпадений по кускам: 0,0 0,0 0,6 3,0 4,8 7,5 11,0 12,4 %  → макс 12,4  │
# │   длиннейший кусок по кускам: 7 10 14 18 22 23 25 27 30 слов      → макс 30    │
# │ Пороги стоят между этими мирами: копию (72% / 119) ловят наверняка, честный    │
# │ пересказ (12% / 30) не трогают.                                               │
# │                                                                               │
# │ ПОЧЕМУ ПОРОГ ПО ДЛИНЕ КУСКА ЩЕДРЫЙ. Те 30 слов — цепочка учебных примеров      │
# │ («Dieses Buch sagt mir nicht zu. Ich lese lieber ein anderes.»). Перефразировать│
# │ их нельзя: в них и есть урок. Резать по 25 значит валить сборку на учебных     │
# │ роликах и не давать человеку НИЧЕГО. Главный признак копии — ДОЛЯ, а не длина: │
# │ у копии она 72%, у пересказа 12%.                                             │
# │ Перемерить: python3 scripts/video_text_overlap.py <video_id>                   │
# └───────────────────────────────────────────────────────────────────────────────┘
VERBATIM_WINDOW = 12          # длина окна в словах
MAX_VERBATIM_OVERLAP = 0.25   # доля совпадающих окон
MAX_VERBATIM_RUN = 45         # самый длинный дословный кусок, слов подряд


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name) or "").strip() or default)
    except Exception:
        return default


# ── Задание модели ────────────────────────────────────────────────────────────

_RETELL_SYSTEM = """\
Du bekommst ein Stück AUTOMATISCH ERKANNTER Untertitel eines Videos. Deine Aufgabe:
dieses Stück NACHERZÄHLEN — mit dem Wortmaterial des Originals, aber in EIGENEN Sätzen.

WOZU: Der Nutzer lernt daran Deutsch. Er soll genau die Wörter, Wendungen und
Konstruktionen aus dem Video wiederfinden. Der Text darf aber KEINE Abschrift der
Untertitel sein.

WAS BLEIBT (das ist der Lernstoff):
  • Wörter, feste Wendungen, Kollokationen und Fachbegriffe des Originals;
  • die grammatischen Konstruktionen, die im Original vorkommen;
  • die Reihenfolge der Gedanken — nichts umstellen;
  • jeder eigenständige Gedanke; Zahlen, Daten, Eigennamen und Fakten unverändert.

WAS DU NEU MACHST:
  • die SÄTZE: gesprochene Sprache wird geschriebene Sprache;
  • Wiederholungen, Füllsel («ähm», «also ja»), abgebrochene Ansätze und
    Erkennungsmüll ([Musik], [Applaus]) fallen weg;
  • Sätze werden zusammengezogen oder geteilt, der Satzbau wird umgestellt;
  • Absätze: eine Leerzeile zwischen Sinnabschnitten.

HARTE GRENZE: KEINE Passage darf Wort für Wort wie im Original dastehen. Mehr als zehn
identische Wörter hintereinander ist eine Abschrift und wird zurückgewiesen. Schreib
denselben Inhalt mit denselben Vokabeln, aber in deinem eigenen Satzbau.

ERKENNUNGSFEHLER: Ein VERHÖRTES Wort stellst du wieder her, wenn der Zusammenhang es
EINDEUTIG macht: «weitere werben mit sagen» → «weitere Verben mit sagen». Der Sprecher
hat das richtige Wort gesagt, die Maschine hat es falsch gehört — und der Nutzer würde
den Fehler auswendig lernen. Ist es NICHT eindeutig, bleibt das Wort stehen. Bei einem
Eigennamen unsicher — dann nenne ihn gar nicht, statt ihn zu raten.

NICHT ERLAUBT: eigene Wertung, eigene Erklärungen, Überschriften, Vorwort, «In diesem
Video geht es um…», Aufzählungszeichen. Es ist ein Text zum LESEN.

Ziellänge dieses Stücks: etwa {target} Zeichen.

Antworte NUR mit validem JSON: {{"text": "<die Nacherzählung>"}}"""


# ── Обращение к модели ────────────────────────────────────────────────────────

def _model_for(mode: str) -> str:
    """Модель под режим.

    Пересказ близко к оригиналу — работа простая: слова уже все в тексте, надо
    переложить их в письменные предложения. Это тянет gpt-4.1-mini, и он в пять раз
    дешевле. Сжатие длинного ролика требует решать, что важно, а что повтор — там
    полный gpt-4.1; по замеру 01.09.2026 в этот режим попадали 4 ролика из 62.
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
        "temperature": 0.4,
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
                logger.warning("пересказ ролика: модель молчала на «%s» — повтор", what)
                continue
            raise RuntimeError(f"модель не ответила на «{what}» за {timeout_sec}s")
        logger.info("пересказ ролика: «%s» — %ds", what, int(time.monotonic() - started))
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
        logger.debug("пересказ ролика: расход не записан", exc_info=True)
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
_WORD_RE = re.compile(r"\w+", re.UNICODE)


def _words(text: str) -> list[str]:
    return _WORD_RE.findall(str(text or "").lower())


def verbatim_overlap(source: str, answer: str, *, window: int = VERBATIM_WINDOW) -> float:
    """Какая доля ответа — буквальная копия исходника.

    Считаем по окнам в `window` слов: сколько окон ответа встречаются в исходнике
    слово в слово. У честного пересказа с той же лексикой это единицы процентов
    (совпадают устойчивые обороты), у переписанных субтитров — десятки.
    """
    source_words = _words(source)
    answer_words = _words(answer)
    if len(answer_words) < window or len(source_words) < window:
        return 0.0
    grams = {tuple(source_words[i:i + window]) for i in range(len(source_words) - window + 1)}
    total = len(answer_words) - window + 1
    hits = sum(1 for i in range(total) if tuple(answer_words[i:i + window]) in grams)
    return hits / max(1, total)


def longest_verbatim_run(source: str, answer: str) -> int:
    """Самый длинный дословный кусок ответа, в словах подряд.

    Доли мало: пересказ может совпадать с оригиналом на единицы процентов и при этом
    содержать одну целиком переписанную страницу. Эта проверка ловит именно её.
    """
    source_words = _words(source)
    answer_words = _words(answer)
    if not source_words or not answer_words:
        return 0
    positions: dict[str, list[int]] = {}
    for index, word in enumerate(source_words):
        positions.setdefault(word, []).append(index)
    best = 0
    for j in range(len(answer_words)):
        if len(answer_words) - j <= best:
            break
        for i in positions.get(answer_words[j], ()):
            run = 0
            while (i + run < len(source_words) and j + run < len(answer_words)
                   and source_words[i + run] == answer_words[j + run]):
                run += 1
            if run > best:
                best = run
    return best


def check_chunk_answer(answer: str, *, source: str, target_chars: int) -> str:
    """Проверить один кусок. Возвращает ПУСТУЮ строку, если кусок годен, иначе —
    причину человеческими словами (она уходит в лог и в текст переспроса).

    Три проверки, и все три — про подмену, а не про красоту. Что именно ловит
    каждая и почему они нужны — в шапке модуля.
    """
    text = str(answer or "").strip()
    if not text:
        return "модель вернула пустой текст"
    letters = sum(1 for ch in text if ch.isalpha())
    if letters:
        cyrillic = len(_CYRILLIC_RE.findall(text))
        if cyrillic / letters > 0.02:
            return "ответ пришёл не по-немецки"
    low = int(target_chars * TARGET_MIN_RATIO)
    high = int(target_chars * TARGET_MAX_RATIO)
    if len(text) < low:
        return f"пересказ усох: {len(text)} символов вместо ≥{low}"
    if len(text) > high:
        return f"пересказ разбух: {len(text)} символов вместо ≤{high}"
    run = longest_verbatim_run(source, text)
    if run > MAX_VERBATIM_RUN:
        return f"это не пересказ, а копия: {run} слов подряд слово в слово"
    overlap = verbatim_overlap(source, text)
    if overlap > MAX_VERBATIM_OVERLAP:
        return f"это не пересказ, а копия: {round(overlap * 100)}% текста совпадает дословно"
    return ""


# ── Сборка ────────────────────────────────────────────────────────────────────

def pick_mode(source_chars: int) -> str:
    """«близко к оригиналу» или «со сжатием» — решает объём в символах, не минуты."""
    return "close" if source_chars <= VERBATIM_CHAR_LIMIT else "condensed"


def target_ratio(source_chars: int) -> float:
    """Во сколько раз пересказ короче исходника. У коротких роликов — постоянная
    доля, у длинных — столько, сколько нужно, чтобы уложиться в читаемый объём."""
    if pick_mode(source_chars) == "close":
        return CLOSE_TARGET_RATIO
    return CONDENSED_TARGET_CHARS / max(1, source_chars)


def build_reader_text(*, items, on_progress=None, user_id: int | None = None,
                      ask=None) -> dict:
    """Собрать пересказ ролика из субтитров.

    `on_progress(done, total)` — чтобы человек на экране видел, сколько осталось.
    `ask` — как обращаться к модели; подменяется в тестах, в бою берётся здешний.

    Возвращает {"text", "mode", "source_chars", "result_chars", "model", "chunks"}.
    Ошибка ЛЮБОГО куска роняет всю сборку: полпересказа ролика — это не пересказ,
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
    ratio = target_ratio(source_chars)

    out_parts: list[str] = []
    for index, chunk in enumerate(chunks):
        target_chars = max(400, int(len(chunk) * ratio))
        system = _RETELL_SYSTEM.format(target=target_chars)
        what = f"кусок {index + 1}/{total}"
        answer = ask(system, chunk, model=model, what=what, user_id=user_id)
        problem = check_chunk_answer(answer, source=chunk, target_chars=target_chars)
        if problem:
            # Один переспрос — и в нём прямо сказано, что было не так. Это не «второй
            # шанс на удачу», а другой запрос: модель видит свою ошибку.
            logger.warning("пересказ ролика: %s не прошёл (%s) — переспрашиваю", what, problem)
            answer = ask(
                system + f"\n\nDein vorheriger Versuch war unbrauchbar: {problem}. "
                         f"Diesmal MUSS der ganze Abschnitt verarbeitet werden, vom ersten "
                         f"bis zum letzten Gedanken — und in DEINEN eigenen Sätzen, nicht "
                         f"in denen des Originals.",
                chunk, model=model, what=what + " (повтор)", user_id=user_id,
            )
            problem = check_chunk_answer(answer, source=chunk, target_chars=target_chars)
            if problem:
                raise RuntimeError(f"{what}: {problem}")
        out_parts.append(str(answer).strip())
        if callable(on_progress):
            try:
                on_progress(index + 1, total)
            except Exception:
                logger.debug("пересказ ролика: прогресс не записан", exc_info=True)

    text = "\n\n".join(part for part in out_parts if part)
    return {
        "text": text,
        "mode": mode,
        "source_chars": source_chars,
        "result_chars": len(text),
        "model": model,
        "chunks": total,
    }
