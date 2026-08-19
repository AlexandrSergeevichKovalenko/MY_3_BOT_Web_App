# -*- coding: utf-8 -*-
"""Происхождение слова для банка артиклей: англицизм или нет — ПО СПРАВОЧНИКУ.

Зачем это есть. 19.08.2026 владелец сыграл спринт по теме «Computer & Geräte» и
получил экран из Upload, Backup, Controller, Export, Hack, Tab. Разбор показал две
разные беды, которые легко перепутать:

  • Слова НЕ выдуманы — они все есть в немецком словаре. Выдуманными были только
    «die Sync» и «der SMS-Ton» (у них вообще нет статьи), это чинит другой страж.
  • Беда в том, что тема на треть состояла из английских слов, у которых род
    в живом языке спорный и на слух не выводится. Учить на них артикль бессмысленно.

Но «англицизм» сам по себе НЕ приговор: der Bus, der Film, das Radio, der Sport,
das Taxi, der Computer — тоже англицизмы по происхождению, и это слова из первого
учебника, у которых артикль как раз и надо выучить. Значит режущая граница не
«пришло из английского», а «пришло из английского И в живой речи почти не
встречается».

ДВА ИСТОЧНИКА, ОБА НАЗЫВАЮТСЯ ВСЛУХ. Ничего не выводится из написания слова.

  1. Происхождение — раздел `{{Herkunft}}` немецкой статьи de.wiktionary.
     Признак: справочник назвал английское слово-источник И немецкое написание
     совпадает с ним (Upload ← upload). Совпадение написания обязательно: без него
     в англицизмы попадают кальки (die Einbahnstraße ← one-way street) и просто
     родственные слова (der Zimmermann ~ timber) — они пишутся по-немецки, и род
     у них немецкий. Замер 19.08.2026: без этой проверки правило дало 214 находок,
     из которых заведомо неверными были der Zaun, der Pfeil, die Pappe, der Schlitz.

  2. Когда de.wiktionary о происхождении молчит (нет раздела Herkunft — таких в
     банке 393 слова) — спрашиваем en.wiktionary про НАПРАВЛЕНИЕ заимствования:
     в немецком разделе оно записано разметкой `{{bor|de|en|…}}`. Направление
     обязательно: «слово есть в английском» ничего не значит, английский взял
     Schnitzel, Blutwurst и Marzipan У НЕМЕЦКОГО, а не наоборот. Проверка «есть
     английская статья» без направления предлагала снять именно их.

  3. Ходовое слово или нет — частотный список `bt_3_word_frequency`
     (OpenSubtitles-2018, топ-50 000; тот же, которым мерили банк 31.07.2026).

РЕШЕНИЕ ВЛАДЕЛЬЦА 19.08.2026: режем хвост за 20 000. Слово снимается, только если
справочник назвал его англицизмом И оно лежит за 20 000 (или его нет в списке
вовсе). Замер того же дня: всё, что владелец обвёл на экране, лежит на 24 119
(Account) — 46 639 (Upload); всё, что трогать нельзя, — в первых 20 000
(der Bus 1603, der Film 673, der Sport 3244, der Laptop 5733, der Monitor 9428).

ЧЕГО ЗДЕСЬ НЕТ И НЕ БУДЕТ. Ни одного правила «по хвосту слова», ни одного списка
англицизмов, вшитого в код, ни одной догадки при недоступном справочнике. Когда
источник молчит — вердикт «не знаем», он СЧИТАЕТСЯ и уходит в отчёт владельцу, а
слово при этом НЕ снимается: недоказанное не равно доказанному.
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request

# Решение владельца 19.08.2026: граница «ходовое / хвост» по частотному списку.
TAIL_RANK = 20000

_DE_API = "https://de.wiktionary.org/w/api.php"
_EN_API = "https://en.wiktionary.org/w/api.php"
_UA = "DeutschBot/1.0 (Artikel Sprint origin check; contact via Telegram bot)"
_BATCH = 20
_RETRY_CODES = (429, 503)
_FETCH_RETRIES = 5
_FETCH_BACKOFF_SECONDS = 8
# Пауза между пачками. Прогон по всему банку (5 108 написаний) стучится в ДВА
# справочника подряд, и без паузы Викисловарь отвечает 429 «слишком часто» уже
# на первой сотне: 19.08.2026 полный прогон на этом и оборвался. Пауза дешёвая —
# прогон ночной, а её отсутствие делает его невозможным в принципе.
_PAUSE_BETWEEN_BATCHES_SECONDS = 0.6

# Вердикты происхождения. «unbekannt» — это НЕ «немецкое», это незакрытая задача.
ANGLICISM = "anglizismus"
OTHER = "andere-herkunft"
UNKNOWN = "unbekannt"

# --- разбор немецкой статьи ------------------------------------------------

_DE_SECTION = re.compile(
    r"==\s*(?P<title>[^=]*?)\s*\(\{\{Sprache\|Deutsch\}\}\)\s*==(?P<body>.*?)(?=\n==\s[^=]|\Z)",
    re.DOTALL,
)
# Раздел «Herkunft» кончается на следующем шаблоне-заголовке в начале строки.
_HERKUNFT = re.compile(r"\{\{Herkunft\}\}(.*?)(?=\n\{\{[A-ZÄÖÜ][^}]*\}\}\n|\Z)", re.DOTALL)
# Английский язык вообще упомянут (иначе кандидатов не ищем).
_SAYS_ENGLISH = re.compile(r"englisch|engl\.|\{\{en\.\}\}|Englischen", re.IGNORECASE)
# Английское слово, помеченное САМИМ справочником: {{Ü|en|upload}}.
_TAGGED_EN = re.compile(r"\{\{Ü\|en\|([^}|]+)")
# Английское слово курсивом рядом со словом «englisch»: englisch ''upload''.
_QUOTED_AFTER_ENGLISH = re.compile(
    r"(?:[Ee]nglisch(?:en|e)?|\bengl\.)\W{0,12}''\s*(?:to\s+)?([A-Za-z][A-Za-z \-']{1,30})''"
)
# То же, но БЕЗ курсива: «aus dem [[engl.]] to scan „untersuchen…“». Так записан
# источник у der Scanner — статья помечена {{QS Herkunft|unbelegt}}, и разметку в ней
# никто не проставил. Кандидат отсюда всё равно проходит проверку совпадения
# написания, поэтому лишнего слова это правило принести не может.
_BARE_AFTER_ENGLISH = re.compile(
    r"(?:[Ee]nglisch(?:en|e)?|\bengl\.)\]*\s+(?:to\s+)?([a-z][a-z\-]{1,20})\b"
)
# Справочник САМ пометил свою этимологию как неподтверждённую. Тогда её нельзя
# считать ответом — надо спросить второй источник. Живой пример: у «Scanner»
# в de.wiktionary написано «aus dem engl. to scan {{QS Herkunft|unbelegt}}», и по
# этому тексту слово не опознаётся (scan + er = «scaner», а не «Scanner»).
# Дописывать сюда правило удвоения согласной — это механическое додумывание, оно
# запрещено. en.wiktionary тем временем говорит прямым текстом: {{bor+|de|en|scanner}}.
_HERKUNFT_UNSOURCED = re.compile(r"\{\{QS[ _]Herkunft\|(?:unbelegt|fehlt|zweifelhaft)", re.IGNORECASE)
# Направление заимствования в en.wiktionary: {{bor|de|en|tab}} = немецкий взял у английского.
_BORROWED_DE_FROM_EN = re.compile(r"\{\{u?bor\+?\|de\|en\||from\s+English\b", re.IGNORECASE)
_EN_GERMAN_SECTION = re.compile(r"^==\s*German\s*==(.*?)(?=^==[^=]|\Z)", re.DOTALL | re.MULTILINE)
_EN_ETYMOLOGY = re.compile(r"===\s*Etymology[^=]*===(.*?)(?====|\Z)", re.DOTALL)


def _norm(text: str) -> str:
    """Написание для сравнения: только буквы, без дефисов, пробелов и регистра.

    «back up» и «Backup» — одно и то же слово, разница только в типографике
    английского источника."""
    return re.sub(r"[^a-z]", "", str(text or "").lower())


def _english_sources(herkunft: str) -> set[str]:
    """Английские слова, которые справочник назвал источником."""
    found = set(_TAGGED_EN.findall(herkunft))
    if _SAYS_ENGLISH.search(herkunft):
        found.update(_QUOTED_AFTER_ENGLISH.findall(herkunft))
        found.update(_BARE_AFTER_ENGLISH.findall(herkunft))
    return {c.strip() for c in found if c.strip()}


def judge_herkunft(word: str, herkunft: str) -> bool:
    """Немецкое написание совпало с английским словом-источником?

    Совпадение написания — обязательное условие: оно и отличает заимствование
    (der Upload ← upload) от кальки и от родственного слова."""
    if not herkunft:
        return False
    target = _norm(word)
    tagged = {c.strip() for c in _TAGGED_EN.findall(herkunft)}
    # Шаблон {{Ü|en|…}} ставит сам справочник — если написание совпало с ним,
    # слова «englisch» рядом можно не требовать (так опознаётся der Messenger).
    for cand in tagged:
        if _norm(cand) == target:
            return True
    if not _SAYS_ENGLISH.search(herkunft):
        return False
    for cand in _english_sources(herkunft):
        norm = _norm(cand)
        if not norm:
            continue
        if norm == target:
            return True
        # Немецкое существительное образовано от английского глагола:
        # «engl. to scan» → der Scanner. Суффикс немецкий, корень английский.
        if target in (norm + "er", norm + "ing"):
            return True
    return False


# --- сеть ------------------------------------------------------------------


def _fetch_wikitext(api: str, titles: list[str]) -> dict[str, str | None]:
    """Викитекст статей одним запросом. None у заголовка = статьи нет.

    Сеть либо отвечает, либо мы честно падаем: подставлять «нет статьи» вместо
    ответа нельзя — «статьи нет» и «мы не смогли спросить» это разные миры."""
    params = {
        "action": "query", "prop": "revisions", "rvprop": "content", "rvslots": "main",
        "format": "json", "formatversion": "2", "titles": "|".join(titles),
    }
    url = api + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": _UA})
    last_error: Exception | None = None
    for attempt in range(_FETCH_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in _RETRY_CODES:
                raise
            time.sleep(_FETCH_BACKOFF_SECONDS * (attempt + 1))
        except Exception as exc:            # сеть/таймаут — те же правила
            last_error = exc
            time.sleep(_FETCH_BACKOFF_SECONDS * (attempt + 1))
    else:
        raise RuntimeError(f"справочник {api} не ответил за {_FETCH_RETRIES} попыток: {last_error}")
    out: dict[str, str | None] = {}
    for page in payload.get("query", {}).get("pages", []):
        title = page.get("title") or ""
        out[title] = None if page.get("missing") else page["revisions"][0]["slots"]["main"]["content"]
    # Викимедиа нормализует заголовки (регистр, подчёркивания) — вернём и исходные ключи.
    for item in payload.get("query", {}).get("normalized", []):
        if item.get("to") in out:
            out[item.get("from")] = out[item["to"]]
    return out


def _herkunft_of(word: str, wikitext: str | None) -> str:
    if not wikitext:
        return ""
    for match in _DE_SECTION.finditer(wikitext):
        body = match.group("body")
        herkunft = _HERKUNFT.search(body)
        if herkunft:
            return re.sub(r"\s+", " ", herkunft.group(1)).strip()
    return ""


def _en_direction(wikitext: str | None) -> bool | None:
    """Немецкий взял слово у английского? None — en.wiktionary тоже молчит."""
    if not wikitext:
        return None
    section = _EN_GERMAN_SECTION.search(wikitext)
    if not section:
        return None
    etymology = _EN_ETYMOLOGY.search(section.group(1))
    if not etymology:
        return None
    text = re.sub(r"\s+", " ", etymology.group(1)).strip()
    if not text:
        return None
    return bool(_BORROWED_DE_FROM_EN.search(text))


# --- кэш -------------------------------------------------------------------


def ensure_origin_cache_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bt_3_word_origin_cache (
                word       TEXT PRIMARY KEY,
                verdict    TEXT NOT NULL,
                basis      TEXT NOT NULL DEFAULT '',
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


def _cached(words: list[str]) -> dict[str, tuple[str, str]]:
    from backend.database import get_db_connection_context
    out: dict[str, tuple[str, str]] = {}
    with get_db_connection_context() as conn:
        ensure_origin_cache_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT word, verdict, basis FROM bt_3_word_origin_cache WHERE word = ANY(%s);",
                ([w.strip() for w in words],),
            )
            for word, verdict, basis in cur.fetchall():
                out[word] = (verdict, basis or "")
        conn.commit()
    return out


def _remember(rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        ensure_origin_cache_schema(conn)
        with conn.cursor() as cur:
            for word, verdict, basis in rows:
                cur.execute(
                    """
                    INSERT INTO bt_3_word_origin_cache (word, verdict, basis, checked_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (word) DO UPDATE
                       SET verdict = EXCLUDED.verdict, basis = EXCLUDED.basis,
                           checked_at = NOW();
                    """,
                    (word, verdict, basis[:300]),
                )
        conn.commit()


# --- публичное ------------------------------------------------------------


def origin_of(words, *, use_cache: bool = True) -> dict[str, tuple[str, str]]:
    """{слово: (вердикт, на каком основании)} для списка слов.

    Вердикты: ANGLICISM / OTHER / UNKNOWN. UNKNOWN означает «оба справочника
    промолчали» — такое слово НЕ снимается, оно идёт в счётчик и в отчёт."""
    wanted = [str(w).strip() for w in (words or []) if str(w).strip()]
    if not wanted:
        return {}
    result: dict[str, tuple[str, str]] = _cached(wanted) if use_cache else {}
    todo = [w for w in wanted if w not in result]
    fresh: list[tuple[str, str, str]] = []
    for i in range(0, len(todo), _BATCH):
        chunk = todo[i:i + _BATCH]
        de_pages = _fetch_wikitext(_DE_API, chunk)
        need_en: list[str] = []
        for word in chunk:
            herkunft = _herkunft_of(word, de_pages.get(word))
            if herkunft and judge_herkunft(word, herkunft):
                result[word] = (ANGLICISM, "de.wiktionary Herkunft: " + herkunft[:160])
            elif herkunft and not _HERKUNFT_UNSOURCED.search(herkunft):
                result[word] = (OTHER, "de.wiktionary Herkunft: " + herkunft[:160])
            else:
                # Раздела нет — либо справочник сам пометил его неподтверждённым.
                # И то и другое означает «здесь ответа нет», спрашиваем второй источник.
                need_en.append(word)
        if need_en:
            time.sleep(_PAUSE_BETWEEN_BATCHES_SECONDS)
            en_pages = _fetch_wikitext(_EN_API, need_en)
            for word in need_en:
                direction = _en_direction(en_pages.get(word))
                if direction is True:
                    result[word] = (ANGLICISM, "en.wiktionary: заимствование de←en")
                elif direction is False:
                    result[word] = (OTHER, "en.wiktionary: другая этимология")
                else:
                    result[word] = (UNKNOWN, "оба справочника молчат о происхождении")
        # В кэш идут только ОТВЕТЫ. «Не знаем» не кэшируется никогда: иначе одно
        # молчание справочника застывает навсегда и слово больше не переспросят —
        # незакрытая задача превратится в «проверено».
        fresh.extend((w, result[w][0], result[w][1])
                     for w in chunk if w in result and result[w][0] != UNKNOWN)
        # Кэш пишем пачками по ходу, а не в конце: прогон по всему банку идёт
        # десятки минут, и обрыв на 4 000-м слове не должен стирать 4 000 ответов.
        if fresh:
            try:
                _remember(fresh)
            except Exception:
                logging.warning("article_anglicism: кэш происхождения не записался", exc_info=True)
            fresh = []
        time.sleep(_PAUSE_BETWEEN_BATCHES_SECONDS)
    return result


def everyday_ranks(words) -> dict[str, int]:
    """{слово: место в частотном списке}. Слова нет в списке → ключа нет."""
    from backend.database import get_db_connection_context
    wanted = [str(w).strip() for w in (words or []) if str(w).strip()]
    if not wanted:
        return {}
    out: dict[str, int] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lemma, rank FROM bt_3_word_frequency WHERE lemma = ANY(%s);",
                ([w.lower() for w in wanted],),
            )
            ranks = dict(cur.fetchall())
    for word in wanted:
        rank = ranks.get(word.lower())
        if rank is not None:
            out[word] = int(rank)
    return out


def tail_anglicisms(words, *, tail_rank: int = TAIL_RANK) -> dict[str, dict]:
    """Слова, которые НЕ должны попадать в банк артиклей.

    → {слово: {"verdict", "basis", "rank"}} только для тех, кого сняли/не пускаем.
    Слово попадает сюда, ТОЛЬКО когда справочник назвал его англицизмом и оно лежит
    за границей ходового языка. Всё остальное (включая «не знаем») сюда не попадает."""
    origins = origin_of(words)
    ranks = everyday_ranks(words)
    hits: dict[str, dict] = {}
    for word, (verdict, basis) in origins.items():
        if verdict != ANGLICISM:
            continue
        rank = ranks.get(word)
        if rank is None or rank > tail_rank:
            hits[word] = {"verdict": verdict, "basis": basis, "rank": rank}
    return hits


def origin_report_lines() -> list[str]:
    """Строки для регулярного отчёта владельцу: происхождение слов банка ЧИСЛОМ.

    Пункт правила: «не знаем» — это не тишина, а счётчик и наряд на работу. Без
    этих строк слова, о происхождении которых справочник промолчал, лежали бы в
    игре бессрочно и незаметно — ровно так и жила «die Sync».

    Кэш `bt_3_word_origin_cache` хранит ТОЛЬКО ответы (молчание в него не пишется),
    поэтому «нет строки в кэше» и означает «спросили и не узнали, либо ещё не
    спрашивали». И то и другое — незакрытая задача, а не норма."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        ensure_origin_cache_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FILTER (WHERE c.word IS NULL)                     AS молчит,
                       COUNT(*) FILTER (WHERE c.verdict = %s)                     AS англицизмы,
                       COUNT(*)                                                   AS живых
                  FROM (SELECT DISTINCT word FROM bt_3_article_sprint_nouns
                         WHERE retired = FALSE AND verified = TRUE) n
                  LEFT JOIN bt_3_word_origin_cache c ON c.word = n.word;
                """, (ANGLICISM,))
            silent, anglicisms, live = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM bt_3_article_sprint_nouns "
                "WHERE retired = TRUE AND retire_reason = 'англицизм вне ходового языка';")
            retired = int(cur.fetchone()[0] or 0)
        conn.commit()
    lines = ["", "<b>Происхождение слов</b>",
             f"• снято как английские слова вне живой речи: {retired}",
             f"• остались как ходовые заимствования (der Bus, der Film): {int(anglicisms or 0)}"]
    if silent:
        lines.append(f"• ⚠️ справочник молчит о происхождении: {int(silent)} из {int(live)} — "
                     f"эти слова в игре, но их происхождение мы не знаем")
    return lines


def origin_counters(words) -> dict[str, int]:
    """Сколько слов в каждом состоянии — чтобы «не знаем» было ЧИСЛОМ, а не тишиной."""
    origins = origin_of(words)
    counters = {ANGLICISM: 0, OTHER: 0, UNKNOWN: 0}
    for verdict, _ in origins.values():
        counters[verdict] = counters.get(verdict, 0) + 1
    return counters
