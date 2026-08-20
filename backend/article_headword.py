# -*- coding: utf-8 -*-
"""Заголовок карточки артиклей обязан быть СЛОВАРНОЙ ФОРМОЙ — проверка по справочнику.

ЗАЧЕМ. Игра спрашивает «der / die / das?». У формы множественного числа артикль
ВСЕГДА die — знать там нечего, вопрос остаётся без содержания, а человек видит
«Band» и «Bänder» как два разных слова. Ещё хуже, когда артикль вклеен в сам
заголовок: карточка «die Die Feier» показывает ответ в вопросе.

ЧЕМ ЭТО ОТЛИЧАЕТСЯ ОТ ПРЕЖНЕГО ПОДХОДА. Разбор 16.08.2026 признал, что дешёвого
признака нет, и поставил на это ПРОВЕРЯЮЩУЮ МОДЕЛЬ большинством из трёх голосов —
потому что два опробованных признака врали:
  • «заголовок значится чужим полем мн. числа» ловил die Kohle (не мн. от der Kohl),
    die Montage (не от der Montag), der Westen (не от die Weste);
  • «своё поле мн. числа пустое» ловило законные die Schulden и das Streben.

Третий признак не пробовали, а он есть, и это не эвристика, а прямое утверждение
справочника: у формы словоизменения de.wiktionary заводит ОТДЕЛЬНУЮ страницу с
пометкой `{{Wortart|Deklinierte Form|Deutsch}}` и ссылкой `{{Grundformverweis}}` на
исходное слово. Слово со своей словарной статьёй помечено `{{Wortart|Substantiv}}`.

Замер 20.08.2026 на контрольном наборе:
  • ловит: Bänder→Band, Sorten→Sorte, Mängel→Mangel, Zitate→Zitat;
  • НЕ задевает pluralia tantum: die Eltern, die Leute, die Kosten, die Ferien —
    у них своя статья Substantiv, и справочник это говорит сам;
  • не задевает обычные слова: Computer, Bildschirm, Feier.

Это дешевле и устойчивее голосования модели: справочник на один и тот же вопрос
отвечает одинаково всегда, а модель 16.08.2026 давала 12% разнобоя на повторе.

ЧЕГО ЗДЕСЬ НЕТ. Ни одного правила «по хвосту слова» (-en, -er, -e как признак
множественного). Немецкое множественное образуется восемью разными способами и
совпадает с кучей единственных — выводить его правилом по написанию запрещено.
Если справочник молчит, вердикт «не знаем»: слово НЕ трогаем, но и «проверено» не
пишем — оно попадает в счётчик.
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request

_API = "https://de.wiktionary.org/w/api.php"
_UA = "DeutschBot/1.0 (Artikel headword check; contact via Telegram bot)"
_BATCH = 20
_RETRY_CODES = (429, 503)
_FETCH_RETRIES = 5
_FETCH_BACKOFF_SECONDS = 8
_PAUSE_BETWEEN_BATCHES_SECONDS = 0.6

# Вердикты.
LEMMA = "словарная форма"          # всё в порядке
DECLINED = "склонённая форма"      # это форма другого слова (мн. число, падеж)
GLUED_ARTICLE = "артикль в заголовке"
UNKNOWN = "справочник молчит"

_DE_SECTION = re.compile(
    r"==\s*[^=]*?\(\{\{Sprache\|Deutsch\}\}\)\s*==(.*?)(?=\n==\s[^=]|\Z)", re.DOTALL)
_WORTART_NOUN = re.compile(r"\{\{Wortart\|Substantiv\|Deutsch\}\}")
_WORTART_DECLINED = re.compile(r"\{\{Wortart\|Deklinierte Form\|Deutsch\}\}")
# {{Grundformverweis Dekl|Band}} / {{Grundformverweis|Nominativ|Plural|…|Sorte}}
_GRUNDFORM = re.compile(r"\{\{Grundformverweis[^|}]*\|(?:[^|}]*\|)*([^|}]+)\}\}")

# Артикль, вклеенный в заголовок: «Die Feier». Справочник для этого не нужен —
# слово из двух частей, первая из которых определённый артикль, словарной формой
# немецкого существительного не бывает.
_GLUED = re.compile(r"^(?:Der|Die|Das|der|die|das)\s+(\S.*)$")


def glued_article(word: str) -> str:
    """Заголовок вида «Die Feier» → «Feier». Пусто, если артикль не вклеен."""
    match = _GLUED.match(str(word or "").strip())
    return match.group(1).strip() if match else ""


def _fetch_wikitext(titles: list[str]) -> dict[str, str | None]:
    """Викитекст статей одним запросом. None = статьи нет.

    Сеть либо отвечает, либо мы честно падаем: «статьи нет» и «мы не смогли
    спросить» — разные вещи, и подменять второе первым нельзя."""
    params = {"action": "query", "prop": "revisions", "rvprop": "content",
              "rvslots": "main", "format": "json", "formatversion": "2",
              "titles": "|".join(titles)}
    request = urllib.request.Request(_API + "?" + urllib.parse.urlencode(params),
                                     headers={"User-Agent": _UA})
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
        except Exception as exc:
            last_error = exc
            time.sleep(_FETCH_BACKOFF_SECONDS * (attempt + 1))
    else:
        raise RuntimeError(f"de.wiktionary не ответил за {_FETCH_RETRIES} попыток: {last_error}")
    out: dict[str, str | None] = {}
    for page in payload.get("query", {}).get("pages", []):
        title = page.get("title") or ""
        out[title] = None if page.get("missing") else page["revisions"][0]["slots"]["main"]["content"]
    for item in payload.get("query", {}).get("normalized", []):
        if item.get("to") in out:
            out[item.get("from")] = out[item["to"]]
    return out


def judge_page(wikitext: str | None) -> tuple[str, str]:
    """(вердикт, исходное слово) по викитексту статьи."""
    if wikitext is None:
        return UNKNOWN, ""
    body = "".join(m.group(1) for m in _DE_SECTION.finditer(wikitext))
    if not body:
        return UNKNOWN, ""
    # Своя словарная статья существительного перевешивает всё: у некоторых написаний
    # есть И статья слова, И статья формы (der Band / die Bände — «Bände» только форма,
    # а «Band» — полноценное слово в трёх родах).
    if _WORTART_NOUN.search(body):
        return LEMMA, ""
    if _WORTART_DECLINED.search(body):
        ref = _GRUNDFORM.search(body)
        return DECLINED, (ref.group(1).strip() if ref else "")
    return UNKNOWN, ""


# Таблица кэша создаётся один раз на процесс, а не перед каждой пачкой: лишний
# поход в базу — лишняя точка отказа (проверено на страже происхождения 20.08.2026).
_schema_ready = False


def ensure_headword_cache_schema(conn, *, force: bool = False) -> None:
    global _schema_ready
    if _schema_ready and not force:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bt_3_word_headword_cache (
                word       TEXT PRIMARY KEY,
                verdict    TEXT NOT NULL,
                lemma      TEXT NOT NULL DEFAULT '',
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    _schema_ready = True


def _cached(words: list[str]) -> dict[str, tuple[str, str]]:
    from backend.database import get_db_connection_context
    out: dict[str, tuple[str, str]] = {}
    with get_db_connection_context() as conn:
        ensure_headword_cache_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT word, verdict, lemma FROM bt_3_word_headword_cache "
                        "WHERE word = ANY(%s);", ([w.strip() for w in words],))
            for word, verdict, lemma in cur.fetchall():
                out[word] = (verdict, lemma or "")
        conn.commit()
    return out


def _remember(rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        ensure_headword_cache_schema(conn)
        with conn.cursor() as cur:
            for word, verdict, lemma in rows:
                cur.execute(
                    """
                    INSERT INTO bt_3_word_headword_cache (word, verdict, lemma, checked_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (word) DO UPDATE
                       SET verdict = EXCLUDED.verdict, lemma = EXCLUDED.lemma, checked_at = NOW();
                    """, (word, verdict, lemma[:120]))
        conn.commit()


def headword_verdicts(words, *, use_cache: bool = True) -> dict[str, tuple[str, str]]:
    """{слово: (вердикт, исходное слово)}.

    UNKNOWN означает «справочник промолчал» — слово НЕ трогаем, но и проверенным
    не считаем: оно идёт в счётчик и в отчёт."""
    wanted = [str(w).strip() for w in (words or []) if str(w).strip()]
    if not wanted:
        return {}
    result: dict[str, tuple[str, str]] = {}
    ask: list[str] = []
    for word in wanted:
        stripped = glued_article(word)
        if stripped:
            # Справочник тут не нужен и спрашивать его не о чем: «Die Feier» —
            # это не написание немецкого слова, это слово со статьёй впереди.
            result[word] = (GLUED_ARTICLE, stripped)
        else:
            ask.append(word)
    if use_cache and ask:
        result.update(_cached(ask))
    todo = [w for w in ask if w not in result]
    for i in range(0, len(todo), _BATCH):
        chunk = todo[i:i + _BATCH]
        pages = _fetch_wikitext(chunk)
        fresh: list[tuple[str, str, str]] = []
        for word in chunk:
            verdict, lemma = judge_page(pages.get(word))
            result[word] = (verdict, lemma)
            # «Не знаем» не кэшируем: одно молчание справочника не должно застыть
            # навсегда и превратиться в «проверено».
            if verdict != UNKNOWN:
                fresh.append((word, verdict, lemma))
        if fresh:
            try:
                _remember(fresh)
            except Exception:
                logging.warning("article_headword: кэш заголовков не записался", exc_info=True)
        time.sleep(_PAUSE_BETWEEN_BATCHES_SECONDS)
    return result


def bad_headwords(words) -> dict[str, dict]:
    """Только негодные заголовки: {слово: {"verdict", "lemma"}}.

    «Справочник молчит» сюда НЕ попадает: недоказанное не равно доказанному."""
    out: dict[str, dict] = {}
    for word, (verdict, lemma) in headword_verdicts(words).items():
        if verdict in (DECLINED, GLUED_ARTICLE):
            out[word] = {"verdict": verdict, "lemma": lemma}
    return out
