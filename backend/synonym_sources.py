"""Источники истины для синонимов: OpenThesaurus (офлайн) и de.wiktionary {{Synonyme}}.

Зачем. Список синонимов спринта пишет модель, и до 06.09.2026 он шёл в базу без единой
проверки: у «die Gelegenheit» лежало «die Option» шесть раз, «rememembern» числился
синонимом «sich erinnern», «die Potenzial» — с неверным артиклем. Правило ноль запрещает
и выдумывать, и молчать: ответ берётся из источника, источник называется вслух.

Два источника, оба открытые, оба читаются здесь и больше нигде:

1. OpenThesaurus (https://www.openthesaurus.de, LGPL 2.1) — 48 484 гнезда синонимов,
   лежат в `bt_3_openthesaurus_synsets`, грузит `scripts/load_openthesaurus.py`.
   Подтверждением считается ТОЛЬКО одно гнездо на оба слова. Связь «через соседнее гнездо»
   (Chance ~ Möglichkeit ~ Option) — вывод по графу, а не утверждение словаря; замер
   06.09.2026 давал ею +87 пар из 405, но это уже наша догадка, и она не источник.
2. de.wiktionary, секция {{Synonyme}} немецкого раздела статьи — кеш `bt_3_wiktionary_synonyms`
   (в нём же {{Gegenwörter}}, раз страница всё равно скачана). Ходок тот же, что у рода:
   `article_wiktionary_ref._fetch_wikitext`, пачками по 45, с повторами на 429/503.

Покрытие у обоих дырявое, и это ИЗМЕРЕНО (06.09.2026): у OpenThesaurus нет гнезда для
«erzählen», у Wiktionary у «Gelegenheit» всего два синонима. Поэтому вердикт этого модуля —
«подтверждено тем-то» либо «не подтверждено», а «не подтверждено» ≠ «не синоним»: такие
пары уходят владельцу с кнопками (`sprint_accepted_review.py`), а не выбрасываются молча.
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field

_ARTICLES = {"der", "die", "das"}

# ── нормализация ключа ────────────────────────────────────────────────────────
#
# Ключ — слово без артикля и без возвратного «sich», в нижнем регистре, без пометок в
# скобках. OpenThesaurus пишет «(sich) erinnern», «Option (fachspr.)»; наш банк — «sich
# erinnern», «die Option». Умляуты НЕ разворачиваем: schon ≠ schön, а в источниках
# написание всегда полное.


def term_key(term: str) -> str:
    t = re.sub(r"\([^)]*\)", " ", str(term or ""))
    t = re.sub(r"\s+", " ", t).strip().lower()
    toks = t.split(" ") if t else []
    if toks and toks[0] in _ARTICLES:
        toks = toks[1:]
    if toks and toks[0] == "sich":
        toks = toks[1:]
    return " ".join(toks)


# ── OpenThesaurus ─────────────────────────────────────────────────────────────

def ensure_openthesaurus_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_openthesaurus_synsets (
                    synset_id  INTEGER NOT NULL,
                    term       TEXT    NOT NULL,
                    term_key   TEXT    NOT NULL,
                    PRIMARY KEY (synset_id, term)
                );
                CREATE INDEX IF NOT EXISTS bt_3_openthesaurus_synsets_key
                    ON bt_3_openthesaurus_synsets (term_key);
                """
            )
        conn.commit()


def openthesaurus_loaded() -> bool:
    """Есть ли выгрузка в базе. Без неё дверь синонимов работать НЕ ИМЕЕТ ПРАВА: пустая
    таблица неотличима от «словарь не знает», и всё уходило бы владельцу как спорное."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('bt_3_openthesaurus_synsets')")
            if not (cur.fetchone() or [None])[0]:
                return False
            cur.execute("SELECT 1 FROM bt_3_openthesaurus_synsets LIMIT 1")
            return cur.fetchone() is not None


def openthesaurus_synsets(term: str) -> set[int]:
    from backend.database import get_db_connection_context
    key = term_key(term)
    if not key:
        return set()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT synset_id FROM bt_3_openthesaurus_synsets WHERE term_key = %s", (key,))
            return {int(r[0]) for r in cur.fetchall() or []}


def openthesaurus_knows(term: str) -> bool:
    return bool(openthesaurus_synsets(term))


# ── de.wiktionary {{Synonyme}} / {{Gegenwörter}} ──────────────────────────────

# Немецкий раздел страницы: на одном заголовке живут и другие языки (образец —
# article_anglicism._DE_SECTION).
_DE_SECTION = re.compile(
    r"==\s*(?P<title>[^=]*?)\s*\(\{\{Sprache\|Deutsch\}\}\)\s*==(?P<body>.*?)(?=\n==\s[^=]|\Z)",
    re.DOTALL,
)
# Секция кончается на следующем шаблоне-заголовке в начале строки ({{Beispiele}}, …).
_SECTION = {
    "synonym": re.compile(r"\{\{Synonyme\}\}(.*?)(?=\n\{\{[A-ZÄÖÜ][^}]*\}\}|\n==|\Z)", re.DOTALL),
    "antonym": re.compile(r"\{\{Gegenwörter\}\}(.*?)(?=\n\{\{[A-ZÄÖÜ][^}]*\}\}|\n==|\Z)", re.DOTALL),
}
_LINK = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]*)?\]\]")


def parse_wiktionary_relations(wikitext: str | None) -> dict[str, list[str]] | None:
    """{'synonym': [...], 'antonym': [...]} из немецкого раздела; None — страницы нет.

    Берём ТОЛЬКО ссылки [[...]] внутри секций: голый текст там — пометки («veraltet»,
    «umgangssprachlich»), а не слова. Порядок сохраняем, дубли внутри секции схлопываем."""
    if wikitext is None:
        return None
    out: dict[str, list[str]] = {"synonym": [], "antonym": []}
    for m in _DE_SECTION.finditer(wikitext):
        body = m.group("body")
        for rel, rx in _SECTION.items():
            for sec in rx.finditer(body):
                for link in _LINK.findall(sec.group(1)):
                    w = link.strip()
                    if w and w not in out[rel]:
                        out[rel].append(w)
    return out


def ensure_wiktionary_synonyms_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_wiktionary_synonyms (
                    title      TEXT PRIMARY KEY,
                    missing    BOOLEAN NOT NULL DEFAULT FALSE,
                    synonyms   JSONB NOT NULL DEFAULT '[]'::jsonb,
                    antonyms   JSONB NOT NULL DEFAULT '[]'::jsonb,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()


def _page_title(term: str) -> str:
    """Заголовок статьи Wiktionary: без артикля и без «sich» («sich erinnern» → страницы
    нет, есть «erinnern»), регистр слова как в банке."""
    t = re.sub(r"\s+", " ", str(term or "")).strip()
    toks = t.split(" ")
    if toks and toks[0].lower() in _ARTICLES:
        toks = toks[1:]
    if toks and toks[0].lower() == "sich":
        toks = toks[1:]
    return " ".join(toks)


@dataclass
class WiktionaryRelations:
    title: str
    missing: bool
    synonyms: list[str] = field(default_factory=list)
    antonyms: list[str] = field(default_factory=list)


def wiktionary_relations(terms: list[str], *, allow_network: bool = True,
                         pause_sec: float = 1.5) -> dict[str, WiktionaryRelations | None]:
    """{term: WiktionaryRelations} — сперва кеш, недостающее — сетью пачками по 45.

    None у термина означает «не удалось узнать» (сеть не ответила или сеть запрещена),
    и это НЕ то же самое, что missing=True («страницы нет»). Вызывающий обязан различать:
    «не узнали» — не решать, «страницы нет» — источник молчит честно."""
    from backend.database import get_db_connection_context
    ensure_wiktionary_synonyms_schema()
    titles = {t: _page_title(t) for t in terms if _page_title(t)}
    out: dict[str, WiktionaryRelations | None] = {t: None for t in terms}
    if not titles:
        return out
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title, missing, synonyms, antonyms FROM bt_3_wiktionary_synonyms "
                "WHERE title = ANY(%s)", (sorted(set(titles.values())),),
            )
            cached = {str(r[0]): WiktionaryRelations(str(r[0]), bool(r[1]),
                                                     list(r[2] or []), list(r[3] or []))
                      for r in cur.fetchall() or []}
    for term, title in titles.items():
        if title in cached:
            out[term] = cached[title]
    todo = sorted({title for title in titles.values() if title not in cached})
    if not todo:
        return out
    if not allow_network:
        return out
    from backend.article_wiktionary_ref import _BATCH, _fetch_wikitext
    fetched: dict[str, WiktionaryRelations] = {}
    for i in range(0, len(todo), _BATCH):
        batch = todo[i:i + _BATCH]
        pages = _fetch_wikitext(batch)
        # Пустой словарь — сеть не ответила (ходок это уже залогировал). Не «страницы
        # нет»: такое в кеш не пишем, иначе один сбой сети навсегда станет «не знаем».
        if not pages:
            continue
        # Ходок ключует по нормализованному заголовку; запрошенный может отличаться
        # регистром или редиректом — сверяем без регистра.
        low = {k.lower(): v for k, v in pages.items()}
        for title in batch:
            if title not in pages and title.lower() not in low:
                continue
            wt = pages.get(title, low.get(title.lower()))
            rel = parse_wiktionary_relations(wt)
            if rel is None:
                fetched[title] = WiktionaryRelations(title, True)
            else:
                fetched[title] = WiktionaryRelations(title, False, rel["synonym"], rel["antonym"])
        if i + _BATCH < len(todo):
            time.sleep(pause_sec)
    if fetched:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO bt_3_wiktionary_synonyms (title, missing, synonyms, antonyms, fetched_at) "
                    "VALUES (%s, %s, %s::jsonb, %s::jsonb, NOW()) "
                    "ON CONFLICT (title) DO UPDATE SET missing = EXCLUDED.missing, "
                    "synonyms = EXCLUDED.synonyms, antonyms = EXCLUDED.antonyms, fetched_at = NOW()",
                    [(r.title, r.missing, json.dumps(r.synonyms, ensure_ascii=False),
                      json.dumps(r.antonyms, ensure_ascii=False))
                     for r in sorted(fetched.values(), key=lambda x: x.title)],
                )
            conn.commit()
    for term, title in titles.items():
        if title in fetched:
            out[term] = fetched[title]
    return out


# ── единый вердикт ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Confirmation:
    confirmed: bool
    # Кто подтвердил — по именам, чтобы в очереди и отчёте был виден источник:
    # 'openthesaurus', 'wiktionary', 'indirect' (косвенная антонимия, см. ниже). Пусто — никто.
    by: tuple[str, ...]
    # Что источники вообще знают об этой паре — для очереди и для проверки существования.
    ot_knows_target: bool
    ot_knows_candidate: bool
    wikt_target: str      # 'listed' | 'not_listed' | 'no_page' | 'unknown'
    wikt_candidate: str
    # Через какое слово подтверждена косвенная антонимия («unfrei» через «abhängig»).
    via: str = ""

    @property
    def exists_in_dictionaries(self) -> bool | None:
        """Есть ли кандидат хоть в одном словаре. None — не удалось проверить (сеть):
        это НЕ «нет», такой кандидат идёт судье. Владелец 08.09.2026: судья впустил
        «befehlsgebunden», которого нет ни в Duden, ни в DWDS, ни в Wiktionary."""
        if self.ot_knows_candidate:
            return True
        if self.wikt_candidate == "unknown":
            return None
        return self.wikt_candidate != "no_page"


def confirm_relation(target: str, candidates: list[str], *, relation: str = "synonym",
                     allow_network: bool = True) -> dict[str, Confirmation]:
    """{candidate: Confirmation}. Симметрично: пара подтверждена, если (синонимы) target и
    candidate делят гнездо OpenThesaurus, ИЛИ candidate стоит в {{Synonyme}} статьи target,
    ИЛИ target — в {{Synonyme}} статьи candidate; (антонимы) то же по {{Gegenwörter}}.

    Антонимы, второй источник (владелец 08.09.2026, «только из словарей, никаких
    приставок») — КОСВЕННАЯ АНТОНИМИЯ, как в WordNet/GermaNet: X — противоположность
    цели по Wiktionary, кандидат лежит с X в одном гнезде OpenThesaurus ⇒ кандидат —
    антоним цели (`by` += 'indirect', `via` = X). Симметрично: противоположность
    КАНДИДАТА по Wiktionary в одном гнезде с ЦЕЛЬЮ. Оговорка записана в стратегии:
    гнёзда OpenThesaurus разбиты по значению, но какое из гнёзд головы отвечает значению
    цели, словари не говорят — берутся все гнёзда головы.
    OpenThesaurus сам по себе антонимов не знает; для антонимов его гнёзда нужны только
    для косвенного шага и для проверки, что слово вообще существует."""
    is_syn = relation == "synonym"
    t_sets = openthesaurus_synsets(target)
    wikt = wiktionary_relations([target, *candidates], allow_network=allow_network)
    tkey = term_key(target)
    w_t = wikt.get(target)

    def _list(rel: WiktionaryRelations | None) -> list[str]:
        if not rel or rel.missing:
            return []
        return rel.synonyms if is_syn else rel.antonyms

    t_syn_keys = {term_key(s) for s in _list(w_t)}
    # Косвенный шаг: гнёзда OpenThesaurus каждой прямой противоположности цели.
    head_sets: dict[str, set[int]] = {}
    if not is_syn:
        for head in _list(w_t):
            sets = openthesaurus_synsets(head)
            if sets:
                head_sets[head] = sets
    out: dict[str, Confirmation] = {}
    for cand in candidates:
        ckey = term_key(cand)
        c_sets = openthesaurus_synsets(cand)
        by: list[str] = []
        via = ""
        if is_syn and t_sets and c_sets and (t_sets & c_sets):
            by.append("openthesaurus")
        w_c = wikt.get(cand)
        c_syn_keys = {term_key(s) for s in _list(w_c)}
        if ckey in t_syn_keys or tkey in c_syn_keys:
            by.append("wiktionary")
        if not is_syn and not by:
            # кандидат — синоним прямой противоположности цели …
            for head, sets in head_sets.items():
                if c_sets & sets:
                    by.append("indirect"); via = head
                    break
            # … либо цель — синоним прямой противоположности кандидата.
            if not by and t_sets:
                for head in _list(w_c):
                    if openthesaurus_synsets(head) & t_sets:
                        by.append("indirect"); via = head
                        break

        def _state(rel: WiktionaryRelations | None, other_key: str, keys: set[str]) -> str:
            if rel is None:
                return "unknown"
            if rel.missing:
                return "no_page"
            return "listed" if other_key in keys else "not_listed"

        out[cand] = Confirmation(
            confirmed=bool(by), by=tuple(by),
            ot_knows_target=bool(t_sets), ot_knows_candidate=bool(c_sets),
            wikt_target=_state(w_t, ckey, t_syn_keys),
            wikt_candidate=_state(w_c, tkey, c_syn_keys),
            via=via,
        )
    return out


def confirm_synonyms(target: str, candidates: list[str], *, allow_network: bool = True
                     ) -> dict[str, Confirmation]:
    return confirm_relation(target, candidates, relation="synonym", allow_network=allow_network)
