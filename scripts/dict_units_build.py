# -*- coding: utf-8 -*-
"""Сборка слоя ЕДИНИЦ из общего банка слов.

Читает bt_3_dictionary_entries и раскладывает его на четыре новые таблицы
(см. backend/lex_units_schema.sql): единицы, указатели, связи-переводы и
происхождение. СТАРЫЙ БАНК НЕ МЕНЯЕТСЯ — ни одна его строка не правится и не
удаляется, на него только ставятся ссылки.

Правила опознания:
  • слово        — одна лемма + часть речи + род («der Kiefer» ≠ «die Kiefer»,
                   но «Rüpel» = «der Rüpel»);
  • сочетание и предложение — сравниваются целиком, ни с чем не сливаются;
  • перевод      — связь единицы с единицей, поэтому обратное направление
                   («враг» → der Feind) работает само, без обратного поиска.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_build.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_build.py --apply
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import time

import psycopg2
import psycopg2.extras

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
SPACE_RE = re.compile(r"\s+")
SENTENCE_END_RE = re.compile(r"[.!?…]\s*$")
# Русский глагол в неопределённой форме и прилагательное — самый надёжный признак
# части речи, когда немецкая сторона записана с заглавной и Wiktionary отдаёт
# существительное («Wählen» → das Wählen, хотя у нас это глагол из начала фразы).
# Перед «-ть» у глагола стоит гласная (выбира-ть, работа-ть). У существительных на
# «-сть» её нет (челюс-ть, час-ть, новос-ть) — без этой оговорки «Kiefer/челюсть»
# уезжает в глаголы.
RU_VERB_RE = re.compile(r"(?:[аеёиоуыэюя]ть|ться|тись|чь)$", re.I)
# Существительные, неотличимые по окончанию от глагола: «путь», «печь», «речь».
# Без списка «Pfad ← Путь» и «Mikrowelle ← Микроволновая печь» уезжают в глаголы.
RU_NOUN_LOOKALIKES = {
    "путь", "печь", "мать", "дочь", "ночь", "речь", "вещь", "часть", "честь",
    "смерть", "жизнь", "соль", "боль", "роль", "цель", "дверь", "зверь", "тень",
    "степь", "сеть", "весть", "власть", "гость", "кость", "кровь", "любовь",
    "мышь", "осень", "память", "площадь", "связь", "совесть", "цепь", "челюсть",
    "новость", "скорость", "радость", "молодость", "мощь", "помощь", "рожь",
}
RU_ADJ_RE = re.compile(r"(ый|ий|ой|ая|яя|ое|ее|ые|ие)$", re.I)
SPLIT_GLOSS_RE = re.compile(r"\s*[;,/]\s*|\s+—\s+")

GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}
NOT_NOUN_POS = {
    "verb", "adverb", "adjective", "preposition", "particle",
    "pronoun", "conjunction", "numeral", "interjection", "participle",
}


def connect(dsn: str):
    last = None
    for attempt in range(6):
        try:
            return psycopg2.connect(dsn, connect_timeout=20)
        except Exception as exc:
            last = exc
            print("  переподключение %d/6: %s" % (attempt + 1, exc))
            time.sleep(5)
    raise SystemExit("база недоступна: %s" % last)


def norm(text: str) -> str:
    return SPACE_RE.sub(" ", (text or "").strip()).casefold()


def strip_article(text: str) -> str:
    return ARTICLE_RE.sub("", (text or "").strip()).strip()


def article_of(text: str) -> str:
    m = ARTICLE_RE.match((text or "").strip())
    return m.group(1).lower() if m else ""


def kind_of(text: str) -> str:
    body = strip_article(text)
    if not body:
        return ""
    if " " not in body:
        return "word"
    return "sentence" if SENTENCE_END_RE.search(body) or len(body.split()) > 4 else "collocation"


class UnitStore:
    """Копит единицы в памяти; ключ опознания — (язык, вид, лемма, часть речи, род)."""

    def __init__(self) -> None:
        self.units: dict[tuple, dict] = {}

    def get(self, *, lang, kind, lemma, pos=None, gender=None,
            pos_source=None, gender_source=None, display=None) -> dict:
        key = (lang, kind, norm(lemma), pos or "", gender or "")
        unit = self.units.get(key)
        if unit is None:
            unit = {
                "lang": lang, "kind": kind, "lemma": lemma.strip(), "lemma_key": norm(lemma),
                "pos": pos, "pos_source": pos_source, "gender": gender,
                "gender_source": gender_source,
                "display": display or ((gender + " " + lemma.strip()) if gender else lemma.strip()),
                "card": None, "card_source": None,
                "surfaces": set(), "sources": set(), "key": key,
            }
            self.units[key] = unit
        return unit


def resolve_pos(*, lemma: str, surfaces: set[str], pool_pos: collections.Counter,
                wikt: dict[str, tuple[list[str], str]], glosses: list[str]) -> tuple[str | None, str | None]:
    """Часть речи: показания Wiktionary + форма русского перевода + то, что знал пул.

    Русская сторона решает споры: если перевод «выбирать», то немецкое «Wählen»
    у нас — глагол, а не существительное «das Wählen», как отдала бы страница
    с заглавной буквы."""
    gloss_says = None
    for g in glosses:
        last_word = g.strip().split()[-1].casefold() if g.strip() else ""
        if last_word in RU_NOUN_LOOKALIKES:
            continue
        if RU_VERB_RE.search(g.strip()):
            gloss_says = "verb"
            break
        if RU_ADJ_RE.search(g.strip()) and gloss_says is None:
            gloss_says = "adjective"

    wikt_pos: list[str] = []
    for title in {lemma, lemma[:1].upper() + lemma[1:], lemma[:1].lower() + lemma[1:]}:
        got = wikt.get(title)
        if got:
            for p in got[0]:
                if p not in wikt_pos:
                    wikt_pos.append(p)

    meaningful = [p for p in wikt_pos if p not in {"deklinierte form", "konjugierte form"}]
    # Русский перевод — только разрешение спора между показаниями Wiktionary,
    # самостоятельным доводом он быть не может: «челюсть» кончается как глагол.
    if gloss_says and gloss_says in meaningful:
        return gloss_says, "wiktionary+перевод"
    if len(meaningful) == 1:
        return meaningful[0], "wiktionary"
    if pool_pos:
        top = pool_pos.most_common(1)[0][0]
        if top and top not in {"other", "unknown"}:
            return top, "пул"
    if meaningful:
        return meaningful[0], "wiktionary"
    if gloss_says:
        return gloss_says, "перевод"
    return None, None


UMLAUT_FOLD = str.maketrans({"ä": "a", "ö": "o", "ü": "u", "ß": "s"})


def fold(text: str) -> str:
    """«Bäume» → «baume»: множественное число часто с умляутом, и без сворачивания
    сравнение формы с леммой ложно давало бы «это про другое слово»."""
    return norm(text).translate(UMLAUT_FOLD)


def card_matches_lemma(card: dict, lemma_key: str) -> bool:
    """Разбор принадлежит слову, только если СОДЕРЖИМОЕ разбора про это слово.

    Заголовка мало: сломанная запись «Грубиян → der Flegel» несёт правильный
    заголовок «der Flegel», а формы и примеры внутри — от «der Rüpel». Поэтому
    сверяем именно формы: множественное число и родительный падеж обязаны
    начинаться с той же основы, что и лемма."""
    if not isinstance(card, dict):
        return False
    stem = fold(lemma_key)[:4]
    if not stem:
        return False
    forms = card.get("forms")
    if isinstance(forms, dict):
        values = [v for v in forms.values() if isinstance(v, str) and v.strip()]
        if values and not any(stem in fold(strip_article(v)) for v in values):
            return False  # формы про другое слово — брать такой разбор нельзя
    return True


def resolve_gender(*, lemma: str, pool_articles: set[str],
                   wikt: dict[str, tuple[list[str], str]], genus: dict[str, str]) -> tuple[str | None, str | None]:
    """Род существительного. Wiktionary старше пула: род оттуда может перекрыть
    род от GPT, обратно — никогда."""
    codes = []
    for title in {lemma, lemma[:1].upper() + lemma[1:]}:
        got = wikt.get(title)
        if got and got[1] and got[1] != "-":
            codes.append(got[1])
        g = genus.get(title)
        if g and g != "-":
            codes.append(g)
    for code in codes:
        arts = {GENUS_TO_ARTICLE[c] for c in code if c in GENUS_TO_ARTICLE}
        if len(arts) == 1:
            return arts.pop(), "wiktionary"
        if len(arts) > 1 and pool_articles & arts:
            # двуродовое слово: оставляем тот род, который у нас реально встречался
            return sorted(pool_articles & arts)[0], "wiktionary+пул"
    if len(pool_articles) == 1:
        return next(iter(pool_articles)), "пул"
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="только посчитать, ничего не писать")
    ap.add_argument("--apply", action="store_true", help="создать таблицы и записать слой")
    ap.add_argument("--show", action="append", default=[],
                    help="показать, что вышло по слову (можно несколько раз)")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")
    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    print("читаю справочники…")
    cur.execute("SELECT title, pos_list, genus FROM bt_3_wiktionary_pos_cache;")
    wikt = {t: ([p for p in (pl or "").split(",") if p], g) for t, pl, g in cur.fetchall()}
    cur.execute("SELECT title, genus FROM bt_3_wiktionary_genus_cache;")
    genus = dict(cur.fetchall())
    print("  Wiktionary: части речи %d, роды %d" % (len(wikt), len(genus)))

    print("читаю банк…")
    cur.execute(
        """
        SELECT id, source_lang, target_lang, source_text, target_text, response_json
        FROM bt_3_dictionary_entries
        WHERE COALESCE(source_text,'') <> '' AND COALESCE(target_text,'') <> '';
        """
    )
    rows = cur.fetchall()
    print("  строк: %d" % len(rows))

    # ── проход 1: собираем показания по каждой лемме ───────────────────────────
    pool_pos: dict[tuple, collections.Counter] = collections.defaultdict(collections.Counter)
    pool_articles: dict[tuple, set] = collections.defaultdict(set)
    glosses: dict[tuple, list] = collections.defaultdict(list)
    for _id, sl, tl, st, tt, rj in rows:
        rj = rj if isinstance(rj, dict) else {}
        for lang, text, other_lang, other in ((sl, st, tl, tt), (tl, tt, sl, st)):
            if kind_of(text) != "word":
                continue
            key = (lang, norm(strip_article(text)))
            art = article_of(text) or str(rj.get("article") or "").strip().lower()
            if art in GENUS_TO_ARTICLE.values():
                pool_articles[key].add(art)
            pos = str(rj.get("part_of_speech") or "").strip().lower()
            if pos:
                pool_pos[key][pos] += 1
            if lang == "de" and other_lang == "ru":
                for part in SPLIT_GLOSS_RE.split(other or ""):
                    if part.strip() and CYRILLIC_RE.search(part):
                        glosses[key].append(part.strip())

    # ── проход 2: строим единицы, указатели и связи ────────────────────────────
    store = UnitStore()
    links: dict[tuple, dict] = {}
    skipped = 0

    def unit_for(lang: str, text: str, entry_id: int, side: str, rj: dict):
        k = kind_of(text)
        if not k:
            return None
        body = strip_article(text)
        if k != "word":
            u = store.get(lang=lang, kind=k, lemma=body, display=body)
            u["surfaces"].add((norm(body), "exact"))
            u["sources"].add((entry_id, side))
            return u
        key = (lang, norm(body))
        pos = pos_src = gender = gender_src = None
        if lang == "de":
            pos, pos_src = resolve_pos(lemma=body, surfaces=set(), pool_pos=pool_pos[key],
                                       wikt=wikt, glosses=glosses[key])
            if pos == "noun" or (pos is None and pool_articles[key]):
                gender, gender_src = resolve_gender(lemma=body, pool_articles=pool_articles[key],
                                                    wikt=wikt, genus=genus)
                # Омограф: «der Kiefer» (челюсть) и «die Kiefer» (сосна) — РАЗНЫЕ слова.
                # Если у леммы в банке встречались оба рода, род берём из самой записи,
                # иначе оба смысла слиплись бы в одну единицу с одним артиклем.
                row_article = article_of(text) or str(rj.get("article") or "").strip().lower()
                if len(pool_articles[key]) > 1 and row_article in GENUS_TO_ARTICLE.values():
                    gender, gender_src = row_article, "запись"
                if gender and pos is None:
                    pos, pos_src = "noun", "род"
            if pos in NOT_NOUN_POS:
                gender, gender_src = None, None
        display = body
        if lang == "de" and gender:
            display = "%s %s" % (gender, body)
        elif lang == "de" and pos in NOT_NOUN_POS:
            # В немецком с заглавной пишутся только существительные. Слово могло осесть
            # в банке с большой буквы просто из начала фразы — показываем «wählen».
            display = body[:1].lower() + body[1:]
        elif lang != "de":
            # В банке слово могло осесть с заглавной просто потому, что стояло в начале
            # фразы. В русском существительные пишутся со строчной — показываем «враг».
            display = body[:1].lower() + body[1:]
        u = store.get(lang=lang, kind="word", lemma=body, pos=pos, gender=gender,
                      pos_source=pos_src, gender_source=gender_src, display=display)
        u["surfaces"].add((norm(body), "no_article" if article_of(text) else "exact"))
        if article_of(text):
            u["surfaces"].add((norm(text), "exact"))
        u["sources"].add((entry_id, side))
        # Разбор кладём самый полный из встретившихся — но ТОЛЬКО если он про это же
        # слово. Именно на этом сломался словарь: карточка «der Flegel» несла формы и
        # примеры от «der Rüpel», и она же была самой полной из строк слова.
        if isinstance(rj, dict) and rj and card_matches_lemma(rj, norm(body)):
            richness = sum(1 for f in ("forms", "pronunciation", "usage_examples",
                                       "meanings", "government_patterns") if rj.get(f))
            if richness and richness > (u.get("card_richness") or 0):
                u["card"], u["card_source"], u["card_richness"] = rj, "пул", richness
        return u

    def units_for_side(lang: str, text: str, entry_id: int, side: str, rj: dict) -> list[dict]:
        """Единицы одной стороны записи. Список через запятую — это НЕ словосочетание,
        а несколько слов: «грубиян, хам» лежит одной строкой, и по слову «хам» сегодня
        не найти ничего. Разбираем ДО создания единицы, иначе «грубиян, хам» осело бы
        отдельным «сочетанием», которого в языке не существует. Список бывает с ОБЕИХ
        сторон — «Челюсть, сосна → Kiefer» такая же запись, как «Rüpel → грубиян, хам»."""
        parts = [p.strip() for p in SPLIT_GLOSS_RE.split(strip_article(text)) if p.strip()]
        if len(parts) > 1 and all(kind_of(p) == "word" for p in parts):
            made = [unit_for(lang, p, entry_id, side, {}) for p in parts]
        else:
            made = [unit_for(lang, text, entry_id, side, rj)]
        return [u for u in made if u]

    for _id, sl, tl, st, tt, rj in rows:
        rj = rj if isinstance(rj, dict) else {}
        sources = units_for_side(sl, st, _id, "source", rj)
        targets = units_for_side(tl, tt, _id, "target", rj)
        if not sources or not targets:
            skipped += 1
            continue
        src = sources[0]
        # Слова из списка на исходной стороне — равноправные единицы, связываем каждое.
        for extra in sources[1:]:
            for t in targets:
                for a, b in ((extra, t), (t, extra)):
                    lk = (a["key"], b["key"])
                    if lk not in links:
                        links[lk] = {"rank": 20, "source": "пул"}
        for i, t in enumerate(targets):
            for a, b, rank in ((src, t, 10 + i), (t, src, 10 + i)):
                lk = (a["key"], b["key"])
                cur_link = links.get(lk)
                if cur_link is None or rank < cur_link["rank"]:
                    links[lk] = {"rank": rank, "source": "пул"}

    words = [u for u in store.units.values() if u["kind"] == "word"]
    de_words = [u for u in words if u["lang"] == "de"]
    print("\n── что получилось ──")
    print("  единиц всего:            %6d" % len(store.units))
    print("    слов:                  %6d  (немецких %d)" % (len(words), len(de_words)))
    print("    сочетаний:             %6d" % sum(1 for u in store.units.values() if u["kind"] == "collocation"))
    print("    предложений:           %6d" % sum(1 for u in store.units.values() if u["kind"] == "sentence"))
    print("  указателей:              %6d" % sum(len(u["surfaces"]) for u in store.units.values()))
    print("  связей-переводов:        %6d" % len(links))
    print("  строк банка пропущено:   %6d" % skipped)
    with_pos = sum(1 for u in de_words if u["pos"])
    with_gender = sum(1 for u in de_words if u["gender"])
    nouns = sum(1 for u in de_words if u["pos"] == "noun")
    print("\n  немецкие слова: часть речи известна у %d из %d" % (with_pos, len(de_words)))
    print("    существительных: %d, из них с родом: %d, без рода: %d"
          % (nouns, with_gender, nouns - with_gender))
    by_pos = collections.Counter(u["pos"] or "—не установлена—" for u in de_words)
    for pos, n in by_pos.most_common(10):
        print("      %-22s %d" % (pos, n))

    for probe in args.show:
        pk = norm(strip_article(probe))
        print("\n── %r ──" % probe)
        found = [u for u in store.units.values() if u["lemma_key"] == pk]
        if not found:
            found = [u for u in store.units.values() if any(s == pk for s, _m in u["surfaces"])]
        for u in found:
            card = u.get("card") or {}
            forms = (card.get("forms") or {}).get("plural") if isinstance(card.get("forms"), dict) else None
            print("   ЕДИНИЦА  %-18s %-4s %-10s род=%-4s (%s)  формы: мн.ч. %s"
                  % (u["display"], u["lang"], u["pos"] or "—", u["gender"] or "—",
                     u["gender_source"] or "—", forms or "—"))
            outs = sorted(((v["rank"], b) for (a, b), v in links.items() if a == u["key"]))[:6]
            for rank, b in outs:
                other = store.units.get(b)
                if other:
                    print("        → %-28s (ранг %d)" % (other["display"], rank))

    if args.dry_run:
        conn.rollback()
        print("\n(--dry-run: в базу ничего не записано)")
        return 0

    # ── запись ────────────────────────────────────────────────────────────────
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(here, "backend", "lex_units_schema.sql"), encoding="utf-8") as fh:
        cur.execute(fh.read())
    print("\nтаблицы созданы (существующие не тронуты)")

    ids: dict[tuple, int] = {}
    payload = [(u["lang"], u["kind"], u["lemma"], u["lemma_key"], u["pos"], u["pos_source"],
                u["gender"], u["gender_source"], u["display"],
                json.dumps(u["card"], ensure_ascii=False) if u["card"] else None, u["card_source"])
               for u in store.units.values()]
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO bt_3_lex_units
            (lang, kind, lemma, lemma_key, pos, pos_source, gender, gender_source, display, card, card_source)
        VALUES %s
        ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
        DO UPDATE SET display = EXCLUDED.display, updated_at = NOW()
        """,
        payload, page_size=500,
    )
    cur.execute("SELECT id, lang, kind, lemma_key, COALESCE(pos,''), COALESCE(gender,'') FROM bt_3_lex_units;")
    for uid, lang, kind, lemma_key, pos, gender in cur.fetchall():
        ids[(lang, kind, lemma_key, pos, gender)] = uid
    print("  единиц записано: %d" % len(ids))

    surf = [(u["lang"], s, ids[u["key"]], mk) for u in store.units.values() for s, mk in u["surfaces"]
            if u["key"] in ids]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) VALUES %s "
        "ON CONFLICT (lang, surface_key, unit_id) DO NOTHING",
        surf, page_size=1000,
    )
    src_rows = [(ids[u["key"]], eid, side) for u in store.units.values() for eid, side in u["sources"]
                if u["key"] in ids]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side) VALUES %s ON CONFLICT DO NOTHING",
        src_rows, page_size=1000,
    )
    link_rows = [(ids[a], ids[b], v["rank"], v["source"]) for (a, b), v in links.items()
                 if a in ids and b in ids]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source) VALUES %s "
        "ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank)",
        link_rows, page_size=1000,
    )
    conn.commit()
    print("  указателей: %d, связей: %d, ссылок на строки банка: %d"
          % (len(surf), len(link_rows), len(src_rows)))
    print("\nстарый банк не изменён ни одной записью.")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
