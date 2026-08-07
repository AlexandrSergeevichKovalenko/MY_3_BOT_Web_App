"""Форма слова — не отдельное слово: сложить её в единицу словарной формы.

Зачем. Заголовком карточки должна стоять СЛОВАРНАЯ форма, а набранное человеком —
строкой рядом («wuchsen — форма слова wachsen»). Так делают Linguee, Reverso, dict.cc:
человек и находит своё, и запоминает правильное. У нас же форма успевала завести
собственную единицу: «angedroht» жило рядом с «androhen» и имело свой разбор.

Кого берём. Единицы, у которых написание единицы расходится с заголовком её же разбора
(замер 07.08.2026 — 97 из 4572 разобранных слов, 2,1%). Внутри этих 97 три разные беды,
и одинаково обходиться с ними нельзя:
  • форма вместо слова — «angedroht» с разбором про «androhen». ЭТО и сворачиваем;
  • обрезанный разбор — «Umschaltsituationen» с разбором про «die Umschaltsituatio».
    Право на стороне ЕДИНИЦЫ, разбор чинится ночью;
  • множественное в разборе — «Behörde» с разбором про «die Behörden». Право снова на
    стороне единицы: словарная форма существительного — именительный единственного.
Различить их правилом нельзя, поэтому спрашиваем модель отдельным дешёвым вопросом
`run_quick_lemma` («это форма или само слово, и какое слово»). Сворачиваем ТОЛЬКО когда
модель говорит «это форма» И названная ею словарная форма совпадает с заголовком разбора:
два независимых источника. Разошлись — не трогаем и пишем в отчёт.

Почему нельзя взять `corrected_form` из разбора. Он отвечает не на тот вопрос и заводит
в обратную сторону: «Annehmlichkeit» → «Annehmlichkeiten» (единственное → множественное).

Личные карточки людей НЕ переписываем: человек сохранил то, что сохранил. Меняется общий
слой — то, что видит любой, кто спросит это слово.

По умолчанию НИЧЕГО НЕ ПИШЕТ и НИЧЕГО НЕ СПРАШИВАЕТ у модели.

    python scripts/dict_units_fold_inflected.py            # список кандидатов
    python scripts/dict_units_fold_inflected.py --ask      # спросить модель, не писать
    python scripts/dict_units_fold_inflected.py --apply    # спросить и свернуть
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import get_db_connection_context  # noqa: E402
from dictionary_intake import clean_text  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das|den|dem|des)\s+", re.I)

# Переименованы прогоном 07.08.2026, когда выравнивание части речи ещё не было написано.
# Оставлять список нужно: после переименования написание совпало с разбором, и обычный
# отбор такие единицы больше не находит, а метка «глагол» у слова «Schwager» осталась.
RENAMED_EARLIER = (2124, 9687, 13984, 18293, 19084, 24393, 24427, 24720, 24833, 25129)

# Справочник форм: в нём парадигмы, прочитанные со страниц Wiktionary.
_FORMS_CACHE: dict[str, set[str]] = {}
_LAST_FETCH = [0.0]
_FETCH_PAUSE_SEC = 1.5


def _fetch_with_pause(title: str) -> list[tuple[str, str]]:
    """Один запрос к Wiktionary, но вежливо. Без паузы справочник отвечает 429, и слово
    отвергается не потому, что оно не форма, а потому что нас не пустили — молчаливый
    недобор, который читается как «проверено, не подходит»."""
    for attempt in range(2):
        wait = _FETCH_PAUSE_SEC - (time.time() - _LAST_FETCH[0])
        if wait > 0:
            time.sleep(wait)
        _LAST_FETCH[0] = time.time()
        try:
            from dict_units_wikt_forms import fetch_forms
            return fetch_forms([title]).get(title) or []
        except Exception as exc:
            if attempt == 0:
                time.sleep(5)
                continue
            print("    справочник форм недоступен для %r: %s" % (title, exc))
    return []


def paradigm_of(cur, lemma: str) -> set[str]:
    """Все написания, которыми это слово бывает, по справочнику. Пусто — не знаем.

    Сначала своя таблица, при промахе — один запрос к Wiktionary с записью в неё же.
    Догадку по окончанию сюда не пускаем: именно она когда-то дала «des Helds» вместо
    «des Helden», и мы условились, что факты о слове берёт авторитет, а не правило."""
    title = bare(lemma)
    if not title:
        return set()
    cached = _FORMS_CACHE.get(title.casefold())
    if cached is not None:
        return cached
    cur.execute("SELECT value FROM bt_3_wiktionary_forms WHERE title = %s;", (title,))
    rows = cur.fetchall() or []
    if not rows:
        fetched = _fetch_with_pause(title)
        for form_key, value in fetched:
            cur.execute(
                """INSERT INTO bt_3_wiktionary_forms (title, form_key, value, checked_at)
                   VALUES (%s, %s, %s, NOW()) ON CONFLICT DO NOTHING;""",
                (title, form_key, value),
            )
        rows = [(value,) for _key, value in fetched]
    forms = {bare(str(r[0])).casefold() for r in rows if str(r[0] or "").strip()}
    forms.add(title.casefold())
    _FORMS_CACHE[title.casefold()] = forms
    return forms


def bare(text: str) -> str:
    return ARTICLE_RE.sub("", re.sub(r"\s+", " ", str(text or "").strip())).strip()


def key_of(text: str) -> str:
    return bare(text).casefold()


def collect(cur) -> list[dict]:
    """Единицы, у которых написание расходится с заголовком собственного разбора."""
    cur.execute(
        """SELECT id, lemma, lemma_key, display, pos, gender, card->>'word_de'
           FROM bt_3_lex_units
           WHERE lang = 'de' AND kind = 'word' AND card IS NOT NULL
             AND coalesce(card->>'word_de', '') <> ''
           ORDER BY id;"""
    )
    out = []
    for uid, lemma, lemma_key, display, pos, gender, card_de in cur.fetchall() or []:
        if key_of(card_de) == key_of(lemma):
            continue
        out.append({"id": uid, "lemma": lemma, "lemma_key": lemma_key, "display": display,
                    "pos": pos, "gender": gender, "card_de": card_de})
    return out


def align_pos_with_card(cur, unit_id: int) -> bool:
    """Часть речи переименованной единицы должна описывать СЛОВО, а не форму.

    «Schwäger» лежала глаголом, «Betätige» — прилагательным: метку ставили по форме.
    После переименования эта метка уезжает на словарную форму и врёт человеку прямо в
    карточке (чип под заголовком). Правду знает разбор, он про это слово и куплен.

    Опознание единицы = лемма + часть речи + род, поэтому правка МЕНЯЕТ ключ: если рядом
    уже живёт такое же слово с такой меткой, молча пропускаем — сливать без решения
    владельца нельзя."""
    cur.execute(
        """SELECT lower(card->>'part_of_speech'), pos, lang, kind, lemma_key, gender
           FROM bt_3_lex_units WHERE id = %s;""",
        (unit_id,),
    )
    row = cur.fetchone()
    if not row:
        return False
    card_pos, pos, lang, kind, lemma_key, gender = row
    card_pos = str(card_pos or "").strip()
    if not card_pos or card_pos == str(pos or "").strip().lower():
        return False
    cur.execute(
        """SELECT 1 FROM bt_3_lex_units
           WHERE lang = %s AND kind = %s AND lemma_key = %s
             AND COALESCE(pos, '') = %s AND COALESCE(gender, '') = COALESCE(%s, '')
             AND id <> %s LIMIT 1;""",
        (lang, kind, lemma_key, card_pos, gender, unit_id),
    )
    if cur.fetchone():
        return False
    cur.execute(
        "UPDATE bt_3_lex_units SET pos = %s, pos_source = 'card', updated_at = NOW() WHERE id = %s;",
        (card_pos, unit_id),
    )
    return bool(cur.rowcount)


def target_unit(cur, lemma: str, *, exclude: int) -> dict | None:
    """Единица словарной формы, если она уже есть."""
    cur.execute(
        """SELECT id, lemma, display FROM bt_3_lex_units
           WHERE lang = 'de' AND kind = 'word' AND lemma_key = %s AND id <> %s
           ORDER BY (card IS NULL), id LIMIT 1;""",
        (key_of(lemma), exclude),
    )
    row = cur.fetchone()
    return {"id": int(row[0]), "lemma": row[1], "display": row[2]} if row else None


def fold_into(cur, unit: dict, target_id: int) -> dict:
    """Перенести всё, что держит единица-форма, на единицу словарной формы и убрать её.

    Порядок важен: сначала переносим, и только потом удаляем. Всё, что не перенесено,
    база унесёт каскадом молча — а там значения, источники ночного добора и связи."""
    uid = unit["id"]
    moved = {"связей": 0, "значений": 0, "источников": 0, "написаний": 0, "карточек": 0}

    cur.execute(
        """UPDATE bt_3_lex_links l SET from_unit = %(t)s
           WHERE l.from_unit = %(u)s AND l.to_unit <> %(t)s
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                             WHERE k.from_unit = %(t)s AND k.to_unit = l.to_unit);""",
        {"t": target_id, "u": uid},
    )
    moved["связей"] += cur.rowcount or 0
    cur.execute(
        """UPDATE bt_3_lex_links l SET to_unit = %(t)s
           WHERE l.to_unit = %(u)s AND l.from_unit <> %(t)s
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                             WHERE k.from_unit = l.from_unit AND k.to_unit = %(t)s);""",
        {"t": target_id, "u": uid},
    )
    moved["связей"] += cur.rowcount or 0

    # Значения нумеруются внутри слова, поэтому переносим их СЛЕДОМ за уже занятыми
    # номерами: иначе второе значение формы столкнётся со вторым значением слова.
    cur.execute("SELECT coalesce(max(sense_no), 0) FROM bt_3_lex_senses WHERE unit_id = %s;", (target_id,))
    shift = int((cur.fetchone() or [0])[0] or 0)
    cur.execute(
        "UPDATE bt_3_lex_senses SET unit_id = %s, sense_no = sense_no + %s WHERE unit_id = %s;",
        (target_id, shift, uid),
    )
    moved["значений"] += cur.rowcount or 0

    # Происхождение: по нему ночной добор ранжирует очередь и цепляются личные карточки.
    cur.execute(
        """UPDATE bt_3_lex_unit_sources s SET unit_id = %(t)s
           WHERE s.unit_id = %(u)s
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_unit_sources k
                             WHERE k.unit_id = %(t)s AND k.entry_id = s.entry_id AND k.side = s.side);""",
        {"t": target_id, "u": uid},
    )
    moved["источников"] += cur.rowcount or 0

    # Написания формы становятся указателями на слово: запрос «angedroht» теперь
    # находит «androhen» — и карточка честно скажет, что это его форма.
    cur.execute(
        """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
           SELECT 'de', s.surface_key, %s, 'inflected' FROM bt_3_lex_surfaces s
           WHERE s.unit_id = %s ON CONFLICT DO NOTHING;""",
        (target_id, uid),
    )
    moved["написаний"] += cur.rowcount or 0

    cur.execute(
        "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s WHERE lex_unit_id = %s;",
        (target_id, uid),
    )
    moved["карточек"] += cur.rowcount or 0

    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (uid,))
    return moved


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ask", action="store_true", help="спросить модель, но не писать")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            candidates = collect(cur)
    if args.limit:
        candidates = candidates[: args.limit]

    print("КАНДИДАТЫ (написание единицы ≠ заголовок её разбора): %d" % len(candidates))
    if not (args.ask or args.apply):
        for c in candidates[:30]:
            print("   %-6s %r ← разбор про %r" % (c["id"], c["lemma"][:32], (c["card_de"] or "")[:32]))
        print()
        print("ВХОЛОСТУЮ. Спросить модель: --ask, свернуть: --apply")
        return 0

    from openai_manager import run_quick_lemma

    folded, renamed, kept = [], [], []
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for c in candidates:
                verdict = run_quick_lemma(surface=c["lemma"])
                lemma = clean_text(verdict.get("lemma") or "")
                is_form = bool(verdict.get("is_form"))
                # Два независимых источника: модель назвала это формой И её словарная
                # форма совпала с заголовком разбора, который куплен отдельно и раньше.
                agrees = bool(lemma) and key_of(lemma) == key_of(c["card_de"])
                if not is_form or not agrees or key_of(lemma) == key_of(c["lemma"]):
                    kept.append((c, lemma, is_form))
                    continue
                # Третья, механическая проверка — и она главная. Два первых источника
                # ошибаются ВМЕСТЕ, когда разбор куплен для испорченного слова: так
                # «wohingenen» (задумано «wohingegen») получило разбор про «wohnen», и
                # модель послушно повторила. Справочник форм такое не подтвердит.
                paradigm = paradigm_of(cur, lemma)
                if not paradigm:
                    kept.append((c, lemma, "справочник не знает %r" % bare(lemma)[:30]))
                    continue
                if key_of(c["lemma"]) not in paradigm:
                    kept.append((c, lemma, "%r не форма %r по справочнику"
                                 % (c["lemma"][:24], bare(lemma)[:24])))
                    continue
                if not args.apply:
                    folded.append((c, lemma, None))
                    continue
                try:
                    target = target_unit(cur, lemma, exclude=c["id"])
                    if target:
                        moved = fold_into(cur, c, target["id"])
                        folded.append((c, lemma, ("свёрнуто в #%s" % target["id"], moved)))
                    else:
                        # Слова ещё нет — переименовываем саму единицу, а прежнее
                        # написание оставляем указателем на неё.
                        new_key = lex_units.normalize_query(lemma)
                        article = lex_units.article_of(lemma)
                        body = bare(lemma)
                        cur.execute(
                            """UPDATE bt_3_lex_units
                               SET lemma = %s, display = %s, lemma_key = %s,
                                   gender = COALESCE(NULLIF(%s, ''), gender), updated_at = NOW()
                               WHERE id = %s;""",
                            (body, body, new_key, article.lower(), c["id"]),
                        )
                        cur.execute(
                            """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                               VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                            (new_key, c["id"]),
                        )
                        cur.execute(
                            """UPDATE bt_3_lex_surfaces SET match_kind = 'inflected'
                               WHERE unit_id = %s AND surface_key <> %s;""",
                            (c["id"], new_key),
                        )
                        align_pos_with_card(cur, c["id"])
                        renamed.append((c, lemma))
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    kept.append((c, lemma, "база отказала: %s" % str(exc).splitlines()[0][:70]))

    # Переименованные прошлым прогоном: `collect` их больше не видит (написание уже
    # сошлось с разбором), а часть речи у них так и осталась от формы.
    if args.apply and RENAMED_EARLIER:
        fixed_pos = 0
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                for uid in RENAMED_EARLIER:
                    fixed_pos += 1 if align_pos_with_card(cur, uid) else 0
            conn.commit()
        print("ЧАСТЬ РЕЧИ ВЫРОВНЕНА ПО РАЗБОРУ: %d" % fixed_pos)

    print()
    print("СВЁРНУТО В СУЩЕСТВУЮЩЕЕ СЛОВО: %d" % len(folded))
    for c, lemma, info in folded:
        print("   %-6s %r → %r   %s" % (c["id"], c["lemma"][:30], lemma[:30], info or ""))
    print("ПЕРЕИМЕНОВАНО (слова ещё не было): %d" % len(renamed))
    for c, lemma in renamed:
        print("   %-6s %r → %r" % (c["id"], c["lemma"][:30], lemma[:30]))
    print("ОСТАВЛЕНО КАК ЕСТЬ: %d" % len(kept))
    for c, lemma, why in kept[:40]:
        print("   %-6s %-32r модель: %r%s"
              % (c["id"], c["lemma"][:30], lemma[:30], "" if why is True else "  (%s)" % why))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
