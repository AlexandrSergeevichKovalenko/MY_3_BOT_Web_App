# -*- coding: utf-8 -*-
"""Заголовок в словарной форме — ВО ВСЕХ трёх хранилищах сразу.

Почему этот скрипт написан отдельно от прежних
──────────────────────────────────────────────
Прежние правки заголовков («schlammigen» → «schlammig», «klarzukommen» → «klarkommen»)
меняли ОДНО хранилище — слово в справочнике. Экран разбора читает не его, а личную
карточку, причём колонку `translation_de`. Замер 16.08.2026 после тех «починок»:

    schlammigen    слово починено · карточка word_de НЕТ · translation_de НЕТ · пул НЕТ
    klarzukommen   word_de починено · translation_de НЕТ · пул НЕТ

Владелец открыл оба слова и увидел ровно то, что я объявил исправленным.

Развозку делает боевая `spread_correction_everywhere` — та же функция, что работает,
когда владелец принимает исправление судьи. Она вычищает старый текст рекурсивно из
разбора на слове, из всех личных карточек этого слова и из общего пула, а заготовки
заданий сносит (их надо пересобрать, строкой не починить).

Классы, которые чинятся здесь. Каждый — правило, а не список слов
────────────────────────────────────────────────────────────────
1. zu-инфинитив: «klarzukommen» → «klarkommen». Правило продукта
   german_grammar_tables.looks_like_zu_infinitive. От такого заголовка движок печатал
   «ich klarzukomme» — форм, которых в языке нет.
2. Неопределённый артикль в заголовке одиночного существительного: «eine Pleite» →
   «Pleite», «die eine Pleite» → «die Pleite». В словарной статье «ein» не бывает, а
   экран дописывает свой артикль по роду — получается «die eine Pleite».
   ⚠ Фразы НЕ трогаем: в «eine Pressekonferenz abhalten» артикль принадлежит фразе.
   Признак — после снятия артикля остаётся РОВНО ОДНО слово.
3. Склонённое прилагательное: «schlammigen» при слове «schlammig». Признак жёсткий —
   заголовок карточки = заголовок слова ПЛЮС окончание склонения (-e/-en/-em/-er/-es),
   основа посимвольно та же.

   ⚠ Первая версия этого правила брала ЛЮБОЕ расхождение карточки со словом, если
   разбор называл слово глаголом или прилагательным. Сухой прогон 16.08.2026 показал,
   что она сделала бы: «aufhören» → «ausgehen», «anfangen» → «loslegen» (разные слова,
   карточка привязана к чужому слову), «das Vermögen» → «vermögen» и «das Auftreten» →
   «auftreten» (законные существительные — субстантивированный инфинитив), «Die Qualen»
   → «qualen». Правило по расхождению — это не правило, а догадка; оставлено только
   совпадение основы.

Чего здесь НЕТ и почему
───────────────────────
Множественное число в личной карточке («die Handschuhe» при слове «Handschuh») НЕ
дефект: решение принято 29.07.2026 — показываем то, что человек искал, а под
заголовком строку «мн. ч. от …». Подменять запрос леммой нельзя.

Неверный артикль в карточке («das Habe» при слове «die Habe») чинится ОТДЕЛЬНО
и не здесь: у существительных на -er/-en/-el множественное совпадает с единственным
(«der Reifen» → «die Reifen»), и слепая правка артикля превратила бы законное
множественное в ошибку. Список таких карточек печатается в конце — для глаз.

    python3 scripts/dict_fix_headwords_everywhere.py            # сухой прогон
    python3 scripts/dict_fix_headwords_everywhere.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                    # noqa: E402
    get_db_connection_context,
    spread_correction_everywhere,
)
from backend.german_grammar_tables import (                       # noqa: E402
    looks_like_zu_infinitive,
    strip_zu_infinitive,
)
from backend.lex_units import normalize_query                     # noqa: E402

_SPACE = re.compile(r"\s+")
_INDEFINITE = re.compile(r"^(?:ein|eine|einen|einem|einer|eines)\s+", re.I)
_DEFINITE_HEAD = re.compile(r"^((?:der|die|das)\s+)", re.I)
# Окончания склонения прилагательного. Больше в этом списке ничего быть не может:
# всё остальное — уже другое слово, а не форма.
_DECLENSION_ENDINGS = {"e", "en", "em", "er", "es"}


def key_of(value) -> str:
    return _SPACE.sub(" ", str(value or "").strip()).casefold()


def dictionary_form(surface: str) -> str:
    """Словарная форма заголовка, или пустая строка, если он и так в порядке."""
    text = _SPACE.sub(" ", str(surface or "").strip())
    if not text:
        return ""

    if " " not in text and looks_like_zu_infinitive(text):
        return strip_zu_infinitive(text)

    # «die eine Pleite» → «die Pleite»; «eine Pleite» → «Pleite».
    head = _DEFINITE_HEAD.match(text)
    prefix = head.group(1) if head else ""
    rest = text[len(prefix):]
    if _INDEFINITE.match(rest):
        bare = _INDEFINITE.sub("", rest).strip()
        # Ровно одно слово — существительное; иначе это фраза, артикль в ней законен.
        # Заглавная и длина от трёх букв: иначе «Einer n» превратится в заголовок «n».
        if (bare and " " not in bare and len(bare) >= 3
                and any(ch.isupper() for ch in bare)):
            return (prefix + bare).strip()
    return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    plan: list[tuple[int, str, str, str]] = []   # unit_id, старое, новое, откуда узнали
    seen: set[tuple[int, str]] = set()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # ── 1-2. дефект виден в самом заголовке, в любом из хранилищ ────────────
            cur.execute("SELECT id, display FROM bt_3_lex_units WHERE lang='de' AND display <> '';")
            for unit_id, display in cur.fetchall():
                fixed = dictionary_form(display)
                if fixed and key_of(fixed) != key_of(display):
                    plan.append((int(unit_id), display, fixed, "слово"))
                    seen.add((int(unit_id), key_of(display)))

            # тот же дефект, но уцелевший в карточке или пуле, когда слово уже починено
            cur.execute(
                """
                SELECT DISTINCT q.lex_unit_id, q.translation_de, q.word_de
                FROM bt_3_webapp_dictionary_queries q
                WHERE q.lex_unit_id IS NOT NULL
                  AND (q.translation_de IS NOT NULL OR q.word_de IS NOT NULL);
                """
            )
            for unit_id, translation_de, word_de in cur.fetchall():
                for value in (translation_de, word_de):
                    fixed = dictionary_form(value or "")
                    if not fixed or key_of(fixed) == key_of(value):
                        continue
                    if (int(unit_id), key_of(value)) in seen:
                        continue
                    plan.append((int(unit_id), value, fixed, "карточка"))
                    seen.add((int(unit_id), key_of(value)))

            # ── 3. склонённое прилагательное: та же основа плюс окончание ──────────
            cur.execute(
                """
                SELECT q.lex_unit_id, q.word_de, q.translation_de, u.display
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id AND u.lang='de' AND u.kind='word'
                WHERE lower(COALESCE(q.response_json->>'part_of_speech','')) IN ('adjective','adverb')
                  AND (lower(BTRIM(COALESCE(q.word_de,''))) <> lower(BTRIM(u.display))
                    OR lower(BTRIM(COALESCE(q.translation_de,''))) <> lower(BTRIM(u.display)));
                """
            )
            for unit_id, word_de, translation_de, display in cur.fetchall():
                base = key_of(display)
                for value in (word_de, translation_de):
                    if not value or key_of(value) == base:
                        continue
                    if not base or not key_of(value).startswith(base):
                        continue
                    if key_of(value)[len(base):] not in _DECLENSION_ENDINGS:
                        continue
                    if (int(unit_id), key_of(value)) in seen:
                        continue
                    plan.append((int(unit_id), value, display, "склонение"))
                    seen.add((int(unit_id), key_of(value)))

    print("ПЛАН: %d правок" % len(plan))
    for unit_id, old, new, why in plan[:40]:
        print("   слово %-7s %-30s → %-30s (%s)" % (unit_id, str(old)[:30], str(new)[:30], why))
    if len(plan) > 40:
        print("   … ещё %d" % (len(plan) - 40))

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    total = {"cards": 0, "places": 0, "pool": 0, "tasks_dropped": 0, "units": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, old, new, why in plan:
                # заголовок самого слова, если дефект был в нём
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                row = cur.fetchone()
                # Рядом уже живёт слово с таким написанием — переименование упрётся в
                # уникальный ключ, а сливать две единицы без решения владельца нельзя.
                # Текст в карточках и пуле всё равно чиним: он от этого не зависит.
                cur.execute(
                    "SELECT id, display FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s "
                    "AND id <> %s LIMIT 1;",
                    (normalize_query(new), unit_id),
                )
                twin = cur.fetchone()
                if twin:
                    print("   ⚠ %-7s «%s» → «%s»: рядом уже есть слово %s — заголовок не трогаю"
                          % (unit_id, old, new, twin[0]))
                if row and not twin and key_of(row[0]) == key_of(old):
                    cur.execute(
                        "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, "
                        "updated_at=NOW() WHERE id=%s;",
                        (new, new, normalize_query(new), unit_id),
                    )
                    # старое написание остаётся дверью для поиска, новое добавляем
                    cur.execute(
                        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                        "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                        (normalize_query(new), unit_id),
                    )
                    total["units"] += 1
                report = spread_correction_everywhere(cur, unit_id=unit_id,
                                                      old_text=old, new_text=new)
                for name in ("cards", "places", "pool", "tasks_dropped"):
                    total[name] += report.get(name, 0)
        conn.commit()

    print()
    print("   слов переименовано:     %d" % total["units"])
    print("   карточек тронуто:       %d" % total["cards"])
    print("   мест внутри разборов:   %d" % total["places"])
    print("   записей пула:           %d" % total["pool"])
    print("   заданий снесено:        %d" % total["tasks_dropped"])


if __name__ == "__main__":
    main()
