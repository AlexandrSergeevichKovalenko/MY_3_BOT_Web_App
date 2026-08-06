"""Достроить переезд словаря: у каждой записи старого слоя должно быть слово в новом.

Три части одной работы — старый общий словарь (`bt_3_dictionary_entries`) не переехал
в слой слов целиком, и из-за этого разбор не доходит до людей.

  А. 115 ЗАГОТОВОК УПРАЖНЕНИЙ с пропуском («Er ___ heute früh mit dem Projekt.»)
     лежат в слое слов рядом с настоящими словами. В выдачу они не попадают — запрос
     их отсекает, — но занимают место, путают замеры и мешают ночному обогатителю
     видеть настоящую очередь. Ни разбора, ни связей, ни карточек у них нет, держит
     только запись в таблице источников. Убираем.

  Б. 1650 ЗАПИСЕЙ СТАРОГО СЛОВАРЯ БЕЗ СЛОВА в новом слое. У 835 из них лежит ПОЛНЫЙ
     разбор — уже оплаченный и никому не доступный, потому что читаем мы теперь из
     слоя слов. Заводим слово, кладём на него разбор, связываем стороны. Модель не
     зовём ни разу: всё берётся из того, что уже есть.

  В. 62 БЕДНЫЕ КАРТОЧКИ у 12 человек — 26 разных записей. Это старые записи, набранные
     списком через запятую («Pupsen, furzen», «Weil, da»). Разбора нет ни в карточке,
     ни в слове, ни в старом словаре: там тоже пусто, 3 поля вместо тридцати. Здесь
     взять неоткуда — покупаем настоящий разбор, ОДИН на запись, и кладём на слово:
     его получают все 12 человек сразу.

По умолчанию НИЧЕГО НЕ ПИШЕТ и НИЧЕГО НЕ ПОКУПАЕТ. Запись — только с --apply.

    python scripts/dict_finish_pool_migration.py                # вхолостую
    python scripts/dict_finish_pool_migration.py --apply        # всё три части
    python scripts/dict_finish_pool_migration.py --apply --only b
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402
from dictionary_intake import clean_text, has_cyrillic  # noqa: E402
import lex_units  # noqa: E402

FULL_CARD_FIELDS = 8


# ── А. Заготовки упражнений ───────────────────────────────────────────────────────

def collect_blanks(cur) -> list:
    cur.execute(
        """SELECT u.id, u.lang, u.display FROM bt_3_lex_units u
           WHERE position('___' in u.display) > 0
             AND u.card IS NULL
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.from_unit=u.id OR l.to_unit=u.id)
             AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id=u.id)
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id)
           ORDER BY u.id;"""
    )
    return cur.fetchall()


# ── Б. Записи старого словаря без слова ───────────────────────────────────────────

def collect_orphan_entries(cur) -> list:
    """Записи, у которых нет дома в слое слов.

    Сравнивать надо по ТОМУ ЖЕ ключу, каким слой ищет: `normalize_query` снимает
    артикль и опускает регистр, а `source_text_norm` артикль хранит. Сравнение
    «в лоб» даёт в пять раз больше «бездомных», чем есть на самом деле, — на этом
    я один раз уже ошибся."""
    cur.execute("SELECT lang, surface_key FROM bt_3_lex_surfaces;")
    surfaces = set(cur.fetchall())
    cur.execute(
        """SELECT e.id, e.source_lang, e.target_lang, e.source_text, e.target_text,
                  e.response_json,
                  CASE WHEN e.response_json IS NULL THEN 0
                       ELSE (SELECT count(*) FROM jsonb_object_keys(e.response_json)) END
           FROM bt_3_dictionary_entries e ORDER BY e.id;"""
    )
    out = []
    for eid, sl, tl, st, tt, card, fields in cur.fetchall():
        if (sl, lex_units.normalize_query(st)) in surfaces:
            continue
        st, tt = clean_text(st), clean_text(tt)
        if not st or not tt:
            continue
        if "___" in st or "___" in tt:
            continue  # заготовка упражнения — слову в слое не место, см. часть А
        out.append((eid, sl, tl, st, tt, card if isinstance(card, dict) else None, int(fields or 0)))
    return out


def collect_swapped_entries(cur) -> list:
    """Записи с перепутанным направлением: в строке написано «источник русский», а
    текст немецкий. Та же беда, что была у слов, — от массовой сборки. Слово по такой
    записи не заведётся: заслон не пустит немецкий текст под русским языком."""
    cur.execute(
        """SELECT id, source_lang, target_lang, source_text, target_text,
                  source_text_norm, target_text_norm
           FROM bt_3_dictionary_entries ORDER BY id;"""
    )
    rows = cur.fetchall()
    taken = {(r[1], r[2], r[5], r[6]) for r in rows}
    out = []
    for eid, sl, tl, st, tt, sn, tn in rows:
        if not sl or not tl or sl == tl:
            continue
        if lex_units.text_matches_language(st, sl):
            continue
        # текст источника не того алфавита; меняем метки местами, только если после
        # обмена сходятся ОБЕ стороны — иначе это не перепутанное направление
        if not (lex_units.text_matches_language(st, tl) and lex_units.text_matches_language(tt, sl)):
            continue
        # такая пара уже есть с правильными метками — это дубликат, не трогаем:
        # обмен упёрся бы в запрет на повтор и уронил бы весь проход
        if (tl, sl, sn, tn) in taken:
            continue
        taken.add((tl, sl, sn, tn))
        out.append((eid, sl, tl, st, tt))
    return out


def collect_failed_lookups(cur) -> list:
    """Записи, где «перевод» дословно равен запросу: поиск не удался, и в общий кеш
    легла сама же фраза («Ic», «Die P», «Aufrechtha» — набранные по буквам обрывки,
    «Heinzelmännchen» → «Heinzelmännchen» — сорвавшийся разбор). Такая запись хуже, чем
    её отсутствие: она отвечает на запрос пустотой и не даёт сходить за настоящим
    разбором. Убираем только те, на которые не ссылается ни одна карточка."""
    cur.execute(
        """SELECT e.id, e.source_lang, e.target_lang, e.source_text
           FROM bt_3_dictionary_entries e
           WHERE lower(btrim(e.source_text)) = lower(btrim(e.target_text))
             AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q
                             WHERE q.canonical_entry_id = e.id)
           ORDER BY e.id;"""
    )
    return cur.fetchall()


def collect_homeless_entries(cur) -> list:
    """Записи, которым дом в слое слов завести НЕЛЬЗЯ. Разобрано поимённо 06.08.2026 —
    все до одной сломаны одинаково: обе стороны на одном языке.

      «Ich schäme mich für jemand anderen» → «Ich geniere mich fremd» — немецкое к немецкому;
      «Problem» → «das Problem», «Tür» → «die Tür» — исправление написания, а не перевод;
      «Не заводись» → «Не заводись» — русское к русскому под меткой «английский»;
      «Пожалуйста не нервничай,» — хвостовая запятая, чистая копия уже лежит рядом.

    Перевода в них нет, и слово по ним не заведётся: заслон не пустит текст чужого
    алфавита. Держать их в общем словаре хуже, чем не держать: они отвечают на запрос
    пустотой и не дают сходить за настоящим разбором.

    Карточка человека от удаления НЕ страдает: своё содержимое она хранит сама, а
    указатель на общую запись мы переставляем на правильного близнеца или обнуляем."""
    cur.execute("SELECT lang, surface_key FROM bt_3_lex_surfaces;")
    surfaces = set(cur.fetchall())
    cur.execute(
        """SELECT id, source_lang, target_lang, source_text, target_text, source_text_norm, target_text_norm
           FROM bt_3_dictionary_entries ORDER BY id;"""
    )
    rows = cur.fetchall()
    by_pair = {}
    for eid, sl, tl, st, tt, sn, tn in rows:
        by_pair.setdefault((sn, tn), []).append((eid, sl, tl))
    out = []
    for eid, sl, tl, st, tt, sn, tn in rows:
        if "___" in (st or "") or "___" in (tt or ""):
            continue
        if (sl, lex_units.normalize_query(st)) in surfaces:
            continue
        twin = next((t[0] for t in by_pair.get((sn, tn), []) if t[0] != eid), None)
        out.append((eid, sl, tl, st, tt, twin))
    return out


def link_units(cur, a: int, b: int, *, rank: int = 10) -> None:
    for x, y in ((a, b), (b, a)):
        cur.execute(
            """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
               VALUES (%s, %s, %s, 'пул')
               ON CONFLICT (from_unit, to_unit) DO NOTHING;""",
            (x, y, rank),
        )


# ── В. Бедные карточки ────────────────────────────────────────────────────────────

def collect_poor_cards(cur) -> dict:
    cur.execute(
        """SELECT q.id, q.word_de, q.word_ru, q.lex_unit_id
           FROM bt_3_webapp_dictionary_queries q
           JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
           WHERE u.card IS NULL
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.from_unit=u.id OR l.to_unit=u.id)
             AND (q.response_json IS NULL OR jsonb_typeof(q.response_json) <> 'object'
                  OR (SELECT count(*) FROM jsonb_object_keys(q.response_json)) < %s)
           ORDER BY q.id;""",
        (FULL_CARD_FIELDS,),
    )
    groups: dict = {}
    for cid, word_de, word_ru, unit_id in cur.fetchall():
        german = clean_text(word_de)
        if not german:
            continue
        groups.setdefault(german, {"cards": [], "unit_id": unit_id})
        groups[german]["cards"].append(cid)
        groups[german]["unit_id"] = groups[german]["unit_id"] or unit_id
    return groups


def buy_card(german: str) -> dict:
    """Разбор одной записи. Обрыв по таймауту — обычное дело на длинных строках;
    пробуем трижды и идём дальше, чтобы одна запись не уронила весь проход."""
    from openai_manager import run_dictionary_lookup_multilang_core_fast

    for attempt in (1, 2, 3):
        try:
            raw = asyncio.run(
                run_dictionary_lookup_multilang_core_fast(
                    word=german, source_lang="de", target_lang="ru", explanation_lang="ru",
                )
            )
            if isinstance(raw, dict) and raw:
                return raw
        except Exception as exc:
            print("   попытка %d для %r не удалась: %s" % (attempt, german[:40], type(exc).__name__))
    return {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--only", choices=["a", "b", "c", "d", "e"], default=None)
    args = parser.parse_args()
    run = lambda part: args.only in (None, part)  # noqa: E731

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            blanks = collect_blanks(cur) if run("a") else []
            swapped = collect_swapped_entries(cur) if run("b") else []
            orphans = collect_orphan_entries(cur) if run("b") else []
            poor = collect_poor_cards(cur) if run("c") else {}
            failed = collect_failed_lookups(cur) if run("d") else []
            homeless = collect_homeless_entries(cur) if run("e") else []

            if run("a"):
                print("А. Заготовки упражнений в слое слов: %d — убираем" % len(blanks))
                for uid, lang, display in blanks[:5]:
                    print("     %s %s %r" % (uid, lang, display[:55]))
            if run("b"):
                rich = [o for o in orphans if o[6] >= FULL_CARD_FIELDS]
                print("Б. Записи с перепутанным направлением (метки меняем местами): %d" % len(swapped))
                for eid, sl, tl, st, tt in swapped[:5]:
                    print("     %s: %s→%s становится %s→%s   %r" % (eid, sl, tl, tl, sl, st[:45]))
                print("Б. Записи старого словаря без слова: %d" % len(orphans))
                print("     из них с полным разбором — переносим бесплатно: %d" % len(rich))
                print("     остальным заводим слово без разбора (ночной обогатитель доберёт): %d"
                      % (len(orphans) - len(rich)))
            if run("c"):
                print("В. Бедные карточки: %d карточек, %d разных записей — столько разборов и покупаем"
                      % (sum(len(g["cards"]) for g in poor.values()), len(poor)))
                for german in list(poor)[:5]:
                    print("     %r → %d карточек" % (german[:45], len(poor[german]["cards"])))

            if run("d"):
                print("Г. Записи, где перевод равен запросу (сорвавшийся поиск): %d — убираем"
                      % len(failed))
                for eid, sl, tl, st in failed[:10]:
                    print("     %s %s->%s %r" % (eid, sl, tl, st[:50]))

            if run("e"):
                print("Д. Сломанные записи словаря без дома: %d — убираем" % len(homeless))
                for eid, sl, tl, st, tt, twin in homeless[:10]:
                    print("     %s %s->%s %r -> %r%s"
                          % (eid, sl, tl, (st or "")[:38], (tt or "")[:28],
                             "  (карточки переставим на %s)" % twin if twin else ""))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            # ── А ──
            removed = 0
            if run("a") and blanks:
                cur.execute("DELETE FROM bt_3_lex_units WHERE id = ANY(%s);",
                            ([int(b[0]) for b in blanks],))
                removed = cur.rowcount or 0
                conn.commit()

            # ── Б ──
            # Сначала направление: пока метка языка врёт, заслон не даст завести слово.
            for eid, sl, tl, _st, _tt in swapped:
                cur.execute(
                    "UPDATE bt_3_dictionary_entries SET source_lang = %s, target_lang = %s WHERE id = %s;",
                    (tl, sl, eid),
                )
            conn.commit()
            if swapped:
                orphans = collect_orphan_entries(cur)
            made_units = moved_cards = made_links = skipped = 0
            for _eid, sl, tl, st, tt, card, fields in orphans:
                # Немецкая сторона — та, у которой язык 'de'. Разбор описывает немецкое
                # слово, и кладём мы его только туда; для пар без немецкого (en↔ru)
                # просто заводим оба слова и связь.
                if sl == "de":
                    main_text, main_lang, other_text, other_lang = st, sl, tt, tl
                elif tl == "de":
                    main_text, main_lang, other_text, other_lang = tt, tl, st, sl
                else:
                    main_text, main_lang, other_text, other_lang = st, sl, tt, tl
                try:
                    main_unit = lex_units.ensure_unit(main_text, main_lang)
                    other_unit = lex_units.ensure_unit(other_text, other_lang)
                except Exception:
                    skipped += 1
                    continue
                if not main_unit:
                    skipped += 1  # слово чужого алфавита — заслон не пустил, и правильно
                    continue
                made_units += 1
                if other_unit:
                    with conn.cursor() as lc:
                        link_units(lc, int(main_unit), int(other_unit))
                    made_links += 1
                if card and fields >= FULL_CARD_FIELDS and main_lang == "de":
                    try:
                        if lex_units.save_unit_card_if_richer(int(main_unit), card, source="переезд пула"):
                            moved_cards += 1
                    except Exception:
                        pass
                conn.commit()

            # ── В ──
            bought = fixed_cards = dropped_failed = 0
            dropped_homeless = repointed = 0
            for german, group in poor.items():
                raw = buy_card(german)
                if not raw:
                    print("   %r: разбор не получен, оставили как есть" % german[:45])
                    continue
                bought += 1
                unit_id = group["unit_id"]
                if unit_id:
                    try:
                        lex_units.save_unit_card_if_richer(int(unit_id), raw, source="достройка переезда")
                        lex_units.sync_unit_links_from_card(int(unit_id), raw)
                    except Exception as exc:
                        print("   разбор на слово %s не лёг: %s" % (unit_id, exc))
                fixed_cards += len(group["cards"])
                print("   %r → разбор куплен, карточек %d" % (german[:45], len(group["cards"])))
            # ── Г ──
            dropped_failed = 0
            if run("d") and failed:
                cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id = ANY(%s);",
                            ([int(f[0]) for f in failed],))
                dropped_failed = cur.rowcount or 0
            # ── Д ──
            dropped_homeless = repointed = 0
            for eid, _sl, _tl, _st, _tt, twin in homeless:
                if twin:
                    # У человека уже может быть карточка на близнеца: на пару
                    # (человек, запись) стоит запрет повтора. Тогда указатель просто
                    # обнуляем — своё содержимое карточка хранит сама.
                    cur.execute(
                        """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id = %(twin)s
                           WHERE q.canonical_entry_id = %(eid)s
                             AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries o
                                             WHERE o.user_id = q.user_id
                                               AND o.canonical_entry_id = %(twin)s);""",
                        {"twin": twin, "eid": eid},
                    )
                    repointed += cur.rowcount or 0
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id = NULL WHERE canonical_entry_id = %s;",
                    (eid,),
                )
                cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id = %s;", (eid,))
                dropped_homeless += cur.rowcount or 0
            conn.commit()

    print()
    print("А. убрано заготовок: %d" % removed)
    print("Б. направлений исправлено: %d, заведено слов: %d, перенесено разборов: %d, связей: %d, пропущено: %d"
          % (len(swapped), made_units, moved_cards, made_links, skipped))
    print("В. куплено разборов: %d, починено карточек: %d" % (bought, fixed_cards))
    print("Г. убрано сорвавшихся поисков: %d" % dropped_failed)
    print("Д. убрано сломанных записей: %d, указателей у карточек переставлено: %d"
          % (dropped_homeless, repointed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
