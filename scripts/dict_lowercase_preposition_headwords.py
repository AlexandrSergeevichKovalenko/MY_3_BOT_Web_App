# -*- coding: utf-8 -*-
"""Предлог в заголовке пишется со строчной: «Aus Gag» → «aus Gag».

ЧТО УВИДЕЛ ВЛАДЕЛЕЦ 22.08.2026. Набрал «Aus Gag» — быстрый словарь показал «die Aus Gag»,
пометил фразу существительным и построил склонение «die Aus Gag / der Aus Gags». Ни одного
из этих слов не существует. Всё началось с одной заглавной буквы: модель скопировала
написание из строки поиска, заглавная сделала оборот похожим на существительное, дальше
приклеился артикль и построилась таблица.

ДЫРА ЗАКРЫТА В КОДЕ, в двух местах:
  • задание модели: немецкий заголовок пишется по правилам немецкого, а НЕ копируется с
    того, что набрал человек (backend/openai_manager.py, dictionary_assistant_multilang);
  • артикль и пометка «существительное» снимаются с любого многословного — теперь и на
    живом ответе (`_build_dictionary_result_from_raw`), а не только при сохранении.
Проверено на семи словах, которых в задании нет: «Mit Absicht» → «mit Absicht», «Zu Fuß»
→ «zu Fuß», «Nach Hause» → «nach Hause» и так далее, семь из семи.

ПРАВИЛО ОТБОРА ЗДЕСЬ — НАРОЧНО УЗКОЕ, и вот почему. Первая версия правила ловила «первое
слово с заглавной и оно служебное» и дала 4209 карточек — потому что считала дефектом
обычные предложения без точки: «Er räumte ein», «Wenn ich mich richtig erinnere». Там
заглавная законна, это предложение. Отличать предложение от оборота по наличию глагола я
не берусь, поэтому сузил до случая, где предложения БЫТЬ НЕ МОЖЕТ: ровно два слова, первое
— предлог из закрытого списка, второе с заглавной. Глагола нет — значит не предложение,
значит заглавная у предлога неверна.

ЗАМЕР 22.08.2026 по этому правилу: слова словаря 17, карточки людей 21, общий пул 5.

    python3 scripts/dict_lowercase_preposition_headwords.py           # показать
    python3 scripts/dict_lowercase_preposition_headwords.py --apply   # починить
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (  # noqa: E402
    _normalize_dictionary_headword_key,
    _normalize_dictionary_text_key,
    get_db_connection_context,
)
from backend.lex_units import retitle_unit  # noqa: E402

# Закрытый список немецких предлогов. Существительных здесь нет и быть не может —
# именно поэтому правило безопасно: «Aus» в двусловном обороте предлогом и является.
PREPOSITIONS = {
    "aus", "in", "an", "auf", "bei", "mit", "nach", "seit", "von", "zu", "über",
    "unter", "vor", "hinter", "neben", "zwischen", "durch", "für", "gegen", "ohne",
    "um", "bis", "ab", "trotz", "während", "wegen", "statt", "innerhalb", "außerhalb",
    "entlang", "gegenüber", "samt", "laut",
}


def needs_lowering(text) -> bool:
    parts = " ".join(str(text or "").split()).split()
    if len(parts) != 2:
        return False
    first, second = parts
    if not first[:1].isupper() or first.lower() not in PREPOSITIONS:
        return False
    return second[:1].isupper()


def lowered(text: str) -> str:
    parts = " ".join(str(text or "").split()).split()
    return " ".join([parts[0][:1].lower() + parts[0][1:]] + parts[1:])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    units = cards = pool = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, display FROM bt_3_lex_units WHERE lang='de';")
            unit_rows = [(i, v) for i, v in cursor.fetchall() if needs_lowering(v)]
            cursor.execute("SELECT id, word_de, translation_de FROM "
                           "bt_3_webapp_dictionary_queries WHERE word_de IS NOT NULL;")
            card_rows = [(i, w, t) for i, w, t in cursor.fetchall() if needs_lowering(w)]
            cursor.execute("SELECT id, source_lang, target_lang, source_text, target_text "
                           "FROM bt_3_dictionary_entries WHERE source_lang='de';")
            pool_rows = [r for r in cursor.fetchall() if needs_lowering(r[3])]

            print(f"\nслова словаря:  {len(unit_rows)}")
            for uid, display in unit_rows:
                print(f"      {uid:>6} {display!r} → {lowered(display)!r}")
            print(f"\nкарточки людей: {len(card_rows)}")
            for cid, word, _t in card_rows[:8]:
                print(f"      {cid:>7} {word!r} → {lowered(word)!r}")
            print(f"\nобщий пул:      {len(pool_rows)}")
            for eid, _sl, _tl, src, _tt in pool_rows:
                print(f"      {eid:>7} {src!r} → {lowered(src)!r}")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            # ── Слова словаря: через retitle_unit, иначе разъедутся ключ и вид записи,
            # а новое написание не станет дверью поиска.
            for uid, display in unit_rows:
                clean = lowered(display)
                cursor.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s "
                    "AND id<>%s LIMIT 1;",
                    (_normalize_dictionary_text_key(clean), uid))
                # Ключ поиска не различает регистр, поэтому столкновения быть не должно;
                # если оно всё же есть — это разные слова, и сливать их молча нельзя.
                if cursor.fetchone():
                    skipped += 1
                    print(f"  ⚠️ {uid} {display!r}: рядом уже живёт такое слово — владельцу")
                    continue
                retitle_unit(cursor, uid, clean)
                units += 1

            for cid, word, trans in card_rows:
                cursor.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, "
                    "translation_de=%s, updated_at=NOW() WHERE id=%s;",
                    (lowered(word),
                     lowered(trans) if needs_lowering(trans) else trans, cid))
                cards += 1

            for eid, sl, tl, src, tgt in pool_rows:
                clean = lowered(src)
                cursor.execute(
                    """SELECT id FROM bt_3_dictionary_entries
                        WHERE source_lang=%s AND target_lang=%s
                          AND source_text_norm=%s AND target_text_norm=%s AND id<>%s
                        LIMIT 1;""",
                    (sl, tl, _normalize_dictionary_text_key(clean),
                     _normalize_dictionary_text_key(tgt), eid))
                twin = cursor.fetchone()
                if twin:
                    # Такая запись уже есть — наша дубль. Карточки перецепляем, а у кого
                    # карточка на близнеце уже есть, тому просто снимаем ссылку.
                    cursor.execute(
                        """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id=%s
                            WHERE q.canonical_entry_id=%s AND NOT EXISTS (
                                SELECT 1 FROM bt_3_webapp_dictionary_queries t
                                 WHERE t.user_id=q.user_id AND t.canonical_entry_id=%s);""",
                        (int(twin[0]), eid, int(twin[0])))
                    cursor.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL "
                        "WHERE canonical_entry_id=%s;", (eid,))
                    cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;", (eid,))
                    print(f"  дубль снят: {eid} {src!r} — такая запись уже есть под {twin[0]}")
                    pool += 1
                    continue
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET source_text=%s, source_text_norm=%s, source_headword_norm=%s,
                              updated_at=NOW()
                        WHERE id=%s;""",
                    (clean, _normalize_dictionary_text_key(clean),
                     _normalize_dictionary_headword_key(clean) or None, eid))
                pool += 1
        conn.commit()

    print(f"\nслов: {units}, карточек: {cards}, записей пула: {pool}, "
          f"отдано владельцу: {skipped}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
