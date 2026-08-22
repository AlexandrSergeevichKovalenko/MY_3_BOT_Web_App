# -*- coding: utf-8 -*-
"""Слить слова с грязным заголовком в их правильных близнецов.

ЧТО ЗА ГРЯЗЬ. Замер 22.08.2026 по 41 628 словам живой базы: семь заголовков не проходят
общую чистку, и четыре из них — одного класса, самого вредного:

    трениe        латинская «e» внутри русского слова
    плаксa        латинская «a»
    устройcтво    латинская «c»
    Грубo …       латинская «o»
    Кроме того,   хвостовая запятая
    Садиться ,    она же
    Falle nicht auf Betrüger herein,

Латинская буква глазом не видна, но она попадает и в `lemma_key` — ключ, по которому
слово ищут. Значит слово НЕ НАХОДИТСЯ по своему же имени: человек набирает «трение»,
а его «трениe» лежит рядом и не отзывается.

Шесть из семи — не грязь, а ДУБЛИКАТЫ: правильное слово уже есть в базе. Поэтому здесь
не переименование, а слияние.

ДЫРА, ЧЕРЕЗ КОТОРУЮ ОНИ ПОПАЛИ, ЗАКРЫТА: `lex_units.retitle_unit` чистит вход сам, а не
надеется на вызывающего (коммит 22.08.2026). Этот скрипт делает вторую половину.

ЧТО ПЕРЕНОСИТСЯ НА ПРАВИЛЬНОЕ СЛОВО, А ЧТО НЕТ
──────────────────────────────────────────────
Порядок взят у `scripts/lex_units_fold_forms.py` — он уже проверен на слиянии форм, и
изобретать свой нельзя: у `bt_3_lex_links` уникальный ключ, и перенос конфликтующей
связи откатывает всю транзакцию целиком (обжигались 20.08.2026).

    разбор          переносится, только если у правильного его нет
    связи-переводы  ON CONFLICT DO NOTHING — конфликтующие просто не едут
    происхождение   так же
    ГРЯЗНОЕ НАПИСАНИЕ становится указателем на правильное слово

Последнее — не косметика. Указатель написаний для того и заведён, чтобы человек,
запомнивший кривой вариант, продолжал находить слово. Грязное написание не выбрасывается,
а начинает вести к правильному заголовку.

КУДА СЛИВАТЬ, ЕСЛИ ПРАВИЛЬНЫХ ДВОЕ. Берётся тот, у кого есть разбор; если разбора нет ни
у кого — старший по возрасту (меньший id). Второй близнец НЕ ТРОГАЕТСЯ: это отдельная
пара дубликатов, она существовала до нас, и решать по ней — отдельный разговор.

ЗАПУСК:
    python3 scripts/lex_units_fix_dirty_headwords.py           # показать
    python3 scripts/lex_units_fix_dirty_headwords.py --apply   # слить
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _план(cur):
    """[(грязный id, написание, правильный id или None, чистое написание, лишние близнецы)]"""
    from backend.dictionary_intake import clean_text

    cur.execute("SELECT id, lang, display, lemma_key FROM bt_3_lex_units WHERE display <> '';")
    все = cur.fetchall() or []
    план = []
    for uid, lang, display, _key in все:
        чисто = clean_text(display)
        if not чисто or чисто == display:
            continue
        cur.execute(
            "SELECT id, card IS NOT NULL FROM bt_3_lex_units "
            "WHERE lang=%s AND lemma_key=%s AND id<>%s ORDER BY id;",
            (lang, чисто.strip().lower(), int(uid)))
        близнецы = cur.fetchall() or []
        цель = None
        лишние = []
        if близнецы:
            сразбором = [b for b in близнецы if b[1]]
            цель = (сразбором or близнецы)[0][0]
            лишние = [b[0] for b in близнецы if b[0] != цель]
        план.append((int(uid), str(display), цель, чисто, лишние))
    return план


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.lex_units import retitle_unit

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            план = _план(cur)

    print(f"Заголовков, не проходящих чистку: {len(план)}")
    for uid, display, цель, чисто, лишние in план:
        куда = f"слить в {цель}" if цель else "переименовать (близнеца нет)"
        хвост = f"  ⚠ ещё дубликаты, НЕ трогаю: {лишние}" if лишние else ""
        print(f"  {uid:6} {display!r} → {чисто!r}   {куда}{хвост}")
    if not план:
        return 0
    if not apply:
        print("\nЭто показ. Чтобы применить — добавь --apply")
        return 0

    слито = переименовано = 0
    for uid, display, цель, чисто, _лишние in план:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                if цель is None:
                    # Близнеца нет — просто чистое имя. `retitle_unit` сама чистит вход,
                    # пересчитывает ключ поиска и вид записи и заводит указатель.
                    retitle_unit(cur, uid, чисто)
                    переименовано += 1
                    conn.commit()
                    continue
                cur.execute("SELECT lang FROM bt_3_lex_units WHERE id=%s;", (uid,))
                lang = str((cur.fetchone() or ("de",))[0])
                # Разбор — только если у правильного его нет.
                cur.execute("""
                    UPDATE bt_3_lex_units AS target
                       SET card = dirty.card, card_source = 'перенесён с грязного заголовка',
                           updated_at = NOW()
                      FROM bt_3_lex_units AS dirty
                     WHERE target.id = %s AND dirty.id = %s
                       AND target.card IS NULL AND dirty.card IS NOT NULL;
                """, (цель, uid))
                cur.execute("""
                    INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count)
                    SELECT %s, to_unit, rank, COALESCE(source, 'слияние заголовков'), saves_count
                      FROM bt_3_lex_links WHERE from_unit = %s
                    ON CONFLICT (from_unit, to_unit) DO NOTHING;
                """, (цель, uid))
                cur.execute("""
                    INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count)
                    SELECT from_unit, %s, rank, COALESCE(source, 'слияние заголовков'), saves_count
                      FROM bt_3_lex_links WHERE to_unit = %s
                    ON CONFLICT (from_unit, to_unit) DO NOTHING;
                """, (цель, uid))
                cur.execute("""
                    INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side)
                    SELECT %s, entry_id, side FROM bt_3_lex_unit_sources WHERE unit_id = %s
                    ON CONFLICT DO NOTHING;
                """, (цель, uid))
                # Грязное написание становится указателем на правильное слово: человек,
                # запомнивший кривой вариант, продолжает находить слово.
                cur.execute("""
                    INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                    VALUES (%s, %s, %s, 'variant') ON CONFLICT DO NOTHING;
                """, (lang, display.strip().casefold(), цель))
                cur.execute("""
                    INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                    SELECT lang, surface_key, %s, match_kind
                      FROM bt_3_lex_surfaces WHERE unit_id = %s
                    ON CONFLICT DO NOTHING;
                """, (цель, uid))
                cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id = %s;", (uid,))
                cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s;",
                            (uid, uid))
                cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (uid,))
                слито += 1
            conn.commit()

    print(f"\nСлито: {слито} · переименовано: {переименовано}")

    # Проверка ФАКТОМ, а не намерением: спрашиваем базу заново.
    from backend.dictionary_intake import clean_text
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, display FROM bt_3_lex_units WHERE display <> '';")
            осталось = [(i, d) for i, d in cur.fetchall() if clean_text(d) != d]
            # И главное — находится ли слово по своему имени.
            cur.execute("""
                SELECT u.id, u.display FROM bt_3_lex_units u
                 WHERE u.id = ANY(%s);
            """, ([p[2] for p in план if p[2]],))
            цели = cur.fetchall() or []
    print(f"Грязных заголовков осталось: {len(осталось)}  {осталось if осталось else ''}")
    print("Правильные слова на месте:")
    for i, d in цели:
        print(f"   {i}  {d!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
