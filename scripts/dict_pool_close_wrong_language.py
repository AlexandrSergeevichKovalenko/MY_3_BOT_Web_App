# -*- coding: utf-8 -*-
"""Общий словарь: убрать задания тренажёра, достать недостающий перевод, развернуть пары.

РЕШЕНИЯ ВЛАДЕЛЬЦА 21.08.2026, дословно:
    A (задания тренажёра) — «им там не место»;
    B и C (записи без второй стороны) — «подбери перевод и оставь их в базе»;
    D (перевёрнутая пара) — развернуть;
    E (разнородные, включая «поднос — das Tablett») — починить раз и навсегда.

ЗАМЕР 21.08.2026 по 17 333 записям пула. Правило отбора взято У ПРОДУКТА: поиск идёт по
`source_text` в паре source_lang→target_lang (`get_pool_dictionary_candidates`), значит
дефект — это когда ОТВЕТ пришёл не на том языке, о котором спросили.

    задания тренажёра в пуле                143  (побеждали в поиске 119)
    ответ целиком на чужом языке             48
    хвост «, -n» в написании (мой недоделок) 15

ПОЧЕМУ ИМЕНА КОЛОНОК НЕ ПРАВИЛО ОТБОРА. `word_de/word_ru/translation_de/translation_ru`
остались от первого продукта, где направление было только рус→нем, и означают «что
спросили» / «что ответили», а не язык. У записи ru→de в поле `word_de` лежит русский, и
это норма. Чинить по именам колонок — сломать верные записи.

ОТКУДА БЕРЁТСЯ НЕДОСТАЮЩИЙ ПЕРЕВОД. Из того же разбора, которым продукт отвечает на любой
новый запрос (`run_dictionary_lookup_multilang_core_fast`), и ответ проверяется на язык
перед записью. Своего мы не сочиняем: не ответила модель — запись остаётся как была и
попадает в отчёт, а не заполняется догадкой.

ДЫРА ЗАКРЫТА В КОДЕ, и в двух местах сразу:
  • вход  — `_upsert_dictionary_canonical_entry_with_cursor` не принимает ни заготовку
            задания, ни ответ на чужом языке (отказ не роняет сохранение человека —
            вокруг записи в пул стоит точка отката);
  • выдача — `get_pool_dictionary_candidates` не отдаёт такие строки, чем бы они туда ни
            попали раньше. Обратный путь эту проверку имел всегда, прямой — не имел.

    python3 scripts/dict_pool_close_wrong_language.py           # показать
    python3 scripts/dict_pool_close_wrong_language.py --apply   # починить
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (  # noqa: E402
    _normalize_dictionary_headword_key,
    _normalize_dictionary_text_key,
    get_db_connection_context,
)
from backend.dictionary_intake import (  # noqa: E402
    answer_language_is_wrong,
    is_exercise_blank,
)

# Школьный хвост «, -n» — тот же, что снят с заголовков слов 21.08.2026. Пятнадцать
# записей пула держат старое написание, потому что тогда я развозил правку сам, а не
# через `spread_correction_everywhere`. Правило то же, чтобы два места не разошлись.
_TAIL = re.compile(r",\s*[-–—]\s*(?:e|en|n|s|er|se|nen)?\s*$", re.I)


def ask_model_for_translation(text: str, source_lang: str, target_lang: str) -> str:
    """Перевод из разбора — тем же путём, каким продукт отвечает на новый запрос."""
    from backend.openai_manager import run_dictionary_lookup_multilang_core_fast
    try:
        raw = asyncio.run(run_dictionary_lookup_multilang_core_fast(
            word=text, source_lang=source_lang, target_lang=target_lang,
            explanation_lang="ru",
        ))
    except Exception as exc:
        print(f"        ⚠️ разбор не получен: {exc}")
        return ""
    if not isinstance(raw, dict):
        return ""
    for name in ("word_target", "target_text", "translation_ru" if target_lang == "ru" else "translation_de"):
        value = str(raw.get(name) or "").strip()
        if value and not answer_language_is_wrong(value, target_lang):
            return value
    for item in (raw.get("translations") or []):
        if isinstance(item, dict):
            value = str(item.get("value") or "").strip()
            if value and not answer_language_is_wrong(value, target_lang):
                return value
    return ""


# Разобранные ГЛАЗАМИ 21.08.2026 записи, у которых испорчены ОБЕ стороны, — правилом
# такое не чинится, потому что чинить не из чего. Список закрытый и с обоснованием
# у каждой строки; «похожее» сюда не добавляется.
#
#   31056 «поднос — das Tablett» → «Таблет (основной перевод, предмет)»
#         Слепок двух слов в одном: das Tablett (поднос) и das Tablet (планшет). Обе
#         правильные пары в пуле УЖЕ ЕСТЬ (348859 das Tablett → поднос, 350123 das
#         Tablet → планшет), эта строка — мусорный третий вариант между ними. Владелец
#         21.08.2026 про это слово: «сколько можно уже этот Tablet исправлять». Снимаем
#         совсем, карточку человека перецепляем на «das Tablett».
#
#   23610 «1 заниматься einen Beruf ausüben — работать по какой-л. специальности»
#         Кусок словарной статьи с номером значения, склеенный из двух языков. Немецкое
#         внутри есть и оно годное — «einen Beruf ausüben», — заменой другой записи в
#         пуле нет. Поэтому не снимаем, а вычищаем до слова и берём перевод разбором.
_HANDPICKED = {
    31056: {"action": "merge_into", "keep": 348859},
    23610: {"action": "retitle", "source": "einen Beruf ausüben"},
}


def repoint_cards(cursor, from_id: int, to_id: int) -> None:
    """Перецепить личные карточки с убираемой строки пула на остающуюся.

    ⚠ У ЧЕЛОВЕКА МОЖЕТ УЖЕ БЫТЬ КАРТОЧКА НА БЛИЗНЕЦЕ. Пара (человек, строка пула)
    уникальна, и слепой UPDATE упирается в неё (прогон 21.08.2026 упал именно здесь).
    Таким карточкам просто снимаем ссылку: сама карточка остаётся у человека — он её
    сохранил, это его право, — она лишь перестаёт указывать на снятую строку.
    """
    cursor.execute(
        """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id=%s
            WHERE q.canonical_entry_id=%s
              AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries t
                               WHERE t.user_id=q.user_id AND t.canonical_entry_id=%s);""",
        (to_id, from_id, to_id))
    cursor.execute(
        "UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL "
        "WHERE canonical_entry_id=%s;", (from_id,))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    removed = translated = turned = retitled = unresolved = deduped = handpicked = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT id, source_lang, target_lang, source_text, target_text
                                FROM bt_3_dictionary_entries ORDER BY id;""")
            rows = cursor.fetchall()

            blanks = [r for r in rows if is_exercise_blank(r[3]) or is_exercise_blank(r[4])]
            blank_ids = {r[0] for r in blanks}
            wrong = [r for r in rows
                     if r[0] not in blank_ids and answer_language_is_wrong(r[4], r[2])]
            tails = [r for r in rows if _TAIL.search(str(r[3] or ""))]

            # Перевёртыш узнаётся точно: обе стороны годятся ДЛЯ ОБРАТНОГО направления.
            # Тогда лечится сменой направления, и ни один текст не трогается.
            def is_flipped(row) -> bool:
                _id, sl, tl, st, tt = row
                return (not answer_language_is_wrong(tt, sl)
                        and not answer_language_is_wrong(st, tl))

            flipped = [r for r in wrong if is_flipped(r)]
            need_translation = [r for r in wrong if not is_flipped(r)]

            print(f"\nзаписей в пуле: {len(rows)}")
            print(f"  A. задания тренажёра — УБИРАЕМ:        {len(blanks)}")
            print(f"  D. пара верная, направление наоборот:  {len(flipped)}")
            print(f"  B+C+E. второй стороны нет, нужен перевод: {len(need_translation)}")
            print(f"  хвост «, -n» в написании:              {len(tails)}\n")

            if not args.apply:
                for r in wrong[:10]:
                    print(f"   {r[0]} [{r[1]}→{r[2]}] {str(r[3])[:40]!r} → {str(r[4])[:34]!r}")
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            # ── A. Задания тренажёра уходят из общего словаря ────────────────────
            # Внешнего ключа на пул нет, поэтому ссылку из личных карточек снимаем
            # РУКАМИ и ДО удаления: иначе останется указатель в никуда.
            if blank_ids:
                ids = sorted(blank_ids)
                cursor.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id = NULL "
                    "WHERE canonical_entry_id = ANY(%s);", (ids,))
                print(f"  отвязано личных карточек: {cursor.rowcount}")
                cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id = ANY(%s);",
                               (ids,))
                removed = cursor.rowcount or 0
            conn.commit()   # шаг закрыт — дальше падать уже не страшно

            # ── D. Перевёрнутая пара: меняем направление, тексты не трогаем ──────
            #
            # ⚠ Строка опознаётся четвёркой (source_lang, target_lang, source_text_norm,
            # target_text_norm). Смена направления МЕНЯЕТ ключ, поэтому сначала смотрим,
            # не живёт ли такая запись уже: если живёт — наша строка дубль, и её надо
            # снять, а не втискивать поверх чужой.
            for entry_id, sl, tl, st, tt in flipped:
                cursor.execute(
                    """SELECT id FROM bt_3_dictionary_entries
                        WHERE source_lang=%s AND target_lang=%s
                          AND source_text_norm=%s AND target_text_norm=%s AND id<>%s
                        LIMIT 1;""",
                    (tl, sl, _normalize_dictionary_text_key(st),
                     _normalize_dictionary_text_key(tt), entry_id))
                twin = cursor.fetchone()
                if twin:
                    repoint_cards(cursor, entry_id, int(twin[0]))
                    cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;",
                                   (entry_id,))
                    print(f"  дубль снят: {entry_id} — такая запись уже есть под {twin[0]}")
                    turned += 1
                    continue
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET source_lang=%s, target_lang=%s, updated_at=NOW()
                        WHERE id=%s;""", (tl, sl, entry_id))
                turned += 1
                print(f"  развернул направление: {entry_id} {st[:40]!r} → {tt[:34]!r}")
            conn.commit()

            # ── B, C, E. Второй стороны нет — достаём перевод из разбора ─────────
            for entry_id, sl, tl, st, tt in need_translation:
                print(f"  {entry_id} [{sl}→{tl}] {st[:44]!r}")
                answer = ask_model_for_translation(st, sl, tl)
                if not answer:
                    unresolved += 1
                    print("        ⚠️ перевод не получен — запись оставлена как была")
                    continue
                # ⚠ У ЧАСТИ ЭТИХ ЗАПИСЕЙ ВЕРНЫЙ БЛИЗНЕЦ УЖЕ ЕСТЬ. «Взять мазок» с
                # ответом «Einen Abstrich machen» лежит в пуле отдельной строкой, и наша
                # битая — просто её дубль с испорченным ответом. Вписать перевод поверх
                # значило бы упереться в уникальность пары (так первый прогон и упал,
                # откатившись целиком). Дубль снимаем, карточки людей перецепляем на
                # хорошую строку — человек своё слово не теряет.
                key_source = _normalize_dictionary_text_key(st)
                key_answer = _normalize_dictionary_text_key(answer)
                cursor.execute(
                    """SELECT id FROM bt_3_dictionary_entries
                        WHERE source_lang=%s AND target_lang=%s
                          AND source_text_norm=%s AND target_text_norm=%s AND id<>%s
                        LIMIT 1;""", (sl, tl, key_source, key_answer, entry_id))
                twin = cursor.fetchone()
                if twin:
                    repoint_cards(cursor, entry_id, int(twin[0]))
                    cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;",
                                   (entry_id,))
                    deduped += 1
                    print(f"        дубль: верная строка уже есть под {twin[0]} — эту снял")
                    continue
                # Ключи считает ПРОДУКТ своей функцией. Своя «нормализация» здесь
                # означала бы, что запись перестанет находиться поиском: он ищет по
                # `_normalize_dictionary_text_key`, а не по casefold.
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET target_text=%s, target_text_norm=%s,
                              target_headword_norm=%s, updated_at=NOW()
                        WHERE id=%s;""",
                    (answer, key_answer,
                     _normalize_dictionary_headword_key(answer) or None, entry_id))
                translated += 1
                print(f"        → {answer[:60]!r}")

            conn.commit()   # перевод стоит денег — закрепляем до следующего шага

            # ── Разобранное глазами: обе стороны испорчены ───────────────────────
            for entry_id, plan in _HANDPICKED.items():
                cursor.execute("SELECT source_lang, target_lang, source_text FROM "
                               "bt_3_dictionary_entries WHERE id=%s;", (entry_id,))
                row = cursor.fetchone()
                if not row:
                    continue                      # уже снята прошлым прогоном
                sl, tl, st = row
                if plan["action"] == "merge_into":
                    repoint_cards(cursor, entry_id, int(plan["keep"]))
                    cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;",
                                   (entry_id,))
                    handpicked += 1
                    print(f"  снял мусорную строку {entry_id}: верная пара уже есть "
                          f"под {plan['keep']}")
                    continue
                clean = str(plan["source"])
                answer = ask_model_for_translation(clean, sl, tl)
                if not answer:
                    unresolved += 1
                    print(f"  ⚠️ {entry_id}: перевод для {clean!r} не получен — оставил как было")
                    continue
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET source_text=%s, source_text_norm=%s, source_headword_norm=%s,
                              target_text=%s, target_text_norm=%s, target_headword_norm=%s,
                              updated_at=NOW()
                        WHERE id=%s;""",
                    (clean, _normalize_dictionary_text_key(clean),
                     _normalize_dictionary_headword_key(clean) or None,
                     answer, _normalize_dictionary_text_key(answer),
                     _normalize_dictionary_headword_key(answer) or None, entry_id))
                handpicked += 1
                print(f"  {entry_id}: {st[:40]!r} → {clean!r} = {answer[:40]!r}")
            conn.commit()

            # ── Повисшие указатели: строки пула больше нет, ссылка осталась ──────
            # Их оставляют удаления выше. Указатель в никуда — это не «пусто», это
            # ложь о том, что у карточки есть общая запись.
            cursor.execute(
                """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id = NULL
                    WHERE q.canonical_entry_id IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM bt_3_dictionary_entries e
                                       WHERE e.id = q.canonical_entry_id);""")
            if cursor.rowcount:
                print(f"  снято повисших указателей: {cursor.rowcount}")
            conn.commit()

            # ── Хвост «, -n»: доделка за собой ──────────────────────────────────
            #
            # Та же ловушка, что выше: «die Regierung, -en» после чистки совпадает с уже
            # живущей «die Regierung» → «правительство». Снятие хвоста МЕНЯЕТ ключ
            # строки, поэтому сначала ищем близнеца.
            for entry_id, sl, tl, st, tt in tails:
                clean = _TAIL.sub("", str(st)).strip()
                if not clean or clean == st:
                    continue
                cursor.execute(
                    """SELECT id FROM bt_3_dictionary_entries
                        WHERE source_lang=%s AND target_lang=%s
                          AND source_text_norm=%s AND target_text_norm=%s AND id<>%s
                        LIMIT 1;""",
                    (sl, tl, _normalize_dictionary_text_key(clean),
                     _normalize_dictionary_text_key(tt), entry_id))
                twin = cursor.fetchone()
                if twin:
                    repoint_cards(cursor, entry_id, int(twin[0]))
                    cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;",
                                   (entry_id,))
                    deduped += 1
                    print(f"  дубль по хвосту: {st[:40]!r} уже есть под {twin[0]} — снял")
                    continue
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET source_text=%s, source_text_norm=%s,
                              source_headword_norm=%s, updated_at=NOW()
                        WHERE id=%s;""",
                    (clean, _normalize_dictionary_text_key(clean),
                     _normalize_dictionary_headword_key(clean) or None, entry_id))
                retitled += 1
        conn.commit()

    print(f"\nубрано заданий: {removed}, переведено: {translated}, развёрнуто: {turned}, "
          f"дублей снято: {deduped}, хвостов снято: {retitled}, "
          f"разобрано глазами: {handpicked}, не смогли: {unresolved}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
