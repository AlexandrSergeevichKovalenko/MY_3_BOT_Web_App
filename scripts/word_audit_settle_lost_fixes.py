# -*- coding: utf-8 -*-
"""Догнать правки владельца, которые экран проверки слов потерял. Разбор 28.08.2026.

ЧТО ЭТО ЧИНИТ. Два дефекта экрана проверки слов, найденные 28.08.2026 и в тот же день
закрытых в коде. Здесь — вторая половина работы: то, что уже накопилось в базе.

  A. ПОДМЕНЁННЫЙ ВАРИАНТ. Экран показывает урезанный список кнопок, а применение брало
     присланный НОМЕР из полного списка. Номера разъезжались, и записывался не тот
     текст, который человек нажал. Чинится в коде тем, что номер с этого экрана больше
     не ходит вовсе (`word_confirm_digest.кнопки_вариантов`).

  B. ПРАВКА, ВЫБРОШЕННАЯ МОЛЧА. Если правильная фраза уже лежала в словаре отдельной
     записью, правка не применялась: вопрос закрывался, а у человека оставался кривой
     немецкий, и на проверку эта фраза больше не приходила НИКОГДА. Чинится в коде
     слиянием двойника (`database.apply_phrase_review_decision`, решение владельца
     28.08.2026 «сливай автоматически»).

ЧЕМ ЧИНИТ. Ничего своего: обе половины гоняют ТУ ЖЕ `apply_phrase_review_decision`,
которой работает продукт. Своя копия этой логики означала бы третий путь правки фразы,
который завтра разойдётся с остальными двумя.

    python3 scripts/word_audit_settle_lost_fixes.py            # только посмотреть
    python3 scripts/word_audit_settle_lost_fixes.py --apply    # починить

Замер 28.08.2026 (перед починкой): класс A — 2 записи (#317, #319), класс B — 5 записей
(#164, #233, #239, #307, #335). Шестая запись класса B (#126 «Ich schämemich für dich»)
на момент разбора уже была вылечена слиянием двойника — сырое число было 6, дефектных
5. Если новый прогон даёт другие числа, это повод разбираться, а не мерить с нуля.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import (  # noqa: E402
    apply_phrase_review_decision, get_db_connection_context, phrase_review_variants,
)
from backend.word_confirm_digest import кнопки_вариантов  # noqa: E402


def _строки(sql, params=()):
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() or []


def _выполнить(sql, params=()):
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            задето = cur.rowcount or 0
        conn.commit()
    return задето


# ── A. ПОДМЕНЁННЫЙ ВАРИАНТ ───────────────────────────────────────────────────
def найти_подмены() -> list[dict]:
    """Где записан НЕ тот вариант, который человек нажал.

    Восстанавливаем нажатое ровно тем же смещением, каким его портил старый код:
    записанный текст стоял в ПОЛНОМ списке под тем номером, под которым на ЭКРАНЕ
    стоял другой. Значит нажатым был вариант экрана под тем же номером. Ничего не
    угадываем — это арифметика того же сдвига, только в обратную сторону.
    """
    находки = []
    for rid, текст, судьи, арбитр, записано, unit_id, лемма in _строки("""
            SELECT r.id, btrim(r.text), r.judges, r.arbiter, COALESCE(r.decided_text,''),
                   r.unit_id, u.lemma
              FROM bt_3_phrase_review r
              JOIN bt_3_lex_units u ON u.id = r.unit_id
             WHERE r.status IN ('accepted','replaced') AND COALESCE(r.decided_text,'') <> ''
             ORDER BY r.id;"""):
        судьи = судьи if isinstance(судьи, list) else []
        арбитр = арбитр if isinstance(арбитр, dict) else None
        полный = [v["text"] for v in phrase_review_variants(
            судьи, текст, арбитр, include_disputed=True)]
        экран = [к["text"] for к in кнопки_вариантов(судьи, текст, арбитр)]
        if записано not in полный:
            continue                      # решение принято не кнопкой (свой текст)
        номер = полный.index(записано)
        if номер >= len(экран) or экран[номер] == записано:
            continue                      # списки в этом месте совпадают — подмены нет
        if (лемма or "").strip() != записано.strip():
            # Запись с тех пор уже переписана чем-то ещё. Возвращать нажатое поверх
            # чужой правки нельзя — это решение владельца, а не арифметика.
            находки.append({"id": rid, "unit_id": unit_id, "было": текст,
                            "записано": записано, "нажато": экран[номер],
                            "лемма": лемма, "чинить": False})
            continue
        находки.append({"id": rid, "unit_id": unit_id, "было": текст,
                        "записано": записано, "нажато": экран[номер],
                        "лемма": лемма, "чинить": True,
                        "ru": _перевод_варианта(судьи, арбитр, экран[номер])})
    return находки


def _перевод_варианта(судьи, арбитр, текст: str) -> str:
    """Русский БЕРЁТСЯ С ТОЙ ЖЕ КНОПКИ, а не собирается заново."""
    for в in phrase_review_variants(судьи, "", арбитр, include_disputed=True):
        if str(в.get("text") or "").strip() == текст.strip():
            return str(в.get("ru") or "").strip()
    for судья in (судьи or []):
        for поле in ("corrected", "proposal"):
            if str((судья or {}).get(поле) or "").strip() == текст.strip():
                return str((судья or {}).get(f"{поле}_ru") or "").strip()
    if str((арбитр or {}).get("better") or "").strip() == текст.strip():
        return str((арбитр or {}).get("better_ru") or "").strip()
    return ""


def починить_подмену(находка: dict) -> dict:
    """Записать то, что человек нажал.

    Новая строка проверки, а не правка старой: старая — след того, ЧТО произошло, и
    переписывать историю мы не станем. Новая заводится с текстом, который лежит в
    словаре СЕЙЧАС, — иначе развоз правки пошёл бы искать по карточкам текст, которого
    там давно нет. Решение — «replace»: это выбор человека, а не приговор судьи, и
    выдумывать под него судейскую строку нельзя.
    """
    занято = _строки("SELECT id FROM bt_3_phrase_review WHERE unit_id=%s AND status='open';",
                     (находка["unit_id"],))
    if занято:
        return {"пропущено": f"по слову уже открыт вопрос #{занято[0][0]}"}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bt_3_phrase_review (unit_id, text, translation, judges,
                                                status, kind)
                     SELECT %s, %s, COALESCE(r.translation,''), '[]'::jsonb, 'open', 'grammar'
                       FROM bt_3_phrase_review r WHERE r.id = %s
                  RETURNING id;""",
                        (находка["unit_id"], находка["записано"], находка["id"]))
            новый = int(cur.fetchone()[0])
        conn.commit()
    итог = apply_phrase_review_decision(новый, "replace", находка["нажато"], 0,
                                        находка.get("ru") or "")
    return {"вопрос": новый, "стало": итог.get("text") or "",
            "разбор пересобран": bool(итог.get("breakdown_rebuilt")),
            "слито с": итог.get("merged_into") or 0}


# ── B. ПРАВКА, ВЫБРОШЕННАЯ МОЛЧА ─────────────────────────────────────────────
def найти_выброшенные() -> list[dict]:
    """Вопрос закрыт, а текст остался прежним — значит правка не доехала.

    Отбираем только те, где кнопка на экране была РОВНО ОДНА: тогда нажатое
    однозначно. Будь их две, мы бы гадали, какую он нажал, — а гадать здесь нельзя.
    """
    находки = []
    for rid, unit_id, текст, судьи, арбитр, лемма in _строки("""
            SELECT r.id, r.unit_id, btrim(r.text), r.judges, r.arbiter, u.lemma
              FROM bt_3_phrase_review r
              JOIN bt_3_lex_units u ON u.id = r.unit_id
             WHERE r.status = 'closed' AND COALESCE(r.decided_text,'') = ''
             ORDER BY r.id;"""):
        судьи = судьи if isinstance(судьи, list) else []
        арбитр = арбитр if isinstance(арбитр, dict) else None
        кнопки = кнопки_вариантов(судьи, текст, арбитр)
        if (лемма or "").strip() != (текст or "").strip():
            continue                      # уже вылечено чем-то другим — не наш случай
        if len(кнопки) != 1:
            находки.append({"id": rid, "unit_id": unit_id, "было": текст,
                            "кнопок": len(кнопки), "чинить": False})
            continue
        находки.append({"id": rid, "unit_id": unit_id, "было": текст,
                        "нажато": кнопки[0]["text"], "кнопок": 1, "чинить": True})
    return находки


def починить_выброшенную(находка: dict) -> dict:
    """Тот же вопрос открывается заново и решается уже починенной машинерией.

    Заводить новую строку здесь незачем: на этот вопрос в самом деле НИКТО не ответил —
    решение человека до базы не доехало. Открываем его обратно и применяем.
    """
    занято = _строки("SELECT id FROM bt_3_phrase_review WHERE unit_id=%s AND status='open';",
                     (находка["unit_id"],))
    if занято:
        return {"пропущено": f"по слову уже открыт вопрос #{занято[0][0]}"}
    _выполнить("UPDATE bt_3_phrase_review SET status='open', decided_at=NULL, "
               "decided_text=NULL WHERE id=%s;", (находка["id"],))
    итог = apply_phrase_review_decision(находка["id"], "accept", "", 0, "",
                                        chosen_text=находка["нажато"])
    return {"стало": итог.get("text") or "",
            "слито с": итог.get("merged_into") or 0,
            "разбор пересобран": bool(итог.get("breakdown_rebuilt"))}


# ── C. ПОВТОР, ОСТАВШИЙСЯ ПОСЛЕ СЛИЯНИЯ ──────────────────────────────────────
def найти_повторы(единицы: list[int]) -> list[dict]:
    """Слияние свело у человека две карточки одного слова. Совпали ли они ЦЕЛИКОМ.

    Правило то же, что у продукта: сносим только полное совпадение — и слово, и
    перевод. Различаются переводом — это две РАЗНЫЕ карточки человека, не наше дело.
    """
    находки = []
    for unit_id in единицы:
        for user_id, in _строки("""SELECT user_id FROM bt_3_webapp_dictionary_queries
                                    WHERE lex_unit_id=%s GROUP BY user_id
                                   HAVING COUNT(*) > 1;""", (unit_id,)):
            карточки = _строки("""SELECT id, word_de, COALESCE(translation_ru,'')
                                    FROM bt_3_webapp_dictionary_queries
                                   WHERE lex_unit_id=%s AND user_id=%s
                                   ORDER BY created_at, id;""", (unit_id, user_id))
            переводы = {(п or "").strip().casefold() for _, _, п in карточки}
            находки.append({
                "unit_id": unit_id, "user_id": user_id,
                "карточки": [(c, п) for c, _, п in карточки],
                "слово": карточки[0][1],
                "чинить": len(переводы) == 1,
            })
    return находки


def убрать_повтор(находка: dict) -> int:
    """Хранитель — самая старая карточка: её разбор собран под верный текст."""
    from backend.database import dedupe_personal_entry_after_save
    хранитель = находка["карточки"][0][0]
    return int(dedupe_personal_entry_after_save(находка["user_id"], хранитель) or 0)


def проверить(unit_id: int, ожидаем: str) -> str:
    """Сверка ПО ФАКТУ: что теперь видит человек в своей карточке."""
    карточки = _строки("""SELECT q.id, q.user_id, q.word_de
                            FROM bt_3_webapp_dictionary_queries q
                           WHERE q.lex_unit_id = %s;""", (unit_id,))
    if not карточки:
        return "карточек на этой единице нет"
    криво = [к for к in карточки if (к[2] or "").strip() != ожидаем.strip()]
    return ("✅ у всех карточек новый текст" if not криво
            else f"⚠ старый текст остался у {len(криво)} карточек: {криво[:3]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="чинить, а не только смотреть")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    подмены = найти_подмены()
    выброшенные = найти_выброшенные()

    print("\n═══ A. ЗАПИСАН НЕ ТОТ ВАРИАНТ, КОТОРЫЙ НАЖАЛ ЧЕЛОВЕК ═══")
    print(f"   найдено: {len(подмены)}, из них чиним {sum(1 for н in подмены if н['чинить'])}")
    for н in подмены:
        метка = "чиним" if н["чинить"] else "НЕ ТРОГАЕМ (запись уже переписана позже)"
        print(f"\n   #{н['id']}  {метка}")
        print(f"      было в словаре : {н['было']!r}")
        print(f"      записалось     : {н['записано']!r}")
        print(f"      человек нажимал: {н['нажато']!r}")

    print("\n═══ B. ПРАВКА ВЫБРОШЕНА МОЛЧА (правильная фраза уже была в словаре) ═══")
    print(f"   найдено: {len(выброшенные)}, "
          f"из них чиним {sum(1 for н in выброшенные if н['чинить'])}")
    for н in выброшенные:
        if not н["чинить"]:
            print(f"\n   #{н['id']}  НЕ ТРОГАЕМ: кнопок на экране было {н['кнопок']}, "
                  f"нажатое неоднозначно — {н['было']!r}")
            continue
        print(f"\n   #{н['id']}  чиним")
        print(f"      у человека сейчас: {н['было']!r}")
        print(f"      человек нажимал  : {н['нажато']!r}")

    # Слитые слова: те, что уже слиты прошлым прогоном, плюс те, что сольются сейчас.
    слитые = [int(u) for (u,) in _строки("""
        SELECT DISTINCT u.id FROM bt_3_lex_units u
          JOIN bt_3_phrase_review r ON r.unit_id = u.id
         WHERE r.status IN ('accepted','replaced')
           AND r.decided_at > NOW() - INTERVAL '2 days';""")]
    повторы = найти_повторы(слитые)
    print("\n═══ C. ПОСЛЕ СЛИЯНИЯ У ЧЕЛОВЕКА ДВЕ КАРТОЧКИ ОДНОГО СЛОВА ═══")
    print(f"   найдено: {len(повторы)}, "
          f"из них убираем {sum(1 for н in повторы if н['чинить'])} "
          f"(только там, где совпал и перевод)")
    for н in повторы:
        метка = "убираем повтор" if н["чинить"] else "НЕ ТРОГАЕМ: переводы разные"
        print(f"\n   {н['слово']!r} — {метка}")
        for cid, перевод in н["карточки"]:
            print(f"      карточка {cid}: {перевод!r}")

    if not args.apply:
        print("\n(это только просмотр — чинить: --apply)")
        return 0

    print("\n═══ ЧИНЮ ═══")
    for н in подмены:
        if not н["чинить"]:
            continue
        итог = починить_подмену(н)
        print(f"   #{н['id']} → {итог}")
        if итог.get("стало"):
            print(f"      {проверить(итог.get('слито с') or н['unit_id'], итог['стало'])}")
    for н in выброшенные:
        if not н["чинить"]:
            continue
        итог = починить_выброшенную(н)
        print(f"   #{н['id']} → {итог}")
        if итог.get("стало"):
            print(f"      {проверить(итог.get('слито с') or н['unit_id'], итог['стало'])}")
    # Повторы пересчитываем ЗАНОВО: часть из них создали слияния этого же прогона.
    for находка in найти_повторы(слитые):
        if not находка["чинить"]:
            continue
        сколько = убрать_повтор(находка)
        print(f"   повтор {находка['слово']!r} → убрано карточек: {сколько}")
    print("\nГотово.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
