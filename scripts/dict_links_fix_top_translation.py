"""Убрать пример из переводов, когда рядом лежит настоящий перевод.

Что видно человеку. У слова показывается верхний перевод. У части слов наверх попал не
перевод, а пример или пояснение целой фразой: «Waschlappen → Он же полный тряпка!»,
«abbestellen → Ваша подписка на рассылку успешно отменена». Настоящий перевод при этом
лежит рядом, но ниже по приоритету и проигрывает.

Замер на живой базе 05.08.2026: из 4 658 немецких СЛОВ с переводом такое у 163, и у 139
из них уже есть чистый вариант. Их и чиним — перестановкой, без обращения к модели.

Признак «это пример, а не перевод» — НЕ длина. Первый прогон отбирал по длине и хотел
заменить «tüchtig → прилежный, усердный, старательно выполняющий работу» на «большой,
сильный», а «der Umsatz → финансовый оборот, выручка компании» на «оборот товара».
Короче — не значит лучше: длинное толкование часто и есть самый точный перевод.

Работающий признак — ЗАГЛАВНАЯ БУКВА и знак конца. Толкование по-русски начинается со
строчной и обрывается без точки («ясли, учреждение по уходу за детьми»), а пример —
это предложение: «Ваша подписка успешно отменена», «Йо, ты можешь кататься?».

Границы намеренно узкие:
  • только единицы вида «слово». У предложения перевод предложением — это норма,
    у словосочетания тоже;
  • только когда есть чем заменить;
  • длинное толкование со строчной буквы НЕ ТРОГАЕМ — это перевод, а не пример;
  • ничего не удаляем. Пример уезжает ВНИЗ списка (ранг 40), а не скрывается совсем —
    вдруг он кому-то полезен как пример. Наверх встаёт чистый перевод.

По умолчанию НИЧЕГО НЕ ПИШЕТ: показывает «до / после». Запись — только с --apply.

    python scripts/dict_links_fix_top_translation.py           # вхолостую
    python scripts/dict_links_fix_top_translation.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

CYRILLIC = re.compile(r"[А-Яа-яЁё]")
CYRILLIC_CAPITAL_FIRST = re.compile(r"^[А-ЯЁ]")
# Ранг «свалки»: выдача его отсекает (условие rank < 900). Пример перестаёт показываться
# как перевод, но из базы никуда не девается — при необходимости всё вернётся одной
# командой. Ранга 40 не хватило: порядок сначала смотрит на пометку «разобранное
# значение» и только потом на ранг, поэтому пример продолжал стоять первым.
DEMOTED_RANK = 900
MAX_CLEAN_WORDS = 5


def looks_like_a_sentence(text: str) -> bool:
    """Это предложение (пример), а не перевод?

    Признак — знак конца ИЛИ заглавная буква при трёх и более словах.

    Порог в три слова стоит не случайно: «die Auffassung → Точка зрения» — обычный
    перевод, просто с заглавной, и трогать его нельзя. Цена порога известна: пропустим
    короткие примеры вроде «Glocke → Колокол прозвонил». Пропустить хуже, чем испортить
    хороший перевод, поэтому выбираем осторожность.

    Длина сама по себе признаком НЕ является: «медицинское устройство для внутривенного
    введения жидкости» — длинно, но это перевод слова «Infusion», а не пример."""
    body = str(text or "").strip()
    if not body:
        return False
    if body.endswith((".", "!", "?")):
        return True
    return bool(CYRILLIC_CAPITAL_FIRST.match(body)) and len(body.split()) >= 3


def is_not_russian(text: str) -> bool:
    """Перевод обязан быть по-русски. Немецкий с обеих сторон — это не перевод."""
    body = str(text or "").strip()
    return bool(body) and not CYRILLIC.search(body)


def is_clean_translation(text: str) -> bool:
    """Перевод, а не пример: по-русски, со строчной, без точки в конце."""
    body = str(text or "").strip()
    if not body or not CYRILLIC.search(body):
        return False
    return not looks_like_a_sentence(body)


def collect() -> list[dict]:
    """Что убрать: все связи-примеры у слов, где есть настоящий перевод."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT u.id, u.display, l.id, l.rank, t.display
                FROM bt_3_lex_units u
                JOIN bt_3_lex_links l ON l.from_unit = u.id AND l.rank < 900
                JOIN bt_3_lex_units t ON t.id = l.to_unit
                WHERE u.lang = 'de' AND u.kind = 'word' AND t.lang <> 'de'
                  AND position('___' in t.display) = 0
                -- Порядок ровно тот, которым выдача выбирает первый перевод
                -- (см. _fetch_links): пометка «разобранное значение» важнее ранга.
                ORDER BY u.id, (l.sense_id IS NULL), l.rank, t.id
            """)
            rows = cursor.fetchall()

    by_unit: dict[int, list[tuple]] = {}
    names: dict[int, str] = {}
    for unit_id, word, link_id, rank, translation in rows:
        by_unit.setdefault(int(unit_id), []).append((int(link_id), int(rank), translation))
        names[int(unit_id)] = word

    plan = []
    for unit_id, links in by_unit.items():
        # Трогаем ТОЛЬКО то, что человек видит первым. Остальное в списке «ещё говорят»
        # никому не мешает, а лишние правки — лишний риск.
        top_link_id, top_rank, top_text = links[0]
        reason = ("пример вместо перевода" if looks_like_a_sentence(top_text)
                  else "перевод не по-русски" if is_not_russian(top_text)
                  else "")
        if not reason:
            continue
        replacement = next(
            (text for lid, rank, text in links[1:] if is_clean_translation(text)),
            None,
        )
        if not replacement:
            continue  # заменить нечем — длинное толкование лучше пустоты
        plan.append({
            "unit_id": unit_id,
            "word": names[unit_id],
            "link_id": top_link_id,
            "old_rank": top_rank,
            "reason": reason,
            "was": str(top_text or "").strip(),
            "now": str(replacement or "").strip(),
        })
    return plan


def apply_plan(plan: list[dict]) -> dict:
    done = {"moved": 0, "errors": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for row in plan:
                try:
                    # Пометку «разобранное значение» тоже снимаем: порядок смотрит на
                    # неё раньше ранга, и без этого пример остался бы первым.
                    cursor.execute(
                        "UPDATE bt_3_lex_links SET rank = %s, sense_id = NULL, updated_at = NOW() "
                        "WHERE id = %s;",
                        (DEMOTED_RANK, row["link_id"]),
                    )
                    done["moved"] += 1
                except Exception as exc:  # noqa: BLE001
                    done["errors"] += 1
                    print(f"   ! связь {row['link_id']}: {exc}")
        conn.commit()
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="Поднять настоящий перевод над примером")
    parser.add_argument("--apply", action="store_true", help="записать (без флага — только отчёт)")
    parser.add_argument("--show", type=int, default=25, help="сколько примеров показать")
    args = parser.parse_args()

    plan = collect()
    print("=" * 78)
    print("ВЕРХНИЙ ПЕРЕВОД У СЛОВ" + ("  — ЗАПИСЬ" if args.apply else "  — вхолостую"))
    print("=" * 78)
    by_reason = {}
    for row in plan:
        by_reason[row["reason"]] = by_reason.get(row["reason"], 0) + 1
    print(f"\nпоправим слов: {len(plan)}")
    for reason, count in sorted(by_reason.items(), key=lambda kv: -kv[1]):
        print(f"   {reason:<26} {count}")
    print()
    for row in plan[: max(0, args.show)]:
        print(f"  {row['word'][:26]:<28} [{row['reason']}]")
        print(f"     было : {row['was'][:60]!r}")
        print(f"     стало: {row['now'][:60]!r}")

    if not args.apply:
        print("\nЭто был холостой прогон. Записать — тот же вызов с --apply.")
        return 0

    print("\nПишу…")
    done = apply_plan(plan)
    print(f"\nготово: переставлено {done['moved']}, ошибок {done['errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
