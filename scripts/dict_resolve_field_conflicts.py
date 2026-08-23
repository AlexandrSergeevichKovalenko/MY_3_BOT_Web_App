# -*- coding: utf-8 -*-
"""ШАГ ВТОРОЙ СПЛОШНОГО ПРОХОДА: разобрать расхождения вторым источником.

ОТКУДА БЕРУТСЯ РАСХОЖДЕНИЯ. Первый шаг (`dict_field_audit_by_reference.py`) сверил каждую
клетку словаря с плоским кэшем справочника и нашёл 118 мест, где наш ответ и справочник
не сходятся. Записывать «справочник всегда прав» нельзя: у «der Eimer» верны МЫ, а кэш
даёт «Eimeren»; у «die Lunge» кэш выдаёт «Lunges». Слепая запись испортила бы верное.

ЧТО ДЕЛАЕТ ЭТОТ ШАГ. Спрашивает ВТОРОЙ, более сильный источник — не плоский кэш, а
страницу склонения/спряжения самого слова, через уже написанный каскад:

    существительные   german_reference_forms.noun_declension_for(allow_network=True)
    глаголы           german_verb_paradigms.paradigm_for_verb(allow_network=True)
    род               article_authority.authoritative_article(allow_network=True)

Свой разбор страниц я НЕ пишу: эти функции уже умеют и сеть, и составные слова, и
двойной спрос модели с полным совпадением, и сами дописывают справочник.

ТРИ ИСХОДА, И КАЖДЫЙ ЗАКАНЧИВАЕТСЯ ДЕЙСТВИЕМ, А НЕ ЗАПИСЬЮ В СПИСОК:
    наш ответ верен      → отметка «подтверждено вторым источником», расхождение снято;
    прав справочник      → ЧИНИМ карточку и ставим отметку с указанием, что исправлено;
    оба молчат/спорят    → остаётся владельцу: отдельный вердикт «нужно решение».

⚠ ЧЕГО ЗДЕСЬ НЕТ. Ни одной формы, выведенной нашей арифметикой. Если второй источник не
ответил — мы не достраиваем ответ сами, а честно оставляем клетку владельцу.

    python3 scripts/dict_resolve_field_conflicts.py            # показать, не писать
    python3 scripts/dict_resolve_field_conflicts.py --apply    # починить и переставить отметки
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context          # noqa: E402
from backend.german_reference_forms import noun_declension_for  # noqa: E402
from backend.german_verb_paradigms import paradigm_for_verb     # noqa: E402
from backend.article_authority import authoritative_article     # noqa: E402

_audit_spec = importlib.util.spec_from_file_location(
    "dict_field_audit", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "dict_field_audit_by_reference.py"))
audit = importlib.util.module_from_spec(_audit_spec)
_audit_spec.loader.exec_module(audit)

NEEDS_OWNER = "нужно решение"
_ARTICLE = re.compile(r"^(?:der|die|das)\s+", re.I)


def bare(word: str) -> str:
    return _ARTICLE.sub("", str(word or "").strip()).strip()


GENDER_KEY = {"der": "m", "die": "f", "das": "n"}


def documented_plural(word: str, our_gender: str) -> tuple[str, str]:
    """Множественное со страницы склонения. Возвращает (форма, почему пусто).

    ⚠ ТАБЛИЦА ЛЕЖИТ ПО КЛЮЧУ РОДА, а не одна на слово, и это не мелочь оформления.
    У «Eimer» на странице есть и мужская таблица (Nominativ Plural «die Eimer», верно),
    и женская — «die Eimeren». Взять первую попавшуюся значит принести чужую форму:
    именно так в кэш и попало «Eimeren», из-за которого верное «die Eimer» числилось
    у нас ошибкой. Берём таблицу ТОГО рода, который стоит у слова.
    """
    tables = noun_declension_for(word, allow_network=True) or {}
    key = GENDER_KEY.get(str(our_gender or "").strip().lower())
    if not key:
        return "", "у слова нет рода, таблицу не выбрать"
    table = tables.get(key)
    if not table:
        available = ", ".join(k for k in tables if k in ("m", "f", "n"))
        return "", (f"страница знает только род {available}" if available
                    else "второй источник тоже молчит")
    for row in table.get("rows") or []:
        if row.get("case") == "nom":
            return bare(str(row.get("plural") or "").strip()), ""
    return "", "в таблице нет именительного множественного"


def same(left: str, right: str) -> bool:
    """Сравнение написаний: артикль и пометка в скобках — оформление, а не разные формы."""
    def clean(value: str) -> str:
        return bare(str(value or "")).split("(")[0].strip().lower()
    return bool(clean(left)) and clean(left) == clean(right)


def resolve_plural(unit) -> tuple[str, str, str]:
    """(что делать, новое значение, откуда). Действия: 'наш', 'чиним', 'владельцу'."""
    ours = str(((unit["card"] or {}).get("forms") or {}).get("plural") or "")
    second, why_empty = documented_plural(bare(unit["display"]), unit["gender"])
    if not second:
        return NEEDS_OWNER, "", why_empty
    if same(ours, second):
        return "наш", ours, "страница склонения подтвердила наш ответ"
    return "чиним", f"die {second}", "страница склонения"


def resolve_verb(unit) -> tuple[str, str, str]:
    table = paradigm_for_verb(bare(unit["display"]), allow_network=True)
    if not table:
        return NEEDS_OWNER, "", "второй источник тоже молчит"
    verdict, ours, _ = audit.check_verb_forms(unit, {bare(unit["display"]).lower(): table})
    if verdict == audit.CONFIRMED:
        return "наш", ours, "страница спряжения подтвердила наш ответ"
    # Форму глагола НЕ достраиваем сами: таблица есть, но наши формы в неё не легли —
    # это разбор для человека, а не повод переписать карточку арифметикой.
    return NEEDS_OWNER, "", "страница спряжения не подтвердила наши формы"


def resolve_gender(unit) -> tuple[str, str, str]:
    article, source = authoritative_article(bare(unit["display"]), allow_network=True)
    if not article:
        return NEEDS_OWNER, "", f"второй источник молчит: {source}"
    if article.lower() == str(unit["gender"] or "").strip().lower():
        return "наш", article, f"подтверждено: {source}"

    # ⛔ РОД АВТОМАТИЧЕСКИ НЕ МЕНЯЕМ. Решение принято 23.08.2026 после живого прогона.
    #
    # «der Angestellte» и «die Angestellte» верны оба — это служащий и служащая; то же
    # у «der/die Dicke». Справочник на такой вопрос возвращает ОДИН артикль, и правка по
    # нему переписала бы верную карточку на противоположный род. Я пробовал отличать
    # такие слова по таблицам склонения на странице — защита сработала не на всех, а
    # «почти надёжно» для рода не годится: человек заучит неверный немецкий и будет
    # годами говорить «die Angestellte» про мужчину.
    #
    # Смотреть на окончание слова (-e → наверное прилагательное) нельзя: это то самое
    # додумывание грамматики своей арифметикой, которое запрещено правилом ноль.
    #
    # Расхождений по роду во всей базе ПЯТЬ. Человек разбирает их за минуту, и это
    # честнее, чем автоматика, которая ошибается на одном из пяти.
    return NEEDS_OWNER, article, f"род не меняем автоматически; справочник предлагает «{article}»"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.unit_id, c.field, u.display, u.pos, u.gender, u.card
                FROM bt_3_field_checks c
                JOIN bt_3_lex_units u ON u.id = c.unit_id
                WHERE c.verdict = %s
                ORDER BY c.field, c.unit_id;""", (audit.CONFLICT,))
            rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]
    print(f"расхождений к разбору: {len(rows)}\n")

    tally = {"наш": 0, "чиним": 0, NEEDS_OWNER: 0}
    lines = []
    for unit_id, field, display, pos, gender, card in rows:
        unit = {"id": unit_id, "display": display, "pos": pos, "gender": gender, "card": card}
        action, value, why = ({"plural": resolve_plural,
                               "verb_forms": resolve_verb,
                               "gender": resolve_gender}[field])(unit)
        tally[action] += 1
        lines.append((unit_id, field, display, action, value, why))
        print(f"   {display[:26]:28} {field:11} {action:14} {str(value)[:26]:28} {why[:44]}")

        if not args.apply:
            continue
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                if action == "чиним" and field == "plural":
                    cur.execute("""UPDATE bt_3_lex_units
                                   SET card = jsonb_set(COALESCE(card,'{}'::jsonb),
                                       '{forms,plural}', %s::jsonb, TRUE), updated_at = NOW()
                                   WHERE id = %s;""",
                                (json.dumps(value, ensure_ascii=False), unit_id))
                elif action == "чиним" and field == "gender":
                    cur.execute("""UPDATE bt_3_lex_units
                                   SET gender = %s, gender_source = 'справочник, разбор расхождения',
                                       display = regexp_replace(display,'^(der|die|das) ', %s || ' '),
                                       updated_at = NOW() WHERE id = %s;""",
                                (value, value, unit_id))
                verdict = audit.CONFIRMED if action in ("наш", "чиним") else NEEDS_OWNER
                cur.execute("""UPDATE bt_3_field_checks
                               SET verdict=%s, source=%s, reference=%s, checked_at=NOW()
                               WHERE unit_id=%s AND field=%s;""",
                            (verdict, why[:200], str(value)[:400], unit_id, field))
                conn.commit()

    print(f"\n   наш ответ верен, расхождение снято: {tally['наш']}")
    print(f"   починено по второму источнику:       {tally['чиним']}")
    print(f"   осталось владельцу:                  {tally[NEEDS_OWNER]}")
    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
