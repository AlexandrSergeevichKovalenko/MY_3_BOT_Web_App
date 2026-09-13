"""Место на экране аналитики и его знаменатель считаются по ОДНИМ И ТЕМ ЖЕ людям.

ЗАЧЕМ ПОЯВИЛСЯ (13.09.2026). На экране аналитики стояло «Ваше место: #3» вообще без
знаменателя, а в недельной сводке «Твоё место в группе: #3» плюс «Ты выше X%
участников», где X считался по длине присланного списка. Список сервер обрезает до
восьми строк (ANALYTICS_LEADERBOARD_SNAPSHOT_LIMIT), поэтому в группе любого размера
процент считался по восьми людям.

Решение владельца 13.09.2026: знаменатель — ТЕ, КТО ЗАНИМАЛСЯ за период. Это то же
правило, что он принял 06.09.2026 для недельного рейтинга: место не даётся за ноль.
Отсюда следствие, без которого знаменатель был бы враньём: место тоже считается
только среди занимавшихся.

Тест держит оба конца: и счёт занимавшихся, и номер места.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.analytics import _mark_studied_and_count, compare_row_studied


def _строка(user_id, *, attempts=0, covered=0, minutes=0.0, words=0, assigned=0,
            missed_days=0, final_score=0.0):
    return {
        "user_id": user_id,
        "translation_attempts": attempts,
        "covered_sentences": covered,
        "total_time_min": minutes,
        "learned_words": words,
        "assigned_sentences": assigned,
        "missed_days": missed_days,
        "final_score": final_score,
    }


def test_показанные_предложения_это_не_занятие():
    """Человеку выдали предложения и он их открыл, но не сделал ни одного.
    Это не занятие: выдали не он, а мы."""
    assert compare_row_studied(_строка(1, assigned=7, missed_days=7)) is False


def test_пропущенные_дни_это_не_занятие():
    assert compare_row_studied(_строка(2, missed_days=5)) is False


def test_любая_настоящая_работа_считается_занятием():
    assert compare_row_studied(_строка(3, attempts=1)) is True
    assert compare_row_studied(_строка(4, covered=2)) is True
    assert compare_row_studied(_строка(5, minutes=0.5)) is True
    assert compare_row_studied(_строка(6, words=3)) is True


def test_знаменатель_считает_только_занимавшихся():
    строки = [
        _строка(1, attempts=10, final_score=90.0),
        _строка(2, assigned=7, missed_days=7),          # ничего не делал
        _строка(3, covered=4, final_score=70.0),
        _строка(4, missed_days=3),                       # ничего не делал
        _строка(5, minutes=12.0, final_score=50.0),
    ]
    всего = _mark_studied_and_count(строки)
    assert всего == 3, "в знаменатель попали те, кто не занимался"
    for строка in строки:
        assert строка["cohort_studied_total"] == 3, (
            "честное число обязано ехать в КАЖДОЙ строке: наружу уходит обрезанный список"
        )


def test_место_не_даётся_за_ноль_и_совпадает_со_знаменателем():
    from backend.backend_server import _compare_rank_among_those_who_studied

    строки = [
        _строка(1, attempts=10, final_score=90.0),
        _строка(3, covered=4, final_score=70.0),
        _строка(5, minutes=12.0, final_score=50.0),
        _строка(2, assigned=7, missed_days=7),
        _строка(4, missed_days=3),
    ]
    _mark_studied_and_count(строки)
    assert _compare_rank_among_those_who_studied(строки, 1) == 1
    assert _compare_rank_among_those_who_studied(строки, 3) == 2
    assert _compare_rank_among_those_who_studied(строки, 5) == 3
    # Не занимался — места нет, а не «четвёртое из трёх».
    assert _compare_rank_among_those_who_studied(строки, 2) is None
    assert _compare_rank_among_those_who_studied(строки, 4) is None


def test_старый_снимок_без_пометки_не_теряет_место():
    """Снимки таблицы, снятые до 13.09, не несут ни пометки, ни числа. Место у них
    считается по-старому, по позиции, — оно было и не должно пропасть. Знаменателя
    при этом НЕ будет: честного числа взять неоткуда, а выдумывать запрещено."""
    from backend.backend_server import _compare_cohort_payload, _compare_rank_among_those_who_studied

    старые = [{"user_id": 11}, {"user_id": 22}, {"user_id": 33}]
    assert _compare_rank_among_those_who_studied(старые, 22) == 2
    assert _compare_cohort_payload(старые) == {"studied_total": None}


def test_знаменатель_достаётся_из_строк():
    from backend.backend_server import _compare_cohort_payload

    строки = [_строка(1, attempts=1), _строка(2, covered=1)]
    _mark_studied_and_count(строки)
    assert _compare_cohort_payload(строки) == {"studied_total": 2}
    assert _compare_cohort_payload([]) == {"studied_total": None}
