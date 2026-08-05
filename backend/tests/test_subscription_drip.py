"""Подписка открывает слова каплей, а не потоком.

До этого подписка отдавала слово ТОЛЬКО внутри очереди интервальных повторений — а её
почти не открывают. Замер 05.08.2026: за всё время подписка не выдала ни одного слова,
хотя механизм работал и слово из неё выигрывало у своего по нужности.

Тренажёров у нас много, и вшивать подписку в каждый значило бы чинить одно и то же в
пяти местах. Поэтому слово просто появляется в библиотеке человека, а дальше его
подхватывают все тренажёры сами — они и так читают библиотеку.

Три ограничителя сразу, и все нужны:
  • за заход — чтобы человек заметил пополнение, а не получил пачку незнакомых слов
    посреди повторения;
  • за сутки — слово, которое некогда повторить, ценности не имеет, а библиотека,
    растущая на сотню в день, перестаёт быть своей;
  • общий потолок подписки — «Быстрый старт» это тысяча слов, и не больше.
"""
from backend.database import (
    SUBSCRIPTION_DAILY_DRIP,
    SUBSCRIPTION_DRIP_PER_VISIT,
    subscription_drip_allowance,
)


def test_fresh_visit_opens_the_per_visit_portion():
    assert subscription_drip_allowance(
        delivered_total=0, delivered_today=0, subscription_limit=None,
    ) == SUBSCRIPTION_DRIP_PER_VISIT


def test_daily_limit_stops_the_drip():
    assert subscription_drip_allowance(
        delivered_total=500, delivered_today=SUBSCRIPTION_DAILY_DRIP, subscription_limit=None,
    ) == 0


def test_last_slot_of_the_day_is_given_out():
    """Осталось место на одно слово — отдаём одно, а не порцию целиком."""
    assert subscription_drip_allowance(
        delivered_total=0, delivered_today=SUBSCRIPTION_DAILY_DRIP - 1, subscription_limit=None,
    ) == 1


def test_subscription_cap_wins_over_the_portion():
    """«Быстрый старт» — тысяча слов. Осталось одно место — открываем одно."""
    assert subscription_drip_allowance(
        delivered_total=999, delivered_today=0, subscription_limit=1000,
    ) == 1
    assert subscription_drip_allowance(
        delivered_total=1000, delivered_today=0, subscription_limit=1000,
    ) == 0


def test_no_cap_means_only_daily_and_visit_limits():
    """«Весь словарь» без потолка — ограничивают только заход и сутки."""
    assert subscription_drip_allowance(
        delivered_total=9000, delivered_today=0, subscription_limit=None,
    ) == SUBSCRIPTION_DRIP_PER_VISIT


def test_overshoot_never_goes_negative():
    """Данные могли обогнать правило: вчера потолок был больше, сегодня меньше."""
    assert subscription_drip_allowance(
        delivered_total=5000, delivered_today=99, subscription_limit=100,
    ) == 0


def test_portion_is_smaller_than_the_daily_limit():
    """Иначе одного захода хватало бы на весь день и «капля» стала бы пачкой."""
    assert SUBSCRIPTION_DRIP_PER_VISIT < SUBSCRIPTION_DAILY_DRIP
