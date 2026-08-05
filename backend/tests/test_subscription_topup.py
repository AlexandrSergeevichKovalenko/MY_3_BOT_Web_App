"""Подписка добирает столько слов, сколько не хватило. Не по расписанию.

Человек подписался на объём — тысячу слов «Быстрого старта» или весь словарь — и должен
его получать по мере надобности. Первая версия этого добора раздавала по два слова за
заход и не больше десяти в сутки; числа были взяты с потолка, и владелец справедливо их
отверг: «он подписался на определённый объём, он должен его получить».

Правило теперь одно: тренажёру нужно пятнадцать карточек, своих хватило на девять —
открываем шесть. Не хватило ни одной — открываем пятнадцать.

Единственный настоящий ограничитель — потолок самой подписки: «Быстрый старт» это
тысяча слов, и больше по ней не откроется. У «Всего словаря» потолка нет.
"""
from backend.database import subscription_drip_allowance


def test_opens_exactly_what_is_missing():
    assert subscription_drip_allowance(
        needed=6, delivered_total=0, subscription_limit=None,
    ) == 6
    assert subscription_drip_allowance(
        needed=15, delivered_total=0, subscription_limit=None,
    ) == 15


def test_nothing_missing_means_nothing_opened():
    assert subscription_drip_allowance(
        needed=0, delivered_total=0, subscription_limit=None,
    ) == 0


def test_no_cap_does_not_limit_a_big_need():
    """«Весь словарь» — потолка нет, отдаём сколько попросили."""
    assert subscription_drip_allowance(
        needed=40, delivered_total=9000, subscription_limit=None,
    ) == 40


def test_subscription_cap_is_the_only_limit():
    """«Быстрый старт» — тысяча слов. Осталось три места, просят десять."""
    assert subscription_drip_allowance(
        needed=10, delivered_total=997, subscription_limit=1000,
    ) == 3
    assert subscription_drip_allowance(
        needed=10, delivered_total=1000, subscription_limit=1000,
    ) == 0


def test_overshoot_never_goes_negative():
    """Потолок мог смениться на меньший — отдаём ноль, а не отрицательное число."""
    assert subscription_drip_allowance(
        needed=5, delivered_total=5000, subscription_limit=100,
    ) == 0


def test_junk_input_is_not_a_crash():
    assert subscription_drip_allowance(
        needed=None, delivered_total=None, subscription_limit=None,
    ) == 0
    assert subscription_drip_allowance(
        needed=-3, delivered_total=0, subscription_limit=None,
    ) == 0
