# -*- coding: utf-8 -*-
"""Ночной добор банка синонимов видит нехватку у ОБОИХ потребителей.

Повод, 14.09.2026. Банк синонимов ровно равнялся цели (30 при цели 30), ночная задача
считала «отставания нет» и не заказывала ничего — а свободным для ТРЕНИРОВКИ было ОДНО
слово из тридцати. Спринт и тренировка держат отдых на разных полях базы, и добор
смотрел только на часы спринта: там было свободно 12, и всё выглядело здоровым.
Итог — капля каждый день не находила отдохнувшего слова и уходила в запасной ход с
кулдауном НОЛЬ: у синонимов 28 повторов из 34 пришли раньше 21 дня.

Тесты держат КЛАСС «нехватку у одного потребителя прикрыли запасом другого».
"""
from backend.sprint_pool_need import decide_topup

# Живые числа 14.09.2026 — чтобы тест ломался ровно на том случае, который был.
SYNONYM = dict(bank=30, target=30, per_day=1.14, free_sprint=12, free_trainer=1,
               rail_span_days=3, cap_per_night=6)
ANTONYM = dict(bank=50, target=50, per_day=1.90, free_sprint=35, free_trainer=6,
               rail_span_days=3, cap_per_night=6)


def test_bank_na_tseli_no_trenirovka_suha_zakazyvaem():
    """ГЛАВНЫЙ случай. Раньше здесь не заказывалось НИЧЕГО."""
    d = decide_topup(**SYNONYM)
    assert d.need is True
    assert d.gap_bank == 0, "по банку отставания нет — и раньше на этом всё кончалось"
    assert d.gap_free == 3, "не хватает трёх свободных до порога в 4"
    assert d.want == 3
    assert "тренировка 1" in d.reason


def test_porog_schitaetsya_ot_dliny_relsa_a_ne_vzyat_s_potolka():
    """Порог = расход × 3 дня (тренировка → спринт), округление вверх."""
    assert decide_topup(**SYNONYM).floor_free == 4      # ceil(1.14 × 3)
    assert decide_topup(**ANTONYM).floor_free == 6      # ceil(1.90 × 3)


def test_rovno_na_poroge_lozhnoy_trevogi_net():
    d = decide_topup(**ANTONYM)                          # свободно 6, порог 6
    assert d.need is False and d.want == 0
    assert "хватает" in d.reason


def test_reshaem_po_HUDSHEMU_iz_dvuh_chasov():
    """Запас у спринта не лечит пустоту у тренировки — и наоборот."""
    a = decide_topup(**{**SYNONYM, "free_sprint": 999})
    assert a.need is True and a.gap_free == 3
    b = decide_topup(**{**SYNONYM, "free_trainer": 999, "free_sprint": 1})
    assert b.need is True and b.gap_free == 3


def test_otstavanie_banka_po_prezhnemu_lovitsya():
    d = decide_topup(**{**SYNONYM, "bank": 20, "free_trainer": 9, "free_sprint": 9})
    assert d.need is True and d.gap_bank == 10 and d.gap_free == 0
    assert d.want == 6, "потолок ночи — шесть карточек, отставание добирается за несколько ночей"
    assert "банк 20" in d.reason


def test_ne_zakazyvaem_dvazhdy_odnu_i_tu_zhe_nehvatku():
    """Оба повода сработали разом — это одна нехватка с двух сторон, а не сумма:
    каждая карточка стоит обращения к модели."""
    d = decide_topup(**{**SYNONYM, "bank": 26, "free_trainer": 0, "free_sprint": 0})
    assert d.gap_bank == 4 and d.gap_free == 4
    assert d.want == 4, "большее из двух, а не 8"


def test_nulevoy_rashod_ne_roniaet_pravilo():
    d = decide_topup(bank=6, target=6, per_day=0.0, free_sprint=0, free_trainer=0,
                     rail_span_days=3, cap_per_night=6)
    assert d.floor_free == 1 and d.need is True and d.want == 1
