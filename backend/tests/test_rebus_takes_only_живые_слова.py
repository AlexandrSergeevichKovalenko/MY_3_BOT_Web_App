# -*- coding: utf-8 -*-
"""В банк ребусов пускаем только ходовые слова. Решение владельца 01.09.2026.

Повод: владельцу пришёл ребус «Eieruhr» (кухонный таймер) — слово настоящее, но
встречается 120 раз на миллиард против 45 509 у Bahnhof. Прогон 338 карточек через
DWDS показал хвост из 12 редких среди 77 выдаваемых и 29 прямых выдумок модели
(Geldbeutelverschluss — 0,0) среди снятых.

Три правила, которые тут держатся:
  1. редкое слово в банк не попадает;
  2. «DWDS не ответил» — НЕ «годится»;
  3. накопленное не трогаем: приёмка касается только новых карточек.
"""

from unittest.mock import patch

import pytest


def test_редкое_слово_не_проходит():
    from backend import rebus_word_gate

    with patch("backend.dwds_frequency.word_per_billion", return_value=119.9):
        ok, why = rebus_word_gate.judge_rebus_word("Eieruhr")
    assert ok is False
    assert "редкое" in why and "120" in why, why


def test_ходовое_слово_проходит():
    from backend import rebus_word_gate

    with patch("backend.dwds_frequency.word_per_billion", return_value=45508.9):
        ok, why = rebus_word_gate.judge_rebus_word("Bahnhof")
    assert ok is True
    assert why == ""


def test_бытовой_предмет_у_самого_порога_проходит():
    """Сковорода по письменному корпусу «нечастая» (602), но это вещь из каждой
    кухни и рисуется прекрасно. Порог 300 обязан её пропускать — иначе картинки
    рисовать будет не с чего."""
    from backend import rebus_word_gate

    with patch("backend.dwds_frequency.word_per_billion", return_value=602.4):
        ok, _ = rebus_word_gate.judge_rebus_word("Bratpfanne")
    assert ok is True


def test_выдумка_модели_не_проходит():
    from backend import rebus_word_gate

    with patch("backend.dwds_frequency.word_per_billion", return_value=0.0):
        ok, why = rebus_word_gate.judge_rebus_word("Geldbeutelverschluss")
    assert ok is False
    assert "редкое" in why


def test_молчание_DWDS_это_не_согласие():
    """Самое важное: при обрыве сети слово НЕ должно проскакивать как годное.
    При первом прогоне так молчали 69 слов из 338, среди них Bahnhof."""
    from backend import rebus_word_gate

    with patch("backend.dwds_frequency.word_per_billion", return_value=None):
        ok, why = rebus_word_gate.judge_rebus_word("Bahnhof")
    assert ok is False, "«не знаем» не имеет права означать «годится»"
    assert "неизвестна" in why and "ночью" in why, why


def test_неответ_не_превращается_в_ноль():
    """ask_dwds при поломке обязан вернуть None, а не {'hits': 0} — иначе честное
    слово получит приговор «редкое» из-за нашей же сетевой икоты."""
    from backend import dwds_frequency

    with patch("urllib.request.urlopen", side_effect=OSError("сеть отвалилась")):
        assert dwds_frequency.ask_dwds("Bahnhof") is None


def test_мера_считается_как_на_миллиард():
    """Ошибка в три порядка здесь стоила бы неверного порога: в первом отчёте
    01.09.2026 я именно так и промахнулся, и Eieruhr попал в верхнюю корзину."""
    from backend.dwds_frequency import DWDS_CORPUS_TOTAL, per_billion

    assert DWDS_CORPUS_TOTAL == 53_303_287_841
    assert round(per_billion(6393), 1) == 119.9        # Eieruhr
    assert round(per_billion(2_425_774), 0) == 45509   # Bahnhof
    with pytest.raises(ValueError):
        per_billion(1, total=0)
