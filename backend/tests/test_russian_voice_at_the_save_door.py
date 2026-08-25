# -*- coding: utf-8 -*-
"""Русский перевод — второй голос НА ВХОДЕ: заглавная не делает из прилагательного слово.

Откуда (владелец, 24–25.08.2026)
────────────────────────────────
Владелец открыл карточку «die Mies» — «Паршивый» — со значениями про неудачников и
примерами «Die Mies sind wieder nicht erfolgreich». Всё это выдумано.

Корень: человек ищет слово, и с клавиатуры оно приходит с ЗАГЛАВНОЙ — это первое и
единственное слово запроса. В немецком заглавная означает существительное. Прилагательное
«mies» легло в базу как «Mies», справочник честно ответил про существительное, а модель
сочинила под него смыслы. Тем же путём пришли «Reif» («зрелый»), «Rein» («чистый»),
«Rudern» («грести»). Замер 25.08.2026: 111 заголовков, 11 сломаны однозначно.

Русский перевод писал ЧЕЛОВЕК, и он не врёт: «Зрелый» — прилагательное. Это и есть
второй голос, и он поставлен в дверь записи — единственное место, через которое проходит
любая карточка: и бот, и веб, и импорт.

ЭТОТ ТЕСТ СТОРОЖИТ ЧЕТЫРЕ ГРАНИЦЫ. Все четыре — не теория, каждая уже ломалась.
"""
from __future__ import annotations

import pytest

from backend.database import _lowercase_when_russian_says_not_a_noun as door


class TestЗаглавнаяСнимаетсяТамГдеДоказано:
    @pytest.mark.parametrize("german,russian,expected", [
        ("Mies", "Паршивый", "mies"),
        ("Reif", "Зрелый", "reif"),
        ("Rein", "Чистый", "rein"),
        ("Stumpf", "Тупой", "stumpf"),
        ("Rudern", "Грести", "rudern"),
        # Артикль впереди значит, что кто-то УЖЕ счёл слово существительным —
        # снимаем и его: «die Mies» неверно ровно так же, как «Mies».
        ("die Mies", "Паршивый", "mies"),
        ("der Reif", "Зрелый", "reif"),
    ])
    def test_прилагательное_и_глагол_опускаются(self, german, russian, expected):
        assert door(german, russian) == expected


class TestСуществительноеНеТрогаем:
    @pytest.mark.parametrize("german,russian", [
        ("Nase", "Нос"), ("Auto", "Автомобиль"), ("Schildkröte", "Черепаха"),
        ("die Nase", "Нос"), ("das Auto", "Автомобиль"),
        # Окончание русского слова НЕ является частью речи: «Ясность» кончается на «ть»,
        # «печь» на «чь». Предыдущая попытка с регуляркой звала их глаголами.
        ("Klarheit", "Ясность"), ("Abfolge", "Последовательность"),
    ])
    def test_существительное_остаётся_как_было(self, german, russian):
        assert door(german, russian) == german


class TestНедоказанноеНеТрогаем:
    @pytest.mark.parametrize("german,russian", [
        # «Простой» — и прилагательное, и существительное. «Правда» — и существительное,
        # и частица. Выбирать за человека между двумя ВЕРНЫМИ разборами нельзя.
        ("Simpel", "Простой"),
        ("Wahrheit", "Правда"),
        # Оборот из нескольких слов — не слово, часть речи одним разбором не берётся.
        ("Kater", "держать пари"),
        ("Palpitation", "Сильное сердцебиение; учащённая пульсация"),
        # Перевода нет вовсе — доказывать нечем.
        ("Etwas", ""), ("Etwas", None),
    ])
    def test_спорное_и_неизвестное_остаются(self, german, russian):
        assert door(german, russian) == german


class TestФразуНеТрогаемНикогда:
    """Отдельное правило владельца от 22.08.2026: регистр и артикль принадлежат фразе."""

    @pytest.mark.parametrize("german,russian", [
        ("eine Pressekonferenz abhalten", "провести пресс-конференцию"),
        ("einen Kater haben", "страдать похмельем"),
        ("Sei kein Dummkopf", "не будь дураком"),
        ("auf die Palme bringen", "выводить из себя"),
    ])
    def test_фраза_остаётся_как_есть(self, german, russian):
        assert door(german, russian) == german


class TestПустоеИНемецкоеБезЗаглавной:
    @pytest.mark.parametrize("german", ["", None, "mies", "reif", "   "])
    def test_нечего_снимать(self, german):
        assert door(german, "Зрелый") == german


class TestБезРазбораНеУгадываем:
    def test_недоступный_pymorphy3_оставляет_слово_как_есть(self, monkeypatch):
        import backend.russian_part_of_speech as ru
        monkeypatch.setattr(ru, "_get_analyzer", lambda: None)
        # Молчание источника — не «докажи обратное», а «не знаем». Слово не трогаем.
        assert door("Mies", "Паршивый") == "Mies"
