# -*- coding: utf-8 -*-
"""Неопределённый артикль в заголовке колоды — заслон обязан его видеть.

Откуда взялся этот тест
───────────────────────
23.08.2026 владелец открыл карточку «Schnapsidee» и спросил: почему запись без артикля.
Разбор показал цепочку: утренний стендап положил в колоду «eine Schnapsidee» (кусок
текста ролика), заслон `daily_video_quality.split_article` принимал ТОЛЬКО der/die/das
и такую карточку не видел вовсе — ни проверки, ни исправления, ни строки в отчёте.
Дальше дверь записи снимала «eine» (правильно, это не словарный артикль), определённый
никто не ставил, а модель разбора, увидев два слова, пометила запись «выражением» —
и этот ярлык навсегда вывел единицу из ночного добора рода.

То есть один незакрытый вход давал три следствия. Тест сторожит вход.

Что здесь проверяется
─────────────────────
1. Заслон ВИДИТ неопределённый артикль у одиночного существительного.
2. Заголовок приводится к словарной форме по ИСТОЧНИКУ, а не догадкой.
3. Оборот не трогается — артикль клеится только к одному слову. Это отдельное правило
   владельца от 22.08.2026, и открывать его обратно запрещено:
   см. test_article_belongs_to_a_word_not_a_phrase.py.
4. Молчание справочника не проходит молча: карточка остаётся как есть, но получает
   строку в отчёт владельцу.
"""
from __future__ import annotations

import pytest

from backend.daily_video_quality import (
    article_disagrees_with_reference,
    correct_article_from_reference,
    is_indefinite_article,
    split_article,
)


class TestЗаслонВидитНеопределённыйАртикль:
    @pytest.mark.parametrize("de,expected", [
        # Ради чего всё затевалось: до 24.08.2026 здесь было (None, None).
        ("eine Schnapsidee", ("eine", "Schnapsidee")),
        ("ein Buch", ("ein", "Buch")),
        ("einer Idee", ("einer", "Idee")),
        # Определённый артикль работает как работал.
        ("die Kommentarspalte", ("die", "Kommentarspalte")),
        # Строчная буква не мешает: неверный регистр — это то, что мы чиним.
        ("die kommentarspalte", ("die", "kommentarspalte")),
    ])
    def test_одиночное_существительное_с_артиклем_разбирается(self, de, expected):
        assert split_article(de) == expected

    @pytest.mark.parametrize("de", [
        "einen Kater haben",             # законная идиома
        "eine Pressekonferenz abhalten",  # законный оборот
        "ein mulmiges Gefühl",            # три слова
        "Schnapsidee",                    # артикля нет вовсе
        "auf die Palme bringen",
    ])
    def test_оборот_и_голое_слово_не_разбираются(self, de):
        assert split_article(de) == (None, None)

    def test_неопределённый_артикль_опознаётся(self):
        assert is_indefinite_article("eine")
        assert is_indefinite_article("EINEM")
        assert not is_indefinite_article("die")
        assert not is_indefinite_article("")


class TestЗаголовокПриводитсяКСловарнойФорме:
    """Источник подменяется заглушкой: тест проверяет ПРАВИЛО, а не наличие сети."""

    def _без_сети(self, monkeypatch, ответ):
        monkeypatch.setattr(
            "backend.daily_video_quality.article_from_reference",
            lambda de, allow_network=False: ответ,
        )

    def test_неопределённый_меняется_на_определённый_из_источника(self, monkeypatch):
        self._без_сети(monkeypatch, ("die", "справочник склонений"))
        fixed, what = correct_article_from_reference({"de": "eine Schnapsidee"})
        assert fixed["de"] == "die Schnapsidee"
        assert "справочник склонений" in what

    def test_оборот_не_трогается_даже_если_источник_что_то_знает(self, monkeypatch):
        # Страховка от возврата старой ошибки: артикль перед оборотом — запрещён.
        self._без_сети(monkeypatch, ("der", "справочник склонений"))
        card = {"de": "einen Kater haben"}
        fixed, what = correct_article_from_reference(card)
        assert fixed["de"] == "einen Kater haben"
        assert what == ""

    def test_молчание_источника_не_проходит_молча(self, monkeypatch):
        self._без_сети(monkeypatch, (None, "справочник не знает слова"))
        card = {"de": "eine Wildcard"}
        fixed, what = correct_article_from_reference(card)
        # Ничего не выдумали…
        assert fixed["de"] == "eine Wildcard"
        # …но и не смолчали: владелец увидит это строкой в отчёте.
        assert "НЕ ИСПРАВЛЕНО" in what
        assert "Wildcard" in what

    def test_верный_определённый_артикль_остаётся_нетронутым(self, monkeypatch):
        self._без_сети(monkeypatch, ("die", "wiktionary"))
        card = {"de": "die Kommentarspalte"}
        fixed, what = correct_article_from_reference(card)
        assert fixed["de"] == "die Kommentarspalte"
        assert what == ""


class TestПретензияЧитаетсяЧеловеком:
    def test_неопределённый_артикль_названа_причина_а_не_расхождение(self, monkeypatch):
        monkeypatch.setattr(
            "backend.daily_video_quality.article_from_reference",
            lambda de, allow_network=False: ("die", "справочник склонений"),
        )
        why = article_disagrees_with_reference({"de": "eine Schnapsidee"})
        assert "неопределённый артикль" in why
        assert "die Schnapsidee" in why

    def test_молчание_источника_тоже_даёт_претензию(self, monkeypatch):
        monkeypatch.setattr(
            "backend.daily_video_quality.article_from_reference",
            lambda de, allow_network=False: (None, "не знает"),
        )
        why = article_disagrees_with_reference({"de": "eine Wildcard"})
        assert "неопределённый артикль" in why
