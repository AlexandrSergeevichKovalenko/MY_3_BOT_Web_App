# -*- coding: utf-8 -*-
"""Судья артикля головного слова: что он обязан принять и что обязан отвергнуть ЦЕЛИКОМ.

Сюда доходят слова, которых не знает НИ ОДИН справочник, — проверить ответ нечем
(замер 16.09.2026: Wiktionary молчит по всем четырём, в том числе по сети). Значит
единственная защита — строгость разбора ответа и форма вопроса. Тест держит обе.

Ни один тест здесь не ходит ни в сеть, ни в базу: ответ модели подставляется.
"""
from __future__ import annotations

import json

import pytest

from backend.genitive_head_article_judge import (
    FORM_CHANGES, STABLE, UNKNOWN, article_from_judge, parse_rows,
)


def _ответ(rows) -> str:
    return json.dumps({"rows": rows}, ensure_ascii=False)


# ── что принимаем ────────────────────────────────────────────────────────────────────

def test_обычное_слово_с_артиклем():
    rows = parse_rows(_ответ([
        {"word": "Aussetzen", "verdict": STABLE, "article": "das",
         "corrected_form": "", "why_ru": "субстантивированный инфинитив"},
    ]), ["Aussetzen"])
    assert rows[0]["article"] == "das"
    assert article_from_judge(rows[0])[0] == "das"


def test_форма_меняется_артикль_не_дописываем():
    """«der Vorsitzender des Vorstands» — ровно то, ради чего вопрос сделан двойным."""
    rows = parse_rows(_ответ([
        {"word": "Vorsitzender", "verdict": FORM_CHANGES, "article": "",
         "corrected_form": "Vorsitzende", "why_ru": "субстантивированное прилагательное"},
    ]), ["Vorsitzender"])
    артикль, причина = article_from_judge(rows[0])
    assert артикль == "", "дописывать артикль к этой форме нельзя"
    assert "Vorsitzende" in причина


def test_модель_не_взялась_это_честный_исход():
    rows = parse_rows(_ответ([
        {"word": "Buntheit", "verdict": UNKNOWN, "article": "",
         "corrected_form": "", "why_ru": "не уверена"},
    ]), ["Buntheit"])
    артикль, причина = article_from_judge(rows[0])
    assert артикль == ""
    assert "не взялась" in причина


# ── что отвергаем ЦЕЛИКОМ: одна кривая строка портит весь ответ ──────────────────────
#
# Строгость намеренная. Сопоставление «ответ ↔ слово» здесь держится только на порядке и
# на эхе слова; ошибись в нём — и артикль уедет к чужому слову, а проверить это нечем.
# Не судить в эту ночь дешевле, чем записать неверный артикль в карточку человека.

@pytest.mark.parametrize("rows, asked, почему", [
    ([{"word": "Aussetzen", "verdict": STABLE, "article": "das", "corrected_form": "",
       "why_ru": ""},
      {"word": "Buntheit", "verdict": STABLE, "article": "die", "corrected_form": "",
       "why_ru": ""}],
     ["Aussetzen"], "строк больше, чем спрашивали"),
    ([{"word": "Buntheit", "verdict": STABLE, "article": "die", "corrected_form": "",
       "why_ru": ""}],
     ["Aussetzen"], "ответ про другое слово"),
    ([{"word": "Aussetzen", "verdict": STABLE, "article": "", "corrected_form": "",
       "why_ru": ""}],
     ["Aussetzen"], "«стоит после артикля», а какого — не сказано"),
    ([{"word": "Aussetzen", "verdict": STABLE, "article": "dem", "corrected_form": "",
       "why_ru": ""}],
     ["Aussetzen"], "не определённый артикль именительного"),
    ([{"word": "Vorsitzender", "verdict": FORM_CHANGES, "article": "",
       "corrected_form": "", "why_ru": ""}],
     ["Vorsitzender"], "«форма меняется», а на какую — молчок"),
    ([{"word": "Aussetzen", "verdict": "maybe", "article": "das", "corrected_form": "",
       "why_ru": ""}],
     ["Aussetzen"], "вердикта такого нет"),
])
def test_негодный_ответ_не_берём_ни_одной_строкой(rows, asked, почему):
    assert parse_rows(_ответ(rows), asked) is None, почему


def test_не_json_это_тоже_негодный_ответ():
    assert parse_rows("извините, не могу", ["Aussetzen"]) is None
    assert parse_rows("", ["Aussetzen"]) is None


def test_порядок_слов_сохраняется():
    слова = ["Aussetzen", "Buntheit", "Schutzpflichten"]
    rows = parse_rows(_ответ([
        {"word": "Aussetzen", "verdict": STABLE, "article": "das", "corrected_form": "",
         "why_ru": ""},
        {"word": "Buntheit", "verdict": STABLE, "article": "die", "corrected_form": "",
         "why_ru": ""},
        {"word": "Schutzpflichten", "verdict": STABLE, "article": "die",
         "corrected_form": "", "why_ru": "множественное"},
    ]), слова)
    assert [r["word"] for r in rows] == слова
    assert [r["article"] for r in rows] == ["das", "die", "die"]


def test_у_модели_не_спрашивали_это_не_отказ():
    артикль, причина = article_from_judge(None)
    assert артикль == ""
    assert "не спрашивали" in причина
