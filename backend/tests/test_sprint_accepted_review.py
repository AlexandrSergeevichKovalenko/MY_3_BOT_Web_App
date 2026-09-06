"""Письмо владельцу о непропущенных синонимах (backend/sprint_accepted_review.py).

Требование владельца 06.09.2026: у кандидата в письме стоят артикль, перевод и вердикт
справочника — «иначе как я буду решать без понимания того, что предлагает модель?»"""
from __future__ import annotations

from backend.sprint_accepted_review import _card_text, _keyboard


def _row(**kw):
    base = {"id": 7, "relation": "synonym", "wort": "die Möglichkeit", "hint_ru": "возможность",
            "de": "die Potenzial", "ru": "потенциал", "reason": "article_mismatch",
            "reasons": ["article_mismatch"], "stored_article": "die", "noun": "Potenzial",
            "reference_article": "das", "reference_source": "wiktionary",
            "confirmed_by": ["openthesaurus"], "ot_knows_candidate": True,
            "wikt_candidate": "not_listed", "wikt_target": "listed"}
    base.update(kw)
    return base


def test_в_карточке_есть_артикль_перевод_и_вердикт_справочника():
    text = _card_text(_row(), index=1, total=3, left=10)
    assert "die Potenzial" in text and "потенциал" in text
    assert "das Potenzial" in text and "wiktionary" in text
    assert "OpenThesaurus" in text   # синонимия подтверждена — владелец видит, что спор только об артикле


def test_кнопка_оставить_называет_артикль_справочника():
    kb = _keyboard(_row())
    texts = [b["text"] for row in kb["inline_keyboard"] for b in row]
    assert texts[0] == "✅ оставить как das Potenzial"
    assert all(len(b["callback_data"].encode()) <= 64 for row in kb["inline_keyboard"] for b in row)


def test_справочник_не_знает_три_артикля_и_убрать():
    kb = _keyboard(_row(reason="article_unknown", reasons=["article_unknown"], reference_article="",
                        reference_source="нет данных"))
    data = [b["callback_data"] for row in kb["inline_keyboard"] for b in row]
    assert data == ["sacc:der:7", "sacc:die:7", "sacc:das:7", "sacc:drop:7"]


def test_неподтверждённый_синоним_говорит_что_знают_источники():
    text = _card_text(_row(de="die Öffnung", ru="открытие", reason="unconfirmed", reasons=["unconfirmed"],
                           stored_article="die", noun="Öffnung", reference_article="die",
                           confirmed_by=[], ot_knows_candidate=True, wikt_target="not_listed",
                           wikt_candidate="not_listed"), index=2, total=3, left=0)
    assert "НЕ подтверждена" in text and "OpenThesaurus" in text and "Wiktionary" in text
    assert "die Öffnung — справочник согласен" in text
