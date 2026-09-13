# -*- coding: utf-8 -*-
"""«Подставь синоним» — сборка пропуска и проверка ответа.

Эти тесты держат КЛАСС, а не отдельный случай: каждый из них закрывает правило,
на котором игра может тихо начать врать по-немецки.
Стратегия: docs/tasks/synonym_gap_wednesday_strategy.md
"""
import pytest

from backend.relation_gap import (
    CORRECT, OTHER_SYNONYM, WRONG, WRONG_FORM,
    build_gap_item, build_gap_items, find_form_in_sentence, find_form_span,
    grade_gap_answer,
)


# ── Где стоит форма ──────────────────────────────────────────────────────────
def test_tochnaya_forma_naydena():
    assert find_form_in_sentence("gründlich", "Eine gründlich geprüfte Zahl.") == "gründlich"


def test_sklonyonnaya_forma_naydena():
    sent = "Der Bericht enthält eine ausführliche Beschreibung des Unfalls."
    assert find_form_in_sentence("ausführlich", sent) == "ausführliche"


def test_slovo_vnutri_drugogo_slova_ne_schitaetsya():
    """ГЛАВНЫЙ тест этого файла. «suchen» внутри «versuchen» — не вхождение слова.

    Прогон по банку 13.09.2026 поймал живой случай: у антонима «vermeiden» синоним
    «suchen» стоит в «Konflikte zu suchen», а вырезание «заменой первого вхождения»
    съедало хвост слова «versuchen» и человек получал задание
    «Wir sollten ver___, Konflikte zu suchen». Пропуск режется ТОЛЬКО по span."""
    sent = "Wir sollten versuchen, Konflikte zu suchen, um ein gutes Klima zu schaffen."
    span = find_form_span("suchen", sent)
    assert span is not None
    assert sent[span[0]:span[1]] == "suchen"
    assert span[0] > sent.index("versuchen")     # НЕ внутри «versuchen»
    item, why = build_gap_item(synonym="suchen", synonym_ru="искать", sentence_de=sent)
    assert item is not None, why
    assert item["sentence_gapped"] == "Wir sollten versuchen, Konflikte zu ___, um ein gutes Klima zu schaffen."


def test_artikl_v_zagolovke_ne_meshaet():
    sent = "Er bedankt sich herzlich für die Hilfe bei seinem Projekt."
    item, why = build_gap_item(synonym="die Hilfe", synonym_ru="помощь", sentence_de=sent)
    assert item is not None, why
    assert item["filler"] == "Hilfe"
    assert item["sentence_gapped"] == "Er bedankt sich herzlich für die ___ bei seinem Projekt."


# ── Когда заготовка НЕ строится (и это не заглушка, а честный отказ) ─────────
def test_slova_v_predlozhenii_net_zagotovka_ne_stroitsya():
    """Отделяемый глагол стоит в предложении в ДВУХ местах («klärten … auf»),
    одним пропуском его не вырезать. Заготовка не строится — и не подставляется
    инфинитив вместо формы."""
    sent = "Seine Aussagen klärten alle Zuhörer im Raum auf."
    item, why = build_gap_item(synonym="aufklären", synonym_ru="разъяснять", sentence_de=sent)
    assert item is None
    assert why == "слова в предложении нет"


def test_pustoe_predlozhenie_ne_stroit_zagotovku():
    item, why = build_gap_item(synonym="gründlich", synonym_ru="", sentence_de="")
    assert item is None and why == "нет предложения"


def test_otkazy_schitayutsya_po_klassam():
    """Молчание — не решение: то, что не построилось, обязано быть ПОСЧИТАНО."""
    tj = {"correct_examples": [
        {"word": "gründlich", "sentence_de": "Eine gründliche Prüfung.", "sentence_ru": "Тщательная проверка."},
        {"word": "aufklären", "sentence_de": "Er klärte alles auf.", "sentence_ru": "Он всё разъяснил."},
        {"word": "egal", "sentence_de": "", "sentence_ru": ""},
    ]}
    items, skipped = build_gap_items(wort="detailliert", accepted=[], trainer_json=tj)
    assert len(items) == 1
    assert skipped == {"слова в предложении нет": 1, "нет предложения": 1}


def test_slovo_samo_sebe_sinonimom_ne_idyot():
    tj = {"correct_examples": [
        {"word": "detailliert", "sentence_de": "Eine detaillierte Prüfung.", "sentence_ru": ""},
    ]}
    items, skipped = build_gap_items(wort="detailliert", accepted=[], trainer_json=tj)
    assert items == []
    assert skipped == {"слово само себе синоним": 1}


def test_skolko_u_slova_primerov_stolko_i_zagotovok():
    """Решение владельца 13.09.2026: цифру не выдумывать — сколько есть, столько и даём."""
    tj = {"correct_examples": [
        {"word": f"wort{i}", "sentence_de": f"Das ist wort{i} heute.", "sentence_ru": ""}
        for i in range(9)
    ]}
    items, skipped = build_gap_items(wort="anker", accepted=[], trainer_json=tj)
    assert len(items) == 9 and not skipped
    assert [it["index"] for it in items] == list(range(9))


# ── Подсказка ────────────────────────────────────────────────────────────────
def test_podskazka_pervaya_bukva_i_dlina():
    sent = "Der Bericht enthält eine ausführliche Beschreibung."
    item, _ = build_gap_item(synonym="ausführlich", synonym_ru="", sentence_de=sent)
    assert item["hint_letter"] == "a"
    assert item["hint_len"] == len("ausführliche")


# ── Четыре исхода проверки ───────────────────────────────────────────────────
@pytest.fixture
def item():
    sent = "Der Bericht enthält eine ausführliche Beschreibung des Unfalls."
    it, why = build_gap_item(synonym="ausführlich", synonym_ru="подробный", sentence_de=sent)
    assert it is not None, why
    return it


ALL = [{"de": "ausführlich", "ru": "подробный"}, {"de": "gründlich", "ru": "тщательный"}]


def test_forma_iz_predlozheniya_verno(item):
    assert grade_gap_answer(item=item, answer="ausführliche", all_synonyms=ALL)["outcome"] == CORRECT


def test_melochi_pravopisaniya_proshcheny(item):
    """Дом решил раньше: ß↔ss, ä↔ae и регистр смысла не меняют."""
    assert grade_gap_answer(item=item, answer="  AUSFUEHRLICHE ", all_synonyms=ALL)["outcome"] == CORRECT


def test_slovarnaya_forma_ne_zaschityvaetsya(item):
    """Решение владельца 13.09.2026: «eine ausführlich Beschreibung» — неверный
    немецкий, галочкой он помечен не будет. Но это отдельный исход, а не «мимо»:
    человеку скажут, что не так, и дадут дописать."""
    v = grade_gap_answer(item=item, answer="ausführlich", all_synonyms=ALL)
    assert v["outcome"] == WRONG_FORM
    assert v["outcome"] != CORRECT


def test_drugoy_sinonim_svoy_ishod(item):
    v = grade_gap_answer(item=item, answer="gründlich", all_synonyms=ALL)
    assert v["outcome"] == OTHER_SYNONYM
    assert v["matched"] == "gründlich"


def test_ne_to_slovo_i_ono_posschitano(item):
    """Форму с чужим окончанием мы распознать НЕ УМЕЕМ — справочника у игры нет,
    а выводить морфологию своей арифметикой запрещено. Значит честное «неверно»
    плюс отметка `unrecognised`, чтобы такие вводы можно было сосчитать."""
    v = grade_gap_answer(item=item, answer="ausführlichen", all_synonyms=ALL)
    assert v["outcome"] == WRONG
    assert v["unrecognised"] is True


def test_pustoy_otvet_ne_verno(item):
    assert grade_gap_answer(item=item, answer="   ", all_synonyms=ALL)["outcome"] == WRONG
