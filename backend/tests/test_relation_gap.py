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


# ── Третий заход: справочник спряжений ───────────────────────────────────────
def test_glagol_v_preterite_naydyotsya_po_spravochniku():
    """Повод, 14.09.2026: /gap_test встал на слове «bemerken» — ни одной заготовки.

    Примеры там ПРАВИЛЬНЫЕ («Plötzlich registrierte sie einen Fehler im Text»), а не
    находил их поиск: он приписывал окончание к полному слову, тогда как немецкий
    глагол спрягается ЗАМЕНОЙ «-en». Форму не выводим — берём из справочника."""
    sent = "Plötzlich registrierte sie einen Fehler im Text."
    assert find_form_in_sentence("registrieren", sent) is None, "без справочника — не находим"
    assert find_form_in_sentence("registrieren", sent, {"registrierte", "registriert"}) == "registrierte"
    item, why = build_gap_item(synonym="registrieren", synonym_ru="", sentence_de=sent,
                               forms={"registrierte"})
    assert item is not None, why
    assert item["filler"] == "registrierte"
    assert item["sentence_gapped"] == "Plötzlich ___ sie einen Fehler im Text."


def test_otdelyaemyy_glagol_ne_rezhetsya_po_obrubku():
    """У «wahrnehmen» в справочнике напечатано «nahm wahr», а в живом предложении эта
    форма РАЗОРВАНА: «nahm sie einen Fehler wahr». Голое «nahm» брать нельзя — пропуск
    встанет на обрубок, страж соберёт предложение обратно (вырезали же ровно его) и
    пропустит задание, где правильный ответ «nahm» вместо слова.
    Такое слово требует ДВУХ пропусков, а это отдельная задача."""
    sent = "Plötzlich nahm sie einen Fehler wahr."
    assert find_form_in_sentence("wahrnehmen", sent, {"nahm", "nahm wahr"}) is None
    item, why = build_gap_item(synonym="wahrnehmen", synonym_ru="", sentence_de=sent,
                               forms={"nahm", "nahm wahr"})
    assert item is None and why == "слова в предложении нет"


def test_slitnaya_mnogoslovnaya_forma_beryotsya_tselikom():
    """А если форма стоит в предложении ЦЕЛИКОМ — её и берём, а не первое слово."""
    sent = "Gestern nahm wahr niemand etwas."     # искусственно слитно
    assert find_form_in_sentence("wahrnehmen", sent, {"nahm", "nahm wahr"}) == "nahm wahr"


def test_bez_spravochnika_povedenie_prezhnee():
    """Справочник не ответил — заготовка просто не строится и считается. Не догадываемся."""
    tj = {"correct_examples": [
        {"word": "registrieren", "sentence_de": "Plötzlich registrierte sie es.", "sentence_ru": ""},
    ]}
    items, skipped = build_gap_items(wort="bemerken", accepted=[], trainer_json=tj)
    assert items == [] and skipped == {"слова в предложении нет": 1}


def test_spravochnik_podayotsya_vyzyvayushchim_i_padenie_ne_roniaet_sborku():
    def broken(_word):
        raise RuntimeError("справочник недоступен")
    tj = {"correct_examples": [
        {"word": "gründlich", "sentence_de": "Eine gründliche Prüfung.", "sentence_ru": ""},
    ]}
    items, skipped = build_gap_items(wort="detailliert", accepted=[], trainer_json=tj,
                                     forms_of=broken)
    assert len(items) == 1 and not skipped, "падение источника не должно ронять сборку"


# ── ОДНА ДВЕРЬ СБОРКИ НА ВСЕХ (15.09.2026) ───────────────────────────────────
# Правил было два: экран человека и проверка ответа строили заготовки СО справочником
# спряжений, а отправители (рассылка по часам и капля) — без него. Отправитель строит
# заготовки только чтобы решить «есть ли что показывать», и по бедному правилу он видел
# ноль там, где у человека на экране собралось бы задание целиком.
# Замер 15.09.2026 на живом банке (77 слов): 475 заготовок без справочника против 485 с
# ним, и ровно три слова — verwirren (4), bemerken (2), versäumen (4) — отправитель
# молча считал пустыми. Слов, где он видит меньше, но не ноль, нет ни одного.
def test_dver_sborki_sama_podayot_spravochnik_form():
    """build_gap_items_for берёт справочник сам — вызывающему о нём знать не нужно."""
    from unittest import mock
    from backend import relation_gap

    строка_банка = {"wort": "bemerken", "accepted": [{"de": "registrieren", "ru": "заметить"}],
                    "trainer_json": {"correct_examples": [
                        {"word": "registrieren",
                         "sentence_de": "Plötzlich registrierte sie einen Fehler im Text.",
                         "sentence_ru": "Вдруг она заметила ошибку в тексте."}]}}
    # Без справочника форма «registrierte» не находится: немецкий глагол спрягается
    # заменой «-en», а не приклейкой окончания.
    без, _ = build_gap_items(wort=строка_банка["wort"], accepted=строка_банка["accepted"],
                             trainer_json=строка_банка["trainer_json"])
    assert без == [], "замер изменился: проверь, не переписан ли поиск формы"

    with mock.patch("backend.answer_eval._gap_forms_lookup",
                    return_value=lambda w: {"registrierte"} if w == "registrieren" else None):
        сдверью, _ = relation_gap.build_gap_items_for(строка_банка)
    assert len(сдверью) == 1, "дверь сборки не подала справочник форм"


def test_nikto_ne_sobiraet_zagotovki_mimo_dveri():
    """Второй текст того же правила — это два экрана с двумя ответами на один вопрос.

    Прямой вызов build_gap_items разрешён ТОЛЬКО внутри самой двери (relation_gap.py) и
    в тестах. Красный тест = где-то опять собирают заготовки своим правилом.
    """
    import os
    import re

    корень = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    нарушители = []
    for путь in ("bot_3.py", "backend/answer_eval.py", "backend/fix_promises.py",
                 "backend/backend_server.py"):
        полный = os.path.join(корень, путь)
        if not os.path.exists(полный):
            continue
        for n, строка in enumerate(open(полный, encoding="utf-8"), 1):
            голая = строка.split("#")[0]
            if re.search(r"\bbuild_gap_items\s*\(", голая) or \
               re.search(r"\bbuild_gap_items\b(?!_for)\s*,", голая):
                нарушители.append(f"{путь}:{n}")
    assert not нарушители, ("заготовки собираются мимо двери build_gap_items_for: "
                            + ", ".join(нарушители))
