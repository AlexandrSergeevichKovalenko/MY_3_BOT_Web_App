"""Принятая правка доезжает до ВСЕХ мест, где лежал старый текст.

Владелец правит спорную фразу, судья предлагает вариант, человек принимает. Раньше
менялась одна строка — слово в справочнике, — а тот же текст к этому моменту скопирован
в карточку, в разбор внутри карточки, в примеры, в общий пул и в заготовку задания.
Замер 16.08.2026: 856 правок осели в 3381 месте, по четыре на правку.
"""
import json

import backend.database as db


class FakeCursor:
    """База из трёх таблиц: слово, карточки, пул."""

    def __init__(self, unit_card, cards, pool):
        self.unit_card = unit_card
        self.cards = cards          # [(id, word_de, translation_de, response_json, canonical_id)]
        self.pool = pool            # {id: (source_text, response_json)}
        self.updates = []
        self._last = None

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self._last = (text, params)
        if text.startswith("UPDATE"):
            self.updates.append((text, params))
            if "bt_3_lex_units" in text:
                self.unit_card = json.loads(params[0])
            elif "bt_3_webapp_dictionary_queries" in text:
                payload, word, tr, entry_id = params
                for index, row in enumerate(self.cards):
                    if row[0] == entry_id:
                        self.cards[index] = (row[0], word or row[1], tr or row[2],
                                             json.loads(payload), row[4])
            elif "bt_3_dictionary_entries" in text:
                payload, source, pool_id = params
                old_source, _ = self.pool[pool_id]
                self.pool[pool_id] = (source or old_source, json.loads(payload))

    def fetchone(self):
        text, params = self._last
        if "SELECT card FROM bt_3_lex_units" in text:
            return (self.unit_card,)
        if "SELECT source_text, response_json FROM bt_3_dictionary_entries" in text:
            return self.pool.get(params[0])
        return None

    def fetchall(self):
        return list(self.cards)


OLD = "Daher vornehme ich Korrekturen selbst"
NEW = "Daher nehme ich Korrekturen selbst vor"


def _build():
    unit_card = {"usage_examples": [{"source": OLD, "target": "Поэтому я сам вношу исправления."}],
                 "original_query": OLD,
                 "pronunciation": {"stress": OLD}}
    cards = [(18, OLD, OLD,
              {"word_de": OLD, "target_text": OLD, "translation_ru": "Поэтому исправления вношу сам",
               "raw_text": OLD,
               "sentence_gap_v2": {"source_sentence": OLD, "payload": {"sentence_with_gap": "Daher ___ selbst"}}},
              900)]
    pool = {900: (OLD, {"source_text": OLD, "meanings": {"primary": {"example_source": OLD}}})}
    return FakeCursor(unit_card, cards, pool)


def test_correction_reaches_card_word_and_breakdown():
    cur = _build()
    report = db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    _entry_id, word_de, translation_de, payload, _canonical = cur.cards[0]
    assert word_de == NEW, "заголовок карточки — то, что видно крупно"
    assert translation_de == NEW
    assert payload["word_de"] == NEW
    assert payload["target_text"] == NEW
    assert report["cards"] == 1


def test_correction_reaches_the_shared_word_breakdown():
    cur = _build()
    db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    assert cur.unit_card["usage_examples"][0]["source"] == NEW


def test_correction_reaches_the_pool():
    """Из пула собирается карточка при поиске: без этого шага человек, набравший фразу,
    получил бы старую версию и положил её себе в словарь заново."""
    cur = _build()
    db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    source_text, payload = cur.pool[900]
    assert source_text == NEW
    assert payload["meanings"]["primary"]["example_source"] == NEW


def test_history_and_stress_are_never_rewritten():
    """История ввода и разметка ударений совпадают с текстом только по буквам."""
    cur = _build()
    db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    assert cur.unit_card["original_query"] == OLD
    assert cur.unit_card["pronunciation"]["stress"] == OLD
    assert cur.cards[0][3]["raw_text"] == OLD


def test_russian_side_is_never_touched():
    cur = _build()
    db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    assert cur.cards[0][3]["translation_ru"] == "Поэтому исправления вношу сам"


def test_task_is_dropped_not_patched():
    """У задания пропуск «___»: заменой строки вопрос разойдётся с ответом.

    Проверка на живых данных 16.08.2026: пять заданий сходились 5 из 5, после наивной
    замены сошлось бы 0 из 5."""
    cur = _build()
    report = db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=NEW)
    assert "sentence_gap_v2" not in cur.cards[0][3]
    assert report["tasks_dropped"] == 1


def test_nothing_happens_without_a_real_change():
    cur = _build()
    assert db.spread_correction_everywhere(cur, unit_id=1, old_text=OLD, new_text=OLD)["cards"] == 0
    assert db.spread_correction_everywhere(cur, unit_id=1, old_text="", new_text=NEW)["cards"] == 0


def test_case_only_correction_is_a_real_change_in_german():
    """Правка ОДНОЙ БУКВЫ по регистру — полноценная правка, а не пустая.

    В немецком регистр это грамматика: «Es ist mir Latte» → «latte», «um Rund die
    Hälfte» → «rund». Здесь стояло сравнение без учёта регистра, и такая правка
    считалась пустой: справочник владелец чинил, а в карточке человека оставалось
    старое написание. Замер 19.08.2026 на живой базе: обе правки регистра, сделанные
    владельцем в тот день, разъехались — то есть класс ломался целиком, 2 из 2.
    """
    old = "Es ist mir Latte"
    new = "Es ist mir latte"
    cur = FakeCursor(
        {"usage_examples": [{"source": old, "target": "Мне всё равно"}]},
        [(18, old, None, {"word_de": old}, 900)],
        {900: (old, {"source_text": old})},
    )
    report = db.spread_correction_everywhere(cur, unit_id=1, old_text=old, new_text=new)
    assert cur.cards[0][1] == new, "карточка человека обязана получить новое написание"
    assert cur.cards[0][3]["word_de"] == new
    assert cur.pool[900][0] == new
    assert report["cards"] == 1


def test_the_function_cleans_its_own_input():
    """Чистка живёт ВНУТРИ развоза, а не у вызывающего.

    Раньше оба вызывающих чистили текст сами, и дырки не было — пока их двое. Третий
    пришёл бы без чистки, и грязь разъехалась бы разом по трём хранилищам. Правило,
    которое держится на дисциплине вызывающего, рано или поздно не держится: 21.08.2026
    соседний агент переименовал семь заголовков своим запросом, и пятнадцать записей
    пула остались со старым написанием.
    """
    cur = _build()
    # Невидимый символ и двойные пробелы — ровно та грязь, из-за которой одно и то же
    # слово перестаёт быть равно самому себе.
    db.spread_correction_everywhere(
        cur, unit_id=1, old_text=OLD, new_text="Daher nehme​ ich  Korrekturen selbst vor")
    _entry_id, word_de, _translation_de, _payload, _canonical = cur.cards[0]
    assert word_de == NEW, f"развоз обязан был почистить вход, а положил {word_de!r}"


def test_mangled_text_is_never_spread():
    """Развоз порчи — худшее, что может случиться: она размножится по трём хранилищам.

    Признак берётся из общего модуля backend/mangled_text.py, своего здесь нет.
    """
    cur = _build()
    report = db.spread_correction_everywhere(
        cur, unit_id=1, old_text=OLD, new_text="Daher nehme ich Korrekturen selbst vor......")
    assert report["cards"] == 0
    assert cur.cards[0][1] == OLD, "испорченный текст не должен никуда уехать"
