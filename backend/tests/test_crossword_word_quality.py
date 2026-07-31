# -*- coding: utf-8 -*-
"""Кроссворд загадывает слова из живой речи, а не выдуманные.

Разбор банка 31.07 (72 кроссворда): в 31 % стояло слово, которого в немецком нет
(TUGENDHAFTIG, DEONTOLIGIE, MIVVERKEHR, обрубок VERSUCHSAN), и только 8 % слов
входили в первые 2000 самых частых. Проверка тут сторожит обе границы.
"""
import pytest

from backend.answer_eval import _summarize_crossword
from backend.crossword_generator import _accept_word_entry, _select_hidden_words
from backend.crossword_word_gate import check_word, is_everyday, normalize_word


NONSENSE = [
    "TUGENDHAFTIG",   # есть tugendhaft, такого слова нет
    "DEONTOLIGIE",    # опечатка
    "ETIMOLOGIE",     # опечатка (Etymologie)
    "AMPLELICHT",
    "MIVVERKEHR",
    "TIERWOEHNDE",
    "VERSUCHSAN",     # обрубок
    "VEROEFFENT",     # обрубок
    "RECHNUNGSEN",    # обрубок
    "ILLUSTRAT",      # обрубок
    "PARKPLATZES",    # родительный падеж
    "NIECE",          # английское
    "BAGGAGE",        # английское
    "EPIKURATION",
    "SYMBOLIKERIN",
]

EVERYDAY = [
    "KÜHLSCHRANK", "BAHNHOF", "RECHNUNG", "SCHLÜSSEL", "FRÜHSTÜCK",
    "REGENSCHIRM", "HALTESTELLE", "ZAHNBÜRSTE", "NACHBARIN", "FÜHRERSCHEIN",
]


@pytest.mark.parametrize("word", NONSENSE)
def test_nonsense_words_never_reach_a_puzzle(word):
    ok, reason = check_word(normalize_word(word))
    assert not ok, f"{word} прошло приёмку"
    assert reason


@pytest.mark.parametrize("word", EVERYDAY)
def test_everyday_words_pass(word):
    normalized = normalize_word(word)
    ok, reason = check_word(normalized)
    assert ok, f"{word} отклонено: {reason}"


def test_verb_stem_is_not_a_word():
    """FÜTTER — основа глагола, а не слово: есть «füttern» и «das Futter».

    Частотный список этого не различает (в живой речи «fütter ihn» встречается),
    поэтому форму подтверждает словарь словарных форм."""
    from backend.crossword_word_gate import check_word, form_lookup_keys

    lemma_pos = {"füttern": "verb", "futter": "noun"}
    ok, reason = check_word("FÜTTER", lemma_pos=lemma_pos)
    assert not ok and "основа глагола" in reason
    # …а сам глагол и само существительное проходят
    assert check_word("FÜTTERN", lemma_pos=lemma_pos)[0]
    assert check_word("FUTTER", lemma_pos=lemma_pos)[0]
    # у словаря спрашиваем и слово, и его инфинитив — иначе основу не отличить
    keys = form_lookup_keys(["FÜTTER"])
    assert "fütter" in keys and "füttern" in keys


def test_a_noun_whose_verb_exists_is_not_mistaken_for_a_stem():
    """BESUCH — существительное, хотя «besuchen» тоже есть. Слово в словаре есть,
    значит форма правильная, и правило про основу к нему не применяется."""
    from backend.crossword_word_gate import check_word

    ok, reason = check_word("BESUCH", lemma_pos={"besuch": "noun", "besuchen": "verb"})
    assert ok, reason


def test_word_rank_does_not_guess_endings():
    """Общий словарь артиклей при промахе дописывает окончание — здесь так нельзя:
    из-за этой догадки «fütter» получал место слова «füttern»."""
    from backend.article_word_gate import word_rank as tolerant_rank
    from backend.crossword_word_gate import word_rank as strict_rank

    assert tolerant_rank("fütter") == tolerant_rank("füttern")
    assert strict_rank("fütter") != strict_rank("füttern")


def test_spelling_is_repaired_from_the_dictionary():
    # Модель пишет умляуты то транслитом, то теряет их вовсе — в сетку должно лечь
    # написание, которое подтверждает словарь.
    assert normalize_word("REISEGEPAECK") == "REISEGEPÄCK"
    assert normalize_word("KUNSTLER") == "KÜNSTLER"
    assert normalize_word("ARCHAEOLOGIE") == "ARCHÄOLOGIE"
    # Сетка заглавная, а заглавная ß в немецком записывается как SS.
    assert normalize_word("Straßenbahn") == "STRASSENBAHN"
    assert normalize_word("Fußgänger") == "FUSSGÄNGER"


def test_accepted_entry_carries_a_translation_not_a_clue():
    entry, reason = _accept_word_entry({
        "word": "kuehlschrank",
        "clue_de": "Hier bleiben Milch und Butter kalt",
        "clue_ru": "Здесь молоко и масло остаются холодными",
        "translation_ru": "холодильник",
    })
    assert entry, reason
    assert entry["word"] == "KÜHLSCHRANK"
    assert entry["translation_ru"] == "холодильник"


def test_entry_without_clues_is_refused():
    entry, reason = _accept_word_entry({"word": "KÜHLSCHRANK", "clue_de": "", "clue_ru": ""})
    assert entry is None and reason


def test_hidden_words_are_the_ones_people_actually_say():
    # Сетка: ходовое слово и редкое пересекаются. Набирать руками просим ходовое.
    words = [
        {"word": "MIETE", "direction": "across", "row": 0, "col": 0, "number": 1},
        {"word": "MUSEUMSAMT", "direction": "down", "row": 0, "col": 0, "number": 2},
        {"word": "TERMIN", "direction": "down", "row": 0, "col": 4, "number": 3},
        {"word": "KAUTION", "direction": "across", "row": 4, "col": 0, "number": 4},
    ]
    result = _select_hidden_words(words, hidden_count=2)
    hidden = [w["word"] for w in result if w["hidden"]]
    assert len(hidden) == 2
    for word in hidden:
        assert is_everyday(word), f"загадали неходовое слово {word}"


def test_a_long_first_word_does_not_block_the_whole_grid():
    """Сетка держится в 12 клеток по стороне, но самое длинное слово ложится всегда.

    Пока предел был жёстким, WASCHMASCHINE (13 букв) сама выходила за границу и
    после неё в сетку не вставало НИ ОДНО слово — кроссворд уходил в брак ещё до
    отправки (в проде 2 из 6 генераций так и падали)."""
    from backend.crossword_generator import _place_words
    words = ["WASCHMASCHINE", "WOHNUNG", "KÜCHE", "MIETE", "TISCH", "SCHRANK", "LAMPE"]
    _grid, placed = _place_words([{"word": w, "clue_de": "x", "clue_ru": "x"} for w in words])
    assert len(placed) >= 5, [p["word"] for p in placed]


def test_saved_word_gets_the_translation_and_falls_back_to_the_clue():
    summary = _summarize_crossword([
        {"number": 1, "direction": "across", "correct": "KÜHLSCHRANK", "user_answer": "",
         "is_correct": False, "clue_de": "", "clue_ru": "Здесь продукты остаются холодными",
         "translation_ru": "холодильник"},
        {"number": 2, "direction": "down", "correct": "MIETE", "user_answer": "",
         "is_correct": False, "clue_de": "", "clue_ru": "Плата за квартиру каждый месяц",
         "translation_ru": ""},
    ], already_answered=False)
    targets = {item["source"]: item["target"] for item in summary["saveable_words"]}
    assert targets["KÜHLSCHRANK"] == "холодильник"
    # У старых кроссвордов перевода в банке нет — подсказка остаётся запасным вариантом.
    assert targets["MIETE"] == "Плата за квартиру каждый месяц"


def test_form_judge_failure_does_not_kill_the_puzzle(monkeypatch):
    """Второе мнение — полировка, а не сито безопасности.

    Запрос не прошёл — слова остаются: существование они уже подтвердили, иначе
    одна сетевая заминка остановила бы наполнение пула целиком."""
    import backend.crossword_word_gate as gate
    from backend.crossword_generator import _apply_form_judge

    def _boom(_words):
        raise gate.FormJudgeUnavailable("нет сети")

    monkeypatch.setattr(gate, "judge_word_forms", _boom)
    words = [{"word": f"WORT{i}"} for i in range(8)]
    rejected: list[str] = []
    assert _apply_form_judge(words, rejected) == words
    assert not rejected


def test_form_judge_drops_what_it_names_and_keeps_the_rest(monkeypatch):
    """Отвергнутое уходит, а слово, о котором модель промолчала, остаётся:
    молчание — не приговор."""
    import backend.crossword_word_gate as gate
    from backend.crossword_generator import _apply_form_judge

    monkeypatch.setattr(gate, "judge_word_forms",
                        lambda words: {"PASSTASCHE": False, "KÜHLSCHRANK": True})
    words = [{"word": "PASSTASCHE"}, {"word": "KÜHLSCHRANK"}, {"word": "TELLER"}]
    rejected: list[str] = []
    kept = [w["word"] for w in _apply_form_judge(words, rejected)]
    assert kept == ["KÜHLSCHRANK", "TELLER"]
    assert rejected and "PASSTASCHE" in rejected[0]


def test_form_judge_reads_the_model_verdicts(monkeypatch):
    """Разбор ответа модели: false — убрать, отсутствие ключа — оставить."""
    import requests
    import backend.crossword_word_gate as gate

    class _Resp:
        ok = True
        @staticmethod
        def json():
            return {"choices": [{"message": {"content": '{"PASSTASCHE": false}'}}]}

    monkeypatch.setenv("OPENAI_API_KEY", "test")
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _Resp())
    verdicts = gate.judge_word_forms(["PASSTASCHE", "KÜHLSCHRANK"])
    assert verdicts["PASSTASCHE"] is False
    assert verdicts["KÜHLSCHRANK"] is True
