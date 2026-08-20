"""Чередование рубрик «Новость дня» / «Стендап дня» и разбор слов стендапа.

Что эти тесты не дают вернуть:

1. Расписание разъезжается. Чередование считается от якоря, а не от хранимого состояния,
   поэтому пропущенный день, перезапуск, 31-е число и граница года ничего не сдвигают.
2. В карточку слова попадает цитата, которой в ролике не звучало. Для языкового
   приложения это тот же класс дефекта, что и выдуманная грамматика: человек читает и
   заучивает. Цитата обязана дословно найтись в субтитрах, иначе карточка выбрасывается.
3. Разбор показывается урезанным. Если годных карточек меньше порога, пакет бракуется
   целиком и генератор берёт следующий ролик — пустого разбора человек не увидит.
4. Новостная рубрика меняет поведение из-за переезда на профили.
"""
import pytest

from backend.daily_video_rubrics import (
    NEWS_PROFILE,
    RUBRIC_NEWS,
    RUBRIC_STANDUP,
    STANDUP_PROFILE,
    get_profile,
    rubric_for_date,
)
from backend.world_news_generator import _length_priority, _validate_and_normalize_pack


# ── Чередование ────────────────────────────────────────────────────────────────

def test_alternates_strictly_day_by_day():
    """20.08.2026 — новости (в это утро они и ушли), дальше строго через день."""
    assert rubric_for_date("2026-08-20") == RUBRIC_NEWS
    assert rubric_for_date("2026-08-21") == RUBRIC_STANDUP
    assert rubric_for_date("2026-08-22") == RUBRIC_NEWS
    assert rubric_for_date("2026-08-23") == RUBRIC_STANDUP


def test_month_and_year_boundaries_do_not_break_the_rhythm():
    """31-е число и Новый год — те места, где ломается чередование по чётности числа."""
    for a, b in [("2026-08-30", "2026-08-31"),   # 30 → 31
                 ("2026-08-31", "2026-09-01"),   # конец месяца
                 ("2026-12-31", "2027-01-01")]:  # конец года
        assert rubric_for_date(a) != rubric_for_date(b), f"{a} и {b} оказались одной рубрикой"


def test_a_missed_day_does_not_shift_the_schedule():
    """Пропуск дня ничего не сдвигает: рубрика считается от даты, а не от «прошлого раза»."""
    assert rubric_for_date("2026-09-10") == rubric_for_date("2026-09-12")
    assert rubric_for_date("2026-09-10") != rubric_for_date("2026-09-11")


def test_alternation_can_be_switched_off(monkeypatch):
    """Выключатель возвращает поведение «каждый день новости» без выката кода."""
    monkeypatch.setenv("DAILY_VIDEO_ALTERNATION_ENABLED", "0")
    assert rubric_for_date("2026-08-21") == RUBRIC_NEWS


def test_unknown_rubric_is_an_error_not_a_default():
    with pytest.raises(ValueError):
        get_profile("kabarett")


# ── Длительность берётся из профиля ────────────────────────────────────────────

def test_length_priority_follows_the_profile():
    """У стендапа предпочтительное окно 5–10 мин, у новостей 5–7. Один и тот же
    восьмиминутный ролик должен считаться идеальным для стендапа и «длиннее нужного»
    для новостей — иначе профиль ни на что не влияет."""
    eight_minutes = 480
    assert _length_priority(eight_minutes, STANDUP_PROFILE)[0] == 0
    assert _length_priority(eight_minutes, NEWS_PROFILE)[0] == 1


# ── Разбор слов стендапа ───────────────────────────────────────────────────────

_TRANSCRIPT = (
    "Also ich sag mal so, ich hab null Bock auf Montag. "
    "Meine Oma sagt immer, das ist doch der Hammer, Junge. "
    "Und dann steh ich da wie bestellt und nicht abgeholt."
)


def _phrase(de, quote, **over):
    item = {
        "de": de,
        "register_ru": "разговорное",
        "translation_ru": "перевод здесь",
        "literal_ru": "",
        "quote_de": quote,
        "quote_ru": "перевод цитаты",
        "usage_ru": "с друзьями свободно",
    }
    item.update(over)
    return item


def _pack(phrases):
    return {
        "summary_points": ["комик про понедельник"],
        "phrases": phrases,
        "quiz": [
            {"question_de": f"Frage {i}?", "options": ["a", "b", "c", "d"],
             "correct_index": 0, "explanation_ru": "потому что"}
            for i in range(4)
        ],
    }


def _good_phrases(n=5):
    quotes = [
        "ich hab null Bock auf Montag",
        "das ist doch der Hammer",
        "wie bestellt und nicht abgeholt",
        "Also ich sag mal so",
        "Meine Oma sagt immer",
    ]
    return [_phrase(f"оборот {i}", quotes[i]) for i in range(n)]


def test_standup_card_keeps_the_linguistic_fields():
    """Помета регистра, цитата из ролика и её перевод обязаны дойти до карточки —
    без них остаётся сухой перевод, из-за которого и заучивается неверный сленг."""
    out = _validate_and_normalize_pack(_pack(_good_phrases()), STANDUP_PROFILE, _TRANSCRIPT)
    card = out["phrases"][0]
    assert card["register_ru"] == "разговорное"
    assert card["quote_de"] == "ich hab null Bock auf Montag"
    assert card["quote_ru"] == "перевод цитаты"
    assert "literal_ru" in card


def test_quote_that_is_not_in_the_video_is_thrown_away():
    """Модель, которой велено скопировать строку, иногда пересказывает её своими словами.
    Такая карточка показала бы человеку фразу, которой в ролике не звучало."""
    phrases = _good_phrases() + [_phrase("выдумка", "diesen Satz hat niemand gesagt")]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert all(p["de"] != "выдумка" for p in out["phrases"])
    assert len(out["phrases"]) == 5


def test_quote_matching_ignores_punctuation_not_words():
    """В субтитрах знаки препинания стоят иначе, чем их перепишет модель, — сверяем слова."""
    phrases = _good_phrases(4) + [_phrase("оборот", "Ich hab null Bock, auf Montag!!")]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


def test_incomplete_card_is_thrown_away():
    """Карточка без пометы регистра или без перевода цитаты не показывается частично."""
    phrases = _good_phrases() + [
        _phrase("без пометы", "Also ich sag mal so", register_ru=""),
        _phrase("без перевода цитаты", "Meine Oma sagt immer", quote_ru=""),
    ]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


def test_too_few_good_cards_rejects_the_whole_video():
    """Порог годности РОЛИКА: меньше четырёх годных карточек — это не материал для
    рубрики, и генератор обязан взять следующий ролик, а не показать пустой разбор."""
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(_pack(_good_phrases(2)), STANDUP_PROFILE, _TRANSCRIPT)


def test_there_is_no_target_number_of_cards():
    """Плана по количеству нет: сколько в ролике вправду есть — столько и показываем.
    Пять годных карточек — валидный разбор, добирать до вилки нечем и незачем."""
    out = _validate_and_normalize_pack(_pack(_good_phrases(5)), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


# ── Новости не изменились ──────────────────────────────────────────────────────

def test_news_pack_still_needs_no_quote():
    """Новостная карточка как была: слово, перевод, как употреблять — цитата не нужна,
    порог прежний (6 фраз). Переезд на профили не должен менять новости."""
    phrases = [{"de": f"das Wort {i}", "translation_ru": "слово", "usage_ru": "с артиклем"}
               for i in range(6)]
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, "")
    assert len(out["phrases"]) == 6
    assert "quote_de" not in out["phrases"][0]


def test_news_pack_below_six_phrases_is_rejected():
    phrases = [{"de": f"das Wort {i}", "translation_ru": "слово", "usage_ru": "с артиклем"}
               for i in range(5)]
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, "")
