"""Вердикт о ВВОДЕ человека и СЛОВАРНАЯ ФОРМА — два разных вопроса.

Откуда взялось. Разбор давно возвращает поле `corrected_form`, и оно отвечает сразу на
оба вопроса. Замер живой базы 06.08.2026: поле заполнено у 785 карточек, у 471 из них
оно отличается от того, что написал человек, — но БОЛЬШИНСТВО этих различий не ошибки:

    «Sie kollabierte vor den Augen ihrer Freunde.» → «kollabieren»
    «Meine Chefin hat wirklich Haare auf den Zähnen.» → «Haare auf den Zähnen haben»
    «verbraucht» → «verbrauchen»

Это словарные формы, а предложения верны. Показать такое человеку как «возможно, вы
имели в виду» — соврать ему про его же правильный текст. Владелец на этом и поймал:
«какая тут ошибка я не пойму? зачем ты изменяешь моё предложение?»

Поэтому в разборе теперь ДВА отдельных поля: `input_is_correct` (верен ли ввод) и
`input_correction` (исправленный текст, только когда ошибка настоящая). Словарная форма
живёт отдельно и в подсказку не попадает никогда.
"""
import pathlib
import re

BACKEND = pathlib.Path(__file__).resolve().parents[1]
FRONTEND = BACKEND.parent / "frontend" / "src"

PROMPTS = ("dictionary_assistant", "dictionary_assistant_de",
           "dictionary_assistant_multilang", "dictionary_assistant_multilang_core_fast")


def test_every_breakdown_prompt_asks_the_two_questions_apart():
    text = (BACKEND / "openai_manager.py").read_text(encoding="utf-8")
    for name in PROMPTS:
        start = text.index('"%s": """' % name)
        end = text.index('""",', start)
        block = text[start:end]
        assert "input_is_correct" in block, "%s не спрашивает, верен ли ввод" % name
        assert "input_correction" in block, "%s не просит исправленный текст" % name


def test_prompt_forbids_putting_the_dictionary_form_into_the_correction():
    """Главное правило: правильное предложение всегда верно, даже если словарная форма
    выглядит иначе. Без этого запрета модель кладёт в правку лемму."""
    text = (BACKEND / "openai_manager.py").read_text(encoding="utf-8")
    assert "kollabieren" in text, "пропал пример, на котором владелец поймал ошибку"
    assert "Haare auf den Zähnen haben" in text
    assert re.search(r"input_is_correct=true", text), "нет правила «верное предложение = true»"


def test_fields_reach_the_card():
    server = (BACKEND / "backend_server.py").read_text(encoding="utf-8")
    for field in ("input_is_correct", "input_correction", "input_correction_reason"):
        assert field in server, "поле %s не доходит до карточки" % field


def test_hint_is_shown_only_on_a_real_error():
    """Подсказка рисуется по `input_is_correct === false`, а НЕ по наличию
    corrected_form — иначе она выскочит на верном предложении."""
    card = (FRONTEND / "dictionary" / "WordBreakdown.jsx").read_text(encoding="utf-8")
    assert "input_is_correct === false" in card
    assert "dq-input-hint" in card
    hint_block = card[card.index("const inputCorrection"):card.index("const inputCorrection") + 400]
    assert "corrected_form" not in hint_block, "подсказка не должна опираться на старое поле"


def test_hint_does_not_rewrite_the_saved_text():
    """Карточка человека остаётся как есть: подсказка только показывает. Владелец сказал
    прямо — «нельзя менять то, что человек захотел сохранить именно в таком формате»."""
    card = (FRONTEND / "dictionary" / "WordBreakdown.jsx").read_text(encoding="utf-8")
    start = card.index("const inputCorrection")
    block = card[start:start + 1200]
    for forbidden in ("setWord", "onSave", "item.word_de =", "item.word_ru ="):
        assert forbidden not in block, "подсказка не имеет права трогать текст карточки"
