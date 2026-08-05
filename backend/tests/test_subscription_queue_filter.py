"""Что подписка предлагает человеку, а что пропускает.

Очередь строится по частотности, поэтому в самое начало попадает то, что встречается
чаще всего, — а там оказался голый артикль «Eine». Он достался бы первым словом каждому
подписчику.

Фильтр намеренно узкий. Замер 05.08.2026 по словарю автора (14 443 слова): годятся
13 767, повторов 673, без перевода 2, голый артикль 1. То есть 95% очереди в порядке —
придирчивый фильтр отнял бы у людей больше, чем дал.

Главная тонкость — повторы. Из 673 совпадений по заголовку РАЗНЫМИ словами оказались
194: «Der Gefallen» (одолжение) против «gefallen» (нравиться), «Das Verhalten»
(поведение) против «Verhalten» (вести себя). Схлопни их по заголовку — человек потерял
бы 194 настоящих слова. Поэтому повтором считается только то, у чего совпадает и
заголовок, и хоть одно слово перевода.
"""
from backend.database import (
    subscription_candidate_is_already_known,
    subscription_candidate_is_worth_offering,
    _subscription_headword_key,
    _subscription_translation_words,
)


def _owned(*pairs):
    owned = {}
    for word, translation in pairs:
        owned.setdefault(_subscription_headword_key(word), set()).update(
            _subscription_translation_words(translation)
        )
    return owned


# ── что вообще годится к выдаче ───────────────────────────────────────────────

def test_bare_article_is_not_a_word_to_learn():
    """Живой случай: «Eine» стоит ПЕРВЫМ в очереди по частотности."""
    assert not subscription_candidate_is_worth_offering("Eine", "Одна")
    assert not subscription_candidate_is_worth_offering("das", "то")


def test_entry_without_a_russian_translation_is_skipped():
    assert not subscription_candidate_is_worth_offering("aspettiamo", "aspettiamo una risposta")
    assert not subscription_candidate_is_worth_offering("der Tisch", "")


def test_ordinary_word_passes():
    for word, translation in (
        ("der Tisch", "стол"),
        ("gelingen", "удаваться"),
        ("das Aus", "конец, аут"),
        ("Das Wohl", "благо"),
        ("hinter", "за, позади"),
    ):
        assert subscription_candidate_is_worth_offering(word, translation), word


# ── повторы ───────────────────────────────────────────────────────────────────

def test_same_word_in_another_spelling_is_not_offered_twice():
    owned = _owned(("die Rede", "Речь"), ("hart", "твёрдый; жёсткий"))
    assert subscription_candidate_is_already_known("Die Rede", "Речь, разговор", owned)
    assert subscription_candidate_is_already_known("Hart", "Трудно, твердый, крепкий", owned)


def test_different_words_with_the_same_headword_are_both_offered():
    """Это и есть та самая ловушка: заголовок один, слова разные."""
    owned = _owned(("gefallen", "Нравиться"))
    assert not subscription_candidate_is_already_known("Der Gefallen", "Одолжение, любезность", owned)

    owned = _owned(("Verhalten", "Вести себя"))
    assert not subscription_candidate_is_already_known("Das Verhalten", "Поведение", owned)

    owned = _owned(("Übel", "Дурной, плохой"))
    assert not subscription_candidate_is_already_known("Das Übel", "Зло", owned)


def test_unknown_word_is_not_a_repeat():
    owned = _owned(("der Tisch", "стол"))
    assert not subscription_candidate_is_already_known("der Stuhl", "стул", owned)


def test_empty_sides_do_not_crash():
    assert not subscription_candidate_is_already_known(None, "перевод", _owned(("x", "y")))
    assert not subscription_candidate_is_already_known("слово", None, {})
    assert not subscription_candidate_is_worth_offering(None, None)
