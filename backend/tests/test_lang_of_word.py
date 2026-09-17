"""Сторож ответчика «какой язык лежит в слове карточки».

Правило одно на всё приложение, и от него зависит, применится ли немецкая грамматика
к английскому слову. Копия правила в другом месте однажды разойдётся с этой — поэтому
тут закреплены именно те случаи, на которых легко ошибиться.
"""
import os

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.lang_of_word import это_немецкое, язык_слова


def _я(s, t, n):
    return язык_слова(source_lang=s, target_lang=t, native_lang=n)


def test_сегодняшние_пары_отвечают_немецкий():
    """Вся живая база на 16.09.2026 — ru/de в обе стороны."""
    assert _я("ru", "de", "ru") == "de"
    assert _я("de", "ru", "ru") == "de"


def test_английская_карточка_отвечает_английский():
    assert _я("en", "ru", "ru") == "en"
    assert _я("ru", "en", "ru") == "en"


def test_пара_английский_немецкий_НЕ_немецкая():
    """⚠ ГЛАВНЫЙ СЛУЧАЙ. У пары en→de немецкий — РОДНОЙ язык человека, а изучаемое
    слово английское. Условие «есть ли de в паре» здесь врёт и пропускает английское
    слово в немецкую машину. Так написан german_form_warm.py — и это его дефект."""
    assert _я("en", "de", "de") == "en"
    assert это_немецкое(source_lang="en", target_lang="de", native_lang="de") is False


def test_не_знаем_родной_язык_значит_не_знаем_ответа():
    """«Не знаю» НЕ равно «немецкий». Подставить сюда de значило бы вернуть ровно ту
    ошибку, ради которой модуль заведён."""
    assert _я("ru", "de", None) is None
    assert _я("ru", "de", "") is None
    assert это_немецкое(source_lang="ru", target_lang="de", native_lang=None) is False


def test_родной_язык_не_из_этой_пары_это_не_знаю():
    """Человек с родным испанским и карточкой ru→de: какая сторона его родная —
    неизвестно, гадать нельзя."""
    assert _я("ru", "de", "es") is None


def test_пара_противоречит_себе():
    assert _я("de", "de", "ru") is None


def test_регистр_и_пробелы_не_решают():
    assert _я(" RU ", "DE", "Ru") == "de"


def test_не_строка_не_становится_языком():
    assert _я(None, "de", "ru") is None
    assert _я(5, "de", "ru") is None


# ── вопрос поуже: без похода за профилем ─────────────────────────────────────────

from backend.lang_of_word import немецкое_наверняка


def _н(s, t):
    return немецкое_наверняка(source_lang=s, target_lang=t)


def test_пара_ровно_русский_немецкий_это_да():
    assert _н("ru", "de") is True
    assert _н("de", "ru") is True


def test_английские_пары_это_нет():
    assert _н("en", "ru") is False
    assert _н("ru", "en") is False


def test_пара_английский_немецкий_это_НЕТ_хотя_de_в_ней_есть():
    """⚠ Главный случай: у en→de немецкий есть в паре, но слово английское."""
    assert _н("en", "de") is False


def test_прочие_языки_это_нет():
    assert _н("ru", "it") is False
    assert _н(None, "de") is False
    assert _н("ru", None) is False
