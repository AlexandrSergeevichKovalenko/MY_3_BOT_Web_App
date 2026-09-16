# -*- coding: utf-8 -*-
"""СУДЬЯ ПЕРЕВОДА СУДИТ СО СЛОВАРЁМ, А НЕ ПО ПАМЯТИ МОДЕЛИ.

Разбор ВСЕХ 46 открытых вопросов о переводе 16.09.2026, каждая спорная претензия
проверена по словарю: 24 справедливы, 12 ложны, 8 — придирки к стилю, 2 не проверены.
Из 12 ложных шесть лечились словарной статьёй, и лечение проверено живым прогоном
в тот же день (gpt-4.1-mini, температура 0):

    ложные претензии сняты:   5 из 5   (Laster, Police, kündigen, festsetzen, auslösen)
    настоящие ошибки пойманы: 5 из 5   (Rückgrat, Kopfnuss, zappeln, verzichten, verrufen)

⚠ ПО ДОРОГЕ БЫЛ РЕГРЕСС, И ИМЕННО ОН ЗДЕСЬ ЗАКРЕПЛЁН. Формулировка «словарь важнее
твоего знания» в первой редакции заставила судью ОПРАВДАТЬ дословный перевод идиомы:
«Rückgrat haben» → «иметь позвоночник» он без словаря браковал дважды из двух, а со
словарём пропустил — анатомическое значение в статье есть. Границу пришлось назвать
прямо: словарь решает, КАКИЕ значения существуют, а не годится ли русский в этой фразе.
"""
import os

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ПРОМПТ = os.path.join(BASE, "backend/openai_manager.py")


def _промпт_судьи() -> str:
    s = open(ПРОМПТ, encoding="utf-8").read()
    i = s.index("def run_translation_pair_check")
    return s[i:s.index("\ndef ", i + 10)]


def test_словарь_описан_как_старший_над_памятью_модели():
    """Без этого судья спорит со статьёй, которую ему показали (проверено 16.09.2026:
    «Police по-немецки — полиция» при статье Versicherungsvertrag перед глазами)."""
    промпт = _промпт_судьи()
    assert "`dictionary`" in промпт
    assert "OVERRIDES YOUR OWN" in промпт
    assert "NEVER write a `why` that denies a sense listed in `dictionary`" in промпт


def test_граница_словаря_названа():
    """⛔ РЕГРЕСС 16.09.2026. Словарь НЕ оправдывает дословный перевод идиомы."""
    промпт = _промпт_судьи()
    assert "settles WHICH senses exist" in промпт
    assert "idiom word by word" in промпт
    assert "Rückgrat" in промпт, "живой пример регресса обязан остаться в правиле"


def test_статья_едет_первым_полем():
    """Словарь должен быть прочитан до приговора, а не после него."""
    промпт = _промпт_судьи()
    i = промпт.index('"dictionary"')
    j = промпт.index('"german": de, "russian": ru', i - 400 if i > 400 else 0)
    assert i < j, "поле dictionary обязано стоять перед german/russian в запросе"


def test_разметка_вычищается_а_пометы_остаются():
    """«ugs.» и «veraltet» — это сигнал «значение редкое», а не «несуществующее».
    Потеряем пометы — судья снова начнёт браковать редкие, но верные значения."""
    from backend.judge_dictionary_context import _очистить

    вышло = _очистить(":[1] {{K|ugs.}} [[Lastkraftwagen]]<ref>что-то</ref>\n"
                      ":[2] ''übertragen'' eine [[schlecht|schlechte]] Angewohnheit")
    assert "Lastkraftwagen" in вышло and "ugs." in вышло
    assert "[[" not in вышло and "{{" not in вышло and "<ref>" not in вышло
    assert "schlechte" in вышло, "подпись ссылки, а не её цель"


def test_во_фразе_глагол_называет_справочник_а_не_мы():
    """Лемму мы не вычисляем. Её называет справочник форм — иначе это догадка."""
    источник = open(os.path.join(BASE, "backend/judge_dictionary_context.py"),
                    encoding="utf-8").read()
    тело = источник[источник.index("def слова_для_справки"):]
    тело = тело[:тело.index("\ndef ")]
    assert "verbs_of_form" in тело
    assert "СЛОВ_НА_ФРАЗУ" in тело, "длина справки обязана быть ограничена: мы платим за токены"


def test_молчание_сети_не_кешируется_как_ответ_словаря():
    """«Не спросили» и «словарь такого не знает» — разные вещи. Смешаем — слово
    навсегда останется без справки из-за одной сетевой ошибки."""
    источник = open(os.path.join(BASE, "backend/judge_dictionary_context.py"),
                    encoding="utf-8").read()
    тело = источник[источник.index("def значения_слова"):]
    assert "if слово not in (тексты or {}):" in тело
    assert "continue" in тело
    assert "ТРИ РАЗНЫХ ИСХОДА" in тело


def test_вопрос_владельцу_несёт_след_справки():
    """Молчащий механизм неотличим от сломанного: в вопросе видно, о чём спрашивали."""
    источник = open(os.path.join(BASE, "backend/translation_links.py"),
                    encoding="utf-8").read()
    assert '"reference": {"lemmas"' in источник
    assert источник.count("справка_для_судьи") >= 2, (
        "справку получают ОБА пути: и ночной подъём, и доспрос")
