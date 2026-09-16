# -*- coding: utf-8 -*-
"""ЛОЖНАЯ ПРЕТЕНЗИЯ НЕ ДОЛЖНА ЖДАТЬ, ПОКА ВЛАДЕЛЕЦ ЕЁ ВСПОМНИТ.

Владелец 16.09.2026: «если мы знаем, что их нельзя нажимать, — таких убери оттуда!
Ты что, надеешься на мою память?»

Разбор всех 46 открытых вопросов о переводе в тот день: 24 претензии справедливы,
12 ложны, 8 — придирки к стилю, 2 не проверены. У восьми ложных на экране стояла
кнопка «Записать: «…»», и четыре из них писали в общий словарь прямую неправду.

Тесты держат то, что иначе тихо сломается: у каждой снятой претензии должен остаться
источник, проверка текста обязана защищать от правки задним числом, а проход — быть
идемпотентным.
"""
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_у_каждой_снятой_претензии_есть_источник():
    """Снять претензию можно только словарём. «Мне так кажется» — не основание."""
    from backend.false_translation_claims import ПРОВЕРЕНО_ЛОЖНЫЕ

    assert len(ПРОВЕРЕНО_ЛОЖНЫЕ) == 8, "список сверки 16.09.2026"
    for запись in ПРОВЕРЕНО_ЛОЖНЫЕ:
        assert запись["de"].strip(), запись
        assert запись["ru"].strip(), запись
        источник = запись["source"]
        назван = ("dwds.de" in источник or "wiktionary" in источник
                  or "сверка самой записи" in источник)
        assert назван, f"у записи {запись['review_id']} источник не назван: {источник}"
        assert len(источник) > 40, "источник должен давать проверить, а не отсылать"


def test_идентификаторы_не_повторяются():
    """Дважды закрыть одну запись значит дважды поднять перевод в общий слой."""
    from backend.false_translation_claims import ПРОВЕРЕНО_ЛОЖНЫЕ

    ids = [з["review_id"] for з in ПРОВЕРЕНО_ЛОЖНЫЕ]
    assert len(ids) == len(set(ids))


def test_запись_изменившуюся_после_сверки_не_трогаем():
    """Сверка относилась к КОНКРЕТНОМУ тексту. Изменился — она к нему не относится.

    Без этой проверки ночной проход закрыл бы вопрос по чужому поводу: id тот же, а
    фразу за это время могли поправить на экране правки слов."""
    источник = open(os.path.join(BASE, "backend/false_translation_claims.py"),
                    encoding="utf-8").read()
    тело = источник[источник.index("def close_false_translation_claims"):]
    assert 'запись["de"].strip()' in тело and 'запись["ru"].strip()' in тело, (
        "перед закрытием обязана идти сверка обоих текстов")
    assert 'отчёт["разошлось"]' in тело, "расхождение обязано считаться, а не молчать"


def test_проход_идемпотентен_и_видит_только_свой_вид():
    """Закрытая запись не берётся повторно; чужие виды вопросов не трогаются."""
    источник = open(os.path.join(BASE, "backend/false_translation_claims.py"),
                    encoding="utf-8").read()
    тело = источник[источник.index("def close_false_translation_claims"):]
    assert "status = 'open'" in тело
    assert "COALESCE(kind, 'grammar') = 'translation'" in тело


def test_ночь_снимает_их_сама():
    """Владелец ничего не вызывает командой (19.08.2026)."""
    ночь = open(os.path.join(BASE, "bot_3.py"), encoding="utf-8").read()
    кусок = ночь[ночь.index("def _run_translation_links_safe"):]
    кусок = кусок[:кусок.index("\ndef ")]
    assert "close_false_translation_claims()" in кусок
    assert re.search(r'stats\["ложные претензии"\]', кусок), (
        "итог обязан попасть в heartbeat, иначе молчащий проход неотличим от сломанного")


def test_обещание_зарегистрировано():
    from backend.fix_promises import PROMISES

    обещание = next((p for p in PROMISES
                     if p.key == "false_translation_claims_are_off_the_screen"), None)
    assert обещание is not None
    assert обещание.expected == 0
    assert обещание.screen is not None


def test_непроверенное_не_попало_в_список():
    """⛔ 1152 («Bitte über den Teller essen») НЕ снимаем: подтверждающей статьи на
    оборот не нашлось. Закрыть вопрос по своему ощущению — ровно то, что запрещено."""
    from backend.false_translation_claims import ПРОВЕРЕНО_ЛОЖНЫЕ

    assert 1152 not in [з["review_id"] for з in ПРОВЕРЕНО_ЛОЖНЫЕ]
    источник = open(os.path.join(BASE, "backend/false_translation_claims.py"),
                    encoding="utf-8").read()
    assert "ОТКРЫТО" in источник, "незакрытое обязано быть помечено как незакрытое"
