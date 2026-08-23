# -*- coding: utf-8 -*-
"""Дверь слова: что чинится, что помечается, что не заводится в словарь.

Ни сети, ни модели, ни боевой базы — справочник и модель подменяются. Проверяется НАША
логика: порядок ступеней и вердикты. Каждый случай взят из живого дефекта 19.08.2026,
а не придуман.
"""
from __future__ import annotations

import pytest

import backend.german_word_gate as G


@pytest.fixture(autouse=True)
def _no_cache_no_network(monkeypatch):
    """Кеш и наши данные отключены: тест проверяет ступени, а не базу."""
    monkeypatch.setattr(G, "_cached", lambda asked: None)
    monkeypatch.setattr(G, "_remember", lambda asked, verdict: None)
    monkeypatch.setattr(G, "_known_by_our_data", lambda word: (False, "", ""))
    # Второй справочник по умолчанию ОТВЕТИЛ и слова не знает: тест не ходит в сеть,
    # а ступень «DWDS» не проглатывает случаи, ради которых написан каждый тест ниже.
    monkeypatch.setattr(G, "_second_reference_says", lambda words: {})


def _reference(pages: dict[str, list[str]]):
    """Подставной справочник: {написание: [части речи]}."""
    def _fake(words):
        return {name: pages[name] for name in words if name in pages}
    return _fake


def test_обрезок_не_заводится_в_словарь(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": False})
    verdict = G.check_word("Abschiebu")
    assert verdict["status"] == G.NOT_A_WORD


def test_потерянный_умлаут_чинится_по_справочнику(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Ärgernisse": ["Deklinierte Form"]}))
    verdict = G.check_word("Argernisse")
    assert verdict["text"] == "Ärgernisse"
    assert verdict["status"] == G.REPAIRED


def test_устаревшее_написание_ведёт_к_современному(monkeypatch):
    """У «verläßlich» страница ЕСТЬ, но она помечена как устаревшее написание.
    Взять её как ответ нельзя — человек выучит форму, которой больше нет."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Verläßlich": ["__устаревшее__verlässlich"]}))
    verdict = G.check_word("Verläßlich")
    assert verdict["text"] == "verlässlich"
    assert verdict["status"] == G.REPAIRED


def test_короткий_обрезок_тоже_проверяется(monkeypatch):
    """«Felg» — четыре буквы. Раньше порог отсекал такие от починки."""
    assert "Felge" in G.repair_candidates("Felg")


def test_существительное_получает_заглавную(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Betäubung": ["Substantiv"]}))
    verdict = G.check_word("betäubung")
    assert verdict["text"] == "Betäubung"
    assert verdict["pos"] == "noun"


def test_прилагательное_получает_строчную(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"grundlegend": ["Adjektiv"]}))
    verdict = G.check_word("Grundlegend")
    assert verdict["text"] == "grundlegend"
    assert verdict["pos"] == "adjective"


def test_подтверждённое_слово_не_трогается(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({"Haus": ["Substantiv"]}))
    verdict = G.check_word("Haus")
    assert verdict["text"] == "Haus"
    assert verdict["status"] == G.CONFIRMED


def test_английское_слово_сохраняется_с_пометкой_языка(monkeypatch):
    """Решение владельца 19.08.2026: не отклонять. Впереди английский, и дверь,
    отклоняющая по языку, пойдёт под снос."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "en",
                                   "wortart": "Substantiv", "korrekt": "Sweatpants"})
    verdict = G.check_word("Sweatpants")
    assert verdict["status"] != G.NOT_A_WORD
    assert "en" in verdict["source"]


def test_редкое_немецкое_слово_не_выбрасывается(monkeypatch):
    """Оба справочника молчат, модель говорит «слово есть» — не выбрасываем.

    Раньше здесь стояло «Arbeitsumfeld»: страницы в Wiktionary у него нет. Теперь
    его знает DWDS, и до модели дело не доходит вовсе — поэтому пример заменён на
    слово, которого не знает ни один из двух справочников."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "de",
                                   "wortart": "Substantiv", "korrekt": "Schwarzflieger"})
    verdict = G.check_word("Schwarzflieger")
    assert verdict["status"] == G.UNCONFIRMED
    assert verdict["text"] == "Schwarzflieger"


def test_второй_справочник_подтверждает_то_чего_нет_в_первом(monkeypatch):
    """«Vergleichbarkeit» — обычное слово, страницы в Wiktionary нет, DWDS знает.

    Замер 21.08.2026: из 12 слов, ушедших человеку на проверку, 8 были такими.
    Подтверждённое вторым справочником к человеку не попадает вовсе."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr(G, "_second_reference_says",
                        lambda words: {"Vergleichbarkeit": "Substantiv"})
    def _model_must_not_be_asked(word):
        raise AssertionError("модель спрошена после того, как ответил справочник")
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        _model_must_not_be_asked)
    verdict = G.check_word("Vergleichbarkeit")
    assert verdict["status"] == G.CONFIRMED
    assert verdict["pos"] == "noun"
    assert verdict["source"] == "DWDS"


def test_молчание_второго_справочника_не_запоминается(monkeypatch):
    """DWDS не ответил — это «не спросили», а не «слова нет»."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr(G, "_second_reference_says", lambda words: None)
    verdict = G.check_word("Nachtdämmerung")
    assert verdict["status"] == G.UNCONFIRMED
    assert not G._is_final(verdict, allow_network=True, allow_model=True)


def test_молчание_справочника_не_приговор(monkeypatch):
    """Справочник не ответил — это НЕ «слова нет». Приговор не запоминается."""
    monkeypatch.setattr(G, "_reference_says_about_all", lambda words: None)
    verdict = G.check_word("Haus")
    assert verdict["status"] == G.UNCONFIRMED
    assert "молчал" in verdict["source"]


def test_дверь_не_ходит_в_сеть_когда_запрещено(monkeypatch):
    def _boom(words):
        raise AssertionError("дверь пошла в справочник, хотя ей запретили")
    monkeypatch.setattr(G, "_reference_says_about_all", _boom)
    verdict = G.check_word("Haus", allow_network=False)
    assert verdict["status"] == G.UNCONFIRMED


def test_пустая_строка_не_слово():
    assert G.check_word("   ")["status"] == G.NOT_A_WORD


def test_дешёвый_вызов_не_затирает_сильный_вердикт():
    """Дефект 19.08.2026: дешёвая половина писала своё «не подтверждено» поверх
    «не слово», и запрет на заведение мусора переставал срабатывать.

    «Не подтверждено» без сети означает «мы не спрашивали», а не «мы проверили»."""
    assert G._is_final({"status": G.NOT_A_WORD, "source": "модель: такого слова нет"},
                       allow_network=True, allow_model=True) is True
    assert G._is_final({"status": G.UNCONFIRMED, "source": "не спрашивали справочник"},
                       allow_network=False, allow_model=False) is False
    assert G._is_final({"status": G.UNCONFIRMED, "source": "справочник молчал"},
                       allow_network=True, allow_model=True) is False
    assert G._is_final({"status": G.UNCONFIRMED, "source": "модель: слово есть, язык en"},
                       allow_network=True, allow_model=True) is True


def test_слабый_вердикт_в_кэше_пересматривается(monkeypatch):
    """Дефект 20.08.2026: «Grundlegend» лежал в кэше с «не спрашивали справочник»
    (запись сделана до запрета слабых вердиктов) и возвращался оттуда ВЕЧНО —
    справочник о нём больше не спрашивали никогда."""
    weak = {"text": "Grundlegend", "status": G.UNCONFIRMED, "pos": "",
            "source": "не спрашивали справочник", "note": ""}
    monkeypatch.setattr(G, "_cached", lambda asked: weak)
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"grundlegend": ["Adjektiv"]}))
    verdict = G.check_word("Grundlegend")
    assert verdict["text"] == "grundlegend", "слабый вердикт обязан пересматриваться"

    # А сильный из кэша берётся как был — второй раз не переспрашиваем.
    strong = {"text": "Abschiebu", "status": G.NOT_A_WORD, "pos": "",
              "source": "модель: такого слова нет", "note": ""}
    monkeypatch.setattr(G, "_cached", lambda asked: strong)
    assert G.check_word("Abschiebu")["status"] == G.NOT_A_WORD


def test_догадка_модели_не_применяется_молча(monkeypatch):
    """Владелец 20.08.2026: «чиним только подтверждённое справочником, остальное —
    в проверку». Модель может предложить написание, которого справочник не знает —
    подставлять его молча нельзя, решение принимает человек."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "de",
                                   "wortart": "Substantiv", "korrekt": "Scheinwerferglas"})
    verdict = G.check_word("Scheinwerfergla")
    assert verdict["text"] == "Scheinwerfergla", "написание не должно подменяться догадкой"
    assert verdict["status"] == G.UNCONFIRMED


def test_подтверждённая_справочником_починка_применяется(monkeypatch):
    """А вот это — факт, а не догадка: справочник знает исправленное написание."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Ärgernisse": ["Deklinierte Form"]}))
    assert G.check_word("Argernisse")["text"] == "Ärgernisse"


def test_модуль_двери_импортирует_всё_что_использует():
    """Дверь падала NameError, потому что `os` использовался, а импорта не было.

    Поймано 22.08.2026 прогоном по живой базе. Тесты этого не видели: у них
    `_second_reference_says` подменён, и до строки с `os.getenv` дело не доходило.
    Сохранение слова тоже не падало — оно зовёт дверь без сети и выходит раньше.
    А вот плашка при сохранении и ночной прогрев ходят В СЕТЬ, доходили до этой
    строки и получали NameError: плашка тихо переставала показываться, прогрев падал.

    Проверяем сам МОДУЛЬ, а не одну функцию: любое имя, которое он использует, должно
    быть в нём определено.
    """
    import ast
    import builtins
    import inspect

    import backend.german_word_gate as G

    дерево = ast.parse(inspect.getsource(G))
    определено = set(dir(G)) | set(dir(builtins))
    неизвестные = set()
    for узел in ast.walk(дерево):
        if isinstance(узел, ast.Name) and isinstance(узел.ctx, ast.Load):
            if узел.id not in определено:
                неизвестные.add(узел.id)
    # Локальные имена внутри функций сюда попадают тоже, поэтому проверяем только
    # модули, которые модуль обязан импортировать сам.
    обязательные = {"os", "re", "logging", "json", "time"} & неизвестные
    assert not обязательные, (
        f"дверь использует, но не импортирует: {sorted(обязательные)} — "
        "в проде это NameError на живом запросе")


# ── Заголовок оказался ФОРМОЙ слова ───────────────────────────────────────────
#
# Владелец 23.08.2026, разбирая глаголы без спряжения: «конечно нужно поменять часть речи
# по источнику в этих 30 словах, которые не глаголы». Оказалось, что у 21 из них беды не
# в подписи, а в самом заголовке: «abgezogen», «hätte», «zurückgetreten» — не слова, а
# формы, своей словарной статьи у них нет. Дверь читала «страница есть» как «слово
# настоящее» и оставляла форму заголовком навсегда.


def test_заголовок_форма_чинится_на_словарное_слово(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"abgezogen": ["__форма__verb|abziehen"]}))
    verdict = G.check_word("abgezogen")
    assert verdict["status"] == G.REPAIRED
    assert verdict["text"] == "abziehen"
    assert "формой слова" in verdict["source"]


def test_часть_речи_берётся_у_базового_слова(monkeypatch):
    """«umgeworfen» лежало прилагательным. После починки заголовка на «umwerfen» подпись
    обязана стать глаголом: причастие бывает только у глагола, и это печатный факт,
    а не наш вывод. Раньше подпись оставалась от прежнего заголовка."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"umgeworfen": ["__форма__verb|umwerfen"]}))
    verdict = G.check_word("umgeworfen", pos_hint="adjective")
    assert verdict["text"] == "umwerfen"
    assert verdict["pos"] == "verb"


def test_у_склонённой_формы_часть_речи_не_придумывается(monkeypatch):
    """«Deklinierte Form» бывает и у существительного, и у прилагательного — молчим."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"laueren": ["__форма__|lau"]}))
    verdict = G.check_word("laueren")
    assert verdict["text"] == "lau"
    assert verdict["pos"] == ""


def test_два_словарных_слова_у_формы_решает_человек(monkeypatch):
    """«rast» — это и «rasten» (отдыхать), и «rasen» (мчаться). Выбирать за человека
    нельзя: это не наша неполнота, а настоящая двусмысленность немецкого."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"rast": ["__форма_неясная__"]}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda word: None)
    verdict = G.check_word("rast")
    assert verdict["status"] == G.UNCONFIRMED
    assert verdict["text"] == "rast", "заголовок не тронут"


def test_настоящее_слово_с_разделом_формы_рядом_остаётся_словом(monkeypatch):
    """У «gefeiert» на странице И «Adjektiv», И «Partizip II». Своя статья есть —
    значит заголовок стоит в словаре по праву."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"gefeiert": ["Adjektiv", "Partizip II"]}))
    verdict = G.check_word("gefeiert")
    assert verdict["status"] == G.CONFIRMED
    assert verdict["pos"] == "adjective"


# ── Дешёвая проверка не перебивает напечатанную страницу ──────────────────────
#
# Справочник родов знает артикль — и этим объявляет слово существительным. Но заглавная
# буква в немецком несёт смысл: «heute» это наречие, «das Heute» — редкое
# существительное. Дешёвый путь стоял первым и переписывал заголовок с заглавной.


def test_строчное_слово_со_своей_страницей_не_становится_существительным(monkeypatch):
    monkeypatch.setattr(G, "_known_by_our_data",
                        lambda word: (True, "справочник родов", "noun"))
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"heute": ["Temporaladverb"]}))
    verdict = G.check_word("heute")
    assert verdict["text"] == "heute", "наречие превратили в существительное «Heute»"
    assert verdict["pos"] == "adverb"


def test_строчное_существительное_по_прежнему_получает_заглавную(monkeypatch):
    """Своей страницы у «brücke» нет — тогда справочник родов снова в силе."""
    monkeypatch.setattr(G, "_known_by_our_data",
                        lambda word: (True, "справочник родов", "noun"))
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    verdict = G.check_word("brücke")
    assert verdict["text"] == "Brücke"
    assert verdict["pos"] == "noun"


def test_без_сети_дешёвая_проверка_отвечает_как_раньше(monkeypatch):
    monkeypatch.setattr(G, "_known_by_our_data",
                        lambda word: (True, "справочник родов", "noun"))
    verdict = G.check_word("brücke", allow_network=False, allow_model=False)
    assert verdict["text"] == "Brücke"


def test_части_речи_берутся_из_общей_таблицы():
    """Своя копия таблицы знала шесть разделов из двадцати — «heute», «wehe»,
    «wohingegen» возвращались вообще без части речи."""
    from backend.german_form_headword import POS_BY_WORTART
    assert G._POS_BY_WORTART is POS_BY_WORTART
    assert POS_BY_WORTART["Temporaladverb"] == "adverb"
    assert POS_BY_WORTART["Interjektion"] == "interjection"
    assert POS_BY_WORTART["Subjunktion"] == "conjunction"
