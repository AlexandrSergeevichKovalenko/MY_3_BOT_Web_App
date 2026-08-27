# -*- coding: utf-8 -*-
"""Формы из справочника: разбор таблицы, композит, согласие двух ответов модели.

Ни сети, ни боевой базы. Разметка зафиксирована здесь же — ровно та, что отдаёт
de.wiktionary (снята запросами 18.08.2026). Если Wiktionary поменяет вёрстку, тест
останется зелёным, а прод сломается — поэтому живое покрытие меряется отдельно,
скриптом прогрева, а здесь проверяется НАША логика.
"""
from __future__ import annotations

import backend.german_reference_forms as R

# ── Разметка, снятая с живых страниц ─────────────────────────────────────────
STUDENT_HTML = """
<h2><span></span>Student (<a href="/wiki/Wiktionary:Deutsch">Deutsch</a>)</h2>
<h3>Substantiv, m</h3>
<table class="wikitable float-right inflection-table flexbox"><tbody>
<tr><th> </th><th>Singular</th><th>Plural</th></tr>
<tr><th>Nominativ</th><td>der Student </td><td>die <a>Studenten</a> </td></tr>
<tr><th>Genitiv</th><td>des <a>Studenten</a> </td><td>der <a>Studenten</a> </td></tr>
<tr><th>Dativ</th><td>dem <a>Studenten</a> </td><td>den <a>Studenten</a> </td></tr>
<tr><th>Akkusativ</th><td>den <a>Studenten</a> </td><td>die <a>Studenten</a> </td></tr>
</tbody></table>
<p>[1] ein Student im Hörsaal</p>
"""

ELTERN_HTML = """
<h2>Eltern (<a>Deutsch</a>)</h2>
<h3>Substantiv</h3>
<table class="wikitable inflection-table"><tbody>
<tr><th> </th><th>Plural</th></tr>
<tr><th>Nominativ</th><td>die Eltern</td></tr>
<tr><th>Genitiv</th><td>der Eltern</td></tr>
<tr><th>Dativ</th><td>den Eltern</td></tr>
<tr><th>Akkusativ</th><td>die Eltern</td></tr>
</tbody></table>
"""

# У «alt» на странице есть ещё итальянский и каталанский разделы со СВОИМИ таблицами.
ALT_HTML = """
<h2>alt (<a>Deutsch</a>)</h2>
<h3>Adjektiv</h3>
<table class="wikitable inflection-table float-right"><tbody>
<tr><th>Positiv</th><th>Komparativ</th><th>Superlativ</th></tr>
<tr><td>alt</td><td>älter</td><td>am ältesten</td></tr>
</tbody></table>
<h2>alt (<a>Italienisch</a>)</h2>
<h3>Adjektiv</h3>
<table class="wikitable inflection-table"><tbody>
<tr><th>Positiv</th><th>Komparativ</th><th>Superlativ</th></tr>
<tr><td>alt</td><td>ITALIENISCH</td><td>am ITALIENISCHSTEN</td></tr>
</tbody></table>
"""

HAUS_HTML = """
<h2>Haus (<a>Deutsch</a>)</h2>
<h3>Substantiv, n</h3>
<table class="wikitable inflection-table"><tbody>
<tr><th> </th><th>Singular</th><th>Plural</th></tr>
<tr><th>Nominativ</th><td>das Haus</td><td>die Häuser</td></tr>
<tr><th>Genitiv</th><td>des Hauses</td><td>der Häuser</td></tr>
<tr><th>Dativ</th><td>dem Haus<br />dem Hause</td><td>den Häusern</td></tr>
<tr><th>Akkusativ</th><td>das Haus</td><td>die Häuser</td></tr>
</tbody></table>
"""


def _rows(tables, gender):
    return {r["case"]: (r["singular"], r["plural"]) for r in tables[gender]["rows"]}


def test_слабое_склонение_берётся_целиком_из_таблицы():
    """Ровно тот дефект, ради которого модуль написан: раньше печаталось «den Student»."""
    tables = R._declension_from_html(R._german_section(STUDENT_HTML))
    rows = _rows(tables, "m")
    assert rows["nom"][0] == "der Student"
    assert rows["akk"][0] == "den Studenten"
    assert rows["dat"][0] == "dem Studenten"
    assert rows["gen"][0] == "des Studenten"


def test_подписи_к_картинкам_в_таблицу_не_попадают():
    tables = R._declension_from_html(R._german_section(STUDENT_HTML))
    for row in tables["m"]["rows"]:
        assert "Hörsaal" not in row["singular"] and "Hörsaal" not in row["plural"]


def test_у_слова_без_единственного_числа_колонка_пустая_а_не_выдуманная():
    tables = R._declension_from_html(R._german_section(ELTERN_HTML))
    rows = _rows(tables, "pl")
    assert rows["nom"][1] == "die Eltern"
    assert rows["nom"][0] == ""
    assert tables["pl"]["has_singular"] is False


def test_из_нескольких_вариантов_берётся_первый_напечатанный():
    tables = R._declension_from_html(R._german_section(HAUS_HTML))
    assert _rows(tables, "n")["dat"][0] == "dem Haus"


def test_читается_только_немецкий_раздел():
    """На странице «alt» есть итальянский раздел со своей таблицей степеней."""
    degrees = R._degrees_from_html(R._german_section(ALT_HTML))
    assert degrees == {"positive": "alt", "comparative": "älter",
                       "superlative": "am ältesten"}


def test_чужой_язык_не_подхватывается_если_немецкого_раздела_нет():
    only_italian = ALT_HTML[ALT_HTML.index("<h2>alt (<a>Italienisch</a>)"):]
    assert R._german_section(only_italian) == ""


def test_противоречие_артикля_и_рода_отбраковывает_чужую_таблицу():
    """Прогон 18.08.2026: в раздел «Substantiv, f» слова «Junge» попадала таблица
    с «der Junge». Артикль не совпал с родом — таблица не принимается."""
    table = {"rows": [{"case": "nom", "label": "Nominativ",
                       "singular": "der Junge", "plural": "die Jungen"}]}
    assert R._agrees_with_gender(table, "m") is True
    assert R._agrees_with_gender(table, "f") is False
    assert R._agrees_with_gender(table, "pl") is True


def test_составное_слово_склоняется_по_голове():
    assert R._compose("Haustürschlüssel", "schlüssel", "des Schlüssels") == "des Haustürschlüssels"
    assert R._compose("Krankenwagen", "wagen", "dem Wagen") == "dem Krankenwagen"


def test_после_дефиса_голова_остаётся_с_заглавной():
    """Ошибка 18.08.2026: выходило «des Pro-Kopf-einkommens»."""
    assert R._compose("Pro-Kopf-Einkommen", "einkommen",
                      "des Einkommens") == "des Pro-Kopf-Einkommens"


def test_модель_принимается_только_при_совпадении_двух_ответов():
    a = {"positive": "alt", "comparative": "älter", "superlative": "am ältesten"}
    b = {"positive": "alt", "comparative": "älter", "superlative": "am ältesten"}
    c = {"positive": "alt", "comparative": "alter", "superlative": "am altesten"}
    assert R._agreed(a, b, R._DEG_KEYS) == a
    assert R._agreed(a, c, R._DEG_KEYS) == {}
    assert R._agreed(a, {}, R._DEG_KEYS) == {}


def test_регистр_и_лишние_пробелы_совпадению_не_мешают():
    a = {"positive": "alt", "comparative": "Älter", "superlative": "am  ältesten"}
    b = {"positive": "alt", "comparative": "älter", "superlative": "am ältesten"}
    assert R._agreed(a, b, R._DEG_KEYS)["comparative"] == "Älter"


def test_ни_одного_окончания_модуль_не_дописывает():
    """Защита от возврата к арифметике: в коде не должно быть склейки основы с окончанием."""
    import inspect
    source = inspect.getsource(R)
    for invented in ('+ "en"', '+ "es"', '+ "er"', '+ "sten"', '+ "esten"'):
        assert invented not in source, f"в модуле появилось дописывание окончания: {invented}"


# ── Быстрый путь: формы из ИСХОДНИКА страницы, пачкой по 50 слов ─────────────
# Каждый тест ниже закрывает ошибку, пойманную сверкой исходника с разметкой 18.08.2026.

MANN_SOURCE = """
{{Deutsch Substantiv Übersicht
|Genus=m
|Nominativ Singular=Mann
|Nominativ Plural 1=Männer
|Nominativ Plural 2=Mann
|Genitiv Singular=Mannes
|Genitiv Plural 1=Männer
|Dativ Singular=Mann
|Dativ Plural 1=Männern
|Akkusativ Singular=Mann
|Akkusativ Plural 1=Männer
}}
"""

HERZ_SOURCE = """
{{Deutsch Substantiv Übersicht
|Genus=n
|Nominativ Singular=Herz
|Nominativ Plural=Herzen
|Genitiv Singular=Herzens
|Genitiv Plural=Herzen
|Dativ Singular=Herzen
|Dativ Plural=Herzen
|Akkusativ Singular=Herz
|Akkusativ Plural=Herzen
}}
{{Deutsch Substantiv Übersicht
|Nominativ Plural=Herzen
}}
"""

ELTERN_SOURCE = """
{{Deutsch Substantiv Übersicht
|Genus=0
|Nominativ Singular=—
|Nominativ Plural=Eltern
|Genitiv Singular=—
|Genitiv Plural=Eltern
|Dativ Singular=—
|Dativ Plural=Eltern
|Akkusativ Singular=—
|Akkusativ Plural=Eltern
}}
"""

ALT_SOURCE = "{{Deutsch Adjektiv Übersicht\n|Positiv=alt\n|Komparativ=älter\n|Superlativ=ältesten\n}}"


def test_множественное_под_номером_варианта_не_теряется():
    """Ошибка 18.08.2026: у Mann, Land, Wagen, Möbel, Junge множественное было записано
    как «Nominativ Plural 1», код искал «Nominativ Plural» и терял всю колонку."""
    tables = R.declension_from_source(MANN_SOURCE)
    rows = {r["case"]: (r["singular"], r["plural"]) for r in tables["m"]["rows"]}
    assert rows["nom"] == ("der Mann", "die Männer")
    assert rows["dat"] == ("dem Mann", "den Männern")
    assert rows["gen"][0] == "des Mannes"


def test_блок_без_рода_не_добавляет_слову_чужую_таблицу():
    """Ошибка 18.08.2026: второй блок без «Genus» ложился в «pl», и у Herz появлялось
    несуществующее отдельное множественное число."""
    tables = R.declension_from_source(HERZ_SOURCE)
    assert set(tables) == {"n"}
    assert tables["n"]["rows"][0] == {"case": "nom", "label": "Nominativ",
                                      "singular": "das Herz", "plural": "die Herzen"}


def test_pluralia_tantum_из_исходника():
    tables = R.declension_from_source(ELTERN_SOURCE)
    assert set(tables) == {"pl"}
    assert tables["pl"]["has_singular"] is False
    assert tables["pl"]["rows"][2]["plural"] == "den Eltern"


def test_степени_из_исходника_получают_am():
    """В исходнике превосходная записана без «am» — справочник дописывает его при показе."""
    assert R.degrees_from_source(ALT_SOURCE) == {
        "positive": "alt", "comparative": "älter", "superlative": "am ältesten",
        "gradable": True}


NICHT_STEIGERBAR_SOURCE = ("{{Deutsch Adjektiv Übersicht|Positiv=absichtlich"
                           "|Komparativ=—|Superlativ=—}}")


def test_несравнимое_слово_это_ответ_а_не_отсутствие_страницы():
    """Ошибка 18.08.2026: «absichtlich» справочник ЗНАЕТ и говорит «степеней нет»
    (Komparativ=—). Это складывалось в «страницы нет», и слово выглядело непокрытым —
    217 из 300 в первом прогоне оказались как раз такими."""
    degrees = R.degrees_from_source(NICHT_STEIGERBAR_SOURCE)
    assert degrees["gradable"] is False
    assert degrees["positive"] == "absichtlich"
    assert degrees["comparative"] == ""


def test_однострочный_шаблон_разбирается():
    """Ошибка 18.08.2026: параметры резались по переносам строк, и однострочные шаблоны
    («arrogant», «bestechlich») давали ноль параметров — ложное «страницы нет»."""
    one_line = ("{{Deutsch Adjektiv Übersicht|Positiv=arrogant|Komparativ=arroganter"
                "|Superlativ=arrogantesten}}")
    assert R.degrees_from_source(one_line)["comparative"] == "arroganter"


def test_подпись_источника_не_затирается(monkeypatch):
    """Ошибка 18.08.2026: выдача штамповала «wiktionary» на таблицу, положенную моделью."""
    monkeypatch.setattr(R, "load_adjective_degrees",
                        lambda word: {"positive": "x", "comparative": "y",
                                      "superlative": "am z", "source": "модель"})
    assert R.adjective_degrees_for("x")["source"] == "модель"
    monkeypatch.setattr(R, "load_adjective_degrees",
                        lambda word: {"positive": "x", "comparative": "y", "superlative": "am z"})
    assert R.adjective_degrees_for("x")["source"] == "wiktionary-steigerung"


# ── Система на будущее: новое слово не должно повторять историю ──────────────
# Владелец 19.08.2026: «когда я буду добавлять слова, которые справочник знает, но форм
# не даёт, они опять будут попадать как непокрытые?» Эти тесты отвечают «нет».

ADVERB_SOURCE = """
== allerdings ({{Sprache|Deutsch}}) ==
=== {{Wortart|Adverb|Deutsch}} ===
{{Bedeutungen}}
:[1] тем не менее
"""

VERB_SOURCE = """
== ausstatten ({{Sprache|Deutsch}}) ==
=== {{Wortart|Verb|Deutsch}} ===
"""

ADJ_NO_TABLE_SOURCE = """
== facile ({{Sprache|Deutsch}}) ==
=== {{Wortart|Adjektiv|Deutsch}} ===
"""


def test_наречие_без_степеней_это_ответ_а_не_пустота():
    """«allerdings» — наречие, степеней у него не бывает. Это знание, а не пробел."""
    degrees = R.degrees_from_source(ADVERB_SOURCE)
    assert degrees["gradable"] is False
    assert degrees["positive"] == "allerdings"


def test_прилагательное_без_таблицы_остаётся_вопросом():
    """У прилагательного таблица быть обязана — её отсутствие может быть пробелом
    справочника, и молча звать слово несравнимым нельзя. Поймано на «facile»."""
    assert R.degrees_from_source(ADJ_NO_TABLE_SOURCE) == {}


def test_чужая_часть_речи_идёт_в_дефекты_заголовка_а_не_владельцу():
    """«ausstatten» лежит у нас как прилагательное, а это глагол. Спрашивать
    у человека «какой артикль у глагола» — впустую тратить его время."""
    where, why = R.classify_uncovered("ausstatten", "adjective", VERB_SOURCE)
    assert where == "заголовок"
    assert "Verb" in why


def test_страницы_нет_это_тоже_дефект_заголовка():
    where, why = R.classify_uncovered("Bierhausschwätzer", "noun", "")
    assert where == "заголовок"
    assert "страницы" in why


def test_настоящее_слово_остаётся_вопросом_владельцу():
    where, _why = R.classify_uncovered("Gleichgesinnte", "noun",
                                       "== Gleichgesinnte ({{Sprache|Deutsch}}) ==\n"
                                       "=== {{Wortart|Substantiv|Deutsch}} ===")
    assert where == "владельцу"


def test_ночная_работа_идёт_тем_же_путём_что_разовая_чистка():
    """Защита от расхождения: ночная работа обязана звать тот же разбор остатка."""
    import inspect
    source = inspect.getsource(R.warm_nightly)
    assert "warm_from_source_bulk" in source, "ночью обязан работать быстрый путь по исходнику"
    assert "triage_unresolved" in source, "ночью обязан работать разбор остатка"


# ── Субстантивация: ответ был в справочнике, а уходил владельцу ──────────────
# Исходник снят запросом к de.wiktionary 27.08.2026 (страница «Gehen»), обрезан до
# нужного. Строчная страница «gehen» — это ГЛАГОЛ, таблицы существительного там нет.
GEHEN_SOURCE = """
== Gehen ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}}, {{n}} ===
{{Deutsch Substantiv Übersicht
|Genus=n
|Nominativ Singular=Gehen
|Nominativ Plural=—
|Genitiv Singular=Gehens
|Genitiv Plural=—
|Dativ Singular=Gehen
|Dativ Plural=—
|Akkusativ Singular=Gehen
|Akkusativ Plural=—
|Bild=Osaka07 D8M Alex Schwazer walking.jpg|mini|2|''Gehen'' bei den Championships
}}
"""


def test_существительное_спрашивается_у_справочника_с_заглавной():
    """По адресу «gehen» напечатан глагол. Существительное лежит только под «Gehen»."""
    assert R._reference_title("gehen", "noun") == "Gehen"
    assert R._reference_title("Gehen", "noun") == "Gehen"
    # Прилагательное и наречие — наоборот, со строчной. Это правило не трогаем.
    assert R._reference_title("Grundlegend", "adjective") == "grundlegend"


def test_субстантивированный_инфинитив_читается_из_исходника():
    """«das Gehen» — настоящее существительное с полной таблицей и без множественного."""
    tables = R.declension_from_source(GEHEN_SOURCE)
    assert list(tables) == ["n"]
    rows = {r["case"]: r for r in tables["n"]["rows"]}
    assert rows["nom"]["singular"] == "das Gehen"
    assert rows["gen"]["singular"] == "des Gehens"
    assert tables["n"]["has_plural"] is False


def test_скачанная_страница_закрывает_слово_а_не_идёт_к_владельцу(monkeypatch):
    """Разбор остатка обязан СНАЧАЛА прочитать страницу. Ответ был у нас в руках."""
    записано, снято = {}, []
    monkeypatch.setattr(R, "store_noun_declension",
                        lambda word, tables: записано.update({word: tables}))
    monkeypatch.setattr(R, "clear_unresolved", lambda word: снято.append(word))
    assert R.close_from_source("Gehen", "noun", GEHEN_SOURCE) is True
    assert записано["Gehen"]["n"]["rows"][1]["singular"] == "des Gehens"
    assert снято == ["Gehen"]


def test_пустая_страница_слово_не_закрывает():
    """Нечего разбирать — значит вопрос остаётся вопросом, а не «закрыт»."""
    assert R.close_from_source("Bierhausschwätzer", "noun", "") is False


def test_разбор_остатка_читает_страницу_прежде_чем_звать_человека():
    """Защита от возврата дефекта: классификация не должна идти раньше разбора."""
    import inspect
    source = inspect.getsource(R.triage_unresolved)
    assert "close_from_source" in source, "остаток обязан сперва попытаться закрыть слово"
    assert source.index("close_from_source") < source.index("classify_uncovered"), \
        "сначала читаем формы, и только потом решаем, звать ли владельца"


def test_отказ_справочника_перепроверяется_а_не_живёт_вечно():
    """«Страницы нет» — незакрытая задача. Ночная работа обязана к ней возвращаться."""
    import inspect
    assert "recheck_negatives" in inspect.getsource(R.warm_nightly)


def test_подтип_наречия_это_наречие_а_не_чужая_часть_речи():
    """«heute» помечено «Temporaladverb», «davor» — «Lokaladverb». Это наречия."""
    assert R._pos_matches("adjective", {"Temporaladverb"}) is True
    assert R._pos_matches("adverb", {"Konjunktionaladverb", "Modaladverb"}) is True
    assert R._pos_matches("noun", {"Substantiv"}) is True


def test_чужая_часть_речи_словом_не_закрывается():
    """«ausstatten» лежит у нас прилагательным, а это глагол. Закрыть его как
    «несравнимое прилагательное» значит спрятать дефект нашего заголовка."""
    assert R._pos_matches("adjective", {"Verb"}) is False
    assert R._pos_matches("adjective", {"Partizip II"}) is False
    assert R._pos_matches("noun", {"Deklinierte Form"}) is False
    assert R.forms_from_source("adjective", VERB_SOURCE) is None


# Страница «Finster», снята запросом 27.08.2026. Помечена И существительным, И
# фамилией, а таблица на ней ОДНА — фамилии. Именно её владелец увидел на экране.
FINSTER_SOURCE = """
== Finster ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}}, {{mf}}, {{Wortart|Nachname|Deutsch}} ===
{{Deutsch Nachname Übersicht
|Nominativ Singular m=Finster
|Genitiv Singular m=Finsters
}}
"""


def test_причина_называется_точно_а_не_одной_фразой_на_всё():
    """Владелец открыл страницу «Finster», увидел там таблицу и назвал общую фразу
    «справочник не печатает» обманом. Таблица там есть — но это склонение ФАМИЛИИ,
    и пометка части речи её не выдаёт: страница помечена и существительным тоже."""
    код, фраза = R.diagnose_source("noun", FINSTER_SOURCE)
    assert код == "только_фамилия" and "ФАМИЛИИ" in фраза
    assert R.forms_from_source("noun", FINSTER_SOURCE) is None, "фамилию в формы не берём"
    assert R.diagnose_source("noun", "")[0] == "нет_страницы"
    assert R.diagnose_source("adjective", "{{Wortart|Verb|Deutsch}}")[0] == "другая_часть_речи"
    assert R.diagnose_source("noun", "{{Wortart|Substantiv|Deutsch}}")[0] == "нет_таблицы"


def test_молчание_справочника_не_выдаётся_за_причину():
    """None — «не спросили/не ответил». Это НЕ «страницы нет»: разные миры."""
    код, _фраза = R.diagnose_source("noun", None)
    assert код == "молчит"


def test_переспрос_отказов_раз_в_месяц_а_не_каждую_неделю():
    """Решение владельца 27.08.2026: «один раз в месяц запрашивать эту статью»."""
    assert R._RECHECK_NEGATIVE_AFTER_DAYS == 30
