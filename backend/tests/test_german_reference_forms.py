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
