# -*- coding: utf-8 -*-
"""Разбор страницы Flexion: подпись — не форма, прочерк — не форма, блок не перелезает.

Дефект принёс соседний агент 23.08.2026 и он подтвердился замером: из 1467 таблиц
справочника у 21 вместо формы стоял служебный текст, у шести сломан именно Präteritum
(bedienen, keilen, stammen, verpönen, zähmen, zeigen). Все 21 были помечены
documented — то есть система считала их подтверждёнными источником, и снаружи это
отличить было нечем. Человек, открывший «zeigen», видел в прошедшем времени слово
«veraltet:».

Ячейки ниже сняты С ЖИВОЙ страницы de.wiktionary 23.08.2026 и урезаны до значимых:
проверяется наш разбор, а не сеть.
"""
from backend.german_verb_paradigms import _column_forms

# Flexion:zeigen, блок Präteritum. Слева направо: Aktiv-Indikativ, Aktiv-Konjunktiv II,
# затем два пассива. У каждой формы есть УСТАРЕВШИЙ вариант под подписью «veraltet:».
ZEIGEN = [
    "Präteritum", "Aktiv", "Vorgangspassiv", "Zustandspassiv",
    "Person", "Indikativ", "Konjunktiv II", "Indikativ", "Konjunktiv II",
    "Indikativ", "Konjunktiv II",
    "1. Person Singular",
    "ich zeigte,", "veraltet:", "ich zeigete",
    "ich zeigte,", "veraltet:", "ich zeigete",
    "ich wurde gezeigt", "ich würde gezeigt", "ich war gezeigt", "ich wäre gezeigt",
    "2. Person Singular",
    "du zeigtest,", "veraltet:", "du zeigetest",
    "du zeigtest,", "veraltet:", "du zeigetest",
    "du wurdest gezeigt", "du würdest gezeigt", "du warst gezeigt", "du wärest gezeigt",
    "3. Person Singular", "er/sie/es",
    "zeigte,", "veraltet:", "zeigete",
    "zeigte,", "veraltet:", "zeigete",
    "wurde gezeigt", "würde gezeigt", "war gezeigt", "wäre gezeigt",
    "1. Person Plural",
    "wir zeigten,", "veraltet:", "wir zeigeten",
    "wir zeigten,", "veraltet:", "wir zeigeten",
    "wir wurden gezeigt", "wir würden gezeigt", "wir waren gezeigt", "wir wären gezeigt",
    "2. Person Plural",
    "ihr zeigtet,", "veraltet:", "ihr zeigetet",
    "ihr zeigtet,", "veraltet:", "ihr zeigetet",
    "ihr wurdet gezeigt", "ihr würdet gezeigt", "ihr wart gezeigt", "ihr wäret gezeigt",
    "3. Person Plural",
    "sie zeigten,", "veraltet:", "sie zeigeten",
    "sie zeigten,", "veraltet:", "sie zeigeten",
    "sie wurden gezeigt", "sie würden gezeigt", "sie waren gezeigt", "sie wären gezeigt",
    # Дальше на странице идёт СЛЕДУЮЩИЙ блок. Разбор Präteritum обязан здесь встать.
    "Perfekt", "Aktiv", "Vorgangspassiv", "Zustandspassiv",
    "Person", "Indikativ", "Konjunktiv I", "Indikativ", "Konjunktiv I",
    "1. Person Singular", "ich habe gezeigt", "ich habe gezeigt",
    "2. Person Singular", "du hast gezeigt", "du habest gezeigt",
    "3. Person Singular", "er/sie/es", "hat gezeigt", "habe gezeigt",
    "1. Person Plural", "wir haben gezeigt", "wir haben gezeigt",
    "2. Person Plural", "ihr habt gezeigt", "ihr habet gezeigt",
    "3. Person Plural", "sie haben gezeigt", "sie haben gezeigt",
]

# Flexion:besagen, блок Präsens: у безличного глагола личных форм НЕТ, и страница
# честно печатает прочерки во всех клетках.
BESAGEN = [
    "Präsens", "Aktiv", "Vorgangspassiv", "Zustandspassiv",
    "Person", "Indikativ", "Konjunktiv I", "Indikativ", "Konjunktiv I",
    "Indikativ", "Konjunktiv I",
    "1. Person Singular", "—", "—", "—", "—", "—", "—",
    "2. Person Singular", "—", "—", "—", "—", "—", "—",
    "3. Person Singular", "er/sie/es", "besagt", "besage", "—", "—", "—", "—",
    "1. Person Plural", "—", "—", "—", "—", "—", "—",
    "2. Person Plural", "—", "—", "—", "—", "—", "—",
    "3. Person Plural", "sie besagen", "sie besagen", "—", "—", "—", "—",
]

# Flexion:halten — обычная страница без подписей. Она обязана разбираться как прежде.
HALTEN = [
    "Präteritum", "Aktiv", "Vorgangspassiv", "Zustandspassiv",
    "Person", "Indikativ", "Konjunktiv II", "Indikativ", "Konjunktiv II",
    "1. Person Singular", "ich hielt", "ich hielte",
    "2. Person Singular", "du hieltest,", "du hieltst", "du hieltest",
    "3. Person Singular", "er/sie/es", "hielt", "hielte",
    "1. Person Plural", "wir hielten", "wir hielten",
    "2. Person Plural", "ihr hieltet", "ihr hieltet",
    "3. Person Plural", "sie hielten", "sie hielten",
]


class TestПодписьЭтоНеФорма:
    def test_прошедшее_время_берётся_современное(self):
        forms = _column_forms(ZEIGEN, 0, column=0)
        assert forms == {"ich": "zeigte", "du": "zeigtest", "er/sie/es": "zeigte",
                         "wir": "zeigten", "ihr": "zeigtet", "sie/Sie": "zeigten"}

    def test_устаревший_вариант_не_побеждает_по_длине(self):
        """«zeigete» длиннее «zeigte», а правило берёт самый длинный вариант — оно
        спасает «ich dämme ein» от разговорного «ich dämm ein». Подпись «veraltet:»
        обязана вывести помеченный ею вариант из соревнования."""
        forms = _column_forms(ZEIGEN, 0, column=0)
        assert "zeigete" not in forms.values()

    def test_конъюнктив_не_съезжает_на_соседний_столбец(self):
        """Подпись стоит между столбцами, и если её просто пропустить, столбцы
        сдвинутся: конъюнктив получит форму пассива."""
        forms = _column_forms(ZEIGEN, 0, column=1)
        assert forms["ich"] == "zeigte"
        assert forms["er/sie/es"] == "zeigte"


class TestБлокНеПерелезаетВСоседний:
    def test_разбор_останавливается_на_следующем_заголовке(self):
        """Когда свой столбец пуст, сканирование доходило до блока Perfekt и заполняло
        конъюнктив формами «habe gezeigt». Проверяем на пустом столбце №3."""
        forms = _column_forms(ZEIGEN, 0, column=3)
        assert not any("gezeigt" in v and v.startswith("hab") for v in forms.values()), \
            f"формы из блока Perfekt утекли в чужой столбец: {forms}"

    def test_у_соседнего_блока_свой_разбор(self):
        start = ZEIGEN.index("Perfekt")
        forms = _column_forms(ZEIGEN, start, column=0)
        assert forms["er/sie/es"] == "hat gezeigt"


class TestПрочеркЭтоОтветАНеМусор:
    """Прочерк — напечатанное «такой формы не существует», и он остаётся в таблице.

    Я сам чуть не потерял эту разницу 23.08.2026: выбросил прочерк как мусор и снял
    таблицы у десяти обычных глаголов — «geschehen», «erfolgen», «vorliegen». Человек
    перестал видеть «es geschieht». Безличный глагол в первом и втором лице форм НЕ
    ИМЕЕТ, и это правда о языке, а не наша неполнота.
    """

    def test_у_безличного_глагола_остаётся_третье_лицо(self):
        forms = _column_forms(BESAGEN, 0, column=0)
        assert forms["er/sie/es"] == "besagt"
        assert forms["sie/Sie"] == "besagen"
        assert forms["ich"] == "—", "первого лица у безличного глагола нет"

    def test_прочерк_не_сдвигает_столбцы(self):
        """Если прочерк просто выкинуть, форма третьего лица «besage» уехала бы в
        колонку пассива."""
        forms = _column_forms(BESAGEN, 0, column=1)
        assert forms["er/sie/es"] == "besage"
        assert forms["du"] == "—"

    def test_блок_из_одних_прочерков_не_таблица(self):
        """Повелительного у безличного глагола нет вовсе — блока быть не должно."""
        only_dashes = [
            "Präsens", "Person", "Indikativ",
            "1. Person Singular", "—", "2. Person Singular", "—",
            "3. Person Singular", "er/sie/es", "—",
            "1. Person Plural", "—", "2. Person Plural", "—", "3. Person Plural", "—",
        ]
        assert _column_forms(only_dashes, 0, column=0) == {}


class TestОбычнаяСтраницаРазбираетсяКакПрежде:
    def test_сильный_глагол(self):
        forms = _column_forms(HALTEN, 0, column=0)
        assert forms["du"] == "hieltest", "полный вариант, а не усечённый «hieltst»"
        assert forms["er/sie/es"] == "hielt"

    def test_конъюнктив_сильного_глагола(self):
        forms = _column_forms(HALTEN, 0, column=1)
        assert forms["ich"] == "hielte"
        assert forms["er/sie/es"] == "hielte"


# Flexion:geschehen, блок Imperative. У безличного глагола повелительного нет, и
# страница печатает прочерк. До 23.08.2026 он уходил на экран как форма: разбор
# повелительного идёт отдельным куском, и общий фильтр служебных ячеек до него не доставал.
GESCHEHEN_IMP = [
    "Imperative", "Singular", "Plural",
    "2. Person Singular", "—",
    "2. Person Plural", "—",
]

HALTEN_IMP = [
    "Imperative", "Singular", "Plural",
    "2. Person Singular", "halt!",
    "2. Person Plural", "haltet!",
]


def _imperativ(cells):
    from backend.german_verb_paradigms import documented_tables
    # documented_tables работает по ячейкам, а не по HTML: подменяем разбор разметки.
    import backend.german_verb_paradigms as V
    original = V._table_cells
    V._table_cells = lambda html: cells
    try:
        return documented_tables("<html/>").get("imperativ")
    finally:
        V._table_cells = original


class TestПовелительноеТожеБезСлужебныхЯчеек:
    def test_у_безличного_глагола_повелительного_нет(self):
        assert _imperativ(GESCHEHEN_IMP) is None, "прочерк ушёл на экран как форма"

    def test_обычный_глагол_повелительное_сохраняет(self):
        assert _imperativ(HALTEN_IMP) == {"du": "halt", "ihr": "haltet"}
