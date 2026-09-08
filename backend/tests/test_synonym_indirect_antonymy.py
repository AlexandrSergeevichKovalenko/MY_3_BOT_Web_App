"""Косвенная антонимия и правило «слово обязано существовать» (backend/synonym_sources.py).

Владелец 08.09.2026: пользователь спросил, почему у «unabhängig» нет «süchtig»; на экране
было три антонима, потому что Wiktionary {{Gegenwörter}} у «unabhängig» знает ОДНО слово
(abhängig), а всё остальное решал судья и пять слов ждали кнопки владельца.
Решение: второй источник — «X противоположность цели по Wiktionary» + «кандидат в одном
гнезде OpenThesaurus с X» (метод WordNet/GermaNet). «Мы сами не будем придумывать,
добавляя приставку un» — никакой морфологии, два словарных факта.
Тесты без базы и сети: оба словаря подменяются."""
from __future__ import annotations

from unittest.mock import patch

from backend import synonym_sources as ss

# Живые данные 08.09.2026 (de.wiktionary, OpenThesaurus): у unabhängig Gegenwörter = [abhängig];
# гнездо OpenThesaurus 10490 = {abhängig, gebunden, unfrei, unmündig, unselbstständig,
# untergeordnet}; 34916 = {abhängig, süchtig}. У determiniert гнёзд с abhängig нет.
SYNSETS = {"abhängig": {10490, 34916}, "gebunden": {10490}, "unfrei": {10490}, "süchtig": {34916},
           "determiniert": {777}, "unabhängig": {5}, "frei": {5, 6}}
WIKT = {
    "unabhängig": ss.WiktionaryRelations("unabhängig", False, ["autark", "autonom"], ["abhängig"]),
    "abhängig": ss.WiktionaryRelations("abhängig", False, ["angewiesen", "hörig", "süchtig"], ["unabhängig"]),
    "gebunden": ss.WiktionaryRelations("gebunden", False, [], []),
    "unfrei": ss.WiktionaryRelations("unfrei", False, [], ["frei"]),
    "süchtig": ss.WiktionaryRelations("süchtig", False, ["abhängig"], []),
    "determiniert": ss.WiktionaryRelations("determiniert", False, [], []),
    "befehlsgebunden": ss.WiktionaryRelations("befehlsgebunden", True),
}


def _confirm(cands, **kw):
    with patch.object(ss, "openthesaurus_synsets", lambda t: set(SYNSETS.get(ss.term_key(t), set()))), \
         patch.object(ss, "wiktionary_relations", lambda terms, allow_network=True: {t: WIKT.get(t) for t in terms}), \
         patch.object(ss, "dwds_knows", lambda t, allow_network=True: False):
        return ss.confirm_relation("unabhängig", cands, relation="antonym", **kw)


def test_прямая_противоположность_по_wiktionary():
    c = _confirm(["abhängig"])["abhängig"]
    assert c.confirmed and c.by == ("wiktionary",) and c.via == ""


def test_косвенная_антонимия_через_гнездо_прямой_противоположности():
    """unfrei и gebunden лежат с abhängig в одном гнезде OpenThesaurus ⇒ антонимы unabhängig.
    Источник назван: 'indirect', via='abhängig'."""
    out = _confirm(["unfrei", "gebunden"])
    assert out["unfrei"].confirmed and out["unfrei"].by == ("indirect",) and out["unfrei"].via == "abhängig"
    assert out["gebunden"].confirmed and out["gebunden"].via == "abhängig"


def test_süchtig_подтверждается_через_гнездо_abhängig_оговорка_записана():
    """Гнёзда OpenThesaurus разбиты по значению, но какое гнездо головы отвечает значению
    цели, словари не говорят: süchtig (гнездо «körperlich abhängig») подтверждается так же,
    как unfrei. Это тот случай, о котором спрашивал пользователь; оговорка — в стратегии."""
    assert _confirm(["süchtig"])["süchtig"].by == ("indirect",)


def test_determiniert_не_подтверждается_и_идёт_судье():
    c = _confirm(["determiniert"])["determiniert"]
    assert not c.confirmed and c.by == () and c.exists_in_dictionaries is True


def test_симметрия_противоположность_кандидата_в_гнезде_цели():
    """unfrei: Gegenwörter = [frei]; frei делит гнездо с unabhängig ⇒ unfrei — антоним
    unabhängig и через обратный шаг (если бы прямого не было)."""
    syn = dict(SYNSETS); syn["abhängig"] = {999}   # прямой шаг ломаем: у abhängig другое гнездо
    with patch.object(ss, "openthesaurus_synsets", lambda t: set(syn.get(ss.term_key(t), set()))), \
         patch.object(ss, "wiktionary_relations", lambda terms, allow_network=True: {t: WIKT.get(t) for t in terms}):
        c = ss.confirm_relation("unabhängig", ["unfrei"], relation="antonym")["unfrei"]
    assert c.confirmed and c.by == ("indirect",) and c.via == "frei"


def test_слова_нет_ни_в_одном_словаре():
    """befehlsgebunden: страницы в Wiktionary нет, OpenThesaurus не знает — не слово, а
    догадка модели (нет и в Duden, и в DWDS; судья 06.09 впустил его по «да»)."""
    c = _confirm(["befehlsgebunden"])["befehlsgebunden"]
    assert c.exists_in_dictionaries is False and c.wikt_candidate == "no_page"


def test_не_удалось_проверить_это_не_нет():
    with patch.object(ss, "openthesaurus_synsets", lambda t: set()), \
         patch.object(ss, "wiktionary_relations", lambda terms, allow_network=True: {t: None for t in terms}):
        c = ss.confirm_relation("unabhängig", ["hörig"], relation="antonym", allow_network=False)["hörig"]
    assert c.exists_in_dictionaries is None and c.wikt_candidate == "unknown"


def test_синонимы_косвенный_шаг_не_делают():
    """Для синонимов hop через гнездо запрещён стратегией 06.09 (Chance~Möglichkeit~Option
    дрейфует): подтверждение только прямое."""
    with patch.object(ss, "openthesaurus_synsets", lambda t: {1} if ss.term_key(t) in ("a", "b") else {2}), \
         patch.object(ss, "wiktionary_relations", lambda terms, allow_network=True:
                      {t: ss.WiktionaryRelations(t, False, ["b"] if t == "a" else [], []) for t in terms}):
        out = ss.confirm_relation("a", ["b", "c"], relation="synonym")
    assert out["b"].confirmed and set(out["b"].by) == {"openthesaurus", "wiktionary"}
    assert not out["c"].confirmed and "indirect" not in out["c"].by


def test_dwds_третий_словарь_существования():
    """Сухой прогон 08.09.2026: правило «нет в Wiktionary и OpenThesaurus» сняло бы
    «entgrenzen» и «unsorgfältig» — оба есть в DWDS и Duden. DWDS спрашивается только
    когда двух первых словарей не хватило; «befehlsgebunden» не знает и он."""
    asked = []
    def dwds(term, allow_network=True):
        asked.append(term)
        return {"entgrenzen": True, "befehlsgebunden": False}.get(term)
    wikt = dict(WIKT); wikt["entgrenzen"] = ss.WiktionaryRelations("entgrenzen", True)
    with patch.object(ss, "openthesaurus_synsets", lambda t: set(SYNSETS.get(ss.term_key(t), set()))), \
         patch.object(ss, "wiktionary_relations", lambda terms, allow_network=True: {t: wikt.get(t) for t in terms}), \
         patch.object(ss, "dwds_knows", dwds):
        out = ss.confirm_relation("unabhängig", ["entgrenzen", "befehlsgebunden", "unfrei"], relation="antonym")
    assert out["entgrenzen"].exists_in_dictionaries is True and out["entgrenzen"].dwds_candidate is True
    assert out["befehlsgebunden"].exists_in_dictionaries is False
    assert out["unfrei"].dwds_candidate is None and "unfrei" not in asked   # Wiktionary знает — DWDS не трогали
    assert asked == ["entgrenzen", "befehlsgebunden"]
