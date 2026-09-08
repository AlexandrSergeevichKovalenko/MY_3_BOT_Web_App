"""Дверь приёма синонимов спринта (backend/sprint_intake.py), 06.09.2026.

Класс дефекта: список accepted от модели шёл в банк без проверки — «die Option» шесть
раз у Gelegenheit, само слово внутри списка, «die Potenzial» с неверным артиклем,
«rememembern» синонимом. Каждый тест — один случай класса на живом примере из базы."""
from __future__ import annotations

from backend.sprint_intake import (ARTICLE_MISMATCH, ARTICLE_UNKNOWN, DUPLICATE, NO_DICTIONARY, SELF,
                                   UNCONFIRMED, clean_accepted, _split_noun)
from backend.synonym_sources import Confirmation


def _conf(confirmed: set[str]):
    """Подтверждение: всё из confirmed — да, остальное — нет."""
    def f(target, cands, relation="synonym"):
        return {c: Confirmation(confirmed=c in confirmed,
                                by=("openthesaurus",) if c in confirmed else (),
                                ot_knows_target=True, ot_knows_candidate=True,
                                wikt_target="not_listed", wikt_candidate="not_listed")
                for c in cands}
    return f


def _art(table: dict[str, str]):
    def f(noun):
        a = table.get(noun)
        return (a, "тест") if a else (None, "нет данных")
    return f


GELEGENHEIT = [
    {"de": "die Möglichkeit", "ru": "возможность"}, {"de": "die Chance", "ru": "шанс"},
    {"de": "die Option", "ru": "опция"}, {"de": "die Gelegenheit", "ru": "случай"},
    {"de": "die Option", "ru": "вариант"}, {"de": "die Option", "ru": "альтернатива"},
    {"de": "der Zufall", "ru": "случайность"}, {"de": "die Gelegenheit", "ru": "удобный случай"},
]
ART = {"Möglichkeit": "die", "Chance": "die", "Option": "die", "Zufall": "der", "Gelegenheit": "die"}


def test_die_option_остаётся_один_раз_с_первым_переводом():
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    kept = [k["de"] for k in res.kept]
    assert kept.count("die Option") == 1
    assert next(k for k in res.kept if k["de"] == "die Option")["ru"] == "опция"
    # 2 повтора Option + 1 повтор самого Gelegenheit
    assert res.stats[DUPLICATE] == 3


def test_само_слово_не_синоним_себе():
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    assert all(k["de"] != "die Gelegenheit" for k in res.kept)
    # первое вхождение — самослово, второе — уже дубль: self считается ОДИН раз
    assert res.stats[SELF] == 1 and res.stats[DUPLICATE] == 3


def test_неподтверждённый_синоним_не_в_списке_а_у_судьи():
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    assert "der Zufall" not in [k["de"] for k in res.kept]
    zufall = next(r for r in res.rejected if r.de == "der Zufall")
    assert zufall.reason == UNCONFIRMED and zufall.reference_article == "der"
    assert res.stats[UNCONFIRMED] == 1


def test_артикль_не_по_справочнику_уходит_судье_с_вердиктом():
    pairs = [{"de": "die Potenzial", "ru": "потенциал"}, {"de": "die Chance", "ru": "шанс"},
             {"de": "die Option", "ru": "вариант"}, {"de": "die Alternative", "ru": "альтернатива"}]
    res = clean_accepted("die Möglichkeit", "synonym", pairs,
                         confirm=_conf({d["de"] for d in pairs}),
                         article=_art({"Potenzial": "das", "Chance": "die", "Option": "die", "Alternative": "die"}))
    assert [k["de"] for k in res.kept] == ["die Chance", "die Option", "die Alternative"]
    bad = next(r for r in res.rejected if r.de == "die Potenzial")
    assert bad.reason == ARTICLE_MISMATCH
    assert (bad.stored_article, bad.noun, bad.reference_article) == ("die", "Potenzial", "das")
    assert bad.confirmed_by == ["openthesaurus"]   # владелец видит: синонимия подтверждена, спор об артикле


def test_справочник_не_знает_слово_судья_спросит_wiktionary():
    pairs = [{"de": "das Okay", "ru": "окей"}, {"de": "die Chance", "ru": "шанс"}]
    res = clean_accepted("die Zustimmung", "synonym", pairs, confirm=_conf({"das Okay", "die Chance"}),
                         article=_art({"Chance": "die"}))
    okay = next(r for r in res.rejected if r.de == "das Okay")
    assert okay.reason == ARTICLE_UNKNOWN and okay.reference_article == ""


def test_порог_три_и_для_синонимов_и_для_антонимов():
    two = [{"de": "die Chance", "ru": "шанс"}, {"de": "die Option", "ru": "вариант"}]
    res = clean_accepted("die Gelegenheit", "synonym", two, confirm=_conf({"die Chance", "die Option"}),
                         article=_art(ART))
    assert len(res.kept) == 2 and not res.enough and res.min_needed == 3
    three = [{"de": w, "ru": ""} for w in ("geizig", "kleinlich", "knauserig")]
    res = clean_accepted("großzügig", "antonym", three, confirm=_conf(set(three_de := {"geizig", "kleinlich", "knauserig"})),
                         article=_art({}))
    assert res.enough and res.min_needed == 3


def test_антонимы_проходят_ту_же_дверь_с_источником_антонимов():
    """Владелец 06.09.2026: «по антонимам механика та же самая». Подтверждение идёт
    с relation='antonym' ({{Gegenwörter}} Wiktionary), неподтверждённое — судье."""
    calls = []
    def spy(t, c, relation="synonym"):
        calls.append(relation)
        return {x: Confirmation(confirmed=(x == "geizig"), by=("wiktionary",) if x == "geizig" else (),
                                ot_knows_target=False, ot_knows_candidate=False,
                                wikt_target="listed", wikt_candidate="not_listed") for x in c}
    pairs = [{"de": "geizig", "ru": ""}, {"de": "geizig", "ru": ""}, {"de": "großzügig", "ru": ""},
             {"de": "freigebig", "ru": ""}]
    res = clean_accepted("großzügig", "antonym", pairs, confirm=spy, article=_art({}))
    assert calls == ["antonym"] and [k["de"] for k in res.kept] == ["geizig"]
    assert [r.de for r in res.rejected if r.reason == UNCONFIRMED] == ["freigebig"]


def test_возвратный_глагол_совпадает_с_самим_собой():
    pairs = [{"de": "sich erinnern", "ru": "помнить"}, {"de": "sich entsinnen", "ru": "припоминать"}]
    res = clean_accepted("sich erinnern", "synonym", pairs, confirm=_conf({"sich entsinnen"}), article=_art({}))
    assert [k["de"] for k in res.kept] == ["sich entsinnen"] and res.stats[SELF] == 1


def test_что_считается_существительным():
    assert _split_noun("die Option") == ("die", "Option")
    assert _split_noun("Backup") == ("", "Backup")
    assert _split_noun("zur Verfügung geben") is None
    assert _split_noun("erzielen") is None


def test_примеры_тренажёра_чистятся_от_дублей_и_снятых_слов():
    from backend.sprint_intake import _filter_examples
    tj = {"correct_examples": [{"word": "die Option", "sentence_de": "a"}, {"word": "der Fall", "sentence_de": "b"},
                               {"word": "die Option", "sentence_de": "c"}, {"word": "die Chance", "sentence_de": "d"}]}
    new, dropped = _filter_examples(tj, {"die option", "die chance"})
    assert [e["word"] for e in new["correct_examples"]] == ["die Option", "die Chance"] and dropped == 2


def test_пример_живёт_пока_кандидат_ждёт_судью():
    from backend.sprint_intake import pending_example_keys
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    # der Zufall не подтверждён → ждёт судью → его пример не стирается; дубли и самослово — стираются
    assert pending_example_keys(res) == {"der zufall"}


# ── 08.09.2026: человека из цепочки убрать; слово обязано существовать; дверь помнит решения ──

def _conf_exist(confirmed: set[str], missing: set[str] = frozenset(), unknown: set[str] = frozenset()):
    """missing — нет страницы и OpenThesaurus не знает; unknown — проверить не удалось."""
    def f(target, cands, relation="synonym"):
        return {c: Confirmation(confirmed=c in confirmed, by=("wiktionary",) if c in confirmed else (),
                                ot_knows_target=True, ot_knows_candidate=c not in missing and c not in unknown,
                                wikt_target="not_listed",
                                wikt_candidate="no_page" if c in missing else ("unknown" if c in unknown else "not_listed"),
                                dwds_candidate=False if c in missing else None)
                for c in cands}
    return f


UNABHAENGIG = [{"de": w, "ru": ""} for w in ("abhängig", "unselbständig", "befehlsgebunden", "hörig", "nicht da")]


def test_слово_без_словаря_снимается_до_судьи():
    """«befehlsgebunden» вошло по «да» судьи, а его нет ни в Duden, ни в DWDS, ни в
    Wiktionary. Однословный кандидат без страницы, которого не знает и OpenThesaurus, —
    окончательный отказ, судье не показывается."""
    res = clean_accepted("unabhängig", "antonym", UNABHAENGIG,
                         confirm=_conf_exist({"abhängig"}, missing={"befehlsgebunden", "nicht da"}), article=_art({}))
    assert [k["de"] for k in res.kept] == ["abhängig"]
    gone = next(r for r in res.rejected if r.de == "befehlsgebunden")
    assert gone.reason == NO_DICTIONARY and res.stats[NO_DICTIONARY] == 1
    # оборот из нескольких слов существованием не проверяется — его судит судья
    phrase = next(r for r in res.rejected if r.de == "nicht da")
    assert phrase.reason == UNCONFIRMED
    from backend.sprint_intake import pending_example_keys
    assert "befehlsgebunden" not in pending_example_keys(res) and "nicht da" in pending_example_keys(res)


def test_не_удалось_проверить_существование_это_судье_а_не_отказ():
    res = clean_accepted("unabhängig", "antonym", [{"de": "hörig", "ru": ""}],
                         confirm=_conf_exist(set(), unknown={"hörig"}), article=_art({}))
    assert res.rejected[0].reason == UNCONFIRMED and res.stats[NO_DICTIONARY] == 0


def test_дверь_помнит_принятое_судьёй_и_владельцем():
    """Перепроверка (--recheck) снимала бы слово, которое судья уже впустил: источник
    его по-прежнему не подтверждает. Найдено трассировкой 08.09.2026. Принятое решением
    дверь не пересматривает — кроме «нет в словаре» для решения судьи."""
    decided = {"unselbständig": "judge_yes", "hörig": "keep", "befehlsgebunden": "judge_yes"}
    res = clean_accepted("unabhängig", "antonym", UNABHAENGIG,
                         confirm=_conf_exist({"abhängig"}, missing={"befehlsgebunden"}), article=_art({}),
                         decided_keep=decided)
    assert [k["de"] for k in res.kept] == ["abhängig", "unselbständig", "hörig"]
    assert next(r for r in res.rejected if r.de == "befehlsgebunden").reason == NO_DICTIONARY


def test_оставленное_владельцем_кнопкой_не_снимается_даже_без_словаря():
    res = clean_accepted("erreichen", "synonym", [{"de": "zustandebringen", "ru": ""}, {"de": "schaffen", "ru": ""}],
                         confirm=_conf_exist({"schaffen"}, missing={"zustandebringen"}), article=_art({}),
                         decided_keep={"zustandebringen": "keep"})
    assert [k["de"] for k in res.kept] == ["zustandebringen", "schaffen"]


def test_принятое_с_артиклем_владельца_не_спрашивает_справочник_снова():
    """Владелец нажал «das» на «Okay»: в accepted лежит «das Okay», справочник его не
    знает — без памяти о решении дверь снова сняла бы слово как article_unknown."""
    res = clean_accepted("die Zustimmung", "synonym", [{"de": "das Okay", "ru": ""}, {"de": "die Chance", "ru": ""}],
                         confirm=_conf({"das Okay", "die Chance"}), article=_art({"Chance": "die"}),
                         decided_keep={"das okay": "das"})
    assert [k["de"] for k in res.kept] == ["das Okay", "die Chance"]


def test_косвенная_антонимия_доходит_до_очереди_с_именем_головы():
    def conf(target, cands, relation="synonym"):
        return {c: Confirmation(confirmed=True, by=("indirect",), ot_knows_target=True, ot_knows_candidate=True,
                                wikt_target="not_listed", wikt_candidate="not_listed", via="abhängig") for c in cands}
    res = clean_accepted("unabhängig", "antonym", [{"de": "unfrei", "ru": ""}], confirm=conf, article=_art({}))
    assert [k["de"] for k in res.kept] == ["unfrei"]
