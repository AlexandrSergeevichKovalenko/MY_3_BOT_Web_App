"""Дверь приёма синонимов спринта (backend/sprint_intake.py), 06.09.2026.

Класс дефекта: список accepted от модели шёл в банк без проверки — «die Option» шесть
раз у Gelegenheit, само слово внутри списка, «die Potenzial» с неверным артиклем,
«rememembern» синонимом. Каждый тест — один случай класса на живом примере из базы."""
from __future__ import annotations

from backend.sprint_intake import (ARTICLE_MISMATCH, ARTICLE_UNKNOWN, DUPLICATE, SELF,
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


def test_неподтверждённый_синоним_не_в_списке_а_у_владельца():
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    assert "der Zufall" not in [k["de"] for k in res.kept]
    zufall = next(r for r in res.rejected if r.de == "der Zufall")
    assert zufall.reason == UNCONFIRMED and zufall.reference_article == "der"
    assert res.stats[UNCONFIRMED] == 1


def test_артикль_не_по_справочнику_уходит_владельцу_с_вердиктом():
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


def test_справочник_не_знает_слово_кнопки_der_die_das():
    pairs = [{"de": "das Okay", "ru": "окей"}, {"de": "die Chance", "ru": "шанс"}]
    res = clean_accepted("die Zustimmung", "synonym", pairs, confirm=_conf({"das Okay", "die Chance"}),
                         article=_art({"Chance": "die"}))
    okay = next(r for r in res.rejected if r.de == "das Okay")
    assert okay.reason == ARTICLE_UNKNOWN and okay.reference_article == ""


def test_порог_три_для_синонимов_и_пять_для_антонимов():
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


def test_пример_живёт_пока_кандидат_ждёт_судью_или_владельца():
    from backend.sprint_intake import pending_example_keys
    res = clean_accepted("die Gelegenheit", "synonym", GELEGENHEIT,
                         confirm=_conf({"die Möglichkeit", "die Chance", "die Option"}), article=_art(ART))
    # der Zufall не подтверждён → ждёт судью → его пример не стирается; дубли и самослово — стираются
    assert pending_example_keys(res) == {"der zufall"}
