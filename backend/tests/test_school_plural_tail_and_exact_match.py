"""Две двери, из-за которых «die Rutsche» отвечала «подскользнуться».

Владелец 21.08.2026: «вы чините эти слова — а вы ставите заслон, чтобы это не появлялось
снова, или в будущем опять придётся править руками?» Оба заслона здесь.

1. ХВОСТ «, -n» НЕ ВХОДИТ В СЛОВАРЬ. «die Brücke, -n» — школьная запись множественного
   числа. Это помета, а не часть слова: множественное лежит отдельным полем разбора.
   21.08.2026 таких заголовков нашлось 15, и столько же личных карточек — человек читал
   «die Brücke, -n» прямо на карточке повторения. Уборки мало: следующий, кто скопирует
   слово из учебника, завёл бы то же самое. Режем на входе, в `clean_text`, — а через неё
   идут и дверь словаря, и `door_check`, то есть все четыре заводчика единиц.

2. ТОЧНОЕ НАПИСАНИЕ БЬЁТ СЛОВОФОРМУ. «rutsche» — заголовок слова «die Rutsche» (горка)
   И форма глагола «ausrutschen». Отбор смотрел, у кого разбор полнее, и уверенно отдавал
   горку за глагол: у глагола разбор был, у существительного нет. Полнота разбора не может
   перевешивать то, ЧЕМ написание является.
"""
from backend.dictionary_intake import clean_text
from backend.lex_units import _pick_unit


class TestSchoolPluralTail:
    def test_tail_is_cut_at_the_door(self):
        assert clean_text("die Brücke, -n") == "die Brücke"
        assert clean_text("das Cockpit, -s") == "das Cockpit"
        assert clean_text("der Simulator, -en") == "der Simulator"
        assert clean_text("Pflegeheim, -e") == "Pflegeheim"
        assert clean_text("die Mutter, -ä") == "die Mutter"

    def test_the_rule_is_idempotent(self):
        once = clean_text("die Regierung, -en")
        assert clean_text(once) == once == "die Regierung"

    def test_what_the_rule_must_never_touch(self):
        # Двойная фамилия: после дефиса заглавная, это не окончание множественного.
        assert clean_text("Meier, -Schulze") == "Meier, -Schulze"
        # Цифры — не окончание.
        assert clean_text("температура, -20 градусов") == "температура, -20 градусов"
        # Обычное перечисление внутри предложения.
        assert clean_text("Ich brauche Milch, Brot") == "Ich brauche Milch, Brot"
        # Слово важнее пометы: пустой остаток не отдаём.
        assert clean_text("a, -n") == "a, -n"

    def test_a_clean_headword_is_left_alone(self):
        assert clean_text("die Brücke") == "die Brücke"
        assert clean_text("der nie versiegende Zapfhahn") == "der nie versiegende Zapfhahn"


class TestExactBeatsInflected:
    def _unit(self, unit_id, display, match_kind, *, card=None, gender=""):
        return {"id": unit_id, "display": display, "match_kind": match_kind,
                "card": card, "gender": gender}

    def test_exact_wins_over_a_richer_word_form(self):
        # Живой случай: «rutsche» ведёт и на форму глагола, и на само слово.
        verb_form = self._unit(10956, "ausrutschen", "inflected", card={"forms": {}})
        noun = self._unit(45688, "die Rutsche", "exact")
        chosen = _pick_unit([verb_form, noun], requested_article="")
        assert chosen["display"] == "die Rutsche", "точное написание обязано бить словоформу"

    def test_among_exact_matches_the_richer_one_still_wins(self):
        thin = self._unit(1, "der Kiefer", "exact")
        rich = self._unit(2, "die Kiefer", "exact", card={"forms": {}})
        assert _pick_unit([thin, rich], requested_article="")["id"] == 2

    def test_requested_article_still_decides_between_homographs(self):
        jaw = self._unit(1, "der Kiefer", "exact", gender="der")
        pine = self._unit(2, "die Kiefer", "exact", gender="die")
        assert _pick_unit([jaw, pine], requested_article="die")["id"] == 2

    def test_only_forms_available_still_answers(self):
        # Если точного написания нет вовсе, форма — единственный ответ, и молчать нельзя.
        form = self._unit(10956, "ausrutschen", "inflected")
        other = self._unit(10957, "rutschen", "inflected", card={"forms": {}})
        assert _pick_unit([form, other], requested_article="") is not None
