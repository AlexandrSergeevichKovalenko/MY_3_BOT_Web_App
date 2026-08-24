"""Две находки владельца в тренажёрах.

1) Карточка «der Band» звучала как «die Band». В базе всё верно — три записи с тремя
   файлами, — но озвучка искалась ПО СЛОВУ без рода, и три записи схлопывались в одну.
   Задето 26 слов-омонимов и 58 карточек: See, Tor, Leiter, Schild, Kiefer, Steuer…

2) «Worauf nimmst du Rücksicht? — Ребёнок.» Ребёнок — человек, правильный вопрос
   «Auf wen». Карточка противоречила сама себе: в пояснении писала «о человеке было бы
   Auf wen», а ответом считала вопрос о вещи. Причина — люди в списках «вещей».
"""

import re
import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
import backend.wofrage_generator as wg


PERSON_HEADS = {
    "kind", "mann", "frau", "freund", "freundin", "bruder", "schwester", "lehrer",
    "lehrerin", "chef", "chefin", "vater", "mutter", "kollege", "kollegin", "arzt",
    "sohn", "tochter", "oma", "opa", "nachbar", "nachbarin", "gast", "kunde",
    "partner", "eltern", "kinder", "leute", "mensch", "junge", "mädchen", "schüler",
    "student", "trainer",
}


def _head_noun(phrase: str) -> str:
    return re.sub(r"^(der|die|das)\s+", "", str(phrase or "")).strip().lower()


class ArtikelHomographAudioTests(unittest.TestCase):
    """Ключ выборки обязан включать род, иначе омоним получает чужую озвучку."""

    def _fake_rows(self, rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = lambda s, *a: False
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = lambda s, *a: False
        return ctx

    def test_each_gender_gets_its_own_audio(self):
        rows = [
            ("band", "der", "artikel/audio/der-band.mp3"),
            ("band", "die", "artikel/audio/die-band.mp3"),
            ("band", "das", "artikel/audio/das-band.mp3"),
        ]
        with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
            got = db.get_article_noun_audio([("Band", "der"), ("Band", "die"), ("Band", "das")])
        self.assertEqual(got[("band", "der")], "artikel/audio/der-band.mp3")
        self.assertEqual(got[("band", "die")], "artikel/audio/die-band.mp3")
        self.assertEqual(got[("band", "das")], "artikel/audio/das-band.mp3")

    def test_mnemonics_and_images_are_keyed_the_same_way(self):
        for getter, column in ((db.get_article_noun_mnemonics, "подсказка"),
                               (db.get_article_noun_images, "картинка")):
            with self.subTest(getter=getter.__name__):
                rows = [("see", "der", f"{column} про озеро"), ("see", "die", f"{column} про море")]
                with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
                    got = getter([("See", "der"), ("See", "die")])
                self.assertNotEqual(got[("see", "der")], got[("see", "die")])

    def test_plain_word_lookup_still_works(self):
        """Старые вызовы «просто по слову» не должны сломаться."""
        rows = [("haus", "das", "artikel/audio/das-haus.mp3")]
        with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
            got = db.get_article_noun_audio(["Haus"])
        self.assertEqual(got["haus"], "artikel/audio/das-haus.mp3")


class WoFrageAnimacyTests(unittest.TestCase):
    """О человеке спрашивают «Auf wen», о вещи — «Worauf». Список «вещей» не должен
    содержать людей: генератор берёт оттуда объект именно для вопроса о вещи."""

    def test_no_people_among_things(self):
        offenders = [
            (entry["lemma"], obj)
            for entry in wg._BANK
            if not entry.get("person_only")
            for obj, _ru in (entry.get("obj") or [])
            if _head_noun(obj) in PERSON_HEADS
        ]
        self.assertEqual(offenders, [], f"люди в списках вещей: {offenders}")

    def test_person_question_uses_wen_form(self):
        self.assertEqual(wg._person_form("auf", "akk"), "Auf wen")
        self.assertEqual(wg._person_form("an", "akk"), "An wen")
        self.assertEqual(wg._wo_form("auf").capitalize(), "Worauf")

    def test_generated_items_never_ask_worauf_about_a_person(self):
        for _ in range(200):
            item = wg._build_one(wg._BANK[_ % len(wg._BANK)])
            head = _head_noun(item.get("obj") or "")
            if head in PERSON_HEADS:
                self.assertEqual(
                    item.get("target"), "person",
                    f"о человеке спрашивают как о вещи: {item.get('a')} … {item.get('obj')}",
                )


class WoFrageApprovedObjectsTests(unittest.TestCase):
    """Список проверенных вещей живёт в самом генераторе (wg._VETTED_THINGS) — там же,
    где он применяется на выдаче. Здесь только следим, что он не разошёлся с банком.
    Подробные проверки генератора — в test_wofrage_generator_quality.py."""

    def test_every_thing_object_is_approved(self):
        unknown = sorted({
            obj for entry in wg._BANK if not entry.get("person_only")
            for obj, _ru in (entry.get("obj") or []) if obj not in wg._VETTED_THINGS
        })
        self.assertEqual(
            unknown, [],
            "В банке Wo-Fragen появились объекты, которых нет в проверенном списке: "
            f"{unknown}.\nПроверь каждое: это ВЕЩЬ или ЧЕЛОВЕК? О человеке спрашивают "
            "«Auf wen», и такое слово в списке вещей ломает задание.",
        )

    def test_approved_list_has_no_people(self):
        offenders = sorted(o for o in wg._VETTED_THINGS if wg._is_person_noun(o))
        self.assertEqual(offenders, [], f"люди в проверенном списке вещей: {offenders}")


# Найдено владельцем 24.08.2026: страж 29.07 стоял только на СБОРКЕ, а очередь
# «работа над ошибками» хранит свою копию карточки и показывает её месяцами. Поэтому
# карточка от 21.07 «Worauf nimmst du Rücksicht? — Ребёнок» всплыла через месяц после
# починки генератора. Ниже — проверки того, что старая копия больше не доходит до
# экрана: её ловят перед показом и приводят к правильному виду.
JULY_CARD = {
    "s": "___ nimmst du Rücksicht?",
    "clue": "— Ребёнок.",
    "opts": ["Worauf", "In wen", "Woraus", "Auf wen"],
    "correct": "Worauf",
    "target": "thing",
    "lemma": "Rücksicht nehmen auf",
    "verb_ru": "считаться (с)",
    "obj": "das Kind",
    "obj_ru": "ребёнок",
    "erklaerung": "«Rücksicht nehmen auf» управляет предлогом «auf» (Akkusativ). "
                  "Вопрос о вещи → Worauf. О человеке было бы «Auf wen».",
    "tip": "Предлог «auf» на гласную → вставляем -r-: wo+r+auf = Worauf.",
}


class WoFrageStoredCardTests(unittest.TestCase):
    """Сохранённая карточка проверяется теми же правилами, что и свежесобранная."""

    def test_july_card_is_caught(self):
        problems = wg.stored_item_problems(JULY_CARD)
        self.assertTrue(problems, "карточка «Worauf … das Kind» обязана быть отбракована")
        self.assertIn("как о вещи", problems[0])

    def test_healthy_card_passes(self):
        good = dict(JULY_CARD, target="person", correct="Auf wen", obj="David", obj_ru="")
        self.assertEqual(wg.stored_item_problems(good), [])

    def test_person_name_clue_is_not_a_defect(self):
        """Ложный след 24.08: в режиме человека подсказка — голое имя, и в списке
        существительных его нет. Это НЕ дефект, отбраковывать такие нельзя."""
        for name in ("David", "Julia", "Ben"):
            card = dict(JULY_CARD, target="person", correct="Auf wen", obj=name, obj_ru="")
            self.assertEqual(wg.stored_item_problems(card), [], f"имя {name} посчитали дефектом")

    def test_repair_switches_to_person_question(self):
        fixed, why = wg.repair_stored_item(JULY_CARD)
        self.assertIsNotNone(fixed, why)
        self.assertEqual(fixed["correct"], "Auf wen")
        self.assertEqual(fixed["target"], "person")
        self.assertEqual(fixed["obj"], "das Kind", "объект менять незачем — он тут ни при чём")
        self.assertIn("Auf wen", fixed["erklaerung"])
        self.assertEqual(wg.stored_item_problems(fixed), [])

    def test_repair_swaps_object_when_verb_takes_no_person(self):
        """«bereit sein zu jemandem» — не немецкий, значит неверен объект, а не режим."""
        card = dict(JULY_CARD, s="___ seid ihr bereit?", clue="— Жертва.", correct="Wozu",
                    opts=["Wofür", "Worum", "Zu wem", "Wozu"], lemma="bereit sein zu",
                    obj="das Opfer", obj_ru="жертва", verb_ru="быть готовым (к)",
                    erklaerung="", tip="")
        fixed, why = wg.repair_stored_item(card)
        self.assertIsNotNone(fixed, why)
        self.assertEqual(fixed["correct"], "Wozu", "ответ верный, менять его нельзя")
        self.assertEqual(fixed["target"], "thing")
        self.assertFalse(wg._is_person_noun(fixed["obj"]), "объект обязан стать вещью")
        self.assertEqual(wg.stored_item_problems(fixed), [])

    def test_repair_refuses_when_the_verb_is_unknown(self):
        """Глагола нет в банке — управление выводить не из чего. Догадываться запрещено."""
        fixed, why = wg.repair_stored_item(dict(JULY_CARD, lemma="выдуманный глагол zu"))
        self.assertIsNone(fixed)
        self.assertIn("банке", why)

    def test_review_page_heals_the_card_before_showing_it(self):
        """Страница «работы над ошибками» отдаёт уже исправленную карточку."""
        cursor = MagicMock()
        cursor.fetchall.return_value = [(256, dict(JULY_CARD))]
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = lambda s, *a: False
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = lambda s, *a: False
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "review_portion_left", return_value=15), \
             patch.object(db, "ensure_aufgabe_mistakes_schema", return_value=None):
            got = db.get_due_wofrage_mistakes_batch(117649764, 15)
        self.assertEqual(len(got), 1, "карточку не выбросили, а починили")
        self.assertEqual(got[0]["payload"]["correct"], "Auf wen")
        self.assertEqual(wg.stored_item_problems(got[0]["payload"]), [])
        wrote = [c for c in cursor.execute.call_args_list if "UPDATE" in str(c[0][0])]
        self.assertTrue(wrote, "исправленная карточка обязана лечь обратно в базу")


if __name__ == "__main__":
    unittest.main()
