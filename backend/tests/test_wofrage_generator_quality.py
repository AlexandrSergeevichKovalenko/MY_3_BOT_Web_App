"""Качество заданий Wo-Fragen: три рубежа вместо точечных заплаток.

Как задания попадают к человеку: дважды в день (11:00 и 16:30) бот собирает ОДИН
набор из 10 заданий, кладёт его в базу и рассылает всем. Значит ошибка не «мелькнёт
у одного» — она замерзает в базе и уходит всем сразу. Ловить надо на входе.

Рубежи:
  1. check_bank()  — данные банка (человек в списке вещей, пустая рамка, непроверенный
                     объект). Битые записи не выдаются вовсе.
  2. запрет второго верного ответа — «___ leidest du?» подходит и Woran (leiden an),
     и Worunter (leiden unter). Замер до правки: 2.1% заданий имели два верных ответа.
  3. check_item()  — проверка готового задания перед выдачей.

Плюс уровень набора: один глагол — один раз, одна фраза — один раз, не больше двух
заданий на предлог (замер до правки: 16% наборов повторяли глагол, 46% — предлог 3+ раза).
"""

import random
import re
import unittest
from collections import Counter

import backend.wofrage_generator as wg


class BankDataTests(unittest.TestCase):
    def test_bank_is_clean(self):
        self.assertEqual(wg.check_bank(), [], "банк заданий содержит ошибки в данных")

    def test_no_people_among_things(self):
        offenders = [
            (e["lemma"], o)
            for e in wg._BANK if not e.get("person_only")
            for o, _ru in (e.get("obj") or []) if wg._is_person_noun(o)
        ]
        self.assertEqual(offenders, [], f"люди в списках вещей: {offenders}")

    def test_every_object_is_vetted(self):
        """Незнакомое слово не проходит молча: о нём спрашивают «Worauf» или «Auf wen»?"""
        unknown = sorted({
            o for e in wg._BANK if not e.get("person_only")
            for o, _ru in (e.get("obj") or []) if o not in wg._VETTED_THINGS
        })
        self.assertEqual(unknown, [], f"объекты без проверки: {unknown}")

    def test_vetted_list_has_no_people(self):
        offenders = sorted(o for o in wg._VETTED_THINGS if wg._is_person_noun(o))
        self.assertEqual(offenders, [], f"люди в списке проверенных вещей: {offenders}")

    def test_person_only_verbs_carry_no_thing_objects(self):
        """Мёртвые списки — мина: снимут флаг, и посыплются битые задания.
        Так и лежали der Lehrer / die Nachbarin у двенадцати глаголов."""
        offenders = [e["lemma"] for e in wg._BANK if e.get("person_only") and (e.get("obj") or [])]
        self.assertEqual(offenders, [], f"у «только о людях» остались объекты: {offenders}")

    def test_broken_entry_never_reaches_a_user(self):
        """Подкладываем заведомо битую запись — она не должна попасть в выдачу."""
        bad = {"lemma": "тест сломан an", "prep": "an", "case": "dat", "ru": "тест",
               "person": False, "q": ["testest du?"], "obj": [("der Chef", "шеф")]}
        original = list(wg._BANK)
        try:
            wg._BANK.append(bad)
            self.assertTrue(any("тест сломан" in p for p in wg.check_bank()))
            healthy = wg._healthy_bank()
            self.assertNotIn("тест сломан an", [e["lemma"] for e in healthy])
        finally:
            wg._BANK[:] = original


class SingleCorrectAnswerTests(unittest.TestCase):
    """Главная находка: у части заданий верных ответов было ДВА."""

    def test_rival_prepositions_are_never_offered(self):
        random.seed(20260729)
        for _ in range(1500):
            entry = random.choice(wg._BANK)
            item = wg._build_one(entry)
            frame = item["s"].replace("___", "").strip()
            rivals = wg._rival_forms(entry, frame)
            # Объявленные «оба верны» показывать МОЖНО — они засчитываются.
            clash = rivals & set(item["opts"]) - wg.accepted_answers(item)
            self.assertFalse(clash, f"второй верный ответ в вариантах: {clash} — {item['s']}")

    def test_known_rival_pairs_are_registered(self):
        """leiden an/unter, kämpfen für/gegen/um, sprechen mit/über и т.д."""
        for head, expected in (("leiden", {"an", "unter"}),
                               ("kämpfen", {"für", "gegen", "um"}),
                               ("sprechen", {"mit", "über"}),
                               ("sich freuen", {"auf", "über"})):
            govs = {p for p, _c in wg._RIVALS_BY_HEAD.get(head, set())}
            self.assertTrue(expected <= govs, f"{head}: не видим соперников {expected - govs}")


class GeneratedItemTests(unittest.TestCase):
    def test_500_distinct_items_are_all_clean(self):
        random.seed(4242)
        seen, checked = set(), 0
        while checked < 500:
            item = wg._build_one(random.choice(wg._BANK))
            key = (item["s"], item["a"], item["clue"])
            if key in seen:
                continue
            seen.add(key)
            checked += 1
            self.assertEqual(wg.check_item(item), [], f"кривое задание: {item['s']} → {item['a']}")

    def test_every_verb_survives_repeated_generation(self):
        """Каждый глагол банка прогоняем много раз: редкая ветка тоже должна быть чистой."""
        random.seed(7)
        for entry in wg._BANK:
            for _ in range(12):
                item = wg._build_one(entry)
                problems = wg.check_item(item)
                self.assertEqual(problems, [], f"{entry['lemma']}: {problems}")

    def test_answer_follows_from_government_not_from_the_clue(self):
        random.seed(11)
        for _ in range(300):
            item = wg._build_one(random.choice(wg._BANK))
            expected = (wg._wo_form(item["prep"]).capitalize() if item["target"] == "thing"
                        else wg._person_form(item["prep"], item["case"]))
            self.assertEqual(item["a"], expected)

    def test_clue_never_leaks_the_answer(self):
        random.seed(13)
        for _ in range(300):
            item = wg._build_one(random.choice(wg._BANK))
            self.assertNotIn(item["a"].lower(), item["clue"].lower())


class BothAnswersAcceptedTests(unittest.TestCase):
    """Прятать второй верный ответ — полумера. Где формы взаимозаменяемы, показываем
    обе, засчитываем любую и объясняем разницу."""

    def test_declared_pairs_are_accepted_and_explained(self):
        random.seed(2)
        seen = set()
        for _ in range(2000):
            entry = random.choice(wg._BANK)
            if not entry.get("also_ok"):
                continue
            item = wg._build_one(entry)
            ok = wg.accepted_answers(item)
            if len(ok) < 2:
                continue
            seen.add(entry["lemma"])
            self.assertTrue(ok <= set(item["opts"]), "верный ответ не показан среди вариантов")
            self.assertTrue(item["unterschied"].strip(), "не объяснили разницу между формами")
            self.assertEqual(wg.check_item(item), [])
        self.assertGreaterEqual(len(seen), 5, f"мало пар проверено: {sorted(seen)}")

    def test_items_without_declaration_accept_exactly_one_answer(self):
        random.seed(6)
        for _ in range(400):
            entry = random.choice(wg._BANK)
            if entry.get("also_ok"):
                continue
            item = wg._build_one(entry)
            self.assertEqual(wg.accepted_answers(item), {item["a"]})

    def test_item_key_is_stable_and_distinguishes_items(self):
        random.seed(8)
        keys = {}
        for _ in range(600):
            item = wg._build_one(random.choice(wg._BANK))
            same = (item["s"], item["a"], item["obj"])
            self.assertEqual(wg.item_key(item), item["key"])
            keys.setdefault(item["key"], same)
            self.assertEqual(keys[item["key"]], same, "разные задания получили один ключ")


class SetLevelTests(unittest.TestCase):
    def test_sets_are_full_and_varied(self):
        random.seed(99)
        for _ in range(120):
            items = wg.build_wofrage_items(10)
            self.assertEqual(len(items), 10, "набор собрался неполным")
            self.assertEqual(len({i["lemma"] for i in items}), 10, "глагол повторился в наборе")
            self.assertEqual(len({i["s"] for i in items}), 10, "фраза повторилась в наборе")
            top = Counter(i["prep"] for i in items).most_common(1)[0][1]
            self.assertLessEqual(top, 2, "один предлог занял больше двух мест из десяти")

    def test_person_and_thing_questions_both_appear(self):
        random.seed(3)
        targets = Counter(i["target"] for i in wg.build_wofrage_items(10) for _ in [0])
        for _ in range(50):
            targets.update(i["target"] for i in wg.build_wofrage_items(10))
        self.assertGreater(targets["person"], 0)
        self.assertGreater(targets["thing"], targets["person"], "вопросов о вещах должно быть больше")


class FrameGrammarTests(unittest.TestCase):
    def test_frames_are_lowercase_questions(self):
        for entry in wg._BANK:
            for frame in entry["q"]:
                self.assertTrue(frame.strip().endswith("?"), f"{entry['lemma']}: «{frame}»")
                self.assertFalse(frame[:1].isupper(), f"{entry['lemma']}: «{frame}» с большой буквы")

    def test_no_frame_contains_a_question_word_already(self):
        """Рамка идёт ПОСЛЕ пропуска — вопросительного слова в ней быть не должно."""
        pattern = re.compile(r"\b(wo[a-zäöü]*|wen|wem|was)\b", re.IGNORECASE)
        offenders = [(e["lemma"], f) for e in wg._BANK for f in e["q"] if pattern.search(f)]
        self.assertEqual(offenders, [], f"в рамке уже есть вопросительное слово: {offenders}")


class QuestionTranslationTests(unittest.TestCase):
    """Перевод вопроса. Разбор без него — ребус: правило есть, смысла фразы нет.

    Перевод пишется руками для каждой пары (управление, фраза) и отдельно для вопроса
    о вещи и о человеке — «Чего ты ждёшь?» и «Кого ты ждёшь?» это разные вопросы.
    Поэтому проверяем не только наличие, но и что переводы не перепутаны местами.
    """

    THING_WORDS = re.compile(r"\b(что|чего|чему|чем|чём)\b", re.IGNORECASE)
    PERSON_WORDS = re.compile(r"\b(кто|кого|кому|ком|кем)\b", re.IGNORECASE)

    def test_every_producible_question_is_translated(self):
        self.assertEqual(wg.missing_translations(), [], "есть фразы без перевода")

    def test_thing_and_person_translations_are_not_swapped(self):
        for (lemma, frame), (thing, person) in wg.QUESTION_RU.items():
            if thing:
                self.assertTrue(self.THING_WORDS.search(thing),
                                f"{lemma} «{frame}»: перевод о вещи без «что/чего/чем»: {thing}")
                self.assertFalse(self.PERSON_WORDS.search(thing),
                                 f"{lemma} «{frame}»: в переводе о вещи спрашивают о человеке: {thing}")
            if person:
                self.assertTrue(self.PERSON_WORDS.search(person),
                                f"{lemma} «{frame}»: перевод о человеке без «кто/кого/кем»: {person}")
                self.assertFalse(self.THING_WORDS.search(person),
                                 f"{lemma} «{frame}»: в переводе о человеке спрашивают о вещи: {person}")

    def test_translations_are_questions(self):
        for (lemma, frame), pair in wg.QUESTION_RU.items():
            for text in pair:
                if text:
                    self.assertTrue(text.strip().endswith("?"), f"{lemma} «{frame}»: «{text}» не вопрос")
                    self.assertTrue(text[:1].isupper(), f"{lemma} «{frame}»: «{text}» с маленькой буквы")

    def test_generated_items_carry_the_translation(self):
        random.seed(11)
        for _ in range(30):
            for item in wg.build_wofrage_items(10):
                self.assertTrue(str(item.get("frage_ru") or "").strip(),
                                f"задание без перевода: {item['lemma']} / {item['s']} / {item['target']}")


if __name__ == "__main__":
    unittest.main()
