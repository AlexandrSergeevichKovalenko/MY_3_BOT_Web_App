"""Справочник родов не берёт родов у написаний, признанных негодной формой слова.

Дефект 17.08.2026. «das Fotos» был снят из игры и занесён в стоп-лист, а справочник на
вопрос про «Fotos» продолжал отвечать «das»: строка осталась в таблице, а выборка
смотрела только на непустой артикль. Через эту подмену german_surface объявляет форму
множественного числа документированным существительным в ЕДИНСТВЕННОМ числе, правило
«у множественного артикль всегда die» не срабатывает — так в базу и легла карточка
«der Handschuhe», которую владелец нашёл глазами.

Почему условие узкое, а не «не брать все снятые строки». Замер обеих правок на живой
базе 17.08.2026:
    не брать снятые строки      → род потеряли бы 1307 слов, приобрело 1, изменилось 0
    не брать негодные написания → род потеряли ровно 8, изменилось 0
Восемь — это ровно те формы множественного числа, которые убрали руками. Снятие само по
себе НЕ значит «слово неправильное»: строку снимают и за дубль, и за ротацию
освоенного, и род у таких строк верный.
"""

import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.article_authority as auth


class _Cur:
    """Отдаёт ответы по порядку запросов _load(): роды Wiktionary → стоп-лист → банк."""

    def __init__(self, wiki, bad_forms, bank):
        self._answers = [wiki, [(w,) for w in bad_forms], bank]
        self._i = -1

    def execute(self, sql, params=None):
        self._i += 1

    def fetchall(self):
        return self._answers[self._i] if self._i < len(self._answers) else []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur


def _load_with(*, wiki=(), bad_forms=(), bank=()):
    cur = _Cur(list(wiki), list(bad_forms), list(bank))

    @contextmanager
    def ctx(*a, **k):
        yield _Conn(cur)

    import backend.database as db
    auth._genus, auth._ambiguous, auth._loaded_at = {}, set(), 0.0
    with patch.object(db, "get_db_connection_context", ctx), \
         patch.object(auth, "_two_gender", lambda: set()):
        return auth._load()


class BadFormsSupplyNoGenderTests(unittest.TestCase):
    def tearDown(self):
        auth._genus, auth._ambiguous, auth._loaded_at = {}, set(), 0.0

    def test_blacklisted_plural_form_gives_no_gender(self):
        """Ровно тот случай: строка «das Fotos» лежит в банке, но написание признано
        негодным — род от неё брать нельзя."""
        genus, _amb = _load_with(bad_forms=["fotos"], bank=[("Fotos", "das")])
        self.assertNotIn("fotos", genus)

    def test_good_word_still_supplies_gender(self):
        genus, _amb = _load_with(bad_forms=["fotos"], bank=[("Foto", "das")])
        self.assertEqual(genus.get("foto"), "das")

    def test_retired_but_not_blacklisted_word_keeps_its_gender(self):
        """Снятие за дубль или ротацию освоенного — не повод терять род: таких слов
        1307, и род у них верный."""
        genus, _amb = _load_with(bank=[("Aktienmarkt", "der")])
        self.assertEqual(genus.get("aktienmarkt"), "der")

    def test_wiktionary_wins_over_the_bank(self):
        genus, _amb = _load_with(wiki=[("kurs", "m")], bank=[("Kurs", "die")])
        self.assertEqual(genus.get("kurs"), "der")

    def test_bad_form_does_not_poison_the_ambiguous_set(self):
        """Негодное написание не должно и объявлять слово двухродовым: иначе оно
        продолжало бы влиять на ответ, только с другой стороны."""
        genus, amb = _load_with(bad_forms=["fotos"],
                                bank=[("Fotos", "das"), ("Fotos", "die")])
        self.assertNotIn("fotos", amb)
        self.assertNotIn("fotos", genus)

    def test_two_articles_on_a_good_word_still_mean_ambiguous(self):
        genus, amb = _load_with(bank=[("Band", "der"), ("Band", "das")])
        self.assertIn("band", amb)
        self.assertNotIn("band", genus)


class NoStageAnswersForABadFormTests(unittest.TestCase):
    """Перекрыть одну ступень мало — вопрос просто уходит следующей. Замер 17.08.2026:
    «Seifenblasen» перестал брать род из банка и тут же взял «das» у правила композита."""

    def tearDown(self):
        auth._genus, auth._ambiguous, auth._loaded_at = {}, set(), 0.0
        auth._bad_forms.clear()

    def test_compound_rule_stays_silent_for_a_bad_form(self):
        _load_with(bad_forms=["seifenblasen"], wiki=[("seife", "f"), ("blasen", "n")])
        self.assertIsNone(auth.compound_article("Seifenblasen"))

    def test_door_says_it_knows_there_is_nothing_to_ask(self):
        """Ответ не «не знаем», а «негодная форма»: разница видна в отчётах."""
        _load_with(bad_forms=["fotos"], bank=[("Fotos", "das")])
        art, src = auth.authoritative_article("Fotos", allow_network=False)
        self.assertIsNone(art)
        self.assertEqual(src, "негодная форма слова")

    def test_network_is_not_asked_for_a_bad_form(self):
        """Живой Wiktionary тоже не спрашиваем: это деньги и задержка ради ответа,
        который мы всё равно не примем."""
        _load_with(bad_forms=["fotos"], bank=[("Fotos", "das")])
        with patch.object(auth, "_wiktionary_live", side_effect=AssertionError("спросили сеть")):
            art, _src = auth.authoritative_article("Fotos", allow_network=True)
        self.assertIsNone(art)


class TheFilterIsNarrowOnPurposeTests(unittest.TestCase):
    def test_only_the_plural_form_reason_is_filtered(self):
        """Причина «не нужно в быту» (8369 записей в стоп-листе) НЕ должна лишать слово
        рода: это про полезность в игре, а не про правильность написания."""
        import inspect
        src = inspect.getsource(auth._load)
        self.assertIn("форма множественного числа", src)
        self.assertNotIn("не нужно в быту", src)

    def test_retired_rows_are_not_excluded_wholesale(self):
        import inspect
        src = inspect.getsource(auth._load)
        bank_query = src[src.index("bt_3_article_sprint_nouns"):]
        self.assertNotIn("NOT retired", bank_query)


if __name__ == "__main__":
    unittest.main()
