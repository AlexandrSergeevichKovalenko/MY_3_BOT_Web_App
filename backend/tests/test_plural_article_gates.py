"""Заслоны, после которых «das Probleme» перестаёт появляться.

Находка владельца: запрос «Проблемы» отдавал карточку «das Probleme». Замеры на
проде 29.07.2026:
  • таблица разобранных статей закрывает 0,6 % наших существительных (24 из 3991),
    поэтому артикль на быстром пути фактически выдавала модель;
  • модель на форме множественного отвечает про ЛЕММУ и ошибается в 10 случаях
    из 20 (Probleme→das, Bücher→das, Häuser→das, Tische→der, Männer→der).

Проверяется главное правило: артикль принадлежит поверхности, которую показываем.
У именительного множественного он может быть только «die», а когда число неизвестно —
не печатаем ничего.
"""

import unittest
from unittest.mock import patch

import backend.german_surface as gs


def _verdict(number, article, lemma="", confidence="high"):
    return {"number": number, "article": article, "lemma": lemma,
            "source": "тест", "confidence": confidence}


class QuickPathArticleGateTests(unittest.TestCase):
    """Быстрый путь: что попадает в компактную карточку сразу после перевода."""

    def setUp(self):
        import backend.backend_server as bs
        self.bs = bs

    def _attach(self, german, verdict, table_article=""):
        result = {"translation": german, "detected_source_lang": "ru"}
        table = {"article": table_article} if table_article else None
        with patch.object(gs, "german_surface", return_value=verdict), \
             patch.object(self.bs, "lookup_wiktionary_entry", return_value=table):
            return self.bs._attach_quick_translate_article(result, "Проблемы", "ru", "de")

    def test_plural_surface_gets_die_and_is_marked_as_a_form(self):
        out = self._attach("Probleme", _verdict(gs.PL, "die", "Problem"))
        self.assertEqual(out["article"], "die")
        self.assertEqual(out["grammatical_number"], gs.PL)
        self.assertEqual(out["lemma_de"], "Problem")

    def test_lemma_article_never_reaches_a_plural_surface(self):
        """Даже если старая таблица статей знает «das Problem», рядом с формой
        множественного этот артикль появиться не может."""
        out = self._attach("Probleme", _verdict(gs.PL, "die", "Problem"), table_article="das")
        self.assertEqual(out["article"], "die")

    def test_singular_keeps_its_own_article(self):
        out = self._attach("Problem", _verdict(gs.SG, "das"))
        self.assertEqual(out["article"], "das")
        self.assertNotIn("grammatical_number", out)

    def test_unknown_number_prints_no_article_at_all(self):
        """Пустое честнее выдуманного: пока не знаем, форма это или слово, артикля
        на экране нет — и «das Probleme» появиться неоткуда."""
        out = self._attach("Quastelbrumm", _verdict(gs.UNKNOWN, ""))
        self.assertEqual(out.get("article", ""), "")

    def test_table_of_lemmas_still_helps_a_word(self):
        """Попадание в словарь ЛЕММ само по себе значит «это слово», поэтому его
        артикль допустим — покрытие быстрого пути от заслона не страдает."""
        out = self._attach("Wortverbindung", _verdict(gs.UNKNOWN, ""), table_article="die")
        self.assertEqual(out["article"], "die")


class SavedHeadwordGateTests(unittest.TestCase):
    """Сохранение: что уезжает в общий пул и попадает ко ВСЕМ пользователям."""

    def setUp(self):
        import backend.backend_server as bs
        self.bs = bs

    def _normalize(self, german, verdict, article="", forms=None):
        payload = {"entry_kind": "word", "part_of_speech": "noun", "article": article,
                   "target_text": german, "word_de": german, "forms": forms or {}}
        with patch.object(gs, "german_surface", return_value=verdict), \
             patch.object(self.bs, "_authoritative_german_article", return_value="das"):
            return self.bs._apply_german_headword_normalization(
                payload=payload, source_lang="ru", target_lang="de")

    def test_confirmed_plural_is_saved_with_die_and_its_lemma(self):
        out = self._normalize("Probleme", _verdict(gs.PL, "die", "Problem"), article="das")
        self.assertEqual(out["article"], "die")
        self.assertEqual(out["word_de"], "die Probleme")
        self.assertEqual(out["lemma_de"], "Problem")
        self.assertEqual(out["grammatical_number"], gs.PL)

    def test_genus_of_the_lemma_is_not_forced_onto_a_plural(self):
        """Справочник родов знает «das» для леммы. На форме множественного этот
        ответ применять нельзя — иначе в пул уедет «das Probleme» для всех."""
        out = self._normalize("Probleme", _verdict(gs.PL, "die", "Problem"))
        self.assertNotEqual(out["article"], "das")

    def test_guess_never_rewrites_a_stored_article(self):
        """Догадка по окончанию принимает за формы настоящие слова («der Stürmer»
        — якобы форма от «Sturm»). Такое подозрение не даёт права ничего менять."""
        out = self._normalize("Stürmer", _verdict(gs.PL, "", "Sturm", confidence="low"),
                              article="der")
        self.assertEqual(out["article"], "der")
        self.assertEqual(out["word_de"], "der Stürmer")

    def test_singular_still_gets_the_authoritative_backstop(self):
        out = self._normalize("Kabel", _verdict(gs.SG, "das"), article="der")
        self.assertEqual(out["article"], "das")


class QuickArticleModelContractTests(unittest.TestCase):
    """Модель — последнее средство, и её ответ проверяется правилом, а не принимается."""

    def test_plural_answer_is_forced_to_die(self):
        import backend.openai_manager as om

        class _Resp:
            class _Choice:
                class _Msg:
                    content = '{"article":"das","number":"pl","lemma":"Problem"}'
                message = _Msg()
            choices = [_Choice()]
            usage = None

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test"}), \
             patch("backend.synthetic_load.build_sync_openai_client") as client:
            client.return_value.chat.completions.create.return_value = _Resp()
            facts = om.run_quick_article_facts(word="Probleme")
        self.assertEqual(facts["article"], "die")
        self.assertEqual(facts["number"], "pl")
        self.assertEqual(facts["lemma"], "Problem")


if __name__ == "__main__":
    unittest.main()
