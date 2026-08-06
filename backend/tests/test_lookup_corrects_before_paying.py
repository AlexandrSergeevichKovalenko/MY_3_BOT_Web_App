"""Правописание проверяется ДО того, как мы платим за разбор.

Как было. Вычитка стояла только на сохранении, а платим мы раньше — на поиске. Поэтому
за неверную форму мы честно платили GPT, строили ей карточку и заводили ей единицу, и
только текст правили потом. Живой след 06.08.2026: в слое лежит единица «das
Neugeborenes» — слова, которого в немецком нет; правильная карточка «das Neugeborene»
у человека при этом сохранена верно.

Денежное правило, которое сторожат эти тесты: вычитка зовётся ТОЛЬКО после промаха по
нашему собственному словарю. Слово, которое у нас уже есть, верное по определению —
на попадании не тратится ничего. А на промахе дешёвая вычитка часто ЭКОНОМИТ полный
разбор: исправленное написание нередко уже лежит у нас.
"""
import unittest
from unittest.mock import patch

from backend import backend_server as bs


class LookupCorrectsBeforePayingTests(unittest.TestCase):
    def test_word_we_already_have_costs_nothing(self):
        """Попадание в свой словарь — модель не зовётся вовсе."""
        calls = []
        with patch.object(bs, "_load_dictionary_item_from_pool", return_value={"word_de": "das Haus"}), \
             patch.object(bs, "_proofread_dictionary_phrase", side_effect=lambda *a, **k: calls.append(a) or ""):
            item, word = bs._dictionary_hit_or_corrected_word(
                word="das Haus", source_lang="de", target_lang="ru", user_id=1,
            )
        self.assertEqual(word, "das Haus")
        self.assertTrue(item)
        self.assertEqual(calls, [], "за слово из своего словаря заплатили вычиткой")

    def test_miss_gets_corrected_and_we_look_again(self):
        """Промах → правим написание → ищем исправленное у себя. Нашли — разбор не покупаем."""
        seen = []

        def fake_pool(*, word, source_lang, target_lang):
            seen.append(word)
            return {"word_de": word} if word == "das Neugeborene" else None

        with patch.object(bs, "_load_dictionary_item_from_pool", side_effect=fake_pool), \
             patch.object(bs, "_proofread_dictionary_phrase", return_value="das Neugeborene"):
            item, word = bs._dictionary_hit_or_corrected_word(
                word="das Neugeborenes", source_lang="de", target_lang="ru", user_id=1,
            )
        self.assertEqual(seen, ["das Neugeborenes", "das Neugeborene"])
        self.assertEqual(word, "das Neugeborene")
        self.assertEqual(item, {"word_de": "das Neugeborene"})

    def test_corrected_word_goes_on_even_when_we_do_not_have_it(self):
        """Не нашли и исправленное — в GPT уходит ИСПРАВЛЕННОЕ написание, не исходное."""
        with patch.object(bs, "_load_dictionary_item_from_pool", return_value=None), \
             patch.object(bs, "_proofread_dictionary_phrase", return_value="das Neugeborene"):
            item, word = bs._dictionary_hit_or_corrected_word(
                word="das Neugeborenes", source_lang="de", target_lang="ru", user_id=1,
            )
        self.assertIsNone(item)
        self.assertEqual(word, "das Neugeborene")

    def test_correction_result_passes_the_same_mechanical_door(self):
        """Модель может вернуть слово в кавычках или с хвостом — оно идёт через ту же чистку."""
        with patch.object(bs, "_load_dictionary_item_from_pool", return_value=None), \
             patch.object(bs, "_proofread_dictionary_phrase", return_value="«das Neugeborene» —"):
            _item, word = bs._dictionary_hit_or_corrected_word(
                word="das Neugeborenes", source_lang="de", target_lang="ru", user_id=1,
            )
        self.assertEqual(word, "das Neugeborene")

    def test_nothing_to_proofread_is_not_paid_for(self):
        """Ссылка, число, чужой язык — вычитка бессмысленна, модель не зовём."""
        for word, lang in (("https://example.com", "de"), ("2026", "de"), ("house", "en")):
            calls = []
            with patch.object(bs, "_load_dictionary_item_from_pool", return_value=None), \
                 patch.object(bs, "_proofread_dictionary_phrase",
                              side_effect=lambda *a, **k: calls.append(a) or ""):
                _item, out = bs._dictionary_hit_or_corrected_word(
                    word=word, source_lang=lang, target_lang="ru", user_id=1,
                )
            self.assertEqual(calls, [], word)
            self.assertEqual(out, word)

    def test_a_broken_corrector_never_blocks_the_lookup(self):
        """Модель отказала — человек всё равно получает ответ по своему написанию."""
        with patch.object(bs, "_load_dictionary_item_from_pool", return_value=None), \
             patch.object(bs, "_proofread_dictionary_phrase", return_value=""):
            item, word = bs._dictionary_hit_or_corrected_word(
                word="das Neugeborenes", source_lang="de", target_lang="ru", user_id=1,
            )
        self.assertIsNone(item)
        self.assertEqual(word, "das Neugeborenes")


if __name__ == "__main__":
    unittest.main()
