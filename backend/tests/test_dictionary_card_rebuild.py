"""Сброс разбора делает слово пересобираемым, а карточку — не теряемой.

Зачем кнопка вообще: слово отдаётся из общего пула БЕЗ обращения к модели, поэтому
«удалить и сохранить заново» вернёт ту же самую кривую карточку. Сброс снимает разбор
во всех хранилищах сразу, и только тогда ночной добор берётся за слово снова.

Здесь проверяется чистая функция чистки тела карточки — та, на которой держится всё
остальное: если она оставит хоть один блок разбора, слово так и не попадёт в очередь,
а если снесёт опознание — ночь не поймёт, что и в какую сторону разбирать.
"""

import unittest

import backend.backend_server as bs
from backend.database import _strip_dictionary_card_body


FULL_CARD = {
    # опознание
    "source_lang": "de", "target_lang": "ru",
    "source_text": "der Beifall", "target_text": "аплодисменты",
    "word_de": "der Beifall", "word_ru": "аплодисменты",
    "translation_de": "der Beifall", "translation_ru": "аплодисменты",
    "entry_kind": "word", "part_of_speech": "noun", "article": "der",
    # разбор
    "usage_examples": [{"source": "Der Beifall war laut.", "target": "Аплодисменты были громкими."}],
    "dictionary_senses": [
        {"rank": 1, "value": "аплодисменты", "context": "зал", "example_source": "…", "example_target": "…"},
        {"rank": 2, "value": "одобрение", "context": "переносное"},
    ],
    "meanings": {"primary": {"value": "аплодисменты", "context": "зал"}},
    "translations": [{"value": "аплодисменты"}, {"value": "одобрение"}],
    "forms": {"plural": None, "genitive": "des Beifalls"},
    "government_patterns": [{"case": "Dativ", "pattern": "jemandem Beifall spenden"}],
    "common_collocations": [{"value": "tosender Beifall"}],
    "synonyms": ["Applaus"], "memory_tip": "…", "etymology_note": "…",
    "enrich_attempts": "3", "enrich_last_error": "timeout",
}


class DictionaryCardRebuildTests(unittest.TestCase):
    def test_full_card_becomes_thin_after_reset(self):
        self.assertFalse(bs._dictionary_payload_needs_enrichment(FULL_CARD))
        stripped = _strip_dictionary_card_body(FULL_CARD)
        self.assertTrue(
            bs._dictionary_payload_needs_enrichment(stripped),
            "после сброса карточка обязана считаться пустой — иначе ночь её не возьмёт",
        )

    def test_identity_survives_the_reset(self):
        stripped = _strip_dictionary_card_body(FULL_CARD)
        for key in ("source_lang", "target_lang", "source_text", "target_text",
                    "word_de", "word_ru", "entry_kind", "part_of_speech", "article"):
            with self.subTest(key=key):
                self.assertEqual(stripped.get(key), FULL_CARD[key])

    def test_every_analysis_block_is_gone(self):
        stripped = _strip_dictionary_card_body(FULL_CARD)
        for key in ("usage_examples", "dictionary_senses", "meanings", "translations",
                    "forms", "government_patterns", "common_collocations", "synonyms",
                    "memory_tip", "etymology_note"):
            with self.subTest(key=key):
                self.assertNotIn(key, stripped)

    def test_quarantine_counter_is_cleared(self):
        """Слово могло стоять в карантине «модель три раза не смогла». Не снять счётчик —
        значит отправить его в очередь, из которой оно тут же выпадет."""
        stripped = _strip_dictionary_card_body(FULL_CARD)
        self.assertNotIn("enrich_attempts", stripped)
        self.assertNotIn("enrich_last_error", stripped)

    def test_empty_and_broken_payloads_do_not_raise(self):
        for payload in (None, {}, "", [], "не json"):
            with self.subTest(payload=payload):
                self.assertIsInstance(_strip_dictionary_card_body(payload), dict)


if __name__ == "__main__":
    unittest.main()
