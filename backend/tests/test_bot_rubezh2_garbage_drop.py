import unittest
import bot_3


class Rubezh2GarbageDropTests(unittest.TestCase):
    def test_garbage_results_detected(self):
        for prep in [
            {"options": [{"source": "/\\", "target": "нет подходящего немецкого слова: неверный ввод"}]},
            {"options": [{"source": "<", "target": "—"}]},
            {"options": []},
            {"options": [{"source": "x", "target": "no valid german word"}]},
            {},
        ]:
            self.assertTrue(bot_3._dictionary_result_is_garbage(prep), msg=repr(prep))

    def test_real_translations_kept(self):
        for prep in [
            {"options": [{"source": "Strafmaß", "target": "мера наказания"}]},
            {"options": [{"source": "begegnen", "target": "встречать"}]},
            # at least one real option among garbage → keep
            {"options": [{"source": "x", "target": "неверный ввод"},
                         {"source": "Haus", "target": "дом"}]},
        ]:
            self.assertFalse(bot_3._dictionary_result_is_garbage(prep), msg=repr(prep))


if __name__ == "__main__":
    unittest.main()
