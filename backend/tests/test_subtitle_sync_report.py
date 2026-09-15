# -*- coding: utf-8 -*-
"""Счётчик сдвига русских субтитров (`/subtitry`) раскладывает число на классы.

ПОВОД, 15.09.2026. Владелец просил числа по поломке «русские субтитры то отстают, то
идут вперёд». Правило владельца о числах: сырой счётчик не выносится — его раскладывают
на классы, и в отчёт идёт только то, что вправду дефект. Здесь это проверяется:

  • «катящаяся» дорожка с переводом → класс 1 (уедет на повторном просмотре);
  • номер перевода за концом немецкого списка → класс 2 (немецкий заменили);
  • дырка внутри переведённого куска → класс 3, и отдельно «модель вернула пустое»;
  • ручные субтитры без повторов → НЕ дефект, в числа не попадают;
  • ещё не заказанный хвост ролика → НЕ дырка.
"""
import unittest

from backend.subtitle_sync_report import audit_one_video, format_subtitle_sync_report


def _frames(*texts):
    return [{"start": i * 2.0, "duration": 2.0, "text": t} for i, t in enumerate(texts)]


ROLLING = _frames(
    "Ich sag, nein.",
    "Ich sag, nein.",
    "Das ist die Thrillerpfeife.",
    "Das ist die Thrillerpfeife.",
    "Und der hat ja so eine weite Hose,",
)  # склеивается в 3 строки
CLEAN = _frames("Guten Abend.", "Wie geht es dir?", "Mir geht es gut.")


class КлассыСчитаютсяРаздельно(unittest.TestCase):
    def test_катящаяся_дорожка_с_переводом_это_класс_1(self):
        report = audit_one_video({
            "video_id": "roll1", "items": ROLLING, "is_generated": True,
            "translations": {"ru:0": "Я говорю, нет.", "ru:1": "Это триллер-свисток.",
                             "ru:2": "И у него широкие штаны,"},
        })
        self.assertTrue(report["rolling"])
        self.assertEqual(report["cues_raw"], 5)
        self.assertEqual(report["cues_rolled"], 3)
        # Строки 1 и 2 склейка перенесёт на другие места; строка 0 остаётся на месте.
        self.assertEqual(report["will_shift"] + report["will_vanish"], 2)
        self.assertEqual(report["orphan"], 0)
        self.assertEqual(report["gaps"], 0)

    def test_ручные_субтитры_в_числа_не_попадают(self):
        report = audit_one_video({
            "video_id": "clean1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "ru:1": "Как дела?", "ru:2": "Хорошо."},
        })
        self.assertFalse(report["rolling"])
        self.assertEqual(report["will_shift"], 0)
        self.assertEqual(report["will_vanish"], 0)
        self.assertEqual(report["orphan"], 0)
        self.assertEqual(report["gaps"], 0)

    def test_номер_за_концом_немецкого_списка_это_класс_2(self):
        report = audit_one_video({
            "video_id": "stale1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "ru:7": "Из прошлой нарезки."},
        })
        self.assertEqual(report["orphan"], 1)
        self.assertEqual(report["gaps"], 0, "за концом списка дырок быть не может")

    def test_дырка_внутри_переведённого_куска_это_класс_3(self):
        report = audit_one_video({
            "video_id": "gap1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "ru:2": "Хорошо."},
        })
        self.assertEqual(report["gaps"], 1)
        self.assertEqual(report["empty_from_model"], 0)

    def test_пустой_ответ_модели_отделён_от_дырки(self):
        report = audit_one_video({
            "video_id": "empty1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "ru:1": "", "ru:2": "Хорошо."},
        })
        self.assertEqual(report["empty_from_model"], 1)
        self.assertEqual(report["gaps"], 0, "ключ есть — это не дырка, это пустой ответ")

    def test_незаказанный_хвост_ролика_не_дырка(self):
        report = audit_one_video({
            "video_id": "tail1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер."},
        })
        self.assertEqual(report["gaps"], 0)

    def test_старый_ключ_без_языка_и_новый_это_одна_строка(self):
        # Русский пишется дважды: «ru:0» и «0». Считать его надо один раз.
        report = audit_one_video({
            "video_id": "dup1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "0": "Добрый вечер."},
        })
        self.assertEqual(report["lines"], 1)
        self.assertEqual(report["languages"], ["ru"])

    def test_другой_язык_считается_своим_рядом(self):
        report = audit_one_video({
            "video_id": "multi1", "items": CLEAN, "is_generated": False,
            "translations": {"ru:0": "Добрый вечер.", "es:0": "Buenas tardes."},
        })
        self.assertEqual(report["languages"], ["es", "ru"])
        self.assertEqual(report["lines"], 2)

    def test_непонятный_ключ_не_притворяется_нулевым_номером(self):
        report = audit_one_video({
            "video_id": "junk1", "items": CLEAN, "is_generated": False,
            "translations": {"мусор": "…", "ru:x": "…", "ru:0": "Добрый вечер."},
        })
        self.assertEqual(report["lines"], 1)


class ОтчётГоворитЧеловеческимЯзыком(unittest.TestCase):
    def test_пустая_база_не_выдаёт_ноль_за_чистоту(self):
        text = format_subtitle_sync_report({"videos": 0})
        self.assertIn("нет ни одной дорожки", text)

    def test_ненастроенный_счётчик_пачек_не_называется_нулём(self):
        text = format_subtitle_sync_report({
            "videos": 3, "videos_with_translation": 2, "lines_total": 40,
            "lines_shift": 4, "lines_vanish": 0, "lines_orphan": 0, "lines_gaps": 0,
            "lines_empty": 0, "videos_shift": 1, "videos_orphan": 0, "videos_gaps": 0,
            "videos_rolling": 1, "worst": [], "count_mismatch": {"measured": False},
        })
        self.assertIn("счётчик ещё не поставлен", text)
        self.assertNotIn("<b>4. Модель вернула не столько строк:</b> 0", text)


if __name__ == "__main__":
    unittest.main()
