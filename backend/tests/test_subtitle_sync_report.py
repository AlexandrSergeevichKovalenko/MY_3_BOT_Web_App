# -*- coding: utf-8 -*-
"""Счётчик сдвига русских субтитров (`/subtitry`) раскладывает число на классы.

ПОВОД, 15.09.2026. Владелец просил числа по поломке «русские субтитры то отстают, то
идут вперёд». Правило владельца о числах: сырой счётчик не выносится — его раскладывают
на классы, и в отчёт идёт только то, что вправду дефект. Здесь это проверяется:

  • дорожка ещё не склеена → класс 1 (остаток работы, чинится сам);
  • номер перевода за концом СКЛЕЕННОГО списка → класс 2 (немецкий заменили);
  • тот же номер у НЕсклеенной дорожки → класс 1 («потеряется при склейке»), а НЕ класс 2:
    это разные причины и разная цена, складывать их нельзя;
  • дырка внутри переведённого куска → класс 3, и отдельно «модель вернула пустое»;
  • ручные субтитры без повторов → не дефект;
  • ещё не заказанный хвост ролика → не дырка.

Главное, что держит этот файл: разбор НЕ пытается угадать пространство номеров по
содержимому. Он смотрит на флаг `cues_rolled`. Склейка не идемпотентна, и «склеить ещё
раз, чтобы проверить» — это ровно тот сдвиг, который мы чиним.
"""
import unittest

from backend.subtitle_sync_report import audit_one_video, format_subtitle_sync_report


def _frames(*texts):
    return [{"start": i * 2.0, "duration": 2.0, "text": t} for i, t in enumerate(texts)]


RAW_ROLLING = _frames(
    "Ich sag, nein.",
    "Ich sag, nein.",
    "Das ist die Thrillerpfeife.",
    "Das ist die Thrillerpfeife.",
    "Und der hat ja so eine weite Hose,",
)  # склеивается в 3 строки
CLEAN = _frames("Guten Abend.", "Wie geht es dir?", "Mir geht es gut.")


class КлассыСчитаютсяРаздельно(unittest.TestCase):
    def test_несклеенная_дорожка_это_остаток_работы(self):
        report = audit_one_video({
            "video_id": "roll1", "items": RAW_ROLLING, "is_generated": True,
            "cues_rolled": False,
            "translations": {"ru:0": "Я говорю, нет.", "ru:1": "Это триллер-свисток.",
                             "ru:2": "И у него широкие штаны,"},
        })
        self.assertTrue(report["pending_roll"])
        self.assertEqual(report["cues_stored"], 5)
        self.assertEqual(report["cues_effective"], 3, "после склейки останется 3 строки")
        # Перевод писался под склеенными номерами — после склейки он сходится целиком.
        self.assertEqual(report["orphan"], 0)
        self.assertEqual(report["gaps"], 0)

    def test_склеенная_дорожка_больше_не_ждёт_работы(self):
        report = audit_one_video({
            "video_id": "roll2", "items": CLEAN, "is_generated": True, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "ru:1": "Как дела?", "ru:2": "Хорошо."},
        })
        self.assertFalse(report["pending_roll"])
        self.assertEqual(report["orphan"], 0)
        self.assertEqual(report["gaps"], 0)

    def test_флаг_главнее_содержимого(self):
        # Дорожка ПОМЕЧЕНА склеенной, хотя повторы в ней ещё видны. Разбор обязан верить
        # флагу: вторая склейка сдвинула бы номера — это и есть чинимый дефект.
        report = audit_one_video({
            "video_id": "flag1", "items": RAW_ROLLING, "is_generated": True,
            "cues_rolled": True, "translations": {"ru:4": "последняя строка"},
        })
        self.assertFalse(report["pending_roll"])
        self.assertEqual(report["cues_effective"], 5, "склейку второй раз не применяем")
        self.assertEqual(report["orphan"], 0, "номер 4 лежит в пределах пяти реплик")

    def test_номер_за_концом_склеенного_списка_это_класс_2(self):
        report = audit_one_video({
            "video_id": "stale1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "ru:7": "Из прошлой нарезки."},
        })
        self.assertEqual(report["orphan"], 1)
        self.assertEqual(report["gaps"], 0, "за концом списка дырок быть не может")

    def test_дырка_внутри_переведённого_куска_это_класс_3(self):
        report = audit_one_video({
            "video_id": "gap1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "ru:2": "Хорошо."},
        })
        self.assertEqual(report["gaps"], 1)
        self.assertEqual(report["empty_from_model"], 0)

    def test_пустой_ответ_модели_отделён_от_дырки(self):
        report = audit_one_video({
            "video_id": "empty1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "ru:1": "", "ru:2": "Хорошо."},
        })
        self.assertEqual(report["empty_from_model"], 1)
        self.assertEqual(report["gaps"], 0, "ключ есть — это не дырка, это пустой ответ")

    def test_незаказанный_хвост_ролика_не_дырка(self):
        report = audit_one_video({
            "video_id": "tail1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер."},
        })
        self.assertEqual(report["gaps"], 0)

    def test_старый_ключ_без_языка_и_новый_это_одна_строка(self):
        # Русский пишется дважды: «ru:0» и «0». Считать его надо один раз.
        report = audit_one_video({
            "video_id": "dup1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "0": "Добрый вечер."},
        })
        self.assertEqual(report["lines"], 1)
        self.assertEqual(report["languages"], ["ru"])

    def test_другой_язык_считается_своим_рядом(self):
        report = audit_one_video({
            "video_id": "multi1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"ru:0": "Добрый вечер.", "es:0": "Buenas tardes."},
        })
        self.assertEqual(report["languages"], ["es", "ru"])
        self.assertEqual(report["lines"], 2)

    def test_непонятный_ключ_не_притворяется_нулевым_номером(self):
        report = audit_one_video({
            "video_id": "junk1", "items": CLEAN, "is_generated": False, "cues_rolled": True,
            "translations": {"мусор": "…", "ru:x": "…", "ru:0": "Добрый вечер."},
        })
        self.assertEqual(report["lines"], 1)


class ОдинИТотЖеНомерНеСчитаетсяДважды(unittest.TestCase):
    def test_потеря_при_склейке_и_замена_немецкого_это_разные_классы(self):
        from unittest import mock

        from backend.subtitle_sync_report import subtitle_sync_state

        rows = [
            # Несклеенная дорожка с переводом под СЫРЫМИ номерами (до 12.07.2026):
            # после склейки строк станет 3, а номер 4 указывать будет некуда.
            {"video_id": "old1", "items": RAW_ROLLING, "is_generated": True,
             "cues_rolled": False, "language": "de", "updated_at": None,
             "translations": {"ru:0": "а", "ru:4": "б"}},
            # Склеенная дорожка, у которой заменили немецкие реплики.
            {"video_id": "new1", "items": CLEAN, "is_generated": False,
             "cues_rolled": True, "language": "de", "updated_at": None,
             "translations": {"ru:0": "в", "ru:9": "г"}},
        ]
        # Счётчик пачек подменяем: без этого тест пошёл бы в БОЕВУЮ базу за таблицей
        # bt_3_subtitle_translate_stats — прогон тестов туда ходить не имеет права.
        with mock.patch("backend.database.iter_youtube_transcripts_for_audit",
                        return_value=iter(rows)), \
             mock.patch("backend.subtitle_translate_counter.mismatch_counters",
                        return_value={"total": 0, "mismatched": 0, "refused": 0, "since": ""}):
            state = subtitle_sync_state()
        self.assertEqual(state["lines_lost_on_roll"], 1, "потеря при склейке — класс 1")
        self.assertEqual(state["lines_orphan"], 1, "замена немецкого — класс 2")
        self.assertEqual(state["videos_pending_roll"], 1)
        self.assertEqual(state["videos_orphan"], 1)


class ОтчётГоворитЧеловеческимЯзыком(unittest.TestCase):
    def test_пустая_база_не_выдаёт_ноль_за_чистоту(self):
        text = format_subtitle_sync_report({"videos": 0})
        self.assertIn("нет ни одной дорожки", text)

    def test_ненастроенный_счётчик_пачек_не_называется_нулём(self):
        text = format_subtitle_sync_report({
            "videos": 3, "videos_with_translation": 2, "lines_total": 40,
            "videos_pending_roll": 1, "lines_lost_on_roll": 0,
            "lines_orphan": 0, "lines_gaps": 0, "lines_empty": 0,
            "videos_orphan": 0, "videos_gaps": 0,
            "worst": [], "count_mismatch": {"measured": False},
        })
        self.assertIn("счётчик ещё не поставлен", text)
        self.assertNotIn("<b>4. Модель вернула не столько строк:</b> 0", text)

    def test_числа_согласованы_по_русски(self):
        text = format_subtitle_sync_report({
            "videos": 5, "videos_with_translation": 5, "lines_total": 40,
            "videos_pending_roll": 1, "lines_lost_on_roll": 1,
            "lines_orphan": 2, "lines_gaps": 5, "lines_empty": 0,
            "videos_orphan": 1, "videos_gaps": 3,
            "worst": [], "count_mismatch": {"measured": False},
        })
        self.assertIn("1 строка", text)
        self.assertIn("2 строки", text)
        self.assertIn("5 строк", text)
        self.assertIn("1 ролика", text)
        self.assertIn("3 роликов", text)


if __name__ == "__main__":
    unittest.main()
