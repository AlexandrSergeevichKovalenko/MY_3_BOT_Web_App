# -*- coding: utf-8 -*-
"""Склейка выполняется РОВНО ОДИН РАЗ и только на сервере.

ПОВОД, 15.09.2026. Русские субтитры уезжали вперёд, потому что склейка «катящихся»
кадров жила в браузере: у реплики было два номера, и перевод перекладывался повторно.
Починка — перенести склейку на сервер и выполнять её единожды. Класс не должен
вернуться, поэтому здесь закреплено:

  • в браузере склейки больше НЕТ (иначе она снова применится вторым слоем);
  • сервер склеивает в одном месте — там, где рождается список реплик;
  • дорожка, помеченная склеенной, второй раз не склеивается;
  • перевод при переносе НЕ перекладывается (он уже в склеенных номерах), удаляются
    только номера, указывающие за конец списка.
"""
import pathlib
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
APP_JSX = ROOT / "frontend" / "src" / "App.jsx"
SERVER = ROOT / "backend" / "backend_server.py"


def _frames(*texts):
    return [{"start": i * 2.0, "duration": 2.0, "text": t} for i, t in enumerate(texts)]


ROLLING = _frames(
    "Ich sag, nein.",
    "Ich sag, nein.",
    "Das ist die Thrillerpfeife.",
    "Das ist die Thrillerpfeife.",
    "Und der hat ja so eine weite Hose,",
)  # 5 кадров → 3 строки


class СклейкиВБраузереБольшеНет(unittest.TestCase):
    def test_функция_склейки_удалена_из_app_jsx(self):
        text = APP_JSX.read_text(encoding="utf-8")
        self.assertNotIn("function derollTranscriptCues", text,
                         "склейка вернулась в браузер — номера снова разъедутся")
        self.assertNotIn("_derollNormWord", text)

    def test_перекладки_номеров_перевода_в_браузере_нет(self):
        text = APP_JSX.read_text(encoding="utf-8")
        self.assertNotIn("mappedTranslations", text,
                         "перевод снова перекладывают по карте — это и был сдвиг")

    def test_запрет_записан_рядом_в_коде(self):
        # Следующий агент должен увидеть, что это место уже разобрано.
        text = APP_JSX.read_text(encoding="utf-8")
        self.assertIn("НЕ ВОЗВРАЩАТЬ СЮДА СКЛЕЙКУ", text)


class СерверСклеиваетВОдномМесте(unittest.TestCase):
    def test_список_реплик_рождается_склеенным(self):
        text = SERVER.read_text(encoding="utf-8")
        start = text.index("def _build_youtube_transcript_result(")
        end = text.index("def _fetch_youtube_transcript(", start)
        body = text[start:end]
        self.assertIn("deroll_transcript_cues", body,
                      "через эту функцию проходят все четыре источника субтитров")
        self.assertIn('"cues_rolled": True', body,
                      "тот, кто будет сохранять, обязан знать: номера уже окончательные")


class ПереносНакопленного(unittest.TestCase):
    def test_перевод_в_склеенных_номерах_не_теряется(self):
        from backend.subtitle_cue_migration import plan_roll

        rolled, drop = plan_roll({
            "items": ROLLING,
            "translations": {"ru:0": "Я говорю, нет.", "ru:1": "Это триллер-свисток.",
                             "ru:2": "И у него широкие штаны,"},
        })
        self.assertEqual(len(rolled), 3)
        self.assertEqual(drop, [], "перевод писался под склеенными номерами — он сходится сам")

    def test_номера_указывающие_в_пустоту_удаляются(self):
        from backend.subtitle_cue_migration import plan_roll

        _rolled, drop = plan_roll({
            "items": ROLLING,
            "translations": {"ru:0": "есть", "ru:4": "запись до 12.07.2026", "4": "она же"},
        })
        self.assertEqual(sorted(drop), ["4", "ru:4"])

    def test_ночной_проход_считает_и_склеенные_и_несмогли(self):
        from backend.subtitle_cue_migration import roll_pending_cues

        rows = [
            {"video_id": "ok1", "items": ROLLING, "translations": {"ru:0": "а"}},
            {"video_id": "", "items": ROLLING, "translations": {}},  # без id — не склеить
        ]
        saved = []
        with mock.patch("backend.database.fetch_unrolled_youtube_transcripts", return_value=rows), \
             mock.patch("backend.database.store_rolled_youtube_cues",
                        side_effect=lambda vid, items, **kw: saved.append((vid, items, kw))):
            report = roll_pending_cues(limit=10)
        self.assertEqual(report["looked_at"], 2)
        self.assertEqual(report["rolled"], 1)
        self.assertEqual(report["failed"], 1)
        self.assertEqual(report["cues_removed"], 2, "из пяти кадров осталось три строки")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0][0], "ok1")
        self.assertEqual(len(saved[0][1]), 3)

    def test_сбой_записи_не_прячется_в_успех(self):
        from backend.subtitle_cue_migration import roll_pending_cues

        rows = [{"video_id": "boom", "items": ROLLING, "translations": {}}]
        with mock.patch("backend.database.fetch_unrolled_youtube_transcripts", return_value=rows), \
             mock.patch("backend.database.store_rolled_youtube_cues",
                        side_effect=RuntimeError("база недоступна")):
            report = roll_pending_cues(limit=10)
        self.assertEqual(report["rolled"], 0)
        self.assertEqual(report["failed"], 1)

    def test_отчёт_молчит_только_когда_вправду_нечего_сказать(self):
        from backend.subtitle_cue_migration import format_roll_sweep_report

        text = format_roll_sweep_report({"rolled": 3, "cues_removed": 40,
                                         "translations_dropped": 2, "failed": 1,
                                         "orphans_removed": 7, "orphan_videos": 2})
        self.assertIn("склеено дорожек за ночь — 3", text)
        self.assertIn("Строк перевода удалено: 2", text)
        self.assertIn("убрано: 7 строк у 2 роликов", text)
        self.assertIn("Не смогли починить: 1", text)

    def test_осиротевший_перевод_убирается_только_у_склеенных(self):
        from backend.subtitle_cue_migration import drop_orphan_translation_keys

        rows = [
            # Склеенная дорожка: номер 9 указывает за конец трёх реплик — убрать.
            {"video_id": "rolled1", "cues_rolled": True,
             "items": [{"text": "a"}, {"text": "b"}, {"text": "c"}],
             "translations": {"ru:0": "а", "ru:9": "в пустоту", "ru#1-9": "тоже в пустоту"}},
            # Несклеенная: её приведёт в порядок сама склейка, трогать нельзя.
            {"video_id": "raw1", "cues_rolled": False,
             "items": [{"text": "a"}, {"text": "b"}],
             "translations": {"ru:7": "пока не наше дело"}},
        ]
        deleted = []
        with mock.patch("backend.database.iter_youtube_transcripts_for_audit",
                        return_value=iter(rows)), \
             mock.patch("backend.database.delete_youtube_translation_keys",
                        side_effect=lambda vid, keys: (deleted.append((vid, sorted(keys))),
                                                       len(keys))[1]):
            report = drop_orphan_translation_keys(limit_videos=50)
        self.assertEqual(report["videos_cleaned"], 1)
        self.assertEqual(report["keys_removed"], 2)
        self.assertEqual(deleted, [("rolled1", ["ru#1-9", "ru:9"])])


if __name__ == "__main__":
    unittest.main()
