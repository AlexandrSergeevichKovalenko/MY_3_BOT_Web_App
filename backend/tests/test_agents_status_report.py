# -*- coding: utf-8 -*-
"""Отчёт «кто чем занят» не должен врать владельцу.

Три вещи, которые сломать легче всего и заметить труднее всего:
  • служебная запись harness'а показана как «просьба владельца» — отчёт подписывает
    сессию технической строкой, и владелец думает, что агент занят не тем;
  • длинный список молча обрезан — восемь строк читаются как «это всё»;
  • несколько сессий в одном каталоге не помечены — исчезает предупреждение о том,
    из-за чего правки перемешиваются.
"""
import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location(
    "agents_status_report", ROOT / "scripts" / "agents_status_report.py")
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


class TestOwnerRequest(unittest.TestCase):
    def _write(self, rows) -> Path:
        path = Path(self.enterContext(__import__("tempfile").TemporaryDirectory())) / "s.jsonl"
        path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows),
                        encoding="utf-8")
        return path

    def test_service_records_are_not_the_task(self):
        """Напоминание системы и доклад фоновой задачи — не слова владельца."""
        path = self._write([
            {"type": "user", "message": {"content": "почини артикль в быстром словаре"}},
            {"type": "user", "message": {"content": "<system-reminder>что-то</system-reminder>"}},
            {"type": "user", "message": {"content": "<task-notification>готово</task-notification>"}},
        ])
        self.assertEqual(report.last_owner_request(path), "почини артикль в быстром словаре")

    def test_short_reply_shown_only_when_nothing_else(self):
        """«ok» — ответ в разговоре, а не занятие; но если больше ничего нет, врать нельзя."""
        both = self._write([
            {"type": "user", "message": {"content": "разберись с ночным обогащением"}},
            {"type": "user", "message": {"content": "ok"}},
        ])
        self.assertEqual(report.last_owner_request(both), "разберись с ночным обогащением")

        only_short = self._write([{"type": "user", "message": {"content": "ok"}}])
        self.assertEqual(report.last_owner_request(only_short), "ok")

    def test_assistant_rows_are_ignored(self):
        path = self._write([
            {"type": "assistant", "message": {"content": "я сделал вот что"}},
            {"type": "user", "message": {"content": "проверь ещё раз все карточки"}},
        ])
        self.assertEqual(report.last_owner_request(path), "проверь ещё раз все карточки")


class TestFolding(unittest.TestCase):
    def test_long_list_says_how_many_are_hidden(self):
        rows = [f"строка {i}" for i in range(report.MAX_ROWS + 5)]
        folded = report._fold(rows)
        self.assertEqual(len(folded), report.MAX_ROWS + 1)
        self.assertEqual(folded[-1], "…и ещё 5")

    def test_short_list_is_untouched(self):
        rows = ["одна", "две"]
        self.assertEqual(report._fold(rows), rows)


class TestReportBody(unittest.TestCase):
    def _build(self, **kwargs):
        base = dict(sessions=[], processes=[], dirty=[], commits=[], hours=1,
                    now_label="22.08 15:00")
        base.update(kwargs)
        return report.build_report(**base)

    def test_several_sessions_in_one_directory_are_flagged(self):
        text = self._build(processes=[{"pid": "1", "dir": report.REPO.name},
                                      {"pid": "2", "dir": report.REPO.name}])
        self.assertIn("в ОДНОМ каталоге", text)
        self.assertIn("agent-worktree.sh", text)

    def test_own_directories_are_not_flagged(self):
        text = self._build(processes=[{"pid": "1", "dir": report.REPO.name},
                                      {"pid": "2", "dir": "repo-dictjudge"}])
        self.assertNotIn("в ОДНОМ каталоге", text)

    def test_empty_state_says_so_instead_of_showing_nothing(self):
        """Пустой раздел обязан быть подписан словами: пустота читается как поломка."""
        text = self._build()
        self.assertIn("тихо:", text)
        self.assertIn("ничего", text)

    def test_stale_edit_is_marked(self):
        text = self._build(dirty=[{"dir": "repo-x", "count": 2, "file": "a.py",
                                   "idle": (report.STALE_HOURS + 1) * 3600}])
        self.assertIn("висит", text)

    def test_fresh_edit_is_not_marked_stale(self):
        text = self._build(dirty=[{"dir": "repo-x", "count": 2, "file": "a.py",
                                   "idle": 120}])
        self.assertNotIn("висит", text)


class TestHumanAgo(unittest.TestCase):
    def test_scale(self):
        self.assertEqual(report.human_ago(30), "только что")
        self.assertEqual(report.human_ago(180), "3 мин назад")
        self.assertEqual(report.human_ago(7200), "2 ч назад")
        self.assertEqual(report.human_ago(4 * 24 * 3600), "4 дн назад")


if __name__ == "__main__":
    unittest.main()
