"""Карантин пула: «оставить» обязано ВЫПУСКАТЬ слово, а не просто не удалять его.

История дефекта. Карантин — не отдельный список, а счётчик неудачных попыток
`enrich_attempts` внутри response_json: дошёл до POOL_ENRICH_MAX_ATTEMPTS — слово
выпало из ночной очереди обогащения. В разборе «оставить» снимало галочку удаления и
на этом всё: счётчик оставался на потолке. Слово не удалялось, но и в работу не
возвращалось — и приходило владельцу в воскресном разборе неделю за неделей. Ровно
это и случилось с нормальным глаголом `einstecken` (08.08.2026): три раза подряд
запрос к GPT вернул пусто, слово осело в карантине навсегда.

Выпустить слово может только сброс счётчика — его и проверяем.
"""
import pathlib
import unittest

SRC = pathlib.Path(__file__).resolve().parents[1] / "database.py"
BOT = pathlib.Path(__file__).resolve().parents[2] / "bot_3.py"


class ReleaseResetsTheCounterTests(unittest.TestCase):
    def test_release_zeroes_enrich_attempts(self):
        src = SRC.read_text(encoding="utf-8")
        start = src.index("def release_pool_entries_from_quarantine")
        block = src[start:start + 2500]
        self.assertIn("'enrich_attempts', 0", block,
                      "без обнуления счётчика слово останется в карантине навсегда")
        self.assertIn("quarantine_releases", block,
                      "не считаем возвраты — не отличить слово, которое не собирается никогда")

    def test_release_is_a_separate_call_from_delete(self):
        """Удаление и возврат — разные действия над разными наборами id."""
        src = SRC.read_text(encoding="utf-8")
        self.assertIn("def delete_pool_entries_by_ids", src)
        self.assertIn("def release_pool_entries_from_quarantine", src)


class ApplyDoesBothTests(unittest.TestCase):
    """┌─ ПРИВЯЗКА ИСПРАВЛЕНА 29.08.2026. НЕ ВОЗВРАЩАТЬ ПОИСК ПО ВСЕМУ ФАЙЛУ. ────────┐
    │ Тесты искали `elif action == "go":` в bot_3.py ОТ НАЧАЛА ФАЙЛА. Пока обработчик │
    │ карантина был единственным с такими ветками, это работало. 29.08.2026 появился  │
    │ второй экран с кнопками (`handle_unit_decision_callback`, слова после двух      │
    │ отказов), он встал в файле выше — и тесты стали проверять ЧУЖОЙ блок, покраснев │
    │ на нетронутом карантине. Продукт был исправен, ломалась привязка.               │
    │ Теперь блок берётся ВНУТРИ своей функции, а не первый попавшийся в файле.       │
    └────────────────────────────────────────────────────────────────────────────────┘
    """

    def _карантинный_блок(self, метка: str, длина: int) -> str:
        src = BOT.read_text(encoding="utf-8")
        начало_функции = src.index("async def handle_quarantine_callback")
        сдвиг = src.index(метка, начало_функции)
        return src[сдвиг:сдвиг + длина]

    def test_apply_button_deletes_and_releases(self):
        """Кнопка внизу разбора должна делать оба действия за один тап: иначе
        «оставленные» слова тихо остаются в карантине и возвращаются через неделю."""
        block = self._карантинный_блок('elif action == "go":', 1200)
        self.assertIn("delete_pool_entries_by_ids", block)
        self.assertIn("release_pool_entries_from_quarantine", block)

    def test_close_changes_nothing_and_says_so(self):
        block = self._карантинный_блок('elif action == "x":', 600)
        self.assertIn("остались в карантине", block,
                      "«Закрыть» не должно выглядеть как решение — оно ничего не меняет")


if __name__ == "__main__":
    unittest.main()
