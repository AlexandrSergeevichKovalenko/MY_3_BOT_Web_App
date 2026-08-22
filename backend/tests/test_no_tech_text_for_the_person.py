# -*- coding: utf-8 -*-
"""Человеку — человеческий текст; техническая правда — в лог.

ГРУППА Ж главного плана. Замер 22.08.2026 по всему серверу: 26 мест отдавали клиенту
сырое сообщение исключения полем `error`, и фронт показывает это поле как текст
сообщения (`data.error` → `message`). То есть человек читал на экране
«KeyError: 'lemma_key'» или «connection already closed» — и не понимал ни что
случилось, ни что делать.

Разделено на два случая, и путать их нельзя:

    except ValueError   намеренное сообщение, его писали ДЛЯ человека  → оставлено (9)
    except Exception    техническая ошибка                            → заменено (17)

Технический текст не потерян: он уходит в лог со стеком и с именем места.
"""
from __future__ import annotations

import re
import unittest

import backend.backend_server as B

# Возврат сырого текста исключения клиенту.
СЫРАЯ_ОШИБКА = re.compile(r'jsonify\(\{"error": str\((?:exc|e)\)\}\)')


def _исходник() -> str:
    import inspect
    return inspect.getsource(B)


class СыраяОшибкаНеУходитЧеловекуTest(unittest.TestCase):
    def test_техническая_ошибка_не_отдаётся_клиенту(self):
        строки = _исходник().splitlines()
        плохие = []
        for i, s in enumerate(строки):
            if not СЫРАЯ_ОШИБКА.search(s):
                continue
            предыдущая = строки[i - 1] if i else ""
            if "ValueError" in предыдущая:
                continue  # намеренное сообщение — оно для человека и написано словами
            плохие.append(s.strip()[:80])
        self.assertEqual(
            плохие, [],
            "техническое сообщение исключения уходит человеку на экран:\n  "
            + "\n  ".join(плохие))

    def test_помощник_логирует_и_отвечает_по_человечески(self):
        from unittest import mock
        with B.app.test_request_context():
            with mock.patch.object(B.logging, "warning") as лог:
                ответ, код = B._сбой_запроса(KeyError("lemma_key"), что="список слов")
        self.assertEqual(код, 500)
        тело = ответ.get_json()
        self.assertNotIn("lemma_key", тело["error"], "техтекст просочился человеку")
        self.assertIn("Попробуйте", тело["error"])
        self.assertTrue(лог.called, "техническая правда не попала в лог")
        self.assertIn("список слов", str(лог.call_args))

    def test_код_ответа_сохраняется(self):
        with B.app.test_request_context():
            _ответ, код = B._сбой_запроса(RuntimeError("x"), что="проверка", код=400)
        self.assertEqual(код, 400)


class ЧислаНеДублируютсяНаЭкранеTest(unittest.TestCase):
    """ГРУППА Е: цена и нормы приходят от сервера, а не лежат копией во фронте."""

    def test_нормы_считает_та_же_функция_что_и_планировщик(self):
        import inspect
        src = inspect.getsource(B._preset_daily_budgets)
        self.assertIn("_preset_budget", src,
                      "нормы взяты не из той функции, по которой шлёт планировщик")

    def test_норму_не_выдумываем_когда_не_прочитали(self):
        from unittest import mock
        with mock.patch.dict("sys.modules", {"bot_3": None}):
            self.assertEqual(B._preset_daily_budgets(), {},
                             "не прочитав норму, подставили выдуманную")


if __name__ == "__main__":
    unittest.main()
