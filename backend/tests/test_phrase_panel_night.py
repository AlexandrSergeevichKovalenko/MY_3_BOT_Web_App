# -*- coding: utf-8 -*-
"""Панель трёх голосов работает НОЧЬЮ САМА — и честно молчит, когда работать нельзя.

ПОВОД, 31.08.2026. Панель жила разовым скриптом, который запускался руками. Замер по
живой базе того дня: 1 302 карточки фраз она не видела ни разу, 89 из них появились за
последнюю неделю — дыра наполнялась дальше. Владелец: «ставь» — ночной порцией, с
потолком расхода и строкой в утренний отчёт.

Здесь заперто то, что нельзя потерять при следующей правке.
"""
import os
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


def _панель_без_сети(self, budget_usd: float = 0.0) -> None:
    """Панель без клиентов: тест не ходит в сеть и не платит."""
    self.budget_usd = float(budget_usd)
    self.cost = 0.0


class ОдинМеханизмНаДваПрогонаTests(unittest.TestCase):
    """Скрипт и ночь судят ОДНИМ кодом. Две копии правил через полгода разойдутся."""

    def test_the_script_is_only_a_wrapper(self):
        src = _src("scripts/dict_phrase_panel_audit.py")
        self.assertIn("from backend import phrase_panel as pp", src)
        self.assertNotIn("SYSTEM = ", src, "промпт снова живёт двумя копиями")
        self.assertNotIn("def judge(", src, "правила судейства снова скопированы в скрипт")

    def test_the_night_job_calls_the_same_batch(self):
        bot = _src("bot_3.py")
        i = bot.index("def _run_phrase_panel_night_safe(")
        тело = bot[i:i + 1200]
        self.assertIn("from backend.phrase_panel import run_batch", тело)
        self.assertIn('_record_sched_heartbeat("phrase_panel_night", "failed"', тело,
                      "упавшая ночная работа снова неотличима от сделанной")

    def test_the_job_is_actually_scheduled(self):
        bot = _src("bot_3.py")
        i = bot.index("_run_phrase_panel_night_safe,\n            \"cron\",")
        self.assertIn("PHRASE_PANEL_HOUR", bot[i:i + 500])
        self.assertIn("max_instances=1", bot[i:i + 500])


class ХудшаяПроверкаНеЗапускаетсяВовсеTests(unittest.TestCase):
    """⛔ Два голоса одного производителя — это не панель, а её видимость.

    Замер 23.08.2026: две модели OpenAI расходятся в 15% случаев (1 718 вопросов
    владельцу), три голоса с чужим производителем — в 2,5% (286). Работать «чем есть»
    здесь означает молча ухудшить проверку и завалить его очередь."""

    def test_no_gemini_key_means_no_run_at_all(self):
        from backend import phrase_panel as pp
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": ""}):
            причина = pp.unavailable_reason()
        self.assertIn("GEMINI_API_KEY", причина)
        # ⛔ ПОДЪЁМ СТАРЫХ ОТМЕТОК ТОЖЕ ЗАМОКАН. Он идёт ДО проверки ключей и ходит в
        # базу: без этой заглушки прогон тестов 31.08.2026 завёл 90 живых вопросов в
        # боевой базе. Тесты не трогают прод — правило проекта, проверенное болью.
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": ""}), \
             patch.object(pp, "count_unchecked", return_value=7), \
             patch.object(pp, "поднять_старые_отметки", return_value=0), \
             patch.object(pp, "count_personal_backlog", return_value=0), \
             patch.object(pp, "unchecked_units") as отбор:
            отчёт = pp.run_batch(limit=5)
        self.assertIn("пропущено", отчёт)
        self.assertEqual(отчёт["проверено"], 0)
        отбор.assert_not_called()          # ни одного платного запроса

    def test_the_morning_report_says_why_it_did_not_run(self):
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:i + 3000]
        self.assertIn('meta.get("пропущено")', тело)
        self.assertIn("не работала", тело)


class ЧтоНеПроверилиНеПомечаемTests(unittest.TestCase):
    """Отметка значит «мы это видели». Поставить её на непроверенное — соврать себе."""

    def test_the_gemini_voice_goes_over_plain_http(self):
        """⛔ `google-genai` нет в requirements проде. Библиотека здесь означала бы,
        что третий голос не отвечает НИКОГДА, а два оставшихся одинаковы."""
        import ast
        src = _src("backend/phrase_panel.py")
        # Смотрим ИМПОРТЫ, а не текст файла: в рамке-предупреждении эта строка стоит
        # нарочно — как запрет её возвращать.
        импорты = set()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.ImportFrom):
                импорты.add(f"{node.module}.{node.names[0].name}")
            elif isinstance(node, ast.Import):
                импорты.update(a.name for a in node.names)
        self.assertNotIn("google.genai", импорты)
        self.assertIn("generativelanguage.googleapis.com", src)
        реквизиты = _src("requirements.txt")
        self.assertNotIn("google-genai", реквизиты,
                         "пакет появился — тогда решение про HTTP надо пересмотреть")

    def test_a_card_stopped_by_the_budget_keeps_no_mark(self):
        from backend import phrase_panel as pp
        строки = [(1, "eine Idee zu eigen machen", "collocation", {}, "перенять мысль")]
        with patch.object(pp, "unavailable_reason", return_value=""), \
             patch.object(pp, "третий_голос_молчит", return_value=""), \
             patch.object(pp, "поднять_старые_отметки", return_value=0), \
             patch.object(pp, "count_personal_backlog", return_value=0), \
             patch.object(pp, "unchecked_units", return_value=строки), \
             patch.object(pp, "count_unchecked", return_value=1), \
             patch.object(pp, "_записать_отметку") as отметка, \
             patch.object(pp.Panel, "__init__", _панель_без_сети), \
             patch.object(pp.Panel, "judge", side_effect=pp.BudgetSpent("кончились")):
            отчёт = pp.run_batch(limit=1)
        отметка.assert_not_called()
        self.assertTrue(отчёт["остановлено потолком"])
        self.assertEqual(отчёт["проверено"], 0)

    def test_a_checked_card_is_never_taken_again(self):
        """Один раз посмотрели — больше не смотрим и денег второй раз не платим.

        ⚠ ПРАВИЛО УТОЧНЕНО 02.09.2026, и это не смягчение. Отбор по-прежнему идёт по
        отметке, а не по дате, — но отметка теперь говорит, ЧТО именно судили
        (`judged_ru`). Карточка возвращается ровно в одном случае: перевод на экране
        стал другим, то есть прежний вердикт вынесен о тексте, которого больше нет.
        Без этого 1227 карточек навсегда остались бы осуждёнными по пустому русскому.
        """
        from backend.phrase_panel import _где_судить
        src = _src("backend/phrase_panel.py")
        i = src.index("def unchecked_units(")
        тело = src[i:i + 1200]
        self.assertIn("LEFT JOIN bt_3_field_checks", тело)
        self.assertIn("_где_судить(", тело)
        условие = _где_судить("(SELECT 'живой')")
        self.assertIn("c.unit_id IS NULL", условие)
        self.assertIn("c.judged_ru IS DISTINCT FROM", условие)

    def test_a_disputed_card_reaches_the_owner_with_the_field_and_the_fix(self):
        from backend import phrase_panel as pp
        строки = [(9, "alte Narren", "collocation", {}, "старые шутники")]
        claims = [{"field": "translation", "what": "Narren — дураки.",
                   "fix": "старые дураки", "voice": 1}]
        with patch.object(pp, "unavailable_reason", return_value=""), \
             patch.object(pp, "третий_голос_молчит", return_value=""), \
             patch.object(pp, "поднять_старые_отметки", return_value=0), \
             patch.object(pp, "count_personal_backlog", return_value=0), \
             patch.object(pp, "unchecked_units", return_value=строки), \
             patch.object(pp, "count_unchecked", return_value=0), \
             patch.object(pp, "_записать_отметку"), \
             patch.object(pp.Panel, "__init__", _панель_без_сети), \
             patch.object(pp.Panel, "judge",
                          return_value=(pp.DISPUTED, "голоса разошлись", claims)), \
             patch.object(pp.Panel, "проверить_претензию",
                          return_value={"claim": "right", "fix": "ok", "why": ""}), \
             patch("backend.database.open_panel_card_question",
                   return_value=True) as вопрос:
            отчёт = pp.run_batch(limit=1)
        self.assertEqual(отчёт["ушло владельцу"], 1)
        _, _, _, отданные = вопрос.call_args[0]
        self.assertEqual(отданные[0]["fix"], "старые дураки")
        self.assertEqual(отданные[0]["fix_check"], {"state": "ok", "why": ""})


class ВладелецВидитЧислоTests(unittest.TestCase):
    """Молчащий механизм неотличим от сломанного — правило владельца 19.08.2026."""

    def test_the_count_says_i_do_not_know_instead_of_zero(self):
        from backend import phrase_panel as pp
        with patch("backend.database.get_db_connection_context",
                   side_effect=RuntimeError("база молчит")):
            self.assertEqual(pp.count_unchecked(), -1)

    def test_the_line_is_in_the_morning_report(self):
        bot = _src("bot_3.py")
        self.assertIn("+ _phrase_panel_line()", bot)
        i = bot.index("def _phrase_panel_line(")
        # Режем ДО следующей функции, а не по числу знаков: строка отчёта растёт, и
        # окно в 4500 знаков однажды обрезало проверку молча (02.09.2026).
        тело = bot[i:bot.index("\ndef ", i + 1)]
        for кусок in ("осталось непроверенных", "ждут твоего решения", "потрачено за ночь"):
            self.assertIn(кусок, тело)
