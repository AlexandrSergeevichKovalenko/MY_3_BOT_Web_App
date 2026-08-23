# -*- coding: utf-8 -*-
"""Голос озвучки выбирается ОДНИМ правилом во всех местах.

Повод (замер 23.08.2026). Имя файла озвучки считается ИЗ ГОЛОСА, поэтому разные голоса —
разные файлы. Плеер для одиночного немецкого слова просит Standard-C, а прогрев при
сохранении слова, ночной прогрев и детерминированная публичная ссылка брали голос по
умолчанию (Polyglot-1). Итог: 240 готовых озвучек из 1356 (18%) лежали под именем,
которого экран никогда не спросит. Для человека их не существовало — карточка молчала
до касания динамика, а мы платили дорогим премиум-ведром за файл, который никто не
откроет, и потом ещё раз за дешёвый.

Тесты держат две вещи: правило одно и оно применяется везде.
"""
import io
import os
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _source(relative_path: str) -> str:
    return io.open(os.path.join(REPO_ROOT, relative_path), encoding="utf-8").read()


class VoiceRuleTests(unittest.TestCase):
    def test_single_german_word_and_phrase_get_different_voices(self):
        """Само правило: одиночное слово -> дешёвый Standard, фраза -> Polyglot.

        Если эти два случая когда-нибудь совпадут, тест ниже про «одно правило везде»
        перестанет что-либо проверять — поэтому разницу закрепляем отдельно.
        """
        from backend.backend_server import _pick_interactive_tts_voice
        word_voice = _pick_interactive_tts_voice("Haus", "de", None)
        phrase_voice = _pick_interactive_tts_voice("Ich habe die Dose nicht aufbekommen", "de", None)
        self.assertNotEqual(word_voice, phrase_voice)
        self.assertIn("Standard", word_voice)

    def test_article_does_not_change_the_voice(self):
        # «der Hund» — тот же один заголовок, что и «Hund»: голос обязан совпасть,
        # иначе одно и то же слово получит два разных файла.
        from backend.backend_server import _pick_interactive_tts_voice
        self.assertEqual(
            _pick_interactive_tts_voice("Hund", "de", None),
            _pick_interactive_tts_voice("der Hund", "de", None),
        )


class SameVoiceEverywhereTests(unittest.TestCase):
    def test_public_url_uses_the_voice_the_player_will_ask_for(self):
        """Ссылка, которую приложение играет напрямую, обязана указывать на файл того
        голоса, который попросит плеер. Раньше она считалась голосом по умолчанию."""
        import backend.backend_server as server
        from backend.tts_generation import _tts_object_key

        text = "Haus"
        voice = server._pick_interactive_tts_voice(text, "de", None)
        cache_key = server._tts_object_cache_key("de", voice, server.TTS_WEBAPP_DEFAULT_SPEED, text)
        expected_object_key = _tts_object_key("de", voice, cache_key)
        # В тестовом окружении нет настроек R2, и настоящая r2_public_url отдаёт пустую
        # строку. Нас интересует не адрес хранилища, а ИМЯ ФАЙЛА, которое зависит от
        # голоса, — поэтому подменяем только сборку адреса.
        original = server.r2_public_url
        server.r2_public_url = lambda object_key: f"https://storage.test/{object_key}"
        try:
            url = server._tts_public_url_for_text(text, "de")
        finally:
            server.r2_public_url = original
        self.assertIn(expected_object_key, url)

    def test_no_prewarm_path_falls_back_to_the_default_voice(self):
        """Страж источника: три пути, кормящие кеш, обязаны спрашивать правило.

        Проверка по тексту исходника намеренно грубая — она ловит возврат старого
        `_normalize_tts_voice_name(None, ...)` в местах, где голос выбирается ДЛЯ ТЕКСТА.
        Оставшееся вхождение — внутри самого правила (`_pick_interactive_tts_voice`),
        где обращение к голосу по умолчанию и есть «всё остальное».
        """
        source = _source("backend/backend_server.py")
        self.assertEqual(
            source.count("_normalize_tts_voice_name(None"), 1,
            "кто-то снова выбирает голос по умолчанию мимо правила — файл ляжет под "
            "именем, которого экран не спросит",
        )


class NightlyReconcileIsWiredTests(unittest.TestCase):
    """Накопленное чинится САМО, ночью, а не по команде."""

    def test_scheduler_dispatches_the_reconcile(self):
        source = _source("backend/scheduler_service.py")
        self.assertIn("_dispatch_tts_voice_reconcile", source)
        self.assertIn("TTS_VOICE_RECONCILE_ENABLED", source)

    def test_worker_has_the_actor(self):
        self.assertIn("def run_tts_voice_reconcile_actor", _source("backend/background_jobs.py"))

    def test_wrapper_is_importable(self):
        from backend import tts_scheduler
        self.assertTrue(hasattr(tts_scheduler, "run_tts_voice_reconcile_scheduler_job"))

    def test_disabled_reconcile_says_so_instead_of_reporting_a_clean_run(self):
        """Выключенная сверка обязана отличаться от сверки, которая ничего не нашла.

        Запись события ПОДМЕНЯЕТСЯ: на машине разработчика лежат боевые креденшелы, и
        первый прогон этого теста насыпал в живую таблицу наблюдений три строки от
        несуществующего источника «test» (поймано 23.08.2026). Тест не имеет права
        оставлять следы в боевых данных.
        """
        import backend.backend_server as server
        original_flag = server.TTS_VOICE_RECONCILE_ENABLED
        original_record = server._record_tts_admin_monitor_event
        recorded = []
        server.TTS_VOICE_RECONCILE_ENABLED = False
        server._record_tts_admin_monitor_event = lambda *a, **kw: recorded.append((a, kw))
        try:
            result = server._reconcile_tts_voices(source="test")
        finally:
            server.TTS_VOICE_RECONCILE_ENABLED = original_flag
            server._record_tts_admin_monitor_event = original_record
        self.assertTrue(recorded, "сверка обязана оставлять след о том, что она не работала")
        self.assertTrue(result.get("skipped"))
        self.assertEqual(result.get("reason"), "disabled")
        self.assertNotIn("found", result)

    def test_batch_cap_exists(self):
        """Потолок за ночь: сбой правила не имеет права обернуться тысячами обращений."""
        from backend.backend_server import TTS_VOICE_RECONCILE_BATCH
        self.assertGreater(TTS_VOICE_RECONCILE_BATCH, 0)
        self.assertLessEqual(TTS_VOICE_RECONCILE_BATCH, 1000)


if __name__ == "__main__":
    unittest.main()
