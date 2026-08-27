"""Имя на соединениях с базой: кто именно их берёт.

Ради чего этот файл: письмо «пул голодает» приходило владельцу со строкой
«Последний триггер: unspecified» и не давало никакого решения (27.08.2026). Имя
ставится ОДИН раз на входе — HTTP-запрос, обработчик бота, актёр очереди — и должно
доставаться всему вложенному, не протекая соседу.
"""
import asyncio
import threading
import unittest

from backend import database as db


class DbAcquireLabelStorageTests(unittest.TestCase):
    def tearDown(self):
        db.set_db_acquire_label(None)

    def test_label_is_readable_after_being_set(self):
        db.set_db_acquire_label("http:/api/whatever")
        self.assertEqual(db.current_db_acquire_label(), "http:/api/whatever")

    def test_empty_label_means_no_name_not_empty_string(self):
        db.set_db_acquire_label("   ")
        self.assertIsNone(db.current_db_acquire_label())

    def test_scope_restores_the_previous_name(self):
        db.set_db_acquire_label("actor:outer")
        with db.db_acquire_scope("inner_job"):
            self.assertEqual(db.current_db_acquire_label(), "inner_job")
        self.assertEqual(db.current_db_acquire_label(), "actor:outer")

    def test_another_thread_does_not_inherit_the_name(self):
        # Поток воркера не должен видеть чужую метку: иначе в письме окажется
        # невиновный актёр.
        db.set_db_acquire_label("actor:first")
        seen = {}

        def _worker():
            seen["label"] = db.current_db_acquire_label()

        thread = threading.Thread(target=_worker)
        thread.start()
        thread.join()
        self.assertIsNone(seen["label"])

    def test_two_asyncio_tasks_do_not_overwrite_each_other(self):
        # Главная причина, по которой метка НЕ threading.local: бот держит сотни
        # обработчиков в ОДНОМ потоке цикла asyncio.
        async def _one(name, hold):
            db.set_db_acquire_label(name)
            await asyncio.sleep(hold)
            return db.current_db_acquire_label()

        async def _both():
            return await asyncio.gather(_one("bot:/start", 0.02), _one("bot:cb:menu", 0.0))

        first, second = asyncio.run(_both())
        self.assertEqual(first, "bot:/start")
        self.assertEqual(second, "bot:cb:menu")

    def test_label_reaches_work_moved_to_a_thread(self):
        # Обработчики бота уводят обращения к базе в asyncio.to_thread(); контекст
        # туда копируется, значит имя обязано доехать.
        async def _run():
            db.set_db_acquire_label("bot:/wort")
            return await asyncio.to_thread(db.current_db_acquire_label)

        self.assertEqual(asyncio.run(_run()), "bot:/wort")


class BotUpdateLabelTests(unittest.TestCase):
    """Имён должно быть немного, и текст человека в них не попадает."""

    @staticmethod
    def _label(**parts):
        import bot_3

        class _Stub:
            def __init__(self, **kwargs):
                self.effective_message = kwargs.get("message")
                self.callback_query = kwargs.get("callback_query")
                self.inline_query = kwargs.get("inline_query")
                self.my_chat_member = kwargs.get("my_chat_member")
                self.chat_member = kwargs.get("chat_member")

        return bot_3._db_acquire_label_for_update(_Stub(**parts))

    def test_command_keeps_only_the_command_itself(self):
        message = type("M", (), {"text": "/wort Haus schnell"})()
        self.assertEqual(self._label(message=message), "bot:/wort")

    def test_command_with_bot_suffix_is_normalized(self):
        message = type("M", (), {"text": "/start@my_bot"})()
        self.assertEqual(self._label(message=message), "bot:/start")

    def test_button_keeps_only_the_prefix_of_its_data(self):
        query = type("Q", (), {"data": "ans_frv_0:12345:yes"})()
        self.assertEqual(self._label(callback_query=query), "bot:cb:ans_frv_0")

    def test_plain_text_never_leaks_into_the_name(self):
        message = type("M", (), {"text": "das Haus ist gross"})()
        self.assertEqual(self._label(message=message), "bot:message")


class EntryPointsActuallyNameThemselvesTests(unittest.TestCase):
    """Метка ставится на ВХОДЕ. Проверяем не намерение, а сами входы."""

    def tearDown(self):
        db.set_db_acquire_label(None)

    def test_http_request_names_itself_by_endpoint(self):
        from backend import backend_server

        self.assertIn(
            backend_server._name_the_db_acquires_of_this_request,
            backend_server.app.before_request_funcs.get(None, []),
        )
        self.assertIn(
            backend_server._forget_the_db_acquire_name,
            backend_server.app.teardown_request_funcs.get(None, []),
        )
        with backend_server.app.test_request_context("/api/healthz"):
            backend_server._name_the_db_acquires_of_this_request()
            named = db.current_db_acquire_label()
        self.assertTrue(named and named.startswith("http:"), named)

    def test_http_request_forgets_the_name_afterwards(self):
        # Поток веб-сервера переиспользуется под следующий запрос: чужое имя на нём —
        # ложное обвинение в письме о голоде пула.
        from backend import backend_server

        db.set_db_acquire_label("http:/api/healthz")
        backend_server._forget_the_db_acquire_name(None)
        self.assertIsNone(db.current_db_acquire_label())

    def test_queue_actor_names_itself_and_lets_go(self):
        from backend.job_queue import _NameDbAcquiresMiddleware

        middleware = _NameDbAcquiresMiddleware()
        message = type("Msg", (), {"actor_name": "run_tts_generation_job"})()
        middleware.before_process_message(None, message)
        self.assertEqual(db.current_db_acquire_label(), "actor:run_tts_generation_job")
        middleware.after_process_message(None, message)
        self.assertIsNone(db.current_db_acquire_label())

    def test_queue_middleware_is_installed_on_the_broker(self):
        from backend import job_queue

        broker = job_queue.get_dramatiq_broker()
        self.assertTrue(
            any(isinstance(item, job_queue._NameDbAcquiresMiddleware)
                for item in broker.middleware),
            [type(item).__name__ for item in broker.middleware],
        )

    def test_bot_registers_its_namer_before_everything_else(self):
        import inspect

        import bot_3

        source = inspect.getsource(bot_3.main)
        self.assertIn("_name_db_acquires_for_update", source)
        self.assertIn("group=-7", source)


if __name__ == "__main__":
    unittest.main()
