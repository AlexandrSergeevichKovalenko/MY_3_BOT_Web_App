# -*- coding: utf-8 -*-
"""Ответ в тренажёре артиклей не теряется молча — и повтор не удваивает его.

ПОВОД. Аудит костылей 16.08.2026, находка 43. Дефект был из ДВУХ половин, и починка
одной без другой ничего не давала:

  · клиент (`ArtikelLearnGame.jsx`) бросал ответ в пустоту: `.catch(() => {})`.
    Моргнул интернет, свернули окно, шёл деплой — ответ пропадал. Человек видел ✅,
    а ошибка не попадала в работу над ошибками и больше ему не возвращалась;
  · сервер ловил сбой записи в базу и ВСЁ РАВНО отвечал `ok: True`. То есть даже
    начни клиент повторять — ему бы соврали, что ответ дошёл.

Механизм надёжной отправки в проекте уже был (`reviewAnswerQueue.js`, повтор +
запас в браузере + досылка); тренажёр артиклей просто не был на него переведён.

⚠ ЗАЧЕМ КЛЮЧ НАЖАТИЯ. Повтор без ключа был бы ХУЖЕ потери. «Освоено» здесь считается
как COUNT(*) FILTER (WHERE is_correct) >= ARTIKEL_MASTERY_CORRECT (по умолчанию 2), и
один и тот же верный ответ, записанный дважды, объявил бы слово выученным после ОДНОГО
правильного нажатия. Поэтому клиент шлёт `answer_id` — ключ ЭТОГО нажатия, одинаковый
у всех попыток, а база отбрасывает повтор через ON CONFLICT DO NOTHING.
"""
import os
import pathlib
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2]
ИГРА = КОРЕНЬ / "frontend" / "src" / "answer" / "ArtikelLearnGame.jsx"

ТЕЛО = {"word": "Tisch", "article": "der", "is_correct": True,
        "theme_key": "haus", "set_id": "s1", "answer_id": "ключ-нажатия-1"}


def _ответ_сервера(тело, запись):
    """Прогнать эндпоинт с подменённой записью в базу. Вернуть разобранный JSON."""
    with server.app.test_request_context("/api/webapp/artikel/learn/answer", json=тело):
        with mock.patch.object(server, "_answer_auth_user_id", return_value=(777, "Кто-то", None)), \
             mock.patch("backend.database.record_article_learn_answer", запись):
            ответ = server.artikel_learn_answer()
    полезное = ответ[0] if isinstance(ответ, tuple) else ответ
    return полезное.get_json()


class СерверНеВрётПроСохранение(unittest.TestCase):

    def test_запись_упала_значит_ok_false(self):
        """Сбой базы больше не выдаётся за успех: клиенту есть что повторять."""
        def падает(**_kw):
            raise RuntimeError("база недоступна")
        self.assertEqual(_ответ_сервера(ТЕЛО, падает).get("ok"), False)

    def test_запись_прошла_значит_ok_true(self):
        """Честный успех остаётся успехом — иначе клиент повторял бы вечно."""
        self.assertEqual(_ответ_сервера(ТЕЛО, mock.Mock()).get("ok"), True)

    def test_ключ_нажатия_доходит_до_базы(self):
        """Без этого ключа повтор удвоил бы ответ — а он приходит из тела запроса."""
        запись = mock.Mock()
        _ответ_сервера(ТЕЛО, запись)
        self.assertEqual(запись.call_args.kwargs.get("client_answer_id"), "ключ-нажатия-1")

    def test_старый_бандл_без_ключа_не_ломается(self):
        """У человека в руках может быть вчерашний бандл: он ключа не шлёт."""
        запись = mock.Mock()
        без_ключа = {k: v for k, v in ТЕЛО.items() if k != "answer_id"}
        self.assertEqual(_ответ_сервера(без_ключа, запись).get("ok"), True)
        self.assertIsNone(запись.call_args.kwargs.get("client_answer_id"))


class ПовторНеУдваиваетОтвет(unittest.TestCase):

    def _собрать_sql(self, ключ):
        курсор, sql = mock.MagicMock(), []
        курсор.execute.side_effect = lambda q, *a, **k: sql.append(" ".join(str(q).split()))
        соединение = mock.MagicMock()
        соединение.cursor.return_value.__enter__.return_value = курсор
        контекст = mock.MagicMock()
        контекст.__enter__.return_value = соединение
        from backend import database
        with mock.patch.object(database, "get_db_connection_context", return_value=контекст), \
             mock.patch.object(database, "ensure_article_learn_schema", lambda: None):
            database.record_article_learn_answer(
                user_id=777, word="Tisch", article="der", is_correct=True,
                client_answer_id=ключ,
            )
        return sql

    def test_с_ключом_вторая_доставка_отбрасывается_базой(self):
        sql = " | ".join(self._собрать_sql("ключ-нажатия-1"))
        self.assertIn("ON CONFLICT (user_id, client_answer_id) DO NOTHING", sql)

    def test_без_ключа_поведение_прежнее(self):
        """Старый бандл пишет как раньше — это не новая дыра, а прежнее поведение."""
        sql = " | ".join(self._собрать_sql(None))
        self.assertNotIn("ON CONFLICT", sql)
        self.assertIn("INSERT INTO bt_3_article_learn_answers", sql)


class КлиентБольшеНеБросаетОтветВПустоту(unittest.TestCase):

    def setUp(self):
        self.текст = ИГРА.read_text(encoding="utf-8")

    def test_отправка_идёт_через_надёжную_очередь(self):
        self.assertIn("sendReviewAnswer(api, '/api/webapp/artikel/learn/answer'", self.текст)

    def test_прежнего_fire_and_forget_не_осталось(self):
        """Именно эта строка теряла ответ. Вернётся — тест покраснеет."""
        self.assertNotIn("}).catch(() => { /* fire-and-forget */ });", self.текст)

    def test_отложенное_досылается_при_открытии(self):
        """Иначе запас в браузере лежал бы вечно и ошибки не вернулись бы человеку."""
        self.assertIn("flushPendingReviewAnswers(api)", self.текст)

    def test_клиент_шлёт_ключ_нажатия(self):
        self.assertIn("answer_id: newAnswerId()", self.текст)


if __name__ == "__main__":
    unittest.main()
