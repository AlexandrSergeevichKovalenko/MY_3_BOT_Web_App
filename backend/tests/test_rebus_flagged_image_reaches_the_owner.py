# -*- coding: utf-8 -*-
"""Картинка, в которой усомнилась машина, обязана доехать до владельца.

Повод (04.09.2026). Ночью машина отложила пять картинок и написала владельцу письмо
с кнопкой «Открыть приёмку». Он открыл — и не увидел ни одной: список на экране
строился ИЗ БАНКА ЗАДАНИЙ (только несобранные и неснятые карточки), а отметка стоит
на самой КАРТИНКЕ. Из пяти отложенных четыре стояли только в снятых карточках, пятая
(Brand) — в уже собранной Waldbrand. При этом Waldbrand был заморожен на выдаче до
решения владельца, то есть задание вставало намертво, а разморозить его было негде.

Страж держит ровно этот класс: очередь спорных картинок строится по картинкам и
НЕ ЗАВИСИТ от того, есть ли у слова живое задание.
"""

import contextlib
import unittest
from unittest.mock import patch

import backend.database as database


class _Cursor:
    """Курсор, который отдаёт заготовленные ответы по очереди и помнит запросы."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchall(self):
        return self.answers.pop(0) if self.answers else []

    def fetchone(self):
        return None


class _Connection:
    def __init__(self, answers):
        self.cursor_obj = _Cursor(answers)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass


@contextlib.contextmanager
def _fake_db(conn):
    yield conn


class FlaggedRebusImageTests(unittest.TestCase):
    def _run(self, images, bank_rows):
        conn = _Connection([images, bank_rows])
        with patch.object(database, "get_db_connection_context", lambda: _fake_db(conn)), \
             patch("backend.r2_storage.r2_public_url", side_effect=lambda k: f"https://cdn/{k}"):
            items = database.list_flagged_rebus_images(40)
        return items, conn.cursor_obj.queries

    def test_картинка_без_единого_живого_задания_всё_равно_видна(self):
        """Helm 04.09.2026: обе карточки с ним сняты — и он пропадал с экрана."""
        items, _ = self._run(
            [("Helm", "rebus/helm.png", "машина усомнилась: на картинке шлем, а не штурвал", 0, None)],
            [("Helm", "Fahrradhelm", "велошлем", "pending", True, "шлем"),
             ("Helm", "Schutzhelm", "каска", "pending", True, "шлем")],
        )
        self.assertEqual([i["word"] for i in items], ["Helm"])
        self.assertFalse(items[0]["blocks_live"])
        self.assertEqual(items[0]["live_cards"], [])
        self.assertEqual(sorted(items[0]["retired_cards"]), ["Fahrradhelm", "Schutzhelm"])
        self.assertEqual(items[0]["meaning_ru"], "шлем")
        self.assertTrue(items[0]["image_url"])

    def test_картинка_собранной_карточки_тоже_видна(self):
        """Brand 04.09.2026: Waldbrand уже собран, поэтому на вкладке «На приёмке»
        его не бывает по устройству — а решение по картинке всё равно нужно."""
        items, _ = self._run(
            [("Brand", "rebus/brand.png", "машина усомнилась: огонь, а не пожар", 1, None)],
            [("Brand", "Waldbrand", "лесной пожар", "ready", False, "пожар")],
        )
        self.assertEqual([i["word"] for i in items], ["Brand"])
        self.assertTrue(items[0]["blocks_live"])
        self.assertEqual(items[0]["live_cards"], ["Waldbrand"])

    def test_отбор_идёт_по_отметке_машины_а_не_по_банку(self):
        """Первый запрос — только картинки. Банк спрашивается ПОТОМ и лишь для того,
        чтобы показать, где слово стоит: он не имеет права никого отсеивать."""
        _, queries = self._run(
            [("Helm", "rebus/helm.png", "машина усомнилась: шлем, а не штурвал", 0, None)],
            [],
        )
        select_images = queries[0][0]
        self.assertIn("machine_doubt_at IS NOT NULL", select_images)
        self.assertNotIn("bt_3_rebus_bank", select_images)
        self.assertNotIn("retired", select_images)

    def test_сперва_то_что_держит_живые_задания(self):
        items, _ = self._run(
            [("Helm", "rebus/helm.png", "шлем", 0, None),
             ("Brand", "rebus/brand.png", "огонь", 0, None)],
            [("Helm", "Fahrradhelm", "велошлем", "pending", True, "шлем"),
             ("Brand", "Waldbrand", "лесной пожар", "ready", False, "пожар")],
        )
        self.assertEqual([i["word"] for i in items], ["Brand", "Helm"])


if __name__ == "__main__":
    unittest.main()
