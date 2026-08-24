"""Ведомость не пишет «стоило ноль» там, где просто не нашла цену.

Разбор 24.08.2026. Судья формы слов зовёт модель `gpt-4.1`, а цена в справочнике лежала
под ДАТИРОВАННЫМ именем `gpt-4.1-2025-04-14`. SKU не совпал, цена не нашлась — и строка
легла в ведомость с `cost_amount = 0`. Замер по боевой базе: 132 события за 21 день
(судья кроссвордов и судья отделяемых глаголов), все по нулю. В отчётах о расходах эти
обращения выглядели бесплатными.

Владелец 24.08.2026: «неважно, копеечные или нет, но чтобы ведомость не врала».

Чинится с двух сторон, и обе половины проверяются здесь:
  1) цена заводится ещё и под коротким именем модели — тем самым, каким её зовёт код;
  2) если цены всё равно нет, строка помечается `pricing_state = missing_snapshot` —
     слово, которое сводки расходов уже умеют считать (unpriced_events).
"""

import os
import re
import unittest
from unittest.mock import MagicMock, patch

from backend.database import log_billing_event


class _Cursor:
    def __init__(self, row=None):
        self.executed = []
        self._row = row

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self._row

    def fetchall(self):
        return []


def _db_context(cursor):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    return MagicMock(return_value=ctx)


_INSERTED_ROW = (
    11, "ev", 1, None, None, "pool_crossword_judge", "openai", "tokens_in", 100.0,
    None, 0.0, "USD", "estimated", {}, None, None,
)


class MissingPriceIsNamedTests(unittest.TestCase):
    def _log(self, snapshot):
        cursor = _Cursor(row=_INSERTED_ROW)
        with patch("backend.database.get_db_connection_context", _db_context(cursor)), \
             patch("backend.database.get_effective_billing_price_snapshot",
                   return_value=snapshot), \
             patch.dict(os.environ, {"SKIP_BILLING_LEDGER_WRITES": ""}, clear=False):
            log_billing_event(
                idempotency_key="ev", user_id=1,
                action_type="pool_crossword_judge", provider="openai",
                units_type="tokens_in", units_value=100.0,
                price_provider="openai", price_sku="gpt-4.1_input",
                price_unit="tokens_in",
            )
        # metadata — 13-й параметр INSERT'а (см. порядок колонок в log_billing_event)
        _, params = cursor.executed[0]
        return params[12].adapted

    def test_no_price_is_marked_missing_and_names_the_sku(self):
        meta = self._log(None)
        self.assertEqual(meta.get("pricing_state"), "missing_snapshot")
        self.assertEqual(meta.get("price_sku_missing"), "gpt-4.1_input")

    def test_found_price_is_marked_priced(self):
        meta = self._log({"id": 5, "price_per_unit": 0.000002, "currency": "USD"})
        self.assertEqual(meta.get("pricing_state"), "priced")
        self.assertNotIn("price_sku_missing", meta)


class ShortModelNameGetsItsOwnPriceTests(unittest.TestCase):
    """Короткое имя выводится СНЯТИЕМ ДАТЫ с конца, а не сравнением по началу строки."""

    ALIAS_RE = re.compile(r"^(?P<alias>.+)-\d{4}-\d{2}-\d{2}$")

    def test_dated_model_gives_exactly_one_short_name(self):
        m = self.ALIAS_RE.match("gpt-4.1-2025-04-14")
        self.assertIsNotNone(m)
        self.assertEqual(m.group("alias"), "gpt-4.1")

    def test_mini_is_a_separate_model_and_keeps_its_own_name(self):
        """Главная ловушка: по началу строки `gpt-4.1` совпало бы и с mini, и судью
        посчитали бы вчетверо дешевле. Неверное число хуже отсутствующего."""
        m = self.ALIAS_RE.match("gpt-4.1-mini-2025-04-14")
        self.assertEqual(m.group("alias"), "gpt-4.1-mini")

    def test_undated_model_gets_no_alias(self):
        self.assertIsNone(self.ALIAS_RE.match("gpt-4.1-mini"))

    def test_the_sync_code_uses_this_very_rule(self):
        """Правило живёт в одном месте — в засеве цен из переменных окружения."""
        import pathlib
        src = pathlib.Path(__file__).resolve().parents[1] / "backend_server.py"
        text = src.read_text(encoding="utf-8")
        self.assertIn(r'^(?P<alias>.+)-\d{4}-\d{2}-\d{2}$', text)
        self.assertIn('source="alias_of_dated"', text)


if __name__ == "__main__":
    unittest.main()
