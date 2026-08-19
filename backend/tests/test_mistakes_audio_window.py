"""Утреннее «аудио с ошибками за вчера» датирует ОШИБКУ, а не выдачу задания.

Разбор 19.08.2026 по живой базе. В выпуск за 18.08 ушло 15 фраз двум людям, и 8 из
них (53%) были ошибками, к которым человек в этот день не притрагивался. Самой старой
— пять месяцев: заведена 12.03, последний раз тронута 18.03.

Причина не в озвучке, а в правиле отбора: единственный фильтр даты стоял на
`bt_3_daily_sentences.date` («фраза была выдана вчера»), а `bt_3_detailed_mistakes`
даты не имела вовсе. План дня НАМЕРЕННО переподаёт старые ошибочные фразы (работа над
ошибками, `translation_workflow.py:4570`) — и любая такая переподача воскрешала старую
ошибку целиком, даже если человек вчера её не открывал.

Решение владельца 19.08.2026: в аудио идёт только то, что человек делал вчера.

Здесь проверяется граница суток — то место, где правило легче всего сломать назад:
время в базе лежит в UTC без зоны, а «вчера» у человека венское.
"""

import unittest
from datetime import date, datetime

from backend.backend_server import vienna_day_bounds_utc


class ViennaDayBoundsTests(unittest.TestCase):
    def test_summer_day_is_shifted_by_two_hours(self):
        """18.08.2026 — летнее время, Вена +2. Сутки человека начинаются в 22:00
        предыдущего дня по UTC, и именно так они лежат в колонке `last_seen`."""
        lo, hi = vienna_day_bounds_utc(date(2026, 8, 18))
        self.assertEqual(lo, datetime(2026, 8, 17, 22, 0))
        self.assertEqual(hi, datetime(2026, 8, 18, 22, 0))

    def test_winter_day_is_shifted_by_one_hour(self):
        """Зимой смещение другое — вшить константу «минус два часа» нельзя."""
        lo, hi = vienna_day_bounds_utc(date(2026, 1, 15))
        self.assertEqual(lo, datetime(2026, 1, 14, 23, 0))
        self.assertEqual(hi, datetime(2026, 1, 15, 23, 0))

    def test_window_is_exactly_one_day_across_the_dst_switch(self):
        """Ночь перевода часов не имеет права ни потерять ошибки, ни удвоить их:
        окно остаётся полусегментом [начало, следующее начало)."""
        for day in (date(2026, 3, 29), date(2026, 10, 25)):
            lo, hi = vienna_day_bounds_utc(day)
            self.assertGreater(hi, lo)
            self.assertIn((hi - lo).total_seconds() / 3600, (23.0, 24.0, 25.0))

    def test_consecutive_days_touch_without_gap_or_overlap(self):
        """Граница одна и та же с обеих сторон — иначе ошибка, сделанная в 23:30,
        либо попадёт в два выпуска, либо не попадёт ни в один."""
        _, first_end = vienna_day_bounds_utc(date(2026, 8, 18))
        second_start, _ = vienna_day_bounds_utc(date(2026, 8, 19))
        self.assertEqual(first_end, second_start)


if __name__ == "__main__":
    unittest.main()
