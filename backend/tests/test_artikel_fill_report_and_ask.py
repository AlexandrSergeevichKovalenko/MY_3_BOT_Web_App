"""Отчёт наполнения читается человеком, а заказ слов считается по остатку.

Прошлый отчёт («Добавлено: 26 · забраковано: 46 · Всего проверенных: 150/150») владелец
прочитать не смог: непонятно, к чему 26, если в теме 150, и что за дробь. Здесь заперты
три вещи, которые чинили: в отчёте есть состояние темы и «было», у каждого отказа есть
человеческая причина, и модель не просят приносить 30 слов на подтему, когда добрать
осталось пять.
"""
import unittest

from backend.article_sprint_generator import _ask_count, _reason_bucket
from backend.background_jobs import _artikel_fill_report_text, _words_ru


class AskCountTests(unittest.TestCase):
    def test_shrinks_only_when_a_full_batch_would_overshoot(self):
        # Три недостающих слова не стоят пачки в тридцать — за неё платим и порождением,
        # и проверкой артикля.
        self.assertLess(_ask_count(remaining=3, ceiling=30), 30)
        self.assertLess(_ask_count(remaining=5, ceiling=30), 30)

    def test_does_not_thin_out_the_batch(self):
        # Ключевое: пока нехватка велика, просим ПРЕЖНИЕ 30. Дробить заказ нельзя —
        # каждый лишний вызов заново тащит тяжёлый постоянный кусок запроса, а слов
        # на выходе столько же. Ровно эта ошибка тут и чинилась.
        self.assertEqual(_ask_count(remaining=26, ceiling=30), 30)
        self.assertEqual(_ask_count(remaining=150, ceiling=30), 30)

    def test_never_exceeds_the_previous_ceiling(self):
        self.assertLessEqual(_ask_count(remaining=999, ceiling=30), 30)

    def test_never_asks_absurdly_little(self):
        # Просить 2 слова бессмысленно: фильтр съест их целиком и прогон встанет впустую.
        self.assertGreaterEqual(_ask_count(remaining=1, ceiling=30), 10)

    def test_nothing_left_asks_nothing(self):
        self.assertEqual(_ask_count(remaining=0, ceiling=30), 0)


class ReasonBucketTests(unittest.TestCase):
    def test_internal_wording_becomes_human_label(self):
        self.assertEqual(_reason_bucket("нужно второе мнение"), "редкое, не для повседневной речи")
        self.assertEqual(
            _reason_bucket("третье производное от «Fest» — род тот же, учить нечему"),
            "третье слово от того же корня",
        )

    def test_unknown_reason_does_not_crash(self):
        self.assertEqual(_reason_bucket("что-то новое"), "прочее")


class ReportTextTests(unittest.TestCase):
    RESULT_FULL = {
        "theme": "party_freizeit", "added": 26, "rejected": 46,
        "final_verified": 150, "target": 150, "had": 124,
        "by_subtopic": {"дискотека": 6, "вечеринка": 16},
        "rejected_by_reason": {"редкое, не для повседневной речи": 20},
        "gen_failures": [],
    }

    def test_first_line_is_theme_state_not_run_event(self):
        text = _artikel_fill_report_text(label="Party & Freizeit", result=self.RESULT_FULL, duration_s=73)
        first = text.splitlines()[0]
        self.assertIn("150 слов из 150 нужных", first)

    def test_added_count_says_what_it_was_before(self):
        # Без «было 124» число 26 повисает в воздухе — с этого и начался разбор.
        text = _artikel_fill_report_text(label="Party & Freizeit", result=self.RESULT_FULL, duration_s=73)
        self.assertIn("было 124", text)

    def test_rejections_come_with_reasons(self):
        text = _artikel_fill_report_text(label="Party & Freizeit", result=self.RESULT_FULL, duration_s=73)
        self.assertIn("редкое, не для повседневной речи — 20", text)

    def test_no_code_words_leak_to_the_reader(self):
        text = _artikel_fill_report_text(label="Party & Freizeit", result=self.RESULT_FULL, duration_s=73)
        for leak in ("verified", "target", "by_subtopic", "забраковано", "150/150"):
            self.assertNotIn(leak, text)

    def test_unfinished_theme_says_what_is_missing_and_what_to_do(self):
        text = _artikel_fill_report_text(label="Küche", duration_s=310, result={
            "theme": "kueche", "added": 40, "rejected": 61, "final_verified": 118,
            "target": 150, "had": 78, "by_subtopic": {}, "rejected_by_reason": {}, "gen_failures": [],
        })
        self.assertIn("не хватает ещё 32", text)
        self.assertIn("/artikel_fill kueche", text)

    def test_failed_subtopic_is_named(self):
        result = dict(self.RESULT_FULL, gen_failures=["танцпол"])
        text = _artikel_fill_report_text(label="Party & Freizeit", result=result, duration_s=73)
        self.assertIn("танцпол", text)

    def test_russian_plurals(self):
        self.assertEqual(_words_ru(1), "1 слово")
        self.assertEqual(_words_ru(2), "2 слова")
        self.assertEqual(_words_ru(5), "5 слов")
        self.assertEqual(_words_ru(11), "11 слов")
        self.assertEqual(_words_ru(61), "61 слово")


if __name__ == "__main__":
    unittest.main()
