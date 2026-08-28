# -*- coding: utf-8 -*-
"""Задание нельзя положить в очередь, которую никто не слушает.

ПОВОД, 28.08.2026. Решения с экрана проверки слов уехали в фон на новую очередь
`word_audit_apply`. По коду всё правильно, по делу задание не взял бы никто: у сервиса
BACKGROUND_JOBS переменная `DRAMATIQ_QUEUES` содержит ЯВНЫЙ список очередей, и новой в
нём нет. Задания молча копились бы в Redis, человек видел бы «Принято» и никогда —
сообщения о готовности. Молчащий механизм неотличим от сломанного.

Имя очереди живёт в двух местах сразу — в коде и в переменной окружения Railway, — и
разъезд между ними ничем не выдаёт себя: ни ошибки, ни лога, ни красного теста. Здесь
лежит СНИМОК того, что сервисы слушают на самом деле, и он держит эти два места вместе.

Перемерить снимок:
    railway variables --service BACKGROUND_JOBS --environment production --kv \
        | grep DRAMATIQ_QUEUES

Красный тест значит одно из двух, и разбирать надо ОБА:
  · очередь завели в коде, а в Railway не добавили — задания будут теряться;
  · очередь в Railway добавили, а снимок здесь не обновили — поправить список ниже.
"""
import re
import unittest
from pathlib import Path

КОРЕНЬ = Path(__file__).resolve().parents[2]

# Снимок 28.08.2026, сервис BACKGROUND_JOBS (DRAMATIQ_QUEUES).
СЛУШАЮТ = {
    "youtube_transcript", "translation_check", "translation_check_completion",
    "translation_fill", "finish_summary", "shortcut_lookup", "translation_pool_refill",
    "image_quiz_prepare", "image_quiz_render", "translation_side_effects",
    "scheduler_jobs", "tts_generation", "reader_ingest", "riddle_prepare",
    "riddle_render",
}
# Снимок той же даты, сервис AUX_BACKGROUND_WORKER.
СЛУШАЮТ |= {"projection_materialization_live", "projection_materialization_backfill"}

# Имя очереди берём ТОЛЬКО из настоящего декоратора: в комментариях этого файла имена
# очередей тоже встречаются (там объясняется, почему одно из них заводить нельзя).
ДЕКОРАТОР = re.compile(r'^@dramatiq\.actor\([^)]*queue_name="([a-z_]+)"', re.M)


class ОчередиСлушают(unittest.TestCase):
    def test_every_actor_queue_is_consumed_by_a_worker(self):
        текст = (КОРЕНЬ / "backend" / "background_jobs.py").read_text(encoding="utf-8")
        очереди = set(ДЕКОРАТОР.findall(текст))
        self.assertTrue(очереди, "не нашли ни одного актёра — сломался разбор файла")
        ничьи = sorted(очереди - СЛУШАЮТ)
        self.assertEqual(ничьи, [], f"эти очереди не слушает ни один воркер: {ничьи}. "
                                    f"См. шапку файла — разбирать надо обе причины.")

    def test_the_word_audit_job_is_on_a_consumed_queue(self):
        """Тот самый случай, ради которого тест написан."""
        текст = (КОРЕНЬ / "backend" / "background_jobs.py").read_text(encoding="utf-8")
        i = текст.index("def run_word_audit_apply_job(")
        очередь = ДЕКОРАТОР.findall(текст[:i])[-1]
        self.assertIn(очередь, СЛУШАЮТ)


if __name__ == "__main__":
    unittest.main()
