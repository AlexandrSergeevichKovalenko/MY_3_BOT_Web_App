# -*- coding: utf-8 -*-
"""За чужой язык мы не платим: синтез отклоняется, готовое — играет.

Решение владельца 23.08.2026: «delete russian — we do not use them», «we do not need to
spend money for that». Накопленные 1612 русских озвучек удалены вместе с файлами
(`scripts/tts_drop_russian_audio.py`), источник закрыт в двух местах:

  1. прогрев при сохранении берёт сторону ИЗУЧАЕМОГО языка
     (`_pick_learning_language_utterance`, тест test_we_only_voice_the_learning_language);
  2. эндпоинт синтеза отказывается делать НОВУЮ озвучку на чужом языке — этот файл.

Проверка стоит именно на синтезе, а не на выдаче ссылки: если файл уже есть, он
по-прежнему отдаётся и играет. Мы отказываемся только платить за новый.
"""
import io
import os
import re
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _source(relative_path: str) -> str:
    return io.open(os.path.join(REPO_ROOT, relative_path), encoding="utf-8").read()


class GenerateRefusesOtherLanguagesTests(unittest.TestCase):
    def test_the_guard_lives_in_generate_not_in_url(self):
        """Отказ — на синтезе. На выдаче ссылки его быть НЕ должно, иначе уже
        оплаченные файлы перестанут играть."""
        source = _source("backend/backend_server.py")
        generate_start = source.index("def webapp_tts_generate():")
        generate_body = source[generate_start:generate_start + 4000]
        self.assertIn("not_learning_language", generate_body,
                      "синтез снова платит за любой язык, который попросят")

        url_start = source.index("def webapp_tts_url():") if "def webapp_tts_url():" in source else -1
        if url_start >= 0:
            url_body = source[url_start:url_start + 4000]
            self.assertNotIn("not_learning_language", url_body,
                             "выдача ссылки не имеет права отказывать: готовый файл должен играть")

    def test_refusal_is_counted_not_silent(self):
        """Отказ обязан оставлять след: молчаливый отказ неотличим от поломки."""
        source = _source("backend/backend_server.py")
        generate_start = source.index("def webapp_tts_generate():")
        generate_body = source[generate_start:generate_start + 4000]
        self.assertIn("_record_tts_admin_monitor_event", generate_body)

    def test_cleanup_script_only_touches_russian_and_never_orphans_a_file(self):
        """Скрипт удаления: только русский язык, и запись не удаляется, если файл в
        хранилище удалить не вышло (иначе файл останется навсегда никому не известным)."""
        source = _source("scripts/tts_drop_russian_audio.py")
        self.assertIn('TARGET_LANGUAGE = "ru-RU"', source)
        self.assertIn("WHERE language = %s", source)
        # порядок: сначала файл, потом запись, и continue при неудаче
        delete_file_at = source.index("r2_delete_object(key)")
        delete_row_at = source.index("DELETE FROM bt_3_tts_object_cache")
        self.assertLess(delete_file_at, delete_row_at,
                        "запись удаляется раньше файла — файл осиротеет в хранилище")
        self.assertIn("continue  # запись оставляем", source)

    def test_cleanup_script_requires_an_explicit_apply_flag(self):
        """Разрушительное действие не должно случаться от простого запуска."""
        source = _source("scripts/tts_drop_russian_audio.py")
        self.assertIn('"--apply"', source)
        self.assertTrue(re.search(r"if not args\.apply:\s*\n\s*print", source),
                        "без --apply скрипт обязан только показывать")


if __name__ == "__main__":
    unittest.main()
