"""Из плеера сохраняют ТРЕМЯ кнопками — источник обязан ехать во всех трёх.

Иначе «все слова из этого фильма» показывают не всё, а понять это по экрану нельзя:
список выглядит нормальным, просто короче правды. Ровно так и вышло 31.08.2026 — две
кнопки источник писали, третья (словарный виджет под роликом) нет.

Тест сторожит КЛАСС, а не тот один случай: любая новая кнопка сохранения из плеера
обязана слать `source`, иначе он покраснеет.
"""

import re
import unittest
from pathlib import Path

APP_JSX = Path(__file__).resolve().parents[2] / "frontend" / "src" / "App.jsx"


class YoutubeSavesCarryTheSourceTests(unittest.TestCase):
    def setUp(self):
        self.source = APP_JSX.read_text(encoding="utf-8")

    def test_every_youtube_save_sends_the_source(self):
        # Тело запроса на сохранение начинается с JSON.stringify({ и заканчивается }),
        # поэтому ищем каждое место, где в теле указан «ютубовский» origin_process, и
        # проверяем ЭТО ЖЕ тело на наличие source.
        bodies = re.findall(r"JSON\.stringify\(\{(.*?)\n        \}\)", self.source, re.S)
        bodies += re.findall(r"JSON\.stringify\(\{(.*?)\n          \}\)", self.source, re.S)
        checked = 0
        for body in bodies:
            found = re.search(r"origin_process:\s*(?:'([^']*)'|isYoutubeSelectionContext\(\))", body)
            if not found:
                continue
            origin = found.group(1) or "youtube"
            if "youtube" not in origin:
                continue
            checked += 1
            self.assertIn(
                "source: youtubeSource", body,
                f"сохранение с origin_process={origin!r} не шлёт источник — "
                "слова этого ролика не соберутся в списке «Откуда»",
            )
        self.assertGreaterEqual(checked, 3, "путей сохранения из плеера должно быть не меньше трёх")

    def test_the_source_payload_is_built_from_the_video_id_not_from_the_title(self):
        """Ключ источника — идентификатор ролика.

        Решение владельца 31.08.2026: один ролик = одна запись, сколько бы дней его ни
        смотрели. Если ключом станет заголовок, ролик распадётся на несколько записей,
        как только YouTube его переименует, — а до 31.08.2026 папки и вовсе клеились по
        первым двум словам заголовка, и разные ролики сливались в одну.
        """
        builder = re.search(
            r"const buildYoutubeSourcePayload = \(\) => \{(.*?)\n  \};", self.source, re.S,
        )
        self.assertIsNotNone(builder, "buildYoutubeSourcePayload не найден")
        body = builder.group(1)
        self.assertIn("key: videoId", body)
        self.assertIn("if (!videoId) return null;", body)

    def test_no_folder_is_created_for_a_video_any_more(self):
        """Папок с названием ролика больше не бывает — тема и источник разъехались."""
        self.assertNotIn("ensureYoutubeAutoFolderId", self.source)
        self.assertNotIn("resolveYoutubeAutoFolderName", self.source)


if __name__ == "__main__":
    unittest.main()
