# -*- coding: utf-8 -*-
"""Склейка «катящихся» субтитров и сдвиг русских строк, который из неё вырастал.

ПОВОД, 15.09.2026. Владелец: «русские субтитры не синхронно идут с оригинальными
немецкими, то отстают, то вперёд идут». Причина найдена: склейка кадров жила только в
браузере, поэтому у одной реплики было ДВА номера — сырой (в базе) и склеенный (на
экране). Перевод сохранялся под склеенным, а при следующем открытии ролика браузер
перекладывал его ещё раз, как сырой.

Тест держит три вещи:
  • склейка в Python делает ровно то же, что делала в браузере (иначе номера разойдутся
    заново, только в другую сторону);
  • ДВОЙНАЯ перекладка номеров действительно уводит русский вперёд — это тот дефект,
    который считает `/subtitry`, и он не должен «рассосаться» незаметно;
  • ОДНА перекладка (разовый перенос накопленного со старых номеров на новые) уводить
    ничего не должна.
"""
import unittest

from backend.subtitle_cues import cues_are_rolling, deroll_transcript_cues


def _frames(*texts):
    """Кадры по две секунды подряд — как их отдаёт YouTube."""
    return [{"start": i * 2.0, "duration": 2.0, "text": t} for i, t in enumerate(texts)]


class СклейкаКатящихсяКадров(unittest.TestCase):
    def test_полный_повтор_кадра_выбрасывается(self):
        items = _frames(
            "Ich sag, nein.",
            "Ich sag, nein.",
            "Das ist die Thrillerpfeife.",
            "Das ist die Thrillerpfeife.",
            "Und der hat ja so eine weite Hose,",
        )
        rolled, index_map = deroll_transcript_cues(items)
        self.assertEqual([c["text"] for c in rolled], [
            "Ich sag, nein.",
            "Das ist die Thrillerpfeife.",
            "Und der hat ja so eine weite Hose,",
        ])
        self.assertEqual(index_map, {0: 0, 1: 0, 2: 1, 3: 1, 4: 2})
        self.assertTrue(cues_are_rolling(items))

    def test_время_выброшенного_кадра_достаётся_предыдущей_строке(self):
        # Иначе перемотка по строке попадёт в момент, когда фраза уже отзвучала.
        rolled, _ = deroll_transcript_cues(_frames("Guten Tag.", "Guten Tag."))
        self.assertEqual(len(rolled), 1)
        self.assertAlmostEqual(rolled[0]["duration"], 4.0)

    def test_частичный_наезд_обрезается_до_нового_хвоста(self):
        rolled, index_map = deroll_transcript_cues(_frames(
            "und der hat ja",
            "und der hat ja so eine weite Hose",
        ))
        self.assertEqual([c["text"] for c in rolled], ["und der hat ja", "so eine weite Hose"])
        self.assertEqual(index_map, {0: 0, 1: 1})

    def test_совпадение_в_одно_слово_не_считается_прокруткой(self):
        # «die» в конце и в начале — обычное совпадение, а не катящаяся лента.
        rolled, _ = deroll_transcript_cues(_frames("Das ist die", "die Thrillerpfeife."))
        self.assertEqual([c["text"] for c in rolled], ["Das ist die", "die Thrillerpfeife."])

    def test_ручные_субтитры_не_трогаются(self):
        items = _frames("Guten Abend.", "Wie geht es dir?", "Mir geht es gut.")
        rolled, index_map = deroll_transcript_cues(items)
        self.assertEqual(len(rolled), len(items))
        self.assertEqual(index_map, {0: 0, 1: 1, 2: 2})
        self.assertFalse(cues_are_rolling(items))

    def test_кадр_без_текста_не_заводит_своей_строки(self):
        rolled, index_map = deroll_transcript_cues(_frames("Hallo.", "   ", "Tschüss."))
        self.assertEqual([c["text"] for c in rolled], ["Hallo.", "Tschüss."])
        self.assertEqual(index_map, {0: 0, 1: 0, 2: 1})


class ДвойнаяПерекладкаУводитРусскийВперёд(unittest.TestCase):
    """Тот самый дефект: перевод сохранён под СКЛЕЕННЫМИ номерами, а открытие ролика
    прогоняет его через карту «сырой → склеенный» ЕЩЁ РАЗ."""

    def setUp(self):
        self.items = _frames(
            "Ich sag, nein.",
            "Ich sag, nein.",
            "Das ist die Thrillerpfeife.",
            "Das ist die Thrillerpfeife.",
            "Und der hat ja so eine weite Hose,",
            "Und der hat ja so eine weite Hose,",
            "ob alles in Ordnung ist.",
        )
        self.rolled, self.index_map = deroll_transcript_cues(self.items)
        # Первый просмотр: перевод заказан и сохранён под номерами склеенных строк.
        self.saved = {
            0: "Я говорю, нет.",
            1: "Это тот самый триллер-свисток.",
            2: "И у него такие широкие штаны,",
            3: "всё ли в порядке.",
        }

    def _remap_once(self, source):
        """Как это делает браузер: `mappedTranslations[indexMap[k]] = v`, первый занявший
        номер побеждает."""
        out = {}
        for key, value in sorted(source.items()):
            target = self.index_map.get(key)
            if target is not None and target not in out:
                out[target] = value
        return out

    def test_первый_просмотр_сходится(self):
        for idx, cue in enumerate(self.rolled):
            self.assertIn(idx, self.saved, f"строка {idx} без перевода: {cue['text']}")

    def test_второй_просмотр_уводит_русский_вперёд_и_обрывает_хвост(self):
        second = self._remap_once(self.saved)
        # Строка 1 («Thrillerpfeife») получает перевод строки 2 — русский забежал вперёд.
        self.assertEqual(second.get(1), "И у него такие широкие штаны,")
        # Хвост остаётся вовсе без перевода.
        self.assertNotIn(3, second)
        self.assertLess(len(second), len(self.saved))

    def test_одна_перекладка_со_старых_номеров_ничего_не_ломает(self):
        # Разовый перенос накопленного: перевод лежал под СЫРЫМИ номерами — вот для этого
        # карта и нужна, и здесь она работает правильно.
        raw_keyed = {0: "Я говорю, нет.", 2: "Это тот самый триллер-свисток.",
                     4: "И у него такие широкие штаны,", 6: "всё ли в порядке."}
        moved = self._remap_once(raw_keyed)
        self.assertEqual(moved, {0: "Я говорю, нет.", 1: "Это тот самый триллер-свисток.",
                                 2: "И у него такие широкие штаны,", 3: "всё ли в порядке."})


if __name__ == "__main__":
    unittest.main()
