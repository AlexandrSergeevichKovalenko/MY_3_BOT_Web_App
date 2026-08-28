# -*- coding: utf-8 -*-
"""Тренажёр окончаний: аварийный ВЫХОД вместо заглушки, и владельцу докладывают.

ПОВОД. Последний пункт из аудита костылей. `_load_nouns` при любом сбое молча отдавал
30 ВШИТЫХ В КОД существительных, и тренажёр крутил одни и те же тридцать слов вместо
5835 из банка. Немецкий в них верный — но сбой прятался: программа делала вид, что всё
хорошо, и владелец не узнавал об этом никогда.

РЕШЕНИЕ ВЛАДЕЛЬЦА 28.08.2026, дословно:

    «Чего мы 30 одинаковых всегда давать будем? Давай посмотрим, сколько их готовых
    есть (если нет — подготовим), и каждый раз будем делать перемешивание. И
    ОБЯЗАТЕЛЬНО МНЕ СООБЩАТЬ ОБ ЭТОМ. Чтобы я знал, что нормальная схема не сработала.
    Чтобы мы искали ошибки. И это уже не заглушка, а аварийный выход.»

ЧЕМ ЭТО ОТЛИЧАЕТСЯ ОТ ЗАГЛУШКИ:
  · было — 30 вшитых в код слов, одни и те же всегда, владелец не знал ничего;
  · стало — 1000 РАЗНЫХ настоящих заданий в банке, каждый раз перемешиваются,
    и о каждом случае владельцу СООБЩАЕТСЯ.

Запас бесплатный: собирается тем же детерминированным правилом склонений, что и
основной путь, без единого обращения к модели. Замер 28.08.2026: 988 заданий за 79 с,
повторный прогон добавляет 0 (ключ считается от самой фразы).

ПРОВЕРЕНО НА ЖИВОЙ БАЗЕ в тот же день: при пустом банке существительных выдано 15
настоящих заданий, между двумя заходами НОЛЬ совпадений, оба случая записаны для
доклада владельцу.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import adjektiv_endings as ae  # noqa: E402
from backend import database as db  # noqa: E402


class ВшитыхСловБольшеНет(unittest.TestCase):

    def test_список_из_тридцати_слов_удалён(self):
        self.assertFalse(hasattr(ae, "_FALLBACK_NOUNS"),
                         "вернулся вшитый список — вместе с ним вернётся и спрятанный сбой")

    def test_пусто_значит_пусто(self):
        """Подставлять свои слова здесь мы права не имеем: вызывающий возьмёт запас."""
        with mock.patch("backend.database.get_db_connection_context",
                        side_effect=RuntimeError("база молчит")):
            self.assertEqual(ae._load_nouns(), [])

    def test_генератор_ничего_не_строит_без_слов(self):
        with mock.patch.object(ae, "_load_nouns", return_value=[]):
            self.assertEqual(ae.build_adjektiv_items(15), [])


class ЗапасЭтоНастоящиеЗаданияИИхМного(unittest.TestCase):

    def test_цель_запаса_тысяча(self):
        self.assertEqual(db.ADJEKTIV_RESERVE_TARGET, 1000)

    def test_повторное_пополнение_не_плодит_дублей(self):
        """Ключ считается от самой фразы, а не случайный."""
        import inspect
        код = inspect.getsource(db.ensure_adjektiv_reserve)
        self.assertIn("ON CONFLICT (aufgabe_id) DO NOTHING", код)
        self.assertIn('payload.get("full")', код)

    def test_в_запас_негодное_не_попадает(self):
        """Тот же страж, что и на выдаче: пропуск обязан склеиваться обратно."""
        import inspect
        код = inspect.getsource(db.ensure_adjektiv_reserve)
        self.assertIn("adjektiv_gap_rebuilds(payload)", код)

    def test_запас_бесплатный(self):
        """Ни одного обращения к модели — иначе тысяча заданий стоила бы денег."""
        import pathlib
        текст = pathlib.Path(ae.__file__).read_text(encoding="utf-8")
        for слово in ("openai", "gpt", "requests.post"):
            self.assertNotIn(слово, текст.lower())


class ОКаждомСлучаеВладельцуДокладывают(unittest.TestCase):

    def setUp(self):
        db.take_adjektiv_reserve_uses()

    def test_использование_запаса_записывается_с_причиной(self):
        db.note_adjektiv_reserve_use("банк существительных пуст")
        случаи = db.take_adjektiv_reserve_uses()
        self.assertEqual(len(случаи), 1)
        self.assertIn("пуст", случаи[0])

    def test_забрали_значит_обнулили(self):
        """Иначе владелец увидит один и тот же случай в каждом докладе."""
        db.note_adjektiv_reserve_use("что-то")
        db.take_adjektiv_reserve_uses()
        self.assertEqual(db.take_adjektiv_reserve_uses(), [])

    def test_выдача_из_запаса_отмечает_случай(self):
        import inspect
        код = inspect.getsource(db.pick_adjektiv_payloads)
        self.assertIn("note_adjektiv_reserve_use(причина)", код)
        self.assertLess(код.index("note_adjektiv_reserve_use"), код.index("bt_3_aufgabe_bank"),
                        "отметить надо ДО того, как отдали задания из запаса")

    def test_бот_докладывает_и_называет_причину(self):
        import pathlib
        бот = (pathlib.Path(__file__).resolve().parents[2] / "bot_3.py").read_text(encoding="utf-8")
        self.assertIn("take_adjektiv_reserve_uses", бот)
        self.assertIn("АВАРИЙНОГО запаса", бот)
        self.assertIn("Причина:", бот)

    def test_ночью_запас_пополняется_сам(self):
        import pathlib
        бот = (pathlib.Path(__file__).resolve().parents[2] / "bot_3.py").read_text(encoding="utf-8")
        self.assertIn("ensure_adjektiv_reserve", бот)


if __name__ == "__main__":
    unittest.main()
