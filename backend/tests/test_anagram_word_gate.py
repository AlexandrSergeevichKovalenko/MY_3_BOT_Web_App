"""Дверь приёмки анаграмм: слово должно существовать, быть ходовым и быть НАПИСАНО верно.

Живой случай, из которого выросла дверь (13.09.2026): владельцу пришла анаграмма с
подсказкой «Закоулок» и словом `Inkelgasse`. Такого слова нет — это «Winkelgasse» без
первой буквы, 0 вхождений на 53,3 млрд слов корпуса DWDS. Карточка ушла трём людям.

Рядом тем же замером нашёлся второй класс: 34 карточки несли глагол или прилагательное,
записанное с большой буквы (`Behaupten` вместо `behaupten`, `Peinlich` вместо
`peinlich`), и человек видел это на экране — буквы отдаются как записаны.

Третий случай — тот, на котором ломается наивная правка: `das Aufstoßen` (отрыжка),
`das Abfangen` (перехват), `das Erschießen` (расстрел). Это отглагольные
существительные, заглавная у них ВЕРНА, и «починить регистр по виду слова» их испортило
бы. Поэтому регистр берётся из источника: часть речи записи и артикль в ней.
"""
import unittest
from unittest.mock import patch

from backend.anagram_word_gate import judge_anagram_word, spelling_by_source
from backend.rebus_word_gate import MIN_PER_BILLION


def запись(word_de: str, pos: str | None) -> dict:
    payload = {"word_de": word_de, "translation_de": word_de}
    if pos is not None:
        payload["part_of_speech"] = pos
    return {"response_json": payload}


class РегистрБерётсяИзИсточника(unittest.TestCase):
    def test_глагол_пишется_строчными(self):
        написание, почему = spelling_by_source("Behaupten", запись("behaupten", "verb"))
        self.assertEqual(написание, "behaupten")
        self.assertEqual(почему, "")

    def test_прилагательное_и_наречие_тоже(self):
        self.assertEqual(spelling_by_source("Peinlich", запись("Peinlich", "adjective"))[0],
                         "peinlich")
        self.assertEqual(spelling_by_source("Massenhaft", запись("massenhaft", "adverb"))[0],
                         "massenhaft")

    def test_существительное_остаётся_с_заглавной(self):
        self.assertEqual(spelling_by_source("Zugehörigkeit",
                                            запись("die Zugehörigkeit", "noun"))[0],
                         "Zugehörigkeit")

    def test_артикль_важнее_части_речи(self):
        """`das Aufstoßen` помечено глаголом, но это существительное: артикль решает."""
        for слово, текст in (("Aufstoßen", "das Aufstoßen"),
                             ("Abfangen", "das Abfangen"),
                             ("Erschießen", "das Erschießen")):
            with self.subTest(слово=слово):
                self.assertEqual(spelling_by_source(слово, запись(текст, "verb"))[0], слово)

    def test_источник_молчит_слово_не_берём(self):
        написание, почему = spelling_by_source("Irgendwas", запись("Irgendwas", None))
        self.assertEqual(написание, "")
        self.assertIn("части речи", почему)


class ХодовостьРешаетDWDS(unittest.TestCase):
    def _судить(self, слово, запись_словаря, ppb):
        with patch("backend.dwds_frequency.word_per_billion", return_value=ppb):
            return judge_anagram_word(слово, запись_словаря)

    def test_несуществующее_слово_не_проходит(self):
        """Inkelgasse: 0 вхождений. Именно эта карточка ушла людям трижды."""
        написание, почему = self._судить("Inkelgasse", запись("die Inkelgasse", "noun"), 0.0)
        self.assertEqual(написание, "")
        self.assertIn("редкое", почему)

    def test_редкое_не_проходит(self):
        написание, _ = self._судить("Hühnerauge", запись("das Hühnerauge", "noun"),
                                    MIN_PER_BILLION - 1)
        self.assertEqual(написание, "")

    def test_ходовое_проходит_в_верном_написании(self):
        написание, почему = self._судить("Behaupten", запись("behaupten", "verb"), 47477.0)
        self.assertEqual(написание, "behaupten")
        self.assertEqual(почему, "")

    def test_нет_ответа_это_не_годится(self):
        """DWDS промолчал — слово НЕ берём. «Не знаем» и «годится» разные вещи."""
        написание, почему = self._судить("Waschmaschine", запись("die Waschmaschine", "noun"),
                                         None)
        self.assertEqual(написание, "")
        self.assertIn("не ответил", почему)

    def test_частота_спрашивается_про_итоговое_написание(self):
        """У DWDS «Behaupten» 102 на миллиард, а «behaupten» 47 477: спрашивать надо
        про то написание, которое уйдёт человеку, иначе дверь зарежет нормальный глагол."""
        with patch("backend.dwds_frequency.word_per_billion", return_value=47477.0) as спрос:
            judge_anagram_word("Behaupten", запись("behaupten", "verb"))
        self.assertEqual(спрос.call_args.args[0], "behaupten")


if __name__ == "__main__":
    unittest.main()
