# -*- coding: utf-8 -*-
"""Кто читает `bt_3_phrase_review`, обязан НАЗВАТЬ вид вопроса.

ПОВОД, 27.08.2026. В этой таблице живут три разных вопроса:

    grammar     — судьи разошлись о немецком самой фразы;
    panel       — три голоса разошлись о карточке (примеры и перевод);
    translation — перевод карточки не прошёл проверку перед подъёмом в общий слой.

Они не взаимозаменяемы: у каждого свой адресат, свои кнопки и свои слова. Пока вид
вычислял каждый читатель сам, любой, кто выбрал «всё со status='open'», получал ЧУЖИЕ
вопросы. Так и вышло: все 38 записей вида translation доехали до экрана проверки слов
у ученика и показались как «фраза, в которой мы усомнились» — включая одиночные слова
«Besprechung» и «Soile». Ни мои тесты, ни тесты соседа этого не видели: у него всё
правильно, у меня всё правильно, ломается НА СТЫКЕ.

Поэтому правило перестало быть обязанностью читателя:
  · вид — КОЛОНКА `kind`, а не вывод из `judges` у каждого;
  · её проставляет САМА ТАБЛИЦА триггером — мест записи четыре, и правило, которое
    держится на памяти пишущего, ломается на пятом;
  · а этот тест держит вторую половину: выбрал открытые записи — назови вид.

Читателю, которому вправду нужны ВСЕ виды (счётчики для владельца, уборка шума),
никто не мешает — он пишет это явно и попадает в список ниже с причиной. Список
короткий и осмысленный; молча в него не попадают.
"""
import os
import re
import unittest

ЗДЕСЬ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Читатели, которым ВСЕ виды нужны по существу. Каждый — с причиной; без причины в
# списке не место, иначе он превратится в свалку «и это тоже пропустите».
ЧИТАЮТ_ВСЕ_ВИДЫ = {
    "list_open_phrase_reviews": "экран владельца показывает все три вида, он их и решает",
    "count_open_phrase_reviews": "счётчик «осталось N» на его экране — про всю очередь",
    "count_open_phrase_reviews_by_kind": "сам раскладывает очередь по видам",
    "drop_noise_phrase_reviews": "пустая придирка это пустая придирка в любом виде",
    "count_noise_phrase_reviews": "тот же счёт, что и у уборки",
    "close_all_ok_phrase_reviews": "закрывает единогласное «ошибки нет» независимо от вида",
    "list_open_phrase_reviews_judged_blind": "переспрос вслепую нужен любому виду",
    "list_open_phrase_reviews_needing_arbiter": "третейский судья зовётся к любому спору",
    "get_open_phrase_review": "берёт ОДНУ запись по номеру — вид уже известен вызвавшему",
    "set_phrase_review_arbiter": "правка одной записи по номеру",
    "update_phrase_review_judges": "правка одной записи по номеру",
    "send_panel_card_to_rewrite": "работает с записью, вид которой уже проверен выше",
    "queue_phrase_for_review": "проверяет занятость слова, а не читает вопрос",
    "apply_translation_link_decision": "применяет решение по ОДНОЙ записи по номеру — "
                                       "вид известен экрану, который её показал",
    "_phrase_owner": "берёт ОДНУ запись по номеру, чтобы узнать автора; вид тут не при чём",
    "words_for_user": "сам таблицу не читает — список берёт через _phrase_items, "
                      "а там вид назван",
}

# «Назвал вид» — это одно из: колонка kind в самом запросе либо явная проверка через
# phrase_review_kind рядом.
НАЗВАЛ_ВИД = re.compile(r"\bkind\b", re.IGNORECASE)


def _функции(текст: str) -> list[tuple[str, str]]:
    """[(имя функции, её тело)] — грубо, по отступу def."""
    куски = re.split(r"\ndef ", "\n" + текст)
    готово = []
    for кусок in куски[1:]:
        имя = кусок.split("(", 1)[0].strip()
        готово.append((имя, кусок))
    return готово


class ВидНазываетсяЯвно(unittest.TestCase):
    def test_every_reader_of_open_reviews_names_the_kind(self):
        безымянные = []
        for файл in ("database.py", "word_confirm_digest.py", "translation_links.py",
                     "phrase_night_check.py", "example_retry.py", "lex_units.py",
                     "word_gate_apply.py"):
            путь = os.path.join(ЗДЕСЬ, файл)
            if not os.path.exists(путь):
                continue
            for имя, тело in _функции(open(путь, encoding="utf-8").read()):
                if "bt_3_phrase_review" not in тело:
                    continue
                плоский = re.sub(r"\s+", " ", тело)
                if "status = 'open'" not in плоский and "status='open'" not in плоский:
                    continue
                if имя in ЧИТАЮТ_ВСЕ_ВИДЫ or НАЗВАЛ_ВИД.search(плоский):
                    continue
                безымянные.append(f"{файл}::{имя}")
        self.assertEqual(
            безымянные, [],
            "эти читатели выбирают открытые вопросы, не называя вид — они получат "
            "чужие и покажут их не тому человеку. Назовите вид в запросе "
            "(r.kind = ANY(...)) либо внесите функцию в ЧИТАЮТ_ВСЕ_ВИДЫ с причиной.")

    def test_the_table_fills_the_kind_itself(self):
        """Мест записи четыре. Правило, которое держится на памяти пишущего, ломается
        на пятом — поэтому вид проставляет сама таблица."""
        слой = open(os.path.join(ЗДЕСЬ, "database.py"), encoding="utf-8").read()
        self.assertIn("bt_3_phrase_review_set_kind", слой)
        self.assertIn("BEFORE INSERT OR UPDATE OF judges ON bt_3_phrase_review", слой)
        self.assertIn("ADD COLUMN IF NOT EXISTS kind TEXT", слой)

    def test_the_learner_gets_only_questions_about_the_phrase_itself(self):
        from backend.word_confirm_digest import ВИДЫ_ДЛЯ_ЧЕЛОВЕКА
        self.assertEqual(sorted(ВИДЫ_ДЛЯ_ЧЕЛОВЕКА), ["grammar", "panel"])
        self.assertNotIn("translation", ВИДЫ_ДЛЯ_ЧЕЛОВЕКА)

    def test_the_allowlist_has_a_reason_for_every_entry(self):
        for имя, причина in ЧИТАЮТ_ВСЕ_ВИДЫ.items():
            self.assertTrue(причина.strip(), f"{имя} попал в список без причины")


if __name__ == "__main__":
    unittest.main()
