# -*- coding: utf-8 -*-
"""ГОЛОС СУДИТ ТОТ ПЕРЕВОД, КОТОРЫЙ ВИДИТ ЧЕЛОВЕК. И экран показывает его же.

ПОВОД, 02.09.2026. Владелец открыл спорную фразу «Sie konnte dem Vorschlag einiges
abgewinnen» и увидел над ней «Перевода нет», а в словаре у той же фразы перевод стоял.
Разбор показал не описку, а два разных ответа на один вопрос:

  • словарь берёт русский из СЛОЯ СВЯЗЕЙ (`lex_units._fetch_links` → `native_display`);
  • панель трёх голосов брала его из КОПИИ в json карточки, ключ `translation_ru`.

Замер по живой базе того дня, пул панели — 6738 карточек фраз:
  752 — ключа в json нет вовсе, а перевод в словаре есть → голосам уходил `null`;
  672 — ключ есть, но разошёлся с тем, что на экране (429 по смыслу, 243 по знакам).
Из уже вынесенных вердиктов 890 оказались о переводе, которого голос не видел или видел
другим (546 — с ПУСТЫМ русским): 741 штамп «чисто» (и такая карточка не возвращалась
никогда), 126 вопросов авторам, 20 владельцу, 3 «дефект». На экранах «Перевода нет»
стояло у 3 панельных вопросов владельца и 53 личных вопросов авторам — при переводе,
лежащем в базе.

Здесь заперто то, что нельзя потерять при следующей правке.
"""
import pathlib
import unittest


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


class ПереводПриходитСнаружиTests(unittest.TestCase):
    """Карточка, показанная голосу, собирается из ЖИВОГО перевода."""

    def test_the_voice_never_sees_a_card_without_a_translation(self):
        from backend.phrase_panel import entry_of
        with self.assertRaises(ValueError):
            entry_of("Sie konnte dem Vorschlag einiges abgewinnen", "sentence", {}, "")
        with self.assertRaises(ValueError):
            entry_of("in der Dämmerung", "collocation", {"translation_ru": "в сумерках"},
                     "   ")

    def test_the_translation_comes_from_the_argument_not_from_the_card(self):
        """Копия в json могла разойтись с экраном у 672 карточек — верен экран."""
        from backend.phrase_panel import entry_of
        карточка = {"translation_ru": "чёткое направление", "usage_examples": [1]}
        запись = entry_of("klare Zielrichtung", "collocation", карточка,
                          "направление, к которому стремятся")
        self.assertEqual(запись["translation"], "направление, к которому стремятся")
        self.assertEqual(запись["saved_meaning"], "направление, к которому стремятся")
        self.assertEqual(запись["examples"], [1])

    def test_the_panel_no_longer_reads_the_copy_from_the_card(self):
        """Ни одного ЧТЕНИЯ `translation_ru` как источника — иначе копия вернётся.

        Разрешено ровно одно место: разовая засыпка старых отметок. Она записывает то,
        что голосам показывали РАНЬШЕ, — это факт о прошлом, а не источник для ответа.
        """
        import ast
        src = _src("backend/phrase_panel.py")
        код = [n for n in ast.walk(ast.parse(src))
               if isinstance(n, ast.Constant) and isinstance(n.value, str)
               and "translation_ru" in n.value]
        строки = [n.value.strip() for n in код]
        лишние = [s for s in строки
                  if "judged_ru = COALESCE(u.card->>'translation_ru', '')" not in s
                  and not s.startswith("Графа «какой перевод показали голосам»")
                  and "ключом `translation_ru`" not in s]
        self.assertEqual(лишние, [], f"панель снова судит по копии из карточки: {лишние}")


class ОтметкаПомнитЧтоСудилиTests(unittest.TestCase):
    """«Проверено» без ответа на «проверено С ЧЕМ» скрыло 890 таких вердиктов."""

    def test_the_mark_stores_the_judged_translation(self):
        src = _src("backend/phrase_panel.py")
        i = src.index("def _записать_отметку(")
        тело = src[i:i + 1200]
        self.assertIn("judged_ru", тело)
        self.assertIn("judged_ru = EXCLUDED.judged_ru", тело,
                      "при повторной проверке отметка забудет, что судили")

    def test_a_card_judged_with_another_translation_comes_back(self):
        from backend.phrase_panel import _где_судить
        условие = _где_судить("(SELECT 'живой')")
        self.assertIn("c.unit_id IS NULL", условие)
        self.assertIn("c.judged_ru IS DISTINCT FROM", условие,
                      "пересуд по изменившемуся переводу снова не заводится")

    def test_a_card_without_any_translation_is_not_judged_at_all(self):
        """Пустота на входе рождает претензию к переводу, которого голос не видел."""
        from backend.phrase_panel import _где_судить
        self.assertIn("<> ''", _где_судить("(SELECT 'живой')"))

    def test_the_selector_and_the_counter_ask_the_same_thing(self):
        """Разойдясь, они врут владельцу в утреннем отчёте — этот урок уже оплачен."""
        src = _src("backend/phrase_panel.py")
        for имя in ("def unchecked_units(", "def count_unchecked("):
            i = src.index(имя)
            self.assertIn("_где_судить(", src[i:i + 1500], f"{имя} завёл своё условие")

    def test_the_owner_sees_the_rejudge_shrinking(self):
        """Уборка накопленного обязана быть видна числом и обязана убывать."""
        src = _src("backend/phrase_panel.py")
        self.assertIn("def count_stale_translation(", src)
        self.assertIn("def count_without_translation(", src)
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:i + 4500]
        self.assertIn('meta.get("пересудить")', тело)
        self.assertIn('meta.get("без перевода")', тело)


class ЭкранПоказываетЖивойПереводTests(unittest.TestCase):
    """«Перевода нет» обязано значить «его нет», а не «мы читаем не оттуда»."""

    def test_the_screen_reads_the_link_layer(self):
        from backend.database import _перевод_для_экрана
        sql = _перевод_для_экрана()
        self.assertIn("bt_3_lex_links", sql)
        self.assertIn("bt_3_lex_units v", sql)

    def test_the_candidate_translation_keeps_its_own_question(self):
        """Вид 'translation' спрашивает про КАНДИДАТА: связи ещё нет, и подменять его
        живым значением значит спросить владельца не о том."""
        from backend.database import _перевод_для_экрана
        sql = _перевод_для_экрана()
        self.assertIn("CASE WHEN COALESCE(r.kind, 'grammar') = 'translation'", sql)
        self.assertIn("THEN COALESCE(r.translation, '')", sql)

    def test_no_screen_shows_the_copy_frozen_in_the_question(self):
        for файл, функция in (("backend/database.py", "def list_open_phrase_reviews("),
                              ("backend/database.py", "def get_open_phrase_review("),
                              ("backend/word_confirm_digest.py", "def _phrase_items(")):
            src = _src(файл)
            i = src.index(функция)
            тело = src[i:i + 2500]
            self.assertIn("_перевод_для_экрана()", тело, f"{функция} снова читает копию")
            self.assertNotIn("r.translation,", тело.split("_перевод_для_экрана()")[1][:400],
                             f"{функция} снова показывает замороженный перевод")

    def test_the_owner_edit_replaces_what_he_saw(self):
        src = _src("backend/database.py")
        i = src.index("def apply_panel_card_edit(")
        тело = src[i:src.index("def send_panel_card_to_rewrite(")]
        self.assertIn("_перевод_для_экрана()", тело,
                      "правка снова ищет в карточке текст, которого владелец не видел")
        self.assertIn("promote_owner_translation(unit_id, перевод)", тело,
                      "выбор владельца снова не доходит до слоя связей")


class ОдноПравилоНаВсёПриложениеTests(unittest.TestCase):
    """Вторая копия правила через полгода разойдётся, и одна из двух станет неверной."""

    def test_the_display_rule_lives_in_one_place(self):
        src = _src("backend/lex_units.py")
        self.assertEqual(src.count("(l.sense_id IS NULL), l.rank, v.id"), 1,
                         "правило порядка связей снова живёт двумя копиями")
        i = src.index("def _fetch_links(")
        self.assertIn("{_LINK_PICK_ORDER}", src[i:i + 2500],
                      "выдача перестала спрашивать общее правило")

    def test_the_subquery_refuses_a_forged_alias(self):
        from backend.lex_units import native_display_sql
        with self.assertRaises(ValueError):
            native_display_sql("u; DROP TABLE bt_3_lex_units")
        with self.assertRaises(ValueError):
            native_display_sql("u", lang="ru'; --")
        self.assertIn("l.from_unit = u.id", native_display_sql("u"))


if __name__ == "__main__":
    unittest.main()
