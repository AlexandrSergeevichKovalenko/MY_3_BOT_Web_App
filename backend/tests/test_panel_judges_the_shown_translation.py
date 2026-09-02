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
import ast
import pathlib
import unittest


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


def derevo_defs(дерево):
    return [n for n in ast.walk(дерево) if isinstance(n, ast.FunctionDef)]


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


class ВопросИзПрозыНеВопросTests(unittest.TestCase):
    """⛔ Претензия, собранная из обрезанного следа, — это не вопрос, а его видимость.

    Владелец 02.09.2026 прочёл на своём экране: «Пример 'Sie stellte sich vor den
    Spiegel zurecht.' не связан с фотографированием, а значит; Выражение
    «sich zurechtstellen» не употребляется в возвратной форме». Это ДВА замечания от
    ДВУХ голосов о РАЗНЫХ частях карточки, каждое обрезано на 90-м знаке и склеено
    через «; » — след отметки писался как `"; ".join(what[:90])`, а вопрос до
    31.08.2026 собирался из него. Полного текста нет нигде: он отброшен при записи.

    Замер 02.09.2026: 36 панельных вопросов без имени поля, у 25 текст оборван
    по-настоящему (ровно 90 знаков и нет точки в конце); 6 таких же у авторов.
    """

    def test_such_a_question_puts_the_card_back_in_the_queue(self):
        from backend.phrase_panel import ВОПРОС_ИЗ_ПРОЗЫ, _где_судить
        условие = _где_судить("(SELECT 'живой')")
        self.assertIn("bt_3_phrase_review", условие,
                      "вопрос из прозы снова не возвращает карточку на пересуд")
        self.assertIn("COALESCE(j->>'voice', '0') <> '0'", ВОПРОС_ИЗ_ПРОЗЫ,
                      "признак происхождения подменён признаком по форме текста")
        self.assertIn("IN ('panel', 'personal')", ВОПРОС_ИЗ_ПРОЗЫ,
                      "оба наших экрана обязаны чиниться вместе")

    def test_the_signature_is_the_origin_not_the_shape_of_the_text(self):
        """⛔ ИСКАЛ ПО ФОРМЕ ТЕКСТА — И ОШИБСЯ (02.09.2026).

        Признак «кусок ровно в 90 знаков без точки» дал ложное срабатывание на целой
        претензии «…правильная форма 'weil es ihr leidtut'» — в ней ровно 90 знаков.
        Признак происхождения (номер голоса) такой ошибки дать не может.
        """
        from backend.phrase_panel import ВОПРОС_ИЗ_ПРОЗЫ
        self.assertNotIn("length(", ВОПРОС_ИЗ_ПРОЗЫ,
                         "отбор снова гадает по длине текста")
        self.assertNotIn("'[.!?", ВОПРОС_ИЗ_ПРОЗЫ)

    def test_the_owner_sees_this_pile_shrink(self):
        from backend import phrase_panel as pp
        self.assertTrue(hasattr(pp, "count_prose_questions"))
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:i + 5000]
        self.assertIn('meta.get("вопросы из прозы")', тело)


class ПересудНеОставляетСтарыйВопросTests(unittest.TestCase):
    """⛔ В базе новый вердикт, а на экране прежняя претензия — так решать нельзя.

    Заведение вопроса защищено от дублей (`ON CONFLICT DO NOTHING`). Пока карточку
    смотрели ОДИН раз, этого хватало; с пересудом (02.09.2026) заведение молча не
    срабатывает, и открытый вопрос обязан переписываться или сниматься.
    """

    def _что_вышло(self, *, открыт, verdict, claims=None):
        from unittest.mock import patch
        from backend import phrase_panel as pp
        claims = claims if claims is not None else [
            {"field": "translation", "what": "не то", "fix": "вот так", "voice": 1}]
        with patch("backend.database.open_question_kind", return_value=открыт), \
             patch("backend.database.replace_question_claims",
                   return_value=True) as переписать, \
             patch("backend.database.close_open_question", return_value=True) as снять, \
             patch("backend.database.open_panel_card_question",
                   return_value=True) as владельцу, \
             patch("backend.database.open_personal_text_question",
                   return_value=True) as человеку:
            код = pp.донести_вердикт(7, "alte Narren", "старые дураки",
                                     verdict, "почему", claims)
        return код, переписать, снять, владельцу, человеку

    def test_an_open_panel_question_is_rewritten_not_left_stale(self):
        код, переписать, снять, владельцу, _ = self._что_вышло(
            открыт="panel", verdict="спорное")
        self.assertEqual(код, "переписан")
        переписать.assert_called_once()
        self.assertEqual(переписать.call_args.kwargs["kind"], "panel")
        снять.assert_not_called()
        владельцу.assert_not_called()

    def test_a_question_nobody_disputes_any_more_is_taken_away(self):
        """Спрашивать о том, что мы САМИ больше не считаем ошибкой, нельзя."""
        код, _, снять, _, _ = self._что_вышло(открыт="panel", verdict="подтверждено")
        self.assertEqual(код, "снят")
        снять.assert_called_once()
        self.assertEqual(снять.call_args.kwargs["kind"], "panel")

    def test_the_addressee_may_change_and_then_the_old_question_goes(self):
        """«Это текст человека» — значит спор владельца больше не тот вопрос."""
        код, _, снять, _, человеку = self._что_вышло(
            открыт="panel", verdict="текст человека — решает он")
        self.assertEqual(код, "человеку")
        снять.assert_called_once()
        человеку.assert_called_once()

    def test_someone_elses_question_is_never_touched(self):
        for чужой in ("grammar", "translation"):
            код, переписать, снять, владельцу, человеку = self._что_вышло(
                открыт=чужой, verdict="спорное")
            self.assertEqual(код, "чужой", чужой)
            for заглушка in (переписать, снять, владельцу, человеку):
                заглушка.assert_not_called()

    def test_the_rule_lives_in_one_place(self):
        """Вторая копия таблицы решений жила в `rejudge_personal` до 02.09.2026."""
        import ast
        src = _src("backend/phrase_panel.py")
        дерево = ast.parse(src)
        тела = {n.name: ast.get_source_segment(src, n) for n in derevo_defs(дерево)}
        for имя in ("rejudge_personal", "run_batch"):
            тело = тела[имя]
            self.assertIn("донести_вердикт(", тело, имя)
            for чужое in ("open_personal_text_question(", "open_panel_card_question(",
                          "close_open_question(", "replace_question_claims("):
                self.assertNotIn(чужое, тело,
                                 f"{имя}: правила снова разъехались на две копии")
