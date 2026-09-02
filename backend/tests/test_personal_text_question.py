# -*- coding: utf-8 -*-
"""Ошибка в СВОЁМ тексте доходит до автора — с готовым вариантом в одно касание.

ПОВОД, 31.08.2026. Панель судит четыре поля карточки. Примеры и значение сочинили мы —
их чиним сами. Саму фразу и её перевод написал человек, и правило владельца от
23.08.2026 запрещает переписывать их молча: «это же сам пользователь записал, мы должны
это оставить». У правила была вторая половина — «помечаем и ПОКАЗЫВАЕМ ему», — и её не
построили: вердикт ложился отметкой в базу и умирал там.

Замер по живой базе 31.08.2026: 384 такие карточки, 369 из них у самого владельца.
Человек продолжал учить «zweiseitiger Verkehr», которого в немецком нет.

Владелец: «Показывать человеку — у него уже есть экран проверки слов… для него это
одно касание: да, правильно Gegenverkehr. Делаем вот это.»
"""
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


ПРЕТЕНЗИЯ = [{"field": "headword", "voice": 2,
              "what": "Так не говорят по-немецки, это калька.",
              "fix": "Gegenverkehr", "fix_check": {"state": "ok", "why": ""}}]


class ВопросАдресованАвторуTests(unittest.TestCase):
    """Свой вид вопроса: у автора он есть, у владельца его нет."""

    def test_the_kind_is_recognised(self):
        from backend.database import PERSONAL_REVIEW_CATEGORY, phrase_review_kind
        self.assertEqual(phrase_review_kind(
            [{"category": PERSONAL_REVIEW_CATEGORY, "field": "headword"}]), "personal")

    def test_the_owner_queue_does_not_swell_with_other_peoples_words(self):
        """369 из 384 — карточки владельца, но 15 чужих. Решать за человека, что ему
        учить, он не должен, и его очередь этими вопросами не наполняется."""
        src = _src("backend/database.py")
        i = src.index("def list_open_phrase_reviews(")
        тело = src[i:i + 2500]
        self.assertIn("<> 'personal'", тело, "личные вопросы снова текут владельцу")
        j = src.index("def count_open_phrase_reviews(")
        self.assertIn("<> 'personal'", src[j:j + 900],
                      "счётчик владельца снова считает чужую очередь")

    def test_the_persons_screen_asks_for_this_kind(self):
        src = _src("backend/word_confirm_digest.py")
        i = src.index("ВИДЫ_ДЛЯ_ЧЕЛОВЕКА = ")
        self.assertIn('"personal"', src[i:i + 120],
                      "вопрос заведён, но до экрана человека не доезжает")

    def test_the_question_carries_the_field_and_the_fix(self):
        from backend.database import _претензии_в_судей, PERSONAL_REVIEW_CATEGORY
        судьи = _претензии_в_судей(ПРЕТЕНЗИЯ, PERSONAL_REVIEW_CATEGORY)
        self.assertEqual(судьи[0]["field"], "headword")
        self.assertEqual(судьи[0]["fix"], "Gegenverkehr")
        self.assertEqual(судьи[0]["category"], PERSONAL_REVIEW_CATEGORY)

    def test_a_question_without_a_single_claim_is_not_opened(self):
        """Пустой звонок тратит касание человека ни на что."""
        from backend.database import open_personal_text_question
        with patch("backend.database.get_db_connection_context") as соединение:
            self.assertFalse(open_personal_text_question(1, "x", "y", []))
        соединение.assert_not_called()


class ГотовыйВариантВОдноКасаниеTests(unittest.TestCase):
    """«Да, правильно Gegenverkehr» — кнопка, а не задача «догадайся сам»."""

    def test_the_button_reaches_the_screen(self):
        from backend.word_confirm_digest import _готовый_вариант
        from backend.database import _претензии_в_судей, PERSONAL_REVIEW_CATEGORY
        судьи = _претензии_в_судей(ПРЕТЕНЗИЯ, PERSONAL_REVIEW_CATEGORY)
        self.assertEqual(_готовый_вариант(судьи),
                         {"field": "headword", "fix": "Gegenverkehr"})

    def test_a_fix_our_own_check_rejected_is_never_offered(self):
        """⛔ На экране человека нет места для возражения проверки, а кнопка «да,
        правильно так» читается как «система уверена». Отдать одним касанием то, что
        система сама отвергла, нельзя: он учит немецкий по нашему ответу."""
        from backend.word_confirm_digest import _готовый_вариант
        from backend.database import _претензии_в_судей, PERSONAL_REVIEW_CATEGORY
        плохой = [{**ПРЕТЕНЗИЯ[0], "fix_check": {"state": "bad", "why": "смысл другой"}}]
        судьи = _претензии_в_судей(плохой, PERSONAL_REVIEW_CATEGORY)
        self.assertEqual(_готовый_вариант(судьи), {})

    def test_the_screen_has_the_button(self):
        src = _src("frontend/src/dictionary/WordAudit.jsx")
        self.assertIn("const FIXP = 'fix_personal';", src)
        self.assertIn("Да, правильно «{it.fix}»", src)
        self.assertIn("Да, правильный перевод — «{it.fix}»", src)

    def test_pressing_it_is_not_counted_as_leaving_things_alone(self):
        """Человек нажал «да, правильно так», а внизу ему написали бы «оставим»."""
        src = _src("frontend/src/dictionary/WordAudit.jsx")
        self.assertIn("const keep = items.length - drop - retrans - исправим;", src)
        self.assertIn("исправим</span>", src)

    def test_the_text_of_the_button_never_travels_from_the_browser(self):
        """⛔ Правка расходится по всей словарной статье и по карточкам других людей.
        Наверх едет только «нажал готовый вариант», а какой это был текст — сервер
        читает сам. То же правило, что у кнопок вариантов (28.08.2026)."""
        src = _src("backend/word_confirm_digest.py")
        i = src.index('if action == "fix_personal":')
        тело = src[i:i + 1800]
        self.assertIn("personal_question_fix(review_id)", тело)
        self.assertNotIn('item.get("fix")', тело, "текст кнопки снова берётся из браузера")

    def test_the_headword_and_the_translation_go_through_different_doors(self):
        """Русский текст в немецкий заголовок не попадает: перевод правится карточкой,
        а сама фраза — переименованием статьи."""
        src = _src("backend/word_confirm_digest.py")
        i = src.index('if action == "fix_personal":')
        тело = src[i:i + 1800]
        self.assertIn('if готовое["field"] == "headword":', тело)
        self.assertIn('apply_phrase_review_decision(review_id, "replace"', тело)
        self.assertIn("apply_panel_card_edit(", тело)

    def test_the_server_only_offers_a_fix_from_an_open_personal_question(self):
        from backend.database import personal_question_fix
        src = _src("backend/database.py")
        i = src.index("def personal_question_fix(")
        тело = src[i:i + 2200]
        self.assertIn("kind = 'personal'", тело)
        self.assertIn("status = 'open'", тело)
        self.assertIn('"bad"', тело, "забракованный вариант снова уходит кнопкой")
        self.assertTrue(callable(personal_question_fix))


class НакопленноеРазбираетсяСамоTests(unittest.TestCase):
    """384 отметки не должны ждать, пока кто-то запустит скрипт."""

    def test_an_old_mark_becomes_a_question(self):
        from backend.phrase_panel import раскрыть_отметку
        претензии = раскрыть_отметку(
            "headword :: Так не говорят по-немецки, это калька.; "
            "По-немецки двустороннее движение называется «Gegenverkehr».")
        self.assertEqual(претензии[0]["field"], "headword")
        self.assertIn("Gegenverkehr", претензии[0]["what"])
        # Готового варианта у старой отметки НЕТ: до 31.08.2026 его не спрашивали, и
        # выдумать его задним числом нельзя.
        self.assertEqual(претензии[0]["fix"], "")

    def test_a_mark_about_our_own_field_is_not_dressed_as_the_persons_text(self):
        from backend.phrase_panel import раскрыть_отметку
        претензии = раскрыть_отметку("examples :: Примеры не про эту фразу.")
        self.assertEqual(претензии[0]["field"], "",
                         "чужое поле не должно притворяться текстом человека")

    def test_the_backfill_runs_every_night_in_portions(self):
        src = _src("backend/phrase_panel.py")
        self.assertIn("BACKFILL_LIMIT", src)
        i = src.index("def поднять_старые_отметки(")
        тело = src[i:i + 2000]
        self.assertIn("NOT EXISTS (SELECT 1 FROM bt_3_phrase_review r", тело,
                      "по одной единице снова заводится второй открытый вопрос")

    def test_the_backfill_does_not_depend_on_the_judges_being_available(self):
        """Это разбор УЖЕ вынесенных вердиктов, ни одного платного запроса. Накопленное
        обязано доходить до людей и в ту ночь, когда панель судить не может."""
        from backend import phrase_panel as pp
        with patch.object(pp, "unavailable_reason", return_value="нет ключа"), \
             patch.object(pp, "поднять_старые_отметки", return_value=4) as подъём, \
             patch.object(pp, "count_personal_backlog", return_value=380), \
             patch.object(pp, "count_unchecked", return_value=1300):
            отчёт = pp.run_batch(limit=5)
        подъём.assert_called_once()
        self.assertEqual(отчёт["поднято из старых"], 4)
        self.assertIn("пропущено", отчёт)

    def test_the_owner_sees_the_number_in_the_morning(self):
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:i + 3500]
        self.assertIn('meta.get("ушло человеку")', тело)
        self.assertIn('meta.get("поднято из старых")', тело)
        self.assertIn("ушли авторам", тело)


class ПересудСтарыхОтметокTests(unittest.TestCase):
    """Разовый пересуд: старым отметкам — тот же вопрос, что и новым.

    Владелец 31.08.2026 на «прогнать их заново за ≈$2?» ответил «yes». Прогон разовый:
    новые отметки уже рождаются с готовым вариантом, и второй раз он не понадобится.
    """

    def _прогон(self, verdict, claims, открыт="personal", **заглушки):
        from backend import phrase_panel as pp
        строки = [(7, "zweiseitiger Verkehr", "collocation", {},
                   "двустороннее движение")]
        общие = {
            "unavailable_reason": lambda: "",
            "units_with_verdict": lambda v, n: строки,
            "_записать_отметку": lambda *a, **k: None,
        }
        with patch.object(pp.Panel, "__init__", lambda self, budget_usd=0: None), \
             patch.object(pp.Panel, "judge", return_value=(verdict, "почему", claims)), \
             patch.object(pp.Panel, "проверить_вариант", return_value={"state": "ok", "why": ""}), \
             patch.object(pp, "unavailable_reason", общие["unavailable_reason"]), \
             patch.object(pp, "units_with_verdict", общие["units_with_verdict"]), \
             patch.object(pp, "_записать_отметку", общие["_записать_отметку"]), \
             patch("backend.database.open_question_kind", return_value=открыт), \
             patch("backend.database.replace_personal_question_claims",
                   **заглушки.get("replace", {"return_value": True})) as переписать, \
             patch("backend.database.close_personal_question",
                   return_value=True) as закрыть, \
             patch("backend.database.open_personal_text_question",
                   return_value=True) as завести, \
             patch("backend.database.open_panel_card_question",
                   return_value=True) as владельцу:
            pp.Panel.cost = 0.0
            отчёт = pp.rejudge_personal(limit=1)
        return отчёт, переписать, закрыть, завести, владельцу

    def test_the_fix_appears_in_the_question_the_person_already_has(self):
        """Второй вопрос об одном и том же — это второе касание. Переписываем первый."""
        отчёт, переписать, _, завести, _ = self._прогон(
            "текст человека — решает он",
            [{"field": "headword", "what": "калька", "fix": "Gegenverkehr"}])
        self.assertEqual(отчёт["вопрос обновлён"], 1)
        self.assertEqual(отчёт["с готовым вариантом"], 1)
        завести.assert_not_called()

    def test_a_claim_the_new_judges_dropped_stops_bothering_the_person(self):
        """Спрашивать человека о том, что мы САМИ больше не считаем ошибкой, нельзя."""
        отчёт, _, закрыть, _, _ = self._прогон("подтверждено", [])
        self.assertEqual(отчёт["снята претензия"], 1)
        закрыть.assert_called_once()

    def test_a_dispute_goes_to_the_owner_and_the_person_is_left_alone(self):
        отчёт, _, закрыть, _, владельцу = self._прогон(
            "спорное", [{"field": "translation", "what": "не то", "fix": "подрезать"}])
        self.assertEqual(отчёт["ушло владельцу"], 1)
        закрыть.assert_called_once()
        владельцу.assert_called_once()

    def test_a_foreign_open_question_is_never_overwritten(self):
        """По этой же единице открыт спор владельца или проверка перевода — чужой
        вопрос мы не подменяем и второй поверх него не заводим."""
        отчёт, переписать, закрыть, завести, _ = self._прогон(
            "текст человека — решает он",
            [{"field": "headword", "what": "калька", "fix": "Gegenverkehr"}],
            открыт="panel")
        переписать.assert_not_called()
        завести.assert_not_called()
        закрыть.assert_not_called()

    def test_no_answer_changes_nothing_at_all(self):
        """«Не спросили» — авария связи, а не новый вердикт: ни отметку, ни вопрос."""
        отчёт, переписать, закрыть, завести, _ = self._прогон("не спросили", [])
        self.assertEqual(отчёт["не спросили"], 1)
        self.assertEqual(отчёт["пересужено"], 0)
        for заглушка in (переписать, закрыть, завести):
            заглушка.assert_not_called()

    def test_it_is_a_one_off_run_not_a_nightly_job(self):
        bot = _src("bot_3.py")
        self.assertNotIn("rejudge_personal", bot,
                         "разовая уборка попала в ночное расписание — платить за неё "
                         "вечно незачем: новые отметки уже с готовым вариантом")


class ПорцияПоДвадцатьTests(unittest.TestCase):
    """Владелец 31.08.2026: «давай по 20 за раз высылать пользователю».

    Пересуд накопленного дал 328 вопросов «ошибка в твоём тексте» одному человеку — на
    экране, где до этого было 105 карточек. Стена из 433 карточек не разбирается: её
    закрывают."""

    def test_the_portion_is_twenty(self):
        from backend.word_confirm_digest import ЛИЧНЫХ_ЗА_РАЗ
        self.assertEqual(ЛИЧНЫХ_ЗА_РАЗ, 20)

    def test_only_the_new_kind_is_capped(self):
        """Остальные виды идут как шли: их поток и так небольшой, резать нечего."""
        src = _src("backend/word_confirm_digest.py")
        i = src.index("личных_показано += 1")
        окно = src[max(0, i - 300):i + 200]
        self.assertIn('if str(вид or "") == "personal":', окно,
                      "порция режет не только личные вопросы")

    def test_the_rest_are_not_marked_and_come_back(self):
        """⛔ Это ОКНО, а не фильтр «навсегда». Непоказанные не помечаются ничем: в
        следующем письме придут те же двадцать, а как человек их решит — следующие."""
        src = _src("backend/word_confirm_digest.py")
        i = src.index("if личных_показано > ЛИЧНЫХ_ЗА_РАЗ:")
        self.assertIn("continue", src[i:i + 120])
        # Ни отметки, ни записи в дневник — только пропуск на экране.
        self.assertNotIn("closed_at", src[i:i + 400])

    def test_the_order_is_stable_so_the_queue_moves(self):
        """Порядок по номеру вопроса: очередь двигается, а не перетасовывается."""
        src = _src("backend/word_confirm_digest.py")
        i = src.index("def _phrase_items(")
        self.assertIn("ORDER BY r.id", src[i:i + 2000])


class ПисьмоОбещаетТоЧтоНаЭкранеTests(unittest.TestCase):
    """⛔ Число в письме и число на экране — это одно и то же число.

    Порция (20 личных вопросов за раз) резала только экран, а счёт для письма считал
    ВСЕ. Владелец получил бы «338 фраз ждут проверки», а на экране нашёл двадцать.
    Ровно тот дефект, ради которого `_phrase_counts_by_author` и заведена: «письмо
    обещает 186 фраз, а человек находит 98»."""

    def test_the_letter_counts_the_same_portion(self):
        from backend.word_confirm_digest import _phrase_counts_by_author, ЛИЧНЫХ_ЗА_РАЗ

        class Курсор:
            def execute(self, sql, args=None):
                self.sql = sql
            def fetchall(self):
                # 30 личных вопросов одного человека и 2 обычных.
                строки = [(5, f"фраза {n}", [{"verdict": "doubt"}], "personal")
                          for n in range(30)]
                строки += [(5, "фраза грамматика", [{"verdict": "doubt"}], "grammar"),
                           (7, "чужая фраза", [{"verdict": "doubt"}], "personal")]
                return строки

        счёт = _phrase_counts_by_author(Курсор())
        self.assertEqual(счёт[5], ЛИЧНЫХ_ЗА_РАЗ + 1,
                         "письмо снова обещает больше, чем покажет экран")
        self.assertEqual(счёт[7], 1)
