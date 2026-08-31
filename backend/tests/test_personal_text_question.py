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
