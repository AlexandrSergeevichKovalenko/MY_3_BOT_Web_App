"""Наполнение темы останавливает прирост, а не заветная цифра.

Цель «150 слов» ничего не решает: в «Чувствах» ходовых слов навалом, в «Вечеринках» их
физически нет. Неприкосновенная цифра заставляла генератор скрести дно — так в банк попали
«центрифуга для салата» и «тёрка для муската». Теперь два пустых прогона подряд ставят
тему на паузу, и владелец решает кнопками: остановить, продолжить или дать свои слова.
"""
import unittest
from unittest.mock import patch

import backend.article_fill_control as ctl
import backend.article_sprint_generator as gen
import backend.database as db


class ReportWordingTests(unittest.TestCase):
    ROWS = [
        {"theme_key": "gefuehle", "label": "Чувства", "target": 150, "state": "auto",
         "dry_streak": 0, "state_at": None, "words": 213, "fresh": 12},
        {"theme_key": "party", "label": "Вечеринки", "target": 150, "state": "auto",
         "dry_streak": 1, "state_at": None, "words": 50, "fresh": 0},
        {"theme_key": "haushalt", "label": "Уборка", "target": 150, "state": "paused",
         "dry_streak": 2, "state_at": None, "words": 55, "fresh": 0},
    ]

    def test_the_report_separates_growing_from_stalled(self):
        text = "\n".join(ctl.report_lines(self.ROWS))
        self.assertIn("Растут", text)
        self.assertIn("Чувства", text)
        self.assertIn("Прирост кончается", text)
        self.assertIn("Добор не идёт", text)

    def test_a_theme_at_its_target_is_not_called_exhausted(self):
        # Тему, набравшую цель, ночной прогон не трогает — прироста у неё нет по
        # здоровой причине, и пугать ею владельца незачем.
        rows = [{"theme_key": "med", "label": "Медицина", "target": 150, "state": "auto",
                 "dry_streak": 0, "state_at": None, "words": 172, "fresh": 0}]
        text = "\n".join(ctl.report_lines(rows))
        self.assertIn("Набрали цель", text)
        self.assertNotIn("Прирост кончается", text)

    def test_words_are_counted_in_russian(self):
        self.assertEqual(ctl._words_ru(1), "1 слово")
        self.assertEqual(ctl._words_ru(3), "3 слова")
        self.assertEqual(ctl._words_ru(11), "11 слов")
        self.assertEqual(ctl._words_ru(172), "172 слова")

    def test_a_paused_theme_says_so_in_russian(self):
        text = "\n".join(ctl.report_lines(self.ROWS))
        self.assertIn("на паузе", text)
        self.assertNotIn("paused", text, "в отчёт для человека внутренние слова не идут")

    def test_an_empty_bank_does_not_crash_the_report(self):
        self.assertTrue(ctl.report_lines([]))


class FillDecisionTests(unittest.TestCase):
    def _decide(self, action, theme="haushalt"):
        calls = []
        with patch.object(db, "get_article_sprint_theme",
                          lambda t: {"label_ru": "Уборка"}), \
                patch.object(db, "set_theme_fill_state",
                             lambda t, s, **k: calls.append((t, s)) or True):
            text = ctl.apply_fill_decision(action, theme)
        return text, calls

    def test_stop_pauses_the_fill_but_keeps_the_theme_in_the_game(self):
        text, calls = self._decide("stop")
        self.assertEqual(calls, [("haushalt", "stopped")])
        self.assertIn("В игре тема остаётся", text)

    def test_go_resumes_the_fill(self):
        _, calls = self._decide("go")
        self.assertEqual(calls, [("haushalt", "auto")])

    def test_cancel_means_the_owner_found_nothing(self):
        text, calls = self._decide("cancel")
        self.assertEqual(calls, [("haushalt", "stopped")])
        self.assertIn("слов не нашлось", text)

    def test_an_unknown_button_does_not_change_anything(self):
        _, calls = self._decide("wat")
        self.assertEqual(calls, [])


class DecidedThemesStopAskingTests(unittest.TestCase):
    """Тему, по которой владелец уже решил, отчёт спрашивать не имеет права.

    Так и было 11.08.2026: «остановить» возвращало тему в `paused`, а вопрос отчёт задаёт
    именно по `paused` с двумя пустыми прогонами. Двадцать закрытых тем возвращались
    каждые три дня одним и тем же списком."""

    @staticmethod
    def _rows(state):
        return [{"theme_key": "haushalt", "label": "Уборка", "target": 150, "state": state,
                 "dry_streak": 2, "state_at": None, "words": 117, "fresh": 0}]

    def _asked(self, state):
        """Про какие темы отчёт задаёт вопрос — тот же отбор, что и в рассылке."""
        return [r for r in self._rows(state) if r["state"] == "paused" and r["dry_streak"] >= 2]

    def test_a_theme_the_owner_stopped_is_never_asked_about_again(self):
        self.assertEqual(self._asked("stopped"), [])

    def test_a_theme_that_stalled_on_its_own_is_still_asked_about(self):
        self.assertEqual(len(self._asked("paused")), 1, "тут решения ещё не было")

    def test_the_report_counts_stopped_themes_without_nagging(self):
        text = "\n".join(ctl.report_lines(self._rows("stopped")))
        self.assertIn("Остановлены тобой: 1", text)
        self.assertNotIn("Что делаем", text)
        self.assertNotIn("stopped", text, "в отчёт для человека внутренние слова не идут")

    def test_supplying_your_own_words_closes_the_question_too(self):
        """Владелец разобрал тему руками — спрашивать его про неё снова незачем."""
        calls = []
        with patch.object(db, "set_theme_fill_state",
                          lambda t, s, **k: calls.append((t, s)) or True), \
                patch.object(gen, "commit_manual_words",
                             lambda t, rows: {"added": 1, "final_verified": 53}):
            ctl.commit_selected_words("haushalt", WordReviewTests.ROWS,
                                      [True, False, False, False])
        self.assertEqual(calls, [("haushalt", "stopped")])


class WordReviewTests(unittest.TestCase):
    """По каждому слову: часть речи, артикль, есть ли в банке, брать или нет.

    Раньше выводилась одна причина отказа, а по принятому слову — вообще ничего. Из такого
    ответа нельзя понять главного: нет ли этого слова у нас уже. Искать глазами по списку
    из полутора тысяч слов — не работа владельца, для этого и есть автоматика."""

    ROWS = [
        {"word": "Lampion", "article": "der", "article_source": "wiktionary",
         "meaning_ru": "фонарик", "is_noun": True, "where": None, "rank": 12000,
         "ok": True, "reason": "", "verified": True},
        {"word": "kotzen", "article": "", "article_source": "", "meaning_ru": "блевать",
         "is_noun": False, "where": None, "rank": None, "ok": False,
         "reason": "не существительное — артикля у него нет", "verified": True},
        {"word": "Feuerwerk", "article": "das", "article_source": "wiktionary",
         "meaning_ru": "салют", "is_noun": True, "rank": 7111,
         "where": {"theme_key": "feste_traditionen", "label": "Праздники и традиции",
                   "article": "das", "retired": False},
         "ok": False, "reason": "уже стоит в теме «Праздники и традиции»", "verified": True},
        {"word": "Konfettikanone", "article": "die", "article_source": "справочник молчит",
         "meaning_ru": "хлопушка", "is_noun": True, "where": None, "rank": None,
         "ok": False, "reason": "род не подтверждён справочником — пойдёт на ревью артикля",
         "verified": False},
    ]

    def _text(self, selected=None):
        sel = selected or [bool(r["ok"]) for r in self.ROWS]
        with patch.object(db, "get_article_sprint_theme", lambda t: {"label_ru": "Вечеринки"}):
            return ctl.review_words_text("party_freizeit", self.ROWS, sel)

    def test_it_says_where_the_word_already_lives(self):
        # Главное, ради чего разбор и нужен: владелец не должен искать дубль глазами.
        text = self._text()
        self.assertIn("уже в банке", text)
        self.assertIn("Праздники и традиции", text)

    def test_a_new_word_is_reported_as_absent(self):
        self.assertIn("в банке нет", self._text())

    def test_part_of_speech_and_article_are_stated_for_every_word(self):
        text = self._text()
        self.assertIn("существительное · артикль", text)
        self.assertIn("wiktionary", text, "источник артикля должен быть виден")
        self.assertIn("не существительное — артикля нет", text)

    def test_frequency_is_translated_into_words(self):
        text = self._text()
        self.assertIn("ходовое", text)
        self.assertIn("в 50 000 самых частых нет", text)

    def test_a_recommendation_is_given_for_every_word(self):
        text = self._text()
        self.assertIn("предлагаю взять", text)
        self.assertIn("не советую", text)

    def test_an_unconfirmed_gender_is_not_called_a_rejection(self):
        # Со словом всё в порядке, подтвердить надо только артикль.
        text = self._text()
        self.assertIn("взять можно", text)
        self.assertNotIn("не советую: род не подтверждён", text)

    def test_ticks_follow_the_recommendation(self):
        text = self._text()
        self.assertIn("☑️ <b>1.", text)
        self.assertIn("⬜️ <b>2.", text)

    def test_the_owner_is_told_he_can_overrule(self):
        self.assertIn("твоё решение главнее", self._text())

    def test_words_are_split_by_commas_and_lines(self):
        self.assertEqual(ctl.split_words("Besen, Eimer\nLappen\n  Schrubber  "),
                         ["Besen", "Eimer", "Lappen", "Schrubber"])

    def test_an_empty_message_yields_no_words(self):
        self.assertEqual(ctl.split_words("   \n  "), [])


class WordCommitTests(unittest.TestCase):
    def _commit(self, selected, added=None):
        seen = {}

        def _commit_rows(theme_key, rows):
            seen["rows"] = rows
            return {"added": len(rows) if added is None else added, "final_verified": 53}

        with patch.object(db, "set_theme_fill_state", lambda *a, **k: True), \
                patch.object(gen, "commit_manual_words", _commit_rows):
            text = ctl.commit_selected_words("party_freizeit", WordReviewTests.ROWS, selected)
        return text, seen

    def test_only_ticked_words_are_written(self):
        _, seen = self._commit([True, False, False, False])
        self.assertEqual([r["word"] for r in seen["rows"]], ["Lampion"])

    def test_the_owner_can_overrule_a_rejection(self):
        # Он отметил слово, которое проверка забраковала: его решение главнее.
        _, seen = self._commit([False, True, False, False])
        self.assertEqual([r["word"] for r in seen["rows"]], ["kotzen"])

    def test_the_answer_names_the_words_not_just_a_count(self):
        text, _ = self._commit([True, False, False, False])
        self.assertIn("der Lampion", text)
        self.assertIn("53 слова", text)

    def test_nothing_ticked_writes_nothing(self):
        text, seen = self._commit([False, False, False, False])
        self.assertNotIn("rows", seen, "до банка пустой выбор доходить не должен")
        self.assertIn("Ничего не отмечено", text)

    def test_a_word_that_did_not_land_is_admitted(self):
        text, _ = self._commit([True, True, False, False], added=1)
        self.assertIn("Легло 1 из 2", text)


class ExhaustionSignalTests(unittest.TestCase):
    """Сигнал «тема выдохлась» = два прогона подряд без прироста."""

    def test_growth_resets_the_streak(self):
        self.assertGreaterEqual(db.THEME_FILL_MIN_GROWTH, 1)
        self.assertEqual(db.THEME_FILL_DRY_LIMIT, 2)

    def test_autofill_skips_a_theme_that_is_not_on_auto(self):
        filled = []
        with patch.object(db, "list_theme_fill_report", lambda: [
                {"theme_key": "party", "state": "paused"},
                {"theme_key": "gefuehle", "state": "auto"}]), \
                patch.object(db, "record_theme_fill_run",
                             lambda t, a: {"dry_streak": 0, "exhausted": False}), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch("backend.article_sprint_themes.article_sprint_themes", lambda: [
                    {"key": "party", "target_count": 150},
                    {"key": "gefuehle", "target_count": 150}]), \
                patch.object(gen, "fill_theme",
                             lambda k, **kw: filled.append(k) or {"added": 5}):
            gen.autofill_growing_themes(per_theme_cap=10, total_cap=50)
        self.assertEqual(filled, ["gefuehle"], "остановленную тему ночной прогон не трогает")

    def test_an_exhausted_theme_is_reported_back(self):
        with patch.object(db, "list_theme_fill_report", lambda: []), \
                patch.object(db, "record_theme_fill_run",
                             lambda t, a: {"dry_streak": 2, "exhausted": True}), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch("backend.article_sprint_themes.article_sprint_themes",
                      lambda: [{"key": "party", "target_count": 150}]), \
                patch.object(gen, "fill_theme", lambda k, **kw: {"added": 0}):
            res = gen.autofill_growing_themes(per_theme_cap=10, total_cap=50)
        self.assertEqual(res["exhausted"], ["party"])


class SendingTellsTheTruthTests(unittest.TestCase):
    """«Отправлено 1» при 401 от Telegram — отчёт, которому верят, а сообщения нет."""

    def _send(self, status):
        import backend.database as dbm

        class _Resp:
            status_code = status
            text = "{\"ok\":false,\"description\":\"Unauthorized\"}"

        with patch.dict("os.environ", {"TELEGRAM_Deutsch_BOT_TOKEN": "t"}), \
                patch.object(dbm, "get_admin_telegram_ids", lambda: {1}), \
                patch.object(dbm, "list_theme_fill_report", lambda: ReportWordingTests.ROWS), \
                patch.object(dbm, "expire_stale_awaiting_themes", lambda *a, **k: []), \
                patch.object(ctl.requests, "post", lambda *a, **k: _Resp()):
            return ctl.send_fill_control_dm(force=True)

    def test_a_rejected_message_is_not_counted_as_sent(self):
        res = self._send(401)
        self.assertEqual(res["sent"], 0)
        self.assertFalse(res["ok"], "не дошло ни до кого — значит не ok")
        self.assertTrue(res["failed"], "причина отказа должна быть видна")

    def test_a_delivered_message_is_counted(self):
        res = self._send(200)
        self.assertEqual(res["sent"], 1)
        self.assertTrue(res["ok"])


class ButtonsStayWithinReachTests(unittest.TestCase):
    """Ссылаться на кнопку, которой рядом нет, — значит заставить искать её в ленте."""

    def test_the_answer_after_manual_words_carries_its_own_buttons(self):
        kb = ctl.after_words_keyboard("party_freizeit")
        data = [b["callback_data"] for row in kb["inline_keyboard"] for b in row]
        self.assertIn("artfill:go:party_freizeit", data)
        self.assertIn("artfill:mine:party_freizeit", data)

    def test_the_answer_does_not_send_the_owner_hunting_for_a_button(self):
        def _commit_rows(theme_key, rows):
            return {"added": len(rows), "final_verified": 52}

        with patch.object(db, "set_theme_fill_state", lambda *a, **k: True), \
                patch.object(gen, "commit_manual_words", _commit_rows):
            text = ctl.commit_selected_words("party_freizeit", WordReviewTests.ROWS,
                                             [True, False, False, False])
        self.assertNotIn("кнопкой", text, "кнопка едет вместе с сообщением, а не словами")
        self.assertIn("52 слова", text, "число слов склоняем")

    def test_a_decision_can_be_taken_back(self):
        stop = ctl.decision_keyboard("stop", "party_freizeit")
        self.assertIn("artfill:go:party_freizeit",
                      [b["callback_data"] for row in stop["inline_keyboard"] for b in row])
        go = ctl.decision_keyboard("go", "party_freizeit")
        self.assertIn("artfill:stop:party_freizeit",
                      [b["callback_data"] for row in go["inline_keyboard"] for b in row])

    def test_an_unknown_action_gets_no_keyboard(self):
        self.assertIsNone(ctl.decision_keyboard("wat", "party_freizeit"))


if __name__ == "__main__":
    unittest.main()
