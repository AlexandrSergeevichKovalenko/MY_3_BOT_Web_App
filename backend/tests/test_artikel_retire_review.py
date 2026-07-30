"""Разбор снятых слов в личке: спрашиваем только спорное, и спрашиваем один раз.

Чистка словника сняла 2 929 слов. Показывать весь список нельзя (2 784 из них вообще нет
в частотном списке — это и есть хлам), поэтому в очередь попадают только слова с рангом
лучше порога. Слово, про которое уже спросили, второй раз не приходит."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.database as db
from backend.article_retire_review import _word_text, _keyboard


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.sql_log: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql_log.append(" ".join(str(sql).split()))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


class RetireReviewQueueTests(unittest.TestCase):
    ROWS = [
        (1, "Kühler", "die", "радиатор", "auto_fahren"),            # ходовое, но артикль в базе кривой
        (2, "Schmetterlings-Tramete", "der", "губка", "pflanzen"),  # в частотном списке нет — хлам
        (3, "Erwachsene", "die", "взрослая", "menschen"),           # субстантивированное прилагательное
        (4, "Flur", "die", "коридор", "haus_wohnen"),               # двуродовое: der Flur / die Flur
        (5, "Segelboot", "das", "парусник", "verkehr_reisen"),      # ходовое, но справочник смолчит
    ]

    def _rows(self, rows=None, *, authority=None):
        cur = _FakeCursor(self.ROWS if rows is None else rows)

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        def _authority(word, *, allow_network=False):
            table = authority if authority is not None else {"Kühler": "der"}
            verdict = table.get(word)
            return (verdict, "wiktionary") if verdict else (None, "нет данных")

        with patch.object(db, "get_db_connection_context", _fake_ctx), \
                patch("backend.article_authority.authoritative_article", _authority):
            out = db.list_retired_review_candidates(limit=10, max_rank=60000)
        return out, cur

    def test_only_words_that_look_common_are_asked_about(self):
        out, _ = self._rows()
        words = [r["word"] for r in out]
        self.assertIn("Kühler", words)
        self.assertNotIn("Schmetterlings-Tramete", words,
                         "слова, которого нет в частотном списке, спрашивать незачем")

    def test_two_gender_words_stay_in_the_game(self):
        # Игра учит ПОНИМАТЬ артикли, поэтому двуродовые из неё не выбрасываются.
        # Справочник их намеренно не решает — значит артикль ставит владелец, а игроку
        # показывается перевод, и вопрос становится честным.
        out, _ = self._rows()
        flur = next(r for r in out if r["word"] == "Flur")
        self.assertEqual(flur["mode"], "sense")
        self.assertEqual(flur["meaning_ru"], "коридор")
        erwachsene = next(r for r in out if r["word"] == "Erwachsene")
        self.assertEqual(erwachsene["mode"], "sense")

    def test_card_shows_the_checked_article_not_the_stored_one(self):
        # В банке лежала «die Kühler» — из-за такого слова и снимали. Показать надо «der».
        out, _ = self._rows()
        row = next(r for r in out if r["word"] == "Kühler")
        self.assertEqual(row["article"], "der")
        self.assertEqual(row["stored_article"], "die")

    def test_word_without_a_translation_is_not_offered(self):
        # Артикль решает смысл, а смысла не видно — спрашивать не о чем ни владельца,
        # ни потом игрока.
        out, _ = self._rows([(9, "Segelboot", "das", "", "verkehr_reisen")])
        self.assertEqual(out, [])

    def test_most_common_word_comes_first(self):
        out, _ = self._rows()
        self.assertEqual([r["rank"] for r in out], sorted(r["rank"] for r in out))

    def test_already_reviewed_words_are_not_asked_again(self):
        _, cur = self._rows()
        self.assertIn("NOT COALESCE(retire_reviewed, FALSE)", cur.sql_log[0])

    def test_the_same_word_in_two_themes_is_asked_once(self):
        out, _ = self._rows([
            (1, "Kühler", "der", "радиатор", "auto_fahren"),
            (2, "Kühler", "der", "радиатор", "technik_computer"),
        ])
        self.assertEqual(len(out), 1)

    def _restore(self, verdict):
        cur = _FakeCursor([("Kühler", "die", "радиатор")])

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx), \
                patch("backend.article_authority.authoritative_article",
                      lambda w, **k: (verdict, "wiktionary" if verdict else "нет данных")):
            res = db.restore_retired_article_noun(1)
        return res, cur

    def test_restore_writes_the_checked_article_in_every_theme(self):
        res, cur = self._restore("der")
        self.assertEqual(res["article"], "der")
        self.assertEqual(res["stored_article"], "die")
        update = [s for s in cur.sql_log if s.startswith("UPDATE")][0]
        self.assertIn("lower(word) = lower(%s)", update, "вернуть надо во всех темах, не одну строку")
        self.assertIn("retired = FALSE", update)
        self.assertIn("article = %s", update, "в игру должен уйти проверенный артикль")
        self.assertTrue([s for s in cur.sql_log if s.startswith("DELETE FROM bt_3_article_word_blacklist")],
                        "возвращённое слово надо снять со стоп-листа")

    def test_restore_asks_for_the_sense_when_the_gender_depends_on_it(self):
        res, cur = self._restore(None)
        self.assertTrue(res.get("needs_sense"), "не тупик: артикль поставит владелец")
        self.assertFalse([s for s in cur.sql_log if s.startswith("UPDATE")],
                         "пока артикль не выбран, в игру ничего не пишем")

    def test_owner_tap_returns_a_two_gender_word_with_its_sense(self):
        cur = _FakeCursor([("Flur", "die", "коридор")])

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx):
            res = db.restore_retired_article_noun(1, article="der")
        self.assertEqual(res["article"], "der")
        self.assertTrue(res["two_gender"])
        update = [s for s in cur.sql_log if s.startswith("UPDATE")][0]
        self.assertIn("two_gender = TRUE", update, "игра должна показать перевод к такому слову")
        self.assertIn("WHERE id = %s", update, "у каждого смысла своя строка — соседний не трогаем")

    def test_keep_puts_the_word_on_the_stop_list(self):
        cur = _FakeCursor([("Schmetterlings-Tramete",)])

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        recorded = []
        with patch.object(db, "get_db_connection_context", _fake_ctx), \
                patch.object(db, "blacklist_article_words", lambda items: recorded.extend(items)):
            res = db.keep_retired_article_noun(1)
        self.assertEqual(res, {"word": "Schmetterlings-Tramete"})
        self.assertEqual([w for w, _r, _t in recorded], ["Schmetterlings-Tramete"])


class RetireReviewCardTests(unittest.TestCase):
    def test_card_says_what_happened_and_why_it_is_being_asked(self):
        text = _word_text({"word": "Kühler", "article": "der", "meaning_ru": "радиатор", "rank": 13465},
                          index=1, total=10, left=145)
        self.assertIn("der Kühler", text)
        self.assertIn("радиатор", text)
        self.assertIn("убрано из игры", text)
        self.assertIn("13465", text)
        self.assertIn("1 из 10", text)

    def test_card_warns_when_the_stored_article_was_wrong(self):
        text = _word_text({"word": "Kühler", "article": "der", "stored_article": "die",
                           "meaning_ru": "радиатор", "rank": 13465}, index=1, total=10, left=145)
        self.assertIn("die Kühler", text, "надо честно показать, что лежало в базе")
        self.assertIn("Вернём с «der»", text)

    def test_buttons_are_restore_and_junk(self):
        kb = _keyboard(7)["inline_keyboard"][0]
        self.assertEqual([b["callback_data"] for b in kb], ["artret:back:7", "artret:keep:7"])


if __name__ == "__main__":
    unittest.main()
