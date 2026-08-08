"""Экран разбора спорных фраз: номер варианта на кнопке = номер, который применит сервер.

Почему это отдельный тест. Судьи расходятся постоянно — ровно поэтому фраза попадает
владельцу. На экране у каждого предложенного варианта своя кнопка с номером, и тот же
номер уходит обратно в решении. Если нумерация на сервере при сборке экрана и при
применении решения разойдётся хоть на единицу, владелец нажмёт «Принять 2», а в словарь
уедет вариант первого судьи — молча и без следа. Проверяем, что оба места считают
варианты одной и той же функцией и одинаково.
"""
import unittest
from unittest.mock import patch


REVIEWS = [
    {
        "id": 7, "unit_id": 42, "text": "Er hat hoch bekommen", "translation": "У него встал",
        "judges": [
            {"verdict": "error", "category": "wortstellung",
             "corrected": "Er hat hochbekommen", "proposal": "", "why": "Слитно."},
            {"verdict": "error", "category": "wortstellung",
             "corrected": "Er hat hoch bekommen", "proposal": "Er hat es hochbekommen",
             "why": "Порядок слов."},
        ],
    },
]


class VariantNumberingTests(unittest.TestCase):
    def _payload(self):
        from backend.backend_server import _phrase_review_payload
        with patch("backend.database.list_open_phrase_reviews", return_value=REVIEWS):
            return _phrase_review_payload()

    def test_every_distinct_judge_variant_becomes_a_button(self):
        item = self._payload()["items"][0]
        self.assertEqual([v["text"] for v in item["variants"]],
                         ["Er hat hochbekommen", "Er hat es hochbekommen"])

    def test_a_fix_that_changes_nothing_is_not_a_button(self):
        """Судья 2 объявил ошибку порядка слов и «исправил» фразу в саму себя. Кнопка на
        такой вариант ничего не меняет, но выглядит как решение — её быть не должно."""
        item = self._payload()["items"][0]
        self.assertNotIn(REVIEWS[0]["text"], [v["text"] for v in item["variants"]])

    def test_button_index_matches_what_the_server_would_apply(self):
        from backend.database import phrase_review_variants
        item = self._payload()["items"][0]
        applied = phrase_review_variants(REVIEWS[0]["judges"], REVIEWS[0]["text"])
        for v in item["variants"]:
            self.assertEqual(v["text"], applied[v["index"]]["text"],
                             "номер на кнопке не совпал с тем, что применит сервер")

    def test_judge_block_carries_the_same_number_as_the_button(self):
        """Рядом с вариантом в разборе стоит тот же номер, что на кнопке — иначе по
        кнопке нельзя понять, чей вариант принимаешь."""
        item = self._payload()["items"][0]
        by_text = {v["text"]: v["index"] for v in item["variants"]}
        for j in item["judges"]:
            if j["corrected"] and j["corrected"] in by_text:
                self.assertEqual(j["corrected_slot"], by_text[j["corrected"]])
            if j["proposal"] and j["proposal"] in by_text:
                self.assertEqual(j["proposal_slot"], by_text[j["proposal"]])

    def test_completion_is_marked_as_added_words(self):
        """Достройка неполной фразы и обычная правка — разные вещи, и владелец должен
        видеть, где судья ДОПИСАЛ слова, а где только поправил окончание."""
        item = self._payload()["items"][0]
        kinds = {v["text"]: v["kind"] for v in item["variants"]}
        self.assertEqual(kinds["Er hat hochbekommen"], "fix")
        self.assertEqual(kinds["Er hat es hochbekommen"], "complete")


if __name__ == "__main__":
    unittest.main()


class DeadEndTests(unittest.TestCase):
    """Разбор обязан иметь выход, когда править нечего.

    Замер 08.08.2026: после переспроса обычный исход — оба судьи говорят «ошибки нет».
    Принимать тогда нечего, и из решений оставались «удалить» (уничтожить ВЕРНУЮ фразу)
    и «отложить» (ничего не решает). Это тупик, и владелец в него упёрся на первой же
    фразе. Решение «keep» закрывает вопрос: фраза остаётся, ночь помечает её проверенной."""

    def test_keep_is_an_accepted_decision(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "backend_server.py").read_text(encoding="utf-8")
        start = src.index("def answer_phrase_review_decide")
        block = src[start:start + 2000]
        self.assertIn('"keep"', block, "экран не может закрыть вопрос по хорошей фразе")

    def test_keep_marks_the_phrase_checked_so_it_never_returns(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "database.py").read_text(encoding="utf-8")
        start = src.index('if decision == "keep":')
        block = src[start:start + 900]
        self.assertIn("bt_3_phrase_check", block,
                      "фраза вернётся в разбор следующей же ночью")
        self.assertIn("'kept'", block)

    def test_screen_offers_keep_even_when_judges_proposed_a_fix(self):
        """Владелец вправе не согласиться с судьями — кнопка не должна прятаться."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[2]
               / "frontend/src/answer/PhraseReviewScreen.jsx").read_text(encoding="utf-8")
        start = src.index("decide('keep')")
        # кнопка стоит вне ветки «вариантов нет»
        self.assertNotIn("!variants.length ? (\n          <button className=\"ans-btn-ghost\" disabled={busy} onClick={() => decide('keep')}",
                         src[max(0, start - 400):start + 50])


class NightlyPickerAdvancesTests(unittest.TestCase):
    """Ночная выборка обязана двигаться вперёд, а не пересуживать проверенное.

    Замер 08.08.2026 на живой базе: проверено 491, непроверенных 8709, а следующая ночь
    брала 500 фраз, из которых 491 уже проверена — то есть продвигалась на девять штук
    и тратила тысячу запросов к GPT впустую. Виновато было условие
    `c.text_hash <> ''` — истина для любой проверенной строки."""

    def test_picker_takes_only_never_checked_phrases(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "database.py").read_text(encoding="utf-8")
        start = src.index("def pick_phrases_for_grammar_check")
        # только сам запрос: в docstring условие процитировано как история дефекта
        sql = src[src.index("cursor.execute(", start):start + 2600]
        self.assertNotIn("c.text_hash <>", sql,
                         "сравнение хеша с пустой строкой снова пересуживает проверенное")
        self.assertIn("AND c.unit_id IS NULL", sql)

    def test_picker_and_the_left_counter_agree(self):
        """Отчёт обещает «осталось N, ≈N/500 ночей». Если выборка и счётчик смотрят на
        разное, это обещание — вымысел."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "database.py").read_text(encoding="utf-8")
        pick = src[src.index("def pick_phrases_for_grammar_check"):][:2600]
        left = src[src.index("def count_phrases_left_for_grammar_check"):][:1200]
        self.assertIn("AND c.unit_id IS NULL", pick)
        self.assertIn("AND c.unit_id IS NULL", left)


# `better` намеренно НЕ совпадает ни с одним вариантом судей: если бы совпал, кнопка
# осталась бы одна — так и задумано, дубли не плодим.
ARBITER = {"winner": 1, "why": "Глагол пишется слитно.",
           "better": "Er hat den Koffer hochbekommen"}
DISPUTED = [dict(REVIEWS[0], arbiter=ARBITER)]


class ArbiterTests(unittest.TestCase):
    """Спор двух судей разрешает третий, и его слово должно совпадать с кнопками.

    Владелец не обязан знать, пишется ли «hochbekommen» слитно. Две кнопки без
    объяснения — это загадка, а не решение: он спросил «а как решить?!», и это был
    честный вопрос к интерфейсу, а не к нему."""

    def _payload(self, rows):
        from backend.backend_server import _phrase_review_payload
        with patch("backend.database.list_open_phrase_reviews", return_value=rows):
            return _phrase_review_payload()

    def test_winner_index_points_at_the_right_button(self):
        item = self._payload(DISPUTED)["items"][0]
        win = item["arbiter"]["winner_index"]
        self.assertEqual(item["variants"][win]["text"], "Er hat hochbekommen")

    def test_arbiters_own_text_goes_last_and_does_not_shift_the_others(self):
        """Владелец мог смотреть на экран ДО того, как спор разрешили. Если бы вариант
        третейского встал первым, «Принять 1» под его рукой стало бы другим текстом."""
        before = self._payload(REVIEWS)["items"][0]["variants"]
        after = self._payload(DISPUTED)["items"][0]["variants"]
        self.assertEqual([v["text"] for v in after][:len(before)], [v["text"] for v in before])
        self.assertEqual(after[-1]["text"], ARBITER["better"])
        self.assertEqual(after[-1]["kind"], "arbiter")

    def test_arbiters_text_equal_to_an_existing_one_does_not_duplicate(self):
        same = {"winner": 2, "why": "…", "better": "Er hat es hochbekommen"}
        item = self._payload([dict(REVIEWS[0], arbiter=same)])["items"][0]
        texts = [v["text"] for v in item["variants"]]
        self.assertEqual(len(texts), len(set(texts)))

    def test_arbiter_variant_is_applied_by_the_same_number(self):
        from backend.database import phrase_review_variants
        item = self._payload(DISPUTED)["items"][0]
        applied = phrase_review_variants(REVIEWS[0]["judges"], REVIEWS[0]["text"], ARBITER)
        for v in item["variants"]:
            self.assertEqual(v["text"], applied[v["index"]]["text"])


class StaleConfirmationTests(unittest.TestCase):
    """Подтверждение решения всегда про ПРЕДЫДУЩУЮ фразу — значит, обязано её называть.

    Безымянное «✅ вопрос закрыт» висело посреди разбора уже следующей фразы, и прочесть
    его иначе как решение по ней было нельзя. Владелец обвёл это на скриншоте."""

    def test_banner_names_the_phrase_and_expires(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[2]
               / "frontend/src/answer/PhraseReviewScreen.jsx").read_text(encoding="utf-8")
        self.assertIn("frrev-done", src, "нет отдельного баннера о прошлом решении")
        self.assertIn("{done.text}", src, "баннер не называет фразу, к которой относится")
        self.assertIn("setTimeout(() => setDone(null)", src, "баннер не гаснет")

    def test_decision_does_not_reuse_the_inline_note(self):
        """Строка note живёт внутри разбора текущей фразы — решению там не место."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[2]
               / "frontend/src/answer/PhraseReviewScreen.jsx").read_text(encoding="utf-8")
        start = src.index("const applyResponse")
        self.assertIn("setNote('')", src[start:start + 400],
                      "подтверждение снова оседает в разборе следующей фразы")


class ScreenFitsOneScreenTests(unittest.TestCase):
    """Разбор судей — главное на экране, и он не должен жить в щели в две строки.

    Видео владельца 08.08.2026: вердикты двух судей приходится прокручивать, потому что
    нижнюю половину занимают кнопки. Смотреть надо на разбор, а решать — потом."""

    def _src(self, name):
        import pathlib
        return (pathlib.Path(__file__).resolve().parents[2]
                / f"frontend/src/answer/{name}").read_text(encoding="utf-8")

    def test_decisions_are_laid_out_in_rows_not_a_column(self):
        src = self._src("PhraseReviewScreen.jsx")
        self.assertGreaterEqual(src.count('className="frrev-row"'), 2,
                                "решения снова занимают по строке каждое")

    def test_review_block_takes_the_free_height(self):
        css = self._src("answer.css")
        self.assertIn(".frrev-w .frrev-scroll { flex: 1 1 auto; }", css)
        self.assertIn(".frrev-w .ans-btn ", css, "кнопки не ужаты — высоту забирают они")

    def test_sweep_button_is_offered_when_the_queue_holds_empty_complaints(self):
        src = self._src("PhraseReviewScreen.jsx")
        self.assertIn("frrev-sweep", src)
        self.assertIn("dropnoise", src, "нельзя убрать пустые придирки одним нажатием")


class AskOnTheScreenTests(unittest.TestCase):
    """Вопрос про фразу задаётся там же, где по ней принимается решение.

    Владельцу пришлось уйти в другое приложение, чтобы выяснить, что «Wappnen mit» и
    «Wappnen gegen» — разные значения, а не ошибка. Ответ он получил верный, а экран
    его к этому вопросу даже не подпускал."""

    def test_endpoint_feeds_the_saved_meaning_into_the_answer(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "backend_server.py").read_text(encoding="utf-8")
        start = src.index("def answer_phrase_review_ask")
        block = src[start:src.index("\n@app.route", start)]
        self.assertIn("row['translation']", block,
                      "ответ будет таким же слепым, как был вердикт")
        self.assertIn("run_quick_ask", block)

    def test_screen_has_the_ask_field(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[2]
               / "frontend/src/answer/PhraseReviewScreen.jsx").read_text(encoding="utf-8")
        self.assertIn("phrasereview/ask", src)
        self.assertIn("❓ Спросить", src)

    def test_answer_does_not_survive_into_the_next_phrase(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[2]
               / "frontend/src/answer/PhraseReviewScreen.jsx").read_text(encoding="utf-8")
        start = src.index("const applyResponse")
        self.assertIn("setAnswer('')", src[start:start + 500],
                      "ответ про прошлую фразу останется висеть над новой")


class VariantTranslationTests(unittest.TestCase):
    """У предложенной замены обязан быть перевод: иначе не видно, сохранил ли судья
    смысл сохранённой фразы или подменил его другим управлением глагола."""

    def test_variant_carries_its_russian(self):
        from backend.database import phrase_review_variants
        got = phrase_review_variants([
            {"verdict": "error", "category": "praeposition", "corrected": "Wappnen gegen",
             "corrected_ru": "вооружиться против чего-то", "why": "…"},
        ], "Wappnen mit")
        self.assertEqual(got[0]["ru"], "вооружиться против чего-то")

    def test_payload_passes_it_to_the_button(self):
        from backend.backend_server import _phrase_review_payload
        rows = [{"id": 1, "unit_id": 2, "text": "Wappnen mit",
                 "translation": "запастись чем-то",
                 "judges": [{"verdict": "error", "category": "praeposition",
                             "corrected": "Wappnen gegen",
                             "corrected_ru": "вооружиться против", "why": "…"}],
                 "arbiter": None}]
        with patch("backend.database.list_open_phrase_reviews", return_value=rows):
            item = _phrase_review_payload()["items"][0]
        self.assertEqual(item["variants"][0]["ru"], "вооружиться против")
        self.assertEqual(item["judges"][0]["corrected_ru"], "вооружиться против")
