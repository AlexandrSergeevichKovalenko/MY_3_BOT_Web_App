# -*- coding: utf-8 -*-
"""Исправленное слово вычищает старое ОТОВСЮДУ, а не переписывает заголовок.

ЧТО СЛОМАЛОСЬ И КАК ЭТО ВЫГЛЯДЕЛО. 25.08.2026 владелец разобрал экран проверки слов:
двенадцать слов, три правки. «das Scheinwerfergla» он переписал в «der Scheinwerfer»,
«KEHREN» — в «den Boden kehren», у обрубка «die Abschiebu» принял подсказку
«die Abschiebung». В базе изменились ровно две графы из восьми: word_de и
translation_ru. В той же строке остались нетронутыми:

    word_ru          «стекло фары»            — старый перевод
    translation_de   «das Scheinwerfergla»    — старое, обрубленное слово
    response_json    весь разбор про обрубок: значения, примеры, формы, род
    canonical_entry_id → запись ОБЩЕГО пула, собранная вокруг обрубка

Это не косметика. Тренажёр берёт текст карточки ИЗ РАЗБОРА, а не из заголовка
(frontend/src/App.jsx, resolveFlashcardTexts: первым стоит responseJson.source_text).
То есть человек исправлял слово — и на повторении продолжал бы учить старое. Ради
этого экран и заводился, так что дефект отменял смысл всей затеи.

Второй дефект, найденный в этом же месте: решение «свой вариант», где человек поправил
ТОЛЬКО перевод, не подходило ни под одну ветку и проваливалось в удаление строки.

Здесь закреплено обратное поведение — по каждому пункту.
"""
import sys
import types
import unittest


class _Cursor:
    """Курсор, который отвечает по существу запроса и запоминает всё, что в него писали."""

    def __init__(self, *, cards, pool):
        self.cards = cards          # строки словаря человека
        self.pool = pool            # id записи пула → (source_text, target_text, word_de, translation_de)
        self.statements: list[tuple[str, tuple]] = []
        self._last = ""

    def execute(self, sql, params=None):
        flat = " ".join(str(sql).split())
        self.statements.append((flat, tuple(params or ())))
        self._last = flat
        self._params = tuple(params or ())

    def fetchall(self):
        if "FROM bt_3_word_check" in self._last:
            return []
        # Перед удалением читаются ТРИ графы — что именно уносим. Отдаём ровно их,
        # а не всю строку: подставной курсор, отвечающий не по форме запроса,
        # прячет ошибки распаковки вместо того, чтобы их показывать.
        if self._last.startswith("SELECT id, word_de, COALESCE(translation_ru"):
            return [(c[0], c[1], c[4]) for c in self.cards]
        if "FROM bt_3_webapp_dictionary_queries" in self._last:
            return list(self.cards)
        return []

    def fetchone(self):
        if "FROM bt_3_dictionary_entries" in self._last:
            return self.pool.get(int(self._params[0]))
        return None

    def __enter__(self): return self
    def __exit__(self, *a): return False


class _Conn:
    def __init__(self, cursor): self._c = cursor; self.committed = False
    def cursor(self): return self._c
    def commit(self): self.committed = True
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _run(decisions, *, cards, pool, article="der"):
    """Прогнать решения через настоящий apply_decisions на подставном курсоре."""
    import backend.database as database
    from backend import word_confirm_digest as mod

    cur = _Cursor(cards=cards, pool=pool)
    conn = _Conn(cur)
    saved_ctx = database.get_db_connection_context
    stub = types.ModuleType("backend.article_authority")
    stub.authoritative_article = lambda word, *, allow_network=False: (article, "тест")
    saved_mod = sys.modules.get("backend.article_authority")
    sys.modules["backend.article_authority"] = stub
    database.get_db_connection_context = lambda *a, **k: conn
    try:
        counts = mod.apply_decisions(117649764, decisions)
    finally:
        database.get_db_connection_context = saved_ctx
        if saved_mod is None:
            sys.modules.pop("backend.article_authority", None)
        else:
            sys.modules["backend.article_authority"] = saved_mod
    return counts, cur


# Карточка ровно в том виде, в каком она лежала у владельца 25.08.2026.
СТЕКЛО = (26494, "das Scheinwerfergla", "стекло фары", "das Scheinwerfergla",
          "стекло фары", 47043)
ПУЛ = {47043: ("das Scheinwerfergla", "стекло фары", "das Scheinwerfergla", "стекло фары")}


class CorrectionWipesTheOldWordTest(unittest.TestCase):

    def setUp(self):
        self.counts, self.cur = _run(
            [{"word": "das Scheinwerfergla", "action": "manual",
              "text": "Scheinwerfer", "translation": "Фара, прожектор"}],
            cards=[СТЕКЛО], pool=dict(ПУЛ))
        # Только переписывание самой карточки: снятие указателей на убранную запись
        # пула — отдельный запрос по той же таблице, и путать их нельзя.
        self.updates = [(s, p) for s, p in self.cur.statements
                        if s.startswith("UPDATE bt_3_webapp_dictionary_queries SET word_de")]

    def test_правка_вообще_доехала(self):
        self.assertEqual(self.counts["исправлено"], 1)
        self.assertEqual(len(self.updates), 1, self.cur.statements)

    def test_старый_разбор_стёрт_целиком(self):
        """Разбор про «стекло фары» под заголовком «der Scheinwerfer» — это ровно то,
        что человек и просил убрать. Правится не текст внутри, а вся карточка."""
        sql, _ = self.updates[0]
        for графа in ("response_json = NULL", "canonical_entry_id = NULL",
                      "lex_unit_id = NULL"):
            self.assertIn(графа, sql, графа)

    def test_обе_стороны_карточки_переписаны(self):
        sql, params = self.updates[0]
        self.assertIn("der Scheinwerfer", params)        # немецкая главная
        self.assertIn("Фара, прожектор", params)         # русская главная
        # Зеркальные графы шли следом за главными и держали старое слово.
        self.assertEqual(params.count("der Scheinwerfer"), 2, params)
        self.assertEqual(params.count("Фара, прожектор"), 2, params)

    def test_обрубок_снят_из_общего_пула(self):
        """Пул — кеш поиска: пока запись жива, «стекло фары» ищется обрубком у ВСЕХ."""
        deletes = [p for s, p in self.cur.statements
                   if s.startswith("DELETE FROM bt_3_dictionary_entries")]
        self.assertEqual(deletes, [(47043,)], self.cur.statements)
        # И ни одна карточка не остаётся с указателем на снятую запись.
        self.assertTrue(any(s.startswith("UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL")
                            for s, _ in self.cur.statements))

    def test_кеш_поиска_по_обрубку_снят(self):
        self.assertTrue(any(s.startswith("DELETE FROM bt_3_dictionary_lookup_cache")
                            for s, _ in self.cur.statements))

    def test_карточка_человека_не_удалена(self):
        self.assertEqual(self.counts["удалено"], 0)
        self.assertFalse([s for s, _ in self.cur.statements
                          if s.startswith("DELETE FROM bt_3_webapp_dictionary_queries")])


class ForeignPoolEntryIsNotTouchedTest(unittest.TestCase):
    """Запись пула про ДРУГОЕ слово не снимаем: указатель мог вести куда угодно."""

    def test_чужая_запись_пула_остаётся(self):
        _counts, cur = _run(
            [{"word": "das Scheinwerfergla", "action": "manual",
              "text": "Scheinwerfer", "translation": ""}],
            cards=[СТЕКЛО],
            pool={47043: ("das Auto", "машина", "das Auto", "машина")})
        self.assertFalse([s for s, _ in cur.statements
                          if s.startswith("DELETE FROM bt_3_dictionary_entries")])


class HandwrittenFieldIsNotOverwrittenTest(unittest.TestCase):
    """Графа, куда человек вписал СВОЁ, не переписывается и не стирается — считается."""

    def test_своя_графа_остаётся_как_есть(self):
        своя = (26494, "das Scheinwerfergla", "моя пометка про фары",
                "das Scheinwerfergla", "стекло фары", None)
        _counts, cur = _run(
            [{"word": "das Scheinwerfergla", "action": "manual",
              "text": "Scheinwerfer", "translation": "Фара"}],
            cards=[своя], pool={})
        sql, params = [(s, p) for s, p in cur.statements
                       if s.startswith("UPDATE bt_3_webapp_dictionary_queries")][0]
        self.assertIn("word_ru = COALESCE(%s, word_ru)", sql)
        self.assertIn(None, params)                       # зеркала не подставили
        self.assertNotIn("моя пометка про фары", params)  # и не тронули


class TranslationOnlyFixDoesNotDeleteTest(unittest.TestCase):
    """Поправлен только перевод — слово остаётся. Удаляет ТОЛЬКО молчание.

    До 25.08.2026 такое решение не подходило ни под одну ветку и проваливалось в
    удаление строки. Дорога туда открыта самим экраном: поле слова предзаполнено
    этим же словом, а поле перевода подписано «если и он не тот»."""

    def test_слово_не_удалено_а_перевод_записан(self):
        counts, cur = _run(
            [{"word": "das Scheinwerfergla", "action": "manual",
              "text": "Scheinwerfergla", "translation": "стекло фары головного света"}],
            cards=[СТЕКЛО], pool=dict(ПУЛ))
        self.assertEqual(counts["удалено"], 0)
        self.assertFalse([s for s, _ in cur.statements
                          if s.startswith("DELETE FROM bt_3_webapp_dictionary_queries")])
        writes = [p for s, p in cur.statements
                  if s.startswith("UPDATE bt_3_webapp_dictionary_queries SET translation_ru")]
        self.assertEqual(writes, [("стекло фары головного света", 117649764,
                                   "Scheinwerfergla")], cur.statements)

    def test_разбор_при_этом_НЕ_стирается(self):
        """Слово то же — разбор про него же. Стирать его было бы потерей данных."""
        _counts, cur = _run(
            [{"word": "das Scheinwerfergla", "action": "manual",
              "text": "Scheinwerfergla", "translation": "стекло фары головного света"}],
            cards=[СТЕКЛО], pool=dict(ПУЛ))
        self.assertFalse([s for s, _ in cur.statements if "response_json = NULL" in s])


class SilenceNeverDeletesTest(unittest.TestCase):
    """МОЛЧАНИЕ НЕ УДАЛЯЕТ. Решение владельца 25.08.2026.

    Отменяет его же правило от 19.08 («отмеченные остаются, остальные удаляются»).
    Дословно: «нельзя удалять просто потому что кто-то не увидел, может просмотрел
    случайно. Чтобы что-то удалить, человек должен САМ нажать удалить это слово».

    Список бывает на сто слов, экран длинный, палец скользит: пропустить карточку —
    норма поведения, а не решение. И цена ошибки несимметрична: лишнее сомнительное
    слово придёт на проверку снова, стёртое нужное не вернуть ничем.
    """

    def setUp(self):
        self.counts, self.cur = _run(
            [{"word": "das Scheinwerfergla", "action": "", "text": "", "translation": ""}],
            cards=[СТЕКЛО], pool=dict(ПУЛ))

    def test_строка_словаря_не_тронута(self):
        self.assertEqual(self.counts["удалено"], 0)
        self.assertFalse([s for s, _ in self.cur.statements
                          if s.startswith("DELETE FROM bt_3_webapp_dictionary_queries")])

    def test_слово_вернётся_на_проверку(self):
        """Строки в дневнике быть НЕ ДОЛЖНО: именно её отсутствие возвращает слово
        в следующий список. Запишем «keep» — и человек больше никогда его не увидит,
        хотя он его не смотрел."""
        self.assertFalse([s for s, _ in self.cur.statements
                          if "bt_3_word_confirm_digest" in s and s.startswith("INSERT")])

    def test_настоящесть_слова_на_это_больше_не_влияет(self):
        """Признак «модель подтвердила» защищал от удаления молчанием (21.08). Теперь
        не удаляется никто, и спрашивать про это базу незачем."""
        self.assertFalse([s for s, _ in self.cur.statements
                          if "FROM bt_3_word_check" in s and s.startswith("SELECT")])


class OnlyTheDeleteButtonDeletesTest(unittest.TestCase):
    """Единственная дорога к удалению — явно нажатая кнопка «Удалить»."""

    def setUp(self):
        self.counts, self.cur = _run(
            [{"word": "das Scheinwerfergla", "action": "drop", "text": "", "translation": ""}],
            cards=[СТЕКЛО], pool=dict(ПУЛ))

    def test_слово_удалено(self):
        self.assertEqual(self.counts["удалено"], 1)
        self.assertTrue([s for s, _ in self.cur.statements
                         if s.startswith("DELETE FROM bt_3_webapp_dictionary_queries")])

    def test_перед_удалением_прочитали_что_уносим(self):
        """Раньше строка исчезала бесшумно: сработай удаление — и сказать человеку,
        ЧТО у него пропало, было бы нечем. Сначала читаем, потом удаляем."""
        порядок = [s for s, _ in self.cur.statements
                   if "bt_3_webapp_dictionary_queries" in s]
        чтение = next(i for i, s in enumerate(порядок) if s.startswith("SELECT id, word_de"))
        удаление = next(i for i, s in enumerate(порядок) if s.startswith("DELETE"))
        self.assertLess(чтение, удаление, порядок)

    def test_удаление_оставляет_след_в_дневнике(self):
        записи = [p for s, p in self.cur.statements
                  if s.startswith("INSERT INTO bt_3_word_confirm_digest")]
        self.assertTrue(записи)
        self.assertIn("drop", записи[0])


if __name__ == "__main__":
    unittest.main()
