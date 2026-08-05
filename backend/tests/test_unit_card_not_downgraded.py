"""Разбор на единице нельзя понизить.

Единица — общая: её разбор видит каждый, кто подписан на слово. Поэтому тонкое
сохранение (быстрый перевод, тап в тренажёре, сохранение из игры) не имеет права
затереть собранный ночью полный разбор — понижение получил бы не тот, кто сохранял,
а вообще все.

Сравниваем по числу заполненных СОДЕРЖАТЕЛЬНЫХ блоков, а не по длине текста: длину
раздувают служебные поля и сырой текст запроса, а ценность карточки — в блоках.
"""
import contextlib

from backend import lex_units
from backend.lex_units import CARD_CONTENT_KEYS, card_content_score

FULL_CARD = {
    "word_de": "der Wandel",
    "usage_examples": [{"de": "Der Wandel kommt.", "ru": "Перемены идут."}],
    "meanings": {"primary": {"value": "перемена"}, "secondary": ["сдвиг"]},
    "forms": {"plural": "die Wandel"},
    "government_patterns": ["Wandel + Genitiv"],
    "pronunciation": {"ipa": "ˈvandl̩"},
    "memory_tip": "Wandel — как «вандал», только про перемены",
}

THIN_CARD = {
    "word_de": "der Wandel",
    "translation_ru": "перемена",
    # Ключи есть, содержимого нет — ровно так выглядит тонкое сохранение.
    "usage_examples": [],
    "meanings": {},
}


def test_full_card_scores_higher_than_thin_one():
    assert card_content_score(FULL_CARD) > card_content_score(THIN_CARD)


def test_thin_card_with_empty_blocks_scores_zero():
    """Пустой список и пустой объект — это НЕ заполненный блок. Именно на этом
    когда-то сломался общий пул: он считал такую карточку полной."""
    assert card_content_score(THIN_CARD) == 0


def test_broken_input_is_not_a_crash():
    for value in (None, "", [], 0, "der Wandel", {"word_de": "x"}):
        assert card_content_score(value) == 0, value


def test_every_content_key_counts_once():
    card = {key: ["что-то"] for key in CARD_CONTENT_KEYS}
    assert card_content_score(card) == len(CARD_CONTENT_KEYS)


def test_text_blocks_count_only_when_not_blank():
    assert card_content_score({"memory_tip": "   "}) == 0
    assert card_content_score({"memory_tip": "подсказка"}) == 1


def test_service_fields_do_not_inflate_the_score():
    """Сырой текст запроса и следы сохранения весят много байт и ноль смысла —
    победитель по ним выбираться не должен."""
    noisy = dict(THIN_CARD)
    noisy["raw_text"] = "очень длинный сырой ответ модели " * 50
    noisy["original_query"] = "wandel"
    assert card_content_score(noisy) == 0


# ── само правило записи ───────────────────────────────────────────────────────

def _fake_db(stored_card):
    """База, которая на любой SELECT отдаёт разбор, уже лежащий на единице."""
    class Cursor:
        def execute(self, *_a, **_k):
            pass

        def fetchone(self):
            return (stored_card,)

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    @contextlib.contextmanager
    def ctx():
        yield Conn()

    return ctx


def _catch_saves(monkeypatch, stored_card):
    saved = []
    monkeypatch.setattr(lex_units, "get_db_connection_context", _fake_db(stored_card))
    monkeypatch.setattr(lex_units, "save_unit_card",
                        lambda unit_id, card, source="": saved.append((unit_id, card, source)) or True)
    return saved


def test_thin_save_never_overwrites_a_full_card(monkeypatch):
    saved = _catch_saves(monkeypatch, FULL_CARD)
    assert lex_units.save_unit_card_if_richer(1, THIN_CARD, source="сохранение") is False
    assert saved == []


def test_richer_card_replaces_a_thin_one(monkeypatch):
    saved = _catch_saves(monkeypatch, THIN_CARD)
    assert lex_units.save_unit_card_if_richer(1, FULL_CARD, source="обогащение") is True
    assert saved and saved[0][2] == "обогащение"


def test_empty_unit_accepts_the_first_real_card(monkeypatch):
    saved = _catch_saves(monkeypatch, None)
    assert lex_units.save_unit_card_if_richer(1, FULL_CARD) is True
    assert len(saved) == 1


def test_equal_cards_are_not_rewritten(monkeypatch):
    """Разбор той же полноты переписывать незачем: лишняя запись в общую таблицу
    и лишний повод разойтись."""
    saved = _catch_saves(monkeypatch, FULL_CARD)
    assert lex_units.save_unit_card_if_richer(1, dict(FULL_CARD)) is False
    assert saved == []


def test_nothing_to_save_is_refused_before_touching_the_database(monkeypatch):
    saved = _catch_saves(monkeypatch, None)
    for value in (None, {}, "разбор", THIN_CARD):
        assert lex_units.save_unit_card_if_richer(1, value) is False
    assert saved == []
