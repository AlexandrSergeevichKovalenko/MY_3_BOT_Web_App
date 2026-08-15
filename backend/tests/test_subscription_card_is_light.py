"""Выдача слова по подписке НЕ делает человеку копию разбора.

Разбор — общий, он живёт на слове. Человеку выдаётся лёгкая карточка с указателем, и
содержимое приезжает со слова при показе. Если у слова разбора ещё нет — его туда
поднимают из карточки-источника, чтобы он достался ВСЕМ, а не одному.

До 15.08.2026 было наоборот: разбор копировался человеку целиком, решение принималось
один раз в момент выдачи и никогда не пересматривалось. Слово улучшали потом — у
человека навсегда оставался снимок того дня.
"""
import backend.database as database

SOURCE_CARD = {
    "word_de": "die Habe",
    "usage_examples": [{"source": "Er musste seine Habe zurücklassen.", "target": "Ему пришлось оставить имущество."}],
    "dictionary_senses": [{"rank": 1, "value": "имущество"}],
}


class _Cursor:
    """Курсор, который отдаёт карточку-источник и запоминает, что в него писали."""

    def __init__(self, unit_has_card: bool, unit_id):
        self._unit_has_card = unit_has_card
        self._unit_id = unit_id
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))
        self._last = (sql, params)

    def fetchone(self):
        return (
            "имущество", "die Habe", "die Habe", "имущество",
            dict(SOURCE_CARD), "de", "ru", None,
            self._unit_has_card, self._unit_id,
        )


def _patch(monkeypatch, created, promoted, *, promotion_succeeds=True):
    monkeypatch.setattr(
        database, "list_admin_subscription_new_candidates",
        lambda **_kw: [{"canonical_entry_id": 42}],
    )

    def _create(cur, **kwargs):
        created.append(kwargs)
        return 555, True

    monkeypatch.setattr(
        database, "_create_or_attach_user_dictionary_entry_with_cursor", _create,
    )
    import backend.lex_units as lex_units

    def _save(unit_id, card, *, source="", cursor=None):
        promoted.append((unit_id, card, source, cursor))
        return promotion_succeeds

    monkeypatch.setattr(lex_units, "save_unit_card_if_richer", _save)


def _content_keys(payload):
    return [k for k in database.CARD_CONTENT_KEYS if payload.get(k)]


def test_card_is_light_when_the_word_already_has_the_breakdown(monkeypatch):
    created, promoted = [], []
    _patch(monkeypatch, created, promoted)
    cur = _Cursor(unit_has_card=True, unit_id=15484)

    entry_id = database.materialize_subscription_card(
        user_id=999, source_user_id=1, source_lang="de", target_lang="ru", cursor=cur,
    )

    assert entry_id == 555
    assert not promoted, "разбор на слове уже есть — поднимать нечего"
    assert _content_keys(created[0]["response_json"]) == [], "копия разбора не кладётся"


def test_breakdown_goes_up_to_the_word_when_it_has_none(monkeypatch):
    created, promoted = [], []
    _patch(monkeypatch, created, promoted)
    cur = _Cursor(unit_has_card=False, unit_id=15484)

    database.materialize_subscription_card(
        user_id=999, source_user_id=1, source_lang="de", target_lang="ru", cursor=cur,
    )

    assert promoted, "разбор обязан подняться на слово, а не лечь копией одному человеку"
    unit_id, card, _source, passed_cursor = promoted[0]
    assert unit_id == 15484
    assert card["usage_examples"] == SOURCE_CARD["usage_examples"]
    assert passed_cursor is cur, "писать надо тем же курсором: второе соединение из пула брать нельзя"
    assert _content_keys(created[0]["response_json"]) == [], "разбор уже на слове — копия не нужна"


def test_copy_stays_when_the_breakdown_could_not_go_up(monkeypatch):
    """Единственный случай, когда копия оправдана: на слово разбор не лёг.

    Пустая карточка хуже лишней копии."""
    created, promoted = [], []
    _patch(monkeypatch, created, promoted, promotion_succeeds=False)
    cur = _Cursor(unit_has_card=False, unit_id=15484)

    database.materialize_subscription_card(
        user_id=999, source_user_id=1, source_lang="de", target_lang="ru", cursor=cur,
    )

    assert _content_keys(created[0]["response_json"]), "человек не должен остаться с пустой карточкой"


def test_pointer_to_the_word_is_set_in_the_same_transaction(monkeypatch):
    """Без указателя лёгкая карточка была бы ПУСТОЙ: содержимое ищут по нему."""
    created, promoted = [], []
    _patch(monkeypatch, created, promoted)
    cur = _Cursor(unit_has_card=True, unit_id=15484)

    database.materialize_subscription_card(
        user_id=999, source_user_id=1, source_lang="de", target_lang="ru", cursor=cur,
    )

    wrote_pointer = [s for s in cur.statements if "SET lex_unit_id" in s]
    assert wrote_pointer, "указатель на слово должен ставиться сразу, а не отдельным проходом потом"


def test_no_pointer_no_pointer_write(monkeypatch):
    """Слова в слое единиц нет — писать нечего, и карточка остаётся с копией."""
    created, promoted = [], []
    _patch(monkeypatch, created, promoted)
    cur = _Cursor(unit_has_card=False, unit_id=None)

    database.materialize_subscription_card(
        user_id=999, source_user_id=1, source_lang="de", target_lang="ru", cursor=cur,
    )

    assert not promoted, "некуда поднимать: у карточки нет слова"
    assert not [s for s in cur.statements if "SET lex_unit_id" in s]
    assert _content_keys(created[0]["response_json"]), "иначе человек остался бы с пустой карточкой"
