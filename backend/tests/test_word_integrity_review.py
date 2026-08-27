"""Разбор противоречивых записей словаря.

Владелец 26.08.2026 выбрал устройство: разбираем в мини-приложении, приглашение приходит
в понедельник и воскресенье, ничего не удаляется само, неразобранное переезжает дальше.

27.08.2026 владелец поймал на экране ЛОЖНЫЙ ДИАГНОЗ: под словом «der Degenerierte» стояло
«существительное, а написано со строчной буквы» — при том, что существительное там с
большой, а со строчной идёт артикль. Проверка сравнивала ОСНОВУ записи (`degeneriert`), а
показывалась ВИТРИНА. Отсюда правила ниже.

Что тут держится:
1. Диагноз описывает то, что проверено, и ничего сверх того.
2. Правка предлагается из ИСТОЧНИКА и называет его. Прочтений бывает два — тогда
   спрашиваем владельца, а не выбираем за него.
3. Артикль к чужому слову не приклеивается, род берётся у арбитра, а не из самой записи.
4. Молчание справочника не становится вечным приговором «правку не предлагаем».
5. Экран в сеть не ходит: в справочник ходит ночь, экран берёт готовое.
6. Очередь копится, «Применить» делает по одному действию на запись.
7. Разбор открыт только владельцу.
"""
import inspect

from backend import backend_server
from backend import database


def _reference(answers):
    """Подменённый справочник: написание → (слово, часть речи)."""
    import backend.german_word_gate as gate

    def fake(word, **kwargs):
        found = answers.get(word)
        if not found:
            return {"status": gate.UNCONFIRMED, "text": word, "pos": "", "source": "молчал"}
        text, pos = found
        status = gate.CONFIRMED if text == word else gate.REPAIRED
        return {"status": status, "text": text, "pos": pos, "source": "справочник"}
    return fake


def test_fix_is_offered_only_when_the_reference_confirms_it(monkeypatch):
    """Заглавная буква не делает слово словом: её подтверждает справочник."""
    import backend.german_word_gate as gate
    import backend.article_authority as authority

    monkeypatch.setattr(gate, "check_word", _reference({"Hammer": ("Hammer", "noun")}))
    monkeypatch.setattr(authority, "authoritative_article", lambda w, **k: ("der", "wiktionary"))

    options = database._word_integrity_options("hammer", "hammer", "noun", "der")
    assert [o["word"] for o in options] == ["Hammer"]
    assert options[0]["gender"] == "der" and options[0]["source"] == "справочник"

    assert database._word_integrity_options("inkelgasse", "die inkelgasse", "noun", "die") == [], (
        "предложили исправить обрывок — заглавная буква не делает слово словом"
    )


def test_a_form_of_another_word_is_offered_as_that_word(monkeypatch):
    """«degeneriert» справочник знает формой глагола — это и предлагаем, БЕЗ артикля.

    Прежде такой ответ выбрасывался целиком («правку не предлагаем»), хотя источник его
    знает: владелец двое суток видел запись без единого предложения починки.
    """
    import backend.german_word_gate as gate
    import backend.article_authority as authority

    monkeypatch.setattr(gate, "check_word", _reference({
        "degeneriert": ("degenerieren", "verb"),
        "Degeneriert": ("degenerieren", "verb"),
    }))
    monkeypatch.setattr(authority, "authoritative_article", lambda w, **k: ("", ""))

    options = database._word_integrity_options("degeneriert", "der Degenerierte", "noun", "der")
    assert [o["word"] for o in options] == ["degenerieren"]
    assert options[0]["pos"] == "verb"
    assert options[0]["gender"] == "", "к глаголу приклеили артикль"


def test_two_legal_readings_go_to_the_owner_as_two_buttons(monkeypatch):
    """Справочник знает оба написания разными словами — выбирает человек, не мы."""
    import backend.german_word_gate as gate
    import backend.article_authority as authority

    monkeypatch.setattr(gate, "check_word", _reference({
        "degeneriert": ("degenerieren", "verb"),
        "Degenerierte": ("Degenerierter", "noun"),
    }))
    monkeypatch.setattr(authority, "authoritative_article", lambda w, **k: ("", ""))

    options = database._word_integrity_options("degeneriert", "der Degenerierte", "noun", "")
    assert [o["word"] for o in options] == ["degenerieren", "Degenerierter"], (
        "одно из законных прочтений выбрано за владельца"
    )


def test_the_article_never_comes_from_the_record_itself(monkeypatch):
    """Род записи сам под подозрением: у «Migrant» в колонке лежит «die» от множественного.

    Сухой прогон 27.08.2026 показал, что старое правило предлагало владельцу «die Migrant».
    Род спрашивается у арбитра — он же чинит род ночью.
    """
    import backend.german_word_gate as gate
    import backend.article_authority as authority

    monkeypatch.setattr(gate, "check_word", _reference({
        "Migrant": ("Migrant", "noun"), "Migranten": ("Migrant", "noun"),
    }))
    monkeypatch.setattr(authority, "authoritative_article", lambda w, **k: ("der", "wiktionary"))

    options = database._word_integrity_options("Migrant", "die Migranten", "noun", "die")
    assert [o["word"] for o in options] == ["Migrant"], "одно слово превратилось в два варианта"
    assert options[0]["gender"] == "der", "род взят из записи, а не у арбитра"


def test_diagnosis_never_claims_lowercase_when_the_shown_word_is_capitalized():
    """Тот самый ложный диагноз: под «der Degenerierte» стояло «написано со строчной»."""
    assert database._word_integrity_issue("degeneriert", "Degenerierte", "noun") == "two_words"
    assert database._word_integrity_issue("hammer", "hammer", "noun") == "noun_lowercase"
    assert database._word_integrity_issue("spr=sk", "Spal", "noun") == "garbage_lemma"
    # ß и ss — РАЗНЫЕ написания. casefold их приравнивает, и пара «zielbewusst» /
    # «zielbewußt» получала диагноз «существительное со строчной», хотя это прилагательное.
    assert database._word_integrity_issue("zielbewusst", "zielbewußt", "adjective") == "two_words"
    # Назвать дефект нечем — владельцу не показываем вовсе.
    assert database._word_integrity_issue("Hammer", "Hammer", "noun") == ""


def test_a_lowercase_noun_still_reaches_the_owner():
    """Заглавная буква и есть предмет разговора — сравнение регистронезависимым быть не может.

    Первая версия правки сравнивала прочтение с записью по `.lower()`, и весь класс
    «существительное со строчной буквы» («hammer» → «Hammer») тихо переставал доходить
    до владельца. Поймано сквозной проверкой на живой базе 27.08.2026.
    """
    question = {"issue": "noun_lowercase", "stored": "hammer", "shown": "hammer",
                "options": [{"word": "Hammer"}]}
    assert database._word_integrity_is_a_question(question) is True
    # А вот когда прочтение уже стоит и в основе, и на витрине — спрашивать не о чем.
    settled = {"issue": "noun_lowercase", "stored": "Hammer", "shown": "Hammer",
               "options": [{"word": "Hammer"}]}
    assert database._word_integrity_is_a_question(settled) is False


def test_a_silent_reference_never_becomes_a_permanent_verdict():
    """Вариантов нет — значит спросим ещё раз, а не «правку не предлагаем» навсегда."""
    src = inspect.getsource(database.scan_word_integrity)
    assert "UPDATE bt_3_word_integrity_review" in src and "jsonb_array_length" in src, (
        "приговор «правки нет» снова бетонируется первым сканом"
    )


def test_the_screen_never_asks_the_reference_over_the_network():
    """В справочник ходит ночь с паузой; экран берёт готовое из кеша двери."""
    assert "allow_network: bool = False" in inspect.getsource(database.scan_word_integrity)
    screen = inspect.getsource(backend_server.list_webapp_word_integrity)
    assert "allow_network" not in screen, "открытие экрана снова пошло в сеть за справочником"

    import io as _io
    from pathlib import Path
    bot = Path(backend_server.__file__).resolve().parents[1] / "bot_3.py"
    text = _io.open(bot, encoding="utf-8").read()
    assert "scan_word_integrity(limit=300, allow_network=True, pace=1.0)" in text, (
        "ночной скан перестал спрашивать справочник — очередь останется без правок"
    )


def test_the_model_hint_reaches_the_queue():
    """«inkelgasse» восстанавливается в «die Winkelgasse» — ночь считает это и для очереди."""
    from backend import german_word_gate
    assert "words_awaiting_integrity_hint" in inspect.getsource(german_word_gate.warm_suggestions), (
        "очередь разбора снова осталась без подсказок модели"
    )
    src = inspect.getsource(database._word_integrity_model_option)
    assert "bt_3_word_suggestion" in src and "модель" in src, (
        "подсказка перестала быть подписанной источником"
    )


def test_the_choice_is_read_from_the_queue_not_from_the_browser():
    """С экрана приходит НОМЕР прочтения; написание сервер берёт из своей очереди."""
    src = inspect.getsource(database.apply_word_integrity_decisions)
    assert 'item.get("option")' in src and "options[index]" in src, (
        "написание принимается с экрана — в словарь можно записать что угодно"
    )
    assert 'confirm_word_by_owner' in src, "своя правка владельца не запоминается как его ответ"


def test_a_new_word_never_keeps_the_old_analysis():
    """Слово сменилось — разбор прежнего слова сносится, а не остаётся под новым."""
    src = inspect.getsource(database._write_word_integrity_choice)
    assert "card = CASE WHEN %(another)s THEN NULL ELSE card END" in src, (
        "под новым заголовком остаётся грамматика чужого слова"
    )
    assert "WHEN %(another)s THEN NULL" in src, "род прежнего слова переезжает на новое"


def test_renaming_keeps_the_word_findable():
    """Переименование идёт через retitle_unit: иначе слово теряет ключ поиска."""
    src = inspect.getsource(database._write_word_integrity_choice)
    assert "retitle_unit" in src, "слово после правки перестанет находиться по своему имени"


def test_the_review_screen_loads_the_stylesheet_that_draws_it():
    """Экран рисуется классами из answer.css. Без импорта текст серый на белом."""
    import io as _io
    from pathlib import Path
    root = Path(backend_server.__file__).resolve().parents[1] / "frontend" / "src"
    screen = _io.open(root / "dictionary" / "WordIntegrityReview.jsx", encoding="utf-8").read()
    assert "answer/answer.css" in screen, "вернулся бледный текст на белом фоне"
    main = _io.open(root / "main.jsx", encoding="utf-8").read()
    start = main.index("async function bootstrapWordIntegrity")
    assert "data-scheme" in main[start:start + 900], "экран снова открывается без отметки темы"


def test_queue_carries_over_and_never_duplicates():
    """Повторный скан не плодит дубли: строка держится на unit_id."""
    src = inspect.getsource(database.scan_word_integrity)
    assert "ON CONFLICT (unit_id) DO NOTHING" in src, "повторный скан начнёт плодить дубли"
    assert "DELETE" not in src.upper(), "скан не имеет права ничего удалять"


def test_apply_does_one_action_per_record():
    """Одно решение на запись. «Применить» — это один заход по списку, а не два действия."""
    src = inspect.getsource(database.apply_word_integrity_decisions)
    assert '{"fix", "own", "keep", "delete"}' in src, "набор решений изменился — проверить экран"
    assert "status = %s, decided_at = NOW()" in src, "решение не фиксируется в очереди"


def test_review_is_admin_only():
    for name in ("list_webapp_word_integrity", "apply_webapp_word_integrity"):
        src = inspect.getsource(getattr(backend_server, name))
        assert "_word_diff_is_admin" in src, f"{name} открыт не только владельцу"


def test_invitation_goes_monday_and_sunday_and_reminds_midweek():
    """Приглашение — Пн и Вс, отдельное напоминание среди недели, если не разобрано."""
    import io
    from pathlib import Path
    bot = Path(backend_server.__file__).resolve().parents[1] / "bot_3.py"
    text = io.open(bot, encoding="utf-8").read()
    assert '"mon,sun"' in text, "приглашение больше не приходит в понедельник и воскресенье"
    assert "WORD_INTEGRITY_REMINDER_DAYS" in text, "напоминание среди недели пропало"
    assert "startapp=slovarcheck" in text, "кнопка ведёт не на экран разбора"


def test_personal_review_shows_only_your_own_cards():
    """Человек видит и правит ТОЛЬКО свои карточки — чужие ему недоступны."""
    list_src = inspect.getsource(database.list_user_word_issues)
    assert "r.user_id = %s AND r.status = 'pending' AND q.user_id = %s" in list_src, (
        "в список могут попасть чужие карточки"
    )
    apply_src = inspect.getsource(database.apply_user_word_decisions)
    assert "WHERE id = %s AND user_id = %s AND status = 'pending'" in apply_src, (
        "решение можно применить к чужой карточке"
    )
    for stmt in ("WHERE id = %s AND user_id = %s;",):
        assert stmt in apply_src, "правка или удаление не ограничены владельцем карточки"


def test_personal_check_ignores_phrases_and_sentences():
    """Во фразе строчная буква и знак процента — норма, а не брак.

    Замер 26.08.2026: без этого 238 нормальных фраз («absolute Kontraindikation») и
    предложения со знаком процента («Rabatt von 3%») выглядели ошибками.
    """
    src = inspect.getsource(database.scan_user_word_issues)
    assert "q.word_de !~ ' '" in src, "проверка снова придирается к фразам"


def test_personal_fix_is_offered_only_from_the_reference(monkeypatch):
    """Часть речи в правке — из справочника, а не наша догадка."""
    src = inspect.getsource(database.list_user_word_issues)
    assert "check_word" in src and "allow_model=False" in src, (
        "правка перестала опираться на справочник"
    )
    assert '"to_pos": true_pos' in src, "исправление части речи пропало"


def test_personal_invitation_goes_to_everyone_weekly():
    import io as _io
    from pathlib import Path
    bot = Path(backend_server.__file__).resolve().parents[1] / "bot_3.py"
    text = _io.open(bot, encoding="utf-8").read()
    assert "_send_my_words_review" in text, "рассылка своих слов пропала"
    assert "startapp=meinewoerter" in text, "кнопка ведёт не на экран своих слов"
    assert "users_with_word_issues" in text, "рассылка идёт не тем, у кого есть что править"


def test_missing_translations_are_filled_at_night_in_one_batch():
    """Перевод карточкам без перевода подбирается ночью ПАЧКОЙ и без участия человека.

    Владелец 26.08.2026: «Если каждый будет отправлять по одному — это много денег и
    нагрузка… Если это может быть сделано без меня, это делается без меня».
    """
    import io as _io
    from pathlib import Path
    bot = Path(backend_server.__file__).resolve().parents[1] / "bot_3.py"
    text = _io.open(bot, encoding="utf-8").read()
    assert "_fill_missing_translations_nightly" in text, "ночной перевод пропал"
    assert "run_missing_translations_batch" in text, "перевод идёт не пачкой"

    apply_src = inspect.getsource(database.apply_translation_proposals)
    assert "UPDATE bt_3_webapp_dictionary_queries SET translation_ru" in apply_src, (
        "найденный перевод не вписывается в карточку"
    )
    assert "status = 'failed'" in apply_src, "неудача не копится — слово потеряется"


def test_unresolved_translations_reach_the_owner():
    """Не смогли перевести — копится и приходит владельцу решением, а не пропадает."""
    src = inspect.getsource(database.list_failed_translations)
    assert "r.status = 'failed'" in src

    list_src = inspect.getsource(backend_server.list_webapp_word_integrity)
    assert "list_failed_translations" in list_src, "непереведённое не доходит до разбора"

    apply_src = inspect.getsource(backend_server.apply_webapp_word_integrity)
    assert "resolve_translation_request" in apply_src, "владелец не может вписать перевод"


def test_manual_lookup_button_is_gone():
    """Ручной поиск по одному слову убран: это делает ночь. Мёртвых ручек не держим."""
    assert not hasattr(backend_server, "find_translation_for_my_word"), (
        "остался ручной поиск перевода по одной карточке"
    )
