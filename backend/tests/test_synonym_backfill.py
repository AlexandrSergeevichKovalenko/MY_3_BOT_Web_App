"""Добор синонимов накопленным словам.

Синонимы мы стали просить только 10.08.2026 — до этого их не просил ни один промпт
живого пути. Значит всё уже обогащённое осталось бы без них навсегда: замер того же дня —
9 469 слов, из них 9 261 лежат у людей в личных карточках.

Основной ночной добор их не видит: он берёт слова БЕЗ разбора. Поэтому нужен свой отбор,
свой короткий запрос и своя строка в утреннем отчёте.
"""
import inspect

from backend import lex_units
from backend.openai_manager import system_message


def test_selector_takes_words_that_have_a_card_but_no_synonyms():
    src = inspect.getsource(lex_units.units_needing_synonyms)
    assert "card IS NOT NULL" in src, "отбор берёт и слова без разбора — это работа другого прохода"
    assert "'synonyms'" in src


def test_already_asked_words_leave_the_queue():
    """У части слов близких синонимов нет. Без отметки «спрашивали» такое слово
    возвращалось бы в очередь каждую ночь — мы платили бы за один и тот же отказ."""
    src = inspect.getsource(lex_units.units_needing_synonyms)
    assert "synonyms_asked_at" in src, "спрошенные слова вернутся в очередь завтра"
    cnt = inspect.getsource(lex_units.count_units_needing_synonyms)
    assert "synonyms_asked_at" in cnt, "остаток в отчёте не будет убывать"


def test_prompt_asks_only_for_synonyms():
    """Карточка целиком уже куплена: переспрашивать её значит платить второй раз."""
    text = system_message["dictionary_synonyms_backfill"]
    assert '"synonyms"' in text and '"antonyms"' in text and '"related_words"' in text
    for heavy in ("usage_examples", "meanings", "grammar", "forms"):
        assert f'"{heavy}"' not in text, f"запрос тянет лишнее ({heavy}) — это переплата"


def test_prompt_anchors_the_sense_by_translation():
    """«der Zug» — поезд или тяга: без опоры на перевод модель выберет наугад."""
    text = system_message["dictionary_synonyms_backfill"]
    assert "translation" in text
    assert "Zug" in text, "нет примера про разные значения — правило легко потерять"


def test_prompt_forbids_inventing():
    text = system_message["dictionary_synonyms_backfill"]
    assert "rather than inventing" in text, (
        "модели не запрещено выдумывать — выдуманный синоним учит слову, которого нет"
    )


def test_backfill_runs_after_the_main_enrichment():
    """Слово без разбора показать вообще нечего, а слово без синонимов показывается,
    просто беднее. Значит основной добор важнее и идёт первым."""
    from backend import backend_server
    src = inspect.getsource(backend_server._run_units_night_enrichment)
    assert "_run_synonym_backfill" in src
    assert src.index("count_units_needing_card") < src.index("_run_synonym_backfill")


def test_backfill_has_its_own_cap():
    from backend import backend_server
    assert hasattr(backend_server, "SYNONYM_BACKFILL_NIGHT_LIMIT")
    assert backend_server.SYNONYM_BACKFILL_NIGHT_LIMIT >= 0
