"""Обогащение карточки видит смысл, который сохранил человек.

Владелец 22.08.2026: карточка «die Hose anhaben» сохранена со смыслом «Быть главным»
(идиома «носить брюки» = быть главой), а ночное обогащение собрало её про БУКВАЛЬНОЕ
ношение брюк — управление «etwas anhaben — Er hat einen Anzug an», устойчивые «ein Hemd
anhaben», примеры «Welche Hose hast du heute an?», мнемоника про одежду. Идиомы в
карточке не было ни строкой, а сверху по-прежнему стояло «Быть главным».

ПРИЧИНА. Перевод человека доезжал до `_rich_enrich_card_fields` аргументом `target_text`,
но использовался ТОЛЬКО чтобы определить, какая сторона немецкая. Если немецкий слева,
русский к модели не уходил вообще — ей нечем было отличить идиому от буквального смысла.

Решение владельца 22.08.2026: сохранённый оттенок ставится ПЕРВЫМ, основное значение —
строкой ниже. Проверку «подходит ли смысл выражению» делает промпт и сообщает полем
`saved_meaning_fits`; слепо сохранённому переводу не верим.
"""
import backend.backend_server as server


class _Captured:
    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs

        async def _noop():
            return {}
        return _noop()


def _run(monkeypatch, **call):
    captured = _Captured()
    monkeypatch.setattr(server, "run_dictionary_lookup_multilang", captured)
    server._rich_enrich_card_fields(**call)
    return captured.kwargs


def test_the_saved_meaning_reaches_the_model(monkeypatch):
    kwargs = _run(monkeypatch,
                  source_text="die Hose anhaben", target_text="Быть главным",
                  source_lang="de", target_lang="ru")
    assert kwargs["word"] == "die Hose anhaben"
    # Смысл едет ВМЕСТЕ С ЯЗЫКОМ: без этого модель принимала русскую подпись за
    # смену направления и клала русское предложение в немецкое поле примера.
    assert kwargs["extra_payload"] == {
        "saved_meaning": {"text": "Быть главным", "language": "ru"}}


def test_it_reaches_the_model_from_the_other_direction(monkeypatch):
    # Человек искал по-русски: немецкое лежит в target_text, смысл — в source_text.
    kwargs = _run(monkeypatch,
                  source_text="Быть главным", target_text="die Hose anhaben",
                  source_lang="ru", target_lang="de")
    assert kwargs["word"] == "die Hose anhaben"
    # Смысл едет ВМЕСТЕ С ЯЗЫКОМ: без этого модель принимала русскую подпись за
    # смену направления и клала русское предложение в немецкое поле примера.
    assert kwargs["extra_payload"] == {
        "saved_meaning": {"text": "Быть главным", "language": "ru"}}


def test_nothing_is_invented_when_there_is_no_saved_meaning(monkeypatch):
    kwargs = _run(monkeypatch,
                  source_text="die Hose anhaben", target_text="",
                  source_lang="de", target_lang="ru")
    assert kwargs["extra_payload"] is None


def test_a_meaning_equal_to_the_word_is_not_passed(monkeypatch):
    # Обе стороны совпали — это не смысл, а та же строка; передавать нечего.
    kwargs = _run(monkeypatch,
                  source_text="die Hose anhaben", target_text="die Hose anhaben",
                  source_lang="de", target_lang="ru")
    assert kwargs["extra_payload"] is None


def test_examples_are_turned_back_if_the_model_mirrors_them(monkeypatch):
    """Модель путает стороны, когда рядом лежит русский смысл, — выправляем кодом.

    Проверено дважды 22.08.2026: с saved_meaning = «Быть главным» модель положила в
    немецкое поле примера русское предложение. Прямая просьба в задании не помогла,
    поэтому стороны выправляются тем же правилом, что чинит уже лежащие разборы.
    """
    mirrored = {
        "word_source": "die Hose anhaben",
        "word_target": "быть главным",
        "usage_examples": [{"source": "В этой семье папа носит штаны.",
                            "target": "In dieser Familie hat der Vater die Hose an."}],
    }

    def _fake(**kwargs):
        async def _done():
            return dict(mirrored)
        return _done()

    monkeypatch.setattr(server, "run_dictionary_lookup_multilang", _fake)
    seen = {}
    monkeypatch.setattr(server, "_build_dictionary_result_from_raw",
                        lambda **kw: (seen.update(kw) or ({}, "", "", "", "")))
    server._rich_enrich_card_fields(
        source_text="die Hose anhaben", target_text="быть главным",
        source_lang="de", target_lang="ru")
    example = (seen["raw"].get("usage_examples") or [{}])[0]
    assert example["source"] == "In dieser Familie hat der Vater die Hose an."
    assert example["target"] == "В этой семье папа носит штаны."


def test_the_prompt_documents_the_field_and_the_order():
    """Поле бесполезно, если промпт про него не знает: сторожим и текст задания."""
    from backend.openai_manager import system_message

    text = system_message["dictionary_assistant_multilang"]
    assert "saved_meaning" in text
    assert "saved_meaning_fits" in text
    # Порядок значений — решение владельца, он должен быть написан словами.
    assert "meanings.primary" in text and "meanings.secondary" in text
    # Живой случай оставлен в задании, чтобы правило не размылось при следующей правке.
    assert "die Hose anhaben" in text
