"""Чередование рубрик «Новость дня» / «Стендап дня» и разбор слов стендапа.

Что эти тесты не дают вернуть:

1. Расписание разъезжается. Чередование считается от якоря, а не от хранимого состояния,
   поэтому пропущенный день, перезапуск, 31-е число и граница года ничего не сдвигают.
2. В карточку слова попадает цитата, которой в ролике не звучало. Для языкового
   приложения это тот же класс дефекта, что и выдуманная грамматика: человек читает и
   заучивает. Цитата обязана дословно найтись в субтитрах, иначе карточка выбрасывается.
3. Разбор показывается урезанным. Если годных карточек меньше порога, пакет бракуется
   целиком и генератор берёт следующий ролик — пустого разбора человек не увидит.
4. Карточка перестаёт быть согласованной сама с собой. Немецкое в винительном падеже с
   русским в именительном («einen hohen genetischen Anteil» — «высокая генетическая
   составляющая») — живой случай владельца от 21.08.2026. Обороты к словарной форме
   приводить запрещено, значит форма обязана быть НАЗВАНА, а оборот показан в предложении.
   Это требование к ОБЕИМ рубрикам, не только к стендапу.
5. Рубрики сводят к одному заданию модели. Новостное задание — словарная логика, для
   сленга она вредна.
"""
import pytest

from backend.daily_video_rubrics import (
    NEWS_PROFILE,
    RUBRIC_NEWS,
    RUBRIC_STANDUP,
    STANDUP_PROFILE,
    get_profile,
    rubric_for_date,
)
from backend.world_news_generator import _length_priority, _validate_and_normalize_pack


# ── Чередование ────────────────────────────────────────────────────────────────

def test_alternates_strictly_day_by_day():
    """21.08.2026 — новости: запись на этот день собралась ночью 20-го и лежит в базе
    новостью, якорь на неё и указывает. Дальше строго через день: 22-е — стендап."""
    assert rubric_for_date("2026-08-21") == RUBRIC_NEWS
    assert rubric_for_date("2026-08-22") == RUBRIC_STANDUP
    assert rubric_for_date("2026-08-23") == RUBRIC_NEWS
    assert rubric_for_date("2026-08-24") == RUBRIC_STANDUP


def test_first_standup_lands_on_the_next_day_not_in_two_days():
    """Смысл сдвига якоря 21.08.2026: вечерняя подготовка 21-го делает 22-е, и оно обязано
    быть стендапом. При прежнем якоре рубрика впервые вышла бы только 23-го."""
    assert rubric_for_date("2026-08-22") == RUBRIC_STANDUP


def test_month_and_year_boundaries_do_not_break_the_rhythm():
    """31-е число и Новый год — те места, где ломается чередование по чётности числа."""
    for a, b in [("2026-08-30", "2026-08-31"),   # 30 → 31
                 ("2026-08-31", "2026-09-01"),   # конец месяца
                 ("2026-12-31", "2027-01-01")]:  # конец года
        assert rubric_for_date(a) != rubric_for_date(b), f"{a} и {b} оказались одной рубрикой"


def test_a_missed_day_does_not_shift_the_schedule():
    """Пропуск дня ничего не сдвигает: рубрика считается от даты, а не от «прошлого раза»."""
    assert rubric_for_date("2026-09-10") == rubric_for_date("2026-09-12")
    assert rubric_for_date("2026-09-10") != rubric_for_date("2026-09-11")


def test_alternation_can_be_switched_off(monkeypatch):
    """Выключатель возвращает поведение «каждый день новости» без выката кода."""
    monkeypatch.setenv("DAILY_VIDEO_ALTERNATION_ENABLED", "0")
    assert rubric_for_date("2026-08-22") == RUBRIC_NEWS


def test_unknown_rubric_is_an_error_not_a_default():
    with pytest.raises(ValueError):
        get_profile("kabarett")


# ── Задание модели у стендапа своё, не новостное ───────────────────────────────

def test_standup_has_its_own_prompt():
    """Новостное задание требует «существительные с артиклем, глаголы в инфинитиве» —
    это словарная логика, и для сленга она вредна: «null Bock haben» под инфинитив не
    приведёшь. Если кто-то однажды сведёт рубрики к одному промпту, тест это поймает."""
    assert STANDUP_PROFILE.llm_system, "у стендапа должно быть собственное задание модели"
    assert not NEWS_PROFILE.llm_system, "новости работают на своём исходном промпте"


def test_standup_prompt_keeps_idioms_whole():
    """Оборот обязан остаться целым. Разобранный на части («haben» вместо «Bock haben»)
    он теряет ровно то, ради чего показан, — а страж на это поставить нельзя, потому что
    механическое додумывание формы в этом репозитории запрещено. Стережём задание."""
    prompt = STANDUP_PROFILE.llm_system
    assert "Bock haben (auf etwas)" in prompt
    assert "wie bestellt und nicht abgeholt" in prompt
    assert "FALSCH" in prompt, "в задании должен быть явный пример НЕПРАВИЛЬНОГО разбора"


def test_standup_prompt_demands_the_register_marking():
    """Помета регистра — не украшение: по ней человек понимает, при ком так говорить
    нельзя, и по ней же словарный слой может узнать живую речь среди словарных единиц."""
    assert "register_ru" in STANDUP_PROFILE.llm_system
    assert "derb/vulgär" in STANDUP_PROFILE.llm_system


# ── Квота YouTube: спрашиваем разрешение ДО траты ──────────────────────────────

def test_low_quota_means_not_a_single_request(monkeypatch):
    """21.08.2026 суточная квота кончилась, и рубрика не смогла подобрать ролик. Причина:
    она ходила в YouTube мимо счётчика — не спрашивала разрешения и не сообщала о тратах.
    При нехватке остатка обход обязан не сделать НИ ОДНОГО запроса, а не долбиться и
    получать отказы."""
    import backend.world_news_generator as G

    G._CAND_CACHE.clear()
    monkeypatch.setattr(G, "_quota_allows", lambda units: False)

    def _no_network(*a, **kw):
        raise AssertionError("при нехватке квоты рубрика не имеет права ходить в сеть")

    monkeypatch.setattr(G.requests, "get", _no_network)
    assert G._gather_candidates(STANDUP_PROFILE) == []
    assert G._QUOTA_LOW is True


def test_quota_estimate_is_asked_before_the_sweep(monkeypatch):
    """Сторожу называется РЕАЛЬНАЯ цена обхода, а не символическая единица: иначе он
    пропустит трату, на которую остатка не хватает."""
    import backend.world_news_generator as G

    G._CAND_CACHE.clear()
    asked = []
    monkeypatch.setattr(G, "_quota_allows", lambda units: (asked.append(units), False)[1])
    monkeypatch.setattr(G.requests, "get", lambda *a, **kw: None)
    G._gather_candidates(STANDUP_PROFILE)
    assert asked, "сторожа вообще не спросили"
    # 12 каналов × 8 страниц = 96 единиц на списки, плюс справка о роликах пачками по 50.
    assert asked[0] >= 96, f"цена обхода занижена: {asked[0]}"


def test_news_rubric_is_not_gated_by_the_archive_guard(monkeypatch):
    """Сторож стоит на архивном обходе стендапа. У новостей обход дешёвый (одна страница
    на канал), и вешать на них тот же порог значило бы ронять утреннюю рубрику зря."""
    import backend.world_news_generator as G

    G._CAND_CACHE.clear()
    asked = []
    monkeypatch.setattr(G, "_quota_allows", lambda units: (asked.append(units), False)[1])
    monkeypatch.setattr(G, "_yt_api_playlist_recent", lambda *a, **kw: [])
    monkeypatch.setattr(G, "_yt_api_search_recent", lambda *a, **kw: [])
    G._gather_candidates(NEWS_PROFILE)
    assert not asked, "новостной обход не должен проходить через архивный сторож"


# ── Отчёт о пуле не тратит квоту ───────────────────────────────────────────────

def test_pool_report_reads_the_snapshot_and_never_calls_youtube(monkeypatch):
    """Первая версия отчёта обходила каналы заново — тратила ту самую квоту, которая нужна
    продукту. Теперь отчёт читает снимок, записанный в момент подготовки выпуска."""
    import backend.database as db
    import backend.standup_pool_report as R
    import backend.world_news_generator as G

    monkeypatch.setattr(db, "get_daily_video_pool_snapshot",
                        lambda rubric: {"scanned": 3756, "in_range": 646,
                                        "manual_captions": 111, "measured_on": "2026-08-21"})
    monkeypatch.setattr(db, "count_shown_daily_videos", lambda rubric: 10)
    monkeypatch.setattr(G, "_gather_candidates", lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("отчёт не имеет права обходить каналы")))

    state = R.standup_pool_state()
    assert state["remaining"] == 636          # 646 подходящих минус 10 показанных
    assert state["days_left"] == 1272         # рубрика выходит через день
    assert state["measured_on"] == "2026-08-21"
    assert "квоту YouTube отчёт не тратит" in R.format_standup_pool_report(state)


def test_pool_report_says_when_it_has_never_measured(monkeypatch):
    """Ноль и «мы ещё не мерили» — разные вещи: ноль означал бы «добавь каналы»."""
    import backend.database as db
    import backend.standup_pool_report as R

    monkeypatch.setattr(db, "get_daily_video_pool_snapshot", lambda rubric: None)
    monkeypatch.setattr(db, "count_shown_daily_videos", lambda rubric: 0)
    text = R.format_standup_pool_report(R.standup_pool_state())
    assert "ещё ни разу не мерился" in text
    assert "закончились" not in text


# ── Прописные буквы в тезисах ──────────────────────────────────────────────────

def test_prompts_demand_capital_letters_in_theses(monkeypatch):
    """На карточке владельца 21.08.2026: «афд лидирует в опросах в двух землях восточной
    германии». Партия, страна и земли — имена собственные. Причина была не только в
    отсутствии правила: САМ ПРИМЕР в задании был написан строчными, и модель его копировала."""
    from backend.world_news_generator import _LLM_SYSTEM
    for prompt in (_LLM_SYSTEM, STANDUP_PROFILE.llm_system):
        assert "SCHREIBWEISE" in prompt
        assert "АдГ" in prompt
    # Пример тезисов обязан быть написан правильно — модель повторяет образец.
    assert '"Правительство Германии' in _LLM_SYSTEM
    assert '"правительство Германии' not in _LLM_SYSTEM


# ── Длительность берётся из профиля ────────────────────────────────────────────

def test_length_priority_follows_the_profile():
    """У стендапа предпочтительное окно 5–10 мин, у новостей 5–7. Один и тот же
    восьмиминутный ролик должен считаться идеальным для стендапа и «длиннее нужного»
    для новостей — иначе профиль ни на что не влияет."""
    eight_minutes = 480
    assert _length_priority(eight_minutes, STANDUP_PROFILE)[0] == 0
    assert _length_priority(eight_minutes, NEWS_PROFILE)[0] == 1


# ── Разбор слов стендапа ───────────────────────────────────────────────────────

_TRANSCRIPT = (
    "Also ich sag mal so, ich hab null Bock auf Montag. "
    "Meine Oma sagt immer, das ist doch der Hammer, Junge. "
    "Und dann steh ich da wie bestellt und nicht abgeholt."
)


def _phrase(de, quote, **over):
    item = {
        "de": de,
        "register_ru": "разговорное",
        "form_ru": "словарная форма",
        "translation_ru": "перевод здесь",
        "literal_ru": "",
        "quote_de": quote,
        "quote_ru": "перевод цитаты",
        "usage_ru": "с друзьями свободно",
    }
    item.update(over)
    return item


def _pack(phrases):
    return {
        "summary_points": ["комик про понедельник"],
        "phrases": phrases,
        "quiz": [
            {"question_de": f"Frage {i}?", "options": ["a", "b", "c", "d"],
             "correct_index": 0, "explanation_ru": "потому что"}
            for i in range(4)
        ],
    }


def _good_phrases(n=5):
    quotes = [
        "ich hab null Bock auf Montag",
        "das ist doch der Hammer",
        "wie bestellt und nicht abgeholt",
        "Also ich sag mal so",
        "Meine Oma sagt immer",
    ]
    return [_phrase(f"оборот {i}", quotes[i]) for i in range(n)]


def test_standup_card_keeps_the_linguistic_fields():
    """Помета регистра, цитата из ролика и её перевод обязаны дойти до карточки —
    без них остаётся сухой перевод, из-за которого и заучивается неверный сленг."""
    out = _validate_and_normalize_pack(_pack(_good_phrases()), STANDUP_PROFILE, _TRANSCRIPT)
    card = out["phrases"][0]
    assert card["register_ru"] == "разговорное"
    assert card["form_ru"] == "словарная форма"
    assert card["quote_de"] == "ich hab null Bock auf Montag"
    assert card["quote_ru"] == "перевод цитаты"
    assert "literal_ru" in card


def test_quote_that_is_not_in_the_video_is_thrown_away():
    """Модель, которой велено скопировать строку, иногда пересказывает её своими словами.
    Такая карточка показала бы человеку фразу, которой в ролике не звучало."""
    phrases = _good_phrases() + [_phrase("выдумка", "diesen Satz hat niemand gesagt")]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert all(p["de"] != "выдумка" for p in out["phrases"])
    assert len(out["phrases"]) == 5


def test_quote_matching_ignores_punctuation_not_words():
    """В субтитрах знаки препинания стоят иначе, чем их перепишет модель, — сверяем слова."""
    phrases = _good_phrases(4) + [_phrase("оборот", "Ich hab null Bock, auf Montag!!")]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


def test_incomplete_card_is_thrown_away():
    """Карточка без пометы регистра или без перевода цитаты не показывается частично."""
    phrases = _good_phrases() + [
        _phrase("без пометы регистра", "Also ich sag mal so", register_ru=""),
        _phrase("без перевода цитаты", "Meine Oma sagt immer", quote_ru=""),
        _phrase("без пометы формы", "das ist doch der Hammer", form_ru=""),
    ]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


def test_too_few_good_cards_rejects_the_whole_video():
    """Порог годности РОЛИКА: меньше четырёх годных карточек — это не материал для
    рубрики, и генератор обязан взять следующий ролик, а не показать пустой разбор."""
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(_pack(_good_phrases(2)), STANDUP_PROFILE, _TRANSCRIPT)


def test_there_is_no_target_number_of_cards():
    """Плана по количеству нет: сколько в ролике вправду есть — столько и показываем.
    Пять годных карточек — валидный разбор, добирать до вилки нечем и незачем."""
    out = _validate_and_normalize_pack(_pack(_good_phrases(5)), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 5


# ── Новости: карточка обязана объяснять то, что показывает ─────────────────────

def test_news_card_also_needs_quote_and_form():
    """Случай владельца 21.08.2026: карточка новостей показывала «einen hohen genetischen
    Anteil», а переводила «высокая генетическая составляющая» — немецкое в винительном,
    русское в именительном. Обороты к словарной форме приводить запрещено (модель
    переписала бы живую речь в грамматически неверную), значит форма обязана быть НАЗВАНА,
    а оборот — показан в предложении. С 21.08.2026 это критично вдвойне: корректор больше
    не правит текст при сохранении, и показанное уезжает человеку в словарь дословно."""
    phrases = [
        {"de": f"das Wort {i}", "form_ru": "именительный падеж", "translation_ru": "слово",
         "usage_ru": "с артиклем", "quote_de": q, "quote_ru": "перевод строки"}
        for i, q in enumerate([
            "ich hab null Bock auf Montag", "das ist doch der Hammer",
            "wie bestellt und nicht abgeholt", "Also ich sag mal so",
            "Meine Oma sagt immer", "steh ich da wie bestellt",
        ])
    ]
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 6
    assert out["phrases"][0]["form_ru"] == "именительный падеж"
    assert out["phrases"][0]["quote_de"]


def test_news_card_without_form_is_thrown_away():
    phrases = [
        {"de": f"das Wort {i}", "form_ru": "" if i == 0 else "именительный падеж",
         "translation_ru": "слово", "usage_ru": "с артиклем",
         "quote_de": q, "quote_ru": "перевод строки"}
        for i, q in enumerate([
            "ich hab null Bock auf Montag", "das ist doch der Hammer",
            "wie bestellt und nicht abgeholt", "Also ich sag mal so",
            "Meine Oma sagt immer", "steh ich da wie bestellt",
        ])
    ]
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)


def test_news_does_not_demand_a_register_marking():
    """Помету регистра спрашиваем только у стендапа. В новостях речь нейтральная, и
    требовать там «сленг/грубое» значило бы принуждать модель выдумывать."""
    assert NEWS_PROFILE.requires_register is False
    assert STANDUP_PROFILE.requires_register is True
    phrases = [
        {"de": f"das Wort {i}", "form_ru": "именительный падеж", "translation_ru": "слово",
         "usage_ru": "с артиклем", "quote_de": q, "quote_ru": "перевод строки"}
        for i, q in enumerate([
            "ich hab null Bock auf Montag", "das ist doch der Hammer",
            "wie bestellt und nicht abgeholt", "Also ich sag mal so",
            "Meine Oma sagt immer", "steh ich da wie bestellt",
        ])
    ]
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 6
    assert "register_ru" not in out["phrases"][0]


def test_news_prompt_forbids_normalizing_word_groups():
    """Причина того самого винительного падежа: промпт просил предпочитать словосочетания,
    но форму оговаривал только для существительных и глаголов. Теперь запрет приводить
    оборот к словарной форме записан прямо, с живым примером владельца."""
    from backend.world_news_generator import _LLM_SYSTEM
    assert "einen hohen genetischen Anteil" in _LLM_SYSTEM
    assert "form_ru" in _LLM_SYSTEM
    assert "NIEMALS" in _LLM_SYSTEM
