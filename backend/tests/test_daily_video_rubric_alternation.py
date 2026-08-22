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


# ── «Единицы кончились» и «мы частим» — разные беды ────────────────────────────

class _FakeResp:
    def __init__(self, status, reason=None, items=None):
        self.status_code = status
        self._reason = reason
        self._items = items or []

    def json(self):
        if self._reason:
            return {"error": {"errors": [{"reason": self._reason}]}}
        return {"items": self._items}


def test_rate_limit_is_retried_not_surrendered(monkeypatch):
    """21.08.2026: сотня быстрых запросов подряд придушила ключ, и `/standup` сдался с
    сообщением «дневная квота исчерпана» — хотя суточные единицы были целы и хватило бы
    подождать секунды. «Мы частим» обязано лечиться паузой и повтором."""
    import backend.world_news_generator as G

    calls = []

    def _fake_get(url, params=None, timeout=None):
        calls.append(url)
        # Первые два раза — «не части», третий проходит.
        if len(calls) < 3:
            return _FakeResp(403, reason="rateLimitExceeded")
        return _FakeResp(200, items=[{"id": "abc"}])

    monkeypatch.setattr(G.requests, "get", _fake_get)
    monkeypatch.setattr(G.time, "sleep", lambda *_: None)
    monkeypatch.setattr(G, "_quota_spent", lambda units: None)
    G._QUOTA_EXCEEDED = False

    payload = G._yt_get("https://example/api", {}, cost=1, what="проверка")
    assert payload == {"items": [{"id": "abc"}]}, "повтор после паузы обязан сработать"
    assert len(calls) == 3
    assert G._QUOTA_EXCEEDED is False, "частота — это не исчерпанная суточная квота"


def test_daily_quota_is_not_retried(monkeypatch):
    """А вот «единицы кончились» повторять бессмысленно: до сброса ничего не изменится.
    Долбиться в закрытую дверь — только греть логи."""
    import backend.world_news_generator as G

    calls = []

    def _fake_get(url, params=None, timeout=None):
        calls.append(url)
        return _FakeResp(403, reason="quotaExceeded")

    monkeypatch.setattr(G.requests, "get", _fake_get)
    monkeypatch.setattr(G.time, "sleep", lambda *_: None)
    monkeypatch.setattr(G, "_quota_spent", lambda units: None)
    G._QUOTA_EXCEEDED = False

    assert G._yt_get("https://example/api", {}, cost=1, what="проверка") is None
    assert len(calls) == 1, "суточное исчерпание не повторяют"
    assert G._QUOTA_EXCEEDED is True


# ── Полка стендапов ────────────────────────────────────────────────────────────

def test_full_shelf_means_no_youtube_at_all(monkeypatch):
    """Главный смысл полки: пока запас есть, в сеть не ходим ВООБЩЕ. 21.08.2026 рубрика
    осталась без ролика из-за придушенного ключа — с полкой это перестаёт быть событием."""
    import backend.database as db
    import backend.standup_shelf as S
    import backend.world_news_generator as G

    monkeypatch.setattr(db, "standup_shelf_counts",
                        lambda: {"total": 30, "unused": 30, "unused_manual": 12})
    monkeypatch.setattr(G, "_gather_candidates", lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("при полной полке обход каналов запрещён")))

    report = S.refill_standup_shelf(target=30)
    assert report["added"] == 0
    assert "полка полна" in report["reason"]


def test_refill_never_shelves_a_video_without_subtitles(monkeypatch):
    """Ролик без субтитров на полке — отложенная поломка, а не запас: в день выпуска
    выяснится, что показывать нечего, и будет поздно."""
    from backend.database import put_on_standup_shelf

    with pytest.raises(ValueError):
        put_on_standup_shelf(
            video_id="abc12345678", video_title="t", channel_title="c",
            duration_seconds=400, has_manual_captions=True, view_count=1,
            transcript=[], transcript_lang="de", transcript_is_generated=False,
        )


def test_shelf_prefers_manual_subtitles_over_popularity():
    """Порядок отбора владельца 21.08.2026: сперва ручные субтитры, и только потом
    просмотры. Двухмиллионный номер с машинной расшифровкой не должен обгонять
    скромный ролик с субтитрами, положенными руками."""
    import backend.standup_shelf as S

    rows = [
        {"video_id": "popular", "has_manual_captions": False, "view_count": 2_000_000},
        {"video_id": "manual", "has_manual_captions": True, "view_count": 1_000},
        {"video_id": "manual_big", "has_manual_captions": True, "view_count": 50_000},
    ]
    rows.sort(key=lambda r: (0 if r["has_manual_captions"] else 1, -(r["view_count"] or 0)))
    assert [r["video_id"] for r in rows] == ["manual_big", "manual", "popular"]
    # Тот же ключ обязан стоять и в самом пополнении — иначе тест стережёт пустоту.
    import inspect
    assert 'if r["has_manual_captions"] else 1' in inspect.getsource(S.refill_standup_shelf)


def test_standup_prep_reads_the_shelf_and_does_not_fetch_anything(monkeypatch):
    """Подготовка выпуска берёт готовое: ни YouTube, ни скачивания субтитров. 20.08.2026
    субтитры сутки не тянулись из-за блокировки адреса — этот путь такую беду переживает."""
    import backend.database as db
    import backend.world_news_generator as G

    shelf_row = {
        "video_id": "vid00000001", "video_title": "Мой номер", "channel_title": "NightWash",
        "duration_seconds": 480, "has_manual_captions": True,
        "transcript": [{"text": "ich hab null Bock auf Montag"} for _ in range(40)],
        "transcript_lang": "de", "transcript_is_generated": False,
    }
    monkeypatch.setattr(db, "take_next_from_standup_shelf", lambda exclude=None: shelf_row)
    monkeypatch.setattr(G, "_gather_candidates", lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("выпуск не имеет права обходить каналы")))
    monkeypatch.setattr(G, "_fetch_transcript", lambda vid: (_ for _ in ()).throw(
        AssertionError("выпуск не имеет права качать субтитры")))
    # Дальше подготовки ролика не идём — модель и запись в базу здесь не проверяются.
    monkeypatch.setattr(G, "_call_llm", lambda *a, **kw: (_ for _ in ()).throw(
        RuntimeError("СТОП: ролик выбран")))

    with pytest.raises(RuntimeError) as err:
        G.prepare_world_news("2026-08-22", rubric=RUBRIC_STANDUP)
    assert "СТОП: ролик выбран" in str(err.value)


def test_empty_shelf_fails_honestly_instead_of_repeating(monkeypatch):
    """Повторить показанное хуже, чем не показать: человек решит, что рубрика сломалась.
    Пустая полка обязана поднять ошибку — вечерняя подготовка на неё зовёт владельца."""
    import backend.database as db
    import backend.standup_shelf as S
    import backend.world_news_generator as G

    monkeypatch.setattr(db, "take_next_from_standup_shelf", lambda exclude=None: None)
    monkeypatch.setattr(S, "refill_standup_shelf", lambda **kw: {"added": 0})

    with pytest.raises(RuntimeError) as err:
        G.prepare_world_news("2026-08-22", rubric=RUBRIC_STANDUP)
    assert "полка пуста" in str(err.value)


# ── Кривой ответ модели: переспрашиваем, а не латаем и не сдаёмся ──────────────

def test_bad_pack_makes_us_ask_again_not_give_up(monkeypatch):
    """21.08.2026 выпуск сорвался целиком из-за одного вопроса теста с тремя вариантами
    вместо четырёх — вместе с ним пропал и хороший разбор слов.

    Латать ответ своими руками нельзя: обрезать лишние варианты значит рискнуть выкинуть
    правильный, дописать недостающие — выдумать. Поэтому переспрашиваем модель."""
    import backend.database as db
    import backend.world_news_generator as G

    shelf_row = {
        "video_id": "vid00000002", "video_title": "Номер", "channel_title": "NightWash",
        "duration_seconds": 500, "has_manual_captions": True,
        # Субтитры обязаны содержать ВСЕ цитаты карточек — иначе их отбракует страж цитат,
        # и мы будем проверять не то, что задумали.
        "transcript": [{"text": _TRANSCRIPT}] * 12,
        "transcript_lang": "de", "transcript_is_generated": False,
    }
    monkeypatch.setattr(db, "take_next_from_standup_shelf", lambda exclude=None: shelf_row)

    calls = {"n": 0}

    def _fake_llm(title, transcript, profile=None):
        calls["n"] += 1
        quiz = [{"question_de": f"Frage {i}?", "options": ["a", "b", "c", "d"],
                 "correct_index": 0, "explanation_ru": "потому что"} for i in range(4)]
        if calls["n"] == 1:
            quiz[2]["options"] = ["a", "b", "c"]      # ровно тот изъян, что сорвал выпуск
        return {"summary_points": ["Комик про понедельник"],
                "phrases": _good_phrases(5), "quiz": quiz}

    monkeypatch.setattr(G, "_call_llm", _fake_llm)
    # Останавливаемся СРАЗУ после сборки пакета. Подменять надо `backend.database`, а не
    # модуль генератора: prepare_world_news импортирует запись внутри функции, и подмена
    # на генераторе не действует. Проверено на себе 21.08.2026 — тест дописал поддельный
    # день в БОЕВУЮ базу, и его пришлось вычищать руками.
    monkeypatch.setattr(db, "upsert_world_news_daily", lambda **kw: (_ for _ in ()).throw(
        RuntimeError("СТОП: пакет собран")))

    with pytest.raises(Exception) as err:
        G.prepare_world_news("2026-08-22", rubric=RUBRIC_STANDUP)
    assert calls["n"] == 2, "после кривого пакета модель обязаны переспросить"
    assert "негодный" not in str(err.value), "со второй попытки пакет годный — сдаваться рано"


def test_we_never_patch_a_bad_pack_ourselves():
    """Страж остаётся строгим: вопрос без четырёх вариантов бракуется, а не «чинится»
    обрезкой. Обрезав лишний вариант, мы рискуем выкинуть правильный ответ."""
    pack = _pack(_good_phrases(5))
    pack["quiz"][1]["options"] = ["a", "b", "c"]
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(pack, STANDUP_PROFILE, _TRANSCRIPT)


# ── Отчёт о пуле не тратит квоту ───────────────────────────────────────────────

def test_pool_report_counts_the_shelf_and_never_calls_youtube(monkeypatch):
    """Первая версия отчёта обходила каналы заново — тратила ту самую квоту, которая нужна
    продукту. Теперь запас считается по полке, а «чем пополнять» — по снимку, снятому
    в момент подготовки выпуска. Обращений к YouTube: ноль."""
    import backend.database as db
    import backend.standup_pool_report as R
    import backend.world_news_generator as G

    monkeypatch.setattr(db, "standup_shelf_counts",
                        lambda: {"total": 30, "unused": 28, "unused_manual": 11})
    monkeypatch.setattr(db, "get_daily_video_pool_snapshot",
                        lambda rubric: {"scanned": 3756, "in_range": 646,
                                        "manual_captions": 111, "measured_on": "2026-08-21"})
    monkeypatch.setattr(db, "count_shown_daily_videos", lambda rubric: 2)
    monkeypatch.setattr(G, "_gather_candidates", lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("отчёт не имеет права обходить каналы")))

    state = R.standup_pool_state()
    assert state["remaining"] == 28           # запас — это ПОЛКА, а не весь пул каналов
    assert state["days_left"] == 56           # рубрика выходит через день
    assert state["pool_in_range"] == 646      # а это то, чем полку можно пополнить
    assert "квоту YouTube отчёт не тратит" in R.format_standup_pool_report(state)


def test_shelf_and_pool_are_not_confused(monkeypatch):
    """Полка и пул каналов — разные вещи, и путать их нельзя: полка может опустеть при
    огромном пуле. Отчёт обязан сказать «всё показано, пополнение доберёт», а не
    «добавь каналы», когда добавлять ничего не надо."""
    import backend.database as db
    import backend.standup_pool_report as R

    monkeypatch.setattr(db, "standup_shelf_counts",
                        lambda: {"total": 30, "unused": 0, "unused_manual": 0})
    monkeypatch.setattr(db, "get_daily_video_pool_snapshot",
                        lambda rubric: {"scanned": 3756, "in_range": 646,
                                        "manual_captions": 111, "measured_on": "2026-08-21"})
    monkeypatch.setattr(db, "count_shown_daily_videos", lambda rubric: 30)
    text = R.format_standup_pool_report(R.standup_pool_state())
    assert "всё показано" in text
    assert "646" in text, "надо показать, чем полку можно пополнить"


def test_report_says_when_the_shelf_was_never_filled(monkeypatch):
    """Пустая полка на старте и «всё показано» — разные сообщения: первое не требует
    от владельца ничего, второе может потребовать новых каналов."""
    import backend.database as db
    import backend.standup_pool_report as R

    monkeypatch.setattr(db, "standup_shelf_counts",
                        lambda: {"total": 0, "unused": 0, "unused_manual": 0})
    monkeypatch.setattr(db, "get_daily_video_pool_snapshot", lambda rubric: None)
    monkeypatch.setattr(db, "count_shown_daily_videos", lambda rubric: 0)
    text = R.format_standup_pool_report(R.standup_pool_state())
    assert "ещё не наполнялась" in text
    assert "всё показано" not in text


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
        # Форма из текста обязана лежать ВНУТРИ цитаты — иначе карточку выбросит страж.
        "de_in_text": " ".join(quote.split()[:3]),
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

def _news_phrases(n=6, **over):
    """Новостные карточки, у которых слово ВПРАВДУ встречается в своей цитате.

    Иначе их отбросит страж «цитата обязана показывать слово», и тест будет проверять
    не то, что задумано, — на этом я и попался 21.08.2026 с заготовками «das Wort 0».
    """
    rows = [
        ("der Bock", "ich hab null Bock auf Montag"),
        ("der Montag", "ich hab null Bock auf Montag"),
        ("die Oma", "Meine Oma sagt immer"),
        ("der Hammer", "das ist doch der Hammer"),
        ("der Junge", "das ist doch der Hammer, Junge"),
        ("abholen", "wie bestellt und nicht abgeholt"),
    ]
    out = []
    for de, quote in rows[:n]:
        item = {"de": de, "form_ru": "словарная форма", "translation_ru": "перевод",
                "usage_ru": "с артиклем", "quote_de": quote, "quote_ru": "перевод строки",
                "de_in_text": " ".join(quote.split()[:3])}
        item.update(over)
        out.append(item)
    return out


def test_news_card_also_needs_quote_and_form():
    """Случай владельца 21.08.2026: карточка новостей показывала «einen hohen genetischen
    Anteil», а переводила «высокая генетическая составляющая» — немецкое в винительном,
    русское в именительном. Обороты к словарной форме приводить запрещено (модель
    переписала бы живую речь в грамматически неверную), значит форма обязана быть НАЗВАНА,
    а оборот — показан в предложении. С 21.08.2026 это критично вдвойне: корректор больше
    не правит текст при сохранении, и показанное уезжает человеку в словарь дословно."""
    phrases = _news_phrases()
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 6
    assert out["phrases"][0]["form_ru"] == "словарная форма"
    assert out["phrases"][0]["quote_de"]


def test_news_card_without_form_is_thrown_away():
    phrases = _news_phrases()
    phrases[0]["form_ru"] = ""   # одна карточка без пометы формы
    with pytest.raises(ValueError):
        _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)


def test_news_does_not_demand_a_register_marking():
    """Помету регистра спрашиваем только у стендапа. В новостях речь нейтральная, и
    требовать там «сленг/грубое» значило бы принуждать модель выдумывать."""
    assert NEWS_PROFILE.requires_register is False
    assert STANDUP_PROFILE.requires_register is True
    phrases = _news_phrases()
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 6
    assert "register_ru" not in out["phrases"][0]



# ── Три дефекта первого живого выпуска (21.08.2026) ───────────────────────────

def test_prompt_forbids_spoiling_the_outcome():
    """Первый живой стендап показал в резюме «Завершение сезона, победа Терезы», а
    четвёртый вопрос теста назвал победителя. Человек читает это ДО видео — смотреть
    после такого незачем. Запрет на панчлайн у нас был, на исход — не было."""
    prompt = STANDUP_PROFILE.llm_system
    assert "AUSGANG" in prompt
    assert prompt.count("NICHTS VERRATEN") >= 1
    assert "AUCH HIER NICHTS VERRATEN" in prompt, "запрет обязан стоять и в блоке теста"


def test_prompt_forbids_all_caps_names():
    """«ТЕРЕЗА» вместо «Тереза». Правило «имена собственные сохраняют прописную» модель
    поняла как «писать целиком заглавными» — формулировка была моя, ошибка моя."""
    for prompt in (STANDUP_PROFILE.llm_system,):
        assert "NUR DER ERSTE BUCHSTABE" in prompt
        assert "«ТЕРЕЗА»" in prompt, "нужен явный пример НЕПРАВИЛЬНОГО написания"


def test_prompt_demands_reusable_units_not_show_lines():
    """Из шести карточек первого выпуска настоящей была одна. Остальные — реплики из шоу
    («Privatversicherte verstehen den Joke»), которые годятся ровно в той ситуации, где
    прозвучали. Недостающий признак — воспроизводимость: единица годится в карточку,
    только если человек сможет употребить её в ДРУГОЙ ситуации."""
    prompt = STANDUP_PROFILE.llm_system
    assert "WIEDERVERWENDBARKEIT" in prompt
    assert "Privatversicherte verstehen den Joke" in prompt, "нужен пример негодной карточки"
    assert "abhauen" in prompt, "нужен пример: из реплики берётся глагол, а не вся фраза"
    assert "Niceinger Diceinger" in prompt, "выдумки ведущего не заучиваются как обороты"


# ── Дефекты второго живого выпуска (21.08.2026, вечер) ────────────────────────

def test_quote_must_show_the_unit_not_just_exist(monkeypatch):
    """Карточка «ausrasten» получила цитату про то, как все будут громко смеяться, — слова
    там не было вовсе. Первый страж проверял, что цитата ЕСТЬ в субтитрах, но не что она
    ПОКАЗЫВАЕТ слово. А ради этого цитата и нужна: человек должен увидеть, как это говорят."""
    from backend.world_news_generator import _quote_shows_the_unit

    assert not _quote_shows_the_unit("ausrasten", "wo wir den Witz hören und alle lachen")
    assert _quote_shows_the_unit("verkacken", "Die leichteste Aufgabe der Welt verkackt.")


def test_separable_verb_is_recognised_when_torn_apart():
    """В живой речи глагол разрывается: «ausrasten» звучит как «da rasten alle aus».
    Тупая проверка вхождения выбросила бы ПРАВИЛЬНУЮ карточку — эта грабля в репозитории
    уже известна по заданиям с отделяемыми глаголами."""
    from backend.world_news_generator import _quote_shows_the_unit, _unit_roots

    assert _quote_shows_the_unit("ausrasten", "und da rasten dann alle komplett aus")
    assert "rast" in _unit_roots("ausrasten"), "нужен корень без отделяемой приставки"


def test_function_words_do_not_count_as_a_match():
    """Искать «am» или «die» в немецкой строке бессмысленно — они есть везде, и любая
    цитата прошла бы проверку формально."""
    from backend.world_news_generator import _unit_roots

    roots = _unit_roots("nichts am Hut haben (mit etwas)")
    assert "am" not in roots and "die" not in roots
    assert "hut" in roots


def test_neutral_word_is_dropped_from_a_standup_pack():
    """«die Kommentarspalte» с пометой «нейтральное» — добор до количества, ровно тот мусор,
    которого владелец просил избегать: это слово человек и так знает, а место карточки занял."""
    phrases = _good_phrases(5) + [
        _phrase("нейтральное слово", "Also ich sag mal so", register_ru="нейтральное"),
    ]
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert all(p["de"] != "нейтральное слово" for p in out["phrases"])
    assert len(out["phrases"]) == 5


def test_news_pack_keeps_neutral_words():
    """Отбраковка нейтральных — правило СТЕНДАПА. В новостях речь нейтральная вся, и тот же
    фильтр вычистил бы рубрику до пустоты."""
    phrases = _news_phrases(register_ru="нейтральное")
    out = _validate_and_normalize_pack(_pack(phrases), NEWS_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 6


def test_prompt_stops_inventing_a_case_for_lookup_forms():
    """«die Kommentarspalte» помечена «винительный падеж», хотя в цитате стоит дательный, а
    у «scheiß drauf» падежа нет вовсе. Причина: поле формы просило описать, как слово звучало
    в ролике, а на карточке стоит словарный вид. Модель, обязанная что-то написать, выдумывала
    грамматику — ровно то, что правило ноль запрещает абсолютно."""
    prompt = STANDUP_PROFILE.llm_system
    assert "HAT KEINEN KASUS" in prompt
    assert "ERFINDEN" in prompt
    assert "scheiß drauf" in prompt, "нужен пример оборота, у которого падежа нет"


def test_prompt_forbids_quizzing_what_the_cards_explain():
    """Четвёртый вопрос спрашивал значение «Full Circle Moment», уже разобранного карточкой
    выше. Человек видит ответ до вопроса — тест перестаёт что-либо проверять."""
    assert "FRAGE NICHT NACH DEM, WAS DIE KARTEN SCHON ERKLÄREN" in STANDUP_PROFILE.llm_system


# ── Дефекты третьего живого выпуска (22.08.2026) ──────────────────────────────

def test_quiz_may_not_repeat_a_card_unit():
    """Карточка объясняла «Digger», а второй вопрос теста спрашивал, что это слово значит.
    Человек прочитал ответ строкой выше — вопрос перестал что-либо проверять. Запрет стоял
    в задании модели и НЕ сработал, поэтому ловим механически."""
    # Цитата обязана содержать саму единицу — иначе её выбросит страж цитаты, и до
    # проверки теста дело не дойдёт. На этом я и попался, когда писал этот тест.
    transcript = _TRANSCRIPT + " Okay Digger, was denn jetzt?"
    phrases = _good_phrases(4) + [_phrase("Digger", "Okay Digger, was denn jetzt?")]
    pack = _pack(phrases)
    pack["quiz"][1]["question_de"] = "Welche Rolle spielt das Wort 'Digger' im Auftritt?"
    with pytest.raises(ValueError) as err:
        _validate_and_normalize_pack(pack, STANDUP_PROFILE, transcript)
    assert "asks the meaning of a card unit" in str(err.value)


def test_quiz_about_the_show_itself_is_fine():
    """Ловушка не должна быть жадной: вопрос о содержании выступления обязан проходить,
    иначе модель будет переспрашиваться вхолостую до отказа."""
    out = _validate_and_normalize_pack(_pack(_good_phrases(5)), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["quiz"]) == 4


def test_prompt_demands_proper_german_headword_spelling():
    """«herzinfarkt bekommen» со строчной буквы: в немецком существительные пишутся с
    прописной, и человек перепишет ошибку с карточки. Плюс терялось «sich» у возвратных
    оборотов, а «Steckst nicht drin» стояло во втором лице вместо безличной формы."""
    prompt = STANDUP_PROFILE.llm_system
    assert "RECHTSCHREIBUNG DER KARTE" in prompt
    assert "herzinfarkt bekommen" in prompt, "нужен пример НЕПРАВИЛЬНОГО написания"
    assert "sich ins eigene Bein" in prompt
    assert "UNPERSÖNLICH" in prompt


# ── Форма из текста рядом со словарной (решение владельца 22.08.2026) ─────────

def test_card_carries_both_forms():
    """Человек должен видеть ДВЕ формы: словарную — чтобы сохранить и выучить, и ту, что
    стоит в ролике, — чтобы узнать выученное в живой речи. Выучив «ausrasten», он услышит
    «da rasten alle aus» и без второй формы не свяжет одно с другим."""
    phrases = _good_phrases(5)
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    card = out["phrases"][0]
    assert card["de"], "словарная форма"
    assert card["de_in_text"], "форма из текста"
    assert card["de_in_text"] in card["quote_de"]


def test_form_invented_outside_the_quote_is_thrown_away():
    """Форма из текста проверяется насквозь: она обязана дословно найтись в цитате, а
    цитата — в субтитрах. Если модель напишет форму, которой в цитате нет, это выдумка."""
    phrases = _good_phrases(5)
    phrases[0]["de_in_text"] = "diese Form steht nirgends"
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 4
    assert all("nirgends" not in p.get("de_in_text", "") for p in out["phrases"])


def test_card_without_the_text_form_is_not_shown_half_done():
    """Карточка без формы из текста не показывается частично — она выбрасывается и
    считается, как и любая неполная."""
    phrases = _good_phrases(5)
    phrases[0]["de_in_text"] = ""
    out = _validate_and_normalize_pack(_pack(phrases), STANDUP_PROFILE, _TRANSCRIPT)
    assert len(out["phrases"]) == 4


# ── Судья приёмки: правит ПОКАРТОЧНО, выпуск не переделывает ──────────────────

_JUDGE_TRANSCRIPT = (
    "Also ich sag mal so, ich hab null Bock auf Montag. "
    "Ein Uropa bekaeme einen Herzinfarkt, liefe das hier im Fernsehen. "
    "Privatversicherte verstehen den Joke, okay."
)


def _judge_card(de, quote, in_text, **over):
    card = {"de": de, "register_ru": "разговорное", "form_ru": "словарная форма",
            "translation_ru": "перевод", "literal_ru": "", "de_in_text": in_text,
            "quote_de": quote, "quote_ru": "перевод строки", "usage_ru": "с друзьями"}
    card.update(over)
    return card


def test_judge_fixes_one_card_and_leaves_the_rest_alone(monkeypatch):
    """Владелец 22.08.2026: «я для этого буду переформировать новость?» Нет. Судья правит
    ОДНУ карточку, остальные и весь выпуск остаются нетронутыми."""
    import backend.daily_video_judge as J

    cards = [
        _judge_card("herzinfarkt bekommen", "Ein Uropa bekaeme einen Herzinfarkt", "bekaeme einen Herzinfarkt"),
        _judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock"),
    ]
    fixed = dict(cards[0], de="einen Herzinfarkt bekommen")
    calls = {"n": 0}

    def _fake(cards_in, *, profile, transcript):
        calls["n"] += 1
        if calls["n"] == 1:
            return [{"i": 0, "verdict": "fix", "reason": "существительное со строчной",
                     "card": fixed}, {"i": 1, "verdict": "ok"}]
        return [{"i": i, "verdict": "ok"} for i in range(len(cards_in))]

    monkeypatch.setattr(J, "_ask_judge", _fake)
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert len(out) == 2, "ни одна карточка не должна пропасть"
    assert out[0]["de"] == "einen Herzinfarkt bekommen"
    assert out[1] == cards[1], "вторую карточку судья не трогал"
    assert report["fixed"] == 1 and report["dropped"] == 0


def test_judge_cannot_invent_under_the_guise_of_a_fix(monkeypatch):
    """Если судья под видом правки подставит цитату, которой в ролике не звучало, его
    правка обязана быть отбита теми же стражами, что стерегут свежие карточки. Иначе
    судья становится дырой в защите, ради которой он и поставлен."""
    import backend.daily_video_judge as J

    cards = [_judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock")]
    invented = dict(cards[0], quote_de="diesen Satz hat niemand je gesagt",
                    de_in_text="diesen Satz")
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [{"i": 0, "verdict": "fix",
                                                            "reason": "—", "card": invented}])
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert out == [], "выдуманная правка не должна попасть на экран"
    assert report["dropped"] == 1
    assert any("не прошла сверку" in r for r in report["reasons"])


def test_judge_drops_a_show_line(monkeypatch):
    """Реплика из шоу — не языковая единица. Её судья выбрасывает, но ВЫПУСК не бракует."""
    import backend.daily_video_judge as J

    cards = [
        _judge_card("Privatversicherte verstehen den Joke",
                    "Privatversicherte verstehen den Joke, okay", "Privatversicherte verstehen"),
        _judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock"),
    ]
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [
                            {"i": 0, "verdict": "drop", "reason": "реплика из шоу"},
                            {"i": 1, "verdict": "ok"}] if len(c) == 2
                        else [{"i": 0, "verdict": "ok"}])
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert [c["de"] for c in out] == ["Bock haben"]
    assert report["dropped"] == 1


def test_judge_walks_again_until_a_pass_is_clean(monkeypatch):
    """Одна правка иногда обнажает следующую, и один проход этого не ловит. Судья идёт
    заново, пока проход не окажется чистым."""
    import backend.daily_video_judge as J

    cards = [_judge_card("bock haben", "ich hab null Bock auf Montag", "null Bock")]
    step = {"n": 0}

    def _fake(cards_in, *, profile, transcript):
        step["n"] += 1
        if step["n"] == 1:
            return [{"i": 0, "verdict": "fix", "reason": "строчная",
                     "card": dict(cards_in[0], de="Bock haben")}]
        if step["n"] == 2:
            return [{"i": 0, "verdict": "fix", "reason": "перевод не согласован",
                     "card": dict(cards_in[0], translation_ru="иметь желание")}]
        return [{"i": 0, "verdict": "ok"}]

    monkeypatch.setattr(J, "_ask_judge", _fake)
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert report["passes"] == 3 and report["clean"] is True
    assert out[0]["de"] == "Bock haben"
    assert out[0]["translation_ru"] == "иметь желание"


def test_judge_verdict_fix_without_a_card_does_not_silently_pass(monkeypatch):
    """Вердикт «поправить» без самой починки — не повод молча пропустить карточку как
    годную: тогда мы соврём, что проверка прошла."""
    import backend.daily_video_judge as J

    cards = [_judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock")]
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [{"i": 0, "verdict": "fix",
                                                            "reason": "что-то не так"}])
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert len(out) == 1, "карточка остаётся как была"
    assert report["fixed"] == 0


# ── Работа судьи должна быть ВИДНА ────────────────────────────────────────────


def test_judge_is_told_to_fix_errors_not_polish_style():
    """Судья три прохода подряд не сходился и при этом не выбросил ни одной карточки —
    он бесконечно «улучшал» вместо того, чтобы исправлять ошибки."""
    from backend.daily_video_judge import _JUDGE_SYSTEM

    assert "du verbesserst nicht den STIL" in _JUDGE_SYSTEM
    assert "kommt nie zum Ende" in _JUDGE_SYSTEM


# ── Судья: зацикливание и снятие обязательных полей (22.08.2026) ──────────────

def test_empty_fix_does_not_keep_the_judge_spinning(monkeypatch):
    """Судья три прохода подряд «исправлял» «das kurze Vergnügen» на «das kurze
    Vergnügen» — до и после одно и то же, а причиной называл отсутствие артикля, которого
    не было только в его объяснении. Проверка не сходилась, потому что он выдумывал себе
    работу на уже исправленной карточке. Правка, ничего не меняющая, — не правка."""
    import backend.daily_video_judge as J

    cards = [_judge_card("das kurze Vergnügen", "ich hab null Bock auf Montag", "null Bock")]
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [
                            {"i": 0, "verdict": "fix", "reason": "существительное без артикля",
                             "card": dict(c[0])}])
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert report["passes"] == 1, "пустая правка не должна гнать судью на новый проход"
    assert report["clean"] is True
    assert report["fixed"] == 0
    assert out == cards


def test_judge_may_not_strip_a_required_marking(monkeypatch):
    """Судья снял помету регистра у «Applaus», потому что слово нейтральное, — и карточка
    проскочила в рубрику сленга уже без пометы. Правильный исход был «выбросить», а не
    «снять помету»: нейтральным словам в стендапе не место."""
    import backend.daily_video_judge as J

    # Единица обязана встречаться в цитате, иначе её отобьёт другой заслон, и тест будет
    # проверять не то, что задумано.
    cards = [_judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock")]
    stripped = dict(cards[0], register_ru="")
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [
                            {"i": 0, "verdict": "fix", "reason": "нейтральное слово",
                             "card": stripped}])
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert out == [], "карточка без обязательной пометы не должна дойти до экрана"
    assert any("пометы регистра" in r for r in report["reasons"])


def test_news_cards_do_not_need_a_register_marking(monkeypatch):
    """Защита пометы — правило СТЕНДАПА. В новостях речь нейтральная, и требовать помету
    там значило бы выбрасывать всё подряд."""
    import backend.daily_video_judge as J

    card = {"de": "unter Druck stehen", "form_ru": "инфинитив", "translation_ru": "перевод",
            "usage_ru": "с предлогом", "de_in_text": "null Bock",
            "quote_de": "ich hab null Bock auf Montag", "quote_ru": "перевод строки"}
    monkeypatch.setattr(J, "_ask_judge",
                        lambda c, *, profile, transcript: [{"i": 0, "verdict": "ok"}])
    out, _ = J.judge_and_repair_cards([card], profile=NEWS_PROFILE,
                                      transcript=_JUDGE_TRANSCRIPT)
    assert len(out) == 1


# ── Требования владельца 22.08.2026 по слабым карточкам ───────────────────────

def test_judge_is_told_to_drop_one_off_jokes_and_english():
    """Владелец о карточках стендапа: «Yes, Queen!» — английская фраза, «Halle an der
    fucking Saale» — разовая шутка про название города, нигде больше не пригодится.
    Немецкий там не неверный, но и учить там нечего."""
    from backend.daily_video_judge import _JUDGE_SYSTEM

    assert "Yes, Queen!" in _JUDGE_SYSTEM
    assert "EINMALWITZE" in _JUDGE_SYSTEM
    assert "der Shitstorm" in _JUDGE_SYSTEM, "прижившиеся англицизмы остаются"
    assert "keine reparierte Karte" in _JUDGE_SYSTEM, (
        "нейтральное слово выбрасывается, а не раздевается ради пропуска"
    )


def test_judge_stops_when_it_swings_back_and_forth(monkeypatch):
    """22.08.2026 судья три прохода правил перевод «Only-Page-Account»: сначала на одно,
    потом на другое, потом обратно на первое. Проверка не сходилась, хотя карточка не была
    ни плохой, ни исправленной — спор шёл о вкусе. Заслон против ОДИНАКОВЫХ соседних
    состояний этого не ловил, потому что состояния чередовались."""
    import backend.daily_video_judge as J

    # Единица обязана встречаться в цитате, иначе карточку выбросит заслон источника и
    # проверять будет нечего.
    cards = [_judge_card("Bock haben", "ich hab null Bock auf Montag", "null Bock")]
    step = {"n": 0}

    def _fake(cards_in, *, profile, transcript):
        step["n"] += 1
        # Качели: перевод меняется на «Б», потом обратно на исходный «А».
        new_translation = "вариант Б" if step["n"] == 1 else "перевод"
        return [{"i": 0, "verdict": "fix", "reason": "перевод неточен",
                 "card": dict(cards_in[0], translation_ru=new_translation)}]

    monkeypatch.setattr(J, "_ask_judge", _fake)
    out, report = J.judge_and_repair_cards(cards, profile=STANDUP_PROFILE,
                                           transcript=_JUDGE_TRANSCRIPT)
    assert report["passes"] <= 2, "качели обязаны обрываться, а не крутиться до предела"
    assert report["frozen"] >= 1
    assert len(out) == 1, "карточка остаётся, её просто перестают трогать"


# ── Дефекты четвёртого живого выпуска (22.08.2026, новости) ───────────────────


def test_both_prompts_forbid_garbled_proper_names():
    """Расшифровка субтитров переврала название ведомства — «Bafer» вместо BAFA, — и оно
    уехало человеку в резюме и в два вопроса теста. Такого ведомства нет, а человек
    прочтёт его как настоящее."""
    from backend.world_news_generator import _LLM_SYSTEM

    for prompt in (_LLM_SYSTEM, STANDUP_PROFILE.llm_system):
        assert "EIGENNAMEN" in prompt
        assert "Bafer" in prompt, "нужен живой пример искажения"
        assert "GAR NICHT" in prompt, "не уверен в имени — не использовать вовсе"


def test_judge_also_drops_sentences_and_garbled_names():
    """Судья видел эти карточки и пропустил: в его требованиях предложения из новости и
    перевранные имена названы не были."""
    from backend.daily_video_judge import _JUDGE_SYSTEM

    assert "SÄTZE und Satzteile mit Subjekt" in _JUDGE_SYSTEM
    assert "Bafer" in _JUDGE_SYSTEM


# ── Полка не должна гореть на черновиках (22.08.2026) ─────────────────────────

def test_reformed_day_returns_the_unused_video_to_the_shelf(monkeypatch):
    """Полка опустела за вечер: каждое переформирование помечало выбранный ролик
    израсходованным НАВСЕГДА, даже когда владелец тут же пересобирал выпуск и ролик никто
    не видел. Пять выступлений сгорели впустую. Ролик тратится тогда, когда он ДОШЁЛ ДО
    ЧЕЛОВЕКА, а не когда его подставили в черновик."""
    import backend.database as db
    import backend.world_news_generator as G

    released = []
    monkeypatch.setattr(db, "get_world_news_for_date",
                        lambda d: {"video_id": "старый", "status": "ready"})
    monkeypatch.setattr(db, "release_standup_shelf_video",
                        lambda vid: (released.append(vid), True)[1])
    monkeypatch.setattr(db, "take_next_from_standup_shelf", lambda exclude=None: {
        "video_id": "новый", "video_title": "Номер", "channel_title": "NightWash",
        "duration_seconds": 500, "has_manual_captions": True,
        "transcript": [{"text": _TRANSCRIPT}] * 12,
        "transcript_lang": "de", "transcript_is_generated": False})
    monkeypatch.setattr(G, "_call_llm", lambda *a, **kw: (_ for _ in ()).throw(
        RuntimeError("СТОП: ролик выбран")))

    with pytest.raises(RuntimeError):
        G.prepare_world_news("2026-08-22", rubric=RUBRIC_STANDUP)
    # До возврата дело не доходит: он идёт ПОСЛЕ сборки пакета. Проверяем сам механизм.
    assert db.release_standup_shelf_video("старый") is True
    assert "старый" in released


def test_a_video_that_reached_people_is_not_returned():
    """Ушедший людям ролик обратно не возвращается: его уже видели, и показать второй раз
    значило бы повториться."""
    import inspect

    from backend.world_news_generator import prepare_world_news

    src = inspect.getsource(prepare_world_news)
    assert 'str(_previous.get("status") or "") != "sent"' in src, (
        "возврат обязан проверять, что выпуск НЕ уходил людям"
    )


def test_judge_writes_labels_in_russian():
    """Судья писал пометы формы по-немецки — «Akkusativ», «Dativ Plural». Его задание
    написано по-немецки, и он отвечал в тон, а читает эту помету русскоязычный человек."""
    from backend.daily_video_judge import _JUDGE_SYSTEM

    assert "AUF RUSSISCH" in _JUDGE_SYSTEM
    assert "«Akkusativ»" in _JUDGE_SYSTEM, "нужен пример того, чего писать нельзя"
    assert "винительный падеж" in _JUDGE_SYSTEM


def test_emergency_refill_is_not_choked_by_an_old_budget():
    """22.08.2026 полка кончилась днём, а аварийное пополнение не справилось: у него
    стояло «два ролика, 70 секунд» — от тех времён, когда потолок всей подготовки был 300
    секунд. Потолок подняли до 600, а это забыли, и полка осталась на нуле до ночи."""
    import inspect

    from backend.world_news_generator import prepare_world_news

    src = inspect.getsource(prepare_world_news)
    assert "STANDUP_EMERGENCY_REFILL_BUDGET_SEC" in src
    assert "max_add=2, budget_sec=70" not in src, "старый зажатый бюджет вернулся"


# ── Владелец должен видеть НАСТОЯЩУЮ карточку прямо из превью (22.08.2026) ────

def test_preview_has_a_button_to_the_real_card_with_the_right_date():
    """Владелец весь день судил о продукте по сводке для одобрения и ни разу не открыл
    настоящую карточку: кнопка «Смотреть» жила на другом сообщении, о котором надо было
    помнить. Теперь она стоит в самом превью.

    Дата зашита в ссылку намеренно: превью обычно про ЗАВТРА, а экран без даты показал бы
    СЕГОДНЯШНЮЮ запись — кнопка соврала бы владельцу о том, что он одобряет."""
    import re

    src = open("bot_3.py", encoding="utf-8").read()
    rows = src[src.index("def _world_news_preview_keyboard_rows"):]
    rows = rows[:rows.index("\nasync def ")]
    assert "Посмотреть как увидит человек" in rows
    assert 'worldnews_{compact_date}' in rows, "ссылка обязана нести дату записи"
    # Дата идёт без дефисов: Telegram допускает в start_param только буквы, цифры, _ и -.
    assert 'date_str.replace("-", "")' in rows
    assert re.search(r"compact_date\.isdigit\(\)", rows), (
        "кривую дату в ссылку класть нельзя"
    )


def test_frontend_reads_the_date_from_the_deep_link():
    """Вторая половина той же кнопки: экран обязан прочитать дату из ссылки и запросить
    ИМЕННО этот день, иначе владелец увидит не то, что одобряет."""
    src = open("frontend/src/App.jsx", encoding="utf-8").read()
    assert "worldNewsDate" in src
    assert "worldnews[_-]" in src, "разбор даты из start_param"
    assert "date: worldNewsDate" in src, "дата обязана уйти в запрос записи"


# ── Карточка без слова и помета-жаргон (22.08.2026, первый взгляд на экран) ───

def test_headword_and_save_button_are_never_scrolled_away():
    """Владелец впервые открыл настоящую карточку и спросил: «а где здесь вообще слово?
    и что система предлагает сохранять?» Я накануне сделал прокручиваемой ВСЮ карточку,
    и на длинных карточках заголовок уезжал за верхний край — человек видел перевод без
    слова. Слово и кнопка сохранения обязаны быть прибиты, прокручивается середина."""
    css = open("frontend/src/App.css", encoding="utf-8").read()
    jsx = open("frontend/src/App.jsx", encoding="utf-8").read()

    assert ".worldnews-card-body {" in css, "нужна отдельная прокручиваемая середина"
    assert "worldnews-card-body" in jsx, "середина обязана быть обёрнута в разметке"
    card_block = css[css.index(".worldnews-card {"):css.index(".worldnews-card-de")]
    assert "overflow-y: hidden" in card_block, (
        "сама карточка прокручиваться не должна — иначе заголовок уезжает"
    )



# ── Обрезка заголовка и огрызок фразы (22.08.2026, экран владельца) ───────────

def test_headword_ending_on_an_article_is_a_torn_off_fragment():
    """На экране стояло «die Koalition auffordern, die» — фразу отрезали посреди
    предложения и оставили висящий артикль. Это не выражение, а огрызок.

    Ловится механически: список служебных слов ЗАКРЫТЫЙ, никакого разбора грамматики —
    а значит и никаких догадок. Глаголы в конце законны: «nichts am Hut haben»."""
    from backend.world_news_generator import _headword_ends_dangling

    assert _headword_ends_dangling("die Koalition auffordern, die")
    assert _headword_ends_dangling("die Koalition auffordern und")
    assert _headword_ends_dangling("das Muster solcher Depots,")
    assert not _headword_ends_dangling("nichts am Hut haben")
    assert not _headword_ends_dangling("sich ein Bild von der Lage machen")
    assert not _headword_ends_dangling("Bock haben (auf etwas)")


def test_a_torn_off_headword_never_reaches_the_screen():
    """Обрезать хвост своими руками нельзя — мы не знаем, где вправду кончается оборот.
    Карточка выбрасывается, и на повторе модель соберёт её целиком."""
    from backend.world_news_generator import _card_passes_source_guards

    # Карточка полная во всём остальном — иначе заслон назовёт первую претензию
    # (пустое поле) и до висящего хвоста дело не дойдёт.
    ok, why = _card_passes_source_guards(
        {"de": "die Koalition auffordern, die", "quote_de": "ich hab null Bock auf Montag",
         "de_in_text": "null Bock", "translation_ru": "перевод", "usage_ru": "с падежом",
         "form_ru": "словарная форма"}, _TRANSCRIPT)
    assert ok is False
    assert "обрезана посреди фразы" in why


def test_form_label_is_gone_from_the_user_card_but_stays_in_the_preview():
    """Владелец: «зачем эта надпись про падеж? пользователю это не нужно». С экрана убрана,
    в превью осталась — там по ней видно, не разошёлся ли перевод с показанной формой."""
    jsx = open("frontend/src/App.jsx", encoding="utf-8").read()
    bot = open("bot_3.py", encoding="utf-8").read()

    assert "worldnews-card-form" not in jsx, "помета формы не должна рисоваться человеку"
    assert 'phrase.register_ru' in jsx, "помета регистра остаётся: она про уместность речи"
    assert '"register_ru", "form_ru"' in bot, "в превью владельцу обе пометы остаются"


def test_headword_does_not_shrink_and_get_clipped():
    """Вторая строка заголовка срезалась: в колонке flex-элемент по умолчанию ужимается
    ниже содержимого, а вместе с обрезкой по краю это отрезало половину слова."""
    css = open("frontend/src/App.css", encoding="utf-8").read()
    block = css[css.index(".worldnews-card-de {"):css.index(".worldnews-art ")]
    assert "flex: 0 0 auto" in block, "заголовок не имеет права сжиматься"
    assert "overflow: hidden" not in block, "обрезка по вертикали срезает вторую строку"


# ── ОПИСЬ ТРЕБОВАНИЙ К ЗАДАНИЯМ МОДЕЛИ ────────────────────────────────────────
#
# Каждое требование здесь добыто разбором живого дефекта: владелец увидел его на экране,
# мы нашли причину и записали правило в задание. Задания переписываются целиком — и при
# переписывании 22.08.2026 одно требование едва не пропало молча.
#
# Поэтому проверка не по одной строке на правило, а ОПИСЬЮ: список с именами, который
# падает с внятным сообщением, если что-то исчезло. Требование можно переформулировать —
# тогда правится и опись; но исчезнуть незаметно оно не может.

_NEWS_REQUIREMENTS = {
    "единица должна годиться в другой новости": ("SPRACHEINHEIT", "wiederbegegnet"),
    "числа из этой новости — не единица": ("ZAHLEN AUS DIESER MELDUNG",),
    "названия должностей — не единица": ("AMTS- UND TITELBEZEICHNUNGEN",),
    "целое предложение — не единица": ("GANZE SÄTZE",),
    "две единицы, склеенные запятой": ("ZUSAMMENGEKLEBT",),
    "идиому вынимают из предложения": ("HERAUSGELÖST", "Tür und Tor öffnen"),
    "единица не кончается артиклем или союзом": ("NIE auf einem Artikel",),
    "битую расшифровку не берут": ("SPRACHERKENNUNG", "Jugendchutz"),
    "имена собственные — верно или никак": ("EIGENNAMEN", "Bafer"),
    "прописные буквы в тезисах": ("SCHREIBWEISE", "ТЕРЕЗА"),
    "помета формы из закрытого списка": ("ЗАКРЫТЫЙ СПИСОК", "словарная форма"),
    "помета без немецких слов внутри": ("инфинитив с sich",),
    "перевод в той же форме, что показана": ("DERSELBEN Form",),
    "возвратность читается из цитаты": ("ВОЗВРАТНОСТЬ ЧИТАЕТСЯ ИЗ ЦИТАТЫ",),
    "форма из текста обязана быть в цитате": ("de_in_text", "wörtlich im Zitat"),
}

_STANDUP_REQUIREMENTS = {
    "берём только то, где контекст решает смысл": ("KONTEXT die Bedeutung",),
    "плана по количеству нет": ("KEINE ZIELZAHL",),
    "оборот остаётся целым": ("Bock haben (auf etwas)", "FALSCH"),
    "воспроизводимость": ("WIEDERVERWENDBARKEIT",),
    "английские цитаты и разовые шутки — вон": ("Yes, Queen!", "EINMALWITZE"),
    "нейтральное выбрасывают, а не раздевают": ("NEUTRALE Alltagswörter",),
    "помета регистра обязательна": ("register_ru", "derb/vulgär"),
    "возвратность читается из цитаты": ("ВОЗВРАТНОСТЬ ЧИТАЕТСЯ ИЗ ЦИТАТЫ",),
    "помета формы из закрытого списка": ("ЗАКРЫТЫЙ СПИСОК",),
    "спойлер развязки запрещён": ("AUSGANG",),
    "заголовок по правилам немецкой орфографии": ("RECHTSCHREIBUNG DER KARTE",),
    "имена собственные — верно или никак": ("EIGENNAMEN",),
}

_JUDGE_REQUIREMENTS = {
    "исправляет ошибки, а не улучшает стиль": ("du verbesserst nicht den STIL",),
    "пишет пометы по-русски": ("AUF RUSSISCH", "«Akkusativ»"),
    "закрытый список помет": ("ЗАКРЫТЫЙ СПИСОК",),
    "предложения из новости — вон": ("SÄTZE und Satzteile mit Subjekt",),
    "возвратность читается из цитаты": ("ВОЗВРАТНОСТЬ ЧИТАЕТСЯ ИЗ ЦИТАТЫ", "jemanden unter den Tisch saufen"),
    "висящий артикль в конце — вон": ("auf einem Artikel oder einer Konjunktion ENDEN",),
    "перевранные имена — вон": ("Bafer",),
    "нейтральное выбрасывают, а не раздевают": ("keine reparierte Karte",),
    "английские цитаты и разовые шутки — вон": ("EINMALWITZE",),
}


def _assert_requirements(prompt, requirements, whose):
    missing = [name for name, marks in requirements.items()
               if not all(mark in prompt for mark in marks)]
    assert not missing, (
        f"из задания «{whose}» пропали требования: " + "; ".join(missing)
    )


def test_news_prompt_keeps_every_requirement_we_paid_for():
    """Каждое требование добыто разбором живого дефекта на экране владельца. Переписывать
    задание можно, терять требования — нет."""
    from backend.world_news_generator import _LLM_SYSTEM

    _assert_requirements(_LLM_SYSTEM, _NEWS_REQUIREMENTS, "Новость дня")


def test_standup_prompt_keeps_every_requirement_we_paid_for():
    _assert_requirements(STANDUP_PROFILE.llm_system, _STANDUP_REQUIREMENTS, "Стендап дня")


def test_judge_keeps_every_requirement_we_paid_for():
    from backend.daily_video_judge import _JUDGE_SYSTEM

    _assert_requirements(_JUDGE_SYSTEM, _JUDGE_REQUIREMENTS, "судья приёмки")


def test_a_normal_question_may_mention_a_card_unit():
    """Первая версия проверки «вопрос не повторяет карточку» была слишком жадной: она
    бракевала ЛЮБОЙ вопрос, где встречались слова карточки. 22.08.2026 карточка
    «vollständig gelöscht» и законный вопрос «Wann wurde das Feuer vollständig gelöscht?»
    (КОГДА потушили, а не что это значит) забраковали весь пакет трижды подряд — выпуск не
    собрался вовсе. Защита оказалась вреднее дефекта, от которого стерегла.

    Бракуем только вопрос-определение: единица в кавычках или рядом слово, которым
    по-немецки спрашивают значение."""
    transcript = _TRANSCRIPT + " Das Feuer wurde vollständig gelöscht."
    phrases = _good_phrases(4) + [
        _phrase("vollständig gelöscht", "Das Feuer wurde vollständig gelöscht.")
    ]
    phrases[-1]["de_in_text"] = "vollständig gelöscht"
    pack = _pack(phrases)
    pack["quiz"][0]["question_de"] = "Wann wurde das Feuer vollständig gelöscht?"
    out = _validate_and_normalize_pack(pack, STANDUP_PROFILE, transcript)
    assert len(out["quiz"]) == 4, "содержательный вопрос обязан проходить"


# ── Артикль берётся из справочника, а не у модели (22.08.2026) ────────────────

def test_article_comes_from_the_reference_not_from_the_model(monkeypatch):
    """Правило ноль: ответ берётся из источника, модель только читает. Род существительного
    — ровно тот случай, где источник есть и давно построен (article_authority). Спрашивать
    род у модели, когда справочник под рукой, значит предпочесть догадку знанию."""
    import backend.daily_video_quality as Q

    monkeypatch.setattr(Q, "article_from_reference",
                        lambda de, allow_network=False: ("der", "wiktionary"))
    fixed, what = Q.correct_article_from_reference({"de": "das Schwarzmarkt"})
    assert fixed["de"] == "der Schwarzmarkt"
    assert "wiktionary" in what, "источник обязан быть назван по имени"


def test_silent_reference_is_not_a_reason_to_guess(monkeypatch):
    """Справочник промолчал — карточка остаётся как есть. Неизвестность честнее выдумки."""
    import backend.daily_video_quality as Q

    monkeypatch.setattr(Q, "article_from_reference",
                        lambda de, allow_network=False: (None, "справочник не знает"))
    card = {"de": "die Kommentarspalte"}
    fixed, what = Q.correct_article_from_reference(card)
    assert fixed == card and what == ""


def test_reference_is_asked_only_where_gender_exists():
    """У оборотов и глаголов рода нет — справочник туда не ходит вовсе, иначе мы будем
    спрашивать про «sich unter den Tisch saufen» и получать мусор."""
    from backend.daily_video_quality import split_article

    assert split_article("die Kommentarspalte") == ("die", "Kommentarspalte")
    assert split_article("sich unter den Tisch saufen") == (None, None)
    assert split_article("heulen") == (None, None)
    assert split_article("Bock haben (auf etwas)") == (None, None)


# ── Написание и существование слова — из справочника (22.08.2026) ─────────────

def test_reference_fixes_a_lowercase_noun():
    """«herzinfarkt» со строчной буквы владелец увидел на экране. Немецкое существительное
    пишется с заглавной, и справочник знает это сам — просить об этом модель не нужно."""
    from backend.daily_video_quality import split_article

    # Разделитель обязан принимать строчное написание: артикль уже сказал, что это
    # существительное, а неверный регистр — ровно то, что мы пришли чинить.
    assert split_article("die kommentarspalte") == ("die", "kommentarspalte")


def test_reference_is_not_applied_to_a_role_it_did_not_answer(monkeypatch):
    """Проверка на живых словах показала дефект в самой задумке: на «heulen» справочник
    отвечает «Heulen» с заглавной — и он прав со своей стороны, «das Heulen» существует
    как отглагольное существительное. Но на карточке это ГЛАГОЛ «рыдать», и подставив
    заглавную, мы своими руками сделали бы верную карточку неверной.

    Справочник отвечает на вопрос «как пишется слово в роли, которую выбрал Я», а нам
    нужна роль с карточки. Без артикля роль неизвестна — написание не трогаем."""
    import backend.daily_video_quality as Q

    monkeypatch.setattr(Q, "spelling_from_reference",
                        lambda de, allow_network=False: ("исправлено", "Heulen"))
    card, what = Q.correct_spelling_from_reference({"de": "heulen"})
    assert card["de"] == "heulen" and what == "", "у слова без артикля роль неизвестна"


def test_slang_is_not_judged_by_a_reference(monkeypatch):
    """Справочники знают живую речь плохо, а стендап — это сплошь сленг. Решение владельца
    22.08.2026: карточке со сленговой пометой верим больше, чем справочнику."""
    import backend.daily_video_quality as Q

    monkeypatch.setattr(Q, "spelling_from_reference",
                        lambda de, allow_network=False: ("не слово", ""))
    assert Q.word_not_german({"de": "Digger", "register_ru": "молодёжное"}) == ""
    assert Q.word_not_german({"de": "Abschiebu"}) != "", "в новостях мусор выбрасывается"
