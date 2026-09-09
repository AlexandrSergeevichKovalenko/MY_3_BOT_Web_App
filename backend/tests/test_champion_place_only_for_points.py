"""«Чемпион дня/недели» и рейтинг мини-аппа: место только за очки (владелец 09.09.2026).

Экран 08.09.2026: Olga — 1 место с 0 очков (0/7 верно) над Oleg 45 и Aleksandr 35, потому
что стояли ворота «ответь на половину заданий». Ворота убраны; вместо них у задания есть
цена по типу; ноль — без места и без номинаций; равные очки — одно место.
Стратегия: docs/tasks/champion_ranking_strategy.md."""
from __future__ import annotations

from backend.quiz_leaderboard import (PLACE_BONUS, TASK_PRICE, assign_ranks, compute_quiz_leaderboard,
                                      task_kind)


def _row(key, uid, name, ok, t=None, score=None):
    r = {"challenge_key": key, "user_id": uid, "name": name, "is_correct": ok, "time_ms": t}
    if score is not None:
        r["score"] = score
    return r


def _screen_08_09():
    """Живой случай: 13 заданий; Olga ответила на 7, все неверно; Oleg и Aleksandr — по 3,
    все верно (Oleg быстрее)."""
    rows = []
    for i in range(7):
        rows.append(_row(f"mc:q{i}", 3, "Olga", False, 4000))
    for i in range(3):
        rows.append(_row(f"au:a{i}", 1, "Oleg", True, 5000))
        rows.append(_row(f"au:a{i}", 2, "Aleksandr", True, 9000))
    for i in range(3):
        rows.append(_row(f"rb:r{i}", 9, "Zero", False, 3000))   # ещё три задания периода
    return rows


def test_olga_с_нулём_не_чемпион_а_внизу_без_места():
    lb = compute_quiz_leaderboard(_screen_08_09())
    names = [l["name"] for l in lb["leaders"]]
    assert names[:2] == ["Oleg", "Aleksandr"]
    by = {l["name"]: l for l in lb["leaders"]}
    assert by["Oleg"]["rank"] == 1 and by["Aleksandr"]["rank"] == 2
    assert by["Olga"]["rank"] is None and by["Olga"]["points"] == 0 and by["Olga"]["answered"] == 7
    assert by["Olga"]["prize_eligible"] is False
    assert lb["min_for_prize"] == 0


def test_ноль_не_получает_номинаций():
    lb = compute_quiz_leaderboard(_screen_08_09())
    assert lb["accurate"]["name"] == "Oleg"          # не «Olga · 0%»
    assert lb["active"]["name"] in ("Oleg", "Aleksandr")   # не «Olga · 7 зад.»
    assert lb["fastest"]["name"] == "Oleg"


def test_цена_задания_по_типу_и_бонус_за_место():
    rows = [_row("cw:1", 1, "A", True, 60000), _row("rb:1", 2, "B", True, 3000), _row("rb:1", 1, "A", True, 5000)]
    lb = compute_quiz_leaderboard(rows)
    by = {l["user_id"]: l for l in lb["leaders"]}
    assert by[1]["points"] == TASK_PRICE["cw"] + PLACE_BONUS[1] + TASK_PRICE["rb"] + PLACE_BONUS[2]   # 30+5 + 10+3
    assert by[2]["points"] == TASK_PRICE["rb"] + PLACE_BONUS[1]                                       # 10+5
    # кроссворд в 60 с стоит втрое дороже ребуса в 9 с — «три быстрых» больше не обгоняют «25 медленных» воротами
    assert TASK_PRICE["cw"] == 3 * TASK_PRICE["rb"]


def test_набор_спринта_стоит_как_кроссворд_по_доле():
    rows = [_row("ast:s1", 1, "A", True, None, 1.0), _row("ast:s2", 1, "A", True, None, 0.5),
            _row("ad:s3", 1, "A", False, None, 0.0)]
    a = compute_quiz_leaderboard(rows)["leaders"][0]
    assert a["points"] == 30 + 15 + 0 and a["correct"] == 2 and a["answered"] == 3 and a["golds"] == 0


def test_равные_очки_одно_место_1_2_2_4():
    leaders = [{"points": 50, "user_id": 1}, {"points": 40, "user_id": 2}, {"points": 40, "user_id": 3},
               {"points": 10, "user_id": 4}, {"points": 0, "user_id": 5}]
    assert [l["rank"] for l in assign_ranks(leaders)] == [1, 2, 2, 4, None]


def test_все_с_нулём_чемпиона_нет():
    lb = compute_quiz_leaderboard([_row("mc:1", 1, "A", False, 1000), _row("mc:1", 2, "B", False, 1000)])
    assert all(l["rank"] is None for l in lb["leaders"])
    assert lb["fastest"] is None and lb["accurate"] is None and lb["active"] is None
    import bot_3
    assert bot_3._build_champion_card(lb, week_no=37, days=1) is None
    from backend.champion_poster import render_champion_poster
    assert render_champion_poster(lb, week_no=37, days=1) is None


def test_карточка_и_плакат_показывают_настоящее_место():
    import bot_3
    rows = [_row("mc:1", 1, "Anna", True, 1000), _row("mc:2", 2, "Boris", True, 1000),
            _row("mc:3", 3, "Olga", False, 1000)]
    lb = compute_quiz_leaderboard(rows)          # Anna 15, Boris 15 → оба 1-е; Olga 0 → прочерк
    text = bot_3._build_champion_card(lb, week_no=37, days=1)
    assert "Anna &amp; Boris" in text or "Anna & Boris" in text
    assert "🥇 <b>Anna</b>" in text and "🥇 <b>Boris</b>" in text and "— <b>Olga</b>" in text


def test_у_каждого_типа_задания_есть_цена():
    """Тип без цены не получает «10 по умолчанию» — строка пропускается и считается,
    а этот тест не даёт каталогу типов разойтись с таблицей цен."""
    from backend.answer_eval import _CONTENT_ID_FIELD
    kinds = set(_CONTENT_ID_FIELD) | {"mc", "nd", "ast", "ad", "wf"}
    assert kinds <= set(TASK_PRICE), kinds - set(TASK_PRICE)
    lb = compute_quiz_leaderboard([_row("zz:1", 1, "A", True, 1000), _row("mc:1", 1, "A", True, 1000)])
    assert lb["unpriced"] == 1 and lb["leaders"][0]["points"] == TASK_PRICE["mc"] + PLACE_BONUS[1]
    assert task_kind("ast:s1") == "ast" and task_kind("mc:artikel:der Tisch") == "mc"
