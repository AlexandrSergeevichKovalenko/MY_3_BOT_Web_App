"""
Artikel Sprint — daily shared set builder.

Freezes ONE ordered word set per day from the day's theme (verified nouns only),
so every player competes on the same set (fair ranking). If the chosen theme is
sparse, tops up from other themes so the 2-minute game never runs short.
"""
from __future__ import annotations

import logging
import random

# ПОТОЛОК набора, а не цель. Больше этого игрок за две минуты не осилит, поэтому
# длиннее набор замораживать незачем. Если в теме слов меньше — набор просто короче,
# и игра честно заканчивается на последнем слове (ArtikelSprintGame: кончились
# слова → результат). Ничего доливать не надо.
DEFAULT_SET_SIZE = 140

# НИЖНЕГО ПОРОГА У ТЕМЫ ДНЯ НЕТ. Это решение владельца 19.08.2026, дословно:
#
#   «даже если в теме есть 50 слов, то значит она может быть темой дня, неважно.
#    Человеку нужно выучить все слова со всех тем, и ему совершенно всё равно,
#    сколько в этой теме слов. Если их нету столько — зачем тужиться и придумывать
#    что-то, чего не существует?»
#
# История вопроса, чтобы порог не завели заново. 19.08.2026 владелец сыграл
# «Computer & Geräte» и увидел 77 слов из 80 живых — весь банк темы разом, вместе
# с её мусорным хвостом. Первой правкой я поставил порог «тема ведёт день, только
# если её нельзя пройти насквозь» (140 слов) — и это была ошибка двух сортов:
#
#   • цифра выдумана. 140 — это размер набора, который сам когда-то поставили
#     на глаз. Подпирать одну произвольную цифру другой — не обоснование;
#   • порог воссоздал ровно то давление, ради снятия которого в тот же день
#     убрали цель «150 слов на тему»: «дорасти до числа, иначе не участвуешь».
#     А именно оно и заставляло генератор скрести дно.
#
# Плохим тот экран делал МУСОР, а не размер темы. Мусор убран (92 англицизма вне
# живой речи, слова без подтверждения справочником) и закрыт стражем на приёмке.
# Тема из 50 честных слов даёт игру из 50 честных слов — с ней всё в порядке.
#
# Единственное, что мешает теме вести день, — отсутствие слов вообще. Это не порог,
# а арифметика: набор не может быть пустым.


def _dedup_words(words: list[dict]) -> list[dict]:
    """Dedup by (word, article) — NOT word alone — so a two-gender noun keeps both
    senses (der See / die See). Carries the Russian meaning (`ru`) and the
    two-gender flag (`tg`) the game needs to show the sense for those words."""
    from backend.article_sprint_generator import resolve_article
    seen: set = set()
    uniq: list[dict] = []
    for w in words:
        wd = str(w.get("w") or "").strip()
        # Deterministic guard: correct a wrong bank article (die Börsenwert → der)
        # before it's frozen into the served set, so grading is right from the start.
        a = resolve_article(wd, w.get("a")) if wd else str(w.get("a") or "").lower()
        k = (wd.lower(), a)
        if wd and a and k not in seen:
            seen.add(k)
            uniq.append({"w": wd, "a": a, "ru": w.get("ru") or "", "tg": bool(w.get("tg"))})
    return uniq


def _pick_fallback_theme(play_date, min_have: int = 1) -> str | None:
    """Ротация тем по дате — среди тех, где вообще есть слова.

    `min_have` по умолчанию 1: годится ЛЮБАЯ непустая тема. Числом больше единицы
    его звать не надо — это снова будет порог, который заставляет тему «дорасти»
    (см. длинный комментарий вверху файла)."""
    from backend.database import list_article_sprint_themes
    # `list_article_sprint_themes` отдаёт и погашенные темы тоже — флаг `active`
    # надо спрашивать явно. Иначе тема, погашенная слиянием или самим владельцем,
    # поведёт день, как только в ней окажется хоть одно слово. Сейчас это не
    # стреляет только потому, что у слитых тем слов не осталось, — то есть держится
    # на совпадении, а не на правиле.
    themes = [t for t in list_article_sprint_themes()
              if t.get("active", True) and int(t.get("verified_count") or 0) >= min_have]
    if not themes:
        return None
    themes.sort(key=lambda t: t["theme_key"])
    idx = play_date.toordinal() % len(themes)
    return themes[idx]["theme_key"]


def build_daily_set(play_date, *, size: int = DEFAULT_SET_SIZE) -> dict:
    """Build (or rebuild) the daily shared set for play_date. Returns stats dict."""
    from backend.database import (
        ensure_article_sprint_schema, get_article_sprint_theme_for_date,
        get_article_sprint_verified_sample, upsert_article_sprint_set,
        count_article_theme_verified,
    )
    ensure_article_sprint_schema()

    # Содержимое набора обязано соответствовать его ЗАГОЛОВКУ. Тему дня НЕ подменяем
    # из-за размера: маленькая тема — полноценная тема дня, набор просто короче.
    # Меняем только если темы на день нет вовсе или в ней нет ни одного слова.
    # Доливать слова из чужих тем нельзя ни при каком размере — именно долив когда-то
    # положил медицинские существительные под заголовок «Technik & Computer».
    scheduled = get_article_sprint_theme_for_date(play_date)
    theme_key = scheduled
    if not theme_key or count_article_theme_verified(theme_key) < 1:
        fallback = _pick_fallback_theme(play_date)
        if scheduled and fallback and fallback != scheduled:
            logging.warning(
                "article_sprint: у темы дня «%s» нет ни одного проверенного слова → ведёт «%s»",
                scheduled, fallback,
            )
        theme_key = fallback
    if not theme_key:
        # Ни в одной теме нет слов → честный смешанный набор под своим заголовком.
        words = get_article_sprint_verified_sample(None, size)
        theme_key = "gemischt"
    else:
        words = get_article_sprint_verified_sample(theme_key, size)

    # dedup (by word+article) + shuffle
    uniq = _dedup_words(words)
    random.shuffle(uniq)

    # Единственная причина не выдать набор — слов нет вообще. Это не порог, а
    # арифметика: играть в пустой набор нельзя.
    if not uniq:
        return {"status": "insufficient", "theme_key": theme_key, "available": 0,
                "hint": "в банке нет ни одного проверенного слова"}

    set_id = f"asd_{play_date.isoformat()}"
    upsert_article_sprint_set(
        set_id=set_id, kind="daily", play_date=play_date,
        theme_key=theme_key, words=uniq,
    )
    logging.info("article_sprint: built daily set %s theme=%s words=%s", set_id, theme_key, len(uniq))
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}


PRACTICE_SET_SIZE = 120
PRACTICE_MIN = 20


def build_practice_set(theme_key: str, user_id: int, play_date, *, size: int = PRACTICE_SET_SIZE) -> dict:
    """Build a fresh personal practice set for a Pro user from one of the 21 themes
    (solo, not ranked). A new set_id each call → always replayable."""
    import time
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    # Личная тренировка — единственное место, где известно, КТО играет, поэтому только
    # здесь работает остывание: слова, которые этот человек недавно взял верно, уходят в
    # конец очереди. Общий набор дня и битвы одни на всех, там личного остывания быть не
    # может — там слово просто остаётся в игре.
    words = get_article_sprint_verified_sample(theme_key, size, user_id=int(user_id))
    uniq = _dedup_words(words)
    random.shuffle(uniq)
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "theme_key": theme_key, "available": len(uniq)}
    set_id = f"asp_{int(user_id)}_{theme_key}_{int(time.time())}"
    upsert_article_sprint_set(
        set_id=set_id, kind="practice", play_date=play_date,
        theme_key=theme_key, words=uniq, owner_user_id=int(user_id),
    )
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}


BATTLE_SET_SIZE = 350   # 2-min battle, ~150 taps/min fast → preload with a buffer


def build_battle_set_mixed(theme_keys, battle_id: int, play_date, *, size: int = BATTLE_SET_SIZE) -> dict:
    """Battle words mixed RANDOMLY (different every time). theme_keys empty/None →
    sample across ALL themes; otherwise across the selected themes only. ~`size`
    words preloaded so a 2-min battle never runs out."""
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    keys = [str(k) for k in (theme_keys or []) if str(k).strip()]
    words: list[dict] = []
    if not keys:
        words = get_article_sprint_verified_sample(None, size)  # ORDER BY random() — fresh mix
    else:
        per = max(1, size // len(keys)) + 8
        have: list[str] = []
        for tk in keys:
            for w in get_article_sprint_verified_sample(tk, per, exclude_words=have):
                words.append(w)
                have.append(str(w["w"]))
        if len(words) < size:  # top up from the SAME selected themes (keeps the focus)
            for tk in keys:
                if len(words) >= size:
                    break
                for w in get_article_sprint_verified_sample(tk, size - len(words), exclude_words=have):
                    words.append(w)
                    have.append(str(w["w"]))
    uniq = _dedup_words(words)
    random.shuffle(uniq)
    uniq = uniq[:size]
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "available": len(uniq)}
    set_id = f"asb_{int(battle_id)}"
    upsert_article_sprint_set(
        set_id=set_id, kind="battle", play_date=play_date,
        theme_key=(keys[0] if len(keys) == 1 else "gemischt"), words=uniq,
    )
    return {"status": "ready", "set_id": set_id, "word_count": len(uniq),
            "themes": keys or ["all"]}


def build_battle_set(theme_key: str, battle_id: int, play_date, *, size: int = DEFAULT_SET_SIZE) -> dict:
    """One frozen shared set for a battle (set_id = 'asb_<battle_id>'). All members
    compete on the same words."""
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    words = get_article_sprint_verified_sample(theme_key, size)
    if len(words) < size:
        have = {str(w["w"]).lower() for w in words}
        words.extend(get_article_sprint_verified_sample(None, size - len(words), exclude_words=list(have)))
    uniq = _dedup_words(words)
    random.shuffle(uniq)
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "theme_key": theme_key, "available": len(uniq)}
    set_id = f"asb_{int(battle_id)}"
    upsert_article_sprint_set(
        set_id=set_id, kind="battle", play_date=play_date, theme_key=theme_key, words=uniq,
    )
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}
