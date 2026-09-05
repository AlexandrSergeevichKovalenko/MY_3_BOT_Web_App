# -*- coding: utf-8 -*-
"""Фильтр полезности слов для тренажёра артиклей.

Разбор словника 29.07 показал, чем он был набит: 48% слов не входили даже в 50 000
самых частых («der Föhnsturm», «das Hygrometer», «das Wassertröpfchen»), а 37% были
производными от другого слова того же банка — 48 «домов», 32 «куртки», 26 «бокалов».
Род у производного тот же, что у основы, поэтому учить его отдельно нечему: кто знает
«das Haus», знает и «das Landhaus».

Причина была в устройстве наполнения: цель 280 слов на тему, а ходовых слов в теме
столько нет — и генератор добивал план чем придётся.

Здесь три проверки, через которые проходит каждое новое слово:
  1. частотность — слово (или его форма множественного числа) входит в первые N
     самых частых; сложные слова этим НЕ отсекаются: «die Waschmaschine» и
     «der Kühlschrank» порог проходят, а «der Sturmtiefausläufer» нет;
  2. семья — не больше двух производных одного корня на тему;
  3. смысл — перевод не должен повторять уже имеющийся в теме.
"""
from __future__ import annotations

import functools
import os
import re

FREQ_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "de_freq_50k.txt")
DEFAULT_MAX_RANK = 20000   # до этого места слово берём без вопросов
SECOND_OPINION_RANK = 60000  # дальше — спрашиваем модель, нужно ли слово в быту
FAMILY_CAP = 2             # сколько производных одного корня оставляем в теме
# ┌─ ГОЛОСОВАНИЕ УБРАНО 05.09.2026. НЕ ВОЗВРАЩАТЬ БЕЗ СЛОВА ВЛАДЕЛЬЦА. ─────────────┐
# │ Владелец: «Три раза спрашивать модель, нужно ли это слово, — ну это перебор.    │
# │ Зачем столько запросов? Одного запроса достаточно».                             │
# │                                                                                │
# │ Три голоса латали неустойчивость: замер 16.08.2026 — один и тот же вопрос про   │
# │ одни и те же слова, заданный дважды, дал 12% разных ответов. Но неустойчив был  │
# │ не судья, а ВОПРОС: «стоит ли учить это слово» — просьба взвесить пользу, а у   │
# │ пользы нет правильного ответа, есть степень. Голосование усредняло шум втрое    │
# │ дороже и правильным ответ не делало.                                            │
# │                                                                                │
# │ Теперь модель НЕ выносит приговор, а РАСКЛАДЫВАЕТ ПО РАЗРЯДАМ (см. промпт), а   │
# │ какие разряды проходят — решаем мы. Сортировка по названным полкам устойчива:   │
# │ у неё есть на что опереться, у «стоит ли учить» — не на что.                     │
# └────────────────────────────────────────────────────────────────────────────────┘
EVERYDAY_VOTES = 1

# ⛔ КАКИЕ РАЗРЯДЫ ПРОХОДЯТ — РЕШЕНИЕ ВЛАДЕЛЬЦА 05.09.2026, НЕ МОДЕЛИ.
#
# Дословно: «Я считаю, а и б подходит» (бытовые вещи И профессиональные, научные,
# медицинские термины), и следом про мелкие детали предмета — «it is necessary to
# learn». Артикль у термина и у дверной рамы учить надо ровно так же: слово есть слово,
# а `der Fenstergriff` понадобится всякому, кто снимал квартиру.
#
# Отсекаем ровно две вещи: то, чего человек в глаза не видел, и то, чего он не знает
# по-русски. Это заметно шире прежней границы — прежде проходил один только «быт».
#
# ⚠ ПРОЧИЕ ФИЛЬТРЫ ПРИ ЭТОМ НА МЕСТЕ, и без них расширение было бы опасным: частотность
# по-прежнему режет «Sturmtiefausläufer», а правило семьи — 48 «домов» на одну тему.
# Этот судья спрашивается ТОЛЬКО про слова, которые частотный список не смог решить сам.
РАЗРЯДЫ_КОТОРЫЕ_ПРОХОДЯТ = {"быт", "термин", "деталь"}
MIN_HEAD_LEN = 4           # корень короче — совпадение случайное («Zirrhose» ≠ «Hose»)
MIN_PREFIX_LEN = 4         # приставка короче — тоже не составное слово
_PLURAL_SUFFIXES = ("en", "e", "n", "er", "s")


@functools.lru_cache(maxsize=1)
def frequency_table() -> dict[str, int]:
    """слово → его место в частотном списке (1 = самое частое)."""
    table: dict[str, int] = {}
    try:
        with open(FREQ_FILE, encoding="utf-8", errors="ignore") as fh:
            for index, line in enumerate(fh, 1):
                parts = line.split()
                if not parts:
                    continue
                word = parts[0].strip().lower()
                if word and word not in table:
                    table[word] = index
    except OSError:
        return {}
    return table


def word_rank(word: str) -> int | None:
    """Частотность слова с оглядкой на множественное число.

    «Kopfschmerz» в списке нет, а «Kopfschmerzen» есть — и это ходовое слово,
    отсекать его нельзя."""
    table = frequency_table()
    low = str(word or "").strip().lower()
    if not low:
        return None
    best = table.get(low)
    for suffix in _PLURAL_SUFFIXES:
        rank = table.get(low + suffix)
        if rank and (best is None or rank < best):
            best = rank
    return best


def head_word(word: str, known_words) -> str | None:
    """Если слово составное и его корень — другое слово темы, вернуть этот корень.

    Требуем осмысленную приставку, а не случайное созвучие: «Zirrhose» не производное
    от «Hose», хотя и заканчивается на неё."""
    low = str(word or "").strip().lower()
    table = frequency_table()
    for head in known_words:
        head = str(head).strip().lower()
        if head == low or len(head) < MIN_HEAD_LEN or not low.endswith(head):
            continue
        prefix = low[: -len(head)]
        if len(prefix) < MIN_PREFIX_LEN:
            continue
        stem = prefix[:-1] if prefix.endswith(("s", "n")) else prefix
        if prefix in table or stem in table or prefix in known_words or stem in known_words:
            return head
    return None


def needs_second_opinion(word: str, *, max_rank: int = DEFAULT_MAX_RANK) -> bool:
    """Слово не прошло по частотности, но может быть обиходным предметом."""
    rank = word_rank(word)
    return rank is None or rank > max_rank


class EverydayJudgeUnavailable(Exception):
    """Ответа от модели не было (сеть, ключ, таймаут).

    «Модель сказала нет» и «ответа не было» — разные вещи. Для приёмки оба означают
    «не берём», но запомнить отказ навечно можно только в первом случае: иначе один
    обрыв сети занёс бы в стоп-лист целую пачку нормальных слов."""


def _judge_everyday_once(words: list[str]) -> dict[str, str]:
    """ОДИН запрос: разложить слова по РАЗРЯДАМ. Возвращает {слово: разряд}.

    ⚠ РАНЬШЕ ЗДЕСЬ СПРАШИВАЛСЯ ПРИГОВОР («стоит ли учить это слово»), и он плыл на
    12% при повторе. Теперь спрашивается разряд — это сортировка, а не взвешивание
    пользы. Какие разряды проходят, модель НЕ решает: это `РАЗРЯДЫ_КОТОРЫЕ_ПРОХОДЯТ`.

    Планка та же, что стояла в прежнем вопросе, слово в слово: границу разбирал
    владелец глазами 31.07.2026 и подтвердил именно её.

    Сбой запроса — не «нет», а EverydayJudgeUnavailable: вызывающий сам решит, что
    делать (в наборе — не берём).
    """
    clean = [str(w).strip() for w in words if str(w or "").strip()]
    if not clean:
        return {}
    prompt = (
        "Ниже немецкие существительные для тренажёра артиклей. Ученик — обычный взрослый "
        "русскоязычный человек, учит немецкий для жизни.\n\n"
        "Для КАЖДОГО слова назови ровно один разряд:\n\n"
        # ⛔ ОБЕ ПОЛОВИНЫ ОПРЕДЕЛЕНИЯ ОБЯЗАТЕЛЬНЫ. Замер 05.09.2026: без второй («и
        # сталкивается с ней») модель поняла «быт» как «нормальное немецкое слово» и
        # пропустила Alm (альпийский луг), Aue (пойма), Laute (лютня), Münster (собор),
        # Pfarrhaus (дом священника) — всё то, что владелец забраковал руками.
        "\"быт\" — вещь, понятие или роль, с которой человек СТАЛКИВАЕТСЯ В СВОЕЙ ЖИЗНИ "
        "и называет её по-русски не задумываясь: Steckdose (розетка), Bohrmaschine "
        "(дрель), Wasserkocher (чайник), Rechnung (счёт), Macht (власть), Richter "
        "(судья), Knochen (кость), Großmutter (бабушка).\n"
        "Знать слово по-русски — МАЛО. «Альпийский луг», «пойма», «лютня», «собор», "
        "«дом священника» русский взрослый понимает, но в жизни с ними не сталкивается: "
        "это НЕ \"быт\".\n\n"
        "\"термин\" — профессиональное, научное, медицинское или техническое понятие, "
        "которое образованный человек знает или встречал: Hygrometer, Synapse, Sepsis, "
        "Zwerchfell (диафрагма), Inventur (инвентаризация), Archäologe (археолог).\n\n"
        "\"экзотика\" — вещь, место или понятие, с которыми человек в своей жизни не "
        "сталкивается, даже если знает слово по-русски. Сюда же ландшафт, природа, "
        "старина, редкие музыкальные инструменты, культовые постройки: Iglu (иглу), "
        "Obelisk (обелиск), Muskatreibe (тёрка для муската), Salatschleuder (центрифуга "
        "для салата), Spaghettiheber (лопатка для спагетти), Stößel (пестик), Alm "
        "(альпийский луг), Aue (пойма), Klamm (ущелье), Laute (лютня), Münster (собор), "
        "Pfarrhaus (дом священника), Amazone (амазонка), Turteltaube (горлица).\n\n"
        "\"деталь\" — часть предмета или помещения: "
        "Fenstergriff (ручка окна), Außenwand (наружная стена), Türrahmen (дверная рама).\n\n"
        "\"незнакомое\" — русский взрослый не знает, что это слово значит ПО-РУССКИ: "
        "Mispel (мушмула), Trepang (трепанг), Reneklode (ренклод), Jostabeere (йошта).\n\n"
        "Не оценивай, стоит ли это учить, — только разложи по разрядам. Не подходит "
        "ни один разряд точно — ставь тот, что ближе. Между \"быт\" и \"термин\" "
        "сомневаешься — это не важно, оба годятся; важно не спутать их с \"экзотикой\", "
        "\"деталью\" и \"незнакомым\".\n\n"
        "Ответь СТРОГИМ JSON: {\"слово\": \"разряд\", ...} — ключи ровно как даны.\n\n"
        + "\n".join("- " + w for w in clean)
    )
    try:
        import json
        import os as _os
        from openai import OpenAI
        client = OpenAI(api_key=_os.getenv("OPENAI_API_KEY"), timeout=90)
        resp = client.chat.completions.create(
            model="gpt-4.1", temperature=0, max_tokens=2000,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
        )
        # ⛔ РАСХОД — В ВЕДОМОСТЬ. Этот судья ходил в OpenAI своим клиентом, мимо учёта:
        # в отчёте по деньгам его не было ни строкой (найдено 04.09.2026).
        try:
            from backend.openai_usage_logging import log_openai_raw_usage
            log_openai_raw_usage(action_type="article_everyday_bucket", model="gpt-4.1",
                                 usage=getattr(resp, "usage", None), user_id=None)
        except Exception:
            pass
        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception as exc:
        import logging
        logging.warning("разряды слов не получены", exc_info=True)
        raise EverydayJudgeUnavailable(str(exc)) from exc
    return {w: str(data.get(w) or "").strip().lower() for w in clean}


def judge_everyday_words(words: list[str], *, votes: int = EVERYDAY_VOTES) -> dict[str, bool]:
    """Годится ли слово в банк артиклей. ОДИН запрос, решение по разряду.

    ⚠ `votes` оставлен в подписи, чтобы не ломать вызовы, но голосования больше нет
    (решение владельца 05.09.2026, см. рамку у EVERYDAY_VOTES). Аргумент игнорируется.

    Проходят разряды `РАЗРЯДЫ_КОТОРЫЕ_ПРОХОДЯТ` — «быт» и «термин». Разряд, которого мы
    не знаем (модель ответила чем-то своим), — это НЕ «да»: неизвестное не пускаем.

    Запрос не дошёл — это не «нет», а EverydayJudgeUnavailable: один обрыв сети не
    должен хоронить пачку нормальных слов.
    """
    clean = [str(w).strip() for w in words if str(w or "").strip()]
    if not clean:
        return {}
    разряды = _judge_everyday_once(clean)
    return {w: разряды.get(w, "") in РАЗРЯДЫ_КОТОРЫЕ_ПРОХОДЯТ for w in clean}


def check_word(
    word: str,
    *,
    meaning_ru: str = "",
    known_words=(),
    known_meanings=(),
    family_counts=None,
    max_rank: int = DEFAULT_MAX_RANK,
) -> tuple[bool, str]:
    """→ (брать ли слово, причина отказа).

    known_words — слова, уже стоящие в теме; family_counts — сколько производных
    каждого корня уже набрано (изменяется на месте, чтобы считать и добавляемые)."""
    clean = str(word or "").strip()
    if not clean:
        return False, "пустое слово"
    known_words = {str(w).strip().lower() for w in known_words}
    if clean.lower() in known_words:
        return False, "уже есть в теме"

    meaning_key = re.sub(r"\s+", " ", str(meaning_ru or "").strip().lower())
    if meaning_key and meaning_key in {str(m).strip().lower() for m in known_meanings}:
        return False, "такой смысл в теме уже есть"

    rank = word_rank(clean)
    if rank is None or rank > max_rank:
        # Частотный список построен на текстах, а не на жизни, и занижает ПРЕДМЕТЫ:
        # «die Spülmaschine» стоит на 33 467 месте, «die Bohrmaschine» на 39 393,
        # «der Wasserkocher» вообще отсутствует — при том что вещи эти есть у каждого.
        # Поэтому за порогом слово не выбрасывается сразу, а уходит на второе мнение.
        return False, "нужно второе мнение"

    head = head_word(clean, known_words)
    if head:
        counts = family_counts if family_counts is not None else {}
        if counts.get(head, 0) >= FAMILY_CAP:
            return False, "третье производное от «%s» — род тот же, учить нечему" % head
        counts[head] = counts.get(head, 0) + 1
    return True, ""
