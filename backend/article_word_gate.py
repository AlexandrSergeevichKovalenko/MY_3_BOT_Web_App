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
# Сколько раз спрашиваем модель об одном и том же. Один голос давал 12% разнобоя на
# повторе (замер 16.08.2026) — слово попадало в игру или в карантин по сути жребием.
EVERYDAY_VOTES = max(1, int((os.getenv("EVERYDAY_JUDGE_VOTES") or "3").strip() or "3"))
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


def _judge_everyday_once(words: list[str]) -> dict[str, bool]:
    """ОДИН голос модели: нужны ли эти слова человеку в быту.

    Второе мнение нужно там, где частотность подводит: предметы, техника, инструменты,
    геометрия. Одним запросом на всю пачку — дёшево. Сбой запроса — не «нет», а
    EverydayJudgeUnavailable: вызывающий сам решит, что делать (в наборе — не берём).

    Планка тут стоит от лица РУССКОЯЗЫЧНОГО ученика, а не от лица немецкого корпуса, и
    это принципиально. Прежняя формулировка спрашивала просто «встречается ли слово в
    обычной жизни» — и пропускала погружной блендер, тёрку для муската и лопатку для
    спагетти: предметы вроде бы бытовые, а по-русски их никто так не называет. Владелец
    31.07 разобрал словник глазами и подтвердил именно эту границу: слово нужно, если
    человек знает его ПО-РУССКИ и вещь у него была.

    Один голос неустойчив — наружу ходить только через judge_everyday_words."""
    clean = [str(w).strip() for w in words if str(w or "").strip()]
    if not clean:
        return {}
    prompt = (
        "Ниже немецкие существительные для тренажёра артиклей. Ученик — обычный взрослый "
        "русскоязычный человек, учит немецкий для жизни. Он не повар, не биолог, "
        "не строитель и не музыкант.\n\n"
        "Для каждого ответь, стоит ли ему учить артикль этого слова.\n"
        "ДА — обычный человек знает эту вещь или понятие ПО-РУССКИ и сталкивается с ней: "
        "Steckdose (розетка), Bohrmaschine (дрель), Wasserkocher (чайник), "
        "Rechnung (счёт), Macht (власть), Richter (судья), Knochen (кость).\n"
        "НЕТ — если верно хотя бы одно:\n"
        "  1) русский взрослый не знает, что это слово значит по-русски: "
        "Mispel (мушмула), Trepang (трепанг), Reneklode (ренклод), Jostabeere (йошта);\n"
        "  2) вещь экзотическая или он ею никогда не пользовался: Iglu (иглу), "
        "Obelisk (обелиск), Stößel (пестик), Muskatreibe (тёрка для муската), "
        "Salatschleuder (центрифуга для салата), Spaghettiheber (лопатка для спагетти);\n"
        "  3) узкий профессиональный, научный или медицинский термин: Hygrometer, "
        "Synapse, Sepsis, Augenhöhle (глазница), Zwerchfell (диафрагма);\n"
        "  4) мелкая деталь предмета, которую в быту отдельно не называют: "
        "Fenstergriff (ручка окна), Außenwand (наружная стена), Türrahmen (дверная рама).\n\n"
        "Сомневаешься — отвечай НЕТ: спросить артикль вещи, которой человек не видел, "
        "хуже, чем потерять слово.\n\n"
        "Ответь СТРОГИМ JSON: {\"слово\": true|false, ...} — ключи ровно как даны.\n\n"
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
        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception as exc:
        import logging
        logging.warning("второе мнение по словам не получено", exc_info=True)
        raise EverydayJudgeUnavailable(str(exc)) from exc
    return {w: bool(data.get(w, False)) for w in clean}


def judge_everyday_words(words: list[str], *, votes: int = EVERYDAY_VOTES) -> dict[str, bool]:
    """Нужны ли эти слова человеку в быту — БОЛЬШИНСТВОМ из нескольких голосов.

    Зачем не один вопрос. Замер 16.08.2026: один и тот же вопрос про одни и те же слова,
    заданный дважды подряд (модель gpt-4.1, температура 0), дал РАЗНЫЕ ответы по 12%
    слов. Тот же судья, спрошенный заново про 859 слов, которые он сам же пропустил в
    игру, 15% из них теперь выбрасывал (Überfahrt, Inventur, Ebbe, Archäologe, Montage).
    А из 150 отвергнутых им слов 36% теперь получали «да». То есть попадёт слово в игру
    или в карантин — во многом зависело от того, в какой день его спросили. Так в
    карантине и скопилось 454 слова, которые владелец разбирал руками по 10 в день.

    Планка НЕ меняется: в промпте как стояло «сомневаешься — отвечай НЕТ», так и стоит.
    Меняется только устойчивость ответа.

    Большинство считаем от ДОШЕДШИХ голосов, а ничья — это «нет» (та же осторожность,
    что и в промпте). Не дошёл ни один голос — это не «нет», а EverydayJudgeUnavailable:
    один обрыв сети не должен хоронить пачку нормальных слов."""
    clean = [str(w).strip() for w in words if str(w or "").strip()]
    if not clean:
        return {}
    yes: dict[str, int] = {w: 0 for w in clean}
    heard = 0
    last_exc: Exception | None = None
    for _ in range(max(1, int(votes))):
        try:
            verdict = _judge_everyday_once(clean)
        except EverydayJudgeUnavailable as exc:
            last_exc = exc
            continue
        heard += 1
        for word, ok in verdict.items():
            if ok and word in yes:
                yes[word] += 1
    if not heard:
        raise EverydayJudgeUnavailable(str(last_exc or "нет ни одного голоса"))
    if heard < max(1, int(votes)):
        import logging
        logging.warning("второе мнение: дошло %s голосов из %s", heard, votes)
    return {w: yes[w] * 2 > heard for w in clean}


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
