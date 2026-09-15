# -*- coding: utf-8 -*-
"""Склейка «катящихся» субтитров YouTube — ОДИН экземпляр правила на всю систему.

Зачем это отдельный модуль (владелец, 15.09.2026: «русские субтитры не совпадают с
немецкими, то отстают, то идут вперёд»).

Автоматические субтитры YouTube — «катящаяся лента»: одна и та же фраза приезжает
2–3 кадрами подряд, пока ползёт вверх по экрану. Если показать их как есть, каждая
строка задвоится. Поэтому кадры склеиваются: полный повтор выбрасывается, частичный
обрезается до нового хвоста.

Ровно здесь и рождался сдвиг русского. Склейка жила ТОЛЬКО в браузере
(`frontend/src/App.jsx`, `derollTranscriptCues`), поэтому в системе было ДВА разных
пространства номеров одной и той же реплики:

    сырые кадры из YouTube   0 1 2 3 4 5 6 7 8     ← что лежит в базе в items
    склеенные строки          0   1   2   3   4    ← что видит человек и под какими
                                                      номерами сохраняется перевод

Браузер честно перекладывал номера один раз — при первом просмотре. Но перевод из базы
приходит УЖЕ в склеенном пространстве, а браузер перекладывал его ЕЩЁ РАЗ, как сырой.
На втором просмотре того же ролика русский уезжал вперёд и обрывался. Проверено
прогоном на нашей же функции (`backend/tests/test_subtitle_cues.py`).

Лечится не «поправкой в браузере», а устранением второго пространства: склейка
переезжает на сервер, в базе лежат уже склеенные реплики, номер у реплики один на всю
систему — от YouTube до экрана. Этот модуль — то самое единственное место.

Правило перенесено из `frontend/src/App.jsx` дословно; расхождение двух реализаций
проверяется тестом на общем наборе случаев, потому что расхождение здесь означает
ровно тот же сдвиг, который мы чиним.
"""
from __future__ import annotations


def _norm_word(word: str) -> str:
    """Слово без регистра и без внешней пунктуации.

    Соответствует `_derollNormWord` в App.jsx: `toLowerCase` + срез небуквенно-нецифровых
    символов с начала и с конца. Сравнение кадров идёт по этим формам, иначе «nein.» и
    «nein» считались бы разными словами и повтор не находился бы.
    """
    text = str(word or "").lower()
    start = 0
    end = len(text)
    while start < end and not text[start].isalnum():
        start += 1
    while end > start and not text[end - 1].isalnum():
        end -= 1
    return text[start:end]


def _cue_text(item) -> str | None:
    """Текст реплики со схлопнутыми пробелами. None — если реплика не похожа на реплику
    (не словарь или без поля text); такие кадры проходят насквозь, как в браузере."""
    if not isinstance(item, dict):
        return None
    raw = item.get("text")
    if raw is None:
        return None
    return " ".join(str(raw).split())


def deroll_transcript_cues(items: list) -> tuple[list, dict[int, int]]:
    """Склеивает «катящиеся» кадры в строки.

    Возвращает (строки, карта «номер кадра → номер строки»). Карта нужна ровно одному
    месту — разовому переносу уже накопленного перевода со старых номеров на новые; в
    обычной работе её применять НЕЛЬЗЯ, потому что второе применение и есть тот сдвиг,
    из-за которого всё затевалось.

    Ошибок не глушит и ничего не додумывает: кадр, который не удалось разобрать,
    проходит насквозь неизменным и занимает свой номер.
    """
    out: list = []
    index_map: dict[int, int] = {}

    for old_idx, item in enumerate(items or []):
        text = _cue_text(item)

        # Не реплика — пропускаем насквозь, номер за ней сохраняется.
        if text is None:
            index_map[old_idx] = len(out)
            out.append(item)
            continue

        # Пустой кадр: своей строки не заводит, прилипает к предыдущей. Пустой кадр в
        # самом начале не прилипать не к чему — он не получает номера вовсе.
        if not text:
            if out:
                index_map[old_idx] = len(out) - 1
            continue

        if not out:
            index_map[old_idx] = len(out)
            out.append({**item, "text": text})
            continue

        prev = out[-1]
        prev_words = str(prev.get("text") or "").split() if isinstance(prev, dict) else []
        cur_words = text.split()
        prev_norm = [_norm_word(w) for w in prev_words]
        cur_norm = [_norm_word(w) for w in cur_words]

        # Самое длинное совпадение «хвост предыдущего кадра = начало текущего».
        overlap = 0
        for k in range(min(len(prev_words), len(cur_words)), 0, -1):
            if prev_norm[len(prev_norm) - k:] == cur_norm[:k]:
                overlap = k
                break

        # Кадр целиком повторяет хвост предыдущего — это тот же текст, проехавший вверх.
        # Выбрасываем, а время предыдущей строки растягиваем до конца этого кадра, чтобы
        # перемотка попадала в нужный момент.
        if overlap == len(cur_words):
            if isinstance(prev, dict):
                prev_start = _as_float(prev.get("start"))
                cur_end = _as_float(item.get("start")) + max(0.0, _as_float(item.get("duration")))
                if cur_end > prev_start:
                    prev["duration"] = max(_as_float(prev.get("duration")), cur_end - prev_start)
            index_map[old_idx] = len(out) - 1
            continue

        # Частичный наезд в два слова и больше — оставляем только новый хвост. Совпадение
        # в одно слово не трогаем: это может быть простое совпадение, а не прокрутка.
        if overlap >= 2:
            remainder = " ".join(cur_words[overlap:]).strip()
            if remainder:
                index_map[old_idx] = len(out)
                out.append({**item, "text": remainder})
            else:
                index_map[old_idx] = len(out) - 1
            continue

        index_map[old_idx] = len(out)
        out.append({**item, "text": text})

    return out, index_map


def _as_float(value) -> float:
    """Число или 0.0. Время у кадра приходит из чужого источника и бывает пустым; ноль
    здесь — не «мы не знаем», а «начало ролика», и он используется только для растяжки
    длительности, где неверный ноль ничего не подменяет в тексте."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def cues_are_rolling(items: list) -> bool:
    """Есть ли в дорожке «катящиеся» повторы, то есть схлопывает ли её склейка."""
    rolled, _ = deroll_transcript_cues(items)
    return len(rolled) < len(items or [])


def split_translation_key(key: str) -> tuple[str, int] | None:
    """Ключ перевода → (язык, номер реплики).

    Ключи двух видов: новый «ru:17» и старый «17» (только русский — так писали до
    появления других языков, и читатель в backend_server.py до сих пор так и читает).
    Не разобрался — возвращаем None: непонятный ключ не притворяется нулевым номером.

    Живёт рядом со склейкой, потому что отвечает на тот же вопрос — «какой номер у этой
    строки», — и второй реализации у него быть не должно.
    """
    raw = str(key or "").strip()
    if not raw:
        return None
    if ":" in raw:
        lang, _, idx = raw.partition(":")
        lang = lang.strip().lower()
        idx = idx.strip()
    else:
        lang, idx = "ru", raw
    if not lang or not idx.isdigit():
        return None
    return lang, int(idx)
