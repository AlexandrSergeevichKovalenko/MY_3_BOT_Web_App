# -*- coding: utf-8 -*-
"""Насколько слово ходовое в живом немецком. Источник — DWDS.

Зачем этот файл существует
──────────────────────────
Владелец 01.09.2026 получил ребус «Eieruhr» (кухонный таймер для варки яиц) и не
узнал слово. Слово настоящее — оно есть в нашем базовом словаре, — но по корпусу
DWDS оно встречается 120 раз на миллиард, тогда как Bahnhof 45 509 раз. То есть
в 380 раз реже. Банк ребусов накопил такой хвост: из 77 выдаваемых карточек 12
редких, а среди снятых лежат прямые выдумки модели — Geldbeutelverschluss
(застёжка кошелька) 0,0 на миллиард, Steuerflasche с переводом «специальная
бутылка для измерения налога».

Наш оффлайновый список (`backend/data/de_frequency_50k.txt`, 49 311 слов) для этого
не годится: он разговорный и составных существительных почти не знает — Kopfschmerzen
с местом 3877 в нём есть, а в наших словарях нет вовсе. DWDS построен на 53,3 млрд
слов и составные слова знает.

Мера
────
`per_billion` — вхождений на миллиард слов корпуса. Сравнима между словами.
Опоры, посчитанные этой же формулой 01.09.2026:

    Fußball        82 162      Kopfschmerzen  6 121
    Bahnhof        45 509      Bratpfanne       602
    Kühlschrank    15 542      Eieruhr          120
                               Suppenkelle       68

Чего этот источник НЕ умеет
───────────────────────────
DWDS собран из письменных текстов и ЗАНИЖАЕТ бытовые предметы: сковорода 602,
яичница 754, кофейная чашка 917 — вещи есть у каждого. Ровно это же записано в
приёмке кроссворда (`backend/crossword_word_gate.py`) по разбору 31.07.2026.
Поэтому порог у нас невысокий, и поднимать его «чтобы почище» нельзя без замера.

«Не ответил» — это НЕ «ноль»
────────────────────────────
При первом прогоне 69 слов из 338 остались без ответа из-за сети, и среди них были
Bahnhof и Badezimmer. Записать им ноль значило бы оболгать слово. Поэтому неответ
возвращается как None и хранится отдельным состоянием, а не нулём.
"""
from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_API = "https://www.dwds.de/api/frequency/?q="
_UA = "TelegramDeutschBot/1.0 (German learning app; vocabulary quality check)"
_TIMEOUT_SEC = 20

# Размер корпуса DWDS. Приходит в каждом ответе полем `total`; здесь — только для
# пересчёта уже сохранённых записей, у которых своё `total` лежит рядом.
DWDS_CORPUS_TOTAL = 53_303_287_841


def per_billion(hits: int, total: int = DWDS_CORPUS_TOTAL) -> float:
    """Вхождений на миллиард слов корпуса.

    Битый размер корпуса (0, None, отрицательный) — это ПОЛОМКА ответа, а не повод
    подставить размер по умолчанию: подстановка дала бы правдоподобное число из
    ничего. Падаем честно.
    """
    if total is None or int(total) <= 0:
        raise ValueError(f"размер корпуса должен быть положительным, пришло {total!r}")
    return int(hits or 0) / int(total) * 1e9


def ask_dwds(word: str) -> dict | None:
    """Спросить DWDS про одно слово. None — НЕ СПРОСИЛИ (сеть, таймаут, кривой ответ).

    Отличать это от «слово редкое» обязательно: ноль вхождений — это ответ,
    а отсутствие ответа — незакрытая задача.
    """
    word = str(word or "").strip()
    if not word:
        return None
    try:
        req = urllib.request.Request(_API + urllib.parse.quote(word),
                                     headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SEC) as resp:
            data = json.load(resp)
        # Оба числа берём из ответа как есть. Подставить свой размер корпуса вместо
        # пришедшего значило бы посчитать частоту не по тем данным, которые нам
        # прислали, — и не заметить, что ответ битый.
        hits = int(data.get("hits"))
        total = int(data.get("total"))
    except Exception:
        logger.warning("dwds: не ответил про «%s»", word, exc_info=True)
        return None
    return {"word": word, "hits": hits, "total": total,
            "band": int(data.get("frequency") or 0),
            "lemma": str(data.get("lemma") or ""),
            "per_billion": per_billion(hits, total)}


def word_per_billion(word: str, *, allow_network: bool = True) -> float | None:
    """Частота слова: сперва из нашего кеша, потом (если разрешено) у DWDS.

    None — «не знаем». Вызывающая сторона ОБЯЗАНА обработать это как отдельный
    случай, а не как ноль и не как «годится».
    """
    from backend.database import get_dwds_frequency, upsert_dwds_frequency

    cached = get_dwds_frequency(word)
    if cached is not None:
        return cached
    if not allow_network:
        return None
    fresh = ask_dwds(word)
    if fresh is None:
        return None
    upsert_dwds_frequency(fresh)
    return fresh["per_billion"]
