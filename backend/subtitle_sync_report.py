# -*- coding: utf-8 -*-
"""Сколько роликов в базе поражено сдвигом русских субтитров — числом, по классам.

Повод (владелец, 15.09.2026): «русские субтитры не синхронно идут с немецкими, то
отстают, то вперёд идут. Это ошибка в реализации или связь?». Ответ — реализация; чтобы
решать по числу, а не по рассказу, нужен счётчик. Это он: `/subtitry` в боте.

── Как вообще держится соответствие ───────────────────────────────────────────
Русского ВРЕМЕНИ у нас нет вообще. Есть немецкие реплики с временем (`items`) и русские
строки под номерами (`translations`: `{"ru:17": "…"}`). Плеер подсвечивает немецкую
строку по времени и берёт русскую с тем же номером. Всё держится на совпадении номеров.

── Три класса, которые этот отчёт считает раздельно ───────────────────────────
Числа НЕ складываются в одно: у них разные причины и разная цена починки.

1. «Ждут склейки» — дорожка ещё лежит сырыми кадрами (`cues_rolled = FALSE`). До
   15.09.2026 склейка «катящихся» кадров жила в браузере, из-за чего у реплики было два
   номера и русский уезжал вперёд при повторном открытии ролика. Теперь склейка на
   сервере и выполняется один раз: дорожка чинится сама при первом открытии, остальные
   добирает ночь. Число — это остаток работы, а не поломка на экране.

2. «Номера за концом списка» — немецкие реплики заменили (их умеют забирать из четырёх
   источников, и режут они по-разному), а перевод остался старый: запрос к базе так и
   написан — `COALESCE(EXCLUDED.translations, старое)`. Прямой признак: номер перевода
   больше, чем строк в немецком списке.

3. «Дырки в переводе» — номер внутри переведённого куска пуст. Класс СМЕШАННЫЙ, и в
   отчёте он так и назван: сюда попадают и обрыв связи, и пустая строка от модели, и
   недосчитанная пачка. Разделить их по тому, что лежит в базе, нельзя, поэтому:
     • «модель вернула пустое» отделено — у такого номера ключ есть, а текст пустой;
     • остальное честно названо «дырка», без приписывания причины.

┌─ ПРОВЕРЕНО 15.09.2026. НЕ ПОДНИМАТЬ КАК НОВУЮ НАХОДКУ. ───────────────────────┐
│ Сдвиг из класса «модель вернула не столько строк, сколько прислали» (пачка в    │
│ 30 реплик раскладывается `zip`-ом по порядку) СЛЕДА В ДАННЫХ НЕ ОСТАВЛЯЕТ:      │
│ строки на месте, просто не те. По базе его посчитать нельзя — можно только      │
│ ловить в момент запроса. Поэтому он считается счётчиком на входе, а не здесь;   │
│ в отчёте для него отдельная строка «поймано на лету». Не искать его в базе      │
│ заново — там его нет.                                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import logging

from backend.subtitle_cues import split_translation_key

logger = logging.getLogger(__name__)


def _by_language(translations: dict) -> dict[str, dict[int, str]]:
    """Плоская карта ключей → {язык: {номер: текст}}. Русский лежит дважды («ru:17» и
    «17») — это одна и та же строка, и считать её надо один раз."""
    out: dict[str, dict[int, str]] = {}
    for key, value in (translations or {}).items():
        parsed = split_translation_key(key)
        if parsed is None:
            continue
        lang, idx = parsed
        out.setdefault(lang, {})[idx] = str(value or "")
    return out


def audit_one_video(row: dict) -> dict:
    """Разбор одной дорожки по классам. Чистая функция — её же гоняет тест.

    Ключевой вопрос для каждой строки: в каком пространстве номеров она лежит. Ответ даёт
    флаг `cues_rolled`, а не догадка по содержимому: склейка не идемпотентна, и «склеить
    ещё раз, чтобы проверить» — это ровно тот сдвиг, который мы чиним.
    """
    from backend.subtitle_cues import deroll_transcript_cues

    items = row.get("items") or []
    already_rolled = bool(row.get("cues_rolled"))
    if already_rolled:
        # Номера окончательные: сравниваем перевод с тем, что лежит.
        effective = items
        pending_roll = False
    else:
        # Дорожка ещё сырая. Склейка случится при первом открытии ролика или ночью;
        # считаем то, чем она станет.
        effective, _index_map = deroll_transcript_cues(items)
        pending_roll = True
    rolled_count = len(effective)

    languages = _by_language(row.get("translations") or {})
    total_lines = 0
    orphan = 0            # номер за концом немецкого списка
    empty_from_model = 0  # ключ есть, текст пустой
    gaps = 0              # номера нет внутри переведённого куска

    for _lang, by_index in languages.items():
        if not by_index:
            continue
        total_lines += len(by_index)
        # Граница «переведённого куска» берётся ТОЛЬКО по номерам, которые попадают в
        # нынешний немецкий список. Иначе один осиротевший номер (класс 2) растягивает
        # кусок до конца и добавляет к нему десятки «дырок» (класс 3) — то есть одна
        # поломка посчиталась бы дважды в двух разных классах. Поймано тестом
        # test_номер_за_концом_немецкого_списка_это_класс_2.
        in_range = [idx for idx in by_index if idx < rolled_count]
        highest = max(in_range) if in_range else -1
        for idx, text in by_index.items():
            if idx >= rolled_count:
                orphan += 1
                continue
            if not str(text).strip():
                empty_from_model += 1
        # Дырка — только ВНУТРИ уже переведённого куска: то, что дальше по ролику ещё не
        # заказывали, дыркой не является и в число не идёт.
        for idx in range(min(highest + 1, rolled_count)):
            if idx in by_index:
                continue
            cue = effective[idx]
            if isinstance(cue, dict) and str(cue.get("text") or "").strip():
                gaps += 1

    return {
        "video_id": row.get("video_id") or "",
        "is_generated": bool(row.get("is_generated")),
        "cues_stored": len(items),
        "cues_effective": rolled_count,
        "pending_roll": pending_roll,
        "languages": sorted(languages.keys()),
        "lines": total_lines,
        "orphan": orphan,
        "empty_from_model": empty_from_model,
        "gaps": gaps,
    }


def subtitle_sync_state() -> dict:
    """Пройти по всем дорожкам и сложить числа по классам.

    Ошибки не глушим: если база не ответила, отчёт обязан упасть, а не показать нули —
    «ноль поражённых» и «не смогли посчитать» это два разных мира.
    """
    from backend.database import iter_youtube_transcripts_for_audit

    videos = 0
    videos_with_translation = 0
    videos_pending_roll = 0
    videos_orphan = 0
    videos_gaps = 0
    lines_total = 0
    lines_orphan = 0
    lines_empty = 0
    lines_gaps = 0
    lines_lost_on_roll = 0
    worst: list[dict] = []

    for row in iter_youtube_transcripts_for_audit():
        videos += 1
        report = audit_one_video(row)
        if report["pending_roll"]:
            videos_pending_roll += 1
        if report["lines"] <= 0:
            continue
        videos_with_translation += 1
        lines_total += report["lines"]
        lines_empty += report["empty_from_model"]
        lines_gaps += report["gaps"]
        if report["gaps"]:
            videos_gaps += 1
        # Номер за концом списка значит РАЗНОЕ у склеенной и у ещё не склеенной дорожки,
        # поэтому и считается в разные классы. У склеенной — немецкие реплики заменили
        # (класс 2). У несклеенной — перевод писался до 12.07.2026, под сырыми номерами;
        # перенести его некуда, и при склейке он потеряется (класс 1).
        if report["pending_roll"]:
            lines_lost_on_roll += report["orphan"]
        else:
            lines_orphan += report["orphan"]
            if report["orphan"]:
                videos_orphan += 1
        if report["orphan"]:
            worst.append({"video_id": report["video_id"], "damage": report["orphan"],
                          "lines": report["lines"]})

    worst.sort(key=lambda item: item["damage"], reverse=True)
    return {
        "videos": videos,
        "videos_with_translation": videos_with_translation,
        "videos_pending_roll": videos_pending_roll,
        "videos_orphan": videos_orphan,
        "videos_gaps": videos_gaps,
        "lines_total": lines_total,
        "lines_orphan": lines_orphan,
        "lines_empty": lines_empty,
        "lines_gaps": lines_gaps,
        "lines_lost_on_roll": lines_lost_on_roll,
        "worst": worst[:5],
        "count_mismatch": _count_mismatch_snapshot(),
    }


def _count_mismatch_snapshot() -> dict:
    """Класс «модель вернула не столько строк» — ловится только на лету, счётчиком на
    входе. Пока счётчик не поставлен, отчёт говорит об этом словами, а не нулём: ноль
    здесь означал бы «проверено, чисто», а это неправда."""
    try:
        from backend.subtitle_translate_counter import mismatch_counters
    except ImportError:
        return {"measured": False}
    return {"measured": True, **mismatch_counters()}


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Русское согласование числа: «1 ролик», «2 ролика», «5 роликов». Отчёт читает
    человек, и «1 строк у 1 ролик» читается как небрежность к нему."""
    tail = n % 100
    if 11 <= tail <= 14:
        return many
    last = n % 10
    if last == 1:
        return one
    if last in (2, 3, 4):
        return few
    return many


def _videos(n: int) -> str:
    return f"{n} " + _plural(n, "ролика", "роликов", "роликов")


def _lines(n: int) -> str:
    return f"{n} " + _plural(n, "строка", "строки", "строк")


def format_subtitle_sync_report(state: dict) -> str:
    """Человеческий текст: взглянул — понял — знаешь, что делать."""
    videos = int(state.get("videos") or 0)
    with_tr = int(state.get("videos_with_translation") or 0)
    lines_total = int(state.get("lines_total") or 0)

    lines = ["🎬 <b>Русские субтитры: сходятся ли номера с немецкими</b>", ""]

    if not videos:
        lines.append("В базе нет ни одной дорожки субтитров — считать нечего.")
        return "\n".join(lines)
    if not with_tr:
        lines += [
            f"Дорожек в базе: <b>{videos}</b>, из них с русским переводом: <b>0</b>.",
            "Сдвигаться нечему.",
        ]
        return "\n".join(lines)

    pending = int(state.get("videos_pending_roll") or 0)
    lost = int(state.get("lines_lost_on_roll") or 0)
    orphan = int(state.get("lines_orphan") or 0)
    gaps = int(state.get("lines_gaps") or 0)
    empty = int(state.get("lines_empty") or 0)

    if pending == 0 and orphan == 0:
        lines.append("✅ Номера сходятся у всех переведённых дорожек.")
    else:
        lines.append("⚠️ Есть дорожки, где номера ещё не в порядке. Подробности ниже.")

    lines += [
        "",
        f"<b>1. Ждут склейки:</b> {_videos(pending)} из {videos}"
        + (f", при склейке потеряется {_lines(lost)} перевода" if lost else ""),
        "<i>склейка «катящихся» кадров переехала на сервер; дорожка чинится сама при первом "
        "открытии ролика, остальные добирает ночь. Строки теряются только у записей до "
        "12.07.2026 — их перевод писался под другими номерами, и переносить его некуда</i>",
        "",
        f"<b>2. Номера за концом немецкого списка:</b> {_lines(orphan)} "
        f"у {_videos(int(state.get('videos_orphan') or 0))}",
        "<i>немецкие реплики заменили, русские к ним остались старые</i>",
        "",
        f"<b>3. Дырки в переводе:</b> {_lines(gaps)} "
        f"у {_videos(int(state.get('videos_gaps') or 0))}"
        + (f", из них модель вернула пустое: {empty}" if empty else ""),
        "<i>класс смешанный: обрыв связи, пустой ответ модели, недосчитанная пачка</i>",
    ]

    mismatch = state.get("count_mismatch") or {}
    lines += [""]
    if not mismatch.get("measured"):
        lines.append(
            "<b>4. Модель вернула не столько строк, сколько прислали:</b> счётчик ещё не "
            "поставлен. В базе этот сдвиг следа не оставляет — строки на месте, просто не "
            "те, — поэтому ловится только на входе."
        )
    else:
        lines.append(
            f"<b>4. Модель вернула не столько строк:</b> {mismatch.get('mismatched', 0)} пачек "
            f"из {mismatch.get('total', 0)} за всё время наблюдения."
        )

    worst = state.get("worst") or []
    if worst:
        lines += ["", "Сильнее всего задеты:"]
        for item in worst:
            lines.append(
                f"  • <code>{item['video_id']}</code> — {_lines(item['damage'])} "
                f"из {item['lines']}"
            )

    lines += [
        "",
        f"<i>Всего дорожек: {videos}, с переводом: {with_tr} ({lines_total} строк). "
        f"К YouTube и к модели отчёт не обращается.</i>",
    ]
    return "\n".join(lines)
