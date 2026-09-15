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

1. «Уедет на повторном просмотре» — дорожка «катящаяся» (см. backend/subtitle_cues.py),
   в базе лежат СЫРЫЕ кадры, а перевод сохранён под номерами СКЛЕЕННЫХ строк. Браузер
   при открытии перекладывает номера ещё раз — и русский уезжает вперёд. Прямой признак
   в данных: номер перевода, который склейка переносит на другую строку.

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

logger = logging.getLogger(__name__)


def _split_translation_key(key: str) -> tuple[str, int] | None:
    """Ключ перевода → (язык, номер реплики).

    Ключи двух видов: новый «ru:17» и старый «17» (только русский — так писали до
    появления других языков, и читатель в backend_server.py до сих пор так и читает).
    Не разобрался — возвращаем None: непонятный ключ не притворяется нулевым номером.
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


def _by_language(translations: dict) -> dict[str, dict[int, str]]:
    """Плоская карта ключей → {язык: {номер: текст}}. Русский лежит дважды («ru:17» и
    «17») — это одна и та же строка, и считать её надо один раз."""
    out: dict[str, dict[int, str]] = {}
    for key, value in (translations or {}).items():
        parsed = _split_translation_key(key)
        if parsed is None:
            continue
        lang, idx = parsed
        out.setdefault(lang, {})[idx] = str(value or "")
    return out


def audit_one_video(row: dict) -> dict:
    """Разбор одной дорожки по трём классам. Чистая функция — её же гоняет тест."""
    from backend.subtitle_cues import deroll_transcript_cues

    items = row.get("items") or []
    rolled, index_map = deroll_transcript_cues(items)
    rolled_count = len(rolled)
    rolling = rolled_count < len(items)

    languages = _by_language(row.get("translations") or {})
    total_lines = 0
    will_shift = 0        # класс 1: склейка перенесёт строку на другую реплику
    will_vanish = 0       # класс 1: строка пропадёт вовсе (номера нет в карте)
    orphan = 0            # класс 2: номер за концом немецкого списка
    empty_from_model = 0  # класс 3а: ключ есть, текст пустой
    gaps = 0              # класс 3б: номера нет внутри переведённого куска

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
            if rolling:
                target = index_map.get(idx)
                if target is None:
                    will_vanish += 1
                elif target != idx:
                    will_shift += 1
        # Дырка — только ВНУТРИ уже переведённого куска: то, что дальше по ролику ещё не
        # заказывали, дыркой не является и в число не идёт.
        for idx in range(min(highest + 1, rolled_count)):
            if idx in by_index:
                continue
            cue = rolled[idx]
            if isinstance(cue, dict) and str(cue.get("text") or "").strip():
                gaps += 1

    return {
        "video_id": row.get("video_id") or "",
        "is_generated": bool(row.get("is_generated")),
        "cues_raw": len(items),
        "cues_rolled": rolled_count,
        "rolling": rolling,
        "languages": sorted(languages.keys()),
        "lines": total_lines,
        "will_shift": will_shift,
        "will_vanish": will_vanish,
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
    videos_rolling = 0
    videos_shift = 0
    videos_orphan = 0
    videos_gaps = 0
    lines_total = 0
    lines_shift = 0
    lines_vanish = 0
    lines_orphan = 0
    lines_empty = 0
    lines_gaps = 0
    worst: list[dict] = []

    for row in iter_youtube_transcripts_for_audit():
        videos += 1
        report = audit_one_video(row)
        if report["rolling"]:
            videos_rolling += 1
        if report["lines"] <= 0:
            continue
        videos_with_translation += 1
        lines_total += report["lines"]
        lines_shift += report["will_shift"]
        lines_vanish += report["will_vanish"]
        lines_orphan += report["orphan"]
        lines_empty += report["empty_from_model"]
        lines_gaps += report["gaps"]
        if report["will_shift"] or report["will_vanish"]:
            videos_shift += 1
        if report["orphan"]:
            videos_orphan += 1
        if report["gaps"]:
            videos_gaps += 1
        damage = report["will_shift"] + report["will_vanish"] + report["orphan"]
        if damage:
            worst.append({"video_id": report["video_id"], "damage": damage,
                          "lines": report["lines"]})

    worst.sort(key=lambda item: item["damage"], reverse=True)
    return {
        "videos": videos,
        "videos_with_translation": videos_with_translation,
        "videos_rolling": videos_rolling,
        "videos_shift": videos_shift,
        "videos_orphan": videos_orphan,
        "videos_gaps": videos_gaps,
        "lines_total": lines_total,
        "lines_shift": lines_shift,
        "lines_vanish": lines_vanish,
        "lines_orphan": lines_orphan,
        "lines_empty": lines_empty,
        "lines_gaps": lines_gaps,
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

    shift = int(state.get("lines_shift") or 0)
    vanish = int(state.get("lines_vanish") or 0)
    orphan = int(state.get("lines_orphan") or 0)
    gaps = int(state.get("lines_gaps") or 0)
    empty = int(state.get("lines_empty") or 0)
    broken_videos = int(state.get("videos_shift") or 0) + int(state.get("videos_orphan") or 0)

    if shift + vanish + orphan == 0:
        lines.append("✅ Съехавших номеров не нашлось.")
    else:
        lines.append(
            f"⚠️ Номера съехали у <b>{_videos(broken_videos)}</b> из {with_tr} переведённых."
        )
    lines += [
        "",
        f"<b>1. Уедет на повторном просмотре:</b> {_lines(shift + vanish)} "
        f"у {_videos(int(state.get('videos_shift') or 0))}",
        "<i>дорожка «катящаяся», перевод сохранён под склеенными номерами, а браузер "
        "перекладывает их ещё раз</i>",
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
        f"«Катящихся» дорожек: {state.get('videos_rolling', 0)}. "
        f"К YouTube и к модели отчёт не обращается.</i>",
    ]
    return "\n".join(lines)
