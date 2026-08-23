"""Полка стендапов: ролики отбираются заранее и лежат готовыми к выпуску.

Решение владельца 21.08.2026. Повод: в этот день один придушенный по частоте ключ
YouTube оставил рубрику без ролика, а накануне сутки не скачивались субтитры из-за
блокировки адреса. Обе поломки случились В МОМЕНТ, когда рубрика была нужна.

Рассуждение простое. Новостям свежесть необходима — новость двухнедельной давности не
новость. Стендапу свежесть не нужна ВООБЩЕ: номер, снятый три года назад, сегодня ровно
так же смешон и так же полезен для языка. Значит ежедневный поход в YouTube не покупает
нам ничего, а платим мы за него полной зависимостью от чужой сети в худший момент.

Поэтому ролики отбираются заранее, пачкой, и ложатся на полку ВМЕСТЕ С ТЕКСТОМ СУБТИТРОВ.
Подготовка выпуска берёт готовое: ни YouTube, ни скачивания субтитров. Остаётся только
запрос к модели за разбором — его заранее делать нельзя, задание модели ещё меняется, и
заготовленные разборы устарели бы.

Пополнение — редкая фоновая работа: пока на полке хватает роликов, в сеть не ходим совсем.

── Порядок отбора (решение владельца 21.08.2026) ──────────────────────────────
1. Есть ли субтитры, положенные РУКАМИ. Машинная расшифровка идёт без знаков препинания
   и угадывает слова на слух, а человек субтитры читает и заучивает.
2. Длительность в нужном окне.
3. И только потом популярность. Она стоит последней осознанно: двухмиллионный просмотрами
   номер запросто окажется тяжёлым диалектом под ор зала — смешно немцу, бесполезно
   учащемуся. Популярность — хороший признак, но не первый.
"""
from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name) or "").strip() or default)
    except Exception:
        return default


def shelf_target() -> int:
    """Сколько непоказанных роликов держим на полке. 30 — это два месяца вещания через
    день: с таким запасом недоступность YouTube перестаёт быть событием."""
    return _env_int("STANDUP_SHELF_TARGET", 30)


def refill_standup_shelf(*, target: int | None = None, max_add: int | None = None,
                         budget_sec: int | None = None) -> dict:
    """Дополнить полку до нужного числа роликов.

    Пока непоказанных хватает — в сеть НЕ ходим вообще, это главный смысл полки.
    Кладём только те ролики, у которых субтитры реально скачались и оказались достаточной
    длины: ролик без субтитров на полке — отложенная поломка, а не запас.

    Возвращает отчёт о том, что произошло: сколько было, сколько добавлено и почему
    остальные не подошли. Числа уходят владельцу — молчащее пополнение неотличимо от
    сломанного.
    """
    from backend.daily_video_rubrics import STANDUP_PROFILE
    from backend.database import (
        get_shown_daily_video_ids, put_on_standup_shelf, standup_shelf_counts,
        standup_shelf_video_ids,
    )
    from backend.world_news_generator import (
        WORLD_NEWS_MAX_TRANSCRIPT_CHARS, WORLD_NEWS_MIN_TRANSCRIPT_CHARS,
        _fetch_transcript, _gather_candidates, _transcript_to_text, _yt_api_video_details,
    )

    # Бюджет по часам, а не по числу роликов. Скачивание субтитров имеет повторы, но НЕ
    # имеет жёсткого таймаута сокета (см. инцидент 10.07.2026): один заблокированный адрес
    # заставляет цикл ползти минутами, и вызвавший команду видит зависшее сообщение.
    # Дошли до предела — оставляем то, что успели положить, и честно об этом говорим.
    budget_sec = int(budget_sec) if budget_sec else _env_int("STANDUP_SHELF_BUDGET_SEC", 150)
    started = time.monotonic()

    want = int(target or shelf_target())
    counts = standup_shelf_counts()
    report = {"had_unused": counts["unused"], "target": want, "added": 0,
              "no_transcript": 0, "short_transcript": 0, "dur_skipped": 0, "swept": 0}

    if counts["unused"] >= want:
        report["reason"] = "полка полна — в YouTube не ходили"
        logger.info("standup shelf: полка полна (%d ≥ %d) — сеть не трогаем",
                    counts["unused"], want)
        return report

    candidates = _gather_candidates(STANDUP_PROFILE)
    report["swept"] = len(candidates)
    if not candidates:
        # Пусто может быть по двум причинам: сторож не пустил из-за квоты или YouTube не
        # ответил. Обе уже записаны в лог теми, кто их обнаружил; здесь честно говорим,
        # что добавить нечего, и НЕ притворяемся, будто пополнили.
        report["reason"] = "кандидатов не получено (квота или сеть)"
        return report

    details = _yt_api_video_details([c["video_id"] for c in candidates])
    on_shelf = standup_shelf_video_ids()
    shown = get_shown_daily_video_ids(STANDUP_PROFILE.key)

    ranked = []
    for cand in candidates:
        vid = cand["video_id"]
        if vid in on_shelf or vid in shown:
            continue
        det = details.get(vid) or {}
        dur = int(det.get("duration_seconds") or 0)
        if not dur or not (STANDUP_PROFILE.min_seconds <= dur <= STANDUP_PROFILE.max_seconds):
            report["dur_skipped"] += 1
            continue
        ranked.append({
            "video_id": vid,
            "title": det.get("title") or cand.get("title") or "",
            "channel_title": det.get("channel_title") or cand.get("channel_title") or "",
            "duration_seconds": dur,
            "has_manual_captions": bool(det.get("has_manual_captions")),
            "view_count": det.get("view_count"),
        })

    # Порядок отбора: ручные субтитры → просмотры. Длительность уже отфильтрована выше.
    ranked.sort(key=lambda r: (0 if r["has_manual_captions"] else 1,
                               -(r["view_count"] or 0)))

    # СНИМОК ПУЛА пишется ЗДЕСЬ, потому что обход каналов происходит именно здесь.
    # Раньше он писался в подготовке выпуска — но с появлением полки выпуск берёт готовое
    # и каналы не обходит вовсе, поэтому снимок не писался никогда, и еженедельный отчёт
    # показывал владельцу «чем пополнять: 0 годных роликов» при сотнях доступных
    # (замечено владельцем 23.08.2026).
    in_range_total = report["swept"] - report["dur_skipped"]
    try:
        from backend.database import upsert_daily_video_pool_snapshot
        upsert_daily_video_pool_snapshot(
            rubric=STANDUP_PROFILE.key,
            scanned=report["swept"],
            in_range=max(0, in_range_total),
            manual_captions=sum(1 for r in ranked if r["has_manual_captions"]),
            measured_on=__import__("datetime").date.today(),
        )
    except Exception:
        logger.warning("standup shelf: снимок пула не записан", exc_info=True)

    need = want - counts["unused"]
    cap = int(max_add) if max_add is not None else _env_int("STANDUP_SHELF_MAX_ADD", 12)
    need = min(need, cap)

    for idx, item in enumerate(ranked, 1):
        if report["added"] >= need:
            break
        if time.monotonic() - started > budget_sec:
            report["budget_spent"] = True
            logger.warning("standup shelf: бюджет %ds исчерпан — положили %d, остальное в следующий раз",
                           budget_sec, report["added"])
            break
        # Строка на каждый ролик: без неё длинный цикл молчит, и снаружи не отличить
        # работу от зависания.
        logger.info("standup shelf: беру субтитры %d/%d — %s (%s)",
                    idx, len(ranked), item["video_id"], item["title"][:50])
        data = _fetch_transcript(item["video_id"])
        if not data or not (data.get("items") or []):
            report["no_transcript"] += 1
            continue
        text = _transcript_to_text(data.get("items") or [])
        if len(text) < WORLD_NEWS_MIN_TRANSCRIPT_CHARS:
            report["short_transcript"] += 1
            continue
        try:
            added = put_on_standup_shelf(
                video_id=item["video_id"],
                video_title=item["title"],
                channel_title=item["channel_title"],
                duration_seconds=item["duration_seconds"],
                has_manual_captions=item["has_manual_captions"],
                view_count=item["view_count"],
                transcript=data.get("items") or [],
                transcript_lang=data.get("language") or "de",
                transcript_is_generated=data.get("is_generated"),
            )
        except Exception:
            logger.warning("standup shelf: не удалось положить %s", item["video_id"], exc_info=True)
            continue
        if added:
            report["added"] += 1

    after = standup_shelf_counts()
    report["now_unused"] = after["unused"]
    report["now_unused_manual"] = after["unused_manual"]
    logger.info("standup shelf: пополнение — было %d, добавлено %d, стало %d "
                "(без субтитров %d, коротких %d)",
                counts["unused"], report["added"], after["unused"],
                report["no_transcript"], report["short_transcript"])
    return report


def format_shelf_refill_report(report: dict) -> str:
    """Человеческий текст о пополнении. Молчащий механизм неотличим от сломанного."""
    if report.get("reason") and not report.get("added"):
        return (f"🎤 <b>Полка стендапов</b>\n\n"
                f"Непоказанных: <b>{report.get('had_unused', 0)}</b> из {report.get('target', 0)}\n"
                f"{report['reason']}")
    lines = [
        "🎤 <b>Полка стендапов пополнена</b>",
        "",
        f"Было непоказанных: {report.get('had_unused', 0)}",
        f"Добавлено: <b>{report.get('added', 0)}</b>",
        f"Стало: <b>{report.get('now_unused', 0)}</b> "
        f"(из них с ручными субтитрами {report.get('now_unused_manual', 0)})",
    ]
    skipped = []
    if report.get("no_transcript"):
        skipped.append(f"без субтитров {report['no_transcript']}")
    if report.get("short_transcript"):
        skipped.append(f"субтитры слишком короткие {report['short_transcript']}")
    if report.get("dur_skipped"):
        skipped.append(f"не та длительность {report['dur_skipped']}")
    if skipped:
        lines += ["", "Не подошли: " + ", ".join(skipped)]
    if report.get("budget_spent"):
        lines += ["", "⏳ Время вышло, положили сколько успели. Повтори /standup_shelf — "
                      "докладёт остальное."]
    return "\n".join(lines)
