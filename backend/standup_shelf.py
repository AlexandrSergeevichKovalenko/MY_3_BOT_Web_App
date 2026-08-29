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
        get_shown_daily_video_ids, put_on_standup_shelf, record_transcript_verdict,
        standup_shelf_counts, standup_shelf_video_ids, transcript_verdict_counts,
        transcript_video_ids_to_skip,
    )
    from backend.transcript_failure import VERDICT_UNUSABLE, is_permanent, verdict_ru
    from backend.world_news_generator import (
        WORLD_NEWS_MAX_TRANSCRIPT_CHARS, WORLD_NEWS_MIN_TRANSCRIPT_CHARS,
        _gather_candidates, _transcript_to_text, _yt_api_video_details,
        fetch_transcript_or_verdict,
    )

    # Бюджет — на СКАЧИВАНИЕ СУБТИТРОВ, и отсчитывается он от начала того цикла, а не от
    # входа в функцию (переделано 29.08.2026). Обход каналов занимает свои 32–40 секунд и
    # не виснет; когда секундомер включали раньше него, он отрезал время не у того, кто
    # его ворует, — до скачивания доживало 110 секунд из 150.
    # У каждого ролика теперь есть свой таймаут (см. fetch_transcript_or_verdict), поэтому
    # общий бюджет перестал быть затычкой и означает ровно то, что написано.
    budget_sec = int(budget_sec) if budget_sec else _env_int("STANDUP_SHELF_BUDGET_SEC", 150)
    # ┌─ ПРОВЕРЕНО 29.08.2026 НА ЖИВОМ ПРОГОНЕ. НЕ УМЕНЬШАТЬ ДО 90. ───────────────────┐
    # │ С таймаутом 90 c оба ролика получили вердикт «не дождались» — то есть ВРЕМЕННЫЙ │
    # │ — хотя лестница на них честно отвечает «субтитров нет». Просто полный проход    │
    # │ лестницы занимает 91–95 c, и мы обрывали её за секунду до ответа. Приговор при  │
    # │ этом не выносится никогда, реестр не наполняется, и через две недели мы жжём то │
    # │ же время заново. 150 c — выше естественной длины лестницы с запасом, поэтому    │
    # │ таймаут срабатывает только на настоящем зависании, а не вместо ответа.          │
    # └───────────────────────────────────────────────────────────────────────────────┘
    item_timeout_sec = _env_int("STANDUP_SHELF_ITEM_TIMEOUT_SEC", 150)
    started = time.monotonic()

    want = int(target or shelf_target())
    counts = standup_shelf_counts()
    report = {"had_unused": counts["unused"], "target": want, "added": 0,
              "no_transcript": 0, "short_transcript": 0, "dur_skipped": 0, "swept": 0,
              "attempted": 0, "budget_sec": budget_sec,
              "item_timeout_sec": item_timeout_sec,
              # Сколько кандидатов даже не трогали: приговор уже вынесен или срок отсрочки
              # ещё не вышел. Именно это число объясняет, почему очередь пошла дальше.
              "skipped_known": 0,
              # Приговоры, вынесенные ЭТОЙ ночью, по видам.
              "verdicts": {}}

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

    # Голова очереди расчищается ЗДЕСЬ. Порядок отбора детерминированный, поэтому без
    # этого списка ночная работа семь раз подряд упиралась в одни и те же два ролика
    # (замер 29.08.2026: 91 секунда на каждый, оба — «субтитров нет»).
    skip_ids = transcript_video_ids_to_skip()
    ranked = []
    for cand in candidates:
        vid = cand["video_id"]
        if vid in on_shelf or vid in shown:
            continue
        if vid in skip_ids:
            report["skipped_known"] += 1
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

    # Секундомер стартует ЗДЕСЬ: бюджет отпущен на скачивание субтитров, а не на обход
    # каналов, который уже позади.
    loop_started = time.monotonic()

    def _judge(video_id: str, verdict: str, reason: str | None) -> None:
        """Записать вердикт по ролику и посчитать его для ночного письма."""
        report["verdicts"][verdict] = int(report["verdicts"].get(verdict) or 0) + 1
        try:
            record_transcript_verdict(video_id=video_id, verdict=verdict, reason=reason)
        except Exception:
            # Реестр — это память о попытке, а не сам ответ. Если запись не удалась,
            # ролик просто попробуют ещё раз; подменять при этом нечего.
            logger.warning("standup shelf: вердикт %s по %s не записан", verdict, video_id,
                           exc_info=True)

    for idx, item in enumerate(ranked, 1):
        if report["added"] >= need:
            break
        if time.monotonic() - loop_started > budget_sec:
            report["budget_spent"] = True
            logger.warning("standup shelf: бюджет %ds исчерпан — положили %d, остальное в следующий раз",
                           budget_sec, report["added"])
            break
        # Строка на каждый ролик: без неё длинный цикл молчит, и снаружи не отличить
        # работу от зависания.
        logger.info("standup shelf: беру субтитры %d/%d — %s (%s)",
                    idx, len(ranked), item["video_id"], item["title"][:50])
        data, verdict, reason = fetch_transcript_or_verdict(
            item["video_id"], timeout_sec=item_timeout_sec)
        if not data:
            report["no_transcript"] += 1
            _judge(item["video_id"], verdict, reason)
            logger.info("standup shelf: %s — %s (%s)", item["video_id"], verdict_ru(verdict),
                        "навсегда" if is_permanent(verdict) else "вернёмся позже")
            continue
        text = _transcript_to_text(data.get("items") or [])
        if len(text) < WORLD_NEWS_MIN_TRANSCRIPT_CHARS:
            # Субтитры есть, но их слишком мало для разбора. Это свойство самого ролика,
            # а не нашей сети, — значит вердикт окончательный, и завтра его не качаем.
            report["short_transcript"] += 1
            _judge(item["video_id"], VERDICT_UNUSABLE,
                   f"субтитры {len(text)} знаков < {WORLD_NEWS_MIN_TRANSCRIPT_CHARS}")
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
    # Сколько роликов вообще дошло до скачивания субтитров. Без этого числа отчёт
    # «добавлено 0» не отличает «перебрали полсотни, ни у кого нет субтитров» от
    # «успели попробовать одного и упёрлись в бюджет» — а это разные поломки.
    report["attempted"] = (report["added"] + report["no_transcript"]
                           + report["short_transcript"])
    try:
        report["registry"] = transcript_verdict_counts()
    except Exception:
        # Числа реестра — материал письма, а не условие работы. Не сосчитали — так и
        # скажем в письме, но пополнение это не отменяет.
        logger.warning("standup shelf: не сосчитали реестр вердиктов", exc_info=True)
        report["registry"] = None
    logger.info("standup shelf: пополнение — было %d, добавлено %d, стало %d "
                "(без субтитров %d, коротких %d)",
                counts["unused"], report["added"], after["unused"],
                report["no_transcript"], report["short_transcript"])
    return report


def _registry_line(report: dict) -> list:
    """Строка о реестре негодных — чтобы он не разрастался молча.

    «Навсегда» стоит рядом с «ждёт решения» осознанно: приговор навсегда выносится только
    по ответу YouTube, а всё, что трижды не далось по другим причинам, копится отдельно и
    ждёт человека (владелец 29.08.2026: «чтобы мы точно знали, что хороший ролик не
    выбросили»).
    """
    reg = report.get("registry")
    if not isinstance(reg, dict):
        return []
    line = (f"Реестр негодных: навсегда {reg.get('permanent', 0)} · "
            f"отложено {reg.get('waiting', 0)}")
    if reg.get("needs_review"):
        line += f" · <b>ждёт решения {reg['needs_review']}</b>"
    return ["", line]


def refill_fell_short(report: dict) -> bool:
    """Полка осталась неполной, и пополнение ничего не добавило.

    Ровно тот случай, о котором владелец обязан узнать: «полка полна, в сеть не ходили» —
    молчим, «полка на 4 из 30 и добавить не смогли» — говорим. Раньше оба случая
    выглядели одинаково (никак), и полка простояла пустеющей семь ночей (29.08.2026).
    """
    if report.get("added"):
        return False
    now_unused = report.get("now_unused")
    if now_unused is None:
        # Ранний выход (полка полна / кандидатов не получено): пересчёта не было,
        # значит на полке столько же, сколько было на входе.
        now_unused = report.get("had_unused")
    return int(now_unused or 0) < int(report.get("target") or 0)


def format_shelf_refill_report(report: dict) -> str:
    """Человеческий текст о пополнении. Молчащий механизм неотличим от сломанного."""
    if report.get("reason") and not report.get("added"):
        return (f"🎤 <b>Полка стендапов</b>\n\n"
                f"Непоказанных: <b>{report.get('had_unused', 0)}</b> из {report.get('target', 0)}\n"
                f"{report['reason']}")
    # ┌─ НАЙДЕНО 29.08.2026: пополнение молча не пополняло. ───────────────────────────┐
    # │ Полка стояла на 4 роликах из 30 с 21.08, семь ночей подряд добавляя ноль, и    │
    # │ владелец об этом не знал: сообщение уходило только при added > 0. Отдельный    │
    # │ текст на случай «пробовали и не смогли» — чтобы «нечего добавить» и «не дошли  │
    # │ руки из-за бюджета» больше не выглядели одинаково (никак).                     │
    # └───────────────────────────────────────────────────────────────────────────────┘
    if not report.get("added"):
        lines = [
            "🎤 <b>Полка стендапов НЕ пополнилась</b>",
            "",
            f"Непоказанных: <b>{report.get('now_unused', report.get('had_unused', 0))}</b> "
            f"из {report.get('target', 0)} — это примерно "
            f"{int(report.get('now_unused', report.get('had_unused', 0))) * 2} дн. вещания.",
            "",
            f"Обошли роликов: {report.get('swept', 0)}, "
            f"до субтитров дошли: {report.get('attempted', 0)}",
        ]
        skipped = []
        if report.get("no_transcript"):
            skipped.append(f"без субтитров {report['no_transcript']}")
        if report.get("short_transcript"):
            skipped.append(f"субтитры слишком короткие {report['short_transcript']}")
        if report.get("dur_skipped"):
            skipped.append(f"не та длительность {report['dur_skipped']}")
        if skipped:
            lines += ["Не подошли: " + ", ".join(skipped)]
        verdicts = report.get("verdicts")
        if isinstance(verdicts, dict) and verdicts:
            from backend.transcript_failure import verdict_ru
            lines += ["Почему не взяли: " + ", ".join(
                f"{verdict_ru(v)} {n}" for v, n in sorted(verdicts.items()))]
        if report.get("skipped_known"):
            lines += [f"Не трогали (уже разобраны): {report['skipped_known']}"]
        if report.get("budget_spent"):
            lines += ["", f"⏳ Время вышло: за {report.get('budget_sec', 0)} c успели "
                          f"попробовать {report.get('attempted', 0)}."]
        lines += _registry_line(report)
        lines += ["", "Проверить руками: /standup_shelf"]
        return "\n".join(lines)
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
    lines += _registry_line(report)
    return "\n".join(lines)
