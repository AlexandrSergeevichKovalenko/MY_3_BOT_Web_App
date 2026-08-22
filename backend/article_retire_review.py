"""Разбор снятых слов в личке: слово → «вернуть в игру» или «нет, мусор».

Зачем. Чистка словника сняла с показа 2 929 слов. Автоматика ошибается в обе стороны, а
владелец снятое не видит: слово просто исчезает, и заметить это можно только случайно.
Смотреть весь список глазами тоже нельзя — 2 784 слова там вообще нет в частотном списке
(«Schmetterlings-Tramete»), и их проверять незачем.

Поэтому спрашиваем ТОЛЬКО про спорное: снятые слова, которые по частотности выглядят
ходовыми (ранг лучше RETIRE_REVIEW_MAX_RANK).

Очередь НЕ конечная — это важно и когда-то было понято неверно. Изначально в ней лежали
145 слов одной разовой чистки, и рассылка задумывалась как «кончится и замолчит». Потом
появился карантин (отказы стража приёмки), и очередь стала постоянным потоком. Замер
16.08.2026: в ней было 533 слова, из них 454 карантинных, приход ~6 слов на каждую сотню
новых в банке. Пачка по 10 такое не разгребала, поэтому владелец поднял её до 20.

Цикл замкнут: раз в день приходит порция с двумя кнопками. «Вернуть» — слово снова в
игре. «Мусор» — остаётся снятым и уходит в стоп-лист, чтобы набор не предложил его
заново. Спрошенное слово второй раз не приходит (retire_reviewed).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "article_retire_review_dm"
_HTTP_TIMEOUT = 15
# Сколько слов за раз. Больше — личка превращается в свалку и её перестают читать.
# 16.08.2026: с 10 до 20. Очередь разрослась до 533 (карантин пополнял её быстрее, чем
# разбирала пачка по 10), после разбора шума осталось 419. Планку назвал владелец сам:
# «20 штук я смогу сделать в день, 30-40 будет сложно» — то есть это не расчётный
# оптимум, а честный предел одного человека, и поднимать выше без него нельзя.
BATCH = max(1, int((os.getenv("RETIRE_REVIEW_BATCH") or "20").strip() or "20"))
# Докуда слово считается ходовым и достойным вопроса. Дальше — снято по делу.
MAX_RANK = max(1, int((os.getenv("RETIRE_REVIEW_MAX_RANK") or "60000").strip() or "60000"))


def _keyboard(row_id: int, *, mode: str = "sure") -> dict[str, Any]:
    """Вернуть / оставить снятым. callback_data: artret:<действие>:<id>.

    У двуродовых слов (der/die Flur) артикль решает смысл, а не написание, поэтому
    кнопка «вернуть» там бессмысленна: владелец ставит артикль ДЛЯ ПОКАЗАННОГО перевода,
    и в игре этот перевод тоже будет виден."""
    if str(mode) == "sense":
        return {
            "inline_keyboard": [
                [
                    {"text": "der", "callback_data": f"artret:der:{row_id}"},
                    {"text": "die", "callback_data": f"artret:die:{row_id}"},
                    {"text": "das", "callback_data": f"artret:das:{row_id}"},
                ],
                [{"text": "🚫 мусор", "callback_data": f"artret:keep:{row_id}"}],
            ]
        }
    return {
        "inline_keyboard": [[
            {"text": "✅ вернуть в игру", "callback_data": f"artret:back:{row_id}"},
            {"text": "🚫 мусор", "callback_data": f"artret:keep:{row_id}"},
        ]]
    }


def _word_text(item: dict, *, index: int, total: int, left: int) -> str:
    word = str(item.get("word") or "")
    article = str(item.get("article") or "")
    stored = str(item.get("stored_article") or "")
    meaning = str(item.get("meaning_ru") or "").strip()
    rank = item.get("rank")
    mode = str(item.get("mode") or "sure")
    if mode == "sense":
        # Артикль зависит от смысла — показывать его как факт нельзя, спрашиваем.
        lines = [f"<b>{word}</b>"]
        if meaning:
            lines.append(f"<i>{meaning}</i>")
        lines.append("")
        lines.append("Слово убрано из игры при чистке словника.")
        if rank:
            lines.append(f"По частоте оно на {int(rank)}-м месте — слово ходовое.")
        lines.append("")
        lines.append("У него артикль зависит от значения, поэтому в игре рядом со словом "
                     f"будет виден перевод «{meaning}» — вопрос честный.")
        lines.append(f"Какой артикль верен именно для «{meaning}»?")
        if stored:
            lines.append(f"<i>В базе стояло «{stored}».</i>")
        lines.append("")
        lines.append(f"<i>{index} из {total} · ещё в очереди: {left}</i>")
        return "\n".join(lines)
    lines = [f"<b>{article} {word}</b>".strip()]
    if meaning:
        lines.append(f"<i>{meaning}</i>")
    lines.append("")
    if item.get("quarantined"):
        # Карантин: слово в игре не было ни дня, его отсеяли на входе. Писать «убрано при
        # чистке» тут значит отправить владельца искать в памяти то, чего он не видел.
        lines.append("Набор предложил это слово для темы, но проверка его не пропустила — "
                     "решила, что в быту оно не встречается.")
        if rank:
            lines.append(f"По частоте оно на {int(rank)}-м месте в немецком — похоже, "
                         "проверка перестраховалась.")
    else:
        lines.append("Слово убрано из игры при чистке словника.")
        if rank:
            lines.append(f"Но по частоте оно на {int(rank)}-м месте в немецком — похоже на ходовое.")
    if stored and stored.lower() != article.lower():
        # Кривой артикль часто и был причиной снятия. Показываем ПРОВЕРЕННЫЙ и говорим
        # прямо, что вернём именно его — иначе человек вернёт в игру старую ошибку.
        lines.append(f"⚠️ В базе стояло «{stored} {word}» — справочник говорит «{article}». "
                     f"Вернём с «{article}».")
    lines.append("")
    lines.append(f"<i>{index} из {total} · ещё в очереди: {left}</i>")
    return "\n".join(lines)


def send_retire_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция спорных снятых слов админам. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        get_admin_telegram_ids,
        list_retired_review_candidates,
        count_retired_review_candidates,
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
    )
    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"batch": BATCH, "max_rank": MAX_RANK},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            return {"ok": False, "error": "no_token_or_admins"}

        items = list_retired_review_candidates(limit=BATCH, max_rank=MAX_RANK)
        left = count_retired_review_candidates(max_rank=MAX_RANK)
        if not items:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="completed",
                                           metadata={"sent": 0, "reason": "nothing_to_review"})
            return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

        # Пачка бывает смешанной: часть слов убрала чистка, часть не пропустила проверка
        # на входе. Обещать в шапке только одно — врать про половину списка.
        fresh = sum(1 for it in items if it.get("quarantined"))
        head = ("🧹 <b>Слова, которых нет в игре — проверь, всё ли верно</b>\n"
                + ("Часть убрала чистка словника, часть не пропустила проверка при наборе.\n"
                   if fresh and fresh < len(items) else
                   "Проверка при наборе их не пропустила, но по частоте они выглядят ходовыми.\n"
                   if fresh else
                   "Чистка убрала их из игры, но по частоте они выглядят ходовыми.\n")
                + "«Вернуть» — слово идёт в тренировку. «Мусор» — больше не предложим никогда.")
        sent = 0
        for uid in admin_ids:
            try:
                # Шапка идёт через честную доставку: Telegram отвечает на отказ телом,
                # а не исключением, и прежний вызов не смотрел на ответ вовсе.
                from backend.telegram_delivery import send_telegram_message
                дошла, почему = send_telegram_message(
                    chat_id=uid, text=head, token=token, what="шапка разбора артиклей")
                if not дошла:
                    logging.warning("шапка разбора не дошла до %s: %s", uid, почему)
                    continue
                все_дошли = True
                for i, item in enumerate(items, start=1):
                    resp = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": uid,
                              "text": _word_text(item, index=i, total=len(items), left=left),
                              "parse_mode": "HTML",
                              "reply_markup": _keyboard(int(item["id"]),
                                                        mode=str(item.get("mode") or "sure"))},
                        timeout=_HTTP_TIMEOUT,
                    )
                    if resp.status_code >= 400:
                        logging.warning("retire review DM failed uid=%s: %s", uid, resp.text[:200])
                        все_дошли = False
                if все_дошли:
                    # «Отправлено» — это доставлено, а не «вызвали отправку». Раньше
                    # счётчик рос даже когда отказ был записан строкой выше, и работа
                    # отчитывалась «выполнено» с числом, которого не было.
                    sent += 1
            except Exception:
                logging.warning("retire review DM failed uid=%s", uid, exc_info=True)
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": sent, "words": len(items)})
        return {"ok": True, "sent": sent, "words": len(items), "left": left}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("retire review DM failed")
        return {"ok": False, "error": str(exc)}


def apply_retire_review(action: str, row_id: int) -> str:
    """Применить тап админа. Возвращает готовый текст для ответа в личку."""
    from backend.database import (
        restore_retired_article_noun, keep_retired_article_noun,
        count_retired_review_candidates,
    )
    act = str(action or "").strip().lower()
    if act in ("back", "der", "die", "das"):
        res = restore_retired_article_noun(int(row_id), article="" if act == "back" else act)
        if not res:
            return "Слово уже разобрано."
        if res.get("blocked"):
            return (f"⚠️ <b>{res['word']}</b> не вернул: {res.get('reason')}.\n"
                    "Без перевода игрок не поймёт, о каком значении вопрос.")
        if res.get("needs_sense"):
            meaning = str(res.get("meaning_ru") or "").strip()
            return (f"🔀 <b>{res['word']}</b> — артикль зависит от значения ({res.get('reason')}).\n"
                    + (f"Значение этой строки: «{meaning}». " if meaning else "")
                    + "Поставь артикль кнопкой der/die/das — в игре рядом со словом будет "
                      "виден перевод, так что вопрос останется честным.")
        head = f"✅ <b>{res['article']} {res['word']}</b> — снова в игре."
        if res.get("two_gender"):
            head += (f"\nСлово двуродовое: в игре к нему покажем перевод "
                     f"«{res.get('meaning_ru')}», чтобы было понятно, о чём вопрос.")
        stored = str(res.get("stored_article") or "")
        if stored and stored.lower() != str(res["article"]).lower():
            head += f"\nАртикль поправлен: в базе стояло «{stored}», записал «{res['article']}» ({res.get('source')})."
    elif act == "keep":
        res = keep_retired_article_noun(int(row_id))
        if not res:
            return "Слово уже разобрано."
        head = f"🚫 <b>{res['word']}</b> — остаётся снятым, в набор больше не попадёт."
    else:
        return "Не понял действие."
    left = count_retired_review_candidates(max_rank=MAX_RANK)
    tail = f"\nЕщё спорных: {left}." if left else "\nСпорных слов больше нет 🎉"
    return head + tail
