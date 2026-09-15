# -*- coding: utf-8 -*-
"""Перевод накопленных дорожек в склеенное пространство номеров — ровно один раз каждую.

Зачем отдельный модуль. Склейка «катящихся» кадров переехала из браузера на сервер
(backend/subtitle_cues.py), и у реплики стал один номер на всю систему. Но в базе уже
лежат дорожки, записанные по-старому: сырые кадры плюс перевод, сохранённый под
склеенными номерами. Их надо привести к новому виду — и привести АККУРАТНО, потому что
склейка не идемпотентна: на 4000 случайных катящихся дорожек второй прогон менял текст
у 31. Поэтому «на всякий случай склеить ещё раз» — запрещено, и единственный ответ на
вопрос «склеена ли эта дорожка» даёт флаг `cues_rolled`, а не содержимое.

Работа идёт двумя путями, и оба нужны:
  • при первом открытии ролика — тот, кого смотрят, чинится сразу и незаметно;
  • ночным проходом — те, кого никто не открыл; иначе они останутся кривыми навсегда.

Перевод при этом НЕ перекладывается: он писался браузером под склеенными номерами и
после замены реплик сходится сам. Поэтому чистка накопленного не стоит ни одного
обращения к модели.
"""
from __future__ import annotations

import logging

from backend.subtitle_cues import deroll_transcript_cues, split_translation_key

logger = logging.getLogger(__name__)


def plan_roll(row: dict) -> tuple[list, list[str]]:
    """Что станет с одной дорожкой: склеенные реплики и ключи перевода на удаление.

    Удаляются только те ключи, которые после склейки указывают ЗА КОНЕЦ списка. Такие
    могли остаться единственным способом: перевод записан до 12.07.2026, когда склейки
    не было вовсе и номера были сырыми. Перенести их некуда — несколько сырых кадров
    схлопываются в одну строку, и обратного однозначного соответствия нет. Оставить
    тоже нельзя: они указывают в пустоту. Удаляются и считаются в `/subtitry`.
    """
    rolled, _index_map = deroll_transcript_cues(row.get("items") or [])
    translations = row.get("translations") or {}
    drop_keys = []
    for key in translations:
        parsed = split_translation_key(key)
        if parsed is not None and parsed[1] >= len(rolled):
            drop_keys.append(key)
    return rolled, drop_keys


def roll_one(video_id: str, row: dict) -> dict:
    """Склеить одну дорожку и запомнить это. Возвращает строку в новом виде.

    Ошибку записи НЕ проглатываем молча в отчёт: она логируется, а дорожка остаётся
    несклеенной и попадёт в `/subtitry` как незакрытая. Человеку при этом уходят уже
    ПРАВИЛЬНЫЕ склеенные реплики — сдвига на экране не будет в любом случае.
    """
    from backend.database import store_rolled_youtube_cues

    rolled, drop_keys = plan_roll(row)
    store_rolled_youtube_cues(video_id, rolled, drop_translation_keys=drop_keys)
    translations = row.get("translations") or {}
    dropped = set(drop_keys)
    return {
        **row,
        "items": rolled,
        "translations": {k: v for k, v in translations.items() if k not in dropped},
        "cues_rolled": True,
    }


def roll_pending_cues(limit: int = 500) -> dict:
    """Ночной проход: склеить дорожки, до которых никто не добрался сам.

    Возвращает {"looked_at", "rolled", "cues_removed", "translations_dropped", "failed"}.
    Сбой на одной дорожке не отменяет остальные, но и не прячется: он считается в
    "failed" и пишется в журнал — «ноль поражённых» и «не смогли» это разные исходы.
    """
    from backend.database import fetch_unrolled_youtube_transcripts

    rows = fetch_unrolled_youtube_transcripts(limit=limit)
    report = {"looked_at": len(rows), "rolled": 0, "cues_removed": 0,
              "translations_dropped": 0, "failed": 0}
    for row in rows:
        video_id = row.get("video_id") or ""
        if not video_id:
            report["failed"] += 1
            continue
        try:
            rolled, drop_keys = plan_roll(row)
            roll_one(video_id, row)
        except Exception:
            logger.exception("склейка дорожки не удалась video_id=%s", video_id)
            report["failed"] += 1
            continue
        report["rolled"] += 1
        report["cues_removed"] += max(0, len(row.get("items") or []) - len(rolled))
        report["translations_dropped"] += len(drop_keys)
    return report


def format_roll_sweep_report(report: dict) -> str:
    """Короткий человеческий текст для владельца. Пишем ТОЛЬКО когда что-то произошло."""
    rolled = int(report.get("rolled") or 0)
    dropped = int(report.get("translations_dropped") or 0)
    failed = int(report.get("failed") or 0)
    lines = [f"🎬 <b>Субтитры: склеено дорожек за ночь — {rolled}</b>"]
    if report.get("cues_removed"):
        lines.append(f"Повторяющихся кадров убрано: {report['cues_removed']}")
    if dropped:
        lines.append(
            f"Строк перевода удалено: {dropped} — это записи до 12.07.2026, их номера "
            f"указывали в пустоту. Заново переведутся при просмотре."
        )
    if failed:
        lines.append(f"⚠️ Не смогли склеить: {failed}. Останутся в `/subtitry` до починки.")
    lines.append("<i>Обращений к модели и к YouTube: ноль.</i>")
    return "\n".join(lines)
