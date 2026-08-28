"""Отчёт «Словарь: база и экономия» — админу в личку по понедельникам и пятницам.

Отвечает на один вопрос: растёт ли наша собственная база слов и насколько реже мы из-за
неё ходим наружу (GPT/переводчик). Плюс качество базы, чтобы гниение было видно раньше
жалоб.

ВАЖНО про историю: телеметрия (bt_3_limit_runtime_events, bt_3_billing_events) чистится
ретенцией через 30 дней. Поэтому каждый отчёт кладёт снимок своих чисел в
bt_3_dictionary_pool_snapshots — только так у нас будет длинная динамика.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from backend.database import (
    claim_scheduler_run_guard,
    finish_scheduler_run_guard,
    get_admin_telegram_ids,
    get_dictionary_pool_report_stats,
    get_last_dictionary_pool_snapshot,
    record_scheduler_heartbeat,
    save_dictionary_pool_snapshot,
)

JOB_KEY = "dictionary_pool_report"
_HTTP_TIMEOUT = 20
# Окно по умолчанию, если снимков ещё нет: Пн←Пт = 3 дня, Пт←Пн = 4. Берём 4, чтобы
# первый отчёт ничего не потерял.
_DEFAULT_WINDOW_DAYS = 4
_KIND_TITLES = {
    "word": "слова",
    "phrase": "фразы",
    "sentence": "предложения",
    "unknown": "без вида",
}


def _split_telegram_text(text: str, limit: int = 3500) -> list[str]:
    parts: list[str] = []
    buf = ""
    for line in str(text or "").split("\n"):
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) > limit:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf = candidate
    if buf:
        parts.append(buf)
    return parts or [""]


def _delta(current: float | int, previous: float | int | None) -> str:
    """Строка изменения к прошлому отчёту — без неё числа не читаются как динамика."""
    if previous is None:
        return ""
    diff = float(current) - float(previous)
    if abs(diff) < 0.5:
        return " (без изменений)"
    sign = "+" if diff > 0 else "−"
    return f" ({sign}{abs(diff):,.0f})".replace(",", " ")


def _pct(part: int, whole: int) -> float:
    return (100.0 * part / whole) if whole else 0.0


def collect_dictionary_pool_report(*, window_start: datetime | None = None) -> dict[str, Any]:
    last = get_last_dictionary_pool_snapshot()
    if window_start is None:
        if last and last.get("taken_at"):
            window_start = last["taken_at"]
        else:
            window_start = datetime.now(timezone.utc) - timedelta(days=_DEFAULT_WINDOW_DAYS)
    stats = get_dictionary_pool_report_stats(window_start=window_start)
    stats["window_start"] = window_start.isoformat() if hasattr(window_start, "isoformat") else str(window_start)
    stats["previous"] = (last or {}).get("payload") or {}
    # Вкладка «Отличия»: сколько пар разобрано и сколько раз мы НЕ смогли ответить.
    # Отдельным try — состояние соседней витрины не имеет права уронить весь отчёт,
    # но и промолчать о своей поломке она не должна, поэтому здесь log, а не pass.
    try:
        from backend.database import word_diff_stats
        stats["word_diff"] = word_diff_stats(days=7)
    except Exception:
        logging.exception("dictionary report: состояние вкладки «Отличия» не собралось")
        stats["word_diff"] = None
    return stats


def _split_llm_actions(actions: list[dict]) -> tuple[int, int, list[dict], list[dict]]:
    """Не всё, что ушло в GPT, — это «промах базы». Запрос перевода — только основной
    lookup; обогащение карточки, коллокации и объяснения идут сверх него и в знаменателе
    доли «закрыли сами» участвовать не должны, иначе отчёт занижает эффект базы."""
    lookup_like, extra = [], []
    for a in actions or []:
        action = str(a.get("action") or "")
        if action.startswith("dictionary_assistant") or action in {"dictionary_lookup", "dictionary_lookup_fallback"}:
            lookup_like.append(a)
        else:
            extra.append(a)
    return (
        sum(int(a.get("count") or 0) for a in lookup_like),
        sum(int(a.get("count") or 0) for a in extra),
        lookup_like,
        extra,
    )


def build_dictionary_pool_report_text(stats: dict[str, Any]) -> str:
    prev = stats.get("previous") or {}
    hits = int(stats.get("hits_total") or 0)
    llm, llm_extra, _lookup_actions, extra_actions = _split_llm_actions(stats.get("llm_by_action") or [])
    asked = hits + llm
    own_share = _pct(hits, asked)

    window_start = str(stats.get("window_start") or "")[:16].replace("T", " ")
    lines: list[str] = [
        "📚 <b>Словарь: база и экономия</b>",
        f"Окно: с {window_start} UTC по сейчас",
        "",
        "<b>Сколько запросов перевода мы закрыли сами</b>",
        f"Спросили всего: <b>{asked}</b>",
        f"Отдали из своих запасов: <b>{hits}</b> ({own_share:.0f}%)"
        f"{_delta(hits, prev.get('hits_total'))}",
        f"Пошли в GPT за переводом: <b>{llm}</b> ({100 - own_share:.0f}%)",
    ]
    scopes = [s for s in (stats.get("hits_by_scope") or []) if int(s.get("count") or 0) > 0]
    if scopes:
        lines.append("  · откуда отдали: " + ", ".join(f"{s['scope']}: {s['count']}" for s in scopes[:6]))
    if llm_extra:
        lines.append(
            f"Ещё вызовов GPT по карточкам (не переводы): <b>{llm_extra}</b> за окно "
            "(обогащение, коллокации, объяснения — суммарно, не на каждую карточку)"
        )
        lines.append("  · " + ", ".join(f"{a['action']}: {a['count']}" for a in extra_actions[:5]))

    pool_total = int(stats.get("pool_total") or 0)
    lines += [
        "",
        "<b>Общая база слов (пул)</b>",
        f"Всего: <b>{pool_total}</b>{_delta(pool_total, prev.get('pool_total'))}",
        f"Добавлено за окно: <b>{int(stats.get('pool_new') or 0)}</b>",
    ]
    kinds = stats.get("pool_kinds") or []
    if kinds:
        lines.append(
            "  · "
            + ", ".join(
                f"{_KIND_TITLES.get(k.get('kind'), k.get('kind'))}: {k.get('total')} (+{k.get('new')})"
                for k in kinds[:4]
            )
        )

    saved_total = int(stats.get("saved_total") or 0)
    lines += [
        "",
        "<b>Личные словари пользователей</b>",
        f"Всего записей: <b>{saved_total}</b>{_delta(saved_total, prev.get('saved_total'))}",
        f"Сохранено за окно: <b>{int(stats.get('saved_new') or 0)}</b>"
        f" (активных пользователей: {int(stats.get('saved_users') or 0)})",
    ]

    rich = int(stats.get("pool_rich") or 0)
    thin = int(stats.get("pool_thin") or 0)
    lines += [
        "",
        "<b>Качество базы</b>",
        f"Полных карточек: <b>{rich}</b> ({_pct(rich, pool_total):.0f}%)"
        f"{_delta(rich, prev.get('pool_rich'))}",
        f"Тонких (очередь на дообогащение): <b>{thin}</b>{_delta(thin, prev.get('pool_thin'))}",
        f"Дублей из-за артикля: <b>{int(stats.get('pool_article_dupes') or 0)}</b>",
        f"Без родного перевода: <b>{int(stats.get('pool_missing_native') or 0)}</b>",
        "",
        f"Кеш запросов: {int(stats.get('cache_rows') or 0)} строк, "
        f"{int(stats.get('cache_hits_alltime') or 0)} переиспользований за всё время",
    ]
    diff_stats = stats.get("word_diff")
    if isinstance(diff_stats, dict):
        misses = diff_stats.get("misses") or {}
        not_found = int(misses.get("not_found") or 0)
        incomplete = int(misses.get("incomplete") or 0) + int(misses.get("model_error") or 0)
        lines += [
            "",
            "<b>Вкладка «Отличия»</b>",
            f"Разобрано пар: <b>{int(diff_stats.get('pairs_total') or 0)}</b> "
            f"(+{int(diff_stats.get('pairs_new') or 0)} за неделю), "
            f"открытий: {int(diff_stats.get('opens_total') or 0)}",
        ]
        if not_found:
            lines.append(
                f"Не хватило источнику слов: <b>{not_found}</b> за неделю — "
                "они уже поставлены в очередь на карточку, ночная работа их доберёт"
            )
        gaps = int(misses.get("gaps") or 0)
        if gaps:
            lines.append(
                f"Разборы с пробелами: <b>{gaps}</b> за неделю — слово осталось без "
                "верного примера или без сочетаний"
            )
        if incomplete:
            lines.append(
                f"⚠️ Модель не собрала разбор: <b>{incomplete}</b> раз за неделю — "
                "это задача на промпт, человек в эти разы остался без ответа"
            )
        # Разбор собран и показан, но осесть ему было некуда — значит за него заплатят
        # ещё раз. Снаружи это не видно ничем, поэтому строка обязательна.
        no_home = int(misses.get("no_home") or 0)
        if no_home:
            lines.append(
                f"Разборы без дома: <b>{no_home}</b> за неделю — слово разобрали, но "
                "часть речи не названа или источники о ней спорят, поэтому разбор не "
                "осел в словаре и соберётся заново"
            )
        if not not_found and not incomplete and not gaps and not no_home:
            lines.append("Ни одного промаха за неделю")
    elif diff_stats is None and "word_diff" in stats:
        lines += ["", "⚠️ Состояние вкладки «Отличия» собрать не удалось — смотри лог."]

    if asked == 0:
        lines += ["", "⚠️ За окно не было ни одного запроса перевода — проверь, жив ли путь словаря."]
    return "\n".join(lines)


def send_dictionary_pool_report(*, force: bool = False) -> dict[str, Any]:
    """Собрать, отправить админам и СОХРАНИТЬ СНИМОК. Снимок пишется только после
    успешной отправки, иначе окно следующего отчёта поехало бы вперёд без отчёта."""
    run_period = datetime.now(timezone.utc).date().isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global", metadata={},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        # Stage 0 (living shared dictionary): keep the corpus frequency_rank current on the
        # pool. Self-seeds the corpus on a fresh DB, then ranks only newly-pooled words. Fully
        # fail-safe — a frequency hiccup must never block the daily pool report.
        try:
            from backend import dictionary_frequency as _freq
            from backend.database import get_db_connection_context as _dbctx
            with _dbctx() as _fc:
                _freq.ensure_frequency_schema(_fc)
                with _fc.cursor() as _cc:
                    _cc.execute("SELECT 1 FROM bt_3_word_frequency LIMIT 1;")
                    _freq_empty = _cc.fetchone() is None
                if _freq_empty:
                    _freq.import_frequency_list(_fc)
                _freq.backfill_pool_frequency_ranks(_fc, only_unranked=True)
        except Exception as _fe:
            logging.warning("dictionary_frequency nightly top-up skipped: %s", _fe)
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        stats = collect_dictionary_pool_report()
        text = build_dictionary_pool_report_text(stats)
        if not token or not admin_ids:
            if not force:
                finish_scheduler_run_guard(
                    job_key=JOB_KEY, run_period=run_period, target_scope="global",
                    status="failed", metadata={"error": "no_token_or_admins"},
                )
            return {"ok": False, "sent": 0, "error": "no_token_or_admins"}
        sent = 0
        for uid in admin_ids:
            for part in _split_telegram_text(text):
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid, "text": part, "parse_mode": "HTML",
                          "disable_web_page_preview": True},
                    timeout=_HTTP_TIMEOUT,
                )
                if resp.status_code >= 400:
                    logging.warning("dictionary pool report DM failed uid=%s: %s", uid, resp.text[:200])
            sent += 1
        snapshot = {k: v for k, v in stats.items() if k != "previous"}
        save_dictionary_pool_snapshot(payload=snapshot, window_start=stats.get("window_start"))
        record_scheduler_heartbeat(
            job_key=JOB_KEY, status="completed",
            metadata={"sent": sent, "hits": stats.get("hits_total"), "llm": stats.get("llm_total")},
        )
        if not force:
            finish_scheduler_run_guard(
                job_key=JOB_KEY, run_period=run_period, target_scope="global",
                status="completed", metadata={"sent": sent},
            )
        return {"ok": True, "sent": sent}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(
                job_key=JOB_KEY, run_period=run_period, target_scope="global",
                status="failed", metadata={"error": str(exc)},
            )
        logging.exception("dictionary pool report failed")
        raise
