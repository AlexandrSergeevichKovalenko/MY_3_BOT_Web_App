"""Ночной прогрев справочника родов + утренний отчёт о запросах к Wiktionary.

Зачем. Кэш bt_3_wiktionary_genus_cache — это журнал ОТВЕТОВ API de.wiktionary, а не
копия словаря: сколько спросили, столько и лежит. Чем он полнее, тем чаще арбитр рода
(article_authority) отвечает «знаю» вместо «не знаю» — и тем реже владельца дёргают
ручным подтверждением артикля (article_review).

Почему понемногу, а не разом. Разовый прогон на 3000 слов упёрся в HTTP 429 и дал всего
248 полезных родов из 3000 запросов: Wiktionary ограничивает частоту, а список кандидатов
был мусорным (брались ВСЕ хвосты всех слов — «elkörper», «senwert»). Здесь исправлено обоё:

  • кандидаты — ТОЛЬКО настоящие слова: слова банка, которых нет в кэше, и те хвосты
    композитов, которые сами встречаются как отдельные слова в нашем словаре;
  • темп — небольшая ночная порция (по умолчанию 200 слов) с паузой между пачками и
    отступлением при 429. За месяц это несколько тысяч слов и ни одного упора в лимит.

Каждый прогон пишет строку в bt_3_wiktionary_warm_runs — из неё утром собирается отчёт.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

WARM_JOB_KEY = "wiktionary_warm_nightly"
REPORT_JOB_KEY = "wiktionary_warm_report"
_HTTP_TIMEOUT = 15

# Порция за ночь. 200 слов ≈ 5 пачек — Wiktionary этого не замечает.
NIGHTLY_LIMIT = max(10, int((os.getenv("WIKTIONARY_WARM_LIMIT") or "200").strip() or "200"))
BATCH = 45                     # столько же, сколько в article_wiktionary_ref
PAUSE_SEC = float((os.getenv("WIKTIONARY_WARM_PAUSE_SEC") or "1.5").strip() or "1.5")


def _tz_name() -> str:
    return (os.getenv("WIKTIONARY_WARM_TZ") or os.getenv("TRIAL_POLICY_TZ") or "Europe/Vienna")


def ensure_warm_runs_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_wiktionary_warm_runs (
                    id BIGSERIAL PRIMARY KEY,
                    run_day DATE NOT NULL,
                    requested INT NOT NULL DEFAULT 0,
                    resolved INT NOT NULL DEFAULT 0,
                    no_genus INT NOT NULL DEFAULT 0,
                    two_gender INT NOT NULL DEFAULT 0,
                    batches_ok INT NOT NULL DEFAULT 0,
                    batches_failed INT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'ok',
                    error TEXT NOT NULL DEFAULT '',
                    duration_sec NUMERIC(10,2) NOT NULL DEFAULT 0,
                    cache_before INT NOT NULL DEFAULT 0,
                    cache_after INT NOT NULL DEFAULT 0,
                    queue_left INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_bt_3_wiktionary_warm_runs_day "
                "ON bt_3_wiktionary_warm_runs (run_day DESC);"
            )
        conn.commit()


def _real_words() -> set[str]:
    """Всё, что мы знаем как настоящее немецкое слово: банк тренажёра + общий словарь."""
    from backend.database import get_db_connection_context
    words: set[str] = set()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT word FROM bt_3_article_sprint_nouns WHERE NOT retired")
            words |= {str(r[0]).strip().lower() for r in cur.fetchall() or [] if r[0]}
            try:
                cur.execute(
                    "SELECT DISTINCT source_text FROM bt_3_dictionary_entries "
                    "WHERE source_lang = 'de' AND source_text <> '' LIMIT 60000"
                )
                words |= {str(r[0]).strip().lower() for r in cur.fetchall() or [] if r[0]}
            except Exception:
                logging.debug("warm: общий словарь недоступен", exc_info=True)
    return {w for w in words if w and " " not in w}


def build_candidates(limit: int) -> list[str]:
    """Настоящие слова, о которых мы ещё НЕ спрашивали Wiktionary.

    Сначала ГОЛОВЫ композитов (частотные слова, страница у них почти всегда есть, и одна
    голова закрывает правилом композита все слова банка с таким окончанием), затем сами
    слова банка. Головы берём только те, что сами встречаются как отдельные слова.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT lower(title) FROM bt_3_wiktionary_genus_cache")
            cached = {r[0] for r in cur.fetchall() or []}
            cur.execute("SELECT DISTINCT word FROM bt_3_article_sprint_nouns WHERE NOT retired")
            bank = [str(r[0]).strip() for r in cur.fetchall() or [] if r[0]]

    real = _real_words()
    out: list[str] = []
    seen: set[str] = set()

    # СНАЧАЛА головы композитов. Отдача несопоставима: голова — обычное частотное слово
    # («Kurs», «Beet», «Strauch»), страница в Wiktionary у неё почти всегда есть. И одна
    # такая голова закрывает правилом композита ВСЕ слова банка, которые ей кончаются.
    # Замер: 90 слов-композитов дали 1 род на 90 запросов — вот почему порядок именно такой.
    for w in bank:
        low = w.lower()
        for cut in range(3, len(low) - 3):
            tail = low[cut:]
            if len(tail) < 4 or tail in cached or tail in seen:
                continue
            if tail in real:
                out.append(tail.capitalize())
                seen.add(tail)
                if len(out) >= limit:
                    return out

    # Затем сами слова банка: у большинства своей страницы нет (это композиты), но
    # подтверждённые снимаются с ручного разбора напрямую.
    for w in bank:
        low = w.lower()
        if low not in cached and low not in seen:
            out.append(w)
            seen.add(low)
            if len(out) >= limit:
                return out
    return out


def run_warm(*, limit: int | None = None) -> dict[str, Any]:
    """Спросить Wiktionary про очередную порцию слов. Пишет строку статистики."""
    from backend.article_wiktionary_ref import _api_fetch  # noqa: PLC2701 — свой же модуль
    from backend.database import get_db_connection_context, upsert_genus_cache

    ensure_warm_runs_schema()
    started = time.time()
    take = max(1, int(limit or NIGHTLY_LIMIT))

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bt_3_wiktionary_genus_cache")
            cache_before = int((cur.fetchone() or [0])[0] or 0)

    titles = build_candidates(take)
    stats = {"requested": 0, "resolved": 0, "no_genus": 0, "two_gender": 0,
             "batches_ok": 0, "batches_failed": 0, "status": "ok", "error": ""}
    if not titles:
        stats["status"] = "nothing_to_do"

    for i in range(0, len(titles), BATCH):
        batch = titles[i:i + BATCH]
        stats["requested"] += len(batch)
        try:
            got = _api_fetch(batch)
        except Exception as exc:
            got = {}
            stats["error"] = str(exc)[:300]
        if not got:
            # Пустой ответ = сеть или 429. Отступаем и заканчиваем ночь: лучше меньше,
            # чем злить API — оставшиеся слова спросим завтра, очередь никуда не денется.
            stats["batches_failed"] += 1
            stats["status"] = "rate_limited"
            logging.warning("wiktionary warm: пачка не ответила, останавливаемся на сегодня")
            break
        stats["batches_ok"] += 1
        # Не пришедшие в ответе титулы — считаем «страницы нет».
        for t in batch:
            got.setdefault(t, "-")
        for code in got.values():
            c = str(code or "").strip().lower()
            if len(c) == 1 and c in "mfn":
                stats["resolved"] += 1
            elif c == "x":
                stats["two_gender"] += 1
            else:
                stats["no_genus"] += 1
        try:
            upsert_genus_cache(got)
        except Exception as exc:
            stats["error"] = str(exc)[:300]
            stats["status"] = "partial"
        time.sleep(PAUSE_SEC)

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bt_3_wiktionary_genus_cache")
            cache_after = int((cur.fetchone() or [0])[0] or 0)
    left = max(0, len(build_candidates(100000)))

    row = dict(stats)
    row.update({"duration_sec": round(time.time() - started, 2),
                "cache_before": cache_before, "cache_after": cache_after, "queue_left": left})
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_wiktionary_warm_runs
                      (run_day, requested, resolved, no_genus, two_gender, batches_ok,
                       batches_failed, status, error, duration_sec, cache_before, cache_after, queue_left)
                    VALUES (CURRENT_DATE, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """,
                    (row["requested"], row["resolved"], row["no_genus"], row["two_gender"],
                     row["batches_ok"], row["batches_failed"], row["status"], row["error"],
                     row["duration_sec"], row["cache_before"], row["cache_after"], row["queue_left"]),
                )
            conn.commit()
    except Exception:
        logging.warning("wiktionary warm: не смог записать статистику", exc_info=True)
    return row


# Слова, где Wiktionary формально расходится с банком, но прав БАНК. Без этого списка
# отчёт вечно показывал бы одни и те же шесть расхождений — а отчёт, который каждый день
# кричит об одном и том же, перестают читать.
_JUSTIFIED_MISMATCHES = {
    # Wiktionary даёт лемму единственного числа, а учим мы форму множественного —
    # именно в ней слово и живёт в речи.
    "geschwister", "adoptivgeschwister", "pumps",
    # Региональная норма: die SMS в Германии, das/der в Австрии и Швейцарии.
    "sms",
    # Wiktionary отвечает про женское имя Jasmin, а у нас растение — der Jasmin.
    "jasmin",
    # das Rom — город; der Rom — представитель народа рома. У нас город.
    "rom",
}


def bank_vs_wiktionary_mismatches() -> list[tuple[str, str, str]]:
    """Слова банка, чей артикль РАСХОДИТСЯ с прямым ответом Wiktionary.

    Это единственная проверка, которая ловит ошибку в уже показываемом слове — не в
    новом на входе, а в том, что человек видит прямо сейчас. Двухродовые пропускаем:
    у них артикль зависит от значения, и расхождение там не ошибка.
    Возвращает [(слово, артикль_в_банке, артикль_по_Wiktionary)].
    """
    from backend.article_authority import _load
    from backend.database import get_db_connection_context
    genus, ambiguous = _load()
    out: list[tuple[str, str, str]] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT word, article FROM bt_3_article_sprint_nouns "
                "WHERE NOT retired AND COALESCE(article,'') <> ''"
            )
            for word, article in cur.fetchall() or []:
                w = str(word).strip()
                art = str(article or "").strip().lower()
                low = w.lower()
                if low in ambiguous or low in _JUSTIFIED_MISMATCHES:
                    continue
                ref = genus.get(low)
                if ref and ref != art:
                    out.append((w, art, ref))
    return sorted(out)


def build_warm_report_text(*, target_day: date | None = None) -> str:
    """Утренний отчёт: что вчера спросили у Wiktionary и чем это кончилось."""
    from backend.database import get_db_connection_context
    ensure_warm_runs_schema()
    day = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT requested, resolved, no_genus, two_gender, batches_ok, batches_failed, "
                "status, error, duration_sec, cache_before, cache_after, queue_left "
                "FROM bt_3_wiktionary_warm_runs WHERE run_day = %s ORDER BY id;",
                (day,),
            )
            runs = cur.fetchall() or []
            cur.execute(
                "SELECT COUNT(*) FROM bt_3_wiktionary_genus_cache "
                "WHERE length(trim(genus)) = 1 AND trim(genus) IN ('m','f','n')"
            )
            decided = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM bt_3_article_sprint_nouns WHERE NOT verified AND NOT retired")
            waiting = int((cur.fetchone() or [0])[0] or 0)

    # Сверка уже показываемых слов с эталоном — то, ради чего отчёт вообще нужен:
    # новое расхождение всплывает на следующее утро, а не через месяц из скриншота.
    try:
        mism = bank_vs_wiktionary_mismatches()
    except Exception:
        logging.warning("warm report: сверка с Wiktionary не удалась", exc_info=True)
        mism = []
    if mism:
        preview = ", ".join(f"{w} ({a}→{r})" for w, a, r in mism[:5])
        more = f" … и ещё {len(mism) - 5}" if len(mism) > 5 else ""
        mism_line = (f"\n⚠️ <b>Расхождений с Wiktionary: {len(mism)}</b>\n<i>{preview}{more}</i>")
    else:
        mism_line = "\n✅ Расхождений с Wiktionary нет — все показываемые артикли сходятся."

    head = f"📚 <b>Справочник родов — {day.strftime('%d.%m.%Y')}</b>"
    if not runs:
        return (f"{head}\n\nНочной прогрев не запускался: строк за этот день нет.\n"
                f"В справочнике сейчас <b>{decided}</b> слов с однозначным родом.\n"
                f"Ждут ручного подтверждения: <b>{waiting}</b>."
                f"\n{mism_line}")

    req = sum(r[0] for r in runs)
    res = sum(r[1] for r in runs)
    nog = sum(r[2] for r in runs)
    two = sum(r[3] for r in runs)
    bok = sum(r[4] for r in runs)
    bad = sum(r[5] for r in runs)
    statuses = {str(r[6]) for r in runs}
    errors = [str(r[7]) for r in runs if str(r[7] or "").strip()]
    dur = sum(float(r[8] or 0) for r in runs)
    left = runs[-1][11]

    # Порядок важен: «нечего спрашивать» — только если ВСЕ прогоны за день были такими.
    # Иначе один пустой прогон затирал вердикт настоящего, и отчёт врал про успех.
    if bad and "rate_limited" in statuses:
        verdict = "⚠️ Прервались: Wiktionary попросил сбавить темп. Остаток спросим следующей ночью."
    elif "partial" in statuses:
        verdict = "⚠️ Прошло частично — часть ответов не записалась, подробности ниже."
    elif statuses == {"nothing_to_do"}:
        verdict = "✅ Спрашивать было нечего: все настоящие слова уже в справочнике."
    else:
        verdict = "✅ Прошло штатно."

    share = f"{round(100.0 * res / req)}%" if req else "—"
    lines = [
        head, "",
        f"Запросов: <b>{req}</b> · получили род: <b>{res}</b> ({share})",
        f"Страницы нет / рода нет: {nog} · двухродовых: {two}",
        f"Пачек успешно: {bok} · неудачных: {bad} · время: {int(dur)} с",
        "",
        f"В справочнике теперь <b>{decided}</b> слов с однозначным родом.",
        f"Осталось спросить: <b>{left}</b> · ждут твоего подтверждения: <b>{waiting}</b>",
        mism_line,
        "", verdict,
    ]
    if errors:
        lines.append(f"\n<i>Ошибка: {errors[0][:200]}</i>")
    return "\n".join(lines)


def send_warm_report(*, target_day: date | None = None, force: bool = False) -> dict[str, Any]:
    from backend.database import (
        get_admin_telegram_ids, claim_scheduler_run_guard, finish_scheduler_run_guard,
    )
    day = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    run_period = day.isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=REPORT_JOB_KEY, run_period=run_period, target_scope="global", metadata={},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admins = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admins:
            return {"ok": False, "error": "no_token_or_admins"}
        text = build_warm_report_text(target_day=day)
        sent = 0
        for uid in admins:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code >= 400:
                logging.warning("wiktionary warm report DM failed uid=%s: %s", uid, resp.text[:200])
            else:
                sent += 1
        if not force:
            finish_scheduler_run_guard(job_key=REPORT_JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": sent})
        return {"ok": True, "sent": sent}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=REPORT_JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("wiktionary warm report failed")
        return {"ok": False, "error": str(exc)}
