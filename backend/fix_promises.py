# -*- coding: utf-8 -*-
"""Реестр обещаний: каждая починка оставляет проверку, которую система гоняет сама.

┌─ ПОВОД, 04.09.2026. «ЭТО УЖЕ 20-Е ПОДРЯД ЗАДАНИЕ, КОТОРОЕ ТЫ ГОВОРИШЬ, ЧТО ИСПРАВИЛ». ─┐
│ 01.09 я закрыл дверь, через которую прогоны тестов клеймили живые слова, снял 162     │
│ ложные метки, написал тест и сказал «готово». 04.09 владелец открыл тот же экран и    │
│ увидел те же слова с той же подписью. Дверь была закрыта честно, а обещание «меток   │
│ больше не появится» жило только в тексте коммита: никто не перепроверял его назавтра,│
│ и владелец узнал о нарушении сам, через три дня, разозлившись.                       │
│                                                                                      │
│ Решение владельца 04.09.2026: «готово» без зарегистрированного обещания не готово.   │
│ Каждая починка записывает сюда, ЧТО она обещает измеримо: запрос, ожидаемое число,   │
│ дата, как перемерить руками. Каждое утро все обещания проверяются, итог идёт строкой │
│ в утренний отчёт, нарушенное приходит отдельным письмом с кнопками. Владелец ничего  │
│ не вызывает и не помнит. Вопрос агенту в моменте один: «какое обещание ты           │
│ зарегистрировал?». Нет обещания — работа не принята.                                 │
└──────────────────────────────────────────────────────────────────────────────────────┘

Три исхода проверки, и путать их нельзя:
  держится    — измерили, число совпало с обещанным;
  нарушено    — измерили, число НЕ совпало: починка откатилась или не работала никогда;
  не измерено — проверка сама не отработала (база, исключение). Это НЕ «держится» и
                НЕ «нарушено», это отдельный исход, и он тоже идёт владельцу вслух.

Обещание снимается только владельцем, кнопкой. Снятое не проверяется и не показывается
числом «держится»: оно просто не входит в реестр. Не снятое и нарушенное приходит
каждое утро, пока держится или не снято, — молчание не согласие.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

HELD, BROKEN, UNMEASURED = "held", "broken", "unmeasured"


@dataclass(frozen=True)
class Promise:
    key: str                    # короткий ключ, уникален, идёт в callback кнопки
    title: str                  # что обещано, человеческими словами
    since: str                  # дата обещания, ДД.ММ.ГГГГ
    expected: int               # обещанное число
    measure: Callable[[], int]  # как система его считает
    how: str                    # как перемерить руками, чтобы не верить на слово


# ── измерители ────────────────────────────────────────────────────────────────────────

_OLD_BANK_TRACE_KEYS = ("enrich_attempts", "enrich_last_reason", "quarantine_releases",
                        "quarantine_released_at", "quarantine_owner_keep",
                        "quarantine_owner_keep_at")


def _old_bank_quarantine_traces() -> int:
    """Сколько строк старого банка словаря несут след карантина. Обещано: 0.

    Клеймо ставили только прогоны тестов (01.09, 04.09), и в проде его ставить нечем с
    04.09.2026 — функция снесена. Появится хоть одна строка, значит либо метку вернул
    новый код, либо деплой откатился на старый."""
    from backend.database import get_db_connection_context
    условие = " OR ".join(f"response_json ? '{k}'" for k in _OLD_BANK_TRACE_KEYS)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM bt_3_dictionary_entries WHERE {условие}")
            return int((cursor.fetchone() or [0])[0] or 0)


def _night_enrichment_runs_in_units_mode() -> int:
    """Последний ночной добор шёл по слою слов (mode=units). Обещано: 1.

    Это и есть гарантия, что ветка по старому банку в проде не выполняется. Если журнал
    прогона пуст, измерить нечего — это исход «не измерено», а не «нарушено»."""
    from backend.database import get_latest_scheduler_run_guard
    row = get_latest_scheduler_run_guard(job_key="pool_night_enrichment")
    if not row:
        raise RuntimeError("журнал ночного добора пуст: прогона не было")
    режим = str(((row or {}).get("metadata") or {}).get("mode") or "")
    return 1 if режим == "units" else 0


_WN_OLD_LOOK = ((".worldnews-card-de", "Georgia"), (".worldnews-step", "clip-path"))


def _count_worldnews_old_look_rules(css: str) -> int:
    """Сколько правил СТАРОГО вида карточки слова «Новость дня» осталось в CSS.

    Старый вид (владелец 04.09.2026: «бедный, блеклый, некачественный») узнаётся по
    двум приметам: газетная Georgia у заголовка .worldnews-card-de и стрелки-шевроны
    (clip-path) у шагов .worldnews-step. Нет самого селектора заголовка — это не тот
    файл, и считать нечего: исход «не измерено», а не «0»."""
    import re
    if not re.search(r"\.worldnews-card-de\s*\{", css):
        raise LookupError("в CSS нет .worldnews-card-de — это не собранный фронт")
    n = 0
    for selector, marker in _WN_OLD_LOOK:
        for m in re.finditer(re.escape(selector) + r"\s*\{([^}]*)\}", css):
            if marker in m.group(1):
                n += 1
    return n


def _access_period_night_sweep() -> int:
    """Сколько начал отсчёта бесплатного месяца за сутки поставила ночная страховка, а
    не дверь записи. Обещано: 0.

    Дверей четыре: /start, первое сообщение в боте, самозапись по ссылке, открытие
    приложения. Строка от страховки значит, что человек прошёл мимо всех четырёх —
    искать, какая молчит (source в bt_3_access_period у соседей подскажет)."""
    from backend.database import count_access_periods_created_by_night_sweep
    return count_access_periods_created_by_night_sweep(days=1)


def _worldnews_card_old_look_rules() -> int:
    """Читает CSS собранного фронта (frontend/dist/assets/*.css) на сервере. Обещано: 0."""
    from pathlib import Path
    assets = Path(__file__).resolve().parent.parent / "frontend" / "dist" / "assets"
    files = sorted(assets.glob("*.css"))
    if not files:
        raise FileNotFoundError(f"собранного CSS нет: {assets}")
    return _count_worldnews_old_look_rules("".join(f.read_text(encoding="utf-8") for f in files))


# ── реестр ────────────────────────────────────────────────────────────────────────────
# Добавляя починку — добавляй строку сюда. Ключ не менять после регистрации: по нему
# лежат журнал проверок и решение владельца.

PROMISES: tuple[Promise, ...] = (
    Promise(
        key="old_bank_quarantine_traces",
        title="Следов карантина в старом банке словаря",
        since="04.09.2026",
        expected=0,
        measure=_old_bank_quarantine_traces,
        how="python3 scripts/pool_quarantine_drop_ghost_marks.py (сухой прогон печатает число)",
    ),
    Promise(
        key="night_enrichment_units_mode",
        title="Ночной добор идёт по слою слов, а не по старому банку",
        since="04.09.2026",
        expected=1,
        measure=_night_enrichment_runs_in_units_mode,
        how="SELECT metadata->>'mode' FROM bt_3_scheduler_run_guards "
            "WHERE job_key='pool_night_enrichment' — ждём units",
    ),
    Promise(
        key="worldnews_card_old_look",
        title="Карточка слова «Новость дня» собрана в новом виде (без Georgia и шевронов)",
        since="04.09.2026",
        expected=0,
        measure=_worldnews_card_old_look_rules,
        how="grep -c 'Georgia' frontend/dist/assets/*.css рядом с .worldnews-card-de "
            "и 'clip-path' рядом с .worldnews-step — ждём 0 и 0",
    ),
    Promise(
        key="access_period_night_sweep",
        title="Людей, кому начало бесплатного месяца поставила ночная страховка, а не дверь",
        since="04.09.2026",
        expected=0,
        measure=_access_period_night_sweep,
        how="SELECT COUNT(*) FROM bt_3_access_period WHERE source='night_sweep' "
            "AND created_at > NOW() - interval '1 day'",
    ),
)


def by_key(key: str) -> Promise | None:
    for p in PROMISES:
        if p.key == key:
            return p
    return None


# ── хранилище: журнал проверок и решения владельца ────────────────────────────────────

def _ensure_tables(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_3_fix_promise_checks (
            id          BIGSERIAL PRIMARY KEY,
            promise_key TEXT NOT NULL,
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            value       INTEGER,
            status      TEXT NOT NULL,          -- held | broken | unmeasured
            error       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bt_3_fix_promise_checks_key
            ON bt_3_fix_promise_checks (promise_key, checked_at DESC);
        CREATE TABLE IF NOT EXISTS bt_3_fix_promise_state (
            promise_key TEXT PRIMARY KEY,
            muted_at    TIMESTAMPTZ,
            muted_by    BIGINT
        );
        """
    )


def muted_keys() -> set[str]:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute("SELECT promise_key FROM bt_3_fix_promise_state WHERE muted_at IS NOT NULL")
            return {str(r[0]) for r in (cursor.fetchall() or [])}


def mute(key: str, admin_id: int) -> bool:
    """Решение владельца: обещание снято. Остаётся след — кто и когда."""
    if not by_key(key):
        return False
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute(
                """
                INSERT INTO bt_3_fix_promise_state (promise_key, muted_at, muted_by)
                VALUES (%s, NOW(), %s)
                ON CONFLICT (promise_key) DO UPDATE SET muted_at = NOW(), muted_by = EXCLUDED.muted_by
                """,
                (key, int(admin_id)),
            )
        conn.commit()
    return True


def unmute(key: str) -> bool:
    if not by_key(key):
        return False
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute("DELETE FROM bt_3_fix_promise_state WHERE promise_key = %s", (key,))
        conn.commit()
    return True


def _record(results: list[dict]) -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            for r in results:
                cursor.execute(
                    "INSERT INTO bt_3_fix_promise_checks (promise_key, value, status, error) "
                    "VALUES (%s, %s, %s, %s)",
                    (r["key"], r.get("value"), r["status"], (r.get("error") or "")[:300] or None),
                )
        conn.commit()


# ── проверка ──────────────────────────────────────────────────────────────────────────

def check_all(*, record: bool = True, promises: tuple[Promise, ...] | None = None,
              muted: set[str] | None = None) -> list[dict]:
    """Прогнать все обещания. Снятые владельцем не измеряются и не входят в итог.

    Исключение внутри измерителя — исход «не измерено», а не падение всей проверки:
    одно сломанное обещание не имеет права спрятать остальные."""
    реестр = PROMISES if promises is None else promises
    снятые = muted_keys() if muted is None else muted
    итог: list[dict] = []
    for p in реестр:
        if p.key in снятые:
            continue
        запись = {"key": p.key, "title": p.title, "since": p.since,
                  "expected": p.expected, "how": p.how, "value": None, "error": ""}
        try:
            значение = int(p.measure())
            запись["value"] = значение
            запись["status"] = HELD if значение == p.expected else BROKEN
        except Exception as exc:  # исход «не измерено» — отдельный, см. шапку модуля
            logging.warning("обещание %s не измерено: %s", p.key, exc, exc_info=True)
            запись["status"] = UNMEASURED
            запись["error"] = str(exc)[:200] or exc.__class__.__name__
        итог.append(запись)
    if record and итог:
        try:
            _record(итог)
        except Exception:
            # Журнал — не сама проверка: не записалось — сказали в лог, отчёт всё равно уйдёт.
            logging.exception("журнал обещаний не записался")
    return итог


def _esc(text: str) -> str:
    from html import escape
    return escape(str(text or ""))


def _plural(n: int, one: str, few: str, many: str) -> str:
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def report_lines(results: list[dict]) -> list[str]:
    """Строки для утреннего отчёта. Первая — итог одним взглядом, дальше только то,
    что требует человека: нарушенное и не измеренное. Держащееся не перечисляется."""
    if not results:
        return ["🤝 Обещаний в реестре нет."]
    держится = [r for r in results if r["status"] == HELD]
    нарушено = [r for r in results if r["status"] == BROKEN]
    не_измерено = [r for r in results if r["status"] == UNMEASURED]
    части = []
    if держится:
        n = len(держится)
        части.append(f"<b>{n}</b> {_plural(n, 'держится', 'держатся', 'держатся')}")
    if нарушено:
        части.append(f"<b>{len(нарушено)}</b> нарушено")
    if не_измерено:
        части.append(f"<b>{len(не_измерено)}</b> не измерено")
    строки = ["🤝 Обещания: " + ", ".join(части) + ("." if not (нарушено or не_измерено) else ":")]
    for r in нарушено:
        строки.append(
            f"   ⛔ {_esc(r['title'])}: обещано <b>{r['expected']}</b>, сейчас <b>{r['value']}</b> "
            f"(обещание от {_esc(r['since'])})"
        )
    for r in не_измерено:
        строки.append(f"   ❓ {_esc(r['title'])}: проверка не отработала — {_esc(r['error'])}")
    return строки


def full_lines(results: list[dict], muted: set[str]) -> list[str]:
    """Полный список для команды /admin_promises: каждое обещание, число, как перемерить."""
    значок = {HELD: "✅", BROKEN: "⛔", UNMEASURED: "❓"}
    строки = ["🤝 <b>Реестр обещаний</b>", ""]
    for r in results:
        сейчас = r["value"] if r["value"] is not None else "—"
        строки.append(f"{значок[r['status']]} <b>{_esc(r['title'])}</b> — обещано {r['expected']}, "
                      f"сейчас {сейчас} (с {_esc(r['since'])})")
        if r["status"] == UNMEASURED:
            строки.append(f"   не отработало: {_esc(r['error'])}")
        строки.append(f"   перемерить: <code>{_esc(r['how'])}</code>")
    for p in PROMISES:
        if p.key in muted:
            строки.append(f"🔕 <b>{_esc(p.title)}</b> — снято владельцем, не проверяется")
    if not results and not muted:
        строки.append("Реестр пуст.")
    return строки



def broken_alert(r: dict) -> tuple[str, dict]:
    """Письмо владельцу об одном нарушенном или не измеренном обещании — с кнопками.

    Кнопок две: снять обещание (решение: больше не следим) и держать дальше (придёт снова
    завтра). Без нажатия — как «держать»: молчание не снимает обещание."""
    if r["status"] == BROKEN:
        text = (
            f"⛔ <b>Обещание нарушено</b>\n\n"
            f"{_esc(r['title'])}: обещано <b>{r['expected']}</b>, сейчас <b>{r['value']}</b>.\n"
            f"Обещание от {_esc(r['since'])}. Починка либо откатилась, либо не работала.\n\n"
            f"Перемерить: <code>{_esc(r['how'])}</code>\n\n"
            f"<i>Ничего не нажать — тоже ответ: обещание остаётся, завтра проверю снова.</i>"
        )
    else:
        text = (
            f"❓ <b>Обещание не удалось проверить</b>\n\n"
            f"{_esc(r['title'])}: проверка не отработала — {_esc(r['error'])}.\n"
            f"Это не «держится» и не «нарушено», это отдельный исход: проверку надо чинить.\n\n"
            f"<i>Ничего не нажать — тоже ответ: завтра проверю снова.</i>"
        )
    markup = {"inline_keyboard": [[
        {"text": "👀 Держать дальше", "callback_data": f"fp:keep:{r['key']}"},
        {"text": "🔕 Снять обещание", "callback_data": f"fp:mute:{r['key']}"},
    ]]}
    return text, markup
