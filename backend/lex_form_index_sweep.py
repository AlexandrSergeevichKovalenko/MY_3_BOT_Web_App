# -*- coding: utf-8 -*-
"""Сверка указателей-словоформ со справочником спряжений — и ночная чистка.

ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ
────────────────────────
Владелец 01.09.2026 нажал слово в фильме и увидел в строке «ПЕРЕВОД» слово «рыется»,
которого в русском языке нет. Один из двух корней — наш собственный указатель форм: он
вёл голое написание НЕ К ТОМУ глаголу («ging» → ausgehen, «gräbt» → untergraben, «auf» →
34 глагола). Родилось это из нарезки ячейки парадигмы на слова: у отделяемого глагола
там напечатано «ging aus», и в указатели уезжали оба куска.

Дверь закрыта правилом `german_grammar_tables.form_token_of_cell`. Здесь — вторая
половина работы: разбор того, что уже натекло.

ПОЧЕМУ ЭТО МОДУЛЬ, А НЕ ТОЛЬКО СКРИПТ (владелец, 02.09.2026)
────────────────────────────────────────────────────────────
Сверку нельзя сделать один раз: она зависит от того, насколько вырос справочник. Пока
справочник не знает базового глагола, кривой указатель ДОКАЗАТЬ нечем, и он остаётся
жить. Справочник растёт каждую ночь — значит и сверка обязана идти каждую ночь, сразу
следом. Иначе выходит ровно то, что владелец запретил: «через одиннадцать ночей
справочник дорастёт, а указатели так и останутся кривыми, пока кто-то не вспомнит».

Поэтому: логика здесь, а `scripts/dict_units_forms_confirm.py` — тонкая обёртка для
человека. Ночью её зовёт `background_jobs.run_verb_paradigm_warm_actor` СРАЗУ ПОСЛЕ
прогрева — не по расписанию в соседний час, а следом в том же прогоне: так порядок
«сначала вырос справочник, потом сверка» гарантирован, а не угадан по времени.

ЧЕМ СУДИМ (источник называется вслух)
─────────────────────────────────────
`bt_3_german_verb_paradigms` — таблицы со страниц Flexion:<глагол> de.wiktionary.org.
Правило одно:

    форма принадлежит глаголу, только если она НАПЕЧАТАНА ЦЕЛОЙ ЯЧЕЙКОЙ его таблицы.

У «ausgehen» это «ging aus», «geht aus», «ausgegangen»; одиночного «ging» там нет. Так
же печатают лидеры: dict.cc — «eingehen | ging ein | eingegangen», Wiktionary заводит у
формы статью «Konjugierte Form … des Verbs gehen», PONS — «ging → von gehen», Duden —
«ging, siehe gehen».

Строки под ключом «модель:…» в судьи НЕ допускаются. Это подтверждённый двумя спросами
ответ модели: им можно ПОКАЗАТЬ таблицу, но нельзя УДАЛЯТЬ чужие данные.

ЧТО СНОСИМ, А ЧТО НЕТ — И ПОЧЕМУ ЭТО РАЗНОЕ
───────────────────────────────────────────
«В таблице не напечатано» и «не является формой» — РАЗНЫЕ утверждения. Проверка
01.09.2026: «gehauen» (настоящее причастие от hauen) в таблице отсутствует, там только
«gehaut»; «auszulaugen» — настоящий zu-инфинитив, но строки zu-инфинитива в нашем
разборе таблицы нет; «umzingele», «heuchele» — настоящие варианты 1-го лица. Снести их
значило бы повторить ошибку «сторож работал идеально и резал нужное».

Поэтому сносится ТОЛЬКО доказанное И объяснённое — где назван настоящий владелец:

    A. написание — отделяемая приставка САМОЙ леммы («auf» у «aufzeichnen»);
    B. написание напечатано целой ячейкой у БАЗОВОГО глагола, а лемма — его
       приставочный родич («ging» напечатано у «gehen», лемма «zugehen» им кончается).

Остальное живёт дальше и попадает в отчёт: B2 (базового глагола нет в справочнике),
D (владельца назвать не смогли), «не спрашивали».
"""
from __future__ import annotations

import collections
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from backend.german_grammar_tables import split_separable_verb
from backend.german_verb_paradigms import whole_cell_forms

# Ответы модели лежат в той же таблице под своим ключом и в судьи не допускаются.
_MODEL_KEY_PREFIX = "модель:"

CONFIRMED = "подтверждено справочником"
CLASS_A = "A: приставка леммы, а не форма"
CLASS_B = "B: форма базового глагола (доказано)"
CLASS_B2 = "B2: форма базового глагола (базового нет в справочнике)"
CLASS_D = "D: не напечатано, владельца не назвали"
NO_REF = "про этот глагол справочник не спрашивали"

CLASS_ORDER = (CONFIRMED, CLASS_A, CLASS_B, CLASS_B2, CLASS_D, NO_REF)
DELETABLE = (CLASS_A, CLASS_B)

REPORT_JOB_KEY = "lex_form_index_report"
_HTTP_TIMEOUT = 20


def ensure_form_index_run_schema() -> None:
    """Журнал ночных сверок: без него недельная строчка не может сказать «за неделю».

    Владелец 02.09.2026 просил в отчёте ДЕЛЬТУ — «справочник вырос на столько-то, снято
    столько-то». Дельту неоткуда взять из текущего состояния: чтобы сказать «за неделю»,
    надо помнить, что было неделю назад. Строчка на прогон, четыре числа, больше ничего.
    """
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS bt_3_lex_form_index_runs (
                       id BIGSERIAL PRIMARY KEY,
                       ran_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                       reference_verbs INTEGER NOT NULL,
                       pointers INTEGER NOT NULL,
                       removed INTEGER NOT NULL,
                       unresolved INTEGER NOT NULL
                   );"""
            )
            cur.execute(
                """CREATE INDEX IF NOT EXISTS bt_3_lex_form_index_runs_ran_at_idx
                       ON bt_3_lex_form_index_runs (ran_at DESC);"""
            )
        conn.commit()


def _remember_run(report: dict[str, Any]) -> None:
    """Записать прогон в журнал. Молча не падаем — но и не притворяемся, что записали."""
    from backend.database import get_db_connection_context

    classes = report.get("classes") or {}
    unresolved = classes.get(CLASS_B2, 0) + classes.get(CLASS_D, 0) + classes.get(NO_REF, 0)
    try:
        ensure_form_index_run_schema()
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO bt_3_lex_form_index_runs
                           (reference_verbs, pointers, removed, unresolved)
                       VALUES (%s, %s, %s, %s);""",
                    (int(report.get("reference_verbs") or 0), int(report.get("pointers") or 0),
                     int(report.get("removed") or 0), int(unresolved)),
                )
            conn.commit()
    except Exception:
        logging.exception("журнал сверки указателей: запись не удалась")


def week_totals() -> dict[str, int]:
    """Что изменилось за последние семь суток. Пусто — журнал ещё не набрался."""
    from backend.database import get_db_connection_context

    try:
        ensure_form_index_run_schema()
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COALESCE(sum(removed), 0), count(*),
                              COALESCE(min(reference_verbs), 0)
                         FROM bt_3_lex_form_index_runs
                        WHERE ran_at >= now() - interval '7 days';"""
                )
                removed, runs, reference_then = cur.fetchone()
        return {"removed": int(removed), "runs": int(runs),
                "reference_then": int(reference_then)}
    except Exception:
        logging.exception("журнал сверки указателей: чтение не удалось")
        return {}


def load_reference(cur) -> dict[str, set[str]]:
    """Глагол → множество форм, напечатанных ЦЕЛЫМИ ячейками."""
    cur.execute(
        """SELECT verb, tables FROM bt_3_german_verb_paradigms
            WHERE documented AND verb NOT LIKE %s;""",
        (_MODEL_KEY_PREFIX + "%",),
    )
    # Ключ — casefold, ТОТ ЖЕ, что у написаний в bt_3_lex_surfaces
    # (`lex_units.normalize_query`). SQL-функция lower() оставляет «ß» как есть, а
    # casefold превращает его в «ss» — из-за этого расхождения «aufgießen» не
    # находился в справочнике и его указатель «auf» переживал чистку (01.09.2026).
    return {str(verb).casefold(): whole_cell_forms(tables)
            for verb, tables in cur.fetchall() if isinstance(tables, dict)}


def load_known_verbs(cur) -> set[str]:
    """Написания, которые мы вправе считать немецкими глаголами.

    Нужны, чтобы отличить «форму базового глагола» от случайного совпадения: базовый
    глагол обязан быть настоящим словом, а не отрезанным хвостом леммы."""
    cur.execute("SELECT DISTINCT lower(lemma) FROM bt_base_dictionary "
                "WHERE source_lang = 'de' AND pos = 'verb';")
    known = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT lower(display) FROM bt_3_lex_units "
                "WHERE lang = 'de' AND pos = 'verb';")
    known |= {r[0] for r in cur.fetchall()}
    return known


def build_owner_index(reference: dict[str, set[str]]) -> dict[str, set[str]]:
    """Написание → глаголы, у которых оно напечатано ОТДЕЛЬНОЙ целой ячейкой."""
    owner_of: dict[str, set[str]] = collections.defaultdict(set)
    for verb, cells in reference.items():
        for cell in cells:
            if " " not in cell:
                owner_of[cell].add(verb)
    return owner_of


def classify(surface: str, lemma: str, reference: dict[str, set[str]],
             owner_of: dict[str, set[str]], known_verbs: set[str]) -> tuple[str, str]:
    """Класс указателя и НАСТОЯЩИЙ владелец написания (пустая строка — не назван)."""
    # Разбор на приставку делается по ИСХОДНОМУ написанию, а не по casefold: в Python
    # «ß».casefold() == «ss», и «aufgießen» превращается в «aufgiessen». Основа «giessen»
    # в справочнике не значится (там «gießen»), разбор срывается, и «auf» оставалось
    # висеть указателем на глагол. Поймано на живых данных 01.09.2026.
    original_lemma = str(lemma or "").strip()
    surface = surface.casefold()
    lemma = lemma.casefold()
    cells = reference.get(lemma)
    if cells is None:
        return NO_REF, ""
    if surface in cells:
        return CONFIRMED, lemma
    # A. Написание — отделяемая приставка самой леммы: «auf» у «aufzeichnen».
    #
    # Сравнивать НАЧАЛО СТРОКИ здесь нельзя, и это проверено на живых данных 01.09.2026:
    # «durchziehe» — тоже начало слова «durchziehen», но это НАСТОЯЩАЯ форма 1-го лица,
    # а не приставка. Приставку берём у разбора `split_separable_verb` — там список
    # собран прогоном по 439 отделяемым глаголам справочника, а не на глаз.
    prefix, _base = split_separable_verb(original_lemma)
    if prefix and surface == prefix.casefold():
        return CLASS_A, surface
    # B. Написание напечатано у базового глагола, а лемма им заканчивается.
    bases = sorted(v for v in owner_of.get(surface, ()) if v != lemma and lemma.endswith(v))
    if bases:
        return CLASS_B, bases[0]
    # B2. Базовый глагол виден, но справочник о нём молчит — доказать нечем.
    tail = ""
    for start in range(1, len(lemma) - 2):
        rest = lemma[start:]
        if rest in known_verbs and rest not in reference and len(rest) > len(tail):
            tail = rest
    if tail:
        return CLASS_B2, tail
    return CLASS_D, ""


def sweep_form_index(*, apply: bool = False, sample: int = 0) -> dict[str, Any]:
    """Сверить указатели-словоформы со справочником; с apply — снести доказанные.

    Ничего не «предполагает»: пока справочник о глаголе молчит, указатель остаётся на
    месте и считается отдельно. Возвращает отчёт числами — он и уходит владельцу.
    """
    from backend.database import get_db_connection_context

    report: dict[str, Any] = {
        "reference_verbs": 0, "pointers": 0, "removed": 0,
        "classes": {name: 0 for name in CLASS_ORDER}, "samples": [],
    }
    with get_db_connection_context() as conn:
        cur = conn.cursor()
        reference = load_reference(cur)
        known_verbs = load_known_verbs(cur) | set(reference)
        owner_of = build_owner_index(reference)
        cur.execute(
            """SELECT s.surface_key, u.display, s.unit_id
                 FROM bt_3_lex_surfaces s
                 JOIN bt_3_lex_units u ON u.id = s.unit_id
                WHERE s.lang = 'de' AND s.match_kind = 'inflected' AND u.pos = 'verb';"""
        )
        rows = cur.fetchall()
        report["reference_verbs"] = len(reference)
        report["pointers"] = len(rows)

        doomed: list[tuple[str, int, str, str]] = []
        for surface, display, unit_id in rows:
            verdict, owner = classify(surface, str(display), reference, owner_of, known_verbs)
            report["classes"][verdict] = report["classes"].get(verdict, 0) + 1
            if verdict in DELETABLE:
                doomed.append((surface, int(unit_id), str(display), owner))
        if sample:
            report["samples"] = [
                "%s → %s (настоящий владелец: %s)" % (surface, display, owner or "не назван")
                for surface, _uid, display, owner in doomed[:sample]
            ]
        report["deletable"] = len(doomed)

        if apply and doomed:
            removed = 0
            for surface, unit_id, _display, _owner in doomed:
                cur.execute(
                    """DELETE FROM bt_3_lex_surfaces
                        WHERE lang = 'de' AND match_kind = 'inflected'
                          AND surface_key = %s AND unit_id = %s;""",
                    (surface, unit_id),
                )
                removed += cur.rowcount
            conn.commit()
            report["removed"] = removed
    if apply:
        # Пишем ВСЕГДА, даже когда сняли ноль: «ноль» — тоже факт недели, а пропуск
        # строки сделал бы недельную дельту неотличимой от «сверка не запускалась».
        _remember_run(report)
    return report


def _ночей(n: int) -> str:
    """«1 ночь», «3 ночи», «11 ночей». Письмо человеку, а не отладочный вывод."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "ночей"
    return {1: "ночь", 2: "ночи", 3: "ночи", 4: "ночи"}.get(n % 10, "ночей")


def build_form_index_report_text(report: dict[str, Any] | None = None) -> str:
    """Письмо владельцу — ЧЕЛОВЕЧЕСКИМ ЯЗЫКОМ.

    ⛔ ПЕРВАЯ ВЕРСИЯ ЭТОГО ТЕКСТА БЫЛА БРАКОМ. 02.09.2026 владелец получил её и ответил:
    «Я вообще не понимаю, к чему?! мы ж вроде говорили про спорные фразы или нет??».
    Там стояли «указатели-словоформ», «подтверждено справочником», «недоказанных 294» —
    мои рабочие слова, по которым нельзя ни понять, о чём речь, ни решить, надо ли
    что-то делать.

    Правила, по которым он переписан (владелец, давние и повторённые):
      • сначала СМЫСЛ обычными словами: про что это письмо и почему оно пришло;
      • сырые внутренние числа не выносить — только то, по чему принимают решение;
      • у каждого числа сказано, хорошо это или плохо и что с этим делать;
      • в конце прямо написано, нужно ли ему шевелиться. Обычно — нет.
    """
    from backend.database import get_db_connection_context

    if report is None:
        report = sweep_form_index(apply=False)
    with get_db_connection_context() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT count(*) FILTER (WHERE documented AND verb NOT LIKE %(model)s)
                 FROM bt_3_german_verb_paradigms;""",
            {"model": _MODEL_KEY_PREFIX + "%"},
        )
        tables_now = int(cur.fetchone()[0])
        from backend.german_verb_paradigms import pending_paradigm_verbs
        waiting = len(pending_paradigm_verbs())

    classes = report.get("classes") or {}
    total = int(report.get("pointers") or 0)
    good = int(classes.get(CONFIRMED, 0))
    unresolved = (classes.get(CLASS_B2, 0) + classes.get(CLASS_D, 0) + classes.get(NO_REF, 0))
    share = round(good * 100 / total) if total else 0
    nights = (waiting + 199) // 200 if waiting else 0
    week = week_totals()

    lines = [
        "🔤 <b>Словарь: формы слов</b>",
        "",
        "Про что это письмо. Человек нажимает в фильме или в книге слово в форме — "
        "«wühlt», «ging» — и должен получить само слово: wühlen, gehen. Отчёт следит, "
        "чтобы эта связь держалась. <i>Со спорными фразами это не связано, они приходят "
        "отдельным письмом.</i>",
        "",
        f"<b>Связей «форма → слово»: {total}.</b>",
        f"Из них проверено и верно — {good} ({share}%).",
    ]
    if unresolved:
        lines += [
            f"Ещё {unresolved} проверить пока нечем: у нас нет таблицы спряжения их "
            "глагола. Мы их не удаляем — среди них есть настоящие формы, и снести их "
            "значило бы потерять слова.",
        ]
    else:
        lines += ["Непроверенных не осталось."]
    lines += [""]
    if waiting:
        lines += [f"Таблицы спряжения дозагружаются сами, по ночам. Осталось примерно "
                  f"{nights} {_ночей(nights)} — после этого оставшиеся связи проверятся сами."]
    else:
        lines += ["Таблицы спряжения добраны полностью — дозагружать больше нечего."]
    if week.get("runs"):
        grew = max(tables_now - week.get("reference_then", tables_now), 0)
        lines += [f"За эту неделю: таблиц прибавилось {grew}, кривых связей убрано "
                  f"{week['removed']}."]
    else:
        lines += ["За эту неделю: это первое письмо, сравнивать пока не с чем — "
                  "числа появятся в следующем."]
    lines += [
        "",
        "<b>Делать ничего не нужно.</b> Позовите меня, только если через две недели "
        "число непроверенных не уменьшится: значит ночная работа встала.",
    ]
    return "\n".join(lines)


def send_form_index_report(*, force: bool = False) -> dict[str, Any]:
    """Отправить недельную строчку админам. Тем же путём, что и отчёт Wiktionary."""
    from backend.database import (
        claim_scheduler_run_guard, finish_scheduler_run_guard, get_admin_telegram_ids,
    )

    run_period = (datetime.now(timezone.utc).date()).isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=REPORT_JOB_KEY, run_period=run_period, target_scope="global", metadata={},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admins = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admins:
            return {"ok": False, "error": "no_token_or_admins"}
        text = build_form_index_report_text()
        sent = 0
        failures: list[str] = []
        for uid in admins:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code >= 400:
                logging.warning("отчёт по указателям форм не ушёл uid=%s: %s", uid, resp.text[:200])
                failures.append("%s: HTTP %s" % (uid, resp.status_code))
            else:
                sent += 1

        # ⛔ НОЛЬ ОТПРАВЛЕННЫХ — ЭТО ПРОВАЛ, А НЕ УСПЕХ.
        #
        # Поймано 02.09.2026 на живой проверке: токен оказался не боевым, Telegram
        # ответил 401, письмо не ушло НИКОМУ — а функция вернула {"ok": True, "sent": 0}
        # и пометила прогон «completed». То есть в понедельник отчёт мог не прийти, и
        # никто бы об этом не узнал: в журнале стояло бы «выполнено».
        #
        # Это ровно то, что владелец запретил 19.08.2026: «молчащий механизм неотличим
        # от сломанного». Адресаты есть, письмо не ушло — говорим это словом.
        if not sent:
            reason = "; ".join(failures) or "адресатов нет"
            if not force:
                finish_scheduler_run_guard(job_key=REPORT_JOB_KEY, run_period=run_period,
                                           target_scope="global", status="failed",
                                           metadata={"sent": 0, "error": reason})
            logging.error("отчёт по указателям форм НЕ ДОСТАВЛЕН ни одному админу: %s", reason)
            return {"ok": False, "sent": 0, "error": reason}

        if not force:
            finish_scheduler_run_guard(job_key=REPORT_JOB_KEY, run_period=run_period,
                                       target_scope="global",
                                       status="completed" if not failures else "partial",
                                       metadata={"sent": sent, "failed": failures})
        return {"ok": True, "sent": sent, "failed": failures}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=REPORT_JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("отчёт по указателям форм не собрался")
        return {"ok": False, "error": str(exc)}
