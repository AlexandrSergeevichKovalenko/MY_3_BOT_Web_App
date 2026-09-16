# -*- coding: utf-8 -*-
"""ПЕРЕВОД ИЗ КАРТОЧКИ ПОДНИМАЕТСЯ В ОБЩИЙ СЛОЙ — с проверкой, а не молча.

ЧТО БЫЛО СЛОМАНО (разобрано с владельцем 27.08.2026).

Одна и та же фраза живёт в двух местах. В личной карточке немецкое и русское лежат
рядом, одной строкой: «Schwein haben» — «повезти (разг.)». В общем слое иначе: немецкая
фраза — отдельная запись, русский перевод — ДРУГАЯ запись, а между ними ставится связь
(`bt_3_lex_links`), которая и говорит «вот это перевод вот этого».

Дверь сохранения (`lex_units.attach_entry_to_unit`) кладёт на склад только немецкую
половину и тянет связь ТОЛЬКО из разбора — из значений, сочинённых моделью. Русский,
с которым карточку сохранили, в общий слой не поднимался НИКОГДА: строчки кода для
этого не было. А у фразы разбора обычно и не бывает — ночной добор (`units_needing_card`)
берёт только одиночные слова. Круг замкнулся: связь тянется из разбора, разбора у фразы
нет, значит связи не будет никогда.

ЧЕМ ЭТО ПЛОХО. Ночная проверка грамматики отбирает фразы ПО СВЯЗИ — иначе судья не видит
смысла, а предлог и падеж выбираются по смыслу («Wappnen mit» оба судьи потребовали
переписать в `gegen` именно вслепую). Нет связи — фраза не попадает в проверку вообще.
Замер 27.08.2026: 1 216 единиц без связи (781 предложение, 403 оборота, 32 слова), и
дыра открыта — за неделю 345 новых фраз из 404 ушли мимо, это 85%.

ПОЧЕМУ ПЕРЕВОД НЕЛЬЗЯ ПОДНЯТЬ ПРОСТО ТАК. Владелец, 27.08.2026, дословно: «человек не
вписывает перевод сам — он пишет боту слово, модель переводит, он выбирает вариант и
жмёт сохранить. Это перевод машинный». Значит в общий слой уходит НЕПРОВЕРЕННЫЙ машинный
текст, который увидят другие люди. Поэтому перед подъёмом — вопрос модели «этот русский
действительно означает эту немецкую фразу?» (`openai_manager.run_translation_pair_check`).

ЧТО ПРОИСХОДИТ ДАЛЬШЕ, ТРИ ИСХОДА И НИ ОДНОГО МОЛЧАЛИВОГО:
  • проверка ЗА  → связь ранга 5, подпись «перевод карточки». Ниже вычитки владельца
    (ранг 1) и выше пула (ранг 10) — так решил владелец 27.08.2026;
  • проверка ПРОТИВ → связи нет, а фраза уходит вопросом владельцу в очередь карточек
    словаря (та, что приходит по вторникам и пятницам), с его переводом и причиной
    отказа. Решает он: принять как есть, вписать свой, оставить личным, удалить;
  • спросить НЕ УДАЛОСЬ → не пишем ничего и вопрос не заводим. Это «не проверяли», а не
    «плохо»: фраза остаётся кандидатом и попадёт в следующий проход. Счётчик в отчёт.
"""
from __future__ import annotations

import json
import logging
import os

LINK_SOURCE = "перевод карточки"
LINK_RANK = 5                    # ниже «вычитки» (1), выше пула (10) — владелец, 27.08.2026
NIGHT_CAP = int(os.getenv("TRANSLATION_LINK_CAP", "300") or "300")
# Потолок расхода за проход. Цена одной проверки ≈ $0.00011 (gpt-4.1-mini, снимки цен
# bt_3_billing_price_snapshots), то есть $0.30 хватает на ~2 700 фраз.
BUDGET_USD = float(os.getenv("TRANSLATION_LINK_BUDGET", "0.30") or "0.30")
PRICE_IN, PRICE_OUT = 0.4 / 1e6, 1.6 / 1e6


def units_missing_ru_link(limit: int) -> list[dict]:
    """Единицы общего слоя без русской связи, у которых перевод в карточке ЕСТЬ.

    Порядок — свежие первыми: дыра открыта, и вчерашнее сохранение важнее прошлогоднего.
    Берём самый частый перевод среди карточек этой единицы: если десять человек сохранили
    фразу с одним и тем же русским, это он и есть, а не случайная строка одного из них.
    """
    from backend.database import get_db_connection_context

    if limit <= 0:
        return []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT u.id, u.display, u.kind, t.translation, t.people
                  FROM bt_3_lex_units u
                  JOIN LATERAL (
                        SELECT TRIM(q.translation_ru) AS translation,
                               count(DISTINCT q.user_id) AS people
                          FROM bt_3_webapp_dictionary_queries q
                         WHERE q.lex_unit_id = u.id
                           AND COALESCE(TRIM(q.translation_ru), '') <> ''
                         GROUP BY TRIM(q.translation_ru)
                         ORDER BY count(DISTINCT q.user_id) DESC, TRIM(q.translation_ru)
                         LIMIT 1
                  ) t ON TRUE
                 WHERE u.lang = 'de'
                   AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                     JOIN bt_3_lex_units v ON v.id = l.to_unit
                                    WHERE l.from_unit = u.id AND v.lang = 'ru')
                   AND NOT EXISTS (SELECT 1 FROM bt_3_phrase_review r
                                    WHERE r.unit_id = u.id AND r.status = 'open')
                 ORDER BY u.id DESC
                 LIMIT %s;
                """,
                (int(limit),),
            )
            rows = cursor.fetchall() or []
    return [{"unit_id": int(r[0]), "display": r[1], "kind": r[2],
             "translation": r[3], "people": int(r[4] or 1)} for r in rows]


def _link_translation(unit_id: int, russian: str) -> bool:
    """Протянуть связь «немецкое → русское» в общем слое.

    Русская сторона заводится тем же `ensure_unit`, что и везде: своей вставкой её
    заводить нельзя — там нормализация ключа поиска, вид записи и указатель написаний.
    """
    from backend.database import get_db_connection_context
    from backend.lex_units import ensure_unit

    ru_unit = ensure_unit(str(russian or "").strip(), "ru")
    if not ru_unit:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (from_unit, to_unit)
                       DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank),
                                     updated_at = NOW();""",
                    (int(unit_id), int(ru_unit), LINK_RANK, LINK_SOURCE),
                )
            conn.commit()
        return True
    except Exception as exc:
        logging.warning("связь перевода для %s не записалась: %s", unit_id, exc)
        return False


def _ask_owner(unit_id: int, display: str, russian: str, why: str,
               better: str = "") -> bool:
    """Перевод не прошёл проверку — вопрос владельцу, в его очередь карточек словаря.

    Ложится в ту же таблицу, что и споры о грамматике, но со своей категорией: экран по
    ней понимает, что вопрос про ПЕРЕВОД, и рисует свои кнопки. Отдельной очереди не
    заводим сознательно — у владельца их уже шесть, седьмая означала бы седьмое место,
    куда надо не забыть заглянуть."""
    from backend.database import TRANSLATION_REVIEW_CATEGORY, get_db_connection_context

    # ⛔ ДИАГНОЗ БЕЗ ГОТОВОГО ВАРИАНТА — НЕ ОТВЕТ. Владелец 31.08.2026: «в немецком не
    # говорят так — ну окей, а как говорят?» Проверка теперь возвращает и правильный
    # русский (`run_translation_pair_check` → `better`), и он едет сюда: на экране это
    # кнопка в одно касание, а не задача «догадайся сам».
    # Поле названо прямо: экран по нему пишет, О ЧЁМ спор, и больше не сочиняет заголовок.
    judges = [{"verdict": "doubt", "category": TRANSLATION_REVIEW_CATEGORY,
               "field": "translation", "voice": 0,
               "fix": str(better or "").strip()[:300],
               "corrected": "", "proposal": "",
               "why": (why or "Проверка не подтвердила, что этот русский означает эту фразу.")[:400]}]
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO bt_3_phrase_review (unit_id, text, translation, judges, status)
                       VALUES (%s, %s, %s, %s::jsonb, 'open') ON CONFLICT DO NOTHING;""",
                    (int(unit_id), str(display or "")[:500], str(russian or "")[:500],
                     json.dumps(judges, ensure_ascii=False)),
                )
                added = bool(cursor.rowcount)
            conn.commit()
        return added
    except Exception as exc:
        logging.warning("вопрос о переводе для %s не поставлен: %s", unit_id, exc)
        return False


def promote_card_translations(*, limit: int | None = None,
                              budget_usd: float | None = None) -> dict:
    """Одна порция: поднять переводы карточек в общий слой. Возвращает отчёт числами."""
    from backend.openai_manager import _LAST_LLM_USAGE, run_translation_pair_check

    cap = int(limit if limit is not None else NIGHT_CAP)
    budget = float(budget_usd if budget_usd is not None else BUDGET_USD)
    report = {"взято": 0, "поднято": 0, "ушло владельцу": 0,
              "не смогли спросить": 0, "потрачено": 0.0}
    rows = units_missing_ru_link(cap)
    report["взято"] = len(rows)
    for row in rows:
        if report["потрачено"] >= budget:
            logging.info("подъём переводов: потолок $%.2f, остановились", budget)
            break
        verdict = run_translation_pair_check(
            german=row["display"], russian=row["translation"],
            kind=str(row.get("kind") or "collocation"))
        usage = _LAST_LLM_USAGE.get() or {}
        report["потрачено"] += (int(usage.get("prompt_tokens") or 0) * PRICE_IN
                                + int(usage.get("completion_tokens") or 0) * PRICE_OUT)
        if not verdict.get("checked"):
            # «Не спросили» — это не «плохо». Ничего не пишем и вопрос не заводим:
            # фраза остаётся кандидатом и придёт в следующий проход.
            report["не смогли спросить"] += 1
            continue
        if verdict.get("ok"):
            if _link_translation(row["unit_id"], row["translation"]):
                report["поднято"] += 1
            continue
        if _ask_owner(row["unit_id"], row["display"], row["translation"],
                      str(verdict.get("why") or ""),
                      str(verdict.get("better") or "")):
            report["ушло владельцу"] += 1
    report["потрачено"] = round(report["потрачено"], 4)
    report["осталось"] = count_units_missing_ru_link()
    logging.info("подъём переводов в общий слой: %s", report)
    return report


def count_units_missing_ru_link() -> int:
    """Сколько единиц ещё ждут подъёма. Число едет владельцу в утренний отчёт:
    молчащий механизм неотличим от сломанного."""
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT count(*) FROM bt_3_lex_units u
                    WHERE u.lang = 'de'
                      AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                        JOIN bt_3_lex_units v ON v.id = l.to_unit
                                       WHERE l.from_unit = u.id AND v.lang = 'ru')
                      AND EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q
                                   WHERE q.lex_unit_id = u.id
                                     AND COALESCE(TRIM(q.translation_ru), '') <> '');"""
            )
            return int((cursor.fetchone() or [0])[0])


# ─────────────────────────────────────────────────────────────────────────────
# ВОПРОС БЕЗ ГОТОВОГО ВАРИАНТА ОБЯЗАН БЫТЬ ДОСПРОШЕН. Владелец, 16.09.2026.
#
# ЧТО БЫЛО СЛОМАНО. Ключ `fix` («как правильно») появился в вопросе о переводе
# 31.08.2026. Всё, что легло в очередь раньше, осталось без него — и получить его
# уже не могло НИКАК: пересуд на экране скрыт для этого вида вопросов, ночной
# подъём не берёт единицу с открытым вопросом (`units_missing_ru_link`), вставка
# идёт `ON CONFLICT DO NOTHING`, а слепой массовый пересуд отфильтровывает всё
# непграмматическое. Замер 16.09.2026 на живой базе: в очереди владельца 68
# вопросов, у 15 из них варианта нет и взяться ему неоткуда (записи 27–31.08.2026).
# Владелец открывал такой вопрос и читал «Готового варианта голос не назвал» —
# при том, что модель за эту фразу уже оплачена, просто её тогда не спросили.
#
# ПОЧЕМУ НЕ «ПРОСТО СПРОСИТЬ ЕЩЁ РАЗ КАЖДУЮ НОЧЬ». Есть законный класс, где
# варианта не будет никогда: немецкого текста такого не существует («Soile»,
# «Ich bin zu für dich» — замер 16.09.2026, 3 записи), и чинить там нечего.
# Спрашивать про них ежедневно — жечь деньги на заведомо пустой ответ. Поэтому
# попытка помечается в самом голосе (`rejudge`), и повторной не будет: один
# доспрос на вопрос, дальше решает владелец.
#
# ЧЕГО ЗДЕСЬ НЕТ И НЕ БУДЕТ: перевод не сочиняется нами. Если модель варианта не
# назвала — на экране так и написано, кнопки нет.
REJUDGE_CAP = int(os.getenv("TRANSLATION_REJUDGE_CAP", "40") or "40")
REJUDGE_BUDGET_USD = float(os.getenv("TRANSLATION_REJUDGE_BUDGET", "0.05") or "0.05")

_ВОПРОС_БЕЗ_ВАРИАНТА = """
    r.status = 'open'
    AND COALESCE(r.kind, 'grammar') = 'translation'
    AND COALESCE(BTRIM(r.translation), '') <> ''
    -- ⛔ ОТЛИЧАЕМ «НЕ СПРАШИВАЛИ» ОТ «СПРОСИЛИ, И ВАРИАНТА НЕТ» — по наличию
    -- САМОГО КЛЮЧА, а не по пустоте текста. Запись, заведённая после 31.08.2026,
    -- всегда несёт `fix`: пустой он там значит, что модель ответила и варианта не
    -- назвала (немецкого такого нет — «Soile», «Ich bin zu für dich», замер
    -- 16.09.2026). Спрашивать её повторно — платить за известный ответ. У записей
    -- 27–31.08.2026 ключа нет вовсе: вот их и доспрашиваем.
    AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(r.judges) j
                     WHERE j->'fix' IS NOT NULL)
    AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(r.judges) j
                     WHERE j->'rejudge' IS NOT NULL)
"""


def translation_questions_without_fix(limit: int) -> list[dict]:
    """Открытые вопросы о переводе, которым готовый вариант ещё не спрашивали.

    Условие отбора — в `_ВОПРОС_БЕЗ_ВАРИАНТА`, оно же считается счётчиком ниже и
    проверяется обещанием: одно правило в одном месте, чтобы замер и работа не
    разъехались."""
    from backend.database import get_db_connection_context

    if limit <= 0:
        return []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""SELECT r.id, r.unit_id, r.text, r.translation,
                           COALESCE(u.kind, 'collocation')
                      FROM bt_3_phrase_review r
                      LEFT JOIN bt_3_lex_units u ON u.id = r.unit_id
                     WHERE {_ВОПРОС_БЕЗ_ВАРИАНТА}
                     ORDER BY r.id
                     LIMIT %s;""",
                (int(limit),),
            )
            rows = cursor.fetchall() or []
    return [{"review_id": int(r[0]), "unit_id": int(r[1] or 0), "text": r[2],
             "translation": r[3], "kind": r[4]} for r in rows]


def count_translation_questions_without_fix() -> int:
    """Сколько вопросов о переводе ждут доспроса. После ночного прохода — ноль.

    Ноль тут значит именно «каждый вопрос спросили»: запись, где модель варианта не
    назвала, из счёта уходит по метке `rejudge`, а не по наличию текста."""
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM bt_3_phrase_review r WHERE {_ВОПРОС_БЕЗ_ВАРИАНТА};")
            return int((cursor.fetchone() or [0])[0])


def голос_после_доспроса(judges: list, verdict: dict) -> dict:
    """Как выглядит голос после доспроса. Чистая сборка, без базы — её проверяет тест.

    Прежняя претензия не теряется — она переезжает в `why_before`. Голос остаётся
    ОДНИМ: два голоса об одном и том же нарисовали бы владельцу две одинаковые
    претензии на экране."""
    from datetime import datetime, timezone

    from backend.database import TRANSLATION_REVIEW_CATEGORY

    прежний = next((j for j in (judges or []) if isinstance(j, dict)), {})
    было = str(прежний.get("why") or "").strip()
    согласился = bool(verdict.get("ok"))
    вариант = "" if согласился else str(verdict.get("better") or "").strip()[:300]
    метка = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
             "ok": согласился,
             "why": str(verdict.get("why") or "").strip()[:400]}
    голос = {
        "verdict": "ok" if согласился else "doubt",
        "category": TRANSLATION_REVIEW_CATEGORY,
        "field": "translation",
        "voice": 0,
        "fix": вариант,
        "corrected": "", "proposal": "",
        # Претензия на экране — свежая, если доспрос её подтвердил; прежняя, если он
        # снял. Молча подменять текст нельзя: владелец видел старую формулировку.
        "why": (было if согласился
                else (str(verdict.get("why") or "").strip()[:400] or было)),
        "why_before": было,
        "rejudge": метка,
    }
    return голос


def _записать_пересуд(review_id: int, judges: list, verdict: dict) -> bool:
    """Положить собранный голос в строку вопроса. Строку, которую успели закрыть, не
    трогаем: `status = 'open'` в условии."""
    from backend.database import get_db_connection_context

    голос = голос_после_доспроса(judges, verdict)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                # Вид назван и здесь, а не только при чтении: перепутанный id иначе
                # переписал бы голоса чужого вопроса — панельного или грамматического.
                "UPDATE bt_3_phrase_review SET judges = %s::jsonb "
                "WHERE id = %s AND status = 'open' "
                "  AND COALESCE(kind, 'grammar') = 'translation';",
                (json.dumps([голос], ensure_ascii=False), int(review_id)))
            записано = cursor.rowcount or 0
        conn.commit()
    return bool(записано)


def rejudge_translation_question(review_id: int) -> dict:
    """Спросить заново по ОДНОМУ вопросу о переводе. Кнопка на экране и ночной добор.

    Исходов четыре, и ни один не молчаливый:
      • `fixed`      — модель назвала готовый перевод, на экране он встал кнопкой;
      • `agreed`     — модель теперь считает перевод верным; вопрос НЕ закрываем сами,
                       владелец решает кнопкой «Сохранить этот перевод как общий»;
      • `no_fix`     — спросили, варианта опять нет. Метка стоит, денег больше не тратим;
      • `not_asked`  — модель не ответила. НИЧЕГО не пишем: вопрос вернётся в следующий
                       проход. Это «не спросили», а не «ответа нет».
    """
    from backend.database import get_db_connection_context
    from backend.openai_manager import _LAST_LLM_USAGE, run_translation_pair_check

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT r.text, r.translation, r.judges, COALESCE(u.kind, 'collocation')
                     FROM bt_3_phrase_review r
                     LEFT JOIN bt_3_lex_units u ON u.id = r.unit_id
                    WHERE r.id = %s AND r.status = 'open'
                      AND COALESCE(r.kind, 'grammar') = 'translation';""",
                (int(review_id),))
            row = cursor.fetchone()
    if not row:
        return {"state": "gone", "spent": 0.0}
    text, translation, judges, kind = row[0], row[1], row[2], row[3]
    if not str(translation or "").strip():
        # Спрашивать «означает ли пустой русский эту фразу» бессмысленно: вопрос не
        # про перевод, а про то, что его нет. Владельцу это и так видно на экране.
        return {"state": "no_translation", "spent": 0.0}

    verdict = run_translation_pair_check(german=str(text or ""),
                                         russian=str(translation or ""),
                                         kind=str(kind or "collocation"))
    usage = _LAST_LLM_USAGE.get() or {}
    потрачено = (int(usage.get("prompt_tokens") or 0) * PRICE_IN
                 + int(usage.get("completion_tokens") or 0) * PRICE_OUT)
    if not verdict.get("checked"):
        return {"state": "not_asked", "spent": round(потрачено, 6)}
    if not _записать_пересуд(review_id, judges if isinstance(judges, list) else [], verdict):
        # Строку закрыли, пока мы спрашивали. Не ошибка, но и не работа.
        return {"state": "gone", "spent": round(потрачено, 6)}
    if verdict.get("ok"):
        состояние = "agreed"
    elif str(verdict.get("better") or "").strip():
        состояние = "fixed"
    else:
        состояние = "no_fix"
    return {"state": состояние, "spent": round(потрачено, 6),
            "fix": str(verdict.get("better") or "").strip(),
            "why": str(verdict.get("why") or "").strip()}


def rejudge_translation_questions(*, limit: int | None = None,
                                  budget_usd: float | None = None) -> dict:
    """Ночной добор: каждому вопросу о переводе — по одной попытке получить вариант."""
    cap = int(limit if limit is not None else REJUDGE_CAP)
    budget = float(budget_usd if budget_usd is not None else REJUDGE_BUDGET_USD)
    report = {"взято": 0, "с вариантом": 0, "судья согласился": 0,
              "снова без варианта": 0, "не смогли спросить": 0, "потрачено": 0.0}
    for row in translation_questions_without_fix(cap):
        if report["потрачено"] >= budget:
            logging.info("доспрос переводов: потолок $%.2f, остановились", budget)
            break
        итог = rejudge_translation_question(row["review_id"])
        report["потрачено"] += float(итог.get("spent") or 0.0)
        состояние = str(итог.get("state") or "")
        if состояние == "fixed":
            report["с вариантом"] += 1
        elif состояние == "agreed":
            report["судья согласился"] += 1
        elif состояние == "no_fix":
            report["снова без варианта"] += 1
        elif состояние == "not_asked":
            report["не смогли спросить"] += 1
        report["взято"] += 1
    report["потрачено"] = round(report["потрачено"], 4)
    report["осталось без доспроса"] = count_translation_questions_without_fix()
    logging.info("доспрос вопросов о переводе: %s", report)
    return report
