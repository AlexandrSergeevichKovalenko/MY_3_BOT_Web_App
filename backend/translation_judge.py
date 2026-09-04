# -*- coding: utf-8 -*-
"""Судья переводов: убрать с экрана русские слова, которые немецкому слову не принадлежат.

ОТКУДА ЗАДАЧА
─────────────
Разбирая слово «rasen» (мчаться), владелец 02.09.2026 увидел рядом с ним «отдыхает»,
«перерыв», «остановка» — похоже, кто-то спутал с «Rast» (привал). У «altwerden»
(стареть) стояло «старение». Доказать программой, что это чужое, нечем: тут надо знать
язык, а не сравнивать строки. Владелец 03.09.2026: «Спросить модель — но не как попало,
а двумя запросами, и принять ответ только если оба совпали. Так мы уже делаем со
спряжениями. Делаем это».

ПОЧЕМУ ИМЕННО ДВА СПРОСА
────────────────────────
Это уже принятый в проекте приём (`german_verb_paradigms.paradigm_from_model`,
`german_reference_forms`): одиночный ответ модели — догадка, совпадение двух
независимых ответов — основание. На спряжениях он себя показал; здесь тот же класс
задачи и та же цена ошибки.

КОГО СУДИМ, А КОГО НЕ ТРОГАЕМ
─────────────────────────────
Судим только те слова, где НАШИ переводы расходятся с базовым словарём: слово в
FreeDict есть, а показываемого перевода у него там нет. Замер 03.09.2026 по живой базе:
видимых переводов у слов — 37 364, из них не подтверждены базовым словарём 13 107 у
2769 слов. «Нет в FreeDict» — НЕ приговор (там мало синонимов), это лишь повод спросить.

ЧЕТЫРЕ ЗАМКА, БЕЗ КОТОРЫХ ЭТОТ МЕХАНИЗМ ОПАСЕН
──────────────────────────────────────────────
  1. Понижаем только те строки, которые ОБА ответа назвали чужими. Разошлись — не
     трогаем и считаем расхождение.
  2. Понижаем только строки ИЗ НАШЕГО СПИСКА. Что модель придумала от себя — мимо.
  3. Никогда не понижаем перевод, подтверждённый базовым словарём, даже если модель
     против: у источника прав больше, чем у модели.
  4. Никогда не понижаем ВСЕ переводы слова. Если модель забраковала всё — это не
     уборка, а потеря слова; случай откладывается владельцу.

Ничего не удаляется. У связи меняется только ранг на `_DEMOTED_RANK` — выдача такие не
показывает (`lex_units._LINK_PICK_WHERE`), а в базе они остаются.

ЦЕНА. Модель — mini (задача простая: «это перевод этого слова или нет»), два спроса на
слово. Замер 03.09.2026: около доллара за все 2769 слов; ночная порция маленькая.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from backend.lex_units import _DEMOTED_RANK

JUDGE_TASK = "translation_belongs_to_word"

_INSTRUCTION = """Ты — редактор немецко-русского словаря.
Тебе дают немецкое слово, его часть речи и список русских переводов, которые сейчас
показываются рядом с ним. Найди среди них ЧУЖИЕ — те, которые этому немецкому слову не
соответствуют вообще (перепутано с другим словом, случайный мусор, не тот смысл).

Верни ТОЛЬКО JSON:
{"чужие": ["строка из списка", "строка из списка"]}

Правила:
- В "чужие" попадают ТОЛЬКО строки, дословно взятые из присланного списка.
- Синоним, разговорный вариант, близкий оттенок смысла — НЕ чужой. Убираем только то,
  что относится к другому слову.
- Форма слова вместо начальной («отдыхает» вместо «отдыхать») — чужая строка.
- Если чужих нет, верни {"чужие": []}.
- Ничего не объясняй и не добавляй от себя."""

# Состояния одного спроса, и путать их нельзя.
НЕ_ОТВЕТИЛА = "не ответила"      # сеть, таймаут, нечитаемый ответ — спросим позже
РАЗОШЛИСЬ = "ответы разошлись"   # спросили дважды, согласия нет — не трогаем
СОГЛАСИЕ = "согласие"


def ensure_translation_judge_schema() -> None:
    """Журнал вердиктов: за один и тот же спор не платим дважды."""
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS bt_3_translation_judgements (
                       unit_id BIGINT PRIMARY KEY,
                       word TEXT NOT NULL,
                       judged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                       verdict TEXT NOT NULL,
                       demoted INTEGER NOT NULL DEFAULT 0,
                       note TEXT
                   );"""
            )
        conn.commit()


def _ask_once(word: str, pos: str, translations: list[str]) -> set[str] | None:
    """Один спрос. None — модель НЕ ОТВЕТИЛА (это не «чужих нет»)."""
    import asyncio

    from backend.openai_manager import llm_execute, parse_llm_json_object, system_message

    system_message.setdefault(JUDGE_TASK, _INSTRUCTION)
    вопрос = json.dumps(
        {"слово": word, "часть речи": pos or "неизвестна", "переводы": translations},
        ensure_ascii=False,
    )
    try:
        # ТОЛЬКО быстрый путь (Responses), без отката на Assistants. Замер 03.09.2026:
        # через Assistants один спрос идёт МИНУТАМИ — там создаётся ассистент, тред и
        # прогон с опросом. Для ночной пачки в сорок слов это часы, а вопрос у нас
        # крошечный: слово и список строк. Не ответил быстрый путь — считаем, что
        # модель не ответила, и спросим завтра.
        ответ = asyncio.run(llm_execute(
            task_name=JUDGE_TASK, system_instruction_key=JUDGE_TASK,
            user_message=вопрос, responses_only=True,
            allow_assistants_fallback=False, responses_timeout_seconds=45.0,
        ))
    except RuntimeError:
        logging.warning("судья переводов: модель вызвана из асинхронного контекста")
        return None
    except Exception:
        logging.warning("судья переводов: модель не ответила про %r", word, exc_info=True)
        return None
    разобрано = parse_llm_json_object(ответ, context=JUDGE_TASK)
    if not isinstance(разобрано, dict):
        return None
    чужие = разобрано.get("чужие")
    if not isinstance(чужие, list):
        return None
    # ЗАМОК 2: принимаем только то, что было в нашем списке. Придуманное — мимо.
    свои = {т.casefold(): т for т in translations}
    return {свои[str(ч).strip().casefold()] for ч in чужие
            if str(ч).strip().casefold() in свои}


def judge_word(word: str, pos: str, translations: list[str]) -> tuple[set[str], str]:
    """Чужие переводы по СОГЛАСИЮ двух независимых спросов. Иначе — пусто и причина."""
    if len(translations) < 2:
        # Единственный перевод судить нельзя: забраковав его, мы оставим слово немым.
        return set(), "переводов меньше двух"
    первый = _ask_once(word, pos, translations)
    if первый is None:
        return set(), НЕ_ОТВЕТИЛА
    второй = _ask_once(word, pos, translations)
    if второй is None:
        return set(), НЕ_ОТВЕТИЛА
    согласие = первый & второй
    if первый != второй:
        logging.info("судья переводов: ответы разошлись про %r (%s / %s)",
                     word, sorted(первый), sorted(второй))
        if not согласие:
            return set(), РАЗОШЛИСЬ
    # ЗАМОК 4: слово не должно остаться без переводов.
    if len(согласие) >= len(translations):
        return set(), "модель забраковала всё — откладываем владельцу"
    return согласие, СОГЛАСИЕ


def _кандидаты(cur, limit: int) -> list[tuple]:
    """Слова, где наши переводы расходятся с базовым словарём и мы ещё не судили."""
    cur.execute(
        f"""
        WITH видимые AS (
            SELECT u.id AS unit_id,
                   lower(regexp_replace(u.display, '^(der|die|das) ', '')) AS слово,
                   lower(v.display) AS перевод
              FROM bt_3_lex_units u
              JOIN bt_3_lex_links l ON l.from_unit = u.id OR l.to_unit = u.id
              JOIN bt_3_lex_units v
                ON v.id = CASE WHEN l.from_unit = u.id THEN l.to_unit ELSE l.from_unit END
             WHERE u.lang = 'de' AND v.lang = 'ru' AND u.kind = 'word'
               AND l.rank < {_DEMOTED_RANK}
        )
        SELECT DISTINCT в.unit_id
          FROM видимые в
         WHERE EXISTS (SELECT 1 FROM bt_base_dictionary b
                        WHERE b.source_lang = 'de' AND lower(b.lemma) = в.слово)
           AND NOT EXISTS (SELECT 1 FROM bt_base_dictionary b
                            WHERE b.source_lang = 'de' AND lower(b.lemma) = в.слово
                              AND EXISTS (SELECT 1 FROM unnest(b.translations_ru) t
                                           WHERE lower(t) = в.перевод))
           AND NOT EXISTS (SELECT 1 FROM bt_3_translation_judgements j
                            WHERE j.unit_id = в.unit_id)
         ORDER BY 1
         LIMIT %s;
        """,
        (int(limit),),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _переводы_слова(cur, unit_id: int) -> tuple[str, str, list[tuple[int, str]]]:
    cur.execute("SELECT display, pos FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
    строка = cur.fetchone()
    if not строка:
        return "", "", []
    слово, pos = строка
    cur.execute(
        f"""SELECT l.id, v.display
              FROM bt_3_lex_links l
              JOIN bt_3_lex_units v
                ON v.id = CASE WHEN l.from_unit = %s THEN l.to_unit ELSE l.from_unit END
             WHERE (l.from_unit = %s OR l.to_unit = %s) AND v.lang = 'ru'
               AND l.rank < {_DEMOTED_RANK}
             ORDER BY l.rank;""",
        (unit_id, unit_id, unit_id),
    )
    return str(слово), str(pos or ""), [(int(i), str(d)) for i, d in cur.fetchall()]


def _подтверждённые_источником(cur, слово: str) -> set[str]:
    """Переводы, которые даёт базовый словарь. ЗАМОК 3: их не понижаем никогда."""
    голое = слово.split(" ", 1)[-1] if слово.lower().startswith(("der ", "die ", "das ")) else слово
    cur.execute(
        """SELECT t FROM bt_base_dictionary b, unnest(b.translations_ru) t
            WHERE b.source_lang = 'de' AND lower(b.lemma) = lower(%s);""",
        (голое,),
    )
    return {str(r[0]).casefold() for r in cur.fetchall()}


def sweep_translations(*, limit: int = 40, apply: bool = False) -> dict[str, Any]:
    """Ночная порция: рассудить спорные переводы и понизить согласованно чужие."""
    from backend.database import get_db_connection_context

    ensure_translation_judge_schema()
    отчёт = {"рассмотрено": 0, "понижено": 0, "чисто": 0,
             "разошлись": 0, "не_ответила": 0, "владельцу": 0}
    with get_db_connection_context() as conn:
        cur = conn.cursor()
        for unit_id in _кандидаты(cur, limit):
            слово, pos, связи = _переводы_слова(cur, unit_id)
            if not слово or len(связи) < 2:
                continue
            # СПРАШИВАЕМ ПРО РАЗНЫЕ СТРОКИ, А НЕ ПРО ВСЕ СВЯЗИ. Одно русское слово
            # живёт в слое дважды (2574 написания), поэтому у «rasen» список выглядел
            # так: мчаться, мчаться, отдыхает, мчаться, отдыхает… Это лишние деньги за
            # токены и путаница для модели. Вердикт потом применяется КО ВСЕМ связям с
            # этой строкой — иначе понизится одна копия, а вторая останется на экране.
            переводы = []
            видели: set[str] = set()
            for _, текст in связи:
                ключ = текст.casefold()
                if ключ not in видели:
                    видели.add(ключ)
                    переводы.append(текст)
            if len(переводы) < 2:
                continue
            чужие, причина = judge_word(слово, pos, переводы)
            отчёт["рассмотрено"] += 1

            if причина == НЕ_ОТВЕТИЛА:
                # Вопрос ОСТАЛСЯ. Ничего не пишем — завтрашняя ночь спросит снова.
                отчёт["не_ответила"] += 1
                continue
            if причина == РАЗОШЛИСЬ:
                отчёт["разошлись"] += 1
            if причина.startswith("модель забраковала"):
                отчёт["владельцу"] += 1

            защищённые = _подтверждённые_источником(cur, слово)
            чужие_ключи = {т.casefold() for т in чужие}
            к_понижению = [(l_id, текст) for l_id, текст in связи
                           if текст.casefold() in чужие_ключи
                           and текст.casefold() not in защищённые]
            if apply:
                if к_понижению:
                    cur.execute(
                        "UPDATE bt_3_lex_links SET rank = %s, updated_at = now() "
                        "WHERE id = ANY(%s);",
                        (_DEMOTED_RANK, [l for l, _ in к_понижению]),
                    )
                cur.execute(
                    """INSERT INTO bt_3_translation_judgements
                           (unit_id, word, verdict, demoted, note)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (unit_id) DO UPDATE
                          SET judged_at = now(), verdict = EXCLUDED.verdict,
                              demoted = EXCLUDED.demoted, note = EXCLUDED.note;""",
                    (unit_id, слово, причина, len(к_понижению),
                     ", ".join(текст for _, текст in к_понижению)[:500] or None),
                )
                conn.commit()
            отчёт["понижено"] += len(к_понижению)
            if причина == СОГЛАСИЕ and not к_понижению:
                отчёт["чисто"] += 1
    return отчёт
