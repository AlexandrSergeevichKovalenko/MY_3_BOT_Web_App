# -*- coding: utf-8 -*-
"""ВТОРОЙ ГОЛОС НА ЗАПИСИ: то, что мы сочинили сами, проверяет модель другого производителя.

ЗАЧЕМ ЭТО ПОЯВИЛОСЬ. 23.08.2026 мы прошли все 5 073 фразы словаря панелью из трёх голосов
и нашли 655 карточек с кривыми примерами — это 13%. Все они когда-то были записаны ночным
обогащением: одна модель сочинила, никто не проверил, ошибка молча уехала человеку.

Владелец в тот же день: «Может, и наполнять когда будем ночью, то будем переспрашивать
две модели?» Ответ — да, но не двумя генераторами: два разбора одинаково хороши, и
выбирать между ними некому, а цена удваивается. Правильно — один пишет, ВТОРОЙ ПРОВЕРЯЕТ:
ответ короткий, цена копеечная, результат однозначный.

ИНАЧЕ УБОРКА БЕСКОНЕЧНА. Проход по базе чинит прошлое. Если дверь не проверяет, ровно
такой же проход придётся делать через год — и владелец справедливо спросит, почему это не
кончается.

ЧТО ПРОВЕРЯЕМ, А ЧТО НЕТ
────────────────────────
Только то, что придумали МЫ: примеры и соответствие разбора сохранённому смыслу.
  • род, формы, склонение, спряжение НЕ проверяем — они приходят из справочника
    (german_reference_forms, german_verb_paradigms), и модели там не место;
  • заголовок НЕ проверяем и не правим: его написал человек. Решение владельца
    23.08.2026: «это же сам пользователь записал, мы должны это оставить».

ГОЛОС ОБЯЗАН БЫТЬ ЧУЖИМ. Замер 23.08.2026: два голоса OpenAI дают 15% разногласий и почти
не спорят по существу — они обучены одинаково и ошибаются одинаково. Поэтому проверяющий
берётся у Google (Gemini), а пишет разбор OpenAI.

⚠ «НЕ СМОГЛИ СПРОСИТЬ» — НЕ «ХОРОШО». Если ключа нет, сеть молчит или ответ не разобран,
возвращается checked=False. Вызывающий обязан считать это НЕПРОВЕРЕННЫМ и не записывать
разбор: слово останется кандидатом и попадёт в следующую ночь. Записать непроверенное
значит вернуть ровно ту дыру, ради которой этот модуль и написан.

⚠ ВОПРОС ФОРМУЛИРУЕТСЯ ОСТОРОЖНО. 23.08.2026 моя же формулировка «пример обязан содержать
заголовок» создала 4 295 ложных дефектов на ровном месте: немецкий склоняется, и «Sie bot
ihrem Chef die Stirn» иллюстрируется примером «Er bietet seinem Vorgesetzten die Stirn».
Кривой вопрос опаснее отсутствия проверки — он создаёт видимость работы.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

MODEL = os.getenv("SECOND_VOICE_MODEL", "gemini-3.6-flash").strip()
TIMEOUT_MS = int(os.getenv("SECOND_VOICE_TIMEOUT_MS", "60000"))

# Поля, которые сочиняем мы сами — только они и проверяются.
OUR_FIELDS = ("examples", "meaning")

SYSTEM = """You check ONE freshly built entry of a German↔Russian learner's dictionary
before it is saved. Another model wrote it; your job is to catch what would teach a
learner something false.

Check ONLY these two things:
  examples — the German must be grammatical; the German sentence must sit in the German
             field and the Russian in the Russian one; the Russian must translate ITS OWN
             German sentence; the example must illustrate the entry.
  meaning  — if a saved meaning is given, the entry must be about THAT meaning. An idiom
             explained by its literal words is a defect.

An example does NOT have to repeat the entry word for word. German inflects: verbs
conjugate, nouns take cases, word order changes, pronouns replace names. «Sie bot ihrem
Chef die Stirn» is properly illustrated by «Er bietet seinem Vorgesetzten die Stirn».
Only call it wrong when the example illustrates something ELSE entirely.

Do NOT judge: the headword itself (a human wrote it), style, register, a missing final
full stop, dictionary placeholders (jemanden, etwas, sich), or regional but attested
German. Do NOT judge grammar tables: they come from a printed reference, not from a model.

Answer STRICT JSON: {"defects":[{"field":"examples|meaning",
"what":"<one short sentence in Russian>"}]}
An empty list means the entry may be saved. When unsure, leave it out."""


def _payload(headword: str, kind: str, card: dict[str, Any]) -> str:
    return json.dumps({
        "headword": headword,
        "kind": kind,
        "saved_meaning": card.get("translation_ru"),
        "examples": card.get("usage_examples"),
    }, ensure_ascii=False)


def review_new_card(*, headword: str, card: dict[str, Any], kind: str = "word") -> dict:
    """Проверить свежесобранный разбор ПЕРЕД записью.

    Возвращает:
        {"checked": True,  "ok": True}                      — можно сохранять;
        {"checked": True,  "ok": False, "fields": [...],
         "why": "<по-русски>"}                              — не сохранять, разбор кривой;
        {"checked": False, "why": "<почему не спросили>"}   — НЕ ПРОВЕРЕНО, тоже не
                                                              сохранять: это не «хорошо».
    """
    api_key = str(os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return {"checked": False, "ok": False, "why": "нет ключа второго голоса"}
    if not isinstance(card, dict) or not card.get("usage_examples"):
        # Проверять нечего: примеров нет. Это не брак разбора и не проверка —
        # пустой список примеров ловит отдельная планка «тонкой карточки».
        return {"checked": True, "ok": True, "fields": [], "why": ""}

    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)
        answer = client.models.generate_content(
            model=MODEL,
            contents=_payload(headword, kind, card),
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM, temperature=0,
                response_mime_type="application/json",
                http_options=types.HttpOptions(timeout=TIMEOUT_MS)),
        )
        defects = json.loads(answer.text or "{}").get("defects") or []
    except Exception as exc:                       # noqa: BLE001 — причину называем наверх
        logging.warning("второй голос не ответил: %s", exc)
        return {"checked": False, "ok": False, "why": f"{type(exc).__name__}"}

    fields = sorted({str(d.get("field") or "") for d in defects} & set(OUR_FIELDS))
    why = "; ".join(str(d.get("what") or "")[:120] for d in defects)[:400]
    return {"checked": True, "ok": not fields, "fields": fields, "why": why}
