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

# ЗАПАСНОЙ ГОЛОС — GPT mini. Решение владельца 28.08.2026, дословно:
# «скорее всего Gemini я не буду пополнять… просто нужно проверить: если нет ответа от
# Gemini, чтобы не падала опять вся работа, а мы шли и переспрашивали у GPT через мини».
#
# ⚠ ЧЕСТНО О ЦЕНЕ РЕШЕНИЯ. Этот модуль написан ради того, чтобы проверяющий был ЧУЖИМ:
# замер 23.08.2026 показал, что два голоса OpenAI обучены одинаково и ошибаются
# одинаково — 15% пустых разногласий и почти ни одного спора по существу. Значит
# запасной голос ЗАВЕДОМО СЛАБЕЕ основного: он поймает грубое (русский в немецком поле,
# пример не о том слове), но плохо поспорит с правдоподобной выдумкой OpenAI.
#
# Почему это всё равно правильно: выбор здесь не между «чужим голосом» и «своим», а
# между «своим голосом» и НИЧЕМ. Без запасного вся ночная работа встаёт целиком, как
# 28.08.2026. Слабая проверка лучше остановленного словаря — но только пока она НЕ
# ПРИТВОРЯЕТСЯ сильной. Поэтому:
#   • кто проверил, пишется в карточку (поле second_voice) и считается (stats());
#   • число проверок запасным голосом идёт в утренний отчёт отдельной строкой;
#   • это НЕ «молчаливая деградация» из правила ноль ровно потому, что она громкая.
RESERVE_MODEL = os.getenv("SECOND_VOICE_RESERVE_MODEL", "gpt-4.1-mini").strip()

# Кто проверил каждую карточку с прошлого сброса. Ночной добор обнуляет счётчик перед
# прогоном и забирает итог в отчёт: «сколько разборов проверил запасной голос» — это
# число владелец обязан видеть, иначе подмена голоса и есть та самая тихая деградация.
_STATS: dict[str, int] = {"gemini": 0, "openai": 0, "unchecked": 0}


def reset_stats() -> None:
    """Обнулить счётчик голосов (зовёт ночной добор перед прогоном)."""
    for k in _STATS:
        _STATS[k] = 0


def stats() -> dict[str, int]:
    """Кто сколько проверил с прошлого сброса."""
    return dict(_STATS)

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


def unavailable_reason() -> str:
    """Почему второго голоса СЕЙЧАС спросить нельзя. Пустая строка — можно.

    ┌─ НАЙДЕНО 28.08.2026. НОЧЬ ПЛАТИЛА ЗА ВЫБРОШЕННОЕ. ─────────────────────────┐
    │ Второй голос стоит ДВЕРЬЮ: не ответил — разбор не записываем (это верно и   │
    │ менять не надо). Но ключ GEMINI_API_KEY стоял только у сервиса BACKEND_WEB, │
    │ а ночной добор крутится в планировщике бота, где ключа не было. Выходило    │
    │ так: ночь спрашивала OpenAI про 400 слов (за деньги, успешно), потом на     │
    │ каждом слове упиралась в закрытую дверь и выбрасывала ВСЕ 400 разборов.     │
    │ В отчёт уходило безымянное «Ошибок: 400» — неотличимо от «GPT не ответил».  │
    │                                                                            │
    │ Замер 28.08.2026: GEMINI_API_KEY есть у BACKEND_WEB и нет у MY_3_BOT,       │
    │ SCHEDULER_SERVICE, BACKGROUND_JOBS. Сам ключ и модель живые — прямой запрос │
    │ боевым ключом вернул HTTP 200.                                              │
    │                                                                            │
    │ Отсюда эта функция: спрашивать её ДО цикла и не тратить ни одного платного  │
    │ запроса, если писать всё равно будет некуда. Дверь остаётся закрытой —      │
    │ меняется только то, что мы перестаём платить за выброшенное.                │
    └────────────────────────────────────────────────────────────────────────────┘
    Сети не трогает: проверяется только то, чего заведомо нет. Живой отказ Gemini
    (сеть, квота, 500) ловится по-прежнему в review_new_card — там же включается
    запасной голос.

    С 28.08.2026 голосов ДВА, поэтому «спросить нельзя» = нет НИ ОДНОГО ключа.
    """
    # Тот же выключатель, что и у двери (lex_units._second_voice_disabled): когда проверка
    # выключена осознанно, дверь открыта, и «недоступен» — неправда.
    if str(os.getenv("SECOND_VOICE_CHECK_DISABLED") or "").strip() == "1":
        return ""
    if str(os.getenv("GEMINI_API_KEY") or "").strip():
        return ""
    if str(os.getenv("OPENAI_API_KEY") or "").strip():
        return ""
    return ("нет ключа ни у одного проверяющего (GEMINI_API_KEY и OPENAI_API_KEY пусты) — "
            "проверить разбор нечем, а непроверенное мы не записываем")


def _ask_gemini(headword: str, kind: str, card: dict[str, Any]) -> tuple[list | None, str]:
    """(список дефектов, «») — спросили; (None, причина) — спросить не вышло.

    # ┌─ ПОЧИНЕНО 26.08.2026. ЗДЕСЬ БЫЛА БИБЛИОТЕКА `google-genai`, И В ПРОДЕ ЕЁ НЕТ. ─┐
    # │ В requirements.txt её никто не добавил, а локально она стояла — поэтому все   │
    # │ мои прогоны проходили, а на сервере каждый вызов падал с                      │
    # │ «cannot import name 'genai' from 'google' (unknown location)»: пакет `google` │
    # │ там namespace-пакет от google-cloud-*, и подпакета genai в нём нет.           │
    # │                                                                              │
    # │ ЧЕМ ЭТО БЫЛО ОПАСНО. Второй голос стоит ДВЕРЬЮ: не ответил — не записываем.   │
    # │ Значит в проде молча отклонялась КАЖДАЯ запись разбора, собранного моделью:   │
    # │ ночное обогащение, пересбор, добор синонимов, дозаполнение при открытии.      │
    # │ Снаружи это выглядело как «разбор ещё готовится» — вечно.                     │
    # │                                                                              │
    # │ Теперь запрос идёт прямым HTTP через `requests` (он и так в зависимостях):    │
    # │ ни библиотеки, ни конфликта пространств имён, и локально с продом одинаково.  │
    # └──────────────────────────────────────────────────────────────────────────────┘
    """
    api_key = str(os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return None, "нет ключа GEMINI_API_KEY"

    import requests

    url = (f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}"
           f":generateContent")
    тело = {
        "system_instruction": {"parts": [{"text": SYSTEM}]},
        "contents": [{"role": "user",
                      "parts": [{"text": _payload(headword, kind, card)}]}],
        "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
    }
    try:
        ответ = requests.post(url, params={"key": api_key}, json=тело,
                              timeout=TIMEOUT_MS / 1000.0)
        if ответ.status_code != 200:
            # Сюда же приходит «кончились деньги» (429/403) — ровно тот случай, ради
            # которого заведён запасной голос.
            logging.warning("основной голос (Gemini) не ответил: HTTP %s %s",
                            ответ.status_code, ответ.text[:200])
            return None, f"Gemini HTTP {ответ.status_code}"
        данные = ответ.json()
        куски = (данные.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
        текст = "".join(str(к.get("text") or "") for к in куски)
        return json.loads(текст or "{}").get("defects") or [], ""
    except Exception as exc:                       # noqa: BLE001 — причину называем наверх
        logging.warning("основной голос (Gemini) не ответил: %s", exc)
        return None, f"Gemini {type(exc).__name__}"


def _ask_openai(headword: str, kind: str, card: dict[str, Any]) -> tuple[list | None, str]:
    """ЗАПАСНОЙ голос: та же проверка через GPT mini. (дефекты, «») или (None, причина).

    Зовётся ТОЛЬКО когда основной не ответил. Вопрос дословно тот же (SYSTEM), чтобы
    разница в вердиктах шла от модели, а не от переформулировки: кривой вопрос опаснее
    отсутствия проверки — это уже проходили 23.08.2026 на 4 295 ложных дефектах.

    О КЕШИРОВАНИИ, чтобы никто не считал несуществующую экономию: автоматический кеш
    OpenAI включается от 1024 токенов общего начала запроса, а наш SYSTEM — около 344
    токенов (замер 28.08.2026: 1 378 символов). То есть на этих запросах кеш НЕ
    работает и скидки за него не будет. Batch API (−50%) сюда тоже не подходит: он
    отвечает часами, а проверка стоит ДВЕРЬЮ перед записью — ждать её никто не может.
    """
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, "нет ключа OPENAI_API_KEY"
    try:
        from backend.synthetic_load import build_sync_openai_client
        client = build_sync_openai_client(api_key=api_key, timeout=TIMEOUT_MS / 1000.0)
        resp = client.chat.completions.create(
            model=RESERVE_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": _payload(headword, kind, card)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        текст = str(resp.choices[0].message.content or "").strip()
        return json.loads(текст or "{}").get("defects") or [], ""
    except Exception as exc:                       # noqa: BLE001 — причину называем наверх
        logging.warning("запасной голос (GPT mini) не ответил: %s", exc)
        return None, f"GPT {type(exc).__name__}"


def review_new_card(*, headword: str, card: dict[str, Any], kind: str = "word") -> dict:
    """Проверить свежесобранный разбор ПЕРЕД записью.

    Возвращает:
        {"checked": True,  "ok": True,  "voice": "gemini"|"openai"}  — можно сохранять;
        {"checked": True,  "ok": False, "fields": [...], "why": "…",
         "voice": …}                                       — не сохранять, разбор кривой;
        {"checked": False, "why": "<почему не спросили>"}  — НЕ ПРОВЕРЕНО, тоже не
                                                             сохранять: это не «хорошо».

    ДВА ГОЛОСА, ПО ОЧЕРЕДИ (решение владельца 28.08.2026). Основной — Gemini, чужой и
    потому сильный. Не ответил (кончились деньги, сеть, квота) — спрашиваем GPT mini.
    Не ответили ОБА — разбор не записывается, как и прежде.

    Подмена голоса НЕ тихая: кто ответил, видно в ответе (`voice`), считается в stats()
    и уходит строкой в утренний отчёт. Слабый голос вместо сильного — это состояние, о
    котором владелец обязан узнать сам, а не выяснять потом по кривым карточкам.
    """
    if not isinstance(card, dict) or not card.get("usage_examples"):
        # Проверять нечего: примеров нет. Это не брак разбора и не проверка —
        # пустой список примеров ловит отдельная планка «тонкой карточки».
        return {"checked": True, "ok": True, "fields": [], "why": "", "voice": "нечего"}

    defects, почему = _ask_gemini(headword, kind, card)
    voice = "gemini"
    if defects is None:
        defects, почему_запасного = _ask_openai(headword, kind, card)
        voice = "openai"
        if defects is None:
            _STATS["unchecked"] += 1
            return {"checked": False, "ok": False,
                    "why": f"{почему}; {почему_запасного}"}
        logging.info("разбор %r проверил ЗАПАСНОЙ голос (%s): основной молчит — %s",
                     headword, RESERVE_MODEL, почему)

    _STATS[voice] += 1
    fields = sorted({str(d.get("field") or "") for d in defects} & set(OUR_FIELDS))
    why = "; ".join(str(d.get("what") or "")[:120] for d in defects)[:400]
    return {"checked": True, "ok": not fields, "fields": fields, "why": why, "voice": voice}
