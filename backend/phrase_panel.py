# -*- coding: utf-8 -*-
"""ПАНЕЛЬ ИЗ ТРЁХ ГОЛОСОВ: карточку фразы смотрят три независимые модели.

ЧТО ЭТО. У фраз и предложений печатного справочника нет: ни Wiktionary, ни DWDS не
знают «die Hose anhaben» как статью. Значит судит модель, и единственная защита от её
выдумок — несколько НЕЗАВИСИМЫХ голосов. Проверяются четыре вещи: сама фраза, перевод,
примеры и записанное значение.

ПРАВИЛО, ПРОВЕРЕННОЕ ЗАМЕРОМ 23.08.2026 на 40 карточках:
    двое из трёх назвали ОДНО И ТО ЖЕ поле  → дефект настоящий, чиним;
    двое из трёх промолчали                  → карточка чистая («чисто» — тоже голос);
    все три разошлись                        → владельцу.
Числа замера: 85% чистых, 12% согласных дефектов, 2,5% спорных.

ПОЧЕМУ ТРИ, А НЕ ДВА. Два голоса OpenAI обучены одинаково и ошибаются одинаково: они
давали 15% разногласий — 1 718 карточек владельцу, которые он физически не разберёт.
Третий голос от ДРУГОГО производителя (Gemini) снял это до 286.

┌─ ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ 31.08.2026. ──────────────────────────────────────────┐
│ Панель жила разовым скриптом, который запускал руками я. Замер 31.08.2026 по     │
│ живой базе: 1 302 карточки фраз панель не видела НИ РАЗУ, и 89 из них появились  │
│ за последнюю неделю — то есть дыра не историческая, она наполняется дальше.      │
│ Владелец: «ставь» — ночной порцией, с потолком расхода и строкой в утренний      │
│ отчёт. Логика судейства переехала сюда целиком, а скрипт стал её вызывать:       │
│ вторая копия правил через полгода разошлась бы с этой, и одна из двух стала бы   │
│ неверной.                                                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

⚠ ДВЕ ОШИБКИ ЗАМЕРА, ЗАПЕРТЫЕ ЗДЕСЬ НАВСЕГДА — обе были в вопросе, а не в данных:
  • «пример обязан содержать заголовок» — неверно: немецкий склоняется, «Sie bot ihrem
    Chef die Stirn» иллюстрируется примером «Er bietet seinem Vorgesetzten die Stirn».
    Gemini прочёл требование буквально и ругал каждую вторую карточку;
  • «спор» считался по согласию О ПОЛЕ, и карточка, где двое молчат, а один придрался,
    уходила владельцу. Молчание большинства — это вердикт «чисто», а не спор.

ПАНЕЛЬ НИЧЕГО НЕ ПЕРЕПИСЫВАЕТ В КАРТОЧКАХ. Она ставит отметку: что проверено, кем,
когда, какой вердикт. Дальше отметку разбирают другие: «дефект» каждую ночь берёт
`backend/example_retry.py` и переписывает примеры, «спорное» уходит владельцу вопросом
с готовым вариантом на кнопке.

ОДНА КАРТОЧКА СМОТРИТСЯ ОДИН РАЗ. Отбор идёт по отсутствию отметки
(`bt_3_field_checks`, поле `phrase_panel`), поэтому проверенная карточка не вернётся
ни этой ночью, ни через месяц — и денег второй раз не стоит.
"""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

FIELD = "phrase_panel"
CLEAN, DEFECT, DISPUTED, NOT_ASKED = "подтверждено", "дефект", "спорное", "не спросили"
# Исход «судить было нечем»: перевод исчез между отбором и запросом (человек стёр связь
# в ту же секунду). Отметку НЕ ставим — карточка не проверена; число уходит в отчёт.
# Отбор такие карточки не берёт, так что это защита от гонки, а не рабочий путь.
NO_RU = "перевода нет — не судили"
HUMANS_OWN = "текст человека — решает он"

# ⛔ ЧТО ЧИНИМ, А ЧТО НЕ НАШЕ. Решение владельца 23.08.2026, дословно: «это же сам
# пользователь записал, мы должны это оставить».
#
# Восемь фраз из десяти человек написал или выбрал сам. Заголовок такой карточки — ЕГО
# текст: он так услышал, так записал, так запомнил. Переписать его молча значит
# подменить человеку память, и никакая правота справочника этого не оправдывает.
# Примеры, разбор и перевод у той же карточки сочинили МЫ — это наша работа, и её мы
# чиним свободно.
OUR_OWN_FIELDS = {"examples", "meaning"}          # это сделали мы — чиним
HUMAN_FIELDS = {"headword", "translation"}        # это его слова — только показать ему

MODEL_A = "gpt-4.1-2025-04-14"
MODEL_B = "gpt-4.1-mini"
MODEL_C = "gemini-3.6-flash"
# Цены OpenAI — из нашей bt_3_billing_price_snapshots. Цена Gemini — публичный прайс
# Google на 23.08.2026; сверена со счётом после первого прогона.
PRICE_OPENAI = {MODEL_A: (2.0 / 1e6, 8.0 / 1e6), MODEL_B: (0.4 / 1e6, 1.6 / 1e6)}
PRICE_GEMINI = (0.30 / 1e6, 2.50 / 1e6)
TIMEOUT_S = 60.0

# Ночная порция и потолок за ночь. Порция маленькая нарочно: это уборка, а не гонка.
#
# ┌─ ЗАМЕРЕНО 31.08.2026 НА ЖИВЫХ КАРТОЧКАХ, НЕ ПОСЧИТАНО НА БУМАГЕ. ─────────────┐
# │ Холостой прогон 12 карточек тремя голосами: $0.05, то есть $0.0042 за штуку.  │
# │ Это ДОРОЖЕ прежнего замера ($0.0017): промпт теперь требует ещё и готовый     │
# │ исправленный текст, а он платный на выходе. 50 карточек за ночь ≈ $0.21.      │
# │ Потолок $0.40 — с запасом, чтобы длинные карточки не обрезали порцию на       │
# │ середине; упрётся в него — оставшиеся не помечаются и ждут следующей ночи.    │
# │ Пока разбирается накопленное (1 302 карточки, ~26 ночей) это ≈$6 за месяц,    │
# │ дальше остаётся только приток — 89 новых фраз в неделю, ≈$1.5 в месяц.        │
# └──────────────────────────────────────────────────────────────────────────────┘
NIGHT_LIMIT = int(os.getenv("PHRASE_PANEL_NIGHT_LIMIT", "50") or "50")
NIGHT_BUDGET = float(os.getenv("PHRASE_PANEL_NIGHT_BUDGET", "0.40") or "0.40")

SYSTEM = """You audit ONE entry of a German↔Russian learner's dictionary. The entry is a
phrase or a sentence, not a single word — no printed dictionary lists it, so judge the
German itself.

Report ONLY defects that would teach a learner something false:
  headword   — not real German, or a broken fragment;
  translation— the Russian does not mean what the German says;
  examples   — the German is ungrammatical, or the sides are swapped (Russian text sitting
               in the German field), or the Russian translation does not match its German
               sentence, or the example has nothing to do with the entry;
  meaning    — the saved meaning is an idiom but the entry explains the literal words.

An example does NOT have to repeat the entry word for word. German inflects: the verb is
conjugated, the noun takes a case, the word order changes, a pronoun replaces a name.
«Sie bot ihrem Chef die Stirn» is properly illustrated by «Er bietet seinem Vorgesetzten
die Stirn». Only call the example wrong when it illustrates something ELSE entirely.

Also NOT defects: style, register, a missing final full stop, a phrase given without
context, a dictionary placeholder (jemanden, etwas, sich), regional but attested German.

EVERY DEFECT MUST COME WITH THE CORRECTED TEXT. A verdict «this is not said in German»
without saying what IS said is useless to the person who has to decide. Fill "fix":
  headword   — the entry written the way German actually says it;
  translation— the Russian that really means the German entry;
  examples, meaning — leave "fix" empty: those are rebuilt by a separate step, and a
               correction nobody can apply is worse than none.
Leave "fix" empty ONLY when you genuinely cannot name a correct version. Never put a
comment, a question or an explanation in "fix" — it is the finished text and nothing else.

Answer STRICT JSON: {"defects":[{"field":"headword|translation|examples|meaning",
"what":"<one short sentence in Russian>","fix":"<corrected text or empty>"}]}
An empty list means the entry is fine. When unsure, leave it out."""


class BudgetSpent(Exception):
    """Деньги кончились — прогон останавливается, а не «доделывает по-быстрому»."""


def unavailable_reason() -> str:
    """Почему панель спрашивать НЕЛЬЗЯ. Пустая строка — можно.

    ⛔ ДВА ГОЛОСА ОДНОГО ПРОИЗВОДИТЕЛЯ — ЭТО НЕ ПАНЕЛЬ. Без ключа Gemini остались бы
    два голоса OpenAI, а они обучены одинаково: замер 23.08.2026 — 15% разногласий
    вместо 2,5%, то есть владельцу поехало бы 1 718 вопросов вместо 286. Работать
    «чем есть» здесь означает молча ухудшить проверку и завалить его очередь, поэтому
    ночь честно не запускается и говорит об этом в отчёте.
    """
    if not str(os.getenv("OPENAI_API_KEY") or "").strip():
        return "нет ключа OPENAI_API_KEY — спрашивать нечем"
    if not str(os.getenv("GEMINI_API_KEY") or "").strip():
        return ("нет ключа GEMINI_API_KEY — остались бы два голоса одного производителя, "
                "а это не панель, а её видимость")
    return ""


def _fields(text: str):
    """Разбор ответа голоса: (поля, сводка словами, претензии по пунктам).

    Поля = None — ответ не разобран, и это НЕ «чисто».

    ПРЕТЕНЗИИ ВОЗВРАЩАЮТСЯ ПОШТУЧНО, а не одной строкой. Склейка через «; » нужна
    колонке `reference`, где лежит человекочитаемый след, — но по дороге к владельцу
    она уносила ДВЕ вещи: имя поля (о чём спор) и готовый вариант. Экран после этого
    печатал «спор о карточке» над претензией к самой фразе, а исправить перевод было
    нечем (разобрано с владельцем 31.08.2026).
    """
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None, "", []
    defects = payload.get("defects") or []
    претензии = [
        {"field": str(d.get("field") or ""),
         "what": str(d.get("what") or "").strip()[:400],
         # Готовый вариант. Пустой — значит голос его НЕ назвал; выдумывать за него
         # нечего и нельзя, экран честно покажет претензию без кнопки.
         "fix": str(d.get("fix") or "").strip()[:300]}
        for d in defects if isinstance(d, dict) and d.get("field")
    ]
    return ({d["field"] for d in претензии},
            "; ".join(d["what"][:90] for d in претензии)[:400],
            претензии)


class Panel:
    """Три голоса и счётчик денег. Один экземпляр на прогон."""

    def __init__(self, budget_usd: float = NIGHT_BUDGET) -> None:
        self.budget_usd = float(budget_usd)
        self.cost = 0.0
        from openai import OpenAI
        # ⏱ ТАЙМАУТ ОБЯЗАТЕЛЕН. Прогон 23.08.2026 завис на 3 800-й карточке из 4 953:
        # запрос ушёл без ограничения и висел 55 минут, а вместе с ним стояла вся
        # очередь. Провайдер, который «думает» дольше минуты, — это авария.
        self._openai = OpenAI(timeout=TIMEOUT_S, max_retries=0)

    def _openai_vote(self, model: str, payload: str):
        answer = self._openai.chat.completions.create(
            model=model, temperature=0, response_format={"type": "json_object"},
            messages=[{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": payload}])
        self.cost += (answer.usage.prompt_tokens * PRICE_OPENAI[model][0]
                      + answer.usage.completion_tokens * PRICE_OPENAI[model][1])
        return _fields(answer.choices[0].message.content)

    def _gemini_vote(self, payload: str):
        """Третий голос — прямым HTTP, а НЕ библиотекой `google-genai`.

        ┌─ ПРОВЕРЕНО 31.08.2026. НЕ ВОЗВРАЩАТЬ СЮДА `from google import genai`. ─────┐
        │ Пакета `google-genai` в requirements.txt нет и не было: локально он стоит, │
        │ поэтому мои прогоны проходили, а в проде каждый вызов падал бы с           │
        │ «cannot import name 'genai' from 'google'» — там `google` это namespace от │
        │ google-cloud-*, и подпакета genai в нём нет. На этом уже обожглись         │
        │ 26.08.2026 во втором голосе (`second_voice_check._ask_gemini`), и ночная    │
        │ панель повторила бы ту же аварию молча: третий голос не отвечает никогда,   │
        │ остаются два одинаковых, разногласия растут вшестеро.                       │
        └────────────────────────────────────────────────────────────────────────────┘
        """
        import requests
        api_key = str(os.getenv("GEMINI_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("нет ключа GEMINI_API_KEY")
        ответ = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_C}:generateContent",
            params={"key": api_key},
            json={
                "system_instruction": {"parts": [{"text": SYSTEM}]},
                "contents": [{"role": "user", "parts": [{"text": payload}]}],
                "generationConfig": {"temperature": 0,
                                     "responseMimeType": "application/json"},
            },
            timeout=TIMEOUT_S)
        if ответ.status_code != 200:
            raise RuntimeError(f"Gemini HTTP {ответ.status_code}")
        данные = ответ.json()
        usage = данные.get("usageMetadata") or {}
        # ⚠ «РАЗМЫШЛЕНИЯ» ТОЖЕ ПЛАТНЫЕ, и это стоило реальных денег 23.08.2026. Замер
        # одного запроса: вход 67, ОТВЕТ 5, размышления 343. Считая только ответ, я
        # занижал выход в семьдесят раз: счётчик показал $5.83, счёт Google пришёл на
        # €8.28. Потолок, построенный на такой арифметике, не защищает — он врёт медленнее.
        self.cost += ((int(usage.get("promptTokenCount") or 0)) * PRICE_GEMINI[0]
                      + (int(usage.get("candidatesTokenCount") or 0)
                         + int(usage.get("thoughtsTokenCount") or 0)) * PRICE_GEMINI[1])
        куски = (данные.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
        return _fields("".join(str(к.get("text") or "") for к in куски))

    def judge(self, entry: dict) -> tuple[str, str, list]:
        """(вердикт, пояснение, претензии по пунктам).

        Голос, который не ответил, НЕ засчитывается молчанием. Третьим значением идут
        претензии каждого голоса поштучно — с именем поля и готовым вариантом; они
        уезжают владельцу, когда вердикт «спорное»."""
        if self.cost >= self.budget_usd:
            raise BudgetSpent(f"потрачено ${self.cost:.2f} — потолок ${self.budget_usd:.2f}")
        payload = json.dumps(entry, ensure_ascii=False)
        votes, reasons, claims = [], [], []
        for номер, asking in enumerate(
                (lambda: self._openai_vote(MODEL_A, payload),
                 lambda: self._openai_vote(MODEL_B, payload),
                 lambda: self._gemini_vote(payload)), 1):
            for attempt in range(3):
                try:
                    fields, why, доводы = asking()
                    break
                except Exception as exc:              # сеть, 429, срез ответа
                    if attempt == 2:
                        fields, why, доводы = None, f"голос не ответил: {type(exc).__name__}", []
                    time.sleep(2 + attempt * 3)
            votes.append(fields)
            if why:
                reasons.append(why)
            for довод in доводы:
                claims.append({**довод, "voice": номер})

        answered = [v for v in votes if v is not None]
        if len(answered) < 2:
            # Меньше двух голосов — большинства не существует. Записать «чисто» здесь
            # значило бы выдать аварию за проверку.
            return NOT_ASKED, "; ".join(reasons)[:400], claims
        union = set().union(*answered)
        majority = {f for f in union if sum(1 for v in answered if f in v) >= 2}
        silent = sum(1 for v in answered if not v)
        if majority:
            # Дефект в НАШЕЙ части карточки — наша работа. Дефект в тексте человека —
            # его дело: помечаем и показываем ему, но не переписываем.
            ours = majority & OUR_OWN_FIELDS
            verdict = DEFECT if ours else HUMANS_OWN
            return (verdict,
                    f"{', '.join(sorted(majority))} :: " + "; ".join(reasons)[:300],
                    claims)
        if not union or silent >= 2:
            return CLEAN, "", []
        return DISPUTED, "; ".join(reasons)[:400], claims

    def проверить_вариант(self, *, поле: str, готовое: str, заголовок: str,
                          перевод: str) -> dict:
        """ВТОРОЙ ГОЛОС НА ГОТОВЫЙ ВАРИАНТ. Предложение судьи — тоже текст модели.

        Владелец 31.08.2026: диагноз без исправления бесполезен. Но исправление,
        которое никто не проверил, — то же самое, только опаснее: оно выглядит как
        ответ и стоит на кнопке. Поэтому предложение сверяется парной проверкой смысла
        (`openai_manager.run_translation_pair_check`, gpt-4.1-mini, ≈$0.0001).

        Три состояния и ни одного молчаливого: годится / не годится / спросить не
        удалось. Последнее НЕ притворяется ни первым, ни вторым.
        """
        готовое = str(готовое or "").strip()
        if not готовое or поле not in ("headword", "translation"):
            return {}
        de = готовое if поле == "headword" else str(заголовок or "").strip()
        ru = готовое if поле == "translation" else str(перевод or "").strip()
        if not de or not ru:
            return {}
        from backend.openai_manager import _LAST_LLM_USAGE, run_translation_pair_check
        try:
            ответ = run_translation_pair_check(german=de, russian=ru)
        except Exception as exc:
            return {"state": "unknown", "why": f"проверить не удалось: {type(exc).__name__}"}
        # ⛔ ЭТОТ ЗАПРОС ТОЖЕ ПЛАТНЫЙ, и не учесть его — значит построить потолок расхода
        # на заниженной арифметике (см. рамку про счёт Google в `_gemini_vote`).
        usage = _LAST_LLM_USAGE.get() or {}
        self.cost += (int(usage.get("prompt_tokens") or 0) * PRICE_OPENAI[MODEL_B][0]
                      + int(usage.get("completion_tokens") or 0) * PRICE_OPENAI[MODEL_B][1])
        if not ответ.get("checked"):
            return {"state": "unknown", "why": "проверить не удалось"}
        return ({"state": "ok", "why": ""} if ответ.get("ok")
                else {"state": "bad", "why": str(ответ.get("why") or "")[:300]})


def entry_of(display: str, kind: str, card: dict | None, translation: str) -> dict:
    """Карточка в том виде, в каком её видит голос. Одна форма на все прогоны.

    ⛔ ПЕРЕВОД ПРИХОДИТ СНАРУЖИ И ОБЯЗАТЕЛЕН. Раньше он брался тут же из карточки,
    ключом `translation_ru`, — и это была КОПИЯ, снятая при сохранении. У 752 карточек
    фраз ключа не было вовсе, и голоса получали `"translation": null`, после чего
    писали «русский перевод не передаёт значение немецкой фразы» о переводе, которого
    им не показали (разобрано с владельцем 02.09.2026). Настоящий перевод живёт в слое
    связей и берётся `lex_units.native_display_sql` — тем же правилом, каким его
    выбирает экран.

    Пустой перевод сюда попасть не может: карточку без перевода панель не судит вовсе
    (`unchecked_units` их не выбирает, `count_without_translation` считает их числом).
    """
    card = card if isinstance(card, dict) else {}
    перевод = str(translation or "").strip()
    if not перевод:
        raise ValueError("карточку без перевода панели не показываем: судить нечего")
    return {"headword": display, "kind": kind,
            "translation": перевод,
            "saved_meaning": перевод,
            "examples": card.get("usage_examples")}


# ⛔ ОТБОР ИДЁТ ПО ТОМУ, ЧТО СУДИЛИ, А НЕ ПО ФАКТУ «отметка есть».
# Отметка `bt_3_field_checks` хранит `judged_ru` — перевод, который в тот раз реально
# показали голосам. Карточка возвращается на пересуд, когда перевод на экране стал
# другим. Замер 02.09.2026 сразу после засыпки: вернулись 890 отметок — 546 вынесены с
# ПУСТЫМ русским, 344 по разошедшейся копии. Тот же отбор закрывает и будущее:
# правка перевода сама приводит карточку обратно, а не оставляет старый вердикт про
# текст, которого больше нет.
# ⛔ ВОПРОС, СОБРАННЫЙ ИЗ ПРОЗЫ, — НЕ ВОПРОС.
# До 31.08.2026 претензии отдельно не хранились, и вопрос человеку собирался из
# человекочитаемого СЛЕДА отметки (`bt_3_field_checks.reference`). След писался как
# «"; ".join(what[:90])» — то есть каждая претензия резалась на 90-м знаке, а разные
# голоса склеивались в одну строку. Владелец 02.09.2026 читал это на экране: «Пример
# … не связан с фотографированием, а значит[обрыв]; Выражение «sich zurechtstellen» не
# употребляется в возвратной форме» — два разных замечания о двух разных частях
# карточки, слипшиеся в самопротиворечивую фразу. Полного текста больше нет НИГДЕ: он
# был отброшен при записи. Восстановить нельзя — можно только спросить голоса заново.
#
# Признак такого вопроса — ни у одной претензии не названо поле (экран честно пишет
# «о чём именно — не записано»). Замер 02.09.2026: 36 панельных вопросов, у 25 текст
# оборван по-настоящему (ровно 90 знаков и нет точки в конце).
ВОПРОС_ИЗ_ПРОЗЫ = """EXISTS (
      SELECT 1 FROM bt_3_phrase_review r
       WHERE r.unit_id = u.id AND r.status = 'open'
         AND COALESCE(r.kind, 'grammar') = 'panel'
         AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(r.judges::jsonb) j
                          WHERE COALESCE(j->>'field', '') <> ''))"""


def _где_судить(ru_sql: str) -> str:
    """Условие «эту карточку надо (пере)судить» — одно на отбор и на счётчики.

    Счётчик и выборка обязаны смотреть на одно и то же: разойдясь, они врут владельцу
    в утреннем отчёте (этот урок уже оплачен — `pick_phrases_for_grammar_check`).
    """
    return (f"u.lang = 'de' AND u.kind <> 'word' AND u.card IS NOT NULL\n"
            # Карточку без перевода судить нельзя: голос получил бы пустоту и написал
            # бы о ней «перевод не передаёт значение». Их считает отдельный счётчик.
            f"   AND COALESCE({ru_sql}, '') <> ''\n"
            f"   AND (c.unit_id IS NULL\n"
            f"        OR c.judged_ru IS DISTINCT FROM {ru_sql}\n"
            f"        OR {ВОПРОС_ИЗ_ПРОЗЫ})")


def unchecked_units(limit: int, *, fresh_first: bool = True) -> list[tuple]:
    """Карточки фраз, которых панель не видела ИЛИ видела с другим переводом.

    Свежие первыми: дыра открыта сегодня, и вчерашнее сохранение важнее прошлогоднего.
    Пятым столбцом идёт перевод — тот самый, что человек видит на экране.
    """
    from backend.database import get_db_connection_context
    from backend.lex_units import native_display_sql
    ru = native_display_sql("u")
    порядок = "u.created_at DESC NULLS LAST, u.id DESC" if fresh_first else "u.id"
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT u.id, u.display, u.kind, u.card, {ru} AS ru
                  FROM bt_3_lex_units u
                  LEFT JOIN bt_3_field_checks c
                         ON c.unit_id = u.id AND c.field = %s
                 WHERE {_где_судить(ru)}
                 ORDER BY {порядок}
                 LIMIT %s;""", (FIELD, int(limit)))
            return list(cur.fetchall() or [])


def _счётчик(условие: str) -> int:
    """Общий счётчик по пулу карточек фраз. -1 значит «прочитать не смогли» — это НЕ
    ноль: молчащий механизм неотличим от сломанного."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT count(*) FROM bt_3_lex_units u
                      LEFT JOIN bt_3_field_checks c
                             ON c.unit_id = u.id AND c.field = %s
                     WHERE {условие};""", (FIELD,))
                return int((cur.fetchone() or (0,))[0])
    except Exception:
        logging.warning("счётчик карточек панели не прочитан", exc_info=True)
        return -1


def count_unchecked() -> int:
    """Сколько карточек панель ещё не видела или видела с другим переводом. Число едет
    владельцу в утренний отчёт."""
    from backend.lex_units import native_display_sql
    return _счётчик(_где_судить(native_display_sql("u")))


def count_stale_translation() -> int:
    """Из них — ПЕРЕСУД: отметка есть, но поставлена по другому переводу.

    Отдельным числом, потому что это уборка накопленного, и владелец должен видеть,
    как она убывает, а не гадать, почему «осталось проверить» не падает."""
    from backend.lex_units import native_display_sql
    ru = native_display_sql("u")
    return _счётчик(f"u.lang = 'de' AND u.kind <> 'word' AND u.card IS NOT NULL\n"
                    f"   AND COALESCE({ru}, '') <> ''\n"
                    f"   AND c.unit_id IS NOT NULL AND c.judged_ru IS DISTINCT FROM {ru}")


def count_prose_questions() -> int:
    """Сколько вопросов ещё стоят на экранах с текстом, собранным из прозы.

    Число обязано убывать: это уборка накопленного, а не вечная строка в отчёте."""
    from backend.lex_units import native_display_sql
    ru = native_display_sql("u")
    return _счётчик(f"u.lang = 'de' AND u.kind <> 'word' AND u.card IS NOT NULL\n"
                    f"   AND COALESCE({ru}, '') <> ''\n"
                    f"   AND {ВОПРОС_ИЗ_ПРОЗЫ}")


def count_without_translation() -> int:
    """Карточки фраз, у которых перевода нет НИГДЕ. Панель их не судит: показать голосу
    пустоту и получить «перевод не передаёт значение» — это выдумка, а не проверка.

    Замер 02.09.2026: таких ноль. Число печатается в утреннем отчёте, чтобы появление
    первой такой карточки было видно сразу, а не «когда-нибудь всплыло»."""
    from backend.lex_units import native_display_sql
    ru = native_display_sql("u")
    return _счётчик(f"u.lang = 'de' AND u.kind <> 'word' AND u.card IS NOT NULL\n"
                    f"   AND COALESCE({ru}, '') = ''")


def ensure_judged_ru_column() -> None:
    """Графа «какой перевод показали голосам» + разовая засыпка прошлого.

    ┌─ ЗАВЕДЕНО 02.09.2026 ПО РАЗБОРУ С ВЛАДЕЛЬЦЕМ. ───────────────────────────────┐
    │ Отметка говорила «эту карточку смотрели» и молчала о том, ЧТО смотрели. Пока  │
    │ панель брала перевод из копии в json, это молчание скрывало 890 отметок,      │
    │ вынесенных по пустому или чужому русскому. Теперь отметка хранит текст, и     │
    │ «проверено» значит «проверено вот с этим переводом».                          │
    │                                                                              │
    │ ЗАСЫПКА НЕ ДОГАДКА. Прежний код кормил голоса ровно `card->>'translation_ru'` │
    │ — это доказуемо по самому коду, а не предположение. Поэтому старым отметкам   │
    │ проставляется именно оно: у кого копия совпала с экраном (5314 карточек) —    │
    │ отметка остаётся в силе и денег второй раз не стоит, у кого разошлась —       │
    │ карточка сама вернётся на пересуд ночными порциями.                           │
    │ Засыпаются только строки с NULL, поэтому повторный вызов ничего не портит.    │
    └──────────────────────────────────────────────────────────────────────────────┘
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE bt_3_field_checks "
                        "ADD COLUMN IF NOT EXISTS judged_ru TEXT;")
            cur.execute("""
                UPDATE bt_3_field_checks c
                   SET judged_ru = COALESCE(u.card->>'translation_ru', '')
                  FROM bt_3_lex_units u
                 WHERE u.id = c.unit_id AND c.field = %s AND c.judged_ru IS NULL;""",
                        (FIELD,))
            if cur.rowcount:
                logging.info("панель: у %s старых отметок записан судимый перевод",
                             cur.rowcount)
        conn.commit()


def _записать_отметку(unit_id: int, verdict: str, why: str, перевод: str) -> None:
    """Отметка о проверке. `перевод` — тот текст, который РЕАЛЬНО показали голосам."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bt_3_field_checks
                    (unit_id, field, verdict, source, ours, reference, checked_at,
                     judged_ru)
                VALUES (%s,%s,%s,%s,NULL,%s,NOW(),%s)
                ON CONFLICT (unit_id, field) DO UPDATE
                   SET verdict = EXCLUDED.verdict, reference = EXCLUDED.reference,
                       checked_at = NOW(), judged_ru = EXCLUDED.judged_ru;""",
                        (unit_id, FIELD, verdict,
                         "панель: gpt-4.1 + gpt-4.1-mini + gemini-3.6-flash",
                         (why or "")[:400] or None, str(перевод or "")))
        conn.commit()


# Сколько НАКОПЛЕННЫХ отметок «текст человека» поднимаем вопросами за ночь. Их 384 на
# 31.08.2026, из них 369 у самого владельца, — вывалить их разом значит завалить его
# экран проверки слов. Порциями они разойдутся за две недели.
BACKFILL_LIMIT = int(os.getenv("PHRASE_PANEL_BACKFILL", "30") or "30")


def раскрыть_отметку(reference: str) -> list[dict]:
    """Старая отметка → претензии в том же виде, в каком их даёт панель сегодня.

    В `bt_3_field_checks.reference` лежит след прежнего формата:
        «headword :: Так не говорят по-немецки.; По-немецки это Gegenverkehr.»
    Слева — поля, о которых сошлись голоса, справа — их слова. Готового варианта там
    НЕТ: до 31.08.2026 его никто не спрашивал. Поэтому старый вопрос приходит человеку
    с претензией и без кнопки «да, правильно так» — выдумать её задним числом нельзя.
    """
    сырое = str(reference or "").strip()
    if not сырое:
        return []
    поля, _, слова = сырое.partition(" :: ")
    if not слова:
        поля, слова = "", сырое
    названные = [f.strip() for f in поля.split(",") if f.strip() in HUMAN_FIELDS]
    return [{"field": (названные[0] if названные else ""),
             "what": слова.strip()[:1000], "fix": "", "voice": 0}]


def поднять_старые_отметки(limit: int = BACKFILL_LIMIT) -> int:
    """Накопленные вердикты «текст человека» — вопросами их авторам, порцией за ночь.

    ⛔ ЭТО НЕ РАЗОВЫЙ СКРИПТ. Владелец 31.08.2026: «показывать человеку». Разовый
    прогон разобрал бы то, что есть сегодня, и завтра куча начала бы копиться заново —
    ровно то, из-за чего эти 384 карточки и лежали мёртвым грузом.

    Берём только те, по которым у единицы НЕТ открытого вопроса: два вопроса об одном
    слове — это два касания вместо одного и потеря доверия к очереди.
    """
    from backend.database import get_db_connection_context, open_personal_text_question
    from backend.lex_units import native_display_sql
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT c.unit_id, u.display, COALESCE({native_display_sql("u")}, ''),
                       COALESCE(c.reference, '')
                  FROM bt_3_field_checks c
                  JOIN bt_3_lex_units u ON u.id = c.unit_id
                 WHERE c.field = %s AND c.verdict = %s
                   AND NOT EXISTS (SELECT 1 FROM bt_3_phrase_review r
                                    WHERE r.unit_id = c.unit_id AND r.status = 'open')
                 ORDER BY c.checked_at DESC
                 LIMIT %s;""", (FIELD, HUMANS_OWN, int(limit)))
            строки = list(cur.fetchall() or [])
    поднято = 0
    for unit_id, display, перевод, reference in строки:
        претензии = раскрыть_отметку(reference)
        if претензии and open_personal_text_question(unit_id, display, перевод, претензии):
            поднято += 1
    if поднято:
        logging.info("панель: поднято старых отметок «текст человека» — %s", поднято)
    return поднято


def count_personal_backlog() -> int:
    """Сколько отметок «текст человека» ещё не превращены в вопрос автору. -1 — не
    смогли прочитать (это НЕ ноль)."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT count(*) FROM bt_3_field_checks c
                     WHERE c.field = %s AND c.verdict = %s
                       AND NOT EXISTS (SELECT 1 FROM bt_3_phrase_review r
                                        WHERE r.unit_id = c.unit_id AND r.status = 'open');""",
                            (FIELD, HUMANS_OWN))
                return int((cur.fetchone() or (0,))[0])
    except Exception:
        logging.warning("остаток отметок «текст человека» не прочитан", exc_info=True)
        return -1


def донести_вердикт(unit_id: int, display: str, перевод: str,
                    verdict: str, why: str, claims: list) -> str:
    """ЧТО ДЕЛАЕМ С ВЕРДИКТОМ — одна таблица решений на все прогоны панели.

    ┌─ ЗАВЕДЕНО 02.09.2026. ДЫРА, КОТОРУЮ ОТКРЫЛ ПЕРЕСУД. ─────────────────────────┐
    │ Пока карточку смотрели ОДИН раз, вопрос просто заводился, а защита от дублей │
    │ (`ON CONFLICT DO NOTHING`) была достаточной. С пересудом карточка приходит    │
    │ второй раз — и заведение молча не срабатывало: в базе новый вердикт, на       │
    │ экране ПРЕЖНЯЯ претензия. Человек решал бы по тексту, который мы сами уже     │
    │ считаем неверным.                                                            │
    │                                                                              │
    │ Вторая копия этой таблицы жила в `rejudge_personal`. Две копии правил через   │
    │ полгода разойдутся, и одна из двух станет неверной, — поэтому копия одна.     │
    └──────────────────────────────────────────────────────────────────────────────┘

    Возвращает КОДОМ, что произошло, — считает каждый прогон по-своему:
      'переписан'   — открытый вопрос того же адресата получил новые претензии;
      'снят'        — претензии больше нет, вопрос закрыт (карточка не тронута);
      'владельцу'   — заведён новый спор владельцу;
      'человеку'    — заведён новый вопрос автору фразы;
      'чужой'       — по единице открыт вопрос ДРУГОГО вида, не трогаем ничего;
      'ничего'      — вердикт не требует вопроса (наши примеры чинит переписчик).
    """
    from backend.database import (
        close_open_question, open_panel_card_question, open_personal_text_question,
        open_question_kind, replace_question_claims,
    )
    открыт = open_question_kind(unit_id)

    # Спор владельца и проверка перевода — чужие вопросы: их заводит и закрывает
    # другой механизм, и подменять их пересудом панели мы не имеем права.
    if открыт in ("grammar", "translation"):
        return "чужой"

    адресат = "panel" if verdict == DISPUTED else "personal" if verdict == HUMANS_OWN else ""

    if открыт and адресат and открыт == адресат:
        if replace_question_claims(unit_id, claims, kind=адресат):
            return "переписан"
        # Сюда попасть нельзя: вердикт «спорное»/«текст человека» рождается ровно из
        # названных претензий (`Panel.judge`), значит их не может не быть. Если всё же
        # попали — говорим об этом громко: молча оставленный старый вопрос вернёт
        # карточку в отбор следующей ночью, и она будет судиться платно по кругу.
        logging.warning("панель: вопрос %s (%s) переписать нечем — вердикт %r без "
                        "претензий; карточка вернётся в отбор", unit_id, адресат, verdict)
        return "ничего"

    # Адресат сменился или претензии больше нет — открытый вопрос снимаем. Спрашивать
    # человека о том, что мы САМИ больше не считаем ошибкой, нельзя: его касание
    # дороже нашей строки в базе (решение владельца 31.08.2026).
    снят = bool(открыт) and close_open_question(
        unit_id, why or "пересуд снял претензию", kind=открыт)

    if verdict == DISPUTED:
        return "владельцу" if open_panel_card_question(
            unit_id, display, перевод, claims) else ("снят" if снят else "ничего")
    if verdict == HUMANS_OWN:
        return "человеку" if open_personal_text_question(
            unit_id, display, перевод, claims) else ("снят" if снят else "ничего")
    return "снят" if снят else "ничего"


def run_batch(*, limit: int = NIGHT_LIMIT, budget_usd: float = NIGHT_BUDGET,
              workers: int = 4, apply: bool = True,
              on_card=None) -> dict[str, Any]:
    """Одна порция панели. Возвращает отчёт числами — его печатает утренний доклад.

    Ночью зовётся с маленьким `limit`; тот же код прогоняет всю базу из скрипта
    `scripts/dict_phrase_panel_audit.py`. Второго механизма нет и не будет.
    """
    отчёт: dict[str, Any] = {
        "взято": 0, "проверено": 0, CLEAN: 0, DEFECT: 0, HUMANS_OWN: 0,
        DISPUTED: 0, NOT_ASKED: 0, "ушло владельцу": 0, "ушло человеку": 0,
        "поднято из старых": 0, "потрачено": 0.0,
        "остановлено потолком": False, "осталось": 0,
        "пересудить": 0, "без перевода": 0, "вопросы из прозы": 0,
        # Пересуд не только заводит вопросы, но и переписывает и снимает уже открытые.
        # Оба числа обязаны быть видны: молча снятый вопрос неотличим от потерянного.
        "вопрос переписан": 0, "вопрос снят": 0,
    }
    if apply:
        # Графа «что судили» и засыпка прошлого — до всего остального: по ней идёт
        # отбор, и без неё ночь не поймёт, что пересуживать.
        ensure_judged_ru_column()
        # Ни одного платного запроса: это разбор УЖЕ вынесенных вердиктов. Делаем до
        # проверки ключей — накопленное должно доходить до людей даже в ту ночь,
        # когда панель судить не может.
        отчёт["поднято из старых"] = поднять_старые_отметки()
        отчёт["осталось у людей"] = count_personal_backlog()
    нельзя = unavailable_reason()
    if нельзя:
        # Не «сделали как смогли», а честное «не делали»: ухудшенная проверка выглядит
        # как проверка, и отличить её потом нельзя.
        отчёт["пропущено"] = нельзя
        отчёт["осталось"] = count_unchecked()
        logging.warning("ночная панель не запускалась: %s", нельзя)
        return отчёт

    rows = unchecked_units(int(limit))
    отчёт["взято"] = len(rows)
    if not rows:
        отчёт["осталось"] = count_unchecked()
        return отчёт

    panel = Panel(budget_usd=budget_usd)

    def one(row):
        unit_id, display, kind, card, перевод = row
        перевод = str(перевод or "")
        if not перевод.strip():
            return unit_id, display, NO_RU, "", [], ""
        try:
            verdict, why, claims = panel.judge(entry_of(display, kind, card, перевод))
        except BudgetSpent as stop:
            # Отметку НЕ ставим: карточку не проверяли. Она останется в остатке и
            # достанется следующей ночи — это честнее, чем записать «чисто».
            return unit_id, display, None, str(stop), [], перевод
        if verdict in (DISPUTED, HUMANS_OWN):
            # Готовый вариант проверяем вторым голосом — но только там, где он поедет
            # человеку: это 2,5% + 7% прохода, и лишний запрос на каждую карточку мы
            # не платим.
            for claim in claims:
                приговор = panel.проверить_вариант(
                    поле=claim.get("field", ""), готовое=claim.get("fix", ""),
                    заголовок=str(display or ""), перевод=перевод)
                if приговор:
                    claim["fix_check"] = приговор
        return unit_id, display, verdict, why, claims, перевод

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = [pool.submit(one, row) for row in rows]
        for future in as_completed(futures):
            unit_id, display, verdict, why, claims, перевод = future.result()
            if verdict is None:
                отчёт["остановлено потолком"] = True
                continue
            if verdict == NO_RU:
                отчёт[NO_RU] = int(отчёт.get(NO_RU) or 0) + 1
                continue
            отчёт["проверено"] += 1
            отчёт[verdict] = int(отчёт.get(verdict) or 0) + 1
            if callable(on_card):
                on_card(unit_id, display, verdict, why)
            if not apply:
                continue
            _записать_отметку(unit_id, verdict, why, перевод)
            # ⛔ ВОПРОС ЗАВОДИТСЯ/ПЕРЕПИСЫВАЕТСЯ/СНИМАЕТСЯ ЗДЕСЬ ЖЕ, а не отдельным
            # прогоном. Отдельный шаг читал из базы одну склеенную строку `reference`
            # — и именно там терялись имя поля и готовый вариант. Здесь они в руках.
            что = донести_вердикт(unit_id, display, перевод, verdict, why, claims)
            if что == "владельцу":
                отчёт["ушло владельцу"] += 1
            elif что == "человеку":
                # Ошибка в САМОЙ фразе или в её переводе — а писал их человек, и
                # переписывать молча мы не имеем права: вопрос уходит АВТОРУ карточки
                # с готовым вариантом на кнопке. Решает он.
                отчёт["ушло человеку"] += 1
            elif что == "переписан":
                отчёт["вопрос переписан"] = int(отчёт.get("вопрос переписан") or 0) + 1
            elif что == "снят":
                отчёт["вопрос снят"] = int(отчёт.get("вопрос снят") or 0) + 1

    отчёт["потрачено"] = round(panel.cost, 4)
    отчёт["осталось"] = count_unchecked()
    # Уборка накопленного обязана быть ВИДНА числом и обязана убывать: иначе владелец
    # не отличит идущий пересуд от вставшего (правило 19.08.2026).
    отчёт["пересудить"] = count_stale_translation()
    отчёт["без перевода"] = count_without_translation()
    отчёт["вопросы из прозы"] = count_prose_questions()
    logging.info("панель, порция: %s", отчёт)
    return отчёт


# ── Пересуд накопленного: старым отметкам — тот же вопрос, что и новым ────────────
def units_with_verdict(verdict: str, limit: int) -> list[tuple]:
    """Единицы с этим вердиктом панели — для повторного прогона.

    Обычный отбор (`unchecked_units`) берёт то, чего панель не видела. Здесь наоборот:
    берём УЖЕ помеченное, потому что вопрос к судье с тех пор изменился.
    """
    from backend.database import get_db_connection_context
    from backend.lex_units import native_display_sql
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT u.id, u.display, u.kind, u.card,
                       {native_display_sql("u")} AS ru
                  FROM bt_3_field_checks c
                  JOIN bt_3_lex_units u ON u.id = c.unit_id
                 WHERE c.field = %s AND c.verdict = %s AND u.card IS NOT NULL
                   -- Карточку без перевода не судим и здесь: пустота на входе рождает
                   -- претензию к переводу, которого голос не видел.
                   AND COALESCE({native_display_sql("u")}, '') <> ''
                 ORDER BY c.checked_at DESC
                 LIMIT %s;""", (FIELD, str(verdict), int(limit)))
            return list(cur.fetchall() or [])


def rejudge_personal(*, limit: int = 500, budget_usd: float = 3.0, workers: int = 4,
                     apply: bool = True, on_card=None) -> dict[str, Any]:
    """РАЗОВЫЙ ПЕРЕСУД отметок «текст человека» — чтобы у них появился готовый вариант.

    ┌─ РЕШЕНИЕ ВЛАДЕЛЬЦА 31.08.2026. ──────────────────────────────────────────────┐
    │ Отметки, поставленные до этого дня, несут только претензию: поля «как надо»  │
    │ в вопросе к панели тогда не было. Человеку они приходят без кнопки «да,      │
    │ правильно так» — то есть ровно с тем дефектом, который мы в этот день чинили. │
    │ Выдумать вариант задним числом нельзя, значит надо спросить заново. Цена по   │
    │ замеру того же дня: $0.0042 за карточку, ≈$2 на все.                          │
    │ Это разовая уборка, а не ночная работа: новые отметки уже рождаются с         │
    │ готовым вариантом, и второй раз этот прогон не понадобится.                   │
    └──────────────────────────────────────────────────────────────────────────────┘

    ЧТО ДЕЛАЕМ С НОВЫМ ВЕРДИКТОМ, и ни одного молчаливого исхода:
      «текст человека» — вопрос автору переписывается новыми претензиями (или заводится);
      «подтверждено»   — претензии больше нет: отметка меняется, вопрос ЗАКРЫВАЕТСЯ.
                         Спрашивать человека о том, что мы сами больше не считаем
                         ошибкой, нельзя — его касание дороже нашей строки в базе;
      «дефект»         — виноваты наши примеры: карточку берёт ночной переписчик,
                         личный вопрос закрывается;
      «спорное»        — голоса разошлись, это вопрос владельцу; личный закрывается;
      «не спросили»    — не трогаем НИЧЕГО: отметка и вопрос остаются как были.

    Чужой открытый вопрос (спор владельца, проверка перевода) не трогаем и поверх
    него ничего не заводим.

    ⚠ САМА ТАБЛИЦА РЕШЕНИЙ ЖИВЁТ В `донести_вердикт` — здесь только счётчики этого
    прогона. Своя копия правил тут была до 02.09.2026 и разошлась бы с ночной.
    """
    отчёт: dict[str, Any] = {
        "взято": 0, "пересужено": 0, "с готовым вариантом": 0, "вопрос обновлён": 0,
        "вопрос заведён": 0, "снята претензия": 0, "ушло владельцу": 0,
        "наши примеры": 0, NOT_ASKED: 0, "потрачено": 0.0,
        "остановлено потолком": False,
    }
    нельзя = unavailable_reason()
    if нельзя:
        отчёт["пропущено"] = нельзя
        return отчёт

    rows = units_with_verdict(HUMANS_OWN, int(limit))
    отчёт["взято"] = len(rows)
    if not rows:
        return отчёт
    panel = Panel(budget_usd=budget_usd)

    def one(row):
        unit_id, display, kind, card, перевод = row
        перевод = str(перевод or "")
        if not перевод.strip():
            return unit_id, display, NO_RU, "", [], ""
        try:
            verdict, why, claims = panel.judge(entry_of(display, kind, card, перевод))
        except BudgetSpent as stop:
            return unit_id, display, None, str(stop), [], перевод
        if verdict in (DISPUTED, HUMANS_OWN):
            for claim in claims:
                приговор = panel.проверить_вариант(
                    поле=claim.get("field", ""), готовое=claim.get("fix", ""),
                    заголовок=str(display or ""), перевод=перевод)
                if приговор:
                    claim["fix_check"] = приговор
        return unit_id, display, verdict, why, claims, перевод

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = [pool.submit(one, row) for row in rows]
        for future in as_completed(futures):
            unit_id, display, verdict, why, claims, перевод = future.result()
            if verdict is None:
                отчёт["остановлено потолком"] = True
                continue
            if verdict == NOT_ASKED:
                # Голоса не ответили. Ни отметку, ни вопрос не трогаем: «не спросили»
                # это не новый вердикт, а авария связи.
                отчёт[NOT_ASKED] += 1
                continue
            if verdict == NO_RU:
                отчёт[NO_RU] = int(отчёт.get(NO_RU) or 0) + 1
                continue
            отчёт["пересужено"] += 1
            if callable(on_card):
                on_card(unit_id, display, verdict, why)
            if not apply:
                continue
            _записать_отметку(unit_id, verdict, why, перевод)
            if verdict == HUMANS_OWN and any(
                    str(c.get("fix") or "").strip() for c in claims):
                отчёт["с готовым вариантом"] += 1
            что = донести_вердикт(unit_id, display, перевод, verdict, why, claims)
            if что == "переписан":
                отчёт["вопрос обновлён"] += 1
            elif что == "человеку":
                отчёт["вопрос заведён"] += 1
            elif что == "снят":
                отчёт["снята претензия"] += 1
            elif что == "владельцу":
                отчёт["ушло владельцу"] += 1
            if verdict == DEFECT:
                # Виноваты НАШИ примеры — карточку заберёт ночной переписчик по отметке.
                отчёт["наши примеры"] += 1

    отчёт["потрачено"] = round(panel.cost, 4)
    logging.info("пересуд отметок «текст человека»: %s", отчёт)
    return отчёт
