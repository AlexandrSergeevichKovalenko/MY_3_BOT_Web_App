# -*- coding: utf-8 -*-
"""СПЛОШНОЙ ПРОХОД ПО ФРАЗАМ: три независимых голоса, решает большинство.

ЗАЧЕМ. 11 453 карточки словаря — словосочетания и предложения. Печатного справочника на
них нет: ни Wiktionary, ни DWDS не знают «die Hose anhaben» как статью. Значит судит
модель, и единственная защита от её выдумок — несколько НЕЗАВИСИМЫХ голосов.

ПРАВИЛО, ПРОВЕРЕННОЕ ЗАМЕРОМ 23.08.2026 на 40 карточках:
    двое из трёх назвали ОДНО И ТО ЖЕ поле  → дефект настоящий, чиним;
    двое из трёх промолчали                  → карточка чистая («чисто» — тоже голос);
    все три разошлись                        → владельцу.
Числа замера: 85% чистых, 12% согласных дефектов, 2,5% спорных (≈286 на всю базу).

ПОЧЕМУ ТРИ, А НЕ ДВА. Два голоса OpenAI обучены одинаково и ошибаются одинаково: они
давали 15% разногласий — 1 718 карточек владельцу, которые он физически не разберёт.
Третий голос от ДРУГОГО производителя (Gemini) снял это до 286.

⚠ ДВЕ ОШИБКИ ЗАМЕРА, ЗАПЕРТЫЕ ЗДЕСЬ НАВСЕГДА — обе были в вопросе, а не в данных:
  • «пример обязан содержать заголовок» — неверно: немецкий склоняется, «Sie bot ihrem
    Chef die Stirn» иллюстрируется примером «Er bietet seinem Vorgesetzten die Stirn».
    Gemini прочёл требование буквально и ругал каждую вторую карточку;
  • «спор» считался по согласию О ПОЛЕ, и карточка, где двое молчат, а один придрался,
    уходила владельцу. Молчание большинства — это вердикт «чисто», а не спор.

ЭТОТ ПРОХОД НИЧЕГО НЕ ПЕРЕПИСЫВАЕТ В КАРТОЧКАХ. Он ставит отметку по каждой карточке:
что проверено, кем, когда, какой вердикт. Исправление текста — отдельный шаг, где панель
предлагает готовый вариант и он тоже принимается только по согласию.

ПРОДОЛЖЕНИЕ С МЕСТА ОСТАНОВКИ. Уже проверенные карточки пропускаются по отметке, поэтому
прогон можно прерывать и запускать снова — он не начнёт сначала и не потратит деньги
дважды.

    python3 scripts/dict_phrase_panel_audit.py --limit 20        # проба, без записи
    python3 scripts/dict_phrase_panel_audit.py --apply           # весь проход
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context      # noqa: E402

FIELD = "phrase_panel"
CLEAN, DEFECT, DISPUTED, NOT_ASKED = "подтверждено", "дефект", "спорное", "не спросили"
HUMANS_OWN = "текст человека — решает он"

# ⛔ ЧТО ЧИНИМ, А ЧТО НЕ НАШЕ. Решение владельца 23.08.2026, дословно: «это же сам
# пользователь записал, мы должны это оставить».
#
# Восемь фраз из десяти человек написал или выбрал сам: 6 658 прислал боту, 490 сохранил
# из словаря, 734 пришло импортом, 2 128 заведены до учёта происхождения. Заголовок такой
# карточки — ЕГО текст: он так услышал, так записал, так запомнил. Переписать его молча
# значит подменить человеку память, и никакая правота справочника этого не оправдывает.
#
# Примеры, разбор и перевод у той же карточки сочинили МЫ — это наша работа, и её мы
# чиним свободно. Заголовок из нашего текста (новость, книга, субтитры) тоже не трогаем:
# это цитата источника, а не наша выдумка.
OUR_OWN_FIELDS = {"examples", "meaning"}          # это сделали мы — чиним
HUMAN_FIELDS = {"headword", "translation"}        # это его слова — только показать ему
MODEL_A = "gpt-4.1-2025-04-14"
MODEL_B = "gpt-4.1-mini"
MODEL_C = "gemini-3.6-flash"
# Цены OpenAI — из нашей bt_3_billing_price_snapshots. Цена Gemini — публичный прайс
# Google на 23.08.2026, помечена как оценка и сверяется по счёту после первого дня.
PRICE_OPENAI = {MODEL_A: (2.0 / 1e6, 8.0 / 1e6), MODEL_B: (0.4 / 1e6, 1.6 / 1e6)}
PRICE_GEMINI = (0.30 / 1e6, 2.50 / 1e6)

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


def prod(var: str, service: str = "Postgres") -> str:
    import subprocess
    out = subprocess.run(["railway", "variables", "--service", service, "--json"],
                         capture_output=True, text=True).stdout
    value = json.loads(out).get(var)
    if not value:
        raise RuntimeError(f"в боевом окружении нет {var}")
    return value


def _fields(text: str):
    """Разбор ответа голоса: (поля, сводка словами, претензии по пунктам).

    Поля = None — ответ не разобран, и это НЕ «чисто».

    ПРЕТЕНЗИИ ВОЗВРАЩАЮТСЯ ПОШТУЧНО, а не одной строкой. Склейка через «; » была
    придумана для колонки `reference`, где нужен человекочитаемый след, — но по дороге
    к владельцу она уносила ДВЕ вещи: имя поля (о чём спор) и готовый вариант. Экран
    после этого печатал «спор о карточке» над претензией к самой фразе, а исправить
    перевод было нечем (разобрано с владельцем 31.08.2026).
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


class BudgetSpent(Exception):
    """Деньги кончились — прогон останавливается, а не «доделывает по-быстрому»."""


class Panel:
    # ⛔ ПОТОЛОК РАСХОДА. Владелец 23.08.2026: «чтобы мы не превышали 15 евро затрат,
    # это важно». Потолок стоит в USD и ниже названной суммы: доллар дешевле евро, так
    # что $15 гарантированно укладывается в €15, даже если курс качнётся.
    #
    # Это не «предупреждение в лог»: по достижении потолка прогон ПАДАЕТ и больше ни
    # одного платного запроса не делает. Уже проверенные карточки помечены, поэтому
    # следующий запуск продолжит с того же места и ничего не оплатит дважды.
    BUDGET_USD = 15.0

    def __init__(self) -> None:
        os.environ.setdefault("OPENAI_API_KEY",
                              prod("OPENAI_API_KEY", "BACKEND_WEB(backend:server.py)"))
        from openai import OpenAI
        from google import genai
        # ⏱ ТАЙМАУТ ОБЯЗАТЕЛЕН. Прогон 23.08.2026 завис на 3 800-й карточке из 4 953:
        # запрос ушёл без ограничения и висел 55 минут, а вместе с ним стояла и вся
        # очередь. Провайдер, который «думает» дольше минуты, — это авария, а не долгий
        # ответ: лучше честно не спросить эту карточку и оставить её в остатке.
        self._openai = OpenAI(timeout=60.0, max_retries=0)
        self._gemini = genai.Client(
            api_key=prod("GEMINI_API_KEY", "BACKEND_WEB(backend:server.py)"))
        self.cost = 0.0

    def _openai_vote(self, model: str, payload: str):
        answer = self._openai.chat.completions.create(
            model=model, temperature=0, response_format={"type": "json_object"},
            messages=[{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": payload}])
        self.cost += (answer.usage.prompt_tokens * PRICE_OPENAI[model][0]
                      + answer.usage.completion_tokens * PRICE_OPENAI[model][1])
        return _fields(answer.choices[0].message.content)

    def _gemini_vote(self, payload: str):
        from google.genai import types
        answer = self._gemini.models.generate_content(
            model=MODEL_C, contents=payload,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM, temperature=0,
                response_mime_type="application/json",
                http_options=types.HttpOptions(timeout=60_000)))   # мс, см. выше
        usage = answer.usage_metadata
        # ⚠ «РАЗМЫШЛЕНИЯ» ТОЖЕ ПЛАТНЫЕ, и это стоило нам реальных денег 23.08.2026.
        # Замер одного запроса: вход 67, ОТВЕТ 5, размышления 343, всего 415 токенов.
        # Считая только ответ, я занижал выход в семьдесят раз: счётчик показал $5.83,
        # счёт Google пришёл на €8.28. Потолок расхода, построенный на такой арифметике,
        # не защищает вообще — он просто врёт медленнее.
        #
        # Поэтому берём выход как candidates + thoughts. Если Google заведёт ещё одно
        # поле выхода, разница снова всплывёт на счёте — сверка со счётом обязательна,
        # мои формулы её не заменяют.
        self.cost += ((usage.prompt_token_count or 0) * PRICE_GEMINI[0]
                      + ((usage.candidates_token_count or 0)
                         + (usage.thoughts_token_count or 0)) * PRICE_GEMINI[1])
        return _fields(answer.text)

    def judge(self, entry: dict) -> tuple[str, str, list]:
        """(вердикт, пояснение, претензии по пунктам).

        Голос, который не ответил, НЕ засчитывается молчанием. Третьим значением идут
        претензии каждого голоса поштучно — с именем поля и готовым вариантом; они
        уезжают владельцу, когда вердикт «спорное», и пропадать по дороге не имеют
        права (см. `_fields`)."""
        if self.cost >= self.BUDGET_USD:
            raise BudgetSpent(f"потрачено ${self.cost:.2f} — потолок ${self.BUDGET_USD:.2f}")
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
        которое никто не проверил, — это ровно то же самое, только опаснее: оно
        выглядит как ответ и стоит на кнопке. Поэтому предложение сверяется парной
        проверкой смысла (`openai_manager.run_translation_pair_check`, gpt-4.1-mini,
        ≈$0.0001): означает ли русский эту немецкую фразу.

        Три состояния и ни одного молчаливого: годится / не годится / спросить не
        удалось. Последнее НЕ притворяется ни первым, ни вторым — кнопку не рисуем,
        но и обвинить вариант не в чем.
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
        # на заниженной арифметике. Один раз это уже стоило разницы между счётчиком
        # ($5.83) и счётом Google (€8.28), см. рамку в `_gemini_vote`.
        usage = _LAST_LLM_USAGE.get() or {}
        self.cost += (int(usage.get("prompt_tokens") or 0) * PRICE_OPENAI[MODEL_B][0]
                      + int(usage.get("completion_tokens") or 0) * PRICE_OPENAI[MODEL_B][1])
        if not ответ.get("checked"):
            return {"state": "unknown", "why": "проверить не удалось"}
        return ({"state": "ok", "why": ""} if ответ.get("ok")
                else {"state": "bad", "why": str(ответ.get("why") or "")[:300]})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--budget", type=float, default=Panel.BUDGET_USD,
                        help="потолок расхода в долларах; по умолчанию 15")
    args = parser.parse_args()
    Panel.BUDGET_USD = float(args.budget)

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.display, u.kind, u.card
                FROM bt_3_lex_units u
                LEFT JOIN bt_3_field_checks c ON c.unit_id = u.id AND c.field = %s
                WHERE u.lang='de' AND u.kind <> 'word' AND u.card IS NOT NULL
                  AND c.unit_id IS NULL
                ORDER BY u.id;""", (FIELD,))
            rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]
    print(f"карточек к проверке: {len(rows)} (уже проверенные пропущены)\n")
    if not rows:
        return 0

    panel = Panel()
    tally: dict[str, int] = {}
    started = time.time()
    done = 0
    отдано = 0          # сколько спорных карточек ушло владельцу вопросом

    def one(row):
        unit_id, display, kind, card = row
        перевод = str((card or {}).get("translation_ru") or "")
        entry = {"headword": display, "kind": kind,
                 "translation": (card or {}).get("translation_ru"),
                 "saved_meaning": (card or {}).get("translation_ru"),
                 "examples": (card or {}).get("usage_examples")}
        try:
            verdict, why, claims = panel.judge(entry)
        except BudgetSpent as stop:
            # Отметку НЕ ставим: карточку не проверяли. Она останется в остатке и
            # достанется следующему запуску — это честнее, чем записать «чисто».
            return unit_id, display, None, str(stop), [], перевод
        if verdict == DISPUTED:
            # Готовый вариант проверяем ВТОРЫМ ГОЛОСОМ — но только у спорных карточек:
            # это 2,5% прохода, и лишний запрос на каждую из пяти тысяч мы не платим.
            for claim in claims:
                приговор = panel.проверить_вариант(
                    поле=claim.get("field", ""), готовое=claim.get("fix", ""),
                    заголовок=str(display or ""),
                    перевод=перевод)
                if приговор:
                    claim["fix_check"] = приговор
        return unit_id, display, verdict, why, claims, перевод

    stopped_by_budget = False
    # ⏱ Результаты берём ПО МЕРЕ ГОТОВНОСТИ, а не по порядку. Прежняя версия шла
    # `pool.map`, и одна зависшая карточка держала всю очередь: работа стояла, лог молчал,
    # а снаружи это выглядело как «идёт».
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(one, row) for row in rows]
        for future in as_completed(futures):
            unit_id, display, verdict, why, claims, перевод = future.result()
            if verdict is None:
                stopped_by_budget = True
                continue
            tally[verdict] = tally.get(verdict, 0) + 1
            done += 1
            if verdict != CLEAN:
                print(f"   {display[:44]:46} {verdict:14} {why[:60]}")
            if args.apply:
                with get_db_connection_context() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO bt_3_field_checks
                                (unit_id, field, verdict, source, ours, reference, checked_at)
                            VALUES (%s,%s,%s,%s,NULL,%s,NOW())
                            ON CONFLICT (unit_id, field) DO UPDATE
                               SET verdict=EXCLUDED.verdict, reference=EXCLUDED.reference,
                                   checked_at=NOW();""",
                                    (unit_id, FIELD, verdict,
                                     "панель: gpt-4.1 + gpt-4.1-mini + gemini-3.6-flash",
                                     why[:400] or None))
                        conn.commit()
                if verdict == DISPUTED:
                    # ⛔ ВОПРОС ВЛАДЕЛЬЦУ ЗАВОДИТСЯ ЗДЕСЬ ЖЕ, а не отдельным прогоном.
                    # Отдельный шаг (`dict_panel_disputes_to_owner.py`) читал из базы
                    # только колонку `reference` — склеенную строку, — и именно там
                    # терялись имя поля и готовый вариант. Здесь они ещё в руках.
                    from backend.database import open_panel_card_question
                    if open_panel_card_question(unit_id, display, перевод, claims):
                        отдано += 1
            if done % 100 == 0:
                speed = done / max(time.time() - started, 1)
                left = (len(rows) - done) / max(speed, 0.001) / 60
                print(f"   … {done}/{len(rows)}, ${panel.cost:.2f}, осталось ~{left:.0f} мин")

    print("\n— ИТОГ")
    for verdict, count in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"   {verdict:16} {count:>6}  ({100*count/len(rows):.1f}%)")
    if отдано:
        print(f"\n   ушло владельцу вопросом: {отдано} "
              f"(экран «Спорные фразы», с именем поля и готовым вариантом)")
    print(f"\n   потрачено: ${panel.cost:.2f} из потолка ${Panel.BUDGET_USD:.2f},"
          f" время {(time.time()-started)/60:.0f} мин")
    if stopped_by_budget:
        checked = sum(tally.values())
        print(f"\n   ⛔ ПРОГОН ОСТАНОВЛЕН ПОТОЛКОМ РАСХОДА. Проверено {checked} карточек,")
        print(f"      не проверено {len(rows) - checked} — они остались в остатке и ждут")
        print("      следующего запуска. Ни одна из них не помечена как проверенная.")
    if not args.apply:
        print("\n(холостой прогон: отметки не записаны, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
