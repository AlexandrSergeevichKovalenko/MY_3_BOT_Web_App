"""Судья синонимов (backend/synonym_judge.py).

06.09.2026: «если модель говорит „да“ — зачем я? если „нет“ — тоже зачем я?»
08.09.2026: «я не понимаю, зачем мне вообще высылается это на согласование… модель ставит
итоговую точку». Вердикта «сомневаюсь» больше нет, голосов три, большинство.
Тесты — без сети и без базы: голоса подменяются."""
from __future__ import annotations

import json

from backend.synonym_judge import SYSTEM, _parse, judge_by_majority, judge_candidates

CANDS = [{"de": "der Zufall", "ru": "случайность"}, {"de": "die Öffnung", "ru": "открытие"},
         {"de": "die Situation", "ru": "ситуация"}]


def _answer(verdicts):
    rows = [{"de": c["de"], "verdict": v, "reason_ru": f"причина {c['de']} {v}",
             "example_target_de": "Das war eine gute Gelegenheit.",
             "example_candidate_de": f"Das war ein guter {c['de']}."} for c, v in zip(CANDS, verdicts)]
    return json.dumps({"ok": True, "rows": rows}, ensure_ascii=False)


def test_ответ_разбирается_по_порядку_и_с_причиной():
    rows = _parse(_answer(["no", "no", "yes"]), CANDS)
    assert [r["verdict"] for r in rows] == ["no", "no", "yes"]
    assert rows[2]["reason_ru"] == "причина die Situation yes"


def test_сомневаюсь_больше_не_вердикт():
    """Владелец 08.09.2026: «нам нужно построить схему, при которой модель не будет
    сомневаться». Ответ с «unsure» — не ответ, голос не засчитывается."""
    assert _parse(_answer(["no", "unsure", "yes"]), CANDS) is None
    assert '"unsure"' not in SYSTEM.split("verdict")[1].split("reason_ru")[0].replace('There is no "unsure"', "")
    assert "Be conservative" not in SYSTEM
    assert "did NOT confirm" not in SYSTEM   # неверная посылка, толкавшая к «нет»


def test_неполный_или_чужой_ответ_не_принимается():
    assert _parse(json.dumps({"ok": True, "rows": []}), CANDS) is None
    bad = json.loads(_answer(["no", "no", "yes"])); bad["rows"][0]["de"] = "der Fall"
    assert _parse(json.dumps(bad), CANDS) is None
    bad = json.loads(_answer(["no", "no", "yes"])); bad["rows"][1]["verdict"] = "maybe"
    assert _parse(json.dumps(bad), CANDS) is None
    assert _parse("```json\n" + _answer(["yes", "no", "no"]) + "\n```", CANDS)[0]["verdict"] == "yes"


def test_основной_голос_gemini_запасной_gpt_и_честное_не_судили():
    calls = []
    def gem(payload, cands):
        calls.append("gemini"); return None, "Gemini HTTP 429"
    def gpt(payload, cands):
        calls.append("openai"); return _parse(_answer(["no", "no", "yes"]), cands), ""
    res = judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="случай, шанс",
                           base_de="", candidates=CANDS, ask_gemini=gem, ask_openai=gpt)
    assert res["voice"] == "openai" and calls == ["gemini", "openai"]
    assert [r["verdict"] for r in res["rows"]] == ["no", "no", "yes"]

    def gpt_down(payload, cands):
        return None, "GPT Timeout"
    res = judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="", base_de="",
                           candidates=CANDS, ask_gemini=gem, ask_openai=gpt_down)
    assert res["rows"] is None and res["voice"] == "" and "429" in res["why"]


def test_три_голоса_большинство_ничья_нет():
    """Три голоса (владелец 08.09.2026, «как у спринта артиклей»): 2 из 3 «да» — да;
    1 из 3 — нет; при двух дошедших 1:1 — нет (та же осторожность, что у стража темы)."""
    answers = iter([["yes", "no", "yes"], ["yes", "no", "no"], ["no", "no", "yes"]])
    def gem(payload, cands):
        return _parse(_answer(next(answers)), cands), ""
    res = judge_by_majority(target="die Gelegenheit", relation="synonym", meaning_ru="случай",
                            base_de="", candidates=CANDS, votes=3, ask_gemini=gem)
    assert res["heard"] == 3 and res["voice"] == "gemini:3"
    assert [(r["verdict"], r["yes_votes"]) for r in res["rows"]] == [("yes", 2), ("no", 0), ("yes", 2)]
    # причина берётся из голоса, совпавшего с итогом
    assert res["rows"][0]["reason_ru"] == "причина der Zufall yes"
    assert res["rows"][1]["reason_ru"] == "причина die Öffnung no"

    answers = iter([["yes", "yes", "yes"], None, ["no", "no", "no"]])
    def gem2(payload, cands):
        a = next(answers)
        return (None, "Gemini HTTP 500") if a is None else (_parse(_answer(a), cands), "")
    def gpt_down(payload, cands):
        return None, "GPT Timeout"
    res = judge_by_majority(target="die Gelegenheit", relation="synonym", meaning_ru="случай",
                            base_de="", candidates=CANDS, votes=3, ask_gemini=gem2, ask_openai=gpt_down)
    assert res["heard"] == 2 and [r["verdict"] for r in res["rows"]] == ["no", "no", "no"]


def test_ни_одного_голоса_это_не_нет():
    def down(payload, cands):
        return None, "нет сети"
    res = judge_by_majority(target="die Gelegenheit", relation="synonym", meaning_ru="", base_de="",
                            candidates=CANDS, votes=3, ask_gemini=down, ask_openai=down)
    assert res["rows"] is None and res["heard"] == 0 and "нет сети" in res["why"]


def test_в_запросе_судье_есть_смысл_слова_и_кандидаты_с_переводом():
    seen = {}
    def gem(payload, cands):
        seen.update(json.loads(payload)); return _parse(_answer(["no", "no", "no"]), cands), ""
    judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="удобный случай",
                     base_de="Das ist eine gute Gelegenheit.", candidates=CANDS, ask_gemini=gem)
    assert seen["meaning_ru"] == "удобный случай" and seen["base_sentence_de"].startswith("Das ist")
    assert seen["candidates"][1] == {"de": "die Öffnung", "ru": "открытие"}
