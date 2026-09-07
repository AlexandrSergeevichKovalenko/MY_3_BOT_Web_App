"""Судья синонимов (backend/synonym_judge.py), 06.09.2026.

Владелец: «если модель говорит „да“ — зачем я? если „нет“ — тоже зачем я?» Человеку —
только сомнения судьи. Тесты — без сети и без базы: голоса подменяются."""
from __future__ import annotations

import json

from backend.synonym_judge import _parse, judge_candidates

CANDS = [{"de": "der Zufall", "ru": "случайность"}, {"de": "die Öffnung", "ru": "открытие"},
         {"de": "die Situation", "ru": "ситуация"}]


def _answer(verdicts):
    rows = [{"de": c["de"], "verdict": v, "reason_ru": f"причина {c['de']}",
             "example_target_de": "Das war eine gute Gelegenheit.",
             "example_candidate_de": f"Das war ein guter {c['de']}."} for c, v in zip(CANDS, verdicts)]
    return json.dumps({"ok": True, "rows": rows}, ensure_ascii=False)


def test_ответ_разбирается_по_порядку_и_с_причиной():
    rows = _parse(_answer(["no", "no", "unsure"]), CANDS)
    assert [r["verdict"] for r in rows] == ["no", "no", "unsure"]
    assert rows[2]["reason_ru"] == "причина die Situation"


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
        calls.append("openai"); return _parse(_answer(["no", "no", "unsure"]), cands), ""
    res = judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="случай, шанс",
                           base_de="", candidates=CANDS, ask_gemini=gem, ask_openai=gpt)
    assert res["voice"] == "openai" and calls == ["gemini", "openai"]
    assert [r["verdict"] for r in res["rows"]] == ["no", "no", "unsure"]

    def gpt_down(payload, cands):
        return None, "GPT Timeout"
    res = judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="", base_de="",
                           candidates=CANDS, ask_gemini=gem, ask_openai=gpt_down)
    assert res["rows"] is None and res["voice"] == "" and "429" in res["why"]


def test_в_запросе_судье_есть_смысл_слова_и_кандидаты_с_переводом():
    seen = {}
    def gem(payload, cands):
        seen.update(json.loads(payload)); return _parse(_answer(["no", "no", "no"]), cands), ""
    judge_candidates(target="die Gelegenheit", relation="synonym", meaning_ru="удобный случай",
                     base_de="Das ist eine gute Gelegenheit.", candidates=CANDS, ask_gemini=gem)
    assert seen["meaning_ru"] == "удобный случай" and seen["base_sentence_de"].startswith("Das ist")
    assert seen["candidates"][1] == {"de": "die Öffnung", "ru": "открытие"}
