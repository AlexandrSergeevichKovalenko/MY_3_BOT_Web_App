# -*- coding: utf-8 -*-
"""Судья синонимов/антонимов: слово, которого не знают словари, оценивает модель.

Владелец 06.09.2026: «мне нужно заключение модели — можно ли использовать это как синоним.
Если модель говорит „да“ — зачем там я? Если говорит „нет, ни в каких предложениях не
взаимозаменяемы“ — тоже зачем я?» Поэтому человеку остаётся ТОЛЬКО то, где судья
сомневается, и то, где справочник рода не знает артикль (артикль — не дело модели).

Как судим: подстановка. Судья берёт предложение с исходным словом, подставляет кандидата
и говорит: «да» (в обычном предложении взаимозаменяемы с тем же смыслом / для антонима —
с противоположным), «нет» (не слово, другая часть речи, лишь тематически связано, не
подставляется), «сомневаюсь» (сильно зависит от контекста или регистра). К каждому
вердикту — причина по-русски и пара предложений, чтобы владелец видел, на чём судья
основался, а не верил на слово.

Голос — ЧУЖОЙ, как у второго голоса словаря (backend/second_voice_check.py, решение
владельца 28.08.2026): список писал OpenAI, судит Gemini; не ответил — запасной GPT,
и кто судил, пишется в строку (judge_voice) и считается в отчёт. Не ответили оба —
строка остаётся НЕ судимой (judge_verdict пуст), владельцу не уходит, ночь повторит.

Вердикт «нет» снимает кандидата сам (status='removed', decision='judge_no').
Вердикт «да» кладёт кандидата в список сам (decision='judge_yes'; у существительного —
артикль справочника). Исключение: справочник рода слова не знает — тогда «да» судьи не
хватает, артикль ставит владелец кнопкой.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

MODEL = os.getenv("SYNONYM_JUDGE_MODEL", os.getenv("SECOND_VOICE_MODEL", "gemini-3.6-flash")).strip()
# Запасной — полный gpt-4.1, не mini: различие «синоним / лишь родственное» — ровно тот
# класс, где mini путается (см. word_diff_multilang в openai_manager._DEFAULT_TASK_MODELS).
RESERVE_MODEL = os.getenv("SYNONYM_JUDGE_RESERVE_MODEL", "gpt-4.1-2025-04-14").strip()
TIMEOUT_SEC = float(os.getenv("SYNONYM_JUDGE_TIMEOUT_SEC", "60"))

SYSTEM = """ROLE: You are an independent German lexicography referee for a B2–C1 vocabulary game.
You have NOT seen any prior reasoning. You get a target word and candidate words that a
different model proposed as its synonyms (relation="synonym") or antonyms (relation="antonym").
Two dictionaries (OpenThesaurus, de.wiktionary) did NOT confirm these pairs. Decide by
SUBSTITUTION, not by association.

INPUT JSON: {"target":"...","relation":"synonym"|"antonym","meaning_ru":"...",
"base_sentence_de":"..." (may be empty),"candidates":[{"de":"...","ru":"..."}]}

For EACH candidate, in input order:
1. Take a natural, typical B2 sentence with `target` in the meaning `meaning_ru`
   (use base_sentence_de if given, else write one).
2. Replace `target` with the candidate, adjusting only grammar (inflection, article).
3. verdict:
   "yes"    — the sentence stays natural and idiomatic, and the meaning is the SAME
              (synonym) / the natural OPPOSITE (antonym). Register may differ slightly.
   "no"     — not a real German word, misspelled, wrong part of speech, a whole phrase
              that cannot replace one word, only thematically related, or the meaning
              shifts. For antonym: not an opposite (a synonym, or merely different).
   "unsure" — replaceable only in some contexts or with a clear change of register,
              and a competent teacher could argue either way.
4. reason_ru — ONE short Russian sentence (max ~15 words) naming the decisive fact.
5. example_target_de / example_candidate_de — the two sentences from steps 1–2
   (empty strings only when verdict is "no" because the word does not exist).

Be conservative: "yes" only when you would accept the candidate as a correct answer
from a learner. Every candidate exactly once, same order.

Return STRICT JSON ONLY, no markdown:
{"ok":true,"rows":[{"de":"...","verdict":"yes|no|unsure","reason_ru":"...",
"example_target_de":"...","example_candidate_de":"..."}]}"""

_STATS: dict[str, int] = {"gemini": 0, "openai": 0, "unjudged": 0}


def stats() -> dict[str, int]:
    return dict(_STATS)


def _payload(target: str, relation: str, meaning_ru: str, base_de: str, cands: list[dict]) -> str:
    return json.dumps({"target": target, "relation": relation, "meaning_ru": meaning_ru,
                       "base_sentence_de": base_de or "", "candidates": cands}, ensure_ascii=False)


def _parse(text: str, cands: list[dict]) -> list[dict] | None:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw[:4].lower() == "json":
            raw = raw[4:]
        raw = raw.strip()
    data = json.loads(raw or "{}")
    if not isinstance(data, dict) or not data.get("ok"):
        return None
    rows = data.get("rows")
    if not isinstance(rows, list) or len(rows) != len(cands):
        return None
    out = []
    for c, r in zip(cands, rows):
        v = str((r or {}).get("verdict") or "").strip().lower()
        if v not in ("yes", "no", "unsure"):
            return None
        if str((r or {}).get("de") or "").strip().lower() != str(c["de"]).strip().lower():
            return None
        out.append({"de": c["de"], "verdict": v,
                    "reason_ru": str((r or {}).get("reason_ru") or "").strip(),
                    "example_target_de": str((r or {}).get("example_target_de") or "").strip(),
                    "example_candidate_de": str((r or {}).get("example_candidate_de") or "").strip()})
    return out


def _ask_gemini(payload: str, cands: list[dict]) -> tuple[list[dict] | None, str]:
    api_key = str(os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return None, "нет ключа GEMINI_API_KEY"
    import requests
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"
    body = {"system_instruction": {"parts": [{"text": SYSTEM}]},
            "contents": [{"role": "user", "parts": [{"text": payload}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"}}
    try:
        resp = requests.post(url, params={"key": api_key}, json=body, timeout=TIMEOUT_SEC)
        if resp.status_code != 200:
            logging.warning("судья синонимов (Gemini) не ответил: HTTP %s %s", resp.status_code, resp.text[:200])
            return None, f"Gemini HTTP {resp.status_code}"
        parts = (resp.json().get("candidates") or [{}])[0].get("content", {}).get("parts") or []
        rows = _parse("".join(str(p.get("text") or "") for p in parts), cands)
        return (rows, "") if rows is not None else (None, "Gemini: ответ не разобран")
    except Exception as exc:                       # noqa: BLE001 — причину называем наверх
        logging.warning("судья синонимов (Gemini) не ответил: %s", exc)
        return None, f"Gemini {type(exc).__name__}"


def _ask_openai(payload: str, cands: list[dict]) -> tuple[list[dict] | None, str]:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, "нет ключа OPENAI_API_KEY"
    try:
        from backend.synthetic_load import build_sync_openai_client
        client = build_sync_openai_client(api_key=api_key, timeout=TIMEOUT_SEC)
        resp = client.chat.completions.create(
            model=RESERVE_MODEL,
            messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": payload}],
            temperature=0, response_format={"type": "json_object"},
        )
        rows = _parse(str(resp.choices[0].message.content or ""), cands)
        return (rows, "") if rows is not None else (None, "GPT: ответ не разобран")
    except Exception as exc:                       # noqa: BLE001
        logging.warning("судья синонимов (запасной GPT) не ответил: %s", exc)
        return None, f"GPT {type(exc).__name__}"


def judge_candidates(*, target: str, relation: str, meaning_ru: str, base_de: str,
                     candidates: list[dict], ask_gemini=None, ask_openai=None) -> dict[str, Any]:
    """{"rows": [...], "voice": "gemini"|"openai", "why": ""} либо {"rows": None, "voice": "",
    "why": "<почему не судили>"}. Оба голоса по очереди; не ответил никто — не судили."""
    cands = [{"de": str(c.get("de") or "").strip(), "ru": str(c.get("ru") or "").strip()}
             for c in candidates if str(c.get("de") or "").strip()]
    if not cands:
        return {"rows": [], "voice": "", "why": ""}
    payload = _payload(target, relation, meaning_ru, base_de, cands)
    rows, why = (ask_gemini or _ask_gemini)(payload, cands)
    if rows is not None:
        _STATS["gemini"] += 1
        return {"rows": rows, "voice": "gemini", "why": ""}
    rows2, why2 = (ask_openai or _ask_openai)(payload, cands)
    if rows2 is not None:
        _STATS["openai"] += 1
        return {"rows": rows2, "voice": "openai", "why": ""}
    _STATS["unjudged"] += 1
    return {"rows": None, "voice": "", "why": f"{why}; {why2}"}


# ── применение к очереди ──────────────────────────────────────────────────────

def judge_open_reviews(*, apply: bool = True, limit_words: int | None = None,
                       log=print, ask_gemini=None, ask_openai=None) -> dict:
    """Все открытые строки очереди без вердикта — судье, пословно (один запрос на слово).

    apply=False — только напечатать вердикты, базу не трогать (и вердикты не записывать).
    Возвращает сводку: слов, кандидатов, да/нет/сомнения/не судили, голоса."""
    from backend.database import get_db_connection_context
    from backend.sprint_intake import ensure_sprint_intake_schema, apply_owner_decision, ARTICLE_UNKNOWN
    ensure_sprint_intake_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT r.sprint_id, r.relation, r.wort, b.hint_ru, b.erklaerung, "
                "       COALESCE(b.trainer_json->'target_example'->>'de', '') "
                "FROM bt_3_sprint_accepted_review r JOIN bt_3_sprint_bank b USING (sprint_id) "
                "WHERE r.status = 'open' AND r.judge_verdict IS NULL "
                "GROUP BY r.sprint_id, r.relation, r.wort, b.hint_ru, b.erklaerung, b.trainer_json "
                "ORDER BY r.wort" + (" LIMIT %s" if limit_words else ""),
                ((int(limit_words),) if limit_words else ()),
            )
            words = cur.fetchall() or []
    summary = {"words": 0, "candidates": 0, "yes": 0, "no": 0, "unsure": 0, "unjudged": 0,
               "kept": 0, "removed": 0, "gemini": 0, "openai": 0}
    for sprint_id, relation, wort, hint_ru, erklaerung, base_de in words:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, de, ru, reason FROM bt_3_sprint_accepted_review "
                            "WHERE sprint_id = %s AND status = 'open' AND judge_verdict IS NULL ORDER BY id",
                            (sprint_id,))
                rows = cur.fetchall() or []
        if not rows:
            continue
        meaning = " — ".join(x for x in (str(hint_ru or "").strip(), str(erklaerung or "").strip()) if x)
        res = judge_candidates(target=wort, relation=relation, meaning_ru=meaning, base_de=base_de,
                               candidates=[{"de": r[1], "ru": r[2]} for r in rows],
                               ask_gemini=ask_gemini, ask_openai=ask_openai)
        summary["words"] += 1
        summary["candidates"] += len(rows)
        if res["rows"] is None:
            summary["unjudged"] += len(rows)
            log(f"{wort}: судья не ответил ({res['why']}) — {len(rows)} кандидатов ждут следующей ночи")
            continue
        summary[res["voice"]] += 1
        for (row_id, de, ru, reason), verdict in zip(rows, res["rows"]):
            v = verdict["verdict"]
            summary[v] += 1
            log(f"{wort} → {de}: {v} · {verdict['reason_ru']}")
            if not apply:
                continue
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE bt_3_sprint_accepted_review SET judge_verdict = %s, judge_reason = %s, "
                        "judge_example_target = %s, judge_example_candidate = %s, judge_voice = %s, "
                        "judged_at = NOW() WHERE id = %s",
                        (v, verdict["reason_ru"], verdict["example_target_de"],
                         verdict["example_candidate_de"], res["voice"], int(row_id)),
                    )
                conn.commit()
            if v == "no":
                out = apply_owner_decision(int(row_id), "drop", by="judge")
                summary["removed"] += int(bool(out))
            elif v == "yes" and reason != ARTICLE_UNKNOWN:
                out = apply_owner_decision(int(row_id), "keep", by="judge")
                summary["kept"] += int(bool(out))
            # yes + артикль неизвестен, и все unsure — остаются владельцу.
    return summary
