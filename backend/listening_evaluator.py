"""
Hörverständnis answer evaluator.

Single GPT call evaluates all 4 user answers against the original text.
Returns structured feedback: factual correctness, grammar, model answer.
"""
from __future__ import annotations

import json
import logging
import os

_EVAL_SYSTEM = """\
Du bist ein strenger, aber fairer Deutschlehrer und bewertest Hörverständnis-Antworten auf B2-Niveau.

Deine Aufgabe: Vergleiche die Antworten des Lernenden mit dem Originaltext und bewerte sie
auf ZWEI getrennten Achsen. WICHTIG: Diese beiden Achsen sind VOLLSTÄNDIG UNABHÄNGIG.

1. INHALT (content_score: 0.0–1.0):
   - Hat der Lernende die geforderten Fakten/Details erfasst (Uhrzeiten, Bedingungen,
     Ausnahmen, alle im Frage-Satz verlangten Punkte)?
   - 1.0 = alle geforderten Inhalte korrekt; 0.5 = etwa die Hälfte / ein Teil fehlt
     oder ist falsch; 0.0 = Kernaussage verfehlt.
   - Bewerte NUR den SINN. GRAMMATIK- ODER RECHTSCHREIBFEHLER DÜRFEN content_score
     NICHT senken. Beispiel: „in den Reihen fünf bis zehnte" statt „5 bis 10" ist
     inhaltlich KORREKT (die Reihen stimmen) — content_score bleibt hoch, der
     Fehler „zehnte" zählt nur bei der Grammatik.
   - content_feedback_ru: 1-2 Sätze auf Russisch, NUR zum Inhalt.

2. GRAMMATIK & SCHREIBWEISE (grammar_score: 0.0–1.0):
   - Wie korrekt ist der deutsche Satz sprachlich (Kasus, Wortstellung,
     Präpositionen, Rechtschreibung, Wortwahl)?
   - 1.0 = fehlerfrei; sinkt mit jedem Fehler (ein kleiner Fehler ≈ 0.8, mehrere
     Fehler / unklarer Satzbau ≈ 0.4, kaum verständlich ≈ 0.1).
   - grammar_errors: konkrete Fehler als Liste ["'...' → '...'", ...]; keine Fehler → [].
   - grammar_feedback_ru: 1 kurzer Satz auf Russisch zur Sprache (oder "").

3. MUSTERLÖSUNG (model_answer_de): der ideale, vollständige deutsche Antwortsatz.

Antworte NUR mit validem JSON."""

_EVAL_USER_TMPL = """\
Originaltext:
\"\"\"{german_text}\"\"\"

Bewerte folgende Antworten:

{answers_block}

JSON-Format:
{{
  "evaluations": [
    {{
      "number": 1,
      "content_score": 1.0,
      "content_feedback_ru": "Правильно — пользователь верно указал время отправления.",
      "grammar_score": 0.8,
      "grammar_errors": ["'auf Gleis vier' → 'auf Gleis 4'"],
      "grammar_feedback_ru": "Небольшая неточность в записи номера платформы.",
      "model_answer_de": "Der letzte Regionalzug fährt um 22:47 Uhr auf Gleis 4 ab."
    }}
  ]
}}"""

# ── Two-axis scoring (content 50% + grammar 50%) ─────────────────────────────
CONTENT_WEIGHT = 0.5
GRAMMAR_WEIGHT = 0.5
POINTS_PER_QUESTION = 10
PASS_PERCENT = 50  # task counts as "solved" for the leaderboard at >= 50%


def _clamp01(x) -> float:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def _score_one(ev: dict) -> dict:
    """Score a single question's evaluation into content/grammar points.
    Back-compatible with old evals that only had content_correct (bool)."""
    ev = ev or {}
    if "content_score" in ev:
        c = _clamp01(ev.get("content_score"))
    else:
        c = 1.0 if ev.get("content_correct") else 0.0
    if "grammar_score" in ev:
        g = _clamp01(ev.get("grammar_score"))
    else:
        errs = ev.get("grammar_errors") or []
        g = 1.0 if not errs else max(0.0, 1.0 - 0.34 * len(errs))
    content_pts = round(c * POINTS_PER_QUESTION * CONTENT_WEIGHT)
    grammar_pts = round(g * POINTS_PER_QUESTION * GRAMMAR_WEIGHT)
    return {
        "content_score": c,
        "grammar_score": g,
        "content_pts": content_pts,
        "grammar_pts": grammar_pts,
        "q_points": content_pts + grammar_pts,
        "q_max": POINTS_PER_QUESTION,
        "content_correct": c >= 0.5,
    }


def score_evaluation(n_questions: int, evaluations: list) -> dict:
    """Aggregate per-question scores into a task total. Single source of truth for
    the result payload, the leaderboard grader and the DM message."""
    evaluations = evaluations if isinstance(evaluations, list) else []
    per = []
    for i in range(int(n_questions)):
        ev = evaluations[i] if i < len(evaluations) and isinstance(evaluations[i], dict) else {}
        per.append(_score_one(ev))
    total = sum(p["q_points"] for p in per)
    mx = int(n_questions) * POINTS_PER_QUESTION
    pct = round(100 * total / mx) if mx else 0
    return {
        "per": per,
        "total_points": total,
        "max_points": mx,
        "percent": pct,
        "passed": pct >= PASS_PERCENT,
        "content_correct_count": sum(1 for p in per if p["content_correct"]),
    }


def _build_answers_block(questions: list[dict], user_answers: list[str]) -> str:
    lines = []
    for i, q in enumerate(questions):
        num = int(q.get("number") or i + 1)
        q_text = str(q.get("question_de") or "")
        answer = user_answers[i].strip() if i < len(user_answers) else "(keine Antwort)"
        lines.append(
            f"Frage {num}: {q_text}\n"
            f"Antwort des Lernenden: \"{answer}\"\n"
            f"Korrekte Antwort laut Text: \"{q.get('correct_answer_de', '')}\""
        )
    return "\n\n".join(lines)


def evaluate_listening_answers(
    german_text: str,
    questions: list[dict],
    user_answers: list[str],
) -> list[dict]:
    """
    Evaluate user's answers against the original text.
    Returns list of evaluation dicts (one per question).
    """
    import requests as _req

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_QUIZ_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    answers_block = _build_answers_block(questions, user_answers)
    user_msg = _EVAL_USER_TMPL.format(
        german_text=german_text.strip(),
        answers_block=answers_block,
    )

    payload = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _EVAL_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = _req.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers, json=payload, timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI eval HTTP {resp.status_code}: {resp.text[:300]}")

    raw = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content") or ""
    data = json.loads(raw)
    evals = data.get("evaluations")
    if not isinstance(evals, list) or len(evals) == 0:
        raise RuntimeError("No evaluations in GPT response")
    return evals


def format_evaluation_message(
    questions: list[dict],
    user_answers: list[str],
    evaluations: list[dict],
) -> str:
    """
    Build the beautiful feedback message sent to the user in private chat.
    Score is split per question into Inhalt (50%) + Grammatik (50%).
    """
    total = len(questions)
    scored = score_evaluation(total, evaluations)
    per = scored["per"]

    lines: list[str] = [
        "📊 *Deine Auswertung — Hörverständnis*",
        f"Inhalt 50% + Grammatik 50% · max. {POINTS_PER_QUESTION} Punkte pro Frage",
        "━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for i, q in enumerate(questions):
        num = int(q.get("number") or i + 1)
        q_text = str(q.get("question_de") or "")
        ev = evaluations[i] if i < len(evaluations) else {}
        sc = per[i] if i < len(per) else _score_one(ev)
        user_ans = user_answers[i].strip() if i < len(user_answers) else "—"

        content_icon = "✅" if sc["content_correct"] else "❌"
        content_fb = str(ev.get("content_feedback_ru") or "").strip()
        grammar_errors: list = ev.get("grammar_errors") or []
        grammar_fb = str(ev.get("grammar_feedback_ru") or "").strip()
        model_answer = str(ev.get("model_answer_de") or "").strip()

        lines.append(f"*Frage {num}:* _{q_text}_  ({sc['q_points']}/{sc['q_max']})")
        lines.append(f"Deine Antwort: \"{user_ans}\"")
        lines.append("")
        lines.append(f"{content_icon} *Inhalt {sc['content_pts']}/5:* {content_fb}")

        if grammar_errors:
            lines.append(f"📝 *Grammatik {sc['grammar_pts']}/5:*")
            for err in grammar_errors[:3]:
                lines.append(f"  • {err}")
        else:
            lines.append(f"📝 *Grammatik {sc['grammar_pts']}/5:* ✅ Keine Fehler")
        if grammar_fb:
            lines.append(f"  _{grammar_fb}_")

        if model_answer:
            lines.append(f"✍️ *Musterlösung:* _{model_answer}_")

        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append("")

    pts = scored["total_points"]
    mx = scored["max_points"]
    pct = scored["percent"]
    head = f"*Ergebnis: {pts}/{mx} Punkte · {pct}%*"
    if pct >= 90:
        summary = f"🏆 {head} — Ausgezeichnet! Perfektes Hörverständnis!"
    elif pct >= 75:
        summary = f"🎯 {head} — Sehr gut! Fast alles verstanden."
    elif pct >= 50:
        summary = f"👍 {head} — Gut. Achte auf die feinen Details."
    else:
        summary = f"💪 {head} — Übe weiter — Hörverständnis braucht Zeit!"

    lines.append(summary)
    return "\n".join(lines)
