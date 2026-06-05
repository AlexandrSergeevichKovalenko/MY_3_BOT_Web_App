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

Deine Aufgabe: Vergleiche die Antworten des Lernenden mit dem Originaltext und bewerte sie.

BEWERTUNGSKRITERIEN:
1. INHALT (content_correct: true/false):
   - Stimmt die KERNAUSSAGE? Wichtige Details müssen präzise sein.
   - Kleine Umformulierungen sind OK, solange der Sinn stimmt.
   - Fehlende oder falsche Details (Uhrzeiten, Bedingungen, Ausnahmen) → false
   - content_feedback: 1-2 Sätze Erklärung auf Russisch

2. GRAMMATIK (grammar_errors):
   - Liste konkrete Fehler im Satz des Lernenden auf
   - Format: ["Fehler: '...' → Korrekt: '...'", ...]
   - Wenn keine Fehler: leere Liste []

3. MUSTERLÖSUNG (model_answer_de):
   - Der ideale, vollständige deutsche Satz als Antwort
   - Bezieht sich direkt auf den Text

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
      "content_correct": true,
      "content_feedback_ru": "Правильно — пользователь верно указал время отправления.",
      "grammar_errors": [],
      "model_answer_de": "Der letzte Regionalzug fährt um 22:47 Uhr auf Gleis 4 ab."
    }}
  ]
}}"""


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
    """
    correct_count = sum(1 for e in evaluations if e.get("content_correct"))
    total = len(questions)

    lines: list[str] = [
        "📊 *Deine Auswertung — Hörverständnis*",
        "━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for i, q in enumerate(questions):
        num = int(q.get("number") or i + 1)
        q_text = str(q.get("question_de") or "")
        ev = evaluations[i] if i < len(evaluations) else {}
        user_ans = user_answers[i].strip() if i < len(user_answers) else "—"

        is_correct = bool(ev.get("content_correct"))
        icon = "✅" if is_correct else "❌"
        content_fb = str(ev.get("content_feedback_ru") or "").strip()
        grammar_errors: list = ev.get("grammar_errors") or []
        model_answer = str(ev.get("model_answer_de") or "").strip()

        lines.append(f"*Frage {num}:* _{q_text}_")
        lines.append(f"Deine Antwort: \"{user_ans}\"")
        lines.append("")
        lines.append(f"{icon} *Inhalt:* {content_fb}")

        if grammar_errors:
            lines.append("📝 *Grammatik:*")
            for err in grammar_errors[:3]:
                lines.append(f"  • {err}")
        else:
            lines.append("📝 *Grammatik:* ✅ Keine Fehler")

        if model_answer:
            lines.append(f"✍️ *Musterlösung:* _{model_answer}_")

        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append("")

    # Summary
    if correct_count == total:
        summary = f"🏆 *Ergebnis: {correct_count}/{total}* — Ausgezeichnet! Perfektes Hörverständnis!"
    elif correct_count >= total * 0.75:
        summary = f"🎯 *Ergebnis: {correct_count}/{total}* — Sehr gut! Fast alles verstanden."
    elif correct_count >= total * 0.5:
        summary = f"👍 *Ergebnis: {correct_count}/{total}* — Gut. Achte auf die feinen Details."
    else:
        summary = f"💪 *Ergebnis: {correct_count}/{total}* — Übe weiter — Hörverständnis braucht Zeit!"

    lines.append(summary)
    return "\n".join(lines)
