"""
Hörverständnis (listening comprehension) generator.

Night job flow:
  1. GPT-4o generates a B2-level German text (10-12 sentences) with tricky
     details + 4 demanding comprehension questions
  2. Google TTS converts the text to an MP3 audio file
  3. Audio uploaded to R2, entry saved to bt_3_listening_bank
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from typing import Optional

# ─── Topic pool ───────────────────────────────────────────────────────────────

_TOPICS: list[dict] = [
    {
        "id": "bahnhof",
        "label": "🚆 Bahnhof & Reise",
        "hint": (
            "Eine Durchsage im Bahnhof mit Gleisänderungen, Verspätungen, Ausnahmen für "
            "bestimmte Wochentage und Bedingungen für Anschlüsse."
        ),
    },
    {
        "id": "oeffnungszeiten",
        "label": "🏪 Öffnungszeiten & Ausnahmen",
        "hint": (
            "Eine Ankündigung über geänderte Öffnungszeiten eines Geschäfts: "
            "Feiertage, Sonderaktionen, Ausnahmen für bestimmte Kundengruppen."
        ),
    },
    {
        "id": "arzt",
        "label": "🏥 Arzt & Medikamente",
        "hint": (
            "Ärztliche Anweisungen mit genauen Bedingungen: wann, wie oft, "
            "vor oder nach dem Essen, Wechselwirkungen, Ausnahmen."
        ),
    },
    {
        "id": "telefonat",
        "label": "📞 Telefonisches Gespräch",
        "hint": (
            "Ein Telefongespräch mit Details zu einem Termin, einer Lieferung oder "
            "Reservierung — mit mehreren Bedingungen und einem Rückruf."
        ),
    },
    {
        "id": "veranstaltung",
        "label": "🎭 Veranstaltung & Tickets",
        "hint": (
            "Ankündigung einer Kulturveranstaltung: Eintrittsbedingungen, "
            "Preisklassen, was inklusive ist, Stornierungsfristen."
        ),
    },
    {
        "id": "flughafen",
        "label": "✈️ Flughafen & Verspätung",
        "hint": (
            "Durchsage wegen Flugverspätung oder -ausfall mit Anweisungen für "
            "Passagiere: Gate-Wechsel, Umbuchung, Entschädigungsregeln."
        ),
    },
    {
        "id": "bank",
        "label": "🏦 Bank & Finanzen",
        "hint": (
            "Telefonische Mitteilung einer Bank zu Kontoänderungen, Gebühren, "
            "Fristen für Widerspruch, Ausnahmen für bestimmte Konten."
        ),
    },
    {
        "id": "wohnungssuche",
        "label": "🏘️ Wohnung & Mietvertrag",
        "hint": (
            "Immobilienanzeige oder Gespräch mit Vermieter: genaue Konditionen, "
            "Nebenkosten, Haustierregelung, Kündigungsfristen."
        ),
    },
    {
        "id": "universitaet",
        "label": "🏫 Universität & Prüfungen",
        "hint": (
            "Ansage zu Prüfungsanmeldung, Fristen, Ausnahmen für Wiederholer, "
            "Zulassungsbedingungen und mitzubringende Unterlagen."
        ),
    },
    {
        "id": "buergeramt",
        "label": "🏛️ Behörde & Formulare",
        "hint": (
            "Auskunft einer Behörde zu Antragsverfahren: benötigte Dokumente, "
            "Bearbeitungszeiten, Ausnahmen, Widerspruchsmöglichkeiten."
        ),
    },
    {
        "id": "supermarkt_lieferung",
        "label": "🛒 Lieferservice & Bestellung",
        "hint": (
            "Nachricht über eine Onlinebestellung: Lieferfenster, Bedingungen "
            "für kostenlosen Versand, Rückgabefrist, Ausnahmen bei bestimmten Artikeln."
        ),
    },
]

# ─── GPT prompts ──────────────────────────────────────────────────────────────

_SYSTEM = """\
Du bist ein erfahrener Deutschlehrer und Experte für Hörverständnis-Übungen auf B2-Niveau.

Deine Aufgabe: Erstelle einen authentischen deutschen Text für eine Hörverständnisübung.

ANFORDERUNGEN AN DEN TEXT:
- Genau 10–12 Sätze, natürliches gesprochenes Deutsch (B2)
- Enthält mindestens 6 konkrete, präzise Details: Uhrzeiten, Daten, Bedingungen,
  Ausnahmen, Personengruppen, Fristen — die ABSICHTLICH schwer zu merken sind
- Einige Details sollen sich ähneln oder widersprechen (z.B. zwei verschiedene Uhrzeiten
  für verschiedene Gruppen), um das Verständnis zu testen
- Natürlicher Sprechstil: wie eine echte Durchsage, Telefonat oder Ansage klingt
- KEINE vereinfachte Sprache, komplexe Nebensätze, Passiv, Konjunktiv II erlaubt

ANFORDERUNGEN AN DIE FRAGEN:
- Genau 4 Fragen, die auf SPEZIFISCHE Details aus dem Text zielen
- Fragen müssen kaverżne Fallen enthalten: ähnliche Zahlen, Ausnahmen, Bedingungen
- Beispiele: "Um wie viel Uhr genau...?", "Für wen gilt die Ausnahme...?",
  "Welche Bedingung muss erfüllt sein, damit...?", "Was passiert, wenn...?"
- Fragen auf Deutsch, klar und präzise formuliert
- Jede Frage hat EINE eindeutige, präzise Antwort aus dem Text

Antworte NUR mit validem JSON, ohne Erklärungen."""

_USER_TMPL = """\
Thema: {topic_label}
Kontext: {topic_hint}

Erstelle einen Hörverständnis-Text mit 4 Fragen.

JSON-Format:
{{
  "german_text": "Vollständiger Text, 10-12 Sätze, als ein Absatz...",
  "questions": [
    {{
      "number": 1,
      "question_de": "Frage auf Deutsch...",
      "correct_answer_de": "Die Modellantwort auf Deutsch (1 präziser Satz)"
    }},
    {{
      "number": 2,
      "question_de": "...",
      "correct_answer_de": "..."
    }},
    {{
      "number": 3,
      "question_de": "...",
      "correct_answer_de": "..."
    }},
    {{
      "number": 4,
      "question_de": "...",
      "correct_answer_de": "..."
    }}
  ]
}}"""


# ─── GPT call ─────────────────────────────────────────────────────────────────

def _call_gpt_generate(topic: dict) -> dict:
    import requests as _req
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_QUIZ_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    payload = {
        "model": model,
        "temperature": 0.8,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": _USER_TMPL.format(
                topic_label=topic["label"],
                topic_hint=topic["hint"],
            )},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = _req.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers, json=payload, timeout=90,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:300]}")
    raw = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content") or ""
    return json.loads(raw)


def _validate_generated(data: dict) -> None:
    text = str(data.get("german_text") or "").strip()
    if not text or len(text) < 100:
        raise ValueError("german_text too short")
    questions = data.get("questions")
    if not isinstance(questions, list) or len(questions) != 4:
        raise ValueError("need exactly 4 questions")
    for q in questions:
        if not str(q.get("question_de") or "").strip():
            raise ValueError("empty question_de")
        if not str(q.get("correct_answer_de") or "").strip():
            raise ValueError("empty correct_answer_de")


# ─── TTS audio generation ─────────────────────────────────────────────────────

def _generate_tts_audio(text: str) -> bytes:
    """Convert German text to MP3 bytes via Google TTS."""
    try:
        from google.cloud import texttospeech
        client = texttospeech.TextToSpeechClient()
        synthesis_input = texttospeech.SynthesisInput(text=text)
        voice = texttospeech.VoiceSelectionParams(
            language_code="de-DE",
            name="de-DE-Neural2-B",  # natural male voice
            ssml_gender=texttospeech.SsmlVoiceGender.MALE,
        )
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            speaking_rate=0.92,  # slightly slower for comprehension
            pitch=0.0,
        )
        response = client.synthesize_speech(
            input=synthesis_input, voice=voice, audio_config=audio_config
        )
        return response.audio_content
    except Exception as exc:
        logging.warning("listening_gen: Google TTS failed, trying fallback: %s", exc)
        raise RuntimeError(f"TTS failed: {exc}") from exc


# ─── Main entry point ─────────────────────────────────────────────────────────

def generate_listening_entry(topic_id: Optional[str] = None) -> str:
    """
    Generate one listening comprehension entry: GPT text + questions + TTS audio.
    Returns listening_id. Raises on failure.
    """
    from backend.database import upsert_listening_bank_entry
    from backend.r2_storage import r2_put_bytes

    topic = next((t for t in _TOPICS if t["id"] == topic_id), None) if topic_id else None
    if not topic:
        topic = random.choice(_TOPICS)

    logging.info("listening_gen: generating topic=%s", topic["id"])

    # 1. Generate text + questions via GPT
    data = _call_gpt_generate(topic)
    _validate_generated(data)

    german_text = str(data["german_text"]).strip()
    questions = [
        {
            "number": int(q.get("number") or i + 1),
            "question_de": str(q.get("question_de") or "").strip(),
            "correct_answer_de": str(q.get("correct_answer_de") or "").strip(),
        }
        for i, q in enumerate(data["questions"])
    ]

    # 2. Generate TTS audio
    try:
        audio_bytes = _generate_tts_audio(german_text)
    except Exception as exc:
        logging.warning("listening_gen: TTS failed, saving text-only entry: %s", exc)
        audio_bytes = None

    # 3. Upload audio to R2
    listening_id = str(uuid.uuid4())
    audio_object_key = None
    audio_status = "pending"

    if audio_bytes:
        safe_id = listening_id.replace("-", "_")
        audio_object_key = f"listening/audio/{safe_id}.mp3"
        try:
            r2_put_bytes(
                audio_object_key,
                audio_bytes,
                content_type="audio/mpeg",
                cache_control="public, max-age=31536000, immutable",
            )
            audio_status = "ready"
            logging.info(
                "listening_gen: audio uploaded key=%s bytes=%d",
                audio_object_key, len(audio_bytes),
            )
        except Exception as exc:
            logging.warning("listening_gen: R2 upload failed: %s", exc)
            audio_object_key = None
            audio_status = "failed"

    # 4. Save to DB
    upsert_listening_bank_entry(
        listening_id=listening_id,
        topic=topic["label"],
        difficulty="B2",
        german_text=german_text,
        questions_json=questions,
        audio_object_key=audio_object_key,
        audio_status=audio_status,
    )

    logging.info(
        "listening_gen: done listening_id=%s topic=%s audio=%s",
        listening_id, topic["id"], audio_status,
    )
    return listening_id


def prepare_listening_pool(*, target_ready: int = 7, max_attempts: int = 10) -> dict:
    """
    Generate listening entries until we have target_ready in the bank.
    Returns stats dict.
    """
    from backend.database import count_listening_bank_entries

    stats = {"attempted": 0, "succeeded": 0, "failed": 0, "skipped": 0}
    existing = count_listening_bank_entries()
    needed = max(0, target_ready - existing)

    if needed == 0:
        stats["skipped"] = existing
        logging.info("listening_pool: already at target existing=%d", existing)
        return stats

    logging.info("listening_pool: existing=%d needed=%d", existing, needed)
    used_topics: list[str] = []

    for _ in range(min(needed, max_attempts)):
        stats["attempted"] += 1
        # Rotate through topics to avoid repetition
        available = [t["id"] for t in _TOPICS if t["id"] not in used_topics]
        if not available:
            used_topics.clear()
            available = [t["id"] for t in _TOPICS]
        topic_id = random.choice(available)
        used_topics.append(topic_id)
        try:
            generate_listening_entry(topic_id=topic_id)
            stats["succeeded"] += 1
        except Exception as exc:
            stats["failed"] += 1
            logging.warning("listening_pool: failed topic=%s: %s", topic_id, exc)
        time.sleep(3.0)

    return stats
