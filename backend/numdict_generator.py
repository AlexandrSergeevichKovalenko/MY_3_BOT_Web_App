"""
Zahlen-Diktat (number dictation) generator.

A session = 3 short audio scenes. In each, a number (phone number, social-security
/ customer number, PIN or an alphanumeric activation code) is embedded in a natural
German micro-scene. The learner must catch the number among the surrounding talk and
type it.

Design choice: the NUMBER is generated deterministically in Python (so its format,
the say-as reading type and the normalized answer are fully controlled). GPT only
writes the natural scene around it, embedding the exact number string wrapped in a
sentinel «NUM»…«/NUM». A consistency guard rejects any scene where the embedded
number was altered — the sentinel content is the single source of truth for the audio
and the answer.

Night-job flow (mirrors listening_generator):
  1. pick a scenario template + generate the number
  2. GPT writes the scene around «NUM»{number}«/NUM»
  3. entry saved to bt_3_numdict_bank as audio_status='pending'
  4. the bot's backfill synthesizes SSML audio → R2 → marks 'ready'
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import uuid
from typing import Optional

# Sentinel that wraps the spoken number inside scenario_text. Must never occur in
# normal German text; the SSML synth + the answer extraction both rely on it.
NUM_OPEN = "«NUM»"
NUM_CLOSE = "«/NUM»"
_SENTINEL_RE = re.compile(re.escape(NUM_OPEN) + r"(.*?)" + re.escape(NUM_CLOSE), re.DOTALL)

# Unambiguous chars for spoken alphanumeric codes (no 0/O, 1/I, B/8, etc.).
_ALNUM_LETTERS = "ACDEFHJKLMNPQRTUVWXY"
_ALNUM_DIGITS = "23456789"


# ─── Scenario templates ───────────────────────────────────────────────────────
# kind → SSML say-as interpret-as; input_mode → frontend keyboard + normalization.

_SCENARIOS: list[dict] = [
    {
        "id": "phone_callback", "kind": "telephone", "input_mode": "numeric",
        "scene": "Ein Kollege spricht dir auf die Mailbox und bittet dich, ihn unter seiner Nummer zurückzurufen.",
        "prompt_de": "Notiere die Rückrufnummer.",
        "prompt_ru": "Запиши номер для перезвона.",
    },
    {
        "id": "phone_friend", "kind": "telephone", "input_mode": "numeric",
        "scene": "Eine Freundin hat eine neue Handynummer und diktiert sie dir am Telefon.",
        "prompt_de": "Notiere die neue Handynummer.",
        "prompt_ru": "Запиши новый номер мобильного.",
    },
    {
        "id": "phone_doctor", "kind": "telephone", "input_mode": "numeric",
        "scene": "Die Arztpraxis ruft an und bittet dich, für die Terminbestätigung zurückzurufen.",
        "prompt_de": "Notiere die Telefonnummer der Praxis.",
        "prompt_ru": "Запиши телефон врачебной практики.",
    },
    {
        "id": "phone_handwerker", "kind": "telephone", "input_mode": "numeric",
        "scene": "Ein Handwerker meldet sich und sagt, unter welcher Nummer du ihn für den Termin erreichst.",
        "prompt_de": "Notiere die Nummer des Handwerkers.",
        "prompt_ru": "Запиши номер мастера.",
    },
    {
        "id": "pin_bank", "kind": "digits", "input_mode": "numeric",
        "scene": "Die Bank-Hotline nennt dir eine einmalige TAN, die du gleich eingeben sollst.",
        "prompt_de": "Notiere die TAN.",
        "prompt_ru": "Запиши одноразовый код (TAN).",
    },
    {
        "id": "customer_number", "kind": "digits", "input_mode": "numeric",
        "scene": "Ein Mitarbeiter im Kundenservice liest dir deine Kundennummer vor.",
        "prompt_de": "Notiere die Kundennummer.",
        "prompt_ru": "Запиши номер клиента.",
    },
    {
        "id": "order_number", "kind": "digits", "input_mode": "numeric",
        "scene": "Ein Online-Shop bestätigt deine Bestellung und nennt die Bestellnummer.",
        "prompt_de": "Notiere die Bestellnummer.",
        "prompt_ru": "Запиши номер заказа.",
    },
    {
        "id": "insurance_number", "kind": "digits", "input_mode": "numeric",
        "scene": "Eine Sachbearbeiterin bittet dich, die Sozialversicherungsnummer eines Kunden zu notieren und ihm einen Brief zu schicken.",
        "prompt_de": "Notiere die Sozialversicherungsnummer.",
        "prompt_ru": "Запиши номер социального страхования.",
    },
    {
        "id": "meter_reading", "kind": "digits", "input_mode": "numeric",
        "scene": "Der Energieversorger braucht deinen Zählerstand und nennt vorher die Zählernummer.",
        "prompt_de": "Notiere die Zählernummer.",
        "prompt_ru": "Запиши номер счётчика.",
    },
    {
        "id": "parcel_pickup", "kind": "digits", "input_mode": "numeric",
        "scene": "Die Packstation schickt dir den Abholcode für dein Paket.",
        "prompt_de": "Notiere den Abholcode.",
        "prompt_ru": "Запиши код выдачи посылки.",
    },
    {
        "id": "activation_code", "kind": "characters", "input_mode": "alnum",
        "scene": "Der technische Support diktiert dir einen Aktivierungscode für die Software.",
        "prompt_de": "Notiere den Aktivierungscode.",
        "prompt_ru": "Запиши код активации.",
    },
    {
        "id": "voucher_code", "kind": "characters", "input_mode": "alnum",
        "scene": "Ein Kundenberater liest dir einen Gutscheincode vor, den du im Shop eingeben kannst.",
        "prompt_de": "Notiere den Gutscheincode.",
        "prompt_ru": "Запиши код ваучера.",
    },
    {
        "id": "booking_ref", "kind": "characters", "input_mode": "alnum",
        "scene": "Die Fluggesellschaft nennt dir deinen Buchungscode für den Check-in.",
        "prompt_de": "Notiere den Buchungscode.",
        "prompt_ru": "Запиши код бронирования.",
    },
]


# ─── Number generation (Python owns format + answer) ──────────────────────────

def _normalize(value: str, input_mode: str) -> str:
    """Canonical comparison form: numeric → digits only; alnum → uppercase A-Z0-9."""
    s = str(value or "").upper()
    if input_mode == "alnum":
        return re.sub(r"[^A-Z0-9]", "", s)
    return re.sub(r"\D", "", s)


def _gen_phone() -> tuple[str, str]:
    """Returns (spoken_token, display). German landline or mobile, grouped in pairs."""
    if random.random() < 0.5:
        # Mobile: 015x / 017x + 8 digits, read in pairs.
        prefix = random.choice(["0151", "0152", "0157", "0160", "0170", "0171", "0176"])
        rest = "".join(random.choice("0123456789") for _ in range(8))
        groups = [rest[i:i + 2] for i in range(0, 8, 2)]
        token = prefix + " " + " ".join(groups)
    else:
        # Landline: area code (3-4 digits incl. leading 0) + 6-8 digits in pairs.
        area = random.choice(["030", "040", "089", "0221", "0211", "0341", "0511"])
        n = random.choice([6, 8])
        rest = "".join(random.choice("0123456789") for _ in range(n))
        groups = [rest[i:i + 2] for i in range(0, n, 2)]
        token = area + " " + " ".join(groups)
    return token, token


def _gen_digits(length: int, group: int = 0) -> tuple[str, str]:
    digits = "".join(random.choice("0123456789") for _ in range(length))
    if group and group > 0:
        disp = " ".join(digits[i:i + group] for i in range(0, length, group))
    else:
        disp = digits
    return digits, disp


def _gen_alnum(length: int, group: int = 0) -> tuple[str, str]:
    """Digit-heavy alphanumeric code: ALWAYS mixed — never letters-only and never
    digits-only. Digits come in CONTIGUOUS PAIRS so the TTS reads each as a two-digit
    number (the whole point of the trainer — single-digit reading has no value, learners
    confuse 2-/3-digit numbers); ~1/3 single letters are sprinkled BETWEEN the pairs.
    Always ≥1 letter and ≥1 digit-pair (≥2 digits)."""
    # ~1/3 single letters; keep the digit count EVEN so every digit lands in a pair.
    # On odd parity prefer DROPPING a letter (stay digit-heavy), unless that leaves none.
    n_letters = max(1, round(length * 0.34))
    if (length - n_letters) % 2 == 1:
        n_letters = n_letters - 1 if n_letters > 1 else n_letters + 1
    n_letters = min(n_letters, length - 2)   # always leave ≥2 digits (one pair)
    n_pairs = (length - n_letters) // 2
    blocks = (
        ["".join(random.choice(_ALNUM_DIGITS) for _ in range(2)) for _ in range(n_pairs)]
        + [random.choice(_ALNUM_LETTERS) for _ in range(n_letters)]
    )
    random.shuffle(blocks)
    code = "".join(blocks)
    if group and group > 0:
        disp = "-".join(code[i:i + group] for i in range(0, len(code), group))
    else:
        disp = code
    return code, disp


def _make_number(scenario: dict) -> dict:
    """Build the number for a scenario → spoken token + display + normalized answer.

    For numeric types the spoken token carries explicit grouping (spaces): the TTS
    reads each group as a compound cardinal ("85" → fünfundachtzig), matching how the
    grouping is shown on the reveal screen. Alphanumeric codes are spelled out, so the
    spoken token is the bare code and the display keeps the dashes for readability."""
    sid = scenario["id"]
    mode = scenario["input_mode"]
    if scenario["kind"] == "characters":
        if sid == "booking_ref":
            code, disp = _gen_alnum(6, group=0)
        else:
            code, disp = _gen_alnum(random.choice([6, 8]), group=4)
        spoken, display = code, disp           # spell the bare code; show the dashed one
    elif scenario["kind"] == "telephone":
        token, disp = _gen_phone()
        spoken, display = token, disp          # already grouped (area + pairs)
    elif sid == "insurance_number":
        _, disp = _gen_digits(11, group=2)
        spoken, display = disp, disp
    elif sid == "parcel_pickup":
        _, disp = _gen_digits(random.choice([6, 9]), group=3)   # triples → hundreds
        spoken, display = disp, disp
    elif sid == "pin_bank":
        _, disp = _gen_digits(random.choice([5, 6]), group=2)
        spoken, display = disp, disp
    elif sid == "meter_reading":
        _, disp = _gen_digits(random.choice([6, 8]), group=2)
        spoken, display = disp, disp
    else:  # digits: customer / order numbers
        _, disp = _gen_digits(random.choice([7, 8, 9]), group=2)
        spoken, display = disp, disp
    return {
        "spoken": spoken,
        "display": display,
        "answer_value": _normalize(spoken, mode),
    }


# ─── GPT prompts ──────────────────────────────────────────────────────────────

_SYSTEM = """\
Du bist Drehbuchautor für kurze, realistische deutsche Hörszenen (Niveau B1).

Aufgabe: Schreibe eine SEHR KURZE, natürliche Sprechszene (2–4 Sätze), in der eine
bestimmte Nummer vorkommt. Die Szene klingt wie eine echte Sprachnachricht, ein
Telefonanruf oder eine Durchsage.

REGELN:
- Beginne mit einer allgemeinen Begrüßung OHNE Namen (z.B. "Hallo!", "Guten Tag!").
- Baue einen kleinen Rahmen ein (z.B. zurückrufen, sich etwas notieren, einen Brief
  schicken) — die Nummer ist die wichtigste Information.
- Die vorgegebene Nummer MUSS GENAU EINMAL vorkommen, unverändert, eingeschlossen in
  die Marker «NUM» und «/NUM». Ändere KEINE Ziffer, KEIN Zeichen und KEINE Leerzeichen
  der Nummer. Schreibe die Marker nirgendwo sonst hin.
- Natürliches gesprochenes Deutsch, keine Aufzählungszeichen, keine Erklärungen.

Antworte NUR mit validem JSON: {"scenario_text": "..."}"""

_USER_TMPL = """\
Szene: {scene}
Worum es geht: {prompt_de}
Vorgegebene Nummer (genau so einbauen, eingeschlossen in «NUM»…«/NUM»): {number}

Schreibe die Szene als JSON: {{"scenario_text": "...{open}{number}{close}..."}}"""


def _call_gpt_scene(scenario: dict, spoken_number: str) -> str:
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
                scene=scenario["scene"], prompt_de=scenario["prompt_de"],
                number=spoken_number, open=NUM_OPEN, close=NUM_CLOSE,
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
    return str(json.loads(raw).get("scenario_text") or "").strip()


def _extract_number(scenario_text: str) -> Optional[str]:
    """The text wrapped by the single «NUM»…«/NUM» marker, or None if not exactly one."""
    matches = _SENTINEL_RE.findall(scenario_text or "")
    if len(matches) != 1:
        return None
    return matches[0].strip()


# ─── Main entry point ─────────────────────────────────────────────────────────

def generate_numdict_entry(scenario_id: Optional[str] = None) -> str:
    """Generate one number-dictation entry (scene + number). Returns numdict_id.
    Audio is synthesized later by the bot's backfill (audio_status starts 'pending')."""
    from backend.database import upsert_numdict_bank_entry

    scenario = next((s for s in _SCENARIOS if s["id"] == scenario_id), None) if scenario_id else None
    if not scenario:
        scenario = random.choice(_SCENARIOS)

    num = _make_number(scenario)
    if not num["answer_value"]:
        raise ValueError("empty answer_value from number generator")

    scene_text = _call_gpt_scene(scenario, num["spoken"])
    embedded = _extract_number(scene_text)
    if embedded is None:
        raise ValueError("scene must contain exactly one «NUM»…«/NUM» marker")
    # Consistency guard: the spoken number must survive unchanged through GPT.
    if _normalize(embedded, scenario["input_mode"]) != num["answer_value"]:
        raise ValueError(
            f"number desync: embedded={embedded!r} expected={num['spoken']!r}"
        )
    if len(scene_text) < 25:
        raise ValueError("scenario_text too short")

    numdict_id = str(uuid.uuid4())
    upsert_numdict_bank_entry(
        numdict_id=numdict_id,
        scenario_id=scenario["id"],
        difficulty="B1",
        scenario_text=scene_text,
        number_type=scenario["kind"],
        answer_value=num["answer_value"],
        display_answer=num["display"],
        prompt_de=scenario["prompt_de"],
        prompt_ru=scenario["prompt_ru"],
        input_mode=scenario["input_mode"],
        audio_object_key=None,
        audio_status="pending",
    )
    logging.info("numdict_gen: done id=%s scenario=%s type=%s",
                 numdict_id, scenario["id"], scenario["kind"])
    return numdict_id


def prepare_numdict_pool(*, target_ready: int = 20, max_attempts: int = 12) -> dict:
    """Generate number-dictation entries until the bank reaches target_ready."""
    from backend.database import count_numdict_bank_entries

    stats = {"attempted": 0, "succeeded": 0, "failed": 0, "skipped": 0}
    existing = count_numdict_bank_entries()
    needed = max(0, target_ready - existing)
    if needed == 0:
        stats["skipped"] = existing
        logging.info("numdict_pool: already at target existing=%d", existing)
        return stats

    logging.info("numdict_pool: existing=%d needed=%d", existing, needed)
    used: list[str] = []
    for _ in range(min(needed, max_attempts)):
        stats["attempted"] += 1
        available = [s["id"] for s in _SCENARIOS if s["id"] not in used]
        if not available:
            used.clear()
            available = [s["id"] for s in _SCENARIOS]
        scenario_id = random.choice(available)
        used.append(scenario_id)
        try:
            generate_numdict_entry(scenario_id=scenario_id)
            stats["succeeded"] += 1
        except Exception as exc:
            stats["failed"] += 1
            logging.warning("numdict_pool: failed scenario=%s: %s", scenario_id, exc)
        time.sleep(2.0)
    return stats
