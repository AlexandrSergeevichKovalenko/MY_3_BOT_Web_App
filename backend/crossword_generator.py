"""
Crossword puzzle generation for German language learning.

Flow:
  1. GPT generates 12 themed German words + clues (DE + RU)
  2. Greedy grid placement builds valid crossword layout (5-9 words placed)
  3. 2 words selected as hidden (user must guess)
  4. Entry saved to bt_3_crossword_bank (image_status='pending')
  5. Separate step (crossword_renderer.py) renders the grid image
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

_TOPICS: list[tuple[str, str]] = [
    # B1 — standard intermediate
    ("Gesundheit und Medizin", "B1"),
    ("Beruf und Arbeitsalltag", "B1"),
    ("Stadt und Verkehr", "B1"),
    ("Natur und Umwelt", "B1"),
    ("Reisen und Urlaub", "B1"),
    ("Sport und Freizeit", "B1"),
    ("Medien und Kommunikation", "B1"),
    ("Wirtschaft und Finanzen", "B1"),
    ("Politik und Gesellschaft", "B1"),
    ("Wissenschaft und Forschung", "B1"),
    # B2 — upper intermediate / advanced
    ("Recht und Verwaltung", "B2"),
    ("Psychologie und Emotionen", "B2"),
    ("Philosophie und Ethik", "B2"),
    ("Technologie und Innovation", "B2"),
    ("Kunst und Literatur", "B2"),
    ("Architektur und Design", "B2"),
    ("Geschichte und Kultur", "B2"),
    ("Umwelt und Nachhaltigkeit", "B2"),
    ("Sprache und Linguistik", "B2"),
    ("Globalisierung und Wirtschaft", "B2"),
]

# ─── GPT prompts ──────────────────────────────────────────────────────────────

_GPT_SYSTEM = """\
Du bist Experte für Deutsch als Fremdsprache (B1-B2) und erstellst anspruchsvolle Kreuzwortraetsel.

Regeln fuer jedes Wort:
- Nur GROSSBUCHSTABEN, keine Leerzeichen, keine Bindestriche, kein Artikel
- Umlaute als einzelne Zeichen: Ae Oe Ue (NICHT AE OE UE) — nein: AEZRZTE → AERZTE, OeL → OEL
- Wortlaenge: 4-12 Zeichen
- NIVEAU: Woerter auf dem angegebenen Niveau — KEINE Grundwoerter (Mutter, Bruder, Hund usw.)
  B1: wichtige Vokabeln die ein B1-Lernender kennen sollte, aber nicht trivial sind
  B2: Fachvokabular, abstrakte Begriffe, seltener aber wichtig
- Bevorzuge laengere Woerter (6-12 Zeichen) — kurze Woerter (3-4 Zeichen) vermeiden
- Hinweise: 1 Satz, beschreibt das Wort praezise ohne es zu nennen
- Russischer Hinweis: natuerliche Uebersetzung

Antworte NUR mit validem JSON, ohne Erklaerungen."""

_GPT_USER_TMPL = """\
Thema: {topic}
Schwierigkeitsgrad: {difficulty}

Erstelle exakt 12 ANSPRUCHSVOLLE deutsche Woerter (Niveau {difficulty}) mit Kreuzwortraetsel-Hinweisen.
WICHTIG: Keine Grundvokabeln (A1/A2)! Waehle Woerter die einen Lernenden wirklich fordern.

Ausgabe:
{{
  "words": [
    {{
      "word": "BEHANDLUNG",
      "clue_de": "Medizinische Massnahme zur Heilung einer Erkrankung",
      "clue_ru": "Медицинская процедура для лечения болезни"
    }}
  ]
}}"""

# ─── GPT call ─────────────────────────────────────────────────────────────────

def _call_gpt_for_words(topic: str, difficulty: str) -> list[dict]:
    import requests as _requests

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_QUIZ_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    payload = {
        "model": model,
        "temperature": 0.75,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _GPT_SYSTEM},
            {"role": "user", "content": _GPT_USER_TMPL.format(topic=topic, difficulty=difficulty)},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = _requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}: {resp.text[:300]}")

    raw = str((resp.json().get("choices") or [{}])[0].get("message", {}).get("content") or "")
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"JSON parse failed: {exc}") from exc

    words = parsed.get("words")
    if not isinstance(words, list):
        raise RuntimeError("GPT returned no 'words' array")
    return words


def _validate_word_entry(entry: dict) -> Optional[str]:
    word = str(entry.get("word") or "").strip().upper()
    if not word:
        return "empty word"
    if len(word) < 3 or len(word) > 12:
        return f"bad length {len(word)}: {word}"
    if not all(c.isalpha() or c in "ÄÖÜ" for c in word):
        return f"invalid chars in: {word}"
    if not str(entry.get("clue_de") or "").strip():
        return "empty clue_de"
    if not str(entry.get("clue_ru") or "").strip():
        return "empty clue_ru"
    return None


# ─── Grid placement ────────────────────────────────────────────────────────────

_MAX_GRID = 25


def _can_place(grid: dict, word: str, row: int, col: int, direction: str) -> bool:
    n = len(word)
    dr, dc = (0, 1) if direction == "across" else (1, 0)

    if row < 0 or col < 0:
        return False
    if direction == "across" and col + n > _MAX_GRID:
        return False
    if direction == "down" and row + n > _MAX_GRID:
        return False

    # Cells immediately before and after the word must be empty
    if grid.get((row - dr, col - dc)):
        return False
    if grid.get((row + dr * n, col + dc * n)):
        return False

    perp_dr, perp_dc = (1, 0) if direction == "across" else (0, 1)
    intersections = 0

    for i, ch in enumerate(word):
        r, c = row + dr * i, col + dc * i
        existing = grid.get((r, c))

        if existing:
            if existing != ch:
                return False  # letter mismatch at overlap
            intersections += 1
            # The crossing word already placed this letter — valid intersection
        else:
            # Empty cell: must not touch adjacent parallel letters (would merge words)
            if grid.get((r - perp_dr, c - perp_dc)) or grid.get((r + perp_dr, c + perp_dc)):
                return False

    return intersections >= 1  # must share at least one letter with existing grid


def _score_placement(grid: dict, word: str, row: int, col: int, direction: str) -> float:
    n = len(word)
    dr, dc = (0, 1) if direction == "across" else (1, 0)
    intersections = sum(
        1 for i in range(n) if grid.get((row + dr * i, col + dc * i))
    )
    center = _MAX_GRID / 2
    mid_r = row + dr * n / 2
    mid_c = col + dc * n / 2
    dist = abs(mid_r - center) + abs(mid_c - center)
    return intersections * 10 - dist * 0.5


def _find_best_placement(grid: dict, word: str) -> Optional[tuple]:
    best_score = -999.0
    best: Optional[tuple] = None

    for (gr, gc), letter in list(grid.items()):
        for i, ch in enumerate(word):
            if ch != letter:
                continue
            # Try ACROSS: word[i] aligns with existing letter at (gr, gc)
            r_a, c_a = gr, gc - i
            if _can_place(grid, word, r_a, c_a, "across"):
                s = _score_placement(grid, word, r_a, c_a, "across")
                if s > best_score:
                    best_score, best = s, ("across", r_a, c_a)
            # Try DOWN
            r_d, c_d = gr - i, gc
            if _can_place(grid, word, r_d, c_d, "down"):
                s = _score_placement(grid, word, r_d, c_d, "down")
                if s > best_score:
                    best_score, best = s, ("down", r_d, c_d)

    return best


def _place_words(word_entries: list[dict]) -> tuple[dict, list[dict]]:
    """
    Greedy crossword placement.
    Returns (grid, placed_words).
      grid: dict of (row, col) -> letter
      placed_words: word_entry dicts with added direction/row/col
    """
    words = sorted(word_entries, key=lambda w: len(w["word"]), reverse=True)
    if not words:
        return {}, []

    grid: dict[tuple[int, int], str] = {}
    placed: list[dict] = []

    # First word: horizontal, centered
    first = words[0]
    w0 = first["word"]
    start_row = _MAX_GRID // 2
    start_col = (_MAX_GRID - len(w0)) // 2
    for i, ch in enumerate(w0):
        grid[(start_row, start_col + i)] = ch
    placed.append({**first, "direction": "across", "row": start_row, "col": start_col})

    for entry in words[1:]:
        placement = _find_best_placement(grid, entry["word"])
        if placement is None:
            continue
        direction, row, col = placement
        dr, dc = (0, 1) if direction == "across" else (1, 0)
        for i, ch in enumerate(entry["word"]):
            grid[(row + dr * i, col + dc * i)] = ch
        placed.append({**entry, "direction": direction, "row": row, "col": col})
        if len(placed) >= 9:
            break

    return grid, placed


def _normalize_and_number(
    raw_grid: dict[tuple[int, int], str],
    placed_words: list[dict],
) -> tuple[list[list], list[dict]]:
    """
    Shift grid so top-left is (0, 0). Assign word numbers in reading order.
    Returns (grid_2d, words_with_numbers).
    """
    if not raw_grid:
        return [], []

    min_r = min(r for r, _ in raw_grid)
    min_c = min(c for _, c in raw_grid)
    max_r = max(r for r, _ in raw_grid)
    max_c = max(c for _, c in raw_grid)

    rows = max_r - min_r + 1
    cols = max_c - min_c + 1
    grid_2d: list[list] = [[None] * cols for _ in range(rows)]
    for (r, c), ch in raw_grid.items():
        grid_2d[r - min_r][c - min_c] = ch

    # Shift word positions
    normalized = [
        {**w, "row": w["row"] - min_r, "col": w["col"] - min_c}
        for w in placed_words
    ]

    # Number in reading order (top→bottom, left→right; across before down at same cell)
    normalized.sort(key=lambda w: (w["row"], w["col"], 0 if w["direction"] == "across" else 1))
    for i, w in enumerate(normalized, start=1):
        w["number"] = i

    return grid_2d, normalized


def _word_cells(w: dict) -> set[tuple[int, int]]:
    """Set of (row, col) grid cells occupied by a placed word."""
    dr, dc = (0, 1) if w["direction"] == "across" else (1, 0)
    return {(w["row"] + dr * i, w["col"] + dc * i) for i in range(len(w["word"]))}


def _select_hidden_words(words: list[dict], hidden_count: int = 3) -> list[dict]:
    """Mark hidden_count words as hidden=True.

    The hidden words are chosen to form a CONNECTED chain — each one directly
    intersects (shares a cell with) at least one other hidden word. This makes
    the puzzle interdependent: solving one hidden word reveals a letter at the
    crossing of the next, so the player can chain deductions instead of solving
    isolated, unrelated blanks. Medium-length words are preferred for richness.
    """
    result = [{**w, "hidden": False} for w in words]
    n = len(result)
    if n <= hidden_count:
        for w in result:
            w["hidden"] = True
        return result

    def _length_pref(idx: int) -> float:
        return -abs(len(result[idx]["word"]) - 5.5)  # peak at 5-6 letters

    # Build the direct-intersection graph between placed words.
    cells = [_word_cells(w) for w in result]
    adj: dict[int, set[int]] = {i: set() for i in range(n)}
    for i in range(n):
        for j in range(i + 1, n):
            if cells[i] & cells[j]:
                adj[i].add(j)
                adj[j].add(i)

    # Grow a connected group of `hidden_count` words, seeding from the best
    # medium-length word and always extending through a direct intersection so
    # the chosen set stays mutually crossing.
    seed_order = sorted(range(n), key=_length_pref, reverse=True)
    chosen: set[int] = set()
    for seed in seed_order:
        group = [seed]
        frontier = set(adj[seed])
        while len(group) < hidden_count and frontier:
            nxt = max(frontier, key=_length_pref)
            group.append(nxt)
            frontier.discard(nxt)
            frontier |= (adj[nxt] - set(group))
        if len(group) == hidden_count:
            chosen = set(group)
            break

    # Fallback: no connected group of the requested size (very sparse grid) —
    # take the best-by-length words and at least keep direction diversity.
    if len(chosen) < hidden_count:
        chosen = set()
        directions_chosen: set[str] = set()
        for idx in seed_order:
            if len(chosen) >= hidden_count:
                break
            d = result[idx]["direction"]
            if d not in directions_chosen or len(chosen) == hidden_count - 1:
                chosen.add(idx)
                directions_chosen.add(d)
        for idx in seed_order:
            if len(chosen) >= hidden_count:
                break
            chosen.add(idx)

    for idx, w in enumerate(result):
        w["hidden"] = idx in chosen
    return result


# ─── Main entry point ─────────────────────────────────────────────────────────

def generate_crossword_entry(topic: str | None = None, difficulty: str | None = None) -> str:
    """
    Generate one crossword puzzle and save to bt_3_crossword_bank.
    Returns crossword_id. Raises on failure.
    """
    from backend.database import upsert_crossword_bank_entry

    if not topic or not difficulty:
        chosen_topic, chosen_diff = random.choice(_TOPICS)
        topic = topic or chosen_topic
        difficulty = difficulty or chosen_diff

    logging.info("crossword_generator: generating topic=%r difficulty=%s", topic, difficulty)

    # 1. Get words from GPT
    raw_words = _call_gpt_for_words(topic, difficulty)

    # 2. Validate entries
    valid_words: list[dict] = []
    seen_words: set[str] = set()
    for entry in raw_words:
        err = _validate_word_entry(entry)
        if err:
            logging.debug("crossword_generator: skip word: %s", err)
            continue
        w = entry["word"].strip().upper()
        if w in seen_words:
            continue
        seen_words.add(w)
        valid_words.append({
            "word": w,
            "clue_de": str(entry["clue_de"]).strip(),
            "clue_ru": str(entry["clue_ru"]).strip(),
        })

    if len(valid_words) < 4:
        raise RuntimeError(f"Too few valid words from GPT: {len(valid_words)}")

    # 3. Place words in grid
    raw_grid, placed = _place_words(valid_words)
    if len(placed) < 3:
        raise RuntimeError(f"Only {len(placed)} words placed — crossword too sparse")

    # 4. Normalize and number words
    grid_2d, words_numbered = _normalize_and_number(raw_grid, placed)

    # 5. Select hidden words — 3 intersecting words when the grid is rich enough,
    #    falling back to 2 / 1 on sparse grids.
    if len(words_numbered) >= 6:
        hidden_count = 3
    elif len(words_numbered) >= 4:
        hidden_count = 2
    else:
        hidden_count = 1
    words_final = _select_hidden_words(words_numbered, hidden_count)

    hidden_count_actual = sum(1 for w in words_final if w.get("hidden"))
    logging.info(
        "crossword_generator: placed=%d hidden=%d grid=%dx%d topic=%r",
        len(words_final), hidden_count_actual,
        len(grid_2d), len(grid_2d[0]) if grid_2d else 0,
        topic,
    )

    # 6. Save to DB
    crossword_id = str(uuid.uuid4())
    upsert_crossword_bank_entry(
        crossword_id=crossword_id,
        topic=topic,
        difficulty=difficulty,
        grid_json=grid_2d,
        words_json=words_final,
        image_status="pending",
    )

    return crossword_id


def prepare_crossword_pool(
    *, target_ready: int = 10, max_attempts: int = 20, force_fresh: bool = False
) -> dict:
    """
    Fill bt_3_crossword_bank up to target_ready entries.
    Returns stats dict.

    force_fresh=True retires all existing entries first, so the whole pool is
    regenerated with the current puzzle format (used after format changes).
    """
    from backend.database import count_crossword_bank_entries, retire_all_crossword_bank_entries

    stats = {"attempted": 0, "succeeded": 0, "failed": 0, "skipped": 0, "retired": 0}
    if force_fresh:
        stats["retired"] = retire_all_crossword_bank_entries()
        logging.info("crossword_pool: force_fresh retired=%d", stats["retired"])
    existing = count_crossword_bank_entries()
    needed = max(0, target_ready - existing)

    if needed == 0:
        stats["skipped"] = existing
        logging.info("crossword_pool: already at target existing=%d", existing)
        return stats

    logging.info("crossword_pool: existing=%d needed=%d", existing, needed)

    for _ in range(min(needed, max_attempts)):
        stats["attempted"] += 1
        try:
            cid = generate_crossword_entry()
            stats["succeeded"] += 1
            logging.info("crossword_pool: generated crossword_id=%s", cid)
        except Exception as exc:
            stats["failed"] += 1
            logging.warning("crossword_pool: generation failed: %s", exc)
        time.sleep(2.0)  # respect OpenAI rate limits

    return stats
