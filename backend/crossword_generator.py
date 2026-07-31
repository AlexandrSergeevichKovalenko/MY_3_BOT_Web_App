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
#
# Темы — только те, из которых слово получается ходовым. Прежний список был
# наполовину университетским («Philosophie und Ethik», «Sprache und Linguistik»),
# и из такой темы бытовое слово взять неоткуда: банк набивался словами вроде
# TUGENDHAFTIG и NORMENSYSTEM, которые человек не произносит никогда.
#
# У каждой темы — набор УГЛОВ. Угол выбирается случайно и подставляется в запрос,
# чтобы «Кухня» не сводилась каждый раз к одним и тем же пяти словам: в старом
# банке 48 % мест занимали повторы (UMWELTSCHUTZ — 10 раз, KLIMAWANDEL — 9).

_TOPICS: list[tuple[str, str, tuple[str, ...]]] = [
    ("Wohnung und Haushalt", "A2", (
        "Möbel und Einrichtung", "Küche und Geschirr", "Putzen und Ordnung",
        "Bad und Waschen", "Reparaturen und Werkzeug")),
    ("Essen und Trinken", "A2", (
        "Frühstück und Abendessen", "Obst und Gemüse", "Kochen und Backen",
        "im Restaurant bestellen", "Getränke und Süßes")),
    ("Einkaufen", "A2", (
        "im Supermarkt", "Kleidung kaufen", "Preise, Kasse und Kassenbon",
        "Umtausch und Reklamation", "Markt und Bäckerei")),
    ("Kleidung und Aussehen", "A2", (
        "Winterkleidung", "Sommerkleidung", "Schuhe und Accessoires",
        "Größen und Anprobe", "Haare und Pflege")),
    ("Familie und Freunde", "A2", (
        "Verwandtschaft", "Freundschaft und Treffen", "Kinder und Erziehung",
        "Streit und Versöhnung", "Feiern zu Hause")),
    ("Gesundheit und Arzt", "B1", (
        "beim Hausarzt", "Erkältung und Grippe", "Apotheke und Medikamente",
        "Zähne und Zahnarzt", "Schmerzen beschreiben")),
    ("Arbeit und Kollegen", "B1", (
        "Bewerbung und Vorstellungsgespräch", "Büroalltag", "Gehalt und Urlaub",
        "Team und Chef", "Kündigung und Vertrag")),
    ("Ämter und Papiere", "B1", (
        "Anmeldung und Ausweis", "Versicherung", "Steuern und Formulare",
        "Bank und Konto", "Post und Briefe")),
    ("Verkehr und unterwegs", "A2", (
        "Bus, Bahn und Fahrkarte", "Auto und Tanken", "Fahrrad und Fußweg",
        "Stau und Verspätung", "Wege und Richtungen")),
    ("Reise und Urlaub", "A2", (
        "Flughafen und Flug", "Hotel und Zimmer", "Koffer packen",
        "Strand und Sonne", "Stadtbesichtigung")),
    ("Wetter und Jahreszeiten", "A2", (
        "Regen und Sturm", "Winter und Schnee", "Sommer und Hitze",
        "Frühling und Herbst", "Wettervorhersage")),
    ("Stadt und Nachbarschaft", "A2", (
        "Straßen und Plätze", "Geschäfte in der Nähe", "Nachbarn und Haus",
        "Park und Spielplatz", "Müll und Ordnung")),
    ("Wohnungssuche und Miete", "B1", (
        "Wohnung besichtigen", "Miete und Nebenkosten", "Umzug",
        "Vermieter und Vertrag", "Renovieren")),
    ("Telefon, Internet und Technik", "B1", (
        "Handy und Apps", "Internet zu Hause", "Computer und Drucker",
        "Fotos und Musik", "Probleme und Support")),
    ("Sport und Bewegung", "A2", (
        "Fitnessstudio", "Ballsport", "Schwimmen und Wasser",
        "Laufen und Radfahren", "Winter- und Bergsport")),
    ("Freizeit und Hobbys", "A2", (
        "Kino und Fernsehen", "Bücher und Lesen", "Musik und Konzerte",
        "Garten und Pflanzen", "Basteln und Sammeln")),
    ("Schule und Lernen", "B1", (
        "Unterricht und Fächer", "Prüfungen und Noten", "Hausaufgaben",
        "Universität und Studium", "Sprachkurs")),
    ("Tiere und Natur", "A2", (
        "Haustiere", "Wald und Wiese", "Vögel und Insekten",
        "Bauernhof", "Meer und Fluss")),
    ("Feste und Geschenke", "A2", (
        "Geburtstag", "Weihnachten und Silvester", "Hochzeit",
        "Einladung und Gäste", "Geschenke und Karten")),
    ("Gefühle und Charakter", "B1", (
        "Freude und Ärger", "Angst und Mut", "gute Eigenschaften",
        "schlechte Eigenschaften", "Stress und Ruhe")),
    ("Zeit und Termine", "A2", (
        "Uhrzeit und Tagesablauf", "Kalender und Termine", "Verabredungen",
        "Pünktlichkeit und Verspätung", "Pläne machen")),
    ("Körper und Aussehen", "A2", (
        "Körperteile", "Gesicht", "Hände und Füße",
        "Körperpflege", "Schlaf und Erholung")),
    ("Geld und Rechnungen", "B1", (
        "bezahlen und sparen", "Rechnungen und Mahnungen", "Kredit und Rate",
        "Preise und Rabatte", "Haushaltskasse")),
    ("Kochen und Küche", "A2", (
        "Küchengeräte", "Zutaten und Gewürze", "Rezept Schritt für Schritt",
        "Tisch decken", "Reste und Kühlschrank")),
]

# ─── GPT prompts ──────────────────────────────────────────────────────────────

_GPT_SYSTEM = """\
Du schreibst Kreuzworträtsel für erwachsene Deutschlerner (Niveau A2-B1), die die
Sprache zum LEBEN brauchen: Wohnung, Arbeit, Arzt, Ämter, Einkaufen, Reisen.

Die eine Regel, die über allen steht: JEDES Wort muss ein Wort sein, das ein
Mensch in einer normalen Woche wirklich benutzt oder hört. Wenn du zögerst, ob
jemand das Wort je gesagt hat — nimm es nicht.

Regeln für jedes Wort:
- Ein einzelnes Wort, normale deutsche Rechtschreibung mit echten Umlauten
  (KÜHLSCHRANK, nicht KUEHLSCHRANK), ohne Artikel, ohne Bindestrich, ohne Leerzeichen
- Grundform: Nominativ Singular oder Plural beim Nomen, Infinitiv beim Verb,
  Grundform beim Adjektiv. Keine Genitive (PARKPLATZES), keine gebeugten Formen
- Länge 4-13 Buchstaben
- Mische Wortarten: etwa die Hälfte Nomen, dazu Verben und Adjektive
- VERBOTEN: Fachbegriffe, Wissenschafts- und Verwaltungssprache, abstrakte
  Substantive auf -ismus, -ität, -ologie, -theorie; Wörter, die man nur in
  Zeitungen oder Lehrbüchern liest; erfundene oder zusammengebastelte Komposita
- Zusammengesetzte Wörter nur, wenn sie im Alltag wirklich vorkommen
  (WASCHMASCHINE ja, UMWELTFAKTOR nein)
- clue_de: EIN einfacher Satz auf A2-Niveau, der das Wort beschreibt, ohne es zu
  nennen. Wo es hilft, ein Alltagsbeispiel statt einer Definition
- clue_ru: derselbe Hinweis auf natürlichem Russisch
- translation_ru: die reine Übersetzung des Wortes, 1-3 Wörter, ohne Erklärung

Antworte NUR mit validem JSON, ohne Erklärungen."""

_GPT_USER_TMPL = """\
Thema: {topic}
Schwerpunkt: {angle}
Niveau: {difficulty}

Erstelle exakt 14 deutsche Alltagswörter zu diesem Schwerpunkt für ein Kreuzworträtsel.

Prüfe jedes Wort selbst: Würde ein Erwachsener dieses Wort diese Woche in einem
Gespräch, im Laden, beim Arzt oder bei der Arbeit benutzen? Nein — streiche es und
nimm ein anderes.
{avoid}
Ausgabe:
{{
  "words": [
    {{
      "word": "KÜHLSCHRANK",
      "clue_de": "Hier bleiben Milch und Butter kalt",
      "clue_ru": "Здесь молоко и масло остаются холодными",
      "translation_ru": "холодильник"
    }}
  ]
}}"""

# ─── GPT call ─────────────────────────────────────────────────────────────────

def _call_gpt_for_words(
    topic: str, difficulty: str, *, angle: str = "", avoid: list[str] | None = None,
) -> list[dict]:
    import requests as _requests

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_QUIZ_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    avoid_block = ""
    if avoid:
        avoid_block = (
            "\nDiese Wörter kamen zuletzt schon dran — nimm andere:\n"
            + ", ".join(avoid) + "\n"
        )
    user = _GPT_USER_TMPL.format(
        topic=topic, difficulty=difficulty, angle=angle or topic, avoid=avoid_block,
    )
    payload = {
        "model": model,
        "temperature": 0.75,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _GPT_SYSTEM},
            {"role": "user", "content": user},
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

    resp_json = resp.json()
    try:
        from backend.openai_usage_logging import log_openai_raw_usage
        log_openai_raw_usage(action_type="pool_crossword", model=model,
                             usage=resp_json.get("usage"), user_id=None)
    except Exception:
        pass
    raw = str((resp_json.get("choices") or [{}])[0].get("message", {}).get("content") or "")
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"JSON parse failed: {exc}") from exc

    words = parsed.get("words")
    if not isinstance(words, list):
        raise RuntimeError("GPT returned no 'words' array")
    return words


def _accept_word_entry(entry: dict) -> tuple[Optional[dict], str]:
    """Приёмка одного слова от модели → (готовая запись, причина отказа).

    Прежняя проверка смотрела только длину и что символы буквенные — этого хватало,
    чтобы в прод уезжали MIVVERKEHR, DEONTOLIGIE и обрубок VERSUCHSAN. Теперь
    написание приводится к подтверждённому словарём виду, а слово, которого в живом
    немецком нет, дальше не проходит (backend/crossword_word_gate.py).
    """
    from backend.crossword_word_gate import check_word, normalize_word

    word = normalize_word(entry.get("word"))
    if not word:
        return None, "пустое слово"
    ok, reason = check_word(word)
    if not ok:
        return None, f"{word}: {reason}"
    clue_de = str(entry.get("clue_de") or "").strip()
    clue_ru = str(entry.get("clue_ru") or "").strip()
    if not clue_de:
        return None, f"{word}: нет немецкой подсказки"
    if not clue_ru:
        return None, f"{word}: нет русской подсказки"
    return {
        "word": word,
        "clue_de": clue_de,
        "clue_ru": clue_ru,
        # Перевод отдельно от подсказки: подсказка — это фраза-описание, и когда
        # её сохраняли в словарь как «перевод», в карточке оказывалось предложение.
        "translation_ru": str(entry.get("translation_ru") or "").strip(),
    }, ""


# ─── Grid placement ────────────────────────────────────────────────────────────

_MAX_GRID = 25

# Puzzle shape floor: every shipped crossword has at least 3 blanks, which needs
# at least 4 placed words (so some letters stay visible as anchors).
_MIN_HIDDEN = 3
_MIN_PLACED = 4


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
    isolated, unrelated blanks.

    Загаданное слово человек набирает руками — поэтому в загаданные идут прежде
    всего слова из обиходной речи (`is_everyday`), и только потом смотрим на длину.
    Слово может быть редковатым и всё равно стоять в сетке как подсказка-опора,
    но вводить с клавиатуры мы просим только то, что человек и правда употребляет.
    """
    from backend.crossword_word_gate import is_everyday

    result = [{**w, "hidden": False} for w in words]
    n = len(result)
    if n <= hidden_count:
        for w in result:
            w["hidden"] = True
        return result

    everyday = [is_everyday(w["word"]) for w in result]

    def _length_pref(idx: int) -> float:
        # обиходность важнее длины: разрыв в 10 больше любой разницы по длине
        bonus = 10.0 if everyday[idx] else 0.0
        return bonus - abs(len(result[idx]["word"]) - 5.5)  # peak at 5-6 letters

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
    from backend.database import recent_crossword_bank_words, upsert_crossword_bank_entry

    angle = ""
    if not topic or not difficulty:
        chosen_topic, chosen_diff, angles = random.choice(_TOPICS)
        topic = topic or chosen_topic
        difficulty = difficulty or chosen_diff
        angle = random.choice(angles) if angles else ""

    # Слова из последних кроссвордов — чтобы «Кухня» не приходила каждый раз с одним
    # и тем же холодильником: в старом банке 48 % мест занимали повторы.
    try:
        avoid = recent_crossword_bank_words(limit_entries=12)
    except Exception:
        logging.debug("crossword_generator: recent words unavailable", exc_info=True)
        avoid = []

    logging.info(
        "crossword_generator: generating topic=%r angle=%r difficulty=%s avoid=%d",
        topic, angle, difficulty, len(avoid),
    )

    # 1. Get words from GPT
    raw_words = _call_gpt_for_words(topic, difficulty, angle=angle, avoid=avoid)

    # 2. Приёмка: написание, существование слова, подсказки
    valid_words: list[dict] = []
    seen_words: set[str] = set()
    rejected: list[str] = []
    for entry in raw_words:
        accepted, reason = _accept_word_entry(entry)
        if not accepted:
            rejected.append(reason)
            continue
        if accepted["word"] in seen_words:
            continue
        seen_words.add(accepted["word"])
        valid_words.append(accepted)

    if rejected:
        logging.info("crossword_generator: отклонено слов %d: %s", len(rejected), "; ".join(rejected))

    if len(valid_words) < 6:
        raise RuntimeError(
            f"после приёмки осталось {len(valid_words)} слов из {len(raw_words)} — кроссворд не собираем"
        )

    # 3. Place words in grid
    raw_grid, placed = _place_words(valid_words)
    if len(placed) < _MIN_PLACED:
        raise RuntimeError(f"Only {len(placed)} words placed — crossword too sparse")

    # 4. Normalize and number words
    grid_2d, words_numbered = _normalize_and_number(raw_grid, placed)

    # 5. Select hidden words — aim for 4 mutually-intersecting words (each crossing
    #    the chain so solving one reveals letters of the next). _MIN_HIDDEN is a
    #    hard floor: a puzzle with fewer blanks is rejected, never shipped.
    hidden_count = 4 if len(words_numbered) >= 6 else _MIN_HIDDEN
    words_final = _select_hidden_words(words_numbered, hidden_count)

    hidden_count_actual = sum(1 for w in words_final if w.get("hidden"))
    if hidden_count_actual < _MIN_HIDDEN:
        raise RuntimeError(
            f"Only {hidden_count_actual} hidden words (need {_MIN_HIDDEN}) — puzzle rejected"
        )

    # Загаданное слово человек набирает руками. Одно слово «на вырост» в наборе
    # допустимо, два и больше — это уже не кроссворд, а экзамен: такой не отправляем.
    from backend.crossword_word_gate import is_everyday
    rare_hidden = [w["word"] for w in words_final if w.get("hidden") and not is_everyday(w["word"])]
    if len(rare_hidden) > 1:
        raise RuntimeError(
            "загаданы неходовые слова (%s) — кроссворд не отправляем" % ", ".join(rare_hidden)
        )

    logging.info(
        "crossword_generator: placed=%d hidden=%d rare_hidden=%d grid=%dx%d topic=%r angle=%r",
        len(words_final), hidden_count_actual, len(rare_hidden),
        len(grid_2d), len(grid_2d[0]) if grid_2d else 0,
        topic, angle,
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
    from backend.database import (
        count_crossword_bank_entries,
        retire_all_crossword_bank_entries,
        retire_undersized_crossword_bank_entries,
    )

    stats = {"attempted": 0, "succeeded": 0, "failed": 0, "skipped": 0, "retired": 0}
    if force_fresh:
        stats["retired"] = retire_all_crossword_bank_entries()
        logging.info("crossword_pool: force_fresh retired=%d", stats["retired"])
    else:
        # Drop legacy puzzles that predate the 3-blank format before topping up,
        # so the refill actually replaces them instead of sitting behind them.
        undersized = retire_undersized_crossword_bank_entries(_MIN_HIDDEN)
        stats["retired"] += undersized
        if undersized:
            logging.info("crossword_pool: retired undersized=%d", undersized)
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
