"""Сборка разбора по шагам: найти → объяснить → пересказать → спросить.

Зачем это появилось (владелец, 23.08.2026): «когда я обращаюсь к модели, она нормально
объясняет, у меня вопросов не возникает. Почему в твоей реализации выходит ерунда?»

Ответ честный и он про меня, а не про модель. Когда владелец спрашивает сам, он задаёт
ОДИН вопрос об ОДНОЙ вещи и читает ответ своей головой. А прежняя реализация просила за
один ответ: пятнадцать карточек, восемь полей в каждой, четыре вопроса теста, пересказ —
и при этом соблюсти два десятка требований. Требования начинали конкурировать: модель
выполняла одни за счёт других, и каждое новое правило ослабляло остальные.

Поэтому работа разложена на четыре РАЗНЫХ дела, у каждого свой запрос и свои три-четыре
требования вместо двадцати:

  1. НАЙТИ   — прочитать транскрипт и назвать единицы, которые стоит объяснить.
               Здесь решается ТОЛЬКО отбор. Ни переводов, ни помет, ни теста.
  2. ОБЪЯСНИТЬ — по уже отобранным единицам заполнить поля карточки.
               Здесь НЕТ решений об отборе: что дали, то и объясняем.
  3. ПЕРЕСКАЗАТЬ — тезисы к ролику.
  4. СПРОСИТЬ — четыре вопроса теста.

Объяснение идёт пачками по несколько единиц: так у каждого ответа остаётся мало
требований, но и запросов не два десятка.
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name) or "").strip() or default)
    except Exception:
        return default


# ── 1. НАЙТИ: только отбор, ничего больше ─────────────────────────────────────

_FIND_SYSTEM = """\
Du bist Deutschlehrer und liest das Transkript eines Videos. Deine EINZIGE Aufgabe hier:
sagen, WELCHE Spracheinheiten dem Lernenden (B1–B2) erklärt werden sollten. Du erklärst
sie NICHT — das macht ein anderer Schritt. Übersetze nichts, markiere nichts.

WAS EINE EINHEIT IST: ein Wort oder eine feste Wendung, die dem Lernenden in einer ANDEREN
Situation wiederbegegnet — "der Schwarzmarkt", "unter Druck stehen", "Tür und Tor öffnen",
"nichts am Hut haben", "einen Kater haben".

WAS KEINE EINHEIT IST:
  • Zahlen dieser Meldung: "rund 300 Aussteller", "bis zu 30.000 Besuchern";
  • Amts- und Titelbezeichnungen: "der Drogenbeauftragte der Bundesregierung";
  • ganze Sätze mit Subjekt und konjugiertem Verb: "Opfer fordern ihre Rechte";
  • zwei Einheiten mit Komma zusammengeklebt: "ein neuer Markt, der Graumarkt";
  • etwas, das auf einem Artikel oder einer Konjunktion ENDET.

ES GIBT KEINE ZIELZAHL. Geh das Transkript von ANFANG BIS ENDE durch, auch die zweite
Hälfte. Findest du 25 lohnende Einheiten — nenn 25. Findest du 8 — nenn 8. Was du
auslässt, erfährt der Nutzer nie.

Das Transkript ist maschinell erkannt und enthält Tippfehler. Nimm eine Einheit NUR aus
einer sauberen Zeile; ist die einzige Stelle verstümmelt, lass die Einheit weg.

Antworte NUR mit validem JSON:
{"units": [{"de": "<Einheit in der Nachschlageform>", "quote_de": "<die Zeile aus dem
Transkript, WÖRTLICH kopiert, 4–20 Wörter, in der die Einheit vorkommt>"}]}"""


# ── 2. ОБЪЯСНИТЬ: только заполнение полей, отбор уже сделан ───────────────────

_EXPLAIN_SYSTEM = """\
Du bist Sprachwissenschaftler. Du bekommst FERTIG AUSGEWÄHLTE Einheiten und füllst zu
jeder die Felder der Lernkarte aus. Du entscheidest NICHT, ob die Einheit gut ist — die
Auswahl ist getroffen. Lass keine Einheit weg und füg keine hinzu.

Zu jeder Einheit:
  - "de": die Einheit, wie sie dir gegeben wurde, korrekt geschrieben. Deutsche
    Substantive GROSS. Reflexivpronomen "sich" NUR, wenn das Zitat es zeigt: steht dort
    ein Objekt wie "dich", gehört "jemanden" davor, nicht "sich".
  - "form_ru": in welcher Form "de" dasteht. ЗАКРЫТЫЙ СПИСОК, дословно одно из:
    «словарная форма» · «устойчивое выражение» · «инфинитив» · «именительный падеж» ·
    «винительный падеж» · «дательный падеж» · «родительный падеж» ·
    «множественное число» · «повелительная форма». Ничего своего, никаких немецких слов.
    Существительное в словарном виде падежа НЕ имеет — это «словарная форма».
  - "translation_ru": перевод в ТОЙ ЖЕ форме, что и "de".
  - "de_in_text": единица ровно так, как она стоит в цитате. Обязана дословно
    встречаться в "quote_de".
  - "quote_ru": перевод строки-цитаты.
  - "usage_ru": одно предложение по-русски — как и когда это употребляют, с управлением.
{extra_fields}
Antworte NUR mit validem JSON: {{"cards": [ …по одной на каждую данную единицу… ]}}"""

_EXPLAIN_STANDUP_EXTRA = """  - "register_ru": помета живой речи по-русски: «разговорное», «сленг», «грубое»,
    «молодёжное», «ироничное». Грубое называй грубым — ученик должен знать, где так
    говорить нельзя.
  - "literal_ru": обычное значение — ТОЛЬКО если оно вправду другое, чем здесь. Одинаково
    — пустая строка. Второе значение НЕ выдумывать.
"""


# ── 3 и 4: пересказ и тест — отдельными, короткими запросами ──────────────────

_SUMMARY_SYSTEM = """\
Du fasst ein Video für russischsprachige Deutschlernende zusammen: 2–4 sehr kurze Zeilen
auf RUSSISCH, je EIN Fakt, 3–9 Wörter, ohne Verbindungswörter und ohne Wertung.

Eigennamen behalten ihren Grossbuchstaben — aber NUR der erste Buchstabe ist gross:
«Тереза», «Германия», НЕ «ТЕРЕЗА». Abkürzungen wie im Russischen üblich (AfD → АдГ).
Die Spracherkennung verstümmelt Namen: bist du dir bei einem Namen nicht sicher, benutz
ihn GAR NICHT — ein falscher Name wird vom Nutzer als richtiger gelesen.
{spoiler_rule}
Antworte NUR mit validem JSON: {{"summary_points": ["…", "…"]}}"""

_SPOILER_RULE = """
NICHTS VERRATEN, was die Spannung nimmt: weder die Pointe noch den AUSGANG — wer gewinnt,
wie es endet. Der Nutzer liest das VOR dem Video. Beschreibe die SITUATION, nicht das
Ergebnis."""

_QUIZ_SYSTEM = """\
Du stellst GENAU 4 Multiple-Choice-Fragen auf DEUTSCH zum Verständnis des Videos.
{focus}
Die Distraktoren sind minimal verschieden und etwa gleich lang — keine offensichtlich
absurde Antwort.

FRAGE NICHT NACH DER BEDEUTUNG der Wörter, die unten in der Liste stehen: der Nutzer hat
sie schon als Karten gelesen, so eine Frage prüft nichts.

Antworte NUR mit validem JSON:
{{"quiz": [{{"question_de": "…", "options": ["…","…","…","…"], "correct_index": 0,
            "explanation_ru": "<кратко по-русски, с опорой на ролик>"}}]}}"""

_QUIZ_NEWS_FOCUS = """\
Frag nach PRÄZISEN Details: Zahlen, Beträge, Prozente, Daten, Fristen, Eigennamen, wer
genau was gesagt hat, exakte Bedingungen. Mindestens 2 der 4 Fragen zielen auf Zahlen oder
Daten, wenn es welche gibt."""

_QUIZ_STANDUP_FOCUS = """\
Frag nach dem VERSTÄNDNIS des Auftritts: wer was macht, welche Situation beschrieben wird,
was der Comedian mit einer Wendung meint, worüber das Publikum lacht. KEINE Fragen nach
Zahlen — das ist kein Nachrichtenvideo. Verrate auch hier den AUSGANG nicht."""


def build_pack_in_steps(*, title: str, transcript: str, profile, call_json) -> dict:
    """Собрать разбор четырьмя отдельными запросами вместо одного большого.

    `call_json(system, user, what)` — как обращаться к модели; передаётся снаружи, чтобы
    этот модуль не знал ни про ключи, ни про учёт расхода.

    Ошибки НЕ глушатся: пустой разбор от сбоя неотличим от честного «в ролике нечего
    объяснять», а это два разных мира.
    """
    is_standup = getattr(profile, "key", "") == "standup"

    # 1. НАЙТИ
    found = call_json(
        _FIND_SYSTEM,
        f"Videotitel: {title or '—'}\n\nTranskript:\n{transcript}",
        "поиск единиц",
    )
    units = [
        {"de": str(u.get("de") or "").strip(), "quote_de": str(u.get("quote_de") or "").strip()}
        for u in (found.get("units") or [])
        if isinstance(u, dict) and str(u.get("de") or "").strip()
    ]
    if not units:
        raise ValueError("шаг «найти» не дал ни одной единицы")
    logger.info("разбор[%s]: найдено единиц — %d", getattr(profile, "key", "?"), len(units))

    # 2. ОБЪЯСНИТЬ — пачками, чтобы у каждого ответа было мало требований
    batch_size = max(1, _env_int("DAILY_VIDEO_EXPLAIN_BATCH", 5))
    system = _EXPLAIN_SYSTEM.format(extra_fields=_EXPLAIN_STANDUP_EXTRA if is_standup else "")
    cards: list = []
    for start in range(0, len(units), batch_size):
        chunk = units[start:start + batch_size]
        answer = call_json(
            system,
            "Transkript (для контекста):\n" + transcript[:6000]
            + "\n\nEinheiten:\n" + json.dumps(chunk, ensure_ascii=False),
            f"объяснение {start + 1}–{start + len(chunk)}",
        )
        got = [c for c in (answer.get("cards") or []) if isinstance(c, dict)]
        # Цитату берём ИЗ ШАГА ПОИСКА, а не из ответа объясняющего: она уже сверена с
        # транскриптом, и переписывать её объясняющему незачем.
        for i, card in enumerate(got[:len(chunk)]):
            card["quote_de"] = chunk[i]["quote_de"]
            card.setdefault("de", chunk[i]["de"])
        cards.extend(got[:len(chunk)])

    # 3. ПЕРЕСКАЗАТЬ
    summary = call_json(
        _SUMMARY_SYSTEM.format(spoiler_rule=_SPOILER_RULE if is_standup else ""),
        f"Videotitel: {title or '—'}\n\nTranskript:\n{transcript}",
        "пересказ",
    )

    # 4. СПРОСИТЬ
    quiz = call_json(
        _QUIZ_SYSTEM.format(focus=_QUIZ_STANDUP_FOCUS if is_standup else _QUIZ_NEWS_FOCUS),
        f"Transkript:\n{transcript}\n\nSchon erklärte Wörter (danach NICHT fragen):\n"
        + json.dumps([c.get("de") for c in cards], ensure_ascii=False),
        "тест",
    )

    return {
        "summary_points": summary.get("summary_points") or [],
        "phrases": cards,
        "quiz": quiz.get("quiz") or [],
    }
