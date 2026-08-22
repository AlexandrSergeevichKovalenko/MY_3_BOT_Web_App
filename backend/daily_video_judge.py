"""Судья приёмки карточек — правит ПОКАРТОЧНО, а не бракует выпуск целиком.

Решение владельца 22.08.2026, дословно: «мне не нужно чтобы было переформировать новость
или стендап, на которой потрачено много денег и времени и ресурсов других».

Почему прежний замысел был негодным. Я собирался выносить вердикт всему пакету: не
понравилась одна карточка — брак, переделываем заново. Это значит выбросить выбранный
ролик, скачанные субтитры, четырнадцать хороших карточек и готовый тест из-за одного
слова со строчной буквы — и получить на выходе новую лотерею. Цена огромная, польза
никакая.

Поэтому судья работает ПО КАРТОЧКЕ, и у каждой три исхода:

  «годна»    — идёт дальше нетронутой;
  «поправить» — карточка по сути хорошая, но в подаче огрех: существительное со строчной,
                потерялся артикль или возвратное «sich», перевод не согласован с
                показанной формой. Судья возвращает исправленную карточку;
  «выбросить» — карточку не спасти: единица оказалась репликой из шоу, цитата не
                показывает слово.

Ролик, субтитры, тест и остальные карточки НЕ ТРОГАЮТСЯ ни при каком исходе.

── Почему правка судьи не является выдумкой ───────────────────────────────────
Правильное написание не сочиняется, оно ЛЕЖИТ В САМИХ СУБТИТРАХ: в тексте ролика стоит
«Herzinfarkt» с прописной, и судья лишь переносит это в заголовок. А чтобы он не начал
сочинять под видом правки, КАЖДАЯ исправленная карточка заново проходит те же
механические стражи, что и свежая: цитата обязана дословно найтись в субтитрах, форма из
текста — внутри цитаты, цитата — показывать разбираемое слово. Не прошла — карточка
выбрасывается, а не показывается «исправленной».

── Несколько проходов ─────────────────────────────────────────────────────────
Судья идёт по карточкам столько раз, сколько нужно, пока проход не окажется чистым (или
пока не кончится лимит проходов). Одна правка иногда обнажает следующую, и один проход
это не ловит.
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


_JUDGE_SYSTEM = """\
Du bist Korrektor für Deutschlernmaterial. Du bekommst fertige Vokabelkarten und prüfst
JEDE EINZELN. Du schreibst das Material NICHT neu — du korrigierst Fehler und wirfst
Unbrauchbares raus.

Prüfe bei jeder Karte:

1) RECHTSCHREIBUNG. Deutsche Substantive werden GROSS geschrieben, auch mitten in einer
   Wendung ("einen Herzinfarkt bekommen", nicht "herzinfarkt bekommen"). Ein einzelnes
   Nomen steht mit Artikel ("die Kohle"). Ein reflexives Verb behält "sich"
   ("sich ins eigene Bein schießen"). Die richtige Schreibweise steht im Transkript —
   nimm sie von dort, denk sie dir NICHT aus.

2) GRAMMATIK der Einheit. Ist "de" eine saubere Nachschlageform? Eine in der 2. Person
   stehende Replik ("Steckst nicht drin") gehört unpersönlich formuliert
   ("da steckt man nicht drin").

3) "form_ru" EHRLICH. Steht in "de" eine Nachschlageform, ist die Antwort «словарная
   форма» — ein Nomen in der Nennform HAT KEINEN KASUS, und einen dazuzuschreiben heisst,
   dem Lernenden Grammatik zu erfinden. Einen Kasus nur, wenn die Wortgruppe absichtlich
   gebeugt stehen blieb. Bei Wendungen ohne Nomen: «устойчивое выражение».

4) "translation_ru" stimmt mit "de" überein — inhaltlich UND in der Form. Zeigt das
   Deutsche einen Akkusativ, steht auch das Russische im Akkusativ.

5) "de_in_text" steht WÖRTLICH im Zitat "quote_de". "quote_ru" übersetzt genau dieses
   Zitat.

6) WIEDERVERWENDBARKEIT. Die Einheit taugt nur, wenn der Lernende sie in einer ANDEREN
   Situation benutzen kann. Repliken aus der Sendung ("Privatversicherte verstehen den
   Joke") und erfundene Wortspiele des Moderators ("Niceinger Diceinger") sind KEINE
   Spracheinheiten — solche Karten wirfst du raus.

{register_rule}
Antworte NUR mit validem JSON:
{{"cards": [{{"i": <Index der Karte>, "verdict": "ok" | "fix" | "drop",
             "reason": "<kurz, auf Russisch, WAS falsch war>",
             "card": {{ …vollständige korrigierte Karte, nur bei verdict "fix"… }}}}]}}

WICHTIGSTE REGEL FÜR DICH: Du korrigierst FEHLER, du verbesserst nicht den STIL.
Ist eine Karte richtig, aber du hättest es anders formuliert — dann ist sie "ok". Nur
das, was FALSCH ist, wird angefasst: falsche Schreibweise, falsche Grammatik, erfundener
Kasus, Übersetzung passt nicht zur Form, Zitat belegt die Einheit nicht, Einheit ist keine
Spracheinheit. Alles andere lässt du in Ruhe.
Ohne diese Regel findest du bei jedem Durchgang wieder etwas «Besseres», und die Prüfung
kommt nie zum Ende — genau das ist am 22.08.2026 passiert: drei Durchgänge, kein einziger
sauber, keine einzige Karte wirklich schlecht.

Bei "ok" lässt du "card" weg. Bei "fix" gibst du die GANZE Karte mit allen Feldern zurück,
auch den unveränderten. Ändere NUR das, was falsch ist — erfinde keine neuen Beispiele,
keine neuen Zitate, keine neuen Bedeutungen."""

_REGISTER_RULE = """\
7) "register_ru" — Stilmarkierung («сленг», «разговорное», «грубое», «молодёжное»,
   «ироничное»). Sie muss stimmen: derbe Sprache darf nicht als «разговорное»
   verharmlost werden. Neutrale Alltagswörter gehören NICHT in diese Rubrik — raus damit.

"""


def _judge_model() -> str:
    return (
        os.getenv("DAILY_VIDEO_JUDGE_MODEL")
        or os.getenv("WORLD_NEWS_MODEL")
        or os.getenv("OPENAI_MODEL")
        or "gpt-4.1-2025-04-14"
    ).strip()


def _ask_judge(cards: list, *, profile, transcript: str) -> list:
    """Один проход судьи. Возвращает список вердиктов. Ошибки НЕ глушим: молча пропущенная
    проверка неотличима от пройденной, а это два разных мира."""
    import requests

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    system = _JUDGE_SYSTEM.format(
        register_rule=_REGISTER_RULE if getattr(profile, "requires_register", False) else ""
    )
    payload = {
        "model": _judge_model(),
        "temperature": 0,           # проверка, а не творчество
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content":
                "Transkript des Videos (die einzige Quelle für Schreibweisen und Zitate):\n"
                f"{transcript[:8000]}\n\nKarten:\n"
                + json.dumps([dict(c, i=i) for i, c in enumerate(cards)], ensure_ascii=False)},
        ],
    }
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload, timeout=_env_int("DAILY_VIDEO_JUDGE_TIMEOUT_SEC", 180),
    )
    if not resp.ok:
        raise RuntimeError(f"судья: OpenAI HTTP {resp.status_code}: {resp.text[:200]}")
    resp_json = resp.json()
    try:
        from backend.openai_usage_logging import log_openai_raw_usage
        log_openai_raw_usage(action_type=f"judge_{getattr(profile, 'key', 'daily_video')}",
                             model=str(payload.get("model") or ""),
                             usage=resp_json.get("usage"), user_id=None)
    except Exception:
        logger.debug("судья: расход не записан", exc_info=True)
    raw = (resp_json.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    data = json.loads(raw)
    verdicts = data.get("cards")
    if not isinstance(verdicts, list):
        raise ValueError("судья вернул ответ без списка карточек")
    return verdicts


def judge_and_repair_cards(cards: list, *, profile, transcript: str) -> tuple[list, dict]:
    """Пройти по карточкам столько раз, сколько нужно, пока проход не станет чистым.

    Возвращает (карточки, отчёт). Ролик, субтитры и тест не затрагиваются вовсе —
    судья видит только карточки.

    Одна правка иногда обнажает следующую, поэтому проходов несколько: судья идёт по
    карточкам заново, пока не окажется, что править нечего.
    """
    from backend.world_news_generator import _card_passes_source_guards

    passes = max(1, _env_int("DAILY_VIDEO_JUDGE_PASSES", 3))
    report = {"passes": 0, "fixed": 0, "dropped": 0, "reasons": [], "clean": False}
    current = list(cards)

    for attempt in range(passes):
        report["passes"] = attempt + 1
        verdicts = _ask_judge(current, profile=profile, transcript=transcript)
        by_index = {}
        for v in verdicts:
            if isinstance(v, dict) and isinstance(v.get("i"), int):
                by_index[v["i"]] = v

        next_cards = []
        touched = 0
        for i, card in enumerate(current):
            verdict = by_index.get(i) or {}
            decision = str(verdict.get("verdict") or "ok").strip().lower()
            reason = str(verdict.get("reason") or "").strip()

            if decision == "drop":
                touched += 1
                report["dropped"] += 1
                report["reasons"].append(f"выброшена «{card.get('de')}»: {reason or '—'}")
                continue

            if decision == "fix":
                fixed = verdict.get("card")
                if not isinstance(fixed, dict) or not str(fixed.get("de") or "").strip():
                    # Судья пометил «поправить», но починки не дал. Оставляем как было и
                    # говорим об этом: молча проглотить — значит соврать, что проверили.
                    logger.warning("судья: вердикт «поправить» без карточки для %r", card.get("de"))
                    next_cards.append(card)
                    continue
                merged = dict(card)
                merged.update({k: v for k, v in fixed.items() if k != "i"})
                # ПРАВКА, НИЧЕГО НЕ МЕНЯЮЩАЯ, — НЕ ПРАВКА. 22.08.2026 судья три прохода
                # подряд «исправлял» «das kurze Vergnügen» на «das kurze Vergnügen»,
                # объясняя это отсутствием артикля, которого не было только в его
                # объяснении. Проверка не сходилась, потому что он выдумывал себе работу
                # на уже исправленной карточке. Если после правки карточка та же — значит
                # править было нечего, и проход считается чистым.
                if merged == card:
                    logger.info("судья: пустая правка на %r — считаем годной", card.get("de"))
                    next_cards.append(card)
                    continue
                # ГЛАВНЫЙ ЗАСЛОН: исправленная карточка проходит те же стражи, что и свежая.
                # Если судья под видом правки что-то присочинил — цитату, которой нет в
                # субтитрах, или форму, которой нет в цитате, — карточка выбрасывается.
                ok, why = _card_passes_source_guards(merged, transcript, profile=profile)
                if not ok:
                    touched += 1
                    report["dropped"] += 1
                    report["reasons"].append(
                        f"правка судьи не прошла сверку с субтитрами «{card.get('de')}»: {why}"
                    )
                    continue
                touched += 1
                report["fixed"] += 1
                report["reasons"].append(f"поправлена «{card.get('de')}» → «{merged.get('de')}»: {reason or '—'}")
                next_cards.append(merged)
                continue

            next_cards.append(card)

        current = next_cards
        if not touched:
            report["clean"] = True
            break

    if not report["clean"]:
        logger.warning("судья: за %d прохода(ов) чистого прогона не вышло — осталось %d карточек",
                       report["passes"], len(current))
    logger.info("судья[%s]: проходов %d, поправлено %d, выброшено %d, осталось %d",
                getattr(profile, "key", "?"), report["passes"], report["fixed"],
                report["dropped"], len(current))
    return current, report
