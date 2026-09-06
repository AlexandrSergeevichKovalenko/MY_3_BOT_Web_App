"""Контроль карточек: контролёр без права переписывать → автор отвечает → повтор только по
исправленным. Всё под капотом, без человека.

┌─ ПЕРЕДЕЛАНО 06.09.2026 (решение владельца) ────────────────────────────────────────┐
│ Прежний судья был вторым редактором с теми же полномочиями, что и сборщик: на       │
│ «поправить» он возвращал карточку целиком, шёл до трёх проходов и получал 8000      │
│ знаков субтитров в каждом. Два редактора одного текста всегда найдут, что           │
│ переписать: на экране владельца «sich versöhnen» → «versöhnen» → «sich versöhnen»   │
│ за один вечер, плюс правки, не менявшие ничего видимого. Заслон от качелей ловил    │
│ это задним числом и не справлялся. Владелец: «чем больше моделей, тем больше        │
│ путаницы; зачем три прохода?»                                                       │
│                                                                                     │
│ Теперь — как в редакции: корректор не переписывает автора, он ставит пометку на     │
│ полях, и автор правит сам.                                                          │
│   1. КОНТРОЛЬ, один проход. Модель читает готовые карточки (цитата уже внутри,      │
│      субтитры не шлём) и на каждую отвечает только «в порядке» или «сомнение: поле, │
│      в чём». Переписывать ей нельзя — спорить не с чем, качелям неоткуда взяться.   │
│   2. АВТОР ОТВЕЧАЕТ. Карточка с сомнением возвращается на шаг «объяснить» одной     │
│      единицей, с замечанием и полным текстом субтитров. Автор с субтитрами и        │
│      конкретной претензией — самая осведомлённая сторона.                           │
│   3. ПОВТОРНЫЙ КОНТРОЛЬ только исправленных. Сомнение второй раз — карточка         │
│      выбрасывается: неверная грамматика к ученику не доходит, выпуск не             │
│      переделывается. Ролик, субтитры, тест и остальные карточки не трогаются.       │
│   4. Раз в неделю владельцу приходит ЧИСЛО (bot_3: run_daily_video_control_report), │
│      не вопрос: «сомневался в N, автор поправил M, выброшено K».                    │
│ Цена: одно обращение без субтитров вместо трёх с субтитрами плюс точечные запросы  │
│ по одной карточке.                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘

Почему исправленная карточка — не выдумка: она заново проходит те же стражи, что и
свежая (цитата в субтитрах, форма из текста внутри цитаты, помета из закрытого списка),
и не имеет права потерять помету формы или регистра.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


_CONTROL_SYSTEM = """\
Du bist Korrektor für Deutschlernmaterial. Du bekommst fertige Vokabelkarten und prüfst
JEDE EINZELN. Du schreibst NICHTS um und lieferst KEINE korrigierte Karte: du sagst nur,
ob die Karte in Ordnung ist, und wenn nicht — WELCHES Feld und WAS daran falsch ist.
Die Korrektur macht der Autor der Karte selbst, mit dem vollen Transkript vor Augen.

Das Zitat "quote_de" ist die einzige Quelle für Schreibweisen und Formen. Prüfe:

1) RECHTSCHREIBUNG in "de". Deutsche Substantive GROSS, auch mitten in einer Wendung.
   Ein einzelnes Nomen steht mit Artikel.
   ВОЗВРАТНОСТЬ ЧИТАЕТСЯ ИЗ ЦИТАТЫ, А НЕ ДОДУМЫВАЕТСЯ: «habe ich DICH unter den Tisch
   gesoffen» → «jemanden unter den Tisch saufen», НЕ «sich…»; «um SICH ein Bild zu
   machen» → «sich ein Bild machen». Нет в цитате ни «sich», ни личного дополнения —
   претензии нет.

2) GRAMMATIK: ist "de" eine saubere Nachschlageform? Eine Replik in der 2. Person
   ("Steckst nicht drin") gehört unpersönlich ("da steckt man nicht drin").

3) "form_ru" — ТОЛЬКО одно из: «словарная форма» · «устойчивое выражение» · «инфинитив» ·
   «именительный падеж» · «винительный падеж» · «дательный падеж» · «родительный падеж» ·
   «множественное число» · «повелительная форма». Существительное в словарном виде падежа
   НЕ имеет — это «словарная форма». Немецкие термины и слова в помете — ошибка.

4) "translation_ru" stimmt mit "de" überein — inhaltlich UND in der Form.

5) "de_in_text" steht WÖRTLICH im Zitat; "quote_ru" übersetzt genau dieses Zitat.

6) WIEDERVERWENDBARKEIT — hier ist die Antwort "drop", nicht "doubt":
     • Repliken aus der Sendung ("Privatversicherte verstehen den Joke");
     • erfundene Wortspiele des Moderators ("Niceinger Diceinger");
     • ENGLISCHE Wendungen, die nur zitiert werden ("Yes, Queen!"); fest eingedeutschte
       Anglizismen bleiben (der Shitstorm);
     • EINMALWITZE über Eigennamen ("Halle an der fucking Saale");
     • ganze SÄTZE mit Subjekt und konjugiertem Verb («Opfer fordern ihre Rechte») —
       das ist "doubt" mit dem Hinweis, welche Wendung herausgehört;
     • durch die Spracherkennung VERSTÜMMELTE Namen («Bafer» statt BAFA) — "doubt" mit
       dem richtigen Namen, wenn du sicher bist, sonst "drop";
     • MITTEN IM SATZ ABGESCHNITTENE Einheiten («die Koalition auffordern, die») — "doubt".
       ACHTUNG: «mir fällt etwas ein» (trennbare Vorsilbe), «es liegt nahe, dass»,
       «ohne Wenn und Aber» sind VOLLSTÄNDIG und in Ordnung;
     • NEUTRALE Alltagswörter, die jeder kennt ("Applaus") — "drop".

{register_rule}
WICHTIGSTE REGEL: Du meldest FEHLER, nicht Geschmack. Ist die Karte richtig, aber du
hättest es anders formuliert — "ok". Ein "doubt" muss einen konkreten, prüfbaren Fehler
nennen: falsche Schreibweise, falsche Grammatik, erfundener Kasus, Übersetzung passt
nicht zur Form, Zitat belegt die Einheit nicht.

Antworte NUR mit validem JSON:
{{"cards": [{{"i": <Index>, "verdict": "ok" | "doubt" | "drop",
             "field": "<Feldname, bei doubt>",
             "reason": "<kurz, auf Russisch: WAS falsch ist und wie es richtig wäre>"}}]}}"""

_REGISTER_RULE = """\
7) "register_ru" — Stilmarkierung («сленг», «разговорное», «грубое», «молодёжное»,
   «ироничное»). Derbe Sprache darf nicht als «разговорное» verharmlost werden — "doubt".
   Neutrale Alltagswörter gehören NICHT in diese Rubrik — "drop".
"""


def _ask_controller(cards: list, *, profile, call_json) -> list:
    """Один проход контролёра: только вердикты, без карточек. Субтитры не шлём — цитата
    к каждой карточке уже лежит в ней самой. Ошибки НЕ глушим: молча пропущенная проверка
    неотличима от пройденной."""
    system = _CONTROL_SYSTEM.format(
        register_rule=_REGISTER_RULE if getattr(profile, "requires_register", False) else ""
    )
    data = call_json(
        system,
        "Karten:\n" + json.dumps([dict(c, i=i) for i, c in enumerate(cards)], ensure_ascii=False),
        "контроль карточек",
        temperature=0,
    )
    verdicts = data.get("cards")
    if not isinstance(verdicts, list):
        raise ValueError("контролёр вернул ответ без списка карточек")
    # Вердикт обязан быть у КАЖДОЙ карточки. Пропущенная — не «в порядке», а
    # непроверенная; считать её проверенной значило бы соврать (проверяющий агент
    # 06.09.2026: пустой ответ давал «чисто» при нуле проверенных).
    got = {v.get("i") for v in verdicts if isinstance(v, dict)}
    missing = [i for i in range(len(cards)) if i not in got]
    if missing:
        raise ValueError(f"контролёр ответил не по всем карточкам: нет вердикта у {missing[:5]}")
    return verdicts


def _by_index(verdicts: list) -> dict:
    out = {}
    for v in verdicts:
        if isinstance(v, dict) and isinstance(v.get("i"), int):
            out[v["i"]] = v
    return out


def _remark(v: dict) -> str:
    field = str(v.get("field") or "").strip()
    reason = str(v.get("reason") or "").strip() or "—"
    return f"{field}: {reason}" if field else reason


def control_cards(cards: list, *, profile, transcript: str, call_json) -> tuple[list, dict]:
    """Контроль → ответ автора → повторный контроль исправленных. Возвращает (карточки, отчёт).

    Отчёт: checked / doubted / repaired / dropped / passes (1 или 2) / clean / reasons.
    `fixed` дублирует `repaired` — так его читает превью и недельный отчёт.
    """
    from backend.daily_video_pack import re_explain_card
    from backend.world_news_generator import _card_passes_source_guards, _quote_shows_the_unit

    # `calls` — сколько обращений к модели сделал контроль. По нему живёт обещание
    # «не больше 2 + число сомнений»: это считается, а не пишется константой.
    report = {"checked": len(cards), "doubted": 0, "repaired": 0, "fixed": 0, "dropped": 0,
              "passes": 1, "calls": 0, "clean": False, "reasons": []}

    def _ask(cards_in):
        report["calls"] += 1
        return _by_index(_ask_controller(cards_in, profile=profile, call_json=call_json))

    verdicts = _ask(cards)

    # Порядок карточек сохраняется: место каждой запоминается, исправленная встаёт на своё.
    kept: dict = {}               # позиция → карточка
    to_repair: list = []          # (позиция, карточка, замечание)
    for i, card in enumerate(cards):
        v = verdicts.get(i) or {}
        decision = str(v.get("verdict") or "ok").strip().lower()
        if decision == "drop":
            report["dropped"] += 1
            report["reasons"].append(f"выброшена «{card.get('de')}»: {_remark(v)}")
            continue
        if decision == "doubt":
            report["doubted"] += 1
            to_repair.append((i, card, _remark(v)))
            continue
        kept[i] = card

    def _ordered():
        return [kept[i] for i in sorted(kept)]

    if not to_repair:
        report["clean"] = report["dropped"] == 0
        logger.info("контроль[%s]: карточек %d, сомнений 0, выброшено %d",
                    getattr(profile, "key", "?"), len(cards), report["dropped"])
        return _ordered(), report

    # ── Автор отвечает на каждое замечание: одна единица, замечание, полные субтитры ──
    repaired: list = []           # (позиция, карточка «до», карточка «после», замечание)
    for pos, card, remark in to_repair:
        try:
            report["calls"] += 1
            answer = re_explain_card(card, remark, transcript=transcript, profile=profile,
                                     call_json=call_json)
        except Exception:
            # Автор не ответил — карточка НЕ идёт к людям как проверенная и не молчит.
            logger.exception("контроль: автор не ответил на замечание к %r", card.get("de"))
            report["dropped"] += 1
            report["reasons"].append(f"выброшена «{card.get('de')}»: {remark} (автор не ответил)")
            continue
        # Исправленная карточка не имеет права потерять помету формы или регистра.
        stripped = [name for name in ("form_ru", "register_ru")
                    if str(card.get(name) or "").strip() and not str(answer.get(name) or "").strip()]
        if stripped:
            report["dropped"] += 1
            what = "пометы регистра" if "register_ru" in stripped else "пометы формы"
            report["reasons"].append(f"выброшена «{card.get('de')}»: после правки нет {what}")
            continue
        # Та же планка, что у свежей карточки на приёме (проверяющий агент 06.09.2026:
        # общие стражи не включали двух проверок приёма — цитата показывает единицу и
        # перевод цитаты не пуст; исправленная карточка проходила ниже свежей).
        ok, why = _card_passes_source_guards(answer, transcript, profile=profile)
        if ok and not _quote_shows_the_unit(str(answer.get("de") or ""),
                                            str(answer.get("quote_de") or "")):
            ok, why = False, "цитата не показывает единицу"
        if ok and not str(answer.get("quote_ru") or "").strip():
            ok, why = False, "нет перевода цитаты"
        if not ok:
            report["dropped"] += 1
            report["reasons"].append(
                f"выброшена «{card.get('de')}»: правка не прошла сверку с субтитрами — {why}")
            continue
        repaired.append((pos, card, answer, remark))

    if not repaired:
        report["clean"] = False
        return _ordered(), report

    # ── Повторный контроль ТОЛЬКО исправленных ─────────────────────────────────────
    report["passes"] = 2
    try:
        second = _ask([a for _, _, a, _ in repaired])
    except Exception as exc:
        # Второй контроль не отработал. Решения первого прохода в силе, оплаченные
        # ответы автора не выбрасываются в никуда, но и к людям непроверенными не идут:
        # исправленные карточки выбывают, и об этом сказано в отчёте и превью.
        logger.exception("контроль: повторный проход не отработал")
        report["second_pass_failed"] = str(exc)[:200] or exc.__class__.__name__
        for _, before, _, remark in repaired:
            report["dropped"] += 1
            report["reasons"].append(
                f"выброшена «{before.get('de')}»: повторный контроль не отработал — {remark}")
        report["clean"] = False
        return _ordered(), report
    for j, (pos, before, after, remark) in enumerate(repaired):
        v = second.get(j) or {}
        decision = str(v.get("verdict") or "ok").strip().lower()
        if decision == "ok":
            report["repaired"] += 1
            report["reasons"].append(
                f"исправлена «{before.get('de')}» → «{after.get('de')}»: {remark}")
            kept[pos] = after
            continue
        # Сомнение второй раз — к ученику не идёт. Выпуск из-за неё не переделывается.
        report["dropped"] += 1
        report["reasons"].append(
            f"выброшена «{before.get('de')}»: сомнение осталось после правки — {_remark(v)}")
    report["fixed"] = report["repaired"]
    report["clean"] = report["dropped"] == 0
    logger.info("контроль[%s]: карточек %d, сомнений %d, исправлено %d, выброшено %d",
                getattr(profile, "key", "?"), len(cards), report["doubted"],
                report["repaired"], report["dropped"])
    return _ordered(), report
