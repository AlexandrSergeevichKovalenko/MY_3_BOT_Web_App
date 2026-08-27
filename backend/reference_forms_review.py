# -*- coding: utf-8 -*-
"""Слова, которым каскад не нашёл форм: разбор владельцем в личке, три раза в неделю.

Зачем. Каскад форм (`backend/german_reference_forms.py`) закрывает слово справочником,
разбором составного слова или моделью с двумя совпавшими ответами. Что не закрылось
ничем — НЕ исчезает и НЕ подставляется наугад: оно попадает сюда.

Владелец 17.08.2026: «отчёт мне обязательно в личку по количеству слов, и сами эти слова
нужно чтобы приходили с кнопками, чтобы я их мог самостоятельно на моё усмотрение
проработать». Периодичность — три раза в неделю (пн/ср/пт).

┌─ ПЕРЕДЕЛАНО 27.08.2026 ПО ЗАМЕЧАНИЮ ВЛАДЕЛЬЦА. НЕ ВОЗВРАЩАТЬ КАК БЫЛО. ───────────┐
│ «Что я должен принять? В чём задача? Где подсказка модели относительно того, что   │
│ она предлагает?» — на экране было слово, строка «форм нет нигде» и кнопки          │
│ der/die/das. Три вещи были не так:                                                 │
│                                                                                    │
│ 1. Спрашивали НЕ О ТОМ. Дыра — таблица форм, а кнопки собирали артикль. Даже       │
│    верное нажатие не создавало таблицу: карточка читает кэш склонений, а артикль   │
│    ложился в банк слов игры «спринт артиклей». Артикль собирает СВОЙ механизм —    │
│    `backend/article_review.py`, и он работает.                                     │
│ 2. Кнопка не могла записать НИКОГДА: `ON CONFLICT (word)` при отсутствии такого    │
│    ключа падал всегда, а слово при этом всё равно помечалось разобранным. Ответ    │
│    «слово осталось в очереди» был неправдой.                                       │
│ 3. Подсказки не было вовсе: два ответа модели, которые как раз и разошлись,        │
│    выбрасывались. Человека звали решать, не показав ему ничего.                    │
│                                                                                    │
│ Теперь карточка показывает перевод, чего именно нет, что предложила модель и чем   │
│ варианты отличаются, а кнопка кладёт выбранное В КЭШ ФОРМ, откуда его берёт        │
│ карточка слова. «Отложить» откладывает на срок, а не хоронит навсегда.             │
└────────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import logging
import os
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "reference_forms_review"
BATCH = max(1, int((os.getenv("REFERENCE_FORMS_REVIEW_BATCH") or "20").strip() or "20"))
_HTTP_TIMEOUT = 20

_KIND_RU = {"noun": "существительное", "adjective": "прилагательное", "adverb": "наречие"}
# Порядок полей в ответе модели и человеческие подписи к ним.
_CASES = (("nom_sg", "им."), ("gen_sg", "род."), ("dat_sg", "дат."), ("akk_sg", "вин."))
_CASES_PL = (("nom_pl", "им."), ("gen_pl", "род."), ("dat_pl", "дат."), ("akk_pl", "вин."))
_DEGREES = (("positive", "положительная"), ("comparative", "сравнительная"),
            ("superlative", "превосходная"))


def _reference_link(word: str, pos: str) -> str:
    from backend.german_reference_forms import _reference_title
    title = _reference_title(word, pos) or word
    return "https://de.wiktionary.org/wiki/" + urllib.parse.quote(title)


def _our_entry(word: str) -> dict[str, Any]:
    """Что записано У НАС: номер строки словаря, перевод, шапка и формы из разбора.

    Владелец решает зряче только тогда, когда видит ОБЕ стороны: что говорит справочник
    и что лежит у нас. У «Finster» именно наша сторона и оказалась выдумкой —
    «das Finster, мн. die Finster, род. des Finsters» при том, что такого
    существительного в немецком нет.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, card FROM bt_3_lex_units "
                    " WHERE lang = 'de' AND lower(lemma) = lower(%s) "
                    " ORDER BY updated_at DESC NULLS LAST LIMIT 1;",
                    (str(word or "").strip(),))
                row = cur.fetchone()
    except Exception:
        logging.debug("разбор форм: словарная запись не прочитана для %s", word, exc_info=True)
        return {}
    if not row:
        return {}
    card = row[1] if isinstance(row[1], dict) else {}
    forms = card.get("forms") if isinstance(card.get("forms"), dict) else {}
    return {"unit_id": int(row[0]),
            "перевод": str(card.get("word_ru") or "").strip(),
            "шапка": str(card.get("word_de") or "").strip(),
            "множественное": str(forms.get("plural") or "").strip(),
            "родительный": str(forms.get("genitive") or "").strip(),
            "сравнительная": str(forms.get("comparative") or "").strip()}


def _translation(word: str) -> str:
    """Перевод из словарной единицы. Пусто — значит пусто, ничего не выдумываем."""
    return str(_our_entry(word).get("перевод") or "")


def _variant_line(answer: dict, pos: str, number: int) -> str:
    """Один вариант модели человеческой строкой."""
    mark = "①②③④"[number - 1] if 1 <= number <= 4 else str(number)
    if pos == "noun":
        singular = " · ".join(str(answer.get(k) or "—") for k, _ in _CASES)
        plural_values = [str(answer.get(k) or "").strip() for k, _ in _CASES_PL]
        plural = " · ".join(v or "—" for v in plural_values) if any(plural_values) else "нет"
        return f"{mark} ед.: {singular}\n    мн.: {plural}"
    degrees = " · ".join(str(answer.get(k) or "—") for k, _ in _DEGREES)
    return f"{mark} {degrees}"


def _difference(answers: list[dict], pos: str) -> str:
    """Чем именно варианты расходятся. Человеку важно ровно это место, а не вся таблица."""
    if len(answers) < 2:
        return ""
    spots = []
    fields = _CASES if pos == "noun" else _DEGREES
    for key, label in fields:
        values = {str(a.get(key) or "").strip() for a in answers}
        if len(values) > 1:
            printed = " / ".join(v or "—" for v in sorted(values))
            spots.append(f"{label} {printed}")
    if pos == "noun":
        # Множественное число — ОДНОЙ строкой. Когда один вариант его вовсе не даёт, а
        # другой даёт, разница у всех четырёх падежей одна и та же, и печатать её
        # четыре раза значит утопить настоящее расхождение в шуме.
        plurals = {" · ".join(str(a.get(k) or "").strip() for k, _ in _CASES_PL)
                   for a in answers}
        if len(plurals) > 1:
            printed = " / ".join(p.strip(" ·") or "нет" for p in sorted(plurals))
            spots.append(f"мн. {printed}")
    return "Расходятся: " + "; ".join(spots) if spots else ""


def _keyboard(row_id: int, variants: int, *, можно_убрать: bool) -> dict[str, Any]:
    """callback_data: reffrm:<действие>:<номер строки>. Номер, а не слово: слово
    приходилось резать до 40 знаков, и длинное потом не находилось при нажатии.

    ⚠ «УБРАТЬ» ПОКАЗЫВАЕТСЯ НЕ ВСЕГДА. Если справочник подтверждает и написание, и
    часть речи, а не хватает только таблицы форм, — слово НАСТОЯЩЕЕ, и удалять его
    из-за нашей нехватки источника нельзя. Кнопка появляется только там, где
    справочник наше написание не подтверждает.
    """
    rows = []
    if variants >= 1:
        rows.append([{"text": f"{'①②③④'[i]} вариант {i + 1}",
                      "callback_data": f"reffrm:v{i + 1}:{row_id}"}
                     for i in range(min(variants, 4))])
    rows.append([{"text": "✏️ разобраться со словом",
                  "callback_data": f"reffrm:fix:{row_id}"}])
    if можно_убрать:
        rows.append([{"text": "🗑 убрать из словаря",
                      "callback_data": f"reffrm:drop:{row_id}"}])
    rows.append([{"text": "✅ оставить как есть", "callback_data": f"reffrm:keep:{row_id}"}])
    return {"inline_keyboard": rows}


# Причины, при которых наше написание справочником НЕ подтверждено. Только на них
# показывается «убрать из словаря».
_НАШЕ_НАПИСАНИЕ_НЕ_ПОДТВЕРЖДЕНО = {"нет_страницы", "только_фамилия",
                                    "другая_часть_речи", "чужая_таблица"}


def _word_text(item: dict, *, index: int, total: int, left: int,
               diagnosis: tuple[str, str] | None = None) -> str:
    from backend.german_reference_forms import _RECHECK_NEGATIVE_AFTER_DAYS

    word, pos = item["word"], item["pos"]
    kind = _KIND_RU.get(pos, pos)
    наше = _our_entry(word)
    head = f"<b>{word}</b> — {kind}"
    if наше.get("перевод"):
        head += f" · {наше['перевод']}"
    lines = [head]

    # ── Что записано у нас ──────────────────────────────────────────────────
    записано = [x for x in (наше.get("шапка") or word,
                            f"мн. {наше['множественное']}" if наше.get("множественное") else "",
                            f"род. {наше['родительный']}" if наше.get("родительный") else "",
                            f"сравн. {наше['сравнительная']}" if наше.get("сравнительная") else "")
                if x]
    lines.append("<b>У нас записано:</b> " + " · ".join(записано))

    # ── Что говорит справочник ──────────────────────────────────────────────
    код, фраза = diagnosis or ("", "")
    lines.append("<b>Справочник:</b> " + (фраза or "формы не напечатаны"))

    variants = [a for a in (item.get("candidates") or []) if isinstance(a, dict)]
    if variants:
        lines.append("")
        lines.append("Модель спрошена дважды и ответила по-разному:")
        lines.append("<code>" + "\n".join(
            _variant_line(a, pos, i + 1) for i, a in enumerate(variants)) + "</code>")
        разница = _difference(variants, pos)
        if разница:
            lines.append(f"<i>{разница}</i>")
        lines.append("Выберешь вариант — он ляжет в карточку слова, и человек увидит "
                     "именно эти формы.")
    else:
        lines.append("Модель тоже ничего не предложила.")

    lines.append("")
    lines.append("<b>Что делают кнопки:</b>")
    lines.append("✏️ <b>разобраться</b> — заведу жалобу на это слово. Ночью модель "
                 "разберёт карточку целиком и предложит исправление; ты решишь на "
                 "экране «было → станет», и правка разойдётся по всем местам.")
    if код in _НАШЕ_НАПИСАНИЕ_НЕ_ПОДТВЕРЖДЕНО:
        lines.append("🗑 <b>убрать из словаря</b> — слово и его запись в общем словаре "
                     "уходят, снимок сохраняется, вернуть можно.")
    lines.append(f"✅ <b>оставить как есть</b> — вопрос закрою, слово останется с тем, "
                 f"что у нас есть. Справочник переспрошу сам раз в "
                 f"{_RECHECK_NEGATIVE_AFTER_DAYS} дней — тебя больше не потревожу.")
    lines.append(f'<a href="{_reference_link(word, pos)}">статья в справочнике</a>')
    lines.append(f"<i>{index} из {total} · ждут ответа: {left}</i>")
    return "\n".join(lines)


def _head_text(left: int, sent: int) -> str:
    return ("📇 <b>Слова без форм — нужен твой ответ</b>\n"
            f"Не собралась таблица форм у {left} {_слов(left)}: справочник их не печатает, "
            "из составных они не выводятся, а модель либо молчит, либо дала два разных "
            "ответа.\n"
            "Ничего не подставлено. Ниже по карточке на слово: что известно, что "
            "предлагает модель и что сделает каждая кнопка.\n"
            f"Сейчас пришло: {sent}.")


def send_reference_forms_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция слов без форм — админам. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
        get_admin_telegram_ids,
    )
    from backend.german_reference_forms import (
        _reference_title, diagnose_source, fetch_sources_bulk, mark_asked,
        unresolved_batch, unresolved_count,
    )

    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"batch": BATCH},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}

    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
    if not token or not admin_ids:
        return {"ok": False, "error": "no_token_or_admins"}

    items = unresolved_batch(limit=BATCH)
    left = unresolved_count()
    if left < 0:
        # Счётчик вернул «не смог посчитать». Пустой список при этом НЕОТЛИЧИМ от
        # честного «очередь пуста», а разница огромна: во втором случае владельцу
        # рапортуют «всё в порядке 🎉», когда на деле мы просто не заглянули в базу.
        return {"ok": False, "error": "очередь не прочиталась"}
    if not items:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": 0, "reason": "nothing_to_review"})
        return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

    # ПРИЧИНУ СПРАШИВАЕМ ЗАНОВО ПЕРЕД ОТПРАВКОЙ, а не берём из ночной пометки:
    # владелец открывает страницу справочника ПРЯМО СЕЙЧАС и сверяет с нашей фразой.
    # Расхождение между тем, что он видит, и тем, что мы написали, — это враньё,
    # даже если ночью мы были правы.
    sources = fetch_sources_bulk([_reference_title(x["word"], x["pos"]) for x in items])
    diagnoses = {}
    for item in items:
        title = _reference_title(item["word"], item["pos"])
        text = None if sources is None else (sources.get(title) or sources.get(item["word"]) or "")
        diagnoses[item["id"]] = diagnose_source(item["pos"], text)

    sent = 0
    delivered_to = 0
    for uid in admin_ids:
        ok_here = False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": _head_text(left, len(items)),
                      "parse_mode": "HTML"}, timeout=_HTTP_TIMEOUT)
            ok_here = resp.status_code < 400
        except Exception:
            logging.warning("разбор форм: шапка не ушла uid=%s", uid, exc_info=True)
        for i, item in enumerate(items, 1):
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid,
                          "text": _word_text(item, index=i, total=len(items), left=left,
                                             diagnosis=diagnoses.get(item["id"])),
                          "parse_mode": "HTML",
                          "disable_web_page_preview": True,
                          "reply_markup": _keyboard(
                              item["id"], len(item.get("candidates") or []),
                              можно_убрать=(diagnoses.get(item["id"], ("", ""))[0]
                                            in _НАШЕ_НАПИСАНИЕ_НЕ_ПОДТВЕРЖДЕНО))},
                    timeout=_HTTP_TIMEOUT)
                if resp.status_code < 400:
                    sent += 1
                    ok_here = True
                else:
                    logging.warning("разбор форм: слово не ушло uid=%s word=%s: %s",
                                    uid, item["word"], resp.text[:200])
            except Exception:
                logging.warning("разбор форм: слово не ушло uid=%s word=%s", uid,
                                item["word"], exc_info=True)
        delivered_to += 1 if ok_here else 0

    if delivered_to:
        # Отмечаем ФАКТ отправки: неотвеченное слово вернётся само через REASK_DAYS,
        # а не будет приходить каждую отправку заново.
        mark_asked([int(x["id"]) for x in items])

    if not force:
        # «Выполнено» ставится по ФАКТУ доставки хотя бы одному админу, а не по факту
        # отправки. Иначе день, когда Telegram ответил ошибкой, выглядит успешным, и
        # порция не повторится.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if delivered_to else "failed",
            metadata={"sent": sent, "left": left, "admins": delivered_to})
    return {"ok": bool(delivered_to), "sent": sent, "left": left,
            "words": [x["word"] for x in items]}


def apply_reference_forms_review(action: str, row_id: str | int,
                                 admin_id: int | None = None) -> str:
    """Нажатие кнопки. Возвращает человеческий текст для замены сообщения.

    Ни одна ветка не помечает слово разобранным, если действие НЕ УДАЛОСЬ: прежний код
    делал ровно это, и слово исчезало из очереди с текстом «осталось в очереди».
    """
    from backend.german_reference_forms import (
        _RECHECK_NEGATIVE_AFTER_DAYS,
        apply_owner_choice,
        clear_unresolved,
        mark_reviewed,
        unresolved_row,
    )

    try:
        rid = int(str(row_id).strip())
    except (TypeError, ValueError):
        return "Не понял, о каком слове речь."

    row = unresolved_row(rid)
    if not row:
        return "Это слово уже закрыто — в очереди его нет."
    слово = row["word"]

    if action in ("v1", "v2", "v3", "v4"):
        applied = apply_owner_choice(rid, int(action[1:]))
        if not applied:
            return (f"⚠️ <b>{слово}</b>: не смог записать этот вариант. "
                    "Слово ОСТАЁТСЯ в очереди — придёт снова.")
        формы = "склонение" if row["pos"] == "noun" else "степени сравнения"
        return (f"✅ <b>{слово}</b> — {формы} записано с твоих слов. "
                "Теперь эти формы видит человек в карточке слова.")

    if action == "fix":
        # ОТДАЁМ В УЖЕ ПОСТРОЕННЫЙ РАЗБОР, а не строим второй редактор. Жалоба →
        # ночной судья с моделью → экран владельца «было → станет» → переименование
        # доводится до всех мест (лемма, ключ поиска, род, пул, кеш, карточки людей).
        from backend.card_complaints import add_complaint
        наше = _our_entry(слово)
        итог = add_complaint(
            user_id=int(admin_id or 0),
            word=слово,
            note=("Из разбора форм: справочник не дал таблицу форм. "
                  f"Причина: {row.get('reason') or 'не названа'}."),
            unit_id=наше.get("unit_id"))
        if not итог.get("ok"):
            return (f"⚠️ <b>{слово}</b>: не смог завести разбор "
                    f"({итог.get('reason') or 'причина в логах'}). Слово осталось в очереди.")
        mark_reviewed(rid, "отдано в разбор карточки по решению владельца")
        return (f"✏️ <b>{слово}</b> — отдал в разбор карточки.\n"
                "Ночью модель разберёт слово целиком и предложит исправление. "
                "Придёт отдельным экраном «было → станет», там и решишь.")

    if action == "drop":
        # Тем же путём, что в разборе слов: правило удаления владелец принял 23.08.2026
        # (слово и общий кеш уходят всегда, снимок всегда, личные карточки — только у
        # обрубков и опечаток). Второго правила удаления в проекте быть не должно.
        from backend.word_review import apply_word_review
        ответ = apply_word_review("drop", слово)
        if ответ.startswith("⚠️"):
            return ответ + "\nСлово осталось в очереди."
        clear_unresolved(слово)
        return ответ

    if action == "keep":
        if not mark_reviewed(rid, "решение владельца: оставить как есть"):
            return f"⚠️ <b>{слово}</b>: не смог закрыть вопрос. Слово остаётся в очереди."
        return (f"✅ <b>{слово}</b> — оставил как есть.\n"
                f"Больше не спрошу. Справочник переспрошу сам раз в "
                f"{_RECHECK_NEGATIVE_AFTER_DAYS} дней: появится статья — формы "
                f"подставятся без тебя.")

    return f"Не понял действие «{action}»."
