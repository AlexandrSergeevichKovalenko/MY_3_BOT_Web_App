# -*- coding: utf-8 -*-
"""«Подставь синоним» — сборка задания с пропуском из того, что УЖЕ лежит в банке.

Стратегия целиком: docs/tasks/synonym_gap_wednesday_strategy.md (решения владельца
13.09.2026). Здесь — только сборка и проверка ответа, без базы и без сети, чтобы
правило проверялось тестами, а не живыми данными.

ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ
────────────────────────
Замер 13.09.2026 на живой базе: до спринта у человека ТРИ касания со словом, и два из
них — одна и та же игра «выбери верный из пяти» (понедельник 11:00 и вторник 13:00).
В четверг сразу спринт: назови всё из головы за 60 секунд. Между «выбрал из пяти» и
«вспомни с нуля» ступеньки нет, и человек прыгает через неё. По синонимам за 30 дней
названо 6 слов, упущено 54.

Среда — эта ступенька: предложение с пропуском и подсказкой «первая буква + длина».

ИСТОЧНИК ИСТИНЫ НАЗЫВАЕТСЯ ВСЛУХ
────────────────────────────────
`bt_3_sprint_bank.trainer_json.correct_examples[]` — массив {word, sentence_de,
sentence_ru, nuance}, собранный дверью приёма синонимов. Плюс `accepted` — список
синонимов с русским переводом.

К МОДЕЛИ ЗДЕСЬ НЕ ХОДЯТ НИ РАЗУ. Верный ответ мы не ВЫВОДИМ, а НАХОДИМ в готовом
предложении: немецкую форму нельзя получить арифметикой из другой формы (правило ноль),
поэтому единственный законный способ — взять ту, которая уже написана.

ЧТО ДЕЛАЕМ, КОГДА ИСТОЧНИК НЕ ЗНАЕТ
───────────────────────────────────
Не нашли форму в предложении или страж не собрал предложение обратно — заготовка
НЕ строится, и случай СЧИТАЕТСЯ по классу (`skipped`). Пустой пропуск человеку не
показывается, словарная форма вместо склонённой не подставляется. Счётчики уходят
владельцу строкой в утренний отчёт: «не смогли» — это наряд на работу для двери
приёма, а не разрешение молчать.

Замер 13.09.2026 по всему банку (67 слов, NOT retired AND trainer_ready):
    строится      409 из 437   (379 точных + 30 склонённых)
    не строится    28 из 437   (21 «слова в предложении нет», 7 «форма разъехалась»)
    64 слова из 67 дают ≥3 заготовки; медиана 5,5; максимум 15.
"""

from __future__ import annotations

import re

# Немецкие окончания, с которыми слово из списка может стоять в предложении. Это НЕ
# правило словообразования и ничего не выводит: список нужен только чтобы УЗНАТЬ уже
# написанную форму. Что именно попадёт в задание, решает страж gap_reconstructs_sentence.
_FORM_ENDINGS = ("e", "er", "es", "en", "em", "et", "te", "t", "st", "s")

_LETTER = r"A-Za-zÄÖÜäöüß"

# Артикль в заголовке синонима («die Hilfe») — часть словарной подписи, а не слова,
# которое стоит в предложении. Ищем по последнему токену.
_ARTICLES = {"der", "die", "das", "den", "dem", "des", "ein", "eine", "einen",
             "einem", "einer", "eines", "sich"}

GAP_TOKEN = "___"


def _core(word: str) -> str:
    """Слово без словарной обвески: «die Hilfe» → «Hilfe», «sich bemühen» → «bemühen».
    Многословные выражения («sich Mühe geben») ужимаются до последнего токена — в
    предложении ищется именно он, а страж потом проверит, что предложение собралось."""
    toks = [t for t in re.split(r"\s+", str(word or "").strip()) if t]
    while len(toks) > 1 and toks[0].lower() in _ARTICLES:
        toks = toks[1:]
    return toks[-1] if toks else ""


def find_form_span(word: str, sentence: str, forms: set | None = None) -> tuple[int, int] | None:
    """ГДЕ в `sentence` стоит форма слова `word`: (начало, конец). None — её там нет.

    Сначала точное вхождение, затем вхождение с немецким окончанием. Ничего не
    достраиваем: указываем ровно на ту подстроку, которая уже стоит в предложении.

    ┌─ ПРОВЕРЕНО 13.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ─────────────────┐
    │ Возвращаем ПОЗИЦИЮ, а не строку, и это не украшательство. Первая версия     │
    │ отдавала подстроку, а вызывающий вырезал её через `sent.replace(f, '___',1)`│
    │ — и заменял ПЕРВОЕ вхождение по тексту, а не то, которое нашла регулярка.   │
    │ Прогон по банку 13.09.2026 поймал живой случай: у антонима «vermeiden»      │
    │ синоним «suchen» стоит в «Konflikte zu suchen», а replace вырезал «suchen»  │
    │ из середины слова «versuchen» → «Wir sollten ver___, Konflikte zu suchen».  │
    │ Это ушло бы человеку как задание. Резать можно ТОЛЬКО по span.              │
    │ Перемерить: scripts/relation_gap_audit.py — класс «страж не собрал».        │
    └────────────────────────────────────────────────────────────────────────────┘
    """
    core = _core(word)
    sent = str(sentence or "")
    if not core or not sent:
        return None
    exact = re.search(rf"(?<![{_LETTER}]){re.escape(core)}(?![{_LETTER}])", sent)
    if exact:
        return exact.span()
    tail = "|".join(_FORM_ENDINGS)
    inflected = re.search(rf"(?<![{_LETTER}]){re.escape(core)}(?:{tail})(?![{_LETTER}])", sent)
    if inflected:
        return inflected.span()
    # ┌─ ТРЕТИЙ ЗАХОД: СПРАВОЧНИК ФОРМ. Разбор 14.09.2026. ───────────────────────────┐
    # │ Два первых правила приписывают окончание к ПОЛНОМУ слову, а немецкий глагол   │
    # │ спрягается ЗАМЕНОЙ «-en»: registrieren → registrierte, entdecken → entdeckte, │
    # │ erkennen → erkannte, sehen → sah. Поэтому любой пример в претерите пролетал   │
    # │ мимо, и три слова банка (bemerken, verwirren, versäumen) не давали НИ ОДНОЙ   │
    # │ заготовки — у них все синонимы глаголы. Владелец упёрся в это первым же       │
    # │ /gap_test 14.09.2026: «У слова bemerken не собралось ни одной заготовки».     │
    # │                                                                              │
    # │ Форму мы по-прежнему НЕ ВЫВОДИМ. `forms` — то, что НАПЕЧАТАНО в справочнике   │
    # │ спряжений (bt_3_german_verb_paradigms, страницы Flexion: de.wiktionary,       │
    # │ 1808 глаголов на 14.09.2026), и подаётся вызывающим. Нет справочника — нет    │
    # │ третьего захода, заготовка просто не строится и считается.                    │
    # │ Формы перебираем от ДЛИННОЙ к короткой: у «wahrnehmen» напечатаны и «nahm»,   │
    # │ и «nahm wahr» — вырезать надо целое, а не его кусок.                          │
    # └──────────────────────────────────────────────────────────────────────────────┘
    known = {str(f or "").strip() for f in (forms or ()) if str(f or "").strip()}
    # ОТДЕЛЯЕМЫЙ ГЛАГОЛ: в справочнике у него напечатана форма из двух слов («nahm wahr»
    # у wahrnehmen), а в живом предложении она РАЗОРВАНА — «nahm sie einen Fehler wahr».
    # Голое «nahm» искать нельзя: пропуск встанет на обрубок, страж соберёт предложение
    # обратно (мы же вырезали ровно его) и пропустит задание с ответом «nahm» вместо
    # слова. Поэтому первые слова многословных форм из поиска ИСКЛЮЧАЮТСЯ: такое слово
    # требует двух пропусков, а это отдельная задача (см. §10 стратегии).
    fragments = {f.split()[0] for f in known if " " in f}
    for form in sorted(known, key=len, reverse=True):
        if " " not in form and form in fragments:
            continue
        hit = re.search(rf"(?<![{_LETTER}]){re.escape(form)}(?![{_LETTER}])", sent)
        if hit:
            return hit.span()
    return None


def find_form_in_sentence(word: str, sentence: str, forms: set | None = None) -> str | None:
    """Сама форма (для тестов и отчётов). Резать предложение по НЕЙ нельзя — см.
    коробку в `find_form_span`; для резки берут span."""
    span = find_form_span(word, sentence, forms)
    return str(sentence or "")[span[0]:span[1]] if span else None


def _reconstructs(gapped: str, filler: str, full: str) -> bool:
    """Страж из продукта, а не своя копия: подставь ответ обратно — обязано выйти
    исходное предложение слово в слово (`backend/backend_server.py:18596`, написан
    14.08.2026 на отделяемых глаголах). Импорт ленивый: backend_server импортирует
    answer_eval, а тот — этот модуль."""
    from backend.backend_server import gap_reconstructs_sentence
    return bool(gap_reconstructs_sentence(gapped, [filler], full))


def build_gap_item(*, synonym: str, synonym_ru: str, sentence_de: str,
                   sentence_ru: str = "", nuance: str = "",
                   forms: set | None = None) -> tuple[dict | None, str]:
    """Одна заготовка. Возвращает (заготовка, причина-отказа).

    Заготовка строится, только если форма слова НАЙДЕНА в его собственном предложении
    И страж собрал предложение обратно. Иначе (None, класс отказа) — и вызывающий
    обязан этот класс посчитать, а не проглотить."""
    sent = str(sentence_de or "").strip()
    if not sent:
        return None, "нет предложения"
    span = find_form_span(synonym, sent, forms)
    if not span:
        # Отделяемый глагол («aufklären» → «klärten … auf») стоит в предложении в ДВУХ
        # местах — одним пропуском его не вырезать. Такие сюда и попадают.
        return None, "слова в предложении нет"
    filler = sent[span[0]:span[1]]
    gapped = sent[:span[0]] + GAP_TOKEN + sent[span[1]:]
    if not _reconstructs(gapped, filler, sent):
        return None, "страж не собрал предложение"
    return {
        "synonym": str(synonym or "").strip(),      # словарная форма (как в списке)
        "synonym_ru": str(synonym_ru or "").strip(),
        "filler": filler,                           # верный ответ — форма из предложения
        "sentence_gapped": gapped,
        "sentence_de": sent,
        "sentence_ru": str(sentence_ru or "").strip(),
        "nuance": str(nuance or "").strip(),
        "hint_letter": filler[:1],
        "hint_len": len(filler),
    }, ""


def build_gap_items(*, wort: str, accepted: list | None,
                    trainer_json: dict | None, forms_of=None) -> tuple[list, dict]:
    """Все заготовки одного слова банка + счётчики отказов по классам.

    Сколько у слова примеров — столько и заготовок (решение владельца 13.09.2026:
    цифру не выдумывать, иначе режем живой материал). Порядок — как в источнике."""
    tj = trainer_json or {}
    ru_by_de = {}
    for pair in (accepted or []):
        if isinstance(pair, dict):
            de = str(pair.get("de") or "").strip()
            if de:
                ru_by_de[de.lower()] = str(pair.get("ru") or "").strip()
    anchor_core = _core(wort).lower()
    items: list = []
    skipped: dict = {}
    for ex in (tj.get("correct_examples") or []):
        if not isinstance(ex, dict):
            continue
        syn = str(ex.get("word") or "").strip()
        if not syn:
            continue
        # Само опорное слово в свои же синонимы не попадает — то же правило, что уже
        # стоит в тренировке (`_not_self`, backend/answer_eval.py:2614).
        if _core(syn).lower() == anchor_core:
            skipped["слово само себе синоним"] = skipped.get("слово само себе синоним", 0) + 1
            continue
        # Формы даёт вызывающий (в проде — справочник спряжений). Модуль остаётся
        # чистым: без справочника работает как раньше, просто строит меньше.
        forms = None
        if callable(forms_of):
            try:
                forms = forms_of(syn)
            except Exception:                      # источник недоступен — не догадываемся
                forms = None
        item, why = build_gap_item(
            synonym=syn, synonym_ru=ru_by_de.get(syn.lower(), ""),
            sentence_de=str(ex.get("sentence_de") or ""),
            sentence_ru=str(ex.get("sentence_ru") or ""),
            nuance=str(ex.get("nuance") or ""),
            forms=forms,
        )
        if item is None:
            skipped[why] = skipped.get(why, 0) + 1
            continue
        item["index"] = len(items)
        items.append(item)
    return items, skipped


# ── Проверка ответа: четыре исхода, каждый из источника ───────────────────────
# Решение владельца 13.09.2026: неверная форма НЕ засчитывается («eine ausführlich
# Beschreibung» — неверный немецкий, галочкой он помечен не будет), но раунд на этом
# не закрывается: человеку говорят, ЧТО не так, и дают дописать. Вторая попытка —
# последняя.
CORRECT = "correct"            # написал форму из предложения
WRONG_FORM = "wrong_form"      # слово то, форма словарная → объясняем и даём переписать
OTHER_SYNONYM = "other_synonym"  # другой синоним этого же слова → «здесь ждём на "a"»
WRONG = "wrong"                # не то слово


def grade_gap_answer(*, item: dict, answer: str, all_synonyms: list | None = None) -> dict:
    """Вердикт по одному пропуску. Сверка — через продуктовый `check_quiz_freeform_
    deterministic`, он уже прощает ß↔ss, ä↔ae, регистр и дефисы (решение дома,
    backend/answer_eval.py:74). Модель не зовётся.

    ЧЕГО МЫ НЕ УМЕЕМ И НЕ ПРИТВОРЯЕМСЯ: ввод «ausführlichen» — не форма из предложения
    и не словарная — уйдёт в WRONG. Сказать «это форма того же слова» можно только по
    справочнику форм; выводить окончание своей арифметикой запрещено. Такие вводы
    считаются отдельно (`unrecognised`), чтобы владелец увидел числом, надо ли
    доставать справочник."""
    from backend.answer_eval import check_quiz_freeform_deterministic
    text = str(answer or "").strip()
    if not text:
        return {"outcome": WRONG, "unrecognised": False}
    if check_quiz_freeform_deterministic(user_text=text, correct_text=item.get("filler") or ""):
        return {"outcome": CORRECT, "unrecognised": False}
    if check_quiz_freeform_deterministic(user_text=text, correct_text=item.get("synonym") or ""):
        return {"outcome": WRONG_FORM, "unrecognised": False}
    for other in (all_synonyms or []):
        other_de = str((other or {}).get("de") if isinstance(other, dict) else other or "").strip()
        if not other_de or other_de.lower() == str(item.get("synonym") or "").lower():
            continue
        if check_quiz_freeform_deterministic(user_text=text, correct_text=other_de):
            return {"outcome": OTHER_SYNONYM, "unrecognised": False, "matched": other_de}
    return {"outcome": WRONG, "unrecognised": True}
