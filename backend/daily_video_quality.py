"""Мера качества карточек — она же заслон на входе.

Зачем этот модуль появился (владелец, 22.08.2026): «мне надоело вылавливать ошибки по
одной. Нужна рабочая система». Он прав, и разбор схемы показал три ошибки в моём подходе.

ОШИБКА ПЕРВАЯ. Я просил модель СОБЛЮДАТЬ двадцать требований в одном ответе, хотя
большинство из них проверяется механически, без всякого разбора грамматики: есть ли в
единице цифры, склеена ли она запятой, из закрытого ли списка помета, лежит ли форма из
текста внутри цитаты. Просьба к модели — это надежда. Проверка в коде — это гарантия.
Всё, что можно проверить, проверяется здесь, а модели остаётся то, где источника нет.

ОШИБКА ВТОРАЯ. Проверки были рассыпаны по генератору, судье и тестам, каждая со своим
счётчиком. Одно правило жило в трёх местах и в двух из них устаревало. Здесь они собраны
в ОДИН именованный список — добавить проверку теперь значит добавить строку в него.

ОШИБКА ТРЕТЬЯ. Качество мерили глаза владельца. Поэтому дефекты и находились по одному.
Тот же список работает мерой: прогнать по накопленным карточкам и получить число.

── Чего здесь НЕТ и почему ────────────────────────────────────────────────────
Здесь нет ни одной проверки, которая выводила бы немецкую грамматику своей арифметикой.
Род, возвратность, управление глагола, существует ли слово — на это есть справочники
(backend/article_authority.py, backend/german_word_gate.py), и спрашивать надо их, а не
догадываться регуляркой. Правило ноль: ответ берётся из источника.

Проверки ниже смотрят только на то, что видно в самой строке: цифры, запятые, длину,
принадлежность закрытому списку, вхождение одной строки в другую.
"""
from __future__ import annotations

import re

# Пометы формы — закрытый список. Всё остальное модель сочинила: «инфинитив с sich»,
# «Akkusativ», «Dativ Plural» — всё это приходило и всё это человеку читать нельзя.
ALLOWED_FORM_LABELS = {
    "словарная форма", "устойчивое выражение", "инфинитив",
    "именительный падеж", "винительный падеж", "дательный падеж",
    "родительный падеж", "множественное число", "повелительная форма",
}

# Единица не может кончаться служебным словом: значит фразу отрезали посреди предложения.
# Список закрытый, из служебных слов — разбора грамматики здесь нет.
_DANGLING_TAIL = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer",
    "und", "oder", "aber", "dass", "weil", "wenn", "als", "sondern", "damit",
}

# Сколько слов может быть в языковой единице. «wie bestellt und nicht abgeholt» — пять,
# и это законная идиома. Восемь и больше — это уже кусок предложения.
MAX_UNIT_WORDS = 7


def _words(text: str) -> list:
    return re.findall(r"[A-Za-zÄÖÜäöüß]+", str(text or ""))


def fingerprint(text: str) -> str:
    """Только буквы и цифры в нижнем регистре. Нужен, чтобы сверять вхождение строк, не
    спотыкаясь о знаки препинания, которые в субтитрах стоят иначе, чем их перепишет
    модель."""
    return re.sub(r"[^0-9a-zäöüß]+", "", str(text or "").lower())


# ── Проверки. Каждая возвращает причину или пустую строку ──────────────────────

def _has_digits(card, ctx):
    """Числа из ЭТОЙ новости — не языковая единица: «rund 300 Aussteller», «bis zu 30.000
    Besuchern». Завтра там другие числа, и человек это нигде не употребит."""
    if re.search(r"\d", str(card.get("de") or "")):
        return "в единице стоят цифры — это факт из новости, а не языковая единица"
    return ""


def _glued_by_comma(card, ctx):
    """Две единицы, склеенные запятой: «ein neuer Markt, der Graumarkt». Учить надо одну."""
    de = str(card.get("de") or "")
    if re.search(r",\s*(der|die|das|ein|eine)\s+\w", de, re.I):
        return "две единицы склеены запятой — на карточке должна быть одна"
    return ""


def _dangling_tail(card, ctx):
    """Единица, кончающаяся артиклем или союзом: «die Koalition auffordern, die» — фразу
    отрезали посреди предложения."""
    de = str(card.get("de") or "").strip()
    if de.endswith(","):
        return "единица кончается запятой — обрезана посреди фразы"
    words = _words(de)
    if words and words[-1].lower() in _DANGLING_TAIL:
        return "единица кончается служебным словом — обрезана посреди фразы"
    return ""


def _too_long(card, ctx):
    """Слишком длинная единица — почти наверняка кусок предложения, а не оборот."""
    n = len(_words(card.get("de")))
    if n > MAX_UNIT_WORDS:
        return f"единица из {n} слов — это кусок предложения, а не оборот"
    return ""


def _bad_form_label(card, ctx):
    """Помета формы вне закрытого списка — значит модель её сочинила."""
    form = str(card.get("form_ru") or "").strip().lower()
    if not form:
        return "нет пометы формы"
    if form not in ALLOWED_FORM_LABELS:
        return f"помета формы не из списка: «{card.get('form_ru')}»"
    return ""


def _quote_not_in_transcript(card, ctx):
    """Цитата обязана дословно найтись в субтитрах — иначе она выдумана."""
    quote = str(card.get("quote_de") or "").strip()
    if not quote:
        return "нет цитаты из ролика"
    if fingerprint(quote) not in fingerprint(ctx.get("transcript") or ""):
        return "цитаты нет в субтитрах ролика"
    return ""


def _text_form_not_in_quote(card, ctx):
    """Форма из текста обязана лежать внутри цитаты — иначе она тоже выдумана."""
    in_text = str(card.get("de_in_text") or "").strip()
    if not in_text:
        return "нет формы из текста"
    if fingerprint(in_text) not in fingerprint(card.get("quote_de")):
        return "формы из текста нет в цитате"
    return ""


def _neutral_in_standup(card, ctx):
    """Нейтральное бытовое слово в рубрике сленга — добор до количества."""
    if not ctx.get("requires_register"):
        return ""
    register = str(card.get("register_ru") or "").strip()
    if not register:
        return "нет пометы регистра"
    if register.lower().startswith("нейтральн"):
        return "нейтральное слово в рубрике живой речи"
    return ""


def _empty_fields(card, ctx):
    for field, label in (("de", "единицы"), ("translation_ru", "перевода"),
                         ("usage_ru", "подсказки об употреблении")):
        if not str(card.get(field) or "").strip():
            return f"нет {label}"
    return ""


# Порядок важен: сперва то, из-за чего карточка бессмысленна, потом частности.
CARD_CHECKS = (
    ("пустое поле", _empty_fields),
    ("цитата выдумана", _quote_not_in_transcript),
    ("форма из текста выдумана", _text_form_not_in_quote),
    ("цифры вместо единицы", _has_digits),
    ("склейка запятой", _glued_by_comma),
    ("обрезано посреди фразы", _dangling_tail),
    ("кусок предложения", _too_long),
    ("сочинённая помета формы", _bad_form_label),
    ("нейтральное слово", _neutral_in_standup),
)


def check_card(card: dict, *, transcript: str = "", requires_register: bool = False) -> list:
    """Все претензии к карточке. Пустой список — карточка чистая."""
    ctx = {"transcript": transcript, "requires_register": requires_register}
    found = []
    for name, check in CARD_CHECKS:
        why = check(card, ctx)
        if why:
            found.append((name, why))
    return found


def check_cards(cards: list, *, transcript: str = "", requires_register: bool = False) -> dict:
    """Мера по пачке карточек: сколько чистых, сколько с какой бедой.

    Возвращает и сами претензии по каждой карточке — чтобы владельцу можно было показать
    не только число, но и что именно не так.
    """
    clean, flagged, by_kind = 0, [], {}
    for card in cards or []:
        problems = check_card(card, transcript=transcript, requires_register=requires_register)
        if not problems:
            clean += 1
            continue
        flagged.append({"de": card.get("de"), "problems": problems})
        for name, _ in problems:
            by_kind[name] = by_kind.get(name, 0) + 1
    total = len(cards or [])
    return {
        "total": total,
        "clean": clean,
        "flagged": len(flagged),
        "share_clean": round(clean / total, 2) if total else 0.0,
        "by_kind": dict(sorted(by_kind.items(), key=lambda kv: -kv[1])),
        "cards": flagged,
    }


# ── Сверка со справочником: артикль берётся из источника, а не у модели ────────
#
# Правило ноль: ответ берётся из источника, модель только читает. Род существительного —
# ровно тот случай, где источник есть и давно построен: backend/article_authority.py
# (собственный справочник + Wiktionary). Спрашивать род у модели, когда есть справочник,
# значит сознательно предпочесть догадку знанию.
#
# Почему сюда можно ходить в сеть, а на сохранение слова — нельзя: рубрика готовится
# фоновой работой раз в день и никого не заставляет ждать. Живой путь сохранения такого
# позволить не может, и там справочник спрашивают только из прогретой памяти.

_ARTICLES = ("der", "die", "das")


def split_article(de: str) -> tuple:
    """Разделить «die Kommentarspalte» на («die», «Kommentarspalte»).

    Возвращает (None, None), если это не одиночное существительное с артиклем: у оборотов
    и глаголов рода нет, и сверять там нечего.
    """
    parts = str(de or "").strip().split()
    if len(parts) != 2:
        return (None, None)
    article, word = parts[0].lower(), parts[1]
    # Заглавная буква НЕ требуется: артикль уже сказал, что это существительное, а
    # неверный регистр — как раз то, что мы пришли чинить. Требовать заглавную здесь
    # значило бы исключить ровно те карточки, ради которых сверка и делается
    # (поймано на «die kommentarspalte» 22.08.2026).
    if article not in _ARTICLES or not re.fullmatch(r"[A-Za-zÄÖÜäöüß-]{2,}", word):
        return (None, None)
    return (article, word)


def article_from_reference(de: str, *, allow_network: bool = False) -> tuple:
    """Что о роде этого слова говорит справочник.

    Возвращает (артикль, откуда) или (None, причина). None означает «справочник не знает» —
    и это НЕ повод что-то подставлять: неизвестность честнее догадки.
    """
    article, word = split_article(de)
    if not word:
        return (None, "не одиночное существительное — рода нет")
    try:
        from backend.article_authority import authoritative_article
        found, source = authoritative_article(word, allow_network=allow_network)
    except Exception:
        return (None, "справочник недоступен")
    if not found:
        return (None, f"справочник не знает слова «{word}»")
    return (found.lower(), source)


def article_disagrees_with_reference(card: dict, *, allow_network: bool = False) -> str:
    """Расходится ли артикль на карточке со справочником.

    Пустая строка — расхождения нет ИЛИ справочник промолчал. Молчание справочника не
    делает карточку виноватой: мы просто не знаем, и придумывать не станем.
    """
    ours, word = split_article(card.get("de"))
    if not ours:
        return ""
    theirs, source = article_from_reference(card.get("de"), allow_network=allow_network)
    if not theirs or theirs == ours:
        return ""
    return (f"артикль расходится со справочником: у нас «{ours} {word}», "
            f"источник даёт «{theirs}» ({source})")


def correct_article_from_reference(card: dict, *, allow_network: bool = False) -> tuple:
    """Поправить артикль по справочнику. Возвращает (карточка, что сделали).

    Это НЕ выдумывание: новый артикль берётся из источника, названного по имени. Если
    источник молчит — карточка возвращается нетронутой. Молчание не повод угадывать.
    """
    ours, word = split_article(card.get("de"))
    if not ours:
        return (card, "")
    theirs, source = article_from_reference(card.get("de"), allow_network=allow_network)
    if not theirs or theirs == ours:
        return (card, "")
    fixed = dict(card)
    fixed["de"] = f"{theirs} {word}"
    return (fixed, f"артикль исправлен по справочнику ({source}): «{ours}» → «{theirs}»")


# ── Существование и написание слова — тоже из справочника ─────────────────────
#
# Второй незакрытый резерв достоверности. `backend/german_word_gate.py` умеет две вещи,
# которые я до сих пор просил у модели:
#   • существует ли такое немецкое слово вообще (ходит в DWDS и второй справочник);
#   • как оно пишется — включая заглавную букву у существительных.
#
# Именно этим закрывается дефект «herzinfarkt bekommen» со строчной буквы, который
# владелец увидел 22.08.2026: справочник поднимает заглавную сам, и просить об этом
# модель больше не нужно. Просьба — надежда, справочник — знание.
#
# Модель здесь НЕ спрашивается (allow_model=False): нам нужен источник, а не вторая
# догадка поверх первой.

def _single_word_of(de: str) -> str:
    """Одно слово из заголовка — с артиклем или без. Для оборотов возвращает пустую
    строку: дверь слова разбирает ОДНО слово, многословное до неё не доходит."""
    text = re.sub(r"\(.*?\)", " ", str(de or "")).strip()
    parts = text.split()
    if len(parts) == 2 and parts[0].lower() in _ARTICLES:
        parts = parts[1:]
    if len(parts) != 1:
        return ""
    word = parts[0].strip(".,!?»«\"'")
    return word if re.fullmatch(r"[A-Za-zÄÖÜäöüß-]{2,}", word) else ""


def spelling_from_reference(de: str, *, allow_network: bool = False) -> tuple:
    """Что справочник говорит о написании и существовании слова.

    Возвращает (статус, исправленное_написание). Статус «неизвестно» — справочник не
    ответил; это НЕ повод ни выбрасывать карточку, ни что-то подставлять.
    """
    word = _single_word_of(de)
    if not word:
        return ("не одиночное слово", "")
    try:
        from backend.german_word_gate import CONFIRMED, NOT_A_WORD, REPAIRED, check_word
        verdict = check_word(word, allow_network=allow_network, allow_model=False)
    except Exception:
        return ("справочник недоступен", "")
    status = str(verdict.get("status") or "")
    fixed = str(verdict.get("text") or "").strip()
    if status == NOT_A_WORD:
        return ("не слово", "")
    if status == REPAIRED and fixed and fixed != word:
        return ("исправлено", fixed)
    if status == CONFIRMED:
        return ("подтверждено", fixed or word)
    return ("не подтверждено", "")


def correct_spelling_from_reference(card: dict, *, allow_network: bool = False) -> tuple:
    """Поправить написание заголовка по справочнику. Возвращает (карточка, что сделали).

    Правится ТОЛЬКО само слово, артикль остаётся на месте — им занимается сверка рода.
    Слово, которого справочник не знает, не трогается: у стендапа это сплошь сленг, и
    решение владельца 22.08.2026 — верить карточке, а не справочнику, когда речь живая.
    """
    # ТОЛЬКО существительное с артиклем. Проверка 22.08.2026 показала, почему это важно:
    # на «heulen» справочник отвечает «Heulen» с заглавной — и он прав со своей стороны,
    # «das Heulen» существует как отглагольное существительное. Но на карточке «heulen» —
    # это ГЛАГОЛ «рыдать», и подставив заглавную, мы своими руками сделали бы верную
    # карточку неверной.
    #
    # Справочник отвечает на вопрос «как пишется это слово в роли, которую я для него
    # выбрал», а нам нужна роль, в которой слово стоит на карточке. Применять ответ
    # источника к другому вопросу — то же выдумывание, только чужими руками.
    #
    # Артикль на карточке роль называет однозначно: это существительное. Без артикля роль
    # неизвестна, и написание не трогаем.
    article, _noun = split_article(card.get("de"))
    if not article:
        return (card, "")
    word = _single_word_of(card.get("de"))
    if not word:
        return (card, "")
    status, fixed = spelling_from_reference(card.get("de"), allow_network=allow_network)
    if status != "исправлено" or not fixed:
        return (card, "")
    updated = dict(card)
    updated["de"] = str(card.get("de") or "").replace(word, fixed, 1)
    return (updated, f"написание исправлено по справочнику: «{word}» → «{fixed}»")


def word_not_german(card: dict, *, allow_network: bool = False) -> str:
    """Справочник прямо говорит, что такого слова нет. Тогда карточке не место на экране.

    «Не подтверждено» сюда НЕ попадает: справочники плохо знают сленг, а стендап — это
    сплошь сленг. Выбрасываем только то, что источник назвал не словом.
    """
    # Живую речь справочники знают плохо, а стендап — это сплошь сленг. Решение владельца
    # 22.08.2026: карточке со сленговой пометой верим больше, чем справочнику. Поэтому
    # помеченную живую речь на существование не проверяем вовсе.
    register = str(card.get("register_ru") or "").strip().lower()
    if register and not register.startswith("нейтральн"):
        return ""
    status, _ = spelling_from_reference(card.get("de"), allow_network=allow_network)
    if status == "не слово":
        return f"справочник не знает такого слова: «{card.get('de')}»"
    return ""
