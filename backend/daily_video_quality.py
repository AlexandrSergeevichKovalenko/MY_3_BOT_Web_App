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
