# -*- coding: utf-8 -*-
"""Разрезание склеенного перевода на отдельные значения.

Один и тот же разрез нужен в трёх местах: в разовом проходе по слою
(scripts/dict_units_split_senses.py), в кнопке «разбить карточку на значения» и на
фронте при показе. Поэтому правило живёт здесь, а не копируется: иначе словарь,
тренировка и разовые проходы начнут расходиться в мелочах.

Правила:
  • режем по номерам значений («1прикладывать … 2 надевать … 3 строить») и по точке
    с запятой;
  • ЗАПЯТУЮ НЕ ТРОГАЕМ: «строить, закладывать, сооружать» — оттенки одного значения,
    дробить их на отдельные карточки мельче, чем нужно человеку;
  • пометки из скобок («перен.», «разг.», «куда-либо») и словарные сокращения в начале
    («vi причаливать») выносим в отдельное поле — это не часть перевода;
  • чисто грамматические записи («приюта; хостела (Genitiv/Dativ)») переводом не
    считаем.
"""
from __future__ import annotations

import re

SPACE_RE = re.compile(r"\s+")
SENSE_NUM_RE = re.compile(r"(?:^|[\s;,])(\d{1,2})\s*[).]?\s*(?=[А-Яа-яЁёA-Za-z])")
PAREN_RE = re.compile(r"\s*\(([^)]{1,60})\)\s*")
MARKER_RE = re.compile(r"^((?:v[itrp]|vimp|refl|adj|adv|разг|перен|уст)\.?)\s+", re.I)
GRAMMAR_NOTE_RE = re.compile(
    r"\b(genitiv|dativ|akkusativ|nominativ|plural|singular|мн\.?\s*ч|ед\.?\s*ч)\b", re.I)


def _clean(value) -> str:
    return SPACE_RE.sub(" ", str(value or "").strip())


def split_translation(text: str) -> list[dict]:
    """Строка перевода → [{"value": …, "label": …}]. Если резать нечего — один элемент."""
    raw = _clean(text)
    if not raw:
        return []

    marks = [(m.start(1), m.end(0)) for m in SENSE_NUM_RE.finditer(raw)]
    if len(marks) >= 2:
        chunks = [
            raw[body_start:(marks[index + 1][0] if index + 1 < len(marks) else len(raw))]
            for index, (_num_start, body_start) in enumerate(marks)
        ]
        head = raw[:marks[0][0]].strip(" ;,")
        if head:
            chunks.insert(0, head)
    else:
        chunks = [raw]

    out: list[dict] = []
    seen: set[str] = set()
    for chunk in chunks:
        for piece in re.split(r"\s*;\s*", chunk):
            labels: list[str] = []

            def _grab(match):
                labels.append(match.group(1).strip())
                return " "

            value = _clean(PAREN_RE.sub(_grab, piece)).strip(" ,;.—-")
            marker = MARKER_RE.match(value)
            if marker:
                labels.append(marker.group(1))
                value = value[marker.end():].strip(" ,;.—-")
            value = re.sub(r"\s+([,;])", r"\1", value)
            if len(value) < 2 or not re.search(r"[А-Яа-яЁёA-Za-z]", value):
                continue
            if GRAMMAR_NOTE_RE.search(value):
                continue
            key = value.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "value": value,
                "label": "; ".join(l for l in labels if l and not GRAMMAR_NOTE_RE.search(l)),
            })
    return out or [{"value": raw, "label": ""}]


def can_split(text: str) -> bool:
    """Есть ли смысл предлагать человеку разбить эту карточку."""
    return len(split_translation(text)) > 1
