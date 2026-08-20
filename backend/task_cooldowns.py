# -*- coding: utf-8 -*-
"""Сколько дней задание «отдыхает» после показа — ОДНО место на весь проект.

Зачем этот файл существует
──────────────────────────
Отдых — это про ОБЩИЙ склад: показали карточку кому угодно, и она столько дней не
предлагается никому. Он не имеет отношения к личной памяти человека (её ведёт
`backend/task_rotation.py`: решил верно — вернём через 90 дней, второй раз — через 120,
третий — никогда). Путать их нельзя, и до 19.08.2026 их путали.

До 19.08.2026 сроки лежали в `bot_3.py` рядом с расписанием, и никто, кроме выдачи, их
не видел. Из-за этого отчёт владельцу считал запас по банку целиком, а выдача брала
только то, что вышло из отдыха: 61 готовый кроссворд при семи реально свободных, и
разницу не показывал никто. Теперь сроки берут отсюда и выдача, и отчёт.

Значения задаются переменными окружения, нижние границы — защита от опечатки в
переменной, а не «умный дефолт».
"""

import os


def _days(env_name: str, default: int, floor: int) -> int:
    return max(floor, int((os.getenv(env_name) or str(default)).strip() or str(default)))


REBUS_COOLDOWN_DAYS = _days("REBUS_COOLDOWN_DAYS", 15, 7)
ARTICLE_QUIZ_COOLDOWN_DAYS = _days("ARTICLE_QUIZ_COOLDOWN_DAYS", 14, 7)
ANAGRAM_COOLDOWN_DAYS = _days("ANAGRAM_COOLDOWN_DAYS", 10, 1)
AUFGABE_SEND_COOLDOWN_DAYS = _days("AUFGABE_SEND_COOLDOWN_DAYS", 15, 1)
LISTENING_COOLDOWN_DAYS = _days("LISTENING_COOLDOWN_DAYS", 7, 5)
CROSSWORD_COOLDOWN_DAYS = _days("CROSSWORD_COOLDOWN_DAYS", 14, 7)

# Вид задания (как он назван в замере запаса) → его срок отдыха.
COOLDOWN_DAYS_BY_KIND = {
    "rb": REBUS_COOLDOWN_DAYS,
    "cw": CROSSWORD_COOLDOWN_DAYS,
    "ag": ANAGRAM_COOLDOWN_DAYS,
    "au": AUFGABE_SEND_COOLDOWN_DAYS,
    "article_quiz": ARTICLE_QUIZ_COOLDOWN_DAYS,
    "ls": LISTENING_COOLDOWN_DAYS,
}
