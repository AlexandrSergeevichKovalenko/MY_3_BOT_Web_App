# -*- coding: utf-8 -*-
"""ЛОЖНАЯ ПРЕТЕНЗИЯ СУДЬИ — СНИМАЕТСЯ ПО СЛОВАРЮ, А НЕ ПО ПАМЯТИ АГЕНТА.

ПОВОД. Владелец 16.09.2026: «если мы знаем что их нельзя нажимать — таких убери
оттуда! Ты что, надеешься на мою память?» Речь о карточках экрана «Спорные фразы»,
где судья перевода заявил претензию, а перевод человека был ВЕРЕН. Разбор всех 46
открытых вопросов о переводе в тот день: 24 претензии справедливы, 12 ложны, 8 —
придирки к стилю (промпт судье это прямо запрещает), 2 не проверены.

ЧЕМ ЭТО ОПАСНО. У ложной претензии на экране стоит кнопка «Записать: «…»» — одно
касание, и в ОБЩИЙ словарь уходит неверный перевод. Четыре кнопки из восьми писали
прямую неправду: Police → «полиция» (это Versicherungsschein, полиция — Polizei),
«Holz vor der Hütte haben» → «быть осторожным» (идиома о большой груди), Kunde →
«клиента» (в «Kunde aus der Hauptstadt» без артикля это die Kunde, весть), «über den
Teller essen» → «ешьте через тарелку» (бессмыслица).

ПОЧЕМУ СПИСОК ЗАПИСАН РУКАМИ, А НЕ ВЫЧИСЛЯЕТСЯ. Потому что вычислить его нечем:
проверка делалась ПО СЛОВАРЯМ, статья за статьёй, и у каждой строки ниже стоит свой
источник. Это не «захардкоженные данные вместо источника» (запрет CLAUDE.md), а
обратное: протокол сверки с источником, который иначе существовал бы только в чате и
пропал бы. Механизм, чтобы такие претензии не рождались ВПРЕДЬ, — отдельная работа
(словарная справка в запросе судье), и она этот список не отменяет: накопленное чинится
само по себе.

ЧТО ДЕЛАЕТСЯ. Ночью, тем же прогоном, что и доспрос: у каждой записи из списка
проверяется, что немецкое и русское СОВПАДАЮТ с тем, что сверялось (иначе не трогаем —
запись за это время могли поправить), и перевод человека признаётся верным: связь в
общий слой, вопрос закрыт, в `decided_text` — след со ссылкой на словарь. Ничего не
исчезает молча (владелец 25.08.2026).

ИДЕМПОТЕНТНО: закрытая запись больше не `open`, второй проход её не видит.
"""
from __future__ import annotations

import logging

# ─────────────────────────────────────────────────────────────────────────────
# Протокол сверки 16.09.2026. Каждая строка: id вопроса, немецкое и русское в том
# виде, в каком они сверялись, и ИСТОЧНИК, по которому претензия признана ложной.
# Запись трогаем, только если оба текста совпали дословно.
ПРОВЕРЕНО_ЛОЖНЫЕ: tuple[dict, ...] = (
    {
        "review_id": 561,
        "de": "Ich kündige die Stelle.",
        "ru": "Я увольняюсь с работы.",
        "claim": "судья прочёл винительную конструкцию как дательную",
        "source": "DWDS, kündigen: «jmdm. kündigen» = уволить кого-то (дательный, лицо); "
                  "«etw. kündigen» — einen Vertrag, seinen Dienst — расторгнуть свой "
                  "договор. die Stelle — винительный, вещь. https://www.dwds.de/wb/kündigen",
    },
    {
        "review_id": 1043,
        "de": "festsetzen",
        "ru": "задержать",
        "claim": "судья знал одно значение и отрицал второе",
        "source": "DWDS, festsetzen: значение 1 «jmdn. gefangen setzen» (задержать) "
                  "наряду со значением «etw. verbindlich bestimmen». "
                  "https://www.dwds.de/wb/festsetzen",
    },
    {
        "review_id": 1044,
        "de": "holz vor der Hütte haben",
        "ru": "иметь большую грудь (разг.)",
        "claim": "судья выдумал толкование идиомы («быть осторожным»)",
        "source": "de.wiktionary, «Holz vor der Hütte haben»: «große Brüste haben, "
                  "vollbusig sein», помета umgangssprachlich. "
                  "https://de.wiktionary.org/wiki/Holz_vor_der_Hütte_haben",
    },
    {
        "review_id": 1045,
        "de": "auslösen",
        "ru": "запустить, выкупить",
        "claim": "судья знал одно значение и отрицал второе",
        "source": "DWDS, auslösen: «einen Gefangenen, Schuldner auslösen» = выкупить "
                  "(помета veraltet, süddeutsch, österreichisch). "
                  "https://www.dwds.de/wb/auslösen",
    },
    {
        "review_id": 1047,
        "de": "Laster",
        "ru": "грузовик",
        "claim": "судья знал одно значение и отрицал второе",
        "source": "DWDS: der Laster = umgangssprachlich Lastkraftwagen («er fährt einen "
                  "Laster») наряду с das Laster = порок. https://www.dwds.de/wb/Laster",
    },
    {
        "review_id": 1130,
        "de": "Sie haben sich in der Nummer geirrt!",
        "ru": "Вы ошиблись номером!",
        "claim": "претензия отрицает сама себя",
        "source": "предложенный судьёй вариант ДОСЛОВНО совпадает с переводом, который "
                  "он забраковал: «Вы ошиблись номером!» — сверка самой записи, "
                  "источник не требуется",
    },
    {
        "review_id": 1148,
        "de": "Police",
        "ru": "полис",
        "claim": "судья предложил прямую неправду («полиция»)",
        "source": "DWDS, Police: «vom Versicherer ausgestellte Urkunde über eine "
                  "abgeschlossene Versicherung, Versicherungsschein». Полиция — Polizei. "
                  "https://www.dwds.de/wb/Police",
    },
    {
        "review_id": 1173,
        "de": "Wir erwarten Kunde aus der Hauptstadt.",
        "ru": "Мы ожидаем известия из столицы.",
        "claim": "судья не разглядел второго слова Kunde",
        "source": "DWDS: die Kunde (gehoben) = Nachricht, «Kunde von jmdm., etw. haben»; "
                  "в «Kunde aus der Hauptstadt» без артикля это она, а не der Kunde "
                  "(клиент, был бы «einen Kunden»). https://www.dwds.de/wb/Kunde",
    },
)

# «Bitte über den Teller essen» (id 1152) в список НЕ вошёл сознательно: претензия
# судьи там тоже выглядит ложной, но подтверждающей словарной статьи на оборот я не
# нашёл, а закрывать вопрос по своему ощущению — ровно то, что запрещено. Он остаётся
# у владельца на экране. НАЙДЕНО 16.09.2026, ОТКРЫТО: нужен источник на оборот.


def _след(запись: dict) -> str:
    """Что останется в базе на месте вопроса. Человек должен понимать это без нас."""
    return (f"претензия судьи ложная: {запись['claim']}. "
            f"Перевод признан верным по источнику — {запись['source']}")[:500]


def close_false_translation_claims() -> dict:
    """Снять ложные претензии: перевод человека — в общий словарь, вопрос — закрыть.

    Возвращает отчёт числами. «Разошлось» — запись изменилась с момента сверки: её не
    трогаем и считаем отдельно, потому что сверка относилась к другому тексту."""
    from backend.database import (apply_translation_link_decision,
                                  get_db_connection_context)

    отчёт = {"в списке": len(ПРОВЕРЕНО_ЛОЖНЫЕ), "закрыто": 0,
             "уже закрыты": 0, "разошлось": 0, "не записалось": 0}
    for запись in ПРОВЕРЕНО_ЛОЖНЫЕ:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT text, translation FROM bt_3_phrase_review "
                    "WHERE id = %s AND status = 'open' "
                    "  AND COALESCE(kind, 'grammar') = 'translation';",
                    (int(запись["review_id"]),))
                row = cursor.fetchone()
        if not row:
            отчёт["уже закрыты"] += 1
            continue
        if str(row[0] or "").strip() != запись["de"].strip() \
                or str(row[1] or "").strip() != запись["ru"].strip():
            logging.warning("ложные претензии: запись %s изменилась с момента сверки — "
                            "не трогаем", запись["review_id"])
            отчёт["разошлось"] += 1
            continue
        итог = apply_translation_link_decision(int(запись["review_id"]), "link_accept")
        if not итог.get("linked"):
            отчёт["не записалось"] += 1
            continue
        отчёт["закрыто"] += 1
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                # Подпись поверх той, что поставил общий путь: она говорит, ПОЧЕМУ
                # вопрос закрыт, а не только каким текстом.
                cursor.execute(
                    "UPDATE bt_3_phrase_review SET decided_text = %s WHERE id = %s;",
                    (_след(запись), int(запись["review_id"])))
            conn.commit()
    logging.info("ложные претензии судьи перевода: %s", отчёт)
    return отчёт


def count_false_claims_still_open() -> int:
    """Сколько проверенных ложных претензий ещё висит у владельца. Обещано: 0."""
    from backend.database import get_db_connection_context

    ids = [int(з["review_id"]) for з in ПРОВЕРЕНО_ЛОЖНЫЕ]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM bt_3_phrase_review "
                "WHERE id = ANY(%s) AND status = 'open' "
                "  AND COALESCE(kind, 'grammar') = 'translation';", (ids,))
            return int((cursor.fetchone() or [0])[0])
