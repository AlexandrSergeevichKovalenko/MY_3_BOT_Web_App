# -*- coding: utf-8 -*-
"""Раз в неделю: «эти слова мы не смогли подтвердить» — человеку, одной пачкой.

Владелец 19.08.2026: «Пользователь может что-то сохранить, и наша система может это
пропустить даже при многих точках контроля. Раз в неделю отправлять ему короткое
сообщение: у тебя есть такие слова, мы не можем подтвердить, что они существуют. К
сообщению прикрепить чекбоксы — отмеченные остаются, остальные удаляются.»
И отдельно: «слова должны приходить ГРУППОЙ, а не по одному».

ПОЧЕМУ ДВА ДЕЙСТВИЯ НА СЛОВО. Владелец заметил случай, который одной галочкой не
закрыть: слово человека устраивает, а перевод — нет. Поэтому у каждого слова две
кнопки: «оставить» и «перевод неверный». Второе не удаляет слово, а ставит карточку
в очередь на пересборку ночью.

КАК ЭТО ВЫГЛЯДИТ. Одно сообщение на всю пачку. Нажатие перерисовывает то же сообщение,
человек видит своё решение сразу и не уходит в конец чата (правило проекта: отвечать
в то же сообщение). Пока «Готово» не нажато, ничего не удаляется.

ЧТО ПОПАДАЕТ В ПАЧКУ. Только слова этого человека, по которым дверь сказала
«не подтверждено» или «не слово». Подтверждённые и исправленные не тревожат никого.
"""
from __future__ import annotations

import logging
import os
from typing import Any

JOB_KEY = "word_confirm_digest"
BATCH = max(1, int((os.getenv("WORD_CONFIRM_DIGEST_BATCH") or "12").strip() or "12"))

KEEP = "keep"
BAD_TRANSLATION = "badtr"
DONE = "done"

# ── Одно опознание слова на все три места ────────────────────────────────────
# Личный словарь хранит существительное ВМЕСТЕ с артиклем: «die Abschiebung»
# (7829 строк из 25240, замер 21.08.2026). Дверь же спрашивает голое слово —
# артикль есть лишь у 1 записи из 149. Пока сравнение шло по сырому word_de,
# соединение рвалось на каждом существительном: экран проверки показывал 24
# слова вместо 100, и очевидный обрубок «das Scheinwerfergla» человек не видел.
#
# Поэтому слово опознаётся ТОЛЬКО по голой форме — и в списке, и в решениях, и в
# плашке при сохранении. Артикль здесь не часть имени слова, а часть карточки.
_BARE = "regexp_replace({col}, '^(der|die|das)[[:space:]]+', '', 'i')"

# ┌─ ТОЛЬКО СЛОВА. ПОЧИНЕНО 29.08.2026. ─────────────────────────────────────────────┐
# │ Это письмо спрашивает «слово настоящее?» — и до сегодня выборка НЕ ПРОВЕРЯЛА,     │
# │ слово перед ней или нет. Поэтому в него попадали целые предложения, и владелец    │
# │ получал кнопки «убрать из словаря / слово настоящее» под «Ich sitze vor dem       │
# │ Fernseher» и «Als Anlage finden Sie eine angehängte Excel-Liste». Ни один         │
# │ справочник не знает предложения — не потому, что оно плохое, а потому что         │
# │ предложений в словарях не бывает.                                                 │
# │                                                                                  │
# │ Замер 29.08.2026: из 63 ждущих ответа только 13 были словами. Остальные 50 —      │
# │ предложения (21), обороты (13) и записи, которых в словаре нет вовсе (16).        │
# │ Хуже того, у предложений УЖЕ ЕСТЬ свой экран разбора, и владелец их там разобрал: │
# │ проверка по текстам показала 20 из 20 со статусом «разобрано на экране» и         │
# │ одновременно «ЖДЁТ» здесь. Он видел одно и то же дважды и был прав.               │
# │                                                                                  │
# │ Признак «одно слово» здесь НЕ грамматическая догадка: многословная строка         │
# │ словом не является по определению, и считаем мы пробелы, а не смысл. Артикль      │
# │ снимается тем же _BARE, что и везде, иначе «die Ernte» выпало бы как «два слова». │
# │                                                                                  │
# │ Фильтр живёт ОДНОЙ строкой на все три выборки этого модуля: скопированное         │
# │ условие расходится с оригиналом на первой же правке, а цена расхождения тут —     │
# │ снова присланное владельцу предложение.                                           │
# └──────────────────────────────────────────────────────────────────────────────────┘
_ТОЛЬКО_СЛОВА = "position(' ' in btrim({bare})) = 0"

# ⚠ КАКИЕ ВОПРОСЫ ИЗ `bt_3_phrase_review` АДРЕСОВАНЫ ЧЕЛОВЕКУ.
# В таблице три вида: 'grammar' (немецкий самой фразы), 'panel' (карточка тремя
# голосами) и 'translation' (перевод карточки перед подъёмом в общий слой). Третий
# сформулирован ДЛЯ ВЛАДЕЛЬЦА и решается на его экране своими кнопками. Пока отбора не
# было, 27.08.2026 все 38 таких записей доехали до ученика и показались ему как
# «фраза, в которой мы усомнились» — среди них одиночные слова «Besprechung», «Soile».
# Правило для всех читателей таблицы — docs/tasks/phrase_review_kinds.md.
ВИДЫ_ДЛЯ_ЧЕЛОВЕКА = ["grammar", "panel"]


def _cleaned(text: str) -> str:
    """Общая чистка входа — та же, что стоит на всех дверях записи слова.

    Отдельной функцией, потому что чистка обязана быть ОДНОЙ на все входы: как
    только у ручного ввода появляется своя, послабленная, — через неё и приезжает
    то, от чего дверь стережёт остальных.

    Если чистка недоступна (её нет в окружении теста), возвращаем строку как есть,
    а не глушим ошибку молча: место записи всё равно одно и видно.
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        from backend.dictionary_intake import clean_text
    except Exception:
        logging.warning("проверка слов: чистка входа недоступна", exc_info=True)
        return raw
    return str(clean_text(raw) or "").strip()


def bare_word(text: str) -> str:
    """Слово без артикля — то, чем его знает дверь."""
    import re
    return re.sub(r"^(der|die|das)\s+", "", str(text or "").strip(), flags=re.I).strip()


def ensure_word_confirm_schema() -> None:
    """Состояние пачки: что человек уже отметил. Хранится до нажатия «Готово»."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_word_confirm_digest (
                        user_id     BIGINT NOT NULL,
                        word        TEXT NOT NULL,
                        decision    TEXT NOT NULL DEFAULT '',
                        sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        closed_at   TIMESTAMPTZ,
                        PRIMARY KEY (user_id, word)
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("сводка слов: схема не создана", exc_info=True)


def words_for_user(user_id: int, limit: int = BATCH) -> list[tuple[str, str]]:
    """[(текст, перевод)] — то, в чём система усомнилась, у ИМЕННО ЭТОГО человека.

    ДВА ИСТОЧНИКА СОМНЕНИЯ, и оба про его собственные сохранения:

      СЛОВА — дверь слова (`bt_3_word_check`) не подтвердила написание справочником;
      ФРАЗЫ — ночная проверка грамматики не смогла решить сама и отложила фразу
              (`bt_3_phrase_review`, status='open').

    ⚠ ЗАЧЕМ СЮДА ДОБАВЛЕНЫ ФРАЗЫ. Владелец 26.08.2026: «фраза сохранилась мгновенно,
    ночью проверилась, если сомнение — оно уходит АВТОРУ в ту же недельную пачку, что и
    слова». До этого сомнения по фразам уходили ТОЛЬКО администратору: обычный человек
    про свою кривую фразу не узнавал вовсе, а на масштабе очередь до неё шла бы неделями.

    Ничего нового не строим: та же пачка, те же кнопки, то же сообщение. Новый канал был
    бы вторым местом, куда надо не забыть заглянуть.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT {bare}, COALESCE(q.translation_ru, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND {только_слова}
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY 1
                     LIMIT %s;
                    """.format(bare=_BARE.format(col="q.word_de"),
                             только_слова=_ТОЛЬКО_СЛОВА.format(
                                 bare=_BARE.format(col="q.word_de"))),
                    (int(user_id), int(limit)),
                )
                найдено = [(str(a), str(b)) for a, b in (cur.fetchall() or [])]
                if len(найдено) >= int(limit):
                    return найдено

                # Фразы этого человека, отложенные ночной проверкой. Список берётся
                # ОДНОЙ функцией с экраном проверки (`_phrase_items`): письмо, которое
                # обещает не то, что покажет экран, — это то же обещание без
                # содержания, из-за которого и затевалась правка 26.08.2026.
                уже = {слово for слово, _ in найдено}
                for карточка in _phrase_items(cur, int(user_id), int(limit) - len(найдено)):
                    if карточка["word"] not in уже:
                        найдено.append((карточка["word"], карточка["translation"]))
                return найдено
    except Exception:
        # ┌─ ПОЧИНЕНО 26.08.2026. ЗДЕСЬ БЫЛО `return []`. ───────────────────────────┐
        # │ Пустой список УЖЕ значит «спрашивать нечего», и та же пустота уходила    │
        # │ наружу при любой поломке: сбой базы, моя же ошибка в коде, смена         │
        # │ колонки. Различить снаружи было нечем. 26.08.2026 этот `except` живьём   │
        # │ съел настоящую поломку (моя правка стала падать на распаковке строки),   │
        # │ и нашлась она только по следу в логе.                                    │
        # │ Теперь поломка идёт наверх, а «нечего спрашивать» остаётся пустым        │
        # │ списком — два разных мира снова различимы.                               │
        # └─────────────────────────────────────────────────────────────────────────┘
        logging.warning("сводка слов: не прочитал список для %s", user_id, exc_info=True)
        raise


def _decisions(user_id: int) -> dict[str, str]:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT word, decision FROM bt_3_word_confirm_digest "
                            "WHERE user_id=%s AND closed_at IS NULL;", (int(user_id),))
                return {str(a): str(b) for a, b in (cur.fetchall() or [])}
    except Exception:
        # Не прочитали отметки — молча показать «ничего не отмечено» нельзя: человек
        # решит, что его нажатия пропали, и нажмёт заново.
        logging.warning("сводка слов: отметки человека %s не прочитаны", user_id, exc_info=True)
        raise


def render(user_id: int, words: list[tuple[str, str]]) -> tuple[str, dict[str, Any]]:
    """(текст сообщения, клавиатура). Одно сообщение на всю пачку."""
    chosen = _decisions(user_id)
    lines = [
        "🔍 <b>Проверь свои слова</b>",
        "",
        "Эти слова ты сохранял, а мы не смогли подтвердить, что они есть в немецком.",
        "Отметь те, что нужно оставить. Остальные удалим.",
        "",
    ]
    for i, (word, translation) in enumerate(words, 1):
        state = chosen.get(word, "")
        mark = {"": "⬜", KEEP: "✅", BAD_TRANSLATION: "✏️"}.get(state, "⬜")
        tail = f" — {translation}" if translation else ""
        note = "  <i>перевод переделаем</i>" if state == BAD_TRANSLATION else ""
        lines.append(f"{mark} <b>{word}</b>{tail}{note}")
    lines += ["", "<i>Ничего не удаляется, пока не нажмёшь «Готово».</i>"]

    rows = []
    for i, (word, _t) in enumerate(words):
        state = chosen.get(word, "")
        rows.append([
            {"text": ("✅ " if state == KEEP else "") + word[:18],
             "callback_data": f"wconf:{KEEP}:{i}"},
            {"text": ("✏️ перевод" if state == BAD_TRANSLATION else "✏️"),
             "callback_data": f"wconf:{BAD_TRANSLATION}:{i}"},
        ])
    rows.append([{"text": "Готово — остальные удалить", "callback_data": f"wconf:{DONE}:0"}])
    return "\n".join(lines), {"inline_keyboard": rows}


def preview(user_id: int, words: list[tuple[str, str]] | None = None) -> str:
    """Текстовый макет для владельца: как это увидит человек в личке."""
    items = words if words is not None else words_for_user(user_id)
    text, keyboard = render(user_id, items)
    plain = text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "")
    out = [plain, "", "┌─ кнопки под сообщением ─────────────────"]
    for row in keyboard["inline_keyboard"]:
        out.append("│  " + "   ".join(f"[ {b['text']} ]" for b in row))
    out.append("└─────────────────────────────────────────")
    return "\n".join(out)


# ── Фразы на том же экране ───────────────────────────────────────────────────
# Причина по-русски берётся ИЗ КАТЕГОРИИ, которую назвал проверяющий, и ниоткуда
# больше. Своих грамматических выводов здесь нет: мы не решаем, что не так с фразой,
# мы переводим на человеческий язык то, что уже сказал источник.
#
# Список составлен по живым данным 26.08.2026 — 232 открытые фразы:
#   wortstellung 85 · kasus 32 · praeposition 30 · rechtschreibung 25 · kongruenz 12
#   stil 2 · sprachmischung 1 · без категории 9
# («панель из трёх голосов» — наша служебная пометка, а не разряд ошибки, и в
# человеческую причину не превращается.)
ФРАЗА_ПРИЧИНЫ = {
    "wortstellung": "Похоже, слова стоят не в том порядке.",
    "kasus": "Похоже, слово стоит не в том падеже.",
    "praeposition": "Похоже, здесь нужен другой предлог.",
    "rechtschreibung": "Похоже, в написании ошибка.",
    "kongruenz": "Похоже, окончание не согласовано с другим словом.",
    "stil": "Так по-немецки почти не говорят.",
    "sprachmischung": "В немецкую фразу попало слово из другого языка.",
}


def _phrase_reason(judges: Any) -> str:
    """Причина человеческим языком. Не знаем разряд — говорим это прямо."""
    названо: list[str] = []
    for судья in (judges if isinstance(judges, list) else []):
        if not isinstance(судья, dict):
            continue
        for кусок in str(судья.get("category") or "").split("|"):
            строка = ФРАЗА_ПРИЧИНЫ.get(кусок.strip().lower())
            if строка and строка not in названо:
                названо.append(строка)
    if not названо:
        # Ни один проверяющий не назвал разряд. Выдумывать причину нельзя, молчать
        # тоже: человек должен понимать, за что его спрашивают.
        return "Проверяющие разошлись во мнении об этой фразе — реши ты."
    return " ".join(названо[:2])


# ┌─ ПОЧИНЕНО 28.08.2026. ОБРЕЗКА ДО ДВУХ КНОПОК СНЯТА. ─────────────────────────┐
# │ Здесь стояло КНОПОК_НА_ФРАЗУ = 2, и третий годный вариант человек не видел    │
# │ вовсе. Замер по живой базе 28.08.2026 по 104 открытым вопросам про немецкий:  │
# │ у 70 годный вариант один, у 31 — два, у 3 — три. То есть у трёх фраз мы прятали│
# │ рабочий вариант, ничего этим не выигрывая: варианты идут блоками по судьям и   │
# │ занимают ровно столько места, сколько их есть.                                │
# │ Владелец 28.08.2026: «зачем обрезать, покажи все и дай выбрать».               │
# └──────────────────────────────────────────────────────────────────────────────┘


def кнопки_вариантов(judges: Any, text: str, arbiter: dict | None) -> list[dict[str, str]]:
    """РОВНО те варианты, которые экран проверки слов рисует кнопками. Один источник.

    ┌─ ПОЧИНЕНО 28.08.2026. НАЖИМАЛСЯ ОДИН ВАРИАНТ, ЗАПИСЫВАЛСЯ ДРУГОЙ. ────────────┐
    │ Экран строил кнопки по ЭТОМУ списку (урезанному), а применение решения брало   │
    │ присланный НОМЕР из полного списка `phrase_review_variants(…,                  │
    │ include_disputed=True)`. Стоило спорному варианту стоять раньше — и номера      │
    │ разъезжались. Замер по живой базе 28.08.2026: из 40 решений владельца за сутки  │
    │ два записали не тот текст, который он нажал:                                    │
    │   #317 нажато «Jemand klagt über etwas.» → записано «Jemand klagt über + A»     │
    │   #319 нажато «Ich bewerbe mich bei der Firma.» → «Ich bewerbe mich bei + D»    │
    │ Лечится не подгонкой флага, а тем, что номер с этого экрана НЕ ХОДИТ ВОВСЕ:     │
    │ наверх уезжает сам текст кнопки, а сервер ищет его в этом же списке.            │
    │ Перемерить: scripts/word_audit_variant_index_audit.py                           │
    └──────────────────────────────────────────────────────────────────────────────┘

    Готовых вариантов может не быть вовсе: на живых данных 28.08.2026 таких 116 из 222.
    Тогда карточка честно предлагает только оставить, вписать своё или удалить —
    придумывать за проверяющих мы не будем.

    ⚠ СПОРНЫЙ ВАРИАНТ ОБЫЧНОМУ ЧЕЛОВЕКУ НЕ ПОКАЗЫВАЕМ.
    `check_disputed_by_arbiter` означает: наша проверка правку ЗАБРАКОВАЛА, а третейский
    судья назвал верной. У владельца такой вариант на экране есть — но рядом печатается
    возражение проверки, и он решает зряче. Здесь экрана для возражения нет, а кнопка
    «Да, правильно так» читается как «система уверена». Отдать одним касанием то, что
    система сама забраковала, нельзя: человек учит немецкий по нашему ответу.
    """
    from backend.database import phrase_review_variants

    варианты = phrase_review_variants(
        judges if isinstance(judges, list) else [], str(text or ""),
        arbiter if isinstance(arbiter, dict) else None)
    return [{"text": str(v.get("text") or ""), "ru": str(v.get("ru") or ""),
             # Кто предложил — чтобы кнопка стояла рядом со словами СВОЕГО судьи.
             "judge": int(v.get("judge") or 0),
             # Чем это является. «Правка» — исправлено то, что было. «Достройка» —
             # судья ДОПИСАЛ слова (местоимение, подлежащее), и фраза стала другой по
             # объёму. Разница видна человеку на кнопке: это разные решения.
             "field": str(v.get("field") or ""),
             # Что это за вариант — человеку словами, до нажатия.
             # У текста третьего судьи ещё и его собственная подпись: он единственный,
             # кто пишет СВОЙ текст, и по замеру 29.08.2026 подпись врёт в 29% случаев.
             # Поэтому ночь по ней ничего не решает, а человек её видит и решает сам.
             "kind": ("достройка: дописаны слова" if v.get("field") == "proposal"
                      else "правка того, что было" if v.get("field") == "corrected"
                      else "третий судья: исправил" if v.get("better_kind") == "fix"
                      else "третий судья: переписал заново"
                      if v.get("better_kind") == "rebuild"
                      else "текст третьего судьи")}
            for v in варианты
            if not v.get("check_disputed_by_arbiter")]


def _phrase_items(cur, user_id: int, limit: int) -> list[dict[str, Any]]:
    """Отложенные ночью фразы ЭТОГО человека — карточками того же экрана.

    ⚠ СПРАШИВАЕМ АВТОРА, А НЕ ВСЕХ ПОДПИСЧИКОВ — то же правило и та же история
    дефекта, что в `words_for_user`. Автор — тот, чья карточка появилась первой.
    """
    from backend.database import phrase_review_is_noise

    cur.execute(
        """
        WITH авторы AS (
          SELECT DISTINCT ON (lex_unit_id) lex_unit_id, user_id
            FROM bt_3_webapp_dictionary_queries
           WHERE lex_unit_id IS NOT NULL
           ORDER BY lex_unit_id, created_at, id
        )
        SELECT r.id, btrim(r.text), COALESCE(r.translation, ''), r.judges, r.arbiter,
               r.unit_id, COALESCE(r.kind, ''), u.card
          FROM bt_3_phrase_review r
          JOIN bt_3_lex_units u ON u.id = r.unit_id
          JOIN авторы a ON a.lex_unit_id = r.unit_id
         -- ВИД НАЗЫВАЕТСЯ ЯВНО, прямо в запросе: человеку адресованы только вопросы о
         -- самой фразе. Вопрос про перевод карточки решает владелец своим экраном.
         WHERE r.status = 'open' AND r.kind = ANY(%s) AND a.user_id = %s
           AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                            WHERE d.user_id = a.user_id AND d.word = btrim(r.text)
                              AND d.closed_at IS NOT NULL)
         ORDER BY r.id
         LIMIT %s;
        """,
        (ВИДЫ_ДЛЯ_ЧЕЛОВЕКА, int(user_id), int(limit)),
    )
    from backend.database import phrase_review_card_examples

    items: list[dict[str, Any]] = []
    for (review_id, текст, перевод, судьи, арбитр,
         unit_id, вид, карточка) in (cur.fetchall() or []):
        судьи = судьи if isinstance(судьи, list) else []
        арбитр = арбитр if isinstance(арбитр, dict) else None
        # ⚠ БЕРЁМ ТОЛЬКО ВОПРОСЫ ПРО САМУ ФРАЗУ.
        # В `bt_3_phrase_review` живут ТРИ вида вопроса, и они не взаимозаменяемы:
        #   grammar     — судьи разошлись о немецком самой фразы;
        #   panel       — три голоса разошлись о карточке;
        #   translation — перевод карточки не прошёл проверку перед подъёмом в общий
        #                 слой (заведён 27.08.2026, backend/translation_links.py).
        # Третий вид сформулирован ДЛЯ ВЛАДЕЛЬЦА и решается на его экране своими
        # кнопками. Прогон по живой базе 27.08.2026 сразу после слияния: все 38 таких
        # записей доехали до экрана проверки слов и показались человеку как «фраза, в
        # которой мы усомнились» — включая одиночные слова «Besprechung» и «Soile».
        # Чужой вопрос, заданный не тому человеку и не теми словами.
        # Шум — это записи, где проверяющий «исправил» фразу в саму себя. На экране
        # владельца они уже отсеиваются; человеку тем более показывать нечего.
        if phrase_review_is_noise(судьи, str(текст)):
            continue
        # ⚠ ДВА РАЗНЫХ ВОПРОСА — ДВЕ РАЗНЫЕ КАРТОЧКИ НА ЭКРАНЕ.
        #
        # ┌─ ПОЧИНЕНО 28.08.2026. ПАНЕЛЬНЫЙ ВОПРОС ПРИХОДИЛ НЕМЫМ. ──────────────────┐
        # │ «panel» — это сомнение НЕ во фразе, а в НАПОЛНЕНИИ карточки: примеры не  │
        # │ иллюстрируют выражение, перевод не той формы. Кнопки «правильно так» у   │
        # │ него не бывает и быть не может — исправлять нечего, спор о другом. А     │
        # │ экран рисовал его как вопрос о фразе, с текстом «проверяющие разошлись   │
        # │ во мнении об этой фразе» и без единой кнопки.                            │
        # │ Замер по живой базе 28.08.2026: таких вопросов 77 из 218 открытых, и все  │
        # │ 77 доезжали до человека в таком виде.                                    │
        # │ Владелец 28.08.2026: «если вопрос в наполнении карточки — я должен каждый │
        # │ пример либо откорректировать, либо удалить, либо оставить».               │
        # │ Теперь панельная карточка везёт то, о чём спор: сами примеры и претензии  │
        # │ проверяющих по пунктам.                                                   │
        # └──────────────────────────────────────────────────────────────────────────┘
        панель = str(вид or "") == "panel"
        items.append({
            "word": str(текст),
            "translation": str(перевод),
            "status": "фраза",
            "why": _phrase_reason(судьи),
            "variants": [] if панель else кнопки_вариантов(судьи, str(текст), арбитр),
            # Слова судей — то, ради чего человек и открывает экран. Их 319 из 322
            # (замер 28.08.2026), и до этого дня на экран не доезжало ни одно.
            "judges": [] if панель else _слова_судей(судьи),
            "arbiter": None if панель else _слова_арбитра(арбитр),
            # Предмет панельного спора: претензии по пунктам и сами примеры.
            "doubts": _претензии(судьи) if панель else [],
            "examples": phrase_review_card_examples(карточка) if панель else [],
            "question": "card" if панель else "text",
            "suggestion": "",
            "safe": False,
            "kind": "phrase",
            "review_id": int(review_id),
            "unit_id": int(unit_id),
        })
    return items


def _слова_судей(судьи: Any) -> list[dict[str, Any]]:
    """Что сказал каждый проверяющий — его словами, а не нашей обобщённой строкой."""
    out = []
    for n, судья in enumerate(судьи if isinstance(судьи, list) else [], 1):
        почему = str((судья or {}).get("why") or "").strip()
        if not почему:
            continue
        out.append({"n": n, "why": почему})
    return out


def _слова_арбитра(арбитр: dict | None) -> dict[str, Any] | None:
    """Вердикт третьего судьи. Он спор уже разрешил — человек должен это видеть."""
    if not isinstance(арбитр, dict):
        return None
    почему = str(арбитр.get("why") or "").strip()
    if not почему:
        return None
    try:
        победитель = int(арбитр.get("winner") or 0)
    except (TypeError, ValueError):
        победитель = 0
    return {"why": почему, "winner": победитель}


def _претензии(судьи: Any) -> list[str]:
    """Претензии панели по пунктам. Панель складывает их в одну строку через «; » —
    разбираем обратно, чтобы каждая читалась отдельным пунктом, а не абзацем."""
    out: list[str] = []
    for судья in (судьи if isinstance(судьи, list) else []):
        for кусок in str((судья or {}).get("why") or "").split(";"):
            кусок = кусок.strip()
            if кусок and кусок not in out:
                out.append(кусок if кусок.endswith(".") else кусок + ".")
    return out


# ── Что показывает экран проверки ────────────────────────────────────────────
def audit_items(user_id: int, limit: int = 200) -> list[dict[str, Any]]:
    """Слова этого человека, которые дверь не подтвердила, с причиной и подсказкой.

    Владелец 20.08.2026: «давать пользователю все слова, а он уже решит, сколько
    обработать». Поэтому потолок высокий — это не пачка, а полный список.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON ({bare})
                           {bare}, COALESCE(q.translation_ru, ''),
                           w.status, w.source, COALESCE(s.suggestion, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                      LEFT JOIN bt_3_word_suggestion s ON s.asked = {bare}
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND {только_слова}
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY 1
                     LIMIT %s;
                    """.format(bare=_BARE.format(col="q.word_de"),
                             только_слова=_ТОЛЬКО_СЛОВА.format(
                                 bare=_BARE.format(col="q.word_de"))),
                    (int(user_id), int(limit)),
                )
                rows = cur.fetchall() or []
                # ⚠ ФРАЗЫ ЖИВУТ НА ТОМ ЖЕ ЭКРАНЕ. До 26.08.2026 письмо считало и слова,
                # и фразы, а экран показывал ТОЛЬКО слова — списки берутся из разных
                # таблиц. Замер того же дня: у трёх авторов 195 отложенных фраз и НОЛЬ
                # слов, то есть письмо «слова ждут проверки» вело на экран «проверять
                # нечего». Обещание без содержания хуже молчания, поэтому источник
                # списка и источник письма обязаны совпадать.
                фразы = _phrase_items(cur, int(user_id), max(0, int(limit) - len(rows)))
    except Exception:
        # Пустой экран «всё в порядке» вместо поломки — обман. Наверх, эндпоинт скажет
        # человеку «не удалось загрузить», и это РАЗНЫЕ экраны (см. WordAudit.jsx).
        logging.warning("экран проверки: не прочитал список для %s", user_id, exc_info=True)
        raise
    слова = [{"word": str(a), "translation": str(b), "status": str(c),
             "why": _human_reason(str(c), str(d)), "suggestion": str(e),
             # Слово, существование которого подтвердила модель, молчанием НЕ удаляется:
             # решение владельца 21.08.2026. Справочники неполны, и предлагать человеку
             # стереть настоящее слово только потому, что страницы нет, — вред.
             "safe": _model_confirmed(str(d)), "kind": "word"}
            for a, b, c, d, e in rows]
    return слова + фразы


def _model_confirmed(source: str) -> bool:
    """Сказал ли источник, что слово СУЩЕСТВУЕТ. Тогда молчание его не удаляет.

    Два таких случая, и оба означают «слово настоящее»:
      «модель: слово есть, …»                  — редкое или иноязычное, страницы нет;
      «модель предложила другое написание, …»  — слово есть, спорно лишь написание.

    Второй случай попадает сюда по той же причине, что и первый: удалить слово
    человека за то, что он пролистал экран, нельзя ни при каком из них. Написание
    он поправит кнопкой, а стёртое слово не вернуть.
    """
    text = str(source or "")
    return text.startswith("модель: слово есть") or text.startswith("модель предложила другое")


def _human_reason(status: str, source: str) -> str:
    """Причина ЧЕЛОВЕЧЕСКИМ языком. Никаких «не подтверждено источником X»."""
    if "язык en" in source:
        return "Настоящее слово, но английское — в немецких справочниках его нет."
    if "язык ru" in source:
        return "Это русское слово, а лежит оно в немецком словаре."
    if source.startswith("модель: слово есть"):
        return "Слово настоящее, просто редкое — справочник его не знает."
    if source.startswith("модель: такого слова нет"):
        return "Похоже, при сохранении слово потеряло часть букв."
    if source == "справочник молчал":
        return "Справочник не ответил, когда мы проверяли. Проверим ещё раз."
    return "Мы не нашли это слово в немецких справочниках."


def ensure_word_suggestion_schema() -> None:
    """Подсказка правильного написания. Считается ночью, показывается человеку кнопкой."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_word_suggestion (
                        asked       TEXT PRIMARY KEY,
                        suggestion  TEXT NOT NULL DEFAULT '',
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("подсказка написания: схема не создана", exc_info=True)


def _with_article(word: str) -> str:
    """Исправленное слово в том виде, в каком оно ляжет в карточку.

    Артикль берётся ТОЛЬКО у справочника рода. Старый артикль строки сюда не
    переносится: «der Schwarzflieger» после починки может оказаться словом
    другого рода, и перенос превратил бы опечатку в грамматическую ошибку.
    Если род неизвестен — пишем голое слово: артикль допишет ночная программа
    рода, когда узнает его. Пустое место честнее выдуманного «der».
    """
    text = bare_word(word)
    if not text or not text[:1].isupper():
        return text  # артикль бывает у существительного, а не у глагола
    try:
        from backend.article_authority import authoritative_article
        art, _src = authoritative_article(text, allow_network=False)
    except Exception:
        art = None
    return f"{art} {text}" if art else text


def _same_text(left: str | None, right: str | None) -> bool:
    """Одно ли это написание. Артикль и регистр различием не считаем."""
    return bare_word(str(left or "")).casefold() == bare_word(str(right or "")).casefold()


def _rewrite_card_to_new_word(cur, *, user_id: int, old_bare: str, new_word_de: str,
                              new_translation: str) -> dict[str, int]:
    """Перевести карточки человека на исправленное слово — ЦЕЛИКОМ, а не заголовок.

    ┌─ НАЙДЕНО И ПОЧИНЕНО 25.08.2026. «ПРАВКУ ДВУХ ГРАФ» НЕ ВОЗВРАЩАТЬ. ────────────┐
    │ Правка меняла word_de и translation_ru — две графы из восьми. В той же строке │
    │ оставались нетронутыми word_ru, translation_de, весь response_json и указатель │
    │ на запись общего пула. Владелец исправил «das Scheinwerfergla» → «der          │
    │ Scheinwerfer», а внутри карточки остался разбор про обрубок — и тренажёр берёт │
    │ текст ИМЕННО оттуда (frontend/src/App.jsx, resolveFlashcardTexts: первым стоит │
    │ responseJson.source_text). То есть человек исправлял слово и продолжал бы      │
    │ учить старое. Замер: за всё время 3 такие правки (1 fixed + 2 manual), все три │
    │ вычищены руками — scripts/word_audit_fix_stale_cards.py.                       │
    └───────────────────────────────────────────────────────────────────────────────┘

    ПОЧЕМУ РАЗБОР СТИРАЕТСЯ, А НЕ ПРАВИТСЯ ПОСТРОЧНО. Исправленное слово — ДРУГОЕ
    слово: у «Scheinwerfergla» перевод «стекло фары», у «Scheinwerfer» — «фара».
    Заменить в старом разборе одно написание на другое значит оставить человеку
    значения, примеры и формы чужого слова под новым заголовком. Правило проекта:
    неверное слово — весь разбор в мусор.

    ПОЧЕМУ ПУСТО НЕ ОСТАНЕТСЯ, И ЭТО НЕ ЗАГЛУШКА. Разбор живёт не в карточке, а на
    слове (`bt_3_lex_units`); читатели берут его по указателю `lex_unit_id`
    (`attach_unit_content_to_cards`). Мы обнуляем указатель — и подбор
    `lex_units.attach_missing_entries` ставит его на НОВОЕ слово, а ночной
    `_run_units_night_enrichment` дособирает сам разбор. Тот же подбор зовётся и на
    чтении (`_attach_missing_entries_quietly`), поэтому дырка живёт часы, а не сутки.
    Ставить указатель прямо здесь нельзя: `ensure_unit` стучится в дверь слова и к
    модели, а человек в этот момент ЖДЁТ ответа экрана.
    """
    report = {"карточек": 0, "снято из пула": 0, "чужих граф не тронуто": 0}
    cur.execute(
        "SELECT id, word_de, word_ru, translation_de, translation_ru, canonical_entry_id "
        "FROM bt_3_webapp_dictionary_queries WHERE user_id=%s AND "
        + _BARE.format(col="word_de") + "=%s;",
        (int(user_id), old_bare))
    rows = cur.fetchall() or []
    pool_ids: set[int] = set()
    for card_id, word_de, word_ru, translation_de, translation_ru, canonical_id in rows:
        # Русская сторона: главная графа — translation_ru. Перевода человек мог и не
        # вписать («да, это …») — тогда остаётся прежний, и это не подстановка
        # значения, а «графу не трогаем».
        new_ru = new_translation if new_translation else str(translation_ru or "")
        # Зеркальные графы (translation_de у немецкой стороны, word_ru у русской)
        # переписываем ТОЛЬКО если в них лежало ровно то же, что в главной. Лежит
        # что-то другое — это текст человека, мы его не переписываем и не стираем,
        # а СЧИТАЕМ: владелец увидит число в логе решения.
        de_mirror = new_word_de if _same_text(translation_de, word_de) else None
        ru_mirror = new_ru if _same_text(word_ru, translation_ru) else None
        if de_mirror is None and str(translation_de or "").strip():
            report["чужих граф не тронуто"] += 1
        if ru_mirror is None and str(word_ru or "").strip():
            report["чужих граф не тронуто"] += 1
        # COALESCE ниже НЕ подставляет значение вместо ответа: None означает «графа не
        # наша, оставить как есть» (см. зеркала выше). Комментарий держим в питоне, а не
        # внутри SQL: «--» в схлопнутой в одну строку команде убил бы остаток запроса.
        cur.execute(
            """UPDATE bt_3_webapp_dictionary_queries
                  SET word_de = %s,
                      translation_ru = %s,
                      translation_de = COALESCE(%s, translation_de),
                      word_ru = COALESCE(%s, word_ru),
                      response_json = NULL,
                      canonical_entry_id = NULL,
                      lex_unit_id = NULL,
                      updated_at = NOW()
                WHERE id = %s;""",
            (new_word_de, new_ru, de_mirror, ru_mirror, int(card_id)))
        report["карточек"] += 1
        if canonical_id:
            pool_ids.add(int(canonical_id))

    # ОБЩИЙ ПУЛ — ЭТО КЕШ ПОИСКА, А НЕ ЧЬИ-ТО ДАННЫЕ (то же правило и в
    # word_gate_apply.py). Запись пула, собранная вокруг обрубка, отвечает на поиск
    # обрубком: «Депортация» → «die Abschiebu», и так — всем, кто это ищет. Снимаем:
    # следующий поиск соберёт запись заново, уже по исправленному слову. Личные
    # карточки других людей НЕ ТРОГАЕМ никогда — только указатель на снятую запись,
    # он служит защите от дублей, а не выдаче.
    for pool_id in sorted(pool_ids):
        cur.execute("SELECT source_text, target_text, word_de, translation_de "
                    "FROM bt_3_dictionary_entries WHERE id=%s;", (pool_id,))
        row = cur.fetchone()
        if not row or not any(_same_text(value, old_bare) for value in row):
            continue  # запись пула про другое слово — не наше дело
        cur.execute("UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL "
                    "WHERE canonical_entry_id=%s;", (pool_id,))
        cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;", (pool_id,))
        report["снято из пула"] += 1

    # Кеш поиска по самому обрубку: он отвечает на набранное слово готовой карточкой,
    # минуя и пул, и дверь. Оставить его значит оставить обрубку последнюю дверь наружу.
    cur.execute("DELETE FROM bt_3_dictionary_lookup_cache WHERE lower(normalized_word)=lower(%s);",
                (old_bare,))
    return report


def _phrase_counts_by_author(cur) -> dict[int, int]:
    """Сколько фраз ждёт КАЖДОГО автора — ровно столько же, сколько покажет экран.

    ⚠ ОДИН ЗАПРОС НА ВСЕХ, А НЕ ПО ЗАПРОСУ НА ЧЕЛОВЕКА. Поиск автора — это проход по
    всем карточкам словаря; сделать его для тысячи получателей по разу значит тысячу
    проходов дважды в неделю. Здесь он делается однажды, а разбор по людям — в памяти.

    Пустые придирки («ошибка есть, а исправить нечего») отсеиваются тем же правилом,
    что и на экране. Иначе письмо обещает 186 фраз, а человек находит 98.
    """
    from backend.database import phrase_review_is_noise

    cur.execute(
        """
        WITH авторы AS (
          SELECT DISTINCT ON (lex_unit_id) lex_unit_id, user_id
            FROM bt_3_webapp_dictionary_queries
           WHERE lex_unit_id IS NOT NULL
           ORDER BY lex_unit_id, created_at, id
        )
        SELECT a.user_id, btrim(r.text), r.judges
          FROM bt_3_phrase_review r
          JOIN авторы a ON a.lex_unit_id = r.unit_id
         -- Тот же отбор вида, что и на экране (см. `_phrase_items`).
         WHERE r.status = 'open' AND r.kind = ANY(%s)
           AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                            WHERE d.user_id = a.user_id AND d.word = btrim(r.text)
                              AND d.closed_at IS NOT NULL);
        """,
        (ВИДЫ_ДЛЯ_ЧЕЛОВЕКА,),
    )
    счёт: dict[int, int] = {}
    for user_id, текст, судьи in (cur.fetchall() or []):
        судьи = судьи if isinstance(судьи, list) else []
        if phrase_review_is_noise(судьи, str(текст)):
            continue
        счёт[int(user_id)] = счёт.get(int(user_id), 0) + 1
    return счёт


def _phrase_owner(review_id: int) -> tuple[int, str, int, list, dict | None]:
    """(номер слова, текст, автор, судьи, арбитр) для открытой фразы.

    Автор — чья карточка первая. Судьи и арбитр нужны здесь же, чтобы применение решения
    собрало ТОТ ЖЕ список кнопок, что видел человек (см. `кнопки_вариантов`), — вторым
    запросом их брать нельзя: между запросами ночь успевает дописать третьего судью, и
    список опять разъедется с экраном."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.unit_id, btrim(r.text),
                          (SELECT q.user_id FROM bt_3_webapp_dictionary_queries q
                            WHERE q.lex_unit_id = r.unit_id
                            ORDER BY q.created_at, q.id LIMIT 1),
                          r.judges, r.arbiter
                     FROM bt_3_phrase_review r
                    WHERE r.id = %s AND r.status = 'open';""",
                (int(review_id),),
            )
            строка = cur.fetchone()
    if not строка or строка[2] is None:
        return 0, "", 0, [], None
    судьи = строка[3] if isinstance(строка[3], list) else []
    арбитр = строка[4] if isinstance(строка[4], dict) else None
    return int(строка[0]), str(строка[1]), int(строка[2]), судьи, арбитр


def _apply_phrase_decision(user_id: int, item: dict[str, Any]) -> str:
    """Решение человека по СВОЕЙ фразе. Возвращает имя счётчика или пустую строку.

    ⚠ ВТОРОГО МЕХАНИЗМА ЗДЕСЬ НЕТ. Правку применяет ровно та же функция, что и на
    экране владельца (`database.apply_phrase_review_decision`): она умеет переписать
    заголовок, разнести правку по всем местам и снять метку проверки, чтобы ночь
    посмотрела фразу заново. Своя копия этой логики означала бы, что через полгода
    два пути разойдутся и один из них станет неверным.

    РАЗЛИЧИЕ ОДНО, И ОНО ПРО «УДАЛИТЬ». У владельца «удалить» уносит фразу из общего
    словаря вместе с подписными карточками — это решение обо ВСЕХ. Обычный человек
    решает только о себе: его «удалить» убирает фразу из ЕГО словаря. Общее слово
    сносится, лишь если больше ни у кого его нет; иначе фраза остаётся в очереди
    владельца, а этому человеку больше не показывается.
    """
    from backend.database import apply_phrase_review_decision, get_db_connection_context

    try:
        review_id = int(item.get("review_id") or 0)
    except (TypeError, ValueError):
        return ""
    action = str(item.get("action") or "").strip()
    if not review_id or not action:
        return ""

    # ⚠ НОМЕР ФРАЗЫ ПРИШЁЛ ИЗ БРАУЗЕРА — ЕМУ ВЕРИТЬ НЕЛЬЗЯ. Экран отдаёт человеку
    # review_id и unit_id, и подменить их в запросе может кто угодно. Без этой
    # проверки чужой номер удалил бы чужую фразу из общего словаря. Автора и номер
    # слова берём ЗАНОВО из базы и сверяем с тем, кто пришёл с ключом.
    unit_id, текст, автор, судьи, арбитр = _phrase_owner(review_id)
    if not unit_id or автор != int(user_id):
        logging.warning("проверка фраз: человек %s прислал решение по чужой фразе %s",
                        user_id, review_id)
        return ""

    if action == "keep":
        apply_phrase_review_decision(review_id, "keep")
        return "оставлено"

    if action == "edit":
        # Правки ВНУТРИ карточки: примеры и перевод. Это решение человека целиком —
        # что он оставил, то и ложится. На пересборку карточка при этом НЕ идёт.
        примеры = item.get("examples")
        if not isinstance(примеры, list):
            примеры = []
        чистые = [{"de": _cleaned(e.get("de") or ""), "ru": _cleaned(e.get("ru") or "")}
                  for e in примеры if isinstance(e, dict)]
        from backend.database import apply_panel_card_edit
        итог = apply_panel_card_edit(
            review_id, translation=_cleaned(item.get("translation") or ""),
            examples=[e for e in чистые if e["de"] and e["ru"]],
            top_up=bool(item.get("top_up")))
        return "карточка поправлена" if итог.get("unit_id") else ""

    if action == "rebuild":
        # «Пересобрать всю карточку ночью» — та же дверь, что у владельца.
        from backend.database import send_panel_card_to_rewrite
        итог = send_panel_card_to_rewrite(review_id)
        return "карточка на пересборку" if итог.get("unit_id") else ""

    if action == "fixed":
        # ⚠ НОМЕР КНОПКИ СЮДА НЕ ПРИХОДИТ И ПРИХОДИТЬ НЕ ДОЛЖЕН — см. рамку в
        # `кнопки_вариантов`. С экрана уезжает САМ ТЕКСТ нажатой кнопки, и он обязан
        # найтись среди тех кнопок, которые экран имел право показать. Не нашёлся —
        # значит либо список под рукой у человека сменился (ночь дописала судью), либо
        # текст подставлен мимо экрана. В обоих случаях применять нечего: молча взять
        # «похожий» вариант значит вернуть ровно тот дефект, который здесь чинится.
        # Сравниваем ТЕМ ЖЕ правилом, каким список кнопок отсеивает повторы
        # (`phrase_review_variants` → `_phrase_same_text`): разойдись эти два правила —
        # и кнопка, которую человек видел, перестанет находиться.
        from backend.database import _phrase_same_text
        нажато = _cleaned(item.get("variant_text") or "")
        разрешённые = кнопки_вариантов(судьи, текст, арбитр)
        if not нажато or not any(_phrase_same_text(нажато, v["text"]) for v in разрешённые):
            logging.warning(
                "проверка фраз: вариант %r не из показанных человеку %s по фразе %s "
                "(на экране было: %s)", нажато[:60], user_id, review_id,
                [v["text"][:40] for v in разрешённые])
            return "не применено"
        # Перевод к варианту берётся из самого варианта — человек читал его на кнопке.
        итог = apply_phrase_review_decision(review_id, "accept", "", 0, "",
                                            chosen_text=нажато)
        return "исправлено" if itog_text(итог) else "оставлено"

    if action == "manual":
        свой = _cleaned(item.get("text") or "")
        if not свой:
            return ""
        итог = apply_phrase_review_decision(
            review_id, "replace", свой, 0, _cleaned(item.get("translation") or ""))
        return "исправлено" if itog_text(итог) else "оставлено"

    if action == "drop":
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries "
                            "WHERE lex_unit_id=%s AND user_id<>%s;", (unit_id, int(user_id)))
                чужих = int((cur.fetchone() or [0])[0])
                cur.execute("DELETE FROM bt_3_webapp_dictionary_queries "
                            "WHERE lex_unit_id=%s AND user_id=%s;", (unit_id, int(user_id)))
                убрано = cur.rowcount or 0
                if чужих:
                    # Слово нужно ещё кому-то — общее не трогаем, а этому человеку
                    # фразу больше не показываем: строка дневника и есть его «нет».
                    cur.execute(
                        """INSERT INTO bt_3_word_confirm_digest
                                  (user_id, word, decision, closed_at)
                           VALUES (%s, %s, 'drop', NOW())
                           ON CONFLICT (user_id, word) DO UPDATE
                              SET decision='drop', closed_at=NOW();""",
                        (int(user_id), текст))
            conn.commit()
        logging.info("проверка фраз: человек %s удалил у себя «%s» (карточек %d, "
                     "осталось у других %d)", user_id, текст, убрано, чужих)
        if not чужих:
            apply_phrase_review_decision(review_id, "delete")
        return "удалено"

    return ""


def itog_text(итог: dict[str, Any]) -> str:
    """Текст, который в самом деле записан. Пустой — значит правка не применилась."""
    return str((итог or {}).get("text") or "")


def apply_decisions(user_id: int, decisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Применить решения человека.

    ╔═══════════════════════════════════════════════════════════════════════════╗
    ║  МОЛЧАНИЕ НЕ УДАЛЯЕТ. УДАЛЯЕТ ТОЛЬКО КНОПКА «УДАЛИТЬ».                     ║
    ║  Решение владельца 25.08.2026, отменяет его же правило от 19.08.2026       ║
    ║  («отмеченные остаются, остальные удаляются»).                             ║
    ║                                                                           ║
    ║  Дословно: «нельзя удалять просто потому что кто-то не увидел, может       ║
    ║  просмотрел случайно. Чтобы что-то удалить, человек должен САМ нажать      ║
    ║  удалить это слово».                                                       ║
    ║                                                                           ║
    ║  ПОЧЕМУ ЭТО ВЕРНО, А СТАРОЕ ПРАВИЛО БЫЛО НЕВЕРНО:                          ║
    ║  · список приходит на 12–100 слов, экран длинный, палец скользит —         ║
    ║    пропустить карточку это НОРМА ПОВЕДЕНИЯ, а не решение;                  ║
    ║  · цена ошибки несимметрична: лишнее сомнительное слово полежит и придёт   ║
    ║    на проверку снова, а стёртое нужное не вернуть ничем;                   ║
    ║  · удаление не оставляло СЛЕДА — ни записи, ни лога. Сработай оно, и мы    ║
    ║    не смогли бы даже сказать человеку, ЧТО у него пропало.                 ║
    ║                                                                           ║
    ║  Не отмеченное НИЧЕМ слово не получает строки в дневнике — и поэтому       ║
    ║  честно приходит на проверку в следующий раз. Это и есть «ничего не        ║
    ║  произошло», а не тихое согласие.                                          ║
    ╚═══════════════════════════════════════════════════════════════════════════╝

    keep    — слово верное, больше не спрашиваем
    fixed   — принял нашу подсказку: заголовок правится на предложенное написание
    manual  — вписал своё написание и/или свой перевод
    retrans — слово верное, перевод плохой: карточка пересобирается ночью
    drop    — ЯВНОЕ «удалить». Единственный путь к удалению строки словаря.
    (нет решения) — НЕ ДЕЛАЕМ НИЧЕГО. Слово вернётся на проверку.
    """
    from backend.database import get_db_connection_context
    counts = {"оставлено": 0, "исправлено": 0, "на пересборку": 0, "удалено": 0}
    if not decisions:
        return counts
    # «Перевод не тот» заводит жалобу, а она пишется своим соединением — после
    # транзакции слов, чтобы не вкладывать одну в другую.
    отложить_жалобу: list[str] = []
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                # Строки этого человека ищем по ГОЛОМУ слову: в словаре лежит
                # «die Abschiebu», а решение пришло про «Abschiebu».
                where_bare = ("user_id=%s AND "
                              + _BARE.format(col="word_de") + "=%s")
                # ┌─ ПРОВЕРЕНО 25.08.2026. СПИСОК «НАСТОЯЩИХ СЛОВ» ЗДЕСЬ БОЛЬШЕ ────┐
                # │ НЕ НУЖЕН И УБРАН НАМЕРЕННО. Он защищал от удаления молчанием    │
                # │ слова, подтверждённые моделью (решение владельца 21.08). С       │
                # │ 25.08 молчание не удаляет НИКОГО — защищать стало не от чего.    │
                # │ Признак `_model_confirmed` живёт дальше: он говорит человеку на  │
                # │ экране, что слово настоящее (см. audit_items → "safe").          │
                # └─────────────────────────────────────────────────────────────────┘
                for item in decisions:
                    # Фразы применяются ОТДЕЛЬНО и после — своей функцией, со своим
                    # соединением. Внутрь чужой транзакции её звать нельзя: она сама
                    # открывает соединение и коммитит, и вложение двух транзакций по
                    # одним и тем же таблицам кончается взаимной блокировкой.
                    if str(item.get("kind") or "") == "phrase":
                        continue
                    word = bare_word(item.get("word") or "")
                    action = str(item.get("action") or "").strip()
                    # Вписанное человеком идёт через ТУ ЖЕ чистку, что и все остальные
                    # пути записи (`dictionary_intake.clean_text`): невидимые символы,
                    # хвостовая пунктуация, двойные пробелы, кавычки-ёлочки. Своё поле
                    # ввода — такой же вход, как модель или ярлык, и поблажки ему нет.
                    text = bare_word(_cleaned(item.get("text") or ""))
                    if not word:
                        continue
                    translation = _cleaned(item.get("translation") or "")
                    if action in ("fixed", "manual"):
                        # ┌─ ПОЧИНЕНО 25.08.2026. ЭТА ВЕТКА НЕ ИМЕЕТ ПРАВА УДАЛЯТЬ. ──┐
                        # │ Раньше «свой вариант» проверялся условием «слово изменилось»│
                        # │ — и решение, где человек поправил ТОЛЬКО перевод, проваливалось│
                        # │ мимо всех ветвей прямо в удаление строки. Дорога туда открыта│
                        # │ экраном: поле слова предзаполнено самим словом, а поле перевода│
                        # │ подписано «если и он не тот». Отмеченное человеком слово не  │
                        # │ удаляется НИ ПРИ КАКОМ решении — удаляет только молчание.    │
                        # └─────────────────────────────────────────────────────────────┘
                        if text and text != word:
                            # Слово изменилось — значит меняется ВСЯ карточка, а не
                            # заголовок: старый разбор про другое слово и уходит целиком.
                            # Подробности и история дефекта — в _rewrite_card_to_new_word.
                            spread = _rewrite_card_to_new_word(
                                cur, user_id=int(user_id), old_bare=word,
                                new_word_de=_with_article(text), new_translation=translation)
                            logging.info(
                                "проверка слов: %s → %s (человек %s): карточек %d, "
                                "снято из пула %d, чужих граф не тронуто %d",
                                word, text, user_id, spread["карточек"],
                                spread["снято из пула"], spread["чужих граф не тронуто"])
                            # Исправленное написание — снова через дверь, уже без спешки.
                            cur.execute("DELETE FROM bt_3_word_check WHERE asked=%s", (text,))
                            counts["исправлено"] += 1
                        elif translation:
                            # Слово то же, поправлен только перевод: разбор про это же
                            # слово, стирать его нечего. Перевод собственный, спорить с ним
                            # нечем — человек видел исходный текст, а мы нет.
                            cur.execute(
                                "UPDATE bt_3_webapp_dictionary_queries SET translation_ru=%s, "
                                "updated_at=NOW() WHERE " + where_bare,
                                (translation, int(user_id), word))
                            counts["исправлено"] += 1
                        else:
                            counts["оставлено"] += 1
                    elif action == "keep":
                        counts["оставлено"] += 1
                    elif action == "retrans":
                        # ┌─ ПОЧИНЕНО 26.08.2026. ЭТА КНОПКА БЫЛА ЗАГЛУШКОЙ. ────────────┐
                        # │ Она писала строку с decision='retrans' и closed_at=NOW() и    │
                        # │ обещала человеку «карточку соберём этой ночью». Строку НЕ     │
                        # │ ЧИТАЛ НИКТО (проверено grep'ом по backend/ и scripts/), а     │
                        # │ closed_at закрывал слово навсегда: нажал → пообещали →        │
                        # │ ничего не сделали → слово ушло из очереди с плохим переводом. │
                        # │ Нажатий за всё время было ноль, чинить накопленное не         │
                        # │ пришлось. Теперь нажатие заводит ЖАЛОБУ: ночью её судит       │
                        # │ модель, пачка уходит владельцу, решает он, а человеку         │
                        # │ приходит ответ. Решение владельца 26.08.2026.                 │
                        # └──────────────────────────────────────────────────────────────┘
                        отложить_жалобу.append(word)
                        counts["на пересборку"] += 1
                    elif action == "drop":
                        # ЕДИНСТВЕННОЕ МЕСТО, ГДЕ СЛОВО ЧЕЛОВЕКА УДАЛЯЕТСЯ.
                        # Удаление обязано оставлять след: раньше строка исчезала
                        # бесшумно, и сказать человеку, ЧТО у него пропало, было
                        # нечем. Сначала читаем, что уносим, потом уносим, потом
                        # пишем в дневник — строка дневника и есть след.
                        cur.execute(
                            "SELECT id, word_de, COALESCE(translation_ru,'') "
                            "FROM bt_3_webapp_dictionary_queries WHERE " + where_bare,
                            (int(user_id), word))
                        уносим = cur.fetchall() or []
                        cur.execute("DELETE FROM bt_3_webapp_dictionary_queries "
                                    "WHERE " + where_bare, (int(user_id), word))
                        for card_id, word_de, перевод in уносим:
                            logging.info("проверка слов: человек %s удалил карточку %s "
                                         "«%s» — «%s»", user_id, card_id, word_de, перевод)
                        counts["удалено"] += len(уносим)
                    # НИКАКОГО else. Слово, не отмеченное ничем, не трогаем и в дневник
                    # не пишем — оно придёт на проверку снова. См. рамку в шапке функции.
                    if action in ("keep", "fixed", "manual", "drop"):
                        cur.execute(
                            """INSERT INTO bt_3_word_confirm_digest
                                      (user_id, word, decision, closed_at)
                               VALUES (%s, %s, %s, NOW())
                               ON CONFLICT (user_id, word) DO UPDATE
                                  SET decision=EXCLUDED.decision, closed_at=NOW();""",
                            (int(user_id), word, action))
            conn.commit()
    except Exception:
        # ┌─ ПОЧИНЕНО 26.08.2026. ЗДЕСЬ ПРОСТО ПИСАЛСЯ ЛОГ И ШЛИ ДАЛЬШЕ. ────────────┐
        # │ Человек нажимал «Готово», решения не применялись, а экран рапортовал     │
        # │ «Готово» с нулевыми счётчиками — неотличимо от «ты ничего не отметил».   │
        # │ Наверх: эндпоинт скажет «Не удалось сохранить, попробуй ещё раз», и      │
        # │ отмеченное останется отмеченным.                                         │
        # └─────────────────────────────────────────────────────────────────────────┘
        logging.warning("экран проверки: решения не применены для %s", user_id, exc_info=True)
        raise

    # «Перевод не тот» → жалоба. Слово при этом НЕ закрывается: закроет его решение
    # владельца по жалобе, а не сам факт нажатия.
    if отложить_жалобу:
        from backend.card_complaints import add_complaint
        for слово in отложить_жалобу:
            try:
                add_complaint(user_id=int(user_id), word=слово,
                              note="человек отметил на проверке слов: перевод не тот")
            except Exception:
                logging.warning("проверка слов: жалоба на %r не заведена", слово, exc_info=True)

    # Фразы — после слов и по одной. Сорвалась одна, остальные обязаны примениться:
    # человек нажал кнопки на всём экране, и терять его работу целиком из-за одной
    # строки нельзя. Каждый срыв виден в логе поимённо, а не растворяется в общем
    # «не применилось».
    for item in decisions:
        if str(item.get("kind") or "") != "phrase":
            continue
        try:
            счётчик = _apply_phrase_decision(int(user_id), item)
        except Exception:
            logging.warning("проверка фраз: решение не применено (человек %s, фраза %s)",
                            user_id, item.get("word"), exc_info=True)
            continue
        if счётчик:
            counts[счётчик] = counts.get(счётчик, 0) + 1
    return counts


# ── Напоминание в личку: два раза в неделю ──────────────────────────────────
def _склонение(count: int, one: str, few: str, many: str) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return one
    if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14:
        return few
    return many


def _reminder_text(count: int, phrases: int = 0,
                   answers: list[dict[str, Any]] | None = None) -> str:
    """Текст без ребусов: что, откуда, зачем, что будет, как делать, сколько.

    Владелец 20.08.2026: «в сообщении всё очень детально и без ребусов описать: что это
    за слова, откуда они появились, зачем мы просим их проверить, что будет, если не
    проверить, механику проверки, сколько делать, что потом происходит».

    ⚠ ЗАГОЛОВОК ОБЯЗАН НАЗЫВАТЬ ТО, ЧТО ЧЕЛОВЕК УВИДИТ. Письмо «5 слов ждут проверки»
    над экраном, где лежат пять ФРАЗ, — тот же обман, что и письмо над пустым экраном.
    """
    # Ответ на жалобу — первое, что человек должен прочитать: он его ЖДЁТ. Если ответ
    # есть, а спрашивать больше не о чем, письмо состоит только из ответа.
    ответы = [о for о in (answers or []) if isinstance(о, dict)]
    блок_ответов = ""
    if ответы:
        строки = "\n".join(
            f"• <b>{о.get('word', '')}</b> — {о.get('result') or 'разобрали'}"
            for о in ответы[:20])
        блок_ответов = (
            "🦊 <b>Разобрали, на что ты жаловался</b>\n\n" + строки +
            "\n\n<i>Спасибо: это чинит карточку не только тебе, а всем, кто учит "
            "это слово.</i>"
        )
    всего = int(count) + int(phrases)
    if not всего and блок_ответов:
        return блок_ответов
    предисловие = (блок_ответов + "\n\n———\n\n") if блок_ответов else ""
    if phrases and count:
        шапка = f"{всего} {_склонение(всего, 'запись', 'записи', 'записей')} в твоём словаре ждут проверки"
    elif phrases:
        шапка = (f"{phrases} {_склонение(phrases, 'фраза', 'фразы', 'фраз')} "
                 f"в твоём словаре {_склонение(phrases, 'ждёт', 'ждут', 'ждут')} проверки")
    else:
        шапка = f"{count} {_склонение(count, 'слово', 'слова', 'слов')} в твоём словаре ждут проверки"
    про_фразы = (
        "\n\n<b>Про фразы.</b> Сохранённые фразы ночью читают два проверяющих. Если они "
        "нашли ошибку или разошлись во мнении — фраза приходит на этот же экран, с готовым "
        "вариантом, если он есть. Молча за тебя мы ничего не переписываем."
    ) if phrases else ""
    return (
        f"{предисловие}🦊 <b>{шапка}</b>\n\n"
        "<b>Что это за слова.</b> Это слова, которые ты сам сохранил. Каждое сохранённое "
        f"слово мы сверяем с немецкими справочниками — эти не нашлись.{про_фразы}\n\n"
        "<b>Почему так вышло.</b> Причины бывают разные: слово редкое и его нет в "
        "справочнике; слово из другого языка; при сохранении потерялась буква — так "
        "бывает, когда текст распознаётся с картинки.\n\n"
        "<b>Зачем проверять.</b> Если слово с ошибкой останется, ты будешь учить его "
        "в таком виде и запомнишь неправильно. Мы не удаляем ничего сами — решаешь "
        "только ты, и только кнопкой.\n\n"
        "<b>Как это работает.</b> Откроется экран со списком. У каждого слова написано, "
        "почему оно там. Если мы догадались, как оно пишется правильно, будет готовая "
        "кнопка «Да, это …» — одно касание. Можно оставить слово как есть, можно "
        "попросить переделать перевод, можно вписать правильное написание руками, "
        "а можно удалить слово — кнопкой «Удалить».\n\n"
        "<b>Ничего не удалится само.</b> Слово уходит из словаря, только если ты сам "
        "нажал у него «Удалить». Всё, что ты не тронул, остаётся на месте и просто "
        "придёт на проверку в следующий раз.\n\n"
        "<b>Сколько это займёт.</b> Меньше минуты на слово. Можно разобрать часть и "
        "вернуться позже — непроверенные придут снова.\n\n"
        "<b>Что потом.</b> Карточки достроим сами: часть речи, род, формы. "
        "Подтверждённые слова больше не спросим."
    )


def send_word_audit_reminders(*, force: bool = False) -> dict[str, Any]:
    """Разослать напоминания всем, у кого накопились неподтверждённые слова.

    Отправка идёт через `telegram_delivery`, а не своим `requests.post`: Telegram
    отвечает на отказ не исключением, а телом `{"ok": false, "description": …}`,
    и своя отправка печатала «доставлено» при мёртвом токене. Ровно из-за этого
    владелец месяц не видел отчёт о фразах (разбор 20.08.2026).
    """
    from datetime import datetime, timezone
    from backend.database import (
        claim_scheduler_run_guard, finish_scheduler_run_guard, get_db_connection_context,
    )
    from backend.telegram_delivery import send_telegram_message

    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global", metadata={}):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}

    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME") or ""
    if not token or not bot_username:
        return {"ok": False, "error": "no_token_or_username"}

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                # Получатели — те, у кого есть СВОИ сомнительные сохранения: слова, не
                # подтверждённые дверью, и фразы, отложенные ночной проверкой.
                cur.execute(
                    """
                    SELECT q.user_id, COUNT(DISTINCT {bare})
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                     WHERE w.status IN ('не подтверждено', 'не слово')
                       AND {только_слова}
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     GROUP BY q.user_id;
                    """.format(bare=_BARE.format(col="q.word_de"),
                             только_слова=_ТОЛЬКО_СЛОВА.format(
                                 bare=_BARE.format(col="q.word_de")))
                )
                слов: dict[int, int] = {int(a): int(b) for a, b in (cur.fetchall() or [])}
                # Фразы — тем же счётом, что и на экране (см. _phrase_counts_by_author).
                фраз: dict[int, int] = _phrase_counts_by_author(cur)
                targets = [(uid, слов.get(uid, 0), фраз.get(uid, 0))
                           for uid in sorted(set(слов) | set(фраз))
                           if слов.get(uid, 0) + фраз.get(uid, 0) > 0]
    except Exception:
        # ┌─ ПОЧИНЕНО 26.08.2026. ЗДЕСЬ БЫЛО `targets = []`. ────────────────────────┐
        # │ Пустой список получателей вёл в строку ниже: статус «completed», если    │
        # │ «доставлено ИЛИ получателей нет». То есть сбой базы в момент рассылки     │
        # │ выглядел как «сегодня никому не нужно было писать», и владелец не узнавал │
        # │ об этом никогда. Теперь прогон честно помечается провалившимся и          │
        # │ повторится: письма не уходят, но и «выполнено» никто не рисует.           │
        # └─────────────────────────────────────────────────────────────────────────┘
        logging.warning("напоминание о словах: не собрал получателей", exc_info=True)
        if not force:
            finish_scheduler_run_guard(
                job_key=JOB_KEY, run_period=run_period, target_scope="global",
                status="failed", metadata={"ошибка": "получатели не собраны"})
        return {"ok": False, "error": "получатели не собраны"}

    # Ответы на жалобы едут в том же письме — нового канала не строим. Человек с одним
    # только ответом тоже получает письмо: он нажал кнопку и ждёт, чем кончилось.
    from backend.card_complaints import answers_by_user, mark_told
    try:
        ответы = answers_by_user()
    except Exception:
        logging.warning("напоминание о словах: ответы на жалобы не прочитаны", exc_info=True)
        ответы = {}
    известные = {uid for uid, _w, _p in targets}
    targets += [(uid, 0, 0) for uid in sorted(ответы) if uid not in известные]

    link = f"https://t.me/{bot_username}?startapp=woerter"
    delivered = 0
    failures: list[tuple[int, str]] = []
    for user_id, слов_у_него, фраз_у_него in targets:
        мои_ответы = ответы.get(user_id) or []
        ok, reason = send_telegram_message(
            chat_id=user_id,
            text=_reminder_text(слов_у_него, фраз_у_него, мои_ответы), token=token,
            reply_markup={"inline_keyboard": [[{"text": "Открыть проверку", "url": link}]]},
            what="напоминание о проверке слов")
        if ok:
            delivered += 1
            # Отметка «сказали» — только после того, как письмо вправду ушло.
            mark_told([о["id"] for о in мои_ответы])
        else:
            failures.append((user_id, reason))

    if not force:
        # «Выполнено» ставится по ФАКТУ доставки, а не по факту отправки.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if (delivered or not targets) else "failed",
            metadata={"получателей": len(targets), "доставлено": delivered,
                      "отказы": [f"{uid}: {why}" for uid, why in failures[:20]]})
    return {"ok": True, "получателей": len(targets), "доставлено": delivered,
            "отказы": failures}


def suggestion_for(word: str) -> str:
    """Готовая подсказка написания, если она уже посчитана ночью. Иначе пусто.

    В сеть и к модели отсюда не ходим: плашка показывается человеку, который ЖДЁТ,
    и заставлять его ждать модель нельзя. Нет подсказки — плашка просто предложит
    вписать свой вариант.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT suggestion FROM bt_3_word_suggestion WHERE asked=%s;",
                            (bare_word(word),))
                row = cur.fetchone()
                return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""
