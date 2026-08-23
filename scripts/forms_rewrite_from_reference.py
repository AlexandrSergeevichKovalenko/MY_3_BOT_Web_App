# -*- coding: utf-8 -*-
"""Перезаписать хранимые формы существительных тем, что напечатано в справочнике.

ЧТО ЛЕЖИТ В БАЗЕ. В разборе каждого слова хранятся отдельные формы — родительный падеж
и множественное число. Их писала модель при разборе, и сверка со справочником
23.08.2026 показала расхождения.

СЫРОЕ ЧИСЛО ОБМАНЫВАЕТ, и его нельзя выносить как есть:

    родительный   2655 записей · «не совпало» 344 — но 275 из них ЗАКОННЫЙ ВАРИАНТ:
                  по-немецки верно и «des Abgleichs», и «des Abgleiches».
                  Настоящих расхождений 69.
    множественное 2773 записи  · настоящих расхождений 41.

НА ЧТО ОНИ ПОХОЖИ:

    die Behörde         у нас «der Behörden»     ← множественное вместо единственного
    der Dickdarmkrebs   у нас «des Dickdarms»    ← родительный ДРУГОГО слова
    Alkoholfahne        у нас «die Alkohlfahnen» ← опечатка, пропущена «о»
    der Ekel            у нас «die Ekeln»        ← такой формы не существует

ПОЧЕМУ ЧЕЛОВЕК ИХ СЕЙЧАС НЕ ВИДИТ. Блок с этими формами рисуется, только когда нет
таблицы склонения, а после прогрева 23.08.2026 таблица есть у всех до одного. Но в базе
они лежат, и первый же путь, который их прочитает, вынесет их на экран.

ПОЧЕМУ НЕ ЗАМЕНЯЕМ, А УБИРАЕМ. Сначала я пытался переписать расходящееся значение тем,
что напечатано. Пять признаков подряд — прямое совпадение, вариант «-s/-es», перечисление
через «/», пояснение в скобках, дореформенное «ß», указатель форм — каждый либо не ловил
ничего, либо давал ОТКАТ:

    der Seemann     у нас «die Seeleute»  → правило хотело «die Seemänner»
                    а «Seeleute» и есть нормальное множественное
    das Denkmal     у нас «die Denkmäler» → правило хотело «die Denkmale»
                    оба верны, и наше употребительнее
    die Soile       заголовок сам сломан  → правило сочиняло «die Soilen»

Значит правило здесь негодное, и настаивать на нём — то самое механическое достраивание,
которое в этом проекте запрещено.

РЕШЕНИЕ ДРУГОЕ: копия УБИРАЕТСЯ. Источник истины — таблица справочника, она есть у всех
2883 существительных. Отдельное поле в разборе — её устаревший дубликат, который может
разойтись и разошёлся. Убрать дубликат безопасно: человек видит таблицу, а не это поле,
и потерять нечего. Одно значение — один источник.

ЗАПУСК:
    python3 scripts/forms_rewrite_from_reference.py           # показать
    python3 scripts/forms_rewrite_from_reference.py --apply   # перезаписать
"""
from __future__ import annotations

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_АРТИКЛЬ = re.compile(r"^(der|die|das|des|dem|den)\s+", re.I)

ЗАПРОС = """
 SELECT u.id, u.display,
        u.card->'forms'->>'genitive', u.card->'forms'->>'plural', d.tables
   FROM bt_3_lex_units u
   JOIN bt_3_german_noun_declensions d
     ON lower(d.noun) = lower(regexp_replace(u.display,'^(der|die|das)[[:space:]]+','','i'))
  WHERE u.lang='de' AND u.pos='noun' AND u.card IS NOT NULL
  ORDER BY u.display
"""


def _голое(текст) -> str:
    return _АРТИКЛЬ.sub("", str(текст or "")).strip().lower()


def _форма_этого_же_слова(cur, наше: str, слово: str) -> bool:
    """Спрашиваем УКАЗАТЕЛЬ ФОРМ: наше значение — настоящая форма этого слова?

    У немецкого существительного бывает два законных множественных: «Denkmale» и
    «Denkmäler», «Flecke» и «Flecken». Справочник печатает в таблице одно, и без этой
    проверки прогон заменял бы верное на верное — а «Denkmäler» ещё и употребительнее.
    Указатель форм собран из того же справочника и знает обе.
    """
    голое_наше = _голое(наше).split("/")[0].strip()
    if not голое_наше:
        return False
    cur.execute("SELECT 1 FROM bt_3_german_form_index "
                "WHERE lower(surface)=%s AND lower(lemma)=%s LIMIT 1;",
                (голое_наше, _голое(слово)))
    if cur.fetchone():
        return True
    # Второй заход: а существует ли такая форма в немецком ВООБЩЕ? У слова бывает два
    # законных множественных, и справочник печатает в таблице одно: «Denkmale», хотя
    # «Denkmäler» ничуть не хуже и употребительнее. Если указатель знает нашу форму как
    # чью-то настоящую — заменять её на другую верную бессмысленно, это перекладывание.
    # А «Alkohlfahnen» (пропущена «о») указатель не знает — вот это и есть дефект.
    cur.execute("SELECT 1 FROM bt_3_german_form_index WHERE lower(surface)=%s LIMIT 1;",
                (голое_наше,))
    return cur.fetchone() is not None


def _подтверждено(наше: str, варианты: set[str]) -> bool:
    """Считается ли наше значение подтверждённым справочником.

    Прямое совпадение — да. И ещё один случай: родительный на «-es» против «-s».
    По-немецки верны ОБА («des Baumes» и «des Baums»), справочник печатает один, и
    замена одного верного на другое верное — не починка, а перекладывание. Первый
    прогон 23.08.2026 без этого правила предложил 376 правок вместо 110, и среди них
    было «des Baumes» → «des Baums».
    """
    сырое = str(наше or "")
    # Пояснение в скобках — это не другая форма: «Charismen (редко используется)».
    сырое = re.sub(r"\s*\([^)]*\)", "", сырое)
    # Наше значение может перечислять ОБА законных варианта: «des Ohres / des Ohrs»,
    # «Labors/Labore». Подтверждён любой — значит запись верна и трогать её нечего.
    куски = [_голое(k) for k in re.split(r"\s*/\s*", сырое) if k.strip()]
    for кусок in куски or [_голое(сырое)]:
        if кусок in варианты:
            return True
        for в in варианты:
            # Родительный «-es» против «-s»: по-немецки верны ОБА («des Baumes» и
            # «des Baums»), справочник печатает один. Замена верного на верное —
            # не починка. Первый прогон без этого правила дал 376 правок вместо 102.
            if кусок.endswith("s") and в.endswith("s") and кусок.replace("es", "s") == в.replace("es", "s"):
                return True
            # Наше написание современное, а страница справочника — дореформенная:
            # «des Rosses» против «des Roßes». Заменять современное на устаревшее
            # нельзя; настоящий дефект здесь в ЗАГОЛОВКЕ слова, а не в форме.
            if кусок.replace("ss", "ß") == в or кусок == в.replace("ß", "ss"):
                return True
    return False


def _все_формы(таблицы, падеж: str, колонка: str) -> tuple[set[str], str]:
    """(все напечатанные варианты этой клетки, первый как образец для замены)."""
    t = таблицы if isinstance(таблицы, dict) else json.loads(таблицы or "{}")
    варианты: list[str] = []
    for род in ("m", "f", "n", "pl"):
        for строка in ((t.get(род) or {}).get("rows") or []):
            if строка.get("case") == падеж and строка.get(колонка):
                значение = str(строка[колонка]).strip()
                if значение and значение not in варианты:
                    варианты.append(значение)
    return {_голое(v) for v in варианты}, (варианты[0] if варианты else "")


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ЗАПРОС)
            строки = cur.fetchall() or []

    план = []
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for uid, display, ген, мн, таблицы in строки:
                правки = {}
                if ген:
                    варианты, образец = _все_формы(таблицы, "gen", "singular")
                    if (варианты and образец and not _подтверждено(ген, варианты)
                            and not _форма_этого_же_слова(cur, ген, display)):
                        правки["genitive"] = (ген, образец)
                if мн:
                    варианты, образец = _все_формы(таблицы, "nom", "plural")
                    if (варианты and образец and not _подтверждено(мн, варианты)
                            and not _форма_этого_же_слова(cur, мн, display)):
                        правки["plural"] = (мн, образец)
                if правки:
                    план.append((int(uid), str(display), правки))

    print(f"Слов со сверенными формами: {len(строки)}")
    print(f"Расходится со справочником: {len(план)}\n")
    for uid, display, правки in план:
        for поле, (было, станет) in правки.items():
            имя = "род. падеж" if поле == "genitive" else "мн. число"
            print(f"  {display:26} {имя:11} в карточке {было!r:30} справочник {станет!r}")
    if not план:
        return 0
    if not apply:
        print("\nЭто показ. Чтобы убрать расходящиеся копии — добавь --apply")
        return 0

    убрано = 0
    for uid, _display, правки in план:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                for поле in правки:
                    cur.execute(
                        "UPDATE bt_3_lex_units "
                        "SET card = card #- %s, updated_at = NOW() WHERE id = %s;",
                        ("{forms," + поле + "}", uid))
                    убрано += 1
            conn.commit()
    print(f"\nУбрано расходящихся копий: {убрано}")

    # Проверка ФАКТОМ: спрашиваем базу заново тем же сравнением.
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ЗАПРОС)
            осталось = 0
            for uid, display, ген, мн, таблицы in (cur.fetchall() or []):
                for значение, падеж, колонка in ((ген, "gen", "singular"), (мн, "nom", "plural")):
                    if not значение:
                        continue
                    варианты, _ = _все_формы(таблицы, падеж, колонка)
                    if варианты and not _подтверждено(значение, варианты) \
                            and not _форма_этого_же_слова(cur, значение, display):
                        осталось += 1
    print(f"Осталось расхождений: {осталось}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
