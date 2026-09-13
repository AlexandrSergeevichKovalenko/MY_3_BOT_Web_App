#!/usr/bin/env python3
"""Заголовок-форма → словарное слово: карточки людей и общий словарь.

Решение владельца 07.08.2026 и 18.08.2026: **заголовок всегда словарная форма**, как у
Linguee и Reverso. 13.09.2026 он подтвердил его для нового списка (и глагольные формы, и
множественное число) и отдельно решил: **перевод чинится сразу вместе с заголовком**,
а не откладывается на ночь.

ЧТО ЧИНИМ. Только подтверждённое СПРАВОЧНИКОМ de.wiktionary
(`scripts/dict_form_headword_audit.py` → `scripts/data/form_headwords.json`, класс
«форма»). Внутренние источники для этого не годятся: указатель `bt_3_lex_surfaces`
считает формой «arbeiten», «schildern», «die Brühe» — прогон 13.09.2026 дал 95 таких
законных слов против 127 настоящих форм.

⛔ ЧЕТЫРЕ КАПКАНА, найденные ДО первой записи (13.09.2026). Все закрыты здесь, и
снимать защиту нельзя:

1. РЕГИСТР. «anmachen» (глагол «включать») и «Anmachen» (форма от «die Anmache») —
   разные слова. Поиск по lower() склеивал их, и правка переименовала бы глагол.
   Здесь сравнение точное.
2. НАША ЗАГЛАВНАЯ ≠ НЕМЕЦКАЯ. Старый импорт записал глаголы с заглавной: у нас «Hüten»
   это «охранять», а справочник знает такое написание только как форму от «Hut» (шляпа).
   Правка сделала бы из глагола шляпу — 24 карточки. Поэтому карточка идёт в работу
   ТОЛЬКО если её собственная часть речи не спорит с целью (`_спорная`).
3. РЕКУРСИВНАЯ ЗАМЕНА. `database.spread_correction_everywhere` меняет текст и в примерах,
   а там форма стоит законно («Die Behörden haben entschieden»). Здесь не используется:
   правим ровно заголовок, а разбор снимаем.
4. ПУСТОЙ РАЗБОР. `response_json = '{}'` — не то же самое, что «нет разбора»: ночной
   добор ищет по другому признаку, а экран карточки открывается белым. Снимаем тело
   разбора штатным `_strip_dictionary_card_body`, опознание остаётся.

ЕДИНИЦЫ СЛОВАРЯ ЗДЕСЬ НЕ ТРОГАЮТСЯ. Переименование и слияние единиц — работа ночной
двери слова (`german_word_gate.warm_word_gate`), у неё для этого есть и проверка, и
перенос связей. Наш скрипт отвечает за то, что видит человек: его карточку и общий
словарь.

    python3 scripts/dict_fix_form_headwords.py                 # сухой прогон, список
    python3 scripts/dict_fix_form_headwords.py --apply         # записать
    python3 scripts/dict_fix_form_headwords.py --only wirbt    # одно написание
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

AUDIT = "scripts/data/form_headwords.json"
ARTICLES = ("der ", "die ", "das ")
# Части речи, при которых «форма существительного» — не про нашу карточку: она про глагол,
# наречие или предлог, записанный с заглавной буквы (капкан 2).
# Часть речи в базе лежит и по-английски, и по-русски — на живых данных 13.09.2026
# встретились обе записи («preposition» и «предлог»), и по одному языку защита
# пропускала карточку.
НЕ_СУЩЕСТВИТЕЛЬНОЕ = {
    "verb", "adverb", "preposition", "participle", "conjunction", "particle",
    "глагол", "наречие", "предлог", "причастие", "союз", "частица", "междометие",
}


def _bare(text: str) -> str:
    t = str(text or "").strip()
    return t.split(" ", 1)[1].strip() if t.lower().startswith(ARTICLES) else t


def _headword_for(cur, lemma: str) -> tuple[str, str]:
    """(как записать заголовок, откуда артикль). Существительному артикль нужен, и берём
    его ТОЛЬКО из справочника: догадка по окончанию здесь запрещена.

    Сперва — готовая словарная строка из таблицы склонений: у субстантивированных
    прилагательных склейка «артикль + база» даёт несуществующее «der Degenerierter»
    (поймано разбором 13.09.2026, в таблице лежит верное «der Degenerierte»)."""
    from backend.article_authority import authoritative_article
    bare = _bare(lemma)
    if not bare[:1].isupper():
        return bare, ""
    # Колонка называется `tables` и хранит таблицы склонения целиком; читаем её ТЕМ ЖЕ
    # разбором, что и продукт (`noun_declension_reference`), своего парсера не заводим.
    cur.execute("SELECT tables FROM bt_3_german_noun_declensions WHERE lower(noun) = %s LIMIT 1",
                (bare.casefold(),))
    row = cur.fetchone()
    готовое = ""
    if row and row[0]:
        таблицы = row[0] if isinstance(row[0], dict) else {}
        for ключ in ("singular", "Singular", "sg"):
            блок = таблицы.get(ключ) if isinstance(таблицы, dict) else None
            if isinstance(блок, dict):
                for падеж in ("nominativ", "Nominativ", "nom"):
                    значение = str(блок.get(падеж) or "").strip()
                    if значение:
                        готовое = значение
                        break
            if готовое:
                break
    if готовое.lower().startswith(ARTICLES):
        return готовое, "справочник склонений"
    article, source = authoritative_article(bare)
    return (f"{article} {bare}" if article else bare), (source or "")


def _translation_for(cur, lemma: str) -> tuple[str, str]:
    """Перевод СЛОВАРНОГО слова: от бесплатного источника к дорогому. ('', '') — никто не
    знает; тогда перевод не трогаем и считаем в «не смогли»."""
    bare = _bare(lemma)
    cur.execute("SELECT card->>'translation_ru', card->>'word_ru' FROM bt_3_lex_units "
                "WHERE lang='de' AND lemma = %s AND card IS NOT NULL LIMIT 1", (bare,))
    row = cur.fetchone()
    if row and (row[0] or row[1]):
        return str(row[0] or row[1]).strip(), "разбор слова"
    cur.execute("SELECT target_text FROM bt_3_dictionary_entries "
                "WHERE source_lang='de' AND target_lang='ru' "
                "AND regexp_replace(source_text,'^(der|die|das) ','') = %s "
                "AND COALESCE(target_text,'') <> '' ORDER BY updated_at DESC NULLS LAST LIMIT 1",
                (bare,))
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip(), "общий словарь"
    for таблица, имя in (("bt_base_dictionary", "FreeDict"), ("bt_wiktionary_dictionary", "Wiktionary")):
        try:
            cur.execute(f"SELECT translations_ru FROM {таблица} WHERE lemma = %s "
                        "AND array_length(translations_ru, 1) > 0 LIMIT 1", (bare,))
            row = cur.fetchone()
        except Exception:
            row = None
        if row and row[0]:
            return ", ".join(list(row[0])[:3]), имя
    return "", ""


def _load_forms(only: str = "") -> dict[str, str]:
    with open(AUDIT, encoding="utf-8") as f:
        data = json.load(f)
    forms = {w: r["база"] for w, r in data["ответы"].items()
             if r.get("класс") == "форма" and r.get("база")}
    if only:
        forms = {w: b for w, b in forms.items() if w.lower() == only.strip().lower()}
    return forms


def _корни(текст: str) -> set[str]:
    """Грубые основы русских слов — только чтобы заметить, что смысл РАЗОШЁЛСЯ."""
    корни = set()
    for слово in str(текст or "").lower().replace(",", " ").replace(";", " ").split():
        чистое = "".join(ch for ch in слово if ch.isalpha())
        if len(чистое) >= 4:
            корни.add(чистое[:5])
    return корни


def _спорная(карточка: dict, новый_заголовок: str, новый_перевод: str = "") -> str:
    """'' — чинить можно; иначе причина, по которой карточка уходит владельцу."""
    pos = str(карточка.get("pos") or "").strip().lower()
    старый_ru = str(карточка.get("ru") or "")
    # Написание может быть И формой существительного, И законным глаголом со строчной:
    # «Schleifen» — множественное от «die Schleife» (бант), а «schleifen» — «шлифовать»,
    # и у человека в карточке стоит «шлифование». Признак берём у СПРАВОЧНИКА (передаётся
    # аргументом), а не по виду перевода: «замечание», «удобство» тоже кончаются на -ние,
    # и правило по окончанию резало здоровые карточки. Поймано 13.09.2026 на карточке
    # 328240, где часть речи ни о чём не спорила.
    if карточка.get("строчный_глагол"):
        return f"строчное «{карточка['строчный_глагол']}» — законный глагол, карточка может быть про него"
    if not _bare(новый_заголовок)[:1].isupper():
        return ""                       # цель не существительное — капкан 2 не про нас
    if pos in НЕ_СУЩЕСТВИТЕЛЬНОЕ:
        return f"у нас это {pos}, а целевое слово — существительное"
    # ⛔ ПРАВИЛО «русское слово кончается на -ть» СНЯТО 13.09.2026, НЕ ВОЗВРАЩАТЬ.
    # Оно казалось признаком глагола, а поймало «Зять» и «Обязательность» — шесть
    # здоровых карточек ушли бы в спорные. Признак глагола здесь ровно один и точный:
    # строчный вариант написания, подтверждённый справочником (выше).
    return ""


_СТРОЧНЫЕ_ОТВЕТЫ: dict[str, str] = {}


def _прогреть_строчные(написания: list[str], log=print) -> None:
    """Спросить справочник про строчные варианты ПАЧКОЙ и разом, ДО работы с базой.

    Сеть внутри открытой транзакции рвёт соединение (13.09.2026: «SSL connection has been
    closed unexpectedly» на середине прогона), поэтому все походы наружу — заранее."""
    from backend.german_form_headword import headword_kinds
    нужны = sorted({_bare(w)[:1].lower() + _bare(w)[1:] for w in написания
                    if _bare(w)[:1].isupper()})
    нужны = [w for w in нужны if w not in _СТРОЧНЫЕ_ОТВЕТЫ]
    for i in range(0, len(нужны), 45):
        пачка = нужны[i:i + 45]
        ответы = headword_kinds(пачка) or {}
        for w in пачка:
            вид = str((ответы.get(w) or {}).get("kind") or "")
            _СТРОЧНЫЕ_ОТВЕТЫ[w] = w if вид == "word" else ""
        log(f"  справочник (строчные): {min(i + 45, len(нужны))} из {len(нужны)}")


def _строчный_глагол(surface: str) -> str:
    """Написание с заглавной, чей строчный вариант — законное немецкое слово (обычно
    глагол): карточка человека может быть про него, а не про существительное."""
    bare = _bare(surface)
    if not bare[:1].isupper():
        return ""
    строчное = bare[:1].lower() + bare[1:]
    return _СТРОЧНЫЕ_ОТВЕТЫ.get(строчное, "")


def _plan_one(cur, surface: str, lemma: str) -> dict:
    from backend import lex_units
    точное = _bare(surface)
    новый, откуда_артикль = _headword_for(cur, lemma)
    перевод, откуда_перевод = _translation_for(cur, lemma)
    строчный = _строчный_глагол(surface)

    cur.execute("""
        SELECT q.id, q.word_de, q.user_id, COALESCE(q.word_ru, q.translation_ru, ''),
               COALESCE(u.pos, ''), q.source_lang, q.target_lang
          FROM bt_3_webapp_dictionary_queries q
          LEFT JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
         WHERE regexp_replace(q.word_de, '^(der|die|das) ', '') = %s
    """, (точное,))
    карточки = [{"id": r[0], "word_de": r[1], "user": r[2], "ru": r[3], "pos": r[4],
                 "src": r[5], "tgt": r[6]} for r in cur.fetchall() or []]
    for c in карточки:
        c["строчный_глагол"] = строчный
        c["спорно"] = _спорная(c, новый, перевод)

    cur.execute("""
        SELECT id, source_text, target_text, source_lang, target_lang
          FROM bt_3_dictionary_entries
         WHERE (source_lang='de' AND regexp_replace(source_text,'^(der|die|das) ','') = %s)
            OR (target_lang='de' AND regexp_replace(target_text,'^(der|die|das) ','') = %s)
    """, (точное, точное))
    пул = [{"id": r[0], "src": r[1], "tgt": r[2], "sl": r[3], "tl": r[4]}
           for r in cur.fetchall() or []]

    cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s LIMIT 1",
                (lex_units.normalize_query(_bare(lemma)),))
    row = cur.fetchone()
    # ⛔ СПОРНОСТЬ — СВОЙСТВО НАПИСАНИЯ, А НЕ ОДНОЙ КАРТОЧКИ. Первый прогон 13.09.2026
    # считал её только по карточкам и всё равно переименовал записи ОБЩЕГО словаря: так
    # «Запрашивать → Abrufen» стало «Запрашивать → der Abruf», то есть глагол получил
    # существительное. Четыре записи пришлось откатывать руками. Поэтому общий словарь
    # трогаем, только когда спорных карточек нет вовсе и строчный вариант не слово.
    спорное_написание = bool(строчный) or any(c["спорно"] for c in карточки)
    return {"написание": surface, "станет": новый, "перевод": перевод,
            "пул_чиним": (not спорное_написание),
            "откуда_перевод": откуда_перевод, "откуда_артикль": откуда_артикль,
            "карточки": карточки, "пул": пул, "unit": int(row[0]) if row else None,
            "чиним": sum(1 for c in карточки if not c["спорно"]),
            "спорных": sum(1 for c in карточки if c["спорно"])}


def _apply_one(cur, план: dict) -> dict:
    from backend.database import _strip_dictionary_card_body
    старое, новый = _bare(план["написание"]), план["станет"]
    итог = {"карточек": 0, "словарь": 0, "перевод": 0}

    for c in план["карточки"]:
        if c["спорно"]:
            continue
        cur.execute("SELECT word_de, response_json FROM bt_3_webapp_dictionary_queries WHERE id=%s",
                    (c["id"],))
        row = cur.fetchone()
        if not row or _bare(row[0]) != старое:
            continue                      # уже поправлено
        тело = _strip_dictionary_card_body(row[1])
        тело.pop("part_of_speech", None)  # часть речи считалась для формы
        тело["word_de"] = новый
        тело["source_text"] = новый
        ставим = bool(план["перевод"])
        if ставим:
            тело["word_ru"] = план["перевод"]
            тело["translation_ru"] = план["перевод"]
        cur.execute(
            "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, "
            "translation_de = CASE WHEN regexp_replace(COALESCE(translation_de,''),"
            "  '^(der|die|das) ', '') = %s THEN %s ELSE translation_de END, "
            "word_ru = CASE WHEN %s THEN %s ELSE word_ru END, "
            "translation_ru = CASE WHEN %s THEN %s ELSE translation_ru END, "
            "response_json = %s::jsonb, lex_unit_id = COALESCE(%s, lex_unit_id), "
            "frequency_rank = NULL, updated_at = NOW() WHERE id=%s",
            (новый, старое, новый, ставим, план["перевод"], ставим, план["перевод"],
             json.dumps(тело, ensure_ascii=False), план["unit"], c["id"]),
        )
        итог["карточек"] += 1
        итог["перевод"] += int(ставим)

    for row in (план["пул"] if план.get("пул_чиним") else []):
        поля, значения = [], []
        if str(row["sl"] or "") == "de" and _bare(row["src"] or "") == старое:
            поля += ["source_text=%s", "source_text_norm=lower(%s)", "source_headword_norm=lower(%s)"]
            значения += [новый, _bare(новый), _bare(новый)]
        if str(row["tl"] or "") == "de" and _bare(row["tgt"] or "") == старое:
            поля += ["target_text=%s", "target_text_norm=lower(%s)", "target_headword_norm=lower(%s)"]
            значения += [новый, _bare(новый), _bare(новый)]
        if not поля:
            continue
        # Пара могла уже существовать — тогда это дубль строки пула, а не потеря данных.
        # SAVEPOINT: побочная правка не имеет права утащить за собой главную (правило ноль).
        cur.execute("SAVEPOINT пул")
        try:
            cur.execute(f"UPDATE bt_3_dictionary_entries SET {', '.join(поля)}, updated_at=NOW() "
                        "WHERE id=%s", (*значения, row["id"]))
            cur.execute("RELEASE SAVEPOINT пул")
            итог["словарь"] += 1
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT пул")
            итог.setdefault("пул_дубли", 0)
            итог["пул_дубли"] += 1
            print(f"    · запись словаря {row['id']} не переименована (такая пара уже есть): "
                  f"{str(exc)[:80]}")
    return итог


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    forms = _load_forms(args.only)
    if args.limit:
        forms = dict(sorted(forms.items())[:args.limit])
    if not forms:
        print("нечего чинить")
        return 1

    from backend.database import get_db_connection_context, dedupe_personal_entry_after_save
    _прогреть_строчные(list(forms))
    планы = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for surface, lemma in sorted(forms.items()):
                планы.append(_plan_one(cur, surface, lemma))

    print(f"{'написание':26s} {'станет':26s} {'перевод':28s} {'карт':>4s} {'спор':>4s} {'слов':>4s}")
    print("─" * 104)
    без_перевода, спорные = [], []
    for p in планы:
        print(f"{p['написание']:26s} {p['станет']:26s} {(p['перевод'] or '—')[:27]:28s} "
              f"{p['чиним']:4d} {p['спорных']:4d} {(len(p['пул']) if p['пул_чиним'] else 0):4d}")
        if not p["перевод"] and p["чиним"]:
            без_перевода.append(p["станет"])
        спорные += [(p["написание"], p["станет"], c["ru"], c["спорно"])
                    for c in p["карточки"] if c["спорно"]]
    print("─" * 104)
    print(f"написаний {len(планы)} · карточек к починке {sum(p['чиним'] for p in планы)} · "
          f"спорных (владельцу) {len(спорные)} · "
          f"записей словаря {sum(len(p['пул']) for p in планы if p['пул_чиним'])}")
    if без_перевода:
        print(f"⚠️ без перевода в источниках: {len(без_перевода)} — {', '.join(без_перевода[:10])}")
    if спорные:
        print("\nСПОРНЫЕ — автоматически НЕ чиним, нужен ваш ответ:")
        for w, станет, ru, почему in спорные[:30]:
            print(f"  {w:22s} → {станет:22s} у нас: «{(ru or '')[:34]}» — {почему}")

    if not args.apply:
        print("\nсухой прогон: база не тронута")
        return 0

    итог = {"карточек": 0, "словарь": 0, "перевод": 0, "слито": 0, "ошибок": 0}
    with get_db_connection_context() as conn:
        for p in планы:
            try:
                with conn.cursor() as cur:
                    done = _apply_one(cur, p)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                итог["ошибок"] += 1
                print(f"  ⛔ {p['написание']}: {exc}")
                continue
            for k in ("карточек", "словарь", "перевод"):
                итог[k] += done[k]
            # Схлопывание с уже существующей карточкой того же слова — ШТАТНОЙ дверью:
            # она переносит прогресс и журнал ответов и кладёт снимок в след удаления.
            for c in p["карточки"]:
                if c["спорно"]:
                    continue
                try:
                    итог["слито"] += int(dedupe_personal_entry_after_save(int(c["user"]), int(c["id"])) or 0)
                except Exception as exc:
                    print(f"  ⚠️ слияние {c['id']}: {str(exc)[:100]}")
    print("ИТОГ:", итог)
    return 0


if __name__ == "__main__":
    sys.exit(main())
