#!/usr/bin/env python3
"""Заголовки-формы: спросить СПРАВОЧНИК по каждому написанию и разложить на классы.

Повод (13.09.2026). В словаре заголовком стоят склонённые формы: «beruhte» вместо
«beruhen», «Belege» вместо «Beleg». Замер по трём хранилищам дал 242 кандидата, но
чинить этот список НЕЛЬЗЯ: наш внутренний указатель `bt_3_lex_surfaces` путает законные
слова с формами — «arbeiten» он считает формой от «Arbeit», «schildern» формой от
«Schild», «die Brühe» формой от «brühen». Все три — настоящие словарные слова.

Поэтому единственный судья здесь — справочник de.wiktionary
(`backend/german_form_headword.headword_kinds`): он отвечает «word» (у написания есть
СВОЙ раздел с частью речи) либо «form» + база. Это правило напечатано в самом
справочнике и не придумано нами.

РЕГИСТР. «blähungen» справочник не знает вовсе, «Blähungen» знает как форму от
«Blähung». Поэтому заглавный вариант спрашивается ТОЛЬКО тогда, когда про исходное
написание справочник промолчал. Если исходное написание он знает как слово — это слово,
и заглавный вариант не спрашиваем: иначе «arbeiten» (глагол) превратился бы в форму от
«Arbeit» через «Arbeiten».

Скрипт ТОЛЬКО ЧИТАЕТ: базу (SELECT) и сеть (Wiktionary). Ничего не пишет ни в базу, ни
в кеш двери слова. Результат кладётся в JSON, из него потом работает чистка.

    python3 scripts/dict_form_headword_audit.py --limit 50      # проба
    python3 scripts/dict_form_headword_audit.py                 # все кандидаты
    python3 scripts/dict_form_headword_audit.py --out scripts/data/form_headwords.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

BATCH = 45          # столько заголовков за один запрос к справочнику (лимит MediaWiki — 50)
PAUSE_SEC = 1.5     # та же пауза, что у прогрева родов: справочник отбивает частые запросы


def _candidates() -> dict[str, list[dict]]:
    """Кандидаты из трёх хранилищ: личные карточки, общий словарь, слой единиц.

    Правило отбора одно: ОДИНОЧНОЕ немецкое написание, которое наш указатель
    (`bt_3_lex_surfaces`, match_kind='inflected') связывает с единицей, чья лемма другая.
    Это ШИРОКАЯ сеть — она ловит и законные слова; разделит их справочник."""
    from backend.database import get_db_connection_context
    ключ = "lower(regexp_replace({}, '^(der|die|das) ', ''))"
    out: dict[str, list[dict]] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT q.id, q.word_de, u.lemma, q.origin_process, q.user_id
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_surfaces s
                  ON s.surface_key = {ключ.format('q.word_de')} AND s.match_kind = 'inflected'
                JOIN bt_3_lex_units u ON u.id = s.unit_id AND u.lang = 'de'
                WHERE q.word_de IS NOT NULL AND q.word_de <> ''
                  AND position(' ' in trim(regexp_replace(q.word_de, '^(der|die|das) ', ''))) = 0
                  AND lower(u.lemma) <> {ключ.format('q.word_de')}
            """)
            out["карточки"] = [{"id": r[0], "написание": r[1], "указатель": r[2],
                                "поверхность": r[3], "человек": r[4]} for r in cur.fetchall() or []]
            cur.execute(f"""
                SELECT e.id, e.source_text, u.lemma
                FROM bt_3_dictionary_entries e
                JOIN bt_3_lex_surfaces s
                  ON s.surface_key = {ключ.format('e.source_text')} AND s.match_kind = 'inflected'
                JOIN bt_3_lex_units u ON u.id = s.unit_id AND u.lang = 'de'
                WHERE e.source_lang = 'de' AND e.target_lang = 'ru'
                  AND position(' ' in trim(regexp_replace(e.source_text, '^(der|die|das) ', ''))) = 0
                  AND lower(u.lemma) <> {ключ.format('e.source_text')}
            """)
            out["общий_словарь"] = [{"id": r[0], "написание": r[1], "указатель": r[2]}
                                    for r in cur.fetchall() or []]
            cur.execute("""
                SELECT u.id, u.lemma, u.display, u.card_source
                FROM bt_3_lex_units u
                JOIN bt_3_lex_surfaces s ON s.surface_key = lower(u.lemma) AND s.match_kind = 'inflected'
                JOIN bt_3_lex_units base ON base.id = s.unit_id AND base.lang = 'de'
                WHERE u.lang = 'de' AND u.kind = 'word'
                  AND position(' ' in trim(u.lemma)) = 0
                  AND lower(base.lemma) <> lower(u.lemma)
            """)
            out["единицы"] = [{"id": r[0], "написание": r[1], "показ": r[2], "откуда": r[3]}
                              for r in cur.fetchall() or []]

            # ⛔ УКАЗАТЕЛЬ — НЕ ЕДИНСТВЕННЫЙ, КТО ЗНАЕТ ПРО ФОРМЫ. Первый прогон 13.09.2026
            # собирал кандидатов только по `bt_3_lex_surfaces` и нашёл 100 написаний из
            # 242, которые до этого дал замер по семи источникам. Поэтому сеть расширена:
            # индекс форм существительных, справочник спряжений и вердикты самой двери.
            # Задача этого списка — НАБРАТЬ ПОДОЗРЕВАЕМЫХ; кто из них правда форма,
            # решает дальше только справочник.
            cur.execute("""
                SELECT q.id, q.word_de, '', q.origin_process, q.user_id
                FROM bt_3_webapp_dictionary_queries q
                WHERE q.word_de IS NOT NULL AND q.word_de <> ''
                  AND position(' ' in trim(regexp_replace(q.word_de, '^(der|die|das) ', ''))) = 0
                  AND (
                    EXISTS (SELECT 1 FROM bt_3_german_form_index f
                            WHERE lower(f.surface) = lower(regexp_replace(q.word_de, '^(der|die|das) ', ''))
                              AND lower(coalesce(f.lemma,'')) <> lower(regexp_replace(q.word_de, '^(der|die|das) ', '')))
                    OR EXISTS (SELECT 1 FROM bt_3_word_check w
                               WHERE lower(w.asked) = lower(regexp_replace(q.word_de, '^(der|die|das) ', ''))
                                 AND w.source = 'справочник (заголовок был формой слова)')
                  )
            """)
            добор = [{"id": r[0], "написание": r[1], "указатель": r[2],
                      "поверхность": r[3], "человек": r[4]} for r in cur.fetchall() or []]
            известные = {x["id"] for x in out["карточки"]}
            out["карточки"].extend(x for x in добор if x["id"] not in известные)
    return out


def _ask_reference(words: list[str], log=print) -> dict[str, dict]:
    """{написание: вердикт справочника}. Вердикт: {'класс': 'форма'|'слово'|'молчит',
    'база': лемма, 'разделы': [...], 'спрошено': какое написание сработало}."""
    from backend.german_form_headword import headword_kinds
    ответы: dict[str, dict] = {}
    чистые = sorted({str(w or "").strip() for w in words if str(w or "").strip()})
    for i in range(0, len(чистые), BATCH):
        пачка = чистые[i:i + BATCH]
        сырое = headword_kinds(пачка)
        for w in пачка:
            r = сырое.get(w) or {}
            kind = str(r.get("kind") or "")
            if kind == "word":
                ответы[w] = {"класс": "слово", "база": "", "разделы": r.get("wortarten") or [], "спрошено": w}
            elif kind == "form":
                базы = [str(b) for b in (r.get("bases") or []) if str(b or "").strip()]
                ответы[w] = {"класс": "форма" if len(базы) == 1 else "форма_неясная",
                             "база": базы[0] if len(базы) == 1 else "",
                             "базы": базы, "разделы": r.get("wortarten") or [], "спрошено": w}
            else:
                ответы[w] = {"класс": "молчит", "база": "", "разделы": [], "спрошено": w}
        log(f"  справочник: {min(i + BATCH, len(чистые))} из {len(чистые)}")
        if i + BATCH < len(чистые):
            time.sleep(PAUSE_SEC)

    # Второй заход ТОЛЬКО для промолчавших со строчной буквы: немецкое существительное
    # пишется с заглавной, и «blähungen» справочник не знает, а «Blähungen» знает.
    повтор = [w for w, r in ответы.items() if r["класс"] == "молчит" and w[:1].islower() and w[:1].isalpha()]
    if повтор:
        log(f"  промолчали со строчной — спрашиваю с заглавной: {len(повтор)}")
        заглавные = {w: w[:1].upper() + w[1:] for w in повтор}
        второй = _ask_reference(list(заглавные.values()), log=lambda *_: None)
        for w, big in заглавные.items():
            r = второй.get(big) or {}
            if r.get("класс") in ("форма", "форма_неясная", "слово"):
                ответы[w] = {**r, "спрошено": big, "регистр_тоже_чинить": True}
    return ответы


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="взять только первые N написаний (проба)")
    ap.add_argument("--out", default="scripts/data/form_headwords.json")
    args = ap.parse_args()

    хранилища = _candidates()
    все_написания = sorted({str(x["написание"]).strip() for v in хранилища.values() for x in v})
    # Заголовок хранится и с артиклем («die Verbindlichkeiten»), справочник знает слово.
    голые = sorted({w.split(" ", 1)[1] if w.lower().startswith(("der ", "die ", "das ")) else w
                    for w in все_написания})
    if args.limit:
        голые = голые[:args.limit]
    print(f"кандидатов: карточек {len(хранилища['карточки'])}, "
          f"в общем словаре {len(хранилища['общий_словарь'])}, единиц {len(хранилища['единицы'])}; "
          f"разных написаний {len(голые)}")

    ответы = _ask_reference(голые)
    классы: dict[str, list] = {"форма": [], "форма_неясная": [], "слово": [], "молчит": []}
    for w in голые:
        r = ответы.get(w) or {"класс": "молчит"}
        классы[r["класс"]].append((w, r.get("база", ""), ",".join(r.get("разделы") or [])))

    print("\nЧТО СКАЗАЛ СПРАВОЧНИК:")
    for k in ("форма", "форма_неясная", "слово", "молчит"):
        print(f"  {k:14s} {len(классы[k]):4d}")
    print("\nФОРМЫ (написание → словарное слово), первые 40:")
    for w, base, sec in классы["форма"][:40]:
        print(f"  {w:28s} → {base:24s} [{sec}]")
    print("\nЗАКОННЫЕ СЛОВА, которые указатель считал формой, первые 20:")
    for w, _b, sec in классы["слово"][:20]:
        print(f"  {w:28s} [{sec}]")

    путь = args.out
    os.makedirs(os.path.dirname(путь), exist_ok=True)
    with open(путь, "w", encoding="utf-8") as f:
        json.dump({"дата": time.strftime("%Y-%m-%d %H:%M"), "хранилища": хранилища,
                   "ответы": ответы}, f, ensure_ascii=False, indent=1)
    print(f"\nсохранено: {путь}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
