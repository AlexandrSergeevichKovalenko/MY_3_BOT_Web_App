"""Свести разбор слова в ОДНО место — на единицу.

Зачем. Сегодня разбор одного и того же слова лежит в трёх местах и в трёх степенях
полноты: на единице (туда пишет ночной добор), в общем пуле (туда пишут сохранения) и
в личной карточке человека (туда попадает раздача из единиц и дообогащение). Из-за этого
поиск, который читает пул, показывает самую бедную копию, а улучшение слова доходит
только до того, кому его успели раздать.

Что делает скрипт. Проходит по единицам, у которых полного разбора ещё нет, находит
лучший готовый разбор среди личных карточек и записей пула и кладёт его НА ЕДИНИЦУ.
Ничего не покупает у модели: всё переносимое уже оплачено. Личные карточки и пул не
трогает вообще — только дописывает единицам.

Правила отбора победителя (когда версий несколько):
  1) полнота по СТРОГОМУ критерию — тому же, которым ночной отбор ищет «тонкие» слова;
  2) при равенстве — больший объём разбора;
  3) при равенстве — свежий.

Защита от чужого разбора. Перед записью сверяем заголовок разбора с самой единицей:
разбор «der Rüpel» не имеет права лечь на единицу «der Flegel», даже если когда-то их
связали по совпавшему переводу. Не сошлось — пропускаем и пишем в отчёт.

По умолчанию НИЧЕГО НЕ ПИШЕТ: показывает отчёт. Запись — только с --apply.

    python scripts/dict_units_absorb.py                 # вхолостую, полный отчёт
    python scripts/dict_units_absorb.py --limit 50      # вхолостую, первые 50
    python scripts/dict_units_absorb.py --apply         # записать
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Скрипт запускают и локально, и внутри боевого контейнера (там он приходит текстом,
# поэтому __file__ может отсутствовать).
_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import (  # noqa: E402
    get_db_connection_context,
    _dictionary_pool_word_fully_rich_sql as _rich_sql,
)

CARD_RICH = _rich_sql("q.response_json")
POOL_RICH = _rich_sql("e.response_json")
UNIT_RICH = _rich_sql("u.card")

# Следы момента сохранения, а не свойства слова. На общей единице им делать нечего.
DROP_KEYS = ("original_query", "raw_text", "save_worthy_options", "correction_applied")


def _clean_card(payload: dict) -> dict:
    card = {k: v for k, v in dict(payload or {}).items() if k not in DROP_KEYS}
    for key in list(card):
        if key.startswith("__"):
            card.pop(key, None)
    return card


# Блоки, ради которых разбор и нужен. По ним сравниваем версии, а не по объёму текста.
CONTENT_KEYS = (
    "usage_examples", "meanings", "dictionary_senses", "forms", "grammar_tables",
    "government_patterns", "common_collocations", "synonym_differences", "false_friends",
    "word_formation", "register_examples", "common_mistakes", "pronunciation",
    "etymology_note", "memory_tip", "translations",
)


def _content_keys(card: dict) -> set:
    """Непустые содержательные блоки разбора."""
    filled = set()
    for key in CONTENT_KEYS:
        value = card.get(key)
        if isinstance(value, (list, dict)):
            if len(value):
                filled.add(key)
        elif isinstance(value, str) and value.strip():
            filled.add(key)
        elif value not in (None, "", [], {}):
            filled.add(key)
    return filled


def _german_side(*, word_de, word_ru, source_lang, target_lang, source_text, target_text, payload):
    """Немецкая сторона записи — то, с чем сверяем заголовок единицы."""
    payload = payload if isinstance(payload, dict) else {}
    for candidate in (
        word_de,
        payload.get("word_de"),
        source_text if str(source_lang or "").lower() == "de" else None,
        target_text if str(target_lang or "").lower() == "de" else None,
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _fetch(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()


def collect(limit: int | None = None) -> dict:
    """Собрать кандидатов. Только чтение."""
    report = {
        "candidates": {},          # unit_id -> лучший кандидат
        "conflicts": [],           # единицы с несколькими РАЗНЫМИ полными разборами
        "mismatch": [],            # заголовок разбора не сошёлся с единицей
        "skipped_lang": 0,         # единица не немецкая — в этот проход не берём
        "orphan_pool": [],         # полный разбор в пуле, единицы нет вовсе
    }
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # Написания единиц загружаем ОДИН раз: сверка заголовка идёт на каждой строке,
            # и запрос внутри цикла превратил бы прогон в часы.
            surfaces: dict[int, set[str]] = {}
            for unit_id, surface_key in _fetch(cur, "SELECT unit_id, surface_key FROM bt_3_lex_surfaces"):
                surfaces.setdefault(int(unit_id), set()).add(surface_key)
            report["surfaces"] = surfaces

            # ── кандидаты из личных карточек ──────────────────────────────────
            rows = _fetch(cur, f"""
                SELECT q.lex_unit_id, u.lang, u.kind, u.lemma, u.lemma_key,
                       q.id, q.response_json, LENGTH(q.response_json::text) AS len,
                       q.updated_at, q.word_de, q.word_ru, q.source_lang, q.target_lang
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE {CARD_RICH} AND NOT {UNIT_RICH}
                ORDER BY q.lex_unit_id, len DESC, q.updated_at DESC
            """)
            seen_json: dict[int, set[str]] = {}
            for r in rows:
                (unit_id, lang, kind, lemma, lemma_key, card_id, payload, size,
                 updated, word_de, word_ru, src_lang, tgt_lang) = r
                seen_json.setdefault(unit_id, set()).add(json.dumps(payload, sort_keys=True, ensure_ascii=False))
                _consider(report, unit_id=unit_id, lang=lang, kind=kind, lemma=lemma,
                          lemma_key=lemma_key, payload=payload, size=size, updated=updated,
                          origin="карточка", origin_id=card_id,
                          german=_german_side(word_de=word_de, word_ru=word_ru,
                                              source_lang=src_lang, target_lang=tgt_lang,
                                              source_text=None, target_text=None, payload=payload))
            for unit_id, variants in seen_json.items():
                slot = report["candidates"].get(unit_id) or {}
                if len(variants) > 1 and slot.get("карточка"):
                    report["conflicts"].append({
                        "unit_id": unit_id,
                        "lemma": slot["карточка"]["lemma"],
                        "variants": len(variants),
                    })

            # ── кандидаты из пула (через связь карточки с записью пула) ────────
            rows = _fetch(cur, f"""
                SELECT DISTINCT q.lex_unit_id, u.lang, u.kind, u.lemma, u.lemma_key,
                       e.id, e.response_json, LENGTH(e.response_json::text) AS len,
                       e.updated_at, e.word_de, e.word_ru, e.source_lang, e.target_lang,
                       e.source_text, e.target_text
                FROM bt_3_dictionary_entries e
                JOIN bt_3_webapp_dictionary_queries q ON q.canonical_entry_id = e.id
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE {POOL_RICH} AND NOT {UNIT_RICH}
            """)
            for r in rows:
                (unit_id, lang, kind, lemma, lemma_key, entry_id, payload, size,
                 updated, word_de, word_ru, src_lang, tgt_lang, src_text, tgt_text) = r
                _consider(report, unit_id=unit_id, lang=lang, kind=kind, lemma=lemma,
                          lemma_key=lemma_key, payload=payload, size=size, updated=updated,
                          origin="пул", origin_id=entry_id,
                          german=_german_side(word_de=word_de, word_ru=word_ru,
                                              source_lang=src_lang, target_lang=tgt_lang,
                                              source_text=src_text, target_text=tgt_text,
                                              payload=payload))

            # ── записи пула, у которых единицы нет вовсе ───────────────────────
            rows = _fetch(cur, f"""
                SELECT e.id, e.source_lang, e.target_lang, e.source_text, e.target_text,
                       e.response_json, e.word_de, e.word_ru
                FROM bt_3_dictionary_entries e
                WHERE {POOL_RICH}
                  AND NOT EXISTS (
                    SELECT 1 FROM bt_3_webapp_dictionary_queries q
                    WHERE q.canonical_entry_id = e.id AND q.lex_unit_id IS NOT NULL)
            """)
            for entry_id, src_lang, tgt_lang, src_text, tgt_text, payload, word_de, word_ru in rows:
                german = _german_side(word_de=word_de, word_ru=word_ru,
                                      source_lang=src_lang, target_lang=tgt_lang,
                                      source_text=src_text, target_text=tgt_text, payload=payload)
                if not german:
                    continue
                report["orphan_pool"].append({
                    "entry_id": entry_id, "german": german, "payload": payload,
                    "size": len(json.dumps(payload, ensure_ascii=False)),
                })

    report.pop("surfaces", None)
    report["duels"] = []
    winners = {}
    for unit_id, slot in report["candidates"].items():
        card_best, pool_best = slot.get("карточка"), slot.get("пул")
        if card_best and pool_best:
            report["duels"].append({
                "lemma": card_best["lemma"],
                "card_size": card_best["size"], "pool_size": pool_best["size"],
                "only_card": sorted(card_best["keys"] - pool_best["keys"]),
                "only_pool": sorted(pool_best["keys"] - card_best["keys"]),
            })
        # Побеждает тот, у кого БОЛЬШЕ содержательных блоков; при равенстве — объём;
        # при полном равенстве предпочитаем карточку: её видел живой человек.
        options = [x for x in (card_best, pool_best) if x]
        options.sort(key=lambda x: (len(x["keys"]), x["size"], x["origin"] == "карточка"), reverse=True)
        winners[unit_id] = options[0]
    report["candidates"] = winners
    if limit:
        picked = dict(list(report["candidates"].items())[: int(limit)])
        report["candidates"] = picked
        report["orphan_pool"] = report["orphan_pool"][: int(limit)]
    return report


def _consider(report, *, unit_id, lang, kind, lemma, lemma_key, payload, size, updated,
              origin, origin_id, german):
    """Запомнить лучшего кандидата ОТДЕЛЬНО по каждому источнику.

    Победитель выбирается позже: так в отчёте видно, чем именно пул спорит с карточкой,
    и решение принимается не вслепую."""
    if str(lang or "").lower() != "de":
        # Разбор описывает немецкое слово. Класть его на русскую единицу без отдельной
        # проверки нельзя — этот случай разбираем отдельным проходом.
        report["skipped_lang"] += 1
        return
    if not _headword_matches(german=german, lemma=lemma, lemma_key=lemma_key,
                             surface_keys=report.get("surfaces", {}).get(int(unit_id), set())):
        report["mismatch"].append({
            "unit_id": unit_id, "lemma": lemma, "german": german, "origin": origin,
        })
        return
    # Размер считаем по тому, что реально ляжет на единицу: сырой текст запроса и другие
    # следы сохранения в разбор не входят, а объём раздували именно они.
    clean = _clean_card(payload)
    clean_size = len(json.dumps(clean, ensure_ascii=False))
    slot = report["candidates"].setdefault(unit_id, {})
    current = slot.get(origin)
    if current is None or clean_size > current["size"]:
        slot[origin] = {
            "unit_id": unit_id, "kind": kind, "lemma": lemma,
            "payload": payload, "size": clean_size, "updated": updated,
            "origin": origin, "origin_id": origin_id, "keys": _content_keys(clean),
        }


def _headword_matches(*, german: str, lemma: str, lemma_key: str, surface_keys: set) -> bool:
    """Заголовок разбора и заголовок единицы — про одно слово?

    Сверяем по написаниям единицы, а не по точному тексту: «Auseinandersetzungen» и
    «die Auseinandersetzung» — одно и то же слово, и оба ведут к одной единице."""
    if not german:
        return False
    key = lex_units.normalize_query(german)
    if not key:
        return False
    if key == (lemma_key or "") or key == lex_units.normalize_query(lemma or ""):
        return True
    return key in (surface_keys or set())


def _link_translations_if_orphan(unit_id: int, card: dict) -> bool:
    """Собрать переводы из разбора, если у единицы нет НИ ОДНОЙ связи.

    Словарь не отдаёт единицу без перевода: «единица есть, а перевода на нужный язык
    нет — отдавать нечего». Перенос клал разбор, но связь с русской стороной не создавал,
    и 58 слов с готовым разбором оставались недостижимыми — «der Abschleppdienst»,
    «die Zulassungsstelle», «der Fahrzeugschein».

    Трогаем ТОЛЬКО единицы без связей: там, где переводы уже настроены, пересборка
    переставила бы ранги и могла вернуть наверх примеры, которые мы оттуда убрали."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM bt_3_lex_links WHERE from_unit = %s AND rank < 900;",
                    (int(unit_id),),
                )
                if int((cur.fetchone() or [0])[0] or 0) > 0:
                    return False
        report = lex_units.sync_unit_links_from_card(int(unit_id), card)
        return bool(report.get("links"))
    except Exception as exc:  # noqa: BLE001
        print(f"   ! связи для единицы {unit_id}: {exc}")
        return False


def apply_report(report: dict) -> dict:
    """Записать отобранное. Идемпотентно: повторный запуск ничего не меняет."""
    done = {"filled": 0, "created": 0, "errors": 0, "linked": 0}
    total = len(report["candidates"])
    for index, (unit_id, best) in enumerate(report["candidates"].items(), 1):
        if index % 500 == 0:
            print(f"   … {index} из {total}", flush=True)
        try:
            card = _clean_card(best["payload"])
            if lex_units.save_unit_card(int(unit_id), card, source="сведение"):
                done["filled"] += 1
                if _link_translations_if_orphan(int(unit_id), card):
                    done["linked"] += 1
        except Exception as exc:  # noqa: BLE001
            done["errors"] += 1
            print(f"   ! единица {unit_id}: {exc}")
    for orphan in report["orphan_pool"]:
        try:
            unit_id = lex_units.ensure_unit(orphan["german"], "de")
            if not unit_id:
                done["errors"] += 1
                continue
            card = _clean_card(orphan["payload"])
            if lex_units.save_unit_card(int(unit_id), card, source="сведение"):
                done["created"] += 1
                if _link_translations_if_orphan(int(unit_id), card):
                    done["linked"] += 1
        except Exception as exc:  # noqa: BLE001
            done["errors"] += 1
            print(f"   ! запись пула {orphan['entry_id']}: {exc}")
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="Свести разбор слова на единицу")
    parser.add_argument("--apply", action="store_true", help="записать (без флага — только отчёт)")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число единиц")
    args = parser.parse_args()

    report = collect(limit=args.limit or None)
    by_kind: dict[str, int] = {}
    by_origin: dict[str, int] = {}
    for best in report["candidates"].values():
        by_kind[best["kind"]] = by_kind.get(best["kind"], 0) + 1
        by_origin[best["origin"]] = by_origin.get(best["origin"], 0) + 1

    print("=" * 72)
    print("СВЕДЕНИЕ РАЗБОРА НА ЕДИНИЦУ" + ("  — ЗАПИСЬ" if args.apply else "  — вхолостую, ничего не пишем"))
    print("=" * 72)
    print(f"\nединиц получат разбор:      {len(report['candidates'])}")
    for kind, count in sorted(by_kind.items(), key=lambda kv: -kv[1]):
        print(f"   вида {kind:<14} {count}")
    print("\nоткуда берём:")
    for origin, count in sorted(by_origin.items(), key=lambda kv: -kv[1]):
        print(f"   из {origin:<16} {count}")
    print(f"\nзаведём новых единиц:       {len(report['orphan_pool'])}")
    print(f"единиц с несколькими версиями (выбран лучший): {len(report['conflicts'])}")
    print(f"пропущено, заголовок не сошёлся:               {len(report['mismatch'])}")
    print(f"пропущено, единица не немецкая:                {report['skipped_lang']}")

    print("\nПРИМЕРЫ (первые 20):")
    for best in list(report["candidates"].values())[:20]:
        print(f"   {best['lemma'][:44]:<46} {best['kind']:<12} из {best['origin']:<9} {best['size']:>6} симв.")

    if report["mismatch"]:
        print("\nНЕ СОШЁЛСЯ ЗАГОЛОВОК (первые 10) — эти НЕ переносим:")
        for m in report["mismatch"][:10]:
            print(f"   единица {m['lemma'][:36]:<38} ← разбор про {m['german'][:32]:<34} ({m['origin']})")

    if report.get("duels"):
        richer_card = sum(1 for d in report["duels"] if d["only_card"] and not d["only_pool"])
        richer_pool = sum(1 for d in report["duels"] if d["only_pool"] and not d["only_card"])
        print(f"\nГДЕ ЕСТЬ ОБЕ ВЕРСИИ: {len(report['duels'])}"
              f"  (только у карточки богаче: {richer_card}, только у пула: {richer_pool})")
        for duel in report["duels"][:8]:
            print(f"   {duel['lemma'][:40]:<42} карточка {duel['card_size']:>5} / пул {duel['pool_size']:>5}")
            if duel["only_card"]:
                print(f"      только в карточке: {', '.join(duel['only_card'][:6])}")
            if duel["only_pool"]:
                print(f"      только в пуле:     {', '.join(duel['only_pool'][:6])}")

    if report["conflicts"]:
        print("\nНЕСКОЛЬКО ВЕРСИЙ У ОДНОГО СЛОВА (первые 10):")
        for conflict in report["conflicts"][:10]:
            print(f"   {conflict['lemma'][:44]:<46} версий: {conflict['variants']}")

    if not args.apply:
        print("\nЭто был холостой прогон. Записать — тот же вызов с --apply.")
        return 0

    print("\nПишу…")
    done = apply_report(report)
    print(f"\nготово: заполнено {done['filled']}, заведено новых единиц {done['created']}, "
          f"собрано переводов {done['linked']}, ошибок {done['errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
