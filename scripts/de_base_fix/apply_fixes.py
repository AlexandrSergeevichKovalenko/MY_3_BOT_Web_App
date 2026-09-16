# -*- coding: utf-8 -*-
"""Правка немецко-русской базы: перецепить связь ЗНАЧЕНИЯ на верный перевод.

Без --commit не пишет НИЧЕГО. С --commit пишет и ДО каждой правки кладёт копию
в rollback-файл: нет копии — нет правки, это условие в коде.

Порядок (нарушение откатывает починку само):
  1. связь значения   2. карточка единицы   3. пул и кеш   4. личные карточки
"""
import argparse, json, os, sys, datetime
import psycopg2, psycopg2.extras

# Данные прогона (items_all.json, dry_run_plan.json, rollback.jsonl) лежат отдельно
# от кода: они большие и разовые. Путь задаётся через DATA_DIR.
S = os.environ.get("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, "/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2")
from backend.lex_units import ensure_unit, OWNER_CHOICE_SOURCE, _DEMOTED_RANK

ИСТОЧНИК = "вычитка 16.09.2026"


def отобрать(план, items):
    """Только то, что прошло ВСЕ проверки и у чего значение имеет ровно один вариант."""
    годные = []
    for p in план:
        if not p.get("итог", "").startswith("связь значения"):
            continue
        годные.append(p)
    return годные


def править(cur, шаг, новый_ru_id, копия):
    uid = int(шаг["key"].split(":")[0])
    sid = шаг["key"].split(":")[1]
    sid = int(sid) if sid != "0" else None
    de, новый = шаг["de"], шаг["станет"]

    # ── 1. СВЯЗЬ ЗНАЧЕНИЯ ────────────────────────────────────────────────────
    cur.execute("""SELECT l.from_unit,l.to_unit,l.sense_id,l.rank,l.source,ru.display
                     FROM bt_3_lex_links l JOIN bt_3_lex_units ru ON ru.id=l.to_unit
                    WHERE l.from_unit=%s AND l.sense_id IS NOT DISTINCT FROM %s""", (uid, sid))
    было_связи = [dict(r) for r in cur.fetchall()]
    копия["связи_до"] = было_связи
    if not было_связи:
        return "ОТКАЗ: связь значения исчезла между прогоном и правкой"

    # ⚠ СТРАЖ ВИДИМОСТИ. Судье я скармливал ВСЕ переводы значения, склеенные в одну
    # строку («доспехи; Вооружение, оснащение»), а человеку выдача отдаёт РОВНО ОДИН
    # (native_display_sql, lex_units.py:95). Значит там, где у значения несколько
    # живых связей, судья судил текст, которого на экране нет. Такие не трогаем.
    живые = [s for s in было_связи if int(s["rank"]) < _DEMOTED_RANK]
    if len(живые) != 1:
        return f"ОТКАЗ: у значения {len(живые)} живых переводов, судья видел не то, что человек"

    cur.execute("""INSERT INTO bt_3_lex_links (from_unit,to_unit,sense_id,rank,source)
                   VALUES (%s,%s,%s,1,%s)
                   ON CONFLICT (from_unit,to_unit)
                   DO UPDATE SET rank=1, sense_id=EXCLUDED.sense_id,
                                 source=EXCLUDED.source, updated_at=NOW()""",
                (uid, новый_ru_id, sid, ИСТОЧНИК))
    # понижаем ТОЛЬКО связи ЭТОГО значения, соседние не трогаем
    cur.execute("""UPDATE bt_3_lex_links SET rank=GREATEST(rank,%s), updated_at=NOW()
                    WHERE from_unit=%s AND sense_id IS NOT DISTINCT FROM %s
                      AND to_unit<>%s AND rank<%s""",
                (_DEMOTED_RANK, uid, sid, новый_ru_id, _DEMOTED_RANK))
    копия["понижено_связей"] = cur.rowcount

    # ── 2. КАРТОЧКА ЕДИНИЦЫ ──────────────────────────────────────────────────
    cur.execute("SELECT card FROM bt_3_lex_units WHERE id=%s", (uid,))
    row = cur.fetchone()
    card = row["card"] if row and isinstance(row["card"], dict) else None
    if card and str(card.get("translation_ru") or "").strip() in {s["display"] for s in было_связи}:
        копия["card_translation_ru_до"] = card.get("translation_ru")
        card = dict(card); card["translation_ru"] = новый
        cur.execute("UPDATE bt_3_lex_units SET card=%s::jsonb, updated_at=NOW() WHERE id=%s",
                    (json.dumps(card, ensure_ascii=False), uid))

    # ── 3. ПУЛ И КЕШ — ТОЛЬКО ПОСЛЕ 1 и 2 ───────────────────────────────────
    старые = [s["display"] for s in было_связи]
    cur.execute("""SELECT id,source_text,target_text,word_de,translation_ru
                     FROM bt_3_dictionary_entries
                    WHERE word_de=%s AND translation_ru = ANY(%s)""", (de, старые))
    копия["пул_до"] = [dict(r) for r in cur.fetchall()]
    if копия["пул_до"]:
        cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id = ANY(%s)",
                    ([r["id"] for r in копия["пул_до"]],))
    cur.execute("SELECT cache_key FROM bt_3_dictionary_lookup_cache WHERE normalized_word=%s",
                (de.strip().lower(),))
    ключи = [r["cache_key"] for r in cur.fetchall()]
    копия["кеш_до"] = ключи
    if ключи:
        cur.execute("DELETE FROM bt_3_dictionary_lookup_cache WHERE cache_key = ANY(%s)", (ключи,))

    # ── 4. ЛИЧНЫЕ КАРТОЧКИ ──────────────────────────────────────────────────
    # не трогаем тех, кто вписал перевод РУКАМИ — это его собственная правка
    cur.execute("""SELECT q.id,q.user_id,q.translation_ru,q.response_json->>'translation_ru' AS rj
                     FROM bt_3_webapp_dictionary_queries q
                    WHERE q.lex_unit_id=%s
                      AND (q.translation_ru = ANY(%s) OR q.response_json->>'translation_ru' = ANY(%s))
                      AND NOT EXISTS (SELECT 1 FROM bt_3_user_word_overrides o
                                       WHERE o.user_id=q.user_id AND o.entry_id=q.id
                                         AND o.field='translation_ru')""",
                (uid, старые, старые))
    личные = [dict(r) for r in cur.fetchall()]
    копия["личные_до"] = личные
    for k in личные:
        cur.execute("""UPDATE bt_3_webapp_dictionary_queries
                          SET translation_ru = CASE WHEN translation_ru = ANY(%s) THEN %s ELSE translation_ru END,
                              response_json = CASE WHEN response_json->>'translation_ru' = ANY(%s)
                                                   THEN jsonb_set(response_json,'{translation_ru}',to_jsonb(%s::text))
                                                   ELSE response_json END,
                              updated_at = NOW()
                        WHERE id=%s""", (старые, новый, старые, новый, k["id"]))
    return "готово"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--commit", action="store_true")
    a = ap.parse_args()

    items = {f'{x["unit_id"]}:{x.get("sense_id") or 0}': x
             for x in json.load(open(os.path.join(S, "items_all.json"), encoding="utf-8"))}
    план = json.load(open(os.path.join(S, "dry_run_plan.json"), encoding="utf-8"))
    готовые = отобрать(план, items)
    сделано = set()
    журнал = os.path.join(S, "rollback.jsonl")
    if os.path.exists(журнал):
        for line in open(журнал, encoding="utf-8"):
            try: сделано.add(json.loads(line)["key"])
            except Exception: pass
    очередь = [p for p in готовые if p["key"] not in сделано][:a.limit]
    print(f"всего готовых: {len(готовые)}; уже сделано: {len(сделано)}; в этот заход: {len(очередь)}")
    if not a.commit:
        for p in очередь:
            print(f"  {p['de']}\n      было  {p['было']}\n      станет {p['станет']}")
        print("\n(сухой прогон — ничего не записано; добавьте --commit)")
        return

    conn = psycopg2.connect(os.environ["DB_URL"], connect_timeout=60)
    conn.autocommit = False
    итоги = {}
    for p in очередь:
        новый_ru_id = ensure_unit(p["станет"], "ru")     # своя транзакция, идемпотентно
        if not новый_ru_id:
            итоги["дверь единиц отказала"] = итоги.get("дверь единиц отказала", 0) + 1
            continue
        копия = {"key": p["key"], "de": p["de"], "было": p["было"], "станет": p["станет"],
                 "новый_ru_id": новый_ru_id, "когда": datetime.datetime.now().isoformat()}
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            итог = править(cur, p, новый_ru_id, копия)
            копия["итог"] = итог
            if итог != "готово":
                conn.rollback(); итоги[итог] = итоги.get(итог, 0) + 1; continue
            # КОПИЯ ЛОЖИТСЯ НА ДИСК ДО COMMIT. Нет копии — нет правки.
            with open(журнал, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(копия, ensure_ascii=False, default=str) + "\n")
                fh.flush(); os.fsync(fh.fileno())
            conn.commit()
            итоги["готово"] = итоги.get("готово", 0) + 1
        except Exception as e:
            conn.rollback()
            итоги[f"сбой: {type(e).__name__}"] = итоги.get(f"сбой: {type(e).__name__}", 0) + 1
            print(f"  !! {p['de']}: {e}")
        finally:
            cur.close()
    conn.close()
    print("\nИТОГ:", json.dumps(итоги, ensure_ascii=False))
    print(f"копии для отката: {журнал}")


if __name__ == "__main__":
    main()
