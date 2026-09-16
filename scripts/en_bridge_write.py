# -*- coding: utf-8 -*-
"""Запись английской стороны в базу. ТОЛЬКО ДОБАВЛЯЕТ: новые английские единицы и
новые связи немецкое→английское. Ничего не удаляет, ничего не понижает, немецкой
и русской стороны не касается вовсе.

Адрес базы берётся из DB_URL: стенд или прод — решает тот, кто запускает.
"""
import argparse, json, os, sys, datetime, collections
import psycopg2, psycopg2.extras

S = os.environ.get("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, "/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2")
from backend.lex_units import ensure_unit

ИСТОЧНИК = "мост de→en 16.09.2026"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 = все")
    ap.add_argument("--commit", action="store_true")
    a = ap.parse_args()

    items = {f'{x["unit_id"]}:{x.get("sense_id") or 0}': x
             for x in json.load(open(os.path.join(S, "bridge_input.json"), encoding="utf-8"))}
    работа = []
    for line in open(os.path.join(S, "bridge_out.jsonl"), encoding="utf-8"):
        try: o = json.loads(line)
        except Exception: continue
        k = str(o.get("key")); en = str(o.get("en") or "").strip()
        if k in items and en:
            работа.append((k, items[k], en))
    if a.limit: работа = работа[:a.limit]
    print(f"к записи: {len(работа)} английских связей")
    if not a.commit:
        for k, x, en in работа[:10]:
            print(f"   {x['de']}  →  {en}")
        print("(сухой прогон — ничего не записано; добавьте --commit)")
        return

    conn = psycopg2.connect(os.environ["DB_URL"], connect_timeout=60)
    conn.autocommit = False
    итоги = collections.Counter()
    # Журнал свой у каждой цели: иначе прогон по стенду и по проду сливаются в один
    # файл, и не понять, что куда легло (поймано 16.09.2026).
    журнал = os.path.join(S, os.environ.get("JOURNAL") or "bridge_write_log.jsonl")
    кеш_единиц = {}
    for n, (k, x, en) in enumerate(работа, 1):
        uid = int(k.split(":")[0]); sid = k.split(":")[1]
        sid = int(sid) if sid != "0" else None
        try:
            eid = кеш_единиц.get(en.lower())
            if eid is None:
                eid = ensure_unit(en, "en")          # своя транзакция, идемпотентно
                кеш_единиц[en.lower()] = eid or 0
            if not eid:
                итоги["дверь английских единиц отказала"] += 1
                continue
            cur = conn.cursor()
            cur.execute("""INSERT INTO bt_3_lex_links (from_unit,to_unit,sense_id,rank,source)
                           VALUES (%s,%s,%s,1,%s)
                           ON CONFLICT (from_unit,to_unit)
                           DO UPDATE SET sense_id=EXCLUDED.sense_id, source=EXCLUDED.source,
                                         updated_at=NOW()""",
                        (uid, eid, sid, ИСТОЧНИК))
            conn.commit(); cur.close()
            итоги["записано"] += 1
            with open(журнал, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"key": k, "de": x["de"], "en": en, "en_unit": eid,
                                     "когда": datetime.datetime.now().isoformat()},
                                    ensure_ascii=False) + "\n")
        except Exception as e:
            conn.rollback()
            итоги[f"сбой: {type(e).__name__}"] += 1
            if итоги[f"сбой: {type(e).__name__}"] <= 3:
                print(f"  !! {x['de']} → {en}: {e}")
        if n % 2000 == 0:
            print(f"  {n}/{len(работа)}  {dict(итоги)}", flush=True)
    conn.close()
    print("\nИТОГ:", json.dumps(dict(итоги), ensure_ascii=False))


if __name__ == "__main__":
    main()
