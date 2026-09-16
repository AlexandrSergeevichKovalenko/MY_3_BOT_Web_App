# -*- coding: utf-8 -*-
"""Запись английской стороны в базу. ТОЛЬКО ДОБАВЛЯЕТ: новые английские единицы и
связи немецкое→английское. Ничего не удаляет, не понижает, немецкой и русской
стороны не касается.

Переписано 16.09.2026 после первого прод-прогона. Что он показал:
  · по одной записи через интернет — 3,5 в секунду, полтора часа на 24 тысячи;
  · соединение рвётся на таком пробеге, а обработчик ошибки падал на закрытом;
  · повторный запуск начинал всё заново.
Поэтому: заведение единиц в потоках, связи ПАЧКАМИ, журнал ведётся по ходу и
повторный запуск продолжает с места.
"""
import argparse, json, os, sys, time, datetime, threading, collections
from concurrent.futures import ThreadPoolExecutor
import psycopg2, psycopg2.extras

S = os.environ.get("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, "/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2")
from backend.lex_units import ensure_unit

ИСТОЧНИК = "мост de→en 16.09.2026"
ПАЧКА = 500


def соединение():
    for п in range(5):
        try:
            c = psycopg2.connect(os.environ["DB_URL"], connect_timeout=60)
            c.autocommit = False
            return c
        except Exception as e:
            if п == 4: raise
            print(f"  соединение не встало ({e}), повтор", flush=True)
            time.sleep(4 * (п + 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--commit", action="store_true")
    a = ap.parse_args()

    items = {f'{x["unit_id"]}:{x.get("sense_id") or 0}': x
             for x in json.load(open(os.path.join(S, "bridge_input.json"), encoding="utf-8"))}
    журнал = os.path.join(S, os.environ.get("JOURNAL") or "bridge_write_log.jsonl")
    # ВОССТАНОВЛЕНИЕ С МЕСТА: что уже записано, повторно не пишем.
    сделано = set()
    if os.path.exists(журнал):
        for line in open(журнал, encoding="utf-8"):
            try: сделано.add(str(json.loads(line)["key"]))
            except Exception: pass

    работа = []
    for line in open(os.path.join(S, "bridge_out.jsonl"), encoding="utf-8"):
        try: o = json.loads(line)
        except Exception: continue
        k = str(o.get("key")); en = str(o.get("en") or "").strip()
        if k in items and en and k not in сделано:
            работа.append((k, items[k], en))
    if a.limit: работа = работа[:a.limit]
    print(f"уже записано: {len(сделано)} | осталось: {len(работа)}", flush=True)
    if not работа:
        print("всё записано."); return
    if not a.commit:
        for k, x, en in работа[:10]: print(f"   {x['de']}  →  {en}")
        print("(сухой прогон — ничего не записано; добавьте --commit)")
        return

    t0 = time.time()
    # ── ШАГ 1: английские единицы. Дверь в сеть не ходит, поэтому можно в потоках. ──
    тексты = sorted({en for _, _, en in работа})
    print(f"шаг 1: завожу английские единицы, уникальных текстов {len(тексты)}", flush=True)
    единицы, lock, n = {}, threading.Lock(), {"i": 0}

    def завести(t):
        try: uid = ensure_unit(t, "en")
        except Exception: uid = None
        with lock:
            единицы[t] = uid or 0
            n["i"] += 1
            if n["i"] % 2000 == 0:
                print(f"   единиц {n['i']}/{len(тексты)}  {round(time.time()-t0)} c", flush=True)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(завести, тексты))
    отказ_двери = sum(1 for v in единицы.values() if not v)
    print(f"   заведено {len(тексты)-отказ_двери}, дверь отказала {отказ_двери}, "
          f"{round(time.time()-t0)} c", flush=True)

    # ── ШАГ 2: связи ПАЧКАМИ. ──────────────────────────────────────────────────────
    # ⚠ ОДНА ПАРА — ОДНА СТРОКА В ПАЧКЕ. Два значения немецкого слова часто дают один
    # и тот же английский перевод (след перекоса «одно значение = один перевод»), и
    # тогда в пачку попадает одинаковая пара дважды. Postgres на это отвечает
    # CardinalityViolation: «ON CONFLICT DO UPDATE не может тронуть строку второй раз»,
    # и падает ВСЯ пачка — 12 770 записей из 23 766 на стенде 16.09.2026.
    # Схлопываем заранее: пишем пару один раз, а в журнал заносим ВСЕ ключи, которые
    # на неё пришлись, иначе повторный запуск будет пытаться дописать их вечно.
    первый, ключи_пары = {}, collections.defaultdict(list)
    for k, x, en in работа:
        if not единицы.get(en):
            continue
        пара = (int(k.split(":")[0]), единицы[en])
        ключи_пары[пара].append((k, x["de"], en))
        if пара not in первый:
            первый[пара] = (пара[0], пара[1],
                            (int(k.split(":")[1]) if k.split(":")[1] != "0" else None),
                            ИСТОЧНИК, k, x["de"], en)
    строки = list(первый.values())
    схлопнуто = sum(len(v) - 1 for v in ключи_пары.values())
    print(f"шаг 2: пишу {len(строки)} связей пачками по {ПАЧКА} "
          f"(схлопнуто одинаковых пар: {схлопнуто})", flush=True)
    итоги = collections.Counter({"дверь английских единиц отказала": отказ_двери})
    conn = соединение()
    for st in range(0, len(строки), ПАЧКА):
        пачка = строки[st:st + ПАЧКА]
        for п in range(4):
            try:
                cur = conn.cursor()
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO bt_3_lex_links (from_unit,to_unit,sense_id,rank,source)
                    VALUES %s
                    ON CONFLICT (from_unit,to_unit)
                    DO UPDATE SET sense_id=EXCLUDED.sense_id, source=EXCLUDED.source,
                                  updated_at=NOW()
                    """, [(r[0], r[1], r[2], 1, r[3]) for r in пачка],
                    template="(%s,%s,%s,%s,%s)")
                conn.commit(); cur.close()
                with open(журнал, "a", encoding="utf-8") as fh:
                    for r in пачка:
                        # в журнал — ВСЕ ключи, схлопнутые в эту пару
                        for kk, de_, en_ in ключи_пары[(r[0], r[1])]:
                            fh.write(json.dumps({"key": kk, "de": de_, "en": en_,
                                                 "en_unit": r[1],
                                                 "когда": datetime.datetime.now().isoformat()},
                                                ensure_ascii=False) + "\n")
                    fh.flush(); os.fsync(fh.fileno())
                итоги["записано"] += len(пачка)
                break
            except Exception as e:
                try: conn.rollback()
                except Exception: pass          # соединение могло уже закрыться
                if type(e).__name__ in ("InterfaceError", "OperationalError") and п < 3:
                    print(f"  пачка с {st} сорвалась ({type(e).__name__}), поднимаю соединение",
                          flush=True)
                    try: conn.close()
                    except Exception: pass
                    conn = соединение()
                    continue
                # ⚠ НЕ СОЕДИНЕНИЕ, А ДАННЫЕ. Одна негодная строка роняла ВСЮ пачку:
                # 1 000 записей из-за одной ссылки на удалённую немецкую единицу
                # (стенд 16.09.2026). Разбираем пачку по одной — падает только виноватая.
                разобрано = 0
                for r in пачка:
                    try:
                        cur = conn.cursor()
                        cur.execute("""INSERT INTO bt_3_lex_links
                                       (from_unit,to_unit,sense_id,rank,source)
                                       VALUES (%s,%s,%s,1,%s)
                                       ON CONFLICT (from_unit,to_unit)
                                       DO UPDATE SET sense_id=EXCLUDED.sense_id,
                                                     source=EXCLUDED.source, updated_at=NOW()""",
                                    (r[0], r[1], r[2], r[3]))
                        conn.commit(); cur.close()
                        with open(журнал, "a", encoding="utf-8") as fh:
                            for kk, de_, en_ in ключи_пары[(r[0], r[1])]:
                                fh.write(json.dumps({"key": kk, "de": de_, "en": en_,
                                                     "en_unit": r[1],
                                                     "когда": datetime.datetime.now().isoformat()},
                                                    ensure_ascii=False) + "\n")
                        разобрано += 1
                    except Exception as e2:
                        try: conn.rollback()
                        except Exception:
                            try: conn.close()
                            except Exception: pass
                            conn = соединение()
                        итоги[f"строка не легла: {type(e2).__name__}"] += 1
                итоги["записано"] += разобрано
                break
                print(f"  пачка с {st} сорвалась ({type(e).__name__}), поднимаю соединение",
                      flush=True)
                try: conn.close()
                except Exception: pass
                conn = соединение()
        if (st // ПАЧКА) % 10 == 0 and st:
            print(f"   {st}/{len(строки)}  {round(time.time()-t0)} c", flush=True)
    try: conn.close()
    except Exception: pass
    print("\nИТОГ:", json.dumps(dict(итоги), ensure_ascii=False),
          f"| минут: {round((time.time()-t0)/60,1)}")


if __name__ == "__main__":
    main()
