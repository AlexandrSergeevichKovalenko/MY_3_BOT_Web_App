# -*- coding: utf-8 -*-
"""СУХОЙ ПРОГОН правок немецко-русской базы. НИЧЕГО НЕ ПИШЕТ. Только читает и печатает план."""
import json, os, re, sys, collections
import psycopg2, psycopg2.extras

S = os.path.dirname(os.path.abspath(__file__))
URL = os.environ["DB_URL"]

items   = {f'{x["unit_id"]}:{x.get("sense_id") or 0}': x
           for x in json.load(open(os.path.join(S, "items_all.json"), encoding="utf-8"))}
actions = json.load(open(os.path.join(S, "final_actions.json"), encoding="utf-8"))
fixes   = json.load(open(os.path.join(S, "final_verdicts.json"), encoding="utf-8"))["fixes"]

цель = {k: v for k, v in actions.items() if v.split()[0].isupper()}
units = sorted({int(k.split(":")[0]) for k in цель})
print(f"записей к правке: {len(цель)}  |  немецких единиц: {len(units)}", flush=True)

conn = psycopg2.connect(URL); conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# все русские связи затронутых единиц
cur.execute("""
    SELECT l.from_unit, l.to_unit, l.sense_id, l.rank, l.source, ru.display AS ru_text
      FROM bt_3_lex_links l
      JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang = 'ru'
     WHERE l.from_unit = ANY(%s)
""", (units,))
связи = collections.defaultdict(list)
for r in cur.fetchall():
    связи[int(r["from_unit"])].append(dict(r))

# существуют ли уже русские единицы с нужным новым текстом
новые = sorted({str(fixes[k]).strip() for k in цель if цель[k] == "ЗАМЕНИТЬ перевод" and fixes.get(k)})
cur.execute("SELECT id, display, lemma_key FROM bt_3_lex_units WHERE lang='ru' AND lower(btrim(display)) = ANY(%s)",
            ([t.lower() for t in новые],))
есть_ru = {r["display"].strip().lower(): int(r["id"]) for r in cur.fetchall()}

ЛАТ = re.compile(r"[A-Za-zÄÖÜäöüß]")
КИР = re.compile(r"[А-Яа-яЁё]")

def чистка_поля(ru: str):
    """Мусор в поле: «немецкое -> русское», склейка, повтор немецкого. Без модели."""
    t = str(ru or "").strip()
    if "->" in t:
        хвост = t.split("->")[-1].strip()
        if КИР.search(хвост) and len(ЛАТ.findall(хвост)) <= len(КИР.findall(хвост)) * 0.15:
            return хвост, "взята часть после стрелки"
    if " — " in t:
        хвост = t.split(" — ")[-1].strip()
        if КИР.search(хвост) and len(ЛАТ.findall(хвост)) <= len(КИР.findall(хвост)) * 0.15:
            return хвост, "взята часть после тире"
    return None, "правило не сработало — НЕ ТРОГАЕМ, в отчёт"

def убрать_повтор(ru: str, kind: str):
    сеп = r"[;,]" if kind in ("word", "collocation") else r";"
    части = [p.strip() for p in re.split(сеп, str(ru or "")) if p.strip()]
    видели, оставить = set(), []
    for p in части:
        ключ = p.lower().rstrip(".")
        if ключ in видели: continue
        видели.add(ключ); оставить.append(p)
    if len(оставить) == len(части): return None, "повтора не нашли"
    склейка = "; " if ";" in str(ru) else ", "
    return склейка.join(оставить), f"убрано повторов: {len(части)-len(оставить)}"

план, проблемы = [], collections.Counter()
for k, действие in цель.items():
    x = items[k]; uid = int(k.split(":")[0]); sid = k.split(":")[1]
    sid = int(sid) if sid != "0" else None
    шаг = {"key": k, "de": x["de"], "kind": x["kind"], "было": x["ru"], "действие": действие}
    if действие == "ЗАМЕНИТЬ перевод":
        новый = str(fixes.get(k) or "").strip()
        шаг["станет"] = новый
        if not новый: шаг["итог"] = "ОТКАЗ: замена пустая"; проблемы["замена пустая"] += 1
        else:
            мои = [s for s in связи.get(uid, []) if (s["sense_id"] == sid)]
            чужие = [s for s in связи.get(uid, []) if s["sense_id"] != sid]
            new_id = есть_ru.get(новый.lower())
            занята = new_id and any(int(s["to_unit"]) == new_id for s in чужие)
            if занята:
                шаг["итог"] = "ОТКАЗ: у этого слова другое значение уже ведёт к этому переводу"
                проблемы["конфликт значений"] += 1
            elif not мои:
                шаг["итог"] = "ОТКАЗ: связь этого значения не найдена"
                проблемы["связь не найдена"] += 1
            else:
                шаг["итог"] = (f"связь значения → «{новый}» ({'единица есть' if new_id else 'единицу завести'}), "
                               f"старую понизить; соседних связей не трогаем: {len(чужие)}")
                проблемы["готово к правке"] += 1
    elif действие.startswith("ПОЧИСТИТЬ"):
        нов, почему = чистка_поля(x["ru"])
        шаг["станет"] = нов; шаг["итог"] = почему
        проблемы["чистка поля: " + ("готово" if нов else "правило не сработало")] += 1
    elif действие.startswith("УБРАТЬ"):
        нов, почему = убрать_повтор(x["ru"], x["kind"])
        шаг["станет"] = нов; шаг["итог"] = почему
        проблемы["повтор: " + ("готово" if нов else "не нашли")] += 1
    план.append(шаг)

json.dump(план, open(os.path.join(S, "dry_run_plan.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("\nИТОГ СУХОГО ПРОГОНА:")
for k, v in проблемы.most_common(): print(f"   {k:<52} {v:>5}")
conn.close()
