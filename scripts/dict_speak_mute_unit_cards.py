# -*- coding: utf-8 -*-
"""Немая карточка: разбор есть, а перевода НЕТ НИ ОДНОГО.

Зачем
─────
У слова собран разбор — значения, примеры, формы, — но ни одной связи-перевода нет.
Человек открывает карточку и не узнаёт, что слово значит. При этом перевод лежит в
том же разборе, в поле translations: он просто не стал связью.

Замер 15.08.2026: таких слов было 172, и у ВСЕХ 172 перевод лежал внутри разбора.
После прогона осталось 51 — те, где перевода на русском нет и в разборе.

Страж
─────
«Переводом» считается только русский текст, не совпадающий с немецкой стороной.
Без этой проверки у «durchgehend rückläufiger Verlauf» переводом стал бы сам немецкий
текст, а у «die раскопки» — «die Ausgrabungen». Отказы и есть отдельная болезнь:
немецкие единицы с русским заголовком («die раскопки», «der кислотность») и
грамматическая помета «vt», заведённая словом.

Раскладку делает боевая функция sync_unit_links_from_card — та же, что работает после
ночного разбора. Ничего не выдумывается, модель не вызывается.

    python3 scripts/dict_speak_mute_unit_cards.py            # сухой прогон
    python3 scripts/dict_speak_mute_unit_cards.py --apply    # записать
"""
import os, re, sys
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP","1"); os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES","1")
sys.path.insert(0,"/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE")
from backend.database import get_db_connection_context
from backend import lex_units as LU

CYR = re.compile(r"[А-Яа-яЁё]")
APPLY = "--apply" in sys.argv

with get_db_connection_context() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT u.id, u.display, u.card
            FROM bt_3_lex_units u
            WHERE u.lang='de' AND u.card IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                              JOIN bt_3_lex_units t ON t.id = CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                              WHERE (l.from_unit=u.id OR l.to_unit=u.id) AND t.lang='ru' AND l.rank < 900)
            ORDER BY u.id
        """)
        rows = cur.fetchall()

good, refused = [], []
for uid, display, card in rows:
    c = card if isinstance(card, dict) else {}
    texts = []
    for item in (c.get("translations") or []):
        v = item.get("value") if isinstance(item, dict) else item
        if isinstance(v, str) and v.strip():
            texts.append(v.strip())
    # СТРАЖ: «перевод», совпадающий с немецкой стороной или без кириллицы, переводом
    # не является. У 43387 в переводах лежал сам немецкий текст.
    russian = [t for t in texts if CYR.search(t) and t.casefold() != str(display or "").casefold()]
    (good if russian else refused).append((uid, display, russian[:2] if russian else texts[:1]))

print("немых карточек: %d" % len(rows))
print("   можно разложить переводы:      %d" % len(good))
print("   перевода на русском нет:       %d" % len(refused))
print()
for uid, d, t in refused[:6]:
    print("   ОТКАЗ %-8s %-34s в разборе: %s" % (uid, str(d)[:34], (t or ["—"])[0][:36]))

if not APPLY:
    print("\nСУХОЙ ПРОГОН. Ничего не записано.")
    raise SystemExit

done = 0
for uid, display, _t in good:
    cur_card = next((c for i, _d, c in rows if i == uid), None)
    try:
        report = LU.sync_unit_links_from_card(uid, cur_card if isinstance(cur_card, dict) else {}, native_lang="ru")
        if report.get("links"):
            done += 1
    except Exception as exc:
        print("   слово %s: %s" % (uid, exc))
print("\nСлов получило переводы: %d из %d" % (done, len(good)))
