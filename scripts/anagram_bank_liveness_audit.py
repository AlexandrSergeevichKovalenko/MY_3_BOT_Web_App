# -*- coding: utf-8 -*-
"""Сколько МЁРТВЫХ слов лежит в банке анаграмм. Только чтение.

Повод
─────
Владельцу 13.09.2026 пришла анаграмма «Winkelgasse» (подсказка «Закоулок»). Слово
настоящее — статья есть у Гримма («winklige, schmale gasse», со второй половины
XVIII века), — но его нет ни в Duden, ни в немецком Wiktionary, ни в современных
словарях DWDS, и по корпусу DWDS оно встречается 58 раз на миллиард. Для сравнения:
Bahnhof 45 509, Eieruhr 120 (то самое слово, из-за которого 01.09.2026 поставили
приёмку ребусов). Владелец слово не узнал и спросил, много ли такого в банке.

Вопрос / популяция / правило отбора (этап 0 процедуры анализа)
──────────────────────────────────────────────────────────────
ВОПРОС:     сколько слов банка анаграмм — редкие (человек их не узнает) и
            сколько из них люди УЖЕ видели.
ПОПУЛЯЦИЯ:  все строки `bt_3_anagram_cards`; отдельно — те, что выдаются
            (retired = false), и те, что уже отправлялись (send_count > 0).
ПРАВИЛО:    берётся ИМПОРТОМ из продукта — `backend.rebus_word_gate.judge_rebus_word`
            (порог MIN_PER_BILLION, решение владельца 01.09.2026). Своего правила
            здесь нет: если продукт поменяет порог, отчёт поменяется вместе с ним.

Три исхода, и они РАЗНЫЕ (не складывать!):
    ходовое      — частота известна и не ниже порога;
    редкое       — частота известна и ниже порога;
    НЕ ЗНАЕМ     — DWDS не ответил. Это не ноль и не приговор, а незакрытая задача.

    python3 scripts/anagram_bank_liveness_audit.py            # отчёт
    python3 scripts/anagram_bank_liveness_audit.py --list 40  # + поимённый список
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.dwds_frequency import per_billion                  # noqa: E402
from backend.rebus_word_gate import MIN_PER_BILLION             # noqa: E402

# Корзины из разбора ребусов 01.09.2026 — те же границы, чтобы числа двух банков
# можно было класть рядом.
BANDS = (("ходовые  (> 3000)", 3000.0), ("нормальные (1000–3000)", 1000.0),
         ("нечастые (300–1000)", MIN_PER_BILLION), ("РЕДКИЕ  (< 300)", 0.0))


def band_of(ppb: float) -> str:
    for name, floor in BANDS:
        if ppb >= floor:
            return name
    return BANDS[-1][0]


def collect_frequencies(words: list[str]) -> dict[str, float | None]:
    """{слово: частота на миллиард} | None, если DWDS не ответил.

    Скрипт ТОЛЬКО ЧИТАЕТ: кеш `bt_3_dwds_frequency` берётся одним запросом, всё
    недостающее спрашивается у DWDS напрямую и в базу НЕ записывается. Пополнять
    кеш — дело ночного воркера (`backend/rebus_generator.py:161`), а не отчёта.

    Спрашиваем по одному ОДНИМ живым соединением (keep-alive). Замер 13.09.2026:
    новым соединением на каждое слово (так устроен `backend/dwds_frequency.py:84`)
    DWDS отвечает примерно на половину запросов, остальные виснут до таймаута —
    это и есть записанные там «69 слов из 338 не ответили». Через одно соединение
    ответили все. «Не ответил» остаётся отдельным состоянием None, нулём не
    подменяется.
    """
    import http.client
    import json as _json
    import urllib.parse as _url

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT word, per_billion FROM bt_3_dwds_frequency WHERE hits IS NOT NULL")
            cached = {w: float(p) for w, p in cur.fetchall() if p is not None}

    missing = [w for w in words if w not in cached]
    print(f"частоты: в кеше {len(words) - len(missing)}, спрашиваем у DWDS {len(missing)}", flush=True)

    conn_http = http.client.HTTPSConnection("www.dwds.de", timeout=20)
    for i, word in enumerate(missing, 1):
        answer = None
        for _try in range(2):
            try:
                conn_http.request("GET", "/api/frequency/?q=" + _url.quote(word),
                                  headers={"User-Agent": "TelegramDeutschBot/1.0", "Connection": "keep-alive"})
                data = _json.loads(conn_http.getresponse().read())
                answer = per_billion(int(data["hits"]), int(data["total"]))
                break
            except Exception:
                conn_http.close()
                conn_http = http.client.HTTPSConnection("www.dwds.de", timeout=20)
        cached[word] = answer
        if i % 20 == 0:
            print(f"  … спрошено {i}/{len(missing)}", flush=True)
    conn_http.close()
    return {w: cached.get(w) for w in words}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", type=int, default=0, help="сколько слов показать поимённо")
    args = ap.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT card_id, word, hint_ru, COALESCE(send_count, 0), COALESCE(retired, false)
                FROM bt_3_anagram_cards
                ORDER BY created_at
            """)
            rows = cur.fetchall()

    print(f"Банк анаграмм: {len(rows)} карточек "
          f"(выдаются {sum(1 for r in rows if not r[4])}, "
          f"сняты {sum(1 for r in rows if r[4])}, "
          f"уже отправлялись {sum(1 for r in rows if r[3] > 0)})")
    print(f"Порог продукта: {MIN_PER_BILLION:.0f} вхождений на миллиард (DWDS)\n", flush=True)

    freq = collect_frequencies([r[1] for r in rows])

    buckets: dict[str, list] = {name: [] for name, _ in BANDS}
    unknown: list = []
    for card_id, word, hint, sent, retired in rows:
        ppb = freq.get(word)
        if ppb is None:
            unknown.append((word, hint, sent, retired, None))
            continue
        buckets[band_of(ppb)].append((word, hint, sent, retired, ppb))

    print()
    for name, _ in BANDS:
        got = buckets[name]
        seen = sum(1 for r in got if r[2] > 0)
        live = sum(1 for r in got if not r[3])
        print(f"{name:<24} {len(got):>4}   из них выдаются {live:>4}, уже видели люди {seen:>4}")
    print(f"{'НЕ ЗНАЕМ (DWDS молчит)':<24} {len(unknown):>4}   "
          f"из них выдаются {sum(1 for r in unknown if not r[3]):>4}, "
          f"уже видели люди {sum(1 for r in unknown if r[2] > 0):>4}")

    if args.list:
        rare = sorted(buckets[BANDS[-1][0]], key=lambda r: r[4])
        print(f"\nРЕДКИЕ, самые редкие сверху (показано {min(args.list, len(rare))} из {len(rare)}):")
        for word, hint, sent, retired, ppb in rare[:args.list]:
            mark = "снято" if retired else ("ОТПРАВЛЕНО x%d" % sent if sent else "ждёт")
            print(f"  {ppb:>8.1f}  {word:<28} {str(hint)[:28]:<28} {mark}")
        # ── Разбор сырого числа на классы (этап 7 процедуры анализа) ──────────
        # «Редкое» само по себе ничего не значит: DWDS считает написание, а не
        # слово. «Verleumden» с заглавной — это ОТГЛАГОЛЬНОЕ существительное,
        # оно и вправду редкое; глагол «verleumden» строчными — обычное слово.
        # Поэтому каждое редкое слово перепроверяется в перевёрнутом регистре:
        # если там частота нормальная — дефект в НАПИСАНИИ, а не в редкости.
        flipped = {w: (w.lower() if w[:1].isupper() else w.capitalize())
                   for w, *_ in rare}
        flip_freq = collect_frequencies(sorted(set(flipped.values())))
        classes = {"нет такого слова": [], "испорчен регистр (слово ходовое)": [],
                   "правда редкое": []}
        for word, hint, sent, retired, ppb in rare:
            other = flip_freq.get(flipped[word])
            if ppb < 1.0 and (other is None or other < 1.0):
                classes["нет такого слова"].append((word, hint, sent, retired, ppb, other))
            elif other is not None and other >= MIN_PER_BILLION:
                classes["испорчен регистр (слово ходовое)"].append((word, hint, sent, retired, ppb, other))
            else:
                classes["правда редкое"].append((word, hint, sent, retired, ppb, other))
        print("\nРАЗБОР 'РЕДКИХ' ПО КЛАССАМ:")
        for name, items in classes.items():
            live = [r for r in items if not r[3]]
            print(f"\n  {name}: {len(items)}, из них выдаются {len(live)}")
            for word, hint, sent, retired, ppb, other in sorted(items, key=lambda r: r[4]):
                mark = "снято" if retired else f"ВЫДАЁТСЯ, отправлено x{sent}"
                oth = "—" if other is None else f"{other:.0f}"
                print(f"    {word:<24} {hint[:22]:<22} как записано {ppb:>8.1f} | "
                      f"другой регистр {oth:>8} | {mark}")

        live_rare = [r for r in rare if not r[3]]
        print(f"\nиз них ВЫДАЮТСЯ ЛЮДЯМ ПРЯМО СЕЙЧАС: {len(live_rare)}")
        for word, hint, sent, retired, ppb in live_rare:
            print(f"  {ppb:>8.1f}  {word:<28} {str(hint)[:28]:<28} отправлено x{sent}")
        if unknown:
            print(f"\nНЕ ЗНАЕМ (показано {min(args.list, len(unknown))} из {len(unknown)}):")
            for word, hint, sent, retired, _ in unknown[:args.list]:
                mark = "снято" if retired else ("ОТПРАВЛЕНО x%d" % sent if sent else "ждёт")
                print(f"            {word:<28} {str(hint)[:28]:<28} {mark}")


if __name__ == "__main__":
    main()
