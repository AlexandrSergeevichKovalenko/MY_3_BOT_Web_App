# -*- coding: utf-8 -*-
"""Перевести заново строки общего пула, пришедшие от слабого переводчика.

┌─ ПОВОД, владелец 09.09.2026 ────────────────────────────────────────────────────────┐
│ Быстрый перевод вызывал DeepL и MyMemory ОДНОВРЕМЕННО и брал MyMemory, когда DeepL   │
│ не успевал за три секунды. MyMemory — память чужих переводов с машинной затычкой:    │
│ замер на 20 живых фразах пула дал у него 6 ошибок против одной у DeepL. Владельцу он │
│ выдал «Ich rate ins Blaue hinein» → «Я даю совет наугад» (raten понят как «советую»).│
│                                                                                     │
│ Система починена в тот же день: гонки больше нет, MyMemory убран, и КАЖДАЯ новая     │
│ строка пула несёт поле translator. Но у накопленных строк этого поля нет, и отличить │
│ строку MyMemory от строки DeepL нечем — логи живут один деплой. Поэтому переводим    │
│ ЗАНОВО весь машинный многословный сентябрь и заменяем там, где ответ разошёлся.      │
└─────────────────────────────────────────────────────────────────────────────────────┘

Трогаем ТОЛЬКО общий пул и ТОЛЬКО машинные строки (без разбора). Личные карточки людей
не трогаем никогда: чужое правит владелец кнопкой, а не скрипт.

⚠ ЗАМЕНА ИДЁТ ЧЕРЕЗ СУДЬЮ, А НЕ ПОДРЯД. Сухой прогон 09.09.2026 на 30 строках дал 23
расхождения с DeepL — но большинство это «верное на другое верное» («Я вывихнул ногу» →
«Я подвернул ногу», «Компания расширила и увеличила производство» → «…объёмы
производства»). Слепая замена всех была бы не починкой, а перемешиванием: она стирает
верные переводы ради одинакового источника. Поэтому каждое расхождение судит модель по
НЕМЕЦКОМУ оригиналу, и старый перевод остаётся, если в нём нет ошибки.

Запуск:
    python3 scripts/pool_retranslate_weak_provider.py            # сухой прогон
    python3 scripts/pool_retranslate_weak_provider.py --apply    # с записью
    python3 scripts/pool_retranslate_weak_provider.py --since 2026-08-01 --apply
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio  # noqa: E402

import requests  # noqa: E402

from backend.database import get_db_connection_context  # noqa: E402
from backend.openai_manager import run_pool_translation_judge  # noqa: E402

DEEPL_URL = "https://api-free.deepl.com/v2/translate"
ПАУЗА_СЕК = 0.2


def deepl(text: str, key: str) -> str:
    """Перевод от DeepL. Ошибка сети — исключение, а не пустая строка: пустая строка
    неотличима от «перевода нет», и такую в базу писать нельзя."""
    resp = requests.post(
        DEEPL_URL,
        data={"text": [text], "source_lang": "DE", "target_lang": "RU"},
        headers={"Authorization": f"DeepL-Auth-Key {key}"},
        timeout=20,
    )
    if not resp.ok:
        raise RuntimeError(f"DeepL HTTP {resp.status_code}: {resp.text[:200]}")
    translations = (resp.json() or {}).get("translations") or []
    text_out = str((translations[0] if translations else {}).get("text") or "").strip()
    if not text_out:
        raise RuntimeError("DeepL вернул пустой перевод")
    return text_out


def строки_к_переводу(cur, since: str) -> list[tuple]:
    """Машинные многословные строки пула: без разбора и без следа переводчика.

    `response_json ? 'translator'` — уже перепроверенная строка (или новая, с полем от
    починенной цепочки); такие не трогаем."""
    cur.execute(
        """
        SELECT id, source_text, target_text, response_json
        FROM bt_3_dictionary_entries
        WHERE source_lang = 'de' AND target_lang = 'ru'
          AND created_at >= %s
          AND source_text LIKE '%% %%'
          AND (response_json IS NULL
               OR NOT (response_json ? 'meanings' OR response_json ? 'translations'))
          AND (response_json IS NULL OR NOT (response_json ? 'translator'))
        ORDER BY id;
        """,
        (since,),
    )
    return cur.fetchall()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать изменения")
    parser.add_argument("--since", default="2026-09-01", help="с какой даты брать строки")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число строк (для пробы)")
    parser.add_argument("--no-judge", action="store_true",
                        help="без судьи: заменять КАЖДОЕ расхождение (только для замера)")
    args = parser.parse_args()

    key = (os.getenv("DEEPL_AUTH_KEY") or "").strip()
    if not key:
        print("DEEPL_AUTH_KEY не задан — переводить нечем", file=sys.stderr)
        sys.exit(2)

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            rows = строки_к_переводу(cur, args.since)
    if args.limit:
        rows = rows[: args.limit]
    print(f"строк к перепроверке с {args.since}: {len(rows)}")

    заменено = сохранено = сбоев = оставлено_судьёй = 0
    примеры: list[tuple[str, str, str]] = []
    for idx, (rid, source, target, payload) in enumerate(rows):
        if idx:
            time.sleep(ПАУЗА_СЕК)
        try:
            новый = deepl(source, key)
        except Exception as exc:
            сбоев += 1
            print(f"  ✗ {rid}: {exc}")
            continue
        одинаково = новый.strip().casefold() == str(target or "").strip().casefold()
        payload = dict(payload or {})
        payload["translator"] = "deepl_free"
        if одинаково:
            сохранено += 1
            if args.apply:
                # След переводчика ставим и совпавшим: иначе следующий прогон снова
                # потратит на них запрос, а вопрос «кто перевёл» останется без ответа.
                with get_db_connection_context() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE bt_3_dictionary_entries SET response_json = %s, updated_at = NOW() "
                            "WHERE id = %s;",
                            (json.dumps(payload, ensure_ascii=False), rid),
                        )
                    conn.commit()
            continue
        # СУДЬЯ. Вопрос ему один: есть ли ошибка в том, что лежит, — а не «какой
        # красивее». Ответ «старый верен» оставляет строку как есть.
        вердикт = {}
        if not args.no_judge:
            try:
                вердикт = asyncio.run(run_pool_translation_judge(
                    german=source, variant_a=str(target or ""), variant_b=новый))
            except Exception as exc:
                сбоев += 1
                print(f"  ✗ судья {rid}: {exc}")
                continue
            лучший = str(вердикт.get("best") or "").strip().lower()
            if лучший == "a":
                оставлено_судьёй += 1
                if args.apply:
                    with get_db_connection_context() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE bt_3_dictionary_entries SET response_json = %s, updated_at = NOW() "
                                "WHERE id = %s;",
                                (json.dumps({**payload, "translator": "deepl_free_checked"},
                                            ensure_ascii=False), rid),
                            )
                        conn.commit()
                continue
            if лучший == "own":
                своё = str(вердикт.get("own") or "").strip()
                if своё:
                    новый = своё
        заменено += 1
        if len(примеры) < 15:
            примеры.append((source, str(target or ""), новый))
        if args.apply:
            for k in ("target_text", "translation_ru", "word_ru", "word_target"):
                if k in payload:
                    payload[k] = новый
            norm = " ".join(новый.split()).casefold()
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE bt_3_dictionary_entries
                        SET target_text = %s, target_text_norm = %s, translation_ru = %s,
                            word_ru = %s, response_json = %s, updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (новый, norm, новый, новый, json.dumps(payload, ensure_ascii=False), rid),
                    )
                conn.commit()

    print()
    print(f"совпало с DeepL:      {сохранено}")
    print(f"судья оставил старое:  {оставлено_судьёй}")
    print(f"разошлось (заменено): {заменено}" if args.apply else f"разошлось (заменить):  {заменено}")
    print(f"сбоев перевода:       {сбоев}")
    if примеры:
        print("\nпримеры расхождений (было → стало):")
        for source, было, стало in примеры:
            print(f"  {source[:60]}\n     было:  {было[:70]}\n     стало: {стало[:70]}")
    if not args.apply:
        print("\nэто сухой прогон, ничего не записано; повторить с --apply")


if __name__ == "__main__":
    main()
