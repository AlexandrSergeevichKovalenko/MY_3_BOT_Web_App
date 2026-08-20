# -*- coding: utf-8 -*-
"""Слова, лежащие не в своей теме, — переселить, а не выбросить.

ПОВОД. Открытым пунктом с 16.08.2026 висело: «проверки „подходит ли слово ТЕМЕ“
не существует вообще». 19–20.08.2026 подтвердилось живыми примерами: `der
Linkshänder` («левша») лежал в подтеме «интернет и связь» темы про компьютеры, а
ночной добор принёс в «Праздники и застолья» — `das Deo` и `die Schwangerschaft`,
в «Магазин и услуги» — `der Hausflur`.

Почему это дефект. Набор дня подписан именем темы. Человек открывает «Праздники»
и получает беременность — подписи он больше не верит.

ДВЕ СТУПЕНИ, и вторая важнее первой.
  1. «На своём ли месте?» — `judge_theme_fit`, три голоса, большинство. Судья
     намеренно строг («сомневаешься — отвечай НЕТ»), поэтому один он ошибался бы
     в сторону лишних придирок: на пробе 20.08.2026 он вынес `der Türsteher`
     (вышибала) из «Праздников» — для «Вечеринок» слово нормальное.
  2. «А куда его?» — `suggest_theme` выбирает тему ИЗ СУЩЕСТВУЮЩИХ. И это чинит
     строгость первой ступени: если лучшая тема совпала с нынешней, слово
     остаётся на месте. `Türsteher` на второй ступени уехал в «Работу и профессии»
     — то есть стало лучше, чем было, а не хуже.

Слово НЕ удаляется и НЕ снимается с показа никогда: оно хорошее, просто лежало не
там. Если переезд невозможен (в теме-приёмнике уже есть такое слово с тем же
артиклем — мешает уникальный индекс), строка снимается как дубль, и живой остаётся
карточка в правильной теме.

Запуск:
    python -m scripts.artikel_bank_fix_theme_fit --theme feste_traditionen
    python -m scripts.artikel_bank_fix_theme_fit               # все темы, отчёт
    python -m scripts.artikel_bank_fix_theme_fit --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

# Публичный прокси базы периодически рвёт соединение (documented: имя перестаёт
# резолвиться, SSL EOF). Лечится ПОВТОРОМ С ОЖИДАНИЕМ, а не пином IP. Без повтора
# обход 20.08.2026 дважды умирал на середине — и терял уже оплаченные ответы модели.
_DB_RETRIES = 4
_DB_WAIT_SECONDS = 20


def _with_retry(fn, *args, **kwargs):
    last: Exception | None = None
    for attempt in range(_DB_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:            # обрыв соединения, не логика
            last = exc
            if attempt + 1 < _DB_RETRIES:
                print(f"  ⏳ база оборвалась ({type(exc).__name__}), жду {_DB_WAIT_SECONDS} с")
                time.sleep(_DB_WAIT_SECONDS)
    raise RuntimeError(f"база недоступна после {_DB_RETRIES} попыток: {last}")


# Пройденные темы запоминаем на диск: ответы модели стоят денег, и повторный
# прогон не должен платить за них второй раз.
_DONE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data",
                          "artikel_theme_fit_done.jsonl")


def _already_done() -> dict:
    out: dict = {}
    if not os.path.exists(_DONE_FILE):
        return out
    with open(_DONE_FILE, encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("theme"):
                out[row["theme"]] = row
    return out


def _remember_done(theme_key: str, moves: list[dict]) -> None:
    os.makedirs(os.path.dirname(_DONE_FILE), exist_ok=True)
    with open(_DONE_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"theme": theme_key, "moves": moves}, ensure_ascii=False) + "\n")


def theme_rows(theme_key: str) -> list[dict]:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, word, article, meaning_ru FROM bt_3_article_sprint_nouns "
                "WHERE theme_key = %s AND retired = FALSE AND verified = TRUE ORDER BY word;",
                (theme_key,))
            return [{"id": r[0], "word": r[1], "article": r[2], "meaning_ru": r[3]}
                    for r in cur.fetchall()]


def move(row: dict, target: str) -> str:
    """Переселить строку. Мешает уникальный индекс — снимаем как дубль."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM bt_3_article_sprint_nouns WHERE theme_key = %s "
                "AND lower(word) = lower(%s) AND article = %s;",
                (target, row["word"], row["article"]))
            if cur.fetchone():
                cur.execute(
                    "UPDATE bt_3_article_sprint_nouns SET retired = TRUE, "
                    "retire_reason = 'дубль: слово уже стоит в подходящей теме', "
                    "retire_reviewed = TRUE, updated_at = NOW() WHERE id = %s;", (row["id"],))
                conn.commit()
                return "снято как дубль — в новой теме такое слово уже есть"
            # Подтему стираем: она была из СТАРОЙ темы и в новой не значит ничего.
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns SET theme_key = %s, subtopic = '', "
                "updated_at = NOW() WHERE id = %s;", (target, row["id"]))
        conn.commit()
    return "переселено"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--theme", default="", help="только одна тема")
    parser.add_argument("--fresh", action="store_true",
                        help="переспросить всё заново, забыв прошлый прогон")
    args = parser.parse_args()

    from backend.article_sprint_themes import article_sprint_themes
    from backend.article_theme_fit import (
        judge_theme_fit, suggest_theme, ThemeFitJudgeUnavailable, BATCH)

    themes = _with_retry(article_sprint_themes)
    done = {} if args.fresh else _already_done()
    targets = [t for t in themes if not args.theme or t["key"] == args.theme]
    if done:
        print(f"уже пройдено ранее: {len(done)} тем — их не переспрашиваю\n")
    if not targets:
        print(f"темы «{args.theme}» нет")
        return 1

    total_moved = 0
    total_checked = 0
    for theme in targets:
        if theme["key"] in done and not args.apply:
            continue
        rows = _with_retry(theme_rows, theme["key"])
        if not rows:
            continue
        total_checked += len(rows)
        misfits: list[dict] = []
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            try:
                fit = judge_theme_fit(theme, chunk)
            except ThemeFitJudgeUnavailable as exc:
                # Ни один голос не дошёл. Это НЕ «все чужие» — пачку просто
                # пропускаем, следующий прогон спросит снова.
                print(f"  ⚠️ {theme['label_ru']}: голосов нет ({exc}) — пачка отложена")
                continue
            misfits.extend(r for r in chunk if not fit.get(r["word"], True))
        if not misfits:
            print(f"{theme['label_ru']}: всё на своих местах ({len(rows)} слов)")
            _remember_done(theme["key"], [])   # иначе следующий прогон заплатит за неё снова
            continue
        # Вторая ступень: куда именно. Она же лечит строгость первой.
        picks: dict[str, str] = {}
        for i in range(0, len(misfits), BATCH):
            try:
                picks.update(suggest_theme(misfits[i:i + BATCH], themes))
            except ThemeFitJudgeUnavailable as exc:
                print(f"  ⚠️ {theme['label_ru']}: не смог выбрать тему ({exc})")
        real = [(r, picks.get(r["word"], "")) for r in misfits]
        real = [(r, p) for r, p in real if p and p != theme["key"]]
        stayed = len(misfits) - len(real)
        labels = {t["key"]: t["label_ru"] for t in themes}
        print(f"{theme['label_ru']}: проверено {len(rows)}, "
              f"не на месте {len(misfits)}, из них переезжают {len(real)}"
              + (f", остаются (лучшей темы нет) {stayed}" if stayed else ""))
        for row, target in real:
            line = f"  {row['article']} {row['word']:20s} {(row['meaning_ru'] or '')[:24]:26s} → {labels.get(target)}"
            if args.apply:
                line += "   [" + _with_retry(move, row, target) + "]"
            print(line)
            total_moved += 1
        _remember_done(theme["key"], [{"word": r["word"], "article": r["article"],
                                       "to": t} for r, t in real])

    print(f"\nпроверено слов: {total_checked}; на переезд: {total_moved}")
    if not args.apply:
        print("(отчёт; чтобы применить — --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
