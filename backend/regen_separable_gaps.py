"""Разовый прогон: заданиям на отделяемый глагол — второй пропуск.

Зачем
─────
До 14.08.2026 задание показывало ОДИН пропуск, а правильным ответом называло
инфинитив: «Er ___ die neuen Aufgaben sofort.» → «annehmen». Такого немецкого
не бывает: у отделяемого глагола два места в предложении — спрягаемая форма и
приставка в конце («Er nimmt die neuen Aufgaben sofort an»). Замер 14.08.2026:
199 записей, 199 из 199 не собирались обратно.

Как чиним
─────────
Сами предложения в базе ПРАВИЛЬНЫЕ — испорчен только показанный пропуск.
Поэтому чиним арифметикой из того, что уже лежит, без обращения к модели:

    было:  «Er ___ die neuen Aufgaben sofort.»   + «Er nimmt … sofort an.»
    стало: «Er ___ die neuen Aufgaben sofort ___.»  verb_form=«nimmt» prefix=«an»

Спрягаемая форма берётся выравниванием старого пропуска с правильным
предложением, а не догадкой. Результат проверяется тем же стражем, что стоит на
приёмке и перед показом (`backend_server.gap_reconstructs_sentence`): подставь
форму в первый пропуск, приставку во второй — обязано выйти правильное
предложение слово в слово. Не сошлось — запись уходит в перегенерацию через
модель (`--no-llm` это запрещает и просто помечает такие записи).

Запуск (нужен доступ к живой базе):
    python -m backend.regen_separable_gaps --dry-run   # посчитать, ничего не менять
    python -m backend.regen_separable_gaps             # починить
"""

from __future__ import annotations

import argparse
import os
import re

# Гасим runtime-side-effects ДО импорта монолита, чтобы разовый процесс не
# поднял планировщики и не писал в биллинговую ведомость.
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
os.environ.setdefault("BILLING_OPENAI_SNAPSHOT_SYNC_ON_STARTUP", "0")

from backend.backend_server import (  # noqa: E402
    _coerce_response_json,
    _normalize_space,
    _get_separable_prefix_quiz_item_with_retry,
    gap_reconstructs_sentence,
    separable_gap_entry_is_sound,
    separable_sentence_sounds_native,
)
from backend.database import (  # noqa: E402
    get_db_connection_context,
    update_webapp_dictionary_entry,
)

TRAILING_PUNCT_RE = re.compile(r"^(.*?)([.!?]*)$", re.DOTALL)


def repair_payload_without_llm(payload: dict) -> dict | None:
    """Достроить второй пропуск из того, что уже лежит в записи.

    Возвращает новый payload или None, если выровнять не удалось (тогда запись
    честно уходит в перегенерацию, а не чинится наугад)."""
    gap = _normalize_space(payload.get("sentence_with_gap"))
    full = _normalize_space(payload.get("correct_full_sentence"))
    prefix = _normalize_space(payload.get("prefix"))
    infinitive = _normalize_space(payload.get("correct_infinitive"))
    if gap.count("___") != 1 or not full or not prefix:
        return None

    left, right = gap.split("___", 1)
    matched = TRAILING_PUNCT_RE.match(right)
    right_core, punct = matched.group(1), matched.group(2)
    if not full.startswith(left):
        return None

    # Хвост правильного предложения обязан заканчиваться на «…остаток + приставка».
    tail = full[len(left):]
    expected_end = f"{right_core} {prefix}{punct}"
    if not tail.endswith(expected_end):
        return None
    verb_form = tail[: len(tail) - len(expected_end)].strip()
    if not verb_form or " " in verb_form or not re.fullmatch(r"[A-Za-zÄÖÜäöüß]+", verb_form):
        return None
    if verb_form.lower() == infinitive.lower():
        return None

    new_gap = f"{left}___{right_core} ___{punct}"
    if not gap_reconstructs_sentence(new_gap, [verb_form, prefix], full):
        return None

    repaired = dict(payload)
    repaired["sentence_with_gap"] = new_gap
    repaired["verb_form"] = verb_form
    repaired["source_text"] = new_gap
    return repaired


def main() -> None:
    parser = argparse.ArgumentParser(description="Дать заданиям на отделяемый глагол второй пропуск")
    parser.add_argument("--dry-run", action="store_true", help="только посчитать")
    parser.add_argument("--limit", type=int, default=None, help="ограничить число записей")
    parser.add_argument("--no-llm", action="store_true",
                        help="не звать модель для тех, кого не удалось выровнять")
    parser.add_argument("--check-sense", action="store_true",
                        help="дополнительно судить предложения на живость и переписывать неживые")
    args = parser.parse_args()

    stats = {
        "scanned": 0, "already_sound": 0, "unnatural": 0,
        "repaired_offline": 0, "regenerated_llm": 0,
        "unfixable": 0, "failed": 0,
    }
    sense_cache: dict[str, bool] = {}

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, user_id, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json::text LIKE '%separable_prefix_verb_gap%'
                ORDER BY id
            """)
            rows = cursor.fetchall()

    if args.limit:
        rows = rows[: args.limit]

    # Дедуп ПОФАМИЛЬНО, как в продукте (`_ensure_sentence_gpt_seed_entries`):
    # одно и то же предложение у РАЗНЫХ людей — норма, у одного человека — нет.
    # Глобальный дедуп здесь оставлял записи непочиненными: модель при
    # temperature 0.35 повторяется, а свободных предложений «на всех» не хватает.
    seen_by_user: dict[int, set] = {}
    for _, uid, rj in rows:
        sentence = _normalize_space(_coerce_response_json(rj).get("correct_full_sentence")).lower()
        seen_by_user.setdefault(int(uid or 0), set()).add(sentence)

    for entry_id, user_id, raw_json in rows:
        seen_sentences = seen_by_user.setdefault(int(user_id or 0), set())
        stats["scanned"] += 1
        payload = _coerce_response_json(raw_json)
        structure_ok = separable_gap_entry_is_sound(payload)

        # Форма сошлась — ещё не значит, что предложение живое. Судья смотрит
        # управление и сочетаемость: «Er steht … zur Arbeit auf» так не говорят.
        # Одно и то же предложение стоит у нескольких людей, поэтому вердикт
        # кешируется — иначе платим за один и тот же вопрос по нескольку раз.
        sense_ok = True
        if structure_ok and args.check_sense:
            sentence = _normalize_space(payload.get("correct_full_sentence"))
            if sentence not in sense_cache:
                sense_cache[sentence] = separable_sentence_sounds_native(
                    sentence, payload.get("translation_ru"), payload.get("correct_infinitive"))
            sense_ok = sense_cache[sentence]
            if not sense_ok:
                stats["unnatural"] += 1
                print(f"[неживое] id={entry_id}: {sentence}")

        if structure_ok and sense_ok:
            stats["already_sound"] += 1
            continue

        # Выравниванием чинится только сломанная ФОРМА. Неживое предложение
        # выравнивать бессмысленно — его надо писать заново.
        repaired = repair_payload_without_llm(payload) if not structure_ok else None
        source = "offline"

        if repaired is None:
            if args.no_llm:
                stats["unfixable"] += 1
                print(f"[не выровнялось] id={entry_id}: {payload.get('sentence_with_gap')!r}")
                continue
            try:
                # Глагол сохраняем — чинить надо КОНТЕКСТ, а не подменять урок.
                # «abfahren» остаётся abfahren, просто уезжает не «zur Arbeit»,
                # а туда, куда по-немецки действительно уезжают.
                fresh = _get_separable_prefix_quiz_item_with_retry(
                    max_retries=2,
                    target_infinitive=payload.get("correct_infinitive"),
                    avoid_sentences=sorted(seen_sentences)[:20],
                )
            except Exception as exc:
                stats["failed"] += 1
                print(f"[ошибка модели] id={entry_id}: {exc}")
                continue
            new_sentence = _normalize_space(fresh.get("correct_full_sentence")).lower()
            if new_sentence in seen_sentences:
                stats["failed"] += 1
                print(f"[дубль] id={entry_id}: {new_sentence!r}")
                continue
            # Свежее предложение проходит того же судью — иначе прогон просто
            # заменит одно неживое предложение другим неживым.
            if args.check_sense and not separable_sentence_sounds_native(
                fresh.get("correct_full_sentence"), fresh.get("translation_ru"),
                fresh.get("correct_infinitive")
            ):
                stats["failed"] += 1
                print(f"[неживое от модели] id={entry_id}: {new_sentence!r}")
                continue
            seen_sentences.add(new_sentence)
            repaired = {
                **payload,
                **fresh,
                "source_text": fresh.get("sentence_with_gap"),
                "target_text": fresh.get("correct_infinitive"),
            }
            source = "llm"

        if not separable_gap_entry_is_sound(repaired):
            stats["failed"] += 1
            print(f"[страж отклонил] id={entry_id}")
            continue

        if args.dry_run:
            stats["repaired_offline" if source == "offline" else "regenerated_llm"] += 1
            if stats["scanned"] <= 5 or stats["scanned"] % 50 == 0:
                print(f"[{source}] id={entry_id}: {repaired['sentence_with_gap']}"
                      f"  ({repaired.get('verb_form')} … {repaired.get('prefix')})")
            continue

        try:
            update_webapp_dictionary_entry(
                int(entry_id), repaired,
                translation_de=str(repaired.get("correct_infinitive") or "") or None,
            )
        except Exception as exc:
            stats["failed"] += 1
            print(f"[ошибка записи] id={entry_id}: {exc}")
            continue
        stats["repaired_offline" if source == "offline" else "regenerated_llm"] += 1

    print("\nRESULT:", stats)
    if args.dry_run:
        print("Это был dry-run: база не тронута.")


if __name__ == "__main__":
    main()
