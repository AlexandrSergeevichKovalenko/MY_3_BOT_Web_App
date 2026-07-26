"""Активный прогон-чистка «Satz Ergänzen» заданий по всему пулу.

Пересобирает кешированные sentence-gap задания с новым стражем качества
(пропускается КЛЮЧЕВОЕ смысловое слово, а не филлер вроде sehr/bitte). Работает
для ВСЕХ пользователей — данные в общем пуле bt_3_webapp_dictionary_queries.

Запуск внутри контейнера Railway (есть доступ к живой базе + OpenAI):
    python -m backend.regen_sentence_gaps --dry-run          # только посчитать
    python -m backend.regen_sentence_gaps --only-trivial     # чистить только мусор
    python -m backend.regen_sentence_gaps                    # пересобрать всё
"""

from __future__ import annotations

import argparse
import os

# ВАЖНО: гасим runtime-side-effects ДО импорта монолита, чтобы этот разовый
# процесс не поднял планировщики/автосиды в контейнере BACKEND_WEB.
os.environ.setdefault("BACKEND_RUNTIME_SIDE_EFFECTS_ENABLED", "0")

from backend.backend_server import regenerate_all_sentence_gap_tasks  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate Satz-Ergänzen gap tasks pool-wide")
    parser.add_argument("--dry-run", action="store_true", help="только посчитать, ничего не менять")
    parser.add_argument("--only-trivial", action="store_true", help="трогать только мусорные (филлер-слово)")
    parser.add_argument("--limit", type=int, default=None, help="ограничить число обработанных записей")
    args = parser.parse_args()

    stats = regenerate_all_sentence_gap_tasks(
        only_trivial=args.only_trivial,
        max_items=args.limit,
        dry_run=args.dry_run,
        log=print,
    )
    print("RESULT:", stats)


if __name__ == "__main__":
    main()
