# -*- coding: utf-8 -*-
"""Добор версий картинок ребуса — чтобы перерисованное ДОЕХАЛО до людей.

Зачем этот файл существует
──────────────────────────
Адрес картинки собирается из номера ребуса и не меняется никогда:
`rebus/composed/<compound_id>.png`. Бот шлёт Telegram не файл, а ССЫЛКУ, а Telegram
скачивает по ссылке один раз и дальше вечно отдаёт свою копию. Поэтому каждая
перерисовка ложилась в тот же адрес и до людей не доезжала.

Живой случай (замер 31.08.2026): картинку «Ei» починили 13.06.2026, правильный файл
лежит в R2 с 03.08.2026 — а 31.08.2026 двенадцати людям снова ушла июньская груша под
подписью «яйцо + часы». По тому же правилу отбора («файл в R2 новее первой отправки»)
таких карточек оказалось 79 из 126 когда-либо отправлявшихся.

Что делает скрипт
─────────────────
1. Проставляет `image_version` половинкам и `composed_image_version` карточкам —
   значение берётся из ETag объекта в R2 (`head_object`), файлы НЕ скачиваются,
   ничего не перерисовывается, модель не зовётся. Денег: ноль.
2. Проставляет `parts_fingerprint` — из каких половинок склеена карточка.
   Без него отправка не знает, разошлась ли склейка с подписью, и по правилу
   «не знаем — не шлём» карточка не ушла бы вовсе.

После прогона адрес каждой карточки становится новым (`?v=…`) — Telegram скачивает
свежий файл, и все 79 застрявших карточек уезжают людям правильными.

Идемпотентен: повторный прогон трогает только строки, где версии ещё нет
(или где она разошлась с тем, что вправду лежит в R2).

    DATABASE_URL="$(railway variables --service Postgres --kv \\
        | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2-)" \\
      python3 scripts/rebus_image_versions_backfill.py --dry-run
    …тот же вызов без --dry-run — записать.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="только показать, сколько строк ждёт версии")
    args = ap.parse_args()

    if not str(os.getenv("DATABASE_URL") or "").strip():
        print("DATABASE_URL не задан", file=sys.stderr)
        return 2

    # Правило берётся ИЗ ПРОДУКТА — та же функция, которую ночью зовёт
    # prepare_rebus_pool_job. Скрипт нужен только чтобы не ждать ночи.
    from backend.database import list_rebus_images_without_version
    from backend.rebus_generator import fill_missing_rebus_image_versions

    todo = list_rebus_images_without_version()
    print(f"Ждут версии: половинок {len(todo['components'])}, карточек {len(todo['cards'])}")
    if args.dry_run:
        print("(--dry-run: ничего не записано)")
        return 0

    result = fill_missing_rebus_image_versions()
    print(f"Половинки: версия проставлена {result['components_versioned']}, "
          f"файла в R2 нет {result['components_file_missing']}")
    print(f"Карточки:  версия+отпечаток {result['cards_versioned']}, "
          f"файла в R2 нет {result['cards_file_missing']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
