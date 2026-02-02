import argparse
import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

from backend.database import get_db_connection_context


def check_db_host_timeout(seconds: int = 5) -> None:
    host = "centerbeam.proxy.rlwy.net"
    print(f"Checking DNS for {host} (timeout {seconds}s)...", flush=True)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(socket.getaddrinfo, host, 5432)
        try:
            future.result(timeout=seconds)
        except TimeoutError as exc:
            print(f"DNS timeout for {host}", flush=True)
            raise TimeoutError(f"DNS timeout for {host}") from exc


def parse_lines(csv_path: Path) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = None
            for sep in (" — ", " – ", " - "):
                if sep in line:
                    left, right = line.split(sep, 1)
                    parts = (left.strip(), right.strip())
                    break
            if not parts:
                continue
            de, ru = parts
            if de.startswith("\ufeff"):
                de = de.lstrip("\ufeff")
            if not de or not ru:
                continue
            rows.append((ru, de))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Import LinguaLeo CSV into webapp dictionary.")
    parser.add_argument("--csv", required=True, help="Path to LinguaLeo_dict.csv")
    parser.add_argument("--user-id", required=True, type=int, help="Telegram user id")
    parser.add_argument("--sql-out", help="Write INSERTs to a .sql file instead of DB insert")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    if not args.sql_out:
        check_db_host_timeout()
        print("DNS OK, connecting to DB...", flush=True)

    existing = set()
    if not args.sql_out:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT word_ru, translation_de
                    FROM bt_3_webapp_dictionary_queries
                    WHERE user_id = %s
                    """,
                    (args.user_id,),
                )
                for row in cursor.fetchall():
                    existing.add((row[0] or "", row[1] or ""))

    rows = parse_lines(csv_path)
    inserted = 0
    skipped = 0

    if args.sql_out:
        out_path = Path(args.sql_out)
        with out_path.open("w", encoding="utf-8") as out:
            for ru, de in rows:
                response_json = {
                    "word_ru": ru,
                    "part_of_speech": "phrase",
                    "translation_de": de,
                    "article": None,
                    "forms": {
                        "plural": None,
                        "praeteritum": None,
                        "perfekt": None,
                        "konjunktiv1": None,
                        "konjunktiv2": None,
                    },
                    "prefixes": [],
                    "usage_examples": [],
                }
                response_json_str = json.dumps(response_json, ensure_ascii=False).replace("'", "''")
                ru_sql = ru.replace("'", "''")
                de_sql = de.replace("'", "''")
                out.write(
                    "INSERT INTO bt_3_webapp_dictionary_queries "
                    "(user_id, word_ru, translation_de, response_json) "
                    f"VALUES ({args.user_id}, '{ru_sql}', '{de_sql}', '{response_json_str}');\n"
                )
                inserted += 1
        print(f"Parsed: {len(rows)}")
        print(f"Wrote SQL inserts: {inserted}")
        print(f"SQL file: {out_path}")
        return

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for ru, de in rows:
                if (ru, de) in existing:
                    skipped += 1
                    continue
                response_json = {
                    "word_ru": ru,
                    "part_of_speech": "phrase",
                    "translation_de": de,
                    "article": None,
                    "forms": {
                        "plural": None,
                        "praeteritum": None,
                        "perfekt": None,
                        "konjunktiv1": None,
                        "konjunktiv2": None,
                    },
                    "prefixes": [],
                    "usage_examples": [],
                }
                cursor.execute(
                    """
                    INSERT INTO bt_3_webapp_dictionary_queries (
                        user_id,
                        word_ru,
                        translation_de,
                        response_json
                    )
                    VALUES (%s, %s, %s, %s);
                    """,
                    (
                        args.user_id,
                        ru,
                        de,
                        json.dumps(response_json, ensure_ascii=False),
                    ),
                )
                inserted += 1

    print(f"Parsed: {len(rows)}")
    print(f"Inserted: {inserted}")
    print(f"Skipped (duplicates): {skipped}")


if __name__ == "__main__":
    main()
