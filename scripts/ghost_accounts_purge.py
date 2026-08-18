"""Убирает из базы аккаунты наших собственных прогонов — целиком, из всех таблиц.

Зачем: 788 служебных аккаунтов оставили после себя 54 тысячи строк в 22 таблицах.
На выдачу живым людям они не влияют (всё персональное), но любой отчёт по базе
считает их наравне с учениками: доля «тип ошибки не определён» по всей таблице
ошибок была 70% — и это были не ученики, а наши прогоны.

КТО ТАКОЙ ПРИЗРАК — правило положительное, а не «его нет в таблице имён»
(проверено 18.08.2026 на живой базе, все три условия обязательны):
  1) id из служебных блоков: отрицательный, либо начинается на 91000/99370;
  2) отсутствует в bt_3_user_identity;
  3) нет ни одного платежа (bt_3_star_payments, bt_3_billing_events — по нулям).
Дополнительная проверка, снявшая последнее сомнение: аккаунтов БЕЗ служебной метки
(load_test / smoke / synthetic / phase / timeout / preflight / shadow) и при этом с
активностью в трёх и более разных днях — НОЛЬ. Все безметочные жили один-два дня
залпами (до 295 аккаунтов в один день), все меченые подписаны `load_test_*`.
Человек так себя не ведёт.

    python scripts/ghost_accounts_purge.py           # только опись
    python scripts/ghost_accounts_purge.py --apply   # удалить

Перед удалением всё выгружается в JSON (путь печатается). Удаление идёт одной
транзакцией: таблицы со ссылками откладываются и повторяются, пока не уйдут все —
если хоть одна не уходит, откатывается ВСЁ.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DSN_ENV = "DATABASE_URL_RAILWAY"

GHOST_BLOCKS = "(user_id < 0 OR user_id::text LIKE '91000%' OR user_id::text LIKE '99370%')"

# Список призраков собирается по ВСЕМ таблицам, где есть user_id, а не по трём
# «главным». Первый заход 18.08.2026 собирал только из allowed_users / mistakes /
# translations — и оставил 2779 строк у аккаунтов, которые нигде из этих трёх не
# появлялись (жили только в планах на день и в состоянии навыков).
GHOST_IDS = f"""
SELECT DISTINCT user_id FROM ({{sources}}) s
WHERE {GHOST_BLOCKS.replace('user_id', 's.user_id')}
  AND NOT EXISTS (SELECT 1 FROM bt_3_user_identity u WHERE u.user_id = s.user_id)
"""


def _user_id_tables(cursor) -> list[str]:
    cursor.execute(
        """
        SELECT table_name FROM information_schema.columns
        WHERE table_schema='public' AND column_name='user_id'
        ORDER BY table_name;
        """
    )
    return [row[0] for row in cursor.fetchall()]


def _ghost_ids(cursor) -> list[int]:
    sources = " UNION ".join(
        f'SELECT user_id FROM "{table}" WHERE {GHOST_BLOCKS}' for table in _user_id_tables(cursor)
    )
    cursor.execute(GHOST_IDS.format(sources=sources))
    return [int(row[0]) for row in cursor.fetchall()]


def _guards_hold(cursor, ghost_ids: list[int]) -> bool:
    """Три страховки, без которых удалять нельзя."""
    ok = True
    cursor.execute(
        "SELECT COUNT(*) FROM bt_3_user_identity WHERE user_id = ANY(%s);", (ghost_ids,)
    )
    in_identity = cursor.fetchone()[0]
    print(f"   есть в таблице имён (должно быть 0): {in_identity}")
    ok &= in_identity == 0

    for table in ("bt_3_star_payments", "bt_3_billing_events"):
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE user_id = ANY(%s);", (ghost_ids,))
            payments = cursor.fetchone()[0]
        except Exception:
            print(f"   {table}: нет такой таблицы — пропускаю")
            continue
        print(f"   платежей в {table} (должно быть 0): {payments}")
        ok &= payments == 0

    cursor.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT t.user_id
            FROM bt_3_translations t
            WHERE t.user_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM bt_3_allowed_users a
                  WHERE a.user_id = t.user_id
                    AND (COALESCE(a.username,'') || ' ' || COALESCE(a.note,''))
                        ~* '(load_test|smoke|synthetic|phase|timeout|preflight|shadow|postclaim|runtime)'
              )
            GROUP BY t.user_id
            HAVING COUNT(DISTINCT t.timestamp::date) >= 3
        ) risky;
        """,
        (ghost_ids,),
    )
    risky = cursor.fetchone()[0]
    print(f"   без метки и с активностью 3+ дней (должно быть 0): {risky}")
    ok &= risky == 0
    return ok


def main(apply: bool = False) -> None:
    import psycopg2

    dsn = os.getenv(DSN_ENV)
    if not dsn:
        sys.exit(f"нет {DSN_ENV} в окружении")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cursor:
            ghost_ids = _ghost_ids(cursor)
            print(f"── призраков найдено: {len(ghost_ids)} ──")
            if not ghost_ids:
                print("удалять нечего.")
                return
            print("── страховки ──")
            if not _guards_hold(cursor, ghost_ids):
                sys.exit("СТОП: страховка не сработала, удаление отменено.")

            all_tables = _user_id_tables(cursor)

        counts: dict[str, int] = {}
        for table in all_tables:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM "{table}" WHERE user_id = ANY(%s);', (ghost_ids,))
                    count = cursor.fetchone()[0]
                except Exception:
                    conn.rollback()
                    continue
            if count:
                counts[table] = count

        print(f"\n── строки призраков по таблицам (всего {sum(counts.values())}) ──")
        for table, count in sorted(counts.items(), key=lambda item: -item[1]):
            print(f"   {table:<48}{count:>8}")

        if not apply:
            print("\nЭто опись. Чтобы удалить: --apply")
            return

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dump_dir = os.getenv("GHOST_PURGE_DUMP_DIR") or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data"
        )
        os.makedirs(dump_dir, exist_ok=True)
        dump_path = os.path.join(dump_dir, f"ghost_purge_{stamp}.json")
        dump: dict[str, list] = {}
        for table in counts:
            with conn.cursor() as cursor:
                cursor.execute(f'SELECT * FROM "{table}" WHERE user_id = ANY(%s);', (ghost_ids,))
                columns = [desc[0] for desc in cursor.description]
                dump[table] = [dict(zip(columns, map(str, row))) for row in cursor.fetchall()]
        with open(dump_path, "w", encoding="utf-8") as handle:
            json.dump({"ghost_ids": ghost_ids, "rows": dump}, handle, ensure_ascii=False)
        print(f"\nвыгрузка удаляемого: {dump_path} ({os.path.getsize(dump_path) // 1024} КБ)")

        # Порядок удаления заранее не известен (ссылки между таблицами), поэтому
        # откладываем упавшие и повторяем, пока идёт продвижение. Всё — одной
        # транзакцией: не ушла хоть одна таблица, откатывается всё.
        pending = list(counts)
        deleted_total = 0
        deleted_by_table: dict[str, int] = {}
        with conn.cursor() as cursor:
            while pending:
                progressed = False
                still: list[str] = []
                for table in pending:
                    cursor.execute("SAVEPOINT purge_step;")
                    try:
                        cursor.execute(f'DELETE FROM "{table}" WHERE user_id = ANY(%s);', (ghost_ids,))
                        deleted_by_table[table] = cursor.rowcount
                        deleted_total += cursor.rowcount
                        cursor.execute("RELEASE SAVEPOINT purge_step;")
                        progressed = True
                    except Exception as exc:
                        cursor.execute("ROLLBACK TO SAVEPOINT purge_step;")
                        still.append(table)
                        last_error = exc
                if not progressed:
                    conn.rollback()
                    sys.exit(f"СТОП: не удалось убрать {still}: {last_error}. Всё откачено.")
                pending = still
        conn.commit()

        print(f"\n── удалено ──")
        for table, count in sorted(deleted_by_table.items(), key=lambda item: -item[1]):
            print(f"   {table:<48}{count:>8}")
        print(f"   ВСЕГО{'':<43}{deleted_total:>8}")

        with conn.cursor() as cursor:
            left = len(_ghost_ids(cursor))
        print(f"\nпризраков осталось: {left}")
    finally:
        conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
