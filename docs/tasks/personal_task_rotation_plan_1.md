# План 1: память ротации + отборщик + картиночный квиз артиклей

> **Для исполнителя:** шаги помечены чекбоксами `- [ ]`, выполнять по порядку, коммитить
> после каждой задачи. Ветка `refactor/interface`, пуш-remote `bot3_webapp`.

**Цель:** разные люди начинают получать разные картинки из одного банка — каждому то,
чего он ещё не видел, а решённое верно возвращается через 90 дней, потом через 120, потом
никогда.

**Устройство:** правило отбора живёт в **чистом модуле без базы** (`backend/task_rotation.py`),
поэтому проверяется тестами без живой БД. Слой базы — тонкий: одна новая таблица личного
состояния и две функции над ней. Рассылка уже идёт циклом по людям, поэтому встраивание —
это перенос выбора задания внутрь цикла.

**Стек:** Python 3.11, psycopg2, pytest + unittest.mock. Тесты — `backend/tests/`.

**Спека:** `docs/tasks/personal_task_rotation_brief.md`

## Общие правила (действуют в каждой задаче)

- Рабочая ветка `refactor/interface`, пуш `git push bot3_webapp refactor/interface`.
  Никогда не коммитить в `main`, никогда не коммитить `frontend/dist`.
- Стейджить только свои куски (`git add <конкретные файлы>`), в репозитории живёт чужой WIP.
- Тесты не должны трогать боевую базу: `backend/tests/conftest.py` ставит
  `SKIP_STARTUP_SCHEMA_BOOTSTRAP=1` и `SKIP_BILLING_LEDGER_WRITES=1`. Новые тесты не
  должны требовать подключения к БД.
- Лестница возврата: **1-й верный ответ → +90 дней, 2-й → +120 дней, 3-й → никогда**.
  Неверный ответ лестницу НЕ двигает (повтором заваленного занимается очередь работы над
  ошибками `bt_3_aufgabe_mistakes`).
- Человек никогда не остаётся без задания: если подходящего нет — выдаём самое давнее.
- Комментарии в коде — по-русски, как в остальном проекте.

---

### Задача 1: правило отбора как чистая функция

**Файлы:**
- Создать: `backend/task_rotation.py`
- Тест: `backend/tests/test_task_rotation.py`

**Интерфейс, на который опираются следующие задачи:**
- `LADDER_DAYS = (90, 120)` — сроки возврата после 1-го и 2-го верного ответа.
- `next_state(seen_count: int, correct_count: int, is_correct: bool, now: datetime) -> dict`
  → `{"seen_count": int, "correct_count": int, "next_eligible_at": datetime | None,
  "retired_at": datetime | None}`
- `order_candidates(candidates: list[dict], state_by_key: dict[str, dict], now: datetime) -> list[dict]`
  — `candidates` это `[{"task_key": str, ...}]`, `state_by_key` это `{task_key: state}`.
  Возвращает тот же список, отсортированный по правилу: не видел → срок вышел (самое
  давнее вперёд) → срок не вышел (самое давнее вперёд). Выброшенные навсегда
  (`retired_at`) исключаются, но если после исключения не осталось ничего — возвращаются
  в конце, чтобы человек не остался без задания.

- [ ] **Шаг 1: написать падающий тест**

```python
# backend/tests/test_task_rotation.py
"""Правило личной ротации: каждому своё из общего банка.

Замер 14.08.2026: сегодня одно задание уходит всем сразу, поэтому «сколько разных
заданий получили разные люди» равно единице по определению. Здесь проверяется само
правило — без базы, чтобы прогон был быстрым и не зависел от живых данных.
"""

from datetime import datetime, timedelta, timezone
import unittest

from backend.task_rotation import LADDER_DAYS, next_state, order_candidates

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)


class LadderTests(unittest.TestCase):
    def test_first_correct_answer_returns_in_90_days(self):
        st = next_state(seen_count=0, correct_count=0, is_correct=True, now=NOW)
        self.assertEqual(st["correct_count"], 1)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=LADDER_DAYS[0]))
        self.assertIsNone(st["retired_at"])

    def test_second_correct_answer_returns_in_120_days(self):
        st = next_state(seen_count=1, correct_count=1, is_correct=True, now=NOW)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=LADDER_DAYS[1]))
        self.assertIsNone(st["retired_at"])

    def test_third_correct_answer_retires_the_task_forever(self):
        st = next_state(seen_count=2, correct_count=2, is_correct=True, now=NOW)
        self.assertEqual(st["retired_at"], NOW)
        self.assertIsNone(st["next_eligible_at"])

    def test_wrong_answer_does_not_move_the_ladder(self):
        st = next_state(seen_count=1, correct_count=1, is_correct=False, now=NOW)
        self.assertEqual(st["correct_count"], 1)
        self.assertIsNone(st["retired_at"])
        self.assertIsNone(st["next_eligible_at"],
                          "заваленное возвращает очередь работы над ошибками, не ротация")


class OrderTests(unittest.TestCase):
    def _cands(self, *keys):
        return [{"task_key": k} for k in keys]

    def test_unseen_goes_first(self):
        state = {"a": {"seen_count": 1, "correct_count": 1,
                       "next_eligible_at": NOW - timedelta(days=1),
                       "retired_at": None, "last_seen_at": NOW - timedelta(days=100)}}
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b", "a"])

    def test_not_yet_due_goes_last(self):
        state = {
            "a": {"seen_count": 1, "correct_count": 1,
                  "next_eligible_at": NOW + timedelta(days=30),   # срок не вышел
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=60)},
            "b": {"seen_count": 1, "correct_count": 1,
                  "next_eligible_at": NOW - timedelta(days=1),    # срок вышел
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=91)},
        }
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b", "a"])

    def test_retired_is_dropped(self):
        state = {"a": {"seen_count": 3, "correct_count": 3, "next_eligible_at": None,
                       "retired_at": NOW - timedelta(days=5),
                       "last_seen_at": NOW - timedelta(days=5)}}
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b"])

    def test_never_leaves_the_person_empty_handed(self):
        """Всё выброшено — всё равно что-то выдаём, пустой экран недопустим."""
        state = {"a": {"seen_count": 3, "correct_count": 3, "next_eligible_at": None,
                       "retired_at": NOW - timedelta(days=5),
                       "last_seen_at": NOW - timedelta(days=5)}}
        out = order_candidates(self._cands("a"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["a"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запустить: `python3 -m pytest backend/tests/test_task_rotation.py -q`
Ожидаемо: `ModuleNotFoundError: No module named 'backend.task_rotation'`

- [ ] **Шаг 3: написать модуль**

```python
# backend/task_rotation.py
# -*- coding: utf-8 -*-
"""Правило личной ротации заданий: кому какое задание из общего банка.

Зачем этот файл существует
──────────────────────────
Замер 14.08.2026: правильность ответов пишется в девяти таблицах, а при следующей
выдаче её читают четыре места. Не угадал слово в кроссворде — оно может не попасться
больше никогда. Здесь лежит ОДНО правило отбора на все тренажёры, без базы: чистые
функции проверяются тестами и не зависят от живых данных.

Что важно не перепутать: заваленное задание возвращает очередь работы над ошибками
(`bt_3_aufgabe_mistakes`, лестница 1/3/7/16 дней). Ротация занимается другим — она
следит, чтобы человек не получал по второму разу то, что уже решил.
"""

from datetime import datetime, timedelta

# Решил верно один раз — вернуть через 90 дней; второй — через 120; третий — никогда.
# Решение владельца 14.08.2026: «глупо показывать мне то, что я правильно решил».
LADDER_DAYS = (90, 120)


def next_state(*, seen_count: int, correct_count: int, is_correct: bool,
               now: datetime) -> dict:
    """Новое состояние задания для человека после его ответа."""
    seen = int(seen_count or 0) + 1
    correct = int(correct_count or 0) + (1 if is_correct else 0)
    if not is_correct:
        # Неверный ответ лестницу не двигает: повтором заведует работа над ошибками.
        return {"seen_count": seen, "correct_count": correct,
                "next_eligible_at": None, "retired_at": None}
    if correct > len(LADDER_DAYS):
        return {"seen_count": seen, "correct_count": correct,
                "next_eligible_at": None, "retired_at": now}
    return {"seen_count": seen, "correct_count": correct,
            "next_eligible_at": now + timedelta(days=LADDER_DAYS[correct - 1]),
            "retired_at": None}


def _bucket(state: dict | None, now: datetime) -> tuple:
    """Ключ сортировки: (корзина, давность). Меньше — раньше в выдаче."""
    if not state:
        return (0, now)                                   # не видел — вперёд
    due = state.get("next_eligible_at")
    last = state.get("last_seen_at") or now
    if due is None or due <= now:
        return (1, last)                                  # срок вышел, самое давнее вперёд
    return (2, last)                                      # срок не вышел — в хвост


def order_candidates(candidates: list[dict], state_by_key: dict[str, dict],
                     now: datetime) -> list[dict]:
    """Отсортировать кандидатов под конкретного человека.

    Выброшенные навсегда исключаются — но если после этого не осталось ничего,
    возвращаем их обратно: пустой экран человеку недопустим, лучше повтор.
    """
    alive, retired = [], []
    for c in candidates:
        st = state_by_key.get(str(c.get("task_key") or ""))
        (retired if (st or {}).get("retired_at") else alive).append(c)
    pool = alive or retired
    return sorted(pool, key=lambda c: _bucket(
        state_by_key.get(str(c.get("task_key") or "")), now))
```

- [ ] **Шаг 4: убедиться, что тесты проходят**

Запустить: `python3 -m pytest backend/tests/test_task_rotation.py -q`
Ожидаемо: `8 passed`

- [ ] **Шаг 5: коммит**

```bash
git add backend/task_rotation.py backend/tests/test_task_rotation.py
git commit -m "Правило личной ротации заданий: не видел — вперёд, решённое — в хвост"
```

---

### Задача 2: таблица личного состояния и две функции над ней

**Файлы:**
- Изменить: `backend/database.py` (рядом с `ensure_challenge_schema`, ~строка 13088)
- Тест: `backend/tests/test_task_rotation_db.py`

**Интерфейс:**
- Использует: `backend.task_rotation.next_state`
- Даёт следующим задачам:
  - `ensure_task_rotation_schema() -> None`
  - `get_user_task_state(user_id: int, kind: str, task_keys: list[str]) -> dict[str, dict]`
  - `record_user_task_answer(*, user_id: int, kind: str, task_key: str, is_correct: bool, source: str = "sprint") -> None`

- [ ] **Шаг 1: написать падающий тест**

```python
# backend/tests/test_task_rotation_db.py
"""Слой базы у ротации тонкий: он только хранит состояние, а решает правило.

Тест не ходит в базу — он проверяет, что слой зовёт правило и складывает в SQL
именно то, что правило вернуло. Живую базу трогать нельзя (см. conftest).
"""

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
from backend.task_rotation import LADDER_DAYS


class RecordAnswerTests(unittest.TestCase):
    def _capture_sql(self, **kwargs):
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"):
            db.record_user_task_answer(**kwargs)
        return cur.execute.call_args_list

    def test_correct_answer_schedules_return_in_90_days(self):
        calls = self._capture_sql(user_id=1, kind="article_quiz",
                                  task_key="w42", is_correct=True)
        params = calls[-1][0][1]
        self.assertIn(1, params)
        self.assertIn("article_quiz", params)
        due = [p for p in params if isinstance(p, datetime)]
        self.assertTrue(
            any(abs((d - datetime.now(timezone.utc)).days - LADDER_DAYS[0]) <= 1
                for d in due),
            f"верный ответ должен вернуть задание через {LADDER_DAYS[0]} дней: {due}")

    def test_wrong_answer_does_not_schedule_a_return(self):
        calls = self._capture_sql(user_id=1, kind="article_quiz",
                                  task_key="w42", is_correct=False)
        params = calls[-1][0][1]
        self.assertFalse([p for p in params if isinstance(p, datetime)],
                         "неверный ответ лестницу не двигает — сроков быть не должно")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запустить: `python3 -m pytest backend/tests/test_task_rotation_db.py -q`
Ожидаемо: `AttributeError: module 'backend.database' has no attribute 'record_user_task_answer'`

- [ ] **Шаг 3: добавить схему и функции в `backend/database.py`**

Вставить после блока `ensure_challenge_schema` (после строки ~13106):

```python
def ensure_task_rotation_schema() -> None:
    """Личное состояние ротации: что этот человек по этому виду заданий уже видел.

    Отдельно от `bt_3_challenge_results` намеренно: та таблица засчитывает ТОЛЬКО
    первый ответ (`ON CONFLICT DO NOTHING`, ниже по файлу), на ней стоят места и
    проценты в рейтингах. Лестница 90/120/никогда считает повторные верные ответы —
    сменить там семантику значило бы рискнуть рейтингами.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_task_state (
                    id               BIGSERIAL PRIMARY KEY,
                    user_id          BIGINT NOT NULL,
                    kind             TEXT NOT NULL,
                    task_key         TEXT NOT NULL,
                    seen_count       INTEGER NOT NULL DEFAULT 0,
                    correct_count    INTEGER NOT NULL DEFAULT 0,
                    source           TEXT NOT NULL DEFAULT 'sprint',
                    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    next_eligible_at TIMESTAMPTZ,
                    retired_at       TIMESTAMPTZ,
                    UNIQUE (user_id, kind, task_key)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_task_state_pick
                ON bt_3_user_task_state (user_id, kind, next_eligible_at);
            """)
        conn.commit()


def get_user_task_state(user_id: int, kind: str, task_keys: list[str]) -> dict:
    """Состояние по перечисленным заданиям: {task_key: {...}}. Чего нет — не видел."""
    keys = [str(k) for k in (task_keys or []) if str(k)]
    if not keys:
        return {}
    try:
        ensure_task_rotation_schema()
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT task_key, seen_count, correct_count,
                              last_seen_at, next_eligible_at, retired_at
                       FROM bt_3_user_task_state
                       WHERE user_id = %s AND kind = %s AND task_key = ANY(%s);""",
                    (int(user_id), str(kind), keys),
                )
                rows = cursor.fetchall()
        return {r[0]: {"seen_count": r[1], "correct_count": r[2], "last_seen_at": r[3],
                       "next_eligible_at": r[4], "retired_at": r[5]} for r in rows}
    except Exception:
        logging.warning("get_user_task_state failed user=%s kind=%s", user_id, kind,
                        exc_info=True)
        return {}


def record_user_task_answer(*, user_id: int, kind: str, task_key: str,
                            is_correct: bool, source: str = "sprint") -> None:
    """Записать ответ и передвинуть лестницу возврата по правилу из task_rotation."""
    from datetime import datetime, timezone
    from backend.task_rotation import next_state
    try:
        ensure_task_rotation_schema()
        now = datetime.now(timezone.utc)
        prev = get_user_task_state(int(user_id), str(kind), [str(task_key)]).get(
            str(task_key)) or {}
        st = next_state(seen_count=int(prev.get("seen_count") or 0),
                        correct_count=int(prev.get("correct_count") or 0),
                        is_correct=bool(is_correct), now=now)
        params = [int(user_id), str(kind), str(task_key), st["seen_count"],
                  st["correct_count"], str(source)[:16]]
        if st["next_eligible_at"] is not None:
            params.append(st["next_eligible_at"])
        if st["retired_at"] is not None:
            params.append(st["retired_at"])
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO bt_3_user_task_state
                           (user_id, kind, task_key, seen_count, correct_count, source,
                            last_seen_at, next_eligible_at, retired_at)
                       VALUES (%s, %s, %s, %s, %s, %s, NOW(), {due}, {ret})
                       ON CONFLICT (user_id, kind, task_key) DO UPDATE SET
                           seen_count = EXCLUDED.seen_count,
                           correct_count = EXCLUDED.correct_count,
                           last_seen_at = NOW(),
                           next_eligible_at = EXCLUDED.next_eligible_at,
                           retired_at = COALESCE(bt_3_user_task_state.retired_at,
                                                 EXCLUDED.retired_at);""".format(
                        due="%s" if st["next_eligible_at"] is not None else "NULL",
                        ret="%s" if st["retired_at"] is not None else "NULL"),
                    tuple(params),
                )
            conn.commit()
    except Exception:
        logging.warning("record_user_task_answer failed user=%s kind=%s key=%s",
                        user_id, kind, task_key, exc_info=True)
```

- [ ] **Шаг 4: убедиться, что тесты проходят**

Запустить: `python3 -m pytest backend/tests/test_task_rotation_db.py -q`
Ожидаемо: `2 passed`

- [ ] **Шаг 5: коммит**

```bash
git add backend/database.py backend/tests/test_task_rotation_db.py
git commit -m "Память ротации: что этот человек по этому виду заданий уже видел"
```

---

### Задача 3: картиночный квиз артиклей начинает запоминать ответы

**Файлы:**
- Изменить: `bot_3.py` рядом с вызовом `record_article_quiz_answer` (~строка 33173)
- Тест: `backend/tests/test_article_quiz_rotation.py`

**Интерфейс:**
- Использует: `record_user_task_answer` из задачи 2.
- Даёт: строки в `bt_3_user_task_state` с `kind="article_quiz"`, `task_key` = `word_id`
  как строка.

- [ ] **Шаг 1: написать падающий тест**

```python
# backend/tests/test_article_quiz_rotation.py
"""Картиночный квиз артиклей: ответ человека должен попадать в память ротации.

До 15.08.2026 ответ писался только в свою табличку `bt_3_article_quiz_answers`, и
следующая картинка выбиралась без оглядки на то, что этот человек уже отвечал.
"""

import unittest
from unittest.mock import patch

import bot_3


class ArticleQuizWritesRotationTests(unittest.TestCase):
    def test_answer_lands_in_rotation_memory(self):
        with patch.object(bot_3, "record_user_task_answer") as rec:
            bot_3._remember_article_quiz_answer(user_id=7, word_id=42, is_correct=True)
        rec.assert_called_once()
        kwargs = rec.call_args.kwargs
        self.assertEqual(kwargs["user_id"], 7)
        self.assertEqual(kwargs["kind"], "article_quiz")
        self.assertEqual(kwargs["task_key"], "42")
        self.assertTrue(kwargs["is_correct"])

    def test_failure_to_remember_never_breaks_the_answer(self):
        """Память — дело служебное: если она упала, человек всё равно получает ответ."""
        with patch.object(bot_3, "record_user_task_answer", side_effect=RuntimeError):
            bot_3._remember_article_quiz_answer(user_id=7, word_id=42, is_correct=False)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запустить: `python3 -m pytest backend/tests/test_article_quiz_rotation.py -q`
Ожидаемо: `AttributeError: module 'bot_3' has no attribute '_remember_article_quiz_answer'`

- [ ] **Шаг 3: добавить помощника и вызвать его**

В `bot_3.py` рядом с импортами функций базы добавить `record_user_task_answer` в
существующий блок `from backend.database import (...)`, затем добавить помощника:

```python
def _remember_article_quiz_answer(*, user_id: int, word_id: int, is_correct: bool) -> None:
    """Запомнить, что этот человек эту картинку уже решал.

    Ротация выдачи опирается только на эту память: без неё следующая картинка
    выбирается вслепую и может прийти человеку по второму разу.
    Ошибка записи не должна ломать ответ человеку — память служебная.
    """
    try:
        record_user_task_answer(user_id=int(user_id), kind="article_quiz",
                                task_key=str(word_id), is_correct=bool(is_correct),
                                source="chat")
    except Exception:
        logging.warning("article_quiz: не удалось запомнить ответ user=%s word=%s",
                        user_id, word_id, exc_info=True)
```

Затем в месте, где уже вызывается `record_article_quiz_answer` (~строка 33173), добавить
сразу после него:

```python
        _remember_article_quiz_answer(user_id=user_id, word_id=word_id,
                                      is_correct=is_correct)
```

- [ ] **Шаг 4: убедиться, что тесты проходят**

Запустить: `python3 -m pytest backend/tests/test_article_quiz_rotation.py -q`
Ожидаемо: `2 passed`

- [ ] **Шаг 5: коммит**

```bash
git add bot_3.py backend/tests/test_article_quiz_rotation.py
git commit -m "Картиночный квиз артиклей запоминает, кто какую картинку уже решал"
```

---

### Задача 4: разным людям — разные картинки

**Файлы:**
- Изменить: `backend/database.py` — новая функция `pick_article_quiz_for_user`
  рядом с `pick_next_article_quiz` (~строка 49281)
- Изменить: `bot_3.py:33090-33108` — перенести выбор задания внутрь цикла по людям
- Тест: `backend/tests/test_article_quiz_rotation.py` (дописать)

**Интерфейс:**
- Использует: `get_user_task_state` (задача 2), `order_candidates` (задача 1),
  существующую `pick_next_article_quiz`.
- Даёт: `pick_article_quiz_for_user(user_id: int, *, cooldown_days: int, card_kind: str | None) -> dict | None`

- [ ] **Шаг 1: написать падающий тест**

Дописать в `backend/tests/test_article_quiz_rotation.py`:

```python
import backend.database as db
from datetime import datetime, timedelta, timezone


class PerUserPickTests(unittest.TestCase):
    def test_two_people_get_different_pictures(self):
        """Главная проверка: сегодня это число равно единице по определению."""
        bank = [{"word_id": i, "word": f"w{i}"} for i in range(1, 6)]
        seen_by_first = {"1": {"seen_count": 1, "correct_count": 1,
                               "last_seen_at": datetime.now(timezone.utc),
                               "next_eligible_at": datetime.now(timezone.utc)
                               + timedelta(days=90),
                               "retired_at": None}}
        with patch.object(db, "list_ready_article_quiz_entries", return_value=bank), \
             patch.object(db, "get_user_task_state",
                          side_effect=lambda u, k, keys: seen_by_first if u == 1 else {}):
            first = db.pick_article_quiz_for_user(1, cooldown_days=14, card_kind=None)
            second = db.pick_article_quiz_for_user(2, cooldown_days=14, card_kind=None)
        self.assertNotEqual(first["word_id"], 1,
                            "решённую картинку человеку сразу не возвращаем")
        self.assertEqual(second["word_id"], 1,
                         "второму человеку она ещё не показывалась — можно")

    def test_empty_bank_returns_none_not_crash(self):
        with patch.object(db, "list_ready_article_quiz_entries", return_value=[]), \
             patch.object(db, "get_user_task_state", return_value={}):
            self.assertIsNone(db.pick_article_quiz_for_user(1, cooldown_days=14,
                                                            card_kind=None))
```

- [ ] **Шаг 2: убедиться, что тест падает**

Запустить: `python3 -m pytest backend/tests/test_article_quiz_rotation.py -q`
Ожидаемо: `AttributeError: ... has no attribute 'pick_article_quiz_for_user'`

- [ ] **Шаг 3: реализовать выбор под человека**

В `backend/database.py` рядом с `pick_next_article_quiz` добавить:

```python
def list_ready_article_quiz_entries(*, card_kind: str | None = None,
                                    limit: int = 400) -> list[dict]:
    """Готовые к выдаче записи банка — весь пул, из которого выбирает ротация."""
    where = ["image_status = 'ready'", "retired = FALSE"]
    if card_kind == "photo":
        where.append("dalle_prompt IS NOT NULL")
    elif card_kind == "grammar":
        where.append("dalle_prompt IS NULL")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT word_id, word, article, meaning_ru, difficulty, category, "
                "       image_object_key, image_status, send_count, last_sent_at, "
                "       retired, dalle_prompt "
                f"FROM bt_3_article_quiz_bank WHERE {' AND '.join(where)} "
                "ORDER BY last_sent_at NULLS FIRST LIMIT %s;",
                (int(limit),),
            )
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]


def pick_article_quiz_for_user(user_id: int, *, cooldown_days: int = 14,
                               card_kind: str | None = None) -> dict | None:
    """Следующая картинка ИМЕННО для этого человека.

    До 15.08.2026 картинка выбиралась одна на всех: `pick_next_article_quiz` смотрела
    только на общий `last_sent_at`. Банк на 452 записи (замер 14.08.2026) позволяет
    каждому идти своим путём, не тратя ни копейки на генерацию: разным людям МОЖНО
    давать одно и то же задание, нельзя только повторять одному и тому же.
    """
    from datetime import datetime, timezone
    from backend.task_rotation import order_candidates
    try:
        bank = list_ready_article_quiz_entries(card_kind=card_kind)
        if not bank:
            return None
        for e in bank:
            e["task_key"] = str(e.get("word_id"))
        state = get_user_task_state(int(user_id), "article_quiz",
                                    [e["task_key"] for e in bank])
        ordered = order_candidates(bank, state, datetime.now(timezone.utc))
        return ordered[0] if ordered else None
    except Exception:
        logging.warning("pick_article_quiz_for_user failed user=%s", user_id,
                        exc_info=True)
        # Страховка: если личный отбор сломался, человек получает общую картинку.
        return pick_next_article_quiz(cooldown_days=cooldown_days, card_kind=card_kind)
```

- [ ] **Шаг 4: перенести выбор внутрь цикла рассылки**

В `bot_3.py` в `_send_scheduled_article_quiz` (~строка 33090) сейчас `entry` и `image_url`
считаются ДО цикла и одни на всех. Внутри `for target in delivery_targets:` перед
`send_article_quiz_to_chat` вставить:

```python
        # Каждому — своя картинка из общего банка: та, которую именно он ещё не решал.
        # card_kind — ровно то же значение, с каким выше по функции звали
        # pick_next_article_quiz; если там оно не заведено в переменную, вынести его
        # в переменную перед циклом, а не придумывать новое.
        personal = await asyncio.to_thread(
            pick_article_quiz_for_user, target_chat_id,
            cooldown_days=ARTICLE_QUIZ_COOLDOWN_DAYS, card_kind=card_kind)
        entry_for_user = personal or entry
        try:
            image_url_for_user = r2_public_url(entry_for_user.get("image_object_key"))
        except Exception:
            logging.warning("aq_slot: r2_public_url failed word=%s",
                            entry_for_user.get("word_id"), exc_info=True)
            entry_for_user, image_url_for_user = entry, image_url
```

и передать в `send_article_quiz_to_chat` `entry=entry_for_user`,
`image_url=image_url_for_user` вместо `entry` / `image_url`.

Добавить `pick_article_quiz_for_user` в блок импорта из `backend.database` в начале
`bot_3.py`.

- [ ] **Шаг 5: убедиться, что тесты проходят и ничего не сломано**

Запустить: `python3 -m pytest backend/tests -q`
Ожидаемо: все тесты зелёные, включая 4 новых из этой задачи.

- [ ] **Шаг 6: коммит**

```bash
git add backend/database.py bot_3.py backend/tests/test_article_quiz_rotation.py
git commit -m "Картиночный квиз: каждому своя картинка из общего банка"
```

---

### Задача 5: замер «сколько разных заданий получили разные люди»

**Файлы:**
- Создать: `scripts/task_rotation_audit.py`

**Интерфейс:**
- Использует: живую базу только на чтение.
- Даёт: воспроизводимый отчёт, который можно прогонять до и после.

- [ ] **Шаг 1: написать скрипт**

```python
# scripts/task_rotation_audit.py
# -*- coding: utf-8 -*-
"""Замер личной ротации: сколько РАЗНЫХ заданий получили разные люди.

До правки это число равно единице по определению: задание выбиралось один раз до
цикла рассылки и уходило всем. Скрипт ТОЛЬКО ЧИТАЕТ.

    DATABASE_URL=... python3 scripts/task_rotation_audit.py
"""

import os
import sys
from collections import Counter

import psycopg2


def main() -> int:
    dsn = (os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if not dsn:
        print("Нужен DATABASE_URL (публичный адрес живой базы).", file=sys.stderr)
        return 2
    if "sslmode" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("""SELECT kind, COUNT(DISTINCT task_key), COUNT(DISTINCT user_id),
                              COUNT(*)
                       FROM bt_3_user_task_state
                       WHERE last_seen_at > NOW() - INTERVAL '7 days'
                       GROUP BY kind ORDER BY 4 DESC;""")
        rows = cur.fetchall()
    if not rows:
        print("За неделю в памяти ротации пусто — либо не запущено, либо никто не играл.")
        return 0
    print("Вид задания                 разных заданий   людей   ответов   заданий на человека")
    for kind, tasks, users, answers in rows:
        per = tasks / users if users else 0
        print(f"{kind:<28}{tasks:>10}{users:>9}{answers:>10}      {per:>5.1f}")
    print("\nЕсли «заданий на человека» близко к 1.0 — ротация не работает,")
    print("все получают одно и то же. Замер до правки: ровно 1.0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Шаг 2: прогнать на живой базе**

```bash
DATABASE_URL="$(railway variables --service Postgres --kv | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2-)" \
  python3 scripts/task_rotation_audit.py
```
Ожидаемо до раскатки: «за неделю в памяти ротации пусто».

- [ ] **Шаг 3: коммит**

```bash
git add scripts/task_rotation_audit.py
git commit -m "Замер личной ротации: сколько разных заданий получили разные люди"
```

---

## Что НЕ входит в этот план

- **Ночной расчёт пополнения банка** и отчёт о нехватке — это План 2.
- **Остальные тренажёры** (ребус, кроссворд, анаграмма, числа, пул заданий) — План 3,
  там повторяются задачи 3 и 4 с другими именами таблиц.
- **Wo-Fragen и Adjektiv Sprint** — у них банка нет, там работает перекос при сборке,
  а не ротация. Отдельная задача.
- **Батлы и групповые чаты** — остаются общими, по решению владельца.

## Проверка перед сдачей

- `python3 -m pytest backend/tests -q` — всё зелёное.
- `scripts/task_rotation_audit.py` через неделю после раскатки показывает «заданий на
  человека» заметно больше 1.0.
- В логах нет `pick_article_quiz_for_user failed` — если есть, личный отбор падает и
  тихо отдаёт общую картинку.
