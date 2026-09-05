# «Слова со вчерашних тренировок» — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Слово, которое человек тапнул в конце любого интерактива, назавтра приходит ему в 07:25 и 19:35 постером с кнопкой, открывающей разовое повторение с настоящими оценками FSRS.

**Architecture:** Одна дверь записи «отобрано на завтра» внутри существующего сохранения слова; две бонусные крон-рассылки по образцу «Работы над ошибками»; отдельный экран в оверлее интерактивов, собранный из деталей Space Rep, который отдаёт оценки в существующий обработчик `/api/cards/review` с источником очереди `pick`. Модель не вызывается нигде.

**Tech Stack:** Flask (`backend/backend_server.py`), psycopg2 (`backend/database.py`), python-telegram-bot + APScheduler (`bot_3.py`), Pillow (`backend/interactive_card.py`), React (`frontend/src/answer/`), unittest/pytest (`backend/tests`).

**Spec:** `docs/tasks/word_pick_review_strategy.md`

## Global Constraints

- Работать ТОЛЬКО в worktree `/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-wordpick`, ветка `agent/wordpick`. Перед первой правкой файла: `./scripts/base-check.sh <файл>` → `0`.
- Никаких `except: pass` с продолжением и «нормальным» ответом наружу. Единственное исключение: запись отбора внутри сохранения слова — ошибка логируется, ответ честно говорит `pick_for_day: null`.
- Ни одного обращения к модели. Разбор для лампочки берётся только готовый (`response_json` + слой единиц).
- Ни одного `or`/`??`/`.get(x, default)`, подставляющего содержательное значение вместо ответа источника.
- Тексты для человека: «Полный доступ», «Лайт», «бесплатный месяц». Слово «Free» в новых текстах не употреблять.
- Граница суток — `Europe/Vienna` (как у `due_at` и рассылки).
- Слоты: 07:25 (`am`) и 19:35 (`pm`). Тихие часы НЕ проверяются (решение владельца 04.09.2026).
- Коммиты маленькие, каждый с `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` и `Claude-Session: https://claude.ai/code/session_01HzqHNfFHt1e5U3p1FmhkaJ`.
- Тесты: `cd <worktree> && python3 -m pytest backend/tests/<файл> -q`. `frontend/dist` НЕ коммитить.

---

## Карта файлов

| файл | что делает в этой задаче |
|---|---|
| `backend/word_pick.py` (создать) | Чистые функции без базы: слоты, «какой сейчас проход», разбор дня из ссылки |
| `backend/database.py` | `WORD_PICK_ORIGINS`, схема `bt_3_word_picks`, `record_word_pick`, `list_word_pick_recipients`, `list_word_pick_cards`, `word_pick_card_ids`, `mark_word_pick_rated`, `word_pick_day_stats`, `count_word_pick_door_misses`, `count_word_pick_posters_missing`; `INBOX_BONUS_KINDS`; allowlist `interactive_save` |
| `backend/backend_server.py` | дверь в `save_webapp_dictionary_entry`; `POST /api/answer/wordpick/set`; ветка `queue_source='pick'` в `review_srs_card` |
| `backend/free_delivery_report.py` | `BONUS_INBOX_KINDS` берётся из `database.INBOX_BONUS_KINDS` |
| `backend/interactive_card.py` | `render_word_pick_card(count)` |
| `bot_3.py` | `WORD_PICK_SLOT_TIMES`, `_send_word_pick_reviews`, два крона, `/admin_wordpick_preview`, строка отчёта `_word_pick_report_line` |
| `backend/fix_promises.py` | два обещания |
| `frontend/src/answer/wordTts.js` (создать) | `playWordTts` вынесен из `AnswerOverlay.jsx` |
| `frontend/src/answer/pickCopy.js` (создать) | одна подпись и одна функция подписи чипа для пяти мест |
| `frontend/src/answer/WordPickGame.jsx` (создать) | экран |
| `frontend/src/answer/AnswerOverlay.jsx` | код `wp` в разборе ссылки, монтирование |
| `frontend/src/answer/answer.css` | стили `wp-*` |
| `frontend/src/dictionary/saveUtils.js` | обогащение только при `inserted=true`; `pickedForDay` в ответе |
| `frontend/src/answer/TrainerGame.jsx`, `SprintGame.jsx`, `ArtikelSprintGame.jsx`, `AdjektivLearnGame.jsx`, `AnswerOverlay.jsx`, `SaveWordChip.jsx` | подпись и состояние чипа |
| `backend/tests/test_word_pick_door.py`, `test_word_pick_delivery.py`, `test_word_pick_review.py`, `test_word_pick_copy.py` | тесты |

---

### Task 1: Чистые функции слотов и дня

**Files:**
- Create: `backend/word_pick.py`
- Test: `backend/tests/test_word_pick_door.py`

**Interfaces:**
- Produces: `WORD_PICK_SLOTS: dict[str, tuple[int,int]]`, `slot_now(now_local: datetime) -> str` (`'am'|'pm'`), `parse_day(raw) -> date | None`, `day_id(day: date, slot: str) -> int` (для `dispatch_id` ведомости), `deeplink_for(day: date) -> str` (`ans_wp_YYYYMMDD`).

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_word_pick_door.py
# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: дверь записи отбора и чистые функции слотов.
Стратегия: docs/tasks/word_pick_review_strategy.md."""
import os
import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import word_pick  # noqa: E402

VIENNA = ZoneInfo("Europe/Vienna")


class Слоты(unittest.TestCase):

    def test_до_вечернего_слота_идёт_утренний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 7, 25, tzinfo=VIENNA)), "am")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 34, tzinfo=VIENNA)), "am")

    def test_с_19_35_идёт_вечерний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 35, tzinfo=VIENNA)), "pm")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 23, 59, tzinfo=VIENNA)), "pm")

    def test_день_из_ссылки_разбирается_строго(self):
        self.assertEqual(word_pick.parse_day("20260905"), date(2026, 9, 5))
        self.assertEqual(word_pick.parse_day("2026-09-05"), date(2026, 9, 5))
        for плохое in ("", None, "2026095", "abc", "20261305", 20260905.0):
            self.assertIsNone(word_pick.parse_day(плохое), плохое)

    def test_номер_строки_ведомости_различает_утро_и_вечер(self):
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "am"), 202609051)
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "pm"), 202609052)
        self.assertEqual(word_pick.deeplink_for(date(2026, 9, 5)), "ans_wp_20260905")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest backend/tests/test_word_pick_door.py -q`
Expected: FAIL, `ModuleNotFoundError: backend.word_pick`

- [ ] **Step 3: Write minimal implementation**

```python
# backend/word_pick.py
# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: чистые правила без базы и без бота.

Стратегия: docs/tasks/word_pick_review_strategy.md. Решение владельца 04.09.2026:
два прохода в день, утром 07:25 и вечером 19:35, один и тот же набор. Здесь лежит то,
что обязано совпадать у трёх сторон — у рассылки (bot_3), у сервера (backend_server) и у
отчёта, — чтобы граница «утро/вечер» не жила в трёх местах тремя разными числами.
"""
from __future__ import annotations

import re
from datetime import date, datetime

# Решение владельца, 04.09.2026.
WORD_PICK_SLOTS: dict[str, tuple[int, int]] = {"am": (7, 25), "pm": (19, 35)}
_SLOT_NO = {"am": 1, "pm": 2}
_DAY_RE = re.compile(r"^(\d{4})-?(\d{2})-?(\d{2})$")


def slot_now(now_local: datetime) -> str:
    """Какой проход идёт сейчас: до 19:35 утренний, с 19:35 вечерний."""
    return "pm" if (now_local.hour, now_local.minute) >= WORD_PICK_SLOTS["pm"] else "am"


def parse_day(raw) -> date | None:
    """День из ссылки/запроса: ГГГГММДД или ГГГГ-ММ-ДД. Всё остальное — None (не дефолт)."""
    if not isinstance(raw, str):
        return None
    m = _DAY_RE.match(raw.strip())
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def day_id(day: date, slot: str) -> int:
    """Номер строки в ведомости: ГГГГММДД·10 + 1 (утро) / 2 (вечер). У постера нет своего
    dispatch, как и у «Работы над ошибками», поэтому ключ — день; два постера в день
    обязаны быть двумя строками, иначе вечерний затирал бы утренний."""
    return int(day.strftime("%Y%m%d")) * 10 + _SLOT_NO[slot]


def deeplink_for(day: date) -> str:
    return f"ans_wp_{day:%Y%m%d}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest backend/tests/test_word_pick_door.py -q`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add backend/word_pick.py backend/tests/test_word_pick_door.py
git commit -m "Слова со вчерашних тренировок: чистые правила слотов и дня"
```

---

### Task 2: Дверь записи отбора при тапе

**Files:**
- Modify: `backend/database.py` — список источников около строки 300 (`"adjektiv_trainer",`), `DICTIONARY_ORIGIN_GROUPS` около 36573, новые функции рядом с `ensure_trainer_schema` (около 64585)
- Modify: `backend/backend_server.py:53082-53160` (`save_webapp_dictionary_entry`, после `_save_dictionary_entry_with_inserted_schema_retry`)
- Test: `backend/tests/test_word_pick_door.py`

**Interfaces:**
- Produces: `database.WORD_PICK_ORIGINS: frozenset[str]`, `database.ensure_word_pick_schema()`, `database.record_word_pick(*, user_id, card_id, origin_process) -> dict` (`{"for_day": date, "inserted": bool}`), ответ `/api/webapp/dictionary/save` получает поле `pick_for_day: "YYYY-MM-DD" | null`.

- [ ] **Step 1: Write the failing tests** (добавить в `backend/tests/test_word_pick_door.py`)

```python
import pathlib
from unittest import mock

from backend import database as db  # noqa: E402

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2]


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class ДверьОтбора(unittest.TestCase):

    def setUp(self):
        db._word_pick_schema_ready = True

    def test_все_источники_тапа_разрешены_и_не_превращаются_в_unknown(self):
        """interactive_save (дискетка в оверлее) до 05.09.2026 в списке не было: такие
        сохранения писались как unknown и затирали источник у старых слов."""
        for источник in db.WORD_PICK_ORIGINS:
            self.assertEqual(db._normalize_dictionary_origin_process(источник), источник, источник)
        self.assertEqual(db.WORD_PICK_ORIGINS, frozenset({
            "trainer_save", "synonym_save", "artikel_sprint_save", "adjektiv_trainer", "interactive_save"}))

    def test_источники_тапа_показываются_в_группе_тренажёров(self):
        группа = db.DICTIONARY_ORIGIN_GROUPS["trainer"][2]
        for источник in ("trainer_save", "synonym_save", "adjektiv_trainer", "interactive_save"):
            self.assertIn(источник, группа)

    def test_первый_тап_пишет_запись_на_завтра(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (date(2026, 9, 6),)
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.record_word_pick(user_id=7, card_id=99, origin_process="trainer_save")
        self.assertEqual(out, {"for_day": date(2026, 9, 6), "inserted": True})
        sql = курсор.execute.call_args_list[0].args[0]
        self.assertIn("INSERT INTO bt_3_word_picks", sql)
        self.assertIn("ON CONFLICT (user_id, card_id, for_day) DO NOTHING", sql)
        self.assertIn("Europe/Vienna", str(курсор.execute.call_args_list[0].args[1]))

    def test_второй_тап_в_тот_же_день_не_дублирует(self):
        курсор = mock.MagicMock()
        курсор.fetchone.side_effect = [None, (date(2026, 9, 6),)]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.record_word_pick(user_id=7, card_id=99, origin_process="trainer_save")
        self.assertEqual(out, {"for_day": date(2026, 9, 6), "inserted": False})

    def test_дверь_стоит_в_сохранении_слова_и_ответ_называет_день(self):
        """Проверка по исходнику: функция сохранения огромна и тянет право доступа, поэтому
        сверяем, что вызов стоит ИМЕННО в ней и что ответ несёт pick_for_day."""
        src = (КОРЕНЬ / "backend/backend_server.py").read_text(encoding="utf-8")
        тело = src.split("def save_webapp_dictionary_entry(", 1)[1].split("\n@app.route", 1)[0]
        self.assertIn("record_word_pick(", тело)
        self.assertIn("WORD_PICK_ORIGINS", тело)
        self.assertIn('"pick_for_day"', тело)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest backend/tests/test_word_pick_door.py -q`
Expected: 5 new FAIL (`AttributeError: WORD_PICK_ORIGINS`, и т.д.)

- [ ] **Step 3: Implement in `backend/database.py`**

3a. В список разрешённых источников (тот `set`, где около строки 310 стоят `"trainer_save"`, `"artikel_sprint_save"`, `"adjektiv_trainer"`) добавить после `"adjektiv_trainer",`:

```python
    # Дискетка в углу карточки интерактива (SaveWordChip.jsx). До 05.09.2026 её здесь НЕ
    # было: сохранения писались как «unknown» и затирали источник у старых слов.
    "interactive_save",
```

3b. В `DICTIONARY_ORIGIN_GROUPS` заменить строку группы `trainer`:

```python
    "trainer": ("Тренажёры",        "🎯", ("trainer_save", "synonym_save", "adjektiv_trainer", "interactive_save")),
```

3c. Рядом с `ensure_trainer_schema` (около строки 64585) добавить:

```python
# ── «Слова со вчерашних тренировок» ───────────────────────────────────────────────────
# Стратегия: docs/tasks/word_pick_review_strategy.md. Тап по слову в конце интерактива
# отбирает его на ЗАВТРА; утром и вечером человеку приходит постер с кнопкой.
# Список источников закрытый: ровно те origin_process, которые шлют чипы и дискетки
# интерактивов. Новый интерактив с чипами добавляет свой источник СЮДА, иначе его тапы
# на завтра не попадут (и тест test_word_pick_door это поймает по allowlist).
WORD_PICK_ORIGINS: frozenset[str] = frozenset({
    "trainer_save", "synonym_save", "artikel_sprint_save", "adjektiv_trainer", "interactive_save",
})
_WORD_PICK_TZ = "Europe/Vienna"   # та же граница суток, что у due_at и у рассылки
_word_pick_schema_ready = False


def ensure_word_pick_schema() -> None:
    global _word_pick_schema_ready
    if _word_pick_schema_ready:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_word_picks (
                    id             BIGSERIAL PRIMARY KEY,
                    user_id        BIGINT NOT NULL,
                    card_id        BIGINT NOT NULL,
                    picked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    pick_day       DATE NOT NULL,
                    for_day        DATE NOT NULL,
                    origin_process TEXT NOT NULL,
                    am_rated_at    TIMESTAMPTZ,
                    pm_rated_at    TIMESTAMPTZ,
                    UNIQUE (user_id, card_id, for_day)
                );
                CREATE INDEX IF NOT EXISTS idx_word_picks_for_day
                    ON bt_3_word_picks (for_day, user_id);
                """
            )
        conn.commit()
    _word_pick_schema_ready = True


def record_word_pick(*, user_id: int, card_id: int, origin_process: str) -> dict:
    """Записать «слово отобрано на завтра». Второй тап в тот же день — та же строка.
    Возвращает {"for_day": date, "inserted": bool}. Ошибки НЕ глушит: решает вызывающий."""
    ensure_word_pick_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_word_picks (user_id, card_id, pick_day, for_day, origin_process)
                VALUES (%s, %s,
                        (NOW() AT TIME ZONE %s)::date,
                        (NOW() AT TIME ZONE %s)::date + 1,
                        %s)
                ON CONFLICT (user_id, card_id, for_day) DO NOTHING
                RETURNING for_day;
                """,
                (int(user_id), int(card_id), _WORD_PICK_TZ, _WORD_PICK_TZ, str(origin_process)),
            )
            row = cursor.fetchone()
            inserted = row is not None
            if row is None:
                cursor.execute("SELECT (NOW() AT TIME ZONE %s)::date + 1;", (_WORD_PICK_TZ,))
                row = cursor.fetchone()
        conn.commit()
    return {"for_day": row[0], "inserted": inserted}
```

- [ ] **Step 4: Implement the door in `backend/backend_server.py`**

В `save_webapp_dictionary_entry`, сразу после блока `if effective_mode == "free" and inserted and not save_is_free_of_charge: increment_free_feature_usage(...)` (около строки 53097) и ДО `missing_russian = (...)`:

```python
        # «Слова со вчерашних тренировок»: тап по слову в интерактиве отбирает его на завтра.
        # Одна дверь для всех пяти мест с чипами (стратегия docs/tasks/word_pick_review_strategy.md).
        # Тап по слову «уже есть» тоже считается — владелец: важно, что человек нажал.
        # Ошибка записи отбора слово НЕ теряет (оно уже записано) и ответ НЕ подменяет:
        # pick_for_day уходит null, интерфейс честно говорит «на завтра не отобралось».
        pick_for_day = None
        if _normalize_dictionary_origin_process(origin_process) in WORD_PICK_ORIGINS:
            try:
                pick = record_word_pick(
                    user_id=int(user_id), card_id=int(entry_id or 0),
                    origin_process=_normalize_dictionary_origin_process(origin_process),
                )
                pick_for_day = pick["for_day"].isoformat()
            except Exception:
                logging.exception("word pick: отбор на завтра не записался user=%s entry=%s origin=%s",
                                  user_id, entry_id, origin_process)
```

В `return jsonify({...})` этой же функции добавить поле после `"inserted": bool(inserted),`:

```python
            # Дата, на которую слово отобрано в «Слова со вчерашних тренировок»; null —
            # источник не из интерактива ЛИБО запись не удалась (в логе exception).
            "pick_for_day": pick_for_day,
```

Импорты: в блок `from backend.database import (...)` файла добавить `WORD_PICK_ORIGINS, record_word_pick, _normalize_dictionary_origin_process` (проверить, не импортирован ли `_normalize_dictionary_origin_process` уже: `grep -n "_normalize_dictionary_origin_process" backend/backend_server.py`).

Переменная `pick_for_day` объявляется внутри `try:` — вынести `pick_for_day = None` ПЕРЕД `try:` того блока (строка `try:` перед `resolved_word_ru = ...`), чтобы `return` ниже её видел.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m pytest backend/tests/test_word_pick_door.py backend/tests/test_dictionary_origin*.py -q` (второй шаблон — если такие тесты есть; иначе только первый)
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add backend/database.py backend/backend_server.py backend/tests/test_word_pick_door.py
git commit -m "Дверь отбора на завтра: тап по слову в интерактиве пишет bt_3_word_picks; interactive_save разрешён"
```

---

### Task 3: Запросы: получатели, набор дня, отметка прохода, статистика

**Files:**
- Modify: `backend/database.py` (после `record_word_pick`)
- Test: `backend/tests/test_word_pick_delivery.py`

**Interfaces:**
- Produces:
  - `list_word_pick_recipients(for_day: date) -> list[dict]` — `[{"user_id": int, "count": int}]`
  - `list_word_pick_cards(*, user_id: int, for_day: date, cursor=None) -> list[dict]` — каждый элемент `{"card": {...}, "srs": {...} | None, "am_rated_at", "pm_rated_at", "origin_process"}`; `card` содержит `id, word_ru, translation_de, word_de, translation_ru, source_lang, target_lang, response_json, user_notes`
  - `word_pick_card_ids(*, user_id, for_day, cursor=None) -> list[int]`
  - `mark_word_pick_rated(*, user_id, card_id, for_day, slot: str, cursor=None) -> None`
  - `word_pick_day_stats(day: date) -> dict` — `{"pickers": int, "cards": int, "am_rated_users": int, "pm_rated_users": int, "posters_am": int, "posters_pm": int, "opened": int}`

- [ ] **Step 1: Write the failing tests**

```python
# backend/tests/test_word_pick_delivery.py
# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: набор дня, получатели, отметка прохода, норма."""
import os
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class НаборДня(unittest.TestCase):

    def setUp(self):
        db._word_pick_schema_ready = True

    def test_получатели_это_все_у_кого_есть_отбор_на_день(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(7, 3), (9, 1)]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.list_word_pick_recipients(date(2026, 9, 6))
        self.assertEqual(out, [{"user_id": 7, "count": 3}, {"user_id": 9, "count": 1}])
        sql = курсор.execute.call_args.args[0]
        self.assertIn("FROM bt_3_word_picks", sql)
        self.assertIn("for_day = %s", sql)

    def test_набор_дня_берёт_карточку_и_состояние_без_проверки_срока(self):
        """Срок карточки (due_at) НЕ фильтруется: вечером набор показывается снова,
        даже если утром поставили «Легко» (решение владельца 04.09.2026)."""
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {"word_de": "schnell"}, [],
            "new", None, None, 0, 0, 0, 0.0, 0.0, 0, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        sql = курсор.execute.call_args.args[0]
        self.assertNotIn("due_at <=", sql)
        self.assertIn("LEFT JOIN bt_3_card_srs_state", sql)
        self.assertEqual(out[0]["card"]["id"], 99)
        self.assertEqual(out[0]["srs"]["status"], "new")
        self.assertIsNone(out[0]["am_rated_at"])

    def test_карточка_без_состояния_отдаёт_srs_none(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {}, [],
            None, None, None, None, None, None, None, None, None, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        self.assertIsNone(out[0]["srs"])

    def test_отметка_прохода_ставится_один_раз_и_только_в_свою_колонку(self):
        курсор = mock.MagicMock()
        db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="pm", cursor=курсор)
        sql = курсор.execute.call_args.args[0]
        self.assertIn("SET pm_rated_at = COALESCE(pm_rated_at, NOW())", sql)
        self.assertNotIn("am_rated_at", sql)
        with self.assertRaises(ValueError):
            db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="noon", cursor=курсор)


class СверхНормы(unittest.TestCase):

    def test_код_wp_исключён_из_нормы_везде_где_исключён_rv(self):
        self.assertEqual(tuple(db.INBOX_BONUS_KINDS), ("rv", "wp"))
        import inspect
        for fn in (db.get_inbox_delivery_stats_today, db.get_inbox_kinds_today):
            src = inspect.getsource(fn)
            self.assertNotIn("kind <> 'rv'", src, fn.__name__)
            self.assertIn("_INBOX_BONUS_KINDS_SQL", src, fn.__name__)
        self.assertIn("'wp'", db._INBOX_BONUS_KINDS_SQL)
        from backend.free_delivery_report import BONUS_INBOX_KINDS
        self.assertEqual(BONUS_INBOX_KINDS, {"rv", "wp"})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py -q`
Expected: FAIL (`AttributeError`)

- [ ] **Step 3: Implement queries in `backend/database.py`** (после `record_word_pick`)

```python
def list_word_pick_recipients(for_day) -> list[dict]:
    """Кому сегодня приходит постер: все, у кого есть отбор на этот день, и сколько слов.
    Замок, «Тишину» и блокировку бота проверяет рассылка — это её правила, не запроса."""
    ensure_word_pick_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, COUNT(*) FROM bt_3_word_picks WHERE for_day = %s "
                "GROUP BY user_id ORDER BY user_id;",
                (for_day,),
            )
            return [{"user_id": int(r[0]), "count": int(r[1])} for r in (cursor.fetchall() or [])]


_WORD_PICK_CARD_SQL = """
    SELECT q.id, q.word_ru, q.translation_de, q.word_de, q.translation_ru,
           q.source_lang, q.target_lang, q.response_json, q.user_notes,
           s.status, s.due_at, s.last_review_at, s.interval_days, s.reps, s.lapses,
           s.stability, s.difficulty, s.step,
           p.am_rated_at, p.pm_rated_at, p.origin_process
    FROM bt_3_word_picks p
    JOIN bt_3_webapp_dictionary_queries q ON q.id = p.card_id AND q.user_id = p.user_id
    LEFT JOIN bt_3_card_srs_state s ON s.card_id = q.id AND s.user_id = q.user_id
    WHERE p.user_id = %s AND p.for_day = %s
    ORDER BY p.picked_at ASC, p.id ASC;
"""


def list_word_pick_cards(*, user_id: int, for_day, cursor=None) -> list[dict]:
    """Набор дня: карточки, отобранные на этот день, с состоянием повторения (или None,
    если карточка в Space Rep ещё не бывала). Срок due_at НЕ проверяется намеренно:
    вечером набор показывается целиком снова (решение владельца 04.09.2026)."""
    def _fetch(cur):
        cur.execute(_WORD_PICK_CARD_SQL, (int(user_id), for_day))
        out = []
        for r in cur.fetchall() or []:
            srs = None
            if r[9] is not None:
                srs = {
                    "status": r[9], "due_at": r[10], "last_review_at": r[11],
                    "interval_days": int(r[12] or 0), "reps": int(r[13] or 0),
                    "lapses": int(r[14] or 0), "stability": float(r[15] or 0.0),
                    "difficulty": float(r[16] or 0.0), "step": int(r[17] or 0),
                }
            out.append({
                "card": {
                    "id": int(r[0]), "word_ru": r[1], "translation_de": r[2], "word_de": r[3],
                    "translation_ru": r[4], "source_lang": r[5], "target_lang": r[6],
                    "response_json": r[7], "user_notes": r[8],
                },
                "srs": srs,
                "am_rated_at": r[18], "pm_rated_at": r[19], "origin_process": r[20],
            })
        return out
    if cursor is not None:
        items = _fetch(cursor)
    else:
        with get_db_connection_context() as conn:
            with conn.cursor() as own:
                items = _fetch(own)
    # Разбор берём с единицы — тот же, что видит словарь и Space Rep.
    attach_unit_content_to_cards([it["card"] for it in items])
    return items


def word_pick_card_ids(*, user_id: int, for_day, cursor=None) -> list[int]:
    def _fetch(cur):
        cur.execute("SELECT card_id FROM bt_3_word_picks WHERE user_id = %s AND for_day = %s;",
                    (int(user_id), for_day))
        return [int(r[0]) for r in (cur.fetchall() or [])]
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own:
            return _fetch(own)


def mark_word_pick_rated(*, user_id: int, card_id: int, for_day, slot: str, cursor=None) -> None:
    """Слово оценено в этом проходе. Первая оценка прохода фиксируется, повторная не
    двигает время — счётчик «оценено N из M» обязан не прыгать."""
    if slot not in ("am", "pm"):
        raise ValueError(f"word pick: неизвестный проход {slot!r}")
    column = f"{slot}_rated_at"
    sql = (f"UPDATE bt_3_word_picks SET {column} = COALESCE({column}, NOW()) "
           "WHERE user_id = %s AND card_id = %s AND for_day = %s;")
    if cursor is not None:
        cursor.execute(sql, (int(user_id), int(card_id), for_day))
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as own:
            own.execute(sql, (int(user_id), int(card_id), for_day))
        conn.commit()


def word_pick_day_stats(day) -> dict:
    """Числа для утреннего отчёта за день `day`: кто получал, сколько открыли, сколько оценили."""
    ensure_word_pick_schema()
    day_no = int(day.strftime("%Y%m%d"))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT user_id), COUNT(*),
                       COUNT(DISTINCT user_id) FILTER (WHERE am_rated_at IS NOT NULL),
                       COUNT(DISTINCT user_id) FILTER (WHERE pm_rated_at IS NOT NULL)
                FROM bt_3_word_picks WHERE for_day = %s;
                """,
                (day,),
            )
            p = cursor.fetchone() or (0, 0, 0, 0)
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE dispatch_id = %s),
                       COUNT(*) FILTER (WHERE dispatch_id = %s),
                       COUNT(DISTINCT user_id) FILTER (WHERE answered)
                FROM bt_3_interactive_inbox WHERE kind = 'wp' AND dispatch_id IN (%s, %s);
                """,
                (day_no * 10 + 1, day_no * 10 + 2, day_no * 10 + 1, day_no * 10 + 2),
            )
            i = cursor.fetchone() or (0, 0, 0)
    return {"pickers": int(p[0]), "cards": int(p[1]), "am_rated_users": int(p[2]),
            "pm_rated_users": int(p[3]), "posters_am": int(i[0]), "posters_pm": int(i[1]),
            "opened": int(i[2])}
```

- [ ] **Step 4: Норма: одна константа вместо трёх литералов**

В `backend/database.py` перед `get_inbox_delivery_stats_today` (около 23590):

```python
# Коды ведомости, которые приходят СВЕРХ дневной нормы и в счёт не идут:
# rv — «Работа над ошибками», wp — «Слова со вчерашних тренировок». Одно место на три
# запроса: раньше литерал 'rv' стоял в каждом отдельно, и второй бонусный код пришлось
# бы вписывать трижды (05.09.2026).
INBOX_BONUS_KINDS: tuple[str, ...] = ("rv", "wp")
_INBOX_BONUS_KINDS_SQL = " AND kind NOT IN (" + ", ".join(f"'{k}'" for k in INBOX_BONUS_KINDS) + ")"
```

В `get_inbox_delivery_stats_today` и `get_inbox_kinds_today` заменить `"WHERE user_id = %s AND created_at >= %s AND kind <> 'rv';"` на `"WHERE user_id = %s AND created_at >= %s" + _INBOX_BONUS_KINDS_SQL + ";"`.

В `backend/free_delivery_report.py:26` заменить `BONUS_INBOX_KINDS = {"rv"}` на:

```python
from backend.database import INBOX_BONUS_KINDS

# Бонусы: приходят сверх нормы и в счёт не идут. Список один на весь проект — в database.
BONUS_INBOX_KINDS = set(INBOX_BONUS_KINDS)
```

(проверить, что `free_delivery_report` уже импортирует что-то из `backend.database` без цикла: `grep -n "^from backend.database\|^import" backend/free_delivery_report.py`).

- [ ] **Step 5: Run tests**

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py backend/tests/test_daily_topup.py backend/tests/test_free_delivery*.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add backend/database.py backend/free_delivery_report.py backend/tests/test_word_pick_delivery.py
git commit -m "Набор дня, получатели и отметка прохода; код wp сверх нормы одной константой"
```

---

### Task 4: Постер, рассылка 07:25 / 19:35, превью-команда

**Files:**
- Modify: `backend/interactive_card.py` (после `render_review_card`, около 786)
- Modify: `bot_3.py` — константа рядом с `TRAINER_SLOT_TIMES` (42031), отправитель рядом с `_send_mistake_review_reminders` (37975), кроны рядом с регистрацией rv (47746), команда рядом с `_admin_trainer_preview_command` (37475) и её регистрация (46070)
- Test: `backend/tests/test_word_pick_delivery.py`

**Interfaces:**
- Consumes: `word_pick.slot_now/day_id/deeplink_for`, `database.list_word_pick_recipients`, `database.get_user_prefs_bulk`, `database.list_bot_blocked_user_ids`, `database.record_interactive_inbox`, `_is_access_locked_cached`, `_is_user_pro_cached`, `_user_send_budget`, `_is_deliverable_recipient_user_id`, `_get_quiz_schedule_now`, `_inbox_kb_json`, `get_webapp_deeplink`, `make_bonus_gated`.
- Produces: `interactive_card.render_word_pick_card(*, count: int) -> bytes | None`; `bot_3.WORD_PICK_SLOT_TIMES`; `bot_3._send_word_pick_reviews(context, slot)`; `/admin_wordpick_preview`.

- [ ] **Step 1: Write the failing tests** (добавить в `test_word_pick_delivery.py`)

```python
class Рассылка(unittest.TestCase):

    def test_постер_рисуется_и_называет_число_слов(self):
        from backend import interactive_card as ic
        if ic.Image is None:
            self.skipTest("Pillow не установлен")
        png = ic.render_word_pick_card(count=7)
        self.assertTrue(png and png[:8] == b"\x89PNG\r\n\x1a\n")
        self.assertEqual(ic._ru_words(1), "1 слово")
        self.assertEqual(ic._ru_words(3), "3 слова")
        self.assertEqual(ic._ru_words(7), "7 слов")
        self.assertEqual(ic._ru_words(21), "21 слово")

    def test_бот_шлёт_ссылку_которую_приложение_умеет_открыть(self):
        """Та же сверка, что в test_every_bot_link_opens, только адресно про wp."""
        import pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        bot = (корень / "bot_3.py").read_text(encoding="utf-8")
        jsx = (корень / "frontend/src/answer/AnswerOverlay.jsx").read_text(encoding="utf-8")
        self.assertIn("deeplink_for(", bot)
        self.assertRegex(jsx, r"\^ans_\([a-z|]*\bwp\b[a-z|]*\)_")

    def test_слоты_рассылки_равны_решению_владельца(self):
        import re, pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        bot = (корень / "bot_3.py").read_text(encoding="utf-8")
        m = re.search(r"WORD_PICK_SLOT_TIMES\s*=\s*\{([^}]*)\}", bot)
        self.assertIsNotNone(m)
        self.assertIn("(7, 25)", m.group(1)); self.assertIn("(19, 35)", m.group(1))
        # Регистрация крона идёт через make_bonus_gated — сверх нормы, без тарифного среза.
        self.assertIn('make_bonus_gated("word_pick"', bot)
        # Тихие часы в отправителе НЕ проверяются (решение владельца 04.09.2026).
        тело = bot.split("async def _send_word_pick_reviews(", 1)[1].split("\nasync def ", 1)[0]
        self.assertNotIn("_is_quiet_hours_now", тело)
        self.assertIn("_is_access_locked_cached", тело)
        self.assertIn("_user_send_budget", тело)          # «Тишина»
        self.assertIn("list_bot_blocked_user_ids", тело)  # заблокировавшие бота
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py -q -k "Рассылка"`
Expected: FAIL

- [ ] **Step 3: Постер в `backend/interactive_card.py`** (после `render_review_card`)

```python
def _ru_words(n: int) -> str:
    """«1 слово / 3 слова / 7 слов» — склонение по правилам русского счёта."""
    n = int(n)
    last, last2 = n % 10, n % 100
    if last == 1 and last2 != 11:
        form = "слово"
    elif 2 <= last <= 4 and not 12 <= last2 <= 14:
        form = "слова"
    else:
        form = "слов"
    return f"{n} {form}"


def render_word_pick_card(*, count: int = 0) -> bytes | None:
    """Постер «Слова со вчерашних тренировок»: человек сам тапнул эти слова вчера,
    сегодня повторяет утром и вечером. Гамма Fuchs-амбер, как у тренажёров."""
    subtitle = (f"{_ru_words(count)}  ·  ты сам их отобрал") if count else "ты сам их отобрал"
    return _card(badge="WIEDERHOLEN", title="Слова со вчерашних тренировок",
                 subtitle=subtitle, accent=(245, 158, 11), motif=_motif_flashcard,
                 cta="Повтори утром и вечером — и они останутся")
```

- [ ] **Step 4: Отправитель и кроны в `bot_3.py`**

4a. Рядом с `TRAINER_SLOT_TIMES` (42031):

```python
# «Слова со вчерашних тренировок»: утро и вечер, решение владельца 04.09.2026.
# Само правило «какой проход» — backend/word_pick.py, здесь только время крона.
WORD_PICK_SLOT_TIMES = {(7, 25): "am", (19, 35): "pm"}
```

4b. После `_send_mistake_review_reminders`:

```python
async def _send_word_pick_reviews(context: CallbackContext, slot: str) -> None:
    """«Слова со вчерашних тренировок»: постер + кнопка тем, у кого на сегодня есть слова,
    отобранные вчера тапом. Стратегия: docs/tasks/word_pick_review_strategy.md.

    Сверх нормы, всем тарифам (бесплатный месяц, Лайт, Полный). Не идёт запертым после
    бесплатного месяца, «Тишине» и заблокировавшим бота. Тихие часы НЕ проверяются:
    07:25 — время владельца (решение 04.09.2026), как у утренней новости."""
    from backend.database import (
        get_user_prefs_bulk, list_bot_blocked_user_ids, list_word_pick_recipients,
    )
    from backend.interactive_card import render_word_pick_card
    from backend.word_pick import day_id, deeplink_for
    today = _get_quiz_schedule_now().date()
    try:
        rows = await asyncio.to_thread(list_word_pick_recipients, today)
    except Exception:
        logging.exception("word pick: список получателей не собрался slot=%s", slot)
        return
    if not rows:
        logging.info("word_pick slot=%s: получателей нет", slot)
        return
    uids = [int(r["user_id"]) for r in rows]
    try:
        blocked = set(await asyncio.to_thread(list_bot_blocked_user_ids))
    except Exception:
        logging.exception("word pick: список заблокировавших не прочитался")
        return
    try:
        prefs = await asyncio.to_thread(get_user_prefs_bulk, tuple(uids))
    except Exception:
        logging.exception("word pick: настройки получателей не прочитались")
        return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(
        "🔁 Повторить слова", url=get_webapp_deeplink(deeplink_for(today)))]])
    when = "Утренний проход" if slot == "am" else "Вечерний проход"
    sent = skipped_locked = skipped_silent = skipped_blocked = failed = 0
    for r in rows:
        uid, n = int(r["user_id"]), int(r["count"])
        if uid <= 0 or n <= 0 or not _is_deliverable_recipient_user_id(uid):
            continue
        if uid in blocked:
            skipped_blocked += 1
            continue
        if await asyncio.to_thread(_is_access_locked_cached, uid):
            skipped_locked += 1
            continue
        p = prefs.get(uid) or {}
        is_pro = await asyncio.to_thread(_is_user_pro_cached, uid)
        if _user_send_budget(uid, is_pro=is_pro, preset=p.get("preset")) <= 0:
            skipped_silent += 1          # «Тишина» соблюдается всегда
            continue
        caption = (
            f"🔁 <b>Слова со вчерашних тренировок</b>\n\n"
            f"{when}: <b>{n}</b> на повтор — ты сам их отобрал вчера. "
            f"Оцени каждое, и они лягут в твоё расписание повторения 👇"
        )
        try:
            poster = await asyncio.to_thread(render_word_pick_card, count=n)
            if poster:
                msg = await context.bot.send_photo(chat_id=uid, photo=io.BytesIO(poster),
                                                   caption=caption, parse_mode="HTML", reply_markup=kb)
            else:
                msg = await context.bot.send_message(chat_id=uid, text=caption,
                                                     parse_mode="HTML", reply_markup=kb)
            await asyncio.to_thread(
                record_interactive_inbox,
                user_id=uid, kind="wp", dispatch_id=day_id(today, slot), chat_id=uid,
                telegram_message_id=int(msg.message_id), deeplink=deeplink_for(today),
                title="🔁 Слова со вчерашних тренировок", keyboard_json=_inbox_kb_json(kb),
            )
            sent += 1
        except Exception:
            failed += 1
            logging.warning("word pick: не ушло uid=%s slot=%s", uid, slot, exc_info=True)
    logging.info("word_pick slot=%s sent=%d locked=%d silent=%d blocked=%d failed=%d candidates=%d",
                 slot, sent, skipped_locked, skipped_silent, skipped_blocked, failed, len(rows))
```

4c. Кроны — сразу после регистрации rv (`hour=11, minute=0` блок около 47746):

```python
        # -- «Слова со вчерашних тренировок»: 07:25 и 19:35, сверх нормы, всем тарифам.
        #    Тихие часы отправитель не проверяет (решение владельца 04.09.2026). --
        for (wp_h, wp_m), wp_slot in WORD_PICK_SLOT_TIMES.items():
            scheduler.add_job(
                make_bonus_gated("word_pick", wp_h, wp_m, _send_word_pick_reviews, wp_slot),
                "cron", hour=wp_h, minute=wp_m, timezone=QUIZ_SCHEDULE_TZ_NAME,
            )
```

4d. Превью-команда после `_admin_trainer_preview_command`:

```python
async def _admin_wordpick_preview_command(update: Update, context: CallbackContext) -> None:
    """/admin_wordpick_preview [am|pm] — прислать СЕБЕ постер «Слова со вчерашних
    тренировок» за сегодня, как его получит человек. Ведомость не пишет."""
    user = update.effective_user; message = update.effective_message
    if not user or not message:
        return
    if not _can_use_image_quiz_test_commands(getattr(user, "id", None)):
        await message.reply_text("Allowed users only."); return
    from backend.database import list_word_pick_cards
    from backend.interactive_card import render_word_pick_card
    from backend.word_pick import deeplink_for
    today = _get_quiz_schedule_now().date()
    items = await asyncio.to_thread(list_word_pick_cards, user_id=int(user.id), for_day=today)
    if not items:
        await message.reply_text(
            f"На {today:%d.%m} у тебя нет отобранных слов: вчера ни одного тапа в интерактивах. "
            "Тапни слово в конце любой тренировки — и постер придёт завтра.")
        return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(
        "🔁 Повторить слова", url=get_webapp_deeplink(deeplink_for(today)))]])
    poster = await asyncio.to_thread(render_word_pick_card, count=len(items))
    caption = (f"🔁 <b>Слова со вчерашних тренировок</b>\n\nПревью: <b>{len(items)}</b> на повтор.")
    await context.bot.send_photo(chat_id=message.chat_id, photo=io.BytesIO(poster),
                                 caption=caption, parse_mode="HTML", reply_markup=kb)
```

Регистрация рядом с `admin_trainer_preview` (46070):

```python
    application.add_handler(CommandHandler("admin_wordpick_preview", _admin_wordpick_preview_command))
```

- [ ] **Step 5: Run tests** (тест ссылки упадёт до Task 7 — это ожидаемо, регулярка `wp` появится там; остальные должны пройти)

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py -q`
Expected: всё PASS, кроме `test_бот_шлёт_ссылку_которую_приложение_умеет_открыть` и `test_every_bot_link_opens` (они зеленеют в Task 7). Не коммитить красное без пометки: коммит-сообщение говорит «ссылка открывается в Task 7».

- [ ] **Step 6: Commit**

```bash
git add backend/interactive_card.py bot_3.py backend/tests/test_word_pick_delivery.py
git commit -m "Постер и рассылка «Слова со вчерашних тренировок» 07:25/19:35 сверх нормы; превью-команда (экран — следующим коммитом)"
```

---

### Task 5: Сервер: набор дня и оценка с источником `pick`

**Files:**
- Modify: `backend/backend_server.py` — `FLASHCARDS_QUEUE_SOURCE_ALLOWED` (6810), `review_srs_card` (58478-58706), новый маршрут рядом с `answer_review_next` (32911)
- Test: `backend/tests/test_word_pick_review.py`

**Interfaces:**
- Produces: `POST /api/answer/wordpick/set` `{initData, day}` → `{ok, day, slot, total, items:[{card, srs, srs_preview, rated}]}`; `POST /api/cards/review` принимает `queue_source: "pick"`, `day` (ГГГГММДД), и тогда: карточка обязана быть в наборе дня (иначе 403), дневной потолок бесплатного тарифа НЕ применяется, `next` = null, проставляется `mark_word_pick_rated`.

- [ ] **Step 1: Write the failing tests**

```python
# backend/tests/test_word_pick_review.py
# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: оценка идёт в НАСТОЯЩЕЕ расписание (решение
владельца 04.09.2026), набор дня не режется сроком, чужая карточка не принимается."""
import os
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import backend_server as srv  # noqa: E402


class ИсточникОчереди(unittest.TestCase):

    def test_pick_разрешён_как_источник_очереди(self):
        self.assertEqual(srv._normalize_flashcards_queue_source("pick"), "pick")
        self.assertEqual(srv._normalize_flashcards_queue_source("PICK "), "pick")
        self.assertEqual(srv._normalize_flashcards_queue_source("чушь"), "system")

    def test_обработчик_оценки_знает_ветку_pick(self):
        import inspect
        src = inspect.getsource(srv.review_srs_card)
        self.assertIn('queue_source == "pick"', src)
        self.assertIn("word_pick_card_ids(", src)
        self.assertIn("mark_word_pick_rated(", src)
        # Оценка пишется тем же путём, что и в Space Rep: schedule_review → upsert → журнал.
        self.assertIn("schedule_review(", src)
        self.assertIn("insert_card_review_log(", src)

    def test_набор_дня_отдаёт_подсказки_интервалов_как_space_rep(self):
        import inspect
        src = inspect.getsource(srv.answer_word_pick_set)
        self.assertIn("_build_srs_review_preview(", src)
        self.assertIn('_inbox_mark_kind_done(int(user_id), "wp")', src)
        self.assertIn("parse_day(", src)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest backend/tests/test_word_pick_review.py -q`
Expected: FAIL

- [ ] **Step 3: Implement in `backend/backend_server.py`**

3a. Строка 6810: `FLASHCARDS_QUEUE_SOURCE_ALLOWED = {"system", "manual", "pick"}` и комментарий над ней:

```python
# pick — «Слова со вчерашних тренировок» (docs/tasks/word_pick_review_strategy.md):
# набор дня человека, оценки в настоящее расписание, вне дневных потолков.
```

3b. Новый маршрут после `answer_review_next`:

```python
@app.route("/api/answer/wordpick/set", methods=["POST"])
def answer_word_pick_set():
    """«Слова со вчерашних тренировок»: набор дня для этого человека и какие из слов уже
    оценены в ТЕКУЩЕМ проходе (утро/вечер). Срок карточек не проверяется: вечером набор
    показывается снова целиком (решение владельца 04.09.2026)."""
    user_id, _user_name, err = _answer_auth_user_id()
    if user_id is None:
        return err
    payload = request.get_json(silent=True) or {}
    from backend.database import list_word_pick_cards
    from backend.word_pick import parse_day, slot_now
    day = parse_day(payload.get("day"))
    if day is None:
        return jsonify({"error": "day обязателен: ГГГГММДД"}), 400
    now_local = datetime.now(ZoneInfo("Europe/Vienna"))
    slot = slot_now(now_local)
    try:
        rows = list_word_pick_cards(user_id=int(user_id), for_day=day)
    except Exception as exc:
        logging.exception("word pick set: набор не прочитался user=%s day=%s", user_id, day)
        return jsonify({"error": f"Набор не прочитался: {exc}"}), 500
    _inbox_mark_kind_done(int(user_id), "wp")
    reviewed_at = datetime.now(timezone.utc)
    new_state = {"status": "new", "due_at": reviewed_at, "last_review_at": None, "interval_days": 0,
                 "reps": 0, "lapses": 0, "stability": 0.0, "difficulty": 0.0}
    items = []
    for r in rows:
        state = r["srs"] if r["srs"] is not None else new_state
        items.append({
            "card": r["card"],
            "srs": r["srs"],
            "srs_preview": _build_srs_review_preview(current_state=state, reviewed_at=reviewed_at),
            "rated": r[f"{slot}_rated_at"] is not None,
        })
    return jsonify({"ok": True, "day": day.isoformat(), "slot": slot, "total": len(items), "items": items})
```

Если `ZoneInfo` в файле не импортирован (`grep -n "ZoneInfo" backend/backend_server.py | head -2`), добавить `from zoneinfo import ZoneInfo` к импортам.

3c. В `review_srs_card`:

После `answered_mode = _normalize_flashcards_mode(payload.get("mode") or "fsrs")`:

```python
    # «Слова со вчерашних тренировок»: карточка обязана быть в наборе дня, потолки не
    # действуют, следующую карточку выбирает экран сам (набор у него на руках).
    pick_day = None
    if queue_source == "pick":
        from backend.word_pick import parse_day
        pick_day = parse_day(payload.get("day"))
        if pick_day is None:
            return jsonify({"error": "day обязателен для queue_source=pick"}), 400
```

Внутри `with ... as cursor:` сразу после `card = get_dictionary_entry_for_user(...)` / `if not card: return 404`:

```python
                if queue_source == "pick":
                    from backend.database import word_pick_card_ids
                    if card_id not in set(word_pick_card_ids(user_id=int(user_id), for_day=pick_day, cursor=cursor)):
                        return jsonify({"error": "Это слово не из сегодняшнего набора."}), 403
```

После `mark("db_write")` заменить хвост блока (от `_log_flashcards_words_answered(` до `mark("build_next")` включительно) на:

```python
                if queue_source == "pick":
                    from backend.database import mark_word_pick_rated
                    from backend.word_pick import slot_now
                    mark_word_pick_rated(user_id=int(user_id), card_id=card_id, for_day=pick_day,
                                         slot=slot_now(datetime.now(ZoneInfo("Europe/Vienna"))), cursor=cursor)
                    payload_next = None      # набор дня у экрана на руках; потолков нет (всем, вне лимитов)
                else:
                    _log_flashcards_words_answered(
                        ... (существующий код без изменений, с отступом на один уровень глубже) ...
                    mark("build_next")
```

То есть существующие строки `_log_flashcards_words_answered(...)`, `fsrs_limit_state = ...`, `if fsrs_limit_state.get("error"): ... else: ...` и `mark("build_next")` целиком уходят в ветку `else:`. Ничего в них не менять.

- [ ] **Step 4: Run tests**

Run: `python3 -m pytest backend/tests/test_word_pick_review.py backend/tests/test_flashcards_free_limit_ux.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/backend_server.py backend/tests/test_word_pick_review.py
git commit -m "Набор дня /api/answer/wordpick/set и оценка с queue_source=pick в настоящее расписание"
```

---

### Task 6: Экран `WordPickGame` в оверлее интерактивов

**Files:**
- Create: `frontend/src/answer/wordTts.js` (вынос `playWordTts` из `AnswerOverlay.jsx:163-185`)
- Create: `frontend/src/answer/WordPickGame.jsx`
- Modify: `frontend/src/answer/AnswerOverlay.jsx` — импорт, регулярка (79), список «грузят себя сами» (832), монтирование (после `kind === 'rv'`, 1030)
- Modify: `frontend/src/answer/answer.css` — стили `wp-*`
- Test: `backend/tests/test_every_bot_link_opens.py` (существующий), `backend/tests/test_word_pick_delivery.py::test_бот_шлёт_ссылку…`

**Interfaces:**
- Consumes: `POST /api/answer/wordpick/set`, `POST /api/cards/review` (`queue_source:'pick'`, `day`, `card_id`, `rating`, `response_ms`, `mode:'fsrs'`), компоненты `FsrsHeadword`, `WordHintModal` (+`collectHintExamples`, `hasHintBreakdown`), `CardOwnNotes`, `splitTranslationSenses`.
- Produces: `WordPickGame({ day, api, haptic, onClose })`; `playWordTts(text)` из `wordTts.js`.

- [ ] **Step 1: Вынести озвучку**

Создать `frontend/src/answer/wordTts.js`: перенести туда функцию `playWordTts` из `AnswerOverlay.jsx:162-185` дословно, с `export async function playWordTts(text)`. Ей нужен `getInitData` — посмотреть, откуда он в `AnswerOverlay.jsx` (`grep -n "getInitData" frontend/src/answer/AnswerOverlay.jsx | head -3`), и импортировать оттуда же. В `AnswerOverlay.jsx` удалить локальную функцию и добавить `import { playWordTts } from './wordTts.js';`.

- [ ] **Step 2: Регулярка и монтирование в `AnswerOverlay.jsx`**

- Строка 79: добавить `wp` в чередование, например после `rv`: `...|rv|wp|adbl|...`.
- Строка 832: добавить `'wp'` в массив «грузят себя сами».
- Импорт: `import WordPickGame from './WordPickGame.jsx';`
- После блока `if (kind === 'rv') { ... }`:

```jsx
  if (kind === 'wp' && parsed?.id != null) {
    // id — день ГГГГММДД: у постера нет своего dispatch, как и у «Работы над ошибками».
    return <WordPickGame day={String(parsed.id)} api={api} haptic={haptic} onClose={close} />;
  }
```

- В `KIND_META` (около 85) добавить `wp: { eyebrow: '🔁 Wiederholen', title: 'Слова со вчерашних тренировок' },` — если объект используется для заголовка у self-loading игр (проверить `grep -n "KIND_META\[" AnswerOverlay.jsx`); иначе не добавлять.

- [ ] **Step 3: Компонент `frontend/src/answer/WordPickGame.jsx`**

```jsx
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import FsrsHeadword from './FsrsHeadword.jsx';
import WordHintModal, { collectHintExamples, hasHintBreakdown } from '../components/WordHintModal.jsx';
import CardOwnNotes from '../components/CardOwnNotes.jsx';
import { splitTranslationSenses } from '../dictionary/senses.js';
import { playWordTts } from './wordTts.js';
import Toast, { useToast } from './Toast.jsx';

// «Слова со вчерашних тренировок» — разовое повторение того, что человек сам тапнул
// вчера в конце интерактивов. Стратегия: docs/tasks/word_pick_review_strategy.md.
//
// Детали карточки — те же, что в «Карточках Space Rep» (App.jsx, блок fsrs-study-card):
// русский вопрос → «Показать ответ» (или сам через 5 с) → немецкое слово с автоподгонкой
// → озвучка сразу и кнопкой → лампочка, если разбор уже есть → заметки → четыре оценки
// с подсказкой интервала. Оценка уходит в /api/cards/review с queue_source='pick' и
// пишется в НАСТОЯЩЕЕ расписание (решение владельца 04.09.2026). Следующую карточку
// выбирает сам экран: набор дня у него на руках, сервер `next` не отдаёт.
const AUTO_REVEAL_SEC = 5;
const RATINGS = [
  ['AGAIN', 'Снова', 'again'],
  ['HARD', 'Трудно', 'hard'],
  ['GOOD', 'Хорошо', 'good'],
  ['EASY', 'Легко', 'easy'],
];
const ru = (s) => s; // CardOwnNotes/WordHintModal просят функцию перевода; здесь всё по-русски

function intervalHint(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return '';
  if (s < 3600) return `${Math.max(1, Math.ceil(s / 60))} мин`;
  if (s < 86400) return `${Math.max(1, Math.ceil(s / 3600))} ч`;
  return `${Math.max(1, Math.ceil(s / 86400))} дн`;
}
const hasCyrillic = (s) => /[а-яё]/i.test(String(s || ''));

// Вопрос — всегда русская сторона, ответ — немецкая (как в Space Rep, App.jsx ~42945).
function sidesOf(card) {
  const a = String(card?.word_ru || card?.translation_ru || '').trim();
  const b = String(card?.word_de || card?.translation_de || '').trim();
  const question = hasCyrillic(a) ? a : (hasCyrillic(b) ? b : a);
  const answer = question === a ? b : a;
  const senses = splitTranslationSenses(answer);
  return {
    question,
    answer: senses.length > 1 ? senses[0].value : answer,
    extra: senses.length > 1
      ? senses.slice(1, 6).map((x, i) => ({ rank: i + 2, text: x.label ? `${x.value} (${x.label})` : x.value }))
      : [],
    spoken: String(card?.word_de || card?.translation_de || answer).trim(),
  };
}

export default function WordPickGame({ day, api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|card|done|error
  const [error, setError] = useState('');
  const [slot, setSlot] = useState('am');
  const [items, setItems] = useState([]);
  const [queue, setQueue] = useState([]);       // индексы ещё не оценённых в этом проходе
  const [revealed, setRevealed] = useState(false);
  const [revealedAt, setRevealedAt] = useState(0);
  const [elapsed, setElapsed] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [hintOpen, setHintOpen] = useState(false);
  const [ratedNow, setRatedNow] = useState(0);
  const toast = useToast();
  const spokenRef = useRef('');

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/wordpick/set', { day });
        if (cancelled) return;
        const list = Array.isArray(data?.items) ? data.items : [];
        setItems(list);
        setSlot(data?.slot === 'pm' ? 'pm' : 'am');
        const pending = list.map((it, i) => (it.rated ? -1 : i)).filter((i) => i >= 0);
        setQueue(pending);
        setPhase(pending.length ? 'card' : 'done');
      } catch (e) {
        if (!cancelled) { setError('Не удалось загрузить набор. Попробуй ещё раз.'); setPhase('error'); }
      }
    })();
    return () => { cancelled = true; };
  }, [api, day]);

  const current = phase === 'card' && queue.length ? items[queue[0]] : null;
  const card = current?.card || null;
  const sides = useMemo(() => (card ? sidesOf(card) : null), [card]);

  // Автораскрытие через 5 с, как в Space Rep.
  useEffect(() => {
    if (!card || revealed) return undefined;
    const t = window.setTimeout(() => reveal(), AUTO_REVEAL_SEC * 1000);
    return () => window.clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [card?.id, revealed]);

  // Секундомер ответа.
  useEffect(() => {
    if (!revealed || !revealedAt) return undefined;
    const id = window.setInterval(() => setElapsed(Math.floor((Date.now() - revealedAt) / 1000)), 250);
    return () => window.clearInterval(id);
  }, [revealed, revealedAt]);

  // Озвучка один раз при открытии ответа.
  useEffect(() => {
    if (!revealed || !sides?.spoken) return;
    const key = `${card?.id}`;
    if (spokenRef.current === key) return;
    spokenRef.current = key;
    playWordTts(sides.spoken).catch(() => { /* звук не обязателен; кнопка ниже повторит */ });
  }, [revealed, sides, card]);

  const reveal = useCallback(() => {
    setRevealedAt(Date.now()); setElapsed(0); setHintOpen(false); setRevealed(true);
  }, []);

  const rate = useCallback(async (rating) => {
    if (!card || submitting) return;
    setSubmitting(true);
    try {
      await api('/api/cards/review', {
        card_id: card.id, rating, queue_source: 'pick', mode: 'fsrs', day,
        response_ms: revealedAt ? Math.max(0, Date.now() - revealedAt) : null,
      });
      try { haptic?.('ok'); } catch (_e) { /* noop */ }
      setRatedNow((n) => n + 1);
      setRevealed(false); setRevealedAt(0); setElapsed(0);
      setQueue((q) => {
        const rest = q.slice(1);
        if (!rest.length) setPhase('done');
        return rest;
      });
    } catch (e) {
      try { haptic?.('bad'); } catch (_e) { /* noop */ }
      toast.show('Оценка не сохранилась. Проверь связь и нажми ещё раз.');
    } finally {
      setSubmitting(false);
    }
  }, [api, card, day, haptic, revealedAt, submitting, toast]);

  const shell = (body, cls = '') => (
    <div className="ans-root ans-root--keepkbd">
      <div className={`ans-card wp-card ${cls}`}>{body}</div>
      <Toast state={toast.state} onClose={toast.hide} />
    </div>
  );

  if (phase === 'loading') return shell(<><div className="ans-skel" /><div className="ans-skel sm" /></>);
  if (phase === 'error') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );
  if (phase === 'done') {
    const total = items.length;
    return shell(
      <>
        <div className="ans-head"><span className="ans-eyebrow">🔁 Слова со вчерашних тренировок</span></div>
        <div className="tr-score">
          <div className="tr-score-num">{total}</div>
          <div className="tr-score-sub">{total === 1 ? 'слово повторено' : 'слов повторено'} · {slot === 'am' ? 'утренний' : 'вечерний'} проход</div>
        </div>
        <div className="tr-done-note">
          {slot === 'am'
            ? 'Вечером в 19:35 этот набор придёт ещё раз. Завтра придут те слова, что отберёшь сегодня.'
            : 'Готово на сегодня. Завтра придут те слова, что отберёшь сегодня в тренировках.'}
        </div>
        <button className="ans-btn" onClick={onClose}>Schließen</button>
      </>
    );
  }

  const done = items.length - queue.length;
  const hintSource = card?.response_json && typeof card.response_json === 'object' ? card.response_json : null;
  const hintExamples = collectHintExamples(hintSource, 3);
  const hintTip = String(hintSource?.memory_tip || '').trim();
  const hintAvailable = hintExamples.length > 0 || hasHintBreakdown(hintSource) || !!hintTip;
  const preview = current?.srs_preview || {};
  const notes = Array.isArray(card?.user_notes) ? card.user_notes : [];

  return shell(
    <>
      <div className="ans-head">
        <span className="ans-eyebrow">🔁 Слова со вчерашних тренировок</span>
        <span className="wp-progress">{done + 1} из {items.length}</span>
      </div>
      <div className={`fsrs-study-card wp-study ${revealed ? 'is-revealed' : ''}`}>
        {revealed && hintAvailable && (
          <button type="button" className="fsrs-hint-btn" onClick={() => setHintOpen(true)}
                  aria-label="Всё об этом слове" title="Всё об этом слове">💡</button>
        )}
        <div className="fsrs-card-source is-muted-top">{sides.question}</div>
        <div className="fsrs-divider" />
        {revealed ? (
          <>
            <FsrsHeadword text={sides.answer} />
            {sides.extra.length > 0 && (
              <div className="flashcard-meaning-list">
                {sides.extra.map((row) => (
                  <div key={row.rank} className="flashcard-meaning-item">
                    <span className="flashcard-meaning-rank">{row.rank}.</span>
                    <span className="flashcard-meaning-text">{row.text}</span>
                  </div>
                ))}
              </div>
            )}
            <CardOwnNotes notes={notes} tr={ru} readOnly />
            <div className="fsrs-divider" />
            <div className="fsrs-card-meta fsrs-card-meta-answer">Response time: {elapsed}s</div>
            <button type="button" className="flashcard-audio-replay"
                    onClick={() => playWordTts(sides.spoken).catch(() => toast.show('Озвучка не загрузилась.'))}
                    aria-label="Повторить аудио" title="Повторить аудио">🔊</button>
          </>
        ) : (
          <>
            <div className="fsrs-card-meta">
              Status: {String(current?.srs?.status || 'new')} · Interval: {current?.srs?.interval_days ?? 0} дн
            </div>
            <button type="button" className="fsrs-show-answer-btn" onClick={reveal} disabled={submitting}>Show Answer</button>
          </>
        )}
      </div>
      {revealed && (
        <div className="fsrs-rating-wrap">
          <div className="fsrs-rating-grid">
            {RATINGS.map(([key, label, cls]) => (
              <div className="fsrs-rate-cell" key={key}>
                <button type="button" className={`fsrs-rate-btn ${cls}`} onClick={() => rate(key)} disabled={submitting}>
                  <span>{label}</span>
                </button>
                <small className="fsrs-rate-hint">{intervalHint(preview?.[key]?.seconds)}</small>
              </div>
            ))}
          </div>
        </div>
      )}
      <WordHintModal
        isOpen={hintOpen}
        onClose={() => setHintOpen(false)}
        tr={ru}
        headword={sides.spoken || sides.answer}
        translation={sides.question}
        item={hintSource}
        examples={hintExamples}
        formRows={[]}
        memoryTip={hintTip}
      />
    </>
  );
}
```

Проверить перед сборкой: как `WordHintModal` в `App.jsx` получает `formRows` (`getDictionaryFormRows` — локальная функция App.jsx, строка 23838). Если она короткая и чистая, вынести её в `frontend/src/dictionary/formRows.js` с `export function getDictionaryFormRows(item)` и использовать в обоих местах вместо `formRows={[]}`; если тянет состояние App — оставить `[]` и записать это в комментарии компонента как известное ограничение (таблицу форм лампочка тут не показывает).

- [ ] **Step 4: Стили в `frontend/src/answer/answer.css`** (в конец файла)

```css
/* ── «Слова со вчерашних тренировок» (WordPickGame) ─────────────────────────────────
   Карточка — детали Space Rep (.fsrs-*, App.css), обёртка — интерактив (.ans-card).
   Гамма интерактивов: янтарный акцент в шапке, сами кнопки оценок оставлены
   семантическими (снова/трудно/хорошо/легко), как в Space Rep. */
.wp-card .ans-head { flex-direction: row; align-items: baseline; justify-content: space-between; }
.wp-progress { font-size: 0.8125rem; font-weight: 600; color: #d97706; white-space: nowrap; }
.wp-card .fsrs-study-card { background: rgba(255,255,255,0.03); border: 1px solid rgba(245,158,11,0.25); }
.wp-card .fsrs-rating-wrap { margin-top: 0.5rem; }
```

После сборки посмотреть, не спорят ли `.fsrs-study-card` из App.css с шириной `.ans-card`; при споре добавить `.wp-card .fsrs-study-card { width: 100%; max-width: none; }`.

- [ ] **Step 5: Сборка и тесты**

Run: `cd frontend && npm run build 2>&1 | tail -5 && cd ..` — Expected: `built in …`, без ошибок. `frontend/dist` НЕ добавлять в git.
Run: `python3 -m pytest backend/tests/test_every_bot_link_opens.py backend/tests/test_word_pick_delivery.py -q` — Expected: PASS (включая `wp`).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/answer/wordTts.js frontend/src/answer/WordPickGame.jsx frontend/src/answer/AnswerOverlay.jsx frontend/src/answer/answer.css
git commit -m "Экран «Слова со вчерашних тренировок»: карточка из деталей Space Rep в оверлее интерактивов, код wp"
```

---

### Task 7: Подпись и чипы в пяти местах; обогащение только для нового слова

**Files:**
- Create: `frontend/src/answer/pickCopy.js`
- Modify: `frontend/src/dictionary/saveUtils.js:102-136` (`saveGermanWordViaLookup`), `:138-170` (`saveDictionaryCard` — вернуть `pickedForDay`)
- Modify: `frontend/src/answer/TrainerGame.jsx:129-144, 264-278`; `SprintGame.jsx:130-150, 248-262`; `ArtikelSprintGame.jsx:131-155, 285-300`; `AdjektivLearnGame.jsx:95-115`; `AnswerOverlay.jsx:517-535, 653-678`; `SaveWordChip.jsx`
- Test: `backend/tests/test_word_pick_copy.py`

**Interfaces:**
- Produces: `pickCopy.PICK_CAPTION` (строка), `pickCopy.chipLabel({ de, saved, known, picked }) -> string`, `pickCopy.PICK_FAILED_TOAST`; `saveGermanWordViaLookup` возвращает `{ sourceText, targetText, inserted, pickedForDay }`.

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_word_pick_copy.py
# -*- coding: utf-8 -*-
"""Одна подпись над чипами во всех пяти местах, и обогащение только для НОВОГО слова.

Тап по слову «уже есть» до 05.09.2026 всё равно запускал запрос разбора (это деньги,
если слова нет в общем пуле). Теперь обогащение ждёт ответа сохранения и идёт только
при inserted=true. Проверка по исходникам: фронт-тестов в проекте нет."""
import pathlib
import re
import unittest

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2] / "frontend/src"
МЕСТА = ["answer/TrainerGame.jsx", "answer/SprintGame.jsx", "answer/ArtikelSprintGame.jsx",
         "answer/AdjektivLearnGame.jsx", "answer/AnswerOverlay.jsx", "answer/SaveWordChip.jsx"]


class ПодписьОднаНаВсех(unittest.TestCase):

    def test_каждое_место_с_чипами_берёт_подпись_из_одного_файла(self):
        for имя in МЕСТА:
            src = (КОРЕНЬ / имя).read_text(encoding="utf-8")
            self.assertIn("from './pickCopy.js'", src, имя)
            self.assertNotIn("нажми, чтобы сохранить в словарь", src, имя)

    def test_подпись_обещает_и_словарь_и_завтра(self):
        src = (КОРЕНЬ / "answer/pickCopy.js").read_text(encoding="utf-8")
        self.assertIn("сохранится в словарь, если его ещё нет, и завтра придёт на повторение", src)
        self.assertIn("завтра повторим", src)

    def test_обогащение_только_после_ответа_и_только_для_нового(self):
        src = (КОРЕНЬ / "dictionary/saveUtils.js").read_text(encoding="utf-8")
        тело = src.split("export async function saveGermanWordViaLookup", 1)[1].split("\nasync function saveDictionaryCard", 1)[0]
        self.assertIn("if (res?.inserted !== false) void enrichSavedGermanWord", тело)
        self.assertIn("pickedForDay", тело)
        self.assertIsNone(re.search(r"void enrichSavedGermanWord\([^)]*\);\s*\n\s*//[^\n]*\n\s*//[^\n]*\n\s*return", тело),
                          "обогащение всё ещё уходит до разбора ответа")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest backend/tests/test_word_pick_copy.py -q`
Expected: FAIL

- [ ] **Step 3: `frontend/src/answer/pickCopy.js`**

```js
// Одна подпись над чипами «сохранить слово» для ВСЕХ интерактивов и одна функция
// надписи на чипе. Тап по слову делает две вещи сразу — кладёт в словарь (если нет) и
// отбирает на завтрашнее повторение «Слова со вчерашних тренировок»; подпись обязана
// обещать обе. Стратегия: docs/tasks/word_pick_review_strategy.md.
export const PICK_CAPTION = 'Нажми на слово: оно сохранится в словарь, если его ещё нет, и завтра придёт на повторение';

// Слово записано, а отбор на завтра — нет (сервер ответил pick_for_day: null; причина в его логе).
export const PICK_FAILED_TOAST = 'Слово в словаре, но на завтра не отобралось. Нажми ещё раз позже.';

export function chipLabel({ de, saved, known, picked }) {
  const word = String(de || '').trim();
  if (!saved) return word;
  const tail = picked ? ' · завтра повторим' : '';
  if (known) return `${word} · уже есть${tail}`;
  return `💾 ${word}${tail}`;
}
```

- [ ] **Step 4: `saveUtils.js`**

В `saveGermanWordViaLookup` заменить хвост после `const res = await postDictionarySave(api, {...});`:

```js
  // Обогащение (запрос разбора) — только для НОВОГО слова: у «уже есть» разбор либо
  // лежит, либо его доберёт ночь. До 05.09.2026 оно уходило до ответа и для всех.
  if (res?.inserted !== false) void enrichSavedGermanWord({ api, word: text, knownRu, origin });
  return {
    sourceText: text,
    targetText: knownRu,
    inserted: res?.inserted !== false,
    // «Слова со вчерашних тренировок»: день, на который слово отобрано; '' — не отобрано.
    pickedForDay: String(res?.pick_for_day || ''),
  };
```

В `saveDictionaryCard` в `return` добавить `pickedForDay: String(res?.pick_for_day || '')`.

- [ ] **Step 5: Пять мест**

Общий приём для `TrainerGame.jsx`, `SprintGame.jsx`, `AnswerOverlay.jsx` (чипы) и `ArtikelSprintGame.jsx`:

1. `import { PICK_CAPTION, PICK_FAILED_TOAST, chipLabel } from './pickCopy.js';`
2. Новое состояние `const [picked, setPicked] = useState(() => new Set());`
3. В `.then((res) => { ... })` обработчика сохранения добавить:
   ```js
   if (res && res.pickedForDay) setPicked((p) => new Set(p).add(de));
   else toast.show(PICK_FAILED_TOAST);
   ```
   (в `ArtikelSprintGame` ключ — `key`, а не `de`).
4. Подпись: `<span className="tr-all-dim">· 👆 {PICK_CAPTION}</span>` вместо «нажми, чтобы сохранить в словарь» (в `ArtikelSprintGame` строка 285: `👆 {PICK_CAPTION} (с артиклем)`).
5. Текст чипа: `{chipLabel({ de, saved: isSaved, known: isKnown, picked: picked.has(de) })}` вместо ручной склейки `💾`/`уже есть`.

`AdjektivLearnGame.jsx` (там всплывашка, не чип): импорт `PICK_CAPTION, PICK_FAILED_TOAST`; в `.then` показывать `showToast(res.inserted === false ? `«${word_de}» уже был в словаре · завтра повторим` : `«${word_de}» сохранено · завтра повторим`, 'info')` при `res.pickedForDay`, иначе `showToast(PICK_FAILED_TOAST, 'info')`; подпись под словом там, где сейчас объясняется тап (найти по `grep -n "сохран" AdjektivLearnGame.jsx`), заменить на `PICK_CAPTION`.

`SaveWordChip.jsx`: импорт `chipLabel, PICK_FAILED_TOAST`; после успешного `saveGermanWordViaLookup` хранить `picked` в состоянии (`'saved'` → `{ state: 'saved', picked: !!res.pickedForDay }`), подпись кнопки в состоянии saved: `picked ? '💾 · завтра повторим' : '💾'`; при `!res.pickedForDay` — `toast.show(PICK_FAILED_TOAST)`. Заголовок кнопки (`title`/`aria-label`) — `PICK_CAPTION`.

`AnswerOverlay.jsx` строка 665: у чипа сейчас нет `known` — добавить `const [known, setKnown]` рядом с `saved` и `setKnown` в `.then`, как в TrainerGame (это тот дефект, что нашёл разбор 04.09: чип там никогда не говорил «уже есть»).

- [ ] **Step 6: Сборка и тест**

Run: `cd frontend && npm run build 2>&1 | tail -3 && cd .. && python3 -m pytest backend/tests/test_word_pick_copy.py -q`
Expected: сборка без ошибок, PASS

- [ ] **Step 7: Commit**

```bash
git add frontend/src/answer/pickCopy.js frontend/src/dictionary/saveUtils.js frontend/src/answer/TrainerGame.jsx frontend/src/answer/SprintGame.jsx frontend/src/answer/ArtikelSprintGame.jsx frontend/src/answer/AdjektivLearnGame.jsx frontend/src/answer/AnswerOverlay.jsx frontend/src/answer/SaveWordChip.jsx backend/tests/test_word_pick_copy.py
git commit -m "Одна подпись над чипами в пяти местах: словарь + завтра на повторение; обогащение только для нового слова"
```

---

### Task 8: Строка отчёта и два обещания

**Files:**
- Modify: `backend/database.py` (после `word_pick_day_stats`): `count_word_pick_door_misses()`, `count_word_pick_posters_missing()`
- Modify: `bot_3.py` — `_word_pick_report_line()` рядом с `_access_state_line` (16824), вызов после `text += _access_state_line()` (10266)
- Modify: `backend/fix_promises.py` — два измерителя и две записи в `PROMISES`
- Test: `backend/tests/test_word_pick_delivery.py`

**Interfaces:**
- Produces: `database.count_word_pick_door_misses() -> int` (вчерашние сохранения из интерактивов без записи отбора на сегодня; ожидание 0), `database.count_word_pick_posters_missing() -> int` (люди с отбором на вчера, не запертые/не в тишине/не заблокировавшие, у которых меньше двух строк `wp` за вчера; ожидание 0); ключи обещаний `word_pick_door_writes_every_tap`, `word_pick_two_posters_per_picker`.

- [ ] **Step 1: Write the failing tests** (добавить в `test_word_pick_delivery.py`)

```python
class ОбещанияИОтчёт(unittest.TestCase):

    def test_обещания_зарегистрированы(self):
        from backend import fix_promises
        ключи = {p.key for p in fix_promises.PROMISES}
        self.assertIn("word_pick_door_writes_every_tap", ключи)
        self.assertIn("word_pick_two_posters_per_picker", ключи)
        for p in fix_promises.PROMISES:
            if p.key.startswith("word_pick_"):
                self.assertEqual(p.expected, 0)
                self.assertTrue(p.how)

    def test_дверь_меряется_по_вчерашним_сохранениям_из_интерактивов(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (0,)
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            self.assertEqual(db.count_word_pick_door_misses(), 0)
        sql = курсор.execute.call_args.args[0]
        self.assertIn("LEFT JOIN bt_3_word_picks", sql)
        self.assertIn("p.id IS NULL", sql)
        for источник in db.WORD_PICK_ORIGINS:
            self.assertIn(источник, str(курсор.execute.call_args.args[1]))

    def test_строка_отчёта_есть_в_утреннем_отчёте(self):
        import pathlib
        bot = (pathlib.Path(__file__).resolve().parents[2] / "bot_3.py").read_text(encoding="utf-8")
        self.assertIn("text += _word_pick_report_line()", bot)
        self.assertIn("🔁 <b>Повтор слов</b>", bot)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py -q -k "ОбещанияИОтчёт"`
Expected: FAIL

- [ ] **Step 3: Измерители в `backend/database.py`** (после `word_pick_day_stats`)

```python
def count_word_pick_door_misses() -> int:
    """Обещание word_pick_door_writes_every_tap: вчерашние (по Вене) сохранения из
    интерактивов, у которых НЕТ записи отбора на сегодня. Ожидание 0. Сохранение метится
    updated_at/created_at; другие правки строки тоже двигают updated_at, поэтому ложный
    «пропуск» возможен — это исход «нарушено», который разбирают руками, а не глушат."""
    ensure_word_pick_schema()
    origins = sorted(WORD_PICK_ORIGINS)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_webapp_dictionary_queries q
                LEFT JOIN bt_3_word_picks p
                       ON p.user_id = q.user_id AND p.card_id = q.id
                      AND p.for_day = (NOW() AT TIME ZONE %s)::date
                WHERE q.origin_process = ANY(%s)
                  AND (COALESCE(q.updated_at, q.created_at) AT TIME ZONE %s)::date
                      = (NOW() AT TIME ZONE %s)::date - 1
                  AND p.id IS NULL;
                """,
                (_WORD_PICK_TZ, origins, _WORD_PICK_TZ, _WORD_PICK_TZ),
            )
            return int((cursor.fetchone() or [0])[0] or 0)


def count_word_pick_posters_missing() -> int:
    """Обещание word_pick_two_posters_per_picker: у каждого, кто вчера имел отбор и кому
    постер положен (не заперт, не «Тишина», не заблокировал бота), за вчера две строки
    `wp` в ведомости. Возвращает число людей, у кого их меньше двух."""
    ensure_word_pick_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT p.user_id, COUNT(i.id)
                FROM (SELECT DISTINCT user_id FROM bt_3_word_picks
                      WHERE for_day = (NOW() AT TIME ZONE %s)::date - 1) p
                LEFT JOIN bt_3_interactive_inbox i
                       ON i.user_id = p.user_id AND i.kind = 'wp'
                      AND i.dispatch_id / 10 = to_char((NOW() AT TIME ZONE %s)::date - 1, 'YYYYMMDD')::bigint
                GROUP BY p.user_id HAVING COUNT(i.id) < 2;
                """,
                (_WORD_PICK_TZ, _WORD_PICK_TZ),
            )
            short = [int(r[0]) for r in (cursor.fetchall() or [])]
    if not short:
        return 0
    blocked = set(list_bot_blocked_user_ids())
    prefs = get_user_prefs_bulk(tuple(short))
    missing = 0
    for uid in short:
        if uid in blocked:
            continue
        if str((prefs.get(uid) or {}).get("preset") or "") == "silent":
            continue
        if resolve_entitlement(uid)["access_state"] == "locked":
            continue
        missing += 1
    return missing
```

(Имена `list_bot_blocked_user_ids`, `get_user_prefs_bulk`, `resolve_entitlement` — функции этого же модуля; проверить точные имена `grep -n "^def list_bot_blocked_user_ids\|^def resolve_entitlement" backend/database.py`.)

- [ ] **Step 4: Обещания в `backend/fix_promises.py`**

Измерители (в раздел `# ── измерители`):

```python
def _word_pick_door_misses() -> int:
    """Вчерашних сохранений из интерактивов без отбора на сегодня. Обещано: 0.
    Дверь стоит внутри сохранения слова (05.09.2026); появится строка — дверь обошли
    (новый источник не в WORD_PICK_ORIGINS) или запись упала (exception в логе)."""
    from backend.database import count_word_pick_door_misses
    return count_word_pick_door_misses()


def _word_pick_posters_missing() -> int:
    """Людей, кому вчера был положен постер «Слова со вчерашних тренировок» дважды,
    а пришло меньше двух. Обещано: 0."""
    from backend.database import count_word_pick_posters_missing
    return count_word_pick_posters_missing()
```

Записи в `PROMISES` (в конец кортежа):

```python
    Promise(
        key="word_pick_door_writes_every_tap",
        title="Тапов по слову в интерактивах за вчера без отбора на сегодня",
        since="05.09.2026",
        expected=0,
        measure=_word_pick_door_misses,
        how="python3 -c \"from backend.database import count_word_pick_door_misses as f; print(f())\"",
    ),
    Promise(
        key="word_pick_two_posters_per_picker",
        title="Людей с отбором на вчера, кому пришло меньше двух постеров «Слова со вчерашних тренировок»",
        since="05.09.2026",
        expected=0,
        measure=_word_pick_posters_missing,
        how="python3 -c \"from backend.database import count_word_pick_posters_missing as f; print(f())\"",
    ),
```

- [ ] **Step 5: Строка отчёта в `bot_3.py`**

Рядом с `_access_state_line` (16824):

```python
def _word_pick_report_line() -> str:
    """Строка утреннего отчёта о «Словах со вчерашних тренировок» за вчера: кто получал,
    сколько открыли и оценили. Сомнительных случаев здесь не бывает — модели нет."""
    try:
        from backend.database import word_pick_day_stats
        d = (_get_quiz_schedule_now().date() - timedelta(days=1))
        s = word_pick_day_stats(d)
    except Exception:
        logging.exception("строка о повторе слов не собралась")
        return "\n🔁 Повтор слов: ❓ не посчитался, подробности в логах.\n"
    return (f"\n🔁 <b>Повтор слов</b> ({d:%d.%m}): отбирали <b>{s['pickers']}</b> чел. · "
            f"слов <b>{s['cards']}</b> · постеров утром <b>{s['posters_am']}</b> / вечером <b>{s['posters_pm']}</b> · "
            f"открыли <b>{s['opened']}</b> · оценили утром <b>{s['am_rated_users']}</b> / вечером <b>{s['pm_rated_users']}</b>\n")
```

(`timedelta` — проверить импорт: `grep -n "^from datetime import" bot_3.py`.)

В сборке отчёта (10266) после `text += _access_state_line()` добавить `text += _word_pick_report_line()`.

- [ ] **Step 6: Run tests**

Run: `python3 -m pytest backend/tests/test_word_pick_delivery.py backend/tests/test_fix_promises*.py -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add backend/database.py backend/fix_promises.py bot_3.py backend/tests/test_word_pick_delivery.py
git commit -m "Повтор слов: строка утреннего отчёта и два обещания в реестр"
```

---

### Task 9: Полный прогон, снимок «после», сдача

**Files:**
- Modify: `docs/tasks/word_pick_review_strategy.md` — раздел «Снимок „после“»

- [ ] **Step 1: Полный прогон тестов**

Run: `python3 -m pytest backend/tests -q -x 2>&1 | tail -5`
Expected: всё зелёное. Красное разбирать, не подгонять (CLAUDE.md §3.1). Помнить: тесты краснеют от ПАРАЛЛЕЛЬНОГО прогона другой сессии — сначала проверить, не идёт ли чужой прогон.

- [ ] **Step 2: Сборка фронта**

Run: `cd frontend && npm run build 2>&1 | tail -3`
Expected: без ошибок. `git status` не показывает `frontend/dist`.

- [ ] **Step 3: Пуш ветки**

```bash
./scripts/push.sh -u bot3_webapp agent/wordpick
```

- [ ] **Step 4: Слово владельца на слияние** — сообщить владельцу: ветка готова, тесты зелёные, что именно уйдёт в прод. Слияние и деплой — только после его «сливай»:

```bash
./agent-worktree.sh --merge wordpick
```

- [ ] **Step 5: Снимок «после» (после деплоя Railway)**

1. `/admin_wordpick_preview` в чате бота — постер приходит; если «нет отобранных слов», тапнуть слово в конце любой тренировки, дождаться следующего дня по Вене (или на тестовом аккаунте владельца) и повторить.
2. Открыть кнопку → первая карточка → оценить → финальный экран. Скриншоты присылает владелец; агент кладёт в отчёт буквальный текст экрана.
3. `/admin_promises` → две новые строки с исходом «держится» либо «не измерено» (первое утро: строк за вчера ещё нет — это «держится» с числом 0, не «не измерено»).
4. Строка «🔁 Повтор слов» в утреннем отчёте.
5. Замер после: `SELECT COUNT(*) FROM bt_3_word_picks` и число сохранений с `origin_process='interactive_save'` за сутки (раньше писались как unknown).

Вписать буквальный вывод в раздел «Снимок „после“» стратегии и закоммитить.

- [ ] **Step 6: Пять вопросов правила ноль — письменно в отчёте владельцу**

1. `except`, после которого выполнение продолжается: один, в двери отбора внутри сохранения слова — слово уже записано, ответ несёт `pick_for_day: null`, интерфейс показывает тост «на завтра не отобралось», exception в логе, обещание `word_pick_door_writes_every_tap` считает такие случаи утром. Остальные `except` в отправителе — `return` с exception в логе, рассылка не идёт.
2. `or`/`??`/`.get(x, default)` с содержательным значением: `prefs.get(uid) or {}` в отправителе — отсутствие настроек значит «пресет не задан», это устройство `get_user_prefs_bulk`; `String(res?.pick_for_day || '')` — пустая строка означает «не отобрано», отдельное состояние, не подмена.
3. Источник не знает ответа: нет отбора → постер не приходит; набор пуст → экран «done»; ссылка с плохим днём → 400; чужая карточка → 403.
4. Счётчик «не знаю»: два обещания в реестре, строка отчёта, `/admin_promises`.
5. Класс, не случай: дверь одна на пять мест, подпись одна, тест по исходникам держит все пять; `interactive_save` в allowlist закрыт тестом.

---

## Self-review

**Spec coverage.** §1 дверь → Task 2. §2 отбор/рассылка/сверх нормы → Tasks 3–4. §3 экран, оценки в FSRS, вечерний повтор без проверки срока → Tasks 5–6 (срок не проверяется тем, что набор отдаётся своим запросом; `bypass_due_at` из стратегии не понадобился — набор не идёт через `get_next_due_srs_card`). §4 подпись и чипы, обогащение только для нового → Task 7. §5 отчёт и обещания → Task 8. §6 тесты → по задачам. «Чего не делаем» соблюдено: потолка нет, модели нет, Space Rep не тронут.

**Отклонение от стратегии, записать в неё при сдаче:** `bypass_due_at` не используется; набор дня читается напрямую (`list_word_pick_cards`), что и даёт «вечером снова целиком».

**Type consistency.** `record_word_pick` → `{"for_day": date, "inserted": bool}` (Task 2) читается в Task 2 двери как `pick["for_day"].isoformat()`. `list_word_pick_cards` элементы `{"card","srs","am_rated_at","pm_rated_at","origin_process"}` (Task 3) читаются в Task 5 как `r["srs"]`, `r[f"{slot}_rated_at"]`. `day_id(day, slot)` (Task 1) используется в Task 4 и совпадает с формулой `day_no*10+1/2` в `word_pick_day_stats` (Task 3) и `dispatch_id / 10` в Task 8. `slot_now` возвращает `'am'|'pm'`, `mark_word_pick_rated` принимает ровно их. Ответ `/api/answer/wordpick/set` `{items:[{card, srs, srs_preview, rated}], slot, total}` совпадает с чтением в `WordPickGame`.
