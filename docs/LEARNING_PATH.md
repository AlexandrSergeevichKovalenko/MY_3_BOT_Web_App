# 🎓 LEARNING PATH — как изучить весь наш код с нуля до профи

> Это **главная точка входа** в обучение по нашему проекту. Здесь: методика (как вообще
> читать большой код, когда ты новичок), **строгий порядок** «что читать и зачем», кликабельное
> меню всех документов и кода, и план углублённых разборов.
>
> **Образец стиля**, к которому мы приводим все объяснения, — [autosave_scaling_explained.md](autosave_scaling_explained.md):
> от азов, с объяснением каждой технологии простыми словами, со ссылками `файл:строка` и
> кусками кода прямо в тексте. По мере работы остальные области будем доводить до этого уровня.

---

## 🧭 Как учиться по этому файлу — ЧИТАЙ ЭТО ПЕРВЫМ

**Главное правило: этот файл — это ПЛАН КУРСА (оглавление), а НЕ книга.** Его не нужно читать
подряд от начала до конца. Ты работаешь по нему циклом, как по плейлисту:

1. Смотришь в **пронумерованный «Маршрут»** ниже (👇 таблица «Порядок обучения»).
2. Берёшь пункт №1 → **открываешь тот файл по ссылке** → читаешь его **до конца** → делаешь в конце
   его **вопросы для самопроверки**.
3. **Возвращаешься сюда** → берёшь пункт №2 → открываешь → читаешь до конца → снова назад.
4. И так по порядку, один пункт за раз. Один пункт = один подход (не пытайся за день пройти всё).

**Про ссылки ВНУТРИ уроков.** Когда читаешь урок и видишь ссылку на термин (например на глоссарий) —
это **сноска**, а не поворот маршрута. Незнакомое слово → быстро глянул по ссылке и вернулся.
Знакомое → пропустил. Твой настоящий маршрут — **только пронумерованный список ниже**.

**Про Части A–F этого файла.** Всё, что идёт ниже маршрута (Часть A «методика», B «карта», C
«уровни», D «таблица», E, F) — это **справочные разделы**. Их НЕ нужно читать подряд. Заходишь в них
точечно, когда маршрут туда отправит или когда захочешь свериться. Не читай их «просто потому что
они идут дальше по файлу».

### 📋 Маршрут (иди строго по номерам, сверху вниз)

| № | Открой и прочитай до конца | Что это даст | Этап |
| --- | --- | --- | --- |
| **1** | [toolbox/stack_explained.md](toolbox/stack_explained.md) | Что вообще означают слова: JS, JSX, Node, Vite, React, Flask, Postgres, Redis, Docker… | Слова |
| **2** | [toolbox/cli_commands.md](toolbox/cli_commands.md) | Как искать по коду в терминале (`grep` и др.) — понадобится сразу | Слова |
| **3** | [security_deep_dives/00a_frontend_foundations.md](security_deep_dives/00a_frontend_foundations.md) | Синтаксис JS/JSX: что такое функция, типы данных, хуки, `fetch` | Язык |
| **4** | [security_deep_dives/00b_http_and_backend_foundations.md](security_deep_dives/00b_http_and_backend_foundations.md) | Что такое HTTP-запрос, GET vs POST, статусы, как Flask принимает запрос | Язык |
| **5** | [security_deep_dives/01_sentence_translation.md](security_deep_dives/01_sentence_translation.md) | **Первый настоящий блок кода**: перевод предложений, разобранный построчно + кибербезопасность | Код |
| **6** | [toolbox/running_and_testing.md](toolbox/running_and_testing.md) | Как запустить фронт/бэк и **как пользоваться тестами** (тест = живой пример вызова) | Практика |
| **7** | [toolbox/backend_interaction.md](toolbox/backend_interaction.md) | `curl` к нашим эндпоинтам, деплой, чтение логов | Практика |
| **8** | [autosave_scaling_explained.md](autosave_scaling_explained.md) | **Большой** учебник по инфраструктуре/масштабированию. Тяжёлый — читай, уже пройдя 1–7 | Каркас |
| **9** | [Backend Study Map](../backend/README.md) → [Frontend Study Map](../frontend/README.md) | Карты всего проекта: какой файл = какой процесс | Карта |
| **10+** | Следующие блоки безопасности (02 Аутентификация, 03 Оплата, …) — по мере готовности, см. [индекс](security_deep_dives/00_INDEX.md) | Остальные фичи, блок за блоком | Код |

После пункта 9 у тебя есть фундамент. Дальше — бери следующий блок из
[security_deep_dives/00_INDEX.md](security_deep_dives/00_INDEX.md) (они добавляются по одному) и
проходи так же: открыл → прочитал до конца → самопроверка → назад.

> Не уверен, что понял пункт? Не иди дальше. Перечитай его или напиши мне — доработаю объяснение.
> Пропускать пункты нельзя: каждый следующий опирается на предыдущий.

---

## Оглавление

- [Часть A. Как учить большой код (методика для новичка)](#часть-a-методика)
- [Часть B. Карта репозитория за 5 минут](#часть-b-карта-репозитория)
- [Часть C. Учебный маршрут по уровням (0 → 5)](#часть-c-маршрут-по-уровням)
  - [Уровень 0 — Базовые технологии и понятия](#уровень-0--базовые-технологии)
  - [Уровень 1 — Общая карта проекта](#уровень-1--общая-карта)
  - [Уровень 2 — Точка входа и путь запроса](#уровень-2--точка-входа-и-путь-запроса)
  - [Уровень 3 — Слой данных: БД, пул, Redis](#уровень-3--слой-данных)
  - [Уровень 4 — Очереди, воркеры, планировщик](#уровень-4--очереди-воркеры-планировщик)
  - [Уровень 5 — Фичи: карточки, TTS, голос, ридер, фронтенд, iOS](#уровень-5--фичи)
- [Часть D. Полная таблица документов (что / когда читать)](#часть-d-все-документы)
- [Часть E. Как проходить сам код по порядку](#часть-e-как-проходить-код)
- [Часть F. План углублённых разборов (что напишем дальше)](#часть-f-план-разборов)

---

<a name="часть-a-методика"></a>
## Часть A. Как учить большой код (методика для новичка)

Наш код большой: `bot_3.py` ~33k строк, `backend/backend_server.py` ~56k строк,
`backend/database.py` ~41k строк. **Читать подряд сверху вниз — худший способ.** Так делать не
нужно. Вот как делать правильно.

1. **Сверху вниз по абстракции, а не по строкам.** Сначала пойми «какие есть отдельные программы
   (сервисы) и кто с кем говорит», потом «какие модули внутри», и только потом конкретные
   функции. Этот документ ведёт тебя именно так.

2. **Иди по одному пути исполнения (trace), а не по файлу.** Выбери одно действие пользователя
   («нажал кнопку перевода») и проследи его от входа до ответа, прыгая по функциям. Один такой
   сквозной проход даёт больше, чем 10 страниц чтения подряд.

3. **Технология → зачем → где в коде.** Встретил незнакомое слово (Redis, очередь, пул)? Сначала
   пойми *зачем оно нужно* (какую боль решает), потом найди *где* мы его применяем. Глоссарий и
   объяснения всех наших технологий — в [autosave_scaling_explained.md](autosave_scaling_explained.md#7-glossary).

4. **`grep` — твой главный инструмент.** Не помнишь, где определена функция? `grep -n "def имя"`.
   Где вызывается? `grep -rn "имя("`. Так ты «ходишь» по коду быстрее, чем читая. **Не знаешь, что
   такое `grep`, где его вводить и какой у него синтаксис? → [toolbox/cli_commands.md](toolbox/cli_commands.md)**
   — там `grep`, `rg`, `find`, `less`, пайпы `|` с примерами прямо на нашем репо.

5. **Проверяй гипотезы запуском.** У нас есть тесты (`backend/tests/`). Прочитал функцию —
   посмотри её тест: он показывает, как её вызывают и что она должна вернуть. Тест = живой пример.
   **Как запускать тесты, фронт (Vite) и бэк, и как читать тест как пример вызова →
   [toolbox/running_and_testing.md](toolbox/running_and_testing.md).**

6. **Делай заметки своими словами.** После каждого раздела запиши в 2–3 предложениях «что делает
   эта часть и зачем». Если не можешь — значит ещё не понял, вернись.

> Практическое правило: **«один день — одна область»**. Не пытайся охватить всё сразу.
> Маршрут ниже специально разбит так, чтобы каждый уровень был самодостаточным.

---

<a name="часть-b-карта-репозитория"></a>
## Часть B. Карта репозитория за 5 минут

Проект — это **не одна программа**, а несколько отдельных процессов («сервисов»), плюс фронтенд и
мобильная часть. Полные карты — в Study-Map файлах (ссылки ниже), здесь — самый верх.

| Слой | Где в репо | Что это |
| --- | --- | --- |
| **Telegram-бот** | [`bot_3.py`](../bot_3.py) | Логика бота в чате: команды, кнопки, сообщения |
| **Веб-бэкенд** | [`backend/web_service.py`](../backend/web_service.py) → [`backend/backend_server.py`](../backend/backend_server.py) | Flask-сервер: HTTP-роуты для фронтенда, iOS, Shortcut |
| **Воркеры** | [`backend/background_jobs.py`](../backend/background_jobs.py) | Фоновые задачи (Dramatiq-акторы): тяжёлая работа |
| **Планировщик** | [`backend/scheduler_service.py`](../backend/scheduler_service.py) | Периодические «тики» (как cron) |
| **Слой БД** | [`backend/database.py`](../backend/database.py) | Доступ к Postgres, пул соединений, схема |
| **Очереди** | [`backend/job_queue.py`](../backend/job_queue.py) | Мост: web/scheduler → очередь → воркер |
| **Фронтенд (Mini App)** | [`frontend/src/`](../frontend/src/) | React-приложение внутри Telegram |
| **iOS / Share Extension** | [`ios/`](../ios/) | Нативная часть для iPhone |

Эти процессы крутятся на **Railway** (хостинг) и общаются через **Postgres** (постоянное
хранилище, через **PgBouncer**) и **Redis** (быстрая память + очереди). Что означает каждое из
этих слов — см. [глоссарий](autosave_scaling_explained.md#8-glossary).

**Готовые карты-справочники** (прочитать после этого файла):
- 🗺️ [Backend Study Map](../backend/README.md) — все backend-процессы, модули, что за что отвечает.
- 🗺️ [Frontend Study Map](../frontend/README.md) — структура React-приложения.
- 🗺️ [Scripts Map](../scripts/README.md) — вспомогательные скрипты.
- 🗺️ [Docs Index](README.md) — индекс точечных deep-dive заметок.
- 🗺️ [Корневой README](../README.md) — обзор всего репозитория.

🧰 **Toolbox — практические навыки (что такое команды, тесты, деплой, весь стек):** если ты споткнулся
о незнакомое слово-инструмент (`grep`, `curl`, `Vite`, `Node`, `Flask`, `Redis`…), тебе сюда. Это
справочники «с нуля», с синтаксисом и примерами на нашем репо:
- 🧰 [toolbox/cli_commands.md](toolbox/cli_commands.md) — команды терминала для чтения/поиска кода.
- 🧰 [toolbox/running_and_testing.md](toolbox/running_and_testing.md) — как запускать фронт/бэк и тесты.
- 🧰 [toolbox/backend_interaction.md](toolbox/backend_interaction.md) — `curl` к нашим эндпоинтам, деплой, логи, БД.
- 🧰 [toolbox/stack_explained.md](toolbox/stack_explained.md) — **глоссарий всех технологий**: JS, JSX, Node, npm, Vite, React, Flask, gunicorn, Postgres, Redis, Dramatiq, Docker, Railway… — что это, зачем, где у нас, ссылки.

🛡️ **Security Deep-Dives — разбор кода по блокам с упором на безопасность:**
[security_deep_dives/00_INDEX.md](security_deep_dives/00_INDEX.md) (фундамент [00a JS/JSX](security_deep_dives/00a_frontend_foundations.md)
+ [00b HTTP/Flask](security_deep_dives/00b_http_and_backend_foundations.md), затем блок
[01 — Перевод предложений](security_deep_dives/01_sentence_translation.md)).

---

<a name="часть-c-маршрут-по-уровням"></a>
## Часть C. Учебный маршрут по уровням

Проходи по порядку. Каждый уровень: **Цель → Что читать → Что попрактиковать.**

<a name="уровень-0--базовые-технологии"></a>
### Уровень 0 — Базовые технологии и понятия

**Цель:** понять словарь, на котором написана вся остальная документация (сервис, процесс,
поток/thread, критический путь, латентность vs throughput, соединение к БД, пул, PgBouncer, кэш,
TTL, Redis, очередь/брокер/воркер, Dramatiq, планировщик, debounce, lock, идемпотентность,
горизонтальное масштабирование).

**Что читать:**
0. 🧰 [toolbox/stack_explained.md](toolbox/stack_explained.md) — **сначала это**, если слова JS, JSX,
   Node, Vite, Flask, Postgres, Redis, Docker, Railway для тебя пустые. Глоссарий всех наших
   технологий: что это, зачем, где у нас в коде. Плюс [toolbox/cli_commands.md](toolbox/cli_commands.md)
   — чтобы уметь искать по коду (`grep` и др.).
1. [autosave_scaling_explained.md](autosave_scaling_explained.md) — **полностью**. Это не только
   про авто-сейв: это наш учебник по всем базовым технологиям с примерами из реального кода.
   Особое внимание — разделам [«Building blocks»](autosave_scaling_explained.md#3-building-blocks),
   **[«Чистота и корректность: борьба с мусором и дублями»](autosave_scaling_explained.md#7-clean-and-correct)**
   (3 рубежа фильтрации, NX-lock / exactly-once, at-least-once durability, чанкинг) и
   [глоссарию](autosave_scaling_explained.md#8-glossary).

**Что попрактиковать:** возьми из глоссария 5 терминов и объясни их вслух своими словами. Не
получилось — перечитай соответствующий раздел.

<a name="уровень-1--общая-карта"></a>
### Уровень 1 — Общая карта проекта

**Цель:** видеть лес целиком — какие сервисы есть и зачем их разделили.

**Что читать:**
1. [Корневой README](../README.md) — обзор.
2. [Backend Study Map](../backend/README.md), разделы «Backend Boundaries» и «Backend Entry Points»
   — список процессов и их точек входа.
3. [Часть B](#часть-b-карта-репозитория) этого файла (уже прочитал).

**Что попрактиковать:** нарисуй от руки 5 квадратов (web, bot, worker, scheduler, db) и стрелки
«кто кому шлёт». Сверь с диаграммой в [autosave_scaling_explained.md §4](autosave_scaling_explained.md#4-the-full-pipeline).

<a name="уровень-2--точка-входа-и-путь-запроса"></a>
### Уровень 2 — Точка входа и путь запроса

**Цель:** проследить один запрос от начала до конца (то самое «trace» из методики).

**Что читать / трейсить:**
1. **Бот:** [`bot_3.py`](../bot_3.py). Найди, где регистрируются обработчики:
   `grep -n "add_handler" bot_3.py`. Возьми один — например кнопку, — и пройди по её функции.
2. **Веб:** [`backend/web_service.py`](../backend/web_service.py) → [`backend/backend_server.py`](../backend/backend_server.py).
   Найди роут: `grep -n "@app.route" backend/backend_server.py | head`.
3. **Готовый сквозной пример** (бот → backend → Redis → scheduler → worker → отправка): весь
   [autosave_scaling_explained.md §4](autosave_scaling_explained.md#4-the-full-pipeline). Это
   эталонный «трейс», на нём учись читать остальные пути.

**Что попрактиковать:** выбери ЛЮБУЮ кнопку бота и пройди её путь до БД/ответа, выписывая имена
функций по порядку.

<a name="уровень-3--слой-данных"></a>
### Уровень 3 — Слой данных: БД, пул, Redis

**Цель:** понять, как мы храним и читаем данные дёшево (это сердце масштабирования).

**Что читать:**
1. Почему соединение к БД дорого, что такое пул и **PgBouncer** —
   [autosave_scaling_explained.md §3.2](autosave_scaling_explained.md#32-database-connections-pooling-pgbouncer).
2. Реальная функция доступа: [`get_db_connection_context`](../backend/database.py) —
   `backend/database.py:4203`. Посмотри, как её используют:
   `grep -n "with get_db_connection_context" backend/database.py | head`.
3. **Redis** (зачем он, какие структуры) — [autosave_scaling_explained.md §3.4](autosave_scaling_explained.md#34-redis).
4. **Кэш и TTL** на живом примере тумблера — [autosave_scaling_explained.md §3.3](autosave_scaling_explained.md#33-cache--ttl)
   и код [`_autosave_toggle_cached`](../backend/backend_server.py) `backend/backend_server.py:39551`.

**Что попрактиковать:** найди в `database.py` любую функцию сохранения (`grep -n "def save_" backend/database.py`)
и проследи, как она берёт соединение из пула, делает запрос, возвращает результат.

<a name="уровень-4--очереди-воркеры-планировщик"></a>
### Уровень 4 — Очереди, воркеры, планировщик

**Цель:** понять, как тяжёлую работу уносят с «горячего» пути в фон (главный приём масштабирования).

**Что читать:**
1. Очереди/брокер/воркер/Dramatiq — [autosave_scaling_explained.md §3.5](autosave_scaling_explained.md#35-queues-brokers-workers-dramatiq).
2. Планировщик и «sweep» — [autosave_scaling_explained.md §3.6](autosave_scaling_explained.md#36-the-scheduler--the-sweep).
3. Код: мост в очередь [`backend/job_queue.py`](../backend/job_queue.py), акторы
   [`backend/background_jobs.py`](../backend/background_jobs.py) (наши примеры:
   `run_autosave_sweep_job` `backend/background_jobs.py:1311`, `run_autosave_flush_job`
   `backend/background_jobs.py:1332`), планировщик [`backend/scheduler_service.py`](../backend/scheduler_service.py).
4. Локи/гонки/идемпотентность/«exactly-once» — [autosave_scaling_explained.md §3.8](autosave_scaling_explained.md#38-locks-races-idempotency).

**Что попрактиковать:** найди другой актор (`grep -n "@dramatiq.actor" backend/background_jobs.py`)
и определи: на какой очереди он висит и кто его ставит (`.send()`).

<a name="уровень-5--фичи"></a>
### Уровень 5 — Фичи (по интересу, после уровней 0–4)

Теперь, понимая каркас, изучай конкретные подсистемы. Порядок — любой, по необходимости. У каждой
есть свой deep-dive документ:

- **Карточки / интервальное повторение (FSRS):** [docs/FSRS.md](FSRS.md) → роуты `/api/cards/*` в `backend_server.py`.
- **Озвучка (TTS):** [docs/tts_generation_audit.md](tts_generation_audit.md) → [`backend/tts_generation.py`](../backend/tts_generation.py) → акторы в `background_jobs.py`.
- **Голос / звонки (LiveKit):** [docs/voice_architecture.md](voice_architecture.md) (+ [migration_plan](voice_migration_plan.md), [schema_draft](voice_schema_draft.md)) → [`backend/agent.py`](../backend/agent.py).
- **Ридер (чтение текстов):** [backend/READER_INTEGRATION.md](../backend/READER_INTEGRATION.md) → [`frontend/src/components/ReaderSection.jsx`](../frontend/src/components/ReaderSection.jsx).
- **Shortcut / авто-сейв слов:** [docs/shortcut_onboarding.md](shortcut_onboarding.md) + [autosave_scaling_explained.md](autosave_scaling_explained.md).
- **iOS Share Extension:** [docs/ios_share_extension_backend.md](ios_share_extension_backend.md) + [ios/ShareExtensionTemplate/README.md](../ios/ShareExtensionTemplate/README.md).
- **Фронтенд (Mini App):** [Frontend Study Map](../frontend/README.md) → [`frontend/src/App.jsx`](../frontend/src/App.jsx).
- **Архитектурные аудиты (для «профи»-уровня, как мы думаем о разбиении):**
  [docs/miniapp_decomposition_audit/](miniapp_decomposition_audit/00_EXECUTIVE_SUMMARY.md),
  [docs/home_screen_refactor_audit/](home_screen_refactor_audit/00_EXECUTIVE_SUMMARY.md).

---

<a name="часть-d-все-документы"></a>
## Часть D. Полная таблица документов (что / когда читать)

| Документ | Что объясняет | Когда читать |
| --- | --- | --- |
| 🧰 [toolbox/stack_explained.md](toolbox/stack_explained.md) | Глоссарий всех технологий (JS/JSX/Node/Vite/Flask/Postgres/Redis/Docker/Railway…) | **Уровень 0**, при любом незнакомом термине |
| 🧰 [toolbox/cli_commands.md](toolbox/cli_commands.md) | Команды терминала: `grep`, `rg`, `find`, `less`, пайпы | когда ищешь по коду |
| 🧰 [toolbox/running_and_testing.md](toolbox/running_and_testing.md) | Запуск фронта/бэка + как пользоваться тестами | Уровень 0–2 |
| 🧰 [toolbox/backend_interaction.md](toolbox/backend_interaction.md) | `curl` к эндпоинтам, деплой (git push→Railway), логи, БД | когда трогаешь бэкенд/деплой |
| 🛡️ [security_deep_dives/00_INDEX.md](security_deep_dives/00_INDEX.md) | Разбор кода по блокам + кибербезопасность (00a JS/JSX, 00b HTTP, 01 Перевод…) | после Уровня 0–2 |
| [autosave_scaling_explained.md](autosave_scaling_explained.md) | **Эталон.** Все базовые технологии + масштабирование на живом примере | **Первым.** Уровень 0 |
| [../README.md](../README.md) | Обзор всего репозитория | Уровень 1 |
| [../backend/README.md](../backend/README.md) | Backend Study Map: процессы, модули, точки входа | Уровень 1 |
| [../frontend/README.md](../frontend/README.md) | Frontend Study Map: структура React-приложения | Уровень 1 / 5 (фронт) |
| [../scripts/README.md](../scripts/README.md) | Карта вспомогательных скриптов | по необходимости |
| [README.md](README.md) | Индекс папки `docs/` | как справочник |
| [FSRS.md](FSRS.md) | Интервальное повторение: таблицы, API | при изучении карточек |
| [tts_generation_audit.md](tts_generation_audit.md) | Подсистема TTS: границы, граф вызовов | при изучении озвучки |
| [voice_architecture.md](voice_architecture.md) | Архитектура голосового домена | при изучении звонков |
| [voice_migration_plan.md](voice_migration_plan.md) | План миграции voice (черновик) | сравнить замысел vs реализацию |
| [voice_schema_draft.md](voice_schema_draft.md) | Черновик схемы voice | планируемая модель данных |
| [../backend/READER_INTEGRATION.md](../backend/READER_INTEGRATION.md) | Интеграция Reader Redesign | при изучении ридера |
| [shortcut_onboarding.md](shortcut_onboarding.md) | Онбординг Shortcut | при изучении Shortcut |
| [ios_share_extension_backend.md](ios_share_extension_backend.md) | Backend-эндпоинты для iOS | при изучении iOS |
| [../ios/ShareExtensionTemplate/README.md](../ios/ShareExtensionTemplate/README.md) | Swift-шаблон расширения | при изучении iOS |
| [miniapp_decomposition_audit/](miniapp_decomposition_audit/00_EXECUTIVE_SUMMARY.md) | Аудит разбиения на mini-apps | продвинутый уровень |
| [home_screen_refactor_audit/](home_screen_refactor_audit/00_EXECUTIVE_SUMMARY.md) | Аудит рефактора главного экрана | продвинутый уровень |
| [../frontend/src/styles/design-tokens.md](../frontend/src/styles/design-tokens.md) | Дизайн-токены / стили | при работе с UI |

---

<a name="часть-е-как-проходить-код"></a>
## Часть E. Как проходить сам код по порядку

Конкретный пошаговый проход, когда хочешь «прочитать наш код целиком»:

1. **Сервисы и точки входа** — [Backend Study Map → Entry Points](../backend/README.md). Запомни,
   какой файл = какой процесс.
2. **Один сквозной трейс** — пройди [autosave_scaling_explained.md §4](autosave_scaling_explained.md#4-the-full-pipeline)
   с открытым кодом рядом, прыгая по ссылкам `файл:строка`. Это калибрует «как у нас всё связано».
3. **Бот** — [`bot_3.py`](../bot_3.py): начни с регистрации хендлеров (`grep -n "add_handler" bot_3.py`),
   разбери 3–4 обработчика, которыми сам пользуешься.
4. **Веб-роуты** — [`backend/backend_server.py`](../backend/backend_server.py): `grep -n "@app.route" …`,
   разбери 3–4 роута, которые дёргает фронтенд/Shortcut.
5. **Слой данных** — [`backend/database.py`](../backend/database.py): пул, пара функций save/get.
6. **Очереди и воркеры** — [`job_queue.py`](../backend/job_queue.py) + [`background_jobs.py`](../backend/background_jobs.py):
   как задача попадает в очередь и кто её исполняет.
7. **Планировщик** — [`scheduler_service.py`](../backend/scheduler_service.py): какие периодические
   задачи существуют.
8. **Фронтенд** — [`frontend/src/App.jsx`](../frontend/src/App.jsx) + [Frontend Study Map](../frontend/README.md).
9. **Фичи по интересу** — Уровень 5 выше.

На каждом шаге держи рядом `grep` и тесты из [`backend/tests/`](../backend/tests/) — они показывают
реальные вызовы.

---

<a name="часть-f-план-разборов"></a>
## Часть F. План углублённых разборов (что напишем дальше)

[autosave_scaling_explained.md](autosave_scaling_explained.md) — первый док в «эталонном» стиле
(от азов, со ссылками и кодом).

> 🛡️ **Новая серия: [Security Deep-Dives](security_deep_dives/00_INDEX.md)** — разбор кода **по
> одному логическому блоку** с сильным упором на кибербезопасность (по схеме: что под капотом →
> киберугрозы → текущая защита → рекомендации), плюс педагогика (интуиция → разбор → диаграмма →
> скелет → комментированный код → вопросы). Пишется на английском, простым языком. Готово:
> **[01 — Перевод предложений](security_deep_dives/01_sentence_translation.md)**. Дальше: 02 Auth
> (`initData`), 03 Оплата (Stripe/Stars), 04 Словарь + Prompt Injection, …

Дальше будем доводить до этого уровня остальные области, по одной
за раз, в таком порядке (от каркаса к фичам):

1. **`architecture_overview_explained.md`** — все сервисы, как они общаются, как деплоятся (Railway,
   Docker-файлы), что где запускается. *(каркас — после него всё остальное проще)*
2. **`bot_request_flow_explained.md`** — путь сообщения/кнопки в боте от Telegram до ответа.
3. **`data_layer_explained.md`** — `database.py`, схема, пул, PgBouncer, транзакции — глубоко.
4. **`web_backend_explained.md`** — Flask-роуты, auth, sync/async-решения, где тяжёлое уносится в очередь.
5. Затем фичи: **FSRS/карточки**, **TTS**, **voice**, **reader**, **frontend Mini App** — каждая
   как отдельный код-связанный разбор.

> Эти файлы ещё **не написаны** — это дорожная карта. Скажи, с какой области начать (рекомендую
> сверху, с обзора архитектуры), и я сделаю её в том же подробном стиле, что и авто-сейв.

---

*Как пользоваться этим файлом:* читай **сверху вниз** один раз (Части A → B → C), потом возвращайся
к [Части D](#часть-d-все-документы) и [Части E](#часть-е-как-проходить-код) как к навигатору. Всё,
что подчёркнуто синим, — кликабельно: документы открываются здесь же, ссылки `файл:строка` ведут
прямо в код в твоём редакторе.

---

> 🔄 **Последняя сверка учебных .md с кодом: 2026-07-01.** Тогда все `файл:строка`, счётчики строк
> и имена функций/таблиц во всех учебных доках были приведены к актуальному коду. Если правишь код
> и трогаешь эти ссылки — обнови дату здесь.
