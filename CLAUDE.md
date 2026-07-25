# Правила работы в этом репозитории (читают все агенты)

## 1. Сборка и деплой — НИКОГДА не коммить `frontend/dist`
- Railway собирает фронт сам: `Dockerfile.backend` (STAGE 1) выполняет `npm run build`
  из `frontend/` на каждом деплое, а `.dockerignore` исключает `frontend/dist/` из
  контекста сборки. Закоммиченный `dist` в прод НЕ попадает.
- **Правило:** коммить только исходники (`.jsx` / `.css` / `.py` / …).
  `npm run build` запускай ТОЛЬКО локально, чтобы убедиться, что компилится.
  Больше НИКАКИХ `git add -f frontend/dist`. `frontend/dist/` — в `.gitignore`.
- SPA раздаёт сервис **BACKEND_WEB** (Flask, `Dockerfile.backend`).

## 2. Несколько агентов = несколько worktree (не сидеть в одной папке!)
Главный источник поломок — когда несколько сессий редактируют ОДИН рабочий каталог
на ОДНОЙ ветке: чужой незакоммиченный WIP протекает в диффы, ребейз одного затирает
коммит другого, HEAD «прыгает» под руками.

**Каждый агент работает в своём git worktree на своей ветке.** Скрипт в корне репо:
```bash
./agent-worktree.sh <имя>          # → ../<repo>-<имя>, ветка agent/<имя> (node_modules симлинкнут)
./agent-worktree.sh --merge <имя>  # влить ветку агента в refactor/interface + запушить + убрать worktree
./agent-worktree.sh --list         # показать все worktree
./agent-worktree.sh --remove <имя> # убрать worktree без слияния
```
- Основной каталог держим «чистым» / для владельца; агентов — по отдельным worktree.
- Работай ТОЛЬКО в своём каталоге. Коммить маленько и часто на ветке `agent/<имя>`.
- Готово → закоммить всё в `agent/<имя>`, затем приёмка работы одной командой:
  `./agent-worktree.sh --merge <имя>` (сливает в `refactor/interface`, пушит, убирает worktree).
  Либо, если нужен формальный ревью, — `git push -u bot3_webapp agent/<имя>` и PR на GitHub.

## 3. Git-гигиена (общий репозиторий)
- Рабочая ветка — `refactor/interface`. **Никогда не коммить/пушить в `main`.**
- Пуш-remote — **`bot3_webapp`** (не `origin`): `git push bot3_webapp <ветка>`.
- Стейджь только СВОИ хунки (`git add -p` / точечно), никогда `git add .` и никогда
  весь файл целиком, если в нём может быть чужой WIP.
- Перед началом — `git fetch bot3_webapp && git pull --rebase` (в своём worktree).
- Railway автодеплоит при пуше в `refactor/interface` (сервисы из репо `MY_3_BOT_Web_App`).
  Инфра (PgBouncer/Redis/Postgres) — отдельные сервисы/репозитории, их не трогаем.

## 4. Частые задачи — где что лежит
(Ориентир, а не догма — перед правкой проверяй актуальность grep’ом.)

**Структура кода**
- Фронт (React SPA): `frontend/src/App.jsx` — основной огромный файл; компоненты в
  `frontend/src/components/` и игры в `frontend/src/answer/`.
- Бэкенд (Flask API): `backend/backend_server.py` — огромный, все эндпоинты;
  `backend/database.py` — слой БД; `backend/admin_command_catalog.py` — админ-команды (~115).

**Проверка изменений**
- Фронт компилится: `cd frontend && npm run build` (ТОЛЬКО локально, dist не коммитим).
- Прод: пуш в `refactor/interface` → Railway пересоберёт и задеплоит.
- Состояние деплоя/сервисов: `railway status --json` (проект MY_THIRD_BOT / production).

**Читалка (Reader)**
- `frontend/src/components/ReaderSection.jsx` + `reader-redesign.css`.
- Paywall = зелёная плашка `.reader-upsell` (крона/иконка/CTA), НЕ красный `webapp-error`.
- «Классика» (public-книги): чтение И озвучка — БЕСПЛАТНО для всех (бэкенд
  `/reader/audio/page` отдаёт public бесплатно). Свои книги: аудио платное (per-book Stars).
- Веб-статьи free: 1/день; тип источника для гейта — `"url"` (не `"html"`).

**Словарь**
- `bt_3_dictionary_entries` — ОБЩИЙ пул слов (кеш→пул→GPT), не личный per-user.
- Не путать общий кеш (экономия GPT) с личным пулом тренировки.

**Тарифы / paywall / гейтинг**
- Free + «Полный доступ» (€5, оплата Telegram Stars; Stripe отключён).
- Paywall показывай через `ProFeatureModal` или амбер `paid-feature-card` (App.css) —
  НИКОГДА через красный `webapp-error`.
- Апгрейд из кода: `handleBillingUpgrade('pro')` (передавай код тарифа, не event!).

**Тексты для пользователя**
- Никаких сырых ошибок сервера/БД в UI — чистое человеческое сообщение, техтекст в консоль.
- Копирайт newcomer: простыми словами что/где/как/зачем, без инсайдерского сленга.

**База в проде**
- Живая база — `zephyr` через PgBouncer. Запросы — через `railway` CLI.
- НЕ использовать `DATABASE_URL_RAILWAY` (указывает на старую мёртвую `centerbeam`).
