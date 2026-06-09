# Docs Index

> 🎓 **Новичок и хочешь изучить весь код с нуля до профи?** Начни с
> **[LEARNING_PATH.md](LEARNING_PATH.md)** — главный учебный маршрут: методика, строгий порядок
> чтения, кликабельное меню и план разборов. Индекс ниже — справочник по отдельным докам.

Этот README — индекс существующих документов в `docs/`. Папка `docs/` не заменяет корневой [README.md](../README.md): здесь лежат точечные deep-dive заметки, audits и проектные черновики.

## 1. How To Use This Folder

Читать документы из этой папки лучше после общего обзора репозитория:

1. сначала [../README.md](../README.md)
2. потом профильный документ из `docs/`

Причина простая: часть файлов здесь описывает текущую реализацию, а часть — только проектные drafts или extraction plans.

## 2. Document Map

| File | What it covers | Status | When to read |
| --- | --- | --- | --- |
| `FSRS.md` | FSRS spaced repetition: tables, API, quick checks | operational note | когда изучаете cards / `/api/cards/*` |
| `ios_share_extension_backend.md` | backend mobile endpoints for iOS share extension | narrow backend note | когда изучаете mobile/iOS integration; читать вместе с `ios/ShareExtensionTemplate/README.md` |
| `tts_generation_audit.md` | boundaries and call graph of TTS generation subsystem | audit / extraction support doc | когда изучаете TTS, prewarm, recovery, scheduler interactions |
| `voice_architecture.md` | responsibility boundaries of the voice domain | architecture note | когда изучаете LiveKit/voice subsystem |
| `voice_migration_plan.md` | proposed migration order for voice persistence | draft, not executable SQL | когда сравниваете intent vs current implementation |
| `voice_schema_draft.md` | proposed minimal voice schema | schema draft, not authoritative current schema | когда хотите понять planned voice data model |

## 3. Reading Order By Topic

### Cards / FSRS

Read:

1. `FSRS.md`
2. then backend routes in `backend/backend_server.py`
3. then `backend/srs/fsrs_scheduler.py`

### iOS share extension

Read:

1. `ios_share_extension_backend.md`
2. `../ios/ShareExtensionTemplate/README.md`
3. mobile endpoints in `backend/backend_server.py`

Important:

- `ios_share_extension_backend.md` documents the core mobile endpoint set.
- It does not serve as the full iOS app map.
- For the actual Swift template and `GET /api/mobile/dashboard`, use the iOS template README.

### TTS

Read:

1. `tts_generation_audit.md`
2. `backend/tts_generation.py`
3. `backend/background_jobs.py`
4. TTS routes in `backend/backend_server.py`

### Voice

Read:

1. `voice_architecture.md`
2. `voice_migration_plan.md`
3. `voice_schema_draft.md`
4. `backend/agent.py`
5. `backend/voice_*_service.py`

Important:

- `voice_architecture.md` is about boundaries and responsibilities.
- `voice_migration_plan.md` and `voice_schema_draft.md` are drafts.
- The authoritative current runtime still has to be checked in code.

## 4. What Is Current vs Draft

### Closest to current implementation

- `FSRS.md`
- `tts_generation_audit.md`
- `voice_architecture.md`

### Narrow or partial

- `ios_share_extension_backend.md`

### Draft / planning docs

- `voice_migration_plan.md`
- `voice_schema_draft.md`

## 5. Gaps This Folder Does Not Cover

This folder currently does not provide:

- a full backend route index
- a full frontend screen/state index
- a full jobs/queues map
- a full deployment/Railway topology map

For those, use:

- [../README.md](../README.md)
- [../backend/README.md](../backend/README.md)
- [../frontend/README.md](../frontend/README.md)

## 6. Study Advice

Если документ в `docs/` выглядит как “audit”, “migration plan” или “schema draft”, не принимайте его как единственный источник правды. Сначала сверяйте его с кодом, особенно если речь идёт о voice, TTS, scheduler или deployment behavior.
