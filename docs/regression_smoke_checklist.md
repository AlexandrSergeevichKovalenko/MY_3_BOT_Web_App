# Regression Smoke Checklist

Run before every deploy.  If any item fails, stop and fix before pushing.

---

## 1. Automated tests (required)

```bash
# Frontend — 144+ unit tests, ~1.4s
cd frontend && npm test

# Backend — shortcut pipeline + idempotency
python -m pytest backend/tests/test_shortcut_ingest_idempotency.py \
                  backend/tests/test_shortcut_ingest_recovery.py \
                  backend/tests/test_shortcut_lookup_split.py \
                  backend/tests/test_shortcut_payload_persistence.py \
                  backend/tests/test_regression_smoke.py -v

# Backend — OCR and extraction
python -m pytest backend/tests/test_ocr_pipeline.py -v

# Frontend build (catches import errors, TDZ, missing exports)
cd frontend && npm run build
```

Expected: all pass, build exits 0.

---

## 2. Critical flow checklist (manual verification — 10 min)

### A. Reader library
- [ ] Open Reader section → library renders without crash
- [ ] Header shows "Моя библиотека" (not blank, not "Архив")
- [ ] Tap archive toggle → header switches to "Архив"
- [ ] Library search filters documents

### B. Translation flow
- [ ] Start translation session → card appears
- [ ] Submit answer → result screen appears with TTS and Explain buttons
- [ ] Tap TTS button → audio plays
- [ ] Tap Explain button → grammar explanation arrives
- [ ] Tap Hide/Show → result collapses and expands
- [ ] Finish session → session ends cleanly

### C. Shortcut / forwarded ingest
- [ ] Trigger iPhone Shortcut with German vocabulary text
- [ ] Backend responds 200 with `accepted: true, queued: true`
- [ ] Within 30s: Telegram DM receives vocabulary cards with inline keyboard buttons
- [ ] Tap a language-pair button → translation arrives

### D. FSRS / flashcards
- [ ] Open flashcards → card appears
- [ ] Flip card → answer revealed
- [ ] Rate card (Easy / Good / Hard / Again) → next card appears
- [ ] Offline review: rate card while offline → review buffered, syncs on reconnect

### E. Dictionary
- [ ] Look up word → result appears
- [ ] Save to folder → success
- [ ] Open folder → entry visible
- [ ] Rename folder → name updates
- [ ] Delete entry → entry removed

### F. Free / paid gating
- [ ] Free user: Today / Skills / Weekly tiles show lock indicator
- [ ] Tap locked tile → toast + subscription section opens
- [ ] Paid user: all tiles open normally

### G. YouTube
- [ ] Search for video → results appear
- [ ] Select video → transcript fetches
- [ ] Manual transcript → accepted and processed

---

## 3. Known past regressions (automatically pinned)

| Regression | Commit fixed | Test covering it |
|---|---|---|
| YouTube opens on startup (`open_section: false` ignored) | `4626b672` | `test_regression_smoke.py::YouTubeStartupNavigationTests` |
| Shortcut delivery aborts on transient DB error | `f7f75b66` | `test_regression_smoke.py::ShortcutWorkerDeliveryResilienceTests` / `test_shortcut_ingest_idempotency.py` |
| ReaderLibraryProvider crash: missing `readerArchiveOpen` | this branch | `test_regression_smoke.py::ReaderSectionValueObjectTests` / `readerLibraryProvider.test.js` |

---

## 4. Before deploying

```bash
git diff main...HEAD --stat        # review what changed
git log main...HEAD --oneline      # confirm commit messages
npm test && python -m pytest backend/tests/ -x -q   # full suite
npm run build                      # production build
```

Push only after all automated tests pass and the 10-min manual checklist is clear.

---

## 5. If psycopg2 architecture mismatch blocks backend tests

On Apple Silicon, `psycopg2` may be built for x86_64 and fail to import.
Backend tests that mock the DB (most of them) still pass.
Tests that require a real DB connection will fail with `ImportError`.
Report this explicitly — do not mark backend tests as "passed" if import fails.

Check with:
```bash
python -c "import psycopg2; print('ok')" 2>&1
```
