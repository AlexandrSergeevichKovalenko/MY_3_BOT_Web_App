# FSRS Spaced Repetition

## What was added
- `fsrs` package is used as the scheduling engine.
- PostgreSQL tables:
  - `bt_3_card_srs_state`
  - `bt_3_card_review_log`
- API:
  - `GET /api/cards/next`
  - `POST /api/cards/review`

## Config
- `SRS_NEW_PER_DAY` (optional, default: `20`)

## Local run
1. Install backend deps:
   - `pip install -r backend/requirements.txt`
2. Start backend as usual.
3. Open Telegram Mini App and go to flashcards section.

## Quick checks

### Get next card
```bash
curl -G "http://localhost:8080/api/cards/next" \
  --data-urlencode "initData=<telegram_init_data>"
```

### Submit review
```bash
curl -X POST "http://localhost:8080/api/cards/review" \
  -H "Content-Type: application/json" \
  -d '{
    "initData": "<telegram_init_data>",
    "card_id": 123,
    "rating": "GOOD",
    "response_ms": 4200
  }'
```

### Dry run simulation
```bash
python scripts/fsrs_dry_run.py
```

