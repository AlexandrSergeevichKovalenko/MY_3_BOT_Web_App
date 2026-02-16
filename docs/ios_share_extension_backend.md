# Backend for iOS Share Extension

Добавлены мобильные endpoints в `backend/backend_server.py`:

- `POST /api/mobile/auth/exchange`
- `POST /api/mobile/dictionary/lookup`
- `POST /api/mobile/dictionary/save`

## 1) Exchange token

Request:
```json
{
  "initData": "<telegram webapp initData>"
}
```

Response:
```json
{
  "ok": true,
  "access_token": "<mobile-token>",
  "expires_in": 2592000,
  "user": {"id": 123, "username": "alex"}
}
```

## 2) Lookup from mobile extension

Headers:
- `Authorization: Bearer <access_token>`

Request:
```json
{
  "word": "Haus"
}
```

Response:
```json
{
  "ok": true,
  "item": {"word_de": "Haus", "translation_ru": "дом", "usage_examples": []},
  "direction": "de-ru"
}
```

## 3) Save to dictionary (same DB table as WebApp)

Headers:
- `Authorization: Bearer <access_token>`

Request:
```json
{
  "word_de": "Haus",
  "translation_ru": "дом",
  "response_json": {"word_de": "Haus", "translation_ru": "дом"}
}
```

Response:
```json
{"ok": true}
```

## ENV

- `MOBILE_AUTH_SECRET` (optional, strongly recommended)
- `MOBILE_AUTH_TTL_SECONDS` (optional, default `2592000` = 30 days)

Если `MOBILE_AUTH_SECRET` не задан, используется `TELEGRAM_Deutsch_BOT_TOKEN`.
