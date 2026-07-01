# Shortcut Onboarding

## Goal

Remove manual Telegram `user_id` entry from iOS Shortcut usage.

> **Current design (2026-07-01): two-shortcut system.** The onboarding evolved into
> two separate iPhone Shortcuts (see `bot_3.py`):
>
> - **Shortcut A — collector** (`📲 1. Шорткат для скриншотов`): saves screenshots
>   into a `Deutsch Queue` photo album. Needs **no** pairing.
> - **Shortcut B — nightly processor** (`📲 2. Шорткат ночного перевода`): reads the
>   album, OCRs it, and pushes words to the DM. This one **does** need pairing and
>   carries the `install_token` — so the pairing-code → install-token → lookup
>   architecture described below applies to Shortcut B.
>
> Install/pairing is also surfaced inside the Mini-App (deeplink `startapp=shortcut`),
> not only via chat buttons.

The pairing flow for the processor Shortcut is:

1. User opens the Telegram bot.
2. User sends `/start` (menu also exposes `📲 Установить Shortcut`).
3. Bot shows the install buttons and a `📱 Подключить кодом` button
   (callback `shortcut:connect`).
4. User installs the shared iPhone Shortcut(s) from the install button(s).
5. User presses `📱 Подключить кодом`.
6. Bot returns a one-time pairing code and detailed iPhone setup instructions.
7. Installed Shortcut exchanges the pairing code for a permanent `install_token`.
8. Future requests use `install_token` only.

The physical Shortcut installation and pairing are separate steps. iOS requires
the user to confirm adding a shared Shortcut in Shortcuts.app; the bot/backend
cannot silently install a Shortcut on the user's phone.

## Database schema

Two new tables are created through the existing schema bootstrap in `backend/database.py`.

### `bt_3_shortcut_pairing_codes`

- `id`
- `user_id`
- `pairing_code_hash`
- `created_at`
- `expires_at`
- `consumed_at`
- `consumed_installation_id`
- `revoked_at`
- `is_active`
- `updated_at`

### `bt_3_shortcut_installations`

- `id`
- `user_id`
- `install_token_hash`
- `source_pairing_code_id`
- `created_at`
- `last_used_at`
- `revoked_at`
- `is_active`
- `updated_at`

Notes:

- Raw tokens are never stored in the clear.
- Pairing codes are short-lived and single-use.
- Install tokens are random and opaque.

## Endpoints

### `GET /api/shortcut/install`

Public redirect endpoint for the shared iPhone Shortcut.

Configuration:

- `SHORTCUT_INSTALL_URL`
- or `SHORTCUT_ICLOUD_URL`
- or `IOS_SHORTCUT_INSTALL_URL`

Expected value is the shared iCloud Shortcut link, for example:

```text
https://www.icloud.com/shortcuts/...
```

Response:

- `302` redirect to the configured install link.
- `503` if no install link is configured.

### `POST /api/shortcut/pairing-code`

Internal endpoint for issuing a pairing code.

Auth:

- `Authorization: Bearer <SHORTCUT_BOT_SECRET>`
- or `X-Shortcut-Bot-Secret: <SHORTCUT_BOT_SECRET>`

Request:

```json
{
  "user_id": 117649764,
  "ttl_seconds": 600
}
```

Response:

```json
{
  "ok": true,
  "pairing_code_id": 7,
  "user_id": 117649764,
  "pairing_code": "A7F4K2",
  "created_at": "2026-05-30T12:00:00Z",
  "expires_at": "2026-05-30T12:10:00Z",
  "expires_in": 600
}
```

### `POST /api/shortcut/link`

Public one-time exchange endpoint.

Request:

```json
{
  "pairing_code": "A7F4K2"
}
```

Response:

```json
{
  "ok": true,
  "installation_id": 11,
  "install_token": "st_...",
  "created_at": "2026-05-30T12:01:00Z",
  "expires_at": "2026-05-30T12:10:00Z"
}
```

Error handling:

- `400` for malformed or invalid codes.
- `409` for already used codes.
- `410` for expired codes.

### `POST /api/shortcut/lookup`

Public lookup endpoint.

Request:

```json
{
  "install_token": "st_...",
  "text": "..."
}
```

Response:

```json
{
  "ok": true,
  "accepted": true,
  "queued": true,
  "duplicate": false,
  "job_id": "..."
}
```

Rules:

- `user_id` is not accepted.
- Requests without a valid `install_token` fail closed.
- The backend resolves `install_token -> user_id` server-side.

## Telegram bot flow

- The private-chat menu exposes `📲 Установить Shortcut`; the install keyboard
  offers both shortcuts (`📲 1. Шорткат для скриншотов`, `📲 2. Шорткат ночного
  перевода`) plus a `📱 Подключить кодом` button (callback `shortcut:connect`,
  handled by `handle_shortcut_connect_callback`).
- The install buttons open the shared Shortcut install links.
- `📱 Подключить кодом` issues a fresh pairing code and sends detailed
  iPhone-specific instructions:
  - Install the shared Shortcut first.
  - Action Button path for supported models.
  - Back Tap `Double Tap` path for models without Action Button.
  - Shortcut setup steps for creating the first-run link exchange.
  - Persistent `install_token` storage instructions.

### Related: nightly auto-save

Layered on top of the collector→processor Shortcuts is an opt-in **nightly
auto-save** (`🌙 Ночной автосейв` toggle; callbacks `asv_tog:` / `asv_save:`):
staged words are debounced, batch-translated, and offered back as a multi-select
digest. The fast-translate queue is snapshotted and purged up front on each batch,
and `/clearqueue` (`clear_dictionary_queue_command`) lets the user drain a stuck
queue. These are separate from the pairing/token security surface documented here.

## Threat model

Addressed risks:

- `user_id` spoofing is removed because clients never send it.
- Pairing codes expire and are single-use.
- Install tokens are random and server-issued.
- Public lookup requests fail if the token is missing or invalid.
- Database stores hashes, not raw secrets.

Remaining risks:

- If a user shares their `install_token`, another device can use it until revoked.
- If a pairing code is intercepted before use, it can be exchanged once.
- Telegram message content is visible in the user’s private chat; this is acceptable only because the code is short-lived and single-use.

Operational controls:

- Old `user_id`-based request shape is not accepted.
- The link flow does not keep a compatibility path.
- Revoking access should also revoke active installations if you later add explicit user revocation hooks.

## Rollout plan

1. Deploy the schema and backend changes.
2. Deploy the bot update that shows the new onboarding button.
3. Verify `/api/shortcut/pairing-code`, `/api/shortcut/link`, and `/api/shortcut/lookup` in staging.
4. Remove any client-side Shortcut presets that still send `user_id`.
5. Monitor link failures, expired code frequency, and duplicate lookup suppression.

## Alternative evaluation

A personal URL such as `https://your-domain.com/shortcut/link/<pairing_token>` is usable as a launch surface, but it is not the architecture implemented here.

Reason:

- it moves a secret into a URL-bearing flow,
- it increases replay and leakage risk,
- it does not remove the need for a server-side one-time exchange,
- it is less explicit than a pairing-code handoff for first-time trust.

The current implementation keeps one path only: pairing code -> install token -> lookup.
