#!/usr/bin/env bash
set -euo pipefail

if [[ "${#}" -lt 2 || "${#}" -gt 3 ]]; then
  echo "Usage: $0 <BASE_URL> <AUDIO_DISPATCH_TOKEN> [YYYY-MM-DD]" >&2
  exit 1
fi

BASE_URL="$1"
TOKEN="$2"
DATE="${3:-}"

payload='{}'
if [[ -n "$DATE" ]]; then
  payload=$(printf '{"date":"%s"}' "$DATE")
fi

curl -sS -X POST "${BASE_URL%/}/api/admin/send-daily-audio" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: $TOKEN" \
  -d "$payload"

