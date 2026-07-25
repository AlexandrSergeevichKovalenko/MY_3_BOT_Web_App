// One place that turns a raw API error into a clean, human-facing message for the
// dictionary surfaces (quick overlay + full breakdown). House rule: the user must
// never see a machine code like "cost_cap_exceeded" — that's a contract between the
// server and the frontend, not a sentence for a person. Every catch-block in the
// dictionary routes its error through here instead of printing e.message blindly.
//
// The dictionary api() helper throws an Error shaped like:
//   err.message = data.error          e.g. "cost_cap_exceeded"
//   err.status  = HTTP status         e.g. 429
//   err.payload = the full JSON body  e.g. { error, message, cap_eur, reset_at, ... }
// so we can read the machine code from either err.payload.error or err.message.

// Cost-cap hit → the same soft line the main app shows (kept identical to App.jsx on
// purpose). It must say WHAT still works: only new heavy generation is paused, while
// familiar words, repetitions, games and reading keep running — otherwise the user
// concludes the whole app broke.
const CAP_MESSAGE_RU = 'Сегодня ты позанимался особенно много — дневной запас на новые разборы исчерпан 🙌 Он обновится в 00:00. Сейчас работают словарь по знакомым словам, повторения, игры и чтение.';

// A per-feature free-tier limit is a DIFFERENT thing from the shared daily budget, so it
// gets its own neutral line instead of borrowing the cap wording.
const LIMIT_MESSAGE_RU = 'На сегодня лимит этой функции исчерпан. Он обновится завтра в 00:00 🙌';

// Telegram-auth error (missing/expired initData) → a plain, actionable hint. The
// standalone browser dictionary authenticates with initData carried in the launch
// URL; it can be absent (opened directly, not from Telegram) or expired (>30 days).
const AUTH_MESSAGE_RU = 'Доступ устарел. Открой словарь заново из Telegram (кнопкой в онбординге) — и, если вынес иконку на экран, добавь её ещё раз.';

export function humanizeDictError(e) {
  // The machine code lives in payload.error (preferred) or falls back to the message.
  const code = String((e && e.payload && e.payload.error) || (e && e.message) || e || '').trim();
  const status = e && e.status;

  // Billing / daily-limit codes → one friendly line (never the raw code).
  if (code === 'cost_cap_exceeded') return CAP_MESSAGE_RU;
  if (code === 'feature_limit_exceeded' || code === 'free_limit_exceeded') {
    // The server sometimes sends a ready human message for a specific feature; prefer it.
    const serverMsg = e && e.payload && String(e.payload.message || '').trim();
    return serverMsg || LIMIT_MESSAGE_RU;
  }

  // Paid-only feature hit by a free (non-trial) user → point them to the paid tier
  // instead of leaking the raw "paid_feature_required" code.
  if (code === 'paid_feature_required') {
    const title = e && e.payload && String(e.payload.feature_title || e.payload.feature || '').trim();
    return title
      ? `«${title}» доступно в тарифе «Полный доступ».`
      : 'Эта функция доступна в тарифе «Полный доступ».';
  }

  // Expired / missing Telegram auth → actionable re-open hint.
  if (status === 401 || /initData|не прошёл проверку/i.test(code)) return AUTH_MESSAGE_RU;

  // Anything else: keep the original text. Genuine user-facing messages we throw
  // ourselves (e.g. "Не удалось перевести слово") are already human and should show.
  return code;
}
