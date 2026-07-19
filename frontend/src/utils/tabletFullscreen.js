// Tablet fullscreen for the lightweight Mini-App overlays (games, interactives,
// hubs). On iPad / wide tablets Telegram presents a Direct-Link Mini App as a
// narrow, phone-width floating sheet in the middle of the screen — so an
// interactive that should fill the tablet ends up a tiny scrollable box. The
// main App.jsx already fixes this for itself; each standalone overlay
// (AnswerOverlay, DeepDive, DeepAnalysis, hubs, Leaderboard…) is a SEPARATE entry
// point that skips that logic, so they need the same treatment.
//
// Phones are left untouched: Telegram already opens them full-height there, and
// requestFullscreen() on a handset would hide the chrome we rely on.

// Tablet/wide-screen (iPad etc.) — NOT a handset. Mirrors detectTabletLikeViewport
// in App.jsx and isTabletLikeViewport in DictionaryOverlay.jsx.
export function isTabletLikeViewport() {
  try {
    const w = window.innerWidth || 0;
    const h = window.innerHeight || 0;
    const ua = String(navigator.userAgent || '');
    if (/iPhone|iPod|Windows Phone|Android.*Mobile/i.test(ua)) return false;
    const isIPadDesktopUA = navigator.platform === 'MacIntel' && Number(navigator.maxTouchPoints || 0) > 1;
    const isTabletUA = /iPad|Tablet|PlayBook|Silk|Android(?!.*Mobile)/i.test(ua) || isIPadDesktopUA;
    return isTabletUA || w >= 700 || (Math.max(w, h) >= 1000 && Math.min(w, h) >= 600);
  } catch (_e) { return false; }
}

// Request true fullscreen so a standalone overlay fills the tablet like the main
// app instead of Telegram's narrow compact sheet. No-op on phones and on older
// clients that lack requestFullscreen (they simply reject → caught). Safe to call
// from a mount effect; Telegram treats the Mini-App launch tap as the gesture.
export function requestTabletFullscreen(tg) {
  try {
    if (!tg || !isTabletLikeViewport()) return;
    if (typeof tg.requestFullscreen !== 'function') return;
    Promise.resolve(tg.requestFullscreen()).catch(() => { /* unsupported client */ });
    try { document.documentElement.setAttribute('data-tablet-fullscreen', '1'); } catch (_e) { /* ignore */ }
  } catch (_e) { /* ignore */ }
}
