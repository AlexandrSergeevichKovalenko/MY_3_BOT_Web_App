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
    // РАЗМЕР ФИЗИЧЕСКОГО ЭКРАНА, А НЕ ОКНА. Это главный признак, и он идёт первым.
    //
    // Прошлая проверка мерила ОКНО (`innerWidth`) — а окно здесь и есть та самая узкая
    // шторка, из которой мы пытаемся выйти: на iPad она шириной с телефон (~390 px).
    // Получалась замкнутая петля: чтобы попросить полный экран, надо было уже не быть
    // в шторке. Оставалась одна надежда — строка User-Agent, но Telegram на iPad
    // представляется айфоном, и первая же строчка ниже отправляла нас в «это телефон».
    // Так «Как пользоваться» и оставался коробочкой посреди планшета.
    //
    // `screen` описывает УСТРОЙСТВО и от размера шторки не зависит: меньшая сторона
    // экрана у телефонов 320–440 px, у планшетов от 744. Порог 700 — тот же, что у всей
    // планшетной вёрстки.
    const sw = Number(window.screen?.width) || 0;
    const sh = Number(window.screen?.height) || 0;
    if (sw > 0 && sh > 0 && Math.min(sw, sh) >= 700) return true;

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
    if (!tg) return recordFullscreenTry('нет tg');
    if (!isTabletLikeViewport()) return recordFullscreenTry('не планшет');
    if (typeof tg.requestFullscreen !== 'function') return recordFullscreenTry('клиент без requestFullscreen');
    recordFullscreenTry('просим');
    Promise.resolve(tg.requestFullscreen())
      .then(() => recordFullscreenTry('раскрыт'))
      .catch((e) => recordFullscreenTry(`отказ: ${e?.message || e}`));
    try { document.documentElement.setAttribute('data-tablet-fullscreen', '1'); } catch (_e) { /* ignore */ }
  } catch (e) { recordFullscreenTry(`сбой: ${e?.message || e}`); }
}

// ВРЕМЕННО: что именно решил и чем кончился запрос полного экрана. Пишем в одно место,
// чтобы владельцу можно было показать факт, а не догадку (строку рисует OnboardingWizard,
// видна только владельцу). Убрать вместе с той строкой.
export function recordFullscreenTry(state) {
  try { window.__fsTry = state; } catch (_e) { /* ignore */ }
}
export function fullscreenFacts() {
  const scr = `${Number(window.screen?.width) || 0}x${Number(window.screen?.height) || 0}`;
  const win = `${window.innerWidth || 0}x${window.innerHeight || 0}`;
  const ua = String(navigator.userAgent || '');
  const kind = /iPad/.test(ua) ? 'iPad' : /iPhone/.test(ua) ? 'iPhone' : /Macintosh/.test(ua) ? 'Mac' : 'иное';
  const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
  return `экран ${scr} · окно ${win} · планшет=${isTabletLikeViewport() ? 'да' : 'нет'}`
    + ` · ua=${kind} · вер.${tg?.version || '?'} · rf=${typeof tg?.requestFullscreen === 'function' ? 'есть' : 'нет'}`
    + ` · ${window.__fsTry || 'не вызывали'}`;
}
