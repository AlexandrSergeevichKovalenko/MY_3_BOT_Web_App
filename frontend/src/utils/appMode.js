export function detectAppMode() {
  if (typeof window !== 'undefined') {
    // The installed home-screen PWA is its own surface. It loads the Telegram JS SDK
    // too (index.html includes telegram-web-app.js), so window.Telegram.WebApp EXISTS
    // even outside Telegram — checking that first would misreport the PWA as
    // 'telegram'. A standalone display-mode is NEVER the in-app Telegram webview, so
    // detect it FIRST. This keeps the server-side single-instance app_context
    // consistent with the client's per-surface lock, so the PWA and the Telegram
    // Mini-App stop deactivating each other ("Diese Kopie ist nicht mehr aktiv" flash).
    const standaloneByMedia = typeof window.matchMedia === 'function'
      && window.matchMedia('(display-mode: standalone)').matches;
    const standaloneByNavigator = typeof navigator !== 'undefined'
      && navigator.standalone === true;
    if (standaloneByMedia || standaloneByNavigator) {
      return 'pwa';
    }
    if (window.Telegram?.WebApp) {
      return 'telegram';
    }
  }
  return 'browser';
}
