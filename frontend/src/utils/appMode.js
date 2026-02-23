export function detectAppMode() {
  if (typeof window !== 'undefined' && window.Telegram?.WebApp) {
    return 'telegram';
  }
  if (typeof window !== 'undefined') {
    const standaloneByMedia = typeof window.matchMedia === 'function'
      && window.matchMedia('(display-mode: standalone)').matches;
    const standaloneByNavigator = typeof navigator !== 'undefined'
      && navigator.standalone === true;
    if (standaloneByMedia || standaloneByNavigator) {
      return 'pwa';
    }
  }
  return 'browser';
}
