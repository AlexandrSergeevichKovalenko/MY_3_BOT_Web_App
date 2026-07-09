import React from 'react'
import ReactDOM from 'react-dom/client'
import { detectAppMode } from './utils/appMode.js'
// ./ в начале пути означает, что файл App.jsx находится в той же папке, что и текущий файл main.jsx
import './theme.css'

const appMode = detectAppMode();
const params = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : new URLSearchParams();
const isWebappPath = typeof window !== 'undefined'
  && (window.location.pathname === '/webapp' || window.location.pathname === '/webapp/review');
const hasTelegramUrlHints = params.has('tgWebAppData') || params.get('mode') === 'webapp' || isWebappPath;
const shouldTreatAsTelegram = appMode === 'telegram' || hasTelegramUrlHints;

function getCurrentWebappAssetPath() {
  if (typeof document === 'undefined') return '';
  const moduleScript = document.querySelector('script[type="module"][src]');
  if (!moduleScript) return '';
  const src = String(moduleScript.getAttribute('src') || moduleScript.src || '').trim();
  if (!src) return '';
  try {
    return new URL(src, window.location.origin).pathname;
  } catch (_error) {
    return src;
  }
}

function buildTelegramReloadUrl(buildId = '') {
  const url = new URL(window.location.href);
  if (buildId) {
    url.searchParams.set('_wb', buildId);
  } else {
    url.searchParams.set('_wb', String(Date.now()));
  }
  return url.toString();
}

async function ensureFreshTelegramBundle() {
  if (!shouldTreatAsTelegram || typeof window === 'undefined' || typeof fetch !== 'function') {
    return true;
  }
  try {
    const response = await fetch('/api/webapp/version', {
      method: 'GET',
      cache: 'no-store',
      headers: { 'Cache-Control': 'no-cache' },
    });
    if (!response.ok) return true;
    const data = await response.json();
    const serverScriptSrc = String(data?.script_src || '').trim();
    const serverBuildId = String(data?.build_id || '').trim();
    const currentAssetPath = getCurrentWebappAssetPath();
    const serverAssetPath = serverScriptSrc
      ? new URL(serverScriptSrc, window.location.origin).pathname
      : '';
    if (!currentAssetPath || !serverAssetPath || currentAssetPath === serverAssetPath) {
      return true;
    }
    const reloadMarkerKey = serverBuildId ? `telegram-webapp-reload:${serverBuildId}` : '';
    if (reloadMarkerKey) {
      try {
        if (window.sessionStorage.getItem(reloadMarkerKey) === '1') {
          return true;
        }
        window.sessionStorage.setItem(reloadMarkerKey, '1');
      } catch (_storageError) {
        // ignore storage failures
      }
    }
    window.location.replace(buildTelegramReloadUrl(serverBuildId));
    return false;
  } catch (_error) {
    return true;
  }
}


function shouldForceTelegramRecover(errorLike) {
  const message = String(errorLike?.message || errorLike || '').toLowerCase();
  return message.includes('before initialization') || message.includes('cannot access');
}

function installTelegramRuntimeRecovery() {
  if (!shouldTreatAsTelegram || typeof window === 'undefined') return;
  const markerKey = 'telegram-webapp-runtime-recover-v1';
  const triggerRecover = () => {
    try {
      if (window.sessionStorage.getItem(markerKey) === '1') return;
      window.sessionStorage.setItem(markerKey, '1');
    } catch (_storageError) {
      // ignore storage failures
    }
    window.location.replace(buildTelegramReloadUrl());
  };

  window.addEventListener('error', (event) => {
    if (!shouldForceTelegramRecover(event?.error || event?.message)) return;
    triggerRecover();
  });

  window.addEventListener('unhandledrejection', (event) => {
    if (!shouldForceTelegramRecover(event?.reason)) return;
    triggerRecover();
  });
}
if (shouldTreatAsTelegram) {
  if (typeof navigator !== 'undefined' && 'serviceWorker' in navigator) {
    navigator.serviceWorker.getRegistrations()
      .then((registrations) => Promise.all(registrations.map((item) => item.unregister())))
      .catch(() => {
        // ignore SW cleanup errors in Telegram webview
      });
  }
} else {
  import('virtual:pwa-register')
    .then(({ registerSW }) => {
      registerSW({ immediate: true });
    })
    .catch(() => {
      // ignore SW registration errors in non-PWA environments
    });
}

installTelegramRuntimeRecovery();

// Lock-screen "Now Playing" branding: without MediaSession metadata iOS falls back
// to the page <title> ("Vite + React") for any audio we play (TTS / Hörverständnis /
// Reader). Set our title + mascot artwork once at startup, and re-assert on the first
// media `play` (some engines reset metadata when a new media element starts).
function setupMediaSession() {
  if (typeof navigator === 'undefined' || !('mediaSession' in navigator)
      || typeof window === 'undefined' || typeof window.MediaMetadata !== 'function') {
    return;
  }
  const apply = () => {
    try {
      navigator.mediaSession.metadata = new window.MediaMetadata({
        title: 'Das Deutsche Schlümpfchen',
        artist: 'Deutsch lernen',
        artwork: [
          { src: '/hero_sticker.webp', sizes: '512x512', type: 'image/webp' },
          { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png' },
        ],
      });
    } catch (_error) {
      // ignore — metadata is best-effort branding
    }
  };
  apply();
  try {
    document.addEventListener('play', apply, true);
  } catch (_error) {
    // ignore
  }
}

setupMediaSession();

async function loadAppComponent() {
  try {
    const module = await import('./App.jsx');
    return module?.default || null;
  } catch (error) {
    if (shouldTreatAsTelegram && shouldForceTelegramRecover(error)) {
      try {
        const markerKey = 'telegram-webapp-runtime-recover-v1';
        if (window.sessionStorage.getItem(markerKey) !== '1') {
          window.sessionStorage.setItem(markerKey, '1');
          window.location.replace(buildTelegramReloadUrl());
          return null;
        }
      } catch (_storageError) {
        window.location.replace(buildTelegramReloadUrl());
        return null;
      }
    }
    throw error;
  }
}

// Lightweight answer overlay: launched from a group task button via
// startapp=ans_rb_<id> / ans_cw_<id>. Mounts ONLY the tiny overlay (lazy chunk)
// and skips the heavy main App so it opens instantly over the group chat.
function getAnswerStartParam() {
  const fromTelegram = String(window.Telegram?.WebApp?.initDataUnsafe?.start_param || '').trim();
  if (fromTelegram) return fromTelegram;
  const fromQuery = String(params.get('startapp') || params.get('start_param') || '').trim();
  if (fromQuery) return fromQuery;
  // Path-based entry: a short URL like /dict (or /d) opens the quick-dictionary
  // overlay directly. The BotFather "Main Mini App" URL has a length cap and the
  // long Railway domain leaves no room for ?startapp=dict, so we expose a short
  // path the backend already serves (catch-all → index.html, absolute assets).
  const path = String(window.location?.pathname || '').replace(/\/+$/, '').toLowerCase();
  if (path === '/dict' || path === '/d') return 'dict';
  if (path === '/settings') return 'settings';
  // Public shareable tour: /tour (or /onboarding) opens the onboarding wizard as a
  // presentation — works in a plain browser for people who don't have the bot yet.
  if (path === '/tour' || path === '/onboarding') return 'onboarding';
  return '';
}

async function bootstrapAnswerOverlay(startParam) {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: AnswerOverlay } = await import('./answer/AnswerOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <AnswerOverlay startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapDeepDive(startParam) {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: DeepDiveOverlay } = await import('./answer/DeepDiveOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DeepDiveOverlay startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapLeaderboard(startParam) {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: Leaderboard } = await import('./leaderboard/Leaderboard.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <Leaderboard startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapPlanTable() {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: PlanTable } = await import('./plan/PlanTable.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <PlanTable />
    </React.StrictMode>,
  );
}

async function bootstrapShortcutGuide() {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: ShortcutGuide } = await import('./shortcut/ShortcutGuide.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <ShortcutGuide />
    </React.StrictMode>,
  );
}

async function bootstrapOnboarding() {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: OnboardingWizard } = await import('./onboarding/OnboardingWizard.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <OnboardingWizard />
    </React.StrictMode>,
  );
}

// The quick dictionary gets its OWN home-screen identity: a dictionary icon and a
// dedicated manifest with start_url "/dict", so "Add to Home Screen" opens the
// dictionary (not the app root → login gate) and shows a dictionary icon, not the
// app hero. Injected only on the /dict page; the main app keeps its own manifest/icon.
function applyDictHomeScreenMeta() {
  try {
    const head = document.head;
    const setLink = (rel, href) => {
      let el = head.querySelector(`link[rel="${rel}"]`);
      if (!el) { el = document.createElement('link'); el.setAttribute('rel', rel); head.appendChild(el); }
      el.setAttribute('href', href);
    };
    // Carry the durable auth token into the manifest link so iOS "Add to Home Screen"
    // captures a start_url that includes it (the runtime href wins over the server-baked
    // one, since iOS reads the manifest when the user taps install — after JS ran). A
    // standalone PWA has its own storage partition, so a tokenless start_url cold-launches
    // unauthenticated and audio / breakdown / save all 401. Prefer the launch-URL token,
    // fall back to the cached one so re-installing from any /dict page stays authed.
    let dqt = '';
    try {
      const p = new URLSearchParams(window.location.search || '');
      dqt = String(p.get('dqt') || '').trim() || String(localStorage.getItem('dq_browser_token_v1') || '').trim();
    } catch (_e) { /* ignore */ }
    const manifestHref = dqt
      ? `/dict-manifest.webmanifest?dqt=${encodeURIComponent(dqt)}`
      : '/dict-manifest.webmanifest';
    setLink('manifest', manifestHref);
    setLink('apple-touch-icon', '/icons/dict-apple-touch-icon.png');
    const lang = (() => {
      try { return (localStorage.getItem('ui_lang') || '').toLowerCase() === 'de' ? 'de' : 'ru'; }
      catch (_e) { return 'ru'; }
    })();
    const title = lang === 'de' ? 'Wörterbuch' : 'Словарь';
    const setMeta = (name, content) => {
      let m = head.querySelector(`meta[name="${name}"]`);
      if (!m) { m = document.createElement('meta'); m.setAttribute('name', name); head.appendChild(m); }
      m.setAttribute('content', content);
    };
    setMeta('apple-mobile-web-app-capable', 'yes');
    setMeta('apple-mobile-web-app-title', title);
    try { document.title = title; } catch (_e) { /* ignore */ }
  } catch (_e) { /* non-fatal */ }
}

async function bootstrapDictionary() {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  applyDictHomeScreenMeta();
  const { default: DictionaryOverlay } = await import('./dictionary/DictionaryOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DictionaryOverlay />
    </React.StrictMode>,
  );
}

// Standalone settings page — opened from the reply-keyboard «⚙️ Настройки» button
// (startapp=settings). Bot-native prefs (autosave, battle-readiness, schedule, theme).
async function bootstrapSettings() {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: SettingsScreen } = await import('./settings/SettingsScreen.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <SettingsScreen />
    </React.StrictMode>,
  );
}

// Full "Полный разбор" card: launched from a DM chat button via startapp=razbor_<id>.
// Reads the pre-computed lookup by id and mounts the rich WOW breakdown only.
async function bootstrapDeepAnalysis(startParam) {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: DeepAnalysis } = await import('./dictionary/DeepAnalysis.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DeepAnalysis startParam={startParam} />
    </React.StrictMode>,
  );
}

// Standalone / home-screen auth bridge. The full app and its sub-views authenticate via
// Telegram initData, but a home-screen quick-dict PWA (or standalone Safari) has only a
// durable dict token. Capture it from the launch URL and transparently attach it to
// same-origin API calls so token-accepting endpoints (dictionary, audio, save) work
// outside Telegram — this is what makes «Открыть полный словарь» work from the home-screen
// icon instead of dead-ending on "initData nicht gefunden". Completely inert inside
// Telegram (initData is the auth of record there) and for non-API / non-string requests.
function installDictTokenAuthShim() {
  if (typeof window === 'undefined' || typeof window.fetch !== 'function') return;
  if (window.__dictAuthShimInstalled) return;
  window.__dictAuthShimInstalled = true;
  try {
    const p = new URLSearchParams(window.location.search || '');
    const urlTok = String(p.get('dqt') || '').trim();
    if (urlTok) { try { localStorage.setItem('dq_browser_token_v1', urlTok); } catch (_e) { /* ignore */ } }
    const urlInit = String(p.get('initData') || '').trim();
    if (urlInit) { try { localStorage.setItem('dq_initdata_v1', urlInit); } catch (_e) { /* ignore */ } }
  } catch (_e) { /* ignore */ }
  const getToken = () => {
    try {
      const p = new URLSearchParams(window.location.search || '');
      return String(p.get('dqt') || '').trim() || String(localStorage.getItem('dq_browser_token_v1') || '').trim();
    } catch (_e) { return ''; }
  };
  const inTelegram = () => { try { return Boolean(window.Telegram?.WebApp?.initData); } catch (_e) { return false; } };
  const origFetch = window.fetch.bind(window);
  window.fetch = (input, init) => {
    try {
      if (typeof input !== 'string') return origFetch(input, init);
      const token = getToken();
      if (!token || inTelegram()) return origFetch(input, init);
      let u;
      try { u = new URL(input, window.location.origin); } catch (_e) { return origFetch(input, init); }
      if (u.origin !== window.location.origin || !u.pathname.startsWith('/api/')) return origFetch(input, init);
      const opts = { ...(init || {}) };
      const method = String(opts.method || 'GET').toUpperCase();
      const headers = new Headers(opts.headers || {});
      if (!headers.has('X-Dict-Token')) headers.set('X-Dict-Token', token);
      opts.headers = headers;
      // JSON POST bodies: add dqt so body-based resolvers see it (never overwrite an existing one).
      if (method !== 'GET' && method !== 'HEAD' && typeof opts.body === 'string' && opts.body.trim().startsWith('{')) {
        try {
          const parsed = JSON.parse(opts.body);
          if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && !('dqt' in parsed)) {
            parsed.dqt = token;
            opts.body = JSON.stringify(parsed);
          }
        } catch (_e) { /* leave body untouched */ }
      }
      // GET (e.g. tts/url): add dqt as a query param if absent.
      if ((method === 'GET' || method === 'HEAD') && !u.searchParams.has('dqt')) {
        u.searchParams.set('dqt', token);
        return origFetch(u.toString(), opts);
      }
      return origFetch(input, opts);
    } catch (_e) {
      return origFetch(input, init);
    }
  };
}

async function bootstrapApp() {
  installDictTokenAuthShim();
  const answerStartParam = getAnswerStartParam();
  if (/^ans_/i.test(answerStartParam)) {
    await bootstrapAnswerOverlay(answerStartParam);
    return;
  }
  if (/^dive_/i.test(answerStartParam)) {
    await bootstrapDeepDive(answerStartParam);
    return;
  }
  if (/^plan$/i.test(answerStartParam)) {
    await bootstrapPlanTable();
    return;
  }
  if (/^shortcut$/i.test(answerStartParam)) {
    await bootstrapShortcutGuide();
    return;
  }
  if (/^onboarding$/i.test(answerStartParam)) {
    await bootstrapOnboarding();
    return;
  }
  if (/^razbor_/i.test(answerStartParam) || /^share_/i.test(answerStartParam)) {
    await bootstrapDeepAnalysis(answerStartParam);
    return;
  }
  if (/^dict$/i.test(answerStartParam)) {
    await bootstrapDictionary();
    return;
  }
  if (/^settings$/i.test(answerStartParam)) {
    await bootstrapSettings();
    return;
  }
  if (/^lb/i.test(answerStartParam)) {
    await bootstrapLeaderboard(answerStartParam);
    return;
  }
  const canRender = await ensureFreshTelegramBundle();
  if (!canRender) return;
  const App = await loadAppComponent();
  if (!App) return;
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}

void bootstrapApp();

// 2. Детально о "глобальном объекте document"
// Теория: Когда ваш браузер получает от сервера текст файла index.html, 
// он не просто показывает этот текст. Он парсит его 
// (читает и анализирует структуру) и строит у себя в памяти объектную 
// модель этой страницы. Эта модель называется 
// DOM (Document Object Model). DOM — это "живое дерево" вашей страницы.
// Объект document — это и есть корень этого "дерева". Это глобальный объект, который JavaScript в браузере предоставляет вам как точку входа для взаимодействия со всей страницей.

//Каждый элемент в этом дереве — это тоже объект со своими 
//свойствами (например, .id, .textContent) и методами 
//(функциями, как .getElementById()). Когда вы в JavaScript меняете свойство 
// у одного из этих объектов (например, titleElement.textContent = 
// 'Новый текст'), браузер мгновенно перерисовывает соответствующую часть 
// страницы, чтобы отразить это изменение.
// Вот точная, пошаговая цепочка:
// Запрос: Браузер отправляет запрос на http://localhost:5173.
// Ответ: Vite-сервер отправляет в ответ текст файла index.html.
// Построение DOM: Браузер получает этот текст, парсит его и строит в своей памяти то самое "живое дерево" — DOM. С этого момента объект document существует и ссылается на это дерево.
// Запуск скрипта: Браузер доходит до тега <script src="/src/main.jsx">, загружает и начинает выполнять этот JavaScript-файл.
// Команда React: Теперь, когда скрипт main.jsx уже выполняется в контексте этой страницы, он выполняет команду document.getElementById('root'). Он обращается к уже с
// уществующему в памяти объекту document и находит в нем нужный div.
// ...теперь React делает этот div массив своим корнем. И теперь мы говорим чтобы этот массив внутри себя нарисовал App. Верно так или нет?
// Почти! Единственное маленькое уточнение: React делает div не "массивом", а "корнем" (root) своего приложения. Массив — это структура данных ([]), 
// а корень — это концепция, точка управления. В остальном — все абсолютно верно! React берет этот "корень" и "рисует" внутри него компонент <App />.
