import React from 'react'
import ReactDOM from 'react-dom/client'
import { detectAppMode } from './utils/appMode.js'
// ./ в начале пути означает, что файл App.jsx находится в той же папке, что и текущий файл main.jsx
import './theme.css'

// РАЗВЕРНУТЬ МИНИ-АПП НА ВЕСЬ ЭКРАН — и только потом что-то мерить.
//
// Без `expand()` Telegram открывает мини-апп ШТОРКОЙ: сам webview при этом выложен на всю
// высоту экрана, а видно только его верхнюю часть. В этом состоянии про экран есть ДВА
// разных числа, и оба «правильные»: браузерное (`visualViewport.height` — высота webview) и
// телеграмовское (`viewportStableHeight` — высота видимой части). Любая подгонка под экран
// вынуждена выбирать, и выбор всегда бьёт по одному из случаев: возьмёшь браузерное —
// карточка уедет под шторку, возьмёшь телеграмовское (а оно ещё и отстаёт во время
// анимации) — карточка окажется вдвое ниже экрана посреди пустых полос.
//
// `expand()` убирает саму развилку: мини-апп занимает экран целиком, и оба числа сходятся.
// Так и задумано в Telegram, это штатный вызов при старте — а не наша хитрость.
function tgReady() {
  try {
    const tg = window.Telegram?.WebApp;
    if (!tg) return;
    tg.ready?.();
    if (tg.isExpanded !== true) tg.expand?.();
  } catch (_e) { /* ignore */ }
}

const appMode = detectAppMode();
const params = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : new URLSearchParams();
const isWebappPath = typeof window !== 'undefined'
  && (window.location.pathname === '/webapp' || window.location.pathname === '/webapp/review');
const hasTelegramUrlHints = params.has('tgWebAppData') || params.get('mode') === 'webapp' || isWebappPath;
const shouldTreatAsTelegram = appMode === 'telegram' || hasTelegramUrlHints;
// Both the Telegram webview AND the installed home-screen PWA must guard against a stale
// bundle: Telegram relaunches from suspension without a hard navigation, and the PWA's
// service-worker precache can pin an old app shell for days. Without this, the PWA keeps
// rendering a previous build even though the server already serves the new one.
const shouldEnsureFreshBundle = shouldTreatAsTelegram || appMode === 'pwa';

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

// Wipe the service-worker precache + registrations. A standalone PWA is served its app
// shell FROM the SW precache, so a plain reload just re-serves the SAME stale bundle — we
// must drop the caches and unregister the worker first, then reload to fetch the live build.
async function purgeAppShellCaches() {
  try {
    if (typeof caches !== 'undefined' && typeof caches.keys === 'function') {
      const keys = await caches.keys();
      await Promise.all(keys.map((key) => caches.delete(key)));
    }
  } catch (_cacheError) {
    // best-effort
  }
  try {
    if (typeof navigator !== 'undefined' && navigator.serviceWorker?.getRegistrations) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      await Promise.all(registrations.map((registration) => registration.unregister()));
    }
  } catch (_swError) {
    // best-effort
  }
}

async function ensureFreshBundle() {
  if (!shouldEnsureFreshBundle || typeof window === 'undefined' || typeof fetch !== 'function') {
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
    // Stale bundle detected. Attempt the heavy recovery at most once per build per session
    // (after a successful purge+reload the asset paths match, so this won't loop).
    const reloadMarkerKey = serverBuildId ? `webapp-stale-reload:${serverBuildId}` : '';
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
    // PWA: the stale shell comes from the SW precache — a reload alone re-serves it. Drop the
    // caches + unregister the worker so the reload hits the server for the current build.
    if (appMode === 'pwa') {
      await purgeAppShellCaches();
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
  // Pick up a fresh deploy on THIS open instead of one open later. The SW skip-waits +
  // clients-claims, so a new bundle activates immediately — but the already-loaded page keeps
  // running the old JS/CSS (StaleWhileRevalidate served the previous bundle) until the next
  // launch. Reload once when the UPDATED worker takes control so the new build is applied now.
  // Guarded so we don't reload on the first-ever control acquisition (initial install claim)
  // and never loop.
  if (typeof navigator !== 'undefined' && 'serviceWorker' in navigator) {
    const hadControllerAtLoad = Boolean(navigator.serviceWorker.controller);
    let reloadingForSwUpdate = false;
    navigator.serviceWorker.addEventListener('controllerchange', () => {
      if (reloadingForSwUpdate || !hadControllerAtLoad) return;
      reloadingForSwUpdate = true;
      window.location.reload();
    });
  }
  import('virtual:pwa-register')
    .then(({ registerSW }) => {
      registerSW({
        immediate: true,
        // Make a fresh deploy land WITHOUT any user action — no "clear your cache",
        // no reinstall, no per-user hand-holding. registerSW on its own only checks
        // for a new worker on a hard navigation; an iOS home-screen PWA relaunched
        // from a suspended state does none, so it can keep serving a stale bundle for
        // days (the root cause of "nothing changed after deploy"). Force
        // registration.update() every time the app returns to the foreground (plus a
        // slow safety interval for sessions left open): that re-fetches sw.js, the new
        // worker installs, skipWaiting + clientsClaim make it take control, and the
        // controllerchange handler above reloads once — so the newest build goes live
        // on the very next open, invisibly, for every user.
        onRegisteredSW(_swScriptUrl, registration) {
          if (!registration) return;
          const checkForUpdate = () => { registration.update().catch(() => {}); };
          const checkIfVisible = () => {
            if (document.visibilityState === 'visible') checkForUpdate();
          };
          document.addEventListener('visibilitychange', checkIfVisible);
          window.addEventListener('focus', checkForUpdate);
          window.addEventListener('pageshow', checkForUpdate);
          setInterval(checkForUpdate, 60 * 60 * 1000); // hourly safety net for long-open sessions
          checkForUpdate(); // and once right now
        },
      });
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
        title: 'Schlaufuchs',
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
  // Явный ?startapp в адресе ГЛАВНЕЕ того, что Telegram прислал при запуске.
  // Внутри мини-приложения мы иногда переходим сами (например, из интерактива в раздел
  // «Подписка»): при таком переходе Telegram по-прежнему отдаёт start_param запуска
  // (ans_sp_<id>), и он возвращал человека обратно в ту же игру — переход «не работал»,
  // а тренировка начиналась заново.
  const fromQuery = String(params.get('startapp') || params.get('start_param') || '').trim();
  if (fromQuery) return fromQuery;
  const fromTelegram = String(window.Telegram?.WebApp?.initDataUnsafe?.start_param || '').trim();
  if (fromTelegram) return fromTelegram;
  // Path-based entry: a short URL like /dict (or /d) opens the quick-dictionary
  // overlay directly. The BotFather "Main Mini App" URL has a length cap and the
  // long Railway domain leaves no room for ?startapp=dict, so we expose a short
  // path the backend already serves (catch-all → index.html, absolute assets).
  const path = String(window.location?.pathname || '').replace(/\/+$/, '').toLowerCase();
  // /dict, short /d, and the path-token form /dict/t/<token> (iOS-safe home-screen launch).
  if (path === '/dict' || path === '/d' || path.startsWith('/dict/t/')) return 'dict';
  if (path === '/settings') return 'settings';
  if (path === '/interactive') return 'interactive';
  if (path === '/battles') return 'battles';
  if (path === '/woerter') return 'woerter';
  if (path === '/zhaloby') return 'zhaloby';
  // Public shareable tour: /tour (or /onboarding) opens the onboarding wizard as a
  // presentation — works in a plain browser for people who don't have the bot yet.
  if (path === '/tour' || path === '/onboarding') return 'onboarding';
  return '';
}

async function bootstrapAnswerOverlay(startParam) {
  tgReady();
  const { default: AnswerOverlay } = await import('./answer/AnswerOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <AnswerOverlay startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapDeepDive(startParam) {
  tgReady();
  const { default: DeepDiveOverlay } = await import('./answer/DeepDiveOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DeepDiveOverlay startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapLeaderboard(startParam) {
  tgReady();
  const { default: Leaderboard } = await import('./leaderboard/Leaderboard.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <Leaderboard startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapPlanTable() {
  tgReady();
  const { default: PlanTable } = await import('./plan/PlanTable.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <PlanTable />
    </React.StrictMode>,
  );
}

// Light-theme boot spinner painted into #root BEFORE the async chunk import — so the
// onboarding / shortcut screens never show a bare white page while the JS downloads
// over a slow connection. React's first render replaces it.
function showBootSpinner() {
  try {
    const root = document.getElementById('root');
    if (!root || root.childElementCount > 0) return;
    root.innerHTML =
      '<div style="position:fixed;inset:0;display:flex;align-items:center;justify-content:center;'
      + 'background:linear-gradient(180deg,#f6f9ff 0%,#eef3fb 100%)">'
      + '<div style="width:34px;height:34px;border-radius:50%;border:3px solid #d5e2f5;'
      + 'border-top-color:#3a7bd5;animation:obspin .8s linear infinite"></div></div>'
      + '<style>@keyframes obspin{to{transform:rotate(360deg)}}</style>';
  } catch (_e) { /* noop */ }
}

async function bootstrapShortcutGuide() {
  tgReady();
  showBootSpinner();
  const { default: ShortcutGuide } = await import('./shortcut/ShortcutGuide.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <ShortcutGuide />
    </React.StrictMode>,
  );
}

async function bootstrapOnboarding() {
  tgReady();
  showBootSpinner();
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
      const pm = String(window.location.pathname || '').match(/^\/dict\/t\/([^/]+)/);
      const p = new URLSearchParams(window.location.search || '');
      dqt = (pm && pm[1] ? decodeURIComponent(pm[1]) : '')
        || String(p.get('dqt') || '').trim()
        || String(localStorage.getItem('dq_browser_token_v1') || '').trim();
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

// Перехватчик ошибок отрисовки для отдельного словаря.
//
// У приложения такой есть давно, а у словаря с рабочего стола не было — и 10.08.2026
// это стоило белого экрана: одна необъявленная функция в новой закладке уронила всё
// дерево, и человек увидел пустоту без единого слова о том, что случилось.
//
// Сборка такие ошибки не ловит (это не синтаксис), линтера в проекте нет. Значит
// единственная надёжная защита — не дать падению одного блока стереть весь экран.
class DictErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { failed: false };
  }

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error, info) {
    // Техническое — в консоль, человеку — человеческое.
    console.error('[dict] отрисовка упала', error, info);
  }

  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <div className="ans-root dq-scroll">
        <div className="ans-card dq-card">
          <div className="dq-gate">
            <div className="dq-gate-badge">🦊</div>
            <h2 className="dq-gate-title">Словарь не открылся</h2>
            <p className="dq-gate-text">
              Что-то пошло не так. Закройте окно и откройте словарь заново — обычно этого хватает.
            </p>
            <button type="button" className="dq-gate-btn" onClick={() => window.location.reload()}>
              Открыть заново
            </button>
          </div>
        </div>
      </div>
    );
  }
}

// Разбор противоречивых записей словаря — экран владельца, приходит ссылкой из лички
// по понедельникам и воскресеньям.
async function bootstrapWordIntegrity(scope = 'shared') {
  tgReady();
  // ⛔ ОТМЕТКУ ТЕМЫ СТАВЯТ ВСЕ ЭКРАНЫ, КРОМЕ ЭТОГО — И ЭТО БЫЛО ВИДНО ГЛАЗОМ. Светлые
  // цвета текста в answer.css висят на `html[data-scheme="light"]`; без отметки в дело
  // шли тёмные из theme.css, и на белом фоне текст читался как водяной знак
  // (владелец 27.08.2026). Схему спрашиваем у Telegram, как это делает словарь.
  try {
    const scheme = window.Telegram?.WebApp?.colorScheme === 'dark' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-scheme', scheme);
  } catch (_e) { /* ignore */ }
  const { default: WordIntegrityReview } = await import('./dictionary/WordIntegrityReview.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DictErrorBoundary>
        <WordIntegrityReview scope={scope} />
      </DictErrorBoundary>
    </React.StrictMode>,
  );
}


async function bootstrapDictionary(sharedDiffToken = '') {
  tgReady();
  applyDictHomeScreenMeta();
  const { default: DictionaryOverlay } = await import('./dictionary/DictionaryOverlay.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <DictErrorBoundary>
        <DictionaryOverlay sharedDiffToken={sharedDiffToken} />
      </DictErrorBoundary>
    </React.StrictMode>,
  );
}

// Standalone settings page — opened from the reply-keyboard «⚙️ Настройки» button
// (startapp=settings). Bot-native prefs (autosave, battle-readiness, schedule, theme).
async function bootstrapSettings() {
  tgReady();
  const { default: SettingsScreen } = await import('./settings/SettingsScreen.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <SettingsScreen />
    </React.StrictMode>,
  );
}

// Standalone «📚 Интерактив» hub — opened from the reply-keyboard button
// (startapp=interactive). A light card page linking to the existing ans_* games.
async function bootstrapInteractive() {
  tgReady();
  const { default: InteractiveHub } = await import('./interactive/InteractiveHub.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <InteractiveHub />
    </React.StrictMode>,
  );
}

// Проверка слов: экран, куда ведёт напоминание из лички (startapp=woerter).
// Отдельная страница, а не оверлей: человек приходит сюда по ссылке из сообщения и
// занимается только этим — списком слов, которые дверь не смогла подтвердить.
async function bootstrapWordAudit() {
  tgReady();
  const { default: WordAudit } = await import('./dictionary/WordAudit.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <WordAudit />
    </React.StrictMode>,
  );
}

// Разбор жалоб на карточки — экран владельца (startapp=zhaloby). Отдельная страница по
// той же причине, что и проверка слов: приходят по ссылке из сообщения и занимаются
// только этим.
async function bootstrapComplaintReview() {
  tgReady();
  const { default: ComplaintReview } = await import('./dictionary/ComplaintReview.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <ComplaintReview />
    </React.StrictMode>,
  );
}

// Standalone «⚔️ Battles» hub — opened from the reply-keyboard button (startapp=battles).
async function bootstrapBattles() {
  tgReady();
  const { default: BattlesHub } = await import('./battles/BattlesHub.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <BattlesHub />
    </React.StrictMode>,
  );
}

// Full "Полный разбор" card: launched from a DM chat button via startapp=razbor_<id>.
// Reads the pre-computed lookup by id and mounts the rich WOW breakdown only.
async function bootstrapDeepAnalysis(startParam) {
  tgReady();
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
  const tokenFromLaunch = () => {
    try {
      const pm = String(window.location.pathname || '').match(/^\/dict\/t\/([^/]+)/);
      if (pm && pm[1]) return decodeURIComponent(pm[1]);
      return String(new URLSearchParams(window.location.search || '').get('dqt') || '').trim();
    } catch (_e) { return ''; }
  };
  try {
    const urlTok = tokenFromLaunch();
    if (urlTok) { try { localStorage.setItem('dq_browser_token_v1', urlTok); } catch (_e) { /* ignore */ } }
    const urlInit = String(new URLSearchParams(window.location.search || '').get('initData') || '').trim();
    if (urlInit) { try { localStorage.setItem('dq_initdata_v1', urlInit); } catch (_e) { /* ignore */ } }
  } catch (_e) { /* ignore */ }
  const getToken = () => {
    try {
      return tokenFromLaunch() || String(localStorage.getItem('dq_browser_token_v1') || '').trim();
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
      // РУКОПОЖАТИЕ ВХОДА НЕ ТРОГАЕМ. Тело запроса /api/web/auth/* подписано Telegram, и
      // любое наше добавление ломает подпись: дописанное поле `aqt` попадало в строку
      // подписи на сервере, и вход через виджет не работал НИКОГДА (23.08.2026). Токен
      // здесь и не нужен — человек как раз входит, чтобы его получить.
      if (u.pathname.startsWith('/api/web/auth/')) return origFetch(input, init);
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

// --- Standalone MAIN app (home-screen icon, opened OUTSIDE Telegram) -----------------------
// Mirror of the dictionary's token plumbing, but for the WHOLE app: a durable app-browser
// token (minted in onboarding) rides in the launch URL and is attached to every /api/ call so
// the detached icon stays logged in without Telegram initData. Keys are app-specific ('aqt',
// 'X-App-Token', localStorage 'app_browser_token_v1') so they never collide with the dict token.
function appTokenFromLaunch() {
  try {
    const pm = String(window.location.pathname || '').match(/^\/webapp\/t\/([^/]+)/);
    if (pm && pm[1]) return decodeURIComponent(pm[1]);
    return String(new URLSearchParams(window.location.search || '').get('aqt') || '').trim();
  } catch (_e) { return ''; }
}

function getAppToken() {
  try {
    return appTokenFromLaunch() || String(localStorage.getItem('app_browser_token_v1') || '').trim();
  } catch (_e) { return ''; }
}

function applyAppHomeScreenMeta() {
  // Bake the durable app token into the manifest link so iOS "Add to Home Screen" captures a
  // start_url that carries it (iOS reads the manifest when the user taps install — after JS ran,
  // so this runtime href wins over the static one). A standalone PWA has its own storage
  // partition, so a tokenless start_url would cold-launch unauthenticated and every /api/ 401s.
  try {
    const token = getAppToken();
    if (!token) return; // no token → leave the static /manifest.webmanifest (plain browser user)
    const head = document.head;
    const setLink = (rel, href) => {
      let el = head.querySelector(`link[rel="${rel}"]`);
      if (!el) { el = document.createElement('link'); el.setAttribute('rel', rel); head.appendChild(el); }
      el.setAttribute('href', href);
    };
    setLink('manifest', `/app-manifest.webmanifest?aqt=${encodeURIComponent(token)}`);
    setLink('apple-touch-icon', '/icons/apple-touch-icon.png');
    const setMeta = (name, content) => {
      let m = head.querySelector(`meta[name="${name}"]`);
      if (!m) { m = document.createElement('meta'); m.setAttribute('name', name); head.appendChild(m); }
      m.setAttribute('content', content);
    };
    setMeta('apple-mobile-web-app-capable', 'yes');
    setMeta('apple-mobile-web-app-title', 'Schlaufuchs');
  } catch (_e) { /* non-fatal */ }
}

let __appBlockedGateShown = false;
function showAppBlockedGate(botUsername) {
  if (__appBlockedGateShown) return;
  __appBlockedGateShown = true;
  const uname = String(botUsername || 'Ich_Deutsch_bot').trim().replace(/^@/, '') || 'Ich_Deutsch_bot';
  const de = (() => { try { return (localStorage.getItem('ui_lang') || '').toLowerCase() === 'de'; } catch (_e) { return false; } })();
  const title = de ? 'Die App arbeitet zusammen mit dem Bot' : 'Приложение работает вместе с ботом';
  const body = de
    ? 'Es sieht so aus, als hättest du den Bot entfernt. Hol ihn zurück — dann funktioniert das Icon sofort wieder.'
    : 'Похоже, ты удалил бота. Верни его — и иконка снова заработает сразу.';
  const btn = de ? 'Bot öffnen' : 'Открыть бота';
  const openBot = () => {
    const https = `https://t.me/${uname}`;
    let done = false;
    try { window.location.href = `tg://resolve?domain=${uname}`; } catch (_e) { /* ignore */ }
    setTimeout(() => { if (done || document.hidden) return; done = true; try { window.location.href = https; } catch (_e) { /* ignore */ } }, 800);
  };
  try {
    const wrap = document.createElement('div');
    wrap.setAttribute('style', [
      'position:fixed', 'inset:0', 'z-index:2147483647', 'display:flex',
      'align-items:center', 'justify-content:center', 'padding:24px', 'box-sizing:border-box',
      'background:linear-gradient(160deg,#6366F1 0%,#4f46e5 100%)', 'color:#fff',
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif', 'text-align:center',
    ].join(';'));
    wrap.innerHTML = `
      <div style="max-width:340px;display:flex;flex-direction:column;align-items:center;gap:16px">
        <div style="font-size:56px;line-height:1">📚</div>
        <div style="font-size:22px;font-weight:700;line-height:1.25">${title}</div>
        <div style="font-size:15px;line-height:1.5;opacity:.92">${body}</div>
        <button type="button" style="margin-top:8px;border:0;border-radius:14px;padding:14px 22px;font-size:16px;font-weight:600;background:#fff;color:#4f46e5;cursor:pointer">${btn}</button>
      </div>`;
    wrap.querySelector('button').addEventListener('click', openBot);
    document.body.appendChild(wrap);
  } catch (_e) { /* ignore */ }
}

let __accessClosedGateShown = false;
/** Access was closed by an admin (/deny). Same shape as the bot-left gate, amber instead of
 *  indigo: the app is not broken, someone decided this — so no red, no server text, one way
 *  out (open the bot, where «Запросить доступ» lives). */
// Открыть чат с ботом. ⛔ НЕ ЧЕРЕЗ window.location.href.
//
// Замер 28.08.2026 на живом телефоне владельца: кнопка «Открыть чат с ботом» на экране
// очереди не делала НИЧЕГО. Внутри мини-аппа Telegram переход по `tg://resolve` и по
// `https://t.me/...` через location.href не срабатывает — оболочка его игнорирует.
// Штатный способ ровно один: Telegram.WebApp.openTelegramLink(), им и пользуется весь
// остальной проект (App.jsx, AnswerOverlay.jsx, SettingsScreen.jsx).
//
// Прежний код с location.href писался для экрана «доступ закрыт», а тот показывается
// ТОЛЬКО вне Telegram (токенный шим выходит раньше при inTelegram()) — там location.href
// работает, поэтому поломка и не всплывала. Экран очереди показывается ВНУТРИ мини-аппа,
// и на нём она вылезла сразу.
//
// Кнопка не украшение: если человек пришёл в приложение по ссылке и с ботом ни разу не
// переписывался, чата с ботом у него нет, и написать ему мы физически не сможем.
// Нажатие создаёт чат — и только после этого обещание «напишу, когда откроем» выполнимо.
function openBotChat(botUsername, startPayload) {
  const uname = String(botUsername || 'Ich_Deutsch_bot').trim().replace(/^@/, '') || 'Ich_Deutsch_bot';
  const suffix = startPayload ? `?start=${encodeURIComponent(startPayload)}` : '';
  const httpsUrl = `https://t.me/${uname}${suffix}`;
  try {
    const tg = window.Telegram && window.Telegram.WebApp;
    if (tg && typeof tg.openTelegramLink === 'function') {
      tg.openTelegramLink(httpsUrl);
      return;
    }
  } catch (_e) { /* не в Telegram — уходим на обычный переход ниже */ }
  // Вне Telegram (браузер, установленное приложение) обычный переход — рабочий путь.
  try { window.location.href = `tg://resolve?domain=${uname}${startPayload ? `&start=${encodeURIComponent(startPayload)}` : ''}`; } catch (_e) { /* ignore */ }
  setTimeout(() => {
    if (document.hidden) return;
    try { window.location.href = httpsUrl; } catch (_e) { /* ignore */ }
  }, 800);
}

function showAccessClosedGate(botUsername) {
  if (__accessClosedGateShown || __appBlockedGateShown) return;
  __accessClosedGateShown = true;
  const uname = String(botUsername || 'Ich_Deutsch_bot').trim().replace(/^@/, '') || 'Ich_Deutsch_bot';
  const de = (() => { try { return (localStorage.getItem('ui_lang') || '').toLowerCase() === 'de'; } catch (_e) { return false; } })();
  const title = de ? 'Zugang geschlossen' : 'Доступ закрыт';
  const body = de
    ? 'Der Administrator hat den Zugang zur App geschlossen. Wenn das ein Versehen ist, öffne den Bot und stelle eine Anfrage.'
    : 'Администратор закрыл доступ к приложению. Если это ошибка — откройте бота и отправьте запрос.';
  const btn = de ? 'Bot öffnen' : 'Открыть бота';
  const openBot = () => openBotChat(uname, 'access');
  try {
    const wrap = document.createElement('div');
    wrap.setAttribute('style', [
      'position:fixed', 'inset:0', 'z-index:2147483647', 'display:flex',
      'align-items:center', 'justify-content:center', 'padding:24px', 'box-sizing:border-box',
      'background:linear-gradient(160deg,#f59e0b 0%,#d97706 100%)', 'color:#fff',
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif', 'text-align:center',
    ].join(';'));
    wrap.innerHTML = `
      <div style="max-width:340px;display:flex;flex-direction:column;align-items:center;gap:16px">
        <div style="font-size:56px;line-height:1">🦊</div>
        <div style="font-size:22px;font-weight:700;line-height:1.25">${title}</div>
        <div style="font-size:15px;line-height:1.5;opacity:.94">${body}</div>
        <button type="button" style="margin-top:8px;border:0;border-radius:14px;padding:14px 22px;font-size:16px;font-weight:600;background:#fff;color:#b45309;cursor:pointer">${btn}</button>
      </div>`;
    wrap.querySelector('button').addEventListener('click', openBot);
    document.body.appendChild(wrap);
  } catch (_e) { /* ignore */ }
}

let __accessQueueGateShown = false;

// Экран «вы в очереди». Отдельный от «доступ закрыт» намеренно: это ДВА РАЗНЫХ
// состояния, и путать их нельзя. «Закрыт администратором» человеку, который просто
// пришёл по ссылке, сообщает, что его за что-то наказали, — а его никто не наказывал,
// он просто пришёл раньше, чем мы успели открыть дверь шире.
//
// Владелец 27.08.2026: отказанный обязан понимать, что с ним происходит, почему, и
// что от него ничего не требуется. Поэтому здесь три вещи и ровно в этом порядке:
// его НОМЕР, ПРИЧИНА человеческими словами и обещание НАПИСАТЬ САМИМ.
function showAccessQueueGate(position, botUsername) {
  if (__accessQueueGateShown || __appBlockedGateShown || __accessClosedGateShown) return;
  __accessQueueGateShown = true;
  const uname = String(botUsername || 'Ich_Deutsch_bot').trim().replace(/^@/, '') || 'Ich_Deutsch_bot';
  const de = (() => { try { return (localStorage.getItem('ui_lang') || '').toLowerCase() === 'de'; } catch (_e) { return false; } })();
  const num = Number(position) > 0 ? Number(position) : null;
  const title = de ? 'Du bist in der Warteschlange' : 'Вы в очереди на подключение';
  const numLine = num
    ? (de ? `Deine Nummer: ${num}` : `Ваш номер — ${num}`)
    : (de ? 'Dein Platz ist reserviert.' : 'Ваше место уже занято за вами.');
  const body = de
    ? 'Wir öffnen den Zugang portionsweise, damit die App für alle schnell bleibt. Sobald du dran bist, schreibe ich dir im Bot-Chat — du musst nichts tun.'
    : 'Мы открываем доступ порциями, чтобы приложение отвечало быстро всем, кто уже занимается. Как только очередь дойдёт до вас, я напишу в чат с ботом — делать ничего не нужно.';
  const btn = de ? 'Bot-Chat öffnen' : 'Открыть чат с ботом';
  const openBot = () => openBotChat(uname, 'queue');
  try {
    const wrap = document.createElement('div');
    wrap.setAttribute('style', [
      'position:fixed', 'inset:0', 'z-index:2147483647', 'display:flex',
      'align-items:center', 'justify-content:center', 'padding:24px', 'box-sizing:border-box',
      'background:linear-gradient(160deg,#0f766e 0%,#115e59 100%)', 'color:#fff',
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif', 'text-align:center',
    ].join(';'));
    wrap.innerHTML = `
      <div style="max-width:340px;display:flex;flex-direction:column;align-items:center;gap:16px">
        <div style="font-size:56px;line-height:1">🦊</div>
        <div style="font-size:22px;font-weight:700;line-height:1.25">${title}</div>
        <div style="font-size:34px;font-weight:800;line-height:1;letter-spacing:-.02em">${numLine}</div>
        <div style="font-size:15px;line-height:1.5;opacity:.94">${body}</div>
        <button type="button" style="margin-top:8px;border:0;border-radius:14px;padding:14px 22px;font-size:16px;font-weight:600;background:#fff;color:#115e59;cursor:pointer">${btn}</button>
      </div>`;
    wrap.querySelector('button').addEventListener('click', openBot);
    document.body.appendChild(wrap);
  } catch (_e) { /* ignore */ }
}

// Перехватчик СОСТОЯНИЯ ДВЕРИ — узкий и отдельный от токенных шимов.
// Те работают только вне Telegram (`if (!token || inTelegram()) return`), а очередь
// обязана показываться и внутри мини-аппа: именно оттуда приходит большинство людей.
// Поэтому смотрим только на 403 и только на поле `reason`, ничего больше не трогая.
function installAccessGateInterceptor() {
  if (typeof window === 'undefined' || typeof window.fetch !== 'function') return;
  if (window.__accessGateInterceptorInstalled) return;
  window.__accessGateInterceptorInstalled = true;
  const origFetch = window.fetch.bind(window);
  window.fetch = (input, init) => {
    const out = origFetch(input, init);
    try {
      return out.then((resp) => {
        try {
          if (resp && resp.status === 403) {
            resp.clone().json().then((j) => {
              const reason = String((j && j.reason) || '').trim();
              if (reason === 'in_queue') showAccessQueueGate(j.queue_position, j.bot_username);
              else if (reason === 'access_closed') showAccessClosedGate(j && j.bot_username);
            }).catch(() => {});
          }
        } catch (_e) { /* ignore */ }
        return resp;
      });
    } catch (_e) {
      return out;
    }
  };
}

function installAppTokenAuthShim() {
  if (typeof window === 'undefined' || typeof window.fetch !== 'function') return;
  if (window.__appAuthShimInstalled) return;
  window.__appAuthShimInstalled = true;
  try {
    const urlTok = appTokenFromLaunch();
    if (urlTok) { try { localStorage.setItem('app_browser_token_v1', urlTok); } catch (_e) { /* ignore */ } }
  } catch (_e) { /* ignore */ }
  const inTelegram = () => { try { return Boolean(window.Telegram?.WebApp?.initData); } catch (_e) { return false; } };
  const origFetch = window.fetch.bind(window);
  window.fetch = (input, init) => {
    try {
      if (typeof input !== 'string') return origFetch(input, init);
      const token = getAppToken();
      if (!token || inTelegram()) return origFetch(input, init);
      let u;
      try { u = new URL(input, window.location.origin); } catch (_e) { return origFetch(input, init); }
      if (u.origin !== window.location.origin || !u.pathname.startsWith('/api/')) return origFetch(input, init);
      const opts = { ...(init || {}) };
      const method = String(opts.method || 'GET').toUpperCase();
      const headers = new Headers(opts.headers || {});
      if (!headers.has('X-App-Token')) headers.set('X-App-Token', token);
      opts.headers = headers;
      // JSON POST bodies: add aqt so body-based resolvers see it (never overwrite an existing one).
      if (method !== 'GET' && method !== 'HEAD' && typeof opts.body === 'string' && opts.body.trim().startsWith('{')) {
        try {
          const parsed = JSON.parse(opts.body);
          if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && !('aqt' in parsed)) {
            parsed.aqt = token;
            opts.body = JSON.stringify(parsed);
          }
        } catch (_e) { /* leave body untouched */ }
      }
      let out;
      if ((method === 'GET' || method === 'HEAD') && !u.searchParams.has('aqt')) {
        u.searchParams.set('aqt', token);
        out = origFetch(u.toString(), opts);
      } else {
        out = origFetch(input, opts);
      }
      // Bot-left gate: the server 403s with {blocked:true, bot_username} once the user removed
      // the bot. Detect it centrally here (the full App has ~15 scattered fetches) and show the
      // branded return-to-bot screen. Only inspect 403s, and only clone then (cheap, rare).
      return out.then((resp) => {
        try {
          if (resp && resp.status === 403) {
            resp.clone().json().then((j) => {
              if (j && (j.blocked || j.reason === 'bot_blocked')) showAppBlockedGate(j.bot_username);
              // Access closed by an admin (/deny). Rare and deliberate — but it must read as
              // a decision, not as a crash, so it gets a calm card instead of a red banner.
              else if (j && (j.reason === 'access_closed'
                || (typeof j.error === 'string' && j.error.includes('закрыт администратором')))) {
                showAccessClosedGate(j.bot_username);
              }
            }).catch(() => {});
          }
        } catch (_e) { /* ignore */ }
        return resp;
      });
    } catch (_e) {
      return origFetch(input, init);
    }
  };
}

async function bootstrapApp() {
  // Состояние двери — раньше всех прочих шимов: человек в очереди не должен сначала
  // увидеть кусок приложения, а потом узнать, что его туда ещё не пустили.
  installAccessGateInterceptor();
  installDictTokenAuthShim();
  installAppTokenAuthShim();
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
  // Ссылка «Поделиться» на разбор отличий: wdiff_<токен>. Открываем тот же словарь
  // сразу на вкладке «Отличия» — гость видит разбор, но сохранять не может.
  if (/^slovarcheck$/i.test(answerStartParam)) {
    await bootstrapWordIntegrity('shared');
    return;
  }
  // Проверка СВОИХ слов — приходит каждому человеку, не только владельцу.
  if (/^meinewoerter$/i.test(answerStartParam)) {
    await bootstrapWordIntegrity('mine');
    return;
  }
  if (/^wdiff_/i.test(answerStartParam)) {
    await bootstrapDictionary(answerStartParam.replace(/^wdiff_/i, ''));
    return;
  }
  if (/^settings$/i.test(answerStartParam)) {
    await bootstrapSettings();
    return;
  }
  if (/^interactive$/i.test(answerStartParam)) {
    await bootstrapInteractive();
    return;
  }
  if (/^battles$/i.test(answerStartParam)) {
    await bootstrapBattles();
    return;
  }
  if (/^woerter$/i.test(answerStartParam)) {
    await bootstrapWordAudit();
    return;
  }
  if (/^zhaloby$/i.test(answerStartParam)) {
    await bootstrapComplaintReview();
    return;
  }
  if (/^lb/i.test(answerStartParam)) {
    await bootstrapLeaderboard(answerStartParam);
    return;
  }
  // Full app path. If launched as the standalone home-screen icon (app token in the URL,
  // outside Telegram), bake the token into the manifest link so a re-install stays authed.
  applyAppHomeScreenMeta();
  const canRender = await ensureFreshBundle();
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
