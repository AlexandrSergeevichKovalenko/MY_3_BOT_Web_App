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
  return String(params.get('startapp') || params.get('start_param') || '').trim();
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

async function bootstrapLeaderboard(startParam) {
  try { window.Telegram?.WebApp?.ready?.(); } catch (_e) { /* ignore */ }
  const { default: Leaderboard } = await import('./leaderboard/Leaderboard.jsx');
  ReactDOM.createRoot(document.getElementById('root')).render(
    <React.StrictMode>
      <Leaderboard startParam={startParam} />
    </React.StrictMode>,
  );
}

async function bootstrapApp() {
  const answerStartParam = getAnswerStartParam();
  if (/^ans_/i.test(answerStartParam)) {
    await bootstrapAnswerOverlay(answerStartParam);
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
