import React from 'react'
import ReactDOM from 'react-dom/client'
import { detectAppMode } from './utils/appMode.js'
// ./ в начале пути означает, что файл App.jsx находится в той же папке, что и текущий файл main.jsx
import App from './App.jsx'
import './theme.css'

// Импортируем стили для компонентов LiveKit для красивого отображения
import '@livekit/components-styles';

const appMode = detectAppMode();
const params = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : new URLSearchParams();
const isWebappPath = typeof window !== 'undefined'
  && (window.location.pathname === '/webapp' || window.location.pathname === '/webapp/review');
const hasTelegramUrlHints = params.has('tgWebAppData') || params.get('mode') === 'webapp' || isWebappPath;
const shouldTreatAsTelegram = appMode === 'telegram' || hasTelegramUrlHints;

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

// Полная последовательность команд: 
// "Эй, браузер, дай мне твой document. 
// В этом document найди элемент с id 'root'. 
// Теперь, ReactDOM, возьми этот элемент и сделай его своим 'корнем'. 
// А теперь, 'корень', нарисуй внутри себя компонент App"
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)

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
