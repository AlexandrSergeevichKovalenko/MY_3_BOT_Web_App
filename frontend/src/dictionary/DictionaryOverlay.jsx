import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';
import { WordBreakdown, useTts, SpeakButton, genderClass, resolveArticle, resolveNumber, resolveLemma, clean, cleanArticle as cleanArticleText, stripLeadingArticle, api, haptic, getInitData, getDictToken } from './WordBreakdown';
import BreakdownSkeleton from './BreakdownSkeleton';
import LiveExamples from './LiveExamples';
import { guessPair, buildDictionarySavePayload } from './saveUtils';
import { languageName, resolvePair, parsePairCode, displayPair } from './langPair.js';
import { humanizeDictError } from './errors.js';
import ProFeatureModal from '../components/ProFeatureModal';
import SaveWordHint from './SaveWordHint';

/**
 * Lightweight "quick dictionary" overlay — a compact bottom-sheet translator
 * launched as a Direct-Link Mini App via ?startapp=dict (see main.jsx). It mounts
 * ONLY this tiny screen and skips the heavy main App, so the circled chat-list
 * "Open" button opens an instant, neat dictionary instead of the full app.
 *
 * The deep word breakdown is the shared <WordBreakdown> component (also used by the
 * full dictionary inside the main app). This file only owns the compose UI + the
 * translate/save flow.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

// Tablet/wide-screen (iPad etc.) — NOT a handset. Mirrors detectTabletLikeViewport
// in App.jsx. On tablet the quick-dict should open FULLSCREEN like the main app
// (Telegram otherwise presents it as a narrow ~20% compact sheet). Phones untouched.
function isTabletLikeViewport() {
  try {
    // Размер ФИЗИЧЕСКОГО экрана, а не окна: окно на планшете — узкая шторка Telegram
    // шириной с телефон, и по нему планшет не опознать (см. utils/tabletFullscreen.js).
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

const QUICK_POS_LABELS = {
  noun: 'существительное', verb: 'глагол', adjective: 'прилагательное',
  adverb: 'наречие', pronoun: 'местоимение', preposition: 'предлог',
  conjunction: 'союз', phrase: 'выражение', participle: 'причастие',
};

// Языки и выбор пары живут в одном месте — ./langPair.js. Здесь раньше стояла своя
// копия правила «есть кириллица → ru-de, иначе de-ru» и свой список имён языков;
// с третьим языком две копии разошлись бы, а «table» правило назвало бы немецким.

// All dictionary errors go through the shared humanizer so a raw machine code
// (e.g. "cost_cap_exceeded") is never shown to the user — see ./errors.js. Kept as a
// thin local alias so the ~9 call sites below stay unchanged.
const friendlyError = humanizeDictError;

// «20 слов», «21 слово», «22 слова» — окно нормы называет число так, как человек говорит.
function wordsPlural(n) {
  const abs = Math.abs(Number(n) || 0) % 100;
  const tail = abs % 10;
  if (abs > 10 && abs < 20) return 'слов';
  if (tail === 1) return 'слово';
  if (tail >= 2 && tail <= 4) return 'слова';
  return 'слов';
}

// Время сброса нормы берём ИЗ ОТВЕТА СЕРВЕРА (reset_at — ближайшая полночь по Вене) и
// печатаем по Вене же. Поля нет или оно нечитаемо — предложение про сброс не печатаем
// вовсе: назвать час, которого нам не сказали, значит соврать человеку.
function resetClause(resetAt) {
  const raw = String(resetAt || '').trim();
  if (!raw) return '';
  const at = new Date(raw);
  if (Number.isNaN(at.getTime())) return '';
  try {
    const hhmm = new Intl.DateTimeFormat('ru-RU', {
      timeZone: 'Europe/Vienna', hour: '2-digit', minute: '2-digit', hour12: false,
    }).format(at);
    return ` Норма обновится в ${hhmm} по Вене.`;
  } catch (_e) {
    return '';
  }
}

// The German side of a quick result when it's a lone noun (single capitalized token)
// still lacking an article — mirrors the backend's noun-candidate check. When this is
// non-empty the article is being filled in the background and we should poll for it.
function germanNounAwaitingArticle(q) {
  if (!q || String(q.article || '').trim()) return '';
  // Слово нашлось в словаре статей — артикль там либо есть, либо его сознательно
  // не печатают (род неизвестен или их два). Опрашивать фоновый добор незачем.
  if (Array.isArray(q.entries) && q.entries.length) return '';
  let german = '';
  if (q.targetLang === 'de') german = String(q.translation || '').trim();
  else if (q.sourceLang === 'de') german = String(q.source || '').trim();
  if (!german || /\s/.test(german) || german[0] !== german[0].toUpperCase()) return '';
  return german;
}

// История поиска. Список общий с приложением — один ключ на оба словаря, поэтому
// найденное здесь видно и во вкладке «История» внутри приложения.
//
// Храним 60, показываем 6. Раньше хранились те же шесть, и любой поиск в быстром
// словаре обрезал историю приложения до шести — два словаря затирали друг другу память.
const RECENTS_KEY = 'dq_recents_v1';
const RECENTS_KEEP = 60;
const RECENTS_SHOW = 6;
function loadRecentsAll() {
  try {
    const raw = JSON.parse(localStorage.getItem(RECENTS_KEY) || '[]');
    return Array.isArray(raw) ? raw.filter((x) => typeof x === 'string').slice(0, RECENTS_KEEP) : [];
  } catch (_e) { return []; }
}
function loadRecents() {
  return loadRecentsAll().slice(0, RECENTS_SHOW);
}
function pushRecent(word) {
  const w = String(word || '').trim();
  if (!w) return loadRecents();
  const next = [w, ...loadRecentsAll().filter((x) => x.toLowerCase() !== w.toLowerCase())]
    .slice(0, RECENTS_KEEP);
  try { localStorage.setItem(RECENTS_KEY, JSON.stringify(next)); } catch (_e) { /* ignore */ }
  return next.slice(0, RECENTS_SHOW);
}

// "Tap a synonym to save it" hint — shown a few times total, then it stops nagging.
const CHIP_HINT_KEY = 'dq_chip_hint_count_v1';
const CHIP_HINT_MAX_SHOWS = 3;
function chipHintCount() {
  try { return parseInt(localStorage.getItem(CHIP_HINT_KEY) || '0', 10) || 0; } catch (_e) { return 0; }
}
function bumpChipHintCount() {
  try { localStorage.setItem(CHIP_HINT_KEY, String(chipHintCount() + 1)); } catch (_e) { /* ignore */ }
}

// Full-screen "return to bot" gate shown when the user has blocked/deleted the bot. The
// standalone home-screen dictionary is part of the bot; leaving the bot turns it off. A return
// (press «Запустить» in the bot) clears the server-side flag, so the next translate just works.
// Hardcoded final fallback so the button always has a real handle even if the API omits it.
const DICT_BOT_USERNAME_FALLBACK = 'Ich_Deutsch_bot';

// Открыть бота (при желании — сразу на нужном экране мини-аппа через startapp).
// Одна реализация на оба места: экран «бот заблокирован» и окно дневной нормы. Раньше
// она лежала внутри экрана-заглушки, и второй вызывающий получил бы её копию.
function openBotLink(botUsername, startParam = '') {
  const uname = (String(botUsername || '').replace(/^@/, '').trim()) || DICT_BOT_USERNAME_FALLBACK;
  const start = String(startParam || '').trim();
  const https = `https://t.me/${uname}${start ? `?startapp=${start}` : ''}`;
  const tgApp = window?.Telegram?.WebApp;
  try {
    // Inside Telegram (initData present) → native opener. Otherwise we're the detached PWA.
    if (tgApp && tgApp.initData && typeof tgApp.openTelegramLink === 'function') {
      tgApp.openTelegramLink(https);
      return;
    }
  } catch (_e) { /* fall through */ }
  // Detached home-screen PWA: jump straight into the Telegram app via the tg:// scheme.
  // If that scheme isn't handled (no app), fall back to the universal https link — but skip
  // the fallback if the app actually opened (page went hidden), so we don't also load t.me.
  let done = false;
  try {
    window.location.href = `tg://resolve?domain=${uname}${start ? `&startapp=${start}` : ''}`;
  } catch (_e) { /* ignore */ }
  setTimeout(() => {
    if (done || document.hidden) return;
    done = true;
    try { window.location.href = https; } catch (_e) { /* ignore */ }
  }, 800);
}

function DictBlockedGate({ botUsername }) {
  const openBot = () => openBotLink(botUsername);
  return (
    <div className="ans-root dq-scroll">
      <div className="ans-card dq-card">
        <div className="dq-gate">
          <div className="dq-gate-badge">📖</div>
          <h2 className="dq-gate-title">Словарь работает вместе с ботом</h2>
          <p className="dq-gate-text">
            Похоже, бот удалён или заблокирован. Быстрый словарь — часть бота, поэтому переводы
            доступны, пока бот у тебя запущен.
          </p>
          <p className="dq-gate-text">
            Вернись в бота и нажми «Запустить» — словарь тут же снова заработает.
          </p>
          <button type="button" className="dq-gate-btn" onClick={openBot}>
            Открыть бота
          </button>
        </div>
      </div>
    </div>
  );
}

export default function DictionaryOverlay({ onClose } = {}) {
  const [query, setQuery] = useState('');
  const [phase, setPhase] = useState('idle'); // idle|loading|done|error
  const [quick, setQuick] = useState(null);   // { source, target, translation, sourceLang, targetLang, direction }
  const [item, setItem] = useState(null);     // rich GPT item (for enrich + canonical save)
  const [enrich, setEnrich] = useState('idle'); // idle|loading|streaming|done|error
  const [streamSections, setStreamSections] = useState(() => new Set()); // section names arrived
  const [deepLoading, setDeepLoading] = useState(false); // background enrichment poll
  const [deepId, setDeepId] = useState('');   // shareable id (same «Поделиться» as «Полный разбор»)
  const [sharing, setSharing] = useState(false);
  const [save, setSave] = useState('idle');   // idle|saving|done
  const [cardSave, setCardSave] = useState('idle'); // idle|done — «Учить» (SRS)
  // Дневная норма сохранений кончилась. Держим ответ сервера целиком: числа в окне —
  // его, а не наши. {used, limit, resetAt}
  const [saveLimit, setSaveLimit] = useState(null);
  // Плашка «такого слова в немецком нет» — только когда дверь НЕ смогла решить сама.
  const [wordHint, setWordHint] = useState(null); // {word, suggestion, why}
  const [savedChips, setSavedChips] = useState(() => new Set()); // synonyms/collocations tapped to save
  const [error, setError] = useState('');
  // Пара, которую видит человек в панели. Пересчитывается на КАЖДОЕ нажатие клавиши:
  // начал писать по-русски — стало «Русский → Deutsch», по-немецки — наоборот.
  // Если на экране уже лежит ответ ровно на этот текст, показываем не догадку по
  // алфавиту, а то, в какую сторону его вправду перевели.
  //
  // Пока идёт перевод, прежний ответ намеренно НЕ учитывается: он про то, что было
  // до нажатия, и панель на секунду показывала бы прошлую пару.
  const panelPair = useMemo(() => displayPair(query, {
    result: (quick && phase !== 'loading') ? { text: quick.source, pair: parsePairCode(quick.direction) } : null,
  }), [query, quick, phase]);
  const [autoOn, setAutoOn] = useState(() => {
    try { return localStorage.getItem('dq_auto') !== '0'; } catch (_e) { return true; }
  });
  const [chipHint, setChipHint] = useState(false); // brief "tap a synonym to save it" toast
  const [blocked, setBlocked] = useState(null); // {botUsername} when the user left the bot → gate screen
  // Закладки быстрого словаря — те же, что во внутреннем.
  const [tab, setTab] = useState('search');
  const [historyList, setHistoryList] = useState(() => loadRecentsAll());
  const [mine, setMine] = useState([]);
  const [mineState, setMineState] = useState('idle'); // idle | loading | ready | error
  const [mineQuery, setMineQuery] = useState('');
  // Открытая карточка своего слова. Разбор приезжает ВМЕСТЕ со списком, поэтому
  // открытие не стоит ни запроса, ни денег.
  const [mineCard, setMineCard] = useState(null);

  // Слово из списка открывается КАРТОЧКОЙ, а не отправляется в поиск.
  //
  // Сначала я сделал наоборот: нажатие подставляло фразу в строку поиска и переводило
  // заново. Владелец 10.08.2026 справедливо возразил — зачем переводить то, что уже
  // переведено и сохранено. Это и лишние деньги, и лишнее ожидание, и человек теряет
  // место, на котором стоял в списке.
  const openMineCard = useCallback((row) => {
    const raw = row && row.response_json;
    let card = raw;
    if (typeof raw === 'string') {
      try { card = JSON.parse(raw); } catch (_e) { card = null; }
    }
    if (!card || typeof card !== 'object') {
      // Разбора у строки нет (тонкая запись) — показываем хотя бы пару «слово — перевод»,
      // а не пустоту.
      card = {
        word_de: clean(row?.word_de || row?.translation_de),
        word_ru: clean(row?.word_ru || row?.translation_ru),
        translation_ru: clean(row?.translation_ru || row?.word_ru),
        translation_de: clean(row?.translation_de || row?.word_de),
      };
    }
    setMineCard(card);
  }, []);
  const [mineHasMore, setMineHasMore] = useState(false);
  const MINE_PAGE = 50;

  // Свои слова тянем ТОЛЬКО когда человек открыл эту закладку: экран поиска не должен
  // платить за список, который на нём не виден.
  //
  // Порциями и с поиском. Первая версия показывала первые сто строк и упиралась в конец:
  // при пятнадцати тысячах слов это не список, а случайная выборка. Владелец справедливо
  // спросил, как этим пользоваться.
  const loadMine = useCallback(async ({ reset = false, needle = '' } = {}) => {
    const offset = reset ? 0 : mine.length;
    setMineState(reset ? 'loading' : 'more');
    try {
      const data = await api('/api/webapp/dictionary/cards', {
        limit: MINE_PAGE, offset, search: needle || undefined,
      });
      const items = Array.isArray(data?.items) ? data.items : [];
      setMine((prev) => (reset ? items : [...prev, ...items]));
      setMineHasMore(items.length === MINE_PAGE);
      setMineState('ready');
    } catch (_e) {
      setMineState('error');
    }
  }, [mine.length]);

  useEffect(() => {
    if (tab !== 'mine' || mineState !== 'idle') return;
    void loadMine({ reset: true });
  }, [tab, mineState, loadMine]);

  // Поиск по своим словам — с паузой, чтобы не дёргать сервер на каждую букву.
  useEffect(() => {
    if (tab !== 'mine') return undefined;
    const id = setTimeout(() => { void loadMine({ reset: true, needle: mineQuery.trim() }); }, 400);
    return () => clearTimeout(id);
  }, [mineQuery]);   // eslint-disable-line react-hooks/exhaustive-deps

  // Какую статью из списка человек выбрал. Одно написание — несколько словарных
  // статей («толстый» → dick прилаг. / der Dicke сущ.), и выбирать между ними
  // обязан человек: любой автоматический выбор здесь — это догадка, а именно
  // догадка 11.08.2026 превратила прилагательное в толстяка. По умолчанию 0 —
  // самая частотная, но список рядом и виден.
  const [chosen, setChosen] = useState(0);
  const lastAutoRef = useRef(''); // text already auto/manually translated (debounce dedupe)
  const chipHintDoneRef = useRef(false); // shown for the current breakdown already
  const seqRef = useRef(0);
  const inputRef = useRef(null);
  const streamAbortRef = useRef(null); // aborts an in-flight breakdown SSE stream
  const lookupPromiseRef = useRef(null); // in-flight breakdown promise (shared by tap + save)
  const savedGermanRef = useRef(''); // немецкое слово последнего сохранения — его и проверяем
  const correctionCacheRef = useRef(new Map()); // typed phrase → proofread form (dedupes «В словаре»/«Учить»)
  const tts = useTts();

  // Surface the "tap a word in Synonyms/Antonyms to save it" hint the first few times
  // the deep breakdown (where those tappable blocks live) appears. Auto-dismisses.
  useEffect(() => {
    if (!item || chipHintDoneRef.current || chipHintCount() >= CHIP_HINT_MAX_SHOWS) return undefined;
    chipHintDoneRef.current = true;
    bumpChipHintCount();
    setChipHint(true);
    const id = setTimeout(() => setChipHint(false), 3000);
    return () => clearTimeout(id);
  }, [item]);

  useEffect(() => {
    try {
      tg?.ready?.();
      tg?.expand?.();
      // On tablet, request true fullscreen so the quick-dict fills the screen like
      // the main app instead of Telegram's narrow compact sheet. Phone untouched;
      // older clients that lack requestFullscreen simply reject (caught).
      if (isTabletLikeViewport() && typeof tg?.requestFullscreen === 'function') {
        Promise.resolve(tg.requestFullscreen()).catch(() => { /* unsupported client */ });
        try { document.documentElement.setAttribute('data-dq-tablet', '1'); } catch (_e) { /* ignore */ }
      }
      tg?.setHeaderColor?.('secondary_bg_color');
      tg?.disableVerticalSwipes?.();
    } catch (_e) { /* ignore */ }
    const applyScheme = () => {
      const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
      try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
    };
    applyScheme();
    try { tg?.onEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ }
    setTimeout(() => { try { inputRef.current?.focus(); } catch (_e) { /* ignore */ } }, 250);
    return () => { try { tg?.offEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ } };
  }, []);

  // Выбранная статья. Всё, что ниже — заголовок, артикль, часть речи, озвучка,
  // разбор и сохранение — берётся из НЕЁ, а не из строки переводчика.
  const entries = Array.isArray(quick?.entries) ? quick.entries : [];
  const chosenEntry = entries[Math.min(chosen, Math.max(entries.length - 1, 0))] || null;

  // ПРЕДМЕТ РАЗБОРА — то слово, о котором мы просим карточку. Выбрал человек «dick»
  // из списка — разбор идёт про «dick», а не про то, что он набрал: иначе выбор
  // остаётся украшением экрана, а модель заново гадает, о чём речь, и снова может
  // выбрать «der Dicke». Существительное отправляем С АРТИКЛЕМ — так словарь
  // отличает «der Kiefer» (челюсть) от «die Kiefer» (сосна).
  // Держим в ref, чтобы разбор и поток не пересоздавались на каждый тап по списку.
  const subjectRef = useRef(null);
  subjectRef.current = () => {
    if (chosenEntry) {
      const head = String(chosenEntry.headword || '').trim();
      const article = String(chosenEntry.gender || '').trim();
      const word = (chosenEntry.pos === 'noun' && article) ? `${article} ${head}` : head;
      if (word) return { text: word, lang: 'de' };
    }
    const typed = query.trim();
    return { text: typed, lang: guessPair(typed).source };
  };

  // German text of the current result, for pronunciation.
  const germanText = (() => {
    if (chosenEntry) return String(chosenEntry.headword || '').trim();
    if (item?.word_de) return String(item.word_de).trim();
    if (!quick) return '';
    if (quick.sourceLang === 'de') return quick.source;
    if (quick.targetLang === 'de') return quick.translation;
    return '';
  })();

  // The instant translate uses fast non-LLM engines that mishandle typos /
  // compounds. Once the LLM breakdown arrives it carries the corrected German form
  // and a proper translation, so prefer those for the headword. word_de already
  // includes the article; we render the article in a colored span separately, so
  // strip it here to avoid "die die Dunstabzugshaube".
  const corrDe = stripLeadingArticle(String(item?.word_de || '').trim());
  const bestRu = String(
    item?.translation_ru
    || item?.word_ru
    || item?.meanings?.primary?.value
    || '',
  ).trim();
  // Strip the article from EVERY German fallback (not just word_de). The colored
  // article renders in its own span, so an un-stripped "das Kabel" here plus the
  // span produced "der das Kabel".
  const headTranslation = chosenEntry
    ? (quick?.targetLang === 'de'
      ? (chosenEntry.headword || '—')
      : (chosenEntry.translation || chosenEntry.translations?.[0] || '—'))
    : (quick?.targetLang === 'de'
      ? (corrDe || stripLeadingArticle(quick?.translation) || '—')
      : (bestRu || quick?.translation || '—'));
  // Текст для обратного поиска — ровно то, что человек видит крупно как перевод.
  // Пусто — значит переворачивать нечего (ещё не переводили, или ответ пуст), и
  // кнопка ⇄ гаснет, а не делает вид, что работает.
  const reverseQuery = (() => {
    const t = String(headTranslation || '').trim();
    if (!t || t === '—') return '';
    if (t === query.trim()) return '';  // искать то же самое — не действие
    return t;
  })();
  const headSource = (quick?.sourceLang === 'de')
    ? ((chosenEntry ? chosenEntry.headword : '') || corrDe || stripLeadingArticle(quick?.source) || '')
    : (quick?.source || '');
  // One clean der/die/das for both the source and translation spans. Артикль
  // выбранной статьи главнее всего: он приехал вместе с её частью речи и родом,
  // а не был приклеен к чужому слову отдельным запросом.
  const dqArticle = chosenEntry ? String(chosenEntry.gender || '') : resolveArticle(item, quick);
  // Показанная поверхность может быть формой слова. Тогда артикль у неё свой («die»
  // у именительного множественного), а само слово подписывается отдельной строкой —
  // как это делают dict.cc и DWDS. Артикль леммы берём из разбора, если он уже пришёл.
  const dqNumber = resolveNumber(item, quick);
  const dqLemma = resolveLemma(item, quick);
  const dqLemmaArticle = dqNumber === 'pl' ? cleanArticleText(item?.article) : '';
  const correctedNote = (corrDe && quick?.sourceLang === 'de'
    && corrDe.toLowerCase() !== String(quick?.source || '').trim().toLowerCase())
    ? corrDe : '';

  // Resolve the headword's R2 audio URL as soon as the German text is known (no synthesis —
  // zero cost) so tapping 🔊 plays a cached clip instantly; an un-cached clip synthesises only
  // on the tap itself.
  const { resolveUrls: resolveTtsUrls } = tts;
  useEffect(() => {
    if (germanText) resolveTtsUrls([germanText], 'de-DE');
  }, [germanText, resolveTtsUrls]);

  const translate = useCallback(async (overrideText) => {
    const text = (typeof overrideText === 'string' ? overrideText : query).trim();
    if (!text || phase === 'loading') return;
    if (text !== query) setQuery(text);
    lastAutoRef.current = text; // mark as handled so the auto-translate effect won't repeat it
    const mySeq = ++seqRef.current;
    tts.stop();
    setPhase('loading'); setError(''); setItem(null); setEnrich('idle'); setSave('idle'); setCardSave('idle'); setSavedChips(new Set());
    setDeepId(''); setStreamSections(new Set());
    try { streamAbortRef.current?.abort(); } catch (_e) { /* ignore */ }
    streamAbortRef.current = null;
    lookupPromiseRef.current = null;
    chipHintDoneRef.current = false; setChipHint(false);
    haptic('light');
    try {
      // Направление — по алфавиту, и только по нему (langPair.js). Ручного
      // переключателя больше нет: ⇄ переворачивает СОДЕРЖИМОЕ, а не режим, и для
      // перевёрнутого текста алфавит спрашивают заново. Прошлый ответ здесь не
      // участвует вовсе — именно он раньше и залипал.
      const pair = resolvePair(text);
      const data = await api('/api/translate/quick', {
        text, source_lang: pair.source, target_lang: pair.target,
      });
      if (mySeq !== seqRef.current) return;
      const detected = String(data?.detected_source_lang || pair.source).toLowerCase();
      const targetLang = detected === pair.target ? pair.source : pair.target;
      // ЗДЕСЬ РАНЬШЕ СТОЯЛО setForcedDir(`${detected}-${targetLang}`) — и оно
      // выключало автоопределение навсегда. Направление ответа теперь никуда не
      // закрепляется: панель читает его из самой карточки (quick.direction), и
      // только пока в поле лежит ТОТ ЖЕ текст.
      const nextQuick = {
        source: text,
        translation: String(data?.translation || '').trim(),
        sourceLang: detected,
        targetLang,
        direction: `${detected}-${targetLang}`,
        provider: String(data?.provider || '').trim(),
        // Article for a single German noun, resolved instantly from the local
        // reference so "die Wortverbindung" shows without the full breakdown.
        article: String(data?.article || '').trim(),
        // Часть речи из нашего банка слов — переводчики её не отдают.
        partOfSpeech: String(data?.part_of_speech || '').trim(),
        // Число и слово, формой которого оказалась поверхность: без них артикль
        // выбрать нельзя («die Probleme», а не «das Probleme»), а склонение
        // построилось бы от формы.
        number: String(data?.grammatical_number || '').trim(),
        lemma: String(data?.lemma_de || '').trim(),
        // Словарные статьи. Приходят, когда слово нам знакомо: тогда переводчика
        // не спрашивали вовсе и грамматика в ответе настоящая, а не выведенная из
        // написания. Пусто — значит ответила машина, и это подписано на экране.
        entries: Array.isArray(data?.entries) ? data.entries : [],
        machine: !!data?.machine,
      };
      setChosen(0);
      setQuick(nextQuick);
      setPhase('done'); haptic('ok');
      pushRecent(text);   // пополняем историю, показывает её своя закладка
      setHistoryList(loadRecentsAll());
      // A German noun whose article missed the instant Wiktionary lookup gets its
      // der/die/das filled by a background LLM job that patches the cache. Poll for it
      // so it appears on its own — never make the user press «Перевести» a second time.
      if (germanNounAwaitingArticle(nextQuick)) {
        (async () => {
          for (const delay of [900, 1300, 1600, 2000, 2500]) {
            await new Promise((r) => setTimeout(r, delay));
            if (mySeq !== seqRef.current) return;
            let art = '';
            let num = '';
            let lem = '';
            try {
              const a = await api('/api/translate/quick/article', {
                text, source_lang: pair.source, target_lang: pair.target,
              });
              art = String(a?.article || '').trim();
              num = String(a?.number || '').trim();
              lem = String(a?.lemma || '').trim();
            } catch (_e) { /* keep polling */ }
            if (mySeq !== seqRef.current) return;
            if (art) {
              setQuick((prev) => (prev && !prev.article
                ? { ...prev, article: art, number: num || prev.number, lemma: lem || prev.lemma }
                : prev));
              return;
            }
          }
        })();
      }
    } catch (e) {
      if (mySeq !== seqRef.current) return;
      // The user blocked/deleted the bot → the dictionary is gated. Show the return screen
      // instead of a raw error; a return to the bot unlocks it again on the next translate.
      if (e && e.status === 403 && (e.payload?.blocked || e.payload?.reason === 'bot_blocked')) {
        setBlocked({ botUsername: String(e.payload?.bot_username || '').trim() });
        setPhase('idle'); haptic('bad');
        return;
      }
      setError(friendlyError(e)); setPhase('error'); haptic('bad');
    }
  }, [query, phase, tts]);

  // Drop the current result and return to the initial compose screen. Called when the
  // field is emptied (manually or via the × button) so a stale card never lingers.
  const resetResult = useCallback(() => {
    seqRef.current += 1; // abort any in-flight translate/lookup
    tts.stop();
    setQuick(null); setItem(null); setEnrich('idle'); setPhase('idle'); setChosen(0);
    setError(''); setSave('idle'); setCardSave('idle'); setSavedChips(new Set());
    setDeepId(''); setStreamSections(new Set());
    lastAutoRef.current = '';
    try { streamAbortRef.current?.abort(); } catch (_e) { /* ignore */ }
    streamAbortRef.current = null;
    lookupPromiseRef.current = null;
  }, [tts]);

  // Выбрали другую статью — прежний разбор был про ДРУГОЕ слово, и оставлять его
  // на экране нельзя. Сбрасываем карточку; новую человек откроет тем же «Подробным
  // разбором», уже про выбранное слово.
  const chooseEntry = useCallback((index) => {
    setChosen((prev) => {
      if (prev === index) return prev;
      seqRef.current += 1;
      try { streamAbortRef.current?.abort(); } catch (_e) { /* ignore */ }
      streamAbortRef.current = null;
      lookupPromiseRef.current = null;
      tts.stop();
      setItem(null); setEnrich('idle'); setDeepId(''); setStreamSections(new Set());
      setSave('idle'); setCardSave('idle'); setSavedChips(new Set());
      return index;
    });
    haptic('light');
  }, [tts]);

  const clearInput = useCallback(() => {
    setQuery('');
    resetResult();
    haptic('light');
    try { inputRef.current?.focus(); } catch (_e) { /* ignore */ }
  }, [resetResult]);

  // Авто-перевод по паузе. Ждать надо ровно столько, сколько человек думает над
  // СЛЕДУЮЩИМ словом, а не сколько он печатает текущее.
  //
  // Было 800 мс на любой текст. Владелец 09.08.2026: «задумался на секунду — а меня
  // уже выбривает, перевод готов». Так и есть: секундная пауза посреди фразы длиннее
  // 800 мс, и отсчёт начинается заново с каждой буквы, поэтому спасает только
  // безостановочный набор.
  //
  // Сколько ждут Google и DeepL, публично неизвестно — я искал и не нашёл, поэтому
  // цифры ниже наши, а не «как у них». Но разница с ними не в миллисекундах:
  // у них ранний перевод БЕЗВРЕДЕН, он появляется в отдельной половине экрана и
  // ничего не двигает. У нас он разворачивает карточку и тратит запрос, поэтому
  // ошибиться в раннюю сторону нам дороже.
  //
  // Отсюда два срока вместо одного. Одно слово человек дописывает не задумываясь —
  // ему хватает короткой паузы. Фраза пишется с раздумьями между словами, и ей нужен
  // срок, переживающий «секунду на подумать».
  const autoDelayMs = query.trim().includes(' ') ? 2200 : 900;
  useEffect(() => {
    if (!autoOn) return undefined;
    const t = query.trim();
    if (!t || t === lastAutoRef.current || phase === 'loading') return undefined;
    const id = setTimeout(() => translate(t), autoDelayMs);
    return () => clearTimeout(id);
  }, [query, phase, translate, autoOn, autoDelayMs]);

  const toggleAuto = useCallback(() => {
    setAutoOn((v) => {
      const next = !v;
      try { localStorage.setItem('dq_auto', next ? '1' : '0'); } catch (_e) { /* ignore */ }
      return next;
    });
    haptic('light');
  }, []);

  // The first lookup returns a FAST "core" item; the heavy parts enrich in the
  // background. Poll the status endpoint and swap in the fuller item as it arrives.
  const pollEnrichment = useCallback(async (lookupId, base) => {
    if (!lookupId) return;
    const mySeq = seqRef.current; // bumped by translate(); abort if a new lookup starts
    setDeepLoading(true);
    try {
      for (let i = 0; i < 24; i += 1) {
        // Fast first check (the mini breakdown often lands in ~1–2s), then a tight
        // cadence so a ready result shows almost immediately instead of after a 3s gap.
        await new Promise((r) => setTimeout(r, i === 0 ? 500 : 1200));
        if (mySeq !== seqRef.current) return;
        let data;
        try { data = await api('/api/webapp/dictionary/status', { lookup_id: lookupId }); }
        catch (_e) { break; }
        if (mySeq !== seqRef.current) return;
        if (data?.item) {
          const merged = { ...data.item, __direction: base?.__direction, __language_pair: base?.__language_pair };
          setItem(merged);
        }
        if (data?.deep_id) setDeepId(String(data.deep_id));
        if (String(data?.status || '') === 'ready' || data?.enrichment_pending === false) break;
      }
    } finally {
      if (mySeq === seqRef.current) setDeepLoading(false);
    }
  }, []);

  // Promote a fetched dictionary response into the visible breakdown + start enrichment
  // polling. Returns the rich item (or null). Shared by the streaming-final and the
  // non-stream fallback paths so they stay in lock-step.
  const applyDeep = useCallback((data) => {
    const rich = data?.item || null;
    if (!rich) return null;
    // РАЗБОР ОБЯЗАН БЫТЬ ПРО ТО СЛОВО, КОТОРОЕ СПРОСИЛИ. Человек набрал «Blad»,
    // заголовок показал верное «толстый», а разбор под ним приехал про «das Blatt»:
    // лист, страница, die Seite. Причину чиним на бэкенде, но карточку про чужое
    // слово нельзя показывать ни при какой причине — лучше без разбора, чем про
    // другое слово. Проверяем только когда спрашивали немецкую статью: на русский
    // запрос немецкий заголовок отличается законно.
    const asked = subjectRef.current();
    if (asked.lang === 'de') {
      const want = stripLeadingArticle(asked.text).trim().toLowerCase();
      const got = stripLeadingArticle(String(rich.word_de || '')).trim().toLowerCase();
      if (want && got && want !== got) {
        // eslint-disable-next-line no-console
        console.warn('разбор пришёл про другое слово:', { want, got });
        // МОЛЧАТЬ НЕЛЬЗЯ. Первая версия этой страховки просто возвращала null, и
        // человек, нажав «Подробный разбор», не получал НИЧЕГО — ни карточки, ни
        // объяснения (владелец 11.08.2026: «нажимаю и просто отбрасывает»). Пустой
        // экран человек читает как поломку, и он прав.
        setEnrich('error');
        setError('Разбор для этого слова пока недоступен — попробуйте ещё раз через минуту.');
        return null;
      }
    }
    rich.__direction = String(data?.direction || rich.__direction || '').trim();
    rich.__language_pair = data?.language_pair || null;
    setItem(rich);
    setEnrich('done');
    if (data?.deep_id) setDeepId(String(data.deep_id));
    if (data?.enrichment_pending && data?.lookup_id) {
      pollEnrichment(data.lookup_id, rich);
    }
    return rich;
  }, [pollEnrichment]);

  // Non-stream breakdown — the proven, atomic path. Used as the fallback when SSE
  // streaming is unsupported or fails (see runLookup). Errors surface loudly.
  const fetchDeepBreakdown = useCallback(async () => {
    const { text: w, lang: lookupLang } = subjectRef.current();
    const data = await api('/api/webapp/dictionary', { word: w, lookup_lang: lookupLang });
    const rich = applyDeep(data);
    setEnrich(rich ? 'done' : 'error');
    return rich;
  }, [query, applyDeep]);

  // Streaming breakdown — opens the SSE endpoint and merges each structured section
  // into `item` the moment it lands (head → meanings → grammar → examples → extra), so
  // the card fills progressively behind a skeleton. A `done` event carries the fully
  // decorated item (reconciled server-side through the same pipeline as the non-stream
  // path) which replaces the partial. Returns the final rich item, or null if the stream
  // ended without one (caller then falls back). A 4xx (e.g. daily limit) throws with
  // .status so the caller surfaces it instead of falling back.
  const streamLookup = useCallback(async () => {
    const { text: w, lang: lookupLang } = subjectRef.current();
    const mySeq = seqRef.current;
    const controller = new AbortController();
    streamAbortRef.current = controller;

    const dictToken = getDictToken();
    const streamHeaders = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
    if (dictToken) streamHeaders['X-Dict-Token'] = dictToken;
    const resp = await fetch('/api/webapp/dictionary/stream', {
      method: 'POST',
      headers: streamHeaders,
      body: JSON.stringify({ initData: getInitData(), ...(dictToken ? { dqt: dictToken } : {}), word: w, lookup_lang: lookupLang }),
      signal: controller.signal,
    });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      const err = new Error(data?.error || 'Fehler');
      err.status = resp.status; err.payload = data;
      throw err;
    }
    // Cached hit / immediate result comes back as plain JSON, not a stream.
    if ((resp.headers.get('Content-Type') || '').includes('application/json')) {
      const data = await resp.json().catch(() => ({}));
      return applyDeep(data);
    }
    if (!resp.body || typeof resp.body.getReader !== 'function') {
      throw new Error('stream unsupported');
    }

    if (mySeq === seqRef.current) { setEnrich('streaming'); setStreamSections(new Set()); }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let sawSection = false;
    let finalRich = null;

    const handleFrame = (block) => {
      let ev = 'message';
      const dataLines = [];
      for (const line of block.split('\n')) {
        if (line.startsWith('event:')) ev = line.slice(6).trim();
        else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
      }
      if (!dataLines.length) return;
      let payload;
      try { payload = JSON.parse(dataLines.join('\n')); } catch (_e) { return; }
      if (ev === 'section') {
        sawSection = true;
        const fields = (payload && payload.fields) || {};
        if (mySeq === seqRef.current) {
          setItem((prev) => ({ ...(prev || {}), ...fields }));
          setStreamSections((prev) => new Set(prev).add(String(payload?.name || '')));
        }
      } else if (ev === 'done') {
        finalRich = applyDeep(payload);
      } else if (ev === 'error') {
        throw new Error(payload?.error || 'stream error');
      }
    };

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (mySeq !== seqRef.current) { try { controller.abort(); } catch (_e) { /* ignore */ } return finalRich; }
      buffer += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buffer.indexOf('\n\n')) >= 0) {
        const frame = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        handleFrame(frame);
      }
    }
    if (buffer.trim()) handleFrame(buffer);
    if (!finalRich && !sawSection) throw new Error('empty stream');
    return finalRich;
  }, [query, applyDeep]);

  // Full GPT breakdown. Streams by default; falls back to the atomic path on any
  // transport/streaming failure. A real 4xx (limit / bad request) is surfaced, not
  // retried, to avoid a confusing double request. Returns the FINAL rich item — while a
  // stream is mid-flight, a Save tap reuses the same promise so it never persists a
  // half-streamed item.
  const runLookup = useCallback(async () => {
    if (item && enrich === 'done') return item;
    if (lookupPromiseRef.current) return lookupPromiseRef.current;
    setEnrich('loading'); setError('');
    const p = (async () => {
      try {
        const rich = await streamLookup();
        if (rich) return rich;
        return await fetchDeepBreakdown(); // stream ended with no final item
      } catch (e) {
        if (e && e.name === 'AbortError') throw e;
        if (e && e.status && e.status >= 400 && e.status < 500) {
          setEnrich('error'); setError(friendlyError(e)); throw e;
        }
        try {
          return await fetchDeepBreakdown();
        } catch (e2) {
          setEnrich('error'); setError(friendlyError(e2)); throw e2;
        }
      }
    })();
    lookupPromiseRef.current = p;
    try {
      return await p;
    } finally {
      lookupPromiseRef.current = null;
    }
  }, [item, enrich, streamLookup, fetchDeepBreakdown]);

  // Proofread the SOURCE phrase before it lands in the shared dictionary, so a typo, a
  // wrong/absent article, a wrong case or preposition is not saved verbatim (the user
  // asked to be silently corrected). ONE cheap LLM call, cached server-side AND per
  // typed phrase here (dedupes the «В словаре» + «Учить» double-save). Best-effort: on
  // any failure we return exactly what the user typed — a save NEVER waits on or dies
  // with this call.
  const proofreadSource = useCallback(async (typed) => {
    const t = String(typed || '').trim();
    if (!t) return t;
    const memo = correctionCacheRef.current;
    if (memo.has(t)) return memo.get(t) || t;
    try {
      const c = await api('/api/translate/quick/correct', {
        text: t, source_lang: quick?.sourceLang || undefined,
      });
      const fixed = String(c?.corrected || '').trim();
      memo.set(t, fixed);
      return fixed || t;
    } catch (_e) {
      return t; // cap reached / offline / error — keep the user's text, still save it
    }
  }, [quick]);

  // Canonical save through the lookup→save pipeline; returns the save response
  // (incl. entry_id) so callers can chain (e.g. add to the SRS deck).
  const persistEntry = useCallback(async () => {
    const typed = query.trim();
    // Save straight from the quick translation — do NOT trigger the full GPT breakdown
    // just to save. The user asked to save the simple translation without generating or
    // showing the whole explanation (no LLM cost, no card). If they already opened
    // «Подробный разбор» (item ready), save that richer item instead. We fold in the
    // article we already resolved cheaply so a saved noun keeps its der/die/das
    // ("die Rotznase") — the deeper grammar table is built by the engine on view.
    const rich = (item && enrich === 'done') ? item : null;

    // Silently correct the typed phrase first, and reflect it in the field + card so the
    // user sees (and saves) the clean form. Mark it handled so the auto-translate effect
    // doesn't re-fire a fresh translation on the corrected text.
    // Вычитка нужна только тому, чего мы не знаем. Слово нашлось в словаре статей —
    // значит оно написано верно, и платный запрос к корректору здесь лишний.
    const corrected = chosenEntry ? typed : await proofreadSource(typed);
    if (corrected && corrected !== typed) {
      lastAutoRef.current = corrected;
      setQuery(corrected);
      setQuick((prev) => (prev ? { ...prev, source: corrected } : prev));
    }

    // Сохраняем РАЗРЕШЁННЫЙ артикль, а не сырой быстрый: у формы множественного это
    // «die», а не род леммы. Иначе в ОБЩИЙ пул уезжало «das Probleme» — и доставалось
    // всем, кто потом искал это слово.
    const art = chosenEntry ? String(chosenEntry.gender || '') : resolveArticle(item, quick);
    const hasArticle = (s) => /^(der|die|das)\s/i.test(String(s || ''));
    let sourceText = corrected;
    let quickForSave = quick ? { ...quick, source: corrected } : quick;
    // Человек выбрал статью — сохраняем ЕЁ, а не то, что первым ответил переводчик.
    // Без этого выбор «dick, а не der Dicke» жил бы только на экране, а в словарь
    // и в общий пул уезжало бы прежнее.
    if (chosenEntry && quick) {
      const head = String(chosenEntry.headword || '').trim();
      const german = (chosenEntry.pos === 'noun' && art) ? `${art} ${head}` : head;
      const native = String(chosenEntry.translation || (chosenEntry.translations || [])[0] || '').trim();
      if (quick.targetLang === 'de') {
        quickForSave = { ...quickForSave, translation: german };
      } else {
        sourceText = german;
        quickForSave = { ...quickForSave, source: german, translation: native || quick.translation };
      }
    } else if (!rich && art && quick) {
      if (quick.targetLang === 'de' && !hasArticle(quick.translation)) {
        quickForSave = { ...quickForSave, translation: `${art} ${quick.translation}` };
      } else if (quick.sourceLang === 'de' && !hasArticle(sourceText)) {
        sourceText = `${art} ${sourceText}`;
      }
    }
    // Что именно уехало в словарь ПО-НЕМЕЦКИ — это и пойдёт в дверь на проверку.
    // Плашка спрашивает про немецкое слово, а человек мог набрать русское: тогда
    // немецкое лежит в переводе, а не в исходной строке.
    const savedGerman = (quickForSave && quickForSave.targetLang === 'de')
      ? String(quickForSave.translation || '')
      : sourceText;
    savedGermanRef.current = savedGerman.replace(/^(der|die|das)\s+/i, '').trim();
    return api('/api/webapp/dictionary/save', buildDictionarySavePayload({
      rich, sourceText, quick: quickForSave, origin: 'webapp_quick_dictionary',
    }));
  }, [item, enrich, quick, query, proofreadSource, chosenEntry]);

  // Сохранение не прошло. Дневная норма — это не ошибка, а понятное состояние, и у
  // приложения для него есть своё окно (ProFeatureModal, им же закрыты озвучка книги и
  // русские субтитры). Всё остальное — прежней строкой.
  //
  // Почему окно, а не строка: строка ошибки в этом экране рисуется только при
  // phase === 'error', а после удачного перевода phase === 'done'. Отказ сохранения
  // менял только текст ошибки — и не показывал его никому. 16.08.2026 сервер отказал
  // пять раз подряд, человек видел лишь мигнувшую галочку.
  const reportSaveFailure = useCallback((e) => {
    haptic('bad');
    const payload = (e && e.payload) || {};
    // Именно норма сохранений. Другой лимит (например «Разбор новых слов») сюда не
    // попадает: у него свой текст, и подписать его этим окном значило бы соврать.
    //
    // Числа в окне — только настоящие, из ответа сервера. Пустого места вместо цифры и
    // придуманной «двадцатки» здесь нет: если сервер прислал отказ без чисел, это
    // сломанный контракт (build_free_limit_error всегда кладёт used и limit) — тогда
    // окно не открываем, печатаем его же готовое сообщение строкой и кричим в консоль,
    // чтобы поломка была видна, а не замазана.
    if (payload.error === 'free_limit_exceeded' && payload.feature === 'dictionary_lookup_save_daily') {
      const used = Number(payload.used);
      const limit = Number(payload.limit);
      if (Number.isFinite(used) && Number.isFinite(limit)) {
        setSaveLimit({ used, limit, resetAt: String(payload.reset_at || '').trim() });
        return;
      }
      console.error('save limit refusal without numbers', payload);
    }
    setError(friendlyError(e));
  }, []);

  // Проверка сохранённого слова — уже ПОСЛЕ сохранения, отдельным запросом.
  //
  // Сохранение её не ждёт и на ней не спотыкается: карточка у человека в ту же
  // секунду, а плашка приходит следом. Дверь молчит про всё, что подтвердила или
  // молча починила («Argernisse» → «Ärgernisse») — решение владельца 20.08.2026:
  // факт правится без вопросов, спрашиваем только там, где решает человек.
  const askAboutSavedWord = useCallback(() => {
    const word = savedGermanRef.current;
    if (!word || /\s/.test(word)) return; // дверь про ОДНО слово, фразы не разбираем
    (async () => {
      try {
        const res = await api('/api/webapp/word-audit/check-one', { word });
        if (res && res.ask) {
          setWordHint({
            word: String(res.word || word),
            suggestion: String(res.suggestion || ''),
            why: String(res.why || ''),
          });
        }
      } catch (e) {
        // Проверка — не часть сохранения. Слово уже в словаре и всё равно попадёт
        // в экран проверки, который напоминает о себе два раза в неделю.
        console.error('плашка проверки слова не ответила', e);
      }
    })();
  }, []);

  // Решение из плашки. Ровно те же действия, что и в экране проверки, — один
  // механизм на оба места, чтобы «оставить» значило одно и то же везде.
  const applyWordHint = useCallback(async ({ action, text, translation }) => {
    const asked = wordHint?.word || '';
    try {
      await api('/api/webapp/word-audit/apply', {
        decisions: [{ word: asked, action, text, translation }],
      });
      if (text && text !== asked) {
        // Человек исправил слово — экран показывает исправленное, а не старое.
        setQuery(text);
        lastAutoRef.current = text;
        setQuick((prev) => (prev ? { ...prev, source: text } : prev));
      }
      haptic('ok');
    } catch (e) {
      console.error('решение по слову не сохранилось', e);
      setError('Не удалось сохранить выбор. Слово останется в проверке.');
    }
    setWordHint(null);
  }, [wordHint]);

  const onSave = useCallback(() => {
    if (save !== 'idle') return;
    setSave('done'); setError('');
    haptic('ok');
    (async () => {
      try { await persistEntry(); askAboutSavedWord(); }
      catch (e) { setSave('idle'); reportSaveFailure(e); }
    })();
  }, [save, persistEntry, reportSaveFailure, askAboutSavedWord]);

  // «Учить»: save the word AND queue it into the manual SRS training selection.
  const onAddToCards = useCallback(() => {
    if (cardSave !== 'idle') return;
    setCardSave('done'); setError('');
    haptic('ok');
    (async () => {
      try {
        const res = await persistEntry();
        const entryId = Number(res?.entry_id || 0);
        if (entryId > 0) {
          await api('/api/webapp/flashcards/manual-selection/add', { card_ids: [entryId] });
        }
        askAboutSavedWord();
      } catch (e) {
        setCardSave('idle');
        reportSaveFailure(e);
      }
    })();
  }, [cardSave, persistEntry, reportSaveFailure, askAboutSavedWord]);

  // Tap a synonym / collocation / antonym / related word → save it to the dictionary.
  const saveChip = useCallback((text) => {
    const t = String(text || '').trim();
    if (!t) return;
    setSavedChips((prev) => {
      if (prev.has(t)) return prev;
      const next = new Set(prev);
      next.add(t);
      return next;
    });
    haptic('ok');
    (async () => {
      try {
        // Run the SAME canonical pipeline as a typed word: a deterministic quick
        // translate (reliable target-language gloss) IN PARALLEL with the GPT
        // breakdown, so a tapped synonym/related word is stored as a proper card
        // (article + translation + grammar) WITH its translation — never bare German
        // text. Passing the quick result as `quick` is exactly what makes the typed
        // path keep its translation; omitting it was why chips saved German-only cards.
        const pair = guessPair(t);
        const [quickData, richData] = await Promise.all([
          api('/api/translate/quick', { text: t, source_lang: pair.source, target_lang: pair.target }).catch(() => null),
          api('/api/webapp/dictionary', { word: t, lookup_lang: pair.source }).catch(() => null),
        ]);
        const rich = richData?.item || null;
        if (rich) {
          rich.__direction = String(richData?.direction || rich.__direction || `${pair.source}-${pair.target}`).trim();
          rich.__language_pair = richData?.language_pair || null;
        }
        const detected = String(quickData?.detected_source_lang || pair.source).toLowerCase();
        const chipTargetLang = detected === pair.target ? pair.source : pair.target;
        const quick = quickData ? {
          source: t,
          translation: String(quickData?.translation || '').trim(),
          sourceLang: detected,
          targetLang: chipTargetLang,
          direction: `${detected}-${chipTargetLang}`,
        } : null;
        if (!rich && !(quick && quick.translation)) throw new Error('Не удалось перевести слово');
        await api('/api/webapp/dictionary/save', buildDictionarySavePayload({
          rich, sourceText: t, quick, origin: 'webapp_quick_dictionary_related',
        }));
      } catch (e) {
        setSavedChips((prev) => { const n = new Set(prev); n.delete(t); return n; });
        reportSaveFailure(e);
      }
    })();
  }, [reportSaveFailure]);

  // Tap an example SENTENCE → save it through the SAME canonical pipeline as chips,
  // but as a full sentence (not a noun lookup): we already have its German text + the
  // shown Russian translation, so skip the GPT/word breakdown entirely and hand the
  // pair straight to /save. The backend classifies it as a sentence (no article
  // normalisation) and stores the de→ru direction. If the Russian gloss is missing we
  // fall back to a deterministic quick-translate so nothing is ever saved German-only.
  const saveExample = useCallback((de, ru) => {
    const src = String(de || '').trim();
    if (!src) return;
    setSavedChips((prev) => {
      if (prev.has(src)) return prev;
      const next = new Set(prev);
      next.add(src);
      return next;
    });
    haptic('ok');
    (async () => {
      try {
        let translation = String(ru || '').trim();
        if (!translation) {
          const q = await api('/api/translate/quick', {
            text: src, source_lang: 'de', target_lang: 'ru',
          }).catch(() => null);
          translation = String(q?.translation || '').trim();
        }
        if (!translation) throw new Error('Не удалось перевести пример');
        await api('/api/webapp/dictionary/save', buildDictionarySavePayload({
          rich: null,
          sourceText: src,
          quick: {
            source: src,
            translation,
            sourceLang: 'de',
            targetLang: 'ru',
            direction: 'de-ru',
          },
          origin: 'webapp_quick_dictionary_example',
        }));
      } catch (e) {
        setSavedChips((prev) => { const n = new Set(prev); n.delete(src); return n; });
        reportSaveFailure(e);
      }
    })();
  }, [reportSaveFailure]);

  // Share this breakdown — SAME pattern as «Полный разбор»: one fast call mints a
  // durable share token, then open Telegram's native share sheet with the deep-link.
  // Recipient (even without the bot) taps it → a read-only guest view of the same
  // breakdown + "request access" CTA, showcasing what the bot can do.
  const doShare = useCallback(async () => {
    if (!deepId || sharing) return;
    setSharing(true); haptic('light');
    try {
      const data = await api('/api/webapp/dictionary/share/link', { deep_id: deepId });
      const link = String(data?.deeplink || '').trim();
      if (!link) throw new Error('Не удалось создать ссылку');
      const text = 'Полный разбор немецкого слова — в боте «Deutsche Sprache» 🇩🇪';
      const shareUrl = `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${encodeURIComponent(text)}`;
      if (typeof tg?.openTelegramLink === 'function') tg.openTelegramLink(shareUrl);
      else window.open(shareUrl, '_blank');
      haptic('ok');
    } catch (e) {
      setError(friendlyError(e)); haptic('bad');
    } finally {
      setSharing(false);
    }
  }, [deepId, sharing]);

  // Paste from clipboard (fires inside the tap gesture) and translate immediately.
  const onPaste = useCallback(async () => {
    try {
      const text = (await navigator.clipboard.readText() || '').trim();
      if (text) { setQuery(text); translate(text); }
    } catch (_e) { try { inputRef.current?.focus(); } catch (_e2) { /* ignore */ } }
  }, [translate]);

  // ⇄ — «перевести обратно»: ПРОШЛЫЙ ПЕРЕВОД становится новым запросом. Нашли
  // «Krieg → война», нажали ⇄ — в поле встаёт «война» и ищется обратно, в немецкий.
  // Направление для неё определит алфавит, как и для любого другого набранного слова.
  //
  // РАНЬШЕ ЭТА КНОПКА МЕНЯЛА НАПРАВЛЕНИЕ — и была единственным местом в словаре, где
  // человек мог получить заведомо мусорный ответ: нажатие на немецком слове говорило
  // переводчику «Krieg — это русский», и «Krieg» возвращалось само себе. Кириллица и
  // латиница не пересекаются, значит на паре ru↔de переключать нечего.
  //
  // Решение владельца 24.08.2026 после разбора чужих решений: у dict.cc и Linguee
  // направления как настройки нет вовсе, у LEO сторону выбирают данные, у Google
  // Translate переключатель недоступен при автоопределении, а патент US9524293B2
  // описывает ровно это — прежний перевод становится новым исходным текстом.
  const onSwap = useCallback(() => {
    const back = reverseQuery;
    if (!back) return;   // переворачивать нечего — кнопка в этот момент погашена
    haptic('light');
    setQuery(back);
    lastAutoRef.current = back;
    translate(back);
  }, [reverseQuery, translate]);

  // Enter translates; Shift+Enter inserts a newline.
  // Ярлык части речи для быстрого ответа. Показываем только когда разбора ещё нет:
  // у разбора своя строка помет, и дублировать её незачем.
  // Помета берётся у ВЫБРАННОЙ статьи. Раньше бралась у ответа целиком, то есть у
  // первой статьи, и не менялась при выборе: человек нажимал «wehen — глагол», а под
  // примерами по-прежнему стояло «существительное».
  const quickPos = (!item && quick && QUICK_POS_LABELS[
    String((chosenEntry?.pos ?? quick.partOfSpeech) || '').toLowerCase()
  ]) || '';

  const onKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); translate(); }
  };

  // Поле растёт под текст. Считать высоту в одном useLayoutEffect оказалось
  // недостаточно: после перевода поле — ДРУГОЙ элемент (компоновка ввода сменяется
  // компактной строкой), и замер попадал на момент, когда новый элемент ещё не встал
  // на своё место. Высота оставалась однострочной, и фраза обрезалась ровно так, как
  // на скриншоте владельца 10.08.2026: две строки, вторая срезана пополам.
  //
  // Поэтому меряем не «когда-то», а в три надёжных момента: при появлении элемента
  // (ref), при каждом вводе и ещё раз на следующем кадре — последний нужен, когда
  // ширина и шрифт встают уже после монтирования. Потолок задан в CSS: дальше поле
  // прокручивается само и не съедает экран под перевод.
  const fitInputHeight = useCallback((el) => {
    // Трогаем ТОЛЬКО компактное поле после перевода. Большое поле на пустом экране
    // должно занимать пол-экрана независимо от того, сколько в нём текста, — как во
    // всех словарях, на которые мы смотрели; подгонять его под содержимое значило бы
    // схлопнуть его до одной строки.
    if (!el || el.tagName !== 'TEXTAREA' || !el.classList.contains('dq-input--multi')) return;
    el.style.height = 'auto';
    el.style.height = `${el.scrollHeight}px`;
  }, []);

  const attachInput = useCallback((el) => {
    inputRef.current = el;
    if (!el) return;
    fitInputHeight(el);
    requestAnimationFrame(() => fitInputHeight(el));
  }, [fitInputHeight]);

  useLayoutEffect(() => {
    fitInputHeight(inputRef.current);
  }, [query, quick, fitInputHeight]);

  if (blocked) {
    return <DictBlockedGate botUsername={blocked.botUsername} />;
  }

  return (
    <div className="ans-root dq-scroll">
      <div className="ans-card dq-card">
        <div className="ans-head dq-head-row">
          <span className="ans-eyebrow">📖 Быстрый словарь</span>
          {typeof onClose === 'function' && (
            <button
              type="button"
              className="dq-close-btn"
              onClick={onClose}
              aria-label="Закрыть словарь"
              title="Закрыть"
            >
              <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><path d="M6 6l12 12M18 6L6 18" /></svg>
            </button>
          )}
          {deepId && item && (
            <button
              type="button"
              className={`dq-share-btn${sharing ? ' is-busy' : ''}`}
              onClick={doShare}
              disabled={sharing}
              aria-label="Поделиться разбором"
              title="Поделиться"
            >
              {sharing ? (
                <span className="dq-share-spin" />
              ) : (
                <svg viewBox="0 0 24 24" width="20" height="20" fill="none"
                     stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 3v13" />
                  <path d="M8 7l4-4 4 4" />
                  <path d="M5 12v6a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-6" />
                </svg>
              )}
            </button>
          )}
        </div>

        {/* Те же три закладки, что и в словаре внутри приложения. Владелец 09.08.2026:
            «в словаре иконкой — всё как было, интерфейс же должен быть одинаков». Он
            прав: это один продукт в двух местах, и раскладка у них общая. Отличаются
            только цвета — так и договаривались. */}
        <div className="vocab-tabs dq-tabs">
          <button type="button" className={`vocab-tab ${tab === 'search' ? 'is-active' : ''}`}
                  onClick={() => setTab('search')}>🔍 Поиск</button>
          <button type="button" className={`vocab-tab ${tab === 'mine' ? 'is-active' : ''}`}
                  onClick={() => { setTab('mine'); setMineCard(null); }}>📚 Мои слова</button>
          {historyList.length > 0 && (
            <button type="button" className={`vocab-tab ${tab === 'history' ? 'is-active' : ''}`}
                    onClick={() => setTab('history')}>🕘 История</button>
          )}
        </div>

        {tab === 'history' && (
          <div className="dict-history">
            {historyList.map((w) => (
              <button key={w} type="button" className="dict-history-row"
                      onClick={() => { setTab('search'); setQuery(w); translate(w); }}>
                {w}
              </button>
            ))}
          </div>
        )}

        {tab === 'mine' && mineCard && (
          <div className="dq-mine-card">
            <button type="button" className="dq-mine-back" onClick={() => setMineCard(null)}>
              ← К моим словам
            </button>
            <div className="dq-translation">
              {clean(mineCard.word_de || mineCard.translation_de)}
              {clean(mineCard.word_de || mineCard.translation_de) && (
                <SpeakButton text={clean(mineCard.word_de || mineCard.translation_de)} tts={tts} />
              )}
            </div>
            <div className="dq-source">{clean(mineCard.translation_ru || mineCard.word_ru)}</div>
            <WordBreakdown item={mineCard} tts={tts} tablesOpen={false} />
          </div>
        )}

        {tab === 'mine' && !mineCard && (
          <div className="dict-history">
            <div className="dq-input-wrap dq-mine-search">
              <input
                className="ans-input dq-input"
                type="text"
                autoComplete="off"
                placeholder="Найти среди своих слов…"
                value={mineQuery}
                onChange={(e) => setMineQuery(e.target.value)}
              />
              {mineQuery && (
                <button type="button" className="dq-clear" aria-label="Очистить"
                        onClick={() => setMineQuery('')}>×</button>
              )}
            </div>
            {mineState === 'loading' && <div className="dict-history-empty">Загружаю…</div>}
            {mineState === 'error' && (
              <div className="dict-history-empty">Не получилось открыть ваши слова. Попробуйте ещё раз.</div>
            )}
            {mineState === 'ready' && mine.length === 0 && (
              <div className="dict-history-empty">
                {mineQuery.trim()
                  ? 'Среди ваших слов такого нет.'
                  : 'Здесь появятся слова, которые вы сохранили.'}
              </div>
            )}
            {mine.map((row, i) => {
              const de = clean(row.word_de || row.translation_de);
              const ru = clean(row.translation_ru || row.word_ru);
              if (!de) return null;
              return (
                <button key={`${de}-${i}`} type="button" className="dict-history-row"
                        onClick={() => openMineCard(row)}>
                  <b>{de}</b>{ru ? <span className="dq-chip-gloss"> — {ru}</span> : null}
                </button>
              );
            })}
            {mineHasMore && (
              <button type="button" className="dict-history-row dq-mine-more"
                      disabled={mineState === 'more'}
                      onClick={() => void loadMine({ needle: mineQuery.trim() })}>
                {mineState === 'more' ? 'Загружаю…' : 'Показать ещё'}
              </button>
            )}
          </div>
        )}

        {tab === 'search' && (<>


        {(() => {
          const [src, tgt] = [panelPair.source, panelPair.target];
          return (
            <div className="dq-langrow">
              <div className="dq-langbar">
                <span className="dq-lang">{languageName(src)}</span>
                <button
                  type="button"
                  className="dq-swap"
                  onClick={onSwap}
                  disabled={!reverseQuery}
                  aria-label="Перевести обратно"
                  title={reverseQuery ? `Перевести обратно: ${reverseQuery}` : 'Сначала переведите слово'}
                >⇄</button>
                <span className="dq-lang">{languageName(tgt)}</span>
              </div>
              <button
                type="button"
                className={`dq-auto-toggle${autoOn ? ' on' : ''}`}
                onClick={toggleAuto}
                aria-pressed={autoOn}
                title="Автоматический перевод по паузе"
              >
                ⚡ Авто
              </button>
            </div>
          );
        })()}

        {!quick ? (
          /* COMPOSE — full-height input like Google Translate / DeepL. */
          <div className="dq-compose">
            <textarea
              ref={attachInput}
              className="dq-textarea"
              autoComplete="off"
              placeholder="Слово или фраза…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={onKeyDown}
            />
            {phase === 'error' && error && <div className="dd-err">{error}</div>}
            {/* Блок «Недавние» убран 10.08.2026. Владелец: «если есть закладка История,
                зачем здесь недавние запросы?» — и он прав, это было одно и то же в двух
                местах. Длинные фразы к тому же занимали чипами полэкрана, ради которого
                поле ввода и делалось большим. Историю теперь держит своя закладка. */}
            <div className="dq-compose-foot">
              <button type="button" className="dq-paste-btn" onClick={onPaste}>📋 Вставить</button>
              <button
                type="button"
                className="dq-go dq-go-full"
                onClick={() => translate()}
                disabled={!query.trim() || phase === 'loading'}
              >
                {phase === 'loading' ? 'Перевожу…' : 'Перевести'}
              </button>
            </div>
          </div>
        ) : (
        <>
        <div className="dq-search dq-search--multi">
          <div className="dq-input-wrap dq-input-wrap--multi">
            {/* Поле остаётся МНОГОСТРОЧНЫМ и после перевода. Раньше здесь стоял
                <input type="text">, и фраза обрезалась на первой строке: человек не
                видел, что именно он отправил. Меняли размер — а дело было в самом
                элементе. Высота подгоняется под текст в useLayoutEffect ниже. */}
            <textarea
              ref={attachInput}
              className="ans-input dq-input dq-input--multi"
              rows={1}
              autoComplete="off"
              placeholder="Слово или фраза…"
              value={query}
              onChange={(e) => { const v = e.target.value; setQuery(v); if (!v.trim()) resetResult(); }}
              onKeyDown={onKeyDown}
            />
            {query && (
              <button
                type="button"
                className="dq-clear"
                onClick={clearInput}
                aria-label="Очистить поле"
              >
                ×
              </button>
            )}
          </div>
          <button
            type="button"
            className="dq-go"
            onClick={() => translate()}
            disabled={!query.trim() || phase === 'loading'}
          >
            {phase === 'loading' ? '…' : 'Перевести'}
          </button>
        </div>

        {phase === 'error' && error && <div className="dd-err">{error}</div>}

        {quick && phase !== 'loading' && (
          <div className="dq-result">
            <div className="dq-source">
              {(dqArticle && quick.sourceLang === 'de')
                ? <><span className={`dq-art ${genderClass(dqArticle)}`}>{dqArticle}</span> </> : ''}{headSource}
              {correctedNote && <span className="dq-corrected">исправлено с «{quick.source}»</span>}
            </div>
            <div className="dq-translation">
              {(dqArticle && quick.targetLang === 'de')
                ? <><span className={`dq-art ${genderClass(dqArticle)}`}>{dqArticle}</span> </> : ''}
              {headTranslation}
              {germanText && <SpeakButton text={germanText} tts={tts} />}
            </div>
            {/* Спросили форму множественного — показываем именно её (с «die», как и
                положено множественному), а слово называем строкой ниже: тап открывает
                его карточку. Подменять запрос леммой нельзя — человек просил другое. */}
            {dqNumber === 'pl' && dqLemma && (
              <button
                type="button"
                className="dq-lemma-note"
                onClick={() => { setQuery(dqLemma); translate(dqLemma); }}
              >
                мн. ч. от <b>{dqLemmaArticle ? `${dqLemmaArticle} ` : ''}{dqLemma}</b>
              </button>
            )}
            {/* ВЫБОР СТАТЬИ. Одно написание может стоять за несколькими словами:
                «толстый» — это и прилагательное dick, и существительное der Dicke.
                Раньше мы выбирали за человека, и выбирал по сути таймаут сетевого
                запроса. Теперь показываем все статьи, как PONS и dict.cc, и
                воткнуть неправильно нельзя — выбирает человек. */}
            {entries.length > 1 && (
              <div className="dq-entries">
                <div className="dq-entries-title">Найдено несколько слов — выберите нужное:</div>
                {entries.map((entry, index) => (
                  <button
                    key={`${entry.headword}-${entry.pos}-${entry.gender}`}
                    type="button"
                    className={`dq-entry${index === chosen ? ' is-chosen' : ''}`}
                    onClick={() => chooseEntry(index)}
                  >
                    <span className="dq-entry-head">
                      {entry.gender && (
                        <span className={`dq-art ${genderClass(entry.gender)}`}>{entry.gender} </span>
                      )}
                      {entry.headword}
                    </span>
                    {entry.pos && (
                      <span className="dq-entry-pos">{QUICK_POS_LABELS[entry.pos] || entry.pos}</span>
                    )}
                    <span className="dq-entry-tr">{(entry.translations || []).join(', ')}</span>
                  </button>
                ))}
              </div>
            )}
            {/* Ответ машинного переводчика, а не словарная статья: слова у нас нет,
                грамматику мы про него не знаем и выдумывать не станем. Человек
                должен видеть разницу — это ровно то, чего не хватало. */}
            {quick.machine && !entries.length && (
              <div className="dq-machine-note">машинный перевод — этого слова нет в словаре</div>
            )}
            {/* Живые примеры — общий компонент на оба словаря.

                Спрашиваем их по ВЫБРАННОМУ слову, а не по строке из поля поиска.
                До 15.08.2026 сюда уходило quick.source — то, что человек набрал. На
                «Wehe» это давало примеры от wehtun («Tut das weh?» — Болит?): третье
                слово, ни к «die Wehe», ни к «wehen» отношения не имеющее. И при выборе
                другой статьи примеры не менялись — строка поиска-то прежняя. */}
            {!item && (
              <LiveExamples
                germanWord={
                  chosenEntry?.headword
                  || (quick?.sourceLang === 'de' ? quick?.source : quick?.translation)
                }
                ownExamples={chosenEntry?.examples}
                pos={chosenEntry?.pos || quick?.partOfSpeech}
              />
            )}
            {/* Часть речи у быстрого перевода. Переводчики её не отдают, поэтому она
                приходит из нашего же банка слов — тем же дешёвым путём, что и артикль.
                Владелец 08.08.2026: «Soweit → Насколько» без единой пометы; с ней сразу
                видно, что слово с большой буквы — наречие, а не существительное. */}
            {quickPos && <div className="dq-quick-pos">{quickPos}</div>}
            {tts.errorMsg && <div className="dd-err" role="status">🔊 {tts.errorMsg}</div>}
            {item && (
              <WordBreakdown
                item={item}
                tts={tts}
                // Строку «форма слова …» здесь не дублируем: своя «мн. ч. от …» уже выше.
                hideFormNote={dqNumber === 'pl' && !!dqLemma}
                onSaveChip={saveChip}
                onSaveExample={saveExample}
                savedChips={savedChips}
                // Тап по «так же пишется» открывает соседнее слово: запрос идёт С АРТИКЛЕМ,
                // иначе снова вернулось бы то же самое — угадать по написанию нечем.
                onPickHomograph={(text) => { setQuery(text); translate(text); }}
              />
            )}
            {(enrich === 'loading' || enrich === 'streaming') && (
              <BreakdownSkeleton arrived={streamSections} />
            )}
            {deepLoading && <div className="dq-muted dq-deep-loading">Дополняю: этимология, примеры, как запомнить…</div>}

            <div className="dq-actions">
              {!item && enrich !== 'loading' && enrich !== 'streaming' && (
                <button type="button" className="dd-action" onClick={() => runLookup().catch(() => {})}>
                  📖 Подробный разбор
                </button>
              )}
              <div className="dq-save-row">
                <button
                  type="button"
                  className="dd-save dq-save-half"
                  onClick={onSave}
                  disabled={save !== 'idle'}
                >
                  {save === 'done' ? '✅ В словаре' : '💾 В словарь'}
                </button>
                <button
                  type="button"
                  className="dd-save dq-save-half dq-cards-btn"
                  onClick={onAddToCards}
                  disabled={cardSave !== 'idle'}
                >
                  {cardSave === 'done' ? '✅ В карточках' : '📚 Учить'}
                </button>
              </div>
              {/* Отказ сохранения виден ЗДЕСЬ, у кнопок, которыми человек его вызвал.
                  Обе плашки выше показываются только при phase === 'error', а после
                  удачного перевода phase === 'done' — из-за этого любой отказ (сеть,
                  сервер, лимит) уходил в невидимое состояние. */}
              {error && phase !== 'error' && <div className="dd-err" role="status">{error}</div>}
              {wordHint && (
                <SaveWordHint
                  word={wordHint.word}
                  suggestion={wordHint.suggestion}
                  why={wordHint.why}
                  onApply={applyWordHint}
                  onDismiss={() => setWordHint(null)}
                />
              )}
            </div>
          </div>
        )}
        </>
        )}
        </>)}
      </div>

      {chipHint && (
        <div
          className="dq-chip-hint"
          role="status"
          onClick={() => setChipHint(false)}
        >
          <span className="dq-chip-hint-ic">💡</span>
          <span className="dq-chip-hint-text">
            Нажми на слово в блоках <b>«Синонимы»</b>, <b>«Антонимы»</b> или в примерах — и оно сохранится в твой словарь для изучения.
          </span>
        </div>
      )}

      {/* Дневная норма сохранений кончилась. Числа — из ответа сервера. */}
      {saveLimit && (
        <ProFeatureModal
          isOpen
          onClose={() => setSaveLimit(null)}
          onUpgrade={() => {
            setSaveLimit(null);
            openBotLink(DICT_BOT_USERNAME_FALLBACK, 'subscription');
          }}
          tr={(ru) => ru}
          badge="💾 Дневная норма"
          emoji="💾"
          title={`Сегодня сохранено ${saveLimit.used} ${wordsPlural(saveLimit.used)} — это дневная норма`}
          intro={`На бесплатном тарифе можно сохранять ${saveLimit.limit} новых ${wordsPlural(saveLimit.limit)} и фраз в день.${resetClause(saveLimit.resetAt)}`}
          bullets={[
            'Слова из заданий, игр и видео норму не тратят — их сохраняй сколько угодно',
            'Норму тратят только новые слова и фразы, которых у нас ещё нет',
            'С «Полным доступом» нормы нет совсем',
          ]}
        />
      )}
    </div>
  );
}
