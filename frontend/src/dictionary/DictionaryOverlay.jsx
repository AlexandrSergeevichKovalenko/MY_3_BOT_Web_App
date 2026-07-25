import React, { useCallback, useEffect, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';
import { WordBreakdown, useTts, SpeakButton, genderClass, resolveArticle, stripLeadingArticle, api, haptic, getInitData, getDictToken } from './WordBreakdown';
import BreakdownSkeleton from './BreakdownSkeleton';
import { guessPair, buildDictionarySavePayload } from './saveUtils';
import { humanizeDictError } from './errors.js';

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
    const w = window.innerWidth || 0;
    const h = window.innerHeight || 0;
    const ua = String(navigator.userAgent || '');
    if (/iPhone|iPod|Windows Phone|Android.*Mobile/i.test(ua)) return false;
    const isIPadDesktopUA = navigator.platform === 'MacIntel' && Number(navigator.maxTouchPoints || 0) > 1;
    const isTabletUA = /iPad|Tablet|PlayBook|Silk|Android(?!.*Mobile)/i.test(ua) || isIPadDesktopUA;
    return isTabletUA || w >= 700 || (Math.max(w, h) >= 1000 && Math.min(w, h) >= 600);
  } catch (_e) { return false; }
}

const LANG_NAMES = { ru: 'Русский', de: 'Deutsch' };

// Effective direction: an explicit user choice (forced) wins; otherwise auto from
// the script of the text (Cyrillic → ru→de). Returns 'ru-de' | 'de-ru'.
function effectiveDir(text, forced) {
  if (forced === 'ru-de' || forced === 'de-ru') return forced;
  return /[А-Яа-яЁё]/.test(String(text || '')) ? 'ru-de' : 'de-ru';
}
function dirToPair(dir) {
  return dir === 'de-ru' ? { source: 'de', target: 'ru' } : { source: 'ru', target: 'de' };
}

// All dictionary errors go through the shared humanizer so a raw machine code
// (e.g. "cost_cap_exceeded") is never shown to the user — see ./errors.js. Kept as a
// thin local alias so the ~9 call sites below stay unchanged.
const friendlyError = humanizeDictError;

// The German side of a quick result when it's a lone noun (single capitalized token)
// still lacking an article — mirrors the backend's noun-candidate check. When this is
// non-empty the article is being filled in the background and we should poll for it.
function germanNounAwaitingArticle(q) {
  if (!q || String(q.article || '').trim()) return '';
  let german = '';
  if (q.targetLang === 'de') german = String(q.translation || '').trim();
  else if (q.sourceLang === 'de') german = String(q.source || '').trim();
  if (!german || /\s/.test(german) || german[0] !== german[0].toUpperCase()) return '';
  return german;
}

// Recent lookups persisted locally (most-recent first, max 6).
const RECENTS_KEY = 'dq_recents_v1';
function loadRecents() {
  try {
    const raw = JSON.parse(localStorage.getItem(RECENTS_KEY) || '[]');
    return Array.isArray(raw) ? raw.filter((x) => typeof x === 'string').slice(0, 6) : [];
  } catch (_e) { return []; }
}
function pushRecent(word) {
  const w = String(word || '').trim();
  if (!w) return loadRecents();
  const next = [w, ...loadRecents().filter((x) => x.toLowerCase() !== w.toLowerCase())].slice(0, 6);
  try { localStorage.setItem(RECENTS_KEY, JSON.stringify(next)); } catch (_e) { /* ignore */ }
  return next;
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

function DictBlockedGate({ botUsername }) {
  const uname = (String(botUsername || '').replace(/^@/, '').trim()) || DICT_BOT_USERNAME_FALLBACK;
  const openBot = () => {
    const https = `https://t.me/${uname}`;
    const tg = window?.Telegram?.WebApp;
    try {
      // Inside Telegram (initData present) → native opener. Otherwise we're the detached PWA.
      if (tg && tg.initData && typeof tg.openTelegramLink === 'function') {
        tg.openTelegramLink(https);
        return;
      }
    } catch (_e) { /* fall through */ }
    // Detached home-screen PWA: jump straight into the Telegram app via the tg:// scheme.
    // If that scheme isn't handled (no app), fall back to the universal https link — but skip
    // the fallback if the app actually opened (page went hidden), so we don't also load t.me.
    let done = false;
    try { window.location.href = `tg://resolve?domain=${uname}`; } catch (_e) { /* ignore */ }
    setTimeout(() => {
      if (done || document.hidden) return;
      done = true;
      try { window.location.href = https; } catch (_e) { /* ignore */ }
    }, 800);
  };
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
  const [savedChips, setSavedChips] = useState(() => new Set()); // synonyms/collocations tapped to save
  const [error, setError] = useState('');
  const [recents, setRecents] = useState(loadRecents);
  const [forcedDir, setForcedDir] = useState(null); // null=auto, else 'ru-de'|'de-ru'
  const [autoOn, setAutoOn] = useState(() => {
    try { return localStorage.getItem('dq_auto') !== '0'; } catch (_e) { return true; }
  });
  const [chipHint, setChipHint] = useState(false); // brief "tap a synonym to save it" toast
  const [blocked, setBlocked] = useState(null); // {botUsername} when the user left the bot → gate screen
  const lastAutoRef = useRef(''); // text already auto/manually translated (debounce dedupe)
  const chipHintDoneRef = useRef(false); // shown for the current breakdown already
  const seqRef = useRef(0);
  const inputRef = useRef(null);
  const streamAbortRef = useRef(null); // aborts an in-flight breakdown SSE stream
  const lookupPromiseRef = useRef(null); // in-flight breakdown promise (shared by tap + save)
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

  // German text of the current result, for pronunciation.
  const germanText = (() => {
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
  const headTranslation = quick?.targetLang === 'de'
    ? (corrDe || stripLeadingArticle(quick?.translation) || '—')
    : (bestRu || quick?.translation || '—');
  const headSource = (quick?.sourceLang === 'de')
    ? (corrDe || stripLeadingArticle(quick?.source) || '')
    : (quick?.source || '');
  // One clean der/die/das for both the source and translation spans.
  const dqArticle = resolveArticle(item, quick);
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

  const translate = useCallback(async (overrideText, dirOverride) => {
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
      // Direction: an explicit swap wins, then the panel choice, else auto by script.
      const chosenDir = (dirOverride === 'ru-de' || dirOverride === 'de-ru')
        ? dirOverride : effectiveDir(text, forcedDir);
      const pair = dirToPair(chosenDir);
      const data = await api('/api/translate/quick', {
        text, source_lang: pair.source, target_lang: pair.target,
      });
      if (mySeq !== seqRef.current) return;
      const detected = String(data?.detected_source_lang || pair.source).toLowerCase();
      const targetLang = detected === pair.target ? pair.source : pair.target;
      // Keep the language panel in sync with what was actually detected.
      setForcedDir(`${detected}-${targetLang}`);
      const nextQuick = {
        source: text,
        translation: String(data?.translation || '').trim(),
        sourceLang: detected,
        targetLang,
        direction: `${detected}-${targetLang}`,
        provider: String(data?.provider || '').trim(),
        // Article for a single German noun, resolved instantly from the local
        // Wiktionary table so "die Wortverbindung" shows without the full breakdown.
        article: String(data?.article || '').trim(),
      };
      setQuick(nextQuick);
      setPhase('done'); haptic('ok');
      setRecents(pushRecent(text));
      // A German noun whose article missed the instant Wiktionary lookup gets its
      // der/die/das filled by a background LLM job that patches the cache. Poll for it
      // so it appears on its own — never make the user press «Перевести» a second time.
      if (germanNounAwaitingArticle(nextQuick)) {
        (async () => {
          for (const delay of [900, 1300, 1600, 2000, 2500]) {
            await new Promise((r) => setTimeout(r, delay));
            if (mySeq !== seqRef.current) return;
            let art = '';
            try {
              const a = await api('/api/translate/quick/article', {
                text, source_lang: pair.source, target_lang: pair.target,
              });
              art = String(a?.article || '').trim();
            } catch (_e) { /* keep polling */ }
            if (mySeq !== seqRef.current) return;
            if (art) {
              setQuick((prev) => (prev && !prev.article ? { ...prev, article: art } : prev));
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
  }, [query, phase, tts, forcedDir]);

  // Drop the current result and return to the initial compose screen. Called when the
  // field is emptied (manually or via the × button) so a stale card never lingers.
  const resetResult = useCallback(() => {
    seqRef.current += 1; // abort any in-flight translate/lookup
    tts.stop();
    setQuick(null); setItem(null); setEnrich('idle'); setPhase('idle');
    setError(''); setSave('idle'); setCardSave('idle'); setSavedChips(new Set());
    setDeepId(''); setStreamSections(new Set());
    lastAutoRef.current = '';
    try { streamAbortRef.current?.abort(); } catch (_e) { /* ignore */ }
    streamAbortRef.current = null;
    lookupPromiseRef.current = null;
  }, [tts]);

  const clearInput = useCallback(() => {
    setQuery('');
    resetResult();
    haptic('light');
    try { inputRef.current?.focus(); } catch (_e) { /* ignore */ }
  }, [resetResult]);

  // Auto-translate (DeepL-style): translate ~800ms after the user stops typing.
  useEffect(() => {
    if (!autoOn) return undefined;
    const t = query.trim();
    if (!t || t === lastAutoRef.current || phase === 'loading') return undefined;
    const id = setTimeout(() => translate(t), 800);
    return () => clearTimeout(id);
  }, [query, forcedDir, phase, translate, autoOn]);

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
    const w = query.trim();
    const pair = guessPair(w);
    const data = await api('/api/webapp/dictionary', { word: w, lookup_lang: pair.source });
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
    const w = query.trim();
    const pair = guessPair(w);
    const mySeq = seqRef.current;
    const controller = new AbortController();
    streamAbortRef.current = controller;

    const dictToken = getDictToken();
    const streamHeaders = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
    if (dictToken) streamHeaders['X-Dict-Token'] = dictToken;
    const resp = await fetch('/api/webapp/dictionary/stream', {
      method: 'POST',
      headers: streamHeaders,
      body: JSON.stringify({ initData: getInitData(), ...(dictToken ? { dqt: dictToken } : {}), word: w, lookup_lang: pair.source }),
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
    const corrected = await proofreadSource(typed);
    if (corrected && corrected !== typed) {
      lastAutoRef.current = corrected;
      setQuery(corrected);
      setQuick((prev) => (prev ? { ...prev, source: corrected } : prev));
    }

    const art = String(quick?.article || '').trim();
    const hasArticle = (s) => /^(der|die|das)\s/i.test(String(s || ''));
    let sourceText = corrected;
    let quickForSave = quick ? { ...quick, source: corrected } : quick;
    if (!rich && art && quick) {
      if (quick.targetLang === 'de' && !hasArticle(quick.translation)) {
        quickForSave = { ...quickForSave, translation: `${art} ${quick.translation}` };
      } else if (quick.sourceLang === 'de' && !hasArticle(sourceText)) {
        sourceText = `${art} ${sourceText}`;
      }
    }
    return api('/api/webapp/dictionary/save', buildDictionarySavePayload({
      rich, sourceText, quick: quickForSave, origin: 'webapp_quick_dictionary',
    }));
  }, [item, enrich, quick, query, proofreadSource]);

  const onSave = useCallback(() => {
    if (save !== 'idle') return;
    setSave('done'); setError('');
    haptic('ok');
    (async () => {
      try { await persistEntry(); }
      catch (e) { setSave('idle'); setError(friendlyError(e)); haptic('bad'); }
    })();
  }, [save, persistEntry]);

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
      } catch (e) {
        setCardSave('idle');
        setError(friendlyError(e)); haptic('bad');
      }
    })();
  }, [cardSave, persistEntry]);

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
        setError(friendlyError(e)); haptic('bad');
      }
    })();
  }, []);

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
        setError(friendlyError(e)); haptic('bad');
      }
    })();
  }, []);

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

  // Swap the language direction (⇄) and re-translate with the new direction.
  const onSwap = useCallback(() => {
    const next = effectiveDir(query, forcedDir) === 'ru-de' ? 'de-ru' : 'ru-de';
    setForcedDir(next);
    haptic('light');
    const t = query.trim();
    if (t) { lastAutoRef.current = t; translate(t, next); }
  }, [query, forcedDir, translate]);

  // Enter translates; Shift+Enter inserts a newline.
  const onKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); translate(); }
  };

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

        {(() => {
          const dir = effectiveDir(query, forcedDir);
          const [src, tgt] = dir.split('-');
          return (
            <div className="dq-langrow">
              <div className="dq-langbar">
                <span className="dq-lang">{LANG_NAMES[src]}</span>
                <button type="button" className="dq-swap" onClick={onSwap} aria-label="Поменять языки">⇄</button>
                <span className="dq-lang">{LANG_NAMES[tgt]}</span>
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
              ref={inputRef}
              className="dq-textarea"
              autoComplete="off"
              placeholder="Слово или фраза…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={onKeyDown}
            />
            {phase === 'error' && error && <div className="dd-err">{error}</div>}
            {recents.length > 0 && (
              <div className="dq-recent">
                <span className="dq-recent-label">Недавние</span>
                <div className="dq-recent-chips">
                  {recents.map((w) => (
                    <button key={w} type="button" className="dq-recent-chip" onClick={() => translate(w)}>
                      {w}
                    </button>
                  ))}
                </div>
              </div>
            )}
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
        <div className="dq-search">
          <div className="dq-input-wrap">
            <input
              ref={inputRef}
              className="ans-input dq-input"
              type="text"
              inputMode="text"
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
            {tts.errorMsg && <div className="dd-err" role="status">🔊 {tts.errorMsg}</div>}
            {item && <WordBreakdown item={item} tts={tts} onSaveChip={saveChip} onSaveExample={saveExample} savedChips={savedChips} />}
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
            </div>
          </div>
        )}
        </>
        )}
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
    </div>
  );
}
