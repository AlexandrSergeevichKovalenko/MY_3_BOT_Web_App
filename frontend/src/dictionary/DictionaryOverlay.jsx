import React, { useCallback, useEffect, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';
import { WordBreakdown, useTts, SpeakButton, genderClass, api, haptic } from './WordBreakdown';

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

// Cheap language guess for the instant path: any Cyrillic → translate ru→de,
// otherwise treat as German → ru. The full GPT lookup auto-detects properly.
function guessPair(text) {
  const hasCyrillic = /[А-Яа-яЁё]/.test(String(text || ''));
  return hasCyrillic
    ? { source: 'ru', target: 'de' }
    : { source: 'de', target: 'ru' };
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

// The GPT breakdown carries the target-language meaning in `translations`
// (array of {value, context} or strings) / `meanings.primary` — NOT in
// word_ru/translation_ru. Pull a concise translation string out of it so a save
// made straight from the deep card (without a fresh quick-translate) still keeps
// the translation instead of falling back to the German word. Top 2 meanings.
function extractRichTranslation(item) {
  if (!item || typeof item !== 'object') return '';
  const out = [];
  const push = (v) => {
    const s = String((v && typeof v === 'object') ? v.value : v || '').trim();
    if (s && !out.includes(s)) out.push(s);
  };
  if (Array.isArray(item.translations)) {
    for (const t of item.translations) {
      if (out.length >= 2) break;
      push(t);
    }
  }
  if (!out.length && item.meanings && typeof item.meanings === 'object') {
    push(item.meanings.primary);
  }
  return out.join(', ');
}

// Build the canonical /api/webapp/dictionary/save payload from a GPT breakdown item.
// EVERY save source (typed word OR tapped synonym/related chip) goes through this so
// the entry always lands in the same standard card format (article+Grundform /
// translation / grammar), never bare German text without a translation.
function buildDictionarySavePayload({ rich, sourceText, quick, origin }) {
  const quickTranslation = quick?.translation || '';
  const quickDirection = quick?.direction || '';
  const quickSourceLang = quick?.sourceLang || '';
  const quickTargetLang = quick?.targetLang || '';
  const direction = String(rich?.__direction || quickDirection || '').trim();
  const [dirSource, dirTarget] = direction.includes('-') ? direction.split('-', 2) : [];
  const sourceLang = (dirSource || quickSourceLang || '').toLowerCase();
  const targetLang = (dirTarget || quickTargetLang || '').toLowerCase();
  const richTranslation = extractRichTranslation(rich);
  const targetText = (quickTranslation || richTranslation || '').trim();
  return {
    word_de: String(rich?.word_de || '').trim(),
    word_ru: String(rich?.word_ru || '').trim(),
    translation_de: String(rich?.translation_de || (targetLang === 'de' ? richTranslation : '')).trim(),
    translation_ru: String(rich?.translation_ru || (targetLang === 'ru' ? richTranslation : '')).trim(),
    source_text: sourceText,
    target_text: targetText,
    source_lang: sourceLang || undefined,
    target_lang: targetLang || undefined,
    direction: direction || undefined,
    response_json: rich || undefined,
    origin_process: origin,
  };
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

export default function DictionaryOverlay() {
  const [query, setQuery] = useState('');
  const [phase, setPhase] = useState('idle'); // idle|loading|done|error
  const [quick, setQuick] = useState(null);   // { source, target, translation, sourceLang, targetLang, direction }
  const [item, setItem] = useState(null);     // rich GPT item (for enrich + canonical save)
  const [enrich, setEnrich] = useState('idle'); // idle|loading|done|error
  const [deepLoading, setDeepLoading] = useState(false); // background enrichment poll
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
  const lastAutoRef = useRef(''); // text already auto/manually translated (debounce dedupe)
  const chipHintDoneRef = useRef(false); // shown for the current breakdown already
  const seqRef = useRef(0);
  const inputRef = useRef(null);
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
  const corrDe = String(item?.word_de || '').trim().replace(/^(der|die|das)\s+/i, '');
  const bestRu = String(
    item?.translation_ru
    || item?.word_ru
    || item?.meanings?.primary?.value
    || '',
  ).trim();
  const headTranslation = quick?.targetLang === 'de'
    ? (corrDe || quick?.translation || '—')
    : (bestRu || quick?.translation || '—');
  const headSource = (quick?.sourceLang === 'de' && corrDe) ? corrDe : (quick?.source || '');
  const correctedNote = (corrDe && quick?.sourceLang === 'de'
    && corrDe.toLowerCase() !== String(quick?.source || '').trim().toLowerCase())
    ? corrDe : '';

  // Prewarm pronunciation as soon as the German text is known.
  const { warm: warmTts } = tts;
  useEffect(() => {
    if (germanText) warmTts(germanText, 'de-DE');
  }, [germanText, warmTts]);

  const translate = useCallback(async (overrideText, dirOverride) => {
    const text = (typeof overrideText === 'string' ? overrideText : query).trim();
    if (!text || phase === 'loading') return;
    if (text !== query) setQuery(text);
    lastAutoRef.current = text; // mark as handled so the auto-translate effect won't repeat it
    const mySeq = ++seqRef.current;
    tts.stop();
    setPhase('loading'); setError(''); setItem(null); setEnrich('idle'); setSave('idle'); setCardSave('idle'); setSavedChips(new Set());
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
      setQuick({
        source: text,
        translation: String(data?.translation || '').trim(),
        sourceLang: detected,
        targetLang,
        direction: `${detected}-${targetLang}`,
        provider: String(data?.provider || '').trim(),
        // Article for a single German noun, resolved instantly from the local
        // Wiktionary table so "die Wortverbindung" shows without the full breakdown.
        article: String(data?.article || '').trim(),
      });
      setPhase('done'); haptic('ok');
      setRecents(pushRecent(text));
    } catch (e) {
      if (mySeq !== seqRef.current) return;
      setError(String(e.message || e)); setPhase('error'); haptic('bad');
    }
  }, [query, phase, tts, forcedDir]);

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
      for (let i = 0; i < 15; i += 1) {
        await new Promise((r) => setTimeout(r, i === 0 ? 1500 : 3000));
        if (mySeq !== seqRef.current) return;
        let data;
        try { data = await api('/api/webapp/dictionary/status', { lookup_id: lookupId }); }
        catch (_e) { break; }
        if (mySeq !== seqRef.current) return;
        if (data?.item) {
          const merged = { ...data.item, __direction: base?.__direction, __language_pair: base?.__language_pair };
          setItem(merged);
        }
        if (String(data?.status || '') === 'ready' || data?.enrichment_pending === false) break;
      }
    } finally {
      if (mySeq === seqRef.current) setDeepLoading(false);
    }
  }, []);

  // Full GPT breakdown. Returns the rich item so save can reuse it.
  const runLookup = useCallback(async () => {
    if (item) return item;
    setEnrich('loading'); setError('');
    try {
      const pair = guessPair(query.trim());
      const data = await api('/api/webapp/dictionary', {
        word: query.trim(), lookup_lang: pair.source,
      });
      const rich = data?.item || null;
      if (rich) {
        rich.__direction = String(data?.direction || rich.__direction || '').trim();
        rich.__language_pair = data?.language_pair || null;
      }
      setItem(rich);
      setEnrich(rich ? 'done' : 'error');
      if (rich && data?.enrichment_pending && data?.lookup_id) {
        pollEnrichment(data.lookup_id, rich);
      }
      return rich;
    } catch (e) {
      setEnrich('error');
      setError(String(e.message || e));
      throw e;
    }
  }, [item, query, pollEnrichment]);

  // Canonical save through the lookup→save pipeline; returns the save response
  // (incl. entry_id) so callers can chain (e.g. add to the SRS deck).
  const persistEntry = useCallback(async () => {
    const typed = query.trim();
    const rich = await runLookup();
    return api('/api/webapp/dictionary/save', buildDictionarySavePayload({
      rich, sourceText: typed, quick, origin: 'webapp_quick_dictionary',
    }));
  }, [runLookup, quick, query]);

  const onSave = useCallback(() => {
    if (save !== 'idle') return;
    setSave('done'); setError('');
    haptic('ok');
    (async () => {
      try { await persistEntry(); }
      catch (e) { setSave('idle'); setError(String(e.message || e)); haptic('bad'); }
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
        setError(String(e.message || e)); haptic('bad');
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
        // Run the SAME canonical breakdown→save pipeline as a typed word, so a tapped
        // synonym/related word is stored as a proper card (article + translation +
        // grammar) instead of bare German text without a translation. (Does not touch
        // the on-screen breakdown state — that still shows the original word.)
        const pair = guessPair(t);
        const data = await api('/api/webapp/dictionary', { word: t, lookup_lang: pair.source });
        const rich = data?.item || null;
        if (rich) {
          rich.__direction = String(data?.direction || rich.__direction || `${pair.source}-${pair.target}`).trim();
          rich.__language_pair = data?.language_pair || null;
        }
        await api('/api/webapp/dictionary/save', buildDictionarySavePayload({
          rich, sourceText: t, quick: null, origin: 'webapp_quick_dictionary_related',
        }));
      } catch (e) {
        setSavedChips((prev) => { const n = new Set(prev); n.delete(t); return n; });
        setError(String(e.message || e)); haptic('bad');
      }
    })();
  }, []);

  const openFull = useCallback(() => {
    try { window.location.assign('/webapp?startapp=dictionary'); } catch (_e) { /* ignore */ }
  }, []);

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

  return (
    <div className="ans-root dq-scroll">
      <div className="ans-card dq-card">
        <div className="ans-head">
          <span className="ans-eyebrow">📖 Быстрый словарь</span>
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
          <input
            ref={inputRef}
            className="ans-input dq-input"
            type="text"
            inputMode="text"
            autoComplete="off"
            placeholder="Слово или фраза…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
          />
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
              {((item?.article || quick.article) && quick.sourceLang === 'de')
                ? <><span className={`dq-art ${genderClass(item?.article || quick.article)}`}>{item?.article || quick.article}</span> </> : ''}{headSource}
              {correctedNote && <span className="dq-corrected">исправлено с «{quick.source}»</span>}
            </div>
            <div className="dq-translation">
              {((item?.article || quick.article) && quick.targetLang === 'de')
                ? <><span className={`dq-art ${genderClass(item?.article || quick.article)}`}>{item?.article || quick.article}</span> </> : ''}
              {headTranslation}
              {germanText && <SpeakButton text={germanText} tts={tts} />}
            </div>
            {item && <WordBreakdown item={item} tts={tts} onSaveChip={saveChip} savedChips={savedChips} />}
            {enrich === 'loading' && <div className="dq-muted">Готовлю полный разбор…</div>}
            {deepLoading && <div className="dq-muted dq-deep-loading">Дополняю: этимология, примеры, как запомнить…</div>}

            <div className="dq-actions">
              {!item && enrich !== 'loading' && (
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

        <button type="button" className="dq-full" onClick={openFull}>
          Открыть полный словарь →
        </button>
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
