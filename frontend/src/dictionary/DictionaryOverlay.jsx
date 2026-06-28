import React, { useCallback, useEffect, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';

/**
 * Lightweight "quick dictionary" overlay — a compact bottom-sheet translator
 * launched as a Direct-Link Mini App via ?startapp=dict (see main.jsx). It mounts
 * ONLY this tiny screen and skips the heavy main App, so the circled chat-list
 * "Open" button opens an instant, neat dictionary instead of the full app.
 *
 * Flow: type a word/phrase → instant free quick-translate (/api/translate/quick,
 * never gated) → optional GPT breakdown (/api/webapp/dictionary) → 💾 save through
 * the canonical save pipeline (/api/webapp/dictionary/save). 🔊 reuses the shared
 * TTS pipeline for the German side. For folders / PDF / senses the footer opens
 * the full dictionary screen.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    return new URLSearchParams(window.location.search).get('initData') || '';
  }
  return '';
}

function haptic(type) {
  try {
    if (type === 'ok') tg?.HapticFeedback?.notificationOccurred?.('success');
    else if (type === 'bad') tg?.HapticFeedback?.notificationOccurred?.('error');
    else tg?.HapticFeedback?.impactOccurred?.('light');
  } catch (_e) { /* ignore */ }
}

async function api(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() },
    body: JSON.stringify({ initData: getInitData(), ...body }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const err = new Error(data?.error || 'Fehler');
    err.status = response.status;
    err.payload = data;
    throw err;
  }
  return data;
}

// Cheap language guess for the instant path: any Cyrillic → translate ru→de,
// otherwise treat as German → ru. The full GPT lookup auto-detects properly.
function guessPair(text) {
  const hasCyrillic = /[А-Яа-яЁё]/.test(String(text || ''));
  return hasCyrillic
    ? { source: 'ru', target: 'de' }
    : { source: 'de', target: 'ru' };
}

function formatExample(example) {
  if (typeof example === 'string') return String(example || '').trim();
  if (example && typeof example === 'object') {
    const source = String(example.source || '').trim();
    const target = String(example.target || '').trim();
    if (source && target) return `${source} → ${target}`;
    return source || target;
  }
  return '';
}

// Russian labels for the part-of-speech badge.
const POS_LABELS = {
  noun: 'существительное',
  verb: 'глагол',
  adjective: 'прилагательное',
  adverb: 'наречие',
  pronoun: 'местоимение',
  preposition: 'предлог',
  phrase: 'выражение',
  other: '',
};

function clean(value) {
  return String(value || '').trim();
}

// Alternative translation variants beyond the headword (formal/informal/slang…).
function translationVariants(item) {
  const list = Array.isArray(item?.translations) ? item.translations : [];
  return list
    .map((t) => (t && typeof t === 'object'
      ? { value: clean(t.value), context: clean(t.context) }
      : { value: clean(t), context: '' }))
    .filter((t) => t.value);
}

// Ordered meanings: the single primary, then secondary senses. Each carries an
// optional context label and one example pair (source → target).
function meaningList(item) {
  const m = item?.meanings;
  if (!m || typeof m !== 'object') return [];
  const out = [];
  const take = (entry) => {
    if (!entry || typeof entry !== 'object') return;
    const value = clean(entry.value);
    const context = clean(entry.context);
    if (!value && !context) return;
    const exSource = clean(entry.example_source);
    const exTarget = clean(entry.example_target);
    out.push({
      value,
      context,
      example: exSource && exTarget ? `${exSource} → ${exTarget}` : (exSource || exTarget || ''),
    });
  };
  take(m.primary);
  if (Array.isArray(m.secondary)) m.secondary.forEach(take);
  return out;
}

// Part-of-speech-specific grammar rows (article/plural for nouns, conjugation for
// verbs, comparison for adjectives). Returns [label, value] pairs, present only.
function grammarRows(item) {
  const pos = clean(item?.part_of_speech).toLowerCase();
  const f = (item?.forms && typeof item.forms === 'object') ? item.forms : {};
  const rows = [];
  const push = (label, value) => { const v = clean(value); if (v) rows.push([label, v]); };
  if (pos === 'noun') {
    push('Артикль', item?.article);
    push('Мн. число', f.plural);
    push('Род. падеж', f.genitive);
  } else if (pos === 'verb') {
    if (item?.is_separable === true) push('Тип', 'отделяемый');
    else if (item?.is_separable === false) push('Тип', 'неотделяемый');
    push('Präsens (er/sie/es)', f.present_3sg);
    push('Präteritum', f.praeteritum);
    push('Perfekt', f.perfekt);
    push('Konjunktiv II', f.konjunktiv2);
  } else if (pos === 'adjective' || pos === 'adverb') {
    push('Сравнит.', f.comparative);
    push('Превосх.', f.superlative);
  } else {
    // Fall back to whatever forms exist for anything else.
    push('Мн. число', f.plural);
    push('Род. падеж', f.genitive);
    push('Präteritum', f.praeteritum);
    push('Perfekt', f.perfekt);
  }
  return rows;
}

// Verb/adjective government (preposition + case) with an example.
function governmentList(item) {
  const list = Array.isArray(item?.government_patterns) ? item.government_patterns : [];
  return list
    .map((g) => {
      if (!g || typeof g !== 'object') return null;
      const head = clean(g.pattern) || [clean(g.preposition), clean(g.case)].filter(Boolean).join(' + ');
      const exSource = clean(g.example_source);
      const exTarget = clean(g.example_target);
      const example = exSource && exTarget ? `${exSource} → ${exTarget}` : (exSource || exTarget || '');
      return head ? { head, example } : null;
    })
    .filter(Boolean);
}

function collocationList(item) {
  return (Array.isArray(item?.common_collocations) ? item.common_collocations : [])
    .map(clean)
    .filter(Boolean);
}

function pronunciationText(item) {
  const p = item?.pronunciation;
  if (!p || typeof p !== 'object') return '';
  const ipa = clean(p.ipa);
  const stress = clean(p.stress);
  if (ipa && stress && stress !== ipa) return `${ipa} · ${stress}`;
  return ipa || stress || '';
}

// Full dictionary-grade breakdown of a looked-up item, adapting to its part of
// speech (article/plural for nouns, conjugation for verbs, comparison for
// adjectives, register notes for phrases) plus meanings, collocations, government,
// examples and etymology — only the sections that actually carry data are shown.
function RichBreakdown({ item }) {
  if (!item) return null;
  const pos = clean(item.part_of_speech).toLowerCase();
  const posLabel = Object.prototype.hasOwnProperty.call(POS_LABELS, pos)
    ? POS_LABELS[pos]
    : clean(item.part_of_speech);
  const pron = pronunciationText(item);
  const variants = translationVariants(item);
  const meanings = meaningList(item);
  const grammar = grammarRows(item);
  const government = governmentList(item);
  const collocations = collocationList(item);
  const examples = (Array.isArray(item.usage_examples) ? item.usage_examples : [])
    .map(formatExample)
    .filter(Boolean);
  const etymology = clean(item.etymology_note);
  const memoryTip = clean(item.memory_tip);
  const usage = [
    clean(item.real_life_usage),
    clean(item.register_note),
    clean(item.expression_note),
    clean(item.usage_note),
  ].filter(Boolean);

  return (
    <>
      {(posLabel || pron) && (
        <div className="dq-meta">
          {posLabel && <span className="dq-pos-chip">{posLabel}</span>}
          {pron && <span className="dq-ipa">{pron}</span>}
        </div>
      )}

      {meanings.length > 0 && (
        <div className="dq-block">
          <strong>Значения</strong>
          <ol className="dq-meanings">
            {meanings.map((m, i) => (
              <li key={`${m.value}-${i}`} className="dq-meaning">
                <span className="dq-mean-head">
                  {m.value}{m.context ? <em> · {m.context}</em> : null}
                </span>
                {m.example && <span className="dq-mean-ex">{m.example}</span>}
              </li>
            ))}
          </ol>
        </div>
      )}

      {variants.length > 0 && (
        <div className="dq-block">
          <strong>Варианты перевода</strong>
          <div className="dq-vars">
            {variants.map((v, i) => (
              <span key={`${v.value}-${i}`} className="dq-var">
                {v.value}{v.context ? <em> · {v.context}</em> : null}
              </span>
            ))}
          </div>
        </div>
      )}

      {grammar.length > 0 && (
        <div className="dq-block">
          <strong>Грамматика</strong>
          <div className="dq-grammar">
            {grammar.map(([label, value]) => (
              <span key={label} className="dq-chip"><em>{label}</em>{value}</span>
            ))}
          </div>
        </div>
      )}

      {government.length > 0 && (
        <div className="dq-block">
          <strong>Управление</strong>
          {government.map((g, i) => (
            <span key={`${g.head}-${i}`} className="dq-gov">
              <b>{g.head}</b>{g.example ? ` — ${g.example}` : ''}
            </span>
          ))}
        </div>
      )}

      {collocations.length > 0 && (
        <div className="dq-block">
          <strong>Устойчивые сочетания</strong>
          <div className="dq-vars">
            {collocations.map((c, i) => <span key={`${c}-${i}`} className="dq-var">{c}</span>)}
          </div>
        </div>
      )}

      {examples.length > 0 && (
        <div className="dq-block">
          <strong>Примеры</strong>
          {examples.slice(0, 3).map((ex, i) => <span key={`${ex}-${i}`}>{ex}</span>)}
        </div>
      )}

      {usage.length > 0 && (
        <div className="dq-block">
          <strong>Где и как употреблять</strong>
          {usage.map((u, i) => <span key={`${u}-${i}`}>{u}</span>)}
        </div>
      )}

      {(etymology || memoryTip) && (
        <div className="dq-block dq-note">
          {etymology && <><strong>Этимология</strong><span>{etymology}</span></>}
          {memoryTip && <><strong>Как запомнить</strong><span>{memoryTip}</span></>}
        </div>
      )}
    </>
  );
}

// Listen via the shared TTS pipeline: POST generate → poll url → play the MP3.
// (Mirrors DeepDiveOverlay's useTts.)
function useTts() {
  const audioRef = useRef(null);
  const seqRef = useRef(0);
  const [state, setState] = useState('idle'); // idle|loading|playing|error

  const stop = useCallback(() => {
    seqRef.current += 1;
    if (audioRef.current) { try { audioRef.current.pause(); } catch (_e) { /* ignore */ } audioRef.current = null; }
    setState('idle');
  }, []);

  const play = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    const mySeq = ++seqRef.current;
    setState('loading');
    haptic('light');
    try {
      await api('/api/webapp/tts/generate', { text: t, language });
      const params = new URLSearchParams({ text: t, language });
      let url = '';
      for (let i = 0; i < 30 && !url; i += 1) {
        if (mySeq !== seqRef.current) return;
        const res = await fetch(`/api/webapp/tts/url?${params.toString()}`, {
          method: 'GET', headers: { 'X-Telegram-InitData': getInitData() },
        });
        const data = await res.json().catch(() => ({}));
        if (data.status === 'ready' && data.audio_url) { url = data.audio_url; break; }
        if (data.status === 'failed') throw new Error(data.message || 'TTS fehlgeschlagen');
        await new Promise((r) => setTimeout(r, data.retry_after_ms || 700));
      }
      if (!url) throw new Error('Zeitüberschreitung');
      if (mySeq !== seqRef.current) return;
      const audio = new Audio(url);
      audioRef.current = audio;
      audio.onended = () => { if (mySeq === seqRef.current) setState('idle'); };
      audio.onerror = () => { if (mySeq === seqRef.current) setState('error'); };
      setState('playing');
      await audio.play();
    } catch (_e) {
      if (mySeq === seqRef.current) { setState('error'); haptic('bad'); }
    }
  }, []);

  // Fire-and-forget prewarm: kick off generation so the audio is already cached
  // (status ready) by the time the user taps 🔊. No polling, no playback, never
  // touches playback state — so it can't disrupt an in-flight play().
  const warm = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    try { await api('/api/webapp/tts/generate', { text: t, language }); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => () => stop(), [stop]);
  return { state, play, stop, warm };
}

export default function DictionaryOverlay() {
  const [query, setQuery] = useState('');
  const [phase, setPhase] = useState('idle'); // idle|loading|done|error
  const [quick, setQuick] = useState(null);   // { source, target, translation, sourceLang, targetLang, direction }
  const [item, setItem] = useState(null);     // rich GPT item (for enrich + canonical save)
  const [enrich, setEnrich] = useState('idle'); // idle|loading|done|error
  const [save, setSave] = useState('idle');   // idle|saving|done
  const [error, setError] = useState('');
  const seqRef = useRef(0);
  const inputRef = useRef(null);
  const tts = useTts();

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

  // Prewarm pronunciation as soon as the German text is known (user accepted the
  // small TTS-quota cost), so tapping 🔊 plays from cache instead of waiting on the
  // queue+worker round-trip.
  const { warm: warmTts } = tts;
  useEffect(() => {
    if (germanText) warmTts(germanText, 'de-DE');
  }, [germanText, warmTts]);

  const translate = useCallback(async () => {
    const text = query.trim();
    if (!text || phase === 'loading') return;
    const mySeq = ++seqRef.current;
    tts.stop();
    setPhase('loading'); setError(''); setItem(null); setEnrich('idle'); setSave('idle');
    haptic('light');
    try {
      const pair = guessPair(text);
      const data = await api('/api/translate/quick', {
        text, source_lang: pair.source, target_lang: pair.target,
      });
      if (mySeq !== seqRef.current) return;
      const detected = String(data?.detected_source_lang || pair.source).toLowerCase();
      const targetLang = detected === pair.target ? pair.source : pair.target;
      setQuick({
        source: text,
        translation: String(data?.translation || '').trim(),
        sourceLang: detected,
        targetLang,
        direction: `${detected}-${targetLang}`,
        provider: String(data?.provider || '').trim(),
      });
      setPhase('done'); haptic('ok');
    } catch (e) {
      if (mySeq !== seqRef.current) return;
      setError(String(e.message || e)); setPhase('error'); haptic('bad');
    }
  }, [query, phase, tts]);

  // Full GPT breakdown (article, Grundform, senses, examples). Returns the rich
  // item so save can reuse it. Gracefully surfaces free-tier limit (429).
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
      return rich;
    } catch (e) {
      setEnrich('error');
      setError(String(e.message || e));
      throw e;
    }
  }, [item, query]);

  const onSave = useCallback(() => {
    if (save === 'saving' || save === 'done') return;
    // Optimistic: flip to ✅ instantly and release the user — the canonical
    // lookup→save (article + Grundform) runs in the background and also enriches
    // the visible card. Revert to idle only if it genuinely fails.
    setSave('done'); setError('');
    haptic('ok');
    const typed = query.trim();
    const quickTranslation = quick?.translation || '';
    const quickDirection = quick?.direction || '';
    const quickSourceLang = quick?.sourceLang || '';
    const quickTargetLang = quick?.targetLang || '';
    (async () => {
      try {
        const rich = await runLookup();
        const direction = String(rich?.__direction || quickDirection || '').trim();
        const [dirSource, dirTarget] = direction.includes('-') ? direction.split('-', 2) : [];
        const sourceLang = (dirSource || quickSourceLang || '').toLowerCase();
        const targetLang = (dirTarget || quickTargetLang || '').toLowerCase();
        await api('/api/webapp/dictionary/save', {
          word_de: String(rich?.word_de || '').trim(),
          word_ru: String(rich?.word_ru || '').trim(),
          translation_de: String(rich?.translation_de || '').trim(),
          translation_ru: String(rich?.translation_ru || '').trim(),
          source_text: typed,
          target_text: quickTranslation,
          source_lang: sourceLang || undefined,
          target_lang: targetLang || undefined,
          direction: direction || undefined,
          response_json: rich || undefined,
          origin_process: 'webapp_quick_dictionary',
        });
      } catch (e) {
        setSave('idle');
        setError(String(e.message || e)); haptic('bad');
      }
    })();
  }, [save, runLookup, quick, query]);

  const openFull = useCallback(() => {
    try { window.location.assign('/webapp?startapp=dictionary'); } catch (_e) { /* ignore */ }
  }, []);

  const onKeyDown = (e) => { if (e.key === 'Enter') { e.preventDefault(); translate(); } };

  return (
    <div className="ans-root">
      <div className="ans-card dq-card">
        <div className="ans-head">
          <span className="ans-eyebrow">📖 Быстрый словарь</span>
        </div>

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
            onClick={translate}
            disabled={!query.trim() || phase === 'loading'}
          >
            {phase === 'loading' ? '…' : 'Перевести'}
          </button>
        </div>

        {phase === 'idle' && (
          <div className="dq-hint">Введите слово — увидите перевод. Можно прослушать и сохранить в словарь.</div>
        )}

        {phase === 'error' && error && <div className="dd-err">{error}</div>}

        {quick && phase !== 'loading' && (
          <div className="dq-result">
            <div className="dq-source">
              {(item?.article && quick.sourceLang === 'de') ? `${item.article} ` : ''}{quick.source}
            </div>
            <div className="dq-translation">
              {/* The German article only belongs to the German word, never to the
                  Russian translation (was producing "die Порядок действий"). */}
              {(item?.article && item?.word_de && quick.targetLang === 'de') ? `${item.article} ` : ''}
              {(item?.word_de && quick.targetLang === 'de') ? item.word_de : (quick.translation || '—')}
              {germanText && (
                <button
                  type="button"
                  className={`dq-tts${tts.state === 'playing' ? ' on' : ''}`}
                  onClick={() => (tts.state === 'playing' ? tts.stop() : tts.play(germanText, 'de-DE'))}
                  disabled={tts.state === 'loading'}
                  aria-label="Прослушать"
                >
                  {tts.state === 'loading' ? '⏳' : tts.state === 'playing' ? '⏹' : '🔊'}
                </button>
              )}
            </div>
            {item && <RichBreakdown item={item} />}
            {enrich === 'loading' && <div className="dq-muted">Готовлю полный разбор…</div>}

            <div className="dq-actions">
              {!item && enrich !== 'loading' && (
                <button type="button" className="dd-action" onClick={() => runLookup().catch(() => {})}>
                  📖 Подробный разбор
                </button>
              )}
              {save === 'done' ? (
                <div className="dd-saved">✅ Сохранено в словарь</div>
              ) : (
                <button type="button" className="dd-save" onClick={onSave} disabled={save === 'saving'}>
                  {save === 'saving' ? 'Сохраняю…' : '💾 Сохранить в словарь'}
                </button>
              )}
            </div>
          </div>
        )}

        <button type="button" className="dq-full" onClick={openFull}>
          Открыть полный словарь →
        </button>
      </div>
    </div>
  );
}
