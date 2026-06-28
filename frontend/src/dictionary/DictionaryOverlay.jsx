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

// Recent lookups persisted locally (most-recent first, max 6). Pure helpers so
// the start card can offer one-tap repeat lookups.
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

const hasCyrillic = (s) => /[А-Яа-яЁё]/.test(String(s || ''));

// Collect structured example pairs {de, ru} from usage_examples + meaning
// examples. The German side is the non-Cyrillic one, the Russian side Cyrillic —
// reliable for the RU↔DE pair. Deduplicated by the German sentence, max 4.
function collectExamples(item) {
  const pairs = [];
  const add = (a, b) => {
    const x = clean(a);
    const y = clean(b);
    if (!x && !y) return;
    let de = '';
    let ru = '';
    if (hasCyrillic(x) && !hasCyrillic(y)) { ru = x; de = y; }
    else if (!hasCyrillic(x) && hasCyrillic(y)) { de = x; ru = y; }
    else { de = x; ru = y; } // fallback: keep order
    if (de) pairs.push({ de, ru });
  };
  (Array.isArray(item?.usage_examples) ? item.usage_examples : []).forEach((ex) => {
    if (ex && typeof ex === 'object') add(ex.source, ex.target);
  });
  const m = item?.meanings;
  if (m && typeof m === 'object') {
    const take = (e) => { if (e && typeof e === 'object') add(e.example_source, e.example_target); };
    take(m.primary);
    if (Array.isArray(m.secondary)) m.secondary.forEach(take);
  }
  const seen = new Set();
  const out = [];
  for (const p of pairs) {
    const key = p.de.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
    if (out.length >= 4) break;
  }
  return out;
}

// 🔊 button that reflects only its own text's play state (headword + each example
// share one TTS hook). Compact variant for example rows.
function SpeakButton({ text, tts, sm }) {
  const t = clean(text);
  if (!t) return null;
  const isLoading = tts.state === 'loading' && tts.playingText === t;
  const isPlaying = tts.state === 'playing' && tts.playingText === t;
  return (
    <button
      type="button"
      className={`dq-tts${sm ? ' sm' : ''}${isPlaying ? ' on' : ''}`}
      onClick={() => (isPlaying ? tts.stop() : tts.play(t, 'de-DE'))}
      disabled={isLoading}
      aria-label="Прослушать"
    >
      {isLoading ? '⏳' : isPlaying ? '⏹' : '🔊'}
    </button>
  );
}

// Example sentences: German + 🔊 always visible; the Russian translation is
// hidden behind a tap (self-test) with a block-level "show/hide all" toggle.
function ExamplesBlock({ examples, tts }) {
  const [revealed, setRevealed] = useState(() => new Set());
  const [all, setAll] = useState(false);
  if (!examples || examples.length === 0) return null;
  const toggleOne = (i) => setRevealed((prev) => {
    const next = new Set(prev);
    if (next.has(i)) next.delete(i); else next.add(i);
    return next;
  });
  const anyRu = examples.some((e) => e.ru);
  return (
    <div className="dq-block">
      <div className="dq-ex-head">
        <strong>Примеры</strong>
        {anyRu && (
          <button type="button" className="dq-ex-toggle" onClick={() => setAll((v) => !v)}>
            {all ? 'Скрыть перевод' : 'Показать перевод'}
          </button>
        )}
      </div>
      <div className="dq-ex-list">
        {examples.map((ex, i) => {
          const show = all || revealed.has(i);
          return (
            <div key={`${ex.de}-${i}`} className="dq-ex">
              <div className="dq-ex-de">
                <SpeakButton text={ex.de} tts={tts} sm />
                <span>{ex.de}</span>
              </div>
              {ex.ru && (show
                ? <button type="button" className="dq-ex-ru" onClick={() => toggleOne(i)}>{ex.ru}</button>
                : <button type="button" className="dq-ex-reveal" onClick={() => toggleOne(i)}>Показать перевод</button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
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

// Russian labels for the frequency band.
const FREQ_LABELS = {
  very_common: 'очень частое',
  common: 'частое',
  uncommon: 'нечастое',
  rare: 'редкое',
};

// Compound / affix breakdown ({is_compound, parts:[{text,gloss}], note}) → the
// list of parts worth showing. Single non-compound words yield [].
function wordFormationParts(item) {
  const wf = item?.word_formation;
  if (!wf || typeof wf !== 'object') return { parts: [], note: '' };
  const parts = (Array.isArray(wf.parts) ? wf.parts : [])
    .map((p) => (p && typeof p === 'object'
      ? { text: clean(p.text), gloss: clean(p.gloss) }
      : { text: clean(p), gloss: '' }))
    .filter((p) => p.text);
  return { parts: parts.length >= 2 ? parts : [], note: clean(wf.note) };
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

// Gender → color class (der=blue, die=red, das=green), the classic learner
// mnemonic. Applied to every article we render so the gender is felt visually.
function genderClass(article) {
  const a = clean(article).toLowerCase();
  if (a === 'der') return 'g-m';
  if (a === 'die') return 'g-f';
  if (a === 'das') return 'g-n';
  return '';
}

// Wrap the leading article of a "der Tisch" / "den Tischen" string in a colored
// span so the gender reads at a glance. The gender class is passed in (the table
// knows it) so declined forms like den/dem/des still color correctly.
function ColoredForm({ text, genderCls }) {
  const t = clean(text);
  const m = t.match(/^(der|die|das|den|dem|des)\s+(.*)$/i);
  if (!m) return <>{t}</>;
  return <><span className={`dq-art ${genderCls || ''}`}>{m[1]}</span> {m[2]}</>;
}

// 'm'|'f'|'n' (declension table gender) → the same color class as the article.
function genderClassFromKey(g) {
  return g === 'm' ? 'g-m' : g === 'f' ? 'g-f' : g === 'n' ? 'g-n' : '';
}

const PRON_ORDER = ['ich', 'du', 'er/sie/es', 'wir', 'ihr', 'sie/Sie'];

// One labelled conjugation block (Präsens / Präteritum / …) as a pronoun→form grid.
function ConjBlock({ title, forms }) {
  if (!forms || typeof forms !== 'object') return null;
  const rows = PRON_ORDER.filter((p) => clean(forms[p]));
  if (rows.length === 0) return null;
  return (
    <div className="dq-conj-block">
      <div className="dq-conj-title">{title}</div>
      <div className="dq-conj-grid">
        {rows.map((p) => (
          <React.Fragment key={p}>
            <span className="dq-conj-pron">{p}</span>
            <span className="dq-conj-form">{forms[p]}</span>
          </React.Fragment>
        ))}
      </div>
    </div>
  );
}

// POS-aware deterministic grammar tables built server-side (item.grammar_tables):
// full noun declension, verb conjugation, adjective comparison. Each section is a
// collapsible <details> so the compact card stays light until the user expands.
function GrammarTables({ tables }) {
  if (!tables || typeof tables !== 'object') return null;
  const decl = tables.declension;
  const conj = tables.conjugation;
  const comp = tables.comparison;

  if (decl && Array.isArray(decl.rows) && decl.rows.length > 0) {
    const gc = genderClassFromKey(decl.gender);
    // Plurals share one article color (die/den/der) — neutral, since plural has no
    // gender; keep the singular column gendered for the mnemonic.
    return (
      <details className="dq-gt" open>
        <summary>Склонение{decl.plural ? ` · мн. ${decl.plural}` : ''}</summary>
        <table className="dq-decl">
          <thead>
            <tr><th /><th>Singular</th>{decl.has_plural && <th>Plural</th>}</tr>
          </thead>
          <tbody>
            {decl.rows.map((r) => (
              <tr key={r.case}>
                <td className="dq-decl-case">{r.label}</td>
                <td><ColoredForm text={r.singular} genderCls={gc} /></td>
                {decl.has_plural && <td><ColoredForm text={r.plural} /></td>}
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    );
  }

  if (conj && conj.praesens) {
    const stamm = [conj.infinitive, conj.praeteritum?.['er/sie/es'], conj.partizip2]
      .filter(Boolean);
    return (
      <details className="dq-gt" open>
        <summary>Спряжение{conj.auxiliary ? ` · ${conj.auxiliary}` : ''}</summary>
        {stamm.length === 3 && (
          <div className="dq-stamm">{stamm.join(' – ')}</div>
        )}
        <ConjBlock title="Präsens" forms={conj.praesens} />
        <ConjBlock title="Präteritum" forms={conj.praeteritum} />
        <ConjBlock title="Perfekt" forms={conj.perfekt} />
        <ConjBlock title="Konjunktiv II" forms={conj.konjunktiv2} />
        {conj.imperativ && (clean(conj.imperativ.du) || clean(conj.imperativ.ihr)) && (
          <div className="dq-imp">
            <span className="dq-conj-title">Imperativ</span>
            <span>{[
              conj.imperativ.du && `du: ${conj.imperativ.du}`,
              conj.imperativ.ihr && `ihr: ${conj.imperativ.ihr}`,
              conj.imperativ.Sie && conj.imperativ.Sie,
            ].filter(Boolean).join(' · ')}</span>
          </div>
        )}
      </details>
    );
  }

  if (comp && comp.positive) {
    return (
      <details className="dq-gt" open>
        <summary>Степени сравнения</summary>
        <div className="dq-deg">
          <span className="dq-deg-item"><em>Positiv</em>{comp.positive}</span>
          <span className="dq-deg-arrow">→</span>
          <span className="dq-deg-item"><em>Komparativ</em>{comp.comparative}</span>
          <span className="dq-deg-arrow">→</span>
          <span className="dq-deg-item"><em>Superlativ</em>{comp.superlative}</span>
        </div>
      </details>
    );
  }

  return null;
}

// Full dictionary-grade breakdown of a looked-up item, adapting to its part of
// speech (article/plural for nouns, conjugation for verbs, comparison for
// adjectives, register notes for phrases) plus meanings, collocations, government,
// examples and etymology — only the sections that actually carry data are shown.
function RichBreakdown({ item, tts }) {
  if (!item) return null;
  const pos = clean(item.part_of_speech).toLowerCase();
  const posLabel = Object.prototype.hasOwnProperty.call(POS_LABELS, pos)
    ? POS_LABELS[pos]
    : clean(item.part_of_speech);
  const pron = pronunciationText(item);
  const variants = translationVariants(item);
  const meanings = meaningList(item);
  const grammar = grammarRows(item);
  const gt = item.grammar_tables;
  const hasTables = !!(gt && (gt.declension || gt.conjugation || gt.comparison));
  const government = governmentList(item);
  const collocations = collocationList(item);
  const examples = collectExamples(item);
  const etymology = clean(item.etymology_note);
  const memoryTip = clean(item.memory_tip);
  const level = clean(item.level).toUpperCase();
  const freqLabel = FREQ_LABELS[clean(item.frequency).toLowerCase()] || '';
  const formation = wordFormationParts(item);
  const usage = [
    clean(item.real_life_usage),
    clean(item.register_note),
    clean(item.expression_note),
    clean(item.usage_note),
  ].filter(Boolean);

  return (
    <>
      {(posLabel || pron || level || freqLabel) && (
        <div className="dq-meta">
          {posLabel && <span className="dq-pos-chip">{posLabel}</span>}
          {level && <span className="dq-level-chip">{level}</span>}
          {freqLabel && <span className="dq-freq-chip">{freqLabel}</span>}
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

      {hasTables ? (
        <div className="dq-block">
          <GrammarTables tables={item.grammar_tables} />
        </div>
      ) : grammar.length > 0 && (
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

      <ExamplesBlock examples={examples} tts={tts} />

      {usage.length > 0 && (
        <div className="dq-block">
          <strong>Где и как употреблять</strong>
          {usage.map((u, i) => <span key={`${u}-${i}`}>{u}</span>)}
        </div>
      )}

      {formation.parts.length > 0 && (
        <div className="dq-block">
          <strong>Состав слова</strong>
          <div className="dq-formation">
            {formation.parts.map((p, i) => (
              <React.Fragment key={`${p.text}-${i}`}>
                {i > 0 && <span className="dq-formation-plus">+</span>}
                <span className="dq-formation-part">
                  <b>{p.text}</b>{p.gloss ? <em>{p.gloss}</em> : null}
                </span>
              </React.Fragment>
            ))}
          </div>
          {formation.note && <span className="dq-muted">{formation.note}</span>}
        </div>
      )}

      {etymology && (
        <div className="dq-block">
          <strong>Происхождение</strong>
          <span>{etymology}</span>
        </div>
      )}

      {memoryTip && (
        <div className="dq-block dq-note">
          <strong>💡 Как запомнить</strong>
          <span>{memoryTip}</span>
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
  // The exact text currently loading/playing, so several 🔊 buttons (headword +
  // each example) can each reflect only their own state.
  const [playingText, setPlayingText] = useState('');

  const stop = useCallback(() => {
    seqRef.current += 1;
    if (audioRef.current) { try { audioRef.current.pause(); } catch (_e) { /* ignore */ } audioRef.current = null; }
    setState('idle');
    setPlayingText('');
  }, []);

  const play = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    const mySeq = ++seqRef.current;
    setState('loading');
    setPlayingText(t);
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
      audio.onended = () => { if (mySeq === seqRef.current) { setState('idle'); setPlayingText(''); } };
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
  return { state, playingText, play, stop, warm };
}

export default function DictionaryOverlay() {
  const [query, setQuery] = useState('');
  const [phase, setPhase] = useState('idle'); // idle|loading|done|error
  const [quick, setQuick] = useState(null);   // { source, target, translation, sourceLang, targetLang, direction }
  const [item, setItem] = useState(null);     // rich GPT item (for enrich + canonical save)
  const [enrich, setEnrich] = useState('idle'); // idle|loading|done|error
  const [deepLoading, setDeepLoading] = useState(false); // background enrichment poll
  const [save, setSave] = useState('idle');   // idle|saving|done
  const [error, setError] = useState('');
  const [recents, setRecents] = useState(loadRecents);
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

  // The instant translate uses fast non-LLM engines that mishandle typos /
  // compounds (e.g. "Klimzugstange" → "Тяга климатической системы"). Once the LLM
  // breakdown arrives it carries the corrected German form and a proper
  // translation, so prefer those for the headword and replace the raw MT result.
  // word_de from the LLM already includes the article ("die Dunstabzugshaube");
  // we render the article in a colored span separately, so strip it here to avoid
  // "die die Dunstabzugshaube".
  const corrDe = String(item?.word_de || '').trim().replace(/^(der|die|das)\s+/i, '');
  const bestRu = String(
    item?.translation_ru
    || item?.word_ru
    || item?.meanings?.primary?.value
    || '',
  ).trim();
  // What to show big (the translation side) and small (the source side).
  const headTranslation = quick?.targetLang === 'de'
    ? (corrDe || quick?.translation || '—')
    : (bestRu || quick?.translation || '—');
  const headSource = (quick?.sourceLang === 'de' && corrDe) ? corrDe : (quick?.source || '');
  // Show the spelling correction only when it actually changed the typed word.
  const correctedNote = (corrDe && quick?.sourceLang === 'de'
    && corrDe.toLowerCase() !== String(quick?.source || '').trim().toLowerCase())
    ? corrDe : '';

  // Prewarm pronunciation as soon as the German text is known (user accepted the
  // small TTS-quota cost), so tapping 🔊 plays from cache instead of waiting on the
  // queue+worker round-trip.
  const { warm: warmTts } = tts;
  useEffect(() => {
    if (germanText) warmTts(germanText, 'de-DE');
  }, [germanText, warmTts]);

  const translate = useCallback(async (overrideText) => {
    const text = (typeof overrideText === 'string' ? overrideText : query).trim();
    if (!text || phase === 'loading') return;
    if (text !== query) setQuery(text);
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
  }, [query, phase, tts]);

  // The first lookup returns a FAST "core" item (article, senses, basic forms)
  // with the heavy parts (etymology, memory tip, word formation, collocations,
  // government, level/frequency) still enriching in the background. Poll the
  // status endpoint and swap in the fuller item as it arrives, so the user sees
  // every block without any extra action.
  const pollEnrichment = useCallback(async (lookupId, base) => {
    if (!lookupId) return;
    const mySeq = seqRef.current; // bumped by translate(); abort if a new lookup starts
    setDeepLoading(true);
    try {
      // Humans read for a few seconds before noticing new blocks — no need to
      // hammer the server. First check after 1.5s (enrichment is often done by
      // then), then every 3s. ~15 tries = a ~45s ceiling. Cuts request volume
      // ~3× vs a tight loop, which matters at scale.
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

  // Paste from clipboard (fires inside the tap gesture, so the browser grants
  // read access) and translate immediately.
  const onPaste = useCallback(async () => {
    try {
      const text = (await navigator.clipboard.readText() || '').trim();
      if (text) { setQuery(text); translate(text); }
    } catch (_e) { try { inputRef.current?.focus(); } catch (_e2) { /* ignore */ } }
  }, [translate]);

  const onKeyDown = (e) => { if (e.key === 'Enter') { e.preventDefault(); translate(); } };

  return (
    <div className="ans-root dq-scroll">
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
          <>
            <div className="dq-hint">
              Введите слово — увидите перевод, грамматику и пример.{' '}
              <button type="button" className="dq-paste" onClick={onPaste}>Вставить</button>
            </div>
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
          </>
        )}

        {phase === 'error' && error && <div className="dd-err">{error}</div>}

        {quick && phase !== 'loading' && (
          <div className="dq-result">
            <div className="dq-source">
              {((item?.article || quick.article) && quick.sourceLang === 'de')
                ? <><span className={`dq-art ${genderClass(item?.article || quick.article)}`}>{item?.article || quick.article}</span> </> : ''}{headSource}
              {correctedNote && <span className="dq-corrected">исправлено с «{quick.source}»</span>}
            </div>
            <div className="dq-translation">
              {/* The German article only belongs to the German word, never to the
                  Russian translation (was producing "die Порядок действий"). */}
              {((item?.article || quick.article) && quick.targetLang === 'de')
                ? <><span className={`dq-art ${genderClass(item?.article || quick.article)}`}>{item?.article || quick.article}</span> </> : ''}
              {headTranslation}
              {germanText && <SpeakButton text={germanText} tts={tts} />}
            </div>
            {item && <RichBreakdown item={item} tts={tts} />}
            {enrich === 'loading' && <div className="dq-muted">Готовлю полный разбор…</div>}
            {deepLoading && <div className="dq-muted dq-deep-loading">Дополняю: этимология, примеры, как запомнить…</div>}

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
