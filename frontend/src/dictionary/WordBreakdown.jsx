import React, { useCallback, useEffect, useRef, useState } from 'react';
import './dict.css';

/**
 * Shared word-breakdown rendering — the deep, POS-aware dictionary card used by
 * BOTH the lightweight quick-dictionary overlay AND the full dictionary inside the
 * main app (Search results + Library word detail). It renders a looked-up item OR
 * a saved entry's response_json: meanings, declension/conjugation tables, synonyms/
 * antonyms/related (tap to save), examples (audio + reveal), etymology, memory tip,
 * word formation, level/frequency/register/IPA, with der/die/das gender color.
 *
 * Exports: WordBreakdown (the renderer), useTts (audio hook), SpeakButton, genderClass,
 * and the api/getInitData/haptic helpers so callers share one TTS + save pipeline.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

export function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    return new URLSearchParams(window.location.search).get('initData') || '';
  }
  return '';
}

export function haptic(type) {
  try {
    if (type === 'ok') tg?.HapticFeedback?.notificationOccurred?.('success');
    else if (type === 'bad') tg?.HapticFeedback?.notificationOccurred?.('error');
    else tg?.HapticFeedback?.impactOccurred?.('light');
  } catch (_e) { /* ignore */ }
}

export async function api(path, body) {
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

const hasCyrillic = (s) => /[А-Яа-яЁё]/.test(String(s || ''));

export function clean(value) {
  return String(value || '').trim();
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

// Russian labels for the frequency band.
const FREQ_LABELS = {
  very_common: 'очень частое',
  common: 'частое',
  uncommon: 'нечастое',
  rare: 'редкое',
};
const FREQ_RANK = { very_common: 4, common: 3, uncommon: 2, rare: 1 };

const PRON_ORDER = ['ich', 'du', 'er/sie/es', 'wir', 'ihr', 'sie/Sie'];

// Collect structured example pairs {de, ru} from usage_examples + meaning examples.
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
    else { de = x; ru = y; }
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

// 🔊 button that reflects only its own text's play state.
export function SpeakButton({ text, tts, sm }) {
  const t = clean(text);
  if (!t || !tts) return null;
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

// Example sentences: German + 🔊 always visible; the Russian hidden behind a tap.
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

// Frequency as a 4-segment bar + label.
function FreqBar({ frequency }) {
  const key = clean(frequency).toLowerCase();
  const rank = FREQ_RANK[key] || 0;
  const label = FREQ_LABELS[key];
  if (!rank || !label) return null;
  return (
    <span className="dq-freqbar" title={`Частотность: ${label}`}>
      <span className="dq-freqbar-track">
        {[1, 2, 3, 4].map((i) => (
          <span key={i} className={`dq-freqbar-seg${i <= rank ? ' on' : ''}`} />
        ))}
      </span>
      <span className="dq-freqbar-label">{label}</span>
    </span>
  );
}

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

function translationVariants(item) {
  const list = Array.isArray(item?.translations) ? item.translations : [];
  return list
    .map((t) => (t && typeof t === 'object'
      ? { value: clean(t.value), context: clean(t.context) }
      : { value: clean(t), context: '' }))
    .filter((t) => t.value);
}

function meaningList(item) {
  const m = item?.meanings;
  if (!m || typeof m !== 'object') return [];
  const out = [];
  const take = (entry) => {
    if (!entry || typeof entry !== 'object') return;
    const value = clean(entry.value);
    const context = clean(entry.context);
    if (!value && !context) return;
    out.push({ value, context });
  };
  take(m.primary);
  if (Array.isArray(m.secondary)) m.secondary.forEach(take);
  return out;
}

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
    push('Мн. число', f.plural);
    push('Род. падеж', f.genitive);
    push('Präteritum', f.praeteritum);
    push('Perfekt', f.perfekt);
  }
  return rows;
}

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

function stringList(value) {
  return (Array.isArray(value) ? value : []).map(clean).filter(Boolean);
}

function relatedList(item) {
  return (Array.isArray(item?.related_words) ? item.related_words : [])
    .map((r) => (r && typeof r === 'object'
      ? { word: clean(r.word), gloss: clean(r.gloss) }
      : { word: clean(r), gloss: '' }))
    .filter((r) => r.word);
}

function registerLabel(item) {
  const r = clean(item?.register).toLowerCase();
  if (!r || r === 'нейтральное' || r === 'neutral') return '';
  return clean(item.register);
}

function pronunciationText(item) {
  const p = item?.pronunciation;
  if (!p || typeof p !== 'object') return '';
  const ipa = clean(p.ipa);
  const stress = clean(p.stress);
  if (ipa && stress && stress !== ipa) return `${ipa} · ${stress}`;
  return ipa || stress || '';
}

// A tappable German word/phrase pill that saves to the dictionary on tap.
function SaveChip({ text, className, label, saved, onSave }) {
  const t = clean(text);
  if (!t) return null;
  const isSaved = saved && saved.has(t);
  return (
    <button
      type="button"
      className={`dq-var dq-savechip ${className || ''}${isSaved ? ' is-saved' : ''}`}
      onClick={() => onSave && onSave(t)}
      title={isSaved ? 'Сохранено в словарь' : 'Нажмите, чтобы сохранить в словарь'}
    >
      {label || t}{isSaved ? ' ✓' : ''}
    </button>
  );
}

// Gender → color class (der=blue, die=red, das=green).
export function genderClass(article) {
  const a = clean(article).toLowerCase();
  if (a === 'der') return 'g-m';
  if (a === 'die') return 'g-f';
  if (a === 'das') return 'g-n';
  return '';
}

function ColoredForm({ text, genderCls }) {
  const t = clean(text);
  const m = t.match(/^(der|die|das|den|dem|des)\s+(.*)$/i);
  if (!m) return <>{t}</>;
  return <><span className={`dq-art ${genderCls || ''}`}>{m[1]}</span> {m[2]}</>;
}

function genderClassFromKey(g) {
  return g === 'm' ? 'g-m' : g === 'f' ? 'g-f' : g === 'n' ? 'g-n' : '';
}

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

function GrammarTables({ tables }) {
  if (!tables || typeof tables !== 'object') return null;
  const decl = tables.declension;
  const conj = tables.conjugation;
  const comp = tables.comparison;

  if (decl && Array.isArray(decl.rows) && decl.rows.length > 0) {
    const gc = genderClassFromKey(decl.gender);
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

// The full POS-aware breakdown of an item (fresh lookup OR saved response_json).
// `tts` enables audio; `onSaveChip`/`savedChips` enable tap-to-save chips (both
// optional — when omitted the chips are inert text and audio buttons hide).
export function WordBreakdown({ item, tts, onSaveChip, savedChips, hideMeanings }) {
  if (!item) return null;
  const pos = clean(item.part_of_speech).toLowerCase();
  const phraseKind = clean(item.phrase_kind).toLowerCase();
  const PHRASE_KIND_LABELS = { idiom: 'идиома', saying: 'поговорка', collocation: 'устойчивое сочетание' };
  const isPhrase = pos === 'phrase' || pos === 'other';
  const posLabel = (isPhrase && PHRASE_KIND_LABELS[phraseKind])
    || (Object.prototype.hasOwnProperty.call(POS_LABELS, pos) ? POS_LABELS[pos] : clean(item.part_of_speech));
  const literal = clean(item.literal_meaning);
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
  const synonyms = stringList(item.synonyms);
  const antonyms = stringList(item.antonyms);
  const related = relatedList(item);
  const register = registerLabel(item);
  const usage = [
    clean(item.when_to_use),
    clean(item.real_life_usage),
    clean(item.register_note),
    clean(item.expression_note),
    clean(item.usage_note),
  ].filter(Boolean);

  return (
    <>
      {(posLabel || pron || level || freqLabel || register) && (
        <div className="dq-meta">
          {posLabel && <span className="dq-pos-chip">{posLabel}</span>}
          {level && <span className="dq-level-chip">{level}</span>}
          {register && <span className="dq-register-chip">{register}</span>}
          {pron && <span className="dq-ipa">{pron}</span>}
          <FreqBar frequency={item.frequency} />
        </div>
      )}

      {literal && (
        <div className="dq-block dq-note dq-literal">
          <strong>Дословно</strong>
          <span>{literal}</span>
        </div>
      )}

      {!hideMeanings && meanings.length > 0 && (
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

      {!hideMeanings && variants.length > 0 && (
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

      {synonyms.length > 0 && (
        <div className="dq-block">
          <strong>{isPhrase ? 'Похожие выражения' : 'Синонимы'}</strong>
          <div className="dq-vars">
            {synonyms.map((s, i) => (
              <SaveChip key={`${s}-${i}`} text={s} className="dq-syn" saved={savedChips} onSave={onSaveChip} />
            ))}
          </div>
        </div>
      )}

      {antonyms.length > 0 && (
        <div className="dq-block">
          <strong>Антонимы</strong>
          <div className="dq-vars">
            {antonyms.map((a, i) => (
              <SaveChip key={`${a}-${i}`} text={a} className="dq-ant" saved={savedChips} onSave={onSaveChip} />
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
            {collocations.map((c, i) => (
              <SaveChip key={`${c}-${i}`} text={c} saved={savedChips} onSave={onSaveChip} />
            ))}
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

      {related.length > 0 && (
        <div className="dq-block">
          <strong>Родственные слова</strong>
          <div className="dq-vars">
            {related.map((r, i) => (
              <SaveChip
                key={`${r.word}-${i}`}
                text={r.word}
                className="dq-related"
                label={<>{r.word}{r.gloss ? <em> · {r.gloss}</em> : null}</>}
                saved={savedChips}
                onSave={onSaveChip}
              />
            ))}
          </div>
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
export function useTts() {
  const audioRef = useRef(null);
  const seqRef = useRef(0);
  const [state, setState] = useState('idle'); // idle|loading|playing|error
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

  const warm = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    try { await api('/api/webapp/tts/generate', { text: t, language }); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => () => stop(), [stop]);
  return { state, playingText, play, stop, warm };
}
