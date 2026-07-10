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

// Persisted initData for the standalone browser dictionary (?startapp=dict opened in
// Safari / added to the Home Screen). Inside Telegram tg.initData is always present and
// wins; outside it we take initData from the launch URL and cache it so the home-screen
// app stays authenticated across relaunches (valid up to 30 days server-side).
const DQ_INITDATA_LS_KEY = 'dq_initdata_v1';

export function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    const fromUrl = new URLSearchParams(window.location.search).get('initData') || '';
    if (fromUrl) {
      try { localStorage.setItem(DQ_INITDATA_LS_KEY, fromUrl); } catch (_e) { /* ignore */ }
      return fromUrl;
    }
    try {
      const cached = localStorage.getItem(DQ_INITDATA_LS_KEY);
      if (cached) return cached;
    } catch (_e) { /* ignore */ }
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

// Durable browser-dictionary token (?dqt=… on the standalone Safari / home-screen app).
// It authenticates deep breakdown / save / audio WITHOUT initData and never expires
// (revocable server-side), unlike the 30-day initData. Cached from the launch URL so
// the home-screen app keeps working across relaunches. Empty inside Telegram (initData
// is used there instead).
const DQ_TOKEN_LS_KEY = 'dq_browser_token_v1';

export function getDictToken() {
  if (typeof window === 'undefined') return '';
  try {
    // Path form: /dict/t/<token>. iOS keeps a path segment in a home-screen start_url
    // (it drops ?query=), so this is what a cold-launched installed icon actually carries.
    const pathMatch = String(window.location.pathname || '').match(/^\/dict\/t\/([^/]+)/);
    if (pathMatch && pathMatch[1]) {
      const tok = decodeURIComponent(pathMatch[1]);
      try { localStorage.setItem(DQ_TOKEN_LS_KEY, tok); } catch (_e) { /* ignore */ }
      return tok;
    }
    const fromUrl = new URLSearchParams(window.location.search).get('dqt') || '';
    if (fromUrl) {
      try { localStorage.setItem(DQ_TOKEN_LS_KEY, fromUrl); } catch (_e) { /* ignore */ }
      return fromUrl;
    }
    return localStorage.getItem(DQ_TOKEN_LS_KEY) || '';
  } catch (_e) { return ''; }
}

export async function api(path, body) {
  const token = getDictToken();
  const headers = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
  if (token) headers['X-Dict-Token'] = token;
  const response = await fetch(path, {
    method: 'POST',
    headers,
    body: JSON.stringify({ initData: getInitData(), ...(token ? { dqt: token } : {}), ...body }),
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

// Example sentences: German + 🔊, with the Russian translation always shown below
// (no reveal toggle — simpler and nothing hidden). Tapping the German sentence saves it
// to the dictionary as a full sentence (same canonical pipeline as chips) — de→ru, with
// the shown Russian gloss, so the whole example lands as one card. The 🔊 button keeps
// its own tap target so listening never triggers a save.
function ExamplesBlock({ examples, tts, onSaveExample, savedChips }) {
  if (!examples || examples.length === 0) return null;
  return (
    <div className="dq-block">
      <strong>Примеры</strong>
      <div className="dq-ex-list">
        {examples.map((ex, i) => {
          const isSaved = !!(savedChips && savedChips.has(clean(ex.de)));
          return (
            <div key={`${ex.de}-${i}`} className="dq-ex">
              <div className="dq-ex-de">
                <SpeakButton text={ex.de} tts={tts} sm />
                {onSaveExample ? (
                  <button
                    type="button"
                    className={`dq-ex-save${isSaved ? ' is-saved' : ''}`}
                    onClick={() => onSaveExample(ex.de, ex.ru)}
                    title={isSaved ? 'Сохранено в словарь' : 'Нажмите, чтобы сохранить пример в словарь'}
                  >
                    <span>{ex.de}</span>{isSaved ? ' ✓' : ''}
                  </button>
                ) : (
                  <span>{ex.de}</span>
                )}
              </div>
              {ex.ru && <div className="dq-ex-ru-static">{ex.ru}</div>}
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

// ── Client-side grammar-table engine (mirrors backend german_grammar_tables.py).
// Used as a FALLBACK when an item has no server-built `grammar_tables` (e.g. saved
// Library entries from before the engine existed) — builds the full declension/
// conjugation/comparison from `forms` + `article`, so every card shows real tables
// instead of bare "Артикль/Мн.число" chips. ────────────────────────────────────
const _ART_SG = {
  m: { nom: 'der', akk: 'den', dat: 'dem', gen: 'des' },
  f: { nom: 'die', akk: 'die', dat: 'der', gen: 'der' },
  n: { nom: 'das', akk: 'das', dat: 'dem', gen: 'des' },
};
const _ART_PL = { nom: 'die', akk: 'die', dat: 'den', gen: 'der' };
const _CASES = ['nom', 'akk', 'dat', 'gen'];
const _CASE_LABELS = { nom: 'Nominativ', akk: 'Akkusativ', dat: 'Dativ', gen: 'Genitiv' };
const _ARTICLE_TOKENS = new Set(['der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen', 'einem', 'einer', 'eines']);
const _PRON = ['ich', 'du', 'er/sie/es', 'wir', 'ihr', 'sie/Sie'];

function _stripArticle(s) {
  const parts = String(s || '').trim().split(/\s+/);
  if (parts[0] && _ARTICLE_TOKENS.has(parts[0].toLowerCase())) return parts.slice(1).join(' ');
  return String(s || '').trim();
}
function _genderFromArticle(a) {
  const x = String(a || '').trim().toLowerCase();
  return x === 'der' ? 'm' : x === 'die' ? 'f' : x === 'das' ? 'n' : null;
}
function buildNounDeclension(wordDe, article, plural, genitive) {
  const gender = _genderFromArticle(article);
  if (!gender) return null;
  const noun = _stripArticle(wordDe);
  if (!noun) return null;
  const pluralNoun = plural ? _stripArticle(plural) : '';
  let genSg = _stripArticle(genitive || '');
  if (!genSg) {
    if (gender === 'f') genSg = noun;
    else genSg = noun + (/(s|ß|x|z|sch)$/i.test(noun) ? 'es' : 's');
  }
  const sg = { nom: noun, akk: noun, dat: noun, gen: genSg };
  const rows = _CASES.map((c) => {
    const row = { case: c, label: _CASE_LABELS[c], singular: `${_ART_SG[gender][c]} ${sg[c]}`.trim() };
    if (pluralNoun) {
      const pl = (c === 'dat' && !/[ns]$/i.test(pluralNoun)) ? pluralNoun + 'n' : pluralNoun;
      row.plural = `${_ART_PL[c]} ${pl}`.trim();
    }
    return row;
  });
  return { gender, article: _ART_SG[gender].nom, singular: noun, plural: pluralNoun || null, has_plural: !!pluralNoun, rows };
}
function _expandFromBase(base) {
  const low = base.toLowerCase();
  const endsE = low.endsWith('e');
  let du = base + ((/[td]$/.test(low) && !endsE) ? 'est' : 'st');
  if (/[sßzx]$/.test(low) && !endsE) du = base + 't';
  const pl = base + (endsE ? 'n' : 'en');
  const ihr = base + ((endsE || !/[td]$/.test(low)) ? 't' : 'et');
  return { ich: base, du, 'er/sie/es': base, wir: pl, ihr, 'sie/Sie': pl };
}
const _AUX_FORMS = {
  haben: ['habe', 'hast', 'hat', 'haben', 'habt', 'haben'],
  sein: ['bin', 'bist', 'ist', 'sind', 'seid', 'sind'],
};
function _parsePerfekt(perfekt) {
  const text = String(perfekt || '').trim();
  if (!text) return ['haben', ''];
  const tokens = text.split(/\s+/);
  const head = tokens[0].toLowerCase();
  if (['hat', 'habe', 'hast', 'haben', 'habt'].includes(head)) return ['haben', tokens.slice(1).join(' ')];
  if (['ist', 'bin', 'bist', 'sind', 'seid'].includes(head)) return ['sein', tokens.slice(1).join(' ')];
  return ['haben', text];
}
function buildVerbConjugation(wordDe, seed) {
  const inf = _stripArticle(wordDe);
  if (!inf || /\s/.test(inf)) return null;
  seed = seed || {};
  let stem; let plEnd;
  if (inf.endsWith('eln') || inf.endsWith('ern')) { stem = inf.slice(0, -1); plEnd = 'n'; }
  else if (inf.endsWith('en')) { stem = inf.slice(0, -2); plEnd = 'en'; }
  else if (inf.endsWith('n')) { stem = inf.slice(0, -1); plEnd = 'n'; }
  else { stem = inf; plEnd = 'en'; }
  const low = stem.toLowerCase();
  let regDu; let regEr;
  if (/(t|d|ffn|chn|tm)$/.test(low)) { regDu = stem + 'est'; regEr = stem + 'et'; }
  else if (/[sßzx]$/.test(low)) { regDu = stem + 't'; regEr = stem + 't'; }
  else { regDu = stem + 'st'; regEr = stem + 't'; }
  let du = clean(seed.present_2sg) || regDu;
  const er = clean(seed.present_3sg) || regEr;
  if (clean(seed.present_3sg) && !clean(seed.present_2sg) && er.toLowerCase().endsWith('t')) {
    const cand = er.slice(0, -1);
    if (!/[sßzx]$/i.test(cand)) du = cand + 'st';
  }
  const ihr = stem + (/[td]$/.test(low) ? 'et' : 't');
  const tables = { praesens: { ich: stem + 'e', du, 'er/sie/es': er, wir: stem + plEnd, ihr, 'sie/Sie': stem + plEnd } };
  const praet = _stripArticle(clean(seed.praeteritum));
  if (praet) tables.praeteritum = _expandFromBase(praet);
  const konj2 = _stripArticle(clean(seed.konjunktiv2));
  if (konj2 && konj2.toLowerCase().startsWith('würde')) {
    tables.konjunktiv2 = { ich: 'würde ' + inf, du: 'würdest ' + inf, 'er/sie/es': 'würde ' + inf, wir: 'würden ' + inf, ihr: 'würdet ' + inf, 'sie/Sie': 'würden ' + inf };
  } else if (konj2) tables.konjunktiv2 = _expandFromBase(konj2);
  const [aux, participle] = _parsePerfekt(clean(seed.perfekt));
  if (participle && _AUX_FORMS[aux]) {
    tables.perfekt = {};
    _PRON.forEach((p, i) => { tables.perfekt[p] = `${_AUX_FORMS[aux][i]} ${participle}`; });
    tables.auxiliary = aux;
    tables.partizip2 = participle;
  }
  tables.imperativ = { du: clean(seed.imperative_sg) || stem, ihr, Sie: `${stem + plEnd} Sie` };
  tables.infinitive = inf;
  return tables;
}
function buildAdjectiveComparison(wordDe, comparative, superlative) {
  const positive = _stripArticle(wordDe);
  if (!positive || /\s/.test(positive)) return null;
  let comp = clean(comparative);
  let sup = clean(superlative);
  if (!comp) comp = positive + 'er';
  if (!sup) sup = `am ${positive}sten`;
  else if (!sup.toLowerCase().startsWith('am ')) sup = 'am ' + sup;
  return { positive, comparative: comp, superlative: sup };
}
// Build tables from an item's forms+article when the server didn't (idempotent).
function buildGrammarTablesJS(item) {
  if (!item || typeof item !== 'object') return null;
  const pos = clean(item.part_of_speech).toLowerCase();
  const f = (item.forms && typeof item.forms === 'object') ? item.forms : {};
  const wordDe = clean(item.word_de);
  if (pos === 'noun') {
    const t = buildNounDeclension(wordDe, item.article, f.plural, f.genitive);
    return t ? { declension: t } : null;
  }
  if (pos === 'verb') {
    const t = buildVerbConjugation(wordDe, {
      present_2sg: f.present_2sg, present_3sg: f.present_3sg, praeteritum: f.praeteritum,
      perfekt: f.perfekt, konjunktiv2: f.konjunktiv2, imperative_sg: f.imperative_sg,
    });
    return t ? { conjugation: t } : null;
  }
  if (pos === 'adjective' || pos === 'adverb') {
    const t = buildAdjectiveComparison(wordDe, f.comparative, f.superlative);
    return t ? { comparison: t } : null;
  }
  return null;
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
      <details className="dq-gt">
        <summary>Склонение{decl.plural ? ` · мн. ${decl.plural}` : ''}</summary>
        <table className="dq-decl" lang="de">
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
      <details className="dq-gt">
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
      <details className="dq-gt">
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
export function WordBreakdown({ item, tts, onSaveChip, onSaveExample, savedChips, hideMeanings }) {
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
  // Prefer server-built tables; fall back to the client engine (saved entries
  // without grammar_tables still get a full declension/conjugation table).
  const serverGt = item.grammar_tables;
  const gt = (serverGt && (serverGt.declension || serverGt.conjugation || serverGt.comparison))
    ? serverGt : buildGrammarTablesJS(item);
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
          <GrammarTables tables={gt} />
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

      <ExamplesBlock examples={examples} tts={tts} onSaveExample={onSaveExample} savedChips={savedChips} />

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

// Tiny valid silent WAV — used to "unlock" the audio element inside the tap gesture so a
// later programmatic play() (after our async fetch) is allowed by iOS.
const SILENT_WAV = 'data:audio/wav;base64,UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA=';

// Listen via the shared TTS pipeline: POST generate → poll url → play the MP3.
export function useTts() {
  const audioRef = useRef(null);
  const seqRef = useRef(0);
  // Client-side cache: (lang::text) → blob object-URL. Once a word is played, replays
  // are INSTANT — no /tts/generate, no /tts/url poll, no re-download from R2.
  const blobCacheRef = useRef(new Map());
  const [state, setState] = useState('idle'); // idle|loading|playing|error
  const [playingText, setPlayingText] = useState('');
  const [errorMsg, setErrorMsg] = useState('');

  // ONE reused, off-screen <video> element (NOT <audio>). On iOS a <video> plays through the
  // MEDIA channel and IGNORES the hardware silent/ringer switch — an <audio> element RESPECTS
  // it and stays mute when the phone is on silent, which is why nothing sounded. It also
  // plays a cross-origin MP3 URL directly (R2 sends no CORS, so fetch().blob() fails and we
  // play the URL). Kept in the DOM + "unlocked" during the tap so a post-fetch play() is
  // allowed (iOS blocks play() outside the gesture stack). playsinline stops fullscreen.
  const ensureAudioEl = useCallback(() => {
    let el = audioRef.current;
    if (!el) {
      try {
        el = document.createElement('video');
        el.setAttribute('playsinline', '');
        el.setAttribute('webkit-playsinline', '');
        el.playsInline = true;
        el.preload = 'auto';
        el.muted = false;
        el.volume = 1;
        el.style.cssText = 'position:fixed;left:-9999px;top:0;width:1px;height:1px;opacity:0;pointer-events:none;';
        if (typeof document !== 'undefined' && document.body) document.body.appendChild(el);
      } catch (_e) { el = new Audio(); }
      audioRef.current = el;
    }
    return el;
  }, []);

  const stop = useCallback(() => {
    seqRef.current += 1;
    const el = audioRef.current;
    if (el) { try { el.pause(); } catch (_e) { /* ignore */ } }
    setState('idle');
    setPlayingText('');
  }, []);

  const _startAudio = useCallback(async (src, mySeq) => {
    if (mySeq !== seqRef.current) return;
    const el = ensureAudioEl();
    el.onended = () => { if (mySeq === seqRef.current) { setState('idle'); setPlayingText(''); } };
    el.onerror = () => { if (mySeq === seqRef.current) { setState('error'); setErrorMsg('Не удалось воспроизвести аудио'); } };
    el.muted = false;
    el.volume = 1;
    el.src = src;
    try { el.load(); } catch (_e) { /* ignore */ }
    setState('playing');
    await el.play();
  }, [ensureAudioEl]);

  const play = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    const key = `${language}::${t}`;
    const mySeq = ++seqRef.current;
    setPlayingText(t);
    setErrorMsg('');
    haptic('light');
    // iOS unlock — MUST run synchronously inside the tap, before any await. Playing a silent
    // clip on the reused element within the gesture authorizes the real play() that follows
    // our async /tts fetch. Without this, standalone-PWA audio never sounds.
    try {
      const el = ensureAudioEl();
      el.muted = false;
      el.src = SILENT_WAV; // 0-length clip: unlocks the element, then ends on its own
      const p = el.play();
      if (p && typeof p.catch === 'function') p.catch(() => {});
    } catch (_e) { /* ignore */ }
    // Fast path: replay from the in-memory blob — instant, zero network.
    const cachedBlobUrl = blobCacheRef.current.get(key);
    if (cachedBlobUrl) {
      try { await _startAudio(cachedBlobUrl, mySeq); return; }
      catch (_e) { blobCacheRef.current.delete(key); /* fall through to refetch */ }
    }
    setState('loading');
    try {
      // /tts/generate ALREADY returns the ready URL for a cached word (or after the
      // inline fast-lane synthesises it). Use that response directly — only fall back
      // to polling /tts/url when generation is genuinely still pending. This drops a
      // whole extra ~2.5s round-trip for the common (cached) case.
      const gen = await api('/api/webapp/tts/generate', { text: t, language });
      let url = (gen && String(gen.status || '') === 'ready' && gen.audio_url) ? String(gen.audio_url) : '';
      if (!url && String(gen?.status || '') === 'failed') {
        throw new Error(gen?.message || 'TTS fehlgeschlagen');
      }
      if (!url) {
        const dictToken = getDictToken();
        const params = new URLSearchParams({ text: t, language });
        if (dictToken) params.set('dqt', dictToken);
        const urlHeaders = { 'X-Telegram-InitData': getInitData() };
        if (dictToken) urlHeaders['X-Dict-Token'] = dictToken;
        for (let i = 0; i < 30 && !url; i += 1) {
          if (mySeq !== seqRef.current) return;
          const res = await fetch(`/api/webapp/tts/url?${params.toString()}`, {
            method: 'GET', headers: urlHeaders,
          });
          const data = await res.json().catch(() => ({}));
          if (data.status === 'ready' && data.audio_url) { url = data.audio_url; break; }
          if (data.status === 'failed') throw new Error(data.message || 'TTS fehlgeschlagen');
          await new Promise((r) => setTimeout(r, data.retry_after_ms || 700));
        }
      }
      if (!url) throw new Error('Zeitüberschreitung');
      if (mySeq !== seqRef.current) return;
      // Download the MP3 once and cache it as a blob so every later tap is instant.
      let playSrc = url;
      try {
        const blob = await (await fetch(url)).blob();
        playSrc = URL.createObjectURL(blob);
        blobCacheRef.current.set(key, playSrc);
      } catch (_e) { /* blob fetch failed — play straight from the URL */ }
      await _startAudio(playSrc, mySeq);
    } catch (e) {
      if (mySeq === seqRef.current) {
        setState('error'); haptic('bad');
        const name = String(e?.name || '');
        setErrorMsg(
          name === 'NotAllowedError'
            ? 'iOS заблокировал звук. Проверь боковой переключатель «бесшумно» и громкость.'
            : (String(e?.message || '').trim() || 'Не удалось воспроизвести аудио'),
        );
      }
    }
  }, [_startAudio, ensureAudioEl]);

  const warm = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    try { await api('/api/webapp/tts/generate', { text: t, language }); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => () => stop(), [stop]);
  return { state, playingText, play, stop, warm, errorMsg };
}
