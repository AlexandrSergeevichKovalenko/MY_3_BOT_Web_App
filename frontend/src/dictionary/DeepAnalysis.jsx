import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';
import './deep.css';
import { WordBreakdown, useTts, SpeakButton, genderClass, api, haptic } from './WordBreakdown';
import { guessPair, extractRichTranslation, buildDictionarySavePayload } from './saveUtils';

/**
 * «Полный разбор» — the full word/phrase breakdown, opened directly from the DM
 * «🧠 Расширенный перевод» button (startapp=razbor_req_<id> → lookup runs on open;
 * or razbor_deep_<id> → pre-computed lookup). Renders a hero + the shared
 * <WordBreakdown> + 4 depth sections, and — crucially — ALL the chat-card actions
 * BELOW the breakdown: save variants (multiselect) + folder picker, listen, feel,
 * collocations, ask-your-own-question. Nothing from the old chat flow is dropped.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function clean(v) { return String(v || '').trim(); }
const hasCyrillic = (s) => /[А-Яа-яЁё]/.test(String(s || ''));

// razbor_req_<key> → run lookup on open; razbor_deep_<id> → pre-computed lookup;
// share_<token> → public guest view of someone else's shared breakdown.
function parseTarget(startParam) {
  const raw = clean(startParam);
  if (/^share_/i.test(raw)) return { kind: 'share', id: raw.replace(/^share_/i, '') };
  const rest = raw.replace(/^razbor_/i, '');
  if (/^req_/i.test(rest)) return { kind: 'req', id: rest.replace(/^req_/i, '') };
  if (rest) return { kind: 'deep', id: rest };
  return { kind: '', id: '' };
}

// The ask/feel model replies in light Markdown (*bold*, _italic_, `code`, "* " bullets).
// Render it instead of showing raw asterisks/underscores.
function renderInline(text) {
  const nodes = [];
  let rest = String(text || '');
  const re = /(\*\*([^*]+)\*\*|\*([^*\n]+)\*|__([^_\n]+)__|_([^_\n]+)_|`([^`\n]+)`)/;
  let key = 0;
  while (rest) {
    const m = re.exec(rest);
    if (!m) { nodes.push(rest); break; }
    if (m.index > 0) nodes.push(rest.slice(0, m.index));
    if (m[2] != null) nodes.push(<b key={key++}>{m[2]}</b>);
    else if (m[3] != null) nodes.push(<b key={key++}>{m[3]}</b>);
    else if (m[4] != null) nodes.push(<i key={key++}>{m[4]}</i>);
    else if (m[5] != null) nodes.push(<i key={key++}>{m[5]}</i>);
    else if (m[6] != null) nodes.push(<code key={key++}>{m[6]}</code>);
    rest = rest.slice(m.index + m[0].length);
  }
  return nodes;
}

function RichText({ text }) {
  const lines = String(text || '').split(/\n/);
  const out = [];
  let bullets = null;
  const flush = () => {
    if (bullets) { out.push(<ul key={`u${out.length}`} className="deep-md-ul">{bullets}</ul>); bullets = null; }
  };
  lines.forEach((line, i) => {
    const bm = /^\s*[*\-•]\s+(.*)$/.exec(line);
    if (bm) {
      if (!bullets) bullets = [];
      bullets.push(<li key={i}>{renderInline(bm[1])}</li>);
    } else {
      flush();
      const t = line.trim();
      if (t) out.push(<p key={i} className="deep-md-p">{renderInline(t)}</p>);
    }
  });
  flush();
  return <>{out}</>;
}

const REGISTER_LABELS = { colloquial: 'разговорный', neutral: 'нейтральный', formal: 'официальный' };

function RegisterRow({ row, tts }) {
  const [open, setOpen] = useState(false);
  const de = clean(row?.example_target);
  const ru = clean(row?.example_source);
  const level = clean(row?.level).toLowerCase();
  const tone = clean(row?.tone);
  if (!de) return null;
  return (
    <div className={`deep-reg-row lvl-${level || 'neutral'}`}>
      <div className="deep-reg-top">
        <span className="deep-reg-badge">{REGISTER_LABELS[level] || level || '—'}</span>
        {tone && <span className="deep-reg-tone">{tone}</span>}
      </div>
      <div className="deep-reg-de"><span>{de}</span><SpeakButton text={de} tts={tts} sm /></div>
      {ru && (open
        ? <div className="deep-reg-ru">{ru}</div>
        : <button type="button" className="deep-reveal" onClick={() => setOpen(true)}>Показать перевод</button>)}
    </div>
  );
}

export default function DeepAnalysis({ startParam }) {
  const target = useMemo(() => parseTarget(startParam), [startParam]);
  const [item, setItem] = useState(null);
  const [phase, setPhase] = useState('loading'); // loading | done | error
  const [error, setError] = useState('');
  const [savedChips, setSavedChips] = useState(() => new Set());
  const [isGuest, setIsGuest] = useState(false);       // opened via share_<token> by a non-owner
  const [botUsername, setBotUsername] = useState('');  // for the guest "request access" CTA
  const [deepId, setDeepId] = useState('');            // id we can share (owner view only)
  const [sharing, setSharing] = useState(false);
  const tts = useTts();

  // Folder + save-variant selection state (shared by all saves on this screen).
  const [folders, setFolders] = useState([]);
  const [folderId, setFolderId] = useState(null);       // null = GENERAL
  const [folderName, setFolderName] = useState('GENERAL');
  const [folderOpen, setFolderOpen] = useState(false);
  const [selectedOpts, setSelectedOpts] = useState(() => new Set());
  const [optsSaved, setOptsSaved] = useState(false);

  // Action states.
  const [feel, setFeel] = useState({ state: 'idle', text: '' });   // idle|loading|done
  const [collos, setCollos] = useState({ state: 'idle', items: [] });
  const [ask, setAsk] = useState({ q: '', a: '', state: 'idle', history: [] });

  useEffect(() => {
    try {
      tg?.ready?.(); tg?.expand?.();
      tg?.setHeaderColor?.('secondary_bg_color');
      tg?.disableVerticalSwipes?.();
    } catch (_e) { /* ignore */ }
    const applyScheme = () => {
      const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
      try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
    };
    applyScheme();
    try { tg?.onEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ }
    return () => { try { tg?.offEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ } };
  }, []);

  // Load the breakdown: run lookup on open (req_) or fetch pre-computed (deep_).
  useEffect(() => {
    let alive = true;
    if (!target.id) { setPhase('error'); setError('Разбор не найден'); return undefined; }
    (async () => {
      try {
        const path = target.kind === 'req'
          ? '/api/webapp/dictionary/lookup-by-request'
          : target.kind === 'share'
            ? '/api/webapp/dictionary/shared'
            : '/api/webapp/dictionary/deep-analysis';
        const body = target.kind === 'req'
          ? { request_id: target.id }
          : target.kind === 'share'
            ? { share_token: target.id }
            : { deep_id: target.id };
        const data = await api(path, body);
        if (!alive) return;
        const rich = data?.item || null;
        if (rich) {
          rich.__direction = clean(data?.direction) || rich.__direction || '';
          rich.__language_pair = data?.language_pair || null;
        }
        if (!rich) throw new Error('Пустой разбор');
        if (data?.is_guest) {
          setIsGuest(true);
          setBotUsername(clean(data?.bot_username));
        } else {
          // Owner view: remember an id we can share (pre-computed deep_id).
          setDeepId(clean(data?.deep_id) || (target.kind === 'deep' ? target.id : ''));
        }
        setItem(rich);
        setPhase('done');
        haptic('ok');
      } catch (e) {
        if (!alive) return;
        setError(e?.status === 429 ? (e?.payload?.message || 'Дневной лимит исчерпан') : String(e?.message || e));
        setPhase('error');
        haptic('bad');
      }
    })();
    return () => { alive = false; };
  }, [target.id, target.kind]);

  // Load folders once the breakdown is ready (for the save picker).
  useEffect(() => {
    if (phase !== 'done' || isGuest) return;   // guests can't save → no folder picker
    let alive = true;
    (async () => {
      try {
        const data = await api('/api/webapp/dictionary/folders', {});
        if (alive) setFolders(Array.isArray(data?.items) ? data.items : []);
      } catch (_e) { /* folders are optional */ }
    })();
    return () => { alive = false; };
  }, [phase, isGuest]);

  const germanText = clean(item?.word_de).replace(/^(der|die|das)\s+/i, '');
  const { warm: warmTts } = tts;
  useEffect(() => { if (germanText) warmTts(germanText, 'de-DE'); }, [germanText, warmTts]);

  const dir = clean(item?.__direction);
  const translation = extractRichTranslation(item) || clean(item?.word_ru) || clean(item?.meanings?.primary?.value);

  // Save a chip (synonym / related / collocation) via the canonical pipeline, into
  // the currently-selected folder.
  const saveChip = useCallback((text) => {
    if (isGuest) return;   // read-only guest view: chips are not clickable-to-save
    const t = clean(text);
    if (!t) return;
    setSavedChips((prev) => { if (prev.has(t)) return prev; const n = new Set(prev); n.add(t); return n; });
    haptic('ok');
    (async () => {
      try {
        const pair = guessPair(t);
        const [quickData, richData] = await Promise.all([
          api('/api/translate/quick', { text: t, source_lang: pair.source, target_lang: pair.target }).catch(() => null),
          api('/api/webapp/dictionary', { word: t, lookup_lang: pair.source }).catch(() => null),
        ]);
        const rich = richData?.item || null;
        if (rich) rich.__direction = clean(richData?.direction) || `${pair.source}-${pair.target}`;
        const detected = clean(quickData?.detected_source_lang).toLowerCase() || pair.source;
        const chipTargetLang = detected === pair.target ? pair.source : pair.target;
        const quick = quickData ? {
          source: t, translation: clean(quickData?.translation),
          sourceLang: detected, targetLang: chipTargetLang, direction: `${detected}-${chipTargetLang}`,
        } : null;
        if (!rich && !(quick && quick.translation)) throw new Error('Не удалось перевести слово');
        const base = buildDictionarySavePayload({ rich, sourceText: t, quick, origin: 'webapp_deep_analysis_related' });
        await api('/api/webapp/dictionary/save', { ...base, folder_id: folderId ?? undefined });
      } catch (e) {
        setSavedChips((prev) => { const n = new Set(prev); n.delete(t); return n; });
        setError(String(e?.message || e)); haptic('bad');
      }
    })();
  }, [folderId, isGuest]);

  // Share this breakdown: get a Telegram prepared inline message (photo card +
  // deep-link button) and hand it to the native share sheet.
  const doShare = useCallback(async () => {
    if (!deepId || sharing) return;
    setSharing(true); haptic('light');
    try {
      const data = await api('/api/webapp/dictionary/share/prepare', { deep_id: deepId });
      const pmid = clean(data?.prepared_message_id);
      if (pmid && typeof tg?.shareMessage === 'function') {
        tg.shareMessage(pmid, (ok) => { if (ok) haptic('ok'); });
      } else if (clean(data?.deeplink) && typeof tg?.openTelegramLink === 'function') {
        // Fallback for older Telegram clients without shareMessage.
        tg.openTelegramLink(`https://t.me/share/url?url=${encodeURIComponent(data.deeplink)}`);
      } else {
        setError('Обновите Telegram, чтобы делиться разбором.');
      }
    } catch (e) {
      setError(String(e?.message || e)); haptic('bad');
    } finally {
      setSharing(false);
    }
  }, [deepId, sharing]);

  // Guest CTA: open the bot so a non-user can request access.
  const requestAccess = useCallback(() => {
    haptic('light');
    if (botUsername && typeof tg?.openTelegramLink === 'function') {
      tg.openTelegramLink(`https://t.me/${botUsername}?start=access`);
    } else if (typeof tg?.close === 'function') {
      tg.close();
    }
  }, [botUsername]);

  // Build a save payload for a «Вариант для сохранения» (both sides + direction known).
  const optionPayload = useCallback((opt) => {
    const src = clean(opt?.source);
    const tgt = clean(opt?.target);
    const [sl, tl] = dir.includes('-') ? dir.split('-', 2) : ['', ''];
    const deText = sl === 'de' ? src : (tl === 'de' ? tgt : (hasCyrillic(src) ? tgt : src));
    const ruText = sl === 'ru' ? src : (tl === 'ru' ? tgt : (hasCyrillic(src) ? src : tgt));
    return {
      word_de: deText, word_ru: ruText, translation_de: deText, translation_ru: ruText,
      source_text: src, target_text: tgt,
      source_lang: sl || undefined, target_lang: tl || undefined, direction: dir || undefined,
      folder_id: folderId ?? undefined, origin_process: 'webapp_deep_analysis_option',
    };
  }, [dir, folderId]);

  const options = Array.isArray(item?.save_worthy_options)
    ? item.save_worthy_options.filter((x) => clean(x?.source) || clean(x?.target)) : [];

  const toggleOpt = useCallback((i) => {
    haptic('light');
    setSelectedOpts((prev) => { const n = new Set(prev); n.has(i) ? n.delete(i) : n.add(i); return n; });
  }, []);
  const selectAllOpts = useCallback(() => {
    haptic('light');
    setSelectedOpts((prev) => prev.size === options.length ? new Set() : new Set(options.map((_, i) => i)));
  }, [options.length]);

  // «Сохранить выбранное» — optimistic: mark saved instantly, persist in background.
  const saveSelected = useCallback(() => {
    const idxs = [...selectedOpts];
    if (!idxs.length || optsSaved) return;
    setOptsSaved(true); setError(''); haptic('ok');
    (async () => {
      try {
        await Promise.all(idxs.map((i) => api('/api/webapp/dictionary/save', optionPayload(options[i]))));
      } catch (e) {
        setOptsSaved(false); setError(String(e?.message || e)); haptic('bad');
      }
    })();
  }, [selectedOpts, optsSaved, optionPayload, options]);

  const pickFolder = useCallback((f) => {
    setFolderId(f ? f.id : null);
    setFolderName(f ? (f.name || 'Папка') : 'GENERAL');
    setFolderOpen(false);
    haptic('light');
  }, []);
  const createFolder = useCallback(async () => {
    const name = clean(window.prompt?.('Название новой папки:', ''));
    if (!name) return;
    try {
      const data = await api('/api/webapp/dictionary/folders/create', { name, color: '#5ddcff', icon: 'book' });
      const created = data?.item;
      if (created?.id) { setFolders((prev) => [created, ...prev]); pickFolder(created); }
    } catch (e) { setError(String(e?.message || e)); }
  }, [pickFolder]);

  const runFeel = useCallback(async () => {
    if (feel.state === 'loading') return;
    setFeel({ state: 'loading', text: '' }); haptic('light');
    try {
      const [sl, tl] = dir.includes('-') ? dir.split('-', 2) : ['', ''];
      const data = await api('/api/webapp/dictionary/feel', {
        source_text: clean(item?.source_text) || germanText,
        target_text: clean(item?.target_text) || translation,
        source_lang: sl || undefined, target_lang: tl || undefined,
      });
      setFeel({ state: 'done', text: clean(data?.feel_text) });
    } catch (e) { setFeel({ state: 'idle', text: '' }); setError(String(e?.message || e)); haptic('bad'); }
  }, [feel.state, dir, item, germanText, translation]);

  const runCollocations = useCallback(async () => {
    if (collos.state === 'loading') return;
    setCollos({ state: 'loading', items: [] }); haptic('light');
    try {
      const data = await api('/api/webapp/dictionary/collocations', {
        word: germanText, direction: dir || undefined, translation,
      });
      setCollos({ state: 'done', items: Array.isArray(data?.items) ? data.items : [] });
    } catch (e) { setCollos({ state: 'idle', items: [] }); setError(String(e?.message || e)); haptic('bad'); }
  }, [collos.state, germanText, dir, translation]);

  const runAsk = useCallback(async () => {
    const q = clean(ask.q);
    if (!q || ask.state === 'loading') return;
    setAsk((s) => ({ ...s, state: 'loading', a: '' })); haptic('light');
    try {
      const context = `${germanText}${translation ? ` — ${translation}` : ''}`;
      const data = await api('/api/webapp/ask', { context, learner_question: q, history: ask.history });
      const answer = clean(data?.answer);
      setAsk((s) => ({
        q: '', a: answer, state: 'done',
        history: [...s.history, { q, a: answer }].slice(-4),
      }));
    } catch (e) {
      const msg = e?.status === 429 ? (e?.payload?.message || 'Дневной лимит вопросов исчерпан') : String(e?.message || e);
      setAsk((s) => ({ ...s, state: 'idle' })); setError(msg); haptic('bad');
    }
  }, [ask.q, ask.state, ask.history, germanText, translation]);

  if (phase === 'loading') {
    return (
      <div className="ans-root deep-scroll">
        <div className="deep-card deep-loading"><span className="deep-spinner" />Готовим разбор…</div>
      </div>
    );
  }
  if (phase === 'error') {
    return (
      <div className="ans-root deep-scroll">
        <div className="deep-card deep-error">Не удалось открыть разбор.<br />{error}</div>
      </div>
    );
  }

  const article = clean(item?.article);
  const headword = germanText || clean(item?.word_de) || clean(item?.source_text);
  const pron = item?.pronunciation && typeof item.pronunciation === 'object' ? item.pronunciation : {};
  const ipa = clean(pron.ipa);
  const stress = clean(pron.stress);

  const connotation = item?.connotation && typeof item.connotation === 'object' ? item.connotation : null;
  const conTone = clean(connotation?.tone);
  const conNote = clean(connotation?.note);
  const synDiffs = Array.isArray(item?.synonym_differences) ? item.synonym_differences.filter((x) => clean(x?.word)) : [];
  const regExamples = Array.isArray(item?.register_examples) ? item.register_examples.filter((x) => clean(x?.example_target)) : [];
  const mistakes = Array.isArray(item?.common_mistakes) ? item.common_mistakes.filter((x) => clean(x?.mistake) || clean(x?.correction)) : [];
  const falseFriends = Array.isArray(item?.false_friends) ? item.false_friends.filter((x) => clean(x?.word)) : [];

  const colloText = (c) => (c && typeof c === 'object') ? (clean(c.target) || clean(c.source) || clean(c.value)) : clean(c);
  const colloSub = (c) => (c && typeof c === 'object') ? clean(c.source) : '';

  return (
    <div className="ans-root deep-scroll">
      <div className="deep-card">
        <div className="deep-eyebrow-row">
          <div className="deep-eyebrow">🔎 Полный разбор</div>
          {deepId && !isGuest && (
            <button
              type="button"
              className={`deep-share-btn${sharing ? ' is-busy' : ''}`}
              onClick={doShare}
              disabled={sharing}
              aria-label="Поделиться разбором"
              title="Поделиться"
            >
              {sharing ? (
                <span className="deep-share-spin" />
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

        <div className="deep-hero">
          <div className="deep-headword">
            {article && <span className={`deep-art ${genderClass(article)}`}>{article} </span>}
            <span className="deep-head-main">{headword || '—'}</span>
            <SpeakButton text={headword} tts={tts} />
          </div>
          {translation && <div className="deep-head-tr">{translation}</div>}
          {(ipa || stress) && (
            <div className="deep-head-pron">
              {ipa && <span className="deep-ipa">{ipa}</span>}
              {stress && <span className="deep-stress">ударение: {stress}</span>}
            </div>
          )}
        </div>

        <WordBreakdown item={item} tts={tts} onSaveChip={saveChip} savedChips={savedChips} />

        {(conTone || conNote) && (
          <section className="deep-sec sec-nuance">
            <h3 className="deep-sec-h">🎭 Нюанс и коннотация</h3>
            {conTone && <div className="deep-tone-chip">{conTone}</div>}
            {conNote && <p className="deep-sec-note">{conNote}</p>}
          </section>
        )}

        {synDiffs.length > 0 && (
          <section className="deep-sec sec-syn">
            <h3 className="deep-sec-h">🔀 Чем отличается от синонимов</h3>
            <div className="deep-syn-list">
              {synDiffs.map((s, i) => (
                <div className="deep-syn-row" key={`${clean(s.word)}-${i}`}>
                  <button type="button" className={`deep-syn-word${savedChips.has(clean(s.word)) ? ' is-saved' : ''}`}
                    onClick={() => saveChip(clean(s.word))}>
                    {clean(s.word)}{savedChips.has(clean(s.word)) ? ' ✓' : ''}
                  </button>
                  {clean(s.when) && <div className="deep-syn-when"><b>Когда:</b> {clean(s.when)}</div>}
                  {clean(s.nuance) && <div className="deep-syn-nuance"><b>Оттенок:</b> {clean(s.nuance)}</div>}
                </div>
              ))}
            </div>
          </section>
        )}

        {regExamples.length > 0 && (
          <section className="deep-sec sec-reg">
            <h3 className="deep-sec-h">🪜 Регистр и градация</h3>
            <div className="deep-reg-list">
              {regExamples.map((row, i) => <RegisterRow key={i} row={row} tts={tts} />)}
            </div>
          </section>
        )}

        {(mistakes.length > 0 || falseFriends.length > 0) && (
          <section className="deep-sec sec-warn">
            <h3 className="deep-sec-h">⚠️ Ошибки и ложные друзья</h3>
            {mistakes.map((m, i) => (
              <div className="deep-mistake" key={`m-${i}`}>
                <div className="deep-mis-line">
                  <span className="deep-mis-bad">✗ {clean(m.mistake)}</span>
                  <span className="deep-mis-arrow">→</span>
                  <span className="deep-mis-good">✓ {clean(m.correction)}</span>
                </div>
                {clean(m.why) && <div className="deep-mis-why">{clean(m.why)}</div>}
              </div>
            ))}
            {falseFriends.map((f, i) => (
              <div className="deep-ff" key={`f-${i}`}>
                <div className="deep-ff-head"><b>{clean(f.word)}</b>
                  {clean(f.looks_like) && <span className="deep-ff-look"> ≠ «{clean(f.looks_like)}»</span>}</div>
                {clean(f.actual_meaning) && <div className="deep-ff-mean">На самом деле: {clean(f.actual_meaning)}</div>}
              </div>
            ))}
          </section>
        )}

        {/* 📌 Варианты для сохранения — мультивыбор + папка + «Сохранить выбранное» */}
        {!isGuest && options.length > 0 && (
          <section className="deep-sec sec-save">
            <div className="deep-save-head">
              <h3 className="deep-sec-h">📌 Варианты для сохранения</h3>
              <button type="button" className="deep-mini-link" onClick={selectAllOpts}>
                {selectedOpts.size === options.length ? 'Снять все' : 'Выбрать все'}
              </button>
            </div>
            <div className="deep-save-list">
              {options.map((opt, i) => {
                const on = selectedOpts.has(i);
                return (
                  <button type="button" key={i} className={`deep-opt${on ? ' is-on' : ''}`}
                    onClick={() => toggleOpt(i)} disabled={optsSaved}>
                    <span className="deep-opt-box">{on ? '✓' : ''}</span>
                    <span className="deep-opt-texts">
                      <span className="deep-opt-de">{clean(opt.target) || clean(opt.source)}</span>
                      <span className="deep-opt-ru">{clean(opt.source)}</span>
                    </span>
                  </button>
                );
              })}
            </div>

            {/* Папка */}
            <div className="deep-folder">
              <button type="button" className="deep-folder-btn" onClick={() => setFolderOpen((v) => !v)}>
                📁 {folderName} <span className="deep-folder-caret">{folderOpen ? '▴' : '▾'}</span>
              </button>
              {folderOpen && (
                <div className="deep-folder-list">
                  <button type="button" className="deep-folder-item" onClick={() => pickFolder(null)}>📁 GENERAL</button>
                  {folders.map((f) => (
                    <button type="button" className="deep-folder-item" key={f.id} onClick={() => pickFolder(f)}>
                      {f.icon && f.icon.length <= 2 ? f.icon : '📂'} {clean(f.name) || 'Папка'}
                    </button>
                  ))}
                  <button type="button" className="deep-folder-item deep-folder-new" onClick={createFolder}>➕ Новая папка</button>
                </div>
              )}
            </div>

            <button type="button" className={`deep-save-btn${optsSaved ? ' is-saved' : ''}`}
              onClick={saveSelected} disabled={optsSaved || selectedOpts.size === 0}>
              {optsSaved ? '✓ Сохранено' : `💾 Сохранить выбранное${selectedOpts.size ? ` (${selectedOpts.size})` : ''}`}
            </button>
          </section>
        )}

        {/* Панель действий: прослушать / почувствовать / сочетания / вопрос */}
        {!isGuest && (
        <section className="deep-sec sec-actions">
          <h3 className="deep-sec-h">🛠 Действия</h3>
          <div className="deep-act-row">
            <SpeakButton text={headword} tts={tts} />
            <span className="deep-act-hint">Прослушать слово</span>
          </div>

          <div className="deep-act-block">
            <button type="button" className="deep-act-btn" onClick={runFeel} disabled={feel.state === 'loading'}>
              {feel.state === 'loading' ? '⏳ Генерирую…' : '📌 Почувствовать слово'}
            </button>
            {feel.state === 'done' && feel.text && <div className="deep-act-out"><RichText text={feel.text} /></div>}
          </div>

          <div className="deep-act-block">
            <button type="button" className="deep-act-btn" onClick={runCollocations} disabled={collos.state === 'loading'}>
              {collos.state === 'loading' ? '⏳ Подбираю…' : '🧩 Дать сочетание'}
            </button>
            {collos.state === 'done' && (
              collos.items.length > 0 ? (
                <div className="deep-collo-list">
                  {collos.items.map((c, i) => {
                    const txt = colloText(c);
                    if (!txt) return null;
                    const saved = savedChips.has(txt);
                    return (
                      <button type="button" key={i} className={`deep-collo${saved ? ' is-saved' : ''}`} onClick={() => saveChip(txt)}>
                        <span className="deep-collo-de">{txt}{saved ? ' ✓' : ''}</span>
                        {colloSub(c) && <span className="deep-collo-ru">{colloSub(c)}</span>}
                      </button>
                    );
                  })}
                </div>
              ) : <div className="deep-act-out">Ничего не нашлось.</div>
            )}
          </div>

          <div className="deep-act-block">
            <div className="deep-ask-row">
              <input type="text" className="deep-ask-input" placeholder="Задать свой вопрос о слове…"
                value={ask.q} onChange={(e) => setAsk((s) => ({ ...s, q: e.target.value }))}
                onKeyDown={(e) => { if (e.key === 'Enter') runAsk(); }} />
              <button type="button" className="deep-ask-send" onClick={runAsk} disabled={ask.state === 'loading' || !clean(ask.q)}>
                {ask.state === 'loading' ? '⏳' : '➤'}
              </button>
            </div>
            {ask.history.map((turn, i) => (
              <div className="deep-ask-turn" key={i}>
                <div className="deep-ask-q">❓ {turn.q}</div>
                <div className="deep-ask-a"><RichText text={turn.a} /></div>
              </div>
            ))}
          </div>
        </section>
        )}

        {isGuest && (
          <section className="deep-sec sec-guest-cta">
            <div className="deep-guest-title">Понравился разбор? 🤖</div>
            <p className="deep-guest-note">
              Это «Полный разбор» из бота «Deutsche Sprache». Открой доступ — разбирай любое
              слово так же подробно, сохраняй слова и учи их интервальными повторениями.
            </p>
            <button type="button" className="deep-guest-btn" onClick={requestAccess}>
              🤖 Запросить доступ к боту
            </button>
          </section>
        )}

        {error && <div className="deep-inline-err">{error}</div>}
      </div>
    </div>
  );
}
