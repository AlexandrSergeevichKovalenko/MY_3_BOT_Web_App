import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import '../answer/answer.css';
import './dict.css';
import './deep.css';
import { WordBreakdown, useTts, SpeakButton, genderClass, api, haptic } from './WordBreakdown';
import { guessPair, extractRichTranslation, buildDictionarySavePayload } from './saveUtils';

/**
 * «Полный разбор» — the rich, WOW word/phrase/sentence breakdown opened from a DM
 * chat button (startapp=razbor_<id>). It reads a lookup the bot already computed
 * (no second LLM call), then renders a hero header + the shared <WordBreakdown> body
 * + four DEPTH sections that live ONLY here (so «Быстрый словарь» stays unchanged):
 * connotation, synonym differences, register ladder, mistakes/false-friends — plus
 * the «Варианты для сохранения» save buttons and every save chip fully working.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function clean(v) { return String(v || '').trim(); }
const hasCyrillic = (s) => /[А-Яа-яЁё]/.test(String(s || ''));

function parseDeepId(startParam) {
  const m = /^razbor_(.+)$/i.exec(String(startParam || '').trim());
  return m ? m[1] : '';
}

const REGISTER_LABELS = {
  colloquial: 'разговорный',
  neutral: 'нейтральный',
  formal: 'официальный',
};

// One register-ladder example: German + 🔊, Russian revealed on tap.
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
      <div className="deep-reg-de">
        <span>{de}</span>
        <SpeakButton text={de} tts={tts} sm />
      </div>
      {ru && (
        open
          ? <div className="deep-reg-ru">{ru}</div>
          : <button type="button" className="deep-reveal" onClick={() => setOpen(true)}>Показать перевод</button>
      )}
    </div>
  );
}

export default function DeepAnalysis({ startParam }) {
  const deepId = useMemo(() => parseDeepId(startParam), [startParam]);
  const [item, setItem] = useState(null);
  const [phase, setPhase] = useState('loading'); // loading | done | error
  const [error, setError] = useState('');
  const [savedChips, setSavedChips] = useState(() => new Set());
  const [savedOptions, setSavedOptions] = useState(() => new Set());
  const tts = useTts();

  // Telegram chrome + light/dark scheme (same setup as the quick-dictionary overlay).
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
    return () => { try { tg?.offEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ } };
  }, []);

  // Fetch the pre-computed lookup by id — the bot already paid the LLM cost.
  useEffect(() => {
    let alive = true;
    if (!deepId) { setPhase('error'); setError('Разбор не найден'); return undefined; }
    (async () => {
      try {
        const data = await api('/api/webapp/dictionary/deep-analysis', { deep_id: deepId });
        if (!alive) return;
        const rich = data?.item || null;
        if (rich) {
          rich.__direction = clean(data?.direction) || rich.__direction || '';
          rich.__language_pair = data?.language_pair || null;
        }
        if (!rich) throw new Error('Пустой разбор');
        setItem(rich);
        setPhase('done');
        haptic('ok');
      } catch (e) {
        if (!alive) return;
        setError(String(e?.message || e));
        setPhase('error');
        haptic('bad');
      }
    })();
    return () => { alive = false; };
  }, [deepId]);

  // Prewarm pronunciation for the headword.
  const germanText = clean(item?.word_de).replace(/^(der|die|das)\s+/i, '');
  const { warm: warmTts } = tts;
  useEffect(() => { if (germanText) warmTts(germanText, 'de-DE'); }, [germanText, warmTts]);

  // Tap a synonym / related / collocation chip → save via the canonical pipeline
  // (quick-translate + GPT breakdown in parallel), exactly like the quick dictionary.
  const saveChip = useCallback((text) => {
    const t = clean(text);
    if (!t) return;
    setSavedChips((prev) => {
      if (prev.has(t)) return prev;
      const next = new Set(prev); next.add(t); return next;
    });
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
          source: t,
          translation: clean(quickData?.translation),
          sourceLang: detected,
          targetLang: chipTargetLang,
          direction: `${detected}-${chipTargetLang}`,
        } : null;
        if (!rich && !(quick && quick.translation)) throw new Error('Не удалось перевести слово');
        await api('/api/webapp/dictionary/save', buildDictionarySavePayload({
          rich, sourceText: t, quick, origin: 'webapp_deep_analysis_related',
        }));
      } catch (e) {
        setSavedChips((prev) => { const n = new Set(prev); n.delete(t); return n; });
        setError(String(e?.message || e)); haptic('bad');
      }
    })();
  }, []);

  // Save a «Вариант для сохранения» card. We already have both sides + the direction,
  // so save optimistically (instant ✅, persist in background) — no extra lookup.
  const saveOption = useCallback((opt, idx) => {
    const src = clean(opt?.source);
    const tgt = clean(opt?.target);
    if (!src && !tgt) return;
    setSavedOptions((prev) => {
      if (prev.has(idx)) return prev;
      const next = new Set(prev); next.add(idx); return next;
    });
    haptic('ok');
    (async () => {
      try {
        const dir = clean(item?.__direction);
        const [sl, tl] = dir.includes('-') ? dir.split('-', 2) : ['', ''];
        const deText = sl === 'de' ? src : (tl === 'de' ? tgt : (hasCyrillic(src) ? tgt : src));
        const ruText = sl === 'ru' ? src : (tl === 'ru' ? tgt : (hasCyrillic(src) ? src : tgt));
        await api('/api/webapp/dictionary/save', {
          word_de: deText,
          word_ru: ruText,
          translation_de: deText,
          translation_ru: ruText,
          source_text: src,
          target_text: tgt,
          source_lang: sl || undefined,
          target_lang: tl || undefined,
          direction: dir || undefined,
          origin_process: 'webapp_deep_analysis_option',
        });
      } catch (e) {
        setSavedOptions((prev) => { const n = new Set(prev); n.delete(idx); return n; });
        setError(String(e?.message || e)); haptic('bad');
      }
    })();
  }, [item]);

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
  const translation = extractRichTranslation(item) || clean(item?.word_ru) || clean(item?.meanings?.primary?.value);
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
  const options = Array.isArray(item?.save_worthy_options) ? item.save_worthy_options.filter((x) => clean(x?.source) || clean(x?.target)) : [];

  return (
    <div className="ans-root deep-scroll">
      <div className="deep-card">
        <div className="deep-eyebrow">🔎 Полный разбор</div>

        {/* Hero header */}
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

        {/* Shared breakdown body (meanings, grammar tables, examples, etymology…).
            Its own meanings/synonyms/etc. render here; the 4 DEPTH sections below are
            deep-view only, so «Быстрый словарь» is unaffected. */}
        <WordBreakdown item={item} tts={tts} onSaveChip={saveChip} savedChips={savedChips} />

        {/* 🎭 Нюанс и коннотация */}
        {(conTone || conNote) && (
          <section className="deep-sec sec-nuance">
            <h3 className="deep-sec-h">🎭 Нюанс и коннотация</h3>
            {conTone && <div className="deep-tone-chip">{conTone}</div>}
            {conNote && <p className="deep-sec-note">{conNote}</p>}
          </section>
        )}

        {/* 🔀 Чем отличается от синонимов */}
        {synDiffs.length > 0 && (
          <section className="deep-sec sec-syn">
            <h3 className="deep-sec-h">🔀 Чем отличается от синонимов</h3>
            <div className="deep-syn-list">
              {synDiffs.map((s, i) => (
                <div className="deep-syn-row" key={`${clean(s.word)}-${i}`}>
                  <button
                    type="button"
                    className={`deep-syn-word${savedChips.has(clean(s.word)) ? ' is-saved' : ''}`}
                    onClick={() => saveChip(clean(s.word))}
                  >
                    {clean(s.word)}{savedChips.has(clean(s.word)) ? ' ✓' : ''}
                  </button>
                  {clean(s.when) && <div className="deep-syn-when"><b>Когда:</b> {clean(s.when)}</div>}
                  {clean(s.nuance) && <div className="deep-syn-nuance"><b>Оттенок:</b> {clean(s.nuance)}</div>}
                </div>
              ))}
            </div>
          </section>
        )}

        {/* 🪜 Регистр: разговорный → официальный */}
        {regExamples.length > 0 && (
          <section className="deep-sec sec-reg">
            <h3 className="deep-sec-h">🪜 Регистр и градация</h3>
            <div className="deep-reg-list">
              {regExamples.map((row, i) => <RegisterRow key={i} row={row} tts={tts} />)}
            </div>
          </section>
        )}

        {/* ⚠️ Ошибки и ложные друзья */}
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
                <div className="deep-ff-head">
                  <b>{clean(f.word)}</b>
                  {clean(f.looks_like) && <span className="deep-ff-look"> ≠ «{clean(f.looks_like)}»</span>}
                </div>
                {clean(f.actual_meaning) && <div className="deep-ff-mean">На самом деле: {clean(f.actual_meaning)}</div>}
              </div>
            ))}
          </section>
        )}

        {/* 📌 Варианты для сохранения — все кнопки сохранения остаются */}
        {options.length > 0 && (
          <section className="deep-sec sec-save">
            <h3 className="deep-sec-h">📌 Варианты для сохранения</h3>
            <div className="deep-save-list">
              {options.map((opt, i) => {
                const saved = savedOptions.has(i);
                return (
                  <button
                    type="button"
                    key={i}
                    className={`deep-save-card${saved ? ' is-saved' : ''}`}
                    onClick={() => saveOption(opt, i)}
                    disabled={saved}
                  >
                    <span className="deep-save-de">{clean(opt.target) || clean(opt.source)}</span>
                    <span className="deep-save-ru">{clean(opt.source)}</span>
                    <span className="deep-save-cta">{saved ? '✓ Сохранено' : '💾 Сохранить'}</span>
                  </button>
                );
              })}
            </div>
          </section>
        )}

        {error && <div className="deep-inline-err">{error}</div>}
      </div>
    </div>
  );
}
