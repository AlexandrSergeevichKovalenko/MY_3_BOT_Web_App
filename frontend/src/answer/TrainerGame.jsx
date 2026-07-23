import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { saveGermanWordViaLookup } from '../dictionary/saveUtils.js';

// Recognition TRAINER: for a fixed anchor word, each round shows one CORRECT answer
// plus up to four verified DISTRACTORS; tap the right one. It prepares the learner for
// the SPRINT on the same word three days later (there they must PRODUCE it from memory).
// Everything runs LOCALLY — the payload (correct answers + distractors + examples) is
// fetched once, so every tap is instant with zero round-trips.
const REL = {
  synonym: { title: 'Тренировка синонимов', ask: 'синоним', emoji: '🟢' },
  antonym: { title: 'Тренировка антонимов', ask: 'антоним', emoji: '🔴' },
};
const ARTICLES = new Set(['der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen', 'einem', 'einer', 'eines']);
const MAX_ROUNDS = 8;
const OPTIONS_PER_ROUND = 5; // 1 correct + up to 4 distractors

function normCore(s) {
  let x = String(s || '').toLowerCase().replace(/[^a-zäöüßà-ÿ0-9\s'-]/g, ' ').replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
  let toks = x ? x.split(' ') : [];
  if (toks.length && ARTICLES.has(toks[0])) toks = toks.slice(1);
  return toks.join(' ');
}
function shuffle(arr) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
const deOf = (a) => String((a && typeof a === 'object' ? (a.de || a.word) : a) || '').trim();

// Build the round list once: each correct answer becomes one round with its own
// distractor draw (rotating through the pool so options vary round to round).
// Only correct answers that HAVE an example+nuance (in exampleKeys) become rounds, so
// every «right pick» card is complete — falls back to all if too few are example-backed.
function buildRounds(correct, distractors, exampleKeys, anchor) {
  const aKey = normCore(anchor);
  // Never show the anchor itself as its own "synonym" (guard vs a dirty accepted list).
  let corr = (correct || []).filter((c) => deOf(c) && normCore(deOf(c)) !== aKey);
  if (exampleKeys && exampleKeys.size) {
    const withEx = corr.filter((c) => exampleKeys.has(normCore(deOf(c))));
    if (withEx.length >= 2) corr = withEx;
  }
  const dist = (distractors || []).filter((d) => deOf(d) && normCore(deOf(d)) !== aKey);
  const rounds = shuffle(corr).slice(0, Math.min(MAX_ROUNDS, corr.length));
  const nDist = Math.max(0, Math.min(OPTIONS_PER_ROUND - 1, dist.length));
  let cursor = 0;
  return rounds.map((c) => {
    const picks = [];
    const seen = new Set([normCore(deOf(c))]);
    // rotate through the distractor pool for variety, skipping collisions
    for (let tries = 0; picks.length < nDist && tries < dist.length * 2; tries++) {
      const d = dist[cursor % dist.length];
      cursor += 1;
      const k = normCore(deOf(d));
      if (k && !seen.has(k)) { seen.add(k); picks.push(d); }
    }
    const options = shuffle([{ ...c, __correct: true }, ...picks.map((d) => ({ ...d, __correct: false }))]);
    return { correct: c, options };
  });
}

export default function TrainerGame({ id, api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|intro|playing|done|error
  const [meta, setMeta] = useState(null);
  const [error, setError] = useState('');
  const [rounds, setRounds] = useState([]);
  const [ri, setRi] = useState(0);
  const [picked, setPicked] = useState(null);   // the option the user tapped this round
  const [score, setScore] = useState(0);
  const [saved, setSaved] = useState(() => new Set());
  const [saveError, setSaveError] = useState('');

  // word(normalised) → substitution example {sentence_de, sentence_ru}
  const exampleByWord = useMemo(() => {
    const m = new Map();
    for (const e of meta?.correct_examples || []) {
      const k = normCore(e?.word);
      if (k) m.set(k, e);
    }
    return m;
  }, [meta]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/trainer/task', { kind: 'tr', id });
        if (cancelled) return;
        const exKeys = new Set((data.correct_examples || []).map((e) => normCore(e?.word)).filter(Boolean));
        const rs = buildRounds(data.correct, data.distractors, exKeys, data.wort);
        if (!rs.length) { setError('Для этого слова пока нет вариантов.'); setPhase('error'); return; }
        setMeta(data); setRounds(rs); setPhase('intro');
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [id]);

  const rel = REL[meta?.relation] || REL.synonym;
  const round = rounds[ri];
  const total = rounds.length;

  const start = useCallback(() => { setPhase('playing'); setRi(0); setPicked(null); setScore(0); }, []);

  const pick = useCallback((opt) => {
    if (picked) return;                     // one answer per round
    setPicked(opt);
    const ok = !!opt.__correct;
    if (ok) setScore((s) => s + 1);
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
  }, [picked, haptic]);

  const next = useCallback(() => {
    if (ri + 1 >= total) { setPhase('done'); return; }
    setRi((i) => i + 1); setPicked(null);
  }, [ri, total]);

  const saveChip = useCallback((de, ru) => {
    if (!de || saved.has(de)) return;
    setSaved((s) => new Set(s).add(de));
    setSaveError('');
    try { haptic?.('ok'); } catch (_e) { /* noop */ }
    Promise.resolve(saveGermanWordViaLookup({ api, word: de, fallbackTranslation: ru || '', origin: 'trainer_save' }))
      .catch(() => {
        setSaved((s) => { const n = new Set(s); n.delete(de); return n; });
        setSaveError('Не удалось сохранить слово. Нажми ещё раз.');
        try { haptic?.('bad'); } catch (_e2) { /* noop */ }
      });
  }, [api, saved, haptic]);

  const shell = (body, cls = '') => (
    <div className="ans-root"><div className={`ans-card ${cls}`}>{body}</div></div>
  );

  if (phase === 'loading') return shell(<><div className="ans-skel" /><div className="ans-skel sm" /></>);
  if (phase === 'error') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );

  if (phase === 'intro') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">{rel.emoji} {rel.title}</span></div>
      <div className="tr-hero">
        <div className="tr-hero-word">{meta?.wort}</div>
        {meta?.hint_ru ? <div className="tr-hero-hint">{meta.hint_ru}</div> : null}
      </div>
      <div className="tr-intro">
        <p>Для слова <b>{meta?.wort}</b> выбирай верный <b>{rel.ask}</b> из вариантов.</p>
        <p className="tr-intro-dim">
          {total} {total === 1 ? 'слово' : total < 5 ? 'слова' : 'слов'} · через 3 дня они вернутся
          в спринте — там впишешь по памяти ✍️
        </p>
      </div>
      <button className="ans-btn tr-go" onClick={start}>▶️ Начать</button>
    </>
  );

  if (phase === 'playing' && round) {
    const answered = !!picked;
    const correctDe = deOf(round.correct);
    const ex = exampleByWord.get(normCore(correctDe));
    return shell(
      <>
        <div className="tr-top">
          <span className="tr-top-rel">{rel.emoji} {rel.ask}</span>
          <span className="tr-top-prog">{ri + 1} / {total}</span>
        </div>
        <div className="tr-bar"><div className="tr-bar-fill" style={{ width: `${((ri + (answered ? 1 : 0)) / total) * 100}%` }} /></div>

        <div className="tr-anchor">
          <div className="tr-anchor-word">{meta?.wort}</div>
          {meta?.hint_ru ? <div className="tr-anchor-hint">{meta.hint_ru}</div> : null}
          <div className="tr-anchor-ask">Найди {rel.ask}:</div>
        </div>

        <div className="tr-options">
          {round.options.map((opt, i) => {
            let cls = 'tr-opt';
            if (answered) {
              if (opt.__correct) cls += ' correct';
              else if (opt === picked) cls += ' wrong';
              else cls += ' dim';
            }
            return (
              <button key={i} type="button" className={cls} disabled={answered} onClick={() => pick(opt)}>
                <span className="tr-opt-word">{deOf(opt)}</span>
                {answered && opt.__correct ? <span className="tr-opt-mark">✓</span> : null}
                {answered && opt === picked && !opt.__correct ? <span className="tr-opt-mark">✗</span> : null}
              </button>
            );
          })}
        </div>

        {answered ? (
          <div className={`tr-feedback ${picked.__correct ? 'ok' : 'bad'}`}>
            {picked.__correct ? (
              <>
                <div className="tr-fb-head">✅ Верно — <b>{correctDe}</b>{round.correct?.ru ? ` · ${round.correct.ru}` : ''}</div>
                {ex?.nuance ? <div className="tr-fb-nuance">💡 {ex.nuance}</div> : null}
                {ex?.sentence_de ? (
                  <div className="tr-fb-ex">
                    <div className="tr-fb-base">„{meta?.target_example?.de}“</div>
                    <div className="tr-fb-swap">„{ex.sentence_de}“</div>
                    {ex.sentence_ru ? <div className="tr-fb-ru">{ex.sentence_ru}</div> : null}
                  </div>
                ) : null}
              </>
            ) : (
              <>
                <div className="tr-fb-head">✗ <b>{deOf(picked)}</b>{picked?.ru_gloss ? ` — ${picked.ru_gloss}` : ''}</div>
                {picked?.why_not ? <div className="tr-fb-why">{picked.why_not}</div> : null}
                {picked?.example_de ? <div className="tr-fb-base">„{picked.example_de}“</div> : null}
                <div className="tr-fb-right">Верно было: <b>{correctDe}</b>{round.correct?.ru ? ` · ${round.correct.ru}` : ''}</div>
              </>
            )}
            <button className="ans-btn tr-next" onClick={next}>{ri + 1 >= total ? 'Итог →' : 'Дальше →'}</button>
          </div>
        ) : null}
      </>
    );
  }

  // done
  const allCorrect = (meta?.correct || []).filter((c) => deOf(c));
  return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">{rel.emoji} {rel.title}</span></div>
      <div className="tr-score">
        <div className="tr-score-num">{score}<span className="tr-score-of"> / {total}</span></div>
        <div className="tr-score-sub">верных с первого раза</div>
      </div>
      <div className="tr-done-note">
        Через 3 дня <b>{meta?.wort}</b> вернётся в спринте — там впишешь {rel.ask}ы по памяти 🔥
      </div>
      {allCorrect.length ? (
        <div className="tr-all">
          <div className="tr-all-head">Все {rel.ask}ы <span className="tr-all-dim">· 👆 нажми, чтобы сохранить в словарь</span>:</div>
          <div className="tr-chips">
            {allCorrect.map((c, i) => {
              const de = deOf(c); const isSaved = saved.has(de);
              return (
                <button key={i} type="button" className={`tr-chip${isSaved ? ' saved' : ''}`} disabled={isSaved}
                        onClick={() => saveChip(de, c?.ru || '')}>
                  {isSaved ? '💾 ' : ''}{de}
                </button>
              );
            })}
          </div>
          {saveError ? <div className="as-save-error">{saveError}</div> : null}
        </div>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );
}
