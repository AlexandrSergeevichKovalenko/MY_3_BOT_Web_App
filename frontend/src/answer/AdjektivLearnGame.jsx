import React, { useCallback, useEffect, useState } from 'react';
import useFitText from './useFitText.js';

// Adjektiv Trainer — self-paced learning deck for adjective endings (companion to
// the Adjektiv Sprint, same look). Each card: a phrase with a blanked ending +
// the case/gender/declension hint; tap an ending → instant green/red + the full
// rule + a tip; swipe/"Weiter" to the next. Endless (prefetches batches). No timer.
const ENDINGS = ['e', 'en', 'er', 'es', 'em'];

export default function AdjektivLearnGame({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|card|error
  const [deck, setDeck] = useState([]);
  const [i, setI] = useState(0);
  const [pick, setPick] = useState(null);
  const [streak, setStreak] = useState(0);
  const [error, setError] = useState('');
  const phraseFit = useFitText(i, { max: 36 });

  const loadMore = useCallback(async () => {
    try {
      const d = await api('/api/webapp/adjektiv/learn', { count: 12 });
      if (!d.ok) { setError(d.error || 'Недоступно'); setPhase('error'); return; }
      setDeck((prev) => [...prev, ...(d.items || [])]);
      setPhase('card');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadMore(); }, [loadMore]);

  const card = deck[i];
  // Prefetch the next batch as we near the end so it never stalls.
  useEffect(() => {
    if (phase === 'card' && deck.length && i >= deck.length - 2) loadMore();
  }, [i, deck.length, phase, loadMore]);

  const answer = useCallback((e) => {
    if (pick || !card) return;
    setPick(e);
    const ok = e === card.a;
    setStreak((s) => (ok ? s + 1 : 0));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
  }, [pick, card, haptic]);

  const next = useCallback(() => { setPick(null); setI((x) => x + 1); }, []);

  let cls = 'al-card';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">📚 Adjektiv Trainer</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (!card) {
    body = <div className="ans-loading">Lädt…</div>;
  } else {
    const answered = !!pick;
    const correct = card.a;
    body = (<>
      <div className="as-top">
        <span className="al-progress">📚 Adjektivendungen</span>
        {streak > 1 ? <span className="al-streak">🔥 {streak}</span> : <span />}
      </div>
      <div className={`as-word adj-word${answered ? (pick === correct ? ' ok' : ' bad') : ''}`}>
        <span className="fit-line adj-line" ref={phraseFit}>
          <span>{card.before}</span>
          <span className="adj-slot">{answered ? `-${correct}` : '·'}</span>
          <span>{card.after}</span>
        </span>
      </div>
      {card.ru ? <div className="adj-hint">{card.ru}</div> : null}
      <div className="as-buttons adj-buttons">
        {ENDINGS.map((e) => {
          const state = answered ? (e === correct ? ' on' : (e === pick ? ' wrong' : '')) : '';
          return (
            <button key={e} type="button" className={`as-btn-art adj-end${state}`}
              onClick={() => answer(e)} disabled={answered}>-{e}</button>
          );
        })}
      </div>
      {answered ? (
        <div className={`adj-rev ${pick === correct ? 'ok' : 'bad'}`} style={{ marginTop: 14 }}>
          <div className="adj-rev-phrase">{pick === correct ? '✅ Richtig!' : '❌'} <b>{card.full || `${card.before}${correct}${card.after}`}</b></div>
          {card.erklaerung ? <div className="adj-rev-rule">{card.erklaerung}</div> : null}
          {card.tip ? <div className="adj-rev-tip">💡 {card.tip}</div> : null}
        </div>
      ) : null}
      {answered
        ? <button className="ans-btn" onClick={next}>Weiter →</button>
        : <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>}
    </>);
  }

  return <div className="ans-root"><div className={`ans-card as-card ${cls}`}>{body}</div></div>;
}
