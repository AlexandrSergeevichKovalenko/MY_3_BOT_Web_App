import React, { useCallback, useEffect, useState } from 'react';
import useFitText from './useFitText.js';

/**
 * Wo-Fragen section of «работа над ошибками», built on the SAME template as the Wo-Frage
 * Trainer: a tiny Q&A with the question word blanked + 4 options, instant ✅/❌ reveal +
 * the rule + a tip. Fast by design — the whole due page is prefetched in ONE request,
 * every answer is graded LOCALLY (the deterministic answer ships with the card) and the
 * spaced-repetition advance is fire-and-forget in the background. No per-card round-trip.
 */
function splitBlank(s) {
  const str = String(s || '');
  const i = str.indexOf('___');
  if (i < 0) return ['', str];
  return [str.slice(0, i), str.slice(i + 3)];
}

export default function WoFrageReviewGame({ api, haptic, onClose, onBack }) {
  const [phase, setPhase] = useState('loading'); // loading|error|learning|done
  const [error, setError] = useState('');
  const [cards, setCards] = useState([]);
  const [idx, setIdx] = useState(0);
  const [pick, setPick] = useState(null);
  const [stats, setStats] = useState({ correct: 0, answered: 0 });
  const [remaining, setRemaining] = useState(0);
  const card = cards[idx];
  const phraseFit = useFitText(card ? String(card.s || '') : idx, { max: 30 });

  const loadBatch = useCallback(async () => {
    setPhase('loading');
    try {
      const data = await api('/api/answer/review/wofrage/batch', { limit: 20 });
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      const list = data.cards || [];
      setCards(list);
      setRemaining(data.remaining || 0);
      setIdx(0); setPick(null); setStats({ correct: 0, answered: 0 });
      setPhase(list.length ? 'learning' : 'done');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadBatch(); }, [loadBatch]);

  const answer = useCallback((o) => {
    if (pick || !card) return;
    const ok = o === card.a;
    setPick(o);
    setStats((s) => ({ correct: s.correct + (ok ? 1 : 0), answered: s.answered + 1 }));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    // Advance spaced-repetition in the background — never blocks the next card.
    api('/api/answer/review/wofrage/answer', { mistake_id: card.id, is_correct: ok })
      .catch(() => { /* fire-and-forget */ });
  }, [pick, card, api, haptic]);

  const next = useCallback(() => {
    const ni = idx + 1;
    setPick(null);
    if (ni >= cards.length) {
      // Page done. A big backlog may have more due → pull the next page.
      if (remaining > cards.length) { loadBatch(); return; }
      setPhase('done'); return;
    }
    setIdx(ni);
  }, [idx, cards.length, remaining, loadBatch]);

  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">❓ Wo-Fragen wiederholen</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onBack || onClose}>Zurück</button>
    </>);
  } else if (phase === 'done') {
    const { correct, answered } = stats;
    const pct = answered ? Math.round((100 * correct) / answered) : 0;
    body = (<>
      <div className="as-cert">
        <div className="as-cert-medal">{answered > 0 ? '🎉' : '✨'}</div>
        <div className="as-cert-place">{answered > 0 ? 'Fehler wiederholt!' : 'Keine Wo-Fragen-Fehler'}</div>
        {answered > 0 ? <div className="as-cert-sub">{correct} из {answered} верно · {pct}%</div> : null}
        <div className="as-cert-foot">Следующие вернутся в нужное время 🔁</div>
      </div>
      {onBack ? <button className="ans-btn" onClick={onBack}>← К разделам</button> : null}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else {
    const answered = !!pick;
    const correct = card.a;
    const [pre, post] = splitBlank(card.s);
    const total = cards.length;
    body = (<>
      <div className="as-top">
        <span className="as-theme-sm">🔁 Работа над ошибками</span>
        <span className="al-progress">{idx + 1} / {total}</span>
        <span className="as-score">{stats.correct}✓</span>
      </div>
      <div className={`as-word wo-word${answered ? (pick === correct ? ' ok' : ' bad') : ''}`}>
        <span className="fit-line wo-line" ref={phraseFit}>
          <span>{pre}</span>
          <span className="wo-slot">{answered ? correct : '?'}</span>
          <span>{post}</span>
        </span>
      </div>
      {card.clue ? <div className="wo-clue">{card.clue}</div> : null}
      <div className="as-buttons wo-buttons">
        {(card.opts || []).map((o) => {
          const state = answered ? (o === correct ? ' on' : (o === pick ? ' wrong' : '')) : '';
          return (
            <button key={o} type="button" className={`as-btn-art wo-opt${state}`}
              onClick={() => answer(o)} disabled={answered}>{o}</button>
          );
        })}
      </div>
      {answered ? (
        <div className={`wo-rev ans-body ${pick === correct ? 'ok' : 'bad'}`} style={{ marginTop: 14 }}>
          <div className="wo-rev-phrase">{pick === correct ? '✅ Richtig!' : '❌'} <b>{pre}{correct}{post}</b></div>
          {card.frage_ru ? <div className="wo-rev-ru">{card.frage_ru}</div> : null}
          {card.erklaerung ? <div className="wo-rev-rule">📐 {card.erklaerung}</div> : null}
          {card.tip ? <div className="wo-rev-tip">💡 {card.tip}</div> : null}
          {card.lemma ? <div className="wo-rev-rule">📝 {card.lemma}{card.verb_ru ? ` — ${card.verb_ru}` : ''}</div> : null}
          <button className="ans-btn wo-next" onClick={next}>
            {remaining > idx + 1 ? `Дальше (${Math.max(0, remaining - idx - 1)}) →` : 'Fertig →'}
          </button>
        </div>
      ) : (
        <div className="al-hint-pre">Выбери вопрос</div>
      )}
    </>);
  }

  return (
    <div className="ans-root">
      <div className="ans-card as-card al-card">{body}</div>
    </div>
  );
}
