import React, { useCallback, useEffect, useState } from 'react';
import useFitText from './useFitText.js';
import useWideScreen from './useWideScreen.js';
import AskOverlay from './AskOverlay.jsx';

// Wo-Frage Trainer — self-paced learning deck for question words (companion to the
// Wo-Frage Sprint, same look). Each card: a tiny Q&A with the question word blanked
// + 4 options; tap one → instant green/red + the full rule + a tip; "Weiter" to the
// next. Endless (prefetches batches). No timer.
function splitBlank(s) {
  const str = String(s || '');
  const i = str.indexOf('___');
  if (i < 0) return ['', str];
  return [str.slice(0, i), str.slice(i + 3)];
}

export default function WoFrageLearnGame({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|card|error
  const [deck, setDeck] = useState([]);
  const [i, setI] = useState(0);
  const [pick, setPick] = useState(null);
  const [streak, setStreak] = useState(0);
  const [error, setError] = useState('');
  const [askOpen, setAskOpen] = useState(false);
  const _cur = deck[i];
  const wide = useWideScreen();   // планшет: потолок кегля задаёт CSS
  const phraseFit = useFitText(`${wide}|${_cur ? String(_cur.s || '') : i}`, { max: wide ? 'css' : 30 });

  const loadMore = useCallback(async () => {
    try {
      const d = await api('/api/webapp/wofrage/learn', { count: 12 });
      if (!d.ok) { setError(d.error || 'Недоступно'); setPhase('error'); return; }
      setDeck((prev) => [...prev, ...(d.items || [])]);
      setPhase('card');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadMore(); }, [loadMore]);

  const card = deck[i];
  useEffect(() => {
    if (phase === 'card' && deck.length && i >= deck.length - 2) loadMore();
  }, [i, deck.length, phase, loadMore]);

  const answer = useCallback((o) => {
    if (pick || !card) return;
    setPick(o);
    // Верных форм может быть две — засчитываем любую (разницу поясняем ниже).
    const accepted = [String(card.a), ...(Array.isArray(card.also_ok) ? card.also_ok.map(String) : [])];
    const ok = accepted.includes(String(o));
    setStreak((s) => (ok ? s + 1 : 0));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
  }, [pick, card, haptic]);

  const next = useCallback(() => { setPick(null); setI((x) => x + 1); }, []);

  let cls = 'al-card';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">📚 Wo-Frage Trainer</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (!card) {
    body = <div className="ans-loading">Lädt…</div>;
  } else {
    const answered = !!pick;
    const correct = card.a;
  const accepted = [String(card.a), ...(Array.isArray(card.also_ok) ? card.also_ok.map(String) : [])];
  const picked_ok = answered && accepted.includes(String(pick));
    const [pre, post] = splitBlank(card.s);
    body = (<>
      <div className="as-top ans-r-head">
        <span className="al-progress">📚 Wo-Fragen</span>
        {streak > 1 ? <span className="al-streak">🔥 {streak}</span> : <span />}
      </div>
      <div className={`as-word ans-r-prompt wo-word${answered ? (picked_ok ? ' ok' : ' bad') : ''}`}>
        <span className="fit-line wo-line" ref={phraseFit}>
          <span>{pre}</span>
          <span className="wo-slot">{answered ? correct : '?'}</span>
          <span>{post}</span>
        </span>
      </div>
      {card.clue ? <div className="wo-clue">{card.clue}</div> : null}
      <div className="as-buttons ans-r-work wo-buttons">
        {(card.opts || []).map((o) => {
          const state = answered ? (accepted.includes(String(o)) ? ' on' : (o === pick ? ' wrong' : '')) : '';
          return (
            <button key={o} type="button" className={`as-btn-art wo-opt${state}`}
              onClick={() => answer(o)} disabled={answered}>{o}</button>
          );
        })}
      </div>
      {answered ? (
        <div className={`wo-rev ans-body ${picked_ok ? 'ok' : 'bad'}`} style={{ marginTop: 14 }}>
          <div className="wo-rev-phrase">{picked_ok ? '✅ Richtig!' : '❌'} <b>{pre}{picked_ok ? pick : correct}{post}</b></div>
          {card.frage_ru ? <div className="wo-rev-ru">{card.frage_ru}</div> : null}
          {card.unterschied ? <div className="wo-both">Здесь верны обе формы. {card.unterschied}</div> : null}
          {card.erklaerung ? <div className="wo-rev-rule">📐 {card.erklaerung}</div> : null}
          {card.tip ? <div className="wo-rev-tip">💡 {card.tip}</div> : null}
          {card.lemma ? <div className="wo-rev-rule">📝 {card.lemma}{card.verb_ru ? ` — ${card.verb_ru}` : ''}</div> : null}
        </div>
      ) : null}
      <button className="ask-open-btn" onClick={() => setAskOpen(true)}>❓ Спросить</button>
      {answered
        ? <button className="ans-btn" onClick={next}>Weiter →</button>
        : <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>}
      {askOpen ? (
        <AskOverlay api={api} onClose={() => setAskOpen(false)}
          saveText={card.lemma || ''}
          saveTranslation={card.verb_ru || ''}
          context={[
            'Интерактив: Wo-Fragen (вопросительные местоименные наречия / Präpositionaladverbien).',
            card.s ? `Вопрос: ${card.s.replace('___', correct)}` : '',
            card.clue ? `Ответ: ${card.clue}` : '',
            card.lemma ? `Управление: ${card.lemma}${card.verb_ru ? ` (${card.verb_ru})` : ''}.` : '',
            card.erklaerung ? `Правило: ${card.erklaerung}` : '',
          ].filter(Boolean).join(' ')} />
      ) : null}
    </>);
  }

  return <div className="ans-root ans-root--keepkbd"><div className={`ans-card as-card ${cls}`}>{body}</div></div>;
}
