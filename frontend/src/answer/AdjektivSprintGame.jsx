import React, { useCallback, useEffect, useRef, useState } from 'react';
import useFitText from './useFitText.js';

// Adjektiv Sprint — a set of 15 adjective-ending situations, 5 s each (auto-advance
// or tap "Weiter"). Same engine/feel as Artikel Sprint: whole set preloaded, each
// tap graded LOCALLY (instant green/red), re-graded server-side on submit. The
// result lists every case with the official rule + tip.
const ENDINGS = ['e', 'en', 'er', 'es', 'em'];
const PER_ITEM_S = 5;

export default function AdjektivSprintGame({ api, haptic, onClose, battleId = null, battleList = false }) {
  const [phase, setPhase] = useState('loading'); // loading|battlelist|intro|countdown|playing|grading|done|error
  const [meta, setMeta] = useState(null);
  const [battles, setBattles] = useState([]);
  const [error, setError] = useState('');
  const [idx, setIdx] = useState(0);
  const [count, setCount] = useState(3);
  const [itemLeft, setItemLeft] = useState(PER_ITEM_S);
  const [flash, setFlash] = useState(null); // {ok}
  const [result, setResult] = useState(null);
  const answersRef = useRef([]);
  const itemsRef = useRef([]);
  const startRef = useRef(0);
  const idxRef = useRef(0);
  const tickRef = useRef(null);
  const phraseFit = useFitText(idx, { max: 36 });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (battleList) {
          const data = await api('/api/webapp/adjektiv/battles', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
          setBattles(data.battles || []); setPhase('battlelist'); return;
        }
        const data = battleId
          ? await api('/api/webapp/adjektiv/battle', { battle_id: battleId })
          : await api('/api/webapp/adjektiv/today', {});
        if (cancelled) return;
        if (!data.ok) { setError(data.error || 'Сет недоступен'); setPhase('error'); return; }
        setMeta(data);
        itemsRef.current = data.items || [];
        if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); }
        else { setPhase('intro'); }
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, battleId, battleList]);

  const playBattle = useCallback(async (bid) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/adjektiv/battle', { battle_id: bid });
      if (!data.ok) { setError(data.error || 'Батл недоступен'); setPhase('error'); return; }
      setMeta(data);
      itemsRef.current = data.items || [];
      if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); return; }
      setPhase('intro');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  const clearTick = () => { if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; } };
  useEffect(() => () => clearTick(), []);

  const finish = useCallback(async () => {
    clearTick();
    setPhase('grading');
    const time_ms = Math.max(0, Date.now() - startRef.current);
    try {
      const data = await api('/api/webapp/adjektiv/submit', {
        set_id: meta?.set_id, answers: answersRef.current, time_ms,
      });
      setResult(data);
    } catch (e) {
      const ans = answersRef.current;
      const correct = ans.filter((a) => a.ok).length;
      setResult({ correct, answered: ans.filter((a) => a.chosen).length, total: itemsRef.current.length,
        pct: ans.length ? Math.round((100 * correct) / ans.length) : 0, items: [] });
    }
    setPhase('done');
  }, [api, meta]);

  // advance to next item (or finish). `entry` = {chosen, ok} recorded for THIS item.
  const advance = useCallback((entry) => {
    clearTick();
    answersRef.current.push(entry);
    const ni = idxRef.current + 1;
    idxRef.current = ni;
    if (ni >= itemsRef.current.length) { finish(); return; }
    setIdx(ni);
    setFlash(null);
    setItemLeft(PER_ITEM_S);
  }, [finish]);

  // per-item 5s countdown → auto-skip on timeout
  useEffect(() => {
    if (phase !== 'playing') return undefined;
    clearTick();
    setItemLeft(PER_ITEM_S);
    tickRef.current = setInterval(() => {
      setItemLeft((v) => {
        if (v <= 1) { advance({ chosen: '', ok: false }); return PER_ITEM_S; }
        return v - 1;
      });
    }, 1000);
    return () => clearTick();
  }, [phase, idx, advance]);

  const startCountdown = useCallback(() => {
    setPhase('countdown'); setCount(3);
    let c = 3;
    const t = setInterval(() => {
      c -= 1;
      if (c <= 0) {
        clearInterval(t);
        startRef.current = Date.now();
        answersRef.current = []; idxRef.current = 0; setIdx(0); setItemLeft(PER_ITEM_S);
        setPhase('playing');
      } else { setCount(c); }
    }, 800);
  }, []);

  const answer = useCallback((ending) => {
    if (phase !== 'playing') return;
    const it = itemsRef.current[idxRef.current];
    if (!it) return;
    const ok = String(ending) === String(it.a);
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    setFlash({ ok });
    setTimeout(() => advance({ chosen: ending, ok }), 360);
  }, [phase, haptic, advance]);

  let cls = '';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">🔠 Adjektiv Sprint</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'battlelist') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚔️ Мои батлы (Adjektiv)</div>
      {battles.length ? (
        <div className="as-themes">
          {battles.map((b) => (
            <button key={b.battle_id} type="button" className="as-theme-btn"
              disabled={b.played} onClick={() => !b.played && playBattle(b.battle_id)}>
              <span>⚔️ {b.creator_name || 'Батл'} · {b.theme_label}</span>
              <span className="as-theme-cnt">{b.played ? '✓ сыграно' : '▶️'}</span>
            </button>
          ))}
        </div>
      ) : (
        <div className="ans-explain">Активных батлов нет. Прими вызов в личке или создай свой (/adjbattle, Premium).</div>
      )}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'intro') {
    cls = 'as-intro';
    body = (<>
      <div className="as-eyebrow">{battleId || meta?.theme_label === '⚔️ Battle' ? '⚔️ Adjektiv Battle' : '🔠 Adjektiv Sprint'}</div>
      <div className="as-theme">Adjektivendungen</div>
      <div className="as-rules">{itemsRef.current.length} Situationen · <b>5 Sek.</b> pro Frage · wähle die Endung!</div>
      <button className="ans-btn as-go" onClick={startCountdown}>▶️ Старт</button>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'countdown') {
    cls = 'as-countdown';
    body = <div className="as-count" key={count}>{count}</div>;
  } else if (phase === 'playing') {
    const it = itemsRef.current[idx];
    cls = 'as-play';
    const score = answersRef.current.filter((a) => a.ok).length;
    body = (<>
      <div className="as-top">
        <span className="as-theme-sm">{idx + 1} / {itemsRef.current.length}</span>
        <span className={`as-timer${itemLeft <= 2 ? ' urgent' : ''}`}>{itemLeft}s</span>
        <span className="as-score">{score}</span>
      </div>
      <div className={`as-word adj-word${flash ? (flash.ok ? ' ok' : ' bad') : ''}`} key={idx}>
        <span className="fit-line adj-line" ref={phraseFit}>
          {it ? (<>
            <span>{it.before}</span>
            <span className="adj-slot">{flash && it ? `-${it.a}` : '·'}</span>
            <span>{it.after}</span>
          </>) : '…'}
        </span>
      </div>
      {it && it.ru ? <div className="adj-hint">{it.ru}</div> : null}
      <div className="as-buttons adj-buttons">
        {ENDINGS.map((e) => (
          <button key={e} type="button" className="as-btn-art adj-end" onClick={() => answer(e)}>-{e}</button>
        ))}
      </div>
      <button className="ans-btn-ghost adj-skip" onClick={() => advance({ chosen: '', ok: false })}>Weiter →</button>
    </>);
  } else if (phase === 'grading') {
    body = <div className="ans-loading">Zähle…</div>;
  } else {
    const r = result || {};
    const items = r.items || [];
    const rank = r.ranking || null;
    const place = rank?.your_place || null;
    const medal = place === 1 ? '🥇' : place === 2 ? '🥈' : place === 3 ? '🥉' : '🎖️';
    cls = 'as-done';
    body = (<>
      {place ? (
        <div className="as-cert">
          <div className="as-cert-medal">{medal}</div>
          <div className="as-cert-place">{place} место</div>
          <div className="as-cert-sub">из {rank.total} · {r.correct || 0} верных ({r.pct || 0}%)</div>
          <div className="as-cert-foot">🔠 Adjektiv Sprint</div>
        </div>
      ) : (<>
        <div className="ans-verdict">🔠 Adjektiv Sprint</div>
        <div className="as-result-score"><b>{r.correct || 0}</b> верных из {r.answered || 0} · {r.pct || 0}%</div>
      </>)}
      {r.already_played ? <div className="ans-explain">Ты уже играл этот сет.</div> : null}
      {rank && rank.total ? (
        <div className="sp-rank">
          <div className="sp-rank-head">🏆 Топ-3</div>
          {(rank.top3 || []).map((p, i) => (
            <div className={`sp-rank-row${rank.your_place === i + 1 ? ' me' : ''}`} key={p.user_id || i}>
              <span>{['🥇', '🥈', '🥉'][i] || '•'} {p.name || 'Игрок'}</span>
              <span className="sp-rank-n">{p.count}</span>
            </div>
          ))}
        </div>
      ) : null}
      {items.length ? (
        <div className="as-result-list">
          {items.map((it, i) => (
            <div key={i} className={`adj-rev ${it.ok ? 'ok' : 'bad'}`}>
              <div className="adj-rev-phrase">
                {it.ok ? '✅' : '❌'} <b>{it.full || `${it.before}${it.a}${it.after}`}</b>
                {!it.ok ? <span className="as-mine"> (ты: {it.chosen ? `-${it.chosen}` : '—'})</span> : null}
              </div>
              {it.erklaerung ? <div className="adj-rev-rule">{it.erklaerung}</div> : null}
              {it.tip ? <div className="adj-rev-tip">💡 {it.tip}</div> : null}
            </div>
          ))}
        </div>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  }

  return <div className="ans-root"><div className={`ans-card as-card ${cls}`}>{body}</div></div>;
}
