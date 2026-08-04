import React, { useCallback, useEffect, useRef, useState } from 'react';
import useFitText from './useFitText.js';
import useWideScreen from './useWideScreen.js';

// Wo-Frage Sprint — a set of 10 "pick the right question word" situations, 8 s each
// (auto-advance or tap "Weiter"). Same engine/feel as Adjektiv Sprint: whole set
// preloaded, each tap graded LOCALLY (instant green/red), re-graded server-side on
// submit. Each item is a tiny Q&A dialog with the question word blanked; you choose
// the right Wo(r)+Präposition (thing) or Präposition+wen/wem (person) from 4 options.
const PER_ITEM_S = 8;

// Split "___ wartest du?" into the part before/after the blank.
function splitBlank(s) {
  const str = String(s || '');
  const i = str.indexOf('___');
  if (i < 0) return ['', str];
  return [str.slice(0, i), str.slice(i + 3)];
}

export default function WoFrageSprintGame({ api, haptic, onClose, battleId = null, battleList = false }) {
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
  const _it = itemsRef.current[idx];
  const wide = useWideScreen();   // планшет: потолок кегля задаёт CSS
  const phraseFit = useFitText(`${wide}|${_it ? String(_it.s || '') : idx}`, { max: wide ? 'css' : 30 });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (battleList) {
          const data = await api('/api/webapp/wofrage/battles', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
          setBattles(data.battles || []); setPhase('battlelist'); return;
        }
        const data = battleId
          ? await api('/api/webapp/wofrage/battle', { battle_id: battleId })
          : await api('/api/webapp/wofrage/today', {});
        if (cancelled) return;
        if (!data.ok) { setError(data.error || 'Сет недоступен'); setPhase('error'); return; }
        setMeta(data);
        itemsRef.current = data.items || [];
        if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); }
        else { setPhase('intro'); }
      } catch (e) { if (!cancelled) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, battleId, battleList]);

  const playBattle = useCallback(async (bid) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/wofrage/battle', { battle_id: bid });
      if (!data.ok) { setError(data.error || 'Батл недоступен'); setPhase('error'); return; }
      setMeta(data);
      itemsRef.current = data.items || [];
      if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); return; }
      setPhase('intro');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
  }, [api]);

  const clearTick = () => { if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; } };
  useEffect(() => () => clearTick(), []);

  const finish = useCallback(async () => {
    clearTick();
    setPhase('grading');
    const time_ms = Math.max(0, Date.now() - startRef.current);
    try {
      const data = await api('/api/webapp/wofrage/submit', {
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

  // per-item 8s countdown → auto-skip on timeout
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

  const answer = useCallback((opt) => {
    if (phase !== 'playing') return;
    const it = itemsRef.current[idxRef.current];
    if (!it) return;
    // Верных ответов может быть два: «Woran» и «Worunter leidest du?» — оба немецкий.
    // Считаем так же, как сервер, иначе подсветка скажет «неверно», а зачтётся верно.
    const accepted = [String(it.a), ...(Array.isArray(it.also_ok) ? it.also_ok.map(String) : [])];
    const ok = accepted.includes(String(opt));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    setFlash({ ok });
    setTimeout(() => advance({ chosen: opt, ok }), 420);
  }, [phase, haptic, advance]);

  let cls = '';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">❓ Wo-Frage Sprint</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'battlelist') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚔️ Мои батлы (Wo-Fragen)</div>
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
        <div className="ans-explain">Активных батлов нет. Чтобы поучаствовать, жди приглашение в личке. Premium-пользователь может создать новый батл.</div>
      )}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'intro') {
    cls = 'as-intro';
    body = (<>
      <div className="as-eyebrow">{battleId || meta?.theme_label === '⚔️ Battle' ? '⚔️ Wo-Frage Battle' : '❓ Wo-Frage Sprint'}</div>
      <div className="as-theme">Wo-Fragen</div>
      <div className="as-rules">{itemsRef.current.length} Fragen · <b>8 Sek.</b> pro Frage · wähle das Fragewort!</div>
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
    const [pre, post] = splitBlank(it?.s);
    body = (<>
      <div className="as-top ans-r-head">
        <span className="as-theme-sm">{idx + 1} / {itemsRef.current.length}</span>
        <span className={`as-timer${itemLeft <= 3 ? ' urgent' : ''}`}>{itemLeft}s</span>
        <span className="as-score">{score}</span>
      </div>
      <div className={`as-word ans-r-prompt wo-word${flash ? (flash.ok ? ' ok' : ' bad') : ''}`} key={idx}>
        <span className="fit-line wo-line" ref={phraseFit}>
          {it ? (<>
            <span>{pre}</span>
            <span className="wo-slot">{flash ? it.a : '?'}</span>
            <span>{post}</span>
          </>) : '…'}
        </span>
      </div>
      {it && it.clue ? <div className="wo-clue">{it.clue}</div> : null}
      <div className="as-buttons ans-r-work wo-buttons">
        {(it?.opts || []).map((o) => (
          <button key={o} type="button" className="as-btn-art wo-opt" onClick={() => answer(o)}>{o}</button>
        ))}
      </div>
      <button className="ans-btn-ghost wo-skip" onClick={() => advance({ chosen: '', ok: false })}>Weiter →</button>
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
          <div className="as-cert-foot">❓ Wo-Frage Sprint</div>
        </div>
      ) : (<>
        <div className="ans-verdict">❓ Wo-Frage Sprint</div>
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
        <div className="as-result-list ans-body">
          {items.map((it, i) => {
            const [pre, post] = splitBlank(it.s);
            return (
              <div key={i} className={`wo-rev ${it.ok ? 'ok' : 'bad'}`}>
                <div className="wo-rev-phrase">
                  {it.ok ? '✅' : '❌'} {pre}<b>{it.also_ok_used ? it.chosen : it.a}</b>{post}
                  {!it.ok ? <span className="as-mine"> (ты: {it.chosen || '—'})</span> : null}
                  {it.unterschied ? <div className="wo-both">Здесь верны обе формы. {it.unterschied}</div> : null}
                </div>
                {it.frage_ru ? <div className="wo-rev-ru">{it.frage_ru}</div> : null}
                {it.clue ? <div className="wo-rev-clue">{it.clue}</div> : null}
                {it.erklaerung ? <div className="wo-rev-rule">📐 {it.erklaerung}</div> : null}
                {it.tip ? <div className="wo-rev-tip">💡 {it.tip}</div> : null}
              </div>
            );
          })}
        </div>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  }

  return <div className="ans-root ans-root--keepkbd"><div className={`ans-card as-card ${cls}`}>{body}</div></div>;
}
