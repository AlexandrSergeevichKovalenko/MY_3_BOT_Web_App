import React, { useCallback, useEffect, useRef, useState } from 'react';

// 2-minute der/die/das speed game. The whole word set is preloaded, so each tap
// is graded LOCALLY (instant green/red flash + auto-advance, zero round-trip).
// The official score is re-graded server-side on submit.
const ARTICLES = ['der', 'die', 'das'];

export default function ArtikelSprintGame({ api, haptic, onClose, practice = false, battleId = null, battleList = false }) {
  const [phase, setPhase] = useState('loading'); // loading|themepick|battlelist|intro|countdown|playing|done|error
  const [meta, setMeta] = useState(null);
  const [themes, setThemes] = useState([]);
  const [battles, setBattles] = useState([]);
  const [error, setError] = useState('');
  const [idx, setIdx] = useState(0);
  const [left, setLeft] = useState(120);
  const [count, setCount] = useState(3);
  const [flash, setFlash] = useState(null); // {ok:bool} transient
  const [result, setResult] = useState(null);
  const answersRef = useRef([]);
  const wordsRef = useRef([]);
  const startRef = useRef(0);
  const timerRef = useRef(null);
  const idxRef = useRef(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (battleList) {
          const data = await api('/api/webapp/artikel/battles', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
          setBattles(data.battles || []); setPhase('battlelist'); return;
        }
        if (practice) {
          const data = await api('/api/webapp/artikel/themes', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Доступно на Premium'); setPhase('error'); return; }
          setThemes(data.themes || []); setPhase('themepick'); return;
        }
        const data = battleId
          ? await api('/api/webapp/artikel/battle', { battle_id: battleId })
          : await api('/api/webapp/artikel/today', {});
        if (cancelled) return;
        if (!data.ok) { setError(data.error || 'Набор недоступен'); setPhase('error'); return; }
        setMeta(data);
        wordsRef.current = data.words || [];
        if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); }
        else { setLeft(data.duration_s || 120); setPhase('intro'); }
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, practice, battleId, battleList]);

  const playBattle = useCallback(async (bid) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/artikel/battle', { battle_id: bid });
      if (!data.ok) { setError(data.error || 'Батл недоступен'); setPhase('error'); return; }
      setMeta(data);
      wordsRef.current = data.words || [];
      if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); return; }
      setLeft(data.duration_s || 120);
      setPhase('intro');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  const pickTheme = useCallback(async (themeKey) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/artikel/practice', { theme_key: themeKey });
      if (!data.ok) { setError(data.error || 'Тема недоступна'); setPhase('error'); return; }
      setMeta(data);
      wordsRef.current = data.words || [];
      setLeft(data.duration_s || 120);
      setPhase('intro');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current); }, []);

  const finish = useCallback(async () => {
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
    setPhase('grading');
    const time_ms = Math.max(0, Date.now() - startRef.current);
    try {
      const data = await api('/api/webapp/artikel/submit', {
        set_id: meta?.set_id, answers: answersRef.current, time_ms,
      });
      setResult(data);
    } catch (e) {
      // fall back to a local tally so the user still sees a result
      const ans = answersRef.current;
      const correct = ans.filter((a) => a.ok).length;
      setResult({ correct, answered: ans.length, total: wordsRef.current.length,
        pct: ans.length ? Math.round((100 * correct) / ans.length) : 0, items: [] });
    }
    setPhase('done');
  }, [api, meta]);

  const startCountdown = useCallback(() => {
    setPhase('countdown'); setCount(3);
    let c = 3;
    const t = setInterval(() => {
      c -= 1;
      if (c <= 0) {
        clearInterval(t);
        startRef.current = Date.now();
        answersRef.current = []; idxRef.current = 0; setIdx(0);
        setPhase('playing');
        timerRef.current = setInterval(() => {
          setLeft((v) => { if (v <= 1) { finish(); return 0; } return v - 1; });
        }, 1000);
      } else { setCount(c); }
    }, 800);
  }, [finish]);

  const answer = useCallback((article) => {
    if (phase !== 'playing') return;
    const i = idxRef.current;
    const w = wordsRef.current[i];
    if (!w) return;
    const ok = String(article) === String(w.a);
    answersRef.current.push({ w: w.w, chosen: article, ok });
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    setFlash({ ok, key: i });
    const ni = i + 1;
    idxRef.current = ni; setIdx(ni);
    if (ni >= wordsRef.current.length) { finish(); }
  }, [phase, haptic, finish]);

  let cls = '';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">⚡ Artikel Sprint</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'battlelist') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚔️ Мои батлы</div>
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
        <div className="ans-explain">Активных батлов нет. Прими вызов в личке или создай свой (/battle, Premium).</div>
      )}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'themepick') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚡ Artikel Sprint · своя тема</div>
      <div className="as-rules">Выбери тему — 2 минуты тренировки. <b>Premium</b>.</div>
      <div className="as-themes">
        {themes.map((t) => (
          <button key={t.theme_key} type="button" className="as-theme-btn" onClick={() => pickTheme(t.theme_key)}>
            <span>{t.label_ru || t.label_de}</span>
            <span className="as-theme-cnt">{t.count}</span>
          </button>
        ))}
      </div>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'intro') {
    cls = 'as-intro';
    body = (<>
      <div className="as-eyebrow">⚡ Artikel Sprint</div>
      <div className="as-theme">{meta?.theme_label || ''}</div>
      <div className="as-rules">2 Minuten · tippe <b>der/die/das</b> · so viele wie möglich!</div>
      <button className="ans-btn as-go" onClick={startCountdown}>▶️ Старт</button>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'countdown') {
    cls = 'as-countdown';
    body = <div className="as-count" key={count}>{count}</div>;
  } else if (phase === 'playing') {
    const w = wordsRef.current[idx];
    cls = 'as-play';
    body = (<>
      <div className="as-top">
        <span className="as-theme-sm">{meta?.theme_label || ''}</span>
        <span className="as-timer">{Math.floor(left / 60)}:{String(left % 60).padStart(2, '0')}</span>
        <span className="as-score">{answersRef.current.filter((a) => a.ok).length}</span>
      </div>
      <div className={`as-word${flash ? (flash.ok ? ' ok' : ' bad') : ''}`} key={idx}>
        {w ? w.w : '…'}
      </div>
      <div className="as-buttons">
        {ARTICLES.map((art) => (
          <button key={art} type="button" className={`as-btn-art art-${art}`} onClick={() => answer(art)}>
            {art}
          </button>
        ))}
      </div>
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
          <div className="as-cert-foot">⚡ Artikel Sprint{meta?.theme_label ? ` · ${meta.theme_label}` : ''}</div>
        </div>
      ) : (<>
        <div className="ans-verdict">⚡ Artikel Sprint</div>
        <div className="as-result-score"><b>{r.correct || 0}</b> верных из {r.answered || 0} · {r.pct || 0}%</div>
      </>)}
      {r.already_played ? <div className="ans-explain">Ты уже играл этот сет сегодня.</div> : null}
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
            <div key={i} className={`as-row ${it.ok ? 'ok' : 'bad'}`}>
              {it.ok ? (
                <>✅ <b>{it.a}</b> {it.w}</>
              ) : (
                <>❌ <b className="as-correct-article">{it.a}</b> {it.w}<span className="as-mine"> (ты: {it.chosen || '—'})</span></>
              )}
              {it.ru ? <span className="as-ru"> · {it.ru}</span> : null}
            </div>
          ))}
        </div>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  }

  return <div className="ans-root"><div className={`ans-card as-card ${cls}`}>{body}</div></div>;
}
