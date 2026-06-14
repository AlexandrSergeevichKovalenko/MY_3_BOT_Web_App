import React, { useCallback, useEffect, useRef, useState } from 'react';

// Artikel Trainer — the self-paced LEARNING deck (companion to the timed Sprint).
// Look at a noun, tap der/die/das, get instant ✅/❌ + a memory hint, then swipe
// left (or tap "Дальше") to the next card. No timer. Grading is local (not
// competitive); each answer is posted so the user's mistakes resurface later.
const ARTICLES = ['der', 'die', 'das'];
const ART_CLASS = { der: 'art-der', die: 'art-die', das: 'art-das' };

export default function ArtikelLearnGame({ api, haptic, onClose, focus = false }) {
  const [phase, setPhase] = useState('loading'); // loading|error|empty|learning|done|focuspick|focusdone
  const [error, setError] = useState('');
  const [meta, setMeta] = useState(null);
  const cardsRef = useRef([]);
  const [idx, setIdx] = useState(0);
  const [chosen, setChosen] = useState(null);
  const [stats, setStats] = useState({ correct: 0, answered: 0 });
  const [themes, setThemes] = useState([]);
  const [focusLabel, setFocusLabel] = useState('');
  const touchX = useRef(null);
  const audioRef = useRef(null);

  const playAudio = useCallback((url) => {
    if (!url) return;
    try {
      if (!audioRef.current) audioRef.current = new Audio();
      audioRef.current.src = url;
      audioRef.current.play().catch(() => { /* autoplay blocked */ });
    } catch (_e) { /* noop */ }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (focus) {
          const data = await api('/api/webapp/artikel/themes', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Доступно на Premium'); setPhase('error'); return; }
          setThemes(data.themes || []); setPhase('focuspick'); return;
        }
        const data = await api('/api/webapp/artikel/learn/today', {});
        if (cancelled) return;
        if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
        cardsRef.current = data.cards || [];
        setMeta(data);
        setPhase(cardsRef.current.length ? 'learning' : 'empty');
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, focus]);

  const pickFocus = useCallback(async (themeKey, label) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/artikel/learn/focus', { theme_key: themeKey });
      if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
      setFocusLabel(data.theme_label || label || themeKey);
      setPhase('focusdone');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  const answer = useCallback((article) => {
    if (chosen) return;
    const c = cardsRef.current[idx];
    if (!c) return;
    const ok = article === c.a;
    setChosen(article);
    setStats((s) => ({ correct: s.correct + (ok ? 1 : 0), answered: s.answered + 1 }));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    // Play "der/die/das + Wort" right on the tap (a user gesture → iOS allows it).
    playAudio(c.audio);
    api('/api/webapp/artikel/learn/answer', {
      word: c.w, article: c.a, is_correct: ok,
      theme_key: meta?.theme_key || '', set_id: meta?.set_id || '',
    }).catch(() => { /* fire-and-forget */ });
  }, [chosen, idx, api, haptic, meta, playAudio]);

  const next = useCallback(() => {
    const ni = idx + 1;
    setChosen(null);
    if (ni >= cardsRef.current.length) { setPhase('done'); return; }
    setIdx(ni);
  }, [idx]);

  const restart = useCallback(() => {
    setIdx(0); setChosen(null); setStats({ correct: 0, answered: 0 }); setPhase('learning');
  }, []);

  const onTouchStart = (e) => { touchX.current = e.touches?.[0]?.clientX ?? null; };
  const onTouchEnd = (e) => {
    if (touchX.current == null) return;
    const dx = (e.changedTouches?.[0]?.clientX ?? touchX.current) - touchX.current;
    touchX.current = null;
    if (dx < -50 && chosen) next(); // swipe left → next (only once answered)
  };

  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">📚 Artikel lernen</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'focuspick') {
    body = (<>
      <div className="as-eyebrow">🎯 Своя тема на завтра · Premium</div>
      <div className="as-rules">Выбери тему — подготовим её к завтрашнему дню (со звуком и картинками).</div>
      <div className="as-themes">
        {themes.map((t) => (
          <button key={t.theme_key} type="button" className="as-theme-btn"
            onClick={() => pickFocus(t.theme_key, t.label_ru || t.label_de)}>
            <span>{t.label_ru || t.label_de}</span>
            <span className="as-theme-cnt">{t.count}</span>
          </button>
        ))}
      </div>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'focusdone') {
    body = (<>
      <div className="as-cert">
        <div className="as-cert-medal">🎯</div>
        <div className="as-cert-place">Готово!</div>
        <div className="as-cert-sub">Завтра учишь: <b>{focusLabel}</b></div>
        <div className="as-cert-foot">Подготовим ночью — со звуком и картинками</div>
      </div>
      <button className="ans-btn" onClick={onClose}>Отлично</button>
    </>);
  } else if (phase === 'empty') {
    body = (<>
      <div className="ans-verdict">📚 Artikel lernen</div>
      <div className="ans-explain">На сегодня слов нет. Загляни позже.</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'done') {
    const { correct, answered } = stats;
    const pct = answered ? Math.round((100 * correct) / answered) : 0;
    body = (<>
      <div className="as-cert">
        <div className="as-cert-medal">📚</div>
        <div className="as-cert-place">Готово!</div>
        <div className="as-cert-sub">{correct} из {answered} верно · {pct}%</div>
        <div className="as-cert-foot">📘 {meta?.theme_label || ''}</div>
      </div>
      {meta?.progress?.theme_total ? (
        <div className="al-progress-card">
          <div>📈 Тема «{meta.theme_label}»: выучено <b>{meta.progress.mastered}</b> из {meta.progress.theme_total}</div>
          <div className="al-bar">
            <div className="al-bar-fill" style={{ width: `${Math.min(100, Math.round((100 * meta.progress.mastered) / meta.progress.theme_total))}%` }} />
          </div>
        </div>
      ) : null}
      <button className="ans-btn al-next" onClick={restart}>🔁 Ещё раз</button>
      <button className="ans-btn-ghost" onClick={onClose}>Закрыть</button>
    </>);
  } else {
    const c = cardsRef.current[idx];
    const total = cardsRef.current.length;
    body = (<>
      <div className="as-top">
        <span className="as-theme-sm">📘 {meta?.theme_label || ''}</span>
        <span className="al-progress">{idx + 1} / {total}</span>
        <span className="as-score">{stats.correct}✓</span>
      </div>
      {c?.review ? <div className="al-review-badge">🔁 повтор твоей ошибки</div> : null}
      {c?.image ? (
        <div className="al-img"><img src={c.image} alt="" loading="eager" /></div>
      ) : null}
      <div className={`as-word ${chosen ? c.a : ''}`}>{c ? c.w : '…'}</div>
      <div className="as-buttons">
        {ARTICLES.map((a) => {
          let cls = `as-btn-art ${ART_CLASS[a]}`;
          if (chosen) {
            if (a === c.a) cls += ' al-correct';
            else if (a === chosen) cls += ' al-wrong';
            else cls += ' al-dim';
          }
          return (
            <button key={a} type="button" className={cls} disabled={!!chosen}
              onClick={() => answer(a)}>{a}</button>
          );
        })}
      </div>
      {chosen ? (
        <div className="al-reveal" onTouchStart={onTouchStart} onTouchEnd={onTouchEnd}>
          <div className={`al-feedback ${chosen === c.a ? 'ok' : 'bad'}`}>
            {chosen === c.a ? '✅ Верно!' : `❌ Правильно: ${c.a}`}
            <span className="al-ru"> · {c.ru}</span>
            {c.audio ? (
              <button type="button" className="al-audio" onClick={() => playAudio(c.audio)}
                aria-label="Послушать">🔊</button>
            ) : null}
          </div>
          {c.tip ? <div className="al-tip">{c.tip}</div> : null}
          <button className="ans-btn al-next" onClick={next}>Дальше →</button>
          <div className="al-swipe-hint">или свайпни влево 👈</div>
        </div>
      ) : (
        <div className="al-hint-pre">Выбери артикль</div>
      )}
    </>);
  }

  return <div className="ans-root"><div className="ans-card as-card al-card">{body}</div></div>;
}
