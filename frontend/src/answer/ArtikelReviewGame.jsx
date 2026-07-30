import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';

/**
 * Artikel section of «работа над ошибками», built on the SAME template as the Artikel
 * Trainer: colour-coded der/die/das (blue/red/green), a photo of the word, instant
 * ✅/❌ reveal + a memory tip. Fast by design — the whole due page is prefetched in ONE
 * request, every answer is graded LOCALLY (the deterministic article ships with the
 * card) and the spaced-repetition advance is fire-and-forget in the background. No
 * per-card round-trip, so it stays instant at any scale.
 */
const ARTICLES = ['der', 'die', 'das'];
const ART_CLASS = { der: 'art-der', die: 'art-die', das: 'art-das' };

export default function ArtikelReviewGame({ api, haptic, onClose, onBack }) {
  const [phase, setPhase] = useState('loading'); // loading|error|learning|done
  const [error, setError] = useState('');
  const cardsRef = useRef([]);
  const [idx, setIdx] = useState(0);
  const [chosen, setChosen] = useState(null);
  const [stats, setStats] = useState({ correct: 0, answered: 0 });
  const [remaining, setRemaining] = useState(0);
  const touchX = useRef(null);
  const audioRef = useRef(null);
  const wordRef = useRef(null);

  // Shrink long compounds to fit the card on one line (no ugly mid-word wrap).
  const fitWord = useCallback(() => {
    const el = wordRef.current;
    const box = el?.parentElement;
    if (!el || !box) return;
    let size = 40;
    el.style.fontSize = `${size}px`;
    const avail = box.clientWidth - 28;
    while (el.scrollWidth > avail && size > 15) { size -= 1; el.style.fontSize = `${size}px`; }
  }, []);

  const playAudio = useCallback((url) => {
    if (!url) return;
    try {
      if (!audioRef.current) audioRef.current = new Audio();
      audioRef.current.src = url;
      audioRef.current.play().catch(() => { /* autoplay blocked */ });
    } catch (_e) { /* noop */ }
  }, []);

  const loadBatch = useCallback(async () => {
    setPhase('loading');
    try {
      const data = await api('/api/answer/review/artikel/batch', { limit: 20 });
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      cardsRef.current = data.cards || [];
      setRemaining(data.remaining || 0);
      setIdx(0); setChosen(null); setStats({ correct: 0, answered: 0 });
      setPhase(cardsRef.current.length ? 'learning' : 'done');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadBatch(); }, [loadBatch]);

  const answer = useCallback((article) => {
    if (chosen) return;
    const c = cardsRef.current[idx];
    if (!c) return;
    const ok = article === c.a;
    setChosen(article);
    setStats((s) => ({ correct: s.correct + (ok ? 1 : 0), answered: s.answered + 1 }));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    playAudio(c.audio); // "der/die/das + Wort" on the tap (user gesture → iOS allows it)
    // Advance spaced-repetition in the background — never blocks the next card.
    api('/api/answer/review/artikel/answer', { mistake_id: c.id, is_correct: ok })
      .catch(() => { /* fire-and-forget */ });
  }, [chosen, idx, api, haptic, playAudio]);

  const next = useCallback(() => {
    const ni = idx + 1;
    setChosen(null);
    if (ni >= cardsRef.current.length) {
      // Page done. More may already be due (a big backlog) → pull the next page.
      if (remaining > cardsRef.current.length) { loadBatch(); return; }
      setPhase('done'); return;
    }
    setIdx(ni);
  }, [idx, remaining, loadBatch]);

  useLayoutEffect(() => { if (phase === 'learning') fitWord(); }, [idx, phase, fitWord]);

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
      <div className="ans-verdict">⚡ Artikel wiederholen</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onBack || onClose}>Zurück</button>
    </>);
  } else if (phase === 'done') {
    const { correct, answered } = stats;
    const pct = answered ? Math.round((100 * correct) / answered) : 0;
    body = (<>
      <div className="as-cert">
        <div className="as-cert-medal">{answered > 0 ? '🎉' : '✨'}</div>
        <div className="as-cert-place">{answered > 0 ? 'Fehler wiederholt!' : 'Keine Artikel-Fehler'}</div>
        {answered > 0 ? <div className="as-cert-sub">{correct} из {answered} верно · {pct}%</div> : null}
        <div className="as-cert-foot">Следующие вернутся в нужное время 🔁</div>
      </div>
      {onBack ? <button className="ans-btn" onClick={onBack}>← К разделам</button> : null}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else {
    const c = cardsRef.current[idx];
    const total = cardsRef.current.length;
    body = (<>
      <div className="as-top">
        <span className="as-theme-sm">🔁 Работа над ошибками</span>
        <span className="al-progress">{idx + 1} / {total}</span>
        <span className="as-score">{stats.correct}✓</span>
      </div>
      {c?.image ? (
        <div className="al-img"><img src={c.image} alt="" loading="eager" /></div>
      ) : null}
      <div className={`as-word ${chosen ? c.a : ''}`}>
        <span className="al-word-text" ref={wordRef}>{c ? c.w : '…'}</span>
      </div>
      {/* Двуродовые (der/die Flur): артикль решает смысл, поэтому перевод показываем
          ВМЕСТЕ с вопросом — и только у них. Иначе вопрос неотвечаем. */}
      {c && c.tg && c.ru ? <div className="as-sense">({c.ru})</div> : null}
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
            {c.ru ? <span className="al-ru"> · {c.ru}</span> : null}
            {c.audio ? (
              <button type="button" className="al-audio" onClick={() => playAudio(c.audio)}
                aria-label="Послушать">🔊</button>
            ) : null}
          </div>
          {c.tip ? <div className="al-tip">{c.tip}</div> : null}
          <button className="ans-btn al-next" onClick={next}>
            {remaining > idx + 1 ? `Дальше (${Math.max(0, remaining - idx - 1)}) →` : 'Fertig →'}
          </button>
          <div className="al-swipe-hint">или свайпни влево 👈</div>
        </div>
      ) : (
        <div className="al-hint-pre">Выбери артикль</div>
      )}
    </>);
  }

  return (
    <div className="ans-root">
      <div className="ans-card as-card al-card">{body}</div>
    </div>
  );
}
