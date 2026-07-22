import React, { useCallback, useEffect, useRef, useState } from 'react';

/**
 * Acceptance screen for «Finde im Bild» (admin only).
 *
 * The answer region is what decides right/wrong, and the model draws it badly — it has
 * framed the book stack instead of the marker lying below it. Judging its draft with
 * approve/reject means rejecting almost everything, and every rejection costs a fresh
 * DALL·E + vision generation. So the admin DRAWS the region: drag a rectangle over the
 * object. The model's box is shown as a draft — when it happens to be right, saving is
 * one tap; when it isn't, one drag fixes it.
 */
export default function PinReviewScreen({ api, haptic, onClose }) {
  const [items, setItems] = useState(null);
  const [idx, setIdx] = useState(0);
  const [rect, setRect] = useState(null);      // normalized {x, y, w, h}
  const [drag, setDrag] = useState(null);      // in-progress {x0, y0, x1, y1}
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [done, setDone] = useState(0);
  const wrapRef = useRef(null);

  useEffect(() => {
    let alive = true;
    api('/api/answer/pinreview/queue', {})
      .then((d) => { if (alive) setItems(d.items || []); })
      .catch((e) => { if (alive) { setItems([]); setError(String(e?.message || e)); } });
    return () => { alive = false; };
  }, [api]);

  const task = items && items[idx];
  // Reset the frame to the model's draft whenever we move to another task.
  useEffect(() => {
    if (!task) return;
    const b = task.bbox;
    setRect(Array.isArray(b) && b.length === 4 ? { x: b[0], y: b[1], w: b[2], h: b[3] } : null);
    setDrag(null);
    setError('');
  }, [task]);

  const pointOf = useCallback((e) => {
    const el = wrapRef.current;
    if (!el) return null;
    const r = el.getBoundingClientRect();
    const t = e.touches?.[0] || e.changedTouches?.[0] || e;
    return {
      x: Math.min(1, Math.max(0, (t.clientX - r.left) / r.width)),
      y: Math.min(1, Math.max(0, (t.clientY - r.top) / r.height)),
    };
  }, []);

  const start = (e) => {
    const p = pointOf(e);
    if (!p) return;
    setDrag({ x0: p.x, y0: p.y, x1: p.x, y1: p.y });
  };
  const move = (e) => {
    if (!drag) return;
    e.preventDefault();                     // the drag must not scroll the page
    const p = pointOf(e);
    if (p) setDrag((d) => ({ ...d, x1: p.x, y1: p.y }));
  };
  const end = () => {
    if (!drag) return;
    const x = Math.min(drag.x0, drag.x1), y = Math.min(drag.y0, drag.y1);
    const w = Math.abs(drag.x1 - drag.x0), h = Math.abs(drag.y1 - drag.y0);
    setDrag(null);
    // A stray tap (no real drag) must not wipe a good draft box.
    if (w < 0.02 || h < 0.02) return;
    setRect({ x, y, w, h });
    haptic?.('ok');
  };

  const live = drag
    ? {
      x: Math.min(drag.x0, drag.x1), y: Math.min(drag.y0, drag.y1),
      w: Math.abs(drag.x1 - drag.x0), h: Math.abs(drag.y1 - drag.y0),
    }
    : rect;

  const advance = () => {
    setDone((n) => n + 1);
    if (idx + 1 < (items?.length || 0)) setIdx(idx + 1);
    else setItems([]);                       // queue exhausted → the summary below
  };

  const save = async () => {
    if (!task || !live || busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/save', {
        aufgabe_id: task.aufgabe_id, bbox: [live.x, live.y, live.w, live.h],
      });
      haptic?.('ok');
      advance();
    } catch (e) {
      setError(String(e?.message || e));
      haptic?.('bad');
    } finally { setBusy(false); }
  };

  const reject = async () => {
    if (!task || busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/save', { aufgabe_id: task.aufgabe_id, action: 'reject' });
      haptic?.('bad');
      advance();
    } catch (e) {
      setError(String(e?.message || e));
    } finally { setBusy(false); }
  };

  if (items === null) return <div className="ans-loading">Загружаю очередь…</div>;
  if (!items.length) {
    return (
      <div className="pinrev">
        <div className="pinrev-empty">
          <div className="pinrev-empty-title">✅ Очередь пуста</div>
          <div className="pinrev-empty-sub">
            {done ? `Проверено за заход: ${done}.` : 'Заданий на приёмке нет.'}
          </div>
          {error ? <div className="pinrev-err">{error}</div> : null}
          <button className="ans-btn-ghost" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  return (
    <div className="pinrev">
      <div className="pinrev-head">
        <span className="pinrev-count">{idx + 1} / {items.length}</span>
        <span className="pinrev-word">{task.target_label}</span>
      </div>
      <p className="pinrev-hint">
        Обведи предмет пальцем — эта область засчитает тап ученика.
        {task.bbox ? ' Зелёная рамка — черновик модели, поверх неё можно рисовать.' : ''}
      </p>
      <div
        className="pinrev-wrap"
        ref={wrapRef}
        onTouchStart={start} onTouchMove={move} onTouchEnd={end}
        onMouseDown={start} onMouseMove={move} onMouseUp={end} onMouseLeave={end}
      >
        <img className="pin-img" src={task.image_url} alt="" draggable="false" />
        {live ? (
          <span
            className={`pinrev-box ${drag ? 'live' : ''}`}
            style={{
              left: `${live.x * 100}%`, top: `${live.y * 100}%`,
              width: `${live.w * 100}%`, height: `${live.h * 100}%`,
            }}
          />
        ) : null}
      </div>
      <div className="pinrev-meta">
        Ответ: <b>{task.target_label}</b>
        {task.hint_ru ? <span className="pinrev-dim"> · {task.hint_ru}</span> : null}
      </div>
      {error ? <div className="pinrev-err">{error}</div> : null}
      <div className="pinrev-actions">
        <button className="ans-btn" disabled={!live || busy} onClick={save}>
          {busy ? 'Сохраняю…' : '✅ Сохранить рамку'}
        </button>
        <button className="ans-btn-ghost" disabled={busy} onClick={reject}>
          🗑 Удалить (предмет виден плохо / слишком очевиден / не на картинке)
        </button>
      </div>
    </div>
  );
}
