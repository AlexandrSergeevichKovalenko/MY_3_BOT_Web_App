import React, { useCallback, useEffect, useRef, useState } from 'react';

/**
 * «Finde im Bild» scene studio (admin only).
 *
 * Two jobs on one screen:
 *  1) Composer — describe several scenes in Russian (one per line); the bot renders each
 *     into a busy image where no object repeats.
 *  2) Targeting — for each ready image, draw a box around an object and type its German
 *     word (with article). One image yields as many tasks as objects you label. The drawn
 *     box is the answer region (human-verified); the typed word gives the article.
 *
 * This replaces "generate a specific object" (DALL-E made it giant/obvious) with "generate
 * a rich scene, you pick what's actually hidden."
 */
export default function PinReviewScreen({ api, haptic, onClose }) {
  const [status, setStatus] = useState(null);      // {approved,target,ready_scenes,generating,needed}
  const [scenes, setScenes] = useState(null);      // ready scenes to target
  const [idx, setIdx] = useState(0);
  const [descs, setDescs] = useState('');
  const [rect, setRect] = useState(null);
  const [drag, setDrag] = useState(null);
  const [word, setWord] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');
  const wrapRef = useRef(null);

  const refresh = useCallback(async () => {
    try {
      const [st, sc] = await Promise.all([
        api('/api/answer/pinreview/status', {}),
        api('/api/answer/pinreview/scenes', {}),
      ]);
      setStatus(st);
      setScenes(sc.scenes || []);
    } catch (e) {
      setError(String(e?.message || e));
      setScenes([]);
    }
  }, [api]);

  useEffect(() => { refresh(); }, [refresh]);

  const scene = scenes && scenes[idx];
  useEffect(() => { setRect(null); setDrag(null); setWord(''); setError(''); }, [scene]);

  // ── drawing ──────────────────────────────────────────────────────────────
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
  const start = (e) => { const p = pointOf(e); if (p) setDrag({ x0: p.x, y0: p.y, x1: p.x, y1: p.y }); };
  const move = (e) => { if (!drag) return; e.preventDefault(); const p = pointOf(e); if (p) setDrag((d) => ({ ...d, x1: p.x, y1: p.y })); };
  const end = () => {
    if (!drag) return;
    const x = Math.min(drag.x0, drag.x1), y = Math.min(drag.y0, drag.y1);
    const w = Math.abs(drag.x1 - drag.x0), h = Math.abs(drag.y1 - drag.y0);
    setDrag(null);
    if (w < 0.02 || h < 0.02) return;
    setRect({ x, y, w, h });
    haptic?.('ok');
  };
  const live = drag
    ? { x: Math.min(drag.x0, drag.x1), y: Math.min(drag.y0, drag.y1), w: Math.abs(drag.x1 - drag.x0), h: Math.abs(drag.y1 - drag.y0) }
    : rect;

  // ── actions ──────────────────────────────────────────────────────────────
  const submitScenes = async () => {
    const list = descs.split('\n').map((s) => s.trim()).filter(Boolean);
    if (!list.length || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/pinreview/scenes/create', { descriptions: list });
      setDescs('');
      setNote(`🎨 В очереди на генерацию: ${r.queued}. Появятся здесь через 1–2 минуты.`);
      haptic?.('ok');
      await refresh();
    } catch (e) { setError(String(e?.message || e)); } finally { setBusy(false); }
  };

  const addTarget = async () => {
    if (!scene || !live || busy) return;
    const w = word.trim();
    if (!/^(der|die|das)\s+\S/i.test(w)) { setError('Слово с артиклем: der/die/das …'); return; }
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/pinreview/addtarget', {
        scene_id: scene.scene_id, bbox: [live.x, live.y, live.w, live.h], word: w,
      });
      setScenes((prev) => prev.map((s, i) => (i === idx ? { ...s, targets: [...(s.targets || []), r.target_label] } : s)));
      setRect(null); setWord('');
      setNote(r.duplicate ? `⚠️ «${r.target_label}» уже был в другом задании — добавил всё равно.` : '');
      haptic?.('ok');
    } catch (e) { setError(String(e?.message || e)); haptic?.('bad'); } finally { setBusy(false); }
  };

  const finishScene = async (action) => {
    if (!scene || busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/scenedone', { scene_id: scene.scene_id, action });
      const rest = scenes.filter((_, i) => i !== idx);
      setScenes(rest);
      setIdx(0);
      await refresh();
    } catch (e) { setError(String(e?.message || e)); } finally { setBusy(false); }
  };

  if (scenes === null || status === null) return <div className="ans-loading">Загружаю студию…</div>;

  return (
    <div className="pinrev">
      <div className="pinrev-status">
        Готово: <b>{status.approved}</b>/{status.target} · на обводке: <b>{status.ready_scenes}</b> ·
        генерируется: <b>{status.generating}</b>
      </div>

      {/* Composer */}
      <div className="pinrev-composer">
        <div className="pinrev-comp-title">🎨 Заказать сцены{status.needed > 0 ? ` (нужно ещё ~${status.needed})` : ''}</div>
        <textarea
          className="pinrev-textarea" rows={3} value={descs}
          onChange={(e) => setDescs(e.target.value)}
          placeholder={'Опиши сцены по-русски, каждую с новой строки:\nрабочий стол с офисными мелочами\nдетская с игрушками на полу\nполка в гараже с инструментами'}
        />
        <button className="ans-btn" disabled={!descs.trim() || busy} onClick={submitScenes}>
          Сгенерировать сцены
        </button>
      </div>
      {note ? <div className="pinrev-note">{note}</div> : null}

      {/* Targeting */}
      {scene ? (
        <div className="pinrev-target">
          <div className="pinrev-head">
            <span className="pinrev-count">Картинка {idx + 1} / {scenes.length}</span>
            {scenes.length > 1 ? (
              <button className="pinrev-link" disabled={busy} onClick={() => setIdx((idx + 1) % scenes.length)}>
                другая картинка →
              </button>
            ) : null}
          </div>
          <p className="pinrev-hint">Обведи предмет пальцем и подпиши его по-немецки с артиклем. Можно несколько предметов с одной картинки.</p>
          <div
            className="pinrev-wrap" ref={wrapRef}
            onTouchStart={start} onTouchMove={move} onTouchEnd={end}
            onMouseDown={start} onMouseMove={move} onMouseUp={end} onMouseLeave={end}
          >
            <img className="pin-img" src={scene.image_url} alt="" draggable="false" />
            {live ? (
              <span className={`pinrev-box ${drag ? 'live' : ''}`}
                style={{ left: `${live.x * 100}%`, top: `${live.y * 100}%`, width: `${live.w * 100}%`, height: `${live.h * 100}%` }} />
            ) : null}
          </div>
          <input
            className="ans-input" value={word} onChange={(e) => setWord(e.target.value)}
            placeholder="der Feuerlöscher" autoCapitalize="off" autoCorrect="off"
            onKeyDown={(e) => { if (e.key === 'Enter') addTarget(); }}
          />
          {(scene.targets || []).length ? (
            <div className="pinrev-chips">
              {scene.targets.map((t, i) => <span className="pinrev-chip" key={i}>✅ {t}</span>)}
            </div>
          ) : null}
          {error ? <div className="pinrev-err">{error}</div> : null}
          <div className="pinrev-actions">
            <button className="ans-btn" disabled={!live || !word.trim() || busy} onClick={addTarget}>
              ➕ Добавить этот предмет
            </button>
            <button className="ans-btn-ghost" disabled={busy} onClick={() => finishScene('done')}>
              Готово с картинкой →
            </button>
            <button className="ans-btn-ghost" disabled={busy} onClick={() => finishScene('skip')}>
              🗑 Плохая картинка — пропустить
            </button>
          </div>
        </div>
      ) : (
        <div className="pinrev-empty">
          <div className="pinrev-empty-sub">
            {status.generating > 0
              ? `⏳ Генерируется картинок: ${status.generating}. Обнови через минуту.`
              : 'Готовых картинок для обводки пока нет — закажи сцены выше.'}
          </div>
          <button className="ans-btn-ghost" onClick={refresh}>Обновить</button>
        </div>
      )}
      {error && !scene ? <div className="pinrev-err">{error}</div> : null}
      <button className="ans-btn-ghost" onClick={onClose}>Закрыть</button>
    </div>
  );
}
