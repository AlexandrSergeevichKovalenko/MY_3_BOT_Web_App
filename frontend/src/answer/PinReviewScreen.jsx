import React, { useCallback, useEffect, useRef, useState } from 'react';

/**
 * «Finde im Bild» scene studio (admin only). Two fill paths, one targeting screen:
 *   A) describe scenes in Russian → the bot renders busy images (no repeated objects);
 *   B) upload your OWN photos → they become scenes immediately.
 * Then on each image: draw ONE box, type the German word (with article), add it. One image
 * yields as many tasks as objects you label; the drawn box is the human-verified answer
 * region, the typed word gives the article.
 */
export default function PinReviewScreen({ api, haptic, onClose }) {
  const [status, setStatus] = useState(null);
  const [scenes, setScenes] = useState(null);
  const [idx, setIdx] = useState(0);
  const [descs, setDescs] = useState('');
  const [rect, setRect] = useState(null);
  const [drag, setDrag] = useState(null);
  const [drawing, setDrawing] = useState(true);     // false once a box is locked → image scrolls
  const [word, setWord] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');
  const wrapRef = useRef(null);
  const fileRef = useRef(null);

  const refresh = useCallback(async () => {
    try {
      const [st, sc] = await Promise.all([
        api('/api/answer/pinreview/status', {}),
        api('/api/answer/pinreview/scenes', {}),
      ]);
      setStatus(st);
      setScenes(sc.scenes || []);
    } catch (e) { setError(String(e?.message || e)); setScenes([]); }
  }, [api]);

  useEffect(() => { refresh(); }, [refresh]);

  const scene = scenes && scenes[idx];
  useEffect(() => { setRect(null); setDrag(null); setDrawing(true); setWord(''); setError(''); }, [scene]);

  // ── drawing (only while `drawing`; once a box is set the image scrolls instead) ──
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
  const start = (e) => { if (!drawing) return; const p = pointOf(e); if (p) setDrag({ x0: p.x, y0: p.y, x1: p.x, y1: p.y }); };
  const move = (e) => { if (!drawing || !drag) return; e.preventDefault(); const p = pointOf(e); if (p) setDrag((d) => ({ ...d, x1: p.x, y1: p.y })); };
  const end = () => {
    if (!drag) return;
    const x = Math.min(drag.x0, drag.x1), y = Math.min(drag.y0, drag.y1);
    const w = Math.abs(drag.x1 - drag.x0), h = Math.abs(drag.y1 - drag.y0);
    setDrag(null);
    if (w < 0.02 || h < 0.02) return;
    setRect({ x, y, w, h });
    setDrawing(false);             // lock — a stray touch won't redraw, and the image scrolls
    haptic?.('ok');
  };
  const live = drag
    ? { x: Math.min(drag.x0, drag.x1), y: Math.min(drag.y0, drag.y1), w: Math.abs(drag.x1 - drag.x0), h: Math.abs(drag.y1 - drag.y0) }
    : rect;

  // ── fill path A: scene descriptions ──
  const submitScenes = async () => {
    const list = descs.split('\n').map((s) => s.trim()).filter(Boolean);
    if (!list.length || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/pinreview/scenes/create', { descriptions: list });
      setDescs('');
      setNote(`🎨 В очереди на генерацию: ${r.queued}. Появятся ниже через 1–2 минуты (жми «Обновить»).`);
      haptic?.('ok');
      await refresh();
    } catch (e) { setError(String(e?.message || e)); } finally { setBusy(false); }
  };

  // ── fill path B: upload own photos ──
  const onFiles = async (e) => {
    const files = Array.from(e.target.files || []);
    e.target.value = '';                      // allow re-picking the same file later
    if (!files.length) return;
    setBusy(true); setError('');
    let ok = 0;
    for (const f of files) {
      try {
        const dataUrl = await new Promise((res, rej) => {
          const fr = new FileReader();
          fr.onload = () => res(String(fr.result || ''));
          fr.onerror = rej;
          fr.readAsDataURL(f);
        });
        await api('/api/answer/pinreview/upload', { image_base64: dataUrl, mime: f.type || 'image/jpeg' });
        ok += 1;
      } catch (err) { setError(String(err?.message || err)); }
    }
    setBusy(false);
    if (ok) { setNote(`📷 Загружено картинок: ${ok}. Обводи их ниже.`); haptic?.('ok'); await refresh(); }
  };

  // ── targeting ──
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
      setRect(null); setDrawing(true); setWord('');
      setNote(r.duplicate ? `⚠️ «${r.target_label}» уже был — добавил всё равно.` : '');
      haptic?.('ok');
    } catch (e) { setError(String(e?.message || e)); haptic?.('bad'); } finally { setBusy(false); }
  };

  const finishScene = async (action) => {
    if (!scene || busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/scenedone', { scene_id: scene.scene_id, action });
      setScenes(scenes.filter((_, i) => i !== idx));
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
        <button className="pinrev-link" disabled={busy} onClick={refresh} style={{ marginLeft: 8 }}>обновить</button>
      </div>

      {/* Fill paths */}
      <div className={`pinrev-composer ${scene ? 'compact' : ''}`}>
        <div className="pinrev-comp-title">🎨 Заказать сцены{status.needed > 0 ? ` (нужно ещё ~${status.needed})` : ''}</div>
        <textarea
          className="pinrev-textarea" rows={6} value={descs}
          onChange={(e) => setDescs(e.target.value)}
          placeholder={'Опиши сцены по-русски, каждую с новой строки:\nрабочий стол с офисными мелочами\nдетская с игрушками на полу\nполка в гараже с инструментами'}
        />
        <button className="ans-btn" disabled={!descs.trim() || busy} onClick={submitScenes}>
          Сгенерировать сцены
        </button>
        <div className="pinrev-or">— или —</div>
        <input ref={fileRef} type="file" accept="image/*" multiple hidden onChange={onFiles} />
        <button className="ans-btn-ghost" disabled={busy} onClick={() => fileRef.current?.click()}>
          📷 Загрузить свои картинки
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
                другая →
              </button>
            ) : null}
          </div>
          <p className="pinrev-hint">
            {drawing
              ? '① Обведи предмет пальцем.'
              : '② Впиши слово с артиклем и нажми «Добавить». Картинку можно листать — рамка держится.'}
          </p>
          <div
            className="pinrev-wrap" ref={wrapRef}
            style={{ touchAction: drawing ? 'none' : 'pan-y', cursor: drawing ? 'crosshair' : 'default' }}
            onTouchStart={start} onTouchMove={move} onTouchEnd={end}
            onMouseDown={start} onMouseMove={move} onMouseUp={end} onMouseLeave={end}
          >
            <img className="pin-img" src={scene.image_url} alt="" draggable="false" />
            {live ? (
              <span className={`pinrev-box ${drag ? 'live' : ''}`}
                style={{ left: `${live.x * 100}%`, top: `${live.y * 100}%`, width: `${live.w * 100}%`, height: `${live.h * 100}%` }} />
            ) : null}
          </div>
          {!drawing ? (
            <button className="pinrev-link" onClick={() => { setRect(null); setDrawing(true); }}>✏️ перерисовать рамку</button>
          ) : null}
          <input
            className="pinrev-word" value={word} onChange={(e) => setWord(e.target.value)}
            placeholder="der Feuerlöscher" autoCapitalize="off" autoCorrect="off" enterKeyHint="done"
            onKeyDown={(e) => { if (e.key === 'Enter') { e.target.blur(); addTarget(); } }}
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
              : 'Готовых картинок пока нет — закажи сцены или загрузи свои выше.'}
          </div>
        </div>
      )}
      {error && !scene ? <div className="pinrev-err">{error}</div> : null}
      <button className="ans-btn-ghost" onClick={onClose}>Закрыть</button>
    </div>
  );
}
