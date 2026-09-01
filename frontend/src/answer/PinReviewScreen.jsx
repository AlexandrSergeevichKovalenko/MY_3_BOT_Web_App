import React, { useCallback, useEffect, useRef, useState } from 'react';
import { pointerFraction } from './pointerBox.js';

/**
 * «Finde im Bild» studio (admin) — a two-step wizard, one screen per step, no page scroll.
 *
 *   Step 1 «Ввод»    — either write scene prompts OR upload your own photos → «Далее».
 *   Step 2 «Обводка» — one image at a time: draw a box, type the German word, add it.
 *                      Chips are tappable (highlight the box) and deletable. «← Назад».
 */
export default function PinReviewScreen({ api, haptic, onClose }) {
  const [step, setStep] = useState('compose');       // 'compose' | 'target'
  const [status, setStatus] = useState(null);
  const [scenes, setScenes] = useState(null);
  const [idx, setIdx] = useState(0);
  const [descs, setDescs] = useState('');
  const [rect, setRect] = useState(null);
  const [drag, setDrag] = useState(null);
  const [drawing, setDrawing] = useState(true);
  const [word, setWord] = useState('');
  const [preview, setPreview] = useState(null);
  const [busy, setBusy] = useState(false);
  const [typing, setTyping] = useState(false);       // textarea focused → collapse chrome
  const [error, setError] = useState('');
  const [note, setNote] = useState('');
  const wrapRef = useRef(null);
  const fileRef = useRef(null);
  const taRef = useRef(null);

  const refresh = useCallback(async () => {
    try {
      const [st, sc] = await Promise.all([
        api('/api/answer/pinreview/status', {}),
        api('/api/answer/pinreview/scenes', {}),
      ]);
      setStatus(st);
      setScenes(sc.scenes || []);
      return sc.scenes || [];
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setScenes([]); return []; }
  }, [api]);

  useEffect(() => { refresh(); }, [refresh]);

  // While scenes are still rendering, poll so ready images appear on their own — no
  // manual button-hunting. Stops once nothing is generating.
  useEffect(() => {
    if (step !== 'compose' || !status || status.generating <= 0) return undefined;
    const id = setInterval(() => { refresh(); }, 10000);
    return () => clearInterval(id);
  }, [step, status, refresh]);

  const scene = scenes && scenes[idx];
  useEffect(() => { setRect(null); setDrag(null); setDrawing(true); setWord(''); setPreview(null); setError(''); }, [scene]);

  // ── drawing ──
  const pointOf = useCallback((e) => {
    const el = wrapRef.current;
    if (!el) return null;
    const t = e.touches?.[0] || e.changedTouches?.[0] || e;
    // pointerBox: карточка масштабируется через `zoom`, а WebKit отдаёт координаты блока
    // без его учёта — рамка вставала бы не там, где её ведут пальцем (см. pointerBox.js).
    return pointerFraction(el, t.clientX, t.clientY);
  }, []);
  const start = (e) => { if (!drawing) return; setPreview(null); const p = pointOf(e); if (p) setDrag({ x0: p.x, y0: p.y, x1: p.x, y1: p.y }); };
  const move = (e) => { if (!drawing || !drag) return; e.preventDefault(); const p = pointOf(e); if (p) setDrag((d) => ({ ...d, x1: p.x, y1: p.y })); };
  const end = () => {
    if (!drag) return;
    const x = Math.min(drag.x0, drag.x1), y = Math.min(drag.y0, drag.y1);
    const w = Math.abs(drag.x1 - drag.x0), h = Math.abs(drag.y1 - drag.y0);
    setDrag(null);
    if (w < 0.02 || h < 0.02) return;
    setRect({ x, y, w, h }); setDrawing(false); haptic?.('ok');
  };
  const live = drag
    ? { x: Math.min(drag.x0, drag.x1), y: Math.min(drag.y0, drag.y1), w: Math.abs(drag.x1 - drag.x0), h: Math.abs(drag.y1 - drag.y0) }
    : rect;

  // ── step 1 actions ──
  const submitScenes = async () => {
    const list = descs.split('\n').map((s) => s.trim()).filter(Boolean);
    if (!list.length || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/pinreview/scenes/create', { descriptions: list });
      setDescs('');
      setNote(`🎨 В очереди на генерацию: ${r.queued}. Готовы будут через 1–2 минуты — жми «К обводке».`);
      haptic?.('ok');
      await refresh();
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); } finally { setBusy(false); }
  };
  const onFiles = async (e) => {
    const files = Array.from(e.target.files || []);
    e.target.value = '';
    if (!files.length) return;
    setBusy(true); setError('');
    let ok = 0;
    for (const f of files) {
      try {
        const dataUrl = await new Promise((res, rej) => {
          const fr = new FileReader(); fr.onload = () => res(String(fr.result || '')); fr.onerror = rej; fr.readAsDataURL(f);
        });
        await api('/api/answer/pinreview/upload', { image_base64: dataUrl, mime: f.type || 'image/jpeg' });
        ok += 1;
      } catch (err) { setError((console.warn('[game] error', err), 'Не удалось загрузить. Попробуйте позже.')); }
    }
    setBusy(false);
    if (ok) { haptic?.('ok'); const sc = await refresh(); setIdx(0); if (sc.length) setStep('target'); }
  };
  const goTarget = async () => { const sc = await refresh(); setIdx(0); if (sc.length) setStep('target'); else setNote('Готовых картинок пока нет — сгенерируй или загрузи.'); };

  // ── step 2 actions ──
  const addTarget = async () => {
    if (!scene || !live || busy) return;
    const w = word.trim();
    if (!/^(der|die|das)\s+\S/i.test(w)) { setError('Слово с артиклем: der/die/das …'); return; }
    setBusy(true); setError('');
    try {
      const box = [live.x, live.y, live.w, live.h];
      const r = await api('/api/answer/pinreview/addtarget', { scene_id: scene.scene_id, bbox: box, word: w });
      const chip = { aufgabe_id: r.aufgabe_id, label: r.target_label, bbox: box };
      setScenes((prev) => prev.map((s, i) => (i === idx ? { ...s, targets: [...(s.targets || []), chip] } : s)));
      setRect(null); setDrawing(true); setWord(''); setPreview(null);
      setNote(r.duplicate ? `⚠️ «${r.target_label}» уже был — добавил всё равно.` : '');
      haptic?.('ok');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); haptic?.('bad'); } finally { setBusy(false); }
  };
  const delTarget = async (t) => {
    if (busy || !t?.aufgabe_id) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/deltarget', { aufgabe_id: t.aufgabe_id });
      setScenes((prev) => prev.map((s, i) => (i === idx ? { ...s, targets: (s.targets || []).filter((x) => x.aufgabe_id !== t.aufgabe_id) } : s)));
      if (preview?.id === t.aufgabe_id) setPreview(null);
      haptic?.('ok');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); } finally { setBusy(false); }
  };
  const finishScene = async (action) => {
    if (!scene || busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/pinreview/scenedone', { scene_id: scene.scene_id, action });
      const rest = scenes.filter((_, i) => i !== idx);
      setScenes(rest); setIdx(0);
      if (!rest.length) { setStep('compose'); await refresh(); }
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); } finally { setBusy(false); }
  };

  if (scenes === null || status === null) return <div className="pinw"><div className="ans-loading">Загружаю студию…</div></div>;
  const readyCount = scenes.length;

  // ─────────────────────────── STEP 1: COMPOSE ───────────────────────────
  if (step === 'compose') {
    return (
      <div className={`pinw ${typing ? 'typing' : ''}`}>
        <div className="pinw-top">
          <div className="pinw-title">🖼 Студия «Найди предмет»</div>
          <div className="pinw-sub">Готово {status.approved}/{status.target} · шаг 1 из 2 — задай картинки</div>
        </div>
        <div className="pinw-body">
          <label className="pinw-label">Опиши сцены по-русски (каждую с новой строки):</label>
          <textarea
            ref={taRef} className="pinw-textarea" value={descs} onChange={(e) => setDescs(e.target.value)}
            onFocus={() => setTyping(true)} onBlur={() => setTyping(false)}
            placeholder={'рабочий стол с офисными мелочами\nдетская с игрушками на полу\nполка в гараже с инструментами'}
          />
          {note ? <div className="pinrev-note">{note}</div> : null}
          {error ? <div className="pinrev-err">{error}</div> : null}
        </div>
        <div className="pinw-bar">
          {typing ? (
            <button className="ans-btn pinw-done pinw-typing-keep" onMouseDown={(e) => e.preventDefault()}
              onClick={() => taRef.current?.blur()}>Готово ✓ — свернуть клавиатуру</button>
          ) : null}
          <button className="ans-btn pinw-gen" disabled={!descs.trim() || busy} onClick={submitScenes}>🎨 Сгенерировать сцены</button>
          <input ref={fileRef} type="file" accept="image/*" multiple hidden onChange={onFiles} />
          <button className="ans-btn-ghost pinw-upload" disabled={busy} onClick={() => fileRef.current?.click()}>📷 Или загрузить свои фото</button>
          <button className="ans-btn pinw-next" disabled={busy} onClick={goTarget}>
            {readyCount > 0 ? `К обводке: ${readyCount} картинок →`
              : (status.generating > 0 ? `🔄 Генерируется: ${status.generating} — проверить` : '🔄 Обновить')}
          </button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  // ─────────────────────────── STEP 2: TARGET ───────────────────────────
  return (
    <div className="pinw">
      <div className="pinw-top pinw-top-row">
        <button className="pinw-back" onClick={() => setStep('compose')}>← Назад</button>
        <span className="pinw-count">{scene ? `Картинка ${idx + 1} / ${scenes.length}` : 'Картинок нет'}</span>
        {scenes.length > 1 ? <button className="pinw-back" disabled={busy} onClick={() => setIdx((idx + 1) % scenes.length)}>другая →</button> : <span />}
      </div>

      {scene ? (
        <>
          <div className="pinw-imgarea">
            <div
              className="pinw-imgwrap" ref={wrapRef}
              style={{ touchAction: drawing ? 'none' : 'pan-y', cursor: drawing ? 'crosshair' : 'default' }}
              onTouchStart={start} onTouchMove={move} onTouchEnd={end}
              onMouseDown={start} onMouseMove={move} onMouseUp={end} onMouseLeave={end}
            >
              <img className="pinw-img" src={scene.image_url} alt="" draggable="false" />
              {live ? <span className={`pinrev-box ${drag ? 'live' : ''}`} style={{ left: `${live.x * 100}%`, top: `${live.y * 100}%`, width: `${live.w * 100}%`, height: `${live.h * 100}%` }} /> : null}
              {preview?.bbox ? <span className="pinrev-box preview" style={{ left: `${preview.bbox[0] * 100}%`, top: `${preview.bbox[1] * 100}%`, width: `${preview.bbox[2] * 100}%`, height: `${preview.bbox[3] * 100}%` }} /> : null}
            </div>
          </div>

          <div className="pinw-controls">
            <div className="pinw-hint">{drawing ? '① Обведи предмет пальцем' : '② Впиши слово и добавь. Рамка держится — картинку можно листать.'}</div>
            <div className="pinw-inputrow">
              <input className="pinrev-word" value={word} onChange={(e) => setWord(e.target.value)} placeholder="der Feuerlöscher"
                autoCapitalize="off" autoCorrect="off" enterKeyHint="done"
                onKeyDown={(e) => { if (e.key === 'Enter') { e.target.blur(); addTarget(); } }} />
              <button className="ans-btn pinw-add" disabled={!live || !word.trim() || busy} onClick={addTarget}>➕</button>
            </div>
            {!drawing ? <button className="pinrev-link" onClick={() => { setRect(null); setDrawing(true); setPreview(null); }}>✏️ перерисовать рамку</button> : null}
            {(scene.targets || []).length ? (
              <div className="pinw-chips">
                {scene.targets.map((t) => {
                  const on = preview?.id === t.aufgabe_id;
                  return (
                    <span className={`pinrev-chip ${on ? 'active' : ''}`} key={t.aufgabe_id}>
                      <button className="pinrev-chip-label" onClick={() => setPreview(on ? null : { id: t.aufgabe_id, bbox: t.bbox, label: t.label })}>{on ? '🟠' : '✅'} {t.label}</button>
                      <button className="pinrev-chip-x" disabled={busy} onClick={() => delTarget(t)} aria-label="удалить">✕</button>
                    </span>
                  );
                })}
              </div>
            ) : null}
            {note ? <div className="pinrev-note">{note}</div> : null}
            {error ? <div className="pinrev-err">{error}</div> : null}
          </div>

          <div className="pinw-bar pinw-bar-row">
            <button className="ans-btn-ghost pinrev-skip" disabled={busy} onClick={() => finishScene('skip')}>🗑 Плохая</button>
            <button className="ans-btn pinw-next" disabled={busy} onClick={() => finishScene('done')}>Готово с картинкой →</button>
          </div>
        </>
      ) : (
        <div className="pinw-body">
          <div className="pinrev-empty-sub">{status.generating > 0 ? `⏳ Генерируется: ${status.generating}. Вернись через минуту.` : 'Готовых картинок нет.'}</div>
          <button className="ans-btn-ghost" onClick={() => setStep('compose')}>← К вводу</button>
        </div>
      )}
    </div>
  );
}
