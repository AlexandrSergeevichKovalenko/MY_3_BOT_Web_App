import React, { useCallback, useEffect, useRef, useState } from 'react';

// Floating, draggable "ask the model" window used on every interactive's result.
// No backdrop — it floats over the task (still visible). Drag by the header to any
// spot. Chat thread (iMessage-style): your question, then the model's answer below.
// `api(path, body)` posts JSON (initData injected by the host); `context` is a short
// description of the current task so the answer is on-point.
export default function AskOverlay({ api, context = '', onClose }) {
  const [messages, setMessages] = useState([]); // {role:'user'|'bot', text}
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const [pos, setPos] = useState(null); // {x, y} top-left; null until measured
  const panelRef = useRef(null);
  const threadRef = useRef(null);
  const drag = useRef(null);

  // Initial position: lower-centre, so the task stays visible above the window.
  useEffect(() => {
    const el = panelRef.current;
    if (!el) return;
    const w = el.offsetWidth || 320;
    const h = el.offsetHeight || 320;
    const vw = window.innerWidth || 360;
    const vh = window.innerHeight || 640;
    setPos({ x: Math.max(8, (vw - w) / 2), y: Math.max(8, vh - h - 24) });
  }, []);

  const clamp = useCallback((x, y) => {
    const el = panelRef.current;
    const w = el ? el.offsetWidth : 320;
    const h = el ? el.offsetHeight : 320;
    const vw = window.innerWidth || 360;
    const vh = window.innerHeight || 640;
    return {
      x: Math.min(Math.max(6, x), Math.max(6, vw - w - 6)),
      y: Math.min(Math.max(6, y), Math.max(6, vh - h - 6)),
    };
  }, []);

  const onHeaderPointerDown = useCallback((e) => {
    const start = pos || { x: 0, y: 0 };
    drag.current = { sx: e.clientX, sy: e.clientY, ox: start.x, oy: start.y };
    try { e.target.setPointerCapture?.(e.pointerId); } catch (_e) { /* ignore */ }
  }, [pos]);

  const onHeaderPointerMove = useCallback((e) => {
    if (!drag.current) return;
    const dx = e.clientX - drag.current.sx;
    const dy = e.clientY - drag.current.sy;
    setPos(clamp(drag.current.ox + dx, drag.current.oy + dy));
  }, [clamp]);

  const onHeaderPointerUp = useCallback((e) => {
    drag.current = null;
    try { e.target.releasePointerCapture?.(e.pointerId); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => {
    const t = threadRef.current;
    if (t) t.scrollTop = t.scrollHeight;
  }, [messages, busy]);

  const ask = useCallback(async () => {
    const q = input.trim();
    if (!q || busy) return;
    const history = [];
    for (let i = 0; i < messages.length - 1; i += 1) {
      if (messages[i].role === 'user' && messages[i + 1] && messages[i + 1].role === 'bot') {
        history.push({ q: messages[i].text, a: messages[i + 1].text });
      }
    }
    setMessages((m) => [...m, { role: 'user', text: q }]);
    setInput('');
    setBusy(true);
    setErr('');
    try {
      const res = await api('/api/webapp/ask', { context, learner_question: q, history });
      if (res && res.ok && res.answer) {
        setMessages((m) => [...m, { role: 'bot', text: String(res.answer) }]);
      } else {
        setErr((res && (res.error || res.message)) || 'Не удалось получить ответ');
      }
    } catch (e) {
      setErr(String((e && e.message) || e));
    } finally {
      setBusy(false);
    }
  }, [input, busy, messages, api, context]);

  const style = pos ? { left: `${pos.x}px`, top: `${pos.y}px` } : { left: '50%', top: '60%', visibility: 'hidden' };

  return (
    <div className="ask-pop" ref={panelRef} style={style}>
      <div
        className="ask-pop-head"
        onPointerDown={onHeaderPointerDown}
        onPointerMove={onHeaderPointerMove}
        onPointerUp={onHeaderPointerUp}
        onPointerCancel={onHeaderPointerUp}
      >
        <span className="ask-pop-grip">⋮⋮</span>
        <span className="ask-pop-title">Спросить</span>
        <button type="button" className="ask-pop-close" onClick={onClose} aria-label="Закрыть">✕</button>
      </div>
      <div className="ask-pop-thread" ref={threadRef}>
        {messages.length === 0 ? (
          <div className="ask-pop-hint">Задай любой вопрос по этому заданию — отвечу здесь.</div>
        ) : null}
        {messages.map((m, i) => (
          <div key={i} className={`ask-bubble ${m.role === 'user' ? 'me' : 'bot'}`}>{m.text}</div>
        ))}
        {busy ? <div className="ask-bubble bot ask-typing">…</div> : null}
        {err ? <div className="ask-pop-err">{err}</div> : null}
      </div>
      <div className="ask-pop-input">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ваш вопрос…"
          rows={2}
          autoCapitalize="sentences"
          onKeyDown={(e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) ask(); }}
        />
        <button type="button" className="ask-pop-send" onClick={ask} disabled={busy || !input.trim()}>
          {busy ? '…' : 'Спросить'}
        </button>
      </div>
    </div>
  );
}
