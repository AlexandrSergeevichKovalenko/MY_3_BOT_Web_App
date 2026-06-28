import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/**
 * Zahlen-Diktat — number-dictation session. 3 short audio scenes; in each, a number
 * (phone / customer / code) is embedded in natural German speech and the learner
 * types it. Deterministic instant grade. Audio: max 2 plays before answering; after
 * grading the SAME clip replays as the reveal WITHOUT counting against the quota.
 *
 * Composes the ReviewSession session loop (loading|task|result|done) with a
 * ListeningGame-style custom <audio> player. Rendered by AnswerOverlay for kind="nd".
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() {
  try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ }
}

const MAX_PLAYS = 2;
const SPEEDS = [0.75, 1];

function fmtTime(s) {
  if (!Number.isFinite(s) || s < 0) return '0:00';
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${sec < 10 ? '0' : ''}${sec}`;
}

export function Root({ children }) {
  return <div className="ans-root"><div className="ans-card">{children}</div></div>;
}

/**
 * One scene's audio player. Before answering, plays are quota-limited (MAX_PLAYS,
 * persisted per item so a page reload can't farm extra listens). In `reveal` mode
 * (result screen) playback is unlimited and free.
 */
export function ScenePlayer({ src, quotaKey, reveal }) {
  const audioRef = useRef(null);
  const [playing, setPlaying] = useState(false);
  const [inClip, setInClip] = useState(false);   // a clip is active (consumed, may be paused mid-way)
  const [cur, setCur] = useState(0);
  const [dur, setDur] = useState(0);
  const [rate, setRate] = useState(1);
  const [playsLeft, setPlaysLeft] = useState(MAX_PLAYS);

  useEffect(() => {
    if (reveal || !quotaKey || typeof window === 'undefined') { setPlaysLeft(MAX_PLAYS); return; }
    try {
      const raw = window.localStorage.getItem(quotaKey);
      const stored = Number.parseInt(String(raw || '').trim(), 10);
      setPlaysLeft(Number.isFinite(stored) ? Math.max(0, Math.min(MAX_PLAYS, stored)) : MAX_PLAYS);
    } catch (_e) { setPlaysLeft(MAX_PLAYS); }
  }, [quotaKey, reveal]);

  const consume = useCallback(() => {
    if (reveal) return true;                 // reveal plays are free
    if (playsLeft <= 0) return false;
    const next = playsLeft - 1;
    setPlaysLeft(next);
    try { if (quotaKey) window.localStorage.setItem(quotaKey, String(next)); } catch (_e) { /* ignore */ }
    return true;
  }, [playsLeft, quotaKey, reveal]);

  // A "play" = one fresh listen from the start (costs quota). Pausing then resuming
  // the same clip is free; once it ends, the next listen is a fresh play.
  const start = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    if (!consume()) return;
    tapHaptic();
    setInClip(true);
    a.currentTime = 0;
    a.play().catch(() => {});
  }, [consume]);

  const toggle = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    if (!a.paused) { a.pause(); return; }     // pause
    if (inClip) { tapHaptic(); a.play().catch(() => {}); return; }  // resume (free)
    start();                                  // fresh play (costs quota)
  }, [start, inClip]);

  const setSpeed = useCallback((r) => {
    setRate(r);
    if (audioRef.current) audioRef.current.playbackRate = r;
    tapHaptic();
  }, []);

  // Auto-play once when the reveal screen mounts.
  useEffect(() => {
    if (reveal) { const t = setTimeout(start, 250); return () => clearTimeout(t); }
    return undefined;
  }, [reveal, start]);

  const locked = !reveal && !inClip && playsLeft <= 0;
  const pct = dur > 0 ? (cur / dur) * 100 : 0;

  if (!src) {
    return <div className="ls-player"><span className="ls-noaudio">🔇 Audio wird noch vorbereitet — versuche es gleich nochmal.</span></div>;
  }
  return (
    <>
      <audio
        ref={audioRef}
        src={src}
        preload="metadata"
        onLoadedMetadata={(e) => setDur(e.currentTarget.duration || 0)}
        onTimeUpdate={(e) => setCur(e.currentTarget.currentTime || 0)}
        onPlay={() => setPlaying(true)}
        onPause={() => setPlaying(false)}
        onEnded={() => { setPlaying(false); setInClip(false); }}
      />
      <div className="ls-player">
        <button className="ls-play" onClick={toggle} aria-label="Play" disabled={locked}>
          {playing ? '❚❚' : '▶'}
        </button>
        <div className="ls-player-main">
          <div className="ls-bar">
            <div className="ls-bar-fill" style={{ width: `${pct}%` }} />
            <div className="ls-bar-knob" style={{ left: `${pct}%` }} />
          </div>
          <div className="ls-row">
            <span className="ls-time">{fmtTime(cur)} / {fmtTime(dur)}</span>
            <span className="ls-speeds">
              {SPEEDS.map((r) => (
                <button key={r} className={`ls-speed${rate === r ? ' on' : ''}`} onClick={() => setSpeed(r)}>{r}×</button>
              ))}
            </span>
          </div>
        </div>
      </div>
      {!reveal ? (
        <div className="ans-sub" style={{ textAlign: 'center', marginTop: 2 }}>
          {playsLeft > 0 ? `Noch ${playsLeft}× anhören` : 'Keine Wiederholung mehr — tippe die Zahl'}
        </div>
      ) : null}
    </>
  );
}

export default function NumberDictationGame({ dispatchId, api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|task|result|done|error
  const [items, setItems] = useState([]);
  const [idx, setIdx] = useState(0);
  const [answer, setAnswer] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [correct, setCorrect] = useState(0);
  const startedAt = useRef(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/numdict/session', { dispatch_id: dispatchId });
        if (cancelled) return;
        if (!data.ok || !Array.isArray(data.items) || data.items.length === 0) {
          setError(data.error || 'Keine Aufgaben'); setPhase('error'); return;
        }
        setItems(data.items);
        // Resume past an already-answered prefix (anti-replay): jump to first open item.
        const firstOpen = data.items.findIndex((it) => !it.already);
        setCorrect(data.items.filter((it) => it.already && it.already.is_correct).length);
        if (firstOpen === -1) { setPhase('done'); return; }
        setIdx(firstOpen);
        startedAt.current = Date.now();
        setPhase('task');
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, dispatchId]);

  const cur = items[idx] || null;

  const submit = useCallback(async () => {
    if (submitting || !cur || !answer.trim()) return;
    setSubmitting(true);
    try {
      const data = await api('/api/answer/numdict/submit', {
        dispatch_id: dispatchId, item_index: cur.item_index,
        answer: answer.trim(), time_ms: Math.max(0, Date.now() - startedAt.current),
      });
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      setResult(data);
      if (data.is_correct) setCorrect((n) => n + 1);
      try { haptic?.(data.is_correct ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
      setPhase('result');
    } catch (e) {
      setError(String(e.message || e)); setPhase('error');
    } finally { setSubmitting(false); }
  }, [api, dispatchId, cur, answer, submitting, haptic]);

  const next = useCallback(() => {
    setResult(null);
    setAnswer('');
    if (idx + 1 >= items.length) { setPhase('done'); return; }
    setIdx(idx + 1);
    startedAt.current = Date.now();
    setPhase('task');
  }, [idx, items.length]);

  if (phase === 'loading') {
    return <Root><div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" /></Root>;
  }
  if (phase === 'error') {
    return (
      <Root>
        <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
        <p className="ans-sub">{error}</p>
        <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  if (phase === 'done') {
    const total = items.length;
    return (
      <Root>
        <div className="ans-verdict" style={{ fontSize: 44, textAlign: 'center' }}>{correct === total ? '🎉' : '✅'}</div>
        <div className="ans-verdict" style={{ textAlign: 'center' }}>Fertig!</div>
        <p className="ans-sub" style={{ textAlign: 'center' }}>
          Richtig: {correct} von {total}
        </p>
        <button className="ans-btn" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  if (phase === 'result' && result && cur) {
    const good = !!result.is_correct;
    const last = idx + 1 >= items.length;
    return (
      <Root>
        <div className="ans-head">
          <span className="ans-eyebrow">🔢 Zahlen-Diktat</span>
          <p className="ans-sub">{idx + 1} / {items.length}</p>
        </div>
        <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
          <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
          <div className="ans-answer"><b>{result.display_answer}</b></div>
          {!good && result.typed ? <div className="ans-meaning">du: {result.typed}</div> : null}
        </div>
        <div className="ans-sub" style={{ textAlign: 'center', marginTop: 6 }}>🔁 Hör nochmal genau hin:</div>
        <ScenePlayer src={cur.audio_url} quotaKey={null} reveal />
        <button className="ans-btn" onClick={next}>
          {last ? 'Fertig →' : `Weiter (${items.length - idx - 1}) →`}
        </button>
        <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  // phase === 'task'
  if (!cur) return <Root><div className="ans-skel" /></Root>;
  return (
    <Root>
      <div className="ans-head">
        <span className="ans-eyebrow">🔢 Zahlen-Diktat</span>
        <h1 className="ans-title">{cur.prompt_de}</h1>
        <p className="ans-sub">{cur.prompt_ru} · {idx + 1} / {items.length}</p>
      </div>
      <ScenePlayer
        key={`p${cur.item_index}`}
        src={cur.audio_url}
        quotaKey={`numdict_${dispatchId}_${cur.item_index}`}
        reveal={false}
      />
      <div className="ans-field" style={{ marginTop: 12 }}>
        <input
          className="ans-input"
          type="text"
          inputMode={cur.input_mode === 'alnum' ? 'text' : 'numeric'}
          autoComplete="off"
          autoCapitalize={cur.input_mode === 'alnum' ? 'characters' : 'off'}
          value={answer}
          onChange={(e) => setAnswer(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
          placeholder="Zahl eintippen …"
        />
      </div>
      <button className="ans-btn" disabled={!answer.trim() || submitting} onClick={submit}>
        {submitting ? 'Prüfe …' : 'Antwort prüfen ✓'}
      </button>
    </Root>
  );
}
