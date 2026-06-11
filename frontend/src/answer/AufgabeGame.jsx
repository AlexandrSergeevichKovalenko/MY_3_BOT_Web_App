import React, { useCallback, useEffect, useRef, useState } from 'react';

/**
 * B2+ text tasks ("Aufgabe"), all answered in place. Grading is a fast
 * deterministic check on the server (the generator pre-listed every accepted
 * answer at pool time — no runtime LLM). Formats:
 *  - cloze / wortbildung / transform → typed text (AufgabeText)
 *  - error  → tap the wrong word + type the fix (AufgabeError)
 *  - hoerluecke → listen + type the missing word (AufgabeHoer)
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() { try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ } }

function gapSentence(satz) {
  const parts = String(satz || '').split('_____');
  if (parts.length <= 1) return satz;
  return parts.flatMap((p, i) => (i === 0 ? [p] : [<span className="au-gap" key={i}>＿＿＿</span>, p]));
}

function PrüfenButton({ disabled, submitting, onClick }) {
  return (
    <button className="ans-btn" disabled={disabled || submitting} onClick={onClick}>
      {submitting ? 'Prüfe …' : 'Prüfen ✓'}
    </button>
  );
}

function AufgabeText({ task, onSubmit, submitting }) {
  const [value, setValue] = useState('');
  const fmt = task.format;
  const submit = () => { const v = value.trim(); if (v) onSubmit(v); };

  let body;
  let placeholder = 'fehlendes Wort …';
  if (fmt === 'transform') {
    placeholder = `2–5 Wörter mit „${task.schluesselwort || ''}“`;
    body = (
      <>
        <div className="au-orig">{task.original}</div>
        <div className="au-key">🔑 Schlüsselwort: <b>{task.schluesselwort}</b></div>
        <div className="au-satz">
          {task.target_prefix ? <>{task.target_prefix} </> : null}
          <span className="au-gap">＿＿＿</span>
          {task.target_suffix ? <> {task.target_suffix}</> : null}
        </div>
      </>
    );
  } else if (fmt === 'wortbildung') {
    body = (
      <>
        <div className="au-satz">{gapSentence(task.satz)}</div>
        <div className="au-key">🔧 Stamm: <b>{task.stamm}</b></div>
      </>
    );
  } else {
    body = <div className="au-satz">{gapSentence(task.satz)}</div>;
  }

  return (
    <>
      {body}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <input
        className="ans-input" value={value} onChange={(e) => setValue(e.target.value)}
        placeholder={placeholder} autoFocus autoCapitalize="off" autoCorrect="off" enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
      />
      <PrüfenButton disabled={!value.trim()} submitting={submitting} onClick={submit} />
    </>
  );
}

function AufgabeError({ task, onSubmit, submitting }) {
  const woerter = task.woerter || [];
  const [picked, setPicked] = useState(null);
  const [fix, setFix] = useState('');
  const ready = picked != null && fix.trim();
  const submit = () => { if (ready) onSubmit(`${picked}|${fix.trim()}`); };
  return (
    <>
      <p className="au-hint">Tippe das <b>falsche</b> Wort an und korrigiere es:</p>
      <div className="au-words">
        {woerter.map((w, i) => (
          <button
            key={i}
            className={`au-word${picked === i ? ' picked' : ''}`}
            onClick={() => { setPicked(i); tapHaptic(); }}
          >{w}</button>
        ))}
      </div>
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <input
        className="ans-input" value={fix} onChange={(e) => setFix(e.target.value)}
        placeholder={picked == null ? 'erst ein Wort antippen …' : 'richtige Form …'}
        autoCapitalize="off" autoCorrect="off" enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
      />
      <PrüfenButton disabled={!ready} submitting={submitting} onClick={submit} />
    </>
  );
}

function fmtTime(s) {
  if (!Number.isFinite(s) || s < 0) return '0:00';
  const m = Math.floor(s / 60), sec = Math.floor(s % 60);
  return `${m}:${sec < 10 ? '0' : ''}${sec}`;
}

function AufgabeHoer({ task, onSubmit, submitting }) {
  const audioRef = useRef(null);
  const [value, setValue] = useState('');
  const [playing, setPlaying] = useState(false);
  const [cur, setCur] = useState(0);
  const [dur, setDur] = useState(0);
  const hasAudio = !!task.audio_url;
  const toggle = useCallback(() => {
    const a = audioRef.current; if (!a) return; tapHaptic();
    if (a.paused) a.play().catch(() => {}); else a.pause();
  }, []);
  const seek = useCallback((e) => {
    const a = audioRef.current; if (!a || !dur) return;
    const r = e.currentTarget.getBoundingClientRect();
    a.currentTime = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width)) * dur;
  }, [dur]);
  const submit = () => { const v = value.trim(); if (v) onSubmit(v); };
  const pct = dur > 0 ? (cur / dur) * 100 : 0;
  return (
    <>
      <audio ref={audioRef} src={task.audio_url || undefined} preload="metadata"
        onLoadedMetadata={(e) => setDur(e.currentTarget.duration || 0)}
        onTimeUpdate={(e) => setCur(e.currentTarget.currentTime || 0)}
        onPlay={() => setPlaying(true)} onPause={() => setPlaying(false)} onEnded={() => setPlaying(false)} />
      <div className="ls-player">
        {hasAudio ? (
          <>
            <button className="ls-play" onClick={toggle} aria-label="Play">{playing ? '❚❚' : '▶'}</button>
            <div className="ls-player-main">
              <div className="ls-bar" onClick={seek}>
                <div className="ls-bar-fill" style={{ width: `${pct}%` }} />
                <div className="ls-bar-knob" style={{ left: `${pct}%` }} />
              </div>
              <div className="ls-row"><span className="ls-time">{fmtTime(cur)} / {fmtTime(dur)}</span></div>
            </div>
          </>
        ) : <span className="ls-noaudio">🔇 Audio wird vorbereitet — gleich nochmal.</span>}
      </div>
      <div className="au-satz">{gapSentence(task.satz_luecke)}</div>
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <input
        className="ans-input" value={value} onChange={(e) => setValue(e.target.value)}
        placeholder="gehörtes Wort …" autoCapitalize="off" autoCorrect="off" enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
      />
      <PrüfenButton disabled={!value.trim()} submitting={submitting} onClick={submit} />
    </>
  );
}

function AufgabePin({ task, onSubmit, submitting }) {
  const [tap, setTap] = useState(null); // normalized {x,y}
  const hasImg = !!task.image_url;
  const onImgClick = (e) => {
    const r = e.currentTarget.getBoundingClientRect();
    const x = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width));
    const y = Math.min(1, Math.max(0, (e.clientY - r.top) / r.height));
    setTap({ x, y });
    tapHaptic();
  };
  const submit = () => { if (tap) onSubmit(`${tap.x.toFixed(4)},${tap.y.toFixed(4)}`); };
  return (
    <>
      <p className="au-question">{task.question_de}</p>
      {hasImg ? (
        <div className="pin-wrap" onClick={onImgClick}>
          <img className="pin-img" src={task.image_url} alt="" draggable="false" />
          {tap ? <span className="pin-marker" style={{ left: `${tap.x * 100}%`, top: `${tap.y * 100}%` }} /> : null}
        </div>
      ) : <div className="au-orig">🖼 Bild wird vorbereitet — gleich nochmal.</div>}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <PrüfenButton disabled={!tap} submitting={submitting} onClick={submit} />
    </>
  );
}

export default function AufgabeGame({ task, onSubmit, submitting }) {
  const fmt = task.format || 'cloze';
  if (fmt === 'error') return <AufgabeError task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'hoerluecke') return <AufgabeHoer task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'pin') return <AufgabePin task={task} onSubmit={onSubmit} submitting={submitting} />;
  return <AufgabeText task={task} onSubmit={onSubmit} submitting={submitting} />;
}
