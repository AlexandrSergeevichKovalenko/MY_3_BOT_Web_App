import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { pointerFraction } from './pointerBox.js';

/**
 * Hörverständnis game: a custom audio player (streams the R2 MP3) + one answer
 * field per question. Self-contained input component — calls onSubmit(answers).
 * Rendered by AnswerOverlay for kind="ls"; the overlay polls for the LLM grade.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() {
  try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ }
}

function fmtTime(s) {
  if (!Number.isFinite(s) || s < 0) return '0:00';
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${sec < 10 ? '0' : ''}${sec}`;
}

const SPEEDS = [0.75, 1, 1.25];
const MAX_PLAYBACK_USES = 2;

function getPlaybackQuotaKey(task) {
  const dispatchId = task?.dispatch_id ?? task?.id ?? task?.dispatchId ?? '';
  return dispatchId ? `ls_playback_uses_left:${dispatchId}` : '';
}

export default function ListeningGame({ task, onSubmit, submitting }) {
  const audioRef = useRef(null);
  const sessionConsumedRef = useRef(false);
  const playbackKey = useMemo(() => getPlaybackQuotaKey(task), [task]);
  const questions = useMemo(() => task.questions || [], [task]);
  const [answers, setAnswers] = useState(() => questions.map(() => ''));
  const [playing, setPlaying] = useState(false);
  const [cur, setCur] = useState(0);
  const [dur, setDur] = useState(0);
  const [rate, setRate] = useState(1);
  const [playsLeft, setPlaysLeft] = useState(MAX_PLAYBACK_USES);

  useEffect(() => { setAnswers(questions.map(() => '')); }, [questions]);
  useEffect(() => {
    sessionConsumedRef.current = false;
    setCur(0);
    setDur(0);
    setPlaying(false);
  }, [playbackKey]);
  useEffect(() => {
    if (!playbackKey || typeof window === 'undefined') {
      setPlaysLeft(MAX_PLAYBACK_USES);
      return;
    }
    try {
      const raw = window.localStorage.getItem(playbackKey);
      const stored = Number.parseInt(String(raw || '').trim(), 10);
      setPlaysLeft(Number.isFinite(stored) ? Math.max(0, Math.min(MAX_PLAYBACK_USES, stored)) : MAX_PLAYBACK_USES);
    } catch (_e) {
      setPlaysLeft(MAX_PLAYBACK_USES);
    }
  }, [playbackKey]);
  useEffect(() => {
    if (!playbackKey || typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(playbackKey, String(playsLeft));
    } catch (_e) { /* ignore */ }
  }, [playbackKey, playsLeft]);

  const consumePlayback = useCallback(() => {
    setPlaysLeft((prev) => {
      const next = Math.max(0, prev - 1);
      if (next < prev) sessionConsumedRef.current = true;
      return next;
    });
  }, []);

  const canStartPlayback = playsLeft > 0 || sessionConsumedRef.current;

  const toggle = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    if (!canStartPlayback && a.paused) return;
    tapHaptic();
    if (a.paused) {
      if (!sessionConsumedRef.current) {
        if (playsLeft <= 0) return;
        consumePlayback();
      }
      a.play().catch(() => {});
    } else {
      a.pause();
    }
  }, [canStartPlayback, consumePlayback, playsLeft]);

  const replay = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    if (playsLeft <= 0) return;
    tapHaptic();
    sessionConsumedRef.current = false;
    consumePlayback();
    a.currentTime = 0;
    a.play().catch(() => {});
  }, [consumePlayback, playsLeft]);

  const setSpeed = useCallback((r) => {
    setRate(r);
    if (audioRef.current) audioRef.current.playbackRate = r;
    tapHaptic();
  }, []);

  const seek = useCallback((e) => {
    const a = audioRef.current;
    if (!a || !dur || playsLeft <= 0) return;
    // pointerBox: карточку подгонка масштабирует через `zoom`, и в WebKit координаты
    // блока приходят без его учёта — иначе перемотка встаёт мимо места нажатия.
    const pct = pointerFraction(e.currentTarget, e.clientX, e.clientY).x;
    a.currentTime = pct * dur;
    setCur(a.currentTime);
  }, [dur, playsLeft]);

  const allAnswered = answers.length > 0 && answers.every((s) => s.trim().length > 0);
  const pct = dur > 0 ? (cur / dur) * 100 : 0;
  const hasAudio = !!task.audio_url;

  return (
    <>
      <audio
        ref={audioRef}
        src={task.audio_url || undefined}
        preload="metadata"
        onLoadedMetadata={(e) => setDur(e.currentTarget.duration || 0)}
        onTimeUpdate={(e) => setCur(e.currentTarget.currentTime || 0)}
        onPlay={() => setPlaying(true)}
        onPause={() => setPlaying(false)}
        onEnded={() => {
          setPlaying(false);
          sessionConsumedRef.current = false;
        }}
      />

      <div className="ls-player">
        {hasAudio ? (
          <>
            <button className="ls-play" onClick={toggle} aria-label="Play" disabled={playsLeft <= 0}>
              {playing ? '❚❚' : '▶'}
            </button>
            <div className="ls-player-main">
              <div className={`ls-bar${playsLeft <= 0 ? ' is-locked' : ''}`} onClick={seek}>
                <div className="ls-bar-fill" style={{ width: `${pct}%` }} />
                <div className="ls-bar-knob" style={{ left: `${pct}%` }} />
              </div>
              <div className="ls-row">
                <span className="ls-time">{fmtTime(cur)} / {fmtTime(dur)}</span>
                <span className="ls-speeds">
                  {SPEEDS.map((r) => (
                    <button
                      key={r}
                      className={`ls-speed${rate === r ? ' on' : ''}`}
                      disabled={playsLeft <= 0}
                      onClick={() => setSpeed(r)}
                    >{r}×</button>
                  ))}
                  <button className="ls-speed" onClick={replay} title="Wiederholen" disabled={playsLeft <= 0}>⟳</button>
                </span>
              </div>
            </div>
          </>
        ) : (
          <span className="ls-noaudio">🔇 Audio wird noch vorbereitet — versuche es gleich nochmal.</span>
        )}
      </div>

      <div className="ls-questions ans-body">
        {questions.map((q, i) => (
          <div className="ans-field" key={q.number || i}>
            <span className="ans-field-label"><span className="ans-num">{q.number || i + 1}</span> {q.question_de}</span>
            <textarea
              className="ans-input ls-textarea"
              rows={2}
              value={answers[i] || ''}
              onChange={(e) => setAnswers((prev) => {
                const next = [...prev]; next[i] = e.target.value; return next;
              })}
              placeholder="deine Antwort …"
            />
          </div>
        ))}
      </div>

      <button
        className="ans-btn"
        disabled={!allAnswered || submitting}
        onClick={() => onSubmit(answers.map((s) => s.trim()))}
      >
        {submitting ? 'Sende …' : 'Antworten prüfen ✓'}
      </button>
    </>
  );
}
