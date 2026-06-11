import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

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

export default function ListeningGame({ task, onSubmit, submitting }) {
  const audioRef = useRef(null);
  const questions = useMemo(() => task.questions || [], [task]);
  const [answers, setAnswers] = useState(() => questions.map(() => ''));
  const [playing, setPlaying] = useState(false);
  const [cur, setCur] = useState(0);
  const [dur, setDur] = useState(0);
  const [rate, setRate] = useState(1);

  useEffect(() => { setAnswers(questions.map(() => '')); }, [questions]);

  const toggle = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    tapHaptic();
    if (a.paused) a.play().catch(() => {}); else a.pause();
  }, []);

  const replay = useCallback(() => {
    const a = audioRef.current;
    if (!a) return;
    tapHaptic();
    a.currentTime = 0;
    a.play().catch(() => {});
  }, []);

  const setSpeed = useCallback((r) => {
    setRate(r);
    if (audioRef.current) audioRef.current.playbackRate = r;
    tapHaptic();
  }, []);

  const seek = useCallback((e) => {
    const a = audioRef.current;
    if (!a || !dur) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const pct = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
    a.currentTime = pct * dur;
    setCur(a.currentTime);
  }, [dur]);

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
        onEnded={() => setPlaying(false)}
      />

      <div className="ls-player">
        {hasAudio ? (
          <>
            <button className="ls-play" onClick={toggle} aria-label="Play">
              {playing ? '❚❚' : '▶'}
            </button>
            <div className="ls-player-main">
              <div className="ls-bar" onClick={seek}>
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
                      onClick={() => setSpeed(r)}
                    >{r}×</button>
                  ))}
                  <button className="ls-speed" onClick={replay} title="Wiederholen">⟳</button>
                </span>
              </div>
            </div>
          </>
        ) : (
          <span className="ls-noaudio">🔇 Audio wird noch vorbereitet — versuche es gleich nochmal.</span>
        )}
      </div>

      <div className="ls-questions">
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
