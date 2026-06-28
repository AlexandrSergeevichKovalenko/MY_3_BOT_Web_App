import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Root, ScenePlayer } from './NumberDictationGame.jsx';

/**
 * Zahlen-Diktat — SELF-PACED practice ("Числа на слух"). The endless sibling of the
 * scheduled number-dictation: open from the DM keyboard button → one audio exercise
 * after another, as many as you want. Same player/grade as NumberDictationGame, but
 * a dispatch-less loop (load one item → type → grade → reveal → "Weiter" → next),
 * pulling least-recently-seen items from the pre-generated pool. Free users hit a
 * daily cap (the backend returns a friendly capped screen); Pro is unlimited.
 * Rendered by AnswerOverlay for kind="np".
 */
export default function NumberDictationPractice({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|task|result|done|error
  const [numdictId, setNumdictId] = useState(null);
  const [task, setTask] = useState(null);        // { prompt_de, prompt_ru, input_mode, audio_url }
  const [answer, setAnswer] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [ending, setEnding] = useState(null);    // { capped?, empty?, message?, reset_at? }
  const [stats, setStats] = useState({ done: 0, correct: 0 });
  const startedAt = useRef(0);

  const loadNext = useCallback(async () => {
    setPhase('loading');
    setResult(null);
    setAnswer('');
    try {
      const data = await api('/api/answer/numdict/practice/next', {});
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      if (data.done) {
        setEnding({ capped: !!data.capped, empty: !!data.empty, message: data.message, reset_at: data.reset_at });
        setPhase('done');
        return;
      }
      setNumdictId(data.numdict_id);
      setTask({
        prompt_de: data.prompt_de || '',
        prompt_ru: data.prompt_ru || '',
        input_mode: data.input_mode || 'numeric',
        audio_url: data.audio_url || '',
      });
      startedAt.current = Date.now();
      setPhase('task');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadNext(); }, [loadNext]);

  const submit = useCallback(async () => {
    if (submitting || !task || numdictId == null || !answer.trim()) return;
    setSubmitting(true);
    try {
      const data = await api('/api/answer/numdict/practice/submit', {
        numdict_id: numdictId,
        answer: answer.trim(),
        time_ms: Math.max(0, Date.now() - startedAt.current),
      });
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      setResult(data);
      setStats((s) => ({ done: s.done + 1, correct: s.correct + (data.is_correct ? 1 : 0) }));
      try { haptic?.(data.is_correct ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
      setPhase('result');
    } catch (e) {
      setError(String(e.message || e)); setPhase('error');
    } finally { setSubmitting(false); }
  }, [api, task, numdictId, answer, submitting, haptic]);

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
    const capped = ending?.capped;
    return (
      <Root>
        <div className="ans-verdict" style={{ fontSize: 44, textAlign: 'center' }}>{capped ? '🔒' : '✨'}</div>
        <div className="ans-verdict" style={{ textAlign: 'center' }}>
          {capped ? 'На сегодня хватит!' : 'Пока нет заданий'}
        </div>
        <p className="ans-sub" style={{ textAlign: 'center' }}>
          {capped
            ? (ending?.message || 'Лимит на сегодня исчерпан. Premium снимает ограничение.')
            : 'Тренажёр готовит новые номера — загляни чуть позже 🔁'}
        </p>
        {stats.done > 0 ? (
          <p className="ans-sub" style={{ textAlign: 'center' }}>
            Сегодня: верно {stats.correct} из {stats.done}
          </p>
        ) : null}
        <button className="ans-btn" onClick={onClose}>Закрыть</button>
      </Root>
    );
  }
  if (phase === 'result' && result) {
    const good = !!result.is_correct;
    return (
      <Root>
        <div className="ans-head">
          <span className="ans-eyebrow">🔢 Zahlen üben</span>
          <p className="ans-sub">Верно {stats.correct} из {stats.done}</p>
        </div>
        <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
          <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
          <div className="ans-answer"><b>{result.display_answer}</b></div>
          {!good && result.typed ? <div className="ans-meaning">du: {result.typed}</div> : null}
        </div>
        <div className="ans-sub" style={{ textAlign: 'center', marginTop: 6 }}>🔁 Hör nochmal genau hin:</div>
        <ScenePlayer src={task?.audio_url} quotaKey={null} reveal />
        <button className="ans-btn" onClick={loadNext}>Weiter →</button>
        <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  // phase === 'task'
  if (!task) return <Root><div className="ans-skel" /></Root>;
  return (
    <Root>
      <div className="ans-head">
        <span className="ans-eyebrow">🔢 Zahlen üben</span>
        <h1 className="ans-title">{task.prompt_de}</h1>
        <p className="ans-sub">{task.prompt_ru}</p>
      </div>
      <ScenePlayer
        key={`p_${numdictId}`}
        src={task.audio_url}
        quotaKey={`numdict_practice_${numdictId}`}
        reveal={false}
      />
      <div className="ans-field" style={{ marginTop: 12 }}>
        <input
          className="ans-input"
          type="text"
          inputMode={task.input_mode === 'alnum' ? 'text' : 'numeric'}
          autoComplete="off"
          autoCapitalize={task.input_mode === 'alnum' ? 'characters' : 'off'}
          value={answer}
          onChange={(e) => setAnswer(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
          placeholder="Zahl eintippen …"
        />
      </div>
      <button className="ans-btn" disabled={!answer.trim() || submitting} onClick={submit}>
        {submitting ? 'Prüfe …' : 'Antwort prüfen ✓'}
      </button>
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </Root>
  );
}
