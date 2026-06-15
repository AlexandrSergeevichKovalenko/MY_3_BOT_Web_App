import React, { useCallback, useEffect, useState } from 'react';
import AufgabeGame from './AufgabeGame.jsx';

/**
 * "Работа над ошибками" — spaced-repetition review of the user's past mistakes.
 * Loops: GET next due mistake → render it with the SAME AufgabeGame inputs →
 * grade via /review/submit → show rule + tip → next. In-app UI stays German to
 * match the task screens; the result reuses the rich explanation (rule + tip).
 */

function Root({ children }) {
  return <div className="ans-root"><div className="ans-card">{children}</div></div>;
}

export default function ReviewSession({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|task|result|done|error
  const [mistakeId, setMistakeId] = useState(null);
  const [task, setTask] = useState(null);
  const [remaining, setRemaining] = useState(0);
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [reviewed, setReviewed] = useState(0);

  const loadNext = useCallback(async () => {
    setPhase('loading');
    setResult(null);
    try {
      const data = await api('/api/answer/review/next', {});
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      if (data.done) { setPhase('done'); return; }
      setMistakeId(data.mistake_id);
      setTask(data.task);
      setRemaining(data.remaining || 0);
      setPhase('task');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadNext(); }, [loadNext]);

  const submit = useCallback(async (answer) => {
    if (submitting || mistakeId == null) return;
    setSubmitting(true);
    try {
      const data = await api('/api/answer/review/submit', { mistake_id: mistakeId, answer: String(answer) });
      if (!data.ok || !data.result) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      setResult(data.result);
      setRemaining(data.remaining || 0);
      setReviewed((n) => n + 1);
      try { haptic?.(data.result.is_correct ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
      setPhase('result');
    } catch (e) {
      setError(String(e.message || e));
      setPhase('error');
    } finally {
      setSubmitting(false);
    }
  }, [api, mistakeId, submitting, haptic]);

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
    return (
      <Root>
        <div className="ans-verdict" style={{ fontSize: 44, textAlign: 'center' }}>{reviewed > 0 ? '🎉' : '✨'}</div>
        <div className="ans-verdict" style={{ textAlign: 'center' }}>
          {reviewed > 0 ? 'Alle Fehler wiederholt!' : 'Keine Fehler zur Wiederholung'}
        </div>
        <p className="ans-sub" style={{ textAlign: 'center' }}>
          {reviewed > 0
            ? `Du hast ${reviewed} ${reviewed === 1 ? 'Fehler' : 'Fehler'} wiederholt. Die nächsten kommen zur richtigen Zeit wieder 🔁`
            : 'Sobald du irgendwo einen Fehler machst, taucht er hier zur Wiederholung auf.'}
        </p>
        <button className="ans-btn" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  if (phase === 'result' && result) {
    const good = !!result.is_correct;
    return (
      <Root>
        <div className="ans-head"><span className="ans-eyebrow">🔁 Wiederholung</span></div>
        <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
          <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
          {result.correct_word ? <div className="ans-answer"><b>{result.correct_word}</b></div> : null}
          {!good && result.user_answer ? <div className="ans-meaning">du: {result.user_answer}</div> : null}
          {result.hint_ru ? <div className="ans-meaning">{result.hint_ru}</div> : null}
          {result.explanation ? <div className="ans-explain">{result.explanation}</div> : null}
          {result.tip ? <div className="ans-tip">💡 {result.tip}</div> : null}
        </div>
        <button className="ans-btn" onClick={loadNext}>
          {remaining > 0 ? `Weiter (${remaining}) →` : 'Fertig →'}
        </button>
        <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  // phase === 'task'
  return (
    <Root>
      <div className="ans-head">
        <span className="ans-eyebrow">🔁 Wiederholung</span>
        <h1 className="ans-title">Fehler wiederholen</h1>
        <p className="ans-sub">Noch {remaining} zu wiederholen</p>
      </div>
      {task ? <AufgabeGame task={task} onSubmit={submit} submitting={submitting} /> : null}
    </Root>
  );
}
