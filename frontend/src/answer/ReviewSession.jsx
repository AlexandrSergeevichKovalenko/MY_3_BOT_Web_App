import React, { useCallback, useEffect, useState } from 'react';
import AufgabeGame from './AufgabeGame.jsx';
import ArtikelReviewGame from './ArtikelReviewGame.jsx';
import WoFrageReviewGame from './WoFrageReviewGame.jsx';

/**
 * "Работа над ошибками" — spaced-repetition review of the user's past mistakes.
 * Starts on a SECTION PICKER (overview): a dedicated "Artikel" block (der/die/das)
 * separate from the grammar drills, each with its due-count. Picking a section drills
 * ONLY that family; "Alle" drills everything in due order. Inside a section it loops:
 * GET next due mistake → render it with the SAME AufgabeGame inputs → grade via
 * /review/submit (identical grading to the live task) → show rule + tip → next.
 */

function Root({ children }) {
  return <div className="ans-root"><div className="ans-card">{children}</div></div>;
}

export default function ReviewSession({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('overview'); // overview|loading|task|result|done|error
  const [overview, setOverview] = useState(null);  // { sections, total }
  const [family, setFamily] = useState(null);      // active section: 'artikel'|'grammar'|null(all)
  const [mistakeId, setMistakeId] = useState(null);
  const [task, setTask] = useState(null);
  const [remaining, setRemaining] = useState(0);
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [reviewed, setReviewed] = useState(0);

  const loadOverview = useCallback(async () => {
    setPhase('overview');
    setOverview(null);
    setResult(null);
    setFamily(null);
    try {
      const data = await api('/api/answer/review/overview', {});
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      const ov = { sections: data.sections || [], total: data.total || 0 };
      setOverview(ov);
      if (ov.total <= 0) { setPhase('done'); return; }
      setPhase('overview');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadOverview(); }, [loadOverview]);

  // Fetch the next due item for the currently-selected section (fam === null → all).
  const loadNext = useCallback(async (fam) => {
    setPhase('loading');
    setResult(null);
    try {
      const data = await api('/api/answer/review/next', fam ? { family: fam } : {});
      if (!data.ok) { setError(data.error || 'Fehler'); setPhase('error'); return; }
      if (data.done) { await loadOverview(); return; }
      setMistakeId(data.mistake_id);
      setTask(data.task);
      setRemaining(data.remaining || 0);
      setPhase('task');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [api, loadOverview]);

  const startSection = useCallback((fam) => {
    setFamily(fam || null);
    // Artikel / Wo-Fragen → the fast, template-styled card flow (prefetch + local grade).
    if (fam === 'artikel') { setPhase('artikel'); return; }
    if (fam === 'wofrage') { setPhase('wofrage'); return; }
    loadNext(fam || null);
  }, [loadNext]);

  const submit = useCallback(async (answer) => {
    if (submitting || mistakeId == null) return;
    setSubmitting(true);
    try {
      const data = await api('/api/answer/review/submit', {
        mistake_id: mistakeId, answer: String(answer),
        ...(family ? { family } : {}),
      });
      // Video card ("тебе было сложно"): no grade/result screen — consume + go next.
      if (data.ok && data.video_done) {
        const rem = data.remaining || 0;
        setRemaining(rem);
        setReviewed((n) => n + 1);
        if (rem > 0) { loadNext(family); } else { loadOverview(); }
        return;
      }
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
  }, [api, mistakeId, submitting, haptic, family, loadNext, loadOverview]);

  if (phase === 'overview' && !overview) {
    return <Root><div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" /></Root>;
  }
  if (phase === 'overview' && overview) {
    const many = (overview.sections || []).length > 1;
    return (
      <Root>
        <div className="ans-head">
          <span className="ans-eyebrow">🔁 Wiederholung</span>
          <h1 className="ans-title">Fehler wiederholen</h1>
          <p className="ans-sub">Wähle einen Bereich — {overview.total} zu wiederholen</p>
        </div>
        <div className="ans-sections">
          {(overview.sections || []).map((s) => (
            <button
              key={s.family}
              type="button"
              className="ans-section-card"
              onClick={() => startSection(s.family)}
            >
              <span className="ans-section-emoji">{s.emoji || '📚'}</span>
              <span className="ans-section-body">
                <span className="ans-section-title">{s.title_de}</span>
                {s.subtitle_de ? <span className="ans-section-sub">{s.subtitle_de}</span> : null}
              </span>
              <span className="ans-section-count">{s.count}</span>
            </button>
          ))}
        </div>
        {many ? (
          <button className="ans-btn" onClick={() => startSection(null)}>Alle wiederholen ({overview.total}) →</button>
        ) : null}
        <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
      </Root>
    );
  }
  if (phase === 'artikel') {
    return <ArtikelReviewGame api={api} haptic={haptic} onClose={onClose} onBack={loadOverview} />;
  }
  if (phase === 'wofrage') {
    return <WoFrageReviewGame api={api} haptic={haptic} onClose={onClose} onBack={loadOverview} />;
  }
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
        <button className="ans-btn" onClick={() => (remaining > 0 ? loadNext(family) : loadOverview())}>
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
