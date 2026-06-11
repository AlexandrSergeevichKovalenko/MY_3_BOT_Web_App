import React, { useState } from 'react';

/**
 * B2+ text tasks ("Aufgabe"), all typed (no options — harder, no giveaway):
 *  - cloze:        sentence with a "_____" gap → type the missing word.
 *  - wortbildung:  sentence with a gap + a root word → type the derived form.
 *  - transform:    rewrite (key-word, C1) → type the 2–5-word phrase that fills
 *                  the target sentence using the given key word.
 * Self-contained input; calls onSubmit(answer). Grading is a fast deterministic
 * check on the server (the generator pre-listed all accepted answers).
 */
function gapSentence(satz) {
  const parts = String(satz || '').split('_____');
  if (parts.length <= 1) return satz;
  return parts.flatMap((p, i) => (i === 0 ? [p] : [<span className="au-gap" key={i}>＿＿＿</span>, p]));
}

export default function AufgabeGame({ task, onSubmit, submitting }) {
  const [value, setValue] = useState('');
  const fmt = task.format || 'cloze';
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
        className="ans-input"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder={placeholder}
        autoFocus
        autoCapitalize="off"
        autoCorrect="off"
        enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
      />
      <button className="ans-btn" disabled={submitting || !value.trim()} onClick={submit}>
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>
    </>
  );
}
