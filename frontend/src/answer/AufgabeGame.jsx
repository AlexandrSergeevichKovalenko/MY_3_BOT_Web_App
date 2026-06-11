import React, { useState } from 'react';

/**
 * B2+ text tasks ("Aufgabe"). Phase B1 = open cloze: a German sentence with a
 * "_____" gap; the user types the missing word (no options — harder than MC).
 * Self-contained input; calls onSubmit(answer). Rendered for kind="au".
 * Wortbildung / Satztransformation will plug in here by `format`.
 */
export default function AufgabeGame({ task, onSubmit, submitting }) {
  const [value, setValue] = useState('');
  const fmt = task.format || 'cloze';
  const satz = String(task.satz || '');

  // Render the sentence with the gap visually emphasised.
  const parts = satz.split('_____');
  const sentence = parts.length > 1
    ? parts.flatMap((p, i) => (i === 0 ? [p] : [<span className="au-gap" key={i}>＿＿＿</span>, p]))
    : satz;

  const submit = () => { const v = value.trim(); if (v) onSubmit(v); };

  return (
    <>
      {fmt === 'cloze' ? (
        <div className="au-satz">{sentence}</div>
      ) : (
        <div className="au-satz">{satz}</div>
      )}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <input
        className="ans-input"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="fehlendes Wort …"
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
