import React, { useState } from 'react';

/**
 * Multiple-choice quiz — the in-place Mini-App replacement for the native poll.
 * Tap an option to answer; the verdict renders in the overlay (no message at the
 * chat bottom). The "keine korrekte Antworten" option reveals a text field and
 * submits the typed word (the freeform path), exactly like the old poll did.
 */
export default function MCGame({ task, onSubmit, submitting }) {
  const options = task?.options || [];
  const freeformOption = String(task?.freeform_option || '').trim();
  const [picked, setPicked] = useState(null); // index of the freeform option once tapped
  const [typed, setTyped] = useState('');

  const tap = (idx, text) => {
    if (submitting) return;
    if (freeformOption && text === freeformOption) {
      setPicked(idx); // reveal the text field instead of submitting immediately
      return;
    }
    onSubmit(idx, '');
  };

  return (
    <>
      {task?.question ? <p className="mc-question ans-r-prompt">{task.question}</p> : null}
      <div className="mc-options ans-r-work">
        {options.map((opt, idx) => {
          const isFreeform = freeformOption && opt === freeformOption;
          const isPicked = picked === idx;
          return (
            <button
              key={idx}
              type="button"
              className={`mc-option${isFreeform ? ' freeform' : ''}${isPicked ? ' on' : ''}`}
              disabled={submitting}
              onClick={() => tap(idx, opt)}
            >
              {opt}
            </button>
          );
        })}
      </div>
      {picked != null ? (
        <div className="mc-freeform">
          <input
            className="ans-input"
            value={typed}
            onChange={(e) => setTyped(e.target.value)}
            placeholder="dein Wort …"
            autoFocus
            autoCapitalize="off"
            autoCorrect="off"
            enterKeyHint="send"
            onKeyDown={(e) => { if (e.key === 'Enter' && typed.trim()) onSubmit(picked, typed.trim()); }}
          />
          <button
            type="button"
            className="ans-btn"
            disabled={submitting || !typed.trim()}
            onClick={() => onSubmit(picked, typed.trim())}
          >
            {submitting ? 'Prüfe …' : 'Prüfen ✓'}
          </button>
        </div>
      ) : null}
    </>
  );
}
