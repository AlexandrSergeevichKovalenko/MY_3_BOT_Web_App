import React, { useCallback, useEffect, useMemo, useState } from 'react';

/**
 * WOW anagram game: assemble the German word from scrambled tiles.
 *
 * First and last letters are locked in their slots; the user taps the shuffled
 * middle tiles to fill the empty slots (tap a filled slot to take the letter
 * back). Self-contained input component — calls onSubmit(assembled) when the
 * word is complete. Rendered by AnswerOverlay for kind="ag".
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() {
  try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ }
}

function shuffled(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export default function AnagramGame({ task, onSubmit, submitting, error }) {
  const first = task.first_letter || '';
  const last = task.last_letter || '';
  const poolItems = useMemo(
    () => (task.middle_letters || []).map((ch, i) => ({ id: i, char: ch })),
    [task],
  );
  const middleCount = poolItems.length;

  const [placedIds, setPlacedIds] = useState([]);   // ordered ids in the slots
  const [order, setOrder] = useState(() => poolItems.map((p) => p.id)); // pool display order

  useEffect(() => {
    setPlacedIds([]);
    setOrder(shuffled(poolItems.map((p) => p.id)));
  }, [poolItems]);

  const byId = useCallback((id) => poolItems.find((p) => p.id === id), [poolItems]);
  const placedSet = new Set(placedIds);
  const available = order.filter((id) => !placedSet.has(id));
  const isComplete = placedIds.length === middleCount;
  const assembled = first + placedIds.map((id) => byId(id)?.char || '').join('') + last;

  const place = useCallback((id) => {
    setPlacedIds((prev) => (prev.includes(id) || prev.length >= middleCount ? prev : [...prev, id]));
    tapHaptic();
  }, [middleCount]);

  const removeAt = useCallback((slotIdx) => {
    setPlacedIds((prev) => prev.filter((_, i) => i !== slotIdx));
    tapHaptic();
  }, []);

  const backspace = useCallback(() => {
    setPlacedIds((prev) => prev.slice(0, -1));
    tapHaptic();
  }, []);

  const mischen = useCallback(() => {
    setOrder((prev) => shuffled(prev));
    tapHaptic();
  }, []);

  // Slots: index 0 = first (locked), 1..middleCount = user, last = last (locked).
  const nextEmpty = placedIds.length; // 0-based middle slot awaiting a letter
  const slots = [];
  slots.push({ key: 'first', char: first, locked: true });
  for (let j = 0; j < middleCount; j++) {
    const pid = placedIds[j];
    slots.push({
      key: `m${j}`,
      char: pid != null ? byId(pid)?.char : '',
      locked: false,
      filled: pid != null,
      active: pid == null && j === nextEmpty,
      slotIdx: j,
      animKey: pid != null ? `p${pid}` : `e${j}`,
    });
  }
  slots.push({ key: 'last', char: last, locked: true });

  return (
    <>
      <div className="ag-slots">
        {slots.map((s) => (
          <div
            key={s.key}
            className={`ag-slot${s.locked ? ' locked' : ''}${s.filled ? ' filled' : ''}${s.active ? ' active' : ''}`}
            onClick={s.filled ? () => removeAt(s.slotIdx) : undefined}
          >
            <span key={s.animKey} className={s.char ? 'ag-slot-letter' : ''}>{s.char || ''}</span>
          </div>
        ))}
      </div>

      <div className="ag-pool">
        {available.length === 0 ? (
          <span className="ag-pool-empty">Alle Buchstaben gesetzt — Prüfen ✓</span>
        ) : (
          available.map((id) => (
            <button key={id} className="ag-tile" onClick={() => place(id)} disabled={submitting}>
              {byId(id)?.char}
            </button>
          ))
        )}
      </div>

      <div className="ag-controls">
        <button className="ag-ctrl" onClick={mischen} disabled={submitting} title="Mischen">🔀</button>
        <button className="ag-ctrl" onClick={backspace} disabled={submitting || placedIds.length === 0} title="Löschen">⌫</button>
      </div>

      {error ? <p className="ans-error">{error}</p> : null}
      <button
        className="ans-btn"
        disabled={!isComplete || submitting}
        onClick={() => onSubmit(assembled)}
      >
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>
    </>
  );
}
