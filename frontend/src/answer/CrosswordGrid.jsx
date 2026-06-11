import React, { useCallback, useEffect, useMemo, useState } from 'react';

/**
 * Interactive crossword: type letters straight into the grid via an on-screen
 * German keyboard. Given letters (already visible in the group image) are
 * locked; empty cells are filled by the user; tapping a cell selects its word.
 * On submit, each hidden word's letters are reconstructed and sent in the same
 * space-separated format the server already grades (evaluate_crossword).
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() { try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ } }

const KB_ROWS = [
  ['Q', 'W', 'E', 'R', 'T', 'Z', 'U', 'I', 'O', 'P'],
  ['A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L'],
  ['Y', 'X', 'C', 'V', 'B', 'N', 'M'],
  ['Ä', 'Ö', 'Ü', 'ß'],
];

const k = (r, c) => `${r},${c}`;

export default function CrosswordGrid({ task, onSubmit, submitting }) {
  const grid = task.grid || [];
  const cols = task.cols || (grid[0] ? grid[0].length : 0);
  const words = useMemo(() => task.words || [], [task]);

  // cell "r,c" → [wordIdx, ...]
  const cellWords = useMemo(() => {
    const m = {};
    words.forEach((w, wi) => (w.cells || []).forEach(([r, c]) => {
      (m[k(r, c)] = m[k(r, c)] || []).push(wi);
    }));
    return m;
  }, [words]);

  const emptyKeys = useMemo(() => {
    const s = [];
    grid.forEach((row, r) => row.forEach((cell, c) => { if (cell && cell.e) s.push(k(r, c)); }));
    return s;
  }, [grid]);

  const [inputs, setInputs] = useState({});
  const [activeWord, setActiveWord] = useState(0);
  const [activeCell, setActiveCell] = useState(null);

  const cellAt = useCallback((r, c) => (grid[r] ? grid[r][c] : null), [grid]);
  const isEmpty = useCallback((r, c) => { const x = cellAt(r, c); return !!(x && x.e); }, [cellAt]);

  const firstEmptyOf = useCallback((wi, fromUnfilled = false) => {
    const cells = (words[wi] && words[wi].cells) || [];
    const found = cells.find(([r, c]) => isEmpty(r, c) && (!fromUnfilled || !inputs[k(r, c)]));
    return found || cells.find(([r, c]) => isEmpty(r, c)) || null;
  }, [words, isEmpty, inputs]);

  useEffect(() => {
    if (!activeCell && words.length) setActiveCell(firstEmptyOf(0, true));
  }, [words, activeCell, firstEmptyOf]);

  const selectCell = useCallback((r, c) => {
    const ws = cellWords[k(r, c)];
    if (!ws || !ws.length) return;
    tapHaptic();
    let wi;
    if (activeCell && activeCell[0] === r && activeCell[1] === c && ws.length > 1) {
      wi = ws.find((x) => x !== activeWord) ?? ws[0];   // re-tap → toggle direction
    } else if (ws.includes(activeWord)) {
      wi = activeWord;
    } else {
      wi = ws[0];
    }
    setActiveWord(wi);
    setActiveCell(isEmpty(r, c) ? [r, c] : firstEmptyOf(wi, true));
  }, [cellWords, activeCell, activeWord, isEmpty, firstEmptyOf]);

  const advance = useCallback((wi, fromCell) => {
    const cells = (words[wi] && words[wi].cells) || [];
    const idx = cells.findIndex(([r, c]) => fromCell && r === fromCell[0] && c === fromCell[1]);
    for (let i = idx + 1; i < cells.length; i++) {
      const [r, c] = cells[i];
      if (isEmpty(r, c)) return [r, c];
    }
    return fromCell; // word filled → stay
  }, [words, isEmpty]);

  const typeLetter = useCallback((ch) => {
    let cell = activeCell;
    if (!cell || !isEmpty(cell[0], cell[1])) cell = firstEmptyOf(activeWord, true);
    if (!cell) return;
    tapHaptic();
    setInputs((prev) => ({ ...prev, [k(cell[0], cell[1])]: ch }));
    setActiveCell(advance(activeWord, cell));
  }, [activeCell, activeWord, isEmpty, firstEmptyOf, advance]);

  const backspace = useCallback(() => {
    tapHaptic();
    let cell = activeCell;
    if (cell && inputs[k(cell[0], cell[1])]) {
      setInputs((prev) => { const n = { ...prev }; delete n[k(cell[0], cell[1])]; return n; });
      return;
    }
    // step back to the previous filled empty cell in the word
    const cells = (words[activeWord] && words[activeWord].cells) || [];
    const idx = cell ? cells.findIndex(([r, c]) => r === cell[0] && c === cell[1]) : cells.length;
    for (let i = idx - 1; i >= 0; i--) {
      const [r, c] = cells[i];
      if (isEmpty(r, c)) {
        setInputs((prev) => { const n = { ...prev }; delete n[k(r, c)]; return n; });
        setActiveCell([r, c]);
        return;
      }
    }
  }, [activeCell, activeWord, inputs, words, isEmpty]);

  const allFilled = emptyKeys.length > 0 && emptyKeys.every((key) => inputs[key]);

  const submit = useCallback(() => {
    const guesses = words.map((w) => (w.cells || []).map(([r, c]) => {
      const cell = cellAt(r, c);
      return (cell && cell.l) ? cell.l : (inputs[k(r, c)] || '');
    }).join(''));
    onSubmit(guesses.join(' '));
  }, [words, cellAt, inputs, onSubmit]);

  const activeKeySet = useMemo(() => {
    const s = new Set();
    (words[activeWord]?.cells || []).forEach(([r, c]) => s.add(k(r, c)));
    return s;
  }, [words, activeWord]);

  const aw = words[activeWord];
  const arrow = aw?.direction === 'across' ? '↔' : '↕';

  return (
    <>
      <div className="cw-grid" style={{ gridTemplateColumns: `repeat(${cols}, 1fr)` }}>
        {grid.map((row, r) => row.map((cell, c) => {
          if (!cell) return <div key={k(r, c)} className="cw-cell blocked" />;
          const key = k(r, c);
          const letter = cell.l || inputs[key] || '';
          const active = activeCell && activeCell[0] === r && activeCell[1] === c;
          const inWord = activeKeySet.has(key);
          return (
            <div
              key={key}
              className={`cw-cell${cell.l ? ' given' : ' empty'}${inWord ? ' inword' : ''}${active ? ' active' : ''}`}
              onClick={() => selectCell(r, c)}
            >
              {cell.n ? <span className="cw-num">{cell.n}</span> : null}
              <span className="cw-letter">{letter}</span>
            </div>
          );
        }))}
      </div>

      {aw ? (
        <div className="cw-clue">
          <span className="ans-num">{aw.number}{arrow}</span> {aw.clue_de}
          {aw.clue_ru ? <span className="ans-meaning"> · {aw.clue_ru}</span> : null}
        </div>
      ) : null}

      <div className="cw-kb">
        {KB_ROWS.map((row, ri) => (
          <div className="cw-kb-row" key={ri}>
            {ri === 2 ? <button className="cw-key cw-key-wide" onClick={backspace}>⌫</button> : null}
            {row.map((ch) => (
              <button key={ch} className="cw-key" onClick={() => typeLetter(ch)} disabled={submitting}>{ch}</button>
            ))}
          </div>
        ))}
      </div>

      <button className="ans-btn" disabled={!allFilled || submitting} onClick={submit}>
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>
    </>
  );
}
