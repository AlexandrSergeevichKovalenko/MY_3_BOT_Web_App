import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/**
 * Interactive crossword: type letters straight into the grid via an on-screen
 * German keyboard. Given letters (already visible in the group image) are
 * locked; empty cells are filled by the user; tapping a cell selects its word.
 * On submit, each hidden word's letters are reconstructed and sent in the same
 * space-separated format the server already grades (evaluate_crossword).
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() { try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ } }

// Три ряда, а не четыре: четвёртый ряд отъедал у сетки высоту, из-за которой
// клетки становились мельче. Умляуты переехали в нижний ряд, а «ß» убрана совсем —
// сетка заглавная, а заглавная ß в немецком записывается как SS, так что нажать её
// было некуда, зато перепутать с S можно было легко.
const KB_ROWS = [
  ['Q', 'W', 'E', 'R', 'T', 'Z', 'U', 'I', 'O', 'P'],
  ['A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L'],
  ['Y', 'X', 'C', 'V', 'B', 'N', 'M', 'Ä', 'Ö', 'Ü'],
];

const k = (r, c) => `${r},${c}`;

const CW_GAP = 2;  // keep in sync with .cw-grid gap in answer.css

// Плашка «как играть» показывается первые три раза — потом человек это уже знает,
// и напоминание превращается в шум.
function ruWords(n) {
  const tail = n % 100;
  if (tail >= 12 && tail <= 14) return 'слов';
  const last = n % 10;
  if (last === 1) return 'слово';
  if (last >= 2 && last <= 4) return 'слова';
  return 'слов';
}

const INTRO_KEY = 'cw_intro_seen';
const INTRO_TIMES = 3;
const INTRO_MS = 7000;

function shouldShowIntro() {
  try {
    const seen = Number(window.localStorage.getItem(INTRO_KEY) || 0);
    if (seen >= INTRO_TIMES) return false;
    window.localStorage.setItem(INTRO_KEY, String(seen + 1));
    return true;
  } catch (_e) {
    return true;   // приватный режим — лучше показать, чем промолчать
  }
}

export default function CrosswordGrid({ task, onSubmit, onHint, submitting }) {
  const grid = task.grid || [];
  const cols = task.cols || (grid[0] ? grid[0].length : 0);
  const rows = grid.length;
  const words = useMemo(() => task.words || [], [task]);

  // Размер клетки считается по ОБЕИМ осям — по ширине и по высоте того места,
  // которое осталось от заголовка, подсказки, клавиатуры и кнопки (это место
  // задаёт flex-раскладка в answer.css). Раньше считалась только ширина, поэтому
  // на высоком экране сетка вылезала вниз и кнопка «Prüfen» уезжала под сгиб.
  //
  // Потолок клетки — не фикс, а доля экрана: на телефоне клетка мельче, на
  // планшете крупнее, пропорции те же.
  const wrapRef = useRef(null);
  const [cell, setCell] = useState(0);
  useEffect(() => {
    const el = wrapRef.current;
    if (!el || !cols || !rows) return undefined;
    const compute = () => {
      const w = el.clientWidth;
      const h = el.clientHeight;
      if (!w) return;
      const PAD = 12;  // место под угловые номера + воздух по краю
      const fitW = Math.floor((w - PAD - CW_GAP * (cols - 1)) / cols);
      const fitH = h ? Math.floor((h - 12 - CW_GAP * (rows - 1)) / rows) : fitW;
      const cap = Math.max(40, Math.round(Math.min(window.innerWidth, window.innerHeight) * 0.11));
      // Берём наибольший размер, который влезает по обеим осям. Жёсткого минимума
      // нет: он вытолкнул бы последний столбец за карточку на узком телефоне.
      const size = Math.max(12, Math.min(cap, fitW, fitH));
      setCell(size);
    };
    compute();
    const ro = new ResizeObserver(compute);
    ro.observe(el);
    window.addEventListener('resize', compute);
    return () => { ro.disconnect(); window.removeEventListener('resize', compute); };
  }, [cols, rows]);

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

  // ── Подсказка: одна буква на каждое загаданное слово ────────────────────────
  // Буквы ответов на клиент не приходят, поэтому букву открывает сервер — он же
  // помнит, на какие слова подсказка уже потрачена (перезагрузка её не вернёт).
  const [hintCells, setHintCells] = useState({});      // "r,c" → буква
  const [hintWords, setHintWords] = useState(() => new Set());
  const [hintBusy, setHintBusy] = useState(false);
  const [note, setNote] = useState('');

  useEffect(() => {
    const known = task.hints || [];
    if (!known.length) return;
    setHintCells((prev) => {
      const next = { ...prev };
      known.forEach((h) => { next[k(h.row, h.col)] = h.letter; });
      return next;
    });
    setHintWords(new Set(known.map((h) => h.word_number)));
  }, [task]);

  useEffect(() => {
    if (!note) return undefined;
    const t = setTimeout(() => setNote(''), 3200);
    return () => clearTimeout(t);
  }, [note]);

  const [intro, setIntro] = useState(() => shouldShowIntro());
  useEffect(() => {
    if (!intro) return undefined;
    const t = setTimeout(() => setIntro(false), INTRO_MS);
    return () => clearTimeout(t);
  }, [intro]);

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

  const hintsLeft = Math.max(0, words.length - hintWords.size);

  const askHint = useCallback(async () => {
    if (hintBusy || !onHint) return;
    const word = words[activeWord];
    if (!word) return;
    if (hintWords.has(word.number)) {
      setNote('В этом слове подсказка уже открыта — встань на другое слово');
      return;
    }
    const spot = (word.cells || []).find(([r, c]) => isEmpty(r, c) && !inputs[k(r, c)] && !hintCells[k(r, c)])
      || (word.cells || []).find(([r, c]) => isEmpty(r, c) && !hintCells[k(r, c)]);
    if (!spot) {
      setNote('В этом слове открывать нечего — все буквы уже на месте');
      return;
    }
    tapHaptic();
    setHintBusy(true);
    try {
      const res = await onHint(spot[0], spot[1]);
      if (res && res.letter) {
        setHintCells((prev) => ({ ...prev, [k(res.row, res.col)]: res.letter }));
        setHintWords((prev) => new Set(prev).add(res.word_number));
        setInputs((prev) => { const n = { ...prev }; delete n[k(res.row, res.col)]; return n; });
        setActiveCell(advance(activeWord, [res.row, res.col]));
      } else {
        setNote((res && res.error) || 'Подсказка не открылась, попробуй ещё раз');
      }
    } finally {
      setHintBusy(false);
    }
  }, [hintBusy, onHint, words, activeWord, hintWords, hintCells, inputs, isEmpty, advance]);

  const emptyLeft = emptyKeys.filter((key) => !inputs[key] && !hintCells[key]).length;
  const allFilled = emptyKeys.length > 0 && emptyLeft === 0;
  // Человек думает словами, а не клетками: «осталось два слова» понятнее, чем
  // «осталось девять пустых клеток».
  const wordsLeft = words.filter((w) => (w.cells || [])
    .some(([r, c]) => isEmpty(r, c) && !inputs[k(r, c)] && !hintCells[k(r, c)])).length;

  const submit = useCallback(() => {
    // Незаполненная клетка уходит как «_», а не пустой строкой: сервер раскладывает
    // ответ по словам через пробел, и от пустого слова весь разбор съехал бы на одну
    // позицию — человек увидел бы чужие ответы напротив своих слов.
    const guesses = words.map((w) => (w.cells || []).map(([r, c]) => {
      const cell = cellAt(r, c);
      return (cell && cell.l) ? cell.l : (inputs[k(r, c)] || '_');
    }).join(''));
    onSubmit(guesses.join(' '));
  }, [words, cellAt, inputs, onSubmit]);

  // «Prüfen» с пустыми клетками — это нормально: слово может быть незнакомым, и
  // застревать на нём человек не обязан. Но спрашиваем, чтобы случайный тап не
  // закрыл кроссворд раньше времени.
  const [confirmOpen, setConfirmOpen] = useState(false);
  const onCheck = useCallback(() => {
    if (allFilled) { submit(); return; }
    tapHaptic();
    setConfirmOpen(true);
  }, [allFilled, submit]);

  const activeKeySet = useMemo(() => {
    const s = new Set();
    (words[activeWord]?.cells || []).forEach(([r, c]) => s.add(k(r, c)));
    return s;
  }, [words, activeWord]);

  const aw = words[activeWord];
  const arrow = aw?.direction === 'across' ? '↔' : '↕';

  return (
    <>
      {onHint ? (
        <button
          className="cw-hint-btn"
          onClick={askHint}
          disabled={hintBusy || submitting || hintsLeft === 0}
          aria-label="Открыть одну букву"
        >
          <span className="cw-hint-icon">💡</span>
          <span className="cw-hint-count">{hintsLeft}</span>
        </button>
      ) : null}

      <div className="cw-grid-wrap" ref={wrapRef}>
        {/* Плашка и реплики висят ПОВЕРХ сетки, а не в потоке: иначе их появление
            и исчезновение меняло бы свободную высоту, и сетка на семь секунд
            уменьшалась бы, а потом прыгала обратно. */}
        {intro ? (
          <div className="cw-intro" onClick={() => setIntro(false)}>
            <span className="cw-intro-icon">💡</span>
            <span>
              Не знаешь слово — открой одну букву кнопкой 💡.
              А закончить можно в любой момент: нажми «Prüfen» и посмотри ответы.
            </span>
          </div>
        ) : null}
        {note ? <div className="cw-note">{note}</div> : null}

        <div
          className="cw-grid"
          style={{
            gridTemplateColumns: `repeat(${cols}, ${cell || 28}px)`,
            gridAutoRows: `${cell || 28}px`,
            '--cw-cell': `${cell || 28}px`,
          }}
        >
          {grid.map((row, r) => row.map((c0, c) => {
            if (!c0) return <div key={k(r, c)} className="cw-cell blocked" />;
            const key = k(r, c);
            const hinted = hintCells[key];
            const letter = c0.l || hinted || inputs[key] || '';
            const active = activeCell && activeCell[0] === r && activeCell[1] === c;
            const inWord = activeKeySet.has(key);
            return (
              <div
                key={key}
                className={`cw-cell${c0.l ? ' given' : ' empty'}${hinted ? ' hinted' : ''}${inWord ? ' inword' : ''}${active ? ' active' : ''}`}
                onClick={() => selectCell(r, c)}
              >
                {c0.n ? <span className="cw-num">{c0.n}</span> : null}
                <span className="cw-letter">{letter}</span>
              </div>
            );
          }))}
        </div>
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

      <button className="ans-btn" disabled={submitting} onClick={onCheck}>
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>

      {confirmOpen ? (
        <div className="cw-sheet-backdrop" onClick={() => setConfirmOpen(false)}>
          <div className="cw-sheet" onClick={(e) => e.stopPropagation()}>
            <div className="cw-sheet-icon">🦊</div>
            <div className="cw-sheet-title">Закончить сейчас?</div>
            <p className="cw-sheet-text">
              {wordsLeft === 1 ? 'Одно слово ещё не разгадано.' : `Не разгаданы ещё ${wordsLeft} ${ruWords(wordsLeft)}.`}
              {' '}Покажем правильные слова с переводом — их можно сразу сохранить в словарь.
            </p>
            <button className="cw-sheet-main" onClick={() => { setConfirmOpen(false); submit(); }}>
              Показать ответы
            </button>
            <button className="cw-sheet-ghost" onClick={() => setConfirmOpen(false)}>
              Продолжу решать
            </button>
          </div>
        </div>
      ) : null}
    </>
  );
}
