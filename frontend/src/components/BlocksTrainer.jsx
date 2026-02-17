import React, { useEffect, useMemo, useRef, useState } from 'react';

export const normalizeAnswer = (value) => {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
};

export const shuffle = (items) => {
  const arr = [...items];
  const rand = () => {
    if (window.crypto?.getRandomValues) {
      const buf = new Uint32Array(1);
      window.crypto.getRandomValues(buf);
      return buf[0] / 2 ** 32;
    }
    return Math.random();
  };
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
};

const BASE_SECONDS_MIN = 7;
const BASE_SECONDS_MAX = 10;
const ADAPTIVE_SECONDS_PER_CHAR = 2;
const ADAPTIVE_SECONDS_MAX = 30;
const AUTO_NEXT_DELAY_MS = 2000;

const detectCardType = (answer, explicitType) => {
  if (explicitType === 'WORD' || explicitType === 'PHRASE') return explicitType;
  return /\s+/.test(String(answer || '').trim()) ? 'PHRASE' : 'WORD';
};

const tokenize = (answer, cardType) => {
  const raw = String(answer || '').trim();
  if (!raw) return [];
  if (cardType === 'PHRASE') {
    return raw.split(/\s+/).filter(Boolean);
  }
  return Array.from(raw.replace(/\s+/g, ''));
};

const calcTimerMs = (answer, timerMode) => {
  if (timerMode === 'none') return null;
  if (timerMode === 'adaptive') {
    const len = String(answer || '').length;
    const seconds = Math.min(Math.max(len * ADAPTIVE_SECONDS_PER_CHAR, BASE_SECONDS_MIN), ADAPTIVE_SECONDS_MAX);
    return seconds * 1000;
  }
  return BASE_SECONDS_MAX * 1000;
};

export default function BlocksTrainer({
  card,
  prompt,
  answer,
  cardType,
  timerMode = 'adaptive',
  autoAdvance = true,
  onNext,
  onRoundResult,
}) {
  const containerRef = useRef(null);
  const slotsRowRef = useRef(null);
  const slotRefs = useRef([]);
  const rafRef = useRef(null);
  const pointerRef = useRef({
    pointerId: null,
    tileId: null,
    offsetX: 0,
    offsetY: 0,
    startX: 0,
    startY: 0,
    latestX: 0,
    latestY: 0,
    moving: false,
    dragged: false,
  });
  const finishedRef = useRef(false);
  const startAtRef = useRef(Date.now());
  const autoNextRef = useRef(null);

  const type = useMemo(() => detectCardType(answer, cardType), [answer, cardType]);
  const targetTokens = useMemo(() => tokenize(answer, type), [answer, type]);
  const timerMs = useMemo(() => calcTimerMs(answer, timerMode), [answer, timerMode]);

  const [tiles, setTiles] = useState([]);
  const [slots, setSlots] = useState([]);
  const [slotRects, setSlotRects] = useState([]);
  const [status, setStatus] = useState('idle'); // idle|correct|wrong|timeout
  const [hintsUsed, setHintsUsed] = useState(0);
  const [timeLeftMs, setTimeLeftMs] = useState(timerMs);

  const allSlotsFilled = slots.length > 0 && slots.every((item) => item !== null);
  const isFinished = status !== 'idle';

  const syncSlotRects = () => {
    const containerRect = containerRef.current?.getBoundingClientRect();
    if (!containerRect) return;
    const nextRects = targetTokens.map((_item, idx) => {
      const rect = slotRefs.current[idx]?.getBoundingClientRect();
      if (!rect) return null;
      return {
        left: rect.left - containerRect.left,
        top: rect.top - containerRect.top,
        width: rect.width,
        height: rect.height,
        centerX: rect.left - containerRect.left + rect.width / 2,
        centerY: rect.top - containerRect.top + rect.height / 2,
      };
    });
    setSlotRects(nextRects);
  };

  const scatterTiles = (tileItems, currentSlots = []) => {
    const containerRect = containerRef.current?.getBoundingClientRect();
    const rowRect = slotsRowRef.current?.getBoundingClientRect();
    if (!containerRect) return tileItems;

    const leftPad = 16;
    const rightPad = 16;
    const topBase = rowRect ? Math.max(10, rowRect.bottom - containerRect.top + 8) : 14;
    const usableWidth = Math.max(containerRect.width - leftPad - rightPad, 220);
    const cols = Math.max(2, Math.min(4, Math.floor(usableWidth / 110)));
    const gapX = Math.max(10, Math.floor((usableWidth - cols * 90) / Math.max(cols - 1, 1)));
    const gapY = 16;

    return tileItems.map((tile, index) => {
      const placedSlot = currentSlots.findIndex((slotId) => slotId === tile.id);
      if (placedSlot >= 0 && slotRects[placedSlot]) {
        const rect = slotRects[placedSlot];
        return {
          ...tile,
          slotIndex: placedSlot,
          x: rect.centerX - 45,
          y: rect.centerY - 26,
        };
      }
      const row = Math.floor(index / cols);
      const col = index % cols;
      const jitterX = (Math.random() - 0.5) * 14;
      const jitterY = (Math.random() - 0.5) * 10;
      const x = leftPad + col * (90 + gapX) + jitterX;
      const y = topBase + row * (52 + gapY) + jitterY;
      return {
        ...tile,
        slotIndex: null,
        x,
        y,
        homeX: x,
        homeY: y,
      };
    });
  };

  useEffect(() => {
    const baseTiles = shuffle(
      targetTokens.map((text, idx) => ({
        id: `tile-${idx}`,
        text,
        targetIndex: idx,
        slotIndex: null,
        x: 0,
        y: 0,
        homeX: 0,
        homeY: 0,
      }))
    );
    setSlots(new Array(targetTokens.length).fill(null));
    setTiles(baseTiles);
    setStatus('idle');
    setHintsUsed(0);
    setTimeLeftMs(timerMs);
    finishedRef.current = false;
    startAtRef.current = Date.now();
    if (autoNextRef.current) {
      clearTimeout(autoNextRef.current);
      autoNextRef.current = null;
    }
  }, [card?.id, answer, timerMs, targetTokens.length]);

  useEffect(() => {
    const onResize = () => {
      syncSlotRects();
      setTiles((prev) => scatterTiles(prev, slots));
    };
    syncSlotRects();
    onResize();
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [slots.length, targetTokens.length]);

  useEffect(() => {
    if (!allSlotsFilled || isFinished) return;
    const builtTokens = slots.map((tileId) => {
      const tile = tiles.find((item) => item.id === tileId);
      return tile?.text || '';
    });
    const built = type === 'PHRASE' ? builtTokens.join(' ') : builtTokens.join('');
    const isCorrect = normalizeAnswer(built) === normalizeAnswer(answer);
    setStatus(isCorrect ? 'correct' : 'wrong');
  }, [allSlotsFilled, isFinished, slots, tiles, type, answer]);

  useEffect(() => {
    if (status === 'idle') return;
    if (finishedRef.current) return;
    finishedRef.current = true;
    const isCorrect = status === 'correct';
    const timeSpentMs = Math.max(0, Date.now() - startAtRef.current);
    onRoundResult?.({
      isCorrect,
      timeSpentMs,
      hintsUsed,
      status,
    });
    if (autoAdvance) {
      autoNextRef.current = setTimeout(() => {
        onNext?.();
      }, AUTO_NEXT_DELAY_MS);
    }
  }, [status, hintsUsed, onRoundResult, autoAdvance, onNext]);

  useEffect(() => {
    if (timerMs === null || isFinished) return;
    if (timeLeftMs === null) return;
    if (timeLeftMs <= 0) {
      setStatus('timeout');
      return;
    }
    const started = Date.now();
    const t = setTimeout(() => {
      const spent = Date.now() - started;
      setTimeLeftMs((prev) => (prev === null ? prev : Math.max(prev - spent, 0)));
    }, 100);
    return () => clearTimeout(t);
  }, [timerMs, timeLeftMs, isFinished]);

  useEffect(() => {
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      if (autoNextRef.current) clearTimeout(autoNextRef.current);
    };
  }, []);

  const findSlotByPoint = (x, y) => {
    for (let i = 0; i < slotRects.length; i += 1) {
      const rect = slotRects[i];
      if (!rect) continue;
      if (x >= rect.left && x <= rect.left + rect.width && y >= rect.top && y <= rect.top + rect.height) {
        return i;
      }
    }
    return -1;
  };

  const moveTileToSlot = (tileId, slotIndex) => {
    setTiles((prevTiles) => {
      const nextTiles = prevTiles.map((item) => ({ ...item }));
      setSlots((prevSlots) => {
        const nextSlots = [...prevSlots];
        const tile = nextTiles.find((t) => t.id === tileId);
        if (!tile) return prevSlots;

        if (tile.slotIndex !== null && nextSlots[tile.slotIndex] === tile.id) {
          nextSlots[tile.slotIndex] = null;
        }

        const occupantId = nextSlots[slotIndex];
        if (occupantId && occupantId !== tile.id) {
          const occupiedTile = nextTiles.find((t) => t.id === occupantId);
          if (occupiedTile) {
            occupiedTile.slotIndex = null;
            occupiedTile.x = occupiedTile.homeX;
            occupiedTile.y = occupiedTile.homeY;
          }
        }

        const slotRect = slotRects[slotIndex];
        tile.slotIndex = slotIndex;
        if (slotRect) {
          tile.x = slotRect.centerX - 45;
          tile.y = slotRect.centerY - 26;
        }
        nextSlots[slotIndex] = tile.id;
        return nextSlots;
      });
      return nextTiles;
    });
  };

  const returnTileToHome = (tileId) => {
    setTiles((prev) => prev.map((tile) => {
      if (tile.id !== tileId) return tile;
      return {
        ...tile,
        slotIndex: null,
        x: tile.homeX,
        y: tile.homeY,
      };
    }));
    setSlots((prev) => prev.map((slotId) => (slotId === tileId ? null : slotId)));
  };

  const onPointerMove = (event) => {
    const state = pointerRef.current;
    if (state.pointerId !== event.pointerId || !state.tileId) return;
    const containerRect = containerRef.current?.getBoundingClientRect();
    if (!containerRect) return;
    const pointerX = event.clientX - containerRect.left;
    const pointerY = event.clientY - containerRect.top;
    const deltaX = pointerX - state.startX;
    const deltaY = pointerY - state.startY;
    if (!state.dragged && Math.hypot(deltaX, deltaY) > 4) {
      state.dragged = true;
    }
    state.latestX = pointerX - state.offsetX;
    state.latestY = pointerY - state.offsetY;
    if (state.moving) return;
    state.moving = true;
    rafRef.current = requestAnimationFrame(() => {
      setTiles((prev) => prev.map((tile) => (
        tile.id === state.tileId
          ? { ...tile, x: state.latestX, y: state.latestY, slotIndex: null }
          : tile
      )));
      setSlots((prev) => prev.map((slotId) => (slotId === state.tileId ? null : slotId)));
      state.moving = false;
    });
  };

  const placeTileByTap = (tileId) => {
    const tile = tiles.find((item) => item.id === tileId);
    if (!tile) return;
    if (tile.slotIndex !== null) {
      returnTileToHome(tileId);
      return;
    }
    const firstEmpty = slots.findIndex((slotId) => slotId === null);
    if (firstEmpty >= 0) {
      moveTileToSlot(tileId, firstEmpty);
      return;
    }
    const firstWrong = slots.findIndex((slotId, idx) => {
      if (!slotId) return false;
      const placed = tiles.find((item) => item.id === slotId);
      return placed?.targetIndex !== idx;
    });
    if (firstWrong >= 0) {
      moveTileToSlot(tileId, firstWrong);
    }
  };

  const onPointerUp = (event) => {
    const state = pointerRef.current;
    if (state.pointerId !== event.pointerId || !state.tileId) return;
    const tileId = state.tileId;
    const dragged = state.dragged;
    pointerRef.current = {
      pointerId: null,
      tileId: null,
      offsetX: 0,
      offsetY: 0,
      startX: 0,
      startY: 0,
      latestX: 0,
      latestY: 0,
      moving: false,
      dragged: false,
    };
    if (!dragged) {
      placeTileByTap(tileId);
      return;
    }
    const containerRect = containerRef.current?.getBoundingClientRect();
    if (!containerRect) {
      returnTileToHome(tileId);
      return;
    }
    const localX = event.clientX - containerRect.left;
    const localY = event.clientY - containerRect.top;
    const slotIndex = findSlotByPoint(localX, localY);
    if (slotIndex >= 0) {
      moveTileToSlot(tileId, slotIndex);
    } else {
      returnTileToHome(tileId);
    }
  };

  const onTilePointerDown = (event, tile) => {
    if (isFinished) return;
    event.preventDefault();
    event.stopPropagation();
    const containerRect = containerRef.current?.getBoundingClientRect();
    if (!containerRect) return;
    const offsetX = event.clientX - containerRect.left - tile.x;
    const offsetY = event.clientY - containerRect.top - tile.y;
    pointerRef.current = {
      pointerId: event.pointerId,
      tileId: tile.id,
      offsetX,
      offsetY,
      startX: event.clientX - containerRect.left,
      startY: event.clientY - containerRect.top,
      latestX: tile.x,
      latestY: tile.y,
      moving: false,
      dragged: false,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const onSlotClick = (slotIndex) => {
    if (isFinished) return;
    const tileId = slots[slotIndex];
    if (!tileId) return;
    returnTileToHome(tileId);
  };

  const applyHint = () => {
    if (isFinished) return;
    const firstWrongSlot = targetTokens.findIndex((_token, idx) => {
      const tileId = slots[idx];
      if (!tileId) return true;
      const tile = tiles.find((item) => item.id === tileId);
      return tile?.targetIndex !== idx;
    });
    if (firstWrongSlot < 0) return;
    const correctTile = tiles.find((item) => item.targetIndex === firstWrongSlot);
    if (!correctTile) return;
    moveTileToSlot(correctTile.id, firstWrongSlot);
    setHintsUsed((prev) => prev + 1);
  };

  const forceCheck = () => {
    if (isFinished) return;
    if (!allSlotsFilled) return;
    const builtTokens = slots.map((tileId) => {
      const tile = tiles.find((item) => item.id === tileId);
      return tile?.text || '';
    });
    const built = type === 'PHRASE' ? builtTokens.join(' ') : builtTokens.join('');
    const isCorrect = normalizeAnswer(built) === normalizeAnswer(answer);
    setStatus(isCorrect ? 'correct' : 'wrong');
  };

  const displayAnswer = type === 'PHRASE' ? targetTokens.join(' ') : targetTokens.join('');
  const timerSeconds = timeLeftMs === null ? null : Math.ceil(timeLeftMs / 1000);

  return (
    <div className={`blocks-trainer ${status !== 'idle' ? `is-${status}` : ''}`}>
      <div className="blocks-head">
        <div className="blocks-prompt">{prompt || 'Соберите ответ'}</div>
        {timerSeconds !== null && (
          <div className={`blocks-timer ${timerSeconds <= 3 ? 'is-danger' : ''}`}>{timerSeconds}s</div>
        )}
      </div>

      <div className="blocks-target" ref={slotsRowRef}>
        {targetTokens.map((token, idx) => {
          const tileId = slots[idx];
          const placed = tiles.find((item) => item.id === tileId);
          return (
            <div
              key={`slot-${idx}`}
              ref={(el) => { slotRefs.current[idx] = el; }}
              className={`blocks-slot ${placed ? 'is-filled' : ''}`}
              role="button"
              tabIndex={0}
              onClick={() => onSlotClick(idx)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  onSlotClick(idx);
                }
              }}
            >
              {placed ? placed.text : (type === 'WORD' ? '•' : '...')}
            </div>
          );
        })}
      </div>

      <div
        className="blocks-playground"
        ref={containerRef}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
      >
        {tiles.filter((tile) => tile.slotIndex === null).map((tile) => (
          <button
            key={tile.id}
            type="button"
            className={`blocks-tile color-${tile.targetIndex % 6}`}
            style={{ transform: `translate3d(${tile.x}px, ${tile.y}px, 0)` }}
            onPointerDown={(event) => onTilePointerDown(event, tile)}
            disabled={isFinished}
          >
            {tile.text}
          </button>
        ))}
      </div>

      <div className="blocks-footer">
        <button type="button" className="secondary-button" onClick={applyHint} disabled={isFinished}>
          Подсказка
        </button>
        {!isFinished && (
          <button
            type="button"
            className="primary-button"
            onClick={forceCheck}
            disabled={!allSlotsFilled}
          >
            Проверить
          </button>
        )}
        {isFinished && (
          <button type="button" className="primary-button" onClick={() => onNext?.()}>
            Дальше
          </button>
        )}
      </div>

      {isFinished && (
        <div className={`blocks-result ${status === 'correct' ? 'ok' : 'bad'}`}>
          <div>{status === 'correct' ? 'Верно!' : status === 'timeout' ? 'Время вышло' : 'Неверно'}</div>
          <div>Правильный ответ: {displayAnswer}</div>
          <div>Подсказок: {hintsUsed}</div>
        </div>
      )}
    </div>
  );
}
