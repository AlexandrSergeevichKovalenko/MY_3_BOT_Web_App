import React, { useEffect, useMemo, useRef, useState } from 'react';

export const normalizeAnswer = (value) => {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
};

const normalizeForCompare = (value, cardType) => {
  const normalized = normalizeAnswer(value);
  if (cardType === 'WORD') {
    return normalized.replace(/\s+/g, '');
  }
  return normalized;
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

const BASE_SECONDS_FIXED = 10;
const ADAPTIVE_SECONDS_PER_CHAR = 4;
const ADAPTIVE_SECONDS_MAX = 180;
const AUTO_NEXT_DELAY_OK_MS = 1100;
const AUTO_NEXT_DELAY_FAIL_MS = 4500;
const TILE_WIDTH = 76;
const TILE_HEIGHT = 62;
const GERMAN_ARTICLES = new Set([
  'der', 'die', 'das', 'den', 'dem', 'des',
  'ein', 'eine', 'einen', 'einem', 'einer', 'eines',
]);

const detectCardType = (answer, explicitType) => {
  const tokens = String(answer || '').trim().split(/\s+/).filter(Boolean);
  // Special case: article + noun behaves as one lexical item for this mode.
  if (tokens.length <= 1) return 'WORD';
  if (tokens.length === 2 && GERMAN_ARTICLES.has(tokens[0].toLowerCase())) return 'WORD';
  if (explicitType === 'WORD' || explicitType === 'PHRASE') return explicitType;
  return 'PHRASE';
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
    const len = Math.max(1, String(answer || '').length); // includes spaces by definition
    const seconds = Math.min(len * ADAPTIVE_SECONDS_PER_CHAR, ADAPTIVE_SECONDS_MAX);
    return seconds * 1000;
  }
  return BASE_SECONDS_FIXED * 1000;
};

export default function BlocksTrainer({
  card,
  prompt,
  answer,
  cardType,
  resetSignal = 0,
  timerMode = 'adaptive',
  isExternallyPaused = false,
  autoAdvance = true,
  onNext,
  onRoundResult,
  labels = {},
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
  const snapSlotTimeoutRef = useRef(null);
  const tileMotionTimeoutsRef = useRef({});
  const flyTimeoutsRef = useRef([]);
  const revealInProgressRef = useRef(false);
  const tilesRef = useRef([]);
  const slotsRef = useRef([]);

  const type = useMemo(() => detectCardType(answer, cardType), [answer, cardType]);
  const targetTokens = useMemo(() => tokenize(answer, type), [answer, type]);
  const timerMs = useMemo(() => calcTimerMs(answer, timerMode), [answer, timerMode]);

  const [tiles, setTiles] = useState([]);
  const [slots, setSlots] = useState([]);
  const [slotRects, setSlotRects] = useState([]);
  const [status, setStatus] = useState('idle'); // idle|correct|wrong|timeout
  const [hintsUsed, setHintsUsed] = useState(0);
  const [timeLeftMs, setTimeLeftMs] = useState(timerMs);
  const [playgroundHeight, setPlaygroundHeight] = useState(112);
  const [selectedSlotIndex, setSelectedSlotIndex] = useState(null);
  const [dragTileId, setDragTileId] = useState(null);
  const [hoverSlotIndex, setHoverSlotIndex] = useState(null);
  const [snapSlotIndex, setSnapSlotIndex] = useState(null);
  const [tileMotionMap, setTileMotionMap] = useState({});
  const [flyTokens, setFlyTokens] = useState([]);

  const allSlotsFilled = slots.length > 0 && slots.every((item) => item !== null);
  const isFinished = status !== 'idle';
  const tileWidthPx = type === 'WORD' ? 60 : TILE_WIDTH;
  const isWordMode = type === 'WORD';
  const wordSlotSizing = useMemo(() => {
    const tokenCount = targetTokens.length;
    if (tokenCount >= 12) {
      return { minWidth: 22, minHeight: 34, fontSize: 18 };
    }
    if (tokenCount >= 10) {
      return { minWidth: 24, minHeight: 36, fontSize: 20 };
    }
    if (tokenCount >= 8) {
      return { minWidth: 28, minHeight: 40, fontSize: 22 };
    }
    return { minWidth: 34, minHeight: 46, fontSize: 24 };
  }, [targetTokens.length]);
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
    if (!containerRect) return tileItems;

    const leftPad = 10;
    const rightPad = 10;
    const topBase = type === 'WORD' ? 6 : 8;
    const usableWidth = Math.max(containerRect.width - leftPad - rightPad, 220);
    const desiredTileWidth = tileWidthPx;
    const baseCols = type === 'WORD' ? 5 : 3;
    const cols = Math.max(2, Math.min(baseCols, Math.floor(usableWidth / (desiredTileWidth + 8))));
    const gapX = type === 'WORD'
      ? Math.max(4, Math.floor((usableWidth - cols * desiredTileWidth) / Math.max(cols - 1, 1)))
      : Math.max(8, Math.floor((usableWidth - cols * desiredTileWidth) / Math.max(cols - 1, 1)));
    const gapY = type === 'WORD' ? 8 : 10;
    const occupiedSet = new Set(currentSlots.filter(Boolean));
    const visibleCount = tileItems.filter((item) => !occupiedSet.has(item.id)).length;
    const rows = Math.max(1, Math.ceil(visibleCount / cols));
    const minPlaygroundHeight = visibleCount > 0 ? (type === 'WORD' ? 78 : 96) : 18;
    const targetHeight = visibleCount > 0
      ? Math.max(minPlaygroundHeight, topBase + rows * (TILE_HEIGHT + gapY) + 12)
      : minPlaygroundHeight;
    const boundedHeight = Math.min(targetHeight, type === 'WORD' ? 220 : 320);
    setPlaygroundHeight(boundedHeight);
    const cellCoords = [];
    for (let r = 0; r < rows; r += 1) {
      for (let c = 0; c < cols; c += 1) {
        cellCoords.push({ row: r, col: c });
      }
    }
    const shuffledCells = shuffle(cellCoords);
    let visibleIndex = 0;
    const placedBoxes = [];

    return tileItems.map((tile, index) => {
      const placedSlot = currentSlots.findIndex((slotId) => slotId === tile.id);
      if (placedSlot >= 0 && slotRects[placedSlot]) {
        const rect = slotRects[placedSlot];
        return {
          ...tile,
          slotIndex: placedSlot,
          x: rect.centerX - tileWidthPx / 2,
          y: rect.centerY - TILE_HEIGHT / 2,
        };
      }
      const pickedCell = shuffledCells[visibleIndex] || {
        row: Math.floor(visibleIndex / cols),
        col: visibleIndex % cols,
      };
      const row = pickedCell.row;
      const col = pickedCell.col;
      visibleIndex += 1;
      const baseX = leftPad + col * (desiredTileWidth + gapX);
      const baseY = topBase + row * (TILE_HEIGHT + gapY);
      let x = baseX;
      let y = baseY;

      const minX = leftPad;
      const maxX = Math.max(minX, leftPad + usableWidth - desiredTileWidth);
      const minY = topBase;
      const maxY = Math.max(minY, boundedHeight - TILE_HEIGHT - 10);

      const overlaps = (nextX, nextY) => placedBoxes.some((box) => (
        nextX < box.x + box.w + 6
        && nextX + desiredTileWidth + 6 > box.x
        && nextY < box.y + box.h + 6
        && nextY + TILE_HEIGHT + 6 > box.y
      ));
      if (overlaps(x, y)) {
        x = Math.min(maxX, Math.max(minX, baseX));
        y = Math.min(maxY, Math.max(minY, baseY));
      }
      placedBoxes.push({ x, y, w: desiredTileWidth, h: TILE_HEIGHT });
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
    tilesRef.current = tiles;
  }, [tiles]);

  useEffect(() => {
    slotsRef.current = slots;
  }, [slots]);

  useEffect(() => {
    const baseTiles = shuffle(
      targetTokens.map((text, idx) => ({
        id: `tile-${idx}`,
        text,
        targetIndex: idx,
        angle: 0,
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
    setPlaygroundHeight(112);
    setSelectedSlotIndex(null);
    setDragTileId(null);
    setHoverSlotIndex(null);
    setSnapSlotIndex(null);
    setTileMotionMap({});
    setFlyTokens([]);
    revealInProgressRef.current = false;
    flyTimeoutsRef.current.forEach((timeoutId) => clearTimeout(timeoutId));
    flyTimeoutsRef.current = [];
    finishedRef.current = false;
    startAtRef.current = Date.now();
    if (autoNextRef.current) {
      clearTimeout(autoNextRef.current);
      autoNextRef.current = null;
    }
  }, [card?.id, answer, timerMs, targetTokens.length, resetSignal]);

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
    const isCorrect = normalizeForCompare(built, type) === normalizeForCompare(answer, type);
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
    if (autoAdvance && !isExternallyPaused) {
      const delayMs = isCorrect ? AUTO_NEXT_DELAY_OK_MS : AUTO_NEXT_DELAY_FAIL_MS;
      autoNextRef.current = setTimeout(() => {
        onNext?.();
      }, delayMs);
    }
  }, [status, hintsUsed, onRoundResult, autoAdvance, onNext, isExternallyPaused]);

  useEffect(() => {
    if (timerMs === null || isFinished) return;
    if (isExternallyPaused) return;
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
  }, [timerMs, timeLeftMs, isFinished, isExternallyPaused]);

  useEffect(() => {
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      if (autoNextRef.current) clearTimeout(autoNextRef.current);
      if (snapSlotTimeoutRef.current) clearTimeout(snapSlotTimeoutRef.current);
      Object.values(tileMotionTimeoutsRef.current).forEach((timeoutId) => clearTimeout(timeoutId));
      flyTimeoutsRef.current.forEach((timeoutId) => clearTimeout(timeoutId));
    };
  }, []);

  const markTileMotion = (tileId, motion) => {
    if (!tileId) return;
    setTileMotionMap((prev) => ({ ...prev, [tileId]: motion }));
    if (tileMotionTimeoutsRef.current[tileId]) {
      clearTimeout(tileMotionTimeoutsRef.current[tileId]);
    }
    tileMotionTimeoutsRef.current[tileId] = setTimeout(() => {
      setTileMotionMap((prev) => {
        if (!prev[tileId]) return prev;
        const next = { ...prev };
        delete next[tileId];
        return next;
      });
      delete tileMotionTimeoutsRef.current[tileId];
    }, 420);
  };

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
          tile.x = slotRect.centerX - tileWidthPx / 2;
          tile.y = slotRect.centerY - TILE_HEIGHT / 2;
        }
        nextSlots[slotIndex] = tile.id;
        return nextSlots;
      });
      return nextTiles;
    });
    setSnapSlotIndex(slotIndex);
    if (snapSlotTimeoutRef.current) clearTimeout(snapSlotTimeoutRef.current);
    snapSlotTimeoutRef.current = setTimeout(() => {
      setSnapSlotIndex(null);
    }, 260);
  };

  const returnTileToHome = (tileId) => {
    markTileMotion(tileId, 'return');
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
    const slotIndex = findSlotByPoint(pointerX, pointerY);
    setHoverSlotIndex(slotIndex >= 0 ? slotIndex : null);
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
    if (selectedSlotIndex !== null) {
      moveTileToSlot(tileId, selectedSlotIndex);
      setSelectedSlotIndex(null);
      return;
    }
    if (tile.slotIndex !== null) {
      setSelectedSlotIndex(tile.slotIndex);
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
    setDragTileId(null);
    setHoverSlotIndex(null);
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
    setDragTileId(tile.id);
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const onSlotClick = (slotIndex) => {
    if (isFinished) return;
    const tileId = slots[slotIndex];
    if (selectedSlotIndex === null) {
      if (tileId) {
        // Tap on a filled slot removes this tile back to selection area.
        returnTileToHome(tileId);
      }
      return;
    }
    if (selectedSlotIndex === slotIndex) {
      setSelectedSlotIndex(null);
      return;
    }
    const selectedTileId = slots[selectedSlotIndex];
    if (!selectedTileId) {
      setSelectedSlotIndex(null);
      return;
    }
    if (!tileId) {
      moveTileToSlot(selectedTileId, slotIndex);
      setSelectedSlotIndex(null);
      return;
    }
    // Swap two filled slots.
    setTiles((prevTiles) => {
      const nextTiles = prevTiles.map((item) => ({ ...item }));
      setSlots((prevSlots) => {
        const nextSlots = [...prevSlots];
        const firstTile = nextTiles.find((item) => item.id === selectedTileId);
        const secondTile = nextTiles.find((item) => item.id === tileId);
        if (!firstTile || !secondTile) return prevSlots;
        nextSlots[selectedSlotIndex] = secondTile.id;
        nextSlots[slotIndex] = firstTile.id;
        firstTile.slotIndex = slotIndex;
        secondTile.slotIndex = selectedSlotIndex;
        const firstRect = slotRects[slotIndex];
        const secondRect = slotRects[selectedSlotIndex];
        if (firstRect) {
          firstTile.x = firstRect.centerX - tileWidthPx / 2;
          firstTile.y = firstRect.centerY - TILE_HEIGHT / 2;
        }
        if (secondRect) {
          secondTile.x = secondRect.centerX - tileWidthPx / 2;
          secondTile.y = secondRect.centerY - TILE_HEIGHT / 2;
        }
        return nextSlots;
      });
      return nextTiles;
    });
    setSelectedSlotIndex(null);
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
    const isCorrect = normalizeForCompare(built, type) === normalizeForCompare(answer, type);
    setStatus(isCorrect ? 'correct' : 'wrong');
  };

  const scheduleTimeout = (callback, delayMs) => {
    const timeoutId = setTimeout(callback, delayMs);
    flyTimeoutsRef.current.push(timeoutId);
  };

  const revealCorrectArrangementAnimated = () => {
    if (revealInProgressRef.current) return;
    if (!slotRects.length || slotRects.some((item) => !item)) return;
    const currentTiles = Array.isArray(tilesRef.current) ? tilesRef.current : [];
    if (!currentTiles.length) return;
    revealInProgressRef.current = true;
    setSelectedSlotIndex(null);
    setHoverSlotIndex(null);
    setDragTileId(null);

    const plan = [];
    targetTokens.forEach((_token, targetIndex) => {
      const tile = currentTiles.find((item) => item.targetIndex === targetIndex);
      if (!tile) return;
      if (tile.slotIndex === targetIndex) return;
      const sourceRect = tile.slotIndex !== null && slotRects[tile.slotIndex]
        ? slotRects[tile.slotIndex]
        : null;
      const targetRect = slotRects[targetIndex];
      if (!targetRect) return;
      const fromX = sourceRect
        ? sourceRect.left + Math.max(0, (sourceRect.width - tileWidthPx) / 2)
        : Number(tile.x || 0);
      const fromY = sourceRect
        ? sourceRect.top + Math.max(0, (sourceRect.height - TILE_HEIGHT) / 2)
        : Number(tile.y || 0);
      const toX = targetRect.left + Math.max(0, (targetRect.width - tileWidthPx) / 2);
      const toY = targetRect.top + Math.max(0, (targetRect.height - TILE_HEIGHT) / 2);
      plan.push({
        id: `fly-${tile.id}-${targetIndex}-${Date.now()}-${Math.random().toString(16).slice(2, 7)}`,
        tileId: tile.id,
        targetIndex,
        text: tile.text,
        fromX,
        fromY,
        toX,
        toY,
      });
    });
    if (!plan.length) return;

    plan.forEach((item, index) => {
      const startDelay = index * 120;
      const settleDelay = startDelay + 480;
      const cleanupDelay = startDelay + 680;
      scheduleTimeout(() => {
        setFlyTokens((prev) => [...prev, {
          ...item,
          active: false,
        }]);
        scheduleTimeout(() => {
          setFlyTokens((prev) => prev.map((fly) => (
            fly.id === item.id ? { ...fly, active: true } : fly
          )));
        }, 30);
      }, startDelay);
      scheduleTimeout(() => {
        moveTileToSlot(item.tileId, item.targetIndex);
      }, settleDelay);
      scheduleTimeout(() => {
        setFlyTokens((prev) => prev.filter((fly) => fly.id !== item.id));
      }, cleanupDelay);
    });
  };

  useEffect(() => {
    if (status !== 'timeout') return;
    scheduleTimeout(() => {
      revealCorrectArrangementAnimated();
    }, 120);
  }, [status, slotRects, targetTokens.length]);

  const displayAnswer = type === 'PHRASE' ? targetTokens.join(' ') : targetTokens.join('');
  const visibleTiles = tiles.filter((tile) => tile.slotIndex === null);
  const timerSeconds = timeLeftMs === null ? null : Math.ceil(timeLeftMs / 1000);
  const text = {
    promptFallback: labels.promptFallback || 'Соберите ответ',
    hint: labels.hint || 'Подсказка',
    check: labels.check || 'Проверить',
    next: labels.next || 'Дальше',
    correct: labels.correct || 'Верно!',
    wrong: labels.wrong || 'Неверно',
    timeout: labels.timeout || 'Время вышло',
    correctAnswer: labels.correctAnswer || 'Правильный ответ',
    hintsUsed: labels.hintsUsed || 'Подсказок',
  };

  return (
    <div className={`blocks-trainer ${status !== 'idle' ? `is-${status}` : ''}`}>
      <div className="blocks-section blocks-section-task">
        <div className="blocks-head">
          <div className="blocks-prompt-wrap">
            <div className="blocks-prompt">{prompt || text.promptFallback}</div>
          </div>
          {timerSeconds !== null && (
            <div className={`blocks-timer ${timerSeconds <= 3 ? 'is-danger' : ''}`}>{timerSeconds}s</div>
          )}
        </div>
      </div>

      <div className="blocks-section blocks-section-result">
        <div
          className={[
            'blocks-target',
            isWordMode ? 'is-word' : '',
          ].filter(Boolean).join(' ')}
          ref={slotsRowRef}
          style={isWordMode ? {
            '--word-slot-min-width': `${wordSlotSizing.minWidth}px`,
            '--word-slot-min-height': `${wordSlotSizing.minHeight}px`,
            '--word-slot-font-size': `${wordSlotSizing.fontSize}px`,
          } : undefined}
        >
          {targetTokens.map((_token, idx) => {
            const tileId = slots[idx];
            const placed = tiles.find((item) => item.id === tileId);
            return (
              <div
                key={`slot-${idx}`}
                ref={(el) => { slotRefs.current[idx] = el; }}
                className={[
                  'blocks-slot',
                  placed ? 'is-filled' : '',
                  selectedSlotIndex === idx ? 'is-selected' : '',
                  hoverSlotIndex === idx ? 'is-hovered' : '',
                  snapSlotIndex === idx ? 'is-snapping' : '',
                ].filter(Boolean).join(' ')}
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
      </div>

      <div className="blocks-section blocks-section-pool">
        <div
          className={[
            'blocks-playground',
            dragTileId ? 'is-dragging' : '',
            visibleTiles.length === 0 ? 'is-empty' : '',
          ].filter(Boolean).join(' ')}
          ref={containerRef}
          style={{ height: `${playgroundHeight}px` }}
          onPointerMove={onPointerMove}
          onPointerUp={onPointerUp}
          onPointerCancel={onPointerUp}
        >
          {flyTokens.map((fly) => (
            <div
              key={fly.id}
              className={`blocks-fly-token ${fly.active ? 'is-active' : ''}`}
              style={{
                '--from-x': `${fly.fromX}px`,
                '--from-y': `${fly.fromY}px`,
                '--to-x': `${fly.toX}px`,
                '--to-y': `${fly.toY}px`,
                width: type === 'WORD' ? '60px' : undefined,
                fontSize: type === 'WORD' ? '22px' : undefined,
              }}
            >
              {fly.text}
            </div>
          ))}
          {visibleTiles.map((tile) => (
            <button
              key={tile.id}
              type="button"
              className={[
                'blocks-tile',
                `color-${tile.targetIndex % 6}`,
                `shape-${tile.targetIndex % 5}`,
                dragTileId === tile.id ? 'is-dragging' : '',
                tileMotionMap[tile.id] === 'return' ? 'is-returning' : '',
              ].filter(Boolean).join(' ')}
              style={{
                transform: `translate3d(${tile.x}px, ${tile.y}px, 0) rotate(${dragTileId === tile.id ? tile.angle * 0.25 : tile.angle}deg)`,
                width: type === 'WORD' ? '60px' : undefined,
                fontSize: type === 'WORD' ? '22px' : undefined,
              }}
              onPointerDown={(event) => onTilePointerDown(event, tile)}
              disabled={isFinished}
            >
              {tile.text}
            </button>
          ))}
        </div>
      </div>

      <div className="blocks-footer">
        <button type="button" className="secondary-button" onClick={applyHint} disabled={isFinished}>
          {text.hint}
        </button>
        {!isFinished && (
          <button
            type="button"
            className="primary-button"
            onClick={forceCheck}
            disabled={!allSlotsFilled}
          >
            {text.check}
          </button>
        )}
        {isFinished && (
          <button type="button" className="primary-button" onClick={() => onNext?.()}>
            {text.next}
          </button>
        )}
      </div>

      {isFinished && (
        <div className={`blocks-result ${status === 'correct' ? 'ok' : 'bad'}`}>
          <div className="blocks-result-head">
            <div className="blocks-result-badge">
              {status === 'correct' ? text.correct : status === 'timeout' ? text.timeout : text.wrong}
            </div>
            <div className="blocks-result-meta">
              <span className="blocks-result-meta-label">{text.hintsUsed}</span>
              <strong>{hintsUsed}</strong>
            </div>
          </div>
          <div className="blocks-result-answer">
            <div className="blocks-result-label">{text.correctAnswer}</div>
            <div className="blocks-result-value">{displayAnswer}</div>
          </div>
        </div>
      )}
    </div>
  );
}
