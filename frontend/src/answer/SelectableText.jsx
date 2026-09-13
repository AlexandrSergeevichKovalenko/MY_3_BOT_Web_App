import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  buildPhraseSelection,
  getWordElementByPoint,
  normalizeSelectionText,
  segmentText,
  sentenceWords,
  wordIndexInSentence,
} from '../utils/textSegments';

/**
 * Немецкий текст, который можно трогать пальцем — как в читалке и в субтитрах.
 *
 * Три жеста, и все три ведут себя ровно так же, как там (владелец 10.09.2026: «тот же
 * принцип»), поэтому пороги здесь ТЕ ЖЕ ЧИСЛА, что в App.jsx у читалки:
 *   • тап по слову            → слово;
 *   • двойной тап (450 мс)    → предложение целиком;
 *   • удержание 340 мс + вести пальцем → фраза, слово за словом, внутри предложения.
 *
 * Почему компонент отдельный, а не общий с читалкой: интерактивы монтируются своим
 * react-рутом (main.jsx bootstrapAnswerOverlay), App.jsx в их бандл не попадает, а у
 * читалкиных жестов к тому же примешаны аудио и листалка страниц. Общей сделана резка
 * текста (utils/textSegments.js) — она чистая; жесты у каждой поверхности свои.
 *
 * Наружу отдаёт ОДНО событие onSelect({ text, kind, anchor }), где kind — 'word' |
 * 'phrase' | 'sentence', anchor — точка экрана, у которой показывать плашку.
 */

const HOLD_MS = 340;          // столько палец должен лежать, чтобы начать выделение фразы
const HOLD_SLOP_PX = 10;      // уехал раньше — это прокрутка, жест не наш
const DOUBLE_TAP_MS = 450;    // 320 мс на телефоне не поймать (замер читалки)
const CLICK_SUPPRESS_MS = 420; // клик, прилетающий следом за жестом, гасим

const EMPTY_GESTURE = {
  armed: false, active: false, sentenceId: '', anchorIndex: -1, currentIndex: -1,
  startX: 0, startY: 0,
};

export default function SelectableText({ text, lang = 'de', onSelect, haptic, className = '' }) {
  const rootRef = useRef(null);
  const gestureRef = useRef({ ...EMPTY_GESTURE });
  const holdTimerRef = useRef(null);
  const suppressClickRef = useRef(0);
  const lastTapRef = useRef({ time: 0, sid: '', count: 0 });
  const [dragMeta, setDragMeta] = useState(null);

  const sentences = useMemo(() => segmentText(text, lang), [text, lang]);
  const sentenceMap = useMemo(() => {
    const map = new Map();
    sentences.forEach((s) => map.set(s.sid, s));
    return map;
  }, [sentences]);
  const highlighted = useMemo(() => new Set(dragMeta?.wids || []), [dragMeta]);

  const buzz = () => { try { haptic?.('selection'); } catch (_e) { /* вибро — не повод падать */ } };

  const clearHold = () => {
    if (holdTimerRef.current) { clearTimeout(holdTimerRef.current); holdTimerRef.current = null; }
  };
  const resetGesture = () => {
    clearHold();
    gestureRef.current = { ...EMPTY_GESTURE };
    setDragMeta(null);
  };

  const emit = (payload, point) => {
    if (!payload?.text || typeof onSelect !== 'function') return;
    onSelect({
      ...payload,
      anchor: { x: Number(point?.clientX || 0), y: Number(point?.clientY || 0) },
    });
  };

  // ── палец лёг на слово: взводим удержание ───────────────────────────────────
  const onWordTouchStart = (event) => {
    const touch = event?.touches?.[0];
    if (!touch) return;
    const wordEl = event.target instanceof Element ? event.target.closest('[data-wid][data-sid]') : null;
    if (!wordEl) return;
    const sid = String(wordEl.getAttribute('data-sid') || '').trim();
    const wid = String(wordEl.getAttribute('data-wid') || '').trim();
    const sentence = sentenceMap.get(sid);
    const anchorIndex = sentence ? wordIndexInSentence(sentence, wid) : -1;
    if (!sentence || anchorIndex < 0) return;
    clearHold();
    gestureRef.current = {
      armed: true, active: false, sentenceId: sid,
      anchorIndex, currentIndex: anchorIndex,
      startX: Number(touch.clientX || 0), startY: Number(touch.clientY || 0),
    };
    holdTimerRef.current = setTimeout(() => {
      holdTimerRef.current = null;
      const gesture = gestureRef.current;
      if (!gesture?.armed) return;
      gestureRef.current = { ...gesture, armed: false, active: true };
      setDragMeta(buildPhraseSelection(sentenceMap.get(gesture.sentenceId), gesture.anchorIndex, gesture.anchorIndex));
      buzz();
    }, HOLD_MS);
  };

  // Движение пальца слушаем ВРУЧНУЮ и непассивно. React вешает touchmove пассивно, а
  // пассивному слушателю браузер запрещает остановить прокрутку: страница уезжала прямо
  // во время выделения фразы (поймано на стенде 10.09.2026 — «Unable to preventDefault
  // inside passive event listener»). Тем же приёмом сделаны субтитры в App.jsx.
  const moveRef = useRef(null);
  useEffect(() => {
    const node = rootRef.current;
    if (!node) return undefined;
    const handler = (event) => { moveRef.current?.(event); };
    node.addEventListener('touchmove', handler, { passive: false });
    return () => node.removeEventListener('touchmove', handler);
  }, []);

  const onRootTouchMove = (event) => {
    const gesture = gestureRef.current;
    if (!gesture?.armed && !gesture?.active) return;
    const touch = event?.touches?.[0];
    if (!touch) return;
    const x = Number(touch.clientX || 0);
    const y = Number(touch.clientY || 0);
    if (!gesture.active) {
      const dx = x - Number(gesture.startX || 0);
      const dy = y - Number(gesture.startY || 0);
      if (Math.abs(dx) > HOLD_SLOP_PX || Math.abs(dy) > HOLD_SLOP_PX) {
        clearHold();
        gestureRef.current = { ...gesture, armed: false };
      }
      return;
    }
    // Выделение ведём сами — иначе страница уезжает под пальцем.
    if (event.cancelable) event.preventDefault();
    const wordEl = getWordElementByPoint(x, y);
    if (!wordEl) return;
    const sid = String(wordEl.getAttribute('data-sid') || '').trim();
    const wid = String(wordEl.getAttribute('data-wid') || '').trim();
    if (!sid || sid !== gesture.sentenceId) return;   // за границу предложения не тянем
    const sentence = sentenceMap.get(sid);
    const currentIndex = sentence ? wordIndexInSentence(sentence, wid) : -1;
    if (currentIndex < 0 || currentIndex === gesture.currentIndex) return;
    gestureRef.current = { ...gesture, currentIndex };
    const next = buildPhraseSelection(sentence, gesture.anchorIndex, currentIndex);
    setDragMeta(next);
    if (next) buzz();
  };
  moveRef.current = onRootTouchMove;

  const onRootTouchEnd = (event) => {
    const gesture = gestureRef.current;
    const meta = dragMeta;
    if (!gesture?.active) { resetGesture(); return; }
    const point = event?.changedTouches?.[0] || {};
    if (meta?.text) {
      emit({
        text: meta.text,
        kind: (meta.wids || []).length > 1 ? 'phrase' : 'word',
        sids: meta.sids, wids: meta.wids, start: meta.start, end: meta.end,
      }, point);
    }
    suppressClickRef.current = Date.now();
    resetGesture();
  };

  // ── тап и двойной тап ───────────────────────────────────────────────────────
  const onRootClick = (event) => {
    if (Date.now() - Number(suppressClickRef.current || 0) < CLICK_SUPPRESS_MS) return;
    const target = event?.target;
    if (!(target instanceof Element)) return;
    const wordEl = target.closest('[data-wid][data-sid]');
    if (!wordEl) return;
    const sid = String(wordEl.getAttribute('data-sid') || '').trim();
    const wid = String(wordEl.getAttribute('data-wid') || '').trim();
    const sentence = sentenceMap.get(sid);
    if (!sentence) return;
    const word = sentenceWords(sentence).find((t) => String(t.wid || '') === wid);
    if (!word) return;

    const now = Date.now();
    const last = lastTapRef.current;
    const fast = now - Number(last.time || 0) < DOUBLE_TAP_MS && last.sid === sid;
    const count = fast ? Number(last.count || 0) + 1 : 1;
    lastTapRef.current = { time: now, sid, count };

    if (count >= 2) {
      emit({
        text: normalizeSelectionText(sentence.text),
        kind: 'sentence',
        sids: [sid], wids: [], start: sentence.start, end: sentence.end,
      }, event);
      return;
    }
    emit({
      text: String(word.value || ''),
      kind: 'word',
      sids: [sid], wids: [wid], start: word.start, end: word.end,
    }, event);
  };

  // ── выделение мышью (планшет с клавиатурой, десктопный Telegram) ────────────
  const onRootMouseUp = (event) => {
    const root = rootRef.current;
    const selection = typeof window !== 'undefined' ? window.getSelection?.() : null;
    if (!root || !selection || selection.rangeCount === 0 || selection.isCollapsed) return;
    const range = selection.getRangeAt(0);
    if (range.commonAncestorContainer && !root.contains(range.commonAncestorContainer)) return;
    const picked = [];
    root.querySelectorAll('[data-wid][data-sid]').forEach((node) => {
      try { if (range.intersectsNode(node)) picked.push(node); } catch (_e) { /* чужой узел */ }
    });
    if (!picked.length) return;
    const sid = String(picked[0].getAttribute('data-sid') || '');
    const sentence = sentenceMap.get(sid);
    const text = normalizeSelectionText(String(selection.toString() || ''));
    if (!text || !sentence) return;
    const wids = picked.map((n) => String(n.getAttribute('data-wid') || '')).filter(Boolean);
    const sameSentence = picked.every((n) => String(n.getAttribute('data-sid') || '') === sid);
    suppressClickRef.current = Date.now();
    emit({
      text,
      kind: !sameSentence ? 'sentence' : (wids.length > 1 ? 'phrase' : 'word'),
      sids: [sid], wids,
      start: Number(picked[0].getAttribute('data-start') || 0),
      end: Number(picked[picked.length - 1].getAttribute('data-end') || 0),
    }, event);
  };

  if (!String(text || '').trim()) return null;

  return (
    <span
      ref={rootRef}
      className={`sel-text ${className}`.trim()}
      lang={lang}
      onTouchEnd={onRootTouchEnd}
      onTouchCancel={resetGesture}
      onClick={onRootClick}
      onMouseUp={onRootMouseUp}
    >
      {sentences.map((sentence) => (
        <span key={sentence.sid} className="sel-sentence" data-sid={sentence.sid}>
          {sentence.tokens.map((token, i) => {
            if (token.kind !== 'word') {
              return <span key={`${sentence.sid}-t-${i}`}>{token.value}</span>;
            }
            const wid = String(token.wid || '');
            return (
              <span
                key={wid}
                className={`sel-word${highlighted.has(wid) ? ' is-selected' : ''}`}
                data-wid={wid}
                data-sid={sentence.sid}
                data-start={token.start}
                data-end={token.end}
                onTouchStart={onWordTouchStart}
              >
                {token.value}
              </span>
            );
          })}
        </span>
      ))}
    </span>
  );
}
