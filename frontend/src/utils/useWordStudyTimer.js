import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  FLUSH_INTERVAL_MS,
  TICK_MS,
  clampStep,
  committedAfterFlush,
  displaySeconds as computeDisplaySeconds,
  shouldCountNow,
} from './wordStudyRules';

/**
 * Время активной учёбы словам — один счётчик на все тренировки слов.
 *
 * Правила (см. обсуждение с владельцем):
 *  1. Секунда засчитывается, только если ОДНОВРЕМЕННО: вкладка видима, окно в
 *     фокусе, открыт экран тренировки слов и было касание за последние 30 секунд.
 *  2. Как только любое условие пропало — отрезок закрывается и уходит на сервер.
 *  3. Возврат продолжает дневную сумму, а не начинает заново.
 *  4. Смена режима тренировки — не событие: сумма одна на все режимы.
 *
 * Почему так, а не «вычесть фон потом»: прошлая версия считала время всегда, а
 * потом пыталась угадать отсутствие по провалам в тиках. Провала нет — когда
 * система таймер не заморозила (экран погас, а webview жив), и тогда время шло
 * впустую. Здесь проверка ПОЛОЖИТЕЛЬНАЯ: не доказано, что человек за экраном, —
 * не считаем.
 *
 * Почему сумма на сервере: значение в localStorage терялось целиком, если
 * приложение убивали без события паузы. Открытый отрезок досылается каждые
 * FLUSH_INTERVAL_MS, поэтому потеря ограничена этим интервалом.
 */


const getLocalDayKey = () => {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
};

const makeSegmentId = () => {
  try {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
  } catch (_error) { /* ниже — запасной вариант */ }
  return `seg_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
};

const readMirror = (key) => {
  if (!key || typeof window === 'undefined') return { daySeconds: 0, pending: {} };
  try {
    const parsed = JSON.parse(window.localStorage.getItem(key) || 'null');
    return {
      daySeconds: Math.max(0, Math.floor(Number(parsed?.day_seconds || 0))),
      pending: parsed?.pending && typeof parsed.pending === 'object' ? parsed.pending : {},
    };
  } catch (_error) {
    return { daySeconds: 0, pending: {} };
  }
};

const writeMirror = (key, daySeconds, pending) => {
  if (!key || typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(key, JSON.stringify({
      day_seconds: Math.max(0, Math.floor(Number(daySeconds || 0))),
      pending: pending || {},
      updated_at: new Date().toISOString(),
    }));
  } catch (_error) {
    // Зеркало — только страховка на офлайн; сумма всё равно живёт на сервере.
  }
};

export default function useWordStudyTimer({
  initData,
  userId,
  active,
  telegramApp,
  surface = 'words',
  enabled = true,
}) {
  const [displaySeconds, setDisplaySeconds] = useState(0);
  const [counting, setCounting] = useState(false);

  const stableUserId = String(userId || 'anon').trim() || 'anon';
  const [dayKey, setDayKey] = useState(getLocalDayKey);
  const mirrorKey = useMemo(
    () => `word_study_time_${surface}_${stableUserId}_${dayKey}`,
    [dayKey, stableUserId, surface]
  );

  const committedRef = useRef(0);      // дневная сумма без текущего отрезка
  const segmentRef = useRef(null);     // { id, activeMs, startedAt, sentSeconds }
  const lastInteractionRef = useRef(Date.now());
  const mirrorKeyRef = useRef(mirrorKey);
  const pendingRef = useRef({});
  const displayRef = useRef(0);
  const initDataRef = useRef(initData);
  const activeRef = useRef(Boolean(active));
  const enabledRef = useRef(Boolean(enabled));
  const focusEverTrueRef = useRef(false);
  const telegramInactiveRef = useRef(false);
  const flushInFlightRef = useRef(false);
  const flushRef = useRef(null);

  initDataRef.current = initData;
  activeRef.current = Boolean(active);
  enabledRef.current = Boolean(enabled);

  const publish = useCallback(() => {
    const guarded = computeDisplaySeconds({
      committedSeconds: committedRef.current,
      openSegmentMs: segmentRef.current ? segmentRef.current.activeMs : 0,
      previousDisplaySeconds: displayRef.current,
    });
    displayRef.current = guarded;
    setDisplaySeconds(guarded);
  }, []);

  const persistMirror = useCallback(() => {
    const segmentSeconds = segmentRef.current
      ? Math.max(0, Math.floor(segmentRef.current.activeMs / 1000))
      : 0;
    writeMirror(mirrorKeyRef.current, committedRef.current + segmentSeconds, pendingRef.current);
  }, []);

  const sendSegments = useCallback(async (segments, { keepalive = false } = {}) => {
    const auth = initDataRef.current;
    if (!auth || !Array.isArray(segments) || segments.length === 0) return null;
    const response = await fetch('/api/study-time/segment', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      keepalive,
      body: JSON.stringify({ initData: auth, surface, segments }),
    });
    if (!response.ok) throw new Error(`study-time segment sync failed: ${response.status}`);
    const data = await response.json();
    return Math.max(0, Math.floor(Number(data?.day_seconds || 0)));
  }, [surface]);

  /** Досылает открытый отрезок и всё, что не ушло раньше. */
  const flush = useCallback(async ({ closeSegment = false, keepalive = false } = {}) => {
    const segment = segmentRef.current;
    const segmentSeconds = segment ? Math.max(0, Math.floor(segment.activeMs / 1000)) : 0;

    if (closeSegment && segment) {
      committedRef.current += segmentSeconds;
      segmentRef.current = null;
    }

    // Один segment_id — одна запись в посылке: неудавшаяся прошлая досылка того
    // же отрезка уже лежит в pending, и слать его дважды незачем.
    const batchById = new Map();
    Object.entries(pendingRef.current).forEach(([segmentId, entry]) => {
      batchById.set(segmentId, {
        segment_id: segmentId,
        active_ms: entry.activeMs,
        started_at: entry.startedAt,
        local_day: entry.localDay,
      });
    });
    if (segment && segmentSeconds > 0) {
      batchById.set(segment.id, {
        segment_id: segment.id,
        active_ms: segment.activeMs,
        started_at: segment.startedAt,
        local_day: segment.localDay,
      });
    }
    const batch = Array.from(batchById.values());
    if (batch.length === 0) {
      persistMirror();
      publish();
      return;
    }

    // Отрезок помним локально до подтверждения сервера: обрыв связи не должен
    // стирать уже отсиженное время.
    if (segment && segmentSeconds > 0) {
      pendingRef.current[segment.id] = {
        activeMs: segment.activeMs,
        startedAt: segment.startedAt,
        localDay: segment.localDay,
      };
    }
    persistMirror();
    publish();

    if (flushInFlightRef.current && !keepalive) return;
    flushInFlightRef.current = true;
    try {
      const serverTotal = await sendSegments(batch, { keepalive });
      const batchIds = new Set(batch.map((entry) => entry.segment_id));
      batchIds.forEach((id) => { delete pendingRef.current[id]; });
      if (serverTotal !== null && Number.isFinite(serverTotal)) {
        const openSegment = segmentRef.current;
        const openSeconds = openSegment ? Math.max(0, Math.floor(openSegment.activeMs / 1000)) : 0;
        const sentForOpenSegment = openSegment && batchIds.has(openSegment.id) ? segmentSeconds : 0;
        committedRef.current = committedAfterFlush({
          serverDayTotal: serverTotal,
          sentSecondsForOpenSegment: sentForOpenSegment,
        });
        if (openSegment) openSegment.sentSeconds = openSeconds;
      }
      persistMirror();
      publish();
    } catch (_error) {
      // Останется в pending и уйдёт со следующей досылкой.
    } finally {
      flushInFlightRef.current = false;
    }
  }, [persistMirror, publish, sendSegments]);

  flushRef.current = flush;

  const noteActivity = useCallback(() => {
    lastInteractionRef.current = Date.now();
  }, []);

  // Загрузка дневной суммы при входе/смене пользователя/смене дня.
  useEffect(() => {
    let cancelled = false;
    // Ключ меняется при смене дня или когда наконец узнали id пользователя.
    // Открытый отрезок принадлежит СТАРОМУ ключу — досылаем его до переключения.
    if (segmentRef.current) {
      void flushRef.current?.({ closeSegment: true });
    }
    mirrorKeyRef.current = mirrorKey;
    const mirror = readMirror(mirrorKey);
    committedRef.current = mirror.daySeconds;
    pendingRef.current = mirror.pending || {};
    segmentRef.current = null;
    displayRef.current = 0;
    setDisplaySeconds(mirror.daySeconds);
    displayRef.current = mirror.daySeconds;

    if (!initData || !enabled) return () => { cancelled = true; };

    (async () => {
      try {
        // Сначала доносим то, что не ушло в прошлый раз, — иначе серверная
        // сумма вернётся без него и локальное время пропадёт.
        const leftovers = Object.entries(pendingRef.current).map(([segmentId, entry]) => ({
          segment_id: segmentId,
          active_ms: entry.activeMs,
          started_at: entry.startedAt,
          local_day: entry.localDay,
        }));
        if (leftovers.length > 0) {
          const total = await sendSegments(leftovers);
          if (total !== null) {
            pendingRef.current = {};
            if (!cancelled) {
              committedRef.current = total;
              publish();
              persistMirror();
            }
            return;
          }
        }
        const query = new URLSearchParams({ initData, surface, local_day: dayKey });
        const response = await fetch(`/api/study-time/day?${query.toString()}`);
        if (!response.ok) return;
        const data = await response.json();
        const serverSeconds = Math.max(0, Math.floor(Number(data?.day_seconds || 0)));
        if (cancelled) return;
        committedRef.current = Math.max(committedRef.current, serverSeconds);
        publish();
        persistMirror();
      } catch (_error) {
        // Офлайн — работаем от зеркала, догоним на следующей досылке.
      }
    })();

    return () => { cancelled = true; };
  }, [dayKey, enabled, initData, mirrorKey, persistMirror, publish, sendSegments, surface]);

  // Признаки жизни. scroll ловим в capture: он не всплывает из контейнеров.
  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    const events = ['pointerdown', 'pointerup', 'touchstart', 'keydown', 'wheel'];
    events.forEach((name) => window.addEventListener(name, noteActivity, { passive: true }));
    document.addEventListener('scroll', noteActivity, { capture: true, passive: true });
    return () => {
      events.forEach((name) => window.removeEventListener(name, noteActivity));
      document.removeEventListener('scroll', noteActivity, { capture: true });
    };
  }, [noteActivity]);

  // Telegram сообщает о сворачивании даже там, где DOM-события врут (iOS).
  useEffect(() => {
    if (!telegramApp || typeof telegramApp.onEvent !== 'function') return undefined;
    const onDeactivated = () => { telegramInactiveRef.current = true; };
    const onActivated = () => {
      telegramInactiveRef.current = false;
      noteActivity();
    };
    try {
      telegramApp.onEvent('deactivated', onDeactivated);
      telegramApp.onEvent('activated', onActivated);
    } catch (_error) {
      return undefined;
    }
    return () => {
      try {
        telegramApp.offEvent('deactivated', onDeactivated);
        telegramApp.offEvent('activated', onActivated);
      } catch (_error) { /* клиент постарше — обойдёмся без этого сигнала */ }
    };
  }, [noteActivity, telegramApp]);

  const shouldCount = useCallback(() => {
    if (typeof document === 'undefined') return false;
    let hasFocus;
    if (typeof document.hasFocus === 'function') {
      hasFocus = document.hasFocus();
      if (hasFocus) focusEverTrueRef.current = true;
    }
    return shouldCountNow({
      enabled: enabledRef.current,
      active: activeRef.current,
      visibility: document.visibilityState,
      telegramInactive: telegramInactiveRef.current,
      hasFocus,
      focusEverTrue: focusEverTrueRef.current,
      lastInteractionAt: lastInteractionRef.current,
      now: Date.now(),
    });
  }, []);

  // Главный такт.
  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    let lastTickMs = Date.now();
    let sinceFlushMs = 0;

    const intervalId = window.setInterval(() => {
      const now = Date.now();
      const rawDelta = Math.max(0, now - lastTickMs);
      lastTickMs = now;

      const currentDayKey = getLocalDayKey();
      if (currentDayKey !== dayKey) {
        void flush({ closeSegment: true });
        setDayKey(currentDayKey);
        return;
      }

      if (!shouldCount()) {
        if (segmentRef.current) {
          setCounting(false);
          void flush({ closeSegment: true });
          sinceFlushMs = 0;
        }
        return;
      }

      if (!segmentRef.current) {
        segmentRef.current = {
          id: makeSegmentId(),
          activeMs: 0,
          startedAt: new Date(now).toISOString(),
          localDay: currentDayKey,
          sentSeconds: 0,
        };
        sinceFlushMs = 0;
        setCounting(true);
      }
      segmentRef.current.activeMs += clampStep(rawDelta);
      publish();

      sinceFlushMs += rawDelta;
      if (sinceFlushMs >= FLUSH_INTERVAL_MS) {
        sinceFlushMs = 0;
        void flush();
      }
    }, TICK_MS);

    return () => window.clearInterval(intervalId);
  }, [dayKey, flush, publish, shouldCount]);

  // Закрываем отрезок сразу, как только экран тренировки покинут.
  useEffect(() => {
    if (active) {
      noteActivity();
      return undefined;
    }
    setCounting(false);
    void flush({ closeSegment: true });
    return undefined;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active]);

  // Уход из приложения: успеть отправить отрезок «на выходе».
  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const closeNow = () => {
      if (!segmentRef.current) return;
      setCounting(false);
      void flush({ closeSegment: true, keepalive: true });
    };
    const onVisibility = () => {
      if (document.visibilityState === 'hidden') closeNow();
      else noteActivity();
    };
    document.addEventListener('visibilitychange', onVisibility);
    window.addEventListener('pagehide', closeNow);
    window.addEventListener('blur', closeNow);
    window.addEventListener('focus', noteActivity);
    return () => {
      document.removeEventListener('visibilitychange', onVisibility);
      window.removeEventListener('pagehide', closeNow);
      window.removeEventListener('blur', closeNow);
      window.removeEventListener('focus', noteActivity);
    };
  }, [flush, noteActivity]);

  return { daySeconds: displaySeconds, counting, noteActivity };
}
