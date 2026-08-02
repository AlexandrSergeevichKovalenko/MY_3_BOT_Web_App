/**
 * Правила счёта времени учёбы словам — без React и без DOM, чтобы их можно было
 * прогнать тестом. Хук useWordStudyTimer только подставляет сюда состояние.
 */

export const IDLE_TIMEOUT_MS = 30 * 1000;
export const TICK_MS = 1000;
// Тик может опоздать (дросселирование, занятый поток). Больше двух секунд за
// один тик — это уже не время у экрана.
export const MAX_STEP_MS = 2000;
export const FLUSH_INTERVAL_MS = 10 * 1000;

/**
 * Секунда засчитывается, только если ОДНОВРЕМЕННО верно всё. Проверка
 * положительная: не доказано, что человек за экраном, — не считаем.
 *
 * hasFocus: есть клиенты, где document.hasFocus() всегда false. Доверяем ему
 * только после того, как он хоть раз сказал true (focusEverTrue), иначе счётчик
 * не пошёл бы вообще.
 */
export function shouldCountNow({
  enabled,
  active,
  visibility,
  telegramInactive,
  hasFocus,
  focusEverTrue,
  lastInteractionAt,
  now,
  idleTimeoutMs = IDLE_TIMEOUT_MS,
}) {
  if (!enabled || !active) return false;
  if (visibility !== 'visible') return false;
  if (telegramInactive) return false;
  if (hasFocus === false && focusEverTrue) return false;
  if (now - lastInteractionAt > idleTimeoutMs) return false;
  return true;
}

/** Сколько миллисекунд тика реально засчитать. */
export function clampStep(rawDeltaMs, maxStepMs = MAX_STEP_MS) {
  const safe = Math.max(0, Number(rawDeltaMs) || 0);
  return Math.min(safe, maxStepMs);
}

/**
 * Дневная сумма без открытого отрезка после ответа сервера.
 * Сервер возвращает сумму ВКЛЮЧАЯ отправленную часть открытого отрезка —
 * её надо вычесть, иначе она посчиталась бы дважды вместе с живой частью.
 */
export function committedAfterFlush({ serverDayTotal, sentSecondsForOpenSegment }) {
  return Math.max(0, Math.floor(Number(serverDayTotal) || 0) - Math.max(0, Math.floor(Number(sentSecondsForOpenSegment) || 0)));
}

/** Что показывает бейдж: дневная сумма плюс живая часть открытого отрезка. */
export function displaySeconds({ committedSeconds, openSegmentMs, previousDisplaySeconds = 0 }) {
  const open = Math.max(0, Math.floor((Number(openSegmentMs) || 0) / 1000));
  const next = Math.max(0, Math.floor(Number(committedSeconds) || 0)) + open;
  // Внутри дня показание не должно прыгать назад: ответ сервера может прийти с
  // опозданием, а человек смотрит на секундомер.
  return Math.max(Math.max(0, Math.floor(Number(previousDisplaySeconds) || 0)), next);
}
