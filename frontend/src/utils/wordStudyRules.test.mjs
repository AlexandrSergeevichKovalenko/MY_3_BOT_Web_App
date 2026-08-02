/**
 * Правила счёта времени учёбы словам. Запуск: npm run test:rules (из frontend/).
 *
 * Это те самые случаи, на которых счётчик ломался полгода:
 *  — заблокированный экран при открытой тренировке;
 *  — свёрнутое приложение;
 *  — телефон лежит разблокированный, человек не учится;
 *  — возврат должен продолжать дневную сумму, а не начинать с нуля.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  IDLE_TIMEOUT_MS,
  MAX_STEP_MS,
  clampStep,
  committedAfterFlush,
  displaySeconds,
  shouldCountNow,
} from './wordStudyRules.js';

const NOW = 1_800_000_000_000;

const state = (overrides = {}) => ({
  enabled: true,
  active: true,
  visibility: 'visible',
  telegramInactive: false,
  hasFocus: true,
  focusEverTrue: true,
  lastInteractionAt: NOW - 1000,
  now: NOW,
  ...overrides,
});

test('человек на тренировке и только что нажимал — считаем', () => {
  assert.equal(shouldCountNow(state()), true);
});

test('экран заблокирован: вкладка скрыта — не считаем', () => {
  assert.equal(shouldCountNow(state({ visibility: 'hidden' })), false);
});

test('приложение свёрнуто по сигналу Telegram — не считаем даже при visible', () => {
  assert.equal(shouldCountNow(state({ telegramInactive: true })), false);
});

test('окно потеряло фокус — не считаем', () => {
  assert.equal(shouldCountNow(state({ hasFocus: false })), false);
});

test('клиент, где hasFocus() всегда false, счёт не блокирует', () => {
  // Пока фокус ни разу не подтверждался, доверять ему нельзя: иначе на таком
  // клиенте счётчик не пошёл бы вообще.
  assert.equal(shouldCountNow(state({ hasFocus: false, focusEverTrue: false })), true);
  assert.equal(shouldCountNow(state({ hasFocus: undefined, focusEverTrue: false })), true);
});

test('телефон лежит: 30 секунд без касания — пауза', () => {
  assert.equal(shouldCountNow(state({ lastInteractionAt: NOW - IDLE_TIMEOUT_MS - 1 })), false);
  assert.equal(shouldCountNow(state({ lastInteractionAt: NOW - IDLE_TIMEOUT_MS + 1 })), true);
});

test('ушёл с экрана тренировки — не считаем', () => {
  assert.equal(shouldCountNow(state({ active: false })), false);
});

test('опоздавший тик не дарит время: шаг зажат', () => {
  assert.equal(clampStep(900), 900);
  assert.equal(clampStep(60_000), MAX_STEP_MS);
  assert.equal(clampStep(-5), 0);
  assert.equal(clampStep(undefined), 0);
});

test('после ответа сервера открытый отрезок не считается дважды', () => {
  // Сервер знает 120 секунд за день, из них 45 — уже отправленная часть
  // открытого отрезка. Значит «остальное» = 75, и живая часть ляжет поверх.
  assert.equal(committedAfterFlush({ serverDayTotal: 120, sentSecondsForOpenSegment: 45 }), 75);
  assert.equal(displaySeconds({ committedSeconds: 75, openSegmentMs: 47_000 }), 122);
});

test('возврат продолжает дневную сумму, а не начинает с нуля', () => {
  const dayTotalFromServer = 118; // утренние 1:58 из прошлого захода
  assert.equal(displaySeconds({ committedSeconds: dayTotalFromServer, openSegmentMs: 0 }), 118);
  assert.equal(displaySeconds({ committedSeconds: dayTotalFromServer, openSegmentMs: 3000 }), 121);
});

test('показание не прыгает назад внутри дня', () => {
  assert.equal(
    displaySeconds({ committedSeconds: 10, openSegmentMs: 0, previousDisplaySeconds: 118 }),
    118
  );
});
