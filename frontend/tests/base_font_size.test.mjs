// Базовый кегль экранов на `rem` (answer.css) считает скрипт, а не вёрстка: иначе на
// Android выезд клавиатуры ужимал ВЕСЬ экран (замер 16.09.2026 в быстром словаре —
// 18,46 → 13,91 px). Формула живёт в одном файле и применяется в двух местах —
// интерактивы (answer/fitCard.js) и быстрый словарь (dictionary/DictionaryOverlay.jsx).
//
// Числа здесь закреплены НАМЕРЕННО: они повторяют то, что раньше стояло в CSS
// (clamp(13px, 1.42vh + 0.4vw + 4.8px, 18.5px), а на широком экране — своя ветка).
// Если кто-то тронет формулу, экраны поедут молча — этот тест не даст.
import assert from 'node:assert/strict';
import test from 'node:test';
import { computeBaseFontSize } from '../src/answer/baseFontSize.js';

test('телефон: кегль тот же, что давал CSS до переезда формулы', () => {
  // iPhone 14, экран целиком: 1.42*8.44 + 0.4*3.90 + 4.8
  assert.equal(computeBaseFontSize(844, 390), 18.34);
  // Тот же телефон с клавиатурой: раньше кегль падал сюда — теперь сюда его никто не пустит,
  // но сама формула обязана считать по-прежнему.
  assert.equal(computeBaseFontSize(494, 390), 13.37);  // 1.42*4.94 + 0.4*3.90 + 4.8 = 13.3748
  // Маленький телефон.
  assert.equal(computeBaseFontSize(667, 375), 15.77);
});

test('потолок и пол зажимают величину', () => {
  assert.equal(computeBaseFontSize(2000, 500), 18.5);   // потолок телефона
  assert.equal(computeBaseFontSize(300, 320), 13);      // пол
});

test('широкий экран считается по своей ветке (порог 700×560)', () => {
  // Планшет: 0.9*10.24 + 0.7*7.68 + 5
  assert.equal(computeBaseFontSize(1024, 768), 19.59);
  // Ширина есть, высоты нет — это ещё НЕ широкий экран, ветка телефонная.
  assert.equal(computeBaseFontSize(540, 800), 15.67);  // 1.42*5.40 + 0.4*8.00 + 4.8 = 15.668
});

test('мерить нечего — честный null, а не выдуманное число', () => {
  assert.equal(computeBaseFontSize(0, 390), null);
  assert.equal(computeBaseFontSize(844, 0), null);
  assert.equal(computeBaseFontSize(150, 390), null);
});
