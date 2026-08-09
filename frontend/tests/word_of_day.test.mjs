// «Слово дня» показывает СВОЁ слово человека и не стоит нам ни копейки.
//
// Владелец согласился оставить его на пустом экране поиска — «тогда экран учит, а не
// ждёт». Важно только, чтобы ради этого не появилось лишнего обращения к серверу:
// экран, который ждёт ввода, не повод тратить деньги и трафик.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const start = src.indexOf('const [wordOfDay');
assert.notEqual(start, -1, '«Слово дня» исчезло из кода');
const block = src.slice(start, start + 2000);

test('берётся из повторений человека, а не выдумывается', () => {
  assert.ok(block.includes('srsCard'), 'не смотрит на карточку повторений');
});

test('запасной источник — свои же сохранённые слова с телефона', () => {
  assert.ok(block.includes('getCachedVocab'), 'нет запасного источника из местного запаса');
  assert.ok(block.includes('countCachedVocab'), 'не знает размер запаса — выбор будет всегда одним и тем же');
});

test('ни одного нового обращения к серверу', () => {
  assert.ok(!/fetch\(|fetchWithTimeout\(/.test(block),
    '«Слово дня» ходит на сервер — экран ожидания не должен стоить денег');
});

test('слово держится сутки, а не мигает при перерисовке', () => {
  assert.ok(block.includes('86400000'),
    'выбор не привязан к дню — слово будет меняться при каждом открытии экрана');
});

test('показывается только на пустом экране', () => {
  assert.ok(src.includes("{wordOfDay && !dictionaryWord.trim() && ("),
    '«Слово дня» остаётся на экране поверх набранного запроса');
});
