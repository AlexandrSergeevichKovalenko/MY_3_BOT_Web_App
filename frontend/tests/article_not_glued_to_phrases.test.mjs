// Артикль принадлежит ОДНОМУ существительному, а не предложению.
//
// Владелец 10.08.2026 увидел в вариантах для сохранения «der Sei kein Dummkopf» и
// «der Du bist so ein Dummkopf!». Артикль брался у ЗАГОЛОВКА («der Dummkopf», noun) и
// приклеивался к каждому варианту — а варианты это связки, фразы и целые предложения.
//
// Проверка части речи не спасала: она смотрела на заголовок, а не на текст, к которому
// артикль клеят.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
// Вырезаем функцию целиком: до строки, закрывающей её объявление.
const start = src.indexOf('const prependArticleIfNeeded');
assert.notEqual(start, -1, 'функция исчезла');
const end = src.indexOf('\n  };', start) + '\n  };'.length;
const body = src.slice(start, end).replace('const prependArticleIfNeeded =', 'const f =');
const fn = new Function(`${body}\nreturn f;`)();

test('одиночному существительному артикль ставится', () => {
  assert.equal(fn('Dummkopf', 'der', 'noun'), 'der Dummkopf');
});

test('уже стоящий артикль не удваивается', () => {
  assert.equal(fn('der Dummkopf', 'der', 'noun'), 'der Dummkopf');
});

test('ГЛАВНОЕ: к предложению артикль не клеится', () => {
  assert.equal(fn('Sei kein Dummkopf', 'der', 'noun'), 'Sei kein Dummkopf');
  assert.equal(fn('Du bist so ein Dummkopf!', 'der', 'noun'), 'Du bist so ein Dummkopf!');
});

test('к связке из двух слов тоже не клеится', () => {
  assert.equal(fn('kompletter Dummkopf', 'der', 'noun'), 'kompletter Dummkopf');
});

test('не существительному не ставится вовсе', () => {
  assert.equal(fn('aufstehen', 'der', 'verb'), 'aufstehen');
});

test('без артикля текст не меняется', () => {
  assert.equal(fn('Dummkopf', '', 'noun'), 'Dummkopf');
});
