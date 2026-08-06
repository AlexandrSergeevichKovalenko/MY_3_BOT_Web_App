// Артикль отделяется цветной плашкой только у СЛОВА, не у фразы.
// Владелец 06.08.2026: карточка «Das viele Geld kommt nicht von ungefähr.» рвалась
// пополам — «Das» уходило в плашку и вставало отдельной строкой посреди заголовка.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/dictionary/WordBreakdown.jsx', import.meta.url), 'utf8');
const body = src.slice(src.indexOf('export function splitLeadingArticle'));
const fn = new Function('return ' + body.slice(body.indexOf('function'), body.indexOf('\n}\n') + 2))();

// слово с артиклем — отделяем
assert.deepEqual(fn('das Haus'), { article: 'das', rest: ' Haus' });
assert.deepEqual(fn('die Titanic'), { article: 'die', rest: ' Titanic' });
assert.deepEqual(fn('die Vereinigten Staaten'), { article: 'die', rest: ' Vereinigten Staaten' });

// предложение — НЕ отделяем
assert.equal(fn('Das viele Geld kommt nicht von ungefähr.'), null);
assert.equal(fn('Der Titanic sank 1912.'), null);
assert.equal(fn('die Titanic rammt ein Eisberg und beginnt zu sinken'), null);

// без артикля — нечего отделять
assert.equal(fn('Haus'), null);
assert.equal(fn(''), null);
assert.equal(fn(null), null);

console.log('split_leading_article: ok');
