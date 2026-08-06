// Артикль в заголовке подсвечивается по роду — и у слова, и у фразы.
//
// Владелец 06.08.2026: «Das viele Geld kommt nicht von ungefähr.» рвалось пополам —
// «Das» оставалось слева, остальное уезжало строкой ниже. Сначала я решил, что виновата
// подсветка, и запретил её у фраз. Владелец возразил по делу: подсветка не мешала
// никогда, «Das» в этой фразе — тоже артикль, и цвет рода там полезен. Виновата была
// вёрстка: строка заголовка была flex-контейнером, и голый текст после плашки становился
// отдельным элементом, переносившимся сам под себя. Починено в CSS, подсветка осталась.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/dictionary/WordBreakdown.jsx', import.meta.url), 'utf8');
const body = src.slice(src.indexOf('export function splitLeadingArticle'));
const fn = new Function('return ' + body.slice(body.indexOf('function'), body.indexOf('\n}\n') + 2))();

assert.deepEqual(fn('das Haus'), { article: 'das', rest: ' Haus' });
assert.deepEqual(fn('die Vereinigten Staaten'), { article: 'die', rest: ' Vereinigten Staaten' });
// фраза — артикль тоже подсвечиваем
assert.deepEqual(fn('Das viele Geld kommt nicht von ungefähr.'),
  { article: 'Das', rest: ' viele Geld kommt nicht von ungefähr.' });
// регистр как у человека, не приведённый
assert.equal(fn('Der Titanic sank 1912.').article, 'Der');
// без артикля нечего отделять
assert.equal(fn('Haus'), null);
assert.equal(fn(''), null);
assert.equal(fn(null), null);

const css = readFileSync(new URL('../src/App.css', import.meta.url), 'utf8');
const rule = css.slice(css.indexOf('.vocab-word-fullscreen-word {'), css.indexOf('.vocab-word-fullscreen-trans'));
assert.ok(!/display:\s*flex/.test(rule), 'строка заголовка снова flex — фраза будет рваться');

console.log('split_leading_article: ok');
