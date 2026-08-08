// Синоним показывается С ПЕРЕВОДОМ, и старые записи не теряются.
//
// Было: синонимы — голые немецкие слова. Человек, который синонима ещё не знает, из
// «sich erheben» не понимает ничего, а именно за этим он в словарь и пришёл. Соседний
// блок «близкие слова» перевод имел всегда — 270 из 270, — синонимы не имели никогда.
//
// С 08.08.2026 просим у модели пару {слово, перевод}. Но в базе уже лежат 794 синонима
// строками, и у 473 перевод спрятан внутри скобок. Поэтому карточка обязана понимать
// три вида сразу, иначе накопленное показалось бы мусором или исчезло.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

// Вытаскиваем чистую функцию из модуля: он тянет за собой React и CSS, которых в node нет.
const src = readFileSync(new URL('../src/dictionary/WordBreakdown.jsx', import.meta.url), 'utf8');
const body = src.slice(src.indexOf('export function glossedList'));
const fnText = body.slice(0, body.indexOf('\n}\n') + 2);
const glossedList = new Function('clean', `return ${fnText.replace('export function', 'function')}`)(
  (v) => String(v || '').trim(),
);

test('новый вид: слово и перевод приходят парой', () => {
  assert.deepEqual(
    glossedList([{ word: 'sich erheben', gloss: 'подняться' }]),
    [{ word: 'sich erheben', gloss: 'подняться' }],
  );
});

test('накопленное: перевод спрятан в скобках — достаём его', () => {
  assert.deepEqual(
    glossedList(['aufrichten (поднимать)']),
    [{ word: 'aufrichten', gloss: 'поднимать' }],
  );
});

test('накопленное: в скобках помета стиля — показываем её, это лучше пустоты', () => {
  assert.deepEqual(
    glossedList(['erheben (более официальное)']),
    [{ word: 'erheben', gloss: 'более официальное' }],
  );
});

test('накопленное без перевода не теряется — просто остаётся без него', () => {
  assert.deepEqual(glossedList(['hochkommen']), [{ word: 'hochkommen', gloss: '' }]);
});

test('пустое и мусорное отбрасывается, а не рисуется пустым чипом', () => {
  assert.deepEqual(glossedList(['', '   ', null, undefined, {}]), []);
  assert.deepEqual(glossedList(null), []);
  assert.deepEqual(glossedList('не массив'), []);
});

test('скобки внутри слова не рвут его пополам', () => {
  // «sich (etwas) erheben» — скобка в середине, а не хвостом: перевода тут нет.
  assert.deepEqual(
    glossedList(['sich (etwas) erheben']),
    [{ word: 'sich (etwas) erheben', gloss: '' }],
  );
});

test('в словарь сохраняется СЛОВО, а не слово вместе с переводом', () => {
  // Чип отдаёт наверх text, а text теперь — s.word. Проверяем по исходнику: если бы
  // сюда попал весь объект или строка с переводом, в словарь уехало бы «erheben — подняться».
  assert.ok(src.includes('text={s.word}'), 'синоним сохраняется не как чистое слово');
  assert.ok(src.includes('text={a.word}'), 'антоним сохраняется не как чистое слово');
});
