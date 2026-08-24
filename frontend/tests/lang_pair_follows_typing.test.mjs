// Пара языков идёт ЗА НАБОРОМ, а переключателя направления больше нет.
//
// Что было сломано (проверено прогоном 24.08.2026). Быстрый словарь после КАЖДОГО
// перевода закреплял направление как «выбор человека», а закреплённое сильнее
// алфавита. Первый же перевод выключал автоопределение навсегда:
//
//   набрал «Hund»   → панель de-ru, на сервер ушло source=de   ✓
//   набрал «собака» → панель de-ru, на сервер ушло source=de   ✗ русское слово
//   набрал «Krieg»  → панель ru-de, на сервер ушло source=ru   ✗ немецкое слово
//
// Панель отставала на один перевод, а неверный source_lang стоил дважды: своё
// слово искалось в чужой половине словаря, и переводчику сообщался не тот язык.
//
// Решение владельца 24.08.2026: направление всегда за алфавитом, ручного
// переключателя нет — кнопка ⇄ переворачивает СОДЕРЖИМОЕ (прошлый перевод
// становится новым запросом), а не режим. Так же устроено у dict.cc, Linguee и
// LEO; у Google переключатель недоступен при автоопределении.
import assert from 'node:assert/strict';
import test from 'node:test';
import { displayPair, pairBoundToText, pairCode, resolvePair } from '../src/dictionary/langPair.js';

const code = (text, opts) => pairCode(displayPair(text, opts));
// То, что уходит на сервер: ровно алфавит, без всякой памяти о прошлом ответе.
const outgoing = (text) => pairCode(resolvePair(text));

test('начал писать по-русски — пара стала ru-de, по-немецки — de-ru', () => {
  assert.equal(code('собака'), 'ru-de');
  assert.equal(code('Hund'), 'de-ru');
  assert.equal(code('с'), 'ru-de');          // на первой же букве, а не после перевода
  assert.equal(code('H'), 'de-ru');
});

test('ГЛАВНОЕ: прошлый перевод больше не закрепляет направление', () => {
  // На экране лежит ответ про «Hund» (de→ru), а человек набирает русское слово.
  const result = { text: 'Hund', pair: { source: 'de', target: 'ru' } };
  assert.equal(code('собака', { result }), 'ru-de');
  assert.equal(outgoing('собака'), 'ru-de');

  // И обратно: лежит ответ про «собака», набирают немецкое.
  const ru = { text: 'собака', pair: { source: 'ru', target: 'de' } };
  assert.equal(code('Krieg', { result: ru }), 'de-ru');
  assert.equal(outgoing('Krieg'), 'de-ru');
});

test('панель не спорит с карточкой: для ПЕРЕВЕДЁННОГО текста показывается факт', () => {
  const result = { text: 'Hund', pair: { source: 'de', target: 'ru' } };
  assert.equal(code('Hund', { result }), 'de-ru');
  // Пробелы вокруг — тот же самый текст, а не другой.
  assert.equal(code('  Hund  ', { result }), 'de-ru');
});

test('битый или чужой ответ не подставляется вместо алфавита', () => {
  for (const bad of [null, {}, { text: 'Krieg' },
                     { text: 'Krieg', pair: { source: 'de', target: 'de' } },
                     { text: 'Krieg', pair: { source: 'xx', target: 'ru' } },
                     { text: 'Hund', pair: { source: 'ru', target: 'de' } }]) {
    assert.equal(code('Krieg', { result: bad }), 'de-ru');
  }
  assert.equal(pairBoundToText('Krieg', { text: 'Hund', pair: { source: 'de', target: 'ru' } }), null);
});

test('пустое поле — переводим со своего на изучаемый, как и раньше', () => {
  assert.equal(code(''), 'ru-de');
  assert.equal(code('   '), 'ru-de');
  assert.equal(code('123 —'), 'ru-de');
});

test('⇄ переворачивает СОДЕРЖИМОЕ, и алфавит определяет пару для него сам', () => {
  // «Krieg → война», нажали ⇄: новым запросом становится «война».
  assert.equal(code('Krieg'), 'de-ru');
  assert.equal(outgoing('война'), 'ru-de');   // и она уходит как русская, а не как немецкая
  // И в обратную сторону: «война → Krieg», ⇄ возвращает нас к «Krieg».
  assert.equal(code('война'), 'ru-de');
  assert.equal(outgoing('Krieg'), 'de-ru');
});
