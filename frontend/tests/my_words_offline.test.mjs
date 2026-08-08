// Свои находки должны находиться без сети — по тем же словам, какими их искали.
//
// Владелец 08.08.2026 включил авиарежим и набрал «Змей горыныч» — фразу, которую сам
// переводил месяцем раньше. Приложение ответило «слово не найдено офлайн». Причина
// оказалась не в поиске: офлайн у нас был ровно один — базовый словарь на 10 000 частых
// НЕМЕЦКИХ слов, и личных фраз он не содержит по определению. Личные находки не
// сохранялись локально никогда, ни одна из почти пятнадцати тысяч.
//
// Здесь проверяется ключ, по которому находка ложится и достаётся. Само хранилище —
// IndexedDB, его в node нет; но вся содержательная часть («по каким словам искать»)
// лежит в чистой функции, и именно она ломалась бы молча.
import assert from 'node:assert/strict';
import test from 'node:test';
import { myWordKeys } from '../src/offline/baseDictCache.js';

test('русская фраза находится по себе самой', () => {
  const keys = myWordKeys('Змей горыныч', { word_de: 'Drache Gorynytsch' });
  assert.ok(keys.includes('змей горыныч'), `нет ключа запроса: ${keys.join(' | ')}`);
});

test('находка достаётся и с немецкой стороны', () => {
  const keys = myWordKeys('Сказочный болван', { word_de: 'Märchentrottel' });
  assert.ok(keys.includes('сказочный болван'));
  assert.ok(keys.includes('märchentrottel'));
});

test('артикль не мешает: ищут и «die Mündung», и «mündung»', () => {
  const keys = myWordKeys('die Mündung', { word_de: 'die Mündung' });
  assert.ok(keys.includes('die mündung'));
  assert.ok(keys.includes('mündung'));
});

test('регистр и лишние пробелы не создают вторую запись', () => {
  const a = myWordKeys('  Blödsinn  ', {});
  assert.deepEqual(a, ['blödsinn']);
});

test('ключи не повторяются', () => {
  const keys = myWordKeys('Haus', { word_de: 'Haus', translation_de: 'Haus' });
  assert.equal(new Set(keys).size, keys.length);
});

test('пустой запрос без карточки не даёт ключей — писать нечего', () => {
  assert.deepEqual(myWordKeys('', null), []);
  assert.deepEqual(myWordKeys('   ', undefined), []);
});

test('карточка без немецкой стороны всё равно запоминается по запросу', () => {
  const keys = myWordKeys('блаженство', { translation_ru: 'блаженство' });
  assert.deepEqual(keys, ['блаженство']);
});

// Карточка лежит в ОДНОМ экземпляре под первым ключом, остальные ключи — лёгкие ссылки
// на него. Иначе разбор весом в килобайты копировался бы три-четыре раза, и подкачка
// пятисот слов съела бы мегабайты там, где хватает сотен килобайт. Значит порядок
// ключей — не косметика: первым обязан идти набранный запрос.
test('первым ключом идёт набранный запрос — под ним лежит сама карточка', () => {
  const keys = myWordKeys('Змей горыныч', { word_de: 'Drache Gorynytsch' });
  assert.equal(keys[0], 'змей горыныч');
  assert.ok(keys.length > 1, 'остальные ключи должны быть ссылками, а не отсутствовать');
});

test('запрос с артиклем: сама карточка под тем, что набрали', () => {
  const keys = myWordKeys('die Mündung', { word_de: 'die Mündung' });
  assert.equal(keys[0], 'die mündung');
  assert.ok(keys.includes('mündung'));
});
