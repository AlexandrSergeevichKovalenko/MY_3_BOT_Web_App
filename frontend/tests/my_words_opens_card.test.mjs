// Слово из «Моих слов» открывается КАРТОЧКОЙ, а не уходит в поиск.
//
// Сначала я сделал наоборот: нажатие подставляло фразу в строку поиска и переводило
// её заново. Владелец 10.08.2026 возразил по существу — зачем платить и ждать за
// перевод того, что уже переведено и сохранено. Плюс человек терял место в списке.
//
// Разбор приезжает ВМЕСТЕ со списком, поэтому открытие карточки не стоит ни запроса,
// ни денег: всё нужное уже в руках.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url), 'utf8');

test('нажатие по своему слову НЕ запускает перевод заново', () => {
  // Режем именно РАЗМЕТКУ списка: имена состояний объявлены выше по файлу, и поиск
  // по голому имени попадал в объявление, а не в строку списка.
  const from = src.indexOf('{mine.map(');
  const to = src.indexOf('{mineHasMore &&', from);
  assert.ok(from !== -1 && to > from, 'не найдена разметка списка своих слов');
  const row = src.slice(from, to);
  assert.ok(!/translate\(/.test(row),
    'нажатие по сохранённому слову снова зовёт перевод — это лишние деньги и ожидание');
  assert.ok(row.includes('openMineCard('), 'нажатие не открывает карточку');
});

test('карточка строится из того, что уже пришло со списком', () => {
  const fn = src.slice(src.indexOf('const openMineCard'), src.indexOf('const openMineCard') + 900);
  assert.ok(fn.includes('response_json'), 'карточка не берёт готовый разбор из строки');
  assert.ok(!/api\(|fetch\(/.test(fn), 'открытие карточки идёт на сервер — а разбор уже был в руках');
});

test('у строки без разбора всё равно показывается пара «слово — перевод»', () => {
  const fn = src.slice(src.indexOf('const openMineCard'), src.indexOf('const openMineCard') + 900);
  assert.ok(fn.includes('word_de'), 'тонкая запись откроется пустотой');
});

test('из карточки можно вернуться к списку', () => {
  assert.ok(src.includes('К моим словам'), 'нет возврата к списку');
  assert.ok(src.includes("setTab('mine'); setMineCard(null);"),
    'повторное нажатие по закладке не возвращает к списку');
});
