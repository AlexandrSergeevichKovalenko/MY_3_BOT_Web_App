// Знаменатель места обязан быть назван словом и стоять при самом месте.
//
// ЗАЧЕМ ПОЯВИЛСЯ (10.09.2026). На грамоте результата спринта стояло «1 место», а
// строкой ниже — «из 1 · 12 верных (86%)». Знаменатель места («из 1» — сколько
// человек всего играло) оторвался от места и слипся со счётом ответов в одну
// нечитаемую фразу. Владелец, глядя на свой экран: «нелогично, мы её перенесли на
// нижнюю строку, и я не понимаю, что она показывает».
//
// Дефект был не в одном экране: то же голое «из N» стояло в шапке Wortsprint, в
// истории батлов, в карточке скорости интерактива дня и на постере «Итоги батлов».
// В карточке скорости знаменатель к тому же значил ДРУГОЕ — не всех игроков, а
// только тех, кто ответил верно, — и выглядел при этом точно так же.
//
// Тест держит правило: место и его знаменатель стоят в одной строке, знаменатель
// назван словом, склонение считается, а не приписывается. Возврат голого «из {N}»
// рядом с местом уронит прогон.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

const здесь = dirname(fileURLToPath(import.meta.url));
const путь = (п) => join(здесь, '..', п);
const { playersOf, wordsOf, rightAnswerersOf } = await import(
  join(здесь, '../src/answer/certLine.js')
);

test('«из N игроков» склоняется, а не приписывается', () => {
  assert.equal(playersOf(1), 'из 1 игрока');
  assert.equal(playersOf(2), 'из 2 игроков');
  assert.equal(playersOf(5), 'из 5 игроков');
  assert.equal(playersOf(11), 'из 11 игроков');
  assert.equal(playersOf(12), 'из 12 игроков');
  assert.equal(playersOf(21), 'из 21 игрока');
  assert.equal(playersOf(112), 'из 112 игроков');
});

test('нет игроков — нет и знаменателя, а не «из 0»', () => {
  // Ноль сыгравших значит, что места не существует вовсе. Пустая строка здесь —
  // не заглушка: показывать нечего, потому что нечего считать.
  assert.equal(playersOf(0), '');
  assert.equal(playersOf(null), '');
  assert.equal(playersOf(undefined), '');
});

test('слова тоже склоняются: «41 слово», а не «41 слов»', () => {
  assert.equal(wordsOf(1), '1 слово');
  assert.equal(wordsOf(2), '2 слова');
  assert.equal(wordsOf(5), '5 слов');
  assert.equal(wordsOf(41), '41 слово');
  assert.equal(wordsOf(11), '11 слов');
});

test('знаменатель карточки скорости назван своим именем', () => {
  // Там соревнуются только те, кто ответил ВЕРНО, — знаменатель меньше числа
  // сыгравших, и его нельзя показывать так же, как «из N игроков».
  assert.equal(rightAnswerersOf(1), '1 верно ответившего');
  assert.equal(rightAnswerersOf(12), '12 верно ответивших');
  assert.match(rightAnswerersOf(3), /верно ответивш/);
});

// Голое «из {…}» рядом с местом — ровно то, что владелец не смог прочитать.
const ГОЛОЕ = [
  /из \{rank\.total\}/,
  /из \{total\}/,
  /из \{total_correct\}(?!\s*\))/,
  /из \$\{b\.total\}/,
  /из \{r\.ranking/,
];

const ЭКРАНЫ = [
  'src/answer/AdjektivSprintGame.jsx',
  'src/answer/ArtikelSprintGame.jsx',
  'src/answer/WoFrageSprintGame.jsx',
  'src/answer/SprintGame.jsx',
  'src/answer/AnswerOverlay.jsx',
  'src/answer/BattleHistory.jsx',
];

for (const файл of ЭКРАНЫ) {
  test(`${файл}: знаменатель места назван словом`, () => {
    const исходник = readFileSync(путь(файл), 'utf8');
    assert.ok(исходник.includes("from './certLine.js'"),
      `${файл} перестал брать текст знаменателя из общего места — тексты разъедутся`);
    for (const голое of ГОЛОЕ) {
      assert.equal(голое.test(исходник), false,
        `в ${файл} вернулось голое «${голое}» — человек не поймёт, что это за число`);
    }
  });
}

test('постер «Итоги батлов» называет знаменатель так же, как экран', () => {
  const постер = readFileSync(join(здесь, '../../backend/battle_digest_poster.py'), 'utf8');
  assert.ok(постер.includes('_из_игроков('),
    'постер снова печатает голое «из N» — в чате человек прочитает его как число вопросов');
  assert.equal(/место из \{e\.get\('total'/.test(постер), false);
});
