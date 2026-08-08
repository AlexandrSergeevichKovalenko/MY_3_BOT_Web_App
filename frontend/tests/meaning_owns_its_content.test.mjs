// Значение держит СВОЙ пример и СВОИ синонимы — оно узел, а не строчка текста.
//
// Так устроены все словари, на которые мы смотрели: Wiktionary нумерует синонимы и
// примеры по значениям, Duden вкладывает примеры внутрь смысла, Vocabulary.com ставит
// синонимы прямо под определением. У нас же всё сваливалось в одну кучу на слово:
// «вставать с постели» и «окно стоит открытым» получали общий список, из которого
// человек делал неверный вывод, что «offen sein» — синоним «подниматься».
//
// Причём пример своего значения приходил ВСЕГДА и мы за него платили: замер 08.08.2026 —
// 12 154 карточки из 12 352. Он просто отбрасывался при показе.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/dictionary/WordBreakdown.jsx', import.meta.url), 'utf8');

function extract(name) {
  const start = src.indexOf(`function ${name}`);
  assert.notEqual(start, -1, `${name} не найдена`);
  const tail = src.slice(start);
  return tail.slice(0, tail.indexOf('\n}\n') + 2);
}

const helpers = `
  const clean = (v) => String(v || '').trim();
  const hasCyrillic = (v) => /[А-Яа-яЁё]/.test(String(v || ''));
  const humanContext = (v) => String(v || '').trim();
  ${extract('glossedList').replace('export function', 'function')}
  ${extract('meaningExamplePair')}
`;
const meaningList = new Function(`${helpers}\n${extract('meaningList')}\nreturn meaningList;`)();

const CARD = {
  meanings: {
    primary: {
      value: 'вставать, подниматься',
      context: 'с постели',
      example_source: 'Ich muss früh aufstehen.',
      example_target: 'Мне нужно рано вставать.',
      synonyms: [{ word: 'sich erheben', gloss: 'подняться' }],
    },
    secondary: [{
      value: 'быть открытым',
      context: 'об окне',
      example_source: 'Das Fenster stand auf.',
      example_target: 'Окно стояло открытым.',
      synonyms: [{ word: 'offen sein', gloss: 'быть открытым' }],
    }],
  },
};

test('у каждого значения свой пример', () => {
  const list = meaningList(CARD);
  assert.equal(list.length, 2);
  assert.equal(list[0].example.de, 'Ich muss früh aufstehen.');
  assert.equal(list[0].example.ru, 'Мне нужно рано вставать.');
  assert.equal(list[1].example.de, 'Das Fenster stand auf.');
});

test('у каждого значения свои синонимы, и они не смешиваются', () => {
  const list = meaningList(CARD);
  assert.deepEqual(list[0].synonyms.map((s) => s.word), ['sich erheben']);
  assert.deepEqual(list[1].synonyms.map((s) => s.word), ['offen sein']);
});

test('немецкая сторона примера определяется по тексту, а не по имени поля', () => {
  // При поиске de→ru стороны меняются местами: example_source становится немецким.
  const flipped = meaningList({
    meanings: { primary: { value: 'вставать', example_source: 'Мне рано вставать.', example_target: 'Ich muss früh aufstehen.' } },
  });
  assert.equal(flipped[0].example.de, 'Ich muss früh aufstehen.');
  assert.equal(flipped[0].example.ru, 'Мне рано вставать.');
});

test('накопленные карточки без примеров и синонимов не ломаются', () => {
  const old = meaningList({ meanings: { primary: { value: 'вставать' }, secondary: [{ value: 'быть открытым' }] } });
  assert.equal(old.length, 2);
  assert.equal(old[0].example, null);
  assert.deepEqual(old[0].synonyms, []);
});

test('пример значения не попадает ещё и в общую кучу примеров', () => {
  const collect = src.slice(src.indexOf('function collectExamples'));
  const bodyEnd = collect.indexOf('\n}\n');
  const body = collect.slice(0, bodyEnd);
  assert.ok(!body.includes('example_source'),
    'общий блок примеров снова тянет примеры значений — один пример встанет на экране дважды');
});

test('общий список синонимов уступает место разложенным по значениям', () => {
  assert.ok(src.includes('!meanings.some((m) => m.synonyms.length > 0)'),
    'общий блок синонимов показывается вместе с посменными — будет дубль');
});
