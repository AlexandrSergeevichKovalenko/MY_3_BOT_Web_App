// Пара языков «откуда → куда» — замена переодетому выключателю.
//
// Старое правило звучало так: «есть кириллица → русский, ИНАЧЕ немецкий». Пока языков
// было два, оно работало. Третий язык оно ломает молча: «table» — английское слово,
// а правило «иначе немецкий» назовёт его немецким и пойдёт покупать разбор немецкого
// слова, которого нет. Поэтому тесты ниже проверяют не только «угадал», но и главное —
// что в неоднозначном случае модуль ЧЕСТНО говорит «не уверен», а не гадает молча.
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  LANGUAGES, ACTIVE_LANGUAGES, DEFAULT_PAIR,
  detectScript, candidatesByScript, resolvePair,
  pairCode, parsePairCode, flipPair, samePair, languageName, isKnownLanguage,
} from '../src/dictionary/langPair.js';

const RU_DE = { learningLanguage: 'de', nativeLanguage: 'ru' };

test('алфавит определяется, а не угадывается', () => {
  assert.equal(detectScript('слово'), 'cyrillic');
  assert.equal(detectScript('Haus'), 'latin');
  assert.equal(detectScript('123 —'), null);
  assert.equal(detectScript(''), null);
  assert.equal(detectScript(null), null);
});

test('сегодняшнее поведение сохранено: русский → немецкий и обратно', () => {
  const ru = resolvePair('слияние', { profile: RU_DE });
  assert.deepEqual({ source: ru.source, target: ru.target }, { source: 'ru', target: 'de' });
  assert.equal(ru.confident, true);

  const de = resolvePair('die Fusion', { profile: RU_DE });
  assert.deepEqual({ source: de.source, target: de.target }, { source: 'de', target: 'ru' });
  assert.equal(de.confident, true);
});

test('пустой ввод переводит со своего на изучаемый', () => {
  const p = resolvePair('', { profile: RU_DE });
  assert.deepEqual({ source: p.source, target: p.target }, { source: 'ru', target: 'de' });
  assert.equal(p.confident, true);
});

test('выбор человека сильнее алфавита', () => {
  const p = resolvePair('слияние', { profile: RU_DE, override: { source: 'de', target: 'ru' } });
  assert.deepEqual({ source: p.source, target: p.target }, { source: 'de', target: 'ru' });
  assert.equal(p.confident, true);
});

test('битый выбор человека игнорируется, а не роняет словарь', () => {
  for (const bad of [{ source: 'de', target: 'de' }, { source: 'xx', target: 'ru' }, {}, null]) {
    const p = resolvePair('Haus', { profile: RU_DE, override: bad });
    assert.equal(isKnownLanguage(p.source), true);
    assert.notEqual(p.source, p.target);
  }
});

test('ГЛАВНОЕ: латиница на двух языках — модуль не гадает молча', () => {
  const allowed = ['ru', 'de', 'en'];
  assert.deepEqual(candidatesByScript('table', allowed), ['de', 'en']);

  const p = resolvePair('table', { profile: RU_DE, allowed });
  // Пара всё равно предложена — человека нельзя оставить без ответа…
  assert.equal(p.source, 'de');
  assert.equal(p.target, 'ru');
  // …но помечена как неуверенная, чтобы интерфейс мог спросить.
  assert.equal(p.confident, false);
});

test('пока английский не включён, неоднозначности нет', () => {
  const p = resolvePair('table', { profile: RU_DE });   // allowed = ACTIVE_LANGUAGES
  assert.equal(p.confident, true);
  assert.deepEqual(ACTIVE_LANGUAGES, ['ru', 'de']);
});

test('тот, кто учит английский с опорой на немецкий, получает свою пару', () => {
  const profile = { learningLanguage: 'en', nativeLanguage: 'de' };
  const p = resolvePair('table', { profile, allowed: ['de', 'en'] });
  assert.equal(p.source, 'en');
  assert.equal(p.target, 'de');
});

test('профиль без языков не роняет словарь — берётся умолчание', () => {
  const p = resolvePair('Haus', { profile: { learningLanguage: 'zz', nativeLanguage: null } });
  assert.deepEqual(DEFAULT_PAIR, { source: 'ru', target: 'de' });
  assert.equal(p.source, 'de');
  assert.equal(p.target, 'ru');
});

test('код пары читается и пишется без потерь', () => {
  assert.equal(pairCode({ source: 'ru', target: 'de' }), 'ru-de');
  assert.deepEqual(parsePairCode('de-ru'), { source: 'de', target: 'ru' });
  assert.equal(parsePairCode('de-de'), null);
  assert.equal(parsePairCode('де-ру'), null);
  assert.equal(parsePairCode(''), null);
});

test('переворот и сравнение пар', () => {
  assert.deepEqual(flipPair({ source: 'ru', target: 'de' }), { source: 'de', target: 'ru' });
  assert.equal(samePair({ source: 'ru', target: 'de' }, { source: 'ru', target: 'de' }), true);
  assert.equal(samePair({ source: 'ru', target: 'de' }, { source: 'de', target: 'ru' }), false);
});

test('имена языков берутся из таблицы, а не из кода', () => {
  assert.equal(languageName('ru'), 'Русский');
  assert.equal(languageName('de'), 'Deutsch');
  assert.equal(languageName('en'), 'English');
  assert.equal(Object.keys(LANGUAGES).length >= 3, true);
});
