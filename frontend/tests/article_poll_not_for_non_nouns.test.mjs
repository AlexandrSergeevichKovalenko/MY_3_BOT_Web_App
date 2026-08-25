// Опрос «а артикль не появился?» не запускается там, где артикля быть не может.
//
// Экран быстрого словаря, получив ответ без артикля, до ПЯТИ раз переспрашивает
// сервер с паузами (0,9 + 1,3 + 1,6 + 2,0 + 2,5 = до 8,3 секунды). Решал он это по
// написанию: одиночное слово с большой буквы — значит существительное, значит жди
// артикль. Но по-немецки с большой буквы человек пишет и «Danke», и «Genau», и
// «Falsch», и первое слово фразы.
//
// Замер 25.08.2026 на 299 РЕАЛЬНЫХ немецких однословных запросах из общего пула:
//   230 (77%) — артикль отдаётся сразу, опроса нет;
//    69 (23%) — ответ без артикля, начинался опрос; из них 29 (42% всех опросов)
//               за словами, у которых мы САМИ знаем часть речи, и она не
//               существительное. Эти пять обращений не могли закончиться успехом
//               в принципе — ни разу, ни у кого.
//
// Правило берётся ИЗ ИСТОЧНИКА: часть речи приезжает вместе с ответом, из нашего
// банка слов (сервер: _attach_quick_translate_pos). Не из написания.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url), 'utf8');
const start = src.indexOf('function germanNounAwaitingArticle');
assert.notEqual(start, -1, 'функция germanNounAwaitingArticle исчезла');
const end = src.indexOf('\n}', start) + 2;
const awaitingArticle = new Function(`${src.slice(start, end)}\nreturn germanNounAwaitingArticle;`)();

const de = (word, extra = {}) => ({ source: word, sourceLang: 'de', targetLang: 'ru', ...extra });

test('существительное без артикля — опрос нужен, он и запускается', () => {
  assert.equal(awaitingArticle(de('Wortverbindung')), 'Wortverbindung');
  assert.equal(awaitingArticle(de('Viertel', { partOfSpeech: 'noun' })), 'Viertel');
});

test('ГЛАВНОЕ: часть речи известна и это НЕ существительное — опроса нет', () => {
  for (const [word, pos] of [
    ['Danke', 'interjection'], ['Genau', 'adverb'], ['Falsch', 'adjective'],
    ['Alles', 'pronoun'], ['Zum', 'preposition'], ['Klingt', 'verb'],
  ]) {
    assert.equal(awaitingArticle(de(word, { partOfSpeech: pos })), '',
      `${word} (${pos}) не должно уходить в опрос: артикля у него не бывает`);
  }
});

test('регистр и пробелы в помете не обманывают правило', () => {
  assert.equal(awaitingArticle(de('Danke', { partOfSpeech: '  ADJECTIVE ' })), '');
  assert.equal(awaitingArticle(de('Viertel', { partOfSpeech: ' NOUN ' })), 'Viertel');
});

test('пометы нет — ведём себя как раньше, а не молчим на всякий случай', () => {
  assert.equal(awaitingArticle(de('Sonntagszuschläge')), 'Sonntagszuschläge');
  assert.equal(awaitingArticle(de('Groß', { partOfSpeech: '' })), 'Groß');
});

test('прежние ограничения не потеряны', () => {
  assert.equal(awaitingArticle(de('Wortverbindung', { article: 'die' })), '', 'артикль уже есть');
  assert.equal(awaitingArticle(de('Krieg', { entries: [{ headword: 'Krieg' }] })), '', 'есть словарная статья');
  assert.equal(awaitingArticle(de('sich wappen für den Krieg')), '', 'это фраза, а не слово');
  assert.equal(awaitingArticle(de('krieg')), '', 'с маленькой буквы — не существительное');
  assert.equal(awaitingArticle(null), '');
});

test('обратное направление: немецкое слово стоит в переводе', () => {
  const q = { source: 'война', sourceLang: 'ru', targetLang: 'de', translation: 'Krieg' };
  assert.equal(awaitingArticle(q), 'Krieg');
  assert.equal(awaitingArticle({ ...q, partOfSpeech: 'adjective' }), '');
});
