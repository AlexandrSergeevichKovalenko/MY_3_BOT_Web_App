// Резка немецкого текста и сборка выделенного куска — ядро тапа по слову.
//
// 10.09.2026: механику «тап — слово, двойной тап — предложение, удержание с протяжкой —
// фраза» попросили в тренажёре синонимов. Резка вынесена из App.jsx в
// frontend/src/utils/textSegments.js и теперь общая для читалки и интерактивов, поэтому
// у неё появился свой тест: сломать её — значит сломать сразу обе поверхности.
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildPhraseSelection,
  normalizeSelectionText,
  segmentText,
  sentenceWords,
  wordIndexInSentence,
} from '../src/utils/textSegments.js';

const ФРАЗА = 'Wenn man ein Ziel erreichen möchte, muss man schuften.';

test('предложение режется на слова, знаки остаются отдельно', () => {
  const [s] = segmentText(ФРАЗА, 'de');
  assert.equal(sentenceWords(s).map((t) => t.value).join(' '),
    'Wenn man ein Ziel erreichen möchte muss man schuften');
  assert.ok(s.tokens.some((t) => t.kind === 'punct' && t.value.includes(',')));
});

test('умляуты и ß — часть слова, а не разделитель', () => {
  const [s] = segmentText('Die Größe der Straße überrascht mich.', 'de');
  const words = sentenceWords(s).map((t) => t.value);
  assert.ok(words.includes('Größe'), words.join('|'));
  assert.ok(words.includes('Straße'), words.join('|'));
  assert.ok(words.includes('überrascht'), words.join('|'));
});

test('два предложения — два разных sid, слова знают своё', () => {
  const sentences = segmentText('Ich lese. Du schreibst.', 'de');
  assert.equal(sentences.length, 2);
  assert.notEqual(sentences[0].sid, sentences[1].sid);
  assert.equal(sentenceWords(sentences[1]).map((t) => t.value).join(' '), 'Du schreibst');
});

test('протяжка от слова к слову даёт кусок ИСХОДНОГО текста, а не склейку слов', () => {
  const [s] = segmentText(ФРАЗА, 'de');
  const from = wordIndexInSentence(s, sentenceWords(s)[3].wid);   // Ziel
  const to = wordIndexInSentence(s, sentenceWords(s)[5].wid);     // möchte
  const sel = buildPhraseSelection(s, from, to);
  assert.equal(sel.text, 'Ziel erreichen möchte');
  assert.equal(sel.wids.length, 3);
  assert.equal(sel.sids.length, 1);
});

test('протяжка справа налево даёт то же самое', () => {
  const [s] = segmentText(ФРАЗА, 'de');
  const a = wordIndexInSentence(s, sentenceWords(s)[5].wid);
  const b = wordIndexInSentence(s, sentenceWords(s)[3].wid);
  assert.equal(buildPhraseSelection(s, a, b).text, 'Ziel erreichen möchte');
});

test('одно слово протяжкой — это слово, без соседей и без запятой', () => {
  const [s] = segmentText(ФРАЗА, 'de');
  const i = wordIndexInSentence(s, sentenceWords(s)[5].wid);
  assert.equal(buildPhraseSelection(s, i, i).text, 'möchte');
});

test('номера слов и предложений считаются от текста — при повторной резке те же', () => {
  const a = segmentText(ФРАЗА, 'de');
  const b = segmentText(ФРАЗА, 'de');
  assert.deepEqual(sentenceWords(a[0]).map((t) => t.wid), sentenceWords(b[0]).map((t) => t.wid));
  assert.equal(a[0].sid, b[0].sid);
});

test('пустой текст не ломает резку', () => {
  assert.deepEqual(segmentText('', 'de'), []);
  assert.deepEqual(segmentText(null, 'de'), []);
  assert.equal(buildPhraseSelection(null, 0, 0), null);
});

test('перенос строки — граница предложения, но слова по краям не склеиваются', () => {
  // Так устроен сам сегментатор, и для книги это верно: перенос там обычно конец абзаца.
  // Проверяем не число предложений, а то, ради чего резка и нужна: «ist» и «ein»
  // остаются разными словами, каждое со своим номером.
  const sentences = segmentText('Das ist\nein Test.', 'de');
  const words = sentences.flatMap((s) => sentenceWords(s).map((t) => t.value));
  assert.deepEqual(words, ['Das', 'ist', 'ein', 'Test']);
  assert.equal(new Set(sentences.flatMap((s) => sentenceWords(s).map((t) => t.wid))).size, 4);
});

test('выделенный кусок отдаётся одной строкой без лишних пробелов', () => {
  assert.equal(normalizeSelectionText('  Ziel   erreichen \n möchte '), 'Ziel erreichen möchte');
});
