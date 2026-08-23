// Экран не имеет права придумывать немецкие формы.
//
// До 23.08.2026 во фронте жил «запасной» счётчик: когда сервер не давал таблицу, он
// строил её сам — окончания по последней букве слова. Человек видел «die Ruhestände /
// der Ruhestands» и «die Umschaltsituatio» как готовый ответ. Тот же счёт убрали с
// сервера 17.08.2026, а копию во фронте не тронули — и рисовала именно она.
//
// Этот тест держит удаление: любая попытка вернуть склеивание окончаний в файл
// разбора уронит прогон.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

const здесь = dirname(fileURLToPath(import.meta.url));
const исходник = readFileSync(join(здесь, '../src/dictionary/WordBreakdown.jsx'), 'utf8');

test('во фронте не осталось счёта форм', () => {
  for (const имя of ['buildNounDeclension', 'buildVerbConjugation',
                     'buildAdjectiveComparison', 'buildGrammarTablesJS', '_expandFromBase']) {
    assert.equal(исходник.includes(`function ${имя}`), false,
      `вернулся счётчик форм: ${имя} — экран снова будет придумывать немецкий`);
  }
});

test('окончания не приклеиваются к слову', () => {
  // Ровно те выражения, которыми счётчик и врал.
  const склейки = [
    /\+\s*['"]er['"]/,        // comparative = слово + 'er'
    /\+\s*['"]sten['"]/,      // superlative
    /\+\s*['"]esten['"]/,
    /\+\s*['"]est['"]/,       // du-форма
    /\?\s*['"]es['"]\s*:\s*['"]s['"]/,  // genitive «+es» или «+s»
  ];
  for (const выражение of склейки) {
    assert.equal(выражение.test(исходник), false,
      `в разборе снова склеивают окончание: ${выражение}`);
  }
});

test('таблица берётся только от сервера', () => {
  assert.match(исходник, /\?\s*serverGt\s*:\s*null/,
    'таблица должна быть либо серверной, либо отсутствовать');
});
