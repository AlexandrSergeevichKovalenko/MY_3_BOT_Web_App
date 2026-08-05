// Примеры в карточке лежат в двух видах, и один из них молча выбрасывался.
//
// Сохранённая фраза хранит примеры СТРОКАМИ, а разбор от модели — парами
// {source, target}. Сборщик брал только объекты, поэтому карточка с тремя примерами
// открывалась пустой: заголовок, значок части речи и белое поле во весь экран.
// Замер 05.08.2026: 1 379 карточек и 90 единиц хранят примеры строками.
//
// Проверяем статически: у сборщика должна быть ветка для строки.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(
  path.resolve(__dirname, '../src/dictionary/WordBreakdown.jsx'), 'utf8',
);

const collect = source.slice(
  source.indexOf('function collectExamples'),
  source.indexOf('function ExamplesBlock'),
);

assert.ok(
  /typeof ex === 'string'/.test(collect),
  'сборщик примеров обязан принимать строку: так хранит примеры сохранённая фраза',
);
assert.ok(
  /typeof ex === 'object'/.test(collect),
  'пары {source, target} тоже остаются — так приходит разбор от модели',
);
assert.ok(
  collect.indexOf("typeof ex === 'string'") < collect.indexOf("typeof ex === 'object'"),
  'строку проверяем первой: у строки нет полей, и ветка объекта её не подхватит',
);
