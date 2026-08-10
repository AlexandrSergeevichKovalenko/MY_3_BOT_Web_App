// Быстрый словарь не должен звать функции, которых у него нет.
//
// 10.08.2026 вкладка «Мои слова» роняла словарь в белый экран: я вызвал clean(...),
// не импортировав её. Это не синтаксическая ошибка — сборка проходит, линтера в
// проекте нет, и падение случается только когда до строки дойдёт отрисовка. То есть
// у пользователя, а не у нас.
//
// Проверяем просто: каждое имя-помощник, которое файл вызывает, обязано быть либо
// импортировано, либо объявлено в нём же.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const file = new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url);
const src = readFileSync(file, 'utf8');

// Имена, объявленные в файле (function / const / let) и импортированные.
const declared = new Set();
for (const m of src.matchAll(/\b(?:function|const|let|class)\s+([A-Za-z_$][\w$]*)/g)) declared.add(m[1]);
for (const m of src.matchAll(/import\s*{([^}]*)}/g)) {
  for (const piece of m[1].split(',')) {
    const name = piece.split(/\s+as\s+/).pop().trim();
    if (name) declared.add(name);
  }
}
for (const m of src.matchAll(/import\s+([A-Za-z_$][\w$]*)\s+from/g)) declared.add(m[1]);

// Помощники, которые этот файл действительно берёт со стороны. Список намеренно
// короткий: цель не заменить линтер, а поймать ровно тот класс ошибки, который уже
// стоил белого экрана.
const HELPERS = [
  'clean', 'genderClass', 'resolveArticle', 'resolveNumber', 'resolveLemma',
  'stripLeadingArticle', 'languageName', 'resolvePair', 'parsePairCode', 'pairCode',
  'flipPair', 'guessPair', 'buildDictionarySavePayload', 'humanizeDictError',
  'rememberMyWord', 'api', 'haptic',
];

test('каждый вызванный помощник объявлен или импортирован', () => {
  const missing = [];
  for (const name of HELPERS) {
    const used = new RegExp(`\\b${name}\\s*\\(`).test(src);
    if (used && !declared.has(name)) missing.push(name);
  }
  assert.deepEqual(missing, [], `вызываются, но не объявлены: ${missing.join(', ')} — это белый экран`);
});

test('у отдельного словаря есть перехватчик ошибок отрисовки', () => {
  const main = readFileSync(new URL('../src/main.jsx', import.meta.url), 'utf8');
  assert.ok(main.includes('getDerivedStateFromError'),
    'падение отрисовки снова сотрёт весь экран вместо человеческого сообщения');
  assert.ok(/<DictErrorBoundary>[\s\S]*<DictionaryOverlay/.test(main),
    'словарь смонтирован мимо перехватчика');
});
