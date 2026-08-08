// Многострочное поле поиска должно быть оформлено так же, как было однострочное.
//
// 08.08.2026 поле в словаре стало многострочным (textarea вместо input), чтобы фраза
// не обрезалась. Но все правила формы во внутреннем словаре написаны на `input` —
// и поле разом осталось без оформления: белая коробка на бежевом фоне и крестик
// очистки, съехавший под строку. Сборка этого не видит: CSS не падает, он молчит.
//
// Здесь проверяется, что правила поля перечисляют и новый класс.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const appCss  = readFileSync(new URL('../src/App.css', import.meta.url), 'utf8');
const dictCss = readFileSync(new URL('../src/dictionary/dict.css', import.meta.url), 'utf8');
const appJsx  = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');

test('поле в разметке — многострочное и с нужным классом', () => {
  assert.ok(appJsx.includes('dq-input--multi'), 'класс многострочного поля исчез из разметки');
  assert.ok(appJsx.includes('dq-input-wrap--multi'), 'обёртка поля потеряла свой класс');
});

test('правила формы во внутреннем словаре цепляют многострочное поле', () => {
  // Оба места, где поле оформлялось только как `input`.
  const rules = appCss.split('}');
  const covers = rules.filter((r) => r.includes('.dq-input--multi'));
  assert.ok(covers.length >= 2,
    `многострочное поле упомянуто в ${covers.length} правилах — было бы 2 (общее и внутри .webapp-dictionary)`);
  assert.ok(covers.some((r) => r.includes('min-height')),
    'нет правила с высотой — поле окажется ниже кнопки и крестик съедет');
});

test('крестик очистки привязан к первой строке, а не к центру растущего поля', () => {
  assert.ok(dictCss.includes('.dq-input-wrap--multi .dq-clear'),
    'нет правила положения крестика для многострочного поля');
  const m = dictCss.match(/\.dq-input-wrap--multi \.dq-clear \{ top: (\d+)px/);
  assert.ok(m, 'у крестика не задан отступ сверху');
  // (высота поля − размер крестика) / 2 = (52 − 26) / 2
  assert.equal(Number(m[1]), 13,
    'отступ крестика разошёлся с высотой поля — он снова съедет со строки');
});
