// Две шкурки словаря обязаны задавать набор цветов ЦЕЛИКОМ.
//
// Решение владельца 08.08.2026: два словаря одинаковы во всём, кроме цвета. Быстрый —
// белый с индиго, внутренний — бежевый с амброй. Это сделано не вторым набором правил,
// а вторым набором ЗНАЧЕНИЙ: правило одно, цвета берутся из переменных.
//
// Ловушка у такой схемы ровно одна, зато молчаливая. Если одна шкурка определит только
// часть переменных, остальные утекут из соседней — и получится светлый текст на светлом
// фоне. Ошибка не падает, не логируется и видна только глазами на нужном экране.
// Поэтому набор сверяется здесь.
//
// Отдельно проверяется, что цвет РОДА разный в двух шкурках, и это не оплошность:
// одинаковое значение на двух фонах читается по-разному. Замер до правки —
// «das» на бежевом давал 1.86 контраста при норме 4.5, то есть был водяным знаком.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const css = readFileSync(new URL('../src/dictionary/dict.css', import.meta.url), 'utf8');

function skinTokens(selector) {
  const start = css.indexOf(`${selector} {`);
  assert.notEqual(start, -1, `шкурка ${selector} не найдена в dict.css`);
  const end = css.indexOf('\n}', start);
  const body = css.slice(start, end);
  const out = new Map();
  for (const m of body.matchAll(/(--dq-[a-z-]+)\s*:\s*([^;]+);/g)) {
    out.set(m[1], m[2].trim());
  }
  return out;
}

const quick = skinTokens(':root');
const inapp = skinTokens('.webapp-page.is-theme-light');

test('обе шкурки задают один и тот же набор переменных', () => {
  const onlyQuick = [...quick.keys()].filter((k) => !inapp.has(k));
  const onlyInapp = [...inapp.keys()].filter((k) => !quick.has(k));
  assert.deepEqual(onlyQuick, [], `нет во внутреннем словаре: ${onlyQuick.join(', ')}`);
  assert.deepEqual(onlyInapp, [], `нет в быстром словаре: ${onlyInapp.join(', ')}`);
  assert.ok(quick.size >= 10, `набор подозрительно мал: ${quick.size}`);
});

test('ни одна переменная не ссылается сама на себя', () => {
  for (const [name, value] of [...quick, ...inapp]) {
    assert.ok(!value.includes(`var(${name})`), `${name} ссылается сама на себя`);
  }
});

test('каждая переменная — настоящий цвет, а не пусто', () => {
  for (const [name, value] of [...quick, ...inapp]) {
    assert.ok(/#[0-9a-fA-F]{3,8}|rgba?\(/.test(value), `${name} = «${value}» — это не цвет`);
  }
});

test('все используемые переменные объявлены', () => {
  const used = new Set([...css.matchAll(/var\((--dq-[a-z-]+)/g)].map((m) => m[1]));
  const missing = [...used].filter((k) => !quick.has(k));
  assert.deepEqual(missing, [], `используются, но нигде не заданы: ${missing.join(', ')}`);
});

test('цвет рода у шкурок РАЗНЫЙ — иначе он нечитаем на одном из фонов', () => {
  for (const key of ['--dq-gen-m', '--dq-gen-f', '--dq-gen-n']) {
    assert.ok(quick.has(key) && inapp.has(key), `${key} должен быть в обеих шкурках`);
    assert.notEqual(quick.get(key), inapp.get(key),
      `${key} одинаков в обеих шкурках — на бежевом такой цвет становится водяным знаком`);
  }
});

test('род внутреннего словаря читается на бежевом (контраст ≥ 4.5)', () => {
  const BEIGE = '#fff8ee';
  const lum = (hex) => {
    const h = hex.replace('#', '');
    const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255);
    const f = (c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4);
    return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
  };
  const ratio = (a, b) => {
    const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
    return (hi + 0.05) / (lo + 0.05);
  };
  for (const key of ['--dq-gen-m', '--dq-gen-f', '--dq-gen-n']) {
    const color = inapp.get(key);
    const r = ratio(color, BEIGE);
    assert.ok(r >= 4.5, `${key} (${color}) даёт контраст ${r.toFixed(2)} на бежевом — мало`);
  }
});
