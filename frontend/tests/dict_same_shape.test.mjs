// Оба словаря — один продукт: раскладка общая, отличаются только цвета.
//
// Владелец 09.08.2026: «в словаре иконкой всё как было, интерфейс же должен быть
// одинаков». Он прав: три закладки появились только во внутреннем, а быстрый остался
// с прежним экраном.
//
// Отдельная ловушка: быстрый словарь открывается сам по себе и App.css НЕ грузит.
// Стили закладок обязаны лежать в его собственном dict.css, иначе они молча не
// применятся — сборка такого не замечает.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const overlay = readFileSync(new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url), 'utf8');
const dictCss = readFileSync(new URL('../src/dictionary/dict.css', import.meta.url), 'utf8');
const app = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');

test('в быстром словаре те же три закладки', () => {
  for (const label of ['Поиск', 'Мои слова', 'История']) {
    assert.ok(overlay.includes(label), `нет закладки «${label}» в быстром словаре`);
  }
});

test('во внутреннем они те же самые', () => {
  for (const label of ['Поиск', 'Мои слова', 'История']) {
    assert.ok(app.includes(label), `нет закладки «${label}» во внутреннем словаре`);
  }
});

test('стили закладок лежат в собственных стилях быстрого словаря', () => {
  assert.ok(dictCss.includes('.dq-tabs'), 'закладки быстрого словаря без своих стилей');
  assert.ok(dictCss.includes('.dict-history-row'),
    'список истории в быстром словаре останется без оформления: App.css он не грузит');
});

test('цвет закладок берётся из шкурки, а не вписан значением', () => {
  const block = dictCss.slice(dictCss.indexOf('.dq-tabs'), dictCss.indexOf('.dict-history {'));
  assert.ok(block.includes('var(--dq-accent)'), 'активная закладка покрашена мимо шкурки');
  assert.ok(!/background:\s*#[0-9a-fA-F]{3,6};/.test(block.replace(/var\([^)]*\)/g, '')),
    'в закладках есть цвет значением — в другой шкурке он останется чужим');
});

test('свои слова тянутся только при открытии своей закладки', () => {
  const i = overlay.indexOf("tab !== 'mine'");
  assert.notEqual(i, -1, 'список своих слов грузится всегда — экран поиска платит за него зря');
});
