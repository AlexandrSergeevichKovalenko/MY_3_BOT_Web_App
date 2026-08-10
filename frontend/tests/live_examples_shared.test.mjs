// Живые примеры показываются в ОБОИХ словарях и одним компонентом.
//
// Владелец 10.08.2026 ни разу их не увидел: блок жил только внутри подробного разбора,
// а на быстром экране — там, где человек чаще всего и находится, — его не было вовсе.
//
// Сделан один компонент на оба словаря. Две копии одинаковой разметки в этом проекте
// уже расходились: тот же дефект поля ввода пришлось чинить дважды.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const comp = readFileSync(new URL('../src/dictionary/LiveExamples.jsx', import.meta.url), 'utf8');
const overlay = readFileSync(new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url), 'utf8');
const app = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');

test('оба словаря используют ОДИН компонент', () => {
  assert.ok(overlay.includes('<LiveExamples'), 'быстрый словарь не показывает живые примеры');
  assert.ok(app.includes('<LiveExamples'), 'внутренний словарь не показывает живые примеры');
  assert.ok(overlay.includes("from './LiveExamples'"));
  assert.ok(app.includes("from './dictionary/LiveExamples'"));
});

test('перевод не ждёт примеров', () => {
  // Запрос уходит в эффекте после отрисовки, а не блокирует показ перевода.
  assert.ok(/useEffect\([\s\S]{0,400}dictionary\/examples/.test(comp),
    'примеры грузятся не в фоне — перевод будет ждать их');
});

test('пример сохраняется одним нажатием', () => {
  assert.ok(comp.includes('onClick='), 'пример не нажимается');
  assert.ok(comp.includes('/api/webapp/dictionary/save'), 'нажатие не сохраняет в словарь');
});

test('отметка «сохранено» снимается, если сохранить не удалось', () => {
  // Иначе человек будет уверен, что слово у него есть, а его нет.
  // Режем от объявления до конца файла: искать «return (» нельзя — оно встречается
  // раньше, внутри возврата функции очистки в useEffect.
  const save = comp.slice(comp.indexOf('const save ='));
  assert.ok(save.includes('next.delete(src)'), 'при ошибке отметка остаётся — это обман');
});

test('источник подписан — этого требует лицензия', () => {
  assert.ok(comp.includes('Tatoeba'), 'нет подписи источника');
  assert.ok(comp.includes('ex.author'), 'нет автора примера');
});
