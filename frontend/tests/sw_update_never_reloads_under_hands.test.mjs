// Обновление приложения не имеет права перезагрузить экран под руками у человека.
//
// ЖАЛОБА ВЛАДЕЛЬЦА 15.09.2026: открываешь словарь с иконки на рабочем столе после
// перерыва, тапаешь в поле — курсор мигнул и пропал, экран дёрнулся, нужен второй тап.
// Виноват был не курсор: страница перезагружала сама себя, как только новый service
// worker забирал управление. Замер на стенде (настоящая сборка, профиль иконки,
// сымитирован деплой): controllerchange на 1.9 с → reload на 2.9 с; набранное до этого
// слово «Entschuldigung» стёрлось начисто.
//
// Дверей перезагрузки было ДВЕ, и это главное, что здесь сторожится:
//   1) наша в main.jsx — заменена стражем «применять, только когда нечего терять»;
//   2) чужая, внутри 'virtual:pwa-register': этот модуль генерирует плагин, и в ОБОИХ
//      его режимах внутри лежит безусловная window.location.reload(). Поэтому модуль
//      не подключается вовсе, worker регистрируем сами. Вернётся импорт — вернётся и
//      перезагрузка под пальцами, причём молча: наш страж останется на месте и будет
//      выглядеть работающим.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const main = readFileSync(new URL('../src/main.jsx', import.meta.url), 'utf8');
const dict = readFileSync(new URL('../src/dictionary/DictionaryOverlay.jsx', import.meta.url), 'utf8');

test('чужой регистратор с безусловной перезагрузкой не подключён', () => {
  // Ищем именно ПОДКЛЮЧЕНИЕ, а не упоминание: имя модуля стоит и в вердикте-комментарии
  // рядом с правкой, и проверка «нет такой строки» ловила бы сама себя.
  const подключён = /(?:import\s*\(|from)\s*['"]virtual:pwa-register['"]/.test(main);
  assert.ok(!подключён,
    'вернулся импорт virtual:pwa-register — внутри него своя window.location.reload(), '
    + 'она перезагрузит экран под руками поверх нашего стража');
  assert.ok(main.includes("navigator.serviceWorker.register('/sw.js'"),
    'своя регистрация service worker исчезла — обновления перестанут доезжать до человека');
});

test('перезагрузка по смене worker идёт только через стража', () => {
  const начало = main.indexOf("navigator.serviceWorker.addEventListener('controllerchange'");
  assert.ok(начало > 0, 'исчез обработчик controllerchange — обновление не будет применяться вовсе');
  const обработчик = main.slice(начало, начало + 400);
  assert.ok(!обработчик.includes('location.reload'),
    'в обработчике controllerchange снова стоит прямая перезагрузка — это и есть дефект 15.09.2026');
  assert.ok(обработчик.includes('applyWhenSafe'),
    'обработчик больше не спрашивает стража, можно ли перезагружать');
});

test('страж знает, что человеку есть что терять', () => {
  assert.ok(main.includes('const nothingToLose'), 'проверка «терять нечего» исчезла');
  assert.ok(/nothingToLose[\s\S]{0,700}input, textarea/.test(main),
    'страж перестал смотреть, есть ли набранный текст в полях');
  assert.ok(/nothingToLose[\s\S]{0,900}audio, video/.test(main),
    'страж перестал смотреть на звучащее аудио — перезагрузка оборвёт прослушивание');
  assert.ok(main.includes("applyUpdate('background')"),
    'отложенное обновление больше не применяется при уходе в фон — оно повиснет навсегда');
});

test('словарь не ставит фокус там, где фокус не открывает клавиатуру', () => {
  const начало = dict.indexOf('hasRealKeyboard');
  assert.ok(начало > 0,
    'в быстром словаре снова безусловный автофокус: на телефоне он клавиатуру не откроет, '
    + 'а первый тап человека съест');
  assert.ok(dict.includes("matchMedia('(hover: hover) and (pointer: fine)')"),
    'признак устройства ввода заменён на что-то другое — проверьте, не догадка ли это по строке браузера');
  const фокусы = dict.match(/setTimeout\(\(\) => \{ try \{ inputRef\.current\?\.focus\(\)/g) || [];
  assert.equal(фокусы.length, 1,
    `отложенных автофокусов в словаре ${фокусы.length}, а должен быть один — под условием клавиатуры`);
  assert.ok(/if \(hasRealKeyboard\) \{\s*focusTimer = setTimeout/.test(dict),
    'автофокус вышел из-под условия hasRealKeyboard');
});
