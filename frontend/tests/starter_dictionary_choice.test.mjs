// Базовый словарь: один источник вариантов на все три двери.
//
// Повод 1, 15.09.2026 — ТУПИК. Человек встал на «Шаге 4 из 20»: зелёная плашка
// «✅ Весь словарь подключён», ни одной кнопки, бледная «Далее →». Шаг обязательный,
// «Далее» ждала нажатия кнопки В ЭТОЙ вкладке, а кнопки прячет та же плашка. Нажать
// нечего → отметки нет → выключена навсегда. Заперт был 1 (uid 313002147), ещё 9 с
// подпиской и без строки тура упёрлись бы при первом открытии.
//
// Повод 2, тогда же — ТРИ ДВЕРИ РАЗЪЕХАЛИСЬ. Подписка включается на ОБА размера,
// разница только в потолке (subscription_limit): 6 записей из 19 были «быстрым
// стартом», а тур говорил им «весь словарь». Хуже: окно первого входа и настройки
// предлагали ТОЛЬКО быстрый старт — двое (uid 362151600, 5126120959) тур не открывали
// ни разу и о полном словаре не узнали ниоткуда.
//
// Тест бьёт по общему модулю — тому самому, из которого берут все три двери.
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  starterDictionaryFacts,
  starterDictionaryDecided,
  starterDictionaryBadge,
  starterDictionaryOptions,
} from '../src/shared/starterDictionary.js';

const offer = (state = {}, extra = {}) => ({
  starter_pair_total: 0, suggested_count: 1000, import_limit: 1000, template_total: 17539,
  ...extra,
  state: { decision_status: 'pending', live_subscription: false, subscription_limit: null, ...state },
});
const keys = (o, opts) => starterDictionaryOptions(o, 'ru', opts).map((x) => x.key);

// ── тупик ────────────────────────────────────────────────────────────────────────────
test('ГЛАВНОЕ: решение, записанное сервером, снимает блокировку «Далее»', () => {
  // Ровно случай 15.09.2026: словарь подключён вне тура, в этой вкладке не нажимали ничего.
  const o = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: 1000 });
  assert.equal(starterDictionaryDecided(o), 'quick', 'шаг снова стал тупиком: решение с сервера не читается');
});

test('отказ отметкой НЕ становится: кнопки на экране есть, тупика нет', () => {
  assert.equal(starterDictionaryDecided(offer({ decision_status: 'declined' })), null);
  assert.deepEqual(keys(offer({ decision_status: 'declined' })), ['quick', 'full', 'decline']);
});

test('«согласился», но не подключилось ничего — отметку не ставим, иначе плашка соврёт', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: false }, { starter_pair_total: 0 });
  assert.equal(starterDictionaryDecided(o), null);
  assert.equal(starterDictionaryBadge(o), null);
});

// ── размер набора ────────────────────────────────────────────────────────────────────
test('подписка БЕЗ потолка — это весь словарь', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: null });
  assert.equal(starterDictionaryFacts(o).connected, 'full');
  assert.equal(starterDictionaryDecided(o), 'full');
  assert.match(starterDictionaryBadge(o), /Весь словарь подключён/);
});

test('ГЛАВНОЕ: подписка С потолком — это «Быстрый старт», а НЕ весь словарь', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: 1000 });
  const f = starterDictionaryFacts(o);
  assert.equal(f.connected, 'quick', 'выбравшему быстрый старт снова говорят «весь словарь подключён»');
  assert.equal(f.canUpgrade, true);
  assert.match(starterDictionaryBadge(o), /Быстрый старт подключён — 1\s000 слов/);
  assert.deepEqual(keys(o), ['full', 'keep'], 'кнопка «добрать весь словарь» опять пропала');
});

test('потолок лежит на верхнем уровне (нормализация App.jsx роняет его из state) — всё равно читаем', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: true });
  delete o.state.subscription_limit;
  o.subscription_limit = 1000;
  assert.equal(starterDictionaryFacts(o).connected, 'quick');
});

test('старое копирование: набрано меньше шаблона — весь словарь ещё можно добрать', () => {
  const o = offer({ decision_status: 'accepted' }, { starter_pair_total: 1000 });
  assert.equal(starterDictionaryFacts(o).connected, 'quick');
  assert.deepEqual(keys(o), ['full', 'keep']);
});

test('старое копирование: набрано всё — это весь словарь', () => {
  const o = offer({ decision_status: 'accepted' }, { starter_pair_total: 17539 });
  assert.equal(starterDictionaryFacts(o).connected, 'full');
  assert.deepEqual(keys(o), []);
});

test('шаблон пуст — «весь словарь» не объявляем на пустом месте', () => {
  assert.equal(starterDictionaryFacts(offer({}, { template_total: 0 })).connected, 'none');
});

// ── три двери показывают ОДНО И ТО ЖЕ ─────────────────────────────────────────────────
test('ГЛАВНОЕ: новичку везде предлагают ОБА размера, а не один', () => {
  const o = offer({ decision_status: 'pending' });
  const общие = starterDictionaryOptions(o, 'ru');            // окно первого входа и шаг тура
  const настройки = starterDictionaryOptions(o, 'ru', { allowSkip: false, allowDisconnect: true });
  assert.ok(общие.some((x) => x.key === 'full'), 'дверь снова прячет полный словарь');
  assert.ok(настройки.some((x) => x.key === 'full'), 'настройки снова прячут полный словарь');
  const полный = общие.find((x) => x.key === 'full');
  assert.equal(полный.action, 'accept');
  assert.equal(полный.full, true, 'кнопка «весь словарь» опять шлёт запрос на быстрый старт');
  assert.match(полный.label, /17\s539/);
});

test('в настройках у подключённого есть «Отключить», у неподключённого — нет', () => {
  const подключён = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: 1000 });
  assert.ok(keys(подключён, { allowSkip: false, allowDisconnect: true }).includes('disconnect'));
  assert.ok(!keys(offer(), { allowSkip: false, allowDisconnect: true }).includes('disconnect'));
});

test('гостю сервер размеров не отдаёт — обе кнопки всё равно на месте, без чисел', () => {
  const o = offer({}, { template_total: 0, suggested_count: 0, import_limit: 0 });
  assert.deepEqual(keys(o), ['quick', 'full', 'decline']);
});

test('немецкий язык тоже обслуживается одним списком', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: 1000 });
  assert.match(starterDictionaryBadge(o, 'de'), /Schnellstart verbunden/);
  assert.match(starterDictionaryOptions(o, 'de')[0].label, /Ganzes Wörterbuch verbinden/);
});
