// Шаг «Базовый словарь» в онбординге: тупик и вранье про размер.
//
// 15.09.2026 человек встал на шаге 4 из 20 и не смог пойти дальше. На экране была
// зелёная плашка «✅ Весь словарь подключён», ни одной кнопки и бледная «Далее →».
// Разбор: шаг обязательный, «Далее» ждала нажатия кнопки В ЭТОЙ вкладке, а кнопок
// нет — их прячет та самая плашка. Нажать нечего → отметки нет → «Далее» выключена
// навсегда. Замер на живой базе в тот день: 1 человек стоял в тупике прямо тогда
// (uid 313002147), ещё 9 с подпиской и без строки тура упёрлись бы при первом входе.
//
// Заодно вскрылось, что плашка врала: подписка включается и на «Быстрый старт»,
// разница только в потолке (subscription_limit). 6 записей из 19 подписок были
// «быстрым стартом», а тур говорил им «весь словарь подключён».
//
// Тест проверяет ОБЕ вещи на чистых функциях из самого файла — не на копии правила.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/onboarding/OnboardingWizard.jsx', import.meta.url), 'utf8');
const START = '// ⟦TEST-EXTRACT-START⟧ dictionary-step-state';
const END = '// ⟦TEST-EXTRACT-END⟧';
const a = src.indexOf(START);
const b = src.indexOf(END);
assert.ok(a !== -1 && b > a, 'метки для теста исчезли из OnboardingWizard.jsx');
const block = src.slice(a, b);
const { dictionaryStepState, dictionaryDecisionFromOffer } =
  new Function(`${block}\nreturn { dictionaryStepState, dictionaryDecisionFromOffer };`)();

const offer = (state, extra = {}) => ({
  starter_pair_total: 0, suggested_count: 1000, import_limit: 1000, template_total: 20000,
  ...extra,
  state: { decision_status: 'pending', live_subscription: false, subscription_limit: null, ...state },
});

test('ГЛАВНОЕ: решение, записанное сервером, снимает блокировку «Далее»', () => {
  // Ровно случай 15.09.2026: словарь подключён вне тура, в этой вкладке не нажимали ничего.
  const o = offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: 1000 });
  assert.notEqual(dictionaryDecisionFromOffer(o), null, 'шаг снова стал тупиком: решение с сервера не читается');
  assert.equal(dictionaryDecisionFromOffer(o), 'quick');
});

test('отказ отметкой НЕ становится: кнопки на экране есть, тупика нет', () => {
  // Человек мог закрыть окно в главном приложении. Это не «шаг пройден»: пусть выберет
  // здесь. Отметь мы его — плашка сказала бы «пропущено» и отняла выбор.
  assert.equal(dictionaryDecisionFromOffer(offer({ decision_status: 'declined' })), null);
});

test('«согласился», но не подключилось ничего — отметку не ставим, иначе плашка соврёт', () => {
  const o = offer({ decision_status: 'accepted', live_subscription: false }, { starter_pair_total: 0 });
  assert.equal(dictionaryDecisionFromOffer(o), null);
});

test('решения ещё нет — ничего не подставляем, кнопки выбора обязаны остаться', () => {
  const o = offer({ decision_status: 'pending' });
  assert.equal(dictionaryDecisionFromOffer(o), null);
  const st = dictionaryStepState(o, false);
  assert.equal(st.done, false, 'шаг объявил себя пройденным без выбора человека');
  assert.equal(st.partial, false);
});

test('подписка БЕЗ потолка — это весь словарь', () => {
  const st = dictionaryStepState(offer({ live_subscription: true, subscription_limit: null }), false);
  assert.equal(st.hasFull, true);
  assert.equal(st.subscribedCapped, false);
  assert.equal(dictionaryDecisionFromOffer(
    offer({ decision_status: 'accepted', live_subscription: true, subscription_limit: null })), 'full');
});

test('ГЛАВНОЕ: подписка С потолком — это «Быстрый старт», а НЕ весь словарь', () => {
  const st = dictionaryStepState(offer({ live_subscription: true, subscription_limit: 1000 }), false);
  assert.equal(st.hasFull, false, 'выбравшему быстрый старт снова говорят «весь словарь подключён»');
  assert.equal(st.subscribedCapped, true);
  assert.equal(st.subLimit, 1000);
  assert.equal(st.partial, true, 'кнопка «добрать весь словарь» опять пропала');
});

test('старое копирование: набрано меньше шаблона — весь словарь ещё можно добрать', () => {
  const st = dictionaryStepState(offer({ decision_status: 'accepted' }, { starter_pair_total: 1000 }), false);
  assert.equal(st.hasFull, false);
  assert.equal(st.partial, true);
});

test('старое копирование: набрано всё — это весь словарь', () => {
  const st = dictionaryStepState(offer({ decision_status: 'accepted' }, { starter_pair_total: 20000 }), false);
  assert.equal(st.hasFull, true);
  assert.equal(st.partial, false);
});

test('шаблон пуст — «весь словарь» не объявляем на пустом месте', () => {
  const st = dictionaryStepState(offer({}, { template_total: 0, starter_pair_total: 0 }), false);
  assert.equal(st.hasFull, false);
});
