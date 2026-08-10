// Живым примерам надо отдать НЕМЕЦКОЕ слово, а не то, что лежит в поле с немецким именем.
//
// Быстрый результат внутреннего словаря называет поля по ПОЗИЦИИ, а не по языку:
//     word_de: targetText, word_ru: sourceText
// При поиске de→ru это значит, что в word_de лежит РУССКОЕ слово: для «Die Versammlung»
// там окажется «Собрание». Я передал в блок именно его — корпус искал русское слово
// среди немецких предложений, не находил, и блок молча не показывался.
//
// Владелец три раза говорил, что примеров во внутреннем словаре нет, а я дважды списал
// это на кеш бандла. Кеш был ни при чём.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const i = src.indexOf('<LiveExamples');
assert.notEqual(i, -1, 'блок живых примеров исчез из внутреннего словаря');
const block = src.slice(i, i + 1400);

// Вытаскиваем функцию выбора стороны и проверяем её на настоящих случаях.
const start = block.indexOf('germanWord={(() => {') + 'germanWord={'.length;
const end = block.indexOf('})()}', start) + '})()'.length;
const picker = new Function('dictionaryResult', `return (${block.slice(start, end)});`);

test('поиск de→ru: берётся немецкая сторона, а не поле word_de', () => {
  const got = picker({
    source_text: 'Die Versammlung', target_text: 'Собрание',
    source_lang: 'de', target_lang: 'ru',
    word_de: 'Собрание', word_ru: 'Die Versammlung',   // именно так их и заполняет код
  });
  assert.equal(got, 'Die Versammlung');
});

test('поиск ru→de: берётся немецкая сторона', () => {
  const got = picker({
    source_text: 'собрание', target_text: 'die Versammlung',
    source_lang: 'ru', target_lang: 'de',
  });
  assert.equal(got, 'die Versammlung');
});

test('языки не проставлены — сторона определяется по алфавиту', () => {
  assert.equal(picker({ source_text: 'Собрание', target_text: 'die Versammlung' }), 'die Versammlung');
  assert.equal(picker({ source_text: 'die Versammlung', target_text: 'Собрание' }), 'die Versammlung');
});

test('пусто — отдаём пусто, а не мусор', () => {
  assert.equal(picker({}), '');
});
