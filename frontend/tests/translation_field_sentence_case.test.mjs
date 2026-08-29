// Заглавная в поле перевода — НАША, а не системная.
//
// Владелец 29.08.2026, с видео: «я что-то пишу, потом делаю пробел… но почему-то
// клавиатура пишет следующее слово с большой буквы».
//
// Разбор по кадрам: клавиатура айфона внутри Telegram (WKWebView) полем не владеет —
// чтобы решить, поднимать ли Shift, она спрашивает у страницы текст слева от курсора.
// На видео полоска подсказок ШЕСТЬ СЕКУНД показывала «Ich | Hallo | Ja» — набор для
// ПУСТОГО поля, — хотя в поле лежали три строки текста. Клавиатура текста не видела и
// решала «начало предложения». Починить это в вебвью нельзя, оно не наше (та же жалоба
// годами висит на Google Docs в Safari). Поэтому системную капитализацию выключили
// (autoCapitalize="off" на ВСЕХ платформах, на Android так и было), а заглавную ставим
// сами: начало поля и после . ! ? … или переноса строки.
//
// Тест гоняет САМО ПРАВИЛО из App.jsx на поддельном узле textarea — не грепает исходник.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');

// ── Вырезаем правило из App.jsx и оживляем его ───────────────────────────────────────
const cut = (from, to) => {
  const start = src.indexOf(from);
  assert.notEqual(start, -1, `в App.jsx исчезло: ${from}`);
  const end = src.indexOf(to, start);
  assert.notEqual(end, -1, `в App.jsx не нашёл конец блока, начатого с: ${from}`);
  return src.slice(start, end + to.length);
};

const ruleSource = [
  cut("const SENTENCE_END_CHARS = new Set(", ");"),
  cut('function isAtSentenceStart(text, index) {', '\n}'),
  cut('function applyOwnSentenceCase(node, nativeEvent) {', '\n}'),
  'return { isAtSentenceStart, applyOwnSentenceCase };',
].join('\n\n');

// eslint-disable-next-line no-new-func
const { isAtSentenceStart, applyOwnSentenceCase } = new Function(ruleSource)();

// Поддельная textarea: держит значение и курсор ровно так, как это делает браузер.
function fakeNode(value, caret = value.length) {
  return {
    value,
    selectionStart: caret,
    selectionEnd: caret,
    setRangeText(replacement, start, end) {
      this.value = this.value.slice(0, start) + replacement + this.value.slice(end);
      this.selectionStart = start + replacement.length;
      this.selectionEnd = this.selectionStart;
    },
  };
}

// Напечатали одну букву: браузер уже вставил её в поле, потом позвал наш обработчик.
function type(node, char, { inputType = 'insertText', isComposing = false } = {}) {
  const at = node.selectionStart;
  node.value = node.value.slice(0, at) + char + node.value.slice(node.selectionEnd);
  node.selectionStart = at + char.length;
  node.selectionEnd = node.selectionStart;
  applyOwnSentenceCase(node, { inputType, data: char, isComposing });
  return node.value;
}

// ── Что заглавная ОБЯЗАНА ставиться ─────────────────────────────────────────────────
test('первая буква пустого поля — заглавная', () => {
  assert.equal(type(fakeNode(''), 'i'), 'I');
});

test('после точки с пробелом — заглавная', () => {
  assert.equal(type(fakeNode('Ich lerne Deutsch. '), 'e'), 'Ich lerne Deutsch. E');
});

for (const [end, name] of [['!', 'восклицательного'], ['?', 'вопросительного'], ['…', 'многоточия']]) {
  test(`после ${name} знака — заглавная`, () => {
    assert.equal(type(fakeNode(`Wirklich${end} `), 'j'), `Wirklich${end} J`);
  });
}

test('после переноса строки — заглавная', () => {
  assert.equal(type(fakeNode('Erste Zeile.\n'), 'z'), 'Erste Zeile.\nZ');
});

test('умлаут поднимается тоже', () => {
  assert.equal(type(fakeNode(''), 'ä'), 'Ä');
});

// ── Дефект владельца: посреди предложения заглавной быть НЕ ДОЛЖНО ───────────────────
test('после обычного пробела посреди фразы — строчная (тот самый дефект)', () => {
  const node = fakeNode('Ich muss den Studenten häufig erinnern, dass eine selbstständige Arbeit ');
  assert.equal(type(node, 'n').endsWith('Arbeit n'), true);
});

test('вернулись к прошлому слову, поправили, снова пробел — строчная', () => {
  // Ровно сценарий с видео: стёрли пробел, дописали букву, поставили пробел, печатаем.
  const node = fakeNode('Arbeit ');
  assert.equal(type(node, 'n'), 'Arbeit n');
});

test('вторая буква слова в начале предложения уже не трогается', () => {
  const node = fakeNode('I');
  assert.equal(type(node, 'c'), 'Ic');
});

test('немецкое существительное посреди фразы мы НЕ капитализируем', () => {
  assert.equal(type(fakeNode('eine '), 'a'), 'eine a');
});

// ── Не наши поводы: удаление, автозамена, диктовка, набор иероглифов ─────────────────
test('удаление не поднимает регистр', () => {
  const node = fakeNode('Ich lerne. das X');
  node.value = 'Ich lerne. das';
  node.selectionStart = node.value.length;
  node.selectionEnd = node.value.length;
  applyOwnSentenceCase(node, { inputType: 'deleteContentBackward', data: null, isComposing: false });
  assert.equal(node.value, 'Ich lerne. das');
});

test('подстановка автозамены не трогается', () => {
  const node = fakeNode('');
  node.value = 'i';
  node.selectionStart = 1;
  node.selectionEnd = 1;
  applyOwnSentenceCase(node, { inputType: 'insertReplacementText', data: 'i', isComposing: false });
  assert.equal(node.value, 'i');
});

test('во время композиции (IME) не трогаем', () => {
  assert.equal(type(fakeNode(''), 'i', { isComposing: true }), 'i');
});

test('вставка целого куска текста не трогается', () => {
  const node = fakeNode('');
  node.value = 'ich lerne';
  node.selectionStart = 9;
  node.selectionEnd = 9;
  applyOwnSentenceCase(node, { inputType: 'insertFromPaste', data: 'ich lerne', isComposing: false });
  assert.equal(node.value, 'ich lerne');
});

test('ß не превращается в SS', () => {
  // 'ß'.toUpperCase() === 'SS' — две буквы вместо одной, слово было бы испорчено.
  assert.equal(type(fakeNode(''), 'ß'), 'ß');
});

test('цифра и знак препинания регистра не имеют — поле не меняется', () => {
  assert.equal(type(fakeNode(''), '7'), '7');
  assert.equal(type(fakeNode(''), ','), ',');
});

test('выделен кусок текста — не наше дело', () => {
  const node = fakeNode('ich lerne');
  node.selectionStart = 0;
  node.selectionEnd = 3;
  applyOwnSentenceCase(node, { inputType: 'insertText', data: 'i', isComposing: false });
  assert.equal(node.value, 'ich lerne');
});

// ── Разметка поля: системная капитализация обязана быть выключена ────────────────────
test('поле перевода говорит клавиатуре autoCapitalize=off на ВСЕХ платформах', () => {
  const at = src.indexOf("textareaProps.autoCapitalize = 'off';");
  assert.notEqual(at, -1, 'из поля перевода пропал autoCapitalize=off');
  // Строка обязана стоять ВНЕ ветки `if (isAndroidClient)`: именно из-за этого на айфоне
  // системная капитализация оставалась включённой и давала заглавную посреди фразы.
  const androidBranch = src.indexOf('if (isAndroidClient) {\n    textareaProps.autoComplete');
  assert.notEqual(androidBranch, -1, 'ветка Android у поля перевода изменилась — перечитайте тест');
  assert.equal(at < androidBranch, true, 'autoCapitalize=off снова заперли внутрь ветки Android');
});

test('обработчик ввода зовёт наше правило до чтения значения', () => {
  const handler = cut('const handleInput = useCallback((event) => {', '}, [processInputValue]);');
  const callAt = handler.indexOf('applyOwnSentenceCase(');
  const readAt = handler.indexOf('String(node.value');
  assert.notEqual(callAt, -1, 'handleInput перестал звать applyOwnSentenceCase');
  assert.equal(callAt < readAt, true, 'значение читается раньше, чем поднят регистр');
});

test('нижняя граница правила: isAtSentenceStart честно отвечает по тексту', () => {
  assert.equal(isAtSentenceStart('', 0), true);
  assert.equal(isAtSentenceStart('Hallo ', 6), false);
  assert.equal(isAtSentenceStart('Hallo. ', 7), true);
  assert.equal(isAtSentenceStart('Hallo.   ', 9), true);
  assert.equal(isAtSentenceStart('Hallo,  ', 8), false);
});
