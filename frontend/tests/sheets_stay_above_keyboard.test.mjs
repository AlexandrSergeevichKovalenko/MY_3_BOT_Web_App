// Лист, прижатый к низу экрана, не должен прятаться под клавиатурой.
//
// 30.08.2026, видео владельца: словарь → карточка слова → «Редактировать» → касание в
// поле → лист с полями уходит под клавиатуру, видно только его заголовок.
//
// Разбор. Слой оверлея прибит к экрану во весь его рост (position:fixed; inset:0 — на
// том айфоне 894 px), лист прижат к НИЗУ слоя. Клавиатура забирает нижние 413 px, то
// есть ровно ту полосу, где лист и живёт. До 28.08 это пряталось СЛУЧАЙНОСТЬЮ: iOS сам
// прокручивал страницу на высоту клавиатуры, прибитый слой уезжал вверх вместе с ней —
// и лист оказывался над клавишами. С 28.08 (коммит 0c2d7f89) прокрутку мы держим, иначе
// той же прокруткой уносило высокую карточку «Добавить своё», — случайность пропала.
//
// Правильное лечение — не возвращать прокрутку, а дать слою честный рост: видимую часть
// окна. Её уже считает --app-height (= visualViewport.height), по ней живёт сама карточка
// слова. Этот тест держит связку: слой мерится видимой частью, а лист не выше слоя.
//
// Аудит прибитых листов, прижатых к низу (App.css, 30.08.2026):
//   .vocab-modal-overlay      — поля есть (правка слова, переименование папки) → лечим
//   .youtube-settings-overlay — поле есть (ручная транскрипция, textarea)      → лечим
//   .vocab-move-overlay       — полей нет, только список папок  → клавиатура не выезжает
//   .tr-focus-sheet-overlay   — полей нет, только список тем    → клавиатура не выезжает
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const appCss = readFileSync(new URL('../src/App.css', import.meta.url), 'utf8');

const ruleOf = (selector) => {
  const start = appCss.indexOf(`\n${selector} {`);
  assert.notEqual(start, -1, `правило ${selector} исчезло из App.css`);
  const end = appCss.indexOf('}', start);
  return appCss.slice(start, end);
};

for (const selector of ['.vocab-modal-overlay', '.youtube-settings-overlay']) {
  test(`${selector} мерится видимой частью окна, а не всем экраном`, () => {
    const rule = ruleOf(selector);
    assert.match(rule, /position:\s*fixed/, `${selector} перестал быть прибитым слоем — разбор ниже уже про другое`);
    assert.match(rule, /align-items:\s*flex-end/, `${selector} больше не прижимает лист к низу — разбор ниже уже про другое`);
    assert.match(
      rule,
      /height:\s*var\(--app-height/,
      `${selector} снова ростом во весь экран — лист уйдёт под клавиатуру`,
    );
  });
}

test('лист правки слова не выше своего слоя и прокручивается сам', () => {
  const rule = ruleOf('.vocab-modal-sheet');
  assert.match(rule, /max-height:\s*100%/, 'лист может перерасти слой и вылезти за верх — верхние поля станут недоступны');
  assert.match(rule, /overflow-y:\s*auto/, 'листу нечем прокрутиться: у слоя прокрутки нет');
});

test('лист настроек видео не выше своего слоя', () => {
  const rule = ruleOf('.youtube-settings-sheet');
  const max = rule.match(/max-height:\s*([^;]+);/);
  assert.ok(max, 'у листа настроек пропал предел высоты');
  assert.ok(
    max[1].includes('100%'),
    `предел высоты «${max[1].trim()}» не учитывает рост слоя — при клавиатуре лист вылезет за его верх`,
  );
});
