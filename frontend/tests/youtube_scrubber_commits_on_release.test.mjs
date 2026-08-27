// Перемотка видео обязана происходить ПРИ ОТПУСКАНИИ, а не при каждом движении пальца.
//
// Владелец 27.08.2026: «я его должен потянуть, и видео с этого момента должно
// воспроизводиться, но он почему-то прыгает в другое место».
//
// Разбор: на каждое движение вызывался seekTo(t, false). По документации YouTube IFrame
// API false означает «не запрашивать у сервера то, чего нет в буфере» — плеер садился на
// край уже загруженного куска. Настоящая перемотка seekTo(t, true) висела на onMouseUp и
// onKeyUp, а iOS WebKit подделывает мышиные события только после короткого тапа и
// отключает подделку, как только палец поехал. На телефоне честной перемотки не
// происходило ни разу.
//
// Класс, а не случай: тот же код скопирован в ДВЕ панели — телефонную
// (.youtube-scrubber-input) и планшетно-браузерную (.ypb-scrub-input). Тест сторожит обе.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const src = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const css = readFileSync(new URL('../src/App.css', import.meta.url), 'utf8');

// Вырезаем разметку каждого ползунка: от его className до закрывающей скобки тега.
const sliderMarkup = (className) => {
  const at = src.indexOf(`className="${className}"`);
  assert.notEqual(at, -1, `ползунок ${className} исчез из App.jsx`);
  const end = src.indexOf('/>', at);
  assert.notEqual(end, -1, `не нашёл конец тега у ${className}`);
  return src.slice(at, end);
};

const SLIDERS = ['youtube-scrubber-input', 'ypb-scrub-input'];
// Все виды «отпустили»: палец, отобранный жест, мышь, клавиатура.
const RELEASE_EVENTS = ['onPointerUp', 'onPointerCancel', 'onTouchEnd', 'onMouseUp', 'onKeyUp'];

for (const cls of SLIDERS) {
  test(`${cls}: во время перетаскивания видео не трогаем`, () => {
    const markup = sliderMarkup(cls);
    assert.match(markup, /onChange=\{\(e\) => previewYoutubeScrub\(e\.target\.value\)\}/,
      `${cls}: onChange обязан только рисовать (previewYoutubeScrub), а не перематывать`);
  });

  for (const evt of RELEASE_EVENTS) {
    test(`${cls}: ${evt} доводит перемотку до конца`, () => {
      const markup = sliderMarkup(cls);
      assert.match(markup, new RegExp(`${evt}=\\{\\(e\\) => commitYoutubeScrub\\(`),
        `${cls}: ${evt} обязан звать commitYoutubeScrub — иначе на этом устройстве `
        + 'перемотка не долетит до нужного места');
    });
  }
}

test('единственная перемотка ползунка — честная, с allowSeekAhead = true', () => {
  const start = src.indexOf('const commitYoutubeScrub');
  assert.notEqual(start, -1, 'commitYoutubeScrub исчез');
  const body = src.slice(start, src.indexOf('\n  };', start));
  assert.match(body, /player\.seekTo\(next, true\)/,
    'commitYoutubeScrub обязан звать seekTo с true — false не выходит за пределы буфера');
  // Нигде в коде не должно остаться перемотки с false. Строки-комментарии выкидываем:
  // объяснение дефекта живёт прямо над механизмом и само содержит «seekTo(t, false)».
  const code = src.split('\n').filter((line) => !line.trim().startsWith('//')).join('\n');
  assert.equal(/seekTo\??\.?\([^)]*,\s*false\s*\)/.test(code), false,
    'в коде снова появилась перемотка с allowSeekAhead=false — это и был дефект');
});

test('опрос позиции не затирает число, пока плеер едет к запрошенной точке', () => {
  const start = src.indexOf('const startTimePolling');
  assert.notEqual(start, -1, 'startTimePolling исчез');
  const body = src.slice(start, start + 2500);
  assert.match(body, /youtubeSeekTargetRef\.current/,
    'опрос обязан смотреть на youtubeSeekTargetRef, иначе бегунок дёргается назад '
    + 'в первые 400 мс после отпускания');
});

test('жест на ползунке принадлежит ручке, а не странице', () => {
  for (const cls of SLIDERS) {
    const at = css.indexOf(`.${cls} {`);
    assert.notEqual(at, -1, `правило .${cls} исчезло из App.css`);
    const rule = css.slice(at, css.indexOf('}', at));
    assert.match(rule, /touch-action:\s*none/,
      `.${cls}: без touch-action страница отбирает перетаскивание на полпути`);
  }
});
