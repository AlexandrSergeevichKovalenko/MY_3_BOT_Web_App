// Стенд жеста выделения: монтирует НАСТОЯЩИЙ StructuredSelectableText из App.jsx
// с подставным разрезателем текста. Проверяет ровно то, что чинили 10.09.2026:
// удержание, протяжку, отнятую у прокрутки страницу, тап по одному слову.
// Команда прогона — в dev/gesture-check.html.
import React from 'react';
import { createRoot } from 'react-dom/client';
import { StructuredSelectableText } from '../App.jsx';

const SENTENCE = 'Trotz des schlechten Wetters sind wir spazieren gegangen.';

function fakeSegment(text) {
  const tokens = [];
  const re = /[\p{L}\p{N}]+/gu;
  let cursor = 0;
  let m = re.exec(text);
  let n = 0;
  while (m) {
    if (m.index > cursor) tokens.push({ kind: 'space', value: text.slice(cursor, m.index), start: cursor, end: m.index });
    n += 1;
    tokens.push({ kind: 'word', wid: `w${n}`, value: m[0], start: m.index, end: m.index + m[0].length });
    cursor = m.index + m[0].length;
    m = re.exec(text);
  }
  if (cursor < text.length) tokens.push({ kind: 'space', value: text.slice(cursor), start: cursor, end: text.length });
  return [{ sid: 's1', text, start: 0, end: text.length, tokens }];
}

// Патчрайт исполняет evaluate в изолированном мире, поэтому отчёт кладём в DOM,
// а не в window: узел виден обоим мирам.
const calls = [];
const sink = document.createElement('pre');
sink.id = 'calls';
document.body.appendChild(sink);
const publish = () => { sink.textContent = JSON.stringify(calls); };
publish();
const api = {
  segmentText: (t) => fakeSegment(String(t || '')),
  parseInlineMarkup: (raw) => ({ text: String(raw || ''), ranges: [] }),
  normalizeSelectionText: (v) => String(v || '').trim(),
  normalizeLangCode: () => 'de',
  getLookupLang: () => 'de',
  handleSelection: (event, value, options) => {
    calls.push({ value, type: options?.selectionType, wids: options?.selectedMeta?.wids?.length || 0 });
    publish();
  },
  selectedMeta: null,
};

// Пробник: вешаем свой touchmove на окно ПОЗЖЕ компонентского (через setTimeout из
// первого касания), поэтому в нём уже видно, отменил ли компонент событие по умолчанию.
// Отменённое touchmove = прокрутка отобрана у страницы, палец ведёт выделение.
const probeSink = document.createElement('pre');
probeSink.id = 'probe';
document.body.appendChild(probeSink);
probeSink.textContent = JSON.stringify({ moves: 0, prevented: 0 });
let probeArmed = false;
window.addEventListener('touchstart', () => {
  if (probeArmed) return;
  probeArmed = true;
  setTimeout(() => {
    window.addEventListener('touchmove', (event) => {
      const stat = JSON.parse(probeSink.textContent || '{}');
      stat.moves = (stat.moves || 0) + 1;
      if (event.defaultPrevented) stat.prevented = (stat.prevented || 0) + 1;
      probeSink.textContent = JSON.stringify(stat);
    }, { passive: false });
  }, 0);
}, true);

createRoot(document.getElementById('root')).render(
  <StructuredSelectableText api={api} text={SENTENCE} keyPrefix="check" />
);
