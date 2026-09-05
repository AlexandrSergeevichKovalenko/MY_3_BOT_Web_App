import React, { useCallback, useEffect, useState } from 'react';
import Toast, { useToast } from './Toast.jsx';
import { saveErrorToast } from './saveNotice.js';
import { saveGermanWordViaLookup } from '../dictionary/saveUtils.js';
import { PICK_CAPTION, PICK_FAILED_TOAST } from './pickCopy.js';
import './saveWordChip.css';

/**
 * Дискетка рядом со словом: одно нажатие — слово в словаре.
 *
 * Зачем отдельной кнопкой. Раньше сохранить слово из интерактива можно было только через
 * окно «Спросить»: открой окно, найди внизу кнопку «Сохранить «die Weide»». Человек видит
 * слово на экране, а чтобы его забрать, должен нажать «Спросить» — глухо. Теперь дискетка
 * стоит в углу того самого блока, где слово написано, а «Спросить» занимается вопросами.
 *
 * Порядок «сначала разбор, потом запись» стоил людям слов. Разбор нового слова на
 * бесплатном тарифе — 1 в день; на втором слове он отвечал 429, исключение рвало весь
 * try, и до записи дело не доходило: человек видел плашку про лимит и терял слово.
 * 15.08.2026 в живом журнале за одно утро так пропало 7 слов подряд, причём сохранений у
 * человека было потрачено 11 из 20 — то есть его останавливал лимит, к сохранению
 * отношения не имеющий.
 *
 * Теперь работаем через общий saveGermanWordViaLookup: у заданий русский перевод уже
 * есть (проп `translation`), значит платить за него второй раз незачем — слово ложится
 * сразу, а разбор добирается фоном и молча. Разбор нужен только там, где перевода нет.
 */
export default function SaveWordChip({
  api,
  word = '',
  translation = '',
  originProcess = 'interactive_save',
  className = '',
}) {
  // «Слова со вчерашних тренировок»: `picked` — сервер отобрал слово на завтра.
  const [chip, setChip] = useState({ state: 'idle', picked: false }); // state: idle | saved
  const state = chip.state;
  const toast = useToast();
  const text = String(word || '').trim();

  // Следующая карточка — дискетка снова свободна.
  useEffect(() => { setChip({ state: 'idle', picked: false }); }, [text]);

  const save = useCallback(() => {
    if (!text || state === 'saved') return;
    // Оптимистично: галочка и вибрация сразу, сеть — в фоне. Ждать разбор GPT человек
    // не должен. Не получилось — возвращаем дискетку и говорим причину плашкой.
    setChip({ state: 'saved', picked: false });
    try { window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* ignore */ }
    (async () => {
      try {
        const res = await saveGermanWordViaLookup({
          api,
          word: text,
          fallbackTranslation: translation,
          origin: originProcess,
        });
        const ru = String(res?.targetText || translation || '').trim();
        const pickedForDay = !!(res && res.pickedForDay);
        setChip({ state: 'saved', picked: pickedForDay });
        if (pickedForDay) {
          toast.show({ kind: 'ok', text: ru ? `«${text}» — ${ru} · в словаре · завтра повторим` : `«${text}» в словаре · завтра повторим` });
        } else {
          toast.show(PICK_FAILED_TOAST);
        }
      } catch (err) {
        // Причина важнее факта: чаще всего это дневной лимит бесплатного тарифа, и
        // «повторить» тут не поможет — человек должен это понимать.
        setChip({ state: 'idle', picked: false });
        toast.show(saveErrorToast(err));
        try { window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred?.('error'); } catch (_e) { /* ignore */ }
      }
    })();
  }, [text, translation, state, api, originProcess, toast]);

  if (!text) return null;

  // Кнопка — круг в 17px (saveWordChip.css), надпись «завтра повторим» в него не влезет;
  // видимый знак остаётся одним, а обещание живёт в плашке и в подсказке кнопки.
  const savedTitle = chip.picked ? 'Слово в словаре · завтра повторим' : 'Слово в словаре';

  return (
    <>
      <button
        type="button"
        className={`save-chip ${state === 'saved' ? 'is-saved' : ''} ${className}`.trim()}
        onClick={(e) => { e.stopPropagation(); save(); }}
        disabled={state === 'saved'}
        title={state === 'saved' ? savedTitle : PICK_CAPTION}
        aria-label={state === 'saved' ? savedTitle : PICK_CAPTION}
      >
        {state === 'saved' ? '✓' : '💾'}
      </button>
      <Toast state={toast.state} onClose={toast.hide} />
    </>
  );
}
