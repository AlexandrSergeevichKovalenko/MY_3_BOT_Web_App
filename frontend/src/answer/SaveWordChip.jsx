import React, { useCallback, useEffect, useState } from 'react';
import Toast, { useToast } from './Toast.jsx';
import { saveErrorToast } from './saveNotice.js';
import './saveWordChip.css';

/**
 * Дискетка рядом со словом: одно нажатие — слово в словаре.
 *
 * Зачем отдельной кнопкой. Раньше сохранить слово из интерактива можно было только через
 * окно «Спросить»: открой окно, найди внизу кнопку «Сохранить «die Weide»». Человек видит
 * слово на экране, а чтобы его забрать, должен нажать «Спросить» — глухо. Теперь дискетка
 * стоит в углу того самого блока, где слово написано, а «Спросить» занимается вопросами.
 *
 * Сохраняем тем же каноническим путём, что читалка и словарь: сперва разбор
 * (/api/webapp/dictionary даёт настоящий русский перевод, артикль и начальную форму),
 * потом запись карточки. Если слать немецкий текст сразу в /save, у слов из игр не было
 * перевода и карточка показывала немецкий с обеих сторон.
 */
export default function SaveWordChip({
  api,
  word = '',
  translation = '',
  originProcess = 'interactive_save',
  className = '',
}) {
  const [state, setState] = useState('idle'); // idle | saved
  const toast = useToast();
  const text = String(word || '').trim();

  // Следующая карточка — дискетка снова свободна.
  useEffect(() => { setState('idle'); }, [text]);

  const save = useCallback(() => {
    if (!text || state === 'saved') return;
    // Оптимистично: галочка и вибрация сразу, сеть — в фоне. Ждать разбор GPT человек
    // не должен. Не получилось — возвращаем дискетку и говорим причину плашкой.
    setState('saved');
    try { window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* ignore */ }
    (async () => {
      try {
        const lookup = await api('/api/webapp/dictionary', { word: text });
        const item = (lookup && lookup.item) || {};
        const direction = String(lookup?.direction || '').toLowerCase();
        const isDeRu = direction !== 'ru-de'; // слова заданий немецкие → de-ru
        const sourceText = String(
          (isDeRu ? (item.word_de || item.translation_de) : (item.word_ru || item.translation_ru)) || text,
        ).trim();
        const targetText = String(
          (isDeRu ? (item.translation_ru || item.word_ru) : (item.translation_de || item.word_de))
          || translation || '',
        ).trim();
        await api('/api/webapp/dictionary/save', {
          source_text: sourceText,
          target_text: targetText,
          translation_ru: String(item.translation_ru || (isDeRu ? targetText : '')).trim(),
          translation_de: String(item.translation_de || (isDeRu ? '' : targetText)).trim(),
          direction: direction || 'de-ru',
          response_json: item,
          origin_process: originProcess,
        });
        const ru = (isDeRu ? targetText : sourceText) || translation;
        toast.show({ kind: 'ok', text: ru ? `«${text}» — ${ru} · в словаре` : `«${text}» в словаре` });
      } catch (err) {
        // Причина важнее факта: чаще всего это дневной лимит бесплатного тарифа, и
        // «повторить» тут не поможет — человек должен это понимать.
        setState('idle');
        toast.show(saveErrorToast(err));
        try { window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred?.('error'); } catch (_e) { /* ignore */ }
      }
    })();
  }, [text, translation, state, api, originProcess, toast]);

  if (!text) return null;

  return (
    <>
      <button
        type="button"
        className={`save-chip ${state === 'saved' ? 'is-saved' : ''} ${className}`.trim()}
        onClick={(e) => { e.stopPropagation(); save(); }}
        disabled={state === 'saved'}
        title={state === 'saved' ? 'Слово в словаре' : 'Сохранить в словарь'}
        aria-label={state === 'saved' ? 'Слово в словаре' : 'Сохранить в словарь'}
      >
        {state === 'saved' ? '✓' : '💾'}
      </button>
      <Toast state={toast.state} onClose={toast.hide} />
    </>
  );
}
