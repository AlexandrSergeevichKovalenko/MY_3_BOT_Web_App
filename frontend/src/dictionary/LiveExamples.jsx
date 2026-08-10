import React, { useEffect, useState } from 'react';
import { api, clean, haptic } from './WordBreakdown';
import { buildDictionarySavePayload } from './saveUtils';

/**
 * Живые примеры из корпуса — блок под быстрым переводом.
 *
 * Владелец 10.08.2026 ждал их именно здесь, в ОБОИХ словарях: «сначала покажем перевод,
 * а потом, как подгрузится, — живые примеры». До этого они жили только внутри
 * подробного разбора, и человек, не открывший разбор, не видел их никогда.
 *
 * Перевод их не ждёт ни секунды: запрос уходит следом и блок появляется, когда приедет.
 * Модель тут не участвует — это чтение нашей же таблицы (~57 мс), денег не стоит.
 *
 * Один компонент на оба словаря. Две копии этого блока разошлись бы через неделю —
 * сегодня уже был случай, когда одинаковую разметку чинили дважды.
 */
export default function LiveExamples({ germanWord, onSaved }) {
  const [items, setItems] = useState([]);
  const [saved, setSaved] = useState(() => new Set());

  useEffect(() => {
    setItems([]);
    const word = clean(germanWord);
    if (!word) return undefined;
    let cancelled = false;
    api('/api/webapp/dictionary/examples', { word })
      .then((data) => {
        if (!cancelled) setItems(Array.isArray(data?.items) ? data.items : []);
      })
      .catch(() => { /* примеров нет — просто не показываем, это не ошибка */ });
    return () => { cancelled = true; };
  }, [germanWord]);

  if (!items.length) return null;

  const save = async (de, ru) => {
    const src = clean(de);
    if (!src || saved.has(src)) return;
    // Отмечаем сразу: человек нажал и должен увидеть отклик, не дожидаясь сети.
    setSaved((prev) => new Set(prev).add(src));
    haptic('ok');
    try {
      await api('/api/webapp/dictionary/save', buildDictionarySavePayload({
        rich: null,
        sourceText: src,
        quick: { source: src, translation: clean(ru), sourceLang: 'de', targetLang: 'ru', direction: 'de-ru' },
        origin: 'webapp_corpus_example',
      }));
      if (typeof onSaved === 'function') onSaved(src);
    } catch (_e) {
      // Не сохранилось — снимаем отметку, иначе человек будет думать, что слово у него есть.
      setSaved((prev) => { const next = new Set(prev); next.delete(src); return next; });
      haptic('bad');
    }
  };

  return (
    <div className="dq-live-ex">
      <div className="dq-live-ex-label">Из живой речи</div>
      {items.map((ex, i) => {
        const de = clean(ex.source);
        const ru = clean(ex.target);
        const isSaved = saved.has(de);
        return (
          <button
            key={`${de}-${i}`}
            type="button"
            className={`dq-live-ex-row${isSaved ? ' is-saved' : ''}`}
            onClick={() => { void save(de, ru); }}
            title={isSaved ? 'Сохранено в словарь' : 'Нажмите, чтобы сохранить в словарь'}
          >
            <span className="dq-live-ex-de">{de}</span>
            {ru ? <span className="dq-live-ex-ru">{ru}</span> : null}
            <span className="dq-live-ex-src">
              {clean(ex.origin) || 'Tatoeba'}{ex.author ? ` · ${clean(ex.author)}` : ''}
              {isSaved ? ' · сохранено ✓' : ''}
            </span>
          </button>
        );
      })}
    </div>
  );
}
