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
export default function LiveExamples({ germanWord, ownExamples, pos, onSaved }) {
  const [items, setItems] = useState([]);
  const [saved, setSaved] = useState(() => new Set());

  // ПОКАЗЫВАЕМ ОБА ИСТОЧНИКА, и каждый подписан своим именем.
  //
  // Свои примеры лежат на самом слове и привязаны к КОНКРЕТНОМУ слову — перепутать
  // их нельзя: у «der Kiefer» про челюсть, у «die Kiefer» про сосну. Идут первыми.
  //
  // Корпус ищет по написанию и различать слова не умеет: владелец 15.08.2026 выбрал
  // глагол «wehen» (веять) и увидел «Die Wehen haben eingesetzt» — Схватки начались.
  // Но выбрасывать корпус незачем — это живая речь носителей, ради неё он и заведён.
  // Решение владельца: показывать оба, подпись под примером говорит, откуда он.
  const own = Array.isArray(ownExamples)
    ? ownExamples.filter((x) => x && clean(x.source))
    : [];
  const ownKey = own.map((x) => clean(x.source)).join('|');

  useEffect(() => {
    const mine = own.map((x) => ({ ...x, origin: 'Наш словарь', author: '' }));
    setItems(mine);
    const word = clean(germanWord);
    if (!word) return undefined;
    let cancelled = false;
    // Часть речи выбранной статьи: по ней корпус отличает «die Wehe» (схватка) от
    // «wehe» (горе) — в немецком существительное всегда с заглавной.
    api('/api/webapp/dictionary/examples', { word, pos: clean(pos) })
      .then((data) => {
        if (cancelled) return;
        const fromCorpus = Array.isArray(data?.items) ? data.items : [];
        // Одно и то же предложение из двух источников показывать дважды незачем.
        const seen = new Set(mine.map((x) => clean(x.source).toLowerCase()));
        setItems(mine.concat(
          fromCorpus.filter((x) => x && !seen.has(clean(x.source).toLowerCase())),
        ));
      })
      .catch(() => { /* корпус промолчал — свои примеры уже показаны */ });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [germanWord, ownKey]);

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
