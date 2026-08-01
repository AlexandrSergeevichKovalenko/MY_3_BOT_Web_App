import React from 'react';
import { createPortal } from 'react-dom';
import './WordHintModal.css';

/**
 * «Подсказка по слову» — то, что уже собрано в карточке словаря, но человек этого не
 * видел: живые примеры, формы, ассоциация. Открывается лампочкой на карточке повторения
 * ПОСЛЕ переворота, чтобы не подсказывать до ответа.
 *
 * Компонент ничего не грузит и ничего не досбирает: показывает только то, что уже лежит
 * в карточке. Ни запроса к серверу, ни обращения к модели — значит, ни ожидания, ни
 * расхода дневного лимита человека.
 */
const CYRILLIC_RE = /[А-Яа-яЁё]/;

/** Примеры из карточки парами «фраза → перевод», в правильную сторону и без повторов. */
export function collectHintExamples(item, limit = 3) {
  const pairs = [];
  const add = (a, b) => {
    const x = String(a || '').trim();
    const y = String(b || '').trim();
    if (!x && !y) return;
    // Сторона определяется по алфавиту, а не по имени поля: в сохранённых карточках
    // source/target местами уже путались, и тогда перевод вставал вместо фразы.
    let learning = x;
    let native = y;
    if (CYRILLIC_RE.test(x) && !CYRILLIC_RE.test(y)) {
      learning = y;
      native = x;
    }
    if (learning) pairs.push({ learning, native });
  };
  (Array.isArray(item?.usage_examples) ? item.usage_examples : []).forEach((example) => {
    if (example && typeof example === 'object') add(example.source, example.target);
    else if (typeof example === 'string') add(example, '');
  });
  const meanings = item?.meanings;
  if (meanings && typeof meanings === 'object') {
    const take = (sense) => {
      if (sense && typeof sense === 'object') add(sense.example_source, sense.example_target);
    };
    take(meanings.primary);
    if (Array.isArray(meanings.secondary)) meanings.secondary.forEach(take);
  }
  const seen = new Set();
  const out = [];
  pairs.forEach((pair) => {
    const key = pair.learning.toLowerCase();
    if (seen.has(key) || out.length >= limit) return;
    seen.add(key);
    out.push(pair);
  });
  return out;
}

export default function WordHintModal({
  isOpen,
  onClose,
  tr,
  headword,
  translation,
  examples = [],
  formRows = [],
  memoryTip = '',
}) {
  if (!isOpen) return null;
  const t = tr || ((ru) => ru);
  const closeLabel = t('Закрыть', 'Schließen');
  const hasExamples = examples.length > 0;
  const hasForms = formRows.length > 0;
  const tip = String(memoryTip || '').trim();

  const node = (
    <div className="word-hint-overlay" role="dialog" aria-modal="true">
      <button type="button" className="word-hint-backdrop" aria-label={closeLabel} onClick={onClose} />
      <div className="word-hint-card" onClick={(event) => event.stopPropagation()}>
        <button type="button" className="word-hint-close" aria-label={closeLabel} onClick={onClose}>×</button>
        <div className="word-hint-head">
          <div className="word-hint-word">{headword}</div>
          {translation ? <div className="word-hint-translation">{translation}</div> : null}
        </div>

        <div className="word-hint-body">
          {hasExamples ? (
            <section className="word-hint-section">
              <div className="word-hint-label">{t('Как это говорят', 'So wird es gesagt')}</div>
              {examples.map((example) => (
                <div className="word-hint-example" key={example.learning}>
                  <div className="word-hint-example-learning">{example.learning}</div>
                  {example.native ? (
                    <div className="word-hint-example-native">{example.native}</div>
                  ) : null}
                </div>
              ))}
            </section>
          ) : null}

          {hasForms ? (
            <section className="word-hint-section">
              <div className="word-hint-label">{t('Формы', 'Formen')}</div>
              <div className="word-hint-forms">
                {formRows.map((row) => (
                  <div className="word-hint-form" key={`${row.label}-${row.value}`}>
                    <span className="word-hint-form-label">{row.label}</span>
                    <span className="word-hint-form-value">{row.value}</span>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          {tip ? (
            <section className="word-hint-section">
              <div className="word-hint-label">{t('Как запомнить', 'Eselsbrücke')}</div>
              <p className="word-hint-tip">{tip}</p>
            </section>
          ) : null}
        </div>

        <button type="button" className="word-hint-ok" onClick={onClose}>
          {t('Понятно', 'Verstanden')}
        </button>
      </div>
    </div>
  );

  const target = typeof document !== 'undefined' ? document.body : null;
  return target ? createPortal(node, target) : node;
}
