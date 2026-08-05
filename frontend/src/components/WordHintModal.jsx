import React, { useRef } from 'react';
import { createPortal } from 'react-dom';
import useModalFit from './useModalFit';
import {
  WordBreakdown, SpeakButton, useTts, displaySurface, exampleKeyOf,
} from '../dictionary/WordBreakdown';
import './WordHintModal.css';

/**
 * «Подсказка по слову» — то, что уже собрано в карточке словаря, но человек этого не
 * видел. Открывается лампочкой на карточке повторения ПОСЛЕ переворота, чтобы не
 * подсказывать до ответа.
 *
 * Показывает ТУ ЖЕ карточку, что и словарь (общий `WordBreakdown`): транскрипцию с
 * ударением, часть речи, уровень, частотность, склонение/спряжение, управление,
 * сочетания, примеры. Учить слово и смотреть на его формы человек хочет здесь, а не
 * специальным заходом в словарь; одна общая карточка ещё и не даёт двум местам разойтись.
 *
 * Компонент ничего не досбирает: ни обращения к модели, ни расхода дневного лимита —
 * всё уже лежит в `response_json` карточки. Тяжёлые таблицы приходят свёрнутыми.
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
  // Пример, дословно повторяющий заголовок карточки, ничего не добавляет — и он же не
  // должен зажигать лампочку у сохранённого целого предложения.
  const headKey = exampleKeyOf(item?.word_de);
  const seen = new Set();
  const out = [];
  pairs.forEach((pair) => {
    const key = pair.learning.toLowerCase();
    if (seen.has(key) || out.length >= limit) return;
    if (headKey && exampleKeyOf(pair.learning) === headKey) return;
    seen.add(key);
    out.push(pair);
  });
  return out;
}

/**
 * Есть ли в карточке разбор, который умеет показать общий `WordBreakdown`.
 * Тот же список полей, что у словаря, — иначе лампочка обещала бы больше, чем откроет.
 */
export function hasHintBreakdown(item) {
  if (!item || typeof item !== 'object') return false;
  return !!(
    item.part_of_speech || item.meanings || item.grammar_tables || item.forms
    || item.etymology_note
    || (item.synonyms || []).length || (item.antonyms || []).length
    || (item.related_words || []).length || (item.common_collocations || []).length
    || (item.usage_examples || []).length || (item.government_patterns || []).length
  );
}

export default function WordHintModal({
  isOpen,
  onClose,
  tr,
  headword,
  translation,
  item = null,
  examples = [],
  formRows = [],
  memoryTip = '',
}) {
  const overlayRef = useRef(null);
  const cardRef = useRef(null);
  const bodyRef = useRef(null);
  // Озвучка — та же, что в словаре: звук идёт только по нажатию на 🔊, сам по себе
  // ничего не синтезируется.
  const tts = useTts();
  const t = tr || ((ru) => ru);
  const tip = String(memoryTip || '').trim();
  const breakdown = hasHintBreakdown(item) ? item : null;
  // Регистр заголовка: немецкий глагол пишется со строчной, а часть карточек сохранена
  // с заглавной (слово попало из начала предложения).
  const title = breakdown ? displaySurface(breakdown, headword) : headword;
  // Содержимое сменилось — повод пересчитать: у длинной фразы с тремя примерами и у
  // короткого слова с одной формой масштаб разный.
  useModalFit(
    overlayRef, cardRef, bodyRef, isOpen,
    `${headword}|${breakdown ? 'full' : 'lite'}|${examples.length}|${formRows.length}|${tip.length}`,
  );
  if (!isOpen) return null;
  const closeLabel = t('Закрыть', 'Schließen');
  const hasExamples = examples.length > 0;
  const hasForms = formRows.length > 0;

  const node = (
    <div className="word-hint-overlay" role="dialog" aria-modal="true" ref={overlayRef}>
      <button type="button" className="word-hint-backdrop" aria-label={closeLabel} onClick={onClose} />
      <div className="word-hint-card" ref={cardRef} onClick={(event) => event.stopPropagation()}>
        <button type="button" className="word-hint-close" aria-label={closeLabel} onClick={onClose}>×</button>
        <div className="word-hint-head">
          <div className="word-hint-word">
            {title}
            {breakdown ? <SpeakButton text={title} tts={tts} /> : null}
          </div>
          {translation ? <div className="word-hint-translation">{translation}</div> : null}
        </div>

        <div className="word-hint-body" ref={bodyRef}>
          {breakdown ? (
            <div className="word-hint-breakdown dq-card">
              {/* Значения и варианты перевода прячем: перевод стоит в шапке окна, а сама
                  карточка повторения перечисляет значения под ответом. Здесь важнее то,
                  чего человек больше нигде не видит — звучание, формы, управление,
                  сочетания и живые примеры. */}
              <WordBreakdown item={breakdown} tts={tts} tablesOpen={false} hideMeanings />
            </div>
          ) : null}

          {!breakdown && hasExamples ? (
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

          {!breakdown && hasForms ? (
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
