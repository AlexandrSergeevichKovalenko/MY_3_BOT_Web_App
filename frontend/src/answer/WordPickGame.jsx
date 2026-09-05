import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import FsrsHeadword from './FsrsHeadword.jsx';
import WordHintModal, { collectHintExamples, hasHintBreakdown } from '../components/WordHintModal.jsx';
import CardOwnNotes from '../components/CardOwnNotes.jsx';
import { splitTranslationSenses } from '../dictionary/senses.js';
import { playWordTts } from './wordTts.js';
import Toast, { useToast } from './Toast.jsx';

// «Слова со вчерашних тренировок» — разовое повторение того, что человек сам тапнул
// вчера в конце интерактивов. Стратегия: docs/tasks/word_pick_review_strategy.md.
//
// ⚠️ Стили Space Rep живут в App.css, а оверлей интерактивов его НЕ грузит (main.jsx
// подключает только theme.css, App.css приезжает вместе с App.jsx). 05.09.2026 владелец
// открыл экран и увидел голую разметку: классы .fsrs-* здесь ничего не значат. Поэтому у
// экрана СВОИ классы wp-* в answer.css; из Space Rep взяты только сами элементы и порядок.
// Единственный чужой класс — .fsrs-card-target у FsrsHeadword: он задан в answer.css под .wp-card.
//
// Детали карточки — те же, что в «Карточках Space Rep» (App.jsx, блок fsrs-study-card):
// русский вопрос → «Показать ответ» (или сам через 5 с) → немецкое слово с автоподгонкой
// → озвучка сразу и кнопкой → лампочка, если разбор уже есть → заметки → четыре оценки
// с подсказкой интервала. Оценка уходит в /api/cards/review с queue_source='pick' и
// пишется в НАСТОЯЩЕЕ расписание (решение владельца 04.09.2026). Следующую карточку
// выбирает сам экран: набор дня у него на руках, сервер `next` не отдаёт.
//
// Известное ограничение (05.09.2026): таблицу форм (Plural / Präteritum / Perfekt …)
// лампочка здесь НЕ показывает — `formRows={[]}`. В App.jsx строки форм собирает
// getDictionaryFormRows, а она замкнута на состояние приложения (языковая пара
// dictionaryLanguagePair / languageProfile), которого у лёгкого оверлея нет. Примеры,
// разбор и подсказка памяти из готового response_json показываются полностью.
const AUTO_REVEAL_SEC = 5;
const RATINGS = [
  ['AGAIN', 'Снова', 'again'],
  ['HARD', 'Трудно', 'hard'],
  ['GOOD', 'Хорошо', 'good'],
  ['EASY', 'Легко', 'easy'],
];
const ru = (s) => s; // CardOwnNotes/WordHintModal просят функцию перевода; здесь всё по-русски

function intervalHint(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return '';
  if (s < 3600) return `${Math.max(1, Math.ceil(s / 60))} мин`;
  if (s < 86400) return `${Math.max(1, Math.ceil(s / 3600))} ч`;
  return `${Math.max(1, Math.ceil(s / 86400))} дн`;
}
const hasCyrillic = (s) => /[а-яё]/i.test(String(s || ''));

// Вопрос — всегда русская сторона, ответ — немецкая (как в Space Rep, App.jsx ~42945).
function sidesOf(card) {
  const a = String(card?.word_ru || card?.translation_ru || '').trim();
  const b = String(card?.word_de || card?.translation_de || '').trim();
  const question = hasCyrillic(a) ? a : (hasCyrillic(b) ? b : a);
  const answer = question === a ? b : a;
  const senses = splitTranslationSenses(answer);
  return {
    question,
    answer: senses.length > 1 ? senses[0].value : answer,
    extra: senses.length > 1
      ? senses.slice(1, 6).map((x, i) => ({ rank: i + 2, text: x.label ? `${x.value} (${x.label})` : x.value }))
      : [],
    spoken: String(card?.word_de || card?.translation_de || answer).trim(),
  };
}

export default function WordPickGame({ day, api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|card|done|error
  const [error, setError] = useState('');
  const [slot, setSlot] = useState('am');
  const [items, setItems] = useState([]);
  const [queue, setQueue] = useState([]);       // индексы ещё не оценённых в этом проходе
  const [revealed, setRevealed] = useState(false);
  const [revealedAt, setRevealedAt] = useState(0);
  const [elapsed, setElapsed] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [hintOpen, setHintOpen] = useState(false);
  const [ratedNow, setRatedNow] = useState(0);
  const toast = useToast();
  const spokenRef = useRef('');

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/wordpick/set', { day });
        if (cancelled) return;
        const list = Array.isArray(data?.items) ? data.items : [];
        setItems(list);
        setSlot(data?.slot === 'pm' ? 'pm' : 'am');
        const pending = list.map((it, i) => (it.rated ? -1 : i)).filter((i) => i >= 0);
        setQueue(pending);
        setPhase(pending.length ? 'card' : 'done');
      } catch (e) {
        if (!cancelled) { setError('Не удалось загрузить набор. Попробуй ещё раз.'); setPhase('error'); }
      }
    })();
    return () => { cancelled = true; };
  }, [api, day]);

  const current = phase === 'card' && queue.length ? items[queue[0]] : null;
  const card = current?.card || null;
  const sides = useMemo(() => (card ? sidesOf(card) : null), [card]);

  // Автораскрытие через 5 с, как в Space Rep.
  useEffect(() => {
    if (!card || revealed) return undefined;
    const t = window.setTimeout(() => reveal(), AUTO_REVEAL_SEC * 1000);
    return () => window.clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [card?.id, revealed]);

  // Секундомер ответа.
  useEffect(() => {
    if (!revealed || !revealedAt) return undefined;
    const id = window.setInterval(() => setElapsed(Math.floor((Date.now() - revealedAt) / 1000)), 250);
    return () => window.clearInterval(id);
  }, [revealed, revealedAt]);

  // Озвучка один раз при открытии ответа.
  useEffect(() => {
    if (!revealed || !sides?.spoken) return;
    const key = `${card?.id}`;
    if (spokenRef.current === key) return;
    spokenRef.current = key;
    playWordTts(sides.spoken).catch(() => { /* звук не обязателен; кнопка ниже повторит */ });
  }, [revealed, sides, card]);

  const reveal = useCallback(() => {
    setRevealedAt(Date.now()); setElapsed(0); setHintOpen(false); setRevealed(true);
  }, []);

  const rate = useCallback(async (rating) => {
    if (!card || submitting) return;
    setSubmitting(true);
    try {
      await api('/api/cards/review', {
        card_id: card.id, rating, queue_source: 'pick', mode: 'fsrs', day,
        response_ms: revealedAt ? Math.max(0, Date.now() - revealedAt) : null,
      });
      try { haptic?.('ok'); } catch (_e) { /* noop */ }
      setRatedNow((n) => n + 1);
      setRevealed(false); setRevealedAt(0); setElapsed(0);
      setQueue((q) => {
        const rest = q.slice(1);
        if (!rest.length) setPhase('done');
        return rest;
      });
    } catch (e) {
      try { haptic?.('bad'); } catch (_e) { /* noop */ }
      toast.show('Оценка не сохранилась. Проверь связь и нажми ещё раз.');
    } finally {
      setSubmitting(false);
    }
  }, [api, card, day, haptic, revealedAt, submitting, toast]);

  const shell = (body, cls = '') => (
    <div className="ans-root ans-root--keepkbd">
      <div className={`ans-card wp-card ${cls}`}>{body}</div>
      <Toast state={toast.state} onClose={toast.hide} />
    </div>
  );

  if (phase === 'loading') return shell(<><div className="ans-skel" /><div className="ans-skel sm" /></>);
  if (phase === 'error') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );
  if (phase === 'done') {
    const total = items.length;
    return shell(
      <>
        <div className="ans-head"><span className="ans-eyebrow">🔁 Слова со вчерашних тренировок</span></div>
        <div className="tr-score">
          <div className="tr-score-num">{total}</div>
          <div className="tr-score-sub">{total === 1 ? 'слово повторено' : 'слов повторено'} · {slot === 'am' ? 'утренний' : 'вечерний'} проход</div>
        </div>
        <div className="tr-done-note">
          {slot === 'am'
            ? 'Вечером в 19:35 этот набор придёт ещё раз. Завтра придут те слова, что отберёшь сегодня.'
            : 'Готово на сегодня. Завтра придут те слова, что отберёшь сегодня в тренировках.'}
        </div>
        <button className="ans-btn" onClick={onClose}>Schließen</button>
      </>
    );
  }

  const done = items.length - queue.length;
  const hintSource = card?.response_json && typeof card.response_json === 'object' ? card.response_json : null;
  const hintExamples = collectHintExamples(hintSource, 3);
  const hintTip = String(hintSource?.memory_tip || '').trim();
  const hintAvailable = hintExamples.length > 0 || hasHintBreakdown(hintSource) || !!hintTip;
  const preview = current?.srs_preview || {};
  const notes = Array.isArray(card?.user_notes) ? card.user_notes : [];

  return shell(
    <>
      <div className="ans-head">
        <span className="ans-eyebrow">🔁 Слова со вчерашних тренировок</span>
        <span className="wp-progress">{done + 1} из {items.length}</span>
      </div>
      <div className={`wp-study ${revealed ? 'is-revealed' : ''}`}>
        {revealed && hintAvailable && (
          <button type="button" className="wp-hint-btn" onClick={() => setHintOpen(true)}
                  aria-label="Всё об этом слове" title="Всё об этом слове">💡</button>
        )}
        <div className="wp-source">{sides.question}</div>
        <div className="wp-divider" />
        {revealed ? (
          <>
            <FsrsHeadword text={sides.answer} />
            {sides.extra.length > 0 && (
              <div className="wp-meaning-list">
                {sides.extra.map((row) => (
                  <div key={row.rank} className="wp-meaning-item">
                    <span className="wp-meaning-rank">{row.rank}.</span>
                    <span className="wp-meaning-text">{row.text}</span>
                  </div>
                ))}
              </div>
            )}
            {notes.length > 0 && <CardOwnNotes notes={notes} tr={ru} readOnly />}
            <div className="wp-divider" />
            <div className="wp-meta">Response time: {elapsed}s</div>
            <button type="button" className="wp-audio"
                    onClick={() => playWordTts(sides.spoken).catch(() => toast.show('Озвучка не загрузилась.'))}
                    aria-label="Повторить аудио" title="Повторить аудио">🔊</button>
          </>
        ) : (
          <>
            <div className="wp-meta">
              Status: {String(current?.srs?.status || 'new')} · Interval: {current?.srs?.interval_days ?? 0} дн
            </div>
            <button type="button" className="wp-show-btn" onClick={reveal} disabled={submitting}>Show Answer</button>
          </>
        )}
      </div>
      {revealed && (
        <div className="wp-rating-wrap">
          <div className="wp-rating-grid">
            {RATINGS.map(([key, label, cls]) => (
              <div className="wp-rate-cell" key={key}>
                <button type="button" className={`wp-rate-btn ${cls}`} onClick={() => rate(key)} disabled={submitting}>
                  <span>{label}</span>
                </button>
                <small className="wp-rate-hint">{intervalHint(preview?.[key]?.seconds)}</small>
              </div>
            ))}
          </div>
        </div>
      )}
      <WordHintModal
        isOpen={hintOpen}
        onClose={() => setHintOpen(false)}
        tr={ru}
        headword={sides.spoken || sides.answer}
        translation={sides.question}
        item={hintSource}
        examples={hintExamples}
        formRows={[]}
        memoryTip={hintTip}
      />
    </>
  );
}
