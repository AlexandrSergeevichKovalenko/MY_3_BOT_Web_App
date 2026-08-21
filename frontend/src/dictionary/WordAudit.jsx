import React, { useCallback, useEffect, useState } from 'react';
import './wordAudit.css';
import { requestTabletFullscreen } from '../utils/tabletFullscreen.js';

/**
 * Проверка слов: человек решает, что делать со словами, которые мы не смогли подтвердить.
 *
 * Зачем экран. Дверь слова сверяет каждое сохранённое слово со справочниками. Часть слов
 * не подтверждается: редкие, из другого языка, или потерявшие букву при сохранении.
 * Молча удалять их нельзя — человек сохранял их осознанно. Молча оставлять тоже нельзя:
 * он будет учить слово в кривом виде.
 *
 * Почему не сообщение с кнопками в личке. Владелец 20.08.2026: «сделай красивый интерфейс
 * в мини-аппе, а не сообщение». В чате не помещается объяснение, не помещается подсказка
 * правильного написания и негде править текст руками.
 *
 * Правило экрана: НИКАКИХ РЕБУСОВ. У каждого слова написано, почему оно здесь, простыми
 * словами. Последствие решения видно до нажатия «Готово». До «Готово» не меняется ничего.
 */

async function api(path, body = {}) {
  const initData = window.Telegram?.WebApp?.initData || '';
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Telegram-Init-Data': initData },
    body: JSON.stringify({ initData, ...body }),
  });
  const data = await response.json().catch(() => null);
  if (!response.ok || !data) {
    const error = new Error(data?.message || 'Не удалось связаться с сервером');
    error.status = response.status;
    throw error;
  }
  return data;
}

const KEEP = 'keep';
const FIXED = 'fixed';
const MANUAL = 'manual';
const RETRANS = 'retrans';

export default function WordAudit() {
  const [items, setItems] = useState(null);
  const [state, setState] = useState({});       // слово → решение
  const [typed, setTyped] = useState({});       // слово → написание, вписанное руками
  const [typedTrans, setTypedTrans] = useState({}); // слово → перевод, вписанный руками
  const [editing, setEditing] = useState({});   // слово → открыто ли поле правки
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(null);

  useEffect(() => { requestTabletFullscreen(); }, []);

  const load = useCallback(async () => {
    setError('');
    try {
      const data = await api('/api/webapp/word-audit/list');
      if (!data.ok) { setError(data.message || 'Не удалось загрузить список.'); return; }
      setItems(data.items || []);
    } catch (e) {
      // Пустой список и «не загрузилось» — разные вещи, и человек должен их различать.
      setError('Не удалось загрузить список. Проверь связь и попробуй ещё раз.');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const pick = (word, action) => {
    setState((prev) => ({ ...prev, [word]: prev[word] === action ? '' : action }));
  };

  const saveManual = (word) => {
    const text = (typed[word] || '').trim();
    if (!text) return;
    setState((prev) => ({ ...prev, [word]: MANUAL }));
    setEditing((prev) => ({ ...prev, [word]: false }));
  };

  const submit = async () => {
    if (!items) return;
    setBusy(true);
    setError('');
    const decisions = items.map((it) => ({
      word: it.word,
      action: state[it.word] || '',
      text: state[it.word] === MANUAL ? (typed[it.word] || '').trim() : it.suggestion,
      // Свой перевод уходит только вместе со своим вариантом — в остальных
      // решениях перевод человек не трогал, и переписывать его нечем.
      translation: state[it.word] === MANUAL ? (typedTrans[it.word] || '').trim() : '',
    }));
    try {
      const data = await api('/api/webapp/word-audit/apply', { decisions });
      if (!data.ok) { setError(data.message || 'Не удалось сохранить.'); setBusy(false); return; }
      setDone(data.counts || {});
    } catch (e) {
      setError('Не удалось сохранить решения. Ничего не изменилось — попробуй ещё раз.');
    }
    setBusy(false);
  };

  if (items === null && !error) {
    return <div className="wa"><div className="wa-loading">Загружаю твои слова…</div></div>;
  }

  if (done) {
    const parts = Object.entries(done).filter(([, n]) => n > 0)
      .map(([k, n]) => `${k}: ${n}`);
    return (
      <div className="wa">
        <div className="wa-final">
          <div className="wa-final-emoji">🦊</div>
          <h2>Готово</h2>
          <p>{parts.length ? parts.join(' · ') : 'Ничего не изменилось.'}</p>
          <p className="wa-final-note">
            Карточки достроим этой ночью — часть речи, род и формы появятся сами.
          </p>
        </div>
      </div>
    );
  }

  if (error && !items?.length) {
    return (
      <div className="wa">
        <div className="wa-empty">
          <p>{error}</p>
          <button type="button" className="wa-retry" onClick={load}>Попробовать ещё раз</button>
        </div>
      </div>
    );
  }

  if (!items.length) {
    return (
      <div className="wa">
        <div className="wa-empty">
          <div className="wa-final-emoji">✅</div>
          <h2>Все слова в порядке</h2>
          <p>Мы сверили твой словарь со справочниками — проверять нечего.</p>
        </div>
      </div>
    );
  }

  const keep = items.filter((it) => [KEEP, FIXED, MANUAL].includes(state[it.word])).length;
  const retrans = items.filter((it) => state[it.word] === RETRANS).length;
  const drop = items.length - keep - retrans;

  return (
    <div className="wa">
      <header className="wa-head">
        <div className="wa-kicker">🦊 Проверка слов</div>
        <h1>{items.length} {plural(items.length, 'слово', 'слова', 'слов')} ждут твоего решения</h1>
        <p className="wa-lede">
          Ты их сохранял, а мы не смогли подтвердить, что они есть в немецком языке.
          Посмотри и реши, что с ними делать.
        </p>
      </header>

      <details className="wa-why">
        <summary>Откуда взялись эти слова и что будет дальше</summary>
        <p><b>Откуда.</b> Мы сверяем каждое сохранённое слово с немецкими справочниками.
          Эти не нашлись: возможно, слово редкое, возможно — из другого языка, а возможно,
          при сохранении потерялась буква.</p>
        <p><b>Зачем проверять.</b> Если слово с ошибкой останется, ты будешь учить его
          в таком виде — и запомнишь неправильно.</p>
        <p><b>Что делать.</b> Если мы догадались, как слово пишется правильно, вверху будет
          готовая кнопка <b>«Да, это …»</b> — одно касание. <b>«Оставить как есть»</b> —
          слово верное, ничего не меняем. <b>«Перевод не тот»</b> — слово верное, а перевод
          плохой: соберём карточку заново этой ночью. <b>«Свой вариант»</b> — впиши слово
          так, как надо, и, если нужно, свой перевод: мы сохраним именно их.</p>
        <p><b>Если ничего не нажать.</b> Слово удалится — из твоего словаря и из тренировок.
          Ничего не произойдёт, пока не нажмёшь «Готово» внизу.</p>
        <p><b>Сколько делать.</b> Сколько хочешь. Непроверенные придут снова через несколько дней.</p>
      </details>

      <div className="wa-tally">
        <span className="wa-t-keep"><b>{keep}</b>оставим</span>
        <span className="wa-t-fix"><b>{retrans}</b>переделаем</span>
        <span><b>{drop}</b>удалим</span>
      </div>

      {items.map((it) => {
        const chosen = state[it.word] || '';
        return (
          <div className="wa-card" data-state={chosen} key={it.word}>
            <div className="wa-word-row">
              <span className="wa-word">{state[it.word] === MANUAL ? typed[it.word] : it.word}</span>
            </div>
            {it.translation ? <p className="wa-trans">{it.translation}</p> : null}
            <div className="wa-reason">{it.why}</div>

            {it.suggestion ? (
              <button type="button" className="wa-suggest"
                      onClick={() => pick(it.word, FIXED)}>
                {chosen === FIXED ? '✓ ' : ''}Да, это «{it.suggestion}»
              </button>
            ) : null}

            <div className="wa-actions">
              <button type="button" className="wa-act wa-act-keep"
                      onClick={() => pick(it.word, KEEP)}>Оставить как есть</button>
              <button type="button" className="wa-act wa-act-retrans"
                      onClick={() => pick(it.word, RETRANS)}>Перевод не тот</button>
            </div>

            {editing[it.word] ? (
              <div className="wa-edit">
                <label>Слово по-немецки
                  <input value={typed[it.word] ?? (it.suggestion || it.word)}
                         aria-label="правильное написание"
                         onChange={(e) => setTyped((p) => ({ ...p, [it.word]: e.target.value }))} />
                </label>
                <label>Перевод <span className="wa-opt">— если и он не тот</span>
                  <input value={typedTrans[it.word] ?? ''}
                         aria-label="перевод"
                         placeholder={it.translation || 'как переводится'}
                         onChange={(e) => setTypedTrans((p) => ({ ...p, [it.word]: e.target.value }))} />
                </label>
                <button type="button" onClick={() => saveManual(it.word)}>Сохранить</button>
              </div>
            ) : (
              <button type="button" className="wa-manual"
                      onClick={() => setEditing((p) => ({ ...p, [it.word]: true }))}>
                свой вариант — вписать слово и перевод
              </button>
            )}
          </div>
        );
      })}

      {error ? <div className="wa-error">{error}</div> : null}
      <p className="wa-tail">Проверка приходит два раза в неделю. Подтверждённые слова больше не спросим.</p>

      <div className="wa-bar">
        <div className="wa-bar-inner">
          <p className="wa-bar-note">
            {drop === items.length
              ? `Ничего не отмечено — все ${items.length} ${plural(items.length, 'слово', 'слова', 'слов')} будут удалены`
              : <>Оставим <b>{keep}</b> · переделаем <b>{retrans}</b> · удалим <b>{drop}</b></>}
          </p>
          <button type="button" className="wa-done" disabled={busy} onClick={submit}>
            {busy ? 'Сохраняю…' : 'Готово'}
          </button>
        </div>
      </div>
    </div>
  );
}

function plural(n, one, few, many) {
  const mod10 = n % 10;
  const mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return few;
  return many;
}
