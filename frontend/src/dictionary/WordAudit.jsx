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
 *
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  МОЛЧАНИЕ НЕ УДАЛЯЕТ. УДАЛЯЕТ ТОЛЬКО КНОПКА «УДАЛИТЬ».                        ║
 * ║  Решение владельца 25.08.2026, отменяет его же правило от 19.08 «отмеченные    ║
 * ║  остаются, остальные удаляются». Дословно: «нельзя удалять просто потому что   ║
 * ║  кто-то не увидел, может просмотрел случайно».                                 ║
 * ║                                                                              ║
 * ║  Список бывает на сто слов, экран длинный, палец скользит: пропустить          ║
 * ║  карточку — это норма поведения, а не решение. Цена ошибки несимметрична —    ║
 * ║  лишнее сомнительное слово придёт на проверку снова, стёртое нужное не         ║
 * ║  вернуть. Поэтому не отмеченное ничем слово остаётся жить и приходит опять.    ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
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
const DROP = 'drop';

export default function WordAudit() {
  const [items, setItems] = useState(null);
  const [state, setState] = useState({});       // слово → решение
  const [variant, setVariant] = useState({});   // фраза → какой вариант правки принят
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
      // Фраза правится тем же механизмом, что и на экране владельца, и ему нужен
      // номер строки проверки и номер принятого варианта. Сервер всё равно сверяет
      // их заново по базе — присланному номеру он не верит.
      kind: it.kind || 'word',
      review_id: it.review_id || 0,
      variant: variant[it.word] || 0,
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
          <h2>В словаре всё в порядке</h2>
          <p>Мы сверили твои слова со справочниками, а фразы прочитали ночью — проверять нечего.</p>
        </div>
      </div>
    );
  }

  // Фразы и слова живут на одном экране, но спрашивают о разном, поэтому и
  // заголовок честно называет то, что внизу лежит.
  const phrases = items.filter((it) => it.kind === 'phrase').length;
  const words = items.length - phrases;
  const heading = phrases && words
    ? `${items.length} ${plural(items.length, 'запись', 'записи', 'записей')} ждут твоего решения`
    : phrases
      ? `${phrases} ${plural(phrases, 'фраза', 'фразы', 'фраз')} ${plural(phrases, 'ждёт', 'ждут', 'ждут')} твоего решения`
      : `${words} ${plural(words, 'слово', 'слова', 'слов')} ${plural(words, 'ждёт', 'ждут', 'ждут')} твоего решения`;

  // Удаляется РОВНО то, у чего нажата кнопка «Удалить». Всё остальное — включая
  // слова, которых человек вовсе не касался, — остаётся жить (см. рамку в шапке).
  const drop = items.filter((it) => state[it.word] === DROP).length;
  const retrans = items.filter((it) => state[it.word] === RETRANS).length;
  const keep = items.length - drop - retrans;

  return (
    <div className="wa">
      <header className="wa-head">
        <div className="wa-kicker">🦊 {phrases ? 'Проверка слов и фраз' : 'Проверка слов'}</div>
        <h1>{heading}</h1>
        <p className="wa-lede">
          Ты их сохранял, а мы не смогли подтвердить, что они написаны правильно.
          Посмотри и реши, что с ними делать. Ничего не удалится само — только то,
          что ты сам отметишь кнопкой «Удалить».
        </p>
      </header>

      <details className="wa-why">
        <summary>Откуда это взялось и что будет дальше</summary>
        <p><b>Откуда.</b> Мы сверяем каждое сохранённое слово с немецкими справочниками.
          Эти не нашлись: возможно, слово редкое, возможно — из другого языка, а возможно,
          при сохранении потерялась буква.</p>
        {phrases ? (
          <p><b>Про фразы.</b> Сохранённые фразы ночью читают два проверяющих. Если они
            нашли ошибку или разошлись во мнении, фраза приходит сюда — с готовым
            вариантом, если он есть. Мы ничего не исправляем за тебя молча.</p>
        ) : null}
        <p><b>Зачем проверять.</b> Если слово с ошибкой останется, ты будешь учить его
          в таком виде — и запомнишь неправильно.</p>
        <p><b>Что делать.</b> Если мы догадались, как слово пишется правильно, вверху будет
          готовая кнопка <b>«Да, это …»</b> — одно касание. <b>«Оставить как есть»</b> —
          слово верное, ничего не меняем. <b>«Перевод не тот»</b> — слово верное, а перевод
          плохой: соберём карточку заново этой ночью. <b>«Свой вариант»</b> — впиши слово
          так, как надо, и, если нужно, свой перевод: мы сохраним именно их.
          <b>«Удалить»</b> — слово тебе не нужно, убираем его из словаря.</p>
        <p><b>Ничего не удалится само.</b> Слово уходит из словаря, только если ты сам
          нажал у него «Удалить». Всё, чего ты не тронул, остаётся на месте и просто
          придёт на проверку в следующий раз. И даже отмеченное на удаление ничего не
          теряет, пока ты не нажмёшь «Готово» внизу.</p>
        <p><b>Сколько делать.</b> Сколько хочешь. Непроверенные придут снова через несколько дней.</p>
      </details>

      <div className="wa-tally">
        <span className="wa-t-keep"><b>{keep}</b>оставим</span>
        <span className="wa-t-fix"><b>{retrans}</b>переделаем</span>
        <span className="wa-t-drop"><b>{drop}</b>удалим</span>
      </div>

      {items.map((it) => {
        const chosen = state[it.word] || '';
        const isPhrase = it.kind === 'phrase';
        const variants = Array.isArray(it.variants) ? it.variants : [];
        return (
          <div className={isPhrase ? 'wa-card wa-card-phrase' : 'wa-card'}
               data-state={chosen} key={it.word}>
            <div className="wa-word-row">
              <span className="wa-word">{state[it.word] === MANUAL ? typed[it.word] : it.word}</span>
            </div>
            {it.translation ? <p className="wa-trans">{it.translation}</p> : null}
            <div className="wa-reason">{it.why}</div>
            {it.safe && !chosen ? (
              <div className="wa-safe">Слово настоящее — трогать ничего не нужно.</div>
            ) : null}

            {/* У фразы готовых вариантов бывает несколько, а бывает ни одного:
                проверяющие не всегда сходятся, и придумывать за них мы не станем.
                Каждый вариант — своя кнопка, чтобы по нажатию было видно, что
                именно принимаешь: ровно то же правило, что на экране владельца. */}
            {isPhrase ? variants.map((v, idx) => (
              <button type="button" className="wa-suggest wa-suggest-phrase" key={v.text}
                      onClick={() => {
                        setVariant((p) => ({ ...p, [it.word]: idx }));
                        pick(it.word, FIXED);
                      }}>
                <span className="wa-suggest-de">
                  {chosen === FIXED && (variant[it.word] || 0) === idx ? '✓ ' : ''}
                  Да, правильно так: {v.text}
                </span>
                {v.ru ? <span className="wa-suggest-ru">{v.ru}</span> : null}
              </button>
            )) : null}

            {!isPhrase && it.suggestion ? (
              <button type="button" className="wa-suggest"
                      onClick={() => pick(it.word, FIXED)}>
                {chosen === FIXED ? '✓ ' : ''}Да, это «{it.suggestion}»
              </button>
            ) : null}

            <div className="wa-actions">
              <button type="button" className="wa-act wa-act-keep"
                      onClick={() => pick(it.word, KEEP)}>Оставить как есть</button>
              {/* «Перевод не тот» у фразы нет намеренно: за словом стоит ночная
                  пересборка карточки, а за фразой — нет, и кнопка, которая ничего
                  не делает, хуже отсутствующей. */}
              {isPhrase ? null : (
              <button type="button" className="wa-act wa-act-retrans"
                      onClick={() => pick(it.word, RETRANS)}>Перевод не тот</button>
              )}
              {/* Отдельного «точно удалить?» нет намеренно: кнопка работает как все
                  остальные — нажал, отметилось, нажал ещё раз, снялось. Ничего не
                  происходит до «Готово», а внизу экрана всё это время видно число
                  «удалим N». Переспрашивать на каждом слове в списке из ста — хуже. */}
              <button type="button" className="wa-act wa-act-drop"
                      onClick={() => pick(it.word, DROP)}>
                {chosen === DROP ? '✓ ' : ''}Удалить
              </button>
            </div>

            {editing[it.word] ? (
              <div className="wa-edit">
                <label>{isPhrase ? 'Фраза по-немецки' : 'Слово по-немецки'}
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
                свой вариант — вписать {isPhrase ? 'фразу' : 'слово'} и перевод
              </button>
            )}
          </div>
        );
      })}

      {error ? <div className="wa-error">{error}</div> : null}
      <p className="wa-tail">Проверка приходит два раза в неделю. Подтверждённые слова больше не спросим,
        а те, что ты не тронул, придут снова.</p>

      <div className="wa-bar">
        <div className="wa-bar-inner">
          <p className="wa-bar-note">
            {drop === 0
              ? <>Ничего не удалим — оставим <b>{keep}</b>{retrans ? <> · переделаем <b>{retrans}</b></> : null}</>
              : <>Оставим <b>{keep}</b> · переделаем <b>{retrans}</b> · удалим <b>{drop}</b> — по твоей кнопке</>}
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
