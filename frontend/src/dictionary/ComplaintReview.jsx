import React, { useCallback, useEffect, useState } from 'react';
import './wordAudit.css';
import './complaintReview.css';
import { requestTabletFullscreen } from '../utils/tabletFullscreen.js';

/**
 * Разбор жалоб на карточки — экран владельца.
 *
 * Откуда берётся. Человек нажал в своей карточке «Пожаловаться на разбор». Ночью модель
 * посмотрела слово, его нынешний разбор и жалобу и сказала: надо ли менять карточку,
 * что именно не так и что поставить взамен. Здесь владелец решает по каждой.
 *
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  МОДЕЛЬ НИЧЕГО НЕ ПРИМЕНИЛА. Решение владельца 26.08.2026: разбор лежит на    ║
 * ║  ОБЩЕМ слове, правка меняет карточку всем, кто это слово учит. Жалоба одного  ║
 * ║  человека — повод перепроверить, а не команда изменить.                       ║
 * ║                                                                              ║
 * ║  Не отмеченная ничем жалоба НЕ ЗАКРЫВАЕТСЯ — она придёт в следующей пачке.    ║
 * ║  То же правило, что на экране проверки слов: молчание не решение.             ║
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

// Имена полей карточки по-человечески. Служебное имя на экране решения — это ребус:
// владелец 26.08.2026 смотрел на строку «meanings: …» и не понимал, что ему предлагают.
// Незнакомое поле показываем как есть — это честнее, чем придумать ему красивое имя.
const ИМЕНА_ПОЛЕЙ = {
  translation_ru: 'Перевод',
  target_text: 'Перевод',
  meanings: 'Значения',
  usage_examples: 'Примеры',
  forms: 'Формы',
  grammar_tables: 'Грамматика',
  synonyms: 'Синонимы',
  antonyms: 'Антонимы',
  related_words: 'Однокоренные',
  common_collocations: 'Устойчивые сочетания',
  government_patterns: 'Управление',
  part_of_speech: 'Часть речи',
  memory_tip: 'Как запомнить',
  etymology_note: 'Происхождение',
};

function имяПоля(ключ) {
  return ИМЕНА_ПОЛЕЙ[ключ] || ключ;
}

// Значение поля человеческим текстом. Поля карточки бывают строкой, списком строк и
// списком пар «немецкое — русское»; JSON на экране решения недопустим.
function читаемо(значение) {
  if (значение === null || значение === undefined || значение === '') return '';
  if (typeof значение === 'string') return значение;
  if (typeof значение === 'number' || typeof значение === 'boolean') return String(значение);
  if (Array.isArray(значение)) {
    return значение.map((кусок) => читаемо(кусок)).filter(Boolean).join(' · ');
  }
  if (typeof значение === 'object') {
    const пара = [значение.source, значение.target].filter(Boolean);
    if (пара.length === 2) return `${пара[0]} — ${пара[1]}`;
    const одно = значение.text || значение.meaning || значение.translation
      || значение.value || значение.de || значение.ru;
    if (одно) return String(одно);
    return Object.values(значение).map((v) => читаемо(v)).filter(Boolean).join(' · ');
  }
  return '';
}

const ПРИНЯТЬ = 'принять';
const ПЕРЕСОБРАТЬ = 'пересобрать';
const ОТКЛОНИТЬ = 'отклонить';
const СВОЁ = 'своё';

export default function ComplaintReview() {
  const [items, setItems] = useState(null);
  const [state, setState] = useState({});     // id → решение
  const [typed, setTyped] = useState({});     // id → свой перевод
  const [editing, setEditing] = useState({});
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(null);

  useEffect(() => { requestTabletFullscreen(); }, []);

  const load = useCallback(async () => {
    setError('');
    try {
      const data = await api('/api/webapp/admin/complaints/list');
      if (!data.ok) { setError(data.message || 'Не удалось загрузить жалобы.'); return; }
      setItems(data.items || []);
    } catch (e) {
      // Пустой список и «не загрузилось» — разные вещи, и различать их обязан экран.
      setError('Не удалось загрузить жалобы. Проверь связь и попробуй ещё раз.');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const pick = (id, action) => {
    setState((prev) => ({ ...prev, [id]: prev[id] === action ? '' : action }));
  };

  const submit = async () => {
    if (!items) return;
    setBusy(true);
    setError('');
    const decisions = items
      .filter((it) => state[it.id])
      .map((it) => ({
        id: it.id,
        action: state[it.id],
        text: state[it.id] === СВОЁ ? (typed[it.id] || '').trim() : '',
      }));
    if (!decisions.length) { setBusy(false); setError('Ничего не отмечено.'); return; }
    try {
      const data = await api('/api/webapp/admin/complaints/apply', { decisions });
      if (!data.ok) { setError(data.message || 'Не удалось сохранить.'); setBusy(false); return; }
      setDone(data.counts || {});
    } catch (e) {
      setError('Не удалось сохранить решения. Ничего не изменилось — попробуй ещё раз.');
    }
    setBusy(false);
  };

  if (items === null && !error) {
    return <div className="wa"><div className="wa-loading">Загружаю жалобы…</div></div>;
  }

  if (done) {
    const parts = Object.entries(done).filter(([, n]) => n > 0).map(([k, n]) => `${k}: ${n}`);
    return (
      <div className="wa">
        <div className="wa-final">
          <div className="wa-final-emoji">🦊</div>
          <h2>Готово</h2>
          <p>{parts.length ? parts.join(' · ') : 'Ничего не изменилось.'}</p>
          <p className="wa-final-note">
            Людям, чьи жалобы разобраны, ответ уйдёт в их ближайшей недельной пачке.
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
          <h2>Жалоб нет</h2>
          <p>Никто не жаловался на разборы карточек — разбирать нечего.</p>
        </div>
      </div>
    );
  }

  const отмечено = items.filter((it) => state[it.id]).length;

  return (
    <div className="wa">
      <header className="wa-head">
        <div className="wa-kicker">🦊 Жалобы на разбор</div>
        <h1>{items.length} {plural(items.length, 'жалоба', 'жалобы', 'жалоб')} ждут решения</h1>
        <p className="wa-lede">
          Люди пожаловались на карточки. Ночью модель посмотрела каждую и сказала, надо ли
          менять карточку и что поставить взамен. Решаешь ты — сама она ничего не изменила.
          Правка ложится на общее слово, то есть на карточку всех, кто его учит.
        </p>
      </header>

      {items.map((it) => {
        const chosen = state[it.id] || '';
        const поля = Object.entries(it.proposal || {});
        return (
          <div className="wa-card cr-card" data-state={chosen} key={it.id}>
            <div className="wa-word-row">
              <span className="wa-word">{it.word}</span>
              <span className={it.card_is_wrong ? "cr-tag cr-tag-yes" : "cr-tag cr-tag-no"}>
                {it.card_is_wrong ? 'модель: карточку надо менять' : 'модель: карточка верна'}
              </span>
              {it.confidence ? <span className="cr-tag">уверенность {it.confidence}</span> : null}
            </div>

            <div className="cr-block">
              <div className="cr-block-title">Что сейчас в карточке</div>
              {it.now?.translation ? <p className="cr-line">{it.now.translation}</p> : null}
              {(it.now?.examples || []).map((пример, i) => (
                <p className="cr-line cr-dim" key={i}>{пример.source}</p>
              ))}
              {!it.now?.translation && !(it.now?.examples || []).length
                ? <p className="cr-line cr-dim">разбора нет вовсе</p> : null}
            </div>

            <div className="cr-block">
              <div className="cr-block-title">На что жалуются</div>
              <p className="cr-line">{it.note || 'человек не уточнил, что именно не так'}</p>
            </div>

            <div className="cr-block">
              <div className="cr-block-title">Что говорит модель</div>
              <p className="cr-line">{it.why || '—'}</p>
              {it.proposal_words ? <p className="cr-line cr-fix">{it.proposal_words}</p> : null}
            </div>

            {/* Модель считает, что карточка вообще про другое слово. Кнопкой «заменить
                поле» это не чинится: заголовок живёт не в разборе, а в самой единице —
                правка дала бы полукарточку. Говорим прямо и оставляем пересборку. */}
            {Object.keys(it.renames || {}).length ? (
              <div className="cr-block cr-rename">
                <div className="cr-block-title">Похоже, карточка про другое слово</div>
                {Object.entries(it.renames).map(([ключ, новое]) => (
                  <p className="cr-line" key={ключ}>
                    {имяПоля(ключ)}: <s className="cr-was-inline">{читаемо((it.before || {})[ключ])}</s>
                    {' → '}<b>{читаемо(новое)}</b>
                  </p>
                ))}
                <p className="cr-line cr-dim cr-diff-note">
                  Точечной правкой это не исправить — имя слова хранится отдельно от
                  разбора. Нужна пересборка карточки.
                </p>
              </div>
            ) : null}

            {/* ⚠ ВОТ РАДИ ЧЕГО СУЩЕСТВУЕТ ЭКРАН: видно, ЧТО именно заменят и НА ЧТО.
                Раньше здесь висела строка «meanings: …» — служебное имя поля и новый
                текст без старого. По такому решение принять нельзя, а мы требовали. */}
            {поля.length ? (
              <div className="cr-block cr-diff">
                <div className="cr-block-title">Что изменится, если принять</div>
                {поля.map(([ключ, новое]) => {
                  const было = читаемо((it.before || {})[ключ]);
                  const станет = читаемо(новое);
                  return (
                    <div className="cr-field" key={ключ}>
                      <div className="cr-field-name">{имяПоля(ключ)}</div>
                      <p className="cr-was">
                        <span className="cr-mark">было</span>
                        {было || <i className="cr-empty">пусто</i>}
                      </p>
                      <p className="cr-will">
                        <span className="cr-mark cr-mark-new">станет</span>
                        {станет}
                      </p>
                    </div>
                  );
                })}
                <p className="cr-line cr-dim cr-diff-note">
                  Остальное в карточке не тронем — примеры, синонимы и сочетания
                  останутся прежними. Если карточка неверна целиком, нужна пересборка.
                </p>
              </div>
            ) : null}

            <div className="wa-actions">
              {поля.length ? (
                <button type="button" className="wa-act cr-act-accept"
                        onClick={() => pick(it.id, ПРИНЯТЬ)}>
                  {/* Кнопка называет ТО, ЧТО СДЕЛАЕТ: «принять вариант» само по себе
                      не говорит, что именно заменят. */}
                  {chosen === ПРИНЯТЬ ? '✓ ' : ''}
                  Заменить {поля.map(([ключ]) => имяПоля(ключ).toLowerCase()).join(' и ')}
                </button>
              ) : null}
              <button type="button" className="wa-act"
                      onClick={() => pick(it.id, ПЕРЕСОБРАТЬ)}>
                {chosen === ПЕРЕСОБРАТЬ ? '✓ ' : ''}♻️ Пересобрать всю карточку
              </button>
              <button type="button" className="wa-act wa-act-drop"
                      onClick={() => pick(it.id, ОТКЛОНИТЬ)}>
                {chosen === ОТКЛОНИТЬ ? '✓ ' : ''}Карточка верна — отклонить
              </button>
            </div>

            {editing[it.id] ? (
              <div className="wa-edit">
                <label>Свой перевод
                  <input value={typed[it.id] ?? (it.now?.translation || '')}
                         aria-label="свой перевод"
                         onChange={(e) => setTyped((p) => ({ ...p, [it.id]: e.target.value }))} />
                </label>
                <button type="button" onClick={() => {
                  setState((p) => ({ ...p, [it.id]: СВОЁ }));
                  setEditing((p) => ({ ...p, [it.id]: false }));
                }}>Сохранить
                </button>
              </div>
            ) : (
              <button type="button" className="wa-manual"
                      onClick={() => setEditing((p) => ({ ...p, [it.id]: true }))}>
                свой вариант — вписать перевод
              </button>
            )}
          </div>
        );
      })}

      {error ? <div className="wa-error">{error}</div> : null}
      <p className="wa-tail">
        Не отмеченная жалоба ничего не меняет и придёт в следующей пачке.
      </p>

      <div className="wa-bar">
        <div className="wa-bar-inner">
          <p className="wa-bar-note">Отмечено <b>{отмечено}</b> из {items.length}</p>
          <button type="button" className="wa-done" disabled={busy} onClick={submit}>
            {busy ? 'Применяю…' : 'Готово'}
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
