import React, { useCallback, useEffect, useState } from 'react';
import './dict.css';
import { api, haptic } from './WordBreakdown';
import { humanizeDictError } from './errors.js';

/**
 * Разбор противоречивых записей словаря — экран владельца.
 *
 * Приходит ссылкой из личных сообщений (понедельник и воскресенье, 09:00) и открывается
 * здесь, а не в чате: только на экране можно показать «было → станет» и дать выбрать
 * решение, не набирая команд.
 *
 * Три решения на запись, одно из них:
 *   Исправить — привести запись к подтверждённой справочником форме;
 *   Оставить  — мы придрались зря, больше не показывать;
 *   Удалить   — это не слово.
 * «Применить» выполняет ВСЕ отмеченные решения за один заход — по одному действию на
 * запись, а не два над одной (владелец 26.08.2026 просил сказать это прямо).
 *
 * Ничего не пропадает: неотмеченное остаётся в очереди и придёт снова.
 */

const ACTION_LABEL = { fix: 'Исправить', keep: 'Оставить', delete: 'Удалить' };

const POS_RU = {
  noun: 'существительное', verb: 'глагол', adjective: 'прилагательное',
  adverb: 'наречие', preposition: 'предлог', conjunction: 'союз',
  participle: 'причастие', particle: 'частица', pronoun: 'местоимение',
};

// scope: 'shared' — общий словарь (только владелец), 'mine' — свои слова (каждому).
// Экран один: и там, и там человек решает судьбу записи тремя кнопками.
const ENDPOINTS = {
  shared: { list: '/api/webapp/dictionary/integrity/list', apply: '/api/webapp/dictionary/integrity/apply' },
  mine: { list: '/api/webapp/dictionary/mywords/review', apply: '/api/webapp/dictionary/mywords/apply' },
};

const TITLES = {
  shared: {
    badge: '🧹 Разбор словаря',
    title: 'Записи, которые противоречат себе',
    empty: 'Противоречивых записей нет — словарь чист.',
  },
  mine: {
    badge: '📚 Мои слова',
    title: 'Слова, которые стоит поправить',
    empty: 'Всё в порядке — ваши слова записаны верно.',
  },
};

export default function WordIntegrityReview({ scope = 'shared' }) {
  const urls = ENDPOINTS[scope] || ENDPOINTS.shared;
  const copy = TITLES[scope] || TITLES.shared;
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [decisions, setDecisions] = useState({});
  const [phase, setPhase] = useState('loading');
  const [error, setError] = useState('');
  const [done, setDone] = useState(null);
  const [translations, setTranslations] = useState([]); // не смогли перевести — ждут владельца
  const [typed, setTyped] = useState({});               // что владелец вписал

  const load = useCallback(async (rescan = false) => {
    setPhase('loading');
    setError('');
    try {
      const data = await api(urls.list, { rescan });
      setItems(Array.isArray(data?.items) ? data.items : []);
      setTotal(Number(data?.total) || 0);
      setTranslations(Array.isArray(data?.translations) ? data.translations : []);
      setPhase('ready');
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('error');
    }
  }, [urls]);

  useEffect(() => { void load(true); }, [load]);

  const choose = useCallback((id, action) => {
    haptic('light');
    setDecisions((prev) => {
      const next = { ...prev };
      if (next[id] === action) delete next[id];
      else next[id] = action;
      return next;
    });
  }, []);

  const apply = useCallback(async () => {
    const list = Object.entries(decisions).map(([id, action]) => {
      const row = items.find((x) => String(x.id) === String(id));
      const fix = row?.suggestion || {};
      return {
        id: Number(id),
        action,
        ...(action === 'fix' && fix.to_word ? { to_word: fix.to_word } : {}),
        ...(action === 'fix' && fix.to_pos ? { to_pos: fix.to_pos } : {}),
        ...(action === 'fix' && fix.to_translation ? { to_translation: fix.to_translation } : {}),
      };
    });
    if (!list.length && !Object.keys(typed).length) return;
    setPhase('applying');
    haptic('ok');
    try {
      const filled = Object.entries(typed)
        .filter(([, text]) => String(text || '').trim())
        .map(([id, text]) => ({ id: Number(id), translation: text }));
      const data = await api(urls.apply, { decisions: list, translations: filled });
      setDone({
        fixed: Number(data?.fixed) || 0,
        kept: Number(data?.kept) || 0,
        deleted: Number(data?.deleted) || 0,
        left: Number(data?.left) || 0,
      });
      setDecisions({});
      setTyped({});
      await load(false);
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('ready');
    }
  }, [decisions, items, typed, translations, urls, load]);

  const marked = Object.keys(decisions).length;

  return (
    <div className="ans-root">
      <div className="ans-card wi-root">
        <div className="wi-head">
          <span className="badge">{copy.badge}</span>
          <span className="badge">{total}</span>
        </div>

        <div className="wi-title">{copy.title}</div>
        <div className="wi-sub">
          Отметьте решение у каждой. Применится всё разом — по одному действию на запись.
          Неотмеченное останется и придёт снова.
        </div>

        {done && (
          <div className="wi-done">
            Готово: исправлено <b>{done.fixed}</b>, оставлено <b>{done.kept}</b>,
            удалено <b>{done.deleted}</b>.
            {done.left > 0 ? ` Осталось разобрать: ${done.left}.` : ' Очередь пуста.'}
          </div>
        )}

        {phase === 'loading' && <div className="wi-empty">Смотрю словарь…</div>}
        {phase === 'error' && error && <div className="wi-error">{error}</div>}

        {phase !== 'loading' && !items.length && (
          <div className="wi-empty">{copy.empty}</div>
        )}

        {items.map((row) => {
          const picked = decisions[row.id] || '';
          const fix = row.suggestion;
          return (
            <div className={`wi-row${picked ? ` is-${picked}` : ''}`} key={row.id}>
              <div className="wi-word">{row.display || row.lemma || row.word}</div>
              {row.translation && <div className="wi-what">{row.translation}</div>}
              <div className="wi-what">{row.issue_text}</div>

              {fix && (fix.to_display || fix.to_lemma || fix.to_word) && (
                <div className="wi-fix">
                  <span className="from">{row.lemma || row.word}</span>
                  <span>→</span>
                  <span className="to">{fix.to_display || fix.to_lemma || fix.to_word}</span>
                </div>
              )}
              {fix?.to_translation && (
                <div className="wi-fix">
                  <span className="from">нет перевода</span>
                  <span>→</span>
                  <span className="to">{fix.to_translation}</span>
                </div>
              )}
              {row.issue === 'no_translation' && !fix?.to_translation && (
                <div className="wi-src">Перевод подберёт ночная работа — обычно к утру.</div>
              )}
              {fix && fix.to_pos && (
                <div className="wi-fix">
                  <span className="from">{POS_RU[row.pos] || row.pos || 'часть речи'}</span>
                  <span>→</span>
                  <span className="to">{POS_RU[fix.to_pos] || fix.to_pos}</span>
                </div>
              )}
              {fix?.why && <div className="wi-src">{fix.why}</div>}
              {!fix && (
                <div className="wi-src">
                  {row.issue === 'no_translation'
                    ? 'Перевод придумывать не станем — впишите его в словаре или удалите карточку.'
                    : 'Правку не предлагаем: справочник эту форму не подтвердил.'}
                </div>
              )}

              <div className="wi-acts">
                {(fix ? ['fix', 'keep', 'delete'] : ['keep', 'delete']).map((action) => (
                  <button
                    type="button"
                    key={action}
                    className={`wi-act ${action}${picked === action ? ' is-on' : ''}`}
                    onClick={() => choose(row.id, action)}
                  >
                    {ACTION_LABEL[action]}
                  </button>
                ))}
              </div>
            </div>
          );
        })}

        {scope === 'shared' && translations.length > 0 && (
          <div className="wi-block">
            <div className="wi-title">Не смогли перевести</div>
            <div className="wi-sub">
              Ночь перевода не нашла. Впишите свой — он встанет в карточку человека сразу.
            </div>
            {translations.map((row) => (
              <div className="wi-row" key={`tr-${row.id}`}>
                <div className="wi-word">{row.word}</div>
                <input
                  className="wi-input"
                  type="text"
                  placeholder="Перевод…"
                  value={typed[row.id] || ''}
                  onChange={(e) => setTyped((prev) => ({ ...prev, [row.id]: e.target.value }))}
                />
              </div>
            ))}
          </div>
        )}

        {(items.length > 0 || translations.length > 0) && (
          <>
            <button
              type="button"
              className={`wi-apply${marked ? '' : ' off'}`}
              onClick={apply}
              disabled={!marked || phase === 'applying'}
            >
              {phase === 'applying' ? 'Применяю…' : marked ? `Применить: ${marked} решени${marked === 1 ? 'е' : 'й'}` : 'Применить'}
            </button>
            <div className="wi-hint">
              {marked
                ? `Останется ${Math.max(0, total - marked)} — вернутся в следующий разбор`
                : 'Ничего не отмечено'}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
