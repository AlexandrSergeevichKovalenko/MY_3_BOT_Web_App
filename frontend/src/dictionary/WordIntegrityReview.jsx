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

export default function WordIntegrityReview() {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [decisions, setDecisions] = useState({});
  const [phase, setPhase] = useState('loading');
  const [error, setError] = useState('');
  const [done, setDone] = useState(null);

  const load = useCallback(async (rescan = false) => {
    setPhase('loading');
    setError('');
    try {
      const data = await api('/api/webapp/dictionary/integrity/list', { rescan });
      setItems(Array.isArray(data?.items) ? data.items : []);
      setTotal(Number(data?.total) || 0);
      setPhase('ready');
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('error');
    }
  }, []);

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
    const list = Object.entries(decisions).map(([id, action]) => ({ id: Number(id), action }));
    if (!list.length) return;
    setPhase('applying');
    haptic('ok');
    try {
      const data = await api('/api/webapp/dictionary/integrity/apply', { decisions: list });
      setDone({
        fixed: Number(data?.fixed) || 0,
        kept: Number(data?.kept) || 0,
        deleted: Number(data?.deleted) || 0,
        left: Number(data?.left) || 0,
      });
      setDecisions({});
      await load(false);
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('ready');
    }
  }, [decisions, load]);

  const marked = Object.keys(decisions).length;

  return (
    <div className="ans-root">
      <div className="ans-card wi-root">
        <div className="wi-head">
          <span className="badge">🧹 Разбор словаря</span>
          <span className="badge">{total}</span>
        </div>

        <div className="wi-title">Записи, которые противоречат себе</div>
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
          <div className="wi-empty">Противоречивых записей нет — словарь чист.</div>
        )}

        {items.map((row) => {
          const picked = decisions[row.id] || '';
          const fix = row.suggestion;
          return (
            <div className={`wi-row${picked ? ` is-${picked}` : ''}`} key={row.id}>
              <div className="wi-word">{row.display || row.lemma}</div>
              <div className="wi-what">{row.issue_text}</div>

              {fix && (
                <div className="wi-fix">
                  <span className="from">{row.lemma}</span>
                  <span>→</span>
                  <span className="to">{fix.to_display || fix.to_lemma}</span>
                </div>
              )}
              {fix?.why && <div className="wi-src">{fix.why}</div>}
              {!fix && (
                <div className="wi-src">
                  Правку не предлагаем: справочник эту форму не подтвердил.
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

        {items.length > 0 && (
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
