import React, { useCallback, useEffect, useMemo, useState } from 'react';

/**
 * Спорные фразы общего словаря (админ). Одна фраза на экран.
 *
 * Почему экран, а не чат. Ночная проверка откладывает фразы, по которым двое судей не
 * сошлись, и раньше разбор шёл сообщениями бота. В чате у владельца сотни непрочитанных
 * учебных сообщений, и каждый ответ бота приходил новым сообщением в самый конец
 * переписки — то есть уносил его от того места, где он читал. Здесь весь разбор в одном
 * месте: список, варианты судей, решение.
 *
 * Почему у каждого варианта своя кнопка. Судьи расходятся постоянно — ровно поэтому
 * фраза сюда и попала. Одна кнопка «Принять» брала бы вариант первого судьи молча, и по
 * ней нельзя понять, что принимаешь. Номер на кнопке — тот же, что у варианта в разборе.
 */
const KIND_LABEL = {
  fix: 'правка',
  complete: 'дописано',
};

export default function PhraseReviewScreen({ api, haptic, onClose }) {
  const [items, setItems] = useState(null);
  const [idx, setIdx] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');
  const [own, setOwn] = useState('');
  const [typing, setTyping] = useState(false);
  // Отложенные на этом сеансе: в базе ничего не меняется, они просто уходят в конец
  // очереди, чтобы не крутиться перед глазами, пока разбираешь остальные.
  const [skipped, setSkipped] = useState(() => new Set());

  const load = useCallback(async (loud = false) => {
    if (loud) { setBusy(true); setError(''); }
    try {
      const r = await api('/api/answer/phrasereview/list', {});
      setItems(r.items || []);
      if (loud) setNote(`🔄 Проверил: спорных фраз — ${(r.items || []).length}.`);
    } catch (e) {
      console.warn('[phrasereview] load', e);
      setError(e?.status === 403
        ? 'Этот экран только для администратора.'
        : 'Не удалось загрузить. Попробуйте позже.');
      setItems([]);
    } finally {
      if (loud) setBusy(false);
    }
  }, [api]);

  useEffect(() => { load(false); }, [load]);

  // Отложенные — в конец, порядок остальных не трогаем.
  const queue = useMemo(() => {
    const list = items || [];
    return [...list.filter((it) => !skipped.has(it.id)), ...list.filter((it) => skipped.has(it.id))];
  }, [items, skipped]);

  const card = queue[Math.min(idx, Math.max(0, queue.length - 1))] || null;

  const applyResponse = (r) => {
    setItems(r.items || []);
    setNote(r.note ? `✅ ${r.note}` : '');
    setOwn('');
    // Список сдвинулся на одну — остаёмся на той же позиции, то есть на следующей
    // фразе. Прыжок в начало заставлял бы каждый раз искать, где ты был.
    setIdx((prev) => Math.max(0, Math.min(prev, (r.items || []).length - 1)));
  };

  const decide = async (decision, extra = {}) => {
    if (!card || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/phrasereview/decide',
        { review_id: card.id, decision, ...extra });
      haptic?.('ok');
      applyResponse(r);
    } catch (e) {
      console.warn('[phrasereview] decide', e);
      haptic?.('bad');
      setError(e?.message || 'Не получилось. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const rejudge = async () => {
    if (!card || busy) return;
    setBusy(true); setError(''); setNote('🔁 Спрашиваю судей заново…');
    try {
      const r = await api('/api/answer/phrasereview/rejudge', { review_id: card.id });
      applyResponse(r);
    } catch (e) {
      console.warn('[phrasereview] rejudge', e);
      setNote('');
      setError(e?.message || 'Судьи не ответили. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const skip = () => {
    if (!card) return;
    setSkipped((prev) => new Set(prev).add(card.id));
    setNote('');
    setError('');
  };

  if (items === null) {
    return <div className="pinw"><div className="ans-loading">Загружаю спорные фразы…</div></div>;
  }

  if (!card) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">📝 Спорные фразы</div>
          <div className="pinw-sub">Всё разобрано</div>
        </div>
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">
            Спорных фраз нет. Ночная проверка складывает сюда только то, в чём два судьи
            не сошлись, — если здесь пусто, значит за прошлую ночь спорить было не о чем.
          </div>
          {error ? <div className="pinrev-err">{error}</div> : null}
        </div>
        <div className="pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={() => load(true)}>
            🔄 Проверить ещё раз
          </button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const variants = card.variants || [];
  const position = Math.min(idx, queue.length - 1) + 1;

  return (
    <div className={`pinw${typing ? ' typing' : ''}`}>
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">📝 Спорные фразы</span>
        <span className="pinw-count">{position} из {queue.length}</span>
      </div>

      <div className="frrev-scroll">
        <div className="frrev-phrase">{card.text}</div>
        {card.translation ? <div className="frrev-translation">{card.translation}</div> : null}

        <div className="frrev-judges">
          {(card.judges || []).map((j) => (
            <div className="frrev-judge" key={j.no}>
              <div className="frrev-judge-head">
                Судья {j.no}
                {j.verdict === 'error' && j.category ? ` · ${j.category}` : ''}
                {j.verdict === 'context' ? ' · зависит от контекста' : ''}
                {j.verdict === 'style' ? ' · вопрос вкуса' : ''}
                {j.verdict === 'ok' ? ' · ошибки нет' : ''}
              </div>
              {j.why ? <div className="frrev-judge-why">{j.why}</div> : null}
              {j.corrected ? (
                <div className="frrev-judge-fix">
                  {j.corrected_slot != null ? <b>Вариант {j.corrected_slot + 1}</b> : null}
                  <code>{j.corrected}</code>
                </div>
              ) : null}
              {j.proposal ? (
                <div className="frrev-judge-fix">
                  {j.proposal_slot != null ? <b>Вариант {j.proposal_slot + 1}</b> : null}
                  <code>{j.proposal}</code>
                  <em>дописано недостающее</em>
                </div>
              ) : null}
            </div>
          ))}
        </div>

        {!variants.length ? (
          <div className="frrev-nofix">
            Готового варианта судьи не дали. Спроси заново — промпт с 08.08.2026 требует
            показывать исправленный текст, а эта фраза судилась раньше.
          </div>
        ) : null}

        {note ? <div className="pinrev-note">{note}</div> : null}
        {error ? <div className="pinrev-err">{error}</div> : null}
      </div>

      <div className="pinw-bar">
        {variants.map((v) => (
          <button key={v.index} className="ans-btn frrev-accept" disabled={busy}
            onClick={() => decide('accept', { variant: v.index })}>
            <span className="frrev-accept-no">✅ Принять {v.index + 1}</span>
            <span className="frrev-accept-text">{v.text}</span>
            <span className="frrev-accept-kind">судья {v.judge} · {KIND_LABEL[v.kind]}</span>
          </button>
        ))}

        {!variants.length ? (
          <button className="ans-btn-ghost" disabled={busy} onClick={rejudge}>
            🔁 Спросить судей заново
          </button>
        ) : null}

        <input
          className="pinrev-word" value={own} disabled={busy}
          onChange={(e) => setOwn(e.target.value)}
          onFocus={() => setTyping(true)}
          onBlur={() => setTyping(false)}
          placeholder="или впиши свой вариант"
          enterKeyHint="send"
          onKeyDown={(e) => {
            if (e.key === 'Enter' && own.trim()) {
              e.target.blur();
              decide('replace', { text: own.trim() });
            }
          }}
        />
        {own.trim() ? (
          <button className="ans-btn" disabled={busy}
            onClick={() => decide('replace', { text: own.trim() })}>
            ✏️ Записать мой вариант
          </button>
        ) : null}

        <div className="pinw-bar-row pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={skip}>↷ Отложить</button>
          <button className="ans-btn-ghost pinrev-skip" disabled={busy}
            onClick={() => decide('delete')}>🗑 Удалить</button>
        </div>
        <div className="frrev-hint">
          «Удалить» убирает фразу из общего словаря и подписные карточки людей — кроме
          тех, куда человек вписал что-то своё. «Отложить» ничего не меняет.
        </div>
        <button className="pinw-close" onClick={onClose}>Закрыть</button>
      </div>
    </div>
  );
}
