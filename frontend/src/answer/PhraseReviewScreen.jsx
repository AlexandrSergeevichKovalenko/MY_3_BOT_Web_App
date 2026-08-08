import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

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
  // Подтверждение прошлого решения. Оно ВСЕГДА про предыдущую фразу, поэтому несёт её
  // текст и само гаснет: пока оно висело просто «✅ вопрос закрыт» над уже следующей
  // фразой, его нельзя было не прочитать как решение по ней.
  const [done, setDone] = useState(null);   // { text, what }
  const doneTimer = useRef(null);
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

  const flashDone = useCallback((phrase, what) => {
    if (doneTimer.current) clearTimeout(doneTimer.current);
    setDone({ text: phrase, what });
    doneTimer.current = setTimeout(() => setDone(null), 5000);
  }, []);

  useEffect(() => () => { if (doneTimer.current) clearTimeout(doneTimer.current); }, []);

  const applyResponse = (r) => {
    setItems(r.items || []);
    setNote('');
    setOwn('');
    // Список сдвинулся на одну — остаёмся на той же позиции, то есть на следующей
    // фразе. Прыжок в начало заставлял бы каждый раз искать, где ты был.
    setIdx((prev) => Math.max(0, Math.min(prev, (r.items || []).length - 1)));
  };

  const decide = async (decision, extra = {}) => {
    if (!card || busy) return;
    setBusy(true); setError('');
    try {
      const was = card.text;
      const r = await api('/api/answer/phrasereview/decide',
        { review_id: card.id, decision, ...extra });
      haptic?.('ok');
      applyResponse(r);
      flashDone(was, r.note || 'Готово.');
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

  const settle = async () => {
    if (!card || busy) return;
    setBusy(true); setError(''); setNote('⚖️ Спрашиваю третейского судью…');
    try {
      const r = await api('/api/answer/phrasereview/settle', { review_id: card.id });
      applyResponse(r);
    } catch (e) {
      console.warn('[phrasereview] settle', e);
      setNote('');
      setError(e?.message || 'Третейский судья не ответил. Попробуйте ещё раз.');
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

  const arbiter = card.arbiter || null;

  return (
    <div className={`pinw${typing ? ' typing' : ''}`}>
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">📝 Спорные фразы</span>
        <span className="pinw-count">{position} из {queue.length}</span>
      </div>

      {done ? (
        <div className="frrev-done">
          ✅ «{done.text}» — {done.what.replace(/^Фраза /, '').toLowerCase()}
        </div>
      ) : null}

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

        {arbiter ? (
          <div className="frrev-arbiter">
            <div className="frrev-arbiter-head">
              ⚖️ Третейский судья
              {arbiter.winner_index != null ? ` · прав вариант ${arbiter.winner_index + 1}` : ''}
              {arbiter.winner_index == null && arbiter.better ? ' · оба мимо' : ''}
            </div>
            {arbiter.why ? <div className="frrev-arbiter-why">{arbiter.why}</div> : null}
          </div>
        ) : null}

        {!variants.length && card.all_ok ? (
          <div className="frrev-allok">
            Оба судьи говорят: ошибки нет. Править нечего — оставь фразу как есть,
            и она больше не вернётся в этот разбор.
          </div>
        ) : null}
        {!variants.length && !card.all_ok ? (
          <div className="frrev-nofix">
            Готового варианта судьи не дали. Спроси заново — промпт с 08.08.2026 требует
            показывать исправленный текст, а эта фраза судилась раньше.
          </div>
        ) : null}

        {note ? <div className="pinrev-note">{note}</div> : null}
        {error ? <div className="pinrev-err">{error}</div> : null}
      </div>

      <div className="pinw-bar">
        {variants.map((v) => {
          const picked = arbiter && arbiter.winner_index === v.index;
          return (
            <button key={v.index}
              className={`ans-btn frrev-accept${picked ? ' frrev-accept-picked' : ''}`}
              disabled={busy} onClick={() => decide('accept', { variant: v.index })}>
              <span className="frrev-accept-no">
                ✅ Принять {v.index + 1}{picked ? ' · рекомендую' : ''}
              </span>
              <span className="frrev-accept-text">{v.text}</span>
              <span className="frrev-accept-kind">
                {v.kind === 'arbiter'
                  ? 'третейский судья · свой вариант'
                  : `судья ${v.judge} · ${KIND_LABEL[v.kind]}`}
              </span>
            </button>
          );
        })}

        {/* Спор двух судей разрешает третий. Владелец не обязан знать, пишется ли
            «hochbekommen» слитно, — две кнопки без объяснения это та же загадка. */}
        {variants.length > 1 && !arbiter ? (
          <button className="ans-btn-ghost" disabled={busy} onClick={settle}>
            ⚖️ Судьи разошлись — кто прав?
          </button>
        ) : null}

        {/* «Оставить как есть» есть всегда: даже когда варианты предложены, владелец
            вправе не согласиться с судьями. Когда оба сказали «ошибки нет», принимать
            нечего — и тогда это единственное осмысленное решение, поэтому оно главное. */}
        <button className={variants.length ? 'ans-btn-ghost' : 'ans-btn frrev-keep'}
          disabled={busy} onClick={() => decide('keep')}>
          👍 Фраза хорошая — оставить как есть
        </button>

        {!variants.length && !card.all_ok ? (
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
          «Оставить как есть» закрывает вопрос: фраза не меняется и больше не вернётся.
          «Удалить» убирает её из общего словаря и подписные карточки людей — кроме тех,
          куда человек вписал что-то своё. «Отложить» ничего не меняет.
        </div>
        <button className="pinw-close" onClick={onClose}>Закрыть</button>
      </div>
    </div>
  );
}
