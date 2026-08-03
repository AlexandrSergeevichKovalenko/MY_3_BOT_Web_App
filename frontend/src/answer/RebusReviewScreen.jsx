import React, { useCallback, useEffect, useState } from 'react';

/**
 * Приёмка половинок ребуса (админ). Одна картинка на экран, без прокрутки.
 *
 * Единица приёмки — СЛОВО, а не готовая карточка: слово рисуется один раз и идёт
 * во все композиты, где встречается, поэтому одно решение закрывает их все.
 * «Годится» — карточки, ждавшие эту половинку, собираются сразу.
 * Отказ с причиной — причина уходит в промпт следующей отрисовки, а не в пустоту.
 */
const REJECTS = [
  { key: 'wrong_object', label: '🚫 Не тот предмет' },
  { key: 'shows_sibling', label: '🚫 Видно вторую половину' },
  { key: 'ugly', label: '🚫 Некрасиво / непонятно' },
];

export default function RebusReviewScreen({ api, haptic, onClose }) {
  const [items, setItems] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');

  const refresh = useCallback(async () => {
    try {
      const r = await api('/api/answer/rebusreview/list', {});
      setItems(r.items || []);
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось загрузить. Попробуйте позже.');
      setItems([]);
    }
  }, [api]);

  useEffect(() => { refresh(); }, [refresh]);

  const item = items && items[0];

  const send = async (verdict, reason) => {
    if (!item || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/rebusreview/verdict', { word: item.word, verdict, reason: reason || '' });
      if (r.status === 'approved') {
        const built = r.cards?.composed || 0;
        setNote(built ? `✅ «${item.word}» принято — собрано карточек: ${built}.` : `✅ «${item.word}» принято.`);
      } else if (r.status === 'redraw') {
        setNote(`🔄 «${item.word}» перерисую с учётом причины. Попыток осталось: ${r.redraws_left}.`);
      } else if (r.status === 'blocked') {
        setNote(`🗑 «${item.word}» больше не рисуем. Заданий убрано: ${r.compounds_touched}.`);
      }
      haptic?.(verdict === 'approve' ? 'ok' : 'bad');
      setItems((prev) => (prev || []).slice(1));
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось сохранить решение. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  if (items === null) return <div className="pinw"><div className="ans-loading">Загружаю приёмку…</div></div>;

  if (!item) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">🧩 Приёмка ребусов</div>
          <div className="pinw-sub">Всё разобрано</div>
        </div>
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">Новых картинок на приёмку нет. Как только бот нарисует следующие половинки, они появятся здесь.</div>
        </div>
        <div className="pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={refresh}>🔄 Проверить ещё раз</button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const noRedrawsLeft = item.redraws_left <= 0;

  return (
    <div className="pinw">
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">🧩 Приёмка</span>
        <span className="pinw-count">осталось {items.length}</span>
      </div>

      <div className="pinw-imgarea">
        <div className="pinw-imgwrap">
          <img className="pinw-img" src={item.image_url} alt="" draggable="false" />
        </div>
      </div>

      <div className="pinw-controls">
        <div className="rbrev-word">
          {item.word}
          {item.meaning_ru ? <span className="rbrev-ru"> — {item.meaning_ru}</span> : null}
        </div>
        <div className="rbrev-where">
          {item.compounds?.length
            ? `Пойдёт в слова: ${item.compounds.join(', ')}`
            : 'Пока не используется ни в одном задании'}
        </div>
        {item.last_reason ? <div className="rbrev-again">Перерисовано после замечания: «{item.last_reason}»</div> : null}
        {note ? <div className="pinrev-note">{note}</div> : null}
        {error ? <div className="pinrev-err">{error}</div> : null}
      </div>

      <div className="pinw-bar">
        <button className="ans-btn pinw-next" disabled={busy} onClick={() => send('approve')}>
          ✅ Годится — в банк
        </button>
        <div className="rbrev-rejects">
          {REJECTS.map((r) => (
            <button key={r.key} className="ans-btn-ghost rbrev-reject" disabled={busy}
              onClick={() => send('reject', r.key)}>{r.label}</button>
          ))}
        </div>
        <div className="rbrev-hint">
          {noRedrawsLeft
            ? 'Перерисовок больше нет: любой отказ уберёт слово из игры.'
            : `После отказа перерисую с учётом причины (осталось попыток: ${item.redraws_left}).`}
        </div>
        <button className="ans-btn-ghost pinrev-skip" disabled={busy} onClick={() => send('block')}>
          🗑 Слово не для ребуса — убрать совсем
        </button>
        <button className="pinw-close" onClick={onClose}>Закрыть</button>
      </div>
    </div>
  );
}
