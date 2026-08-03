import React, { useCallback, useEffect, useState } from 'react';

/**
 * Приёмка половинок ребуса (админ).
 *
 * Показываем не картинку саму по себе, а СЛОЖЕНИЕ: [новая] + [вторая] = слово.
 * Отдельно висящая картинка ничего не решает — вопрос всегда «складываются ли
 * эти две в нужное слово». Новая половинка подсвечена, решение касается её:
 * она переиспользуется всеми словами с этой частью, поэтому второй раз в составе
 * другого слова её не показываем.
 */
const REJECTS = [
  { key: 'wrong_object', label: '🚫 Не тот предмет' },
  { key: 'shows_sibling', label: '🚫 Видно вторую половину' },
  { key: 'ugly', label: '🚫 Некрасиво / непонятно' },
];

export default function RebusReviewScreen({ api, haptic, onClose }) {
  const [items, setItems] = useState(null);
  const [status, setStatus] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');

  const refresh = useCallback(async () => {
    try {
      const r = await api('/api/answer/rebusreview/list', {});
      setItems(r.items || []);
      setStatus(r.status || null);
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
        setNote(built
          ? `✅ «${item.word}» принято. Карточек ушло в банк: ${built} — они уже в очереди на отправку.`
          : `✅ «${item.word}» принято. Карточка соберётся, как только примешь вторую половину.`);
      } else if (r.status === 'redraw') {
        setNote(`🔄 «${item.word}» перерисую с учётом причины. Попыток осталось: ${r.redraws_left}.`);
      } else if (r.status === 'blocked') {
        setNote(`🗑 «${item.word}» больше не рисуем. Заданий убрано: ${r.compounds_touched}.`);
      }
      haptic?.(verdict === 'approve' ? 'ok' : 'bad');
      setItems((prev) => (prev || []).slice(1));
      if (r.cards?.composed || r.status !== 'approved') refresh();
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось сохранить решение. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const statusLine = status
    ? `В банке готовых карточек: ${status.ready} · ждут дорисовки: ${status.waiting_draw}`
    : '';

  if (items === null) return <div className="pinw"><div className="ans-loading">Загружаю приёмку…</div></div>;

  if (!item) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">🧩 Приёмка ребусов</div>
          <div className="pinw-sub">{statusLine || 'Всё разобрано'}</div>
        </div>
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">
            Новых картинок на приёмку нет. Как только пул начнёт дорисовывать — они появятся
            здесь, и я напишу тебе в личку.
          </div>
        </div>
        <div className="pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={refresh}>🔄 Проверить ещё раз</button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const noRedrawsLeft = item.redraws_left <= 0;
  const pairs = item.pairs || [];

  return (
    <div className="pinw">
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">🧩 Приёмка</span>
        <span className="pinw-count">осталось {items.length}</span>
      </div>

      <div className="rbrev-scroll">
        <div className="rbrev-word">
          Новая картинка: {item.word}
          {item.meaning_ru ? <span className="rbrev-ru"> — {item.meaning_ru}</span> : null}
        </div>

        {pairs.length ? pairs.map((p) => (
          <div className="rbrev-pair" key={p.compound}>
            <figure className="rbrev-half rbrev-half-new">
              <img src={item.image_url} alt="" draggable="false" />
              <figcaption>{item.word}</figcaption>
            </figure>
            <span className="rbrev-op">+</span>
            <figure className="rbrev-half">
              {p.sibling_image_url
                ? <img src={p.sibling_image_url} alt="" draggable="false" />
                : <span className="rbrev-half-empty">?</span>}
              <figcaption>{p.sibling}</figcaption>
            </figure>
            <span className="rbrev-op">=</span>
            <div className="rbrev-answer">
              <b>{p.compound}</b>
              {p.compound_ru ? <i>{p.compound_ru}</i> : null}
              {p.sibling_image_url ? null : <em>вторая половина ещё не нарисована</em>}
            </div>
          </div>
        )) : (
          <>
            <div className="rbrev-pair">
              <figure className="rbrev-half rbrev-half-new">
                <img src={item.image_url} alt="" draggable="false" />
                <figcaption>{item.word}</figcaption>
              </figure>
            </div>
            <div className="rbrev-where">Пока не используется ни в одном задании</div>
          </>
        )}

        {item.last_reason ? <div className="rbrev-again">Перерисовано после замечания: «{item.last_reason}»</div> : null}
        {statusLine ? <div className="rbrev-where">{statusLine}</div> : null}
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
