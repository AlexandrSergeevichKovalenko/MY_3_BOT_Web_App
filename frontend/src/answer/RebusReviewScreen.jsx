import React, { useCallback, useEffect, useState } from 'react';

/**
 * Приёмка ребусов (админ). Единица решения — КАРТОЧКА, а не картинка.
 *
 * Половинку саму по себе оценить нельзя («годится ли эта ступня» — вопрос ни о чём),
 * а одно и то же слово входит в разные слова: одна пара складывается, другая нет.
 * Поэтому на экране всегда одна пара, и решение касается именно её.
 * Половинка без второй картинки сюда не попадает вовсе — судить нечего.
 */
const REASONS = [
  { key: 'wrong_object', label: 'не тот предмет' },
  { key: 'shows_sibling', label: 'видно вторую половину' },
  { key: 'ugly', label: 'некрасиво / непонятно' },
];

export default function RebusReviewScreen({ api, haptic, onClose }) {
  const [items, setItems] = useState(null);
  const [status, setStatus] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [note, setNote] = useState('');
  const [redrawFor, setRedrawFor] = useState('');   // слово, которое решили перерисовать
  const [comment, setComment] = useState('');      // своя правка вместо кнопки-причины

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

  const card = items && items[0];
  useEffect(() => { setRedrawFor(''); setComment(''); }, [card?.compound_id]);

  const send = async (verdict, extra = {}) => {
    if (!card || busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/rebusreview/verdict', {
        compound_id: card.compound_id, verdict, ...extra,
      });
      if (r.status === 'approved') {
        setNote(r.composed
          ? `✅ «${card.compound}» в банке. Карточек собрано: ${r.composed} — уже в очереди на отправку.`
          : `✅ «${card.compound}» принято.`);
      } else if (r.status === 'redraw') {
        setNote(`🔄 «${extra.word}» перерисую с учётом причины. Попыток осталось: ${r.redraws_left}.`);
      } else if (r.status === 'blocked') {
        setNote(`🗑 «${extra.word}» больше не рисуем. Заданий убрано: ${r.compounds_touched}.`);
      } else if (r.status === 'pair_dropped') {
        setNote(`🗑 «${card.compound}» снято. Картинки остались — они работают в других словах.`);
      }
      if (r.status_pool) setStatus(r.status_pool);
      haptic?.(verdict === 'approve' ? 'ok' : 'bad');
      setItems((prev) => (prev || []).filter((c) => c.compound_id !== card.compound_id));
      setRedrawFor(''); setComment('');
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось сохранить решение. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const statusLine = status
    ? `В банке готовых карточек: ${status.ready} · ждут дорисовки: ${status.waiting_draw}`
    : '';

  if (items === null) return <div className="pinw"><div className="ans-loading">Загружаю приёмку…</div></div>;

  if (!card) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">🧩 Приёмка ребусов</div>
          <div className="pinw-sub">{statusLine || 'Всё разобрано'}</div>
        </div>
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">
            Готовых пар на приёмку нет. Появятся, когда бот дорисует недостающие
            половинки, — я напишу тебе в личку.
          </div>
        </div>
        <div className="pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={refresh}>🔄 Проверить ещё раз</button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const [left, right] = card.halves;
  const target = card.halves.find((h) => h.word === redrawFor);

  return (
    <div className="pinw">
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">🧩 Приёмка</span>
        <span className="pinw-count">осталось {items.length}</span>
      </div>

      <div className="rbrev-scroll">
        <div className="rbrev-pair">
          {[left, right].map((h, i) => (
            <React.Fragment key={h.word}>
              {i === 1 ? <span className="rbrev-op">+</span> : null}
              <figure className={`rbrev-half ${h.is_new ? 'rbrev-half-new' : ''} ${redrawFor === h.word ? 'rbrev-half-picked' : ''}`}>
                <img src={h.image_url} alt="" draggable="false" />
                <figcaption>{h.word}{h.meaning_ru ? ` — ${h.meaning_ru}` : ''}</figcaption>
              </figure>
            </React.Fragment>
          ))}
        </div>
        <div className="rbrev-answer-line">
          = <b>{card.compound}</b>{card.compound_ru ? <i> — {card.compound_ru}</i> : null}
        </div>

        {redrawFor ? (
          <div className="rbrev-redraw">
            <div className="rbrev-redraw-title">
              Перерисую только «{redrawFor}» — вторая картинка останется как есть, она уже
              оплачена.
              {target && target.used_in_cards > 1
                ? ` Эта половинка стоит ещё в ${target.used_in_cards - 1} задании(ях) — они тоже подождут новую.`
                : ''}
            </div>
            <div className="rbrev-rejects">
              {REASONS.map((r) => (
                <button key={r.key} className="ans-btn-ghost rbrev-reject" disabled={busy}
                  onClick={() => send('redraw', { word: redrawFor, reason: r.key })}>{r.label}</button>
              ))}
            </div>
            {/* Своими словами — точнее любой кнопки: текст уходит прямо в задание на
                отрисовку, поэтому «не хватает хвоста» работает лучше, чем «некрасиво». */}
            <input
              className="pinrev-word" value={comment} onChange={(e) => setComment(e.target.value)}
              placeholder="или своими словами: что поправить"
              enterKeyHint="send"
              onKeyDown={(e) => {
                if (e.key === 'Enter' && comment.trim()) {
                  e.target.blur();
                  send('redraw', { word: redrawFor, reason: comment.trim() });
                }
              }}
            />
            <button className="ans-btn" disabled={busy || !comment.trim()}
              onClick={() => send('redraw', { word: redrawFor, reason: comment.trim() })}>
              🔁 Перерисовать по моему комментарию
            </button>
            <button className="pinrev-link" onClick={() => { setRedrawFor(''); setComment(''); }}>← передумал</button>
          </div>
        ) : null}

        {statusLine ? <div className="rbrev-where">{statusLine}</div> : null}
        {note ? <div className="pinrev-note">{note}</div> : null}
        {error ? <div className="pinrev-err">{error}</div> : null}
      </div>

      <div className="pinw-bar">
        <button className="ans-btn pinw-next" disabled={busy} onClick={() => send('approve')}>
          ✅ Пара складывается — в банк
        </button>
        <div className="rbrev-rejects">
          <button className="ans-btn-ghost rbrev-reject" disabled={busy}
            onClick={() => setRedrawFor(left.word)}>🔁 Перерисовать «{left.word}»</button>
          <button className="ans-btn-ghost rbrev-reject" disabled={busy}
            onClick={() => setRedrawFor(right.word)}>🔁 Перерисовать «{right.word}»</button>
        </div>
        <button className="ans-btn-ghost pinrev-skip" disabled={busy} onClick={() => send('drop_pair')}>
          🗑 Пара не работает — снять это задание
        </button>
        <div className="rbrev-hint">
          «Снять задание» убирает только это слово. Картинки останутся — они могут
          работать в других словах.
        </div>
        <button className="pinw-close" onClick={onClose}>Закрыть</button>
      </div>
    </div>
  );
}
