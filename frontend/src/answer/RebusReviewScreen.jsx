import React, { useCallback, useEffect, useState } from 'react';
import { useTypingChrome } from './useTypingChrome.js';

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
  const [tab, setTab] = useState('queue');          // queue | ready | waiting
  const [cards, setCards] = useState(null);         // каталог для вкладок ready/waiting
  const [redrawFor, setRedrawFor] = useState('');   // слово, которое решили перерисовать
  const [comment, setComment] = useState('');      // своя правка вместо кнопки-причины
  // Тот же каркас `.pinw`, что и у «Спорных фраз»: пока курсор в поле, нижняя панель
  // отдаёт свою высоту (замер и правило — в `useTypingChrome.js` / `answer.css`).
  const { typing, onFocus: onTypeFocus, onBlur: onTypeBlur } = useTypingChrome();

  // loud=true — нажали кнопку руками: молчаливая кнопка выглядит сломанной, поэтому
  // всегда отвечаем словами, даже когда ничего не изменилось.
  const refresh = useCallback(async (loud = false) => {
    if (loud) { setBusy(true); setError(''); }
    try {
      const r = await api('/api/answer/rebusreview/list', {});
      const list = r.items || [];
      setItems(list);
      setStatus(r.status || null);
      if (loud) {
        setNote(list.length
          ? `🔄 Проверил: пар на приёмке — ${list.length}.`
          : '🔄 Проверил: новых пар пока нет.');
      }
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось загрузить. Попробуйте позже.');
      setItems([]);
    } finally {
      if (loud) setBusy(false);
    }
  }, [api]);

  const loadCards = useCallback(async (mode) => {
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/rebusreview/browse', { mode });
      setCards(r.items || []);
      setStatus(r.status || null);
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось загрузить каталог.');
      setCards([]);
    } finally { setBusy(false); }
  }, [api]);

  const openTab = (next) => {
    setTab(next); setNote(''); setError('');
    if (next === 'queue') { setCards(null); refresh(false); } else { setCards(null); loadCards(next); }
  };

  // Действие над карточкой ИЗ КАТАЛОГА: снять или перерисовать половинку.
  const actOnCard = async (item, verdict, extra = {}) => {
    if (busy) return;
    setBusy(true); setError('');
    try {
      const r = await api('/api/answer/rebusreview/verdict', {
        compound_id: item.compound_id, verdict, ...extra,
      });
      if (r.status === 'pair_dropped') setNote(`🗑 «${item.compound}» снято. Картинки остались.`);
      else if (r.status === 'redraw') {
        setNote(r.redraw_started
          ? `🔄 Перерисовываю «${extra.word}» — вернётся на приёмку через пару минут.`
          : `🔄 «${extra.word}» помечено, но запустить перерисовку сейчас не вышло.`);
      } else if (r.status === 'blocked') setNote(`🗑 «${extra.word}» больше не рисуем.`);
      if (r.status_pool) setStatus(r.status_pool);
      setCards((prev) => (prev || []).filter((c) => c.compound_id !== item.compound_id));
      haptic?.('bad');
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось сохранить решение.');
    } finally { setBusy(false); }
  };

  const drawCard = async (item) => {
    if (busy) return;
    setBusy(true); setError('');
    try {
      await api('/api/answer/rebusreview/draw', { cards: 1, compound_id: item.compound_id });
      setNote(`🖼 Дорисовываю «${item.compound}» — придёт на приёмку через пару минут.`);
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось начать отрисовку.');
    } finally { setBusy(false); }
  };

  const draw = async (cards) => {
    if (busy) return;
    setBusy(true); setError(''); setNote('');
    try {
      const r = await api('/api/answer/rebusreview/draw', { cards });
      setNote(`🖼 Рисую ${r.cards} карточк(и) — это пара минут. Как будут готовы, я напишу `
        + 'в личку, а здесь поможет «Проверить ещё раз».');
    } catch (e) {
      console.warn('[game] error', e);
      setError('Не удалось начать рисование. Попробуйте позже.');
    } finally { setBusy(false); }
  };

  useEffect(() => { refresh(false); }, [refresh]);

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
        setNote(r.redraw_started
          ? `🔄 Перерисовываю «${extra.word}» с учётом причины — пара вернётся сюда через `
            + `пару минут, вторая картинка останется прежней. Попыток осталось: ${r.redraws_left}.`
          : `🔄 «${extra.word}» помечено на перерисовку, но запустить её сейчас не вышло — `
            + 'нарисуется при следующем прогоне.');
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

  const tabsRow = (
    <div className="rbrev-tabs">
      <button className={`rbrev-tab ${tab === 'queue' ? 'on' : ''}`} onClick={() => openTab('queue')}>На приёмке</button>
      <button className={`rbrev-tab ${tab === 'ready' ? 'on' : ''}`} onClick={() => openTab('ready')}>В банке</button>
      <button className={`rbrev-tab ${tab === 'waiting' ? 'on' : ''}`} onClick={() => openTab('waiting')}>Незаконченные</button>
    </div>
  );

  // ── Каталог: пересмотреть однажды одобренное и проредить ──
  if (tab !== 'queue') {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">🧩 Ребусы</div>
          <div className="pinw-sub">{statusLine}</div>
        </div>
        {tabsRow}
        <div className="rbrev-scroll">
          {note ? <div className="pinrev-note">{note}</div> : null}
          {error ? <div className="pinrev-err">{error}</div> : null}
          {cards === null ? <div className="ans-loading">Загружаю…</div> : null}
          {cards && !cards.length ? (
            <div className="pinrev-empty-sub">
              {tab === 'ready' ? 'Собранных карточек нет.' : 'Незаконченных карточек нет.'}
            </div>
          ) : null}
          {(cards || []).map((c) => (
            <div className="rbrev-row" key={c.compound_id}>
              <div className="rbrev-row-top">
                {c.halves.map((h) => (
                  <figure className="rbrev-mini" key={h.word}>
                    {h.image_url
                      ? <img src={h.image_url} alt="" draggable="false" />
                      : <span className="rbrev-mini-empty">?</span>}
                    <figcaption>{h.word}</figcaption>
                  </figure>
                ))}
                <div className="rbrev-row-name">
                  <b>{c.compound}</b>
                  {c.compound_ru ? <i>{c.compound_ru}</i> : null}
                  {tab === 'ready' ? <em>показана {c.sent_times} раз(а)</em> : null}
                </div>
              </div>
              <div className="rbrev-row-actions">
                {/* Картинка общая для всех карточек со словом: сколько заденет — на кнопке. */}
                {c.halves.filter((h) => h.drawn).map((h) => (
                  <button key={h.word} className="ans-btn-ghost rbrev-reject" disabled={busy}
                    onClick={() => {
                      const extra = h.used_in_cards > 1
                        ? `\n\nЭта картинка стоит в ${h.used_in_cards} карточках — все они уйдут ждать новую.`
                        : '';
                      // eslint-disable-next-line no-alert
                      if (window.confirm(`Перерисовать «${h.word}»?${extra}`)) {
                        actOnCard(c, 'redraw', { word: h.word, reason: 'wrong_object' });
                      }
                    }}>
                    🔁 {h.word}{h.used_in_cards > 1 ? ` (в ${h.used_in_cards})` : ''}
                  </button>
                ))}
                {tab === 'waiting' && c.halves.some((h) => !h.drawn) ? (
                  <button className="ans-btn-ghost" disabled={busy} onClick={() => drawCard(c)}>🖼 Дорисовать</button>
                ) : null}
                <button className="ans-btn-ghost pinrev-skip" disabled={busy}
                  onClick={() => actOnCard(c, 'drop_pair')}>🗑 Снять</button>
              </div>
            </div>
          ))}
        </div>
        <div className="pinw-bar">
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  if (items === null) return <div className="pinw"><div className="ans-loading">Загружаю приёмку…</div></div>;

  if (!card) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">🧩 Приёмка ребусов</div>
          <div className="pinw-sub">{statusLine || 'Всё разобрано'}</div>
        </div>
        {tabsRow}
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">
            Готовых пар на приёмку нет. Бот сам возьмётся за рисование, когда свободных
            карточек останется меньше десяти, — и тогда напишет тебе в личку.
            Хочешь посмотреть сейчас — закажи пару карточек кнопкой ниже.
          </div>
        </div>
        <div className="pinw-bar">
          <button className="ans-btn pinw-next" disabled={busy} onClick={() => draw(2)}>
            🖼 Нарисовать 2 карточки (≈ $0.17)
          </button>
          <button className="ans-btn-ghost" disabled={busy} onClick={() => refresh(true)}>
            🔄 Проверить ещё раз
          </button>
          {error ? <div className="pinrev-err">{error}</div> : null}
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const [left, right] = card.halves;
  const target = card.halves.find((h) => h.word === redrawFor);

  return (
    <div className={`pinw${typing ? ' typing' : ''}`}>
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">🧩 Приёмка</span>
        <span className="pinw-count">осталось {items.length}</span>
      </div>
      {tabsRow}

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
              onFocus={onTypeFocus} onBlur={onTypeBlur}
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
