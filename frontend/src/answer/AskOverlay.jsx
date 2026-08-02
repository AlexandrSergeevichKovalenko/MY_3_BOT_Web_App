import { saveErrorToast } from './saveNotice.js';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { renderRich } from './richText.jsx';

// Полоса экрана, которую пользователь РЕАЛЬНО видит сейчас.
//
// Клавиатура на iOS не меняет ни `window.innerHeight`, ни систему координат `position:
// fixed` — она меняет только visual viewport. Поэтому окно, поставленное по innerHeight,
// остаётся ровно там, где стояло, и клавиатура его просто закрывает. Единственный
// источник правды здесь — visualViewport: его height даёт высоту без клавиатуры, а
// offsetTop — насколько видимая область уехала внутри разметочной.
function readView() {
  const vv = typeof window !== 'undefined' ? window.visualViewport : null;
  if (vv && vv.height > 120) {
    return { top: vv.offsetTop || 0, height: vv.height, width: vv.width || window.innerWidth || 360 };
  }
  return { top: 0, height: window.innerHeight || 640, width: window.innerWidth || 360 };
}

// Floating, draggable "ask the model" window used on every interactive's result.
// No backdrop — it floats over the task (still visible). Drag by the header to any
// spot. Chat thread (iMessage-style): your question, then the model's answer below.
// `api(path, body)` posts JSON (initData injected by the host); `context` is a short
// description of the current task so the answer is on-point.
export default function AskOverlay({ api, context = '', onClose, saveText = '', saveTranslation = '' }) {
  const [messages, setMessages] = useState([]); // {role:'user'|'bot', text}
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const [saveState, setSaveState] = useState('idle'); // 'idle'|'saving'|'saved'|'error'
  const [saveErrorText, setSaveErrorText] = useState('');
  const [savedRu, setSavedRu] = useState('');
  const [savedWord, setSavedWord] = useState(''); // the text actually saved (for the ✓ label)
  const [pos, setPos] = useState(null); // {x, y} top-left; null until measured
  const panelRef = useRef(null);
  const threadRef = useRef(null);
  const drag = useRef(null);

  // Видимая полоса экрана: меняется, когда открывается клавиатура (см. readView).
  const [view, setView] = useState(readView);
  useEffect(() => {
    const on = () => setView(readView());
    const vv = typeof window !== 'undefined' ? window.visualViewport : null;
    vv?.addEventListener('resize', on);
    vv?.addEventListener('scroll', on);
    window.addEventListener('resize', on);
    return () => {
      vv?.removeEventListener('resize', on);
      vv?.removeEventListener('scroll', on);
      window.removeEventListener('resize', on);
    };
  }, []);

  const clamp = useCallback((x, y) => {
    const el = panelRef.current;
    const w = el ? el.offsetWidth : 320;
    const h = el ? el.offsetHeight : 320;
    const top = view.top + 6;
    const bottom = view.top + view.height - h - 6;
    return {
      x: Math.min(Math.max(6, x), Math.max(6, view.width - w - 6)),
      y: Math.min(Math.max(top, y), Math.max(top, bottom)),
    };
  }, [view]);

  // Initial position: lower-centre, so the task stays visible above the window.
  useEffect(() => {
    const el = panelRef.current;
    if (!el) return;
    const w = el.offsetWidth || 320;
    const h = el.offsetHeight || 320;
    setPos({ x: Math.max(8, (view.width - w) / 2), y: Math.max(view.top + 8, view.top + view.height - h - 24) });
    // намеренно один раз: дальше окно двигает пользователь, а видимую полосу стережёт clamp
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Видимая полоса изменилась (открылась/закрылась клавиатура, уехала шторка) — возвращаем
  // окно внутрь неё. Без этого оно остаётся под клавиатурой: ввод есть, а окна не видно.
  useEffect(() => {
    setPos((p) => (p ? clamp(p.x, p.y) : p));
  }, [clamp]);

  const onHeaderPointerDown = useCallback((e) => {
    const start = pos || { x: 0, y: 0 };
    drag.current = { sx: e.clientX, sy: e.clientY, ox: start.x, oy: start.y };
    try { e.target.setPointerCapture?.(e.pointerId); } catch (_e) { /* ignore */ }
  }, [pos]);

  const onHeaderPointerMove = useCallback((e) => {
    if (!drag.current) return;
    const dx = e.clientX - drag.current.sx;
    const dy = e.clientY - drag.current.sy;
    setPos(clamp(drag.current.ox + dx, drag.current.oy + dy));
  }, [clamp]);

  const onHeaderPointerUp = useCallback((e) => {
    drag.current = null;
    try { e.target.releasePointerCapture?.(e.pointerId); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => {
    const t = threadRef.current;
    if (t) t.scrollTop = t.scrollHeight;
  }, [messages, busy]);

  const ask = useCallback(async () => {
    const q = input.trim();
    if (!q || busy) return;
    const history = [];
    for (let i = 0; i < messages.length - 1; i += 1) {
      if (messages[i].role === 'user' && messages[i + 1] && messages[i + 1].role === 'bot') {
        history.push({ q: messages[i].text, a: messages[i + 1].text });
      }
    }
    setMessages((m) => [...m, { role: 'user', text: q }]);
    setInput('');
    setBusy(true);
    setErr('');
    try {
      const res = await api('/api/webapp/ask', { context, learner_question: q, history });
      if (res && res.ok && res.answer) {
        setMessages((m) => [...m, { role: 'bot', text: String(res.answer) }]);
      } else {
        setErr((res && (res.error || res.message)) || 'Не удалось получить ответ');
      }
    } catch (e) {
      setErr(String((e && e.message) || e));
    } finally {
      setBusy(false);
    }
  }, [input, busy, messages, api, context]);

  // Save through the SAME canonical flow as the Reader / YouTube: first a
  // dictionary lookup (LLM builds the real Russian translation + article +
  // examples — the full card), then persist that card. Sending the German text
  // straight to /save (the old behaviour) left grammar-game words with no Russian
  // translation, so the entry showed German on both sides.
  const save = useCallback(() => {
    // Save whatever the user typed in the field; if it's empty, fall back to the
    // task's fixed word/sentence (the old behaviour). The lookup→save pipeline below
    // handles word-vs-sentence, article + Grundform, grammar correction, translation
    // and the language pair — same as the Reader / dictionary save everywhere else.
    const word = (input.trim() || String(saveText || '').trim());
    if (saveState === 'saving' || saveState === 'saved' || !word) return;
    // Optimistic: confirm instantly and release the user — don't make them wait on
    // the GPT lookup + persist. The canonical lookup→save runs in the background and
    // enriches the ✓ label with the translation when it lands. Only if it genuinely
    // fails do we revert to a retryable error state.
    setSavedWord(word);
    setSavedRu('');
    setSaveState('saved');
    try { window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* ignore */ }
    (async () => {
      try {
        const lookup = await api('/api/webapp/dictionary', { word });
        const item = (lookup && lookup.item) || {};
        const direction = String(lookup?.direction || '').toLowerCase();
        const isDeRu = direction !== 'ru-de'; // task words are German → de-ru
        const sourceText = String(
          (isDeRu ? (item.word_de || item.translation_de) : (item.word_ru || item.translation_ru)) || word,
        ).trim();
        const targetText = String(
          (isDeRu ? (item.translation_ru || item.word_ru) : (item.translation_de || item.word_de))
          || saveTranslation || '',
        ).trim();
        await api('/api/webapp/dictionary/save', {
          source_text: sourceText,
          target_text: targetText,
          translation_ru: String(item.translation_ru || (isDeRu ? targetText : '')).trim(),
          translation_de: String(item.translation_de || (isDeRu ? '' : targetText)).trim(),
          direction: direction || 'de-ru',
          response_json: item,
          origin_process: 'ask_overlay',
        });
        setSavedRu(isDeRu ? targetText : sourceText);
      } catch (err) {
        // Причина важнее факта: чаще всего это дневной лимит бесплатного тарифа, и
        // «повторить» тут не поможет — человек должен это понимать.
        setSaveState('error');
        setSaveErrorText(saveErrorToast(err).text);
      }
    })();
  }, [saveState, saveText, saveTranslation, api, input]);

  // Truncated label for the button when saving a long sentence.
  const saveCandidate = (input.trim() || String(saveText || '').trim());
  const saveLabelWord = saveCandidate.length > 40 ? `${saveCandidate.slice(0, 40)}…` : saveCandidate;

  // Высоту окна тоже держим в пределах видимой полосы: с открытой клавиатурой места мало,
  // и окно должно ужаться (переписка внутри получает свою прокрутку), а не вылезти.
  const maxH = Math.max(180, Math.round(view.height - 12));
  const style = pos
    ? { left: `${pos.x}px`, top: `${pos.y}px`, maxHeight: `${maxH}px` }
    : { left: '50%', top: '60%', maxHeight: `${maxH}px`, visibility: 'hidden' };

  // Рисуем окно в <body>, а не внутри карточки. В части игр «Спросить» лежит ВНУТРИ
  // `.ans-card`, а карточку подгонка масштабирует через `zoom` — вместе с ней масштабируются
  // и координаты, и размеры окна, так что расчёт «поместись в видимую полосу» врал бы.
  return createPortal((
    <div className="ask-pop" ref={panelRef} style={style}>
      <div
        className="ask-pop-head"
        onPointerDown={onHeaderPointerDown}
        onPointerMove={onHeaderPointerMove}
        onPointerUp={onHeaderPointerUp}
        onPointerCancel={onHeaderPointerUp}
      >
        <span className="ask-pop-grip">⋮⋮</span>
        {/* Подпись «зажми и перетащи» убрана: она занимала строку, а про перетаскивание уже
            говорит сама ручка слева. Экран телефона дороже подсказки к очевидному. */}
        <span className="ask-pop-title">Спросить</span>
        <button type="button" className="ask-pop-close" onClick={onClose} aria-label="Закрыть">✕</button>
      </div>
      {/* Переписки ещё нет — блока тоже нет. Раньше он занимал четверть экрана ради одной
          фразы «задай любой вопрос — отвечу здесь», которая слово в слово повторяет
          подсказку в поле ввода двумя строками ниже. Пустое место вместо содержания. */}
      {messages.length || busy || err ? (
        <div className="ask-pop-thread" ref={threadRef}>
          {messages.map((m, i) => (
            <div key={i} className={`ask-bubble ${m.role === 'user' ? 'me' : 'bot'}`}>
              {m.role === 'bot' ? renderRich(m.text) : m.text}
            </div>
          ))}
          {busy ? <div className="ask-bubble bot ask-typing">…</div> : null}
          {err ? <div className="ask-pop-err">{err}</div> : null}
        </div>
      ) : null}
      <div className="ask-pop-input">
        <textarea
          value={input}
          onChange={(e) => {
            setInput(e.target.value);
            // editing the field re-enables Save for a fresh entry
            if (saveState === 'saved' || saveState === 'error') { setSaveState('idle'); setSavedWord(''); }
          }}
          placeholder="Вопрос — или впиши слово/фразу и нажми Сохранить"
          rows={3}
          autoCapitalize="sentences"
          onKeyDown={(e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) ask(); }}
        />
        <button type="button" className="ask-pop-send" onClick={ask} disabled={busy || !input.trim()}>
          {busy ? '…' : 'Спросить'}
        </button>
      </div>
      <button
        type="button"
        className={`ask-pop-save ${saveState === 'saved' ? 'is-saved' : ''} ${saveState === 'error' ? 'is-error' : ''}`}
        onClick={save}
        disabled={saveState === 'saving' || saveState === 'saved' || !saveCandidate}
      >
        {saveState === 'saving' ? '⏳ Сохраняю…'
          : saveState === 'saved'
            ? (savedRu ? `✓ «${savedWord}» — ${savedRu}` : `✓ «${savedWord}» в словаре`)
            : saveState === 'error' ? (saveErrorText || '⚠️ Не вышло — повторить')
              : saveCandidate ? `💾 Сохранить «${saveLabelWord}»` : '💾 Сохранить'}
      </button>
    </div>
  ), document.body);
}
