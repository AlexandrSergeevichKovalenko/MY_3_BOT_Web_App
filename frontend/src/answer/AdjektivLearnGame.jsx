import React, { useCallback, useEffect, useRef, useState } from 'react';
import useFitText from './useFitText.js';
import useWideScreen from './useWideScreen.js';
import AskOverlay from './AskOverlay.jsx';
import AdjHint from './AdjHint.jsx';
import { saveGermanWordViaLookup } from '../dictionary/saveUtils.js';
import { saveErrorToast } from './saveNotice.js';

// Adjektiv Trainer — self-paced learning deck for adjective endings (companion to
// the Adjektiv Sprint, same look). Each card: a phrase with a blanked ending +
// the case/gender/declension hint; tap an ending → instant green/red + the full
// rule + a tip; swipe/"Weiter" to the next. Endless (prefetches batches). No timer.
const ENDINGS = ['e', 'en', 'er', 'es', 'em'];

export default function AdjektivLearnGame({ api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|card|error
  const [deck, setDeck] = useState([]);
  const [i, setI] = useState(0);
  const [pick, setPick] = useState(null);
  const [streak, setStreak] = useState(0);
  const [error, setError] = useState('');
  // Re-fit when the actual phrase content arrives (async) or the slot widens after
  // answering — not just on index change, or the first card measures stale/empty text
  // and overflows.
  const _cur = deck[i];
  const wide = useWideScreen();   // планшет: потолок кегля задаёт CSS
  const phraseFit = useFitText(
    _cur ? `${wide}|${_cur.before}${_cur.after}${pick || ''}` : `${wide}|${i}`,
    { max: wide ? 'css' : 36 },
  );

  const loadMore = useCallback(async () => {
    try {
      const d = await api('/api/webapp/adjektiv/learn', { count: 12 });
      if (!d.ok) { setError(d.error || 'Недоступно'); setPhase('error'); return; }
      setDeck((prev) => [...prev, ...(d.items || [])]);
      setPhase('card');
    } catch (e) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
  }, [api]);

  useEffect(() => { loadMore(); }, [loadMore]);

  const card = deck[i];
  // Prefetch the next batch as we near the end so it never stalls.
  useEffect(() => {
    if (phase === 'card' && deck.length && i >= deck.length - 2) loadMore();
  }, [i, deck.length, phase, loadMore]);

  const answer = useCallback((e) => {
    if (pick || !card) return;
    setPick(e);
    const ok = e === card.a;
    setStreak((s) => (ok ? s + 1 : 0));
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
  }, [pick, card, haptic]);

  const next = useCallback(() => { setPick(null); setI((x) => x + 1); setWordPop(null); }, []);

  // Tap-to-translate: a small popup for the adjective or the noun (accurate
  // word-level translation; the phrase itself is auto-generated and may be unreal).
  const [wordPop, setWordPop] = useState(null); // null | {kind, de, ru, saving, saved}
  const [askOpen, setAskOpen] = useState(false);
  const [toast, setToast] = useState(null);     // null | {text, kind: 'bad'|'info'}
  const toastTimer = useRef(null);
  const openWord = useCallback((kind) => {
    if (!card) return;
    try { haptic?.('tap'); } catch (_e) { /* noop */ }
    if (kind === 'adj' && card.adj) {
      setWordPop({ kind, de: card.adj, ru: card.adj_ru || '', save_de: card.adj, saving: false, saved: false });
    } else if (kind === 'noun' && card.noun) {
      const art = card.noun_article ? `${card.noun_article} ` : '';
      setWordPop({ kind, de: `${art}${card.noun}`, ru: card.noun_ru || '',
        save_de: `${art}${card.noun}`.trim(), saving: false, saved: false });
    }
  }, [card, haptic]);

  // Toast over the game. The save popup auto-dismisses after 650 ms, long before a slow
  // request fails — so a failure has nowhere to be shown and used to be swallowed while
  // the user had already seen a ✓. This is that missing channel: 3 seconds, floating,
  // doesn't interrupt the round.
  const showToast = useCallback((text, kind = 'bad') => {
    setToast({ text, kind });
    if (toastTimer.current) clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(null), 5000);
  }, []);
  useEffect(() => () => { if (toastTimer.current) clearTimeout(toastTimer.current); }, []);

  const saveWord = useCallback(() => {
    // Optimistic save: confirm instantly and release the learner. The network call
    // runs in the background (fire-and-forget) — no "Сохраняю…" wait. The popup
    // flips to ✓ and auto-dismisses, so the user goes straight back to learning.
    if (!wordPop || wordPop.saved) return;
    const word_de = wordPop.save_de;
    const fallbackRu = wordPop.ru;
    setWordPop((w) => (w ? { ...w, saving: false, saved: true } : w));
    try { haptic?.('ok'); } catch (_e) { /* noop */ }
    // Canonical lookup→save (same pipeline as the dictionary overlay / Reader): the deck's
    // own gloss is often empty (noun_ru/adj_ru are optional), and the old raw save posted
    // the German headword with an INVERTED ru→de pair, so the entry landed with German on
    // both sides and no card content. Now the breakdown supplies the Russian + metainfo.
    Promise.resolve(
      saveGermanWordViaLookup({
        api,
        word: word_de,
        fallbackTranslation: fallbackRu,
        origin: 'adjektiv_trainer',
      }),
    ).then((res) => {
      // Already in the dictionary: the save refreshed that entry, so it keeps its old
      // place in the list — say so, or the user looks for it at the top and doesn't find it.
      if (res && res.inserted === false) showToast(`«${word_de}» уже был в словаре`, 'info');
    }).catch((err) => {
      // Причину называем вслух: чаще всего это дневной лимит бесплатного тарифа, а не сбой,
      // и «нажми ещё раз» в этом случае — враньё.
      const note = saveErrorToast(err);
      showToast(note.hint ? `${note.text} ${note.hint}` : note.text, note.kind === 'limit' ? 'info' : 'bad');
      try { haptic?.('bad'); } catch (_e) { /* noop */ }
    });
    // Auto-close shortly after the ✓ shows; guard so we don't close a different
    // word the user may have opened in the meantime.
    setTimeout(() => setWordPop((w) => (w && w.save_de === word_de ? null : w)), 650);
  }, [wordPop, api, haptic, showToast]);

  let cls = 'al-card';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">📚 Adjektiv Trainer</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (!card) {
    body = <div className="ans-loading">Lädt…</div>;
  } else {
    const answered = !!pick;
    const correct = card.a;
    body = (<>
      <div className="as-top ans-r-head">
        <span className="al-progress">📚 Adjektivendungen</span>
        {streak > 1 ? <span className="al-streak">🔥 {streak}</span> : <span />}
      </div>
      <div className={`as-word ans-r-prompt adj-word${answered ? (pick === correct ? ' ok' : ' bad') : ''}`}>
        <span className="fit-line adj-line" ref={phraseFit}>
          {(() => {
            const adj = card.adj || ''; const b = card.before || '';
            if (adj && b.endsWith(adj)) {
              const prefix = b.slice(0, b.length - adj.length);
              return (<>
                {prefix ? <span>{prefix}</span> : null}
                <span className="adj-word-tap" onClick={() => openWord('adj')}>{adj}</span>
              </>);
            }
            return <span>{b}</span>;
          })()}
          <span className="adj-slot">{answered ? `-${correct}` : '·'}</span>
          {(() => {
            const noun = card.noun || ''; const a = card.after || '';
            const idx = noun ? a.indexOf(noun) : -1;
            if (idx >= 0) {
              return (<>
                {a.slice(0, idx) ? <span>{a.slice(0, idx)}</span> : null}
                <span className="adj-word-tap" onClick={() => openWord('noun')}>{noun}</span>
                {a.slice(idx + noun.length) ? <span>{a.slice(idx + noun.length)}</span> : null}
              </>);
            }
            return <span>{a}</span>;
          })()}
        </span>
      </div>
      <AdjHint text={card.ru} />
      {(card.adj || card.noun) ? (
        <div className="adj-disclaimer">
          Тапни слово — перевод и «в словарь». Фразы генерируются автоматически и
          бывают нереальными/смешными: суть не в смысле, а в механике — по роду/падежу/числу
          быстро выбрать окончание.
        </div>
      ) : null}
      <div className="as-buttons ans-r-work adj-buttons">
        {ENDINGS.map((e) => {
          const state = answered ? (e === correct ? ' on' : (e === pick ? ' wrong' : '')) : '';
          return (
            <button key={e} type="button" className={`as-btn-art adj-end${state}`}
              onClick={() => answer(e)} disabled={answered}>-{e}</button>
          );
        })}
      </div>
      {answered ? (
        <div className={`adj-rev ans-body ${pick === correct ? 'ok' : 'bad'}`} style={{ marginTop: 14 }}>
          <div className="adj-rev-phrase">{pick === correct ? '✅ Richtig!' : '❌'} <b>{card.full || `${card.before}${correct}${card.after}`}</b></div>
          {card.erklaerung ? <div className="adj-rev-rule">📐 {card.erklaerung}</div> : null}
          {card.tip ? <div className="adj-rev-tip">💡 {card.tip}</div> : null}
          {card.example ? <div className="adj-rev-rule">📝 {card.example}</div> : null}
        </div>
      ) : null}
      <button className="ask-open-btn" onClick={() => setAskOpen(true)}>❓ Спросить</button>
      {answered
        ? <button className="ans-btn" onClick={next}>Weiter →</button>
        : <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>}
      {askOpen ? (
        <AskOverlay api={api} onClose={() => setAskOpen(false)}
          saveText={card.noun
            ? `${card.noun_article ? card.noun_article + ' ' : ''}${card.noun}`.trim()
            : (card.adj || '')}
          saveTranslation={card.noun ? (card.noun_ru || '') : (card.adj_ru || '')}
          context={[
            'Интерактив: Adjektivendungen (окончания прилагательных).',
            card.full ? `Фраза: ${card.full}.` : '',
            (card.adj || card.noun) ? `Слова: ${[card.adj, card.noun].filter(Boolean).join(', ')}.` : '',
            card.ru ? `Грамматика: ${card.ru}.` : '',
            card.erklaerung ? `Правило: ${card.erklaerung}.` : '',
          ].filter(Boolean).join(' ')} />
      ) : null}
      {wordPop ? (
        <div className="adj-wordpop-backdrop" onClick={() => setWordPop(null)}>
          <div className="adj-wordpop" onClick={(e) => e.stopPropagation()}>
            <div className="adj-wordpop-de">{wordPop.de}</div>
            <div className="adj-wordpop-ru">{wordPop.ru || '—'}</div>
            <button className="adj-wordpop-save" onClick={saveWord}
              disabled={wordPop.saving || wordPop.saved}>
              {wordPop.saved ? '✓ В словаре' : (wordPop.saving ? 'Сохраняю…' : '➕ В словарь')}
            </button>
          </div>
        </div>
      ) : null}
    </>);
  }

  return (
    // `ans-root--keepkbd`: под клавиатуру этот интерактив не перестраивается. Печатать в
    // самой карточке нечего — окончание выбирается кнопками; клавиатура выезжает только под
    // окно «Спросить», а оно и так встаёт над ней.
    <div className="ans-root ans-root--keepkbd">
      <div className={`ans-card as-card ${cls}`}>{body}</div>
      {toast ? <div className={`al-toast ${toast.kind}`} role="status">{toast.text}</div> : null}
    </div>
  );
}
