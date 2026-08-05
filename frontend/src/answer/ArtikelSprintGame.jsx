import React, { useCallback, useEffect, useRef, useState } from 'react';
import useFitText from './useFitText.js';
import useWideScreen from './useWideScreen.js';
import { saveGermanWordViaLookup } from '../dictionary/saveUtils.js';
import Toast, { useToast } from './Toast.jsx';
import { saveErrorToast } from './saveNotice.js';

// 2-minute der/die/das speed game. The whole word set is preloaded, so each tap
// is graded LOCALLY (instant green/red flash + auto-advance, zero round-trip).
// The official score is re-graded server-side on submit.
const ARTICLES = ['der', 'die', 'das'];

export default function ArtikelSprintGame({ api, haptic, onClose, practice = false, battleId = null, battleList = false }) {
  const [phase, setPhase] = useState('loading'); // loading|themepick|battlelist|intro|countdown|playing|done|error
  const [meta, setMeta] = useState(null);
  const [themes, setThemes] = useState([]);
  const [battles, setBattles] = useState([]);
  const [error, setError] = useState('');
  const [idx, setIdx] = useState(0);
  const [left, setLeft] = useState(120);
  const [count, setCount] = useState(3);
  const [flash, setFlash] = useState(null); // {ok:bool} transient
  const [result, setResult] = useState(null);
  const [savedWords, setSavedWords] = useState(() => new Set());
  const [knownWords, setKnownWords] = useState(() => new Set());  // already in the dictionary
  const toast = useToast();
  const answersRef = useRef([]);
  const wordsRef = useRef([]);
  const startRef = useRef(0);
  const timerRef = useRef(null);
  const idxRef = useRef(0);
  // На планшете потолок кегля задаёт CSS (он там свой и крупный), на телефоне остаётся
  // проверенные 44 px — телефонный вид не трогаем.
  const wide = useWideScreen();
  const wordFit = useFitText(`${wide}|${idx}`, { max: wide ? 'css' : 44, min: 14, fitBy: 'word' });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (battleList) {
          const data = await api('/api/webapp/artikel/battles', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Недоступно'); setPhase('error'); return; }
          setBattles(data.battles || []); setPhase('battlelist'); return;
        }
        if (practice) {
          const data = await api('/api/webapp/artikel/themes', {});
          if (cancelled) return;
          if (!data.ok) { setError(data.error || 'Доступно на Premium'); setPhase('error'); return; }
          setThemes(data.themes || []); setPhase('themepick'); return;
        }
        const data = battleId
          ? await api('/api/webapp/artikel/battle', { battle_id: battleId })
          : await api('/api/webapp/artikel/today', {});
        if (cancelled) return;
        if (!data.ok) { setError(data.error || 'Набор недоступен'); setPhase('error'); return; }
        setMeta(data);
        wordsRef.current = data.words || [];
        if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); }
        else { setLeft(data.duration_s || 120); setPhase('intro'); }
      } catch (e) { try { console.warn('[artikel-sprint] load failed', e); } catch (_err) { /* noop */ } if (!cancelled) { setError('Не удалось загрузить. Попробуйте позже.'); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [api, practice, battleId, battleList]);

  const playBattle = useCallback(async (bid) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/artikel/battle', { battle_id: bid });
      if (!data.ok) { setError(data.error || 'Батл недоступен'); setPhase('error'); return; }
      setMeta(data);
      wordsRef.current = data.words || [];
      if (data.already_played && data.result) { setResult({ ...data.result, items: [] }); setPhase('done'); return; }
      setLeft(data.duration_s || 120);
      setPhase('intro');
    } catch (e) { try { console.warn('[artikel-sprint] action failed', e); } catch (_err) { /* noop */ } setError('Не удалось загрузить. Попробуйте позже.'); setPhase('error'); }
  }, [api]);

  const pickTheme = useCallback(async (themeKey) => {
    setPhase('loading');
    try {
      const data = await api('/api/webapp/artikel/practice', { theme_key: themeKey });
      if (!data.ok) { setError(data.error || 'Тема недоступна'); setPhase('error'); return; }
      setMeta(data);
      wordsRef.current = data.words || [];
      setLeft(data.duration_s || 120);
      setPhase('intro');
    } catch (e) { try { console.warn('[artikel-sprint] action failed', e); } catch (_err) { /* noop */ } setError('Не удалось загрузить. Попробуйте позже.'); setPhase('error'); }
  }, [api]);

  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current); }, []);

  const finish = useCallback(async () => {
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
    setPhase('grading');
    const time_ms = Math.max(0, Date.now() - startRef.current);
    try {
      const data = await api('/api/webapp/artikel/submit', {
        set_id: meta?.set_id, answers: answersRef.current, time_ms,
      });
      setResult(data);
    } catch (e) {
      // fall back to a local tally so the user still sees a result
      const ans = answersRef.current;
      const correct = ans.filter((a) => a.ok).length;
      setResult({ correct, answered: ans.length, total: wordsRef.current.length,
        pct: ans.length ? Math.round((100 * correct) / ans.length) : 0, items: [] });
    }
    setPhase('done');
  }, [api, meta]);

  const startCountdown = useCallback(() => {
    setPhase('countdown'); setCount(3);
    let c = 3;
    const t = setInterval(() => {
      c -= 1;
      if (c <= 0) {
        clearInterval(t);
        startRef.current = Date.now();
        answersRef.current = []; idxRef.current = 0; setIdx(0);
        setPhase('playing');
        timerRef.current = setInterval(() => {
          setLeft((v) => { if (v <= 1) { finish(); return 0; } return v - 1; });
        }, 1000);
      } else { setCount(c); }
    }, 800);
  }, [finish]);

  // Tap a review row → save the noun WITH its article ("der Rosenkohl") to the
  // dictionary, paired with the Russian meaning.
  const saveWord = useCallback((de, ru) => {
    const key = String(de || '').trim();
    if (!key || savedWords.has(key)) return;
    // Optimistic: mark the row 💾 saved instantly and release the user; the network
    // save runs in the background. Revert only if it genuinely fails.
    setSavedWords((s) => new Set(s).add(key));
    haptic?.('ok');
    Promise.resolve(
      saveGermanWordViaLookup({
        api, word: key, fallbackTranslation: String(ru || '').trim(),
        origin: 'artikel_sprint_save',
      }),
    ).then((res) => {
      // Already in the dictionary: the save refreshed that old entry, so it keeps its old
      // place in the list instead of appearing on top. Say so, don't leave them hunting.
      if (res && res.inserted === false) setKnownWords((k) => new Set(k).add(key));
    }).catch((err) => {
      // A failed save used to be signalled by an error vibration alone — the 💾 just
      // flipped back and the word was gone without a word of explanation. Теперь причина
      // говорится вслух всплывающей плашкой (чаще всего это дневной лимит, а не сбой).
      setSavedWords((s) => { const n = new Set(s); n.delete(key); return n; });
      toast.show(saveErrorToast(err));
      haptic?.('bad');
    });
  }, [api, haptic, savedWords, toast]);

  const answer = useCallback((article) => {
    if (phase !== 'playing') return;
    const i = idxRef.current;
    const w = wordsRef.current[i];
    if (!w) return;
    const ok = String(article) === String(w.a);
    answersRef.current.push({ w: w.w, chosen: article, ok });
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    setFlash({ ok, key: i });
    const ni = i + 1;
    idxRef.current = ni; setIdx(ni);
    if (ni >= wordsRef.current.length) { finish(); }
  }, [phase, haptic, finish]);

  let cls = '';
  let body = null;
  if (phase === 'loading') { body = <div className="ans-loading">Lädt…</div>; }
  else if (phase === 'error') {
    body = (<>
      <div className="ans-verdict">⚡ Artikel Sprint</div>
      <div className="ans-explain">{error}</div>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'battlelist') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚔️ Мои батлы</div>
      {battles.length ? (
        <div className="as-themes">
          {battles.map((b) => (
            <button key={b.battle_id} type="button" className="as-theme-btn"
              disabled={b.played} onClick={() => !b.played && playBattle(b.battle_id)}>
              <span>⚔️ {b.creator_name || 'Батл'} · {b.theme_label}</span>
              <span className="as-theme-cnt">{b.played ? '✓ сыграно' : '▶️'}</span>
            </button>
          ))}
        </div>
      ) : (
        <div className="ans-explain">Активных батлов нет. Прими вызов в личке или создай свой (/battle, Premium).</div>
      )}
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else if (phase === 'themepick') {
    cls = 'as-themepick';
    body = (<>
      <div className="as-eyebrow">⚡ Artikel Sprint · своя тема</div>
      <div className="as-rules">Выбери тему — 2 минуты тренировки. <b>Premium</b>.</div>
      <div className="as-themes">
        {themes.map((t) => (
          <button key={t.theme_key} type="button" className="as-theme-btn" onClick={() => pickTheme(t.theme_key)}>
            <span>{t.label_ru || t.label_de}</span>
            <span className="as-theme-cnt">{t.count}</span>
          </button>
        ))}
      </div>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'intro') {
    cls = 'as-intro';
    body = (<>
      <div className="as-eyebrow">⚡ Artikel Sprint</div>
      <div className="as-theme">{meta?.theme_label || ''}</div>
      <div className="as-rules">2 Minuten · tippe <b>der/die/das</b> · so viele wie möglich!</div>
      <button className="ans-btn as-go" onClick={startCountdown}>▶️ Старт</button>
      <button className="ans-btn-ghost" onClick={onClose}>Später</button>
    </>);
  } else if (phase === 'countdown') {
    cls = 'as-countdown';
    body = <div className="as-count" key={count}>{count}</div>;
  } else if (phase === 'playing') {
    const w = wordsRef.current[idx];
    cls = 'as-play';
    body = (<>
      <div className="as-top ans-r-head">
        <span className="as-theme-sm">{meta?.theme_label || ''}</span>
        <span className="as-timer">{Math.floor(left / 60)}:{String(left % 60).padStart(2, '0')}</span>
        <span className="as-score">{answersRef.current.filter((a) => a.ok).length}</span>
      </div>
      <div className={`as-word ans-r-prompt${flash ? (flash.ok ? ' ok' : ' bad') : ''}`} key={idx}>
        <span className="fit-line" lang="de" ref={wordFit}>{w ? w.w : '…'}</span>
      </div>
      {/* Two-gender nouns (der See / die See) are only answerable if we show which
          sense we mean — reveal the Russian meaning for these, and only these. */}
      {w && w.tg && w.ru ? <div className="as-sense">({w.ru})</div> : null}
      <div className="as-buttons ans-r-work">
        {ARTICLES.map((art) => (
          <button key={art} type="button" className={`as-btn-art art-${art}`} onClick={() => answer(art)}>
            {art}
          </button>
        ))}
      </div>
    </>);
  } else if (phase === 'grading') {
    body = <div className="ans-loading">Zähle…</div>;
  } else {
    const r = result || {};
    const items = r.items || [];
    const rank = r.ranking || null;
    const place = rank?.your_place || null;
    const medal = place === 1 ? '🥇' : place === 2 ? '🥈' : place === 3 ? '🥉' : '🎖️';
    cls = 'as-done';
    body = (<>
      {place ? (
        <div className="as-cert">
          <div className="as-cert-medal">{medal}</div>
          <div className="as-cert-place">{place} место</div>
          <div className="as-cert-sub">из {rank.total} · {r.correct || 0} верных ({r.pct || 0}%)</div>
          <div className="as-cert-foot">⚡ Artikel Sprint{meta?.theme_label ? ` · ${meta.theme_label}` : ''}</div>
        </div>
      ) : (<>
        <div className="ans-verdict">⚡ Artikel Sprint</div>
        <div className="as-result-score"><b>{r.correct || 0}</b> верных из {r.answered || 0} · {r.pct || 0}%</div>
      </>)}
      {r.already_played ? <div className="ans-explain">Ты уже играл этот сет сегодня.</div> : null}
      {rank && rank.total ? (
        <div className="sp-rank">
          <div className="sp-rank-head">🏆 Топ-3</div>
          {(rank.top3 || []).map((p, i) => (
            <div className={`sp-rank-row${rank.your_place === i + 1 ? ' me' : ''}`} key={p.user_id || i}>
              <span>{['🥇', '🥈', '🥉'][i] || '•'} {p.name || 'Игрок'}</span>
              <span className="sp-rank-n">{p.count}</span>
            </div>
          ))}
        </div>
      ) : null}
      {items.length ? (
        <>
          <div className="as-save-hint">👆 нажми на слово, чтобы сохранить в словарь (с артиклем)</div>
          <div className="as-result-list ans-body">
            {items.map((it, i) => {
              const de = `${it.a} ${it.w}`.trim();
              const isSaved = savedWords.has(de);
              const isKnown = knownWords.has(de);
              return (
                <button
                  type="button"
                  key={i}
                  className={`as-row as-row-tap ${it.ok ? 'ok' : 'bad'}${isSaved ? ' saved' : ''}`}
                  disabled={isSaved}
                  onClick={() => saveWord(de, it.ru)}
                >
                  <span className="as-row-mark">{isSaved ? '💾' : (it.ok ? '✅' : '❌')}</span>
                  {' '}<b className={it.ok ? '' : 'as-correct-article'}>{it.a}</b> {it.w}
                  {isKnown ? <span className="as-mine"> · уже есть</span> : null}
                  {!it.ok ? <span className="as-mine"> (ты: {it.chosen || '—'})</span> : null}
                  {it.ru ? <span className="as-ru"> · {it.ru}</span> : null}
                </button>
              );
            })}
          </div>
        </>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  }

  return (
    // `ans-root--keepkbd`: под клавиатуру этот интерактив не перестраивается. Артикль
    // выбирается кнопками, печатать в карточке нечего; клавиатура выезжает только под окно
    // «Спросить», а оно и так встаёт над ней.
    <div className="ans-root ans-root--keepkbd">
      <div className={`ans-card as-card ${cls}`}>{body}</div>
      <Toast state={toast.state} onClose={toast.hide} />
    </div>
  );
}
