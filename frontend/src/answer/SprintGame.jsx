import React, { useCallback, useEffect, useRef, useState } from 'react';

// 60-second "name as many synonyms/antonyms as you can" game. Winner = most
// correct. Live per-word check (fast list membership); the authoritative count
// + LLM batch over the misses happens on /finish.
const REL = {
  synonym: { title: 'Синонимы-спринт', verb: 'синонимов', emoji: '🟢' },
  antonym: { title: 'Антонимы-спринт', verb: 'антонимов', emoji: '🔴' },
};

const normWord = (t) => t.toLowerCase().replace(/^(der|die|das)\s+/, '').replace(/\s+/g, ' ').trim();

function SprintRanking({ ranking }) {
  if (!ranking || !ranking.total) return null;
  const { total, your_place, your_count, top3 } = ranking;
  return (
    <div className="sp-rank">
      <div className="sp-rank-head">🏆 Место {your_place || '—'} из {total} · {your_count} слов</div>
      {(top3 || []).map((r, i) => (
        <div className={`sp-rank-row${r.user_id && ranking.your_place === i + 1 ? ' me' : ''}`} key={r.user_id || i}>
          <span>{['🥇', '🥈', '🥉'][i] || '•'} {r.name || 'Игрок'}</span>
          <span className="sp-rank-n">{r.count}</span>
        </div>
      ))}
    </div>
  );
}

export default function SprintGame({ id, api, haptic, onClose }) {
  const [phase, setPhase] = useState('loading'); // loading|intro|playing|grading|done|error
  const [meta, setMeta] = useState(null);
  const [error, setError] = useState('');
  const [words, setWords] = useState([]);
  const [left, setLeft] = useState(60);
  const [input, setInput] = useState('');
  const [result, setResult] = useState(null);
  const startRef = useRef(0);
  const wordsRef = useRef([]);
  const timerRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/sprint/task', { kind: 'sp', id });
        if (cancelled) return;
        setMeta(data);
        if (data.already_played && data.result) { setResult(data.result); setPhase('done'); }
        else { setLeft(data.duration_s || 60); setPhase('intro'); }
      } catch (e) { if (!cancelled) { setError(String(e.message || e)); setPhase('error'); } }
    })();
    return () => { cancelled = true; };
  }, [id]);

  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current); }, []);

  const finish = useCallback(async () => {
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
    setPhase('grading');
    const time_ms = startRef.current ? Date.now() - startRef.current : 0;
    try {
      const data = await api('/api/sprint/finish', { id, words: wordsRef.current.map((w) => w.text), time_ms });
      setResult(data); setPhase('done');
      try { haptic?.(data.count > 0 ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
  }, [id]);

  const start = useCallback(() => {
    startRef.current = Date.now();
    const dur = meta?.duration_s || 60;
    setLeft(dur);
    setPhase('playing');
    timerRef.current = setInterval(() => {
      setLeft((s) => {
        if (s <= 1) { clearInterval(timerRef.current); timerRef.current = null; finish(); return 0; }
        return s - 1;
      });
    }, 1000);
    setTimeout(() => inputRef.current?.focus(), 50);
  }, [meta, finish]);

  const submitWord = useCallback(async () => {
    const text = input.trim();
    setInput('');
    if (!text) return;
    const n = normWord(text);
    if (!n || wordsRef.current.some((w) => w.norm === n)) {
      try { haptic?.('bad'); } catch (_e) { /* noop */ }
      return;
    }
    const entry = { text, norm: n, status: 'pending' };
    wordsRef.current = [entry, ...wordsRef.current];
    setWords(wordsRef.current.slice());
    try {
      const r = await api('/api/sprint/check', { id, word: text });
      entry.status = r.status === 'hit' ? 'hit' : 'pending';
      setWords(wordsRef.current.slice());
      try { haptic?.(r.status === 'hit' ? 'ok' : 'light'); } catch (_e) { /* noop */ }
    } catch (_e) { /* keep pending; /finish is authoritative */ }
  }, [input, id]);

  const rel = REL[meta?.relation] || REL.synonym;
  const hits = words.filter((w) => w.status === 'hit').length;

  const shell = (body) => (
    <div className="ans-root"><div className="ans-card">{body}</div></div>
  );

  if (phase === 'loading') return shell(<><div className="ans-skel" /><div className="ans-skel sm" /></>);
  if (phase === 'error') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );

  if (phase === 'intro') return shell(
    <>
      <div className="ans-head">
        <span className="ans-eyebrow">{rel.emoji} {rel.title}</span>
        <h1 className="ans-title">{meta?.wort}</h1>
        {meta?.hint_ru ? <p className="ans-sub">{meta.hint_ru}</p> : null}
      </div>
      <div className="sp-intro">
        <p>За <b>60 секунд</b> напиши как можно больше <b>{rel.verb}</b>!</p>
        <p className="sp-intro-dim">Победитель — у кого больше правильных. Таймер пойдёт сразу.</p>
      </div>
      <button className="ans-btn" onClick={start}>▶️ Старт (60 с)</button>
    </>
  );

  if (phase === 'playing') return shell(
    <>
      <div className="sp-top">
        <span className="sp-word">{rel.emoji} {meta?.wort}</span>
        <span className={`sp-timer${left <= 10 ? ' low' : ''}`}>{left}s</span>
      </div>
      <div className="sp-bar"><div className="sp-bar-fill" style={{ width: `${(left / (meta?.duration_s || 60)) * 100}%` }} /></div>
      <div className="sp-count">✅ {hits}<span className="sp-count-sub"> {rel.verb}</span></div>
      <input
        ref={inputRef} className="ans-input" value={input}
        onChange={(e) => setInput(e.target.value)}
        placeholder={`${rel.verb.slice(0, -2)}…`} autoFocus autoCapitalize="off" autoCorrect="off"
        enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submitWord(); }}
      />
      <div className="sp-chips">
        {words.map((w, i) => (
          <span key={i} className={`sp-chip ${w.status}`}>{w.status === 'hit' ? '✓ ' : ''}{w.text}</span>
        ))}
      </div>
      <button className="ans-btn-ghost" onClick={finish}>Готово ⏹</button>
    </>
  );

  if (phase === 'grading') return shell(
    <div className="ls-grading"><div className="ls-spinner" /><p className="ans-sub" style={{ textAlign: 'center' }}>Считаем результат…</p></div>
  );

  // done
  const r = result || {};
  const foundSet = new Set((r.found || []).map(normWord));
  return shell(
    <>
      <div className="ans-head">
        <span className="ans-eyebrow">{rel.emoji} {rel.title}</span>
      </div>
      <div className="ans-result ok">
        <div className="ans-verdict">🏆 Ты нашёл {r.count} {rel.verb}!</div>
        {r.accepted_total ? <div className="ans-meaning">из {r.accepted_total} в нашем списке</div> : null}
      </div>
      <SprintRanking ranking={r.ranking} />
      {(r.accepted || []).length ? (
        <div className="sp-all">
          <div className="sp-all-head">Все варианты:</div>
          <div className="sp-chips">
            {(r.accepted || []).map((a, i) => (
              <span key={i} className={`sp-chip ${foundSet.has(normWord(a)) ? 'hit' : 'missed'}`}>{a}</span>
            ))}
          </div>
        </div>
      ) : null}
      {r.erklaerung ? <div className="ans-explain">{r.erklaerung}</div> : null}
      {r.tip ? <div className="ans-tip">💡 {r.tip}</div> : null}
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );
}
