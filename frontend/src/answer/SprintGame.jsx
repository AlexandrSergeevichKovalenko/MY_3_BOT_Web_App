import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

// 60-second "name as many synonyms/antonyms as you can" game. Winner = most
// correct. The hot path (typing for 60s) makes ZERO server calls: the server
// ships hashes of the accepted answers, the client validates each word locally
// (instant ✓, no key leak). /finish is the authoritative grader (list + one LLM
// batch over the misses). Heavy work stays off the user's path.
const REL = {
  synonym: { title: 'Синонимы-спринт', verb: 'синонимов', one: 'синоним', emoji: '🟢' },
  antonym: { title: 'Антонимы-спринт', verb: 'антонимов', one: 'антоним', emoji: '🔴' },
};
const ARTICLES = new Set(['der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen', 'einem', 'einer', 'eines']);

// Mirrors backend _sprint_core() — keep in sync.
function normalizeCore(s) {
  let x = String(s || '').toLowerCase().replace(/[^a-zäöüßà-ÿ0-9\s'-]/g, ' ').replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
  let toks = x ? x.split(' ') : [];
  if (toks.length && ARTICLES.has(toks[0])) toks = toks.slice(1);
  return toks.join(' ');
}
async function sha16(s) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('').slice(0, 16);
}

function SprintRanking({ ranking }) {
  if (!ranking || !ranking.total) return null;
  const { total, your_place, your_count, top3 } = ranking;
  return (
    <div className="sp-rank">
      <div className="sp-rank-head">🏆 Место {your_place || '—'} из {total} · {your_count} слов</div>
      {(top3 || []).map((r, i) => (
        <div className={`sp-rank-row${your_place === i + 1 ? ' me' : ''}`} key={r.user_id || i}>
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

  const hashes = useMemo(() => new Set(meta?.accepted_hashes || []), [meta]);

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
    setTimeout(() => inputRef.current?.focus(), 60);
  }, [meta, finish]);

  const submitWord = useCallback(async () => {
    const text = input.trim();
    setInput('');
    if (!text) return;
    const n = normalizeCore(text);
    if (!n || wordsRef.current.some((w) => w.norm === n)) {
      try { haptic?.('bad'); } catch (_e) { /* noop */ }
      return;
    }
    const entry = { text, norm: n, status: 'pending' };
    wordsRef.current = [entry, ...wordsRef.current];
    setWords(wordsRef.current.slice());
    let hit = false;
    try { hit = hashes.has(await sha16(n)); } catch (_e) { /* keep pending */ }
    entry.status = hit ? 'hit' : 'pending';
    setWords(wordsRef.current.slice());
    try { haptic?.(hit ? 'ok' : 'light'); } catch (_e) { /* noop */ }
  }, [input, hashes]);

  const rel = REL[meta?.relation] || REL.synonym;
  const hits = words.filter((w) => w.status === 'hit').length;
  const dur = meta?.duration_s || 60;

  const shell = (body, cls = '') => (
    <div className="ans-root"><div className={`ans-card ${cls}`}>{body}</div></div>
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
        <span className="ans-eyebrow">{rel.emoji} {rel.title} · B2+</span>
      </div>
      <div className="sp-hero">
        <div className="sp-hero-word">{meta?.wort}</div>
        {meta?.hint_ru ? <div className="sp-hero-hint">{meta.hint_ru}</div> : null}
      </div>
      <div className="sp-intro">
        <p>За <b>60 секунд</b> напиши как можно больше <b>{rel.verb}</b>!</p>
        <p className="sp-intro-dim">Победитель — у кого больше правильных. Таймер пойдёт сразу ⏱</p>
      </div>
      <button className="ans-btn sp-go" onClick={start}>▶️ Старт · 60 секунд</button>
    </>
  );

  if (phase === 'playing') return shell(
    <>
      <div className="sp-top">
        <span className="sp-word">{rel.emoji} {meta?.wort}</span>
        <span className={`sp-timer${left <= 10 ? ' low' : ''}`}>{left}<span className="sp-timer-s">с</span></span>
      </div>
      <div className="sp-bar"><div className="sp-bar-fill" style={{ width: `${(left / dur) * 100}%` }} /></div>
      <div className="sp-counter">
        <span className="sp-counter-num" key={hits}>{hits}</span>
        <span className="sp-counter-sub">{hits === 1 ? rel.one : rel.verb}</span>
      </div>
      <input
        ref={inputRef} className="ans-input" value={input}
        onChange={(e) => setInput(e.target.value)}
        placeholder="пиши и жми Enter…" autoFocus autoCapitalize="off" autoCorrect="off"
        enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submitWord(); }}
      />
      <div className="sp-chips">
        {words.map((w, i) => (
          <span key={`${w.norm}-${i}`} className={`sp-chip ${w.status}`}>{w.status === 'hit' ? '✓ ' : ''}{w.text}</span>
        ))}
      </div>
      <button className="ans-btn-ghost" onClick={finish}>Готово ⏹</button>
    </>
  );

  if (phase === 'grading') return shell(
    <div className="sp-grading">
      <div className="ls-spinner" />
      <p className="ans-sub" style={{ textAlign: 'center' }}>🏁 Подводим итоги…</p>
    </div>
  );

  // done
  const r = result || {};
  const foundSet = new Set((r.found || []).map(normalizeCore));
  const place = r.ranking?.your_place;
  return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">{rel.emoji} {rel.title}</span></div>
      <div className="sp-score">
        <div className="sp-score-num">{r.count}</div>
        <div className="sp-score-sub">{r.count === 1 ? rel.one : rel.verb}{r.accepted_total ? ` · из ${r.accepted_total}` : ''}</div>
      </div>
      <SprintRanking ranking={r.ranking} />
      {(r.accepted || []).length ? (
        <div className="sp-all">
          <div className="sp-all-head">Все варианты <span className="sp-all-dim">(зелёным — что нашёл)</span>:</div>
          <div className="sp-chips">
            {(r.accepted || []).map((a, i) => (
              <span key={i} className={`sp-chip ${foundSet.has(normalizeCore(a)) ? 'hit' : 'missed'}`}>{a}</span>
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
