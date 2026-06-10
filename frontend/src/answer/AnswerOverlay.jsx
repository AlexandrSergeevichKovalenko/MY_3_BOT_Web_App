import React, { useCallback, useEffect, useMemo, useState } from 'react';
import './answer.css';

/**
 * Lightweight in-place answer overlay for in-group tasks (rebus + crossword).
 *
 * Launched as a Direct-Link Mini App from a task message button
 * (t.me/<bot>/<app>?startapp=ans_rb_<id> | ans_cw_<id>). It opens as an overlay
 * over the chat (group OR private): the user types the answer, gets a private
 * verdict in place, and closing returns them to the same spot — no DM switch,
 * no scroll. Deliberately decoupled from the heavy main App so it loads
 * instantly, and the critical path does no heavy work (the submit endpoint is a
 * handful of indexed queries, no OpenAI / images).
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    return new URLSearchParams(window.location.search).get('initData') || '';
  }
  return '';
}

// start_param: "ans_rb_123" / "ans_cw_45"
function parseStartParam(startParam) {
  const m = /^ans_(rb|cw)_(\d+)$/.exec(String(startParam || '').trim().toLowerCase());
  if (!m) return null;
  return { kind: m[1], id: Number(m[2]) };
}

function haptic(type) {
  try {
    if (type === 'ok') tg?.HapticFeedback?.notificationOccurred?.('success');
    else if (type === 'bad') tg?.HapticFeedback?.notificationOccurred?.('error');
    else tg?.HapticFeedback?.impactOccurred?.('light');
  } catch (_e) { /* ignore */ }
}

async function api(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() },
    body: JSON.stringify({ initData: getInitData(), ...body }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data?.error || 'Fehler');
  return data;
}

const arrowOf = (dir) => (dir === 'across' ? '↔' : '↕');

function RebusResult({ result }) {
  const good = !!result.is_correct;
  return (
    <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
      <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
      <div className="ans-answer">
        {good ? '' : 'Richtige Antwort: '}
        <b>{result.full_word}</b>
        {result.meaning_ru ? <span className="ans-meaning"> · {result.meaning_ru}</span> : null}
      </div>
      {!good && result.word_correct && !result.article_correct ? (
        <div className="ans-meaning" style={{ marginTop: 6 }}>Nur der Artikel war falsch.</div>
      ) : null}
      {result.explanation_ru ? <div className="ans-explain">{result.explanation_ru}</div> : null}
    </div>
  );
}

function CrosswordResult({ result }) {
  const rows = result.results || [];
  const total = result.total || rows.length;
  const correct = result.correct_count || 0;
  const allRight = total > 0 && correct === total;
  const tone = allRight ? 'ok' : (correct > 0 ? 'partial' : 'bad');
  return (
    <div className={`ans-result ${tone}`}>
      <div className="ans-verdict">
        {allRight ? `🎉 Alle ${total} richtig!` : `🏁 ${correct} / ${total} richtig`}
      </div>
      <div className="ans-cw-list">
        {rows.map((r) => (
          <div className="ans-cw-row" key={`${r.number}-${r.direction}`}>
            {r.is_correct ? '✅' : '❌'} {r.number}{arrowOf(r.direction)}: <b>{r.correct}</b>
            {!r.is_correct && r.user_answer ? <span className="mine"> (du: {r.user_answer})</span> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

export default function AnswerOverlay({ startParam }) {
  const parsed = useMemo(() => parseStartParam(startParam), [startParam]);
  const [meta, setMeta] = useState(null);
  const [metaLoading, setMetaLoading] = useState(true);
  const [error, setError] = useState('');
  const [fatal, setFatal] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState(null);
  const [rebusInput, setRebusInput] = useState('');
  const [cwInputs, setCwInputs] = useState([]);

  useEffect(() => {
    try {
      tg?.ready?.();
      tg?.expand?.();
      tg?.setHeaderColor?.('secondary_bg_color');
    } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => {
    if (!parsed) { setFatal('Ungültiger Link.'); setMetaLoading(false); return; }
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/task', { kind: parsed.kind, id: parsed.id });
        if (cancelled) return;
        setMeta(data);
        if (data.already_answered && data.result) setResult(data.result);
        if (parsed.kind === 'cw') setCwInputs((data.words || []).map(() => ''));
      } catch (e) {
        if (!cancelled) setFatal(String(e.message || e));
      } finally {
        if (!cancelled) setMetaLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [parsed]);

  const submit = useCallback(async () => {
    if (!parsed || submitting) return;
    const answer = parsed.kind === 'rb' ? rebusInput.trim() : cwInputs.map((s) => s.trim()).join(' ');
    if (!answer) return;
    haptic('light');
    setSubmitting(true);
    setError('');
    try {
      const data = await api('/api/answer/submit', { kind: parsed.kind, id: parsed.id, answer });
      if (data.needs_article) {
        setError('Bitte mit Artikel antworten — der / die / das …');
        haptic('bad');
        return;
      }
      setResult(data);
      const won = data.is_correct || (data.total && data.correct_count === data.total);
      haptic(won ? 'ok' : 'bad');
    } catch (e) {
      setError(String(e.message || e));
      haptic('bad');
    } finally {
      setSubmitting(false);
    }
  }, [parsed, submitting, rebusInput, cwInputs]);

  const close = useCallback(() => { try { tg?.close?.(); } catch (_e) { /* ignore */ } }, []);

  const isRebus = parsed?.kind === 'rb';
  const heading = isRebus ? 'Deutsches Rätsel' : 'Kreuzworträtsel';
  const eyebrow = isRebus ? '🧩 Rätsel' : '🔤 Kreuzwort';

  // Fatal (bad link / task missing) — only when we have nothing to show.
  if (fatal && !result) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
        <p className="ans-sub">{fatal}</p>
        <button className="ans-btn-ghost" onClick={close}>Schließen</button>
      </div></div>
    );
  }

  // Result view (after submit or already-answered).
  if (result) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
        </div>
        {isRebus ? <RebusResult result={result} /> : <CrosswordResult result={result} />}
        {meta?.already_answered ? <p className="ans-note">Bereits beantwortet</p> : null}
        <button className="ans-btn" onClick={close}>Schließen</button>
      </div></div>
    );
  }

  return (
    <div className="ans-root"><div className="ans-card">
      <div className="ans-head">
        <span className="ans-eyebrow">{eyebrow}</span>
        <h1 className="ans-title">{heading}</h1>
        {isRebus ? (
          <p className="ans-sub">
            Schreibe das Wort{meta?.requires_article ? ' mit Artikel' : ''}
            {meta?.letter_count ? ` — ${meta.letter_count} Buchstaben` : ''} ✍️
          </p>
        ) : (
          <p className="ans-sub">Schreibe jedes versteckte Wort ✍️</p>
        )}
      </div>

      {isRebus ? (
        <input
          className="ans-input"
          value={rebusInput}
          onChange={(e) => setRebusInput(e.target.value)}
          placeholder={meta?.requires_article ? 'z. B. das Wort …' : 'dein Wort …'}
          autoFocus
          autoCapitalize="off"
          autoCorrect="off"
          enterKeyHint="send"
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
        />
      ) : metaLoading ? (
        <>
          <div className="ans-skel sm" /><div className="ans-skel" />
          <div className="ans-skel sm" /><div className="ans-skel" />
        </>
      ) : (
        (meta?.words || []).map((w, i) => (
          <div className="ans-field" key={`${w.number}-${w.direction}`}>
            <span className="ans-field-label">
              <span className="ans-num">{w.number}{arrowOf(w.direction)}</span>
              {w.clue_de}
              {w.length ? <span className="ans-num-len">· {w.length} Bk.</span> : null}
            </span>
            <input
              className="ans-input"
              value={cwInputs[i] || ''}
              maxLength={(w.length || 0) + 4}
              autoCapitalize="characters"
              autoCorrect="off"
              onChange={(e) => setCwInputs((prev) => {
                const next = [...prev]; next[i] = e.target.value; return next;
              })}
              placeholder="…"
            />
          </div>
        ))
      )}

      {error ? <p className="ans-error">{error}</p> : null}
      <button className="ans-btn" disabled={submitting} onClick={submit}>
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>
    </div></div>
  );
}
