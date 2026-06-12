import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './answer.css';
import AnagramGame from './AnagramGame.jsx';
import ListeningGame from './ListeningGame.jsx';
import CrosswordGrid from './CrosswordGrid.jsx';
import AufgabeGame from './AufgabeGame.jsx';
import SprintGame from './SprintGame.jsx';

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

// start_param: ans_rb_123 / ans_cw_45 / ans_ag_7 / ans_ls_3 / ans_qf_9 / ans_au_2
//   ans_qfp_<poll_id> — poll-scoped freeform (button attached under the poll)
function parseStartParam(startParam) {
  const m = /^ans_(rb|cw|ag|ls|qf|qfp|sp|au)_(\d+)$/.exec(String(startParam || '').trim().toLowerCase());
  if (!m) return null;
  // qfp's id is a big Telegram poll_id → keep it a string (Number() loses precision).
  return { kind: m[1], id: m[1] === 'qfp' ? m[2] : Number(m[2]) };
}

const KIND_META = {
  rb: { eyebrow: '🧩 Rätsel', title: 'Deutsches Rätsel' },
  cw: { eyebrow: '🔤 Kreuzwort', title: 'Kreuzworträtsel' },
  ag: { eyebrow: '🔤 Anagramm', title: 'Anagramm' },
  ls: { eyebrow: '🎧 Hörverständnis', title: 'Hörverständnis · B2' },
  qf: { eyebrow: '✍️ Antwort', title: 'Eigene Antwort' },
  qfp: { eyebrow: '✍️ Antwort', title: 'Eigene Antwort' },
  au: { eyebrow: '✏️ Aufgabe', title: 'Aufgabe · B2+' },
};

// au covers several formats — show the right label per format.
const AU_LABELS = {
  cloze: { eyebrow: '✏️ Lückentext', title: 'Lückentext · B2+' },
  wortbildung: { eyebrow: '🔧 Wortbildung', title: 'Wortbildung · B2+' },
  transform: { eyebrow: '🔄 Umformung', title: 'Satztransformation · C1' },
  error: { eyebrow: '🔍 Fehler finden', title: 'Fehler finden · B2+' },
  hoerluecke: { eyebrow: '🎧 Hörlücke', title: 'Hörlücke · B2+' },
  pin: { eyebrow: '🖼 Finde im Bild', title: 'Finde im Bild · B2' },
  satzbau: { eyebrow: '🧩 Satzbau', title: 'Satzbau · B2+' },
  synonym: { eyebrow: '🔄 Synonym', title: 'Synonym · B2+' },
  antonym: { eyebrow: '↔️ Antonym', title: 'Antonym · B2+' },
};

const AU_SUBS = {
  wortbildung: 'Bilde die richtige Wortform ✍️',
  transform: 'Forme den Satz mit dem Schlüsselwort um ✍️',
  error: 'Finde und korrigiere den Fehler 🔍',
  hoerluecke: 'Höre und ergänze das fehlende Wort 🎧',
  pin: 'Tippe auf das gesuchte Objekt 🖼',
  satzbau: 'Baue den richtigen Satz 🧩',
  synonym: 'Tippe ein Synonym ✍️',
  antonym: 'Tippe das Gegenteil ✍️',
  cloze: 'Setze das fehlende Wort ein ✍️',
};

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

function AnagramResult({ result }) {
  const good = !!result.is_correct;
  return (
    <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
      <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
      <div className="ans-answer">
        {good ? '' : 'Richtiges Wort: '}
        <b>{result.correct_word}</b>
        {result.hint_ru ? <span className="ans-meaning"> · {result.hint_ru}</span> : null}
      </div>
      {result.explanation ? <div className="ans-explain">{result.explanation}</div> : null}
      {result.tip ? <div className="ans-tip">💡 {result.tip}</div> : null}
    </div>
  );
}

// LCS word alignment for the sentence diff (case/punctuation-insensitive match,
// original tokens kept for display).
function wordDiff(userText, correctText) {
  const norm = (t) => t.toLowerCase().replace(/[.,!?;:»«"'„“”()]/g, '');
  const a = (userText || '').trim().split(/\s+/).filter(Boolean);
  const b = (correctText || '').trim().split(/\s+/).filter(Boolean);
  const na = a.map(norm), nb = b.map(norm);
  const n = na.length, m = nb.length;
  const dp = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--)
    for (let j = m - 1; j >= 0; j--)
      dp[i][j] = na[i] === nb[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
  const aM = new Set(), bM = new Set();
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (na[i] === nb[j]) { aM.add(i); bM.add(j); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) i++; else j++;
  }
  return { a, b, aM, bM };
}

function AufgabeResult({ result }) {
  const [saved, setSaved] = useState(() => new Set());
  const saveChip = useCallback(async (de, ru) => {
    if (!de || saved.has(de)) return;
    try {
      await api('/api/webapp/dictionary/save', {
        source_text: de, target_text: ru || '',
        source_lang: 'de', target_lang: 'ru', direction: 'de_to_ru',
        origin_process: 'synonym_save',
      });
      setSaved((s) => new Set(s).add(de));
      haptic('ok');
    } catch (_e) { haptic('bad'); }
  }, [saved]);
  const good = !!result.is_correct;
  const correct = result.correct_word || '';
  const mine = result.user_answer || '';
  const showDiff = !good && result.is_sentence && !!mine && !!correct;
  const diff = showDiff ? wordDiff(mine, correct) : null;
  const saveable = result.saveable || [];
  return (
    <div className={`ans-result ${good ? 'ok' : 'bad'}`}>
      <div className="ans-verdict">{good ? '✅ Richtig!' : '❌ Falsch'}</div>
      {good ? (
        <div className="ans-answer"><b>{correct}</b></div>
      ) : showDiff ? (
        <div className="au-diff">
          <div className="au-diff-label">✅ Richtiger Satz</div>
          <div className="au-diff-correct">
            {diff.b.map((w, idx) => (
              <span key={idx} className={diff.bM.has(idx) ? 'au-tok' : 'au-tok au-tok-fix'}>{w} </span>
            ))}
          </div>
          <div className="au-diff-label dim">✍️ Dein Satz</div>
          <div className="au-diff-yours">
            {diff.a.map((w, idx) => (
              <span key={idx} className={diff.aM.has(idx) ? 'au-tok' : 'au-tok au-tok-bad'}>{w} </span>
            ))}
          </div>
        </div>
      ) : (
        <div className="ans-answer">
          Richtige Antwort: <b>{correct}</b>
          {result.hint_ru ? <span className="ans-meaning"> · {result.hint_ru}</span> : null}
        </div>
      )}
      {good && result.hint_ru ? <div className="ans-meaning">{result.hint_ru}</div> : null}
      {saveable.length ? (
        <div className="sp-all">
          <div className="sp-all-head">
            {result.format === 'antonym' ? 'Антонимы' : 'Синонимы'}{' '}
            <span className="sp-all-dim">(👆 нажми, чтобы сохранить в словарь)</span>:
          </div>
          <div className="sp-chips">
            {saveable.map((a, i) => {
              const de = (a && a.de) || '';
              const ru = (a && a.ru) || '';
              const isSaved = saved.has(de);
              return (
                <button key={i} type="button" className={`sp-chip tap ${isSaved ? 'saved' : ''}`} onClick={() => saveChip(de, ru)}>
                  {isSaved ? '💾 ' : ''}{de}
                </button>
              );
            })}
          </div>
        </div>
      ) : null}
      {result.explanation ? <div className="ans-explain">{result.explanation}</div> : null}
      {result.tip ? <div className="ans-tip">💡 {result.tip}</div> : null}
    </div>
  );
}

function ListeningResult({ result }) {
  const items = result.items || [];
  const total = result.total || items.length;
  const correct = result.correct_count || 0;
  const allRight = total > 0 && correct === total;
  const tone = allRight ? 'ok' : (correct > 0 ? 'partial' : 'bad');
  return (
    <div className={`ans-result ${tone}`}>
      <div className="ans-verdict">
        {allRight ? `🎉 Alle ${total} richtig!` : `🏁 ${correct} / ${total} richtig`}
      </div>
      <div className="ls-result-list">
        {items.map((it) => (
          <div className="ls-result-item" key={it.number}>
            <div className="ls-result-q">{it.content_correct ? '✅' : '❌'} {it.number}. {it.question_de}</div>
            {it.user_answer ? <div className="ls-result-you">Du: {it.user_answer}</div> : null}
            {!it.content_correct && it.correct_answer_de ? (
              <div className="ls-result-correct">Korrekt: <b>{it.correct_answer_de}</b></div>
            ) : null}
            {it.content_feedback_ru ? <div className="ans-meaning">{it.content_feedback_ru}</div> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function RankingCard({ ranking }) {
  if (!ranking || !ranking.total) return null;
  const { is_correct, total_correct, pct_correct, your_place, your_time_ms, your_name, top3 } = ranking;
  const fmt = (ms) => `${(ms / 1000).toFixed(1)} с`;
  const medal = (p) => (p === 1 ? '🥇' : p === 2 ? '🥈' : p === 3 ? '🥉' : `#${p}`);

  if (!is_correct) {
    return (
      <div className="rank-card">
        <div className="rank-head">📊 {pct_correct}% ответили верно</div>
        <div className="rank-sub">В следующий раз получится! 💪</div>
      </div>
    );
  }

  const inTop3 = your_place && your_place <= 3;
  return (
    <div className="rank-card">
      <div className="rank-trophy">{your_place === 1 ? '🏆' : '⚡'}</div>
      <div className="rank-head">Место {your_place} из {total_correct} · {fmt(your_time_ms)}</div>
      <div className="rank-list">
        {(top3 || []).map((r) => (
          <div className={`rank-row${r.place === your_place ? ' me' : ''}`} key={r.place}>
            <span className="rank-medal">{medal(r.place)}</span>
            <span className="rank-name">{r.name || '—'}</span>
            <span className="rank-time">{fmt(r.time_ms)}</span>
          </div>
        ))}
        {!inTop3 && your_place ? (
          <>
            <div className="rank-dots">⋮</div>
            <div className="rank-row me">
              <span className="rank-medal">#{your_place}</span>
              <span className="rank-name">{your_name || 'Du'}</span>
              <span className="rank-time">{fmt(your_time_ms)}</span>
            </div>
          </>
        ) : null}
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
  const [grading, setGrading] = useState(false);
  const taskLoadedAt = useRef(0); // for the speed ranking (load → submit)

  useEffect(() => {
    try {
      tg?.ready?.();
      tg?.expand?.();
      tg?.setHeaderColor?.('secondary_bg_color');
    } catch (_e) { /* ignore */ }
    // Respect the user's Telegram light/dark scheme (default dark).
    const applyScheme = () => {
      const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
      try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
    };
    applyScheme();
    try { tg?.onEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ }
    return () => { try { tg?.offEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ } };
  }, []);

  useEffect(() => {
    if (!parsed) { setFatal('Ungültiger Link.'); setMetaLoading(false); return; }
    if (parsed.kind === 'sp') { setMetaLoading(false); return; }  // SprintGame loads itself
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/task', { kind: parsed.kind, id: parsed.id });
        if (cancelled) return;
        setMeta(data);
        taskLoadedAt.current = Date.now(); // start the speed clock
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

  const submit = useCallback(async (answerOverride) => {
    if (!parsed || submitting) return;
    const answer = answerOverride != null
      ? String(answerOverride)
      : parsed.kind === 'rb' ? rebusInput.trim() : cwInputs.map((s) => s.trim()).join(' ');
    if (!answer) return;
    haptic('light');
    setSubmitting(true);
    setError('');
    try {
      const time_ms = taskLoadedAt.current ? Date.now() - taskLoadedAt.current : 0;
      const data = await api('/api/answer/submit', { kind: parsed.kind, id: parsed.id, answer, time_ms });
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

  // Listening: submit kicks off async LLM grading; poll until it's done.
  const pollListening = useCallback(() => {
    let tries = 0;
    const tick = async () => {
      tries += 1;
      try {
        const s = await api('/api/answer/listening_status', { id: parsed.id });
        if (s.status === 'done' && s.result) {
          setGrading(false);
          setResult(s.result);
          haptic(s.result.correct_count === s.result.total ? 'ok' : 'bad');
          return;
        }
        if (s.status === 'failed') {
          setGrading(false);
          setError('Auswertung fehlgeschlagen. Bitte versuche es erneut.');
          return;
        }
      } catch (_e) { /* keep polling */ }
      if (tries < 30) setTimeout(tick, 2000);
      else { setGrading(false); setError('Zeitüberschreitung. Bitte versuche es erneut.'); }
    };
    setTimeout(tick, 2000);
  }, [parsed]);

  const submitListening = useCallback(async (answers) => {
    if (!parsed || submitting || grading) return;
    haptic('light');
    setSubmitting(true);
    setError('');
    try {
      const time_ms = taskLoadedAt.current ? Date.now() - taskLoadedAt.current : 0;
      const data = await api('/api/answer/submit', { kind: 'ls', id: parsed.id, answers, time_ms });
      if (data.items) { // already graded (anti-replay) → direct result
        setResult(data);
        haptic(data.correct_count === data.total ? 'ok' : 'bad');
      } else if (data.status === 'pending') {
        setGrading(true);
        pollListening();
      } else if (data.status === 'failed') {
        setError('Auswertung fehlgeschlagen. Bitte versuche es erneut.');
        haptic('bad');
      }
    } catch (e) {
      setError(String(e.message || e));
      haptic('bad');
    } finally {
      setSubmitting(false);
    }
  }, [parsed, submitting, grading, pollListening]);

  const close = useCallback(() => { try { tg?.close?.(); } catch (_e) { /* ignore */ } }, []);

  const kind = parsed?.kind;
  // Synonym/Antonym sprint is fully self-contained (own timer/check/finish flow).
  if (kind === 'sp' && parsed?.id != null) {
    return <SprintGame id={parsed.id} api={api} haptic={haptic} onClose={close} />;
  }
  const isRebus = kind === 'rb';
  const isAnagram = kind === 'ag';
  const isListening = kind === 'ls';
  const isFreeform = kind === 'qf' || kind === 'qfp';
  const isCrossword = kind === 'cw';
  const isAufgabe = kind === 'au';
  let { eyebrow, title: heading } = KIND_META[kind] || KIND_META.rb;
  if (isAufgabe && meta?.format && AU_LABELS[meta.format]) {
    eyebrow = AU_LABELS[meta.format].eyebrow;
    heading = AU_LABELS[meta.format].title;
  }

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
        {isRebus ? <RebusResult result={result} />
          : isAufgabe ? <AufgabeResult result={result} />
          : (isAnagram || isFreeform) ? <AnagramResult result={result} />
          : isListening ? <ListeningResult result={result} />
          : <CrosswordResult result={result} />}
        {result.ranking ? <RankingCard ranking={result.ranking} /> : null}
        {meta?.already_answered ? <p className="ans-note">Bereits beantwortet</p> : null}
        <button className="ans-btn" onClick={close}>Schließen</button>
      </div></div>
    );
  }

  // Listening input view — custom audio player + answer fields; async grading.
  if (isListening) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
          {meta?.topic ? <p className="ans-sub">📌 {meta.topic}</p> : null}
        </div>
        {grading ? (
          <div className="ls-grading">
            <div className="ls-spinner" />
            <p className="ans-sub" style={{ textAlign: 'center' }}>
              Werte deine Antworten aus … <br />(10–20&nbsp;Sekunden)
            </p>
          </div>
        ) : metaLoading || !meta ? (
          <><div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" /></>
        ) : (
          <ListeningGame task={meta} onSubmit={submitListening} submitting={submitting} />
        )}
        {error ? <p className="ans-error">{error}</p> : null}
      </div></div>
    );
  }

  // Aufgabe input view — B2+ text task (cloze: sentence with a gap + text input).
  if (isAufgabe) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
          <p className="ans-sub">{AU_SUBS[meta?.format] || AU_SUBS.cloze}</p>
        </div>
        {metaLoading || !meta ? (
          <><div className="ans-skel" /><div className="ans-skel sm" /></>
        ) : (
          <AufgabeGame task={meta} onSubmit={(a) => submit(a)} submitting={submitting} />
        )}
        {error ? <p className="ans-error">{error}</p> : null}
      </div></div>
    );
  }

  // Crossword input view — interactive grid + on-screen keyboard.
  if (isCrossword) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
          {meta?.topic ? <p className="ans-sub">📌 {meta.topic} — fülle die leeren Felder ✍️</p> : null}
        </div>
        {metaLoading || !meta || !meta.grid ? (
          <><div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" /></>
        ) : (
          <CrosswordGrid task={meta} onSubmit={(a) => submit(a)} submitting={submitting} />
        )}
        {error ? <p className="ans-error">{error}</p> : null}
      </div></div>
    );
  }

  // Freeform input view — a single text field (the "keine korrekte Antworten" path).
  if (isFreeform) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
          <p className="ans-sub">
            {meta?.hint_ru ? <>Подсказка: <b style={{ color: 'var(--dmps-text-primary, #F8FAFC)' }}>{meta.hint_ru}</b> — </> : null}
            schreib die richtige Antwort ✍️
          </p>
        </div>
        <input
          className="ans-input"
          value={rebusInput}
          onChange={(e) => setRebusInput(e.target.value)}
          placeholder="dein Wort …"
          autoFocus
          autoCapitalize="off"
          autoCorrect="off"
          enterKeyHint="send"
          onKeyDown={(e) => { if (e.key === 'Enter') submit(rebusInput.trim()); }}
        />
        {error ? <p className="ans-error">{error}</p> : null}
        <button className="ans-btn" disabled={submitting || !rebusInput.trim()} onClick={() => submit(rebusInput.trim())}>
          {submitting ? 'Prüfe …' : 'Prüfen ✓'}
        </button>
      </div></div>
    );
  }

  // Anagram input view — its own slots/tiles UI with a built-in Prüfen button.
  if (isAnagram) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">{eyebrow}</span>
          <h1 className="ans-title">{heading}</h1>
          <p className="ans-sub">
            Подсказка: <b style={{ color: 'var(--dmps-text-primary, #F8FAFC)' }}>{meta?.hint_ru || '…'}</b>
            {meta?.length ? ` · ${meta.length} Buchstaben` : ''} 🧩
          </p>
        </div>
        {metaLoading || !meta ? (
          <><div className="ans-skel" /><div className="ans-skel sm" /></>
        ) : (
          <AnagramGame task={meta} onSubmit={(a) => submit(a)} submitting={submitting} error={error} />
        )}
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
      <button className="ans-btn" disabled={submitting} onClick={() => submit()}>
        {submitting ? 'Prüfe …' : 'Prüfen ✓'}
      </button>
    </div></div>
  );
}
