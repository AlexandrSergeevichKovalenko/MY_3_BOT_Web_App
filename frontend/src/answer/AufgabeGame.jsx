import React, { useCallback, useMemo, useRef, useState } from 'react';
import { pointerFraction } from './pointerBox.js';

/**
 * B2+ text tasks ("Aufgabe"), all answered in place. Grading is a fast
 * deterministic check on the server (the generator pre-listed every accepted
 * answer at pool time — no runtime LLM). Formats:
 *  - cloze / wortbildung / transform → typed text (AufgabeText)
 *  - error  → tap the wrong word + type the fix (AufgabeError)
 *  - hoerluecke → listen + type the missing word (AufgabeHoer)
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function tapHaptic() { try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* ignore */ } }

function gapSentence(satz) {
  const parts = String(satz || '').split('_____');
  if (parts.length <= 1) return satz;
  return parts.flatMap((p, i) => (i === 0 ? [p] : [<span className="au-gap" key={i}>＿＿＿</span>, p]));
}

function PrüfenButton({ disabled, submitting, onClick }) {
  return (
    <button className="ans-btn" disabled={disabled || submitting} onClick={onClick}>
      {submitting ? 'Prüfe …' : 'Prüfen ✓'}
    </button>
  );
}

function AufgabeText({ task, onSubmit, submitting }) {
  const [value, setValue] = useState('');
  const fmt = task.format;
  const submit = () => { const v = value.trim(); if (v) onSubmit(v); };

  let body;
  let placeholder = 'fehlendes Wort …';
  if (fmt === 'transform') {
    placeholder = `2–5 Wörter mit „${task.schluesselwort || ''}“`;
    body = (
      <>
        <div className="au-orig">{task.original}</div>
        <div className="au-key">🔑 Schlüsselwort: <b>{task.schluesselwort}</b></div>
        <div className="au-satz">
          {task.target_prefix ? <>{task.target_prefix} </> : null}
          <span className="au-gap">＿＿＿</span>
          {task.target_suffix ? <> {task.target_suffix}</> : null}
        </div>
      </>
    );
  } else if (fmt === 'wortbildung') {
    // The gap is NOT one word: the noun plus the article/preposition that links it
    // on ("Lieferung der", "Interesse an der") — say so, or people type only the
    // noun and get marked wrong for a rule they were never told.
    placeholder = 'Nomen + Artikel/Präposition …';
    // Only the Stamm hint is Russian (the stem's RU meaning); everything else
    // stays German.
    body = (
      <>
        <div className="au-satz">{gapSentence(task.satz)}</div>
        <div className="au-key">🔧 Stamm: <b>{task.stamm_ru || task.stamm}</b></div>
      </>
    );
  } else if (fmt === 'wortgruppe') {
    placeholder = 'ganze Wortgruppe (mit Präposition) …';
    const lemmas = task.lemmas || [];
    body = (
      <>
        <div className="au-satz">{gapSentence(task.satz)}</div>
        {lemmas.length ? (
          <div className="au-lemmas">
            <span className="au-lemmas-label">Benutze:</span>
            {lemmas.map((l, i) => <span className="au-lemma" key={i}>{l}</span>)}
            {task.tense ? <span className="au-lemma au-lemma-tense">{task.tense}</span> : null}
          </div>
        ) : null}
      </>
    );
  } else if (fmt === 'synonym' || fmt === 'antonym') {
    placeholder = fmt === 'antonym' ? 'Gegenteil eintippen …' : 'Synonym eintippen …';
    body = (
      <div className="au-syn">
        <span className="au-syn-label">{fmt === 'antonym' ? '↔️ Antonym zu' : '🔄 Synonym zu'}</span>
        <div className="au-syn-word">{task.wort}</div>
      </div>
    );
  } else {
    body = <div className="au-satz">{gapSentence(task.satz)}</div>;
  }

  return (
    <>
      {body}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <input
        className="ans-input" value={value} onChange={(e) => setValue(e.target.value)}
        placeholder={placeholder} autoFocus autoCapitalize="off" autoCorrect="off" enterKeyHint="send"
        onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
      />
      <PrüfenButton disabled={!value.trim()} submitting={submitting} onClick={submit} />
    </>
  );
}

const CIRCLED = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨'];
const circled = (n) => CIRCLED[n - 1] || `(${n})`;

function AufgabeError({ task, onSubmit, submitting }) {
  // A sentence may hold 1–3 errors; the learner decides how many. Tap every word you
  // think is wrong (each gets a number in tap order), then type its correction in the
  // matching row. The count is NOT revealed — figuring it out is part of the task, so
  // we intentionally show no "how many errors" hint here. Submit = "i1|c1;i2|c2" in tap
  // order (a single tap → "i|c", still accepted by the grader).
  const woerter = task.woerter || [];
  const [picks, setPicks] = useState([]);   // selected indices, in tap order
  const [fixes, setFixes] = useState({});   // index -> correction text
  const toggle = (i) => {
    tapHaptic();
    if (picks.includes(i)) {
      setPicks((prev) => prev.filter((x) => x !== i));
      setFixes((f) => { const n = { ...f }; delete n[i]; return n; });
    } else {
      setPicks((prev) => [...prev, i]);
      setFixes((f) => (i in f ? f : { ...f, [i]: '' }));
    }
  };
  const setFix = (i, v) => setFixes((f) => ({ ...f, [i]: v }));
  const ready = picks.length > 0 && picks.every((i) => String(fixes[i] || '').trim());
  const submit = () => {
    if (!ready) return;
    onSubmit(picks.map((i) => `${i}|${String(fixes[i]).trim()}`).join(';'));
  };
  return (
    <>
      <p className="au-hint">Tippe <b>jedes falsche</b> Wort an und korrigiere es:</p>
      <div className="au-words">
        {woerter.map((w, i) => {
          const n = picks.indexOf(i);
          return (
            <button
              key={i} type="button"
              className={`au-word${n >= 0 ? ' picked' : ''}`}
              onClick={() => toggle(i)} disabled={submitting}
            >
              {n >= 0 ? <span className="au-word-badge">{circled(n + 1)}</span> : null}
              {w}
            </button>
          );
        })}
      </div>
      {picks.length ? (
        <div className="au-fixes ans-body">
          {picks.map((i, n) => (
            <div className="au-fix-row" key={i}>
              <span className="au-fix-badge">{circled(n + 1)}</span>
              <span className="au-fix-word">{woerter[i]}</span>
              <span className="au-fix-arrow">→</span>
              <input
                className="au-fix-input" value={fixes[i] || ''}
                onChange={(e) => setFix(i, e.target.value)}
                placeholder="richtige Form …"
                autoFocus={n === picks.length - 1}
                autoCapitalize="off" autoCorrect="off"
                enterKeyHint={n === picks.length - 1 ? 'send' : 'next'}
                onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
                disabled={submitting}
              />
              <button
                type="button" className="au-fix-remove" aria-label="entfernen"
                onClick={() => toggle(i)} disabled={submitting}
              >×</button>
            </div>
          ))}
        </div>
      ) : (
        <p className="au-hint au-fix-empty">☝️ Tippe ein oder mehrere Wörter an.</p>
      )}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <PrüfenButton disabled={!ready} submitting={submitting} onClick={submit} />
    </>
  );
}

function fmtTime(s) {
  if (!Number.isFinite(s) || s < 0) return '0:00';
  const m = Math.floor(s / 60), sec = Math.floor(s % 60);
  return `${m}:${sec < 10 ? '0' : ''}${sec}`;
}

const MAX_AUDIO_PLAYS = 2;  // fairness: a Hörlücke clip may be played only twice

function AufgabeHoer({ task, onSubmit, submitting }) {
  const audioRef = useRef(null);
  const [playing, setPlaying] = useState(false);
  const [cur, setCur] = useState(0);
  const [dur, setDur] = useState(0);
  const [plays, setPlays] = useState(0);
  const exhausted = plays >= MAX_AUDIO_PLAYS;
  const hasAudio = !!task.audio_url;
  const transcript = task.transcript || '';
  const isMulti = !!transcript;
  const segments = isMulti ? transcript.split('_____') : null;
  const gapN = isMulti ? Math.max(1, segments.length - 1) : 1;
  const gapWords = Array.isArray(task.gap_words) ? task.gap_words : [];
  const [vals, setVals] = useState(() => Array(gapN).fill(''));
  const setVal = (i, v) => setVals((prev) => { const n = [...prev]; n[i] = v; return n; });
  const allFilled = vals.every((v) => v.trim());

  const toggle = useCallback(() => {
    const a = audioRef.current; if (!a) return; tapHaptic();
    if (!a.paused) { a.pause(); return; }
    // A "fresh" start (from the beginning or after the clip ended) consumes one
    // of the limited plays; resuming after a mid-clip pause is free.
    const fresh = a.ended || a.currentTime < 0.15;
    if (fresh) {
      if (exhausted) return;
      setPlays((p) => p + 1);
      if (a.ended) { try { a.currentTime = 0; } catch (_e) { /* ignore */ } }
    }
    a.play().catch(() => {});
  }, [exhausted]);
  const seek = useCallback((e) => {
    const a = audioRef.current; if (!a || !dur) return;
    a.currentTime = pointerFraction(e.currentTarget, e.clientX, e.clientY).x * dur;
  }, [dur]);
  const submit = () => { if (allFilled) onSubmit(vals.map((v) => v.trim()).join('|')); };
  const pct = dur > 0 ? (cur / dur) * 100 : 0;

  return (
    <>
      <audio ref={audioRef} src={task.audio_url || undefined} preload="metadata"
        onLoadedMetadata={(e) => setDur(e.currentTarget.duration || 0)}
        onTimeUpdate={(e) => setCur(e.currentTarget.currentTime || 0)}
        onPlay={() => setPlaying(true)} onPause={() => setPlaying(false)} onEnded={() => setPlaying(false)} />
      <div className="ls-player">
        {hasAudio ? (
          <>
            <button className="ls-play" onClick={toggle} disabled={exhausted && !playing} aria-label="Play">{playing ? '❚❚' : '▶'}</button>
            <div className="ls-player-main">
              <div className="ls-bar" onClick={seek}>
                <div className="ls-bar-fill" style={{ width: `${pct}%` }} />
                <div className="ls-bar-knob" style={{ left: `${pct}%` }} />
              </div>
              <div className="ls-row">
                <span className="ls-time">{fmtTime(cur)} / {fmtTime(dur)}</span>
                <span className="ls-plays">
                  {exhausted ? (playing ? '🎧 letzte Wiedergabe' : '🔇 aufgebraucht')
                    : `🎧 noch ${MAX_AUDIO_PLAYS - plays}×`}
                </span>
              </div>
            </div>
          </>
        ) : <span className="ls-noaudio">🔇 Audio wird vorbereitet — gleich nochmal.</span>}
      </div>
      {isMulti ? (
        <div className="au-satz au-cloze ans-body">
          {segments.flatMap((seg, i) => {
            const nodes = [<span key={`s${i}`}>{seg}</span>];
            if (i < segments.length - 1) {
              // Gaps hide word GROUPS (2–4 words), so the field grows with the text and
              // says up front how many words are missing — otherwise the learner has to
              // guess the shape of the task instead of listening.
              const need = gapWords[i] || 0;
              const typed = vals[i] || '';
              nodes.push(
                <input key={`g${i}`} className="au-gap-input" value={typed}
                  onChange={(e) => setVal(i, e.target.value)}
                  size={Math.min(24, Math.max(need ? need * 7 : 10, typed.length + 2))}
                  autoCapitalize="off" autoCorrect="off"
                  placeholder={need > 1 ? `${need} Wörter …` : '…'} />
              );
            }
            return nodes;
          })}
        </div>
      ) : (
        <>
          <div className="au-satz">{gapSentence(task.satz_luecke)}</div>
          <input className="ans-input" value={vals[0] || ''} onChange={(e) => setVal(0, e.target.value)}
            placeholder="gehörtes Wort …" autoCapitalize="off" autoCorrect="off" enterKeyHint="send"
            onKeyDown={(e) => { if (e.key === 'Enter') submit(); }} />
        </>
      )}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <PrüfenButton disabled={!allFilled} submitting={submitting} onClick={submit} />
    </>
  );
}

function AufgabePin({ task, onSubmit, submitting }) {
  // Два нажатия и всё: ткнуть в предмет на картинке → выбрать артикль → «Prüfen».
  //
  // Поля ввода здесь НЕТ намеренно. Пока артикль вписывали руками, проверка сравнивала
  // введённое со словом «der/die/das», и совершенно правильный ответ «die Lederschnur»
  // засчитывался как ошибка. Так сделает любой — значит виновата не невнимательность, а
  // сам способ ответа. Кнопки эту развилку убирают совсем, а заодно на этом экране больше
  // не нужна клавиатура: карточку не приходится перестраивать под неё.
  const [tap, setTap] = useState(null); // normalized {x,y}
  const [article, setArticle] = useState('');
  const hasImg = !!task.image_url;
  const needsArticle = !!task.needs_article;
  // Доли считаем через pointerBox: карточку подгонка масштабирует через `zoom`, а в WebKit
  // (движок мини-аппа на iPhone) getBoundingClientRect отдаёт координаты БЕЗ учёта zoom —
  // из-за этого метка уезжала от места тапа. Подробности и замер — в pointerBox.js.
  const onImgClick = (e) => {
    setTap(pointerFraction(e.currentTarget, e.clientX, e.clientY));
    tapHaptic();
  };
  const ready = tap && (!needsArticle || article);
  const submit = () => {
    if (!ready) return;
    const coords = `${tap.x.toFixed(4)},${tap.y.toFixed(4)}`;
    onSubmit(needsArticle ? `${coords}|${article}` : coords);
  };
  return (
    <>
      <p className="au-question">{task.question_de}</p>
      {hasImg ? (
        <div className="pin-wrap" onClick={onImgClick}>
          <img className="pin-img" src={task.image_url} alt="" draggable="false" />
          {tap ? <span className="pin-marker" style={{ left: `${tap.x * 100}%`, top: `${tap.y * 100}%` }} /> : null}
        </div>
      ) : <div className="au-orig">🖼 Bild wird vorbereitet — gleich nochmal.</div>}
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      {needsArticle ? (
        <div className="pin-articles">
          {['der', 'die', 'das'].map((a) => (
            <button
              key={a} type="button"
              className={`pin-art-btn ${ART_CLASS[a]}${article === a ? ' on' : ''}`}
              onClick={() => { setArticle(a); tapHaptic(); }} disabled={submitting}
            >{a}</button>
          ))}
        </div>
      ) : null}
      <PrüfenButton disabled={!ready} submitting={submitting} onClick={submit} />
    </>
  );
}

function AufgabeSatzbau({ task, onSubmit, submitting }) {
  const tiles = useMemo(() => (task.tiles || []).map((w, i) => ({ id: i, word: w })), [task.tiles]);
  const [placed, setPlaced] = useState([]);
  const placedIds = new Set(placed.map((t) => t.id));
  const pool = tiles.filter((t) => !placedIds.has(t.id));
  const allPlaced = tiles.length > 0 && placed.length === tiles.length;
  const submit = () => { if (allPlaced) onSubmit(placed.map((t) => t.word).join(' ')); };
  return (
    <>
      <p className="au-question">Собери предложение из слов:</p>
      <div className="sb-answer">
        {placed.length ? placed.map((t) => (
          <button className="au-word picked" key={t.id} onClick={() => setPlaced((p) => p.filter((x) => x.id !== t.id))}>{t.word}</button>
        )) : <span className="sb-ph">нажимай слова по порядку …</span>}
      </div>
      <div className="sb-pool">
        {pool.map((t) => (
          <button className="au-word" key={t.id} onClick={() => { setPlaced((p) => [...p, t]); tapHaptic(); }}>{t.word}</button>
        ))}
      </div>
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <PrüfenButton disabled={!allPlaced} submitting={submitting} onClick={submit} />
    </>
  );
}

const ADJ_ENDINGS = ['e', 'en', 'er', 'es', 'em'];

function AufgabeAdjektiv({ task, onSubmit, submitting }) {
  const [pick, setPick] = useState(null);
  const submit = () => { if (pick) onSubmit(pick); };
  return (
    <>
      <div className="au-satz au-adj">
        <span>{task.before}</span>
        <span className={`au-adj-slot${pick ? ' filled' : ''}`}>{pick ? `-${pick}` : '·'}</span>
        <span>{task.after}</span>
      </div>
      <div className="au-adj-opts">
        {ADJ_ENDINGS.map((e) => (
          <button
            key={e} type="button"
            className={`au-adj-key${pick === e ? ' on' : ''}`}
            onClick={() => { setPick(e); tapHaptic(); }} disabled={submitting}
          >-{e}</button>
        ))}
      </div>
      {task.hint_ru ? <p className="au-hint">💡 {task.hint_ru}</p> : null}
      <PrüfenButton disabled={!pick} submitting={submitting} onClick={submit} />
    </>
  );
}

const ART_CLASS = { der: 'art-der', die: 'art-die', das: 'art-das' };

function AufgabeArtikel({ task, onSubmit, submitting }) {
  // der/die/das review — the Artikel Trainer template: photo of the word + colour-coded
  // der(blue)/die(red)/das(green). One tap = instant answer.
  const options = task.options && task.options.length ? task.options : ['der', 'die', 'das'];
  const [pick, setPick] = useState(null);
  const choose = (a) => {
    if (submitting || pick) return;  // lock after the first tap (submit in flight)
    setPick(a); tapHaptic(); onSubmit(a);
  };
  return (
    <>
      {task.image ? (
        <div className="al-img"><img src={task.image} alt="" loading="eager" /></div>
      ) : null}
      <div className={`as-word ${pick ? pick : ''}`}>
        <span className="al-word-text">{task.wort}</span>
      </div>
      {task.hint_ru ? <p className="au-hint" style={{ textAlign: 'center' }}>{task.hint_ru}</p> : null}
      <div className="as-buttons">
        {options.map((a) => (
          <button
            key={a} type="button"
            className={`as-btn-art ${ART_CLASS[a] || ''}${pick === a ? ' on' : ''}`}
            onClick={() => choose(a)} disabled={submitting || !!pick}
          >{a}</button>
        ))}
      </div>
    </>
  );
}

function wfSplitBlank(s) {
  const str = String(s || '');
  const i = str.indexOf('___');
  if (i < 0) return ['', str];
  return [str.slice(0, i), str.slice(i + 3)];
}

function AufgabeWoFrage({ task, onSubmit, submitting }) {
  // Wo-Frage Sprint review (mixed "Alle" flow): the Q&A sentence with the "___" blank +
  // the 4 question-word options. One tap = instant answer (reveal shown by the caller).
  const [pick, setPick] = useState(null);
  const [pre, post] = wfSplitBlank(task.s);
  const choose = (o) => {
    if (submitting || pick) return;  // lock after the first tap
    setPick(o); tapHaptic(); onSubmit(o);
  };
  return (
    <>
      <div className={`as-word wo-word${pick ? ' picked' : ''}`}>
        <span className="fit-line wo-line">
          <span>{pre}</span>
          <span className="wo-slot">{pick || '?'}</span>
          <span>{post}</span>
        </span>
      </div>
      {task.clue ? <div className="wo-clue">{task.clue}</div> : null}
      {task.hint_ru ? <p className="au-hint" style={{ textAlign: 'center' }}>💡 {task.hint_ru}</p> : null}
      <div className="as-buttons wo-buttons">
        {(task.opts || []).map((o) => (
          <button
            key={o} type="button"
            className={`as-btn-art wo-opt${pick === o ? ' on' : ''}`}
            onClick={() => choose(o)} disabled={submitting || !!pick}
          >{o}</button>
        ))}
      </div>
    </>
  );
}

function AufgabeVideo({ task, onSubmit, submitting }) {
  // Remedial theory card ("тебе было сложно"): a PICKER of curated clips for the topic.
  // The learner taps whichever explanation they like (it plays inline), can open several,
  // then «Посмотрел»/«Пропустить» consumes the card (server marks it done — no grading).
  const videos = Array.isArray(task.videos) && task.videos.length
    ? task.videos
    : (task.video_id ? [{ video_id: task.video_id, video_title: task.video_title }] : []);
  const [active, setActive] = useState(null);   // video_id currently playing inline
  const [chosen, setChosen] = useState(null);
  const topic = task.topic_ru || task.topic_de || 'этой игры';
  const act = (signal) => {
    if (submitting || chosen) return;
    setChosen(signal); tapHaptic(); onSubmit(signal);
  };
  const open = (id) => { tapHaptic(); setActive(id); };
  return (
    <>
      <div className="au-video-intro">
        <p className="au-hint" style={{ textAlign: 'center', marginBottom: 6 }}>
          Вчера тема <b>{topic}</b> далась тяжело — не страшно, она правда неочевидная.
        </p>
        <p className="au-hint" style={{ textAlign: 'center' }}>
          Выбери видео, которое тебе больше зайдёт 👇 Посмотри и завтра сделай ещё попытку 💪
        </p>
      </div>
      <div className="au-video-list">
        {videos.map((v) => {
          const id = String(v.video_id || '');
          if (!id) return null;
          if (active === id) {
            return (
              <div className="au-video-frame" key={id}>
                <iframe
                  title={v.video_title || 'YouTube'}
                  src={`https://www.youtube.com/embed/${id}?rel=0&modestbranding=1&playsinline=1&autoplay=1`}
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                  allowFullScreen
                />
                {v.video_title ? <div className="au-video-caption">{v.video_title}</div> : null}
              </div>
            );
          }
          return (
            <button type="button" className="au-video-pick" key={id} onClick={() => open(id)}>
              <span className="au-video-thumb-wrap">
                <img
                  className="au-video-thumb" loading="lazy" alt=""
                  src={`https://i.ytimg.com/vi/${id}/hqdefault.jpg`}
                />
                <span className="au-video-play">▶</span>
              </span>
              {v.video_title ? <span className="au-video-pick-title">{v.video_title}</span> : null}
            </button>
          );
        })}
      </div>
      <button className="ans-btn" disabled={submitting} onClick={() => act('__watched__')}>
        Посмотрел ✅
      </button>
      <button className="ans-btn-ghost" disabled={submitting} onClick={() => act('__skip__')}>
        Пропустить
      </button>
    </>
  );
}

export default function AufgabeGame({ task, onSubmit, submitting }) {
  const fmt = task.format || 'cloze';
  if (fmt === 'video') return <AufgabeVideo task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'error') return <AufgabeError task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'hoerluecke') return <AufgabeHoer task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'pin') return <AufgabePin task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'satzbau') return <AufgabeSatzbau task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'adjektiv') return <AufgabeAdjektiv task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'artikel') return <AufgabeArtikel task={task} onSubmit={onSubmit} submitting={submitting} />;
  if (fmt === 'wofrage') return <AufgabeWoFrage task={task} onSubmit={onSubmit} submitting={submitting} />;
  return <AufgabeText task={task} onSubmit={onSubmit} submitting={submitting} />;
}
