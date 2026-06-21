import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './answer.css';
import AskOverlay from './AskOverlay.jsx';
import { renderRich } from './richText.jsx';

/**
 * Deep-dive overlay for a quiz result — the in-place replacement for the four
 * callback buttons (🔊 Прослушать / 📌 Почувствовать слово / 🧩 Дать сочетание /
 * ❓ Задать свой вопрос) that used to dump new messages at the bottom of the chat.
 *
 * Launched as a Direct-Link Mini App from the result card (?startapp=dive_<id>).
 * Opens over the chat; every action expands in place — no scrolling away and
 * back. The "ask" action reuses the floating iMessage-style AskOverlay.
 */

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    return new URLSearchParams(window.location.search).get('initData') || '';
  }
  return '';
}

function parseStartParam(startParam) {
  const m = /^dive_(\d+)$/.exec(String(startParam || '').trim().toLowerCase());
  return m ? Number(m[1]) : null;
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

// German side of the pair (for pronunciation). The quiz pair is de/ru, but stay
// robust to either orientation.
function germanText(card) {
  if (!card) return '';
  if (card.source_lang === 'de') return card.source_text;
  if (card.target_lang === 'de') return card.target_text;
  return card.source_text || card.target_text || '';
}

// Listen via the existing TTS pipeline: POST generate → poll url → play the MP3.
function useTts() {
  const audioRef = useRef(null);
  const seqRef = useRef(0);
  const [state, setState] = useState('idle'); // idle|loading|playing|error
  const [err, setErr] = useState('');

  const stop = useCallback(() => {
    seqRef.current += 1;
    if (audioRef.current) { try { audioRef.current.pause(); } catch (_e) { /* ignore */ } audioRef.current = null; }
    setState('idle');
  }, []);

  const play = useCallback(async (text, language = 'de-DE') => {
    const t = String(text || '').trim();
    if (!t) return;
    const mySeq = ++seqRef.current;
    setErr('');
    setState('loading');
    haptic('light');
    try {
      await api('/api/webapp/tts/generate', { text: t, language });
      const params = new URLSearchParams({ text: t, language });
      let url = '';
      for (let i = 0; i < 30 && !url; i += 1) {
        if (mySeq !== seqRef.current) return; // superseded / stopped
        const res = await fetch(`/api/webapp/tts/url?${params.toString()}`, {
          method: 'GET', headers: { 'X-Telegram-InitData': getInitData() },
        });
        const data = await res.json().catch(() => ({}));
        if (data.status === 'ready' && data.audio_url) { url = data.audio_url; break; }
        if (data.status === 'failed') throw new Error(data.message || 'TTS fehlgeschlagen');
        await new Promise((r) => setTimeout(r, data.retry_after_ms || 700));
      }
      if (!url) throw new Error('Zeitüberschreitung');
      if (mySeq !== seqRef.current) return;
      const audio = new Audio(url);
      audioRef.current = audio;
      audio.onended = () => { if (mySeq === seqRef.current) setState('idle'); };
      audio.onerror = () => { if (mySeq === seqRef.current) { setErr('Audio konnte nicht abgespielt werden.'); setState('error'); } };
      setState('playing');
      await audio.play();
    } catch (e) {
      if (mySeq === seqRef.current) { setErr(String(e.message || e)); setState('error'); haptic('bad'); }
    }
  }, []);

  useEffect(() => () => stop(), [stop]);
  return { state, err, play, stop };
}

function PhraseCard({ card }) {
  const [phase, setPhase] = useState('idle'); // idle|loading|ready|error
  const [pair, setPair] = useState(null);
  const [err, setErr] = useState('');
  const [saveState, setSaveState] = useState('idle'); // idle|saving|done

  const load = useCallback(async () => {
    if (phase === 'loading') return;
    setPhase('loading'); setErr(''); haptic('light');
    try {
      const data = await api('/api/answer/deepdive/phrase', { id: card.id });
      setPair(data); setPhase('ready'); haptic('ok');
    } catch (e) {
      setErr(String(e.message || e)); setPhase('error'); haptic('bad');
    }
  }, [card.id, phase]);

  const save = useCallback(() => {
    // Optimistic: confirm instantly, save in the background (fire-and-forget) — no
    // "Сохраняю…" wait. The button flips to "✅ Сохранено в словарь" right away.
    if (!pair || saveState !== 'idle') return;
    setSaveState('done'); haptic('ok');
    Promise.resolve(
      api('/api/answer/deepdive/save', {
        source_text: pair.source_text, target_text: pair.target_text,
        source_lang: pair.source_lang, target_lang: pair.target_lang,
        semantic_category: pair.semantic_category || '',
      }),
    ).catch(() => { /* best-effort background save */ });
  }, [pair, saveState]);

  if (phase === 'idle') {
    return <button type="button" className="dd-action" onClick={load}>🧩 Дать сочетание</button>;
  }
  return (
    <div className="dd-panel">
      <div className="dd-panel-title">🧩 Сочетание</div>
      {phase === 'loading' ? <div className="dd-loading">Подбираю сочетание…</div> : null}
      {phase === 'error' ? <div className="dd-err">{err}</div> : null}
      {phase === 'ready' && pair ? (
        <>
          <div className="dd-pair">
            <div className="dd-pair-line"><b>{pair.source_text}</b></div>
            <div className="dd-pair-line dim">{pair.target_text}</div>
          </div>
          {saveState === 'done' ? (
            <div className="dd-saved">✅ Сохранено в словарь</div>
          ) : (
            <button type="button" className="dd-save" disabled={saveState === 'saving'} onClick={save}>
              {saveState === 'saving' ? 'Сохраняю…' : '💾 Сохранить в словарь'}
            </button>
          )}
        </>
      ) : null}
    </div>
  );
}

function FeelCard({ card }) {
  const [phase, setPhase] = useState('idle'); // idle|loading|ready|error
  const [text, setText] = useState('');
  const [err, setErr] = useState('');

  const load = useCallback(async () => {
    if (phase === 'loading') return;
    setPhase('loading'); setErr(''); haptic('light');
    try {
      const data = await api('/api/answer/deepdive/feel', { id: card.id });
      setText(String(data.feel || '')); setPhase('ready'); haptic('ok');
    } catch (e) {
      setErr(String(e.message || e)); setPhase('error'); haptic('bad');
    }
  }, [card.id, phase]);

  if (phase === 'idle') {
    return <button type="button" className="dd-action" onClick={load}>📌 Почувствовать слово</button>;
  }
  return (
    <div className="dd-panel">
      <div className="dd-panel-title">📌 Разбор</div>
      {phase === 'loading' ? <div className="dd-loading">Готовлю разбор…</div> : null}
      {phase === 'error' ? <div className="dd-err">{err}</div> : null}
      {phase === 'ready' ? <div className="dd-feel">{renderRich(text)}</div> : null}
    </div>
  );
}

export default function DeepDiveOverlay({ startParam }) {
  const cardId = useMemo(() => parseStartParam(startParam), [startParam]);
  const [card, setCard] = useState(null);
  const [loading, setLoading] = useState(true);
  const [fatal, setFatal] = useState('');
  const [askOpen, setAskOpen] = useState(false);
  const tts = useTts();

  useEffect(() => {
    try {
      tg?.ready?.();
      tg?.expand?.();
      tg?.setHeaderColor?.('secondary_bg_color');
      tg?.disableVerticalSwipes?.();
    } catch (_e) { /* ignore */ }
    const applyScheme = () => {
      const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
      try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
    };
    applyScheme();
    try { tg?.onEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ }
    return () => { try { tg?.offEvent?.('themeChanged', applyScheme); } catch (_e) { /* ignore */ } };
  }, []);

  useEffect(() => {
    if (cardId == null) { setFatal('Ungültiger Link.'); setLoading(false); return; }
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/deepdive', { id: cardId });
        if (!cancelled) setCard(data);
      } catch (e) {
        if (!cancelled) setFatal(String(e.message || e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [cardId]);

  const close = useCallback(() => { try { tg?.close?.(); } catch (_e) { /* ignore */ } }, []);

  if (fatal) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
        <p className="ans-sub">{fatal}</p>
        <button className="ans-btn-ghost" onClick={close}>Schließen</button>
      </div></div>
    );
  }

  if (loading || !card) {
    return (
      <div className="ans-root"><div className="ans-card">
        <div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" />
      </div></div>
    );
  }

  const de = germanText(card);
  const ru = card.source_lang === 'de' ? card.target_text : card.source_text;

  return (
    <div className="ans-root">
      <div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">🔍 Разбор слова</span>
          <h1 className="ans-title">{de}</h1>
          {ru ? <p className="ans-sub">{ru}</p> : null}
        </div>

        <div className="dd-actions">
          <button
            type="button"
            className={`dd-action${tts.state === 'playing' ? ' on' : ''}`}
            onClick={() => (tts.state === 'playing' ? tts.stop() : tts.play(de, 'de-DE'))}
            disabled={tts.state === 'loading' || !de}
          >
            {tts.state === 'loading' ? '🔊 Готовлю…' : tts.state === 'playing' ? '⏹ Остановить' : '🔊 Прослушать'}
          </button>
          {tts.state === 'error' && tts.err ? <div className="dd-err">{tts.err}</div> : null}

          <FeelCard card={card} />
          <PhraseCard card={card} />

          <button type="button" className="dd-action" onClick={() => setAskOpen(true)}>
            ❓ Задать свой вопрос
          </button>
        </div>

        <button className="ans-btn" onClick={close}>Schließen</button>
      </div>

      {askOpen ? (
        <AskOverlay
          api={api}
          onClose={() => setAskOpen(false)}
          context={[
            `Слово: ${de}.`,
            ru ? `Перевод: ${ru}.` : '',
          ].filter(Boolean).join(' ')}
        />
      ) : null}
    </div>
  );
}
