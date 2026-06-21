import React, { useCallback, useState } from 'react';
import { renderRich } from './richText.jsx';

/**
 * The 🔊 / 📌 / 🧩 deep-dive actions, rendered in place under a result.
 * Reused by the MC quiz result (and any result that has a deep-dive card).
 *
 * Props:
 *   api        — POST helper (path, body) → json (injects initData)
 *   haptic     — feedback fn
 *   cardId     — deep-dive card id (for 📌 feel / 🧩 phrase); may be null while minting
 *   germanText — the German word/phrase (for 🔊 listen; works without a card)
 *   playTts    — async (text) => plays the German audio
 */
export default function DeepDiveActions({ api, haptic, cardId, germanText, playTts }) {
  const [listen, setListen] = useState('idle'); // idle|loading|error
  const [feel, setFeel] = useState({ phase: 'idle', text: '', err: '' });
  const [phrase, setPhrase] = useState({ phase: 'idle', pair: null, err: '', save: 'idle' });

  const doListen = useCallback(async () => {
    if (listen === 'loading' || !germanText) return;
    setListen('loading'); haptic?.('light');
    try { await playTts(germanText); setListen('idle'); }
    catch (_e) { setListen('error'); haptic?.('bad'); }
  }, [listen, germanText, playTts, haptic]);

  const doFeel = useCallback(async () => {
    if (feel.phase === 'loading' || !cardId) return;
    setFeel({ phase: 'loading', text: '', err: '' }); haptic?.('light');
    try {
      const d = await api('/api/answer/deepdive/feel', { id: cardId });
      setFeel({ phase: 'ready', text: String(d.feel || ''), err: '' }); haptic?.('ok');
    } catch (e) { setFeel({ phase: 'error', text: '', err: String(e.message || e) }); haptic?.('bad'); }
  }, [feel.phase, cardId, api, haptic]);

  const doPhrase = useCallback(async () => {
    if (phrase.phase === 'loading' || !cardId) return;
    setPhrase((s) => ({ ...s, phase: 'loading', err: '' })); haptic?.('light');
    try {
      const d = await api('/api/answer/deepdive/phrase', { id: cardId });
      setPhrase({ phase: 'ready', pair: d, err: '', save: 'idle' }); haptic?.('ok');
    } catch (e) { setPhrase((s) => ({ ...s, phase: 'error', err: String(e.message || e) })); haptic?.('bad'); }
  }, [phrase.phase, cardId, api, haptic]);

  const savePhrase = useCallback(() => {
    // Optimistic: confirm instantly, save in the background (fire-and-forget) — no
    // "Сохраняю…" wait. The button flips to "✅ Сохранено в словарь" right away.
    const p = phrase.pair;
    if (!p || phrase.save !== 'idle') return;
    setPhrase((s) => ({ ...s, save: 'done' })); haptic?.('ok');
    Promise.resolve(
      api('/api/answer/deepdive/save', {
        source_text: p.source_text, target_text: p.target_text,
        source_lang: p.source_lang, target_lang: p.target_lang,
        semantic_category: p.semantic_category || '',
      }),
    ).catch(() => { /* best-effort background save */ });
  }, [phrase.pair, phrase.save, api, haptic]);

  return (
    <div className="dd-actions" style={{ marginTop: 12 }}>
      {germanText ? (
        <button type="button" className="dd-action" disabled={listen === 'loading'} onClick={doListen}>
          {listen === 'loading' ? '🔊 Готовлю…' : listen === 'error' ? '🔊 Ещё раз' : '🔊 Прослушать'}
        </button>
      ) : null}

      {feel.phase === 'idle' ? (
        <button type="button" className="dd-action" disabled={!cardId} onClick={doFeel}>📌 Почувствовать слово</button>
      ) : (
        <div className="dd-panel">
          <div className="dd-panel-title">📌 Разбор</div>
          {feel.phase === 'loading' ? <div className="dd-loading">Готовлю разбор…</div> : null}
          {feel.phase === 'error' ? <div className="dd-err">{feel.err}</div> : null}
          {feel.phase === 'ready' ? <div className="dd-feel">{renderRich(feel.text)}</div> : null}
        </div>
      )}

      {phrase.phase === 'idle' ? (
        <button type="button" className="dd-action" disabled={!cardId} onClick={doPhrase}>🧩 Дать сочетание</button>
      ) : (
        <div className="dd-panel">
          <div className="dd-panel-title">🧩 Сочетание</div>
          {phrase.phase === 'loading' ? <div className="dd-loading">Подбираю сочетание…</div> : null}
          {phrase.phase === 'error' ? <div className="dd-err">{phrase.err}</div> : null}
          {phrase.phase === 'ready' && phrase.pair ? (
            <>
              <div className="dd-pair">
                <div className="dd-pair-line"><b>{phrase.pair.source_text}</b></div>
                <div className="dd-pair-line dim">{phrase.pair.target_text}</div>
              </div>
              {phrase.save === 'done' ? (
                <div className="dd-saved">✅ Сохранено в словарь</div>
              ) : (
                <button type="button" className="dd-save" disabled={phrase.save === 'saving'} onClick={savePhrase}>
                  {phrase.save === 'saving' ? 'Сохраняю…' : '💾 Сохранить в словарь'}
                </button>
              )}
            </>
          ) : null}
        </div>
      )}
    </div>
  );
}
