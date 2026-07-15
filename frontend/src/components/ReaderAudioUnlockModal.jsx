import React, { useMemo, useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ReaderAudioUnlockModal.css';

/**
 * "Unlock this book's narration" plaque. One-time per-book purchase paid natively in
 * Telegram Stars (WebApp.openInvoice) — pay once → listen forever, unlimited. Pick a
 * voice, preview it, pay in one in-app sheet. No wallet, no external browser.
 */
export default function ReaderAudioUnlockModal({
  isOpen,
  info,
  unlockState = 'idle', // idle | unlocking | error
  samples = null, // { neural: url, standard: url } — voice previews
  samplesLoading = false,
  onUnlock,
  onClose,
  tr,
}) {
  const tiers = (info && Array.isArray(info.tiers) ? info.tiers : []);
  const [pickedTier, setPickedTier] = useState('');

  // Voice-preview playback: one shared <audio>, only one tier plays at a time.
  const [playingTier, setPlayingTier] = useState('');
  const audioRef = useRef(null);
  const stopSample = () => {
    const a = audioRef.current;
    if (a) { try { a.pause(); } catch (_e) {} }
    setPlayingTier('');
  };
  const toggleSample = (tier, url) => {
    if (!url) return;
    if (playingTier === tier) { stopSample(); return; }
    let a = audioRef.current;
    if (!a) { a = new Audio(); audioRef.current = a; a.onended = () => setPlayingTier(''); }
    try { a.pause(); } catch (_e) {}
    a.src = url;
    a.currentTime = 0;
    a.play().then(() => setPlayingTier(tier)).catch(() => setPlayingTier(''));
  };
  // Stop preview whenever the modal closes.
  useEffect(() => { if (!isOpen) stopSample(); }, [isOpen]);

  const effTier = useMemo(() => {
    if (pickedTier) return tiers.find((t) => t.tier === pickedTier) || tiers[0] || null;
    return tiers.find((t) => t.tier === (info && info.defaultTier)) || tiers[0] || null;
  }, [pickedTier, tiers, info]);

  if (!isOpen || !info) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const effStars = Math.max(0, Number(effTier?.price_stars || 0));
  const unlocked = Boolean(effTier?.unlocked);
  const busy = unlockState === 'unlocking';

  const tierLabel = (t) => {
    const lbl = t?.label || {};
    return tr(lbl.ru || 'Голос', lbl.de || 'Stimme');
  };

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={busy ? undefined : onClose} />
      <div className="profeat-card rau-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>

        <div className="rau-badge">🎧 {tr('Озвучка книги', 'Hörbuch')}</div>
        <div className="profeat-emoji">🔊</div>
        <h3 className="profeat-title">{tr('Слушай книгу с озвучкой', 'Buch mit Audio hören')}</h3>
        {info.bookTitle ? <div className="rau-book-title">«{info.bookTitle}»</div> : null}
        <p className="profeat-intro">
          {tr(
            'Оплати один раз — и слушай эту книгу с озвучкой всегда, с любого места, без ограничений.',
            'Einmal zahlen — dann dieses Buch jederzeit, überall und unbegrenzt mit Audio hören.',
          )}
        </p>

        {/* Voice choice + listen-before-pay preview */}
        <div className="rau-tiers">
          {tiers.map((t) => {
            const isSel = effTier && t.tier === effTier.tier;
            const sampleUrl = samples && samples[t.tier];
            const isPlaying = playingTier === t.tier;
            return (
              <div
                key={t.tier}
                className={`rau-tier${isSel ? ' is-selected' : ''}${t.unlocked ? ' is-unlocked' : ''}`}
              >
                <button
                  type="button"
                  className="rau-tier-select"
                  onClick={() => setPickedTier(t.tier)}
                >
                  <span className="rau-tier-radio" aria-hidden="true" />
                  <span className="rau-tier-name">{tierLabel(t)}</span>
                </button>
                {sampleUrl ? (
                  <button
                    type="button"
                    className={`rau-tier-preview${isPlaying ? ' is-playing' : ''}`}
                    onClick={() => toggleSample(t.tier, sampleUrl)}
                    aria-label={isPlaying ? tr('Стоп', 'Stopp') : tr('Прослушать голос', 'Stimme anhören')}
                    title={isPlaying ? tr('Стоп', 'Stopp') : tr('Прослушать', 'Anhören')}
                  >
                    {isPlaying ? (
                      <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><rect x="6.5" y="5.5" width="4" height="13" rx="1.2" /><rect x="13.5" y="5.5" width="4" height="13" rx="1.2" /></svg>
                    ) : (
                      <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M8 5.5v13l11-6.5-11-6.5Z" /></svg>
                    )}
                  </button>
                ) : samplesLoading ? (
                  <span className="rau-tier-preview is-loading" aria-hidden="true" />
                ) : null}
                <span className="rau-tier-price">
                  {t.unlocked
                    ? <span className="rau-tier-owned">✓ {tr('куплено', 'gekauft')}</span>
                    : <span className="rau-tier-stars">{Number(t.price_stars || 0)} ⭐</span>}
                </span>
              </div>
            );
          })}
        </div>
        {(samples || samplesLoading) && (
          <div className="rau-preview-hint">🎧 {tr('Нажми ▶, чтобы услышать голос перед оплатой', 'Tippe auf ▶, um die Stimme vor dem Kauf zu hören')}</div>
        )}

        {/* CTA — native Telegram Stars payment */}
        {unlocked ? (
          <button type="button" className="rau-primary is-owned" onClick={onClose}>
            ✅ {tr('Уже куплено — можно слушать', 'Bereits gekauft — anhören')}
          </button>
        ) : (
          <button
            type="button"
            className="rau-primary"
            disabled={busy}
            onClick={() => onUnlock && effTier && onUnlock(effTier.tier)}
          >
            {busy
              ? tr('Открываю оплату…', 'Zahlung wird geöffnet…')
              : `${tr('Озвучить за', 'Vertonen für')} ${effStars} ⭐`}
          </button>
        )}

        {unlockState === 'error' ? (
          <p className="rau-error">{tr('Оплата не прошла — попробуй ещё раз', 'Zahlung fehlgeschlagen — nochmal versuchen')}</p>
        ) : null}

        <p className="rau-fineprint">
          {tr(
            'Оплата в Telegram Stars, прямо здесь. Платишь только за эту книгу — классика в библиотеке озвучивается бесплатно.',
            'Zahlung mit Telegram Stars, direkt hier. Nur für dieses Buch — Klassiker in der Bibliothek: Audio gratis.',
          )}
        </p>
        <button type="button" className="profeat-later" onClick={onClose}>{tr('Позже', 'Später')}</button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
