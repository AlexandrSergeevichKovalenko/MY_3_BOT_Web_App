import React, { useMemo, useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ReaderAudioUnlockModal.css';
import './StarsInfoModal.css';

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
  onStarsInfo,
  tr,
}) {
  const tiers = (info && Array.isArray(info.tiers) ? info.tiers : []);
  const [pickedTier, setPickedTier] = useState('');
  // Partial narration: 25 / 50 / 100. Default to the whole book, but the cheaper
  // chips are right there so a pricey book is "tryable".
  const [pickedCoverage, setPickedCoverage] = useState(100);

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

  // Coverage options for the selected voice (25/50/100 with per-fraction prices).
  const covOptions = useMemo(
    () => (effTier && Array.isArray(effTier.coverage_options) ? effTier.coverage_options : []),
    [effTier],
  );
  const ownedCov = Math.max(0, Number(effTier?.unlocked_coverage || 0));
  const selectedOpt = useMemo(() => {
    if (!covOptions.length) return null;
    return covOptions.find((o) => Number(o.coverage_pct) === Number(pickedCoverage))
      || covOptions.find((o) => Number(o.coverage_pct) === 100)
      || covOptions[covOptions.length - 1];
  }, [covOptions, pickedCoverage]);
  const ownedOpt = ownedCov > 0 ? covOptions.find((o) => Number(o.coverage_pct) === ownedCov) : null;

  if (!isOpen || !info) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  // Charge only the delta when upgrading an already-owned fraction (25% → 100%).
  const selStars = Math.max(0, Number(selectedOpt?.price_stars ?? effTier?.price_stars ?? 0));
  const ownedStars = Math.max(0, Number(ownedOpt?.price_stars || 0));
  const effStars = selectedOpt ? Math.max(0, selStars - ownedStars) : Math.max(0, Number(effTier?.price_stars || 0));
  const fullyUnlocked = ownedCov >= 100;
  const pieceOwned = Boolean(selectedOpt?.owned) && !fullyUnlocked; // this exact fraction already bought
  const busy = unlockState === 'unlocking';

  const tierLabel = (t) => {
    const lbl = t?.label || {};
    return tr(lbl.ru || 'Голос', lbl.de || 'Stimme');
  };
  const covLabel = (pct) => {
    if (pct >= 100) return tr('Вся книга', 'Ganzes Buch');
    if (pct === 50) return tr('Половина', 'Die Hälfte');
    return tr('Кусочек', 'Ein Stück');
  };
  const covSub = (pct) => (pct >= 100 ? tr('целиком', 'komplett') : `${pct}%`);

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
                  {Number(t.unlocked_coverage || 0) >= 100
                    ? <span className="rau-tier-owned">✓ {tr('куплено', 'gekauft')}</span>
                    : (() => {
                        const opts = Array.isArray(t.coverage_options) ? t.coverage_options : [];
                        const from = opts.length ? Math.min(...opts.map((o) => Number(o.price_stars || 0))) : Number(t.price_stars || 0);
                        return <span className="rau-tier-stars">{tr('от', 'ab')} {from} ⭐</span>;
                      })()}
                </span>
              </div>
            );
          })}
        </div>
        {(samples || samplesLoading) && (
          <div className="rau-preview-hint">🎧 {tr('Нажми ▶, чтобы услышать голос перед оплатой', 'Tippe auf ▶, um die Stimme vor dem Kauf zu hören')}</div>
        )}

        {/* How much to narrate — try a piece before buying the whole book */}
        {!fullyUnlocked && covOptions.length > 1 ? (
          <div className="rau-cov">
            <div className="rau-cov-label">{tr('Сколько озвучить', 'Wie viel vertonen')}</div>
            <div className="rau-cov-chips">
              {covOptions.map((o) => {
                const pct = Number(o.coverage_pct);
                const isSel = selectedOpt && Number(selectedOpt.coverage_pct) === pct;
                const owned = Boolean(o.owned);
                return (
                  <button
                    key={pct}
                    type="button"
                    className={`rau-cov-chip${isSel ? ' is-selected' : ''}${owned ? ' is-owned' : ''}`}
                    onClick={() => setPickedCoverage(pct)}
                  >
                    <span className="rau-cov-chip-title">{covLabel(pct)}</span>
                    <span className="rau-cov-chip-sub">{covSub(pct)}</span>
                    <span className="rau-cov-chip-price">
                      {owned ? `✓ ${tr('есть', 'da')}` : `${Number(o.price_stars || 0)} ⭐`}
                    </span>
                  </button>
                );
              })}
            </div>
            {ownedCov > 0 && ownedCov < 100 ? (
              <div className="rau-cov-hint">
                {tr(`У тебя уже озвучено ${ownedCov}% — за остальное доплатишь только разницу.`,
                    `${ownedCov}% sind schon vertont — für den Rest zahlst du nur die Differenz.`)}
              </div>
            ) : null}
          </div>
        ) : null}

        {/* CTA — native Telegram Stars payment */}
        {fullyUnlocked ? (
          <button type="button" className="rau-primary is-owned" onClick={onClose}>
            ✅ {tr('Уже куплено — можно слушать', 'Bereits gekauft — anhören')}
          </button>
        ) : pieceOwned ? (
          <button type="button" className="rau-primary is-owned" onClick={onClose}>
            ✅ {tr('Эта часть уже куплена', 'Dieser Teil ist gekauft')}
          </button>
        ) : (
          <button
            type="button"
            className="rau-primary"
            disabled={busy}
            onClick={() => onUnlock && effTier && selectedOpt && onUnlock(effTier.tier, Number(selectedOpt.coverage_pct))}
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
          {onStarsInfo ? (
            <button type="button" className="stars-info-btn" onClick={onStarsInfo} aria-label={tr('Что такое звёзды Telegram', 'Was sind Telegram-Sterne')} title={tr('Что такое звёзды', 'Was sind Sterne')}>i</button>
          ) : null}
        </p>
        <button type="button" className="profeat-later" onClick={onClose}>{tr('Позже', 'Später')}</button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
