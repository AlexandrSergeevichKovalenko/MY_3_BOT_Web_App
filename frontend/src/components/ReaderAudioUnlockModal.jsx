import React, { useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ReaderAudioUnlockModal.css';

/**
 * Beautiful "unlock this book's narration" plaque. One-time per-book purchase from
 * the prepaid audio wallet: pay once → listen forever, unlimited. Handles both the
 * afford-and-unlock case and the top-up case inline (no modal-hopping). Rendered in
 * our standard profeat-card shell so it matches every other reader popup.
 */
function eur(minor) {
  return `€${(Math.max(0, Number(minor || 0)) / 100).toFixed(2)}`;
}

export default function ReaderAudioUnlockModal({
  isOpen,
  info,
  unlockState = 'idle', // idle | unlocking | error
  topupState = 'idle', // idle | opening
  onUnlock,
  onTopup,
  onRefreshBalance,
  onClose,
  tr,
}) {
  const tiers = (info && Array.isArray(info.tiers) ? info.tiers : []);
  const presets = (info && Array.isArray(info.topupPresets) ? info.topupPresets : [300, 500, 1000, 2000]);
  const [pickedTier, setPickedTier] = useState('');

  const effTier = useMemo(() => {
    if (pickedTier) return tiers.find((t) => t.tier === pickedTier) || tiers[0] || null;
    return tiers.find((t) => t.tier === (info && info.defaultTier)) || tiers[0] || null;
  }, [pickedTier, tiers, info]);

  if (!isOpen || !info) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const balance = Math.max(0, Number(info.balanceMinor || 0));
  const price = Math.max(0, Number(effTier?.price_minor || 0));
  const unlocked = Boolean(effTier?.unlocked);
  const affordable = balance >= price;
  const shortfall = Math.max(0, price - balance);
  const busy = unlockState === 'unlocking' || topupState === 'opening';

  // Smallest preset that clears the shortfall — pre-highlighted so the common path
  // is one tap.
  const suggestedPreset = presets.find((p) => p >= shortfall) || presets[presets.length - 1];

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

        {/* Voice choice */}
        <div className="rau-tiers">
          {tiers.map((t) => {
            const isSel = effTier && t.tier === effTier.tier;
            return (
              <button
                type="button"
                key={t.tier}
                className={`rau-tier${isSel ? ' is-selected' : ''}${t.unlocked ? ' is-unlocked' : ''}`}
                onClick={() => setPickedTier(t.tier)}
              >
                <span className="rau-tier-main">
                  <span className="rau-tier-radio" aria-hidden="true" />
                  <span className="rau-tier-name">{tierLabel(t)}</span>
                </span>
                <span className="rau-tier-price">
                  {t.unlocked ? <span className="rau-tier-owned">✓ {tr('куплено', 'gekauft')}</span> : eur(t.price_minor)}
                </span>
              </button>
            );
          })}
        </div>

        {/* Wallet balance */}
        <div className="rau-balance">
          <span>💳 {tr('Баланс', 'Guthaben')}</span>
          <b>{eur(balance)}</b>
        </div>

        {/* CTA */}
        {unlocked ? (
          <button type="button" className="rau-primary is-owned" onClick={onClose}>
            ✅ {tr('Уже куплено — можно слушать', 'Bereits gekauft — anhören')}
          </button>
        ) : affordable ? (
          <button
            type="button"
            className="rau-primary"
            disabled={busy}
            onClick={() => onUnlock && onUnlock(effTier.tier)}
          >
            {unlockState === 'unlocking'
              ? tr('Оплачиваю…', 'Wird bezahlt…')
              : `🎧 ${tr('Озвучить за', 'Vertonen für')} ${eur(price)}`}
          </button>
        ) : (
          <div className="rau-topup">
            <div className="rau-shortfall">
              {tr('Не хватает', 'Es fehlen')} <b>{eur(shortfall)}</b> — {tr('пополни баланс', 'Guthaben aufladen')}
            </div>
            <div className="rau-presets">
              {presets.map((p) => (
                <button
                  type="button"
                  key={p}
                  className={`rau-preset${p === suggestedPreset ? ' is-suggested' : ''}`}
                  disabled={busy}
                  onClick={() => onTopup && onTopup(p)}
                >
                  {eur(p)}
                </button>
              ))}
            </div>
            <button
              type="button"
              className="rau-refresh"
              disabled={busy}
              onClick={onRefreshBalance}
            >
              🔄 {tr('Оплатил — обновить баланс', 'Bezahlt — Guthaben prüfen')}
            </button>
          </div>
        )}

        {unlockState === 'error' ? (
          <p className="rau-error">{tr('Не получилось — попробуй ещё раз', 'Fehlgeschlagen — nochmal versuchen')}</p>
        ) : null}

        <p className="rau-fineprint">
          {tr(
            'Платишь только за эту книгу. Классика в библиотеке — с озвучкой бесплатно.',
            'Du zahlst nur für dieses Buch. Klassiker in der Bibliothek: Audio gratis.',
          )}
        </p>
        <button type="button" className="profeat-later" onClick={onClose}>{tr('Позже', 'Später')}</button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
