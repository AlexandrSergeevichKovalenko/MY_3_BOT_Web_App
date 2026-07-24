import React from 'react';
import { createPortal } from 'react-dom';
import './NoticeModal.css';

/**
 * A small, prominent house-style modal for a short, soft message — a validation hint,
 * a "come back later" note, anything that a pale inline toast makes too easy to miss.
 * Centered, dimmed backdrop, dismissible (backdrop / × / "Понятно"). NOT a paywall
 * (no badge/CTA) — for those use ProFeatureModal. Keep the copy short and human.
 */
export default function NoticeModal({
  isOpen,
  onClose,
  tr,
  emoji = 'ℹ️',
  title,
  message,
  okLabel,
}) {
  if (!isOpen) return null;
  const closeLabel = tr ? tr('Закрыть', 'Schließen') : 'Закрыть';
  const ok = okLabel || (tr ? tr('Понятно', 'Verstanden') : 'Понятно');
  // Portal to <body> so the modal sits above fullscreen overlays that establish their
  // own stacking context (reader, etc.).
  const target = typeof document !== 'undefined' ? document.body : null;
  const node = (
    <div className="notice-modal-overlay" role="dialog" aria-modal="true">
      <button type="button" className="notice-modal-backdrop" aria-label={closeLabel} onClick={onClose} />
      <div className="notice-modal-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="notice-modal-close" aria-label={closeLabel} onClick={onClose}>×</button>
        {emoji ? <div className="notice-modal-emoji" aria-hidden="true">{emoji}</div> : null}
        {title ? <h3 className="notice-modal-title">{title}</h3> : null}
        {message ? <p className="notice-modal-message">{message}</p> : null}
        <button type="button" className="notice-modal-ok" onClick={onClose}>{ok}</button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
