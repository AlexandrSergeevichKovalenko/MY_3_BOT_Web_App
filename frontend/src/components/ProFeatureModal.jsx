import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';

/**
 * Friendly "this is a Pro feature" plaque, shown IN PLACE as a dismissible modal
 * instead of yanking the user to the subscription page (which, opened from a
 * fullscreen overlay like the reader, had no back button and wouldn't scroll).
 * Generic: pass a title, an intro line and optional bullets. Primary CTA → onUpgrade
 * (open the subscription cleanly), secondary → onClose (stay where you are).
 *
 * `badge` переопределяет верхнюю плашку. Нужно там, где окно показывается НЕ из-за
 * платной функции, а из-за дневной нормы: норма бесплатная, просто закончилась, и
 * подпись «Функция «Полного доступа»» там врёт. Не передали — остаётся прежняя.
 */
export default function ProFeatureModal({
  isOpen,
  onClose,
  onUpgrade,
  tr,
  emoji = '🔒',
  title,
  intro,
  bullets = [],
  badge,
}) {
  if (!isOpen) return null;
  // Portal to <body> so the plaque sits above the fullscreen reader (which establishes
  // its own stacking context and would otherwise clip an in-tree fixed overlay).
  const target = typeof document !== 'undefined' ? document.body : null;
  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="profeat-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>
        <div className="profeat-badge">{badge || <>👑 {tr('Функция «Полного доступа»', 'Funktion mit vollem Zugang')}</>}</div>
        <div className="profeat-emoji">{emoji}</div>
        <h3 className="profeat-title">{title}</h3>
        {intro ? <p className="profeat-intro">{intro}</p> : null}
        {bullets.length > 0 ? (
          <ul className="profeat-list">
            {bullets.map((b, i) => <li key={i}>{b}</li>)}
          </ul>
        ) : null}
        <button type="button" className="profeat-cta" onClick={onUpgrade}>✨ {tr('Оформить полный доступ', 'Vollen Zugang holen')}</button>
        <button type="button" className="profeat-later" onClick={onClose}>{tr('Не сейчас', 'Nicht jetzt')}</button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
