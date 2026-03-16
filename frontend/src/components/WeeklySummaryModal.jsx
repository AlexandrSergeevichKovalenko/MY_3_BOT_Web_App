import React from 'react';

export default function WeeklySummaryModal({
  isOpen,
  title,
  subtitle,
  closeLabel,
  openAnalyticsLabel,
  onClose,
  onOpenAnalytics,
  children,
}) {
  if (!isOpen) {
    return null;
  }

  return (
    <div className="weekly-summary-overlay" role="dialog" aria-modal="true" aria-labelledby="weekly-summary-title">
      <button
        type="button"
        className="weekly-summary-backdrop"
        aria-label={closeLabel}
        onClick={onClose}
      />
      <div className="weekly-summary-modal" onClick={(event) => event.stopPropagation()}>
        <div className="weekly-summary-head">
          <div className="weekly-summary-head-copy">
            <div className="weekly-summary-eyebrow">Weekly Summary</div>
            <h3 id="weekly-summary-title">{title}</h3>
            <p>{subtitle}</p>
          </div>
          <div className="weekly-summary-head-actions">
            <button type="button" className="weekly-summary-link" onClick={onOpenAnalytics}>
              {openAnalyticsLabel}
            </button>
            <button type="button" className="weekly-summary-close" aria-label={closeLabel} onClick={onClose}>
              ×
            </button>
          </div>
        </div>
        {children ? <div className="weekly-summary-body">{children}</div> : null}
      </div>
    </div>
  );
}
