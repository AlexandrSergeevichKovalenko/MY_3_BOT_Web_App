import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ReaderAudioLimitModal.css';

/**
 * Honest "monthly listening limit reached" plaque for the reader audio feature. Shown IN
 * PLACE above the fullscreen reader instead of the misleading "try again in a couple
 * seconds" toast. For a per-user cap it offers a one-tap request to the admin (posted into
 * the existing support inbox); for the service-wide global cap it just explains and closes.
 */
function formatResetDate(resetAt, tr) {
  const raw = String(resetAt || '').trim();
  if (!raw) return '';
  const d = new Date(raw.length <= 10 ? `${raw}T00:00:00` : raw);
  if (Number.isNaN(d.getTime())) return raw;
  try {
    return d.toLocaleDateString(tr('ru-RU', 'de-DE'), { day: 'numeric', month: 'long' });
  } catch (_e) {
    return raw.slice(0, 10);
  }
}

export default function ReaderAudioLimitModal({
  isOpen,
  info,
  requestState,
  onRequestIncrease,
  onClose,
  tr,
}) {
  if (!isOpen || !info) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const limit = Math.max(0, Number(info.limit || 0));
  const used = Math.max(0, Number(info.used || 0));
  const pct = limit > 0 ? Math.max(4, Math.min(100, Math.round((used / limit) * 100))) : 100;
  const resetLabel = formatResetDate(info.resetAt, tr);
  const isGlobal = Boolean(info.isGlobal);

  const requestLabel = {
    idle: `✍️ ${tr('Попросить больше у администратора', 'Mehr beim Admin anfragen')}`,
    sending: tr('Отправляю…', 'Wird gesendet…'),
    sent: `✅ ${tr('Запрос отправлен', 'Anfrage gesendet')}`,
    error: tr('Не отправилось — нажми ещё раз', 'Fehlgeschlagen — nochmal tippen'),
  }[requestState] || `✍️ ${tr('Попросить больше у администратора', 'Mehr beim Admin anfragen')}`;

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="profeat-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>
        <div className="ralimit-badge">🎧 {tr('Озвучка на месяц', 'Vorlesen · Monatslimit')}</div>
        <div className="profeat-emoji">⏳</div>
        <h3 className="profeat-title">
          {isGlobal
            ? tr('Озвучка временно на паузе', 'Vorlesen kurz pausiert')
            : tr('Лимит озвучки на этот месяц исчерпан', 'Monatslimit fürs Vorlesen erreicht')}
        </h3>
        <p className="profeat-intro">
          {isGlobal
            ? tr(
                'Сервис озвучки достиг общего месячного лимита. Чтение, перевод по тапу и словарь работают как обычно.',
                'Das Vorlesen hat sein gemeinsames Monatslimit erreicht. Lesen, Tippen-Übersetzung und Wörterbuch laufen normal weiter.',
              )
            : tr(
                'Само чтение, перевод по тапу и словарь остаются бесплатными и без лимита — на паузе только озвучка голосом.',
                'Lesen, Tippen-Übersetzung und Wörterbuch bleiben kostenlos und unbegrenzt — pausiert ist nur die Sprachausgabe.',
              )}
        </p>

        {limit > 0 ? (
          <div className="ralimit-usage">
            <div className="ralimit-usage-track">
              <div className="ralimit-usage-fill" style={{ width: `${pct}%` }} />
            </div>
            <div className="ralimit-usage-caption">
              <b>{used.toLocaleString()}</b> / {limit.toLocaleString()} {tr('символов', 'Zeichen')}
              {resetLabel ? ` · ${tr('обновится', 'zurückgesetzt am')} ${resetLabel}` : ''}
            </div>
          </div>
        ) : null}

        {!isGlobal ? (
          <>
            {requestState === 'sent' ? (
              <p className="ralimit-sent-note">
                {tr('Ответим в чате поддержки. Загляни туда чуть позже 💬', 'Wir antworten im Support-Chat — schau später kurz rein 💬')}
              </p>
            ) : null}
            <button
              type="button"
              className={`ralimit-request is-${requestState}`}
              onClick={onRequestIncrease}
              disabled={requestState === 'sending' || requestState === 'sent'}
            >
              {requestLabel}
            </button>
          </>
        ) : null}

        <button type="button" className="profeat-later" onClick={onClose}>
          {tr('Понятно', 'Verstanden')}
        </button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
