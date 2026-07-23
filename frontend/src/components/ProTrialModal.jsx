import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ProTrialModal.css';

/**
 * App-entry plaque for the one-time 7-day Pro welcome trial. Two modes:
 *  - 'active': the user is inside their free trial → warm countdown ("осталось N дней"),
 *    what Pro unlocks, a note that book-audio is billed separately. Just a close CTA.
 *  - 'ended': the trial is used up and they're back on Free → same rich card, but the
 *    primary CTA takes them straight to buying Pro.
 * Both have a × to dismiss. Shown once per calendar day on entry.
 */
export default function ProTrialModal({
  isOpen,
  mode = 'active', // 'active' | 'ended'
  daysLeft = 0,
  endsAt = null,
  justGranted = false,
  buying = false,
  onBuyPro,
  onClose,
  tr,
}) {
  if (!isOpen) return null;
  const target = typeof document !== 'undefined' ? document.body : null;
  const isActive = mode === 'active';
  // Concrete end date ("Pro до 23.07"), so the ceil-rounded day counter never looks stuck.
  let endsAtLabel = '';
  if (endsAt) {
    try {
      const d = new Date(endsAt);
      if (!Number.isNaN(d.getTime())) {
        endsAtLabel = d.toLocaleDateString(undefined, { day: '2-digit', month: '2-digit' });
      }
    } catch (_e) { /* ignore */ }
  }

  const perks = [
    tr('Переводы, детальные разборы и словарь — всё открыто', 'Übersetzungen, ausführliche Analysen & Wörterbuch — alles frei'),
    tr('Русские субтитры к видео + загадочная история', 'Russische Video-Untertitel + Rätselgeschichte'),
    tr('Карточки, тренажёры и skill-тренировка — полностью', 'Karten, Übungen & Skill-Training — komplett'),
    tr('Аналитика, план дня и карта слабых навыков', 'Analyse, Tagesplan & Schwache-Skills-Karte'),
  ];

  const badge = isActive
    ? `🎁 ${tr('Полный доступ — бесплатно', 'Voller Zugang — gratis')}`
    : `⌛ ${tr('Пробный период истёк', 'Testphase beendet')}`;
  const emoji = isActive ? '🎁' : '⌛';
  const title = isActive
    ? (justGranted
        ? tr('Тебе открыт полный доступ!', 'Voller Zugang freigeschaltet!')
        : tr('У тебя активен полный доступ', 'Dein voller Zugang ist aktiv'))
    : tr('Бесплатный доступ закончился', 'Der kostenlose Zugang ist zu Ende');
  const intro = isActive
    ? tr(
        'Целых 7 дней тебе открыты все функции — бесплатно. Успей попробовать всё!',
        '7 Tage lang sind alle Funktionen frei — kostenlos. Probier alles aus!',
      )
    : tr(
        'Твои 7 бесплатных дней полного доступа израсходованы. Понравилось? Оформи «Полный доступ» и продолжай.',
        'Deine 7 Gratis-Tage mit vollem Zugang sind vorbei. Gefallen? Hol dir den vollen Zugang und mach weiter.',
      );

  const dLeft = Math.max(0, Number(daysLeft || 0));
  const daysWord = (n) => {
    // RU plural: 1 день, 2–4 дня, 5+ дней
    const mod10 = n % 10;
    const mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return 'день';
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'дня';
    return 'дней';
  };

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={buying ? undefined : onClose} />
      <div className={`profeat-card prot-card${isActive ? ' is-active' : ' is-ended'}`} onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>

        <div className={`prot-badge${isActive ? ' is-active' : ' is-ended'}`}>{badge}</div>
        <div className="profeat-emoji prot-emoji">{emoji}</div>
        <h3 className="profeat-title">{title}</h3>

        {isActive ? (
          <div className="prot-countdown">
            <span className="prot-countdown-num">{dLeft}</span>
            <span className="prot-countdown-word">
              {tr(`${daysWord(dLeft)} осталось`, dLeft === 1 ? 'Tag übrig' : 'Tage übrig')}
              {endsAtLabel ? <span className="prot-countdown-date">{tr('Pro до', 'Pro bis')} {endsAtLabel}</span> : null}
            </span>
          </div>
        ) : null}

        <p className="profeat-intro">{intro}</p>

        <ul className="prot-perks">
          {perks.map((p, i) => (
            <li key={i}><span className="prot-perks-check">✓</span>{p}</li>
          ))}
        </ul>

        <div className="prot-note">
          🔊 {tr(
            'Озвучка книг в читалке оплачивается отдельно, за каждую книгу — в Pro она не входит.',
            'Buch-Vertonung im Reader wird separat je Buch bezahlt — nicht in Pro enthalten.',
          )}
        </div>

        <div className="prot-footnote">
          {tr(
            '* Разговорная практика сейчас на доработке — скоро вернём.',
            '* Sprechpraxis wird gerade überarbeitet — bald wieder da.',
          )}
        </div>

        {isActive ? (
          <button type="button" className="prot-cta is-active" onClick={onClose}>
            👍 {tr('Здорово, пользуюсь!', 'Super, ich nutze es!')}
          </button>
        ) : (
          <button type="button" className="prot-cta is-ended" disabled={buying} onClick={onBuyPro}>
            {buying ? tr('Открываю оплату…', 'Zahlung wird geöffnet…') : `✨ ${tr('Оформить Pro', 'Pro holen')}`}
          </button>
        )}
        <button type="button" className="profeat-later" onClick={onClose}>
          {isActive ? tr('Закрыть', 'Schließen') : tr('Позже', 'Später')}
        </button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
