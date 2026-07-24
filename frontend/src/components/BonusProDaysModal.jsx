import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './StarsInfoModal.css';

/**
 * "Bonus Pro days" explainer plaque. Users earn extra Pro days (referrals, coffee/cheesecake
 * support, the welcome trial, study streaks) and rightly wonder: how many do I have, and when
 * do they kick in? This calm card answers both — with the live count at the top and a short
 * how-it-works list. Opened from the small ⓘ on the bonus-days banner in the «Подписка» screen.
 * Same shell/fonts as StarsInfoModal so it feels native.
 */
export default function BonusProDaysModal({ isOpen, onClose, tr, days = 0, banked = false, until = null }) {
  if (!isOpen) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const untilText = (() => {
    if (!until) return '';
    try {
      return new Date(until).toLocaleDateString();
    } catch (e) {
      return '';
    }
  })();

  const points = [
    {
      emoji: '🎁',
      title: tr('Как их накопить', 'Wie du sie sammelst'),
      body: tr('Пригласи друга — вы оба получите +7 дней полного доступа. Поддержишь проект: кофе — +7, кофе с чизкейком — +14. Плюс приветственные 7 дней и награды за серии занятий.',
               'Lade einen Freund ein — ihr bekommt beide +7 Tage vollen Zugang. Unterstützt du das Projekt: Kaffee — +7, Kaffee mit Cheesecake — +14. Dazu 7 Willkommens-Tage und Belohnungen für Lern-Serien.'),
    },
    {
      emoji: '⏳',
      title: tr('Когда они включаются', 'Wann sie aktiv werden'),
      body: tr('Без подписки — работают сразу: эти дни ты пользуешься полным доступом бесплатно. С активной подпиской «Полный доступ» — откладываются в запас и включаются, когда закончится оплаченный период.',
               'Ohne Abo — sofort aktiv: diese Tage nutzt du vollen Zugang gratis. Mit aktivem Voller-Zugang-Abo — sie wandern in die Reserve und starten, sobald der bezahlte Zeitraum endet.'),
    },
    {
      emoji: '🔁',
      title: tr('Не сгорают при продлении', 'Gehen bei Verlängerung nicht verloren'),
      body: tr('Пока ты продолжаешь оплачивать «Полный доступ», бонусные дни спокойно ждут в запасе. Как только перестанешь платить или отменишь подписку — они продлят твой полный доступ. Ничего не теряется.',
               'Solange du den vollen Zugang weiter bezahlst, warten die Bonustage in der Reserve. Sobald du nicht mehr zahlst oder kündigst, verlängern sie deinen vollen Zugang. Nichts geht verloren.'),
    },
  ];

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="profeat-card star-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>

        <div className="star-badge">🎁 {tr('Бонусные дни полного доступа', 'Bonustage für vollen Zugang')}</div>
        <div className="profeat-emoji star-emoji">🎁</div>
        {days > 0 ? (
          <>
            <h3 className="profeat-title">
              {banked
                ? tr(`${days} дн. полного доступа в запасе`, `${days} Tage vollen Zugang in Reserve`)
                : tr(`У тебя ещё ${days} дн. полного доступа`, `Du hast noch ${days} Tage vollen Zugang`)}
            </h3>
            <p className="profeat-intro">
              {banked
                ? (untilText
                    ? tr(`Включатся после окончания подписки и дадут полный доступ примерно до ${untilText}.`,
                          `Sie starten nach Abo-Ende und geben vollen Zugang etwa bis ${untilText}.`)
                    : tr('Включатся, как только закончится твоя платная подписка.',
                         'Sie starten, sobald dein bezahltes Abo endet.'))
                : (untilText
                    ? tr(`Полный доступ бесплатно — примерно до ${untilText}.`,
                          `Vollen Zugang gratis — etwa bis ${untilText}.`)
                    : tr('Ты пользуешься полным доступом бесплатно.', 'Du nutzt vollen Zugang gratis.'))}
            </p>
          </>
        ) : (
          <>
            <h3 className="profeat-title">{tr('Бонусные дни полного доступа', 'Bonustage für vollen Zugang')}</h3>
            <p className="profeat-intro">
              {tr('Дополнительные дни полного доступа, которые можно получить бесплатно — за друзей, поддержку и занятия.',
                  'Zusätzliche Tage vollen Zugang, die du gratis bekommst — für Freunde, Unterstützung und Lernen.')}
            </p>
          </>
        )}

        <ul className="star-points">
          {points.map((p, i) => (
            <li key={i}>
              <span className="star-point-ic">{p.emoji}</span>
              <span className="star-point-text">
                <b>{p.title}</b>
                <span>{p.body}</span>
              </span>
            </li>
          ))}
        </ul>

        <button type="button" className="star-cta" onClick={onClose}>
          {tr('Понятно', 'Alles klar')}
        </button>
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
