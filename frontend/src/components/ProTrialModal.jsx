import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './ProTrialModal.css';

/**
 * Окно при входе: одно на все состояния бесплатного месяца
 * (docs/tasks/light_tier_strategy.md §6.2). Режимы:
 *  - 'active'       — идут 7 дней полного доступа: отсчёт, что открыто, кнопка «пользуюсь».
 *  - 'ended'        — 7 дней кончились, бесплатный месяц продолжается на уровне Лайт:
 *                     мягкий зов к полному доступу, закрывается.
 *  - 'month_ending' — до конца бесплатного месяца ≤ 5 дней: две кнопки оплаты, «Позже».
 *  - 'locked'       — месяц кончился, подписки нет: НЕ закрывается, две кнопки оплаты
 *                     и ссылка на настройки. Решение владельца 04.09.2026.
 */
export default function ProTrialModal({
  isOpen,
  mode = 'active', // 'active' | 'ended' | 'month_ending' | 'locked'
  daysLeft = 0,
  endsAt = null,
  monthEndsAt = null,
  monthDaysLeft = 0,
  justGranted = false,
  buying = false,
  lightStars = 0,
  proStars = 0,
  onBuyPro,
  onBuyLight,
  onOpenSettings,
  onClose,
  tr,
}) {
  if (!isOpen) return null;
  const target = typeof document !== 'undefined' ? document.body : null;
  const isActive = mode === 'active';
  const isEnding = mode === 'month_ending';
  const isLocked = mode === 'locked';
  const fmtDate = (value) => {
    if (!value) return '';
    try {
      const d = new Date(value);
      if (!Number.isNaN(d.getTime())) {
        return d.toLocaleDateString(undefined, { day: '2-digit', month: '2-digit' });
      }
    } catch (_e) { /* ignore */ }
    return '';
  };
  // Concrete end date ("Pro до 23.07"), so the ceil-rounded day counter never looks stuck.
  const endsAtLabel = fmtDate(endsAt);
  const monthEndsLabel = fmtDate(monthEndsAt);

  const perks = [
    tr('Переводы, детальные разборы и словарь — всё открыто', 'Übersetzungen, ausführliche Analysen & Wörterbuch — alles frei'),
    tr('Русские субтитры к видео + загадочная история', 'Russische Video-Untertitel + Rätselgeschichte'),
    tr('Карточки, тренажёры и skill-тренировка — полностью', 'Karten, Übungen & Skill-Training — komplett'),
    tr('Аналитика, план дня и карта слабых навыков', 'Analyse, Tagesplan & Schwache-Skills-Karte'),
  ];
  const lightPerks = [
    tr('Задания каждый день — 6, как в бесплатный месяц', 'Aufgaben jeden Tag — 6, wie im Gratismonat'),
    tr('Словарь, карточки и тренажёры в базовом объёме', 'Wörterbuch, Karten & Übungen im Basisumfang'),
    tr('Классика и статьи в читалке, игры и дуэли', 'Klassiker & Artikel im Reader, Spiele & Duelle'),
  ];

  const daysWord = (n) => {
    // RU plural: 1 день, 2–4 дня, 5+ дней
    const mod10 = n % 10;
    const mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return 'день';
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'дня';
    return 'дней';
  };
  const dLeft = Math.max(0, Number(daysLeft || 0));
  const mLeft = Math.max(0, Number(monthDaysLeft || 0));
  const lightLabel = lightStars > 0
    ? `🌿 ${tr('Лайт', 'Light')} — ${lightStars} ⭐ / ${tr('мес', 'Monat')}`
    : `🌿 ${tr('Лайт', 'Light')}`;
  const proLabel = proStars > 0
    ? `💎 ${tr('Полный доступ', 'Voller Zugang')} — ${proStars} ⭐ / ${tr('мес', 'Monat')}`
    : `💎 ${tr('Полный доступ', 'Voller Zugang')}`;

  let badge; let emoji; let title; let intro; let cardClass;
  if (isActive) {
    cardClass = 'is-active';
    badge = `🎁 ${tr('Полный доступ — бесплатно', 'Voller Zugang — gratis')}`;
    emoji = '🎁';
    title = justGranted
      ? tr('Тебе открыт полный доступ!', 'Voller Zugang freigeschaltet!')
      : tr('У тебя активен полный доступ', 'Dein voller Zugang ist aktiv');
    intro = tr(
      'Целых 7 дней тебе открыты все функции — бесплатно. Успей попробовать всё!',
      '7 Tage lang sind alle Funktionen frei — kostenlos. Probier alles aus!',
    );
  } else if (isEnding) {
    cardClass = 'is-ending';
    badge = `⏳ ${tr('Бесплатный месяц заканчивается', 'Der Gratismonat endet bald')}`;
    emoji = '⏳';
    title = tr(`Осталось ${mLeft} ${daysWord(mLeft)} бесплатно`, `Noch ${mLeft} ${mLeft === 1 ? 'Tag' : 'Tage'} gratis`);
    intro = tr(
      `Твой бесплатный месяц заканчивается ${monthEndsLabel ? monthEndsLabel : 'скоро'}. Чтобы задания и тренажёры продолжали работать, выбери тариф заранее — он включится сам, когда месяц закончится.`,
      `Dein Gratismonat endet ${monthEndsLabel ? `am ${monthEndsLabel}` : 'bald'}. Damit Aufgaben und Übungen weiterlaufen, wähle jetzt einen Tarif — er startet automatisch nach dem Gratismonat.`,
    );
  } else if (isLocked) {
    cardClass = 'is-locked';
    badge = `🔒 ${tr('Бесплатный месяц закончился', 'Der Gratismonat ist vorbei')}`;
    emoji = '🔒';
    title = tr('Ты позанимался с нами месяц', 'Du hast einen Monat mit uns gelernt');
    intro = tr(
      'Чтобы задания приходили дальше, выбери один из двух вариантов. «Лайт» — тот же объём, что был в бесплатный месяц. «Полный доступ» открывает всё.',
      'Damit die Aufgaben weiterkommen, wähle eine von zwei Optionen. «Light» — derselbe Umfang wie im Gratismonat. «Voller Zugang» schaltet alles frei.',
    );
  } else {
    cardClass = 'is-ended';
    badge = `⌛ ${tr('7 дней полного доступа прошли', '7 Tage voller Zugang sind vorbei')}`;
    emoji = '⌛';
    title = tr('Полный доступ закончился', 'Der volle Zugang ist zu Ende');
    intro = tr(
      'Твои 7 бесплатных дней полного доступа израсходованы. До конца бесплатного месяца работает уровень «Лайт». Понравилось всё? Оформи «Полный доступ» и продолжай.',
      'Deine 7 Gratis-Tage mit vollem Zugang sind vorbei. Bis zum Ende des Gratismonats läuft das Level «Light». Alles gefallen? Hol dir den vollen Zugang und mach weiter.',
    );
  }

  const closable = !isLocked && !buying;
  const showPayButtons = isEnding || isLocked;

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={closable ? onClose : undefined} />
      <div className={`profeat-card prot-card ${cardClass}`} onClick={(e) => e.stopPropagation()}>
        {!isLocked ? (
          <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>
        ) : null}

        <div className={`prot-badge ${cardClass}`}>{badge}</div>
        <div className="profeat-emoji prot-emoji">{emoji}</div>
        <h3 className="profeat-title">{title}</h3>

        {isActive ? (
          <div className="prot-countdown">
            <span className="prot-countdown-num">{dLeft}</span>
            <span className="prot-countdown-word">
              {tr(`${daysWord(dLeft)} осталось`, dLeft === 1 ? 'Tag übrig' : 'Tage übrig')}
              {endsAtLabel ? <span className="prot-countdown-date">{tr('Полный доступ до', 'Voller Zugang bis')} {endsAtLabel}</span> : null}
            </span>
          </div>
        ) : null}

        <p className="profeat-intro">{intro}</p>

        {showPayButtons ? (
          <div className="prot-plans">
            <div className="prot-plan prot-plan--light">
              <div className="prot-plan__name">{tr('Лайт', 'Light')}</div>
              <ul className="prot-perks prot-perks--tight">
                {lightPerks.map((p, i) => (
                  <li key={i}><span className="prot-perks-check">✓</span>{p}</li>
                ))}
              </ul>
            </div>
            <div className="prot-plan prot-plan--pro">
              <div className="prot-plan__name">{tr('Полный доступ', 'Voller Zugang')}</div>
              <ul className="prot-perks prot-perks--tight">
                {perks.map((p, i) => (
                  <li key={i}><span className="prot-perks-check">✓</span>{p}</li>
                ))}
              </ul>
            </div>
          </div>
        ) : (
          <ul className="prot-perks">
            {perks.map((p, i) => (
              <li key={i}><span className="prot-perks-check">✓</span>{p}</li>
            ))}
          </ul>
        )}

        <div className="prot-note">
          🔊 {tr(
            'Озвучка книг в читалке оплачивается отдельно, за каждую книгу — ни в один тариф она не входит.',
            'Buch-Vertonung im Reader wird separat je Buch bezahlt — in keinem Tarif enthalten.',
          )}
        </div>

        {showPayButtons ? (
          <>
            <button type="button" className="prot-cta is-light" disabled={buying} onClick={onBuyLight}>
              {buying ? tr('Открываю оплату…', 'Zahlung wird geöffnet…') : lightLabel}
            </button>
            <button type="button" className="prot-cta is-ended" disabled={buying} onClick={onBuyPro}>
              {buying ? tr('Открываю оплату…', 'Zahlung wird geöffnet…') : proLabel}
            </button>
            <div className="prot-fine">
              {tr('Подписка в Telegram Stars, продление раз в 30 дней, отмена в любой момент в настройках Telegram.',
                  'Abo in Telegram Stars, Verlängerung alle 30 Tage, jederzeit in den Telegram-Einstellungen kündbar.')}
            </div>
            {isLocked ? (
              <button type="button" className="profeat-later" onClick={onOpenSettings}>
                ⚙️ {tr('Настройки', 'Einstellungen')}
              </button>
            ) : (
              <button type="button" className="profeat-later" onClick={onClose}>
                {tr('Позже', 'Später')}
              </button>
            )}
          </>
        ) : isActive ? (
          <>
            <button type="button" className="prot-cta is-active" onClick={onClose}>
              👍 {tr('Здорово, пользуюсь!', 'Super, ich nutze es!')}
            </button>
            <button type="button" className="profeat-later" onClick={onClose}>
              {tr('Закрыть', 'Schließen')}
            </button>
          </>
        ) : (
          <>
            <button type="button" className="prot-cta is-ended" disabled={buying} onClick={onBuyPro}>
              {buying ? tr('Открываю оплату…', 'Zahlung wird geöffnet…') : `✨ ${tr('Оформить полный доступ', 'Vollen Zugang holen')}`}
            </button>
            <button type="button" className="profeat-later" onClick={onClose}>
              {tr('Позже', 'Später')}
            </button>
          </>
        )}
      </div>
    </div>
  );
  return target ? createPortal(node, target) : node;
}
