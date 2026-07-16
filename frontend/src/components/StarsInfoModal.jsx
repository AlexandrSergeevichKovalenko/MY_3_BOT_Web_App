import React from 'react';
import { createPortal } from 'react-dom';
import './ProFeatureModal.css';
import './StarsInfoModal.css';

/**
 * "What are Telegram Stars" explainer plaque. Many users — especially older ones — have
 * never used Stars and worry it's a scam. This calm, reassuring card explains what they
 * are and that paying is safe and in-app. Reused anywhere a Stars price is shown (a small
 * ⓘ next to the price opens it). Same shell/fonts as the other plaques.
 */
export default function StarsInfoModal({ isOpen, onClose, tr }) {
  if (!isOpen) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const points = [
    {
      emoji: '⭐',
      title: tr('Официальная валюта Telegram', 'Offizielle Telegram-Währung'),
      body: tr('Звёзды придумал сам Telegram. Ими оплачивают цифровые товары и подписки прямо внутри приложения.',
               'Sterne stammen von Telegram selbst. Damit zahlt man digitale Waren und Abos direkt in der App.'),
    },
    {
      emoji: '🛡',
      title: tr('Безопасно и без чужих сайтов', 'Sicher, ohne fremde Seiten'),
      body: tr('Оплата проходит в самом Telegram. Не нужно вводить карту на сторонних сайтах и никому её не передаёшь.',
               'Die Zahlung läuft in Telegram selbst. Keine Karteneingabe auf fremden Seiten, nichts wird weitergegeben.'),
    },
    {
      emoji: '👆',
      title: tr('Покупаются в пару тапов', 'In wenigen Taps gekauft'),
      body: tr('Нажимаешь «Оплатить» — Telegram сам предложит купить нужное количество звёзд и тут же спишет их.',
               'Du tippst auf „Bezahlen“ — Telegram bietet an, die nötigen Sterne zu kaufen, und zieht sie direkt ab.'),
    },
  ];

  const node = (
    <div className="profeat-overlay" role="dialog" aria-modal="true">
      <button type="button" className="profeat-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="profeat-card star-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="profeat-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>

        <div className="star-badge">⭐ {tr('Звёзды Telegram', 'Telegram-Sterne')}</div>
        <div className="profeat-emoji star-emoji">⭐</div>
        <h3 className="profeat-title">{tr('Что такое звёзды', 'Was sind Sterne')}</h3>
        <p className="profeat-intro">
          {tr('Короткое пояснение, чтобы не осталось вопросов перед оплатой.',
              'Eine kurze Erklärung, damit vor der Zahlung keine Fragen offen bleiben.')}
        </p>

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
