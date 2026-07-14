import React from 'react';
import { createPortal } from 'react-dom';
import './ReaderGuideModal.css';

/**
 * «Как читать и слушать» — a calm, grandma-proof explainer for the reader.
 * Plain what/where/how/why for a non-tech newcomer: read for free, listen to the
 * classics for free, pay once to narrate your OWN book, top up the wallet.
 * Self-contained light paper card (readable over the dark reader), portal to body.
 */
export default function ReaderGuideModal({ isOpen, onClose, tr }) {
  if (!isOpen) return null;
  const target = typeof document !== 'undefined' ? document.body : null;

  const steps = [
    {
      accent: 'read', emoji: '📖',
      title: tr('Читай на немецком', 'Auf Deutsch lesen'),
      body: tr(
        'Открой книгу или статью. Нажми на любое слово — сразу увидишь перевод.',
        'Öffne ein Buch oder einen Artikel. Tippe auf ein Wort — die Übersetzung erscheint sofort.',
      ),
    },
    {
      accent: 'classics', emoji: '🏛️',
      title: tr('Классика — бесплатно', 'Klassiker — kostenlos'),
      body: tr(
        'В полке «Классика» книги можно читать и слушать голосом бесплатно, сколько хочешь. Открой книгу и нажми ▶.',
        'Im Regal «Klassiker» kannst du Bücher unbegrenzt kostenlos lesen und anhören. Öffne ein Buch und tippe auf ▶.',
      ),
    },
    {
      accent: 'audio', emoji: '🎧',
      title: tr('Озвучка своей книги', 'Dein Buch vertonen'),
      body: tr(
        'Свою книгу или статью тоже можно слушать голосом. Это платно — один раз за книгу. Заплатил раз — слушаешь её всегда, без ограничений.',
        'Auch dein eigenes Buch kannst du anhören. Das kostet einmalig pro Buch. Einmal gezahlt — dann für immer und unbegrenzt hören.',
      ),
    },
    {
      accent: 'wallet', emoji: '💳',
      title: tr('Кошелёк', 'Guthaben'),
      body: tr(
        'Чтобы озвучить свою книгу, пополни кошелёк (от €3). Деньги спишутся только когда ты сам нажмёшь «Озвучить».',
        'Um dein Buch zu vertonen, lade das Guthaben auf (ab €3). Abgebucht wird erst, wenn du selbst auf «Vertonen» tippst.',
      ),
    },
  ];

  const node = (
    <div className="rdg-overlay" role="dialog" aria-modal="true">
      <button
        type="button"
        className="rdg-backdrop"
        aria-label={tr('Закрыть', 'Schließen')}
        onClick={onClose}
      />
      <div className="rdg-card" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="rdg-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>

        <div className="rdg-scroll">
          <div className="rdg-hero">
            <div className="rdg-hero-emoji" aria-hidden="true">📚</div>
            <h3 className="rdg-title">{tr('Как читать и слушать', 'Lesen & Hören')}</h3>
            <p className="rdg-subtitle">{tr('Коротко и просто', 'Kurz und einfach')}</p>
          </div>

          <ol className="rdg-steps">
            {steps.map((s, i) => (
              <li key={s.accent} className={`rdg-step is-${s.accent}`}>
                <span className="rdg-step-icon" aria-hidden="true">
                  <span className="rdg-step-emoji">{s.emoji}</span>
                  <span className="rdg-step-num">{i + 1}</span>
                </span>
                <span className="rdg-step-text">
                  <span className="rdg-step-title">{s.title}</span>
                  <span className="rdg-step-body">{s.body}</span>
                </span>
              </li>
            ))}
          </ol>

          <div className="rdg-reassure">
            <span className="rdg-reassure-emoji" aria-hidden="true">✋</span>
            <span>
              {tr(
                'Чтение и вся классика — всегда бесплатны. Платишь, только если сам захочешь озвучить свою книгу.',
                'Lesen und alle Klassiker sind immer gratis. Du zahlst nur, wenn du dein eigenes Buch vertonen möchtest.',
              )}
            </span>
          </div>

          <div className="rdg-howto">
            <span className="rdg-howto-label">{tr('Как включить озвучку', 'So startest du das Audio')}</span>
            <span className="rdg-howto-steps">
              {tr('Открой книгу', 'Buch öffnen')}
              <span className="rdg-howto-arrow" aria-hidden="true">→</span>
              {tr('внизу нажми', 'unten tippen')}
              <span className="rdg-howto-play">▶</span>
            </span>
          </div>
        </div>

        <button type="button" className="rdg-done" onClick={onClose}>
          {tr('Понятно', 'Alles klar')}
        </button>
      </div>
    </div>
  );

  return target ? createPortal(node, target) : node;
}
