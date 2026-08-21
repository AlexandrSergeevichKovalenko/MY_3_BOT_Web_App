import React, { useState } from 'react';
import './saveWordHint.css';

/**
 * Плашка после сохранения: «такого слова в немецком нет, ты имел в виду …?»
 *
 * Зачем именно здесь и именно сейчас. Владелец 20.08.2026: «спрашивать сразу при
 * сохранении, а не через неделю — контекст свежий, через неделю человек не вспомнит,
 * откуда взял слово». Сохранение при этом НЕ ждёт проверку: карточка уже у человека,
 * плашка приходит следом.
 *
 * Когда плашки НЕТ:
 *   • справочник подтвердил слово или подтвердил починку («Argernisse» → «Ärgernisse») —
 *     это факт, а не решение, правим молча и человека не трогаем;
 *   • человек ушёл с экрана — догоняющих сообщений нет, слово копится и попадёт
 *     в проверку, которая приходит два раза в неделю.
 *
 * Три действия, а не два: владелец 21.08.2026 — «пользователь может вписать что-то
 * третье, своё, вместе с переводом». Поэтому «свой вариант» открывает два поля.
 */
export default function SaveWordHint({ word, suggestion, why, onApply, onDismiss }) {
  const [own, setOwn] = useState(false);
  const [ownWord, setOwnWord] = useState(suggestion || word || '');
  const [ownTranslation, setOwnTranslation] = useState('');
  const [busy, setBusy] = useState(false);

  const apply = async (action, text, translation) => {
    setBusy(true);
    try {
      await onApply({ action, text, translation });
    } finally {
      setBusy(false);
    }
  };

  if (own) {
    return (
      <div className="swh">
        <div className="swh-top"><span>✏️</span><span className="swh-title">Впиши, как правильно</span></div>
        <div className="swh-own">
          <label>Слово по-немецки
            <input value={ownWord} onChange={(e) => setOwnWord(e.target.value)} />
          </label>
          <label>Перевод
            <input value={ownTranslation} onChange={(e) => setOwnTranslation(e.target.value)}
                   placeholder="как переводится" />
          </label>
        </div>
        <div className="swh-actions">
          <button type="button" className="swh-btn swh-btn-main" disabled={busy || !ownWord.trim()}
                  onClick={() => apply('manual', ownWord.trim(), ownTranslation.trim())}>
            Сохранить свой вариант
          </button>
          <button type="button" className="swh-btn" disabled={busy} onClick={() => setOwn(false)}>
            Назад
          </button>
        </div>
        <p className="swh-later">Мы проверим написание по справочнику и достроим карточку.</p>
      </div>
    );
  }

  return (
    <div className="swh">
      <div className="swh-top"><span>⚠️</span><span className="swh-title">Такого слова в немецком нет</span></div>
      <p className="swh-body">
        {why || 'Мы не нашли это слово в немецких справочниках.'}
        {suggestion ? <> Скорее всего, ты имел в виду <b>{suggestion}</b>.</> : null}
      </p>
      <div className="swh-actions">
        {suggestion ? (
          <button type="button" className="swh-btn swh-btn-main" disabled={busy}
                  onClick={() => apply('fixed', suggestion, '')}>
            Да, это «{suggestion}»
          </button>
        ) : null}
        <button type="button" className="swh-btn" disabled={busy}
                onClick={() => apply('keep', word, '')}>
          Оставить как есть
        </button>
        <button type="button" className="swh-btn" disabled={busy} onClick={() => setOwn(true)}>
          Свой вариант
        </button>
      </div>
      <p className="swh-later">
        <button type="button" className="swh-link" onClick={onDismiss}>
          Решу позже
        </button>
        {' '}— слово придёт в проверку.
      </p>
    </div>
  );
}
