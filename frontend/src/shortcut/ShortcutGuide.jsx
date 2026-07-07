import React, { useCallback, useEffect, useState } from 'react';
import './shortcut.css';

// Standalone Mini-App screen for setting up screenshot→vocab word capture.
// Opened via startapp=shortcut. ONE method only — the nightly batch (collector
// saves screenshots to an album, a morning automation OCRs the whole batch and
// POSTs it once to /api/shortcut/lookup). The instant per-screenshot OCR was
// dropped: it fired many small POSTs and was unstable; the nightly one sends a
// single big batch and is reliable. Media tiles auto-hide if the asset is missing.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

async function api(path) {
  const initData = tg?.initData || '';
  const res = await fetch(path, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

function MediaTile({ src, type, caption }) {
  const [hidden, setHidden] = useState(false);
  if (hidden) return null;
  return (
    <figure className="sc-media">
      {type === 'video' ? (
        <video src={src} controls playsInline preload="metadata" onError={() => setHidden(true)} />
      ) : (
        <img src={src} alt={caption || ''} loading="lazy" onError={() => setHidden(true)} />
      )}
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}

function CopyCode({ code }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      setTimeout(() => setCopied(false), 1800);
    } catch (_e) { /* clipboard blocked — user can select manually */ }
  }, [code]);
  return (
    <button type="button" className="sc-code" onClick={copy}>
      <span className="sc-code-val">{code}</span>
      <span className="sc-code-hint">{copied ? '✅ скопировано' : '📋 нажмите, чтобы скопировать'}</span>
    </button>
  );
}

export default function ShortcutGuide() {
  const [info, setInfo] = useState(null);
  const [error, setError] = useState('');
  const [pairing, setPairing] = useState(null);     // {pairing_code, expires_at}
  const [pairingBusy, setPairingBusy] = useState(false);
  const [pairingErr, setPairingErr] = useState('');
  const [botUrl, setBotUrl] = useState('');

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* ignore */ }
    // Force LIGHT — consistent with the onboarding wizard (owner: the whole
    // onboarding is light, so the Shortcut setup screen must not open dark).
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const d = await api('/api/webapp/shortcut/info');
        if (!off) setInfo(d);
      } catch (e) { if (!off) setError(String(e.message || e)); }
    })();
    return () => { off = true; };
  }, []);

  // Bot link — for the «перейти в приложение» button at the end.
  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        if (!off && d.bot_url) setBotUrl(d.bot_url);
      } catch (_e) { /* button falls back to closing the screen */ }
    })();
    return () => { off = true; };
  }, []);

  const openApp = useCallback(() => {
    const url = botUrl ? `${botUrl}?startapp=webapp` : '';
    try {
      if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
      else if (url) window.location.href = url;
      else tg?.close?.();
    } catch (_e) { try { tg?.close?.(); } catch (_e2) { /* noop */ } }
  }, [botUrl]);

  const getCode = useCallback(async () => {
    if (pairingBusy) return;
    setPairingBusy(true); setPairingErr('');
    try {
      const d = await api('/api/webapp/shortcut/pairing-code');
      setPairing(d);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
    } catch (e) {
      setPairingErr(String(e.message || e));
      try { tg?.HapticFeedback?.notificationOccurred?.('error'); } catch (_e) { /* noop */ }
    } finally { setPairingBusy(false); }
  }, [pairingBusy]);

  const collectorUrl = info?.collector_url || '';
  const processorUrl = info?.processor_url || '';

  return (
    <div className="sc-root">
      <header className="sc-hero">
        <div className="sc-hero-emoji">📲</div>
        <h1 className="sc-hero-title">Перевод по скриншотам</h1>
        <p className="sc-hero-sub">
          Ловите немецкие слова прямо во время рилсов и видео — а утром телефон сам всё переведёт и
          пришлёт в личку. Настройка один раз, по шагам. Только для iPhone.
        </p>
      </header>

      <div className="sc-note-box">
        ⏱ Это <b>разовая настройка на ~5–7 минут</b>. Чуть дольше обычного — зато потом ты одним
        движением ловишь слова прямо в рилсах, а перевод приходит сам. Настраивается один раз.
      </div>

      <div className="sc-flow-wrap">
        <p className="sc-flow-title">Что у нас получится — и дальше всё само:</p>
        <div className="sc-flow">
          <div className="sc-flow-step"><span className="sc-flow-emoji">📸</span><span>Увидел немецкое слово — нажал кнопку</span></div>
          <span className="sc-flow-arrow">↓</span>
          <div className="sc-flow-step"><span className="sc-flow-emoji">🗂</span><span>Скриншот сам ушёл в папку</span></div>
          <span className="sc-flow-arrow">↓</span>
          <div className="sc-flow-step"><span className="sc-flow-emoji">🌙</span><span>Утром телефон всё перевёл</span></div>
          <span className="sc-flow-arrow">↓</span>
          <div className="sc-flow-step"><span className="sc-flow-emoji">💾</span><span>Слова пришли в чат — сохранил в словарь</span></div>
        </div>
      </div>

      {error ? <div className="sc-error">{error}</div> : null}

      <ol className="sc-steps">
        <li className="sc-step">
          <div className="sc-step-no">1</div>
          <div className="sc-step-body">
            <h2>Создайте альбом «Deutsch Queue»</h2>
            <p className="sc-why">💡 Зачем: это папка, куда телефон складывает скриншоты со словами — чтобы потом перевести их все разом.</p>
            <p>Откройте приложение <b>«Фото»</b> → внизу <b>«Альбомы»</b> → <b>＋</b> → <b>«Новый альбом»</b>.
              Назовите его <b>точно так</b>: <code>Deutsch Queue</code> (с большой буквы, с пробелом).</p>
            <p className="sc-note">Сюда команда будет складывать скриншоты, а ночная — забирать их для перевода.</p>
            <MediaTile src="/onboarding/shortcut/album.jpg" type="image" caption="Новый альбом «Deutsch Queue»" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">2</div>
          <div className="sc-step-body">
            <h2>Команда «Screenshot to Deutsch Queue»</h2>
            <p className="sc-why">💡 Зачем: это твоя «ловилка». Нажал кнопку — она сама сделала скриншот и убрала его в папку, не отвлекая тебя от видео.</p>
            <p>Нажмите кнопку ниже. Соглашайтесь с предложенными вариантами
              (<b>«Добавить команду»</b> / «Add Shortcut» / «Kurzbefehl hinzufügen») — и команда
              добавится к вам в телефон.</p>
            {collectorUrl
              ? <a className="sc-btn primary" href={collectorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Screenshot to Deutsch Queue»</a>
              : <div className="sc-btn disabled">Ссылка загружается…</div>}
            <p className="sc-sub-h">Привяжите её к быстрому запуску:</p>
            <p>🔹 <b>Двойное касание крышки</b> (любой iPhone): Настройки → Универсальный доступ →
              Касание → Касание задней панели → Двойное касание → выберите команду «Screenshot to Deutsch Queue».</p>
            <p>🔹 <b>Кнопка «Действие»</b> (iPhone 15 Pro и новее): Настройки → Кнопка «Действие» →
              пролистайте до «Быстрая команда» → выберите команду «Screenshot to Deutsch Queue».</p>
            <div className="sc-media-row">
              <MediaTile src="/onboarding/shortcut/back_tap.mp4" type="video" caption="Двойное касание крышки" />
              <MediaTile src="/onboarding/shortcut/action_button.mp4" type="video" caption="Кнопка «Действие»" />
            </div>
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">3</div>
          <div className="sc-step-body">
            <h2>Команда «Ночной перевод»</h2>
            <p className="sc-why">💡 Зачем: утром эта команда сама берёт все накопленные за день скриншоты, распознаёт на них текст и присылает тебе слова в личку — останется только сохранить нужные.</p>
            <p>Нажмите кнопку ниже и добавьте эту команду так же (соглашайтесь с вариантами).</p>
            {processorUrl
              ? <a className="sc-btn primary" href={processorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Ночной перевод»</a>
              : <div className="sc-btn disabled">Ссылка загружается…</div>}
            <p className="sc-sub-h">Теперь подключим её к твоему аккаунту — это делается один раз.</p>
            <p>Код нужен для безопасности: он подтверждает, что это именно ты, чтобы твои слова приходили только тебе и никому другому.</p>
            <p><b>1.</b> Нажмите <b>«Получить код»</b> ниже и скопируйте его:</p>
            <button type="button" className="sc-btn" onClick={getCode} disabled={pairingBusy}>
              {pairingBusy ? '⏳ Получаю код…' : (pairing ? '🔄 Новый код' : '🔑 Получить код')}
            </button>
            {pairing?.pairing_code ? <CopyCode code={pairing.pairing_code} /> : null}
            {pairingErr ? <div className="sc-error sm">{pairingErr}</div> : null}
            <p className="sc-note">⚠️ Код действует 24 часа и нужен только один раз — дальше не понадобится.</p>
            <p><b>2.</b> Откройте приложение <b>«Команды»</b>, запустите <b>«Ночной перевод»</b> один раз вручную — команда попросит код, вставьте его. Готово ✅</p>
            <MediaTile src="/onboarding/shortcut/pair.jpg" type="image" caption="Ввод кода при первом запуске" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">4</div>
          <div className="sc-step-body">
            <h2>Поставьте утренний запуск по времени</h2>
            <p className="sc-why">💡 Зачем: чтобы перевод запускался сам каждое утро — тебе не нужно про него помнить и что-то нажимать.</p>
            <p>В «Командах» откройте вкладку <b>«Автоматизация»</b> → <b>＋</b> →
              <b> «Создать автоматизацию для себя»</b> → <b>«Время суток»</b>.</p>
            <p>Поставьте утро — <b>06:30–07:30</b>, повтор <b>ежедневно</b>. Действие — запустить команду
              <b> «Ночной перевод»</b>. Выключите «Спрашивать до запуска», чтобы команда шла сама.</p>
            <p className="sc-note">⏰ Утром 06:30–07:30 сервер свободен — перевод проходит быстро и без сбоев.
              Слова прилетят в личку примерно через полторы минуты после запуска.</p>
            <MediaTile src="/onboarding/shortcut/automation.jpg" type="image" caption="Автоматизация на 06:30–07:30" />
          </div>
        </li>
      </ol>

      <section className="sc-done">
        <h2>✅ Как это работает каждый день</h2>
        <p>🎬 Видите немецкое слово в видео или чате → кнопка действия или двойное касание крышки →
          скриншот тихо уходит в альбом «Deutsch Queue».</p>
        <p>🌙 Утром команда сама всё переводит → слова приходят вам в личку.</p>
        <p>💾 Останется нажать сохранить — выбранные слова лягут в словарь.</p>
      </section>

      <section className="sc-android">
        <p>📱 <b>Другой телефон (Android и т.п.)?</b> Команды не нужны — пишите боту слово, пересылайте
          немецкий текст или вставляйте целый кусок текста: бот сам выберет слова под ваш уровень.</p>
      </section>

      <button type="button" className="sc-btn primary sc-goapp" onClick={openApp}>
        🎯 Перейти в приложение
      </button>
    </div>
  );
}
