import React, { useCallback, useEffect, useState } from 'react';
import './shortcut.css';

// Standalone Mini-App screen for installing the two-command screenshot system.
// Opened from Telegram via startapp=shortcut. Replaces the old pile of chat
// messages: here the steps are laid out as ordered cards with media slots, the
// two install buttons, and an in-app "get pairing code" action. Media files are
// optional — a tile hides itself if the asset isn't uploaded yet.
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

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* ignore */ }
    const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
    try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
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

      {error ? <div className="sc-error">{error}</div> : null}

      <ol className="sc-steps">
        <li className="sc-step">
          <div className="sc-step-no">1</div>
          <div className="sc-step-body">
            <h2>Создайте альбом «Deutsch Queue»</h2>
            <p>Откройте приложение <b>«Фото»</b> → внизу <b>«Альбомы»</b> → <b>＋</b> → <b>«Новый альбом»</b>.
              Назовите его <b>точно так</b>: <code>Deutsch Queue</code> (с большой буквы, с пробелом).</p>
            <p className="sc-note">Сюда команда будет складывать скриншоты, а ночная — забирать их для перевода.</p>
            <MediaTile src="/onboarding/shortcut/album.jpg" type="image" caption="Новый альбом «Deutsch Queue»" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">2</div>
          <div className="sc-step-body">
            <h2>Команда «Скриншоты»</h2>
            <p>Нажмите кнопку ниже — откроется «Команды». Пролистайте вниз и нажмите
              <b> «Добавить команду»</b> (на всех экранах — соглашайтесь).</p>
            {collectorUrl
              ? <a className="sc-btn primary" href={collectorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Скриншоты»</a>
              : <div className="sc-btn disabled">Ссылка загружается…</div>}
            <p className="sc-sub-h">Привяжите её к быстрому запуску:</p>
            <p>🔹 <b>Двойное касание крышки</b> (любой iPhone): Настройки → Универсальный доступ →
              Касание → Касание задней панели → Двойное касание → выберите команду.</p>
            <p>🔹 <b>Кнопка «Действие»</b> (iPhone 15 Pro и новее): Настройки → Кнопка «Действие» →
              пролистайте до «Быстрая команда» → выберите команду.</p>
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
            <p>Нажмите кнопку ниже и так же добавьте вторую команду.</p>
            {processorUrl
              ? <a className="sc-btn primary" href={processorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Ночной перевод»</a>
              : <div className="sc-btn disabled">Ссылка загружается…</div>}
            <p className="sc-sub-h">Привяжите её к вашему аккаунту — один раз:</p>
            <p>1️⃣ Нажмите <b>«Получить код»</b> — он появится прямо здесь.<br />
              2️⃣ Запустите команду «Ночной перевод» вручную один раз — она попросит код, вставьте его.</p>
            <button type="button" className="sc-btn" onClick={getCode} disabled={pairingBusy}>
              {pairingBusy ? '⏳ Получаю код…' : (pairing ? '🔄 Новый код' : '🔑 Получить код')}
            </button>
            {pairing?.pairing_code ? <CopyCode code={pairing.pairing_code} /> : null}
            {pairingErr ? <div className="sc-error sm">{pairingErr}</div> : null}
            <p className="sc-note">⚠️ Код одноразовый и действует 24 часа — подключитесь сразу. Дальше код не нужен.</p>
            <MediaTile src="/onboarding/shortcut/pair.jpg" type="image" caption="Ввод кода при первом запуске" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">4</div>
          <div className="sc-step-body">
            <h2>Поставьте утренний запуск по времени</h2>
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
    </div>
  );
}
