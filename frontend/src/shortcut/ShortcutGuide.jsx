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
  // Only render once the URL is confirmed to be a REAL media file — a missing asset is
  // served as index.html (200 text/html) and a bare <video> would spin forever without
  // erroring. HEAD-check the content-type and hide otherwise. (Owner's videos, once
  // added, just start appearing.)
  const [hidden, setHidden] = useState(false);
  const [ready, setReady] = useState(false);
  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const res = await fetch(src, { method: 'HEAD' });
        const ct = (res.headers.get('content-type') || '').toLowerCase();
        const ok = res.ok && ct.startsWith(type === 'video' ? 'video/' : 'image/');
        if (!off) { setReady(ok); setHidden(!ok); }
      } catch (_e) {
        if (!off) setReady(true);
      }
    })();
    return () => { off = true; };
  }, [src, type]);
  if (hidden || !ready) return null;
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

// Inline copyable chip (e.g. the exact album name) — tap to copy, no manual typing.
function CopyChip({ text }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      setTimeout(() => setCopied(false), 1600);
    } catch (_e) { /* clipboard blocked — user can select manually */ }
  }, [text]);
  return (
    <button type="button" className="sc-copychip" onClick={copy} title="Скопировать">
      <span className="sc-copychip-val">{text}</span>
      <span className="sc-copychip-ic">{copied ? '✅' : '📋'}</span>
    </button>
  );
}

// Multilingual iOS menu path/name — each language on its OWN line (flag + text), so
// the user reads the one matching their phone language and the flag never orphans.
function NavLangs({ ru, de, en }) {
  return (
    <div className="sc-langs">
      {ru ? <p className="sc-langline"><span className="sc-flag">🇷🇺</span> {ru}</p> : null}
      {de ? <p className="sc-langline"><span className="sc-flag">🇩🇪</span> {de}</p> : null}
      {en ? <p className="sc-langline"><span className="sc-flag">🇬🇧</span> {en}</p> : null}
    </div>
  );
}

// A neat blue clickable link that opens a reference screenshot full-size in a new tab.
function PhotoLink({ src, children }) {
  return (
    <a className="sc-photolink" href={src} target="_blank" rel="noreferrer">
      <span className="sc-photolink-ico">🖼</span>
      <span className="sc-photolink-text">{children}</span>
      <span className="sc-photolink-open">открыть ↗</span>
    </a>
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

      <MediaTile src="/onboarding/shortcut/full_process.mp4" type="video" caption="🎬 Полное видео: вся установка по шагам — рекомендуем посмотреть перед началом" />

      {error ? <div className="sc-error">{error}</div> : null}

      <div className="sc-note-box">
        📱 Названия пунктов и путей в меню iPhone зависят от языка телефона. Ниже они даны
        на трёх языках, каждый со своей строки: <b>🇷🇺 русский · 🇩🇪 немецкий · 🇬🇧 английский</b>.
        Ориентируйся на тот, на котором твой телефон.
      </div>

      <ol className="sc-steps">
        <li className="sc-step">
          <div className="sc-step-no">1</div>
          <div className="sc-step-body">
            <h2>Сначала создайте альбом «Deutsch Queue»</h2>
            <p className="sc-why">💡 Зачем: это место, куда телефон складывает скриншоты со словами — чтобы потом перевести их все разом. Альбом должен существовать заранее, иначе команде некуда сохранять.</p>
            <p><b>Это самый первый шаг</b> — сделайте его <b>до установки команд</b>.</p>
            <p>Откройте приложение <b>«Фото»</b> и создайте новый альбом (именно <b>альбом</b>, а <b>не «Новая папка»</b> — это разные вещи). Путь:</p>
            <NavLangs
              ru="«Фото» → «Альбомы» → ＋ → «Новый альбом»"
              de="«Fotos» → «Alben» → ＋ → «Neues Album» (не «Neuer Ordner»!)"
              en="«Photos» → «Albums» → ＋ → «New Album» (не «New Folder»!)"
            />
            <p>Назовите его <b>точно так, буква в букву</b> — нажмите, чтобы <b className="sc-key">скопировать</b> и вставить (не набирайте вручную):</p>
            <CopyChip text="Deutsch Queue" />
            <p className="sc-note">С большой буквы, с пробелом посередине. Ошибётесь в названии — команда не найдёт альбом.</p>
            <p className="sc-note">Сюда команда будет складывать скриншоты, а «Ночной Переводчик» — забирать их для перевода.</p>
            <MediaTile src="/onboarding/shortcut/album.jpg" type="image" caption="Новый альбом (не папка!) «Deutsch Queue»" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">2</div>
          <div className="sc-step-body">
            <h2>Команда «Сохраняем Скриншот В Папку»</h2>
            <p className="sc-why">💡 Зачем: это твоя «ловилка». Нажал кнопку — она сама сделала скриншот и убрала его в папку, не отвлекая тебя от видео.</p>
            <p>Нажмите кнопку ниже. Соглашайтесь с предложенными вариантами
              (<b className="sc-key">«Добавить команду»</b> / «Add Shortcut» / «Kurzbefehl hinzufügen») — и команда
              добавится к вам в телефон.</p>
            {collectorUrl
              ? <a className="sc-btn primary" href={collectorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Сохраняем Скриншот В Папку»</a>
              : <div className="sc-btn disabled">Ссылка загружается…</div>}
            <p className="sc-sub-h">Привяжите её к быстрому запуску</p>
            <p>💡 Зачем: чтобы потом одним движением ловить слова, не отвлекаясь. Выберите способ, который вам удобнее и который поддерживает ваша модель iPhone:</p>
            <p>🔹 <b>Двойное касание крышки</b> (любой iPhone). Путь в настройках:</p>
            <NavLangs
              ru="Настройки → Универсальный доступ → Касание → Касание задней панели → Двойное касание → команда «Сохраняем Скриншот В Папку»"
              de="Einstellungen → Bedienungshilfen → Tippen → Auf Rückseite tippen → Doppeltippen"
              en="Settings → Accessibility → Touch → Back Tap → Double Tap"
            />
            <p>🔹 <b>Кнопка «Действие»</b> (iPhone 15 Pro и новее). Путь:</p>
            <NavLangs
              ru="Настройки → Кнопка «Действие» → пролистайте до «Быстрая команда» → команда «Сохраняем Скриншот В Папку»"
              de="Einstellungen → Aktionstaste → «Kurzbefehl» → …"
              en="Settings → Action Button → «Shortcut» → …"
            />
            <div className="sc-media-row">
              <MediaTile src="/onboarding/shortcut/back_tap.mp4" type="video" caption="Двойное касание крышки" />
              <MediaTile src="/onboarding/shortcut/action_button.mp4" type="video" caption="Кнопка «Действие»" />
            </div>
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">3</div>
          <div className="sc-step-body">
            <h2>Команда «Ночной Переводчик»</h2>
            <p className="sc-why">💡 Зачем: утром эта команда забирает скриншоты из папки, распознаёт на них текст и присылает тебе слова в личку — останется только сохранить нужные.</p>
            <p className="sc-note">📸 За одну отправку команда берёт из папки до <b>25</b> фото. <b>Pro — ровно 2 отправки в день</b> (в каждой до 25 фото), в утренние часы. ❗️Очень важно <b>не запускать 3-й раз</b>: эти фото просто удалятся, а перевод не придёт. На бесплатном — <b>5 пробных</b> запусков всего, чтобы попробовать функцию (дальше на Pro).</p>
            <p>Нажмите кнопку ниже и добавьте эту команду так же (соглашайтесь с вариантами).</p>
            {processorUrl
              ? <a className="sc-btn primary" href={processorUrl} target="_blank" rel="noreferrer">📲 Установить команду «Ночной Переводчик»</a>
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
            <p><b>2.</b> Теперь запустите эту команду один раз — чтобы ввести код:</p>
            <ul className="sc-sublist">
              <li>Откройте приложение (есть на каждом iPhone):
                <NavLangs ru="«Команды»" de="«Kurzbefehle»" en="«Shortcuts»" />
              </li>
              <li>Внизу выберите раздел:
                <NavLangs ru="«Медиатека»" de="«Mediathek»" en="«Gallery»" />
              </li>
              <li>Вверху нажмите:
                <NavLangs ru="«Все команды»" de="«Alle Kurzbefehle»" en="«All Shortcuts»" />
              </li>
              <li>Найдите установленную команду <b>«Ночной Переводчик»</b> и нажмите <b>по центру плитки</b> (не на три точки — просто по центру). Команда запустится.</li>
              <li>Появится окошко — вставьте <b>скопированный код</b> и подтвердите кнопкой под ним.</li>
            </ul>
            <p>Готово ✅ Команда подключена к вашему аккаунту. Дальше — важный шаг: первый запуск вручную (ниже).</p>
            <MediaTile src="/onboarding/shortcut/pair.jpg" type="image" caption="Ввод кода при первом запуске" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">4</div>
          <div className="sc-step-body">
            <h2>Важные настройки: разрешите авто-удаление обработанных фото 🧹</h2>
            <p className="sc-why">💡 Зачем: чтобы телефон сам удалял уже переведённые скриншоты из альбома (иначе они копятся) и <b>не переспрашивал</b> каждый раз. Без этого автоматика будет останавливаться на вопросах — а нам нужно, чтобы всё шло само.</p>

            <p className="sc-sub-h">Часть 1 — в Настройках телефона</p>
            <p>Открой <b>Настройки телефона</b> → пролистай до пункта «Быстрые команды»:</p>
            <NavLangs ru="Настройки → «Быстрые команды»" de="Einstellungen → «Kurzbefehle»" en="Settings → «Shortcuts»" />
            <p>Включи <b>iCloud-синхронизацию</b> и <b>«Приватный доступ»</b>, затем внизу открой «Дополнительно»:</p>
            <NavLangs ru="«Дополнительно»" de="«Erweitert»" en="«Advanced»" />
            <p>Включи все переключатели — особенно <b className="sc-key">«Разрешить удаление без подтверждения»</b> и <b className="sc-key">«Разрешить удаление большого объёма данных»</b>:</p>
            <NavLangs ru="«Удаление без подтверждения» + «Удаление большого объёма»" de="«Löschen ohne Bestätigung» + «Löschen großer Datenmengen»" en="«Delete Without Confirmation» + «Delete Large Amounts»" />
            <PhotoLink src="/onboarding/shortcut/perm_settings_main.jpg">Фото 1 — переключатели в «Быстрые команды»</PhotoLink>
            <PhotoLink src="/onboarding/shortcut/perm_settings_advanced.jpg">Фото 2 — переключатели в «Дополнительно»</PhotoLink>
            <MediaTile src="/onboarding/shortcut/perm_settings.mp4" type="video" caption="🎬 Как включить эти настройки в Настройках телефона" />

            <p className="sc-sub-h">Часть 2 — в приложении «Быстрые команды»</p>
            <p>Открой приложение <b>«Быстрые команды»</b> (🇩🇪 «Kurzbefehle» · 🇬🇧 «Shortcuts»), найди команду <b>«Ночной Переводчик»</b>, <b className="sc-key">нажми и подержи</b> по центру плитки — появится меню. Выбери «Детали»:</p>
            <NavLangs ru="«Детали»" de="«Details»" en="«Details»" />
            <p>Открой «Конфиденциальность»:</p>
            <NavLangs ru="«Конфиденциальность»" de="«Datenschutz»" en="«Privacy»" />
            <p>Внизу, в разделе «Разрешить удаление данных», для <b>«Фото»</b> выбери <b className="sc-key">«Удалять без запроса»</b>:</p>
            <NavLangs ru="Фото → «Удалять без запроса»" de="Fotos → «Ohne Nachfrage löschen»" en="Fotos → «Delete Without Asking»" />
            <PhotoLink src="/onboarding/shortcut/perm_shortcut_menu.jpg">Фото 3 — меню по долгому нажатию → «Детали»</PhotoLink>
            <PhotoLink src="/onboarding/shortcut/perm_shortcut_privacy.jpg">Фото 4 — «Фото»: «Удалять без запроса»</PhotoLink>
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">5</div>
          <div className="sc-step-body">
            <h2>Первый запуск: подтвердите разрешения (один раз)</h2>
            <p className="sc-why">💡 Зачем: после установки нужно один раз пройти команды вручную. iPhone в первый раз спрашивает разрешение на каждое действие — вы отвечаете <b className="sc-key">«Всегда разрешать»</b>, и дальше всё идёт само, без вопросов. Без этого разового прогона автоматика не заработает.</p>

            <p className="sc-sub-h">Часть 1 — проверьте «ловилку» скриншотов:</p>
            <p>Откройте любую соцсеть (например, <b>Instagram</b>), включите любое видео. Как только на экране появится
              немецкое слово или фраза — <b className="sc-key">поставьте видео на паузу</b> и нажмите свою кнопку (кнопка «Действие»
              или двойное касание крышки — ту, что вы привязали в шаге 2). Какое именно слово — сейчас неважно,
              возьмите первые попавшиеся, просто для теста.</p>
            <p>Первый раз iPhone спросит, разрешить ли сохранение: «Не разрешать» / «Разрешить один раз» /
              <b className="sc-key"> «Всегда разрешать»</b>. Выберите <b className="sc-key">«Всегда разрешать»</b> — тогда телефон больше не будет
              спрашивать, и скриншоты будут сами уходить в ваш альбом.</p>
            <p>Сделайте так ещё пару скриншотов для теста. Больше телефон спрашивать не будет — скриншот сразу
              и сам уходит в альбом «Deutsch Queue».</p>
            <p>📂 Загляните в альбом <b>«Deutsch Queue»</b> — там должны лежать ваши тестовые скриншоты.
              Лежат — значит «ловилка» настроена верно, и больше в неё лезть не нужно: вы просто смотрите
              любимое, а скриншоты копятся сами.</p>

            <p className="sc-sub-h">Часть 2 — запустите «Ночной Переводчик» вручную один раз:</p>
            <p>Вернитесь в приложение <b>«Команды»</b> (там, где вы устанавливали обе команды), найдите команду
              <b> «Ночной Переводчик»</b> и <b className="sc-key">нажмите по центру плитки</b> (не на три точки в углу — просто по центру).
              Команда запустится, вы это увидите. Она задаст два вопроса:</p>
            <ul className="sc-sublist">
              <li>Разрешить <b>отправку</b> ваших фотографий на обработку? → нажмите <b className="sc-key">«Всегда разрешать»</b>.</li>
              <li>Разрешить <b>удаление</b> обработанных фотографий из альбома? → нажмите <b className="sc-key">«Всегда разрешать»</b> (или <b className="sc-key">«Всегда удалять»</b>).</li>
            </ul>
            <p className="sc-note">Так команда сама будет отправлять скриншоты на перевод и убирать из альбома уже
              обработанные — чтобы альбом не забивался старыми фото.</p>

            <p className="sc-sub-h">Проверка настроек (по желанию):</p>
            <p><b>Нажмите и чуть задержите</b> палец на команде — выскочит меню с её настройками. Сверьте по видео,
              что разрешения на отправку и удаление стоят <b>«Всегда»</b>. После шагов выше так и будет — это просто
              финальная проверка.</p>

            <p>Готово ✅ Вы один раз прошли обе команды: сначала разрешили сохранять скриншоты в альбом, затем
              запустили «Ночной Переводчик», который берёт фото, отправляет на перевод и чистит альбом. Дальше
              всё работает <b>само</b>.</p>
            <MediaTile src="/onboarding/shortcut/first_run.mp4" type="video" caption="🎬 Первый запуск и подтверждения — по шагам" />
          </div>
        </li>

        <li className="sc-step">
          <div className="sc-step-no">6</div>
          <div className="sc-step-body">
            <h2>Поставьте утренний запуск по времени</h2>
            <p className="sc-why">💡 Зачем: чтобы перевод запускался сам каждое утро — тебе не нужно про него помнить и что-то нажимать.</p>
            <div className="sc-danger">
              <p><b>⏰ Важно: обработка работает только в утренние часы — 05:00–09:00.</b> Если запустить в другое время — перевод <b>не придёт</b> (сервер его не обработает). Поэтому поставь автозапуск строго на 05:00–09:00 — это <b>твоя настройка и твоя ответственность</b>. Ставь правильные часы, и всё пойдёт само.</p>
              <p>Почему так: распознавание и перевод — тяжёлая работа. Если запускать её днём, она будет замедлять приложение и портить впечатление тебе же в активные часы. Поэтому отправка стоит на самые спокойные утренние часы.</p>
              <p>Исключение — <b>первый день после установки</b>: в этот день можно запускать в любое время, чтобы спокойно всё настроить и проверить.</p>
            </div>
            <p>В приложении «Команды» — путь:</p>
            <NavLangs
              ru="«Автоматизация» → ＋ → «Создать автоматизацию для себя» → «Время суток»"
              de="«Automation» → ＋ → «Persönliche Automation erstellen» → «Tageszeit»"
              en="«Automation» → ＋ → «Create Personal Automation» → «Time of Day»"
            />
            <p>Поставьте утро — <b>06:30–07:30</b>, повтор <b>ежедневно</b>. Действие — запустить команду
              <b> «Ночной Переводчик»</b>. Выключите «Спрашивать до запуска», чтобы команда шла сама.</p>
            <p className="sc-note">👀 Этот шаг тоже детально показан на видео.</p>
            <p className="sc-note">⏰ Утром 06:30–07:30 сервер свободен — перевод проходит быстро и без сбоев.
              Слова прилетят в личку примерно через полторы минуты после запуска.</p>
            <MediaTile src="/onboarding/shortcut/automation.jpg" type="image" caption="Автоматизация на 06:30–07:30" />
          </div>
        </li>
      </ol>

      <section className="sc-done">
        <h2>✅ Как понять, что всё готово</h2>
        <p>📂 У вас появился альбом <b>«Deutsch Queue»</b> — туда падают скриншоты.</p>
        <p>🔑 В <b>iCloud Drive → папка «Shortcuts»</b> появился маленький файл <b>«Shortcut install Token»</b>.
          Что внутри — неважно: это как ваш «паспорт», по нему система понимает, что это именно вы.
          <b> Никому его не показывайте.</b></p>
        <p>💬 Примерно через <b>90 секунд</b> после первого запуска в личку с ботом придёт сообщение
          <b> «Ночная подборка»</b> — количество слов и сами слова с переводом. (В эти 90 секунд можно доснять
          ещё скриншотов — тогда в подборке будет больше слов.)</p>
        <p>✅ Под сообщением — <b>галочки</b>: отметьте нужные слова и нажмите <b>«Сохранить выбранные»</b>.
          Слова сразу попадут в ваш <b>словарь</b>. Можно выбирать вручную, а можно довериться системе — она
          сама подаёт слова по важности для разговора (по частоте использования).</p>
        <MediaTile src="/onboarding/shortcut/result.jpg" type="image" caption="«Ночная подборка» в личке + галочки для сохранения" />
      </section>

      <section className="sc-done">
        <h2>✅ Как пользоваться каждый день</h2>
        <p>🎬 Видишь немецкое слово в рилсе → <b className="sc-key">поставь на паузу</b> (iPhone делает скриншот
          чуть с задержкой — на паузе точно поймаешь кадр) → нажми свою кнопку. Двойное касание
          крышки — просто дважды стукни по задней крышке. Кнопка «Действие» — нажми и <b>подержи
          секунду-полторы</b>. И всё — спокойно смотришь дальше, никуда переходить не нужно.</p>
        <p>⚠️ Работает <b>только твоя кнопка</b>. Обычные скриншоты старой комбинацией клавиш в
          эту схему <b>не попадут</b>.</p>
        <p>🌙 Утром команда сама всё переведёт → слова придут в личку. Что понравилось — сохраняешь
          одной кнопкой.</p>
        <p>📖 Сохранённые слова ложатся в твой <b>словарь</b> — учи их когда удобно. Можно выбрать
          слова вручную в разделе «Словарь», а можно довериться системе: она сама составит список
          для изучения по частоте встречаемости.</p>
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
