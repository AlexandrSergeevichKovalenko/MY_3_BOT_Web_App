import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './onboarding.css';

// Standalone Mini-App first-run onboarding. Opened from Telegram via
// startapp=onboarding (and later reused as the «🎬 Как пользоваться» hub).
// Stage 0 = SCAFFOLD: real step controls land in Stage 1; here we ship the
// frame — progress bar, ←/→ nav, the core-gate (language + dictionary are
// mandatory), tier-aware placeholders, and the resume/complete wiring.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
// Opened in a plain browser (shareable «/tour» presentation) — no Telegram, no
// initData → free navigation, no saving, install-the-bot CTA at the finale.
const IS_PUBLIC = !(tg && tg.initData);

// Opened from the app's own home-screen icon (standalone PWA), outside Telegram. This
// viewer ALREADY has the bot — the Telegram SDK is loaded on every page so there's no
// initData here (→ IS_PUBLIC), but pitching «install the bot» at the finale would be
// nonsense. So a PWA tour ends with a plain «Закрыть» that returns to the app instead.
const IS_STANDALONE_PWA = (() => {
  if (typeof window === 'undefined') return false;
  try {
    const byMedia = typeof window.matchMedia === 'function'
      && window.matchMedia('(display-mode: standalone)').matches;
    const byNav = window.navigator?.standalone === true;
    return Boolean(byMedia || byNav);
  } catch (_e) { return false; }
})();
const IS_PWA_TOUR = IS_PUBLIC && IS_STANDALONE_PWA;

// UI language — same source of truth as the main app (localStorage 'ui_lang').
// Fresh users (never opened the main app) have nothing stored → fall back to the
// Telegram interface language, else Russian. Switchable in-wizard (see LangToggle).
const LANG = (() => {
  try {
    const stored = (localStorage.getItem('ui_lang') || '').toLowerCase();
    if (stored === 'de' || stored === 'ru') return stored;
  } catch (_e) { /* noop */ }
  try {
    const tgLang = String(tg?.initDataUnsafe?.user?.language_code || '').toLowerCase();
    if (tgLang.startsWith('de')) return 'de';
  } catch (_e) { /* noop */ }
  return 'ru';
})();
const t = (ru, de) => (LANG === 'de' ? de : ru);

// R2-hosted onboarding clips. Same insertion pattern as the existing tiles: a small,
// neat inline <video> (capped at 60vh, letterboxed) with native controls → the
// fullscreen button expands it. Each empty slot's MediaTile simply isn't rendered.
const R2 = 'https://pub-6ebcbf6d9aec43d488d3f6a3ba222c14.r2.dev/onboarding_videos_r2';
const MEDIA = {
  install_ios:     `${R2}/how_to_setup_app_icon_on_a_screenphone/install_app_ios.mp4`, // install_app (iPhone)
  forward_chat:  `${R2}/forward_from_chat_and_learning_words/forward_chat_razbor.mp4`, // howto_words
  forward_selection: `${R2}/forward_from_chat_and_learning_words/forward_word_demo.mp4`, // howto_words — выделил текст в браузере/книге → «Поделиться» → бот
  morning_words: `${R2}/morning_words_arrival/morning_words_arrival.mp4`,             // howto_morning
  learn_words:   `${R2}/forward_from_chat_and_learning_words/learn_saved_words.mp4`,   // howto_learn
  world_news:    `${R2}/youtube_video_morningnews/morning_news_video.mp4`,             // howto_interactives — «Starte den Tag mit Nachrichten»
  youtube_dual_subs: `${R2}/youtube_video_morningnews/youtube_double_subtitles.mp4`,   // howto_tools — «YouTube mit doppelten Untertiteln»
  youtube_browser:   `${R2}/youtube_video_morningnews/youtube_in_browser.mp4`,         // howto_tools — то же в браузере
  interactives: [                                                                     // howto_interactives
    { src: `${R2}/interactives/interactive_1.mp4`, caption: t('🎥 Артикли der/die/das — Artikelquiz', '🎥 Artikel der/die/das — Artikelquiz') },
    { src: `${R2}/interactives/interactive_2.mp4`, caption: t('🎥 Вопросы Wo-Fragen', '🎥 Wo-Fragen') },
    { src: `${R2}/interactives/interactive_3.mp4`, caption: t('🎥 Окончания прилагательных', '🎥 Adjektivendungen') },
    { src: `${R2}/interactives/interactive_4.mp4`, caption: t('🎥 Анаграммы — собери слово (Anagramm)', '🎥 Anagramm — Wort zusammensetzen') },
    { src: `${R2}/interactives/interactive_5.mp4`, caption: t('🎥 Artikel Sprint — der/die/das на скорость', '🎥 Artikel Sprint — der/die/das auf Zeit') },
  ],
};

// Switch the whole wizard language (reload so the module-level strings re-read).
function switchLang() {
  try { localStorage.setItem('ui_lang', LANG === 'de' ? 'ru' : 'de'); } catch (_e) { /* noop */ }
  try { window.location.reload(); } catch (_e) { /* noop */ }
}

async function api(path, extra) {
  const initData = tg?.initData || '';
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData, ...(extra || {}) }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

// kind: 'welcome' | 'core' (mandatory) | 'pro' (teaser for Free) | 'opt' | 'info' | 'finale'
const STEPS = [
  { id: 'welcome',    title: t('Willkommen! 👋', 'Willkommen! 👋'),            kind: 'welcome' },
  { id: 'install_app', title: t('Вынеси приложение на экран 📲', 'Leg die App auf den Startbildschirm 📲'), kind: 'info' },
  { id: 'language',   title: t('Твоя языковая пара 🌍', 'Dein Sprachpaar 🌍'),     kind: 'core' },
  { id: 'dictionary', title: t('Базовый словарь 📚', 'Basis-Wörterbuch 📚'),        kind: 'core' },
  { id: 'intensity',  title: t('Сколько заданий в день 🔥', 'Wie viele Aufgaben pro Tag 🔥'), kind: 'pro' },
  { id: 'windows',    title: t('Когда присылать задания ⏰', 'Wann Aufgaben kommen ⏰'), kind: 'pro' },
  { id: 'battles',    title: t('Игры с другими учениками ⚔️', 'Spiele mit anderen Lernenden ⚔️'), kind: 'opt' },
  { id: 'howto_words',        title: t('Твой словарь пополняется сам 📖', 'Dein Wörterbuch füllt sich von selbst 📖'), kind: 'info' },
  { id: 'howto_interactives', title: t('Игры-тренировки приходят сами 🎮', 'Übungsspiele kommen von selbst 🎮'), kind: 'info' },
  { id: 'howto_translations', title: t('Переводы с разбором ✍️', 'Übersetzungen mit Analyse ✍️'),          kind: 'info' },
  { id: 'howto_tools',        title: t('Ещё инструменты 🧰', 'Weitere Werkzeuge 🧰'),              kind: 'info' },
  { id: 'keyboard',   title: t('Меню бота — где нажимать ⌨️', 'Bot-Menü — wo man tippt ⌨️'), kind: 'info' },
  { id: 'shortcut',   title: t('Захват слов со скриншотов (iPhone) 📲', 'Wörter aus Screenshots (iPhone) 📲'), kind: 'opt' },
  { id: 'howto_morning', title: t('Утром твои слова уже ждут 🌅', 'Morgens warten deine Wörter 🌅'), kind: 'info' },
  { id: 'howto_learn',   title: t('А теперь — как их учить 🎓', 'Und jetzt — wie du sie lernst 🎓'), kind: 'info' },
  { id: 'plans',      title: t('Тарифы: Free и Pro 💎', 'Tarife: Free und Pro 💎'), kind: 'info' },
  { id: 'finale',     title: t('Готово! ✅', 'Fertig! ✅'),                kind: 'finale' },
];

// Pro delivery presets + active-hours windows (mirror the bot picker).
const PRESETS = [
  ['intensive', t('🔥 Интенсивно', '🔥 Intensiv'), t('~20 заданий в день', '~20 Aufgaben/Tag')],
  ['normal',    t('🙂 Обычно', '🙂 Normal'),       t('~12 в день (по умолчанию)', '~12/Tag (Standard)')],
  ['rare',      t('🌙 Редко', '🌙 Selten'),        t('~8 в день', '~8/Tag')],
  ['silent',    t('🔕 Тишина', '🔕 Stille'),       t('не присылать автоматически', 'nichts automatisch')],
];
const WINDOWS = [
  ['allday',  t('🌗 Весь день', '🌗 Ganzer Tag'), t('в любое время', 'jederzeit')],
  ['morning', t('🌅 Утро', '🌅 Morgens'),         '06–12'],
  ['evening', t('🌆 Вечер', '🌆 Abends'),         '17:30–22:30'],
  ['morneve', t('🌅🌆 Утро+вечер', '🌅🌆 Morgens+abends'), '06–09 · 18–22:30'],
];

// Media slot that hides itself if the asset isn't there yet (drop the file later).
function MediaTile({ src, type = 'video', caption }) {
  // Only render once we've confirmed the URL is a REAL media file. A missing asset is
  // served by the SPA as index.html (200 text/html) — a bare <video> would then spin
  // forever without firing onError. So HEAD-check the content-type first and hide if
  // it isn't video/*|image/*. (Videos owner will add later just start appearing.)
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
        // Network/HEAD unsupported — show and let onError catch a hard failure.
        if (!off) setReady(true);
      }
    })();
    return () => { off = true; };
  }, [src, type]);
  if (hidden || !ready) return null;
  return (
    <figure className="ob-media">
      {type === 'video' ? (
        <video src={src} controls playsInline preload="none" onError={() => setHidden(true)} />
      ) : (
        <img src={src} alt={caption || ''} loading="lazy" onError={() => setHidden(true)} />
      )}
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}

async function openDictInBrowser() {
  // Authenticate the standalone browser dictionary (and its "Add to Home Screen"
  // install) — without auth, breakdown / save / audio all 401 outside Telegram (only
  // the no-auth quick translate works). Preferred: mint a DURABLE, revocable token that
  // never expires (works forever on the home screen). Fallback: carry the current
  // initData, which the server accepts for 30 days.
  const initData = tg?.initData || '';
  const origin = window.location.origin;
  // Open the QUERY form /dict?dqt=<token> in Safari: it is routed by EVERY bundle version
  // (old and new), so a stale cached bundle still opens the dictionary here — unlike the
  // path form /dict/t/<token>, which only the newest bundle knows and which broke Safari
  // for anyone on a cached older build. The path form is used ONLY for the installed
  // icon's start_url (baked by the manifest), because iOS drops the ?query from a
  // home-screen start_url. So: Safari → ?query (universal), installed icon → path (iOS-safe).
  let url = initData ? `${origin}/dict?initData=${encodeURIComponent(initData)}` : `${origin}/dict`;
  try {
    const res = await fetch('/api/webapp/dict/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Telegram-InitData': initData },
      body: JSON.stringify({ initData }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data?.token) url = `${origin}/dict?dqt=${encodeURIComponent(String(data.token))}`;
  } catch (_e) { /* keep the initData fallback URL */ }
  try {
    if (tg?.openLink) tg.openLink(url);
    else window.open(url, '_blank');
  } catch (_e) {
    try { window.open(url, '_blank'); } catch (_e2) { /* ignore */ }
  }
}

async function openAppInBrowser() {
  // Open the FULL app in the browser and set it up for "Add to Home Screen". Outside Telegram
  // there is no initData, so mint a DURABLE, revocable APP token (never expires) and carry it in
  // the launch URL — the fetch shim then attaches it to every /api/ call so the detached icon
  // stays logged in. Open the QUERY form /webapp?aqt=<token> in Safari (routed by every bundle);
  // the manifest bakes the iOS-safe PATH form /webapp/t/<token> into the installed icon's start_url.
  const initData = tg?.initData || '';
  const origin = window.location.origin;
  let url = initData ? `${origin}/webapp?initData=${encodeURIComponent(initData)}` : `${origin}/webapp`;
  try {
    const res = await fetch('/api/webapp/app/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Telegram-InitData': initData },
      body: JSON.stringify({ initData }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data?.token) url = `${origin}/webapp?aqt=${encodeURIComponent(String(data.token))}`;
  } catch (_e) { /* keep the initData fallback URL */ }
  try {
    if (tg?.openLink) tg.openLink(url);
    else window.open(url, '_blank');
  } catch (_e) {
    try { window.open(url, '_blank'); } catch (_e2) { /* ignore */ }
  }
}

function OptionList({ options, selected, onPick }) {
  return (
    <div className="ob-options">
      {options.map(([code, label, sub]) => (
        <button
          key={code}
          type="button"
          className={`ob-option ${selected === code ? 'is-sel' : ''}`}
          onClick={() => onPick(code)}
        >
          <span className="ob-option-label">{label}</span>
          <span className="ob-option-sub">{sub}</span>
        </button>
      ))}
    </div>
  );
}

const PRO_TEASER = (
  <div className="ob-teaser">
    <p className="ob-lead">
      {t('На бесплатном — подборка заданий в день. В ',
         'Kostenlos — eine Auswahl an Aufgaben pro Tag. In ')}
      <b>Pro</b>
      {t(' можно настроить количество и время доставки.',
         ' kannst du Menge und Uhrzeit einstellen.')}
    </p>
    <span className="ob-lock">{t('🔒 Только в Pro', '🔒 Nur mit Pro')}</span>
  </div>
);

// Body per step. Real controls land case-by-case (Stage 1); the rest stay stubs.
const REAL_STEPS = new Set(['install_app', 'language', 'dictionary', 'intensity', 'windows',
  'battles', 'shortcut', 'howto_words', 'howto_interactives', 'howto_translations',
  'howto_tools', 'keyboard', 'howto_morning', 'howto_learn', 'plans']);
function StepBody(props) {
  const { step, isPro, confirmed, busy, dictBusy, dictChoice, stepErr, onConfirm, dictOffer, onDictAction,
    selPreset, selWindow, onPickPreset, onPickWindow, selBattle, onPickBattle,
    onShortcutSetup, shortcutOpening, proPrice, onOpenSubscription } = props;
  const bodyKey = REAL_STEPS.has(step.id) ? step.id : step.kind;
  switch (bodyKey) {
    case 'install_app':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Наше приложение можно вынести иконкой прямо на рабочий стол телефона — и открывать одним касанием, как обычное приложение. Не нужно заходить в Telegram, искать чат и меню: тапнул по иконке — и ты сразу в приложении.',
               'Unsere App kannst du als Symbol direkt auf den Startbildschirm legen — und mit einem Tipp öffnen, wie eine normale App. Du musst nicht erst Telegram öffnen, den Chat und das Menü suchen: Symbol antippen — und du bist sofort drin.')}
          </p>
          <p className="ob-lead">
            {t('Открывается оно в браузере как самостоятельное приложение (Telegram при этом не запускается). Это заметно удобнее и ощущается как настоящее приложение. Сделать это стоит один раз — дальше пользуешься с рабочего стола.',
               'Es öffnet sich im Browser als eigenständige App (Telegram startet dabei nicht). Das ist deutlich bequemer und fühlt sich wie eine echte App an. Einmal einrichten — danach nutzt du sie vom Startbildschirm.')}
          </p>
          <p className="ob-lead">
            <b>{t('⚠️ Обязательное условие: у тебя должен быть установлен наш бот в Telegram.',
                  '⚠️ Voraussetzung: Unser Bot muss in deinem Telegram installiert sein.')}</b>
            {t(' Именно через него идёт вся коммуникация и приходят все интерактивы — задания, разборы, аудио. Без установленного бота иконка работать не будет.',
               ' Über ihn läuft die gesamte Kommunikation und kommen alle interaktiven Inhalte — Aufgaben, Analysen, Audio. Ohne installierten Bot funktioniert das Symbol nicht.')}
          </p>
          <div className="ob-howbox">
            <p className="ob-howbox-title">{t('📌 iPhone (Safari)', '📌 iPhone (Safari)')}</p>
            <ol className="ob-steps">
              <li>{t('Открой приложение в браузере Safari — кнопкой ниже.', 'Öffne die App im Safari-Browser — mit dem Knopf unten.')}</li>
              <li>{t('Внизу нажми «Поделиться» (квадрат со стрелкой вверх).', 'Tippe unten auf «Teilen» (Quadrat mit Pfeil nach oben).')}</li>
              <li>{t('Пролистай список и выбери «На экран „Домой"».', 'Scrolle die Liste und wähle «Zum Home-Bildschirm».')}</li>
              <li>{t('Нажми «Добавить» — иконка появится на рабочем столе.', 'Tippe «Hinzufügen» — das Symbol erscheint auf dem Startbildschirm.')}</li>
              <li>{t('Готово! Теперь открывай приложение как обычное — одним касанием.', 'Fertig! Öffne die App jetzt wie eine normale — mit einem Tipp.')}</li>
            </ol>
            <button type="button" className="ob-confirm" onClick={openAppInBrowser}>
              {t('🌐 Открыть приложение в Safari', '🌐 App in Safari öffnen')}
            </button>
            <MediaTile src={MEDIA.install_ios} type="video" caption={t('🎥 Как вынести иконку на экран (iPhone)', '🎥 Wie man das Symbol anlegt (iPhone)')} />
          </div>
          <div className="ob-howbox">
            <p className="ob-howbox-title">{t('📌 Android (Chrome)', '📌 Android (Chrome)')}</p>
            <ol className="ob-steps">
              <li>{t('Открой приложение в браузере Chrome (кнопкой выше — она откроет ссылку в браузере).', 'Öffne die App im Chrome-Browser (mit dem Knopf oben — er öffnet den Link im Browser).')}</li>
              <li>{t('Вверху справа нажми на три точки ⋮ (меню).', 'Tippe oben rechts auf die drei Punkte ⋮ (Menü).')}</li>
              <li>{t('Выбери «Добавить на главный экран» (или «Установить приложение»).', 'Wähle «Zum Startbildschirm hinzufügen» (oder «App installieren»).')}</li>
              <li>{t('Подтверди «Добавить» — иконка появится на экране.', 'Bestätige «Hinzufügen» — das Symbol erscheint auf dem Bildschirm.')}</li>
              <li>{t('Готово! Открывай приложение прямо с рабочего стола.', 'Fertig! Öffne die App direkt vom Startbildschirm.')}</li>
            </ol>
            <p className="ob-muted-note">
              {t('🤖 Шаги могут немного отличаться — у Android очень много версий и оболочек. Но смысл везде один: меню браузера → «Добавить на главный экран».',
                 '🤖 Die Schritte können leicht abweichen — Android hat sehr viele Versionen und Oberflächen. Der Kern ist überall gleich: Browser-Menü → «Zum Startbildschirm hinzufügen».')}
            </p>
          </div>
          <p className="ob-lead ob-muted-note">
            {t('Иконка привязана к твоему аккаунту и работает, пока бот у тебя не удалён. Если удалишь бота — приложение попросит вернуть его.',
               'Das Symbol ist mit deinem Konto verknüpft und funktioniert, solange der Bot nicht entfernt ist. Wenn du den Bot löschst, bittet die App dich, ihn zurückzuholen.')}
          </p>
        </div>
      );
    case 'welcome':
      return IS_PUBLIC ? (
        <p className="ob-lead">
          {t('Это бот для изучения немецкого. Пролистай за пару минут — покажу, что он умеет. В конце сможешь установить его себе.',
             'Das ist ein Bot zum Deutschlernen. Blättere in ein paar Minuten durch — ich zeige, was er kann. Am Ende kannst du ihn installieren.')}
        </p>
      ) : (
        <p className="ob-lead">
          {t('Давай за пару минут настроим бота под тебя — язык, словарь, темп заданий. Потом покажу, как всё работает. Всё это потом легко изменить в «🎬 Как пользоваться».',
             'Lass uns den Bot in ein paar Minuten einrichten — Sprache, Wörterbuch, Aufgaben-Tempo. Danach zeige ich, wie alles funktioniert. Alles lässt sich später in «🎬 Wie man es benutzt» ändern.')}
        </p>
      );
    case 'language':
      // German-only mode → the pair is fixed (Немецкий ← Русский); the step just
      // confirms it and upserts the profile row.
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Ты учишь ', 'Du lernst ')}<b>{t('немецкий', 'Deutsch')}</b>
            {t(', а объяснять будем на ', ', erklärt wird auf ')}<b>{t('русском', 'Russisch')}</b>
            {t('. Подтверди — и поехали.', '. Bestätige — und los.')}
          </p>
          <div className="ob-pair">
            <span className="ob-pair-item"><b>🇩🇪 {t('Немецкий', 'Deutsch')}</b><small>{t('учишь', 'du lernst')}</small></span>
            <span className="ob-pair-arrow">↔</span>
            <span className="ob-pair-item"><b>🗣 {t('Русский', 'Russisch')}</b><small>{t('объясняем', 'wir erklären')}</small></span>
          </div>
          {!IS_PUBLIC && (
            <button
              type="button"
              className={`ob-confirm ${confirmed ? 'is-done' : ''}`}
              onClick={onConfirm}
              disabled={busy || confirmed}
            >
              {confirmed ? t('✅ Подтверждено', '✅ Bestätigt') : busy ? t('Сохраняю…', 'Speichere…') : t('✓ Подтвердить', '✓ Bestätigen')}
            </button>
          )}
          {stepErr ? <p className="ob-err">{stepErr}</p> : null}
        </div>
      );
    case 'dictionary': {
      const have = Number(dictOffer?.starter_pair_total || 0);
      const n = Number(dictOffer?.suggested_count || dictOffer?.import_limit || 0);
      const total = Number(dictOffer?.template_total || 0);
      const hasFull = total > 0 && have >= total;
      const partial = have > 0 && !hasFull;          // small starter connected, full still available
      const done = (confirmed && !partial) || hasFull;
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Подключим слова, с которых начнём тренировки и повторения. Выбери, сколько:',
               'Verbinden wir Wörter, mit denen Training und Wiederholungen starten. Wähle, wie viele:')}
          </p>
          {done ? (
            dictChoice === 'decline' && !have ? (
              <span className="ob-lock">{t('⏭ Пропущено — базовый словарь можно подключить позже в ⚙️ Настройках.', '⏭ Übersprungen — das Basis-Wörterbuch kannst du später in ⚙️ Einstellungen verbinden.')}</span>
            ) : (
              <span className="ob-lock ob-ok">{hasFull ? t('✅ Весь словарь подключён', '✅ Ganzes Wörterbuch verbunden') : t('✅ Словарь подключён', '✅ Wörterbuch verbunden')}</span>
            )
          ) : partial ? (
            <div className="ob-actions ob-actions-col">
              <span className="ob-lock ob-ok">{t('✅ Базовый словарь подключён', '✅ Basis-Wörterbuch verbunden')}</span>
              {dictChoice === 'full' ? (
                <span className="ob-lock ob-ok">{t('✅ Догружаю весь словарь — это займёт пару минут, можно идти дальше', '✅ Lade das ganze Wörterbuch — ein paar Minuten, du kannst weitergehen')}</span>
              ) : (
                <>
                  {total > have ? (
                    <button
                      type="button"
                      className="ob-confirm ob-alt"
                      onClick={() => onDictAction('accept', true)}
                      disabled={busy}
                    >
                      {dictBusy === 'full' ? t('⏳ Догружаю…', '⏳ Lade…') : `${t('🔓 Догрузить весь словарь', '🔓 Ganzes Wörterbuch laden')} — ~${total} ${t('слов', 'Wörter')}`}
                    </button>
                  ) : null}
                  <button
                    type="button"
                    className="ob-skip"
                    onClick={() => onDictAction('keep')}
                    disabled={busy}
                  >
                    {confirmed ? t('✅ Оставляю базовый — дальше', '✅ Basis behalten — weiter') : t('Пропустить (оставить базовый)', 'Überspringen (Basis behalten)')}
                  </button>
                </>
              )}
            </div>
          ) : IS_PUBLIC ? null : (
            <div className="ob-actions ob-actions-col">
              <button
                type="button"
                className="ob-confirm"
                onClick={() => onDictAction('accept', false)}
                disabled={busy}
              >
                {dictBusy === 'quick' ? t('Подключаю…', 'Verbinde…') : `${t('📚 Быстрый старт', '📚 Schnellstart')}${n ? ` — ~${n} ${t('слов', 'Wörter')}` : ''}`}
              </button>
              {total > n ? (
                <button
                  type="button"
                  className="ob-confirm ob-alt"
                  onClick={() => onDictAction('accept', true)}
                  disabled={busy}
                >
                  {dictBusy === 'full' ? t('Подключаю…', 'Verbinde…') : `${t('🔓 Весь словарь', '🔓 Ganzes Wörterbuch')} — ~${total} ${t('слов', 'Wörter')}`}
                </button>
              ) : null}
              <button
                type="button"
                className="ob-skip"
                onClick={() => onDictAction('decline')}
                disabled={busy}
              >
                {t('Пропустить', 'Überspringen')}
              </button>
            </div>
          )}
          <p className="ob-muted-note">{t('Быстрый старт — меньше слов, проще начать. Весь словарь — сразу весь набор. Здесь — подключаешь, а отключить эти базовые словари (если захочешь только свои слова) можно потом в ⚙️ Настройках.', 'Schnellstart — weniger Wörter, leichter Einstieg. Ganzes Wörterbuch — der volle Satz. Hier verbindest du sie; später kannst du diese Basis-Wörterbücher in ⚙️ Einstellungen wieder abschalten (wenn du nur deine eigenen Wörter willst).')}</p>
          {stepErr ? <p className="ob-err">{stepErr}</p> : null}
        </div>
      );
    }
    case 'intensity':
      return isPro ? (
        <div className="ob-stub">
          <p className="ob-lead">{t('Выбери темп — можно поменять когда угодно.', 'Wähle das Tempo — jederzeit änderbar.')}</p>
          <OptionList options={PRESETS} selected={selPreset} onPick={onPickPreset} />
        </div>
      ) : PRO_TEASER;
    case 'windows':
      return isPro ? (
        <div className="ob-stub">
          <p className="ob-lead">{t('В какие часы присылать задания? Они придут равномерно внутри окна.', 'Zu welchen Zeiten sollen die Aufgaben kommen? Sie kommen gleichmäßig im Fenster.')}</p>
          <OptionList options={WINDOWS} selected={selWindow} onPick={onPickWindow} />
        </div>
      ) : PRO_TEASER;
    case 'battles':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Дуэль — это короткое соревнование с другим учеником: кто быстрее и без ошибок пройдёт задания на грамматику (артикли, окончания прилагательных, вопросы). Вызвать соперника можно из меню бота. Хочешь получать приглашения на дуэли?',
               'Ein Duell ist ein kurzer Wettkampf mit einem anderen Lernenden: wer schneller und fehlerfrei Grammatik-Aufgaben löst (Artikel, Adjektivendungen, Fragen). Einen Gegner rufst du über das Bot-Menü. Möchtest du Duell-Einladungen bekommen?')}
          </p>
          <div className="ob-options">
            <button
              type="button"
              className={`ob-option ${selBattle === 'yes' ? 'is-sel' : ''}`}
              onClick={() => onPickBattle(true)}
            >
              <span className="ob-option-label">{t('⚔️ Да, зовите меня', '⚔️ Ja, ladet mich ein')}</span>
              <span className="ob-option-sub">{t('буду в списке приглашаемых', 'ich bin in der Einladungsliste')}</span>
            </button>
            <button
              type="button"
              className={`ob-option ${selBattle === 'no' ? 'is-sel' : ''}`}
              onClick={() => onPickBattle(false)}
            >
              <span className="ob-option-label">{t('Пока нет', 'Noch nicht')}</span>
              <span className="ob-option-sub">{t('можно включить позже', 'später aktivierbar')}</span>
            </button>
          </div>
        </div>
      );
    case 'shortcut':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('На ', 'Auf dem ')}<b>iPhone</b>
            {t(' одним движением делаешь скриншот немецкого слова из любого приложения — а утром телефон сам всё переводит и присылает слова в личку. Останется сохранить.',
               ' machst du mit einer Bewegung einen Screenshot eines deutschen Wortes aus jeder App — und morgens übersetzt das Handy alles selbst und schickt die Wörter privat. Nur noch speichern.')}
          </p>
          <p className="ob-lead">{t('Необязательно, настраивается один раз. Можно сейчас или позже.', 'Optional, wird einmal eingerichtet. Jetzt oder später.')}</p>
          <button type="button" className="ob-confirm" onClick={onShortcutSetup} disabled={shortcutOpening}>
            {shortcutOpening ? t('⏳ Открываю…', '⏳ Öffne…') : t('📲 Настроить сейчас', '📲 Jetzt einrichten')}
          </button>
          <span className="ob-muted-note">{t('Пропустишь — откроешь потом через «🎬 Как пользоваться».', 'Überspringst du — öffnest du es später über «🎬 Wie man es benutzt».')}</span>
        </div>
      );
    case 'howto_words':
      return (
        <div className="ob-stub">
          <p className="ob-lead">{t('Слова можно сохранять как удобно — бот сам переведёт и добавит в твой словарь:', 'Wörter kannst du speichern, wie es dir passt — der Bot übersetzt und legt sie in dein Wörterbuch:')}</p>
          <ul className="ob-list">
            <li>
              <b>{t('✍️ Напиши боту', '✍️ Schreib dem Bot')}</b>
              {t(' слово или фразу (по-русски или по-немецки) — получишь быстрый перевод с вариантами на сохранение или полный разбор слова (подробнее, чем в обычном словаре).',
                 ' ein Wort oder einen Satz (auf Russisch oder Deutsch) — du bekommst eine schnelle Übersetzung mit Speicheroptionen oder eine ausführliche Wort-Analyse (genauer als ein normales Wörterbuch).')}
            </li>
            <li>
              <b>{t('📩 Перешли боту', '📩 Leite dem Bot')}</b>
              {t(' любое сообщение с немецким текстом — из Telegram, WhatsApp, откуда угодно. Он достанет оттуда слова и фразы для сохранения.',
                 ' jede Nachricht mit deutschem Text weiter — aus Telegram, WhatsApp, egal woher. Er holt daraus Wörter und Sätze zum Speichern.')}
              <MediaTile
                src={MEDIA.forward_chat}
                type="video"
                caption={t('🎥 Перешли боту сообщение из любого чата — он разберёт слова, а ты сохранишь их в свой словарь и выучишь позже прямо тут, в приложении.',
                           '🎥 Leite dem Bot eine Nachricht aus einem beliebigen Chat weiter — er analysiert die Wörter, du speicherst sie in dein Wörterbuch und lernst sie später direkt hier in der App.')}
              />
            </li>
            <li>
              <b>{t('📋 Вставь большой текст', '📋 Füge einen langen Text ein')}</b>
              {t(' — бот разберёт его и предложит слова на сохранение.', ' — der Bot analysiert ihn und schlägt Wörter zum Speichern vor.')}
            </li>
            <li>
              <b>{t('🔗 Выделил текст', '🔗 Text markiert')}</b>
              {t(' в браузере или книге → «Поделиться» → выбери бота. Текст улетит ему, а ты потом спокойно посмотришь перевод с разбором и сохранишь, что нужно.',
                 ' im Browser oder Buch → «Teilen» → wähle den Bot. Der Text geht an ihn, und du siehst später in Ruhe die Übersetzung mit Analyse und speicherst, was du brauchst.')}
              <MediaTile
                src={MEDIA.forward_selection}
                type="video"
                caption={t('🎥 Понравилось слово или фраза при чтении в браузере? Выдели её → «Поделиться» → выбери бота. Текст улетит ему, а ты потом посмотришь перевод с разбором и сохранишь в свой словарь.',
                           '🎥 Ein Wort oder ein Satz beim Lesen im Browser gefällt dir? Markiere ihn → «Teilen» → wähle den Bot. Der Text geht an ihn, und du siehst später die Übersetzung mit Analyse und speicherst sie in dein Wörterbuch.')}
              />
            </li>
            <li>
              <b>{t('⭐ Быстрый словарь-переводчик — прямо на рабочем столе.', '⭐ Schneller Wörterbuch-Übersetzer — direkt auf dem Startbildschirm.')}</b>
              {t(' Его иконку можно вынести на экран телефона и открывать как обычное приложение-переводчик, когда нужно посмотреть слово или перевести фразу. Только информации в разы больше: перевод, артикль, синонимы, антонимы, примеры и частые словосочетания. И одной кнопкой сохраняешь всё это (не только слово, но и словосочетания с синонимами) в свой словарь — чтобы потом выучить. ',
                 ' Sein Symbol kannst du auf den Startbildschirm legen und wie eine normale Übersetzer-App öffnen, wenn du ein Wort nachschlagen oder einen Satz übersetzen willst. Nur mit viel mehr Infos: Übersetzung, Artikel, Synonyme, Antonyme, Beispiele und häufige Wortverbindungen. Und mit einem Knopf speicherst du all das (nicht nur das Wort, auch Wortverbindungen und Synonyme) in dein Wörterbuch — zum späteren Lernen. ')}
              <i>{t('(как поставить иконку — покажем отдельно)', '(wie man das Symbol anlegt — zeigen wir separat)')}</i>
            </li>
          </ul>
          <div className="ob-howbox">
            <p className="ob-howbox-title">{t('📌 Как вынести иконку переводчика на экран (iPhone)', '📌 Wie du das Übersetzer-Symbol auf den Bildschirm legst (iPhone)')}</p>
            <ol className="ob-steps">
              <li>{t('Открой словарь в браузере Safari — кнопкой ниже.', 'Öffne das Wörterbuch im Safari-Browser — mit dem Knopf unten.')}</li>
              <li>{t('Внизу нажми «Поделиться» (квадрат со стрелкой вверх).', 'Tippe unten auf «Teilen» (Quadrat mit Pfeil nach oben).')}</li>
              <li>{t('Пролистай список и выбери «На экран „Домой"».', 'Scrolle die Liste und wähle «Zum Home-Bildschirm».')}</li>
              <li>{t('Нажми «Добавить» — иконка появится на рабочем столе.', 'Tippe «Hinzufügen» — das Symbol erscheint auf dem Startbildschirm.')}</li>
              <li>{t('Готово! Теперь открывай словарь как обычное приложение.', 'Fertig! Öffne das Wörterbuch jetzt wie eine normale App.')}</li>
            </ol>
            <button type="button" className="ob-confirm" onClick={openDictInBrowser}>
              {t('🌐 Открыть словарь в Safari', '🌐 Wörterbuch in Safari öffnen')}
            </button>
            <MediaTile src="https://pub-6ebcbf6d9aec43d488d3f6a3ba222c14.r2.dev/shortcuts/%D0%98%D0%BA%D0%BE%D0%BD%D0%BA%D0%B0%20%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8C%20%D0%BD%D0%B0%20%D1%80%D0%B0%D0%B1%D0%BE%D1%87%D0%B8%D0%B9%20%D1%81%D1%82%D0%BE%D0%BB/translator_icon.mp4" type="video" caption={t('🎥 Как вынести иконку на экран', '🎥 Wie man das Symbol anlegt')} />
          </div>
          <p className="ob-lead ob-muted-note">
            {t('Всё, что читаешь и сохраняешь, само копится в твоём словаре — заходишь и учишь, когда захочешь.',
               'Alles, was du liest und speicherst, sammelt sich von selbst in deinem Wörterbuch — du gehst rein und lernst, wann du willst.')}
          </p>
        </div>
      );
    case 'howto_interactives':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Ничего вызывать не нужно — бот сам присылает тебе в личку короткие задания по твоему расписанию. Каждое тренирует важную тему:',
               'Du musst nichts aufrufen — der Bot schickt dir kurze Aufgaben privat nach deinem Zeitplan. Jede trainiert ein wichtiges Thema:')}
          </p>
          <ul className="ob-list">
            <li>
              <b>{t('🗞 Начни день с новостей', '🗞 Starte den Tag mit Nachrichten')}</b>
              {t(' — каждое утро свежие настоящие новости коротким видео. Смотришь с двойными кликабельными субтитрами (любое слово — перевёл и сохранил), а потом отвечаешь на тесты по услышанному. Тренирует понимание живой речи — навык, который нужен каждый день в реальной жизни.',
                 ' — jeden Morgen frische echte News als kurzes Video. Du schaust mit doppelten anklickbaren Untertiteln (jedes Wort — übersetzt und gespeichert) und beantwortest dann Tests zum Gehörten. Trainiert das Verstehen echter Sprache — eine Fähigkeit, die man im Alltag jeden Tag braucht.')}
              {MEDIA.world_news ? <MediaTile src={MEDIA.world_news} type="video" caption={t('🎥 Начни день с новостей — двойные субтитры', '🎥 Starte den Tag mit Nachrichten — doppelte Untertitel')} /> : null}
            </li>
            <li>
              <b>{t('🔵 Артикли der/die/das', '🔵 Artikel der/die/das')}</b>
              {t(' — угадываешь род слова. Спокойная тренировка или дуэль на скорость с другим учеником. База, которую надо держать всегда.',
                 ' — du errätst das Geschlecht des Wortes. Ruhiges Training oder ein Duell auf Zeit mit einem anderen Lernenden. Die Basis, die man immer halten muss.')}
              {MEDIA.interactives[0]?.src ? <MediaTile src={MEDIA.interactives[0].src} type="video" caption={MEDIA.interactives[0].caption} /> : null}
              {MEDIA.interactives[4]?.src ? <MediaTile src={MEDIA.interactives[4].src} type="video" caption={MEDIA.interactives[4].caption} /> : null}
            </li>
            <li>
              <b>{t('🟢 Окончания прилагательных', '🟢 Adjektivendungen')}</b>
              {t(' — самая частая ошибка в немецком. Тренировка и дуэль.', ' — der häufigste Fehler im Deutschen. Training und Duell.')}
              {MEDIA.interactives[2]?.src ? <MediaTile src={MEDIA.interactives[2].src} type="video" caption={MEDIA.interactives[2].caption} /> : null}
            </li>
            <li>
              <b>🔢 Zahlendiktat</b>
              {t(' — числа на слух: слышишь и записываешь. Тренировка и дуэль.', ' — Zahlen nach Gehör: du hörst und tippst. Training und Duell.')}
            </li>
            <li>
              <b>❓ Wo-Fragen</b>
              {t(' — вопросы (wo / wohin / woher…): учишься правильно спрашивать. Тренировка и дуэль.',
                 ' — Fragen (wo / wohin / woher…): du lernst richtig zu fragen. Training und Duell.')}
              {MEDIA.interactives[1]?.src ? <MediaTile src={MEDIA.interactives[1].src} type="video" caption={MEDIA.interactives[1].caption} /> : null}
            </li>
            <li>
              <b>{t('🧩 Кроссворды', '🧩 Kreuzworträtsel')}</b>
              {t(' — расширяют словарный запас в игровой форме.', ' — erweitern spielerisch den Wortschatz.')}
            </li>
            <li>
              <b>🎧 Hörverständnis</b>
              {t(' — понимание на слух: слушаешь и вытаскиваешь ключевое (даты, время, факты). Учит ориентироваться в живой речи.',
                 ' — Hörverstehen: du hörst zu und ziehst das Wichtigste heraus (Daten, Uhrzeit, Fakten). Übt, sich in echter Sprache zu orientieren.')}
            </li>
            <li>
              <b>{t('🔁 Разбор твоих ошибок за вчера', '🔁 Analyse deiner Fehler von gestern')}</b>
              {t(' — бот собирает, где ты ошибся, и даёт повторить. Чтобы ошибки не превращались в привычку.',
                 ' — der Bot sammelt, wo du dich geirrt hast, und lässt dich wiederholen. Damit Fehler nicht zur Gewohnheit werden.')}
            </li>
          </ul>
          <p className="ob-lead">
            <b>{t('🔤 Анаграммы', '🔤 Anagramme')}</b>
            {t(' — собери слово из перемешанных букв. Тренирует правописание и словарный запас.',
               ' — setze das Wort aus gemischten Buchstaben zusammen. Übt Rechtschreibung und Wortschatz.')}
          </p>
          {MEDIA.interactives[3]?.src ? <MediaTile src={MEDIA.interactives[3].src} type="video" caption={MEDIA.interactives[3].caption} /> : null}
        </div>
      );
    case 'howto_morning':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Если в предыдущем разделе ты всё установил и настроил верно — дальше всё идёт само. Каждое утро телефон автоматически обработает скриншоты из твоей папки (до 25 за раз) и пришлёт слова в удобном виде: немецкое слово и русский перевод рядом.',
               'Wenn du im vorigen Abschnitt alles richtig eingerichtet hast — läuft alles von selbst. Jeden Morgen verarbeitet das Handy automatisch die Screenshots aus deinem Ordner (bis zu 25 auf einmal) und schickt die Wörter übersichtlich: deutsches Wort und russische Übersetzung nebeneinander.')}
          </p>
          <p className="ob-lead">
            {t('Просто ставишь или снимаешь галочки-чекбоксы — так ты сам решаешь, какое слово сохранить в словарь, а какое нет. Посмотри на видео, как это делается.',
               'Du setzt oder entfernst einfach die Häkchen — so entscheidest du selbst, welches Wort ins Wörterbuch kommt und welches nicht. Schau im Video, wie das geht.')}
          </p>
          <MediaTile
            src={MEDIA.morning_words}
            type="video"
            caption={t('🎥 Так утром приходят слова — отметь галочками нужные и сохрани', '🎥 So kommen die Wörter morgens an — die passenden markieren und speichern')}
          />
        </div>
      );
    case 'howto_learn':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('После того как сохранил слова, спокойно заходишь и учишь их в приложении — в разделе «Карточки» с интервальным повторением: система сама показывает слово тогда, когда его пора повторить, пока оно не осядет в памяти. Вот пример, как это делать — смотри на видео.',
               'Sobald du die Wörter gespeichert hast, gehst du in Ruhe in die App und lernst sie — im Bereich «Karten» mit Spaced Repetition: das System zeigt dir ein Wort genau dann, wenn es Zeit zum Wiederholen ist, bis es sitzt. Hier ein Beispiel, wie das geht — schau im Video.')}
          </p>
          <MediaTile
            src={MEDIA.learn_words}
            type="video"
            caption={t('🎥 А вот так ты учишь сохранённые слова — карточки, интервальное повторение', '🎥 Und so lernst du die gespeicherten Wörter — Karten, Spaced Repetition')}
          />
          <p className="ob-lead ob-muted-note">
            {t('Твои слова всегда под рукой в разделе «Словарь» — а ещё их можно загрузить для офлайн-просмотра, чтобы учить даже без интернета.',
               'Deine Wörter sind immer griffbereit im Bereich «Wörterbuch» — außerdem kannst du sie für die Offline-Ansicht herunterladen und sogar ohne Internet lernen.')}
          </p>
        </div>
      );
    case 'plans': {
      const YES = 'yes', NO = 'no', INF = 'inf';
      const D = (n) => [`${n}/день`, `${n}/Tag`];
      const cmpCell = (spec, col) => {
        if (spec === YES) return <span className="mk yes">✓</span>;
        if (spec === NO) return <span className="mk no">✕</span>;
        if (spec === INF) return <span className="inf">∞</span>;
        const txt = Array.isArray(spec) ? t(spec[0], spec[1]) : spec;
        return <span className={`v ${col}`}>{txt}</span>;
      };
      const SECTIONS = [
        { title: t('📝 Переводы с разбором', '📝 Übersetzungen mit Analyse'), rows: [
          { f: t('Наборы переводов в день', 'Übersetzungs-Sätze pro Tag'), free: D(1), pro: INF },
          { f: t('Объяснить ошибки / грамматику', 'Fehler / Grammatik erklären'), free: D(1), pro: INF },
          { f: t('Спроси GPT (вопросы)', 'GPT fragen'), free: D(1), pro: INF },
          { f: t('🎭 Загадочная история', '🎭 Rätselgeschichte'), free: NO, pro: YES, ex: true },
          { f: t('🎧 Аудио-разбор ошибок утром', '🎧 Morgendliche Fehler-Audio'), free: NO, pro: YES, ex: true },
        ] },
        { title: t('📖 Словарь', '📖 Wörterbuch'), rows: [
          { f: t('Переводы-запросы', 'Wörterbuch-Abfragen'), free: D(10), pro: INF },
          { f: t('Сохранение слов', 'Wörter speichern'), free: D(20), pro: INF },
          { f: t('Полный разбор (GPT)', 'Tiefenanalyse (GPT)'), free: [t('лимит', 'Limit'), 'Limit'], pro: INF },
        ] },
        { title: t('🃏 Карточки', '🃏 Karteikarten'), rows: [
          { f: t('Новые слова', 'Neue Wörter'), free: ['10/режим', '10/Modus'], pro: INF },
          { f: t('Повторения в день', 'Wiederholungen pro Tag'), free: '20', pro: INF },
        ] },
        { title: t('🎯 Тренажёры', '🎯 Übungen'), rows: [
          { f: t('«Почувствуй слово»', '«Wort fühlen»'), free: D(1), pro: INF },
          { f: t('Числа на слух — своя тренировка', 'Zahlen-Diktat — eigenes Training'), free: NO, pro: D(5) },
          { f: t('Прокачка навыков', 'Skill-Training'), free: NO, pro: INF },
        ] },
        { title: t('📚 Читалка', '📚 Reader'), rows: [
          { f: t('Классика (общие книги)', 'Klassiker (geteilte Bücher)'), free: YES, pro: YES },
          { f: t('Статьи из интернета (DW, Tagesschau…)', 'Web-Artikel (DW, Tagesschau…)'), free: YES, pro: YES },
          { f: t('Добавлять свои книги', 'Eigene Bücher hinzufügen'), free: NO, pro: INF, ex: true },
        ] },
        { title: t('▶️ Видео (YouTube)', '▶️ Videos (YouTube)'), rows: [
          { f: t('Немецкие субтитры', 'Deutsche Untertitel'), free: YES, pro: YES },
          { f: t('Синхронные RU-субтитры', 'Synchrone RU-Untertitel'), free: NO, pro: YES, ex: true },
          { f: t('Видео-рекомендация недели', 'Video-Empfehlung der Woche'), free: NO, pro: YES, ex: true },
        ] },
        { title: t('📬 Задания по расписанию', '📬 Aufgaben nach Plan'), rows: [
          { f: t('Заданий в день', 'Aufgaben pro Tag'), free: '6', pro: '12 → 20' },
          { f: t('Своё расписание и часы', 'Eigener Plan & Zeiten'), free: NO, pro: YES, ex: true },
          { f: t('Приоритетная обработка', 'Priorisierte Verarbeitung'), free: NO, pro: YES, ex: true },
        ] },
        { title: t('🧭 Разделы приложения', '🧭 App-Bereiche'), rows: [
          { f: t('📊 Аналитика прогресса', '📊 Fortschritts-Analyse'), free: NO, pro: YES, ex: true },
          { f: t('🗓 Задачи на сегодня', '🗓 Heutige Aufgaben'), free: NO, pro: YES, ex: true },
          { f: t('📅 План на неделю', '📅 Wochenplan'), free: NO, pro: YES, ex: true },
          { f: t('🗺 Слабые навыки: карта + точечная тренировка', '🗺 Schwache Skills: Karte + gezieltes Training'), free: NO, pro: YES, ex: true },
        ] },
        { title: t('⚔️ Дуэли и игры', '⚔️ Duelle & Spiele'), rows: [
          { f: t('Играть по приглашению', 'Auf Einladung spielen'), free: YES, pro: YES },
          { f: t('Создавать свои дуэли', 'Eigene Duelle erstellen'), free: NO, pro: YES, ex: true },
          { f: t('Игры, кроссворды, новости', 'Spiele, Kreuzworträtsel, News'), free: YES, pro: YES },
        ] },
        { title: t('📥 Захват слов', '📥 Worterfassung'), rows: [
          { f: t('Пересылка сообщений боту', 'Nachrichten an den Bot'), free: D(1), pro: INF },
          { f: t('Shortcut «Ночной Переводчик»', 'Shortcut «Nacht-Übersetzer»'), free: [t('5 проб всего', '5 Testläufe'), '5 Testläufe'], pro: D(2) },
        ] },
      ];
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Ты увидел, что умеет бот. Вот что входит в каждый тариф — Free и Pro. Озвучка книг оплачивается отдельно.',
               'Du hast gesehen, was der Bot kann. Das steckt in jedem Tarif — Free und Pro. Buch-Vertonung wird separat bezahlt.')}
          </p>

          <div className="ob-cmp">
            <div className="ob-cmp-head">
              <div className="h-feat">{t('Возможность', 'Funktion')}</div>
              <div className="h-free"><span className="n">Free</span><span className="tag">{t('бесплатно', 'gratis')}</span></div>
              <div className="h-pro"><span className="n">Pro</span><span className="tag">💎</span></div>
            </div>
            <div className="ob-cmp-grid">
              {SECTIONS.map((s, si) => (
                <React.Fragment key={si}>
                  <div className="ob-cmp-sect">{s.title}</div>
                  {s.rows.map((r, ri) => (
                    <div className={`ob-cmp-row${r.ex ? ' ex' : ''}`} key={ri}>
                      <div className="f">{r.f}</div>
                      <div className="c-free">{cmpCell(r.free, 'free')}</div>
                      <div className="c-pro">{cmpCell(r.pro, 'pro')}</div>
                    </div>
                  ))}
                </React.Fragment>
              ))}
            </div>
          </div>

          <div className="ob-cmp-legend">
            <span><i className="yes">✓</i>{t('входит', 'inklusive')}</span>
            <span><i className="no">✕</i>{t('нет', 'nein')}</span>
            <span><b style={{ color: '#1b7a53' }}>∞</b>{t('без ограничений', 'unbegrenzt')}</span>
          </div>

          <div className="ob-info is-trial">
            <div className="ic">🎁</div>
            <div>
              <h4>{t('7 дней Pro — бесплатно каждому', '7 Tage Pro — für jeden gratis')}</h4>
              <p>{t('Каждый новый пользователь сразу получает полный Pro на 7 дней, чтобы попробовать всё. Без карты, автоматически.',
                    'Jeder neue Nutzer erhält sofort 7 Tage volles Pro, um alles auszuprobieren. Ohne Karte, automatisch.')}</p>
            </div>
          </div>

          <div className="ob-info is-audio">
            <div className="ic">🔊</div>
            <div>
              <h4>{t('Озвучка книг — отдельно', 'Buch-Vertonung — separat')}</h4>
              <p>{t('Не входит ни в Free, ни в Pro. Оплачивается за каждую книгу отдельно звёздами: 3 голоса и 25 / 50 / 100 % книги. ',
                    'Weder in Free noch in Pro enthalten. Wird pro Buch separat mit Sternen bezahlt: 3 Stimmen und 25 / 50 / 100 % des Buches. ')}
                <b>{t('Классика озвучена бесплатно для всех.', 'Klassiker sind für alle gratis vertont.')}</b>{' '}
                {t('Озвучить книгу можно прямо в Читалке: открываешь книгу → кнопка ▶.',
                   'Ein Buch vertonst du direkt im Reader: Buch öffnen → ▶-Knopf.')}</p>
            </div>
          </div>

          <div className="ob-info is-stars">
            <div className="ic">⭐</div>
            <div>
              <h4>{t('Что такое звёзды Telegram', 'Was sind Telegram-Sterne')}</h4>
              <p>{t('Звёзды — официальная валюта Telegram. Покупаются в пару тапов прямо в приложении, безопасно, без карт на сторонних сайтах. Ими оплачивается Pro и озвучка книг.',
                    'Sterne sind die offizielle Telegram-Währung. In wenigen Taps direkt in der App gekauft — sicher, ohne Karte auf fremden Seiten. Damit zahlst du Pro und Buch-Vertonung.')}</p>
            </div>
          </div>

          <div className="ob-info is-cancel">
            <div className="ic">🔁</div>
            <div>
              <h4>{t('Как отменить Pro', 'Pro kündigen')}</h4>
              <p>{t('Pro продлевается автоматически раз в месяц. Отменить можно в любой момент прямо в Telegram: Настройки → Telegram Stars и подписки → выбери бота → «Отменить». Доступ сохранится до конца оплаченного периода.',
                    'Pro verlängert sich automatisch monatlich. Jederzeit kündbar direkt in Telegram: Einstellungen → Telegram Stars und Abos → Bot wählen → „Kündigen“. Der Zugang bleibt bis zum Ende des bezahlten Zeitraums.')}</p>
            </div>
          </div>

          <div className="ob-info is-maint">
            <div className="ic">🎙</div>
            <div>
              <h4>{t('Разговорная практика — на доработке', 'Sprechpraxis — in Arbeit')}</h4>
              <p>{t('Раздел временно выключен: оптимизируем, чтобы он работал быстро и стабильно. Скоро вернём.',
                    'Der Bereich ist vorübergehend deaktiviert: wir optimieren ihn für schnelle, stabile Nutzung. Bald wieder da.')}</p>
            </div>
          </div>

          <div className="ob-plan ob-plan-pro">
            <p className="ob-plan-title">💎 <b>Pro</b>{proPrice ? <> — <span className="ob-price">{proPrice}</span></> : <> — <span className="ob-price">292 ⭐ / {t('мес', 'Mon.')}</span></>}</p>
            <button type="button" className="ob-confirm" onClick={onOpenSubscription}>
              {t('✨ Оформить Pro', '✨ Pro holen')}
            </button>
            <span className="ob-muted-note">{t('Кнопка откроет экран «Подписка» — там актуальная цена и оплата в Telegram Stars. Открыть можно в любой момент из меню бота.', 'Der Knopf öffnet den «Abo»-Bildschirm — dort der aktuelle Preis und Zahlung mit Telegram Stars. Jederzeit über das Bot-Menü erreichbar.')}</span>
          </div>
        </div>
      );
    }
    case 'howto_translations':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Бот даёт тебе предложение, ты переводишь на немецкий — а он не просто ставит оценку, а ',
               'Der Bot gibt dir einen Satz, du übersetzt ins Deutsche — und er gibt nicht nur eine Note, sondern ')}
            <b>{t('объясняет грамматику', 'erklärt die Grammatik')}</b>
            {t(': что верно, что нет и почему.', ': was richtig ist, was nicht und warum.')}
          </p>
          <p className="ob-lead">
            {t('Есть режимы и уровни сложности — от простого к продвинутому. Незнакомые слова из перевода тоже сохраняешь в словарь одной кнопкой.',
               'Es gibt Modi und Schwierigkeitsstufen — von einfach bis fortgeschritten. Unbekannte Wörter aus der Übersetzung speicherst du auch mit einem Knopf ins Wörterbuch.')}
          </p>
        </div>
      );
    case 'howto_tools':
      return (
        <div className="ob-stub">
          <p className="ob-lead">{t('И ещё несколько удобных вещей:', 'Und noch ein paar praktische Dinge:')}</p>
          <ul className="ob-list">
            <li>
              <b>{t('🃏 Карточки', '🃏 Karten')}</b>
              {t(' — повторяешь сохранённые слова по умной системе, пока не запомнишь.', ' — du wiederholst gespeicherte Wörter nach einem klugen System, bis du sie kannst.')}
            </li>
            <li>
              <b>{t('▶️ YouTube с двойными субтитрами', '▶️ YouTube mit doppelten Untertiteln')}</b>
              {t(' — смотришь видео с субтитрами на двух языках сразу, а любое слово сохраняешь в словарь в один тап.',
                 ' — du schaust Videos mit Untertiteln in zwei Sprachen gleichzeitig und speicherst jedes Wort mit einem Tipp ins Wörterbuch.')}
              {MEDIA.youtube_dual_subs ? <MediaTile src={MEDIA.youtube_dual_subs} type="video" caption={t('🎥 YouTube с двойными субтитрами', '🎥 YouTube mit doppelten Untertiteln')} /> : null}
              {MEDIA.youtube_browser ? <MediaTile src={MEDIA.youtube_browser} type="video" caption={t('🎥 Так же можно смотреть в браузере', '🎥 So kannst du auch im Browser schauen')} /> : null}
            </li>
            <li>
              <b>{t('📖 Читалка', '📖 Reader')}</b>
              {t(' — загружаешь книгу и читаешь прямо в приложении. Каждое слово кликабельно: нажал — перевод и разбор; можно переводить и целыми предложениями. Что отметил — сохраняешь в словарь.',
                 ' — du lädst ein Buch und liest direkt in der App. Jedes Wort ist anklickbar: getippt — Übersetzung und Analyse; auch ganze Sätze kannst du übersetzen. Was du markierst, speicherst du ins Wörterbuch.')}
              {t(' 🎧 Книги можно не только читать, но и слушать: у базовых книг озвучка доступна бесплатно. А если хочешь послушать свою книгу — её озвучку можно заказать отдельно, по желанию и за небольшую доплату.',
                 ' 🎧 Bücher kannst du nicht nur lesen, sondern auch hören: bei den Basis-Büchern ist die Audio-Vertonung kostenlos. Und wenn du dein eigenes Buch anhören möchtest — die Vertonung dafür kannst du separat auf Wunsch gegen einen kleinen Aufpreis bestellen.')}
            </li>
            <li>
              <b>{t('📰 Новости прямо в читалке', '📰 Nachrichten direkt im Reader')}</b>
              {t(' — ничего не нужно искать и копировать. Заходишь в читалку, в разделе «Источники» выбираешь свежую статью (Deutsche Welle, Tagesschau, Nachrichtenleicht — новости простым языком, A2–B1) — и она открывается прямо в читалке. Читаешь как книгу: нажимаешь на любое слово или целое предложение — получаешь перевод с разбором и сохраняешь нужное в словарь, чтобы выучить позже.',
                 ' — du musst nichts suchen oder kopieren. Du gehst in den Reader, wählst im Bereich «Quellen» einen aktuellen Artikel (Deutsche Welle, Tagesschau, Nachrichtenleicht — Nachrichten in einfacher Sprache, A2–B1) — und er öffnet sich direkt im Reader. Du liest wie ein Buch: tippe auf jedes Wort oder einen ganzen Satz — du bekommst die Übersetzung mit Analyse und speicherst, was du brauchst, ins Wörterbuch, um es später zu lernen.')}
            </li>
          </ul>
        </div>
      );
    case 'keyboard':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('В чате с ботом под полем ввода есть кнопки-меню. Если их не видно — нажми значок с квадратиками справа от строки сообщения. Вот главные:',
               'Im Chat mit dem Bot gibt es unter dem Eingabefeld Menü-Tasten. Siehst du sie nicht — tippe auf das Kästchen-Symbol rechts neben der Nachrichtenzeile. Die wichtigsten:')}
          </p>
          <ul className="ob-list">
            <li>
              <b>{t('▶️ «Следующее задание»', '▶️ «Nächste Aufgabe»')}</b>
              {t(' — показывает, что ты ещё не сделал сегодня. Нажал — и решаешь.', ' — zeigt, was du heute noch nicht gemacht hast. Getippt — und du löst.')}
            </li>
            <li>
              <b>{t('📚 «Интерактив»', '📚 «Interaktiv»')}</b>
              {t(' — одна кнопка открывает все тренажёры: артикли, окончания прилагательных, Wo-Fragen и числа на слух.',
                 ' — eine Taste öffnet alle Übungen: Artikel, Adjektivendungen, Wo-Fragen und Zahlen nach Gehör.')}
            </li>
            <li>
              <b>{t('⚔️ «Battles»', '⚔️ «Battles»')}</b>
              {t(' — дуэли с другими учениками: создать батл, пригласить игроков и посмотреть историю — всё на одной странице.',
                 ' — Duelle mit anderen Lernenden: ein Battle erstellen, Spieler einladen und die Historie ansehen — alles auf einer Seite.')}
            </li>
            <li>
              <b>{t('⚙️ «Настройки»', '⚙️ «Einstellungen»')}</b>
              {t(' — одна кнопка открывает страницу настроек: автосейв, готовность к батлам и расписание (Pro). Всё в одном месте, меняется в любой момент.',
                 ' — eine Taste öffnet die Einstellungsseite: Auto-Speichern, Duell-Bereitschaft und Zeitplan (Pro). Alles an einem Ort, jederzeit änderbar.')}
            </li>
            <li>
              <b>{t('📖 Словарь и 🤖 учитель', '📖 Wörterbuch und 🤖 Lehrer')}</b>
              {t(' — быстро перевести слово или задать вопрос по немецкому.', ' — schnell ein Wort übersetzen oder eine Frage zum Deutschen stellen.')}
            </li>
            <li>
              <b>{t('🎬 «Как пользоваться»', '🎬 «Wie man es benutzt»')}</b>
              {t(' — сюда возвращаешься за настройками и обучающими видео в любой момент.', ' — hierher kommst du jederzeit für Einstellungen und Lern-Videos zurück.')}
            </li>
          </ul>
          <p className="ob-lead"><b>{t('Что внутри «⚙️ Настройки»', 'Was in «⚙️ Einstellungen» steckt')}</b>{t(' — одна страница, всё переключается сразу:', ' — eine Seite, alles sofort umschaltbar:')}</p>
          <ul className="ob-list">
            <li>
              <b>{t('🌙 Автосейв', '🌙 Auto-Speichern')}</b>
              {t(' — как приходят слова, пойманные за день (через Shortcut или пересланные боту). ВКЛ (по умолчанию): копятся и приходят одним вечерним списком с галочками — не дёргают весь день. ВЫКЛ: каждое слово приходит сразу.',
                 ' — wie die tagsüber gefangenen Wörter ankommen (per Shortcut oder an den Bot weitergeleitet). AN (Standard): sie sammeln sich und kommen abends als eine Liste mit Häkchen — kein Stören den ganzen Tag. AUS: jedes Wort kommt sofort.')}
            </li>
            <li>
              <b>{t('🛡 Готовность к батлам', '🛡 Bereit für Duelle')}</b>
              {t(' — зовут ли тебя другие ученики на дуэли по грамматике.', ' — ob dich andere Lernende zu Grammatik-Duellen einladen.')}
            </li>
            <li>
              <b>{t('🗓 Расписание (Pro)', '🗓 Zeitplan (Pro)')}</b>
              {t(' — сколько заданий в день (темп) и в какие часы их присылать.', ' — wie viele Aufgaben pro Tag (Tempo) und zu welchen Zeiten sie kommen.')}
            </li>
          </ul>
        </div>
      );
    case 'core':
      return (
        <div className="ob-stub">
          <p className="ob-lead">Здесь будет настройка (обязательный шаг). Пока — заглушка каркаса.</p>
          <button
            type="button"
            className={`ob-confirm ${confirmed ? 'is-done' : ''}`}
            onClick={onConfirm}
            disabled={confirmed}
          >
            {confirmed ? '✅ Подтверждено' : '✓ Подтвердить (заглушка)'}
          </button>
        </div>
      );
    case 'pro':
      return isPro ? (
        <p className="ob-lead">Здесь будет выбор (Pro). Пока — заглушка каркаса.</p>
      ) : (
        <div className="ob-teaser">
          <p className="ob-lead">
            На бесплатном — подборка заданий в день. В <b>Pro</b> можно настроить количество
            и время. Пока — заглушка каркаса.
          </p>
          <span className="ob-lock">🔒 Только в Pro</span>
        </div>
      );
    case 'opt':
      return <p className="ob-lead">Необязательный шаг. Здесь будет выбор. Пока — заглушка каркаса.</p>;
    case 'info':
      return <p className="ob-lead">Короткое объяснение + медиа. Пока — заглушка каркаса.</p>;
    case 'finale':
      return IS_PWA_TOUR ? (
        <p className="ob-lead">
          {t('Вот и весь обзор 🎉 Это был краткий тур по возможностям бота. Закрой его — и продолжай пользоваться приложением.',
             'Das war der Überblick 🎉 Ein kurzer Rundgang durch die Möglichkeiten des Bots. Schließe ihn — und nutze die App weiter.')}
        </p>
      ) : IS_PUBLIC ? (
        <p className="ob-lead">
          {t('Вот и всё, что умеет бот 🎉 Понравилось? Установи его и начни учить немецкий по-настоящему — каждый день. Жми кнопку ниже 👇',
             'Das war alles, was der Bot kann 🎉 Gefällt es dir? Installiere ihn und lerne Deutsch richtig — jeden Tag. Tippe auf die Taste unten 👇')}
        </p>
      ) : (
        <p className="ob-lead">
          {t('Всё настроено! Задания уже ждут в чате. Захочешь что-то поменять — всё здесь же, под кнопкой ',
             'Alles eingerichtet! Die Aufgaben warten schon im Chat. Willst du etwas ändern — alles ist hier, unter der Taste ')}
          <b>{t('🎬 Как пользоваться', '🎬 Wie man es benutzt')}</b>.
        </p>
      );
    default:
      return null;
  }
}

export default function OnboardingWizard() {
  const [idx, setIdx] = useState(0);
  const [isPro, setIsPro] = useState(false);
  const [confirmed, setConfirmed] = useState({});
  const [loading, setLoading] = useState(true);
  const [finishing, setFinishing] = useState(false);
  const [done, setDone] = useState(false);
  const [busy, setBusy] = useState(false);      // per-step async action in flight
  const [dictBusy, setDictBusy] = useState(''); // which dict button is in flight: quick|full|decline
  const [dictChoice, setDictChoice] = useState(''); // final dict decision: quick|full|decline
  const [stepErr, setStepErr] = useState('');
  const [dictOffer, setDictOffer] = useState(null);  // starter-dictionary offer
  const [selPreset, setSelPreset] = useState('normal');   // intensity (visual default)
  const [selWindow, setSelWindow] = useState('allday');   // active window (visual default)
  const [selBattle, setSelBattle] = useState(null);       // battle readiness: null|'yes'|'no'
  const [botUrl, setBotUrl] = useState('');               // install link (public tour only)
  const [shortcutOpening, setShortcutOpening] = useState(false); // «Настроить сейчас» in flight
  const [proPrice, setProPrice] = useState('');           // live Pro price label (Stripe-configured)

  // Force LIGHT theme (owner: onboarding is always light, in the interactive style).
  useEffect(() => {
    try {
      tg?.ready?.();
      tg?.expand?.();
      // Lock the sheet vertically: a careless swipe-down must NOT close/minimize the wizard
      // (Bot API 7.7+). Without this the long, scrollable steps slide the whole sheet away on
      // any downward drag. Closing stays possible only via the header. Same as the other overlays.
      tg?.disableVerticalSwipes?.();
    } catch (_e) { /* ignore */ }
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* ignore */ }
  }, []);

  // Bot link — for the public-tour install CTA and the «open Shortcut setup» deeplink.
  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        if (!off && d.bot_url) setBotUrl(d.bot_url);
      } catch (_e) { /* CTA falls back to a generic label */ }
    })();
    return () => { off = true; };
  }, []);

  // Load state (resume point + tier).
  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const d = await api('/api/webapp/onboarding/status');
        if (off) return;
        setIsPro(!!d.is_pro);
        const resume = d.completed ? 0 : Math.max(0, Math.min(Number(d.current_step) || 0, STEPS.length - 1));
        setIdx(resume);
      } catch (_e) {
        /* start from the top if status is unreachable */
      } finally {
        if (!off) setLoading(false);
      }
    })();
    return () => { off = true; };
  }, []);

  // Persist the resume point when the step changes (fire-and-forget).
  useEffect(() => {
    if (loading) return;
    setStepErr('');
    setBusy(false);
    api('/api/webapp/onboarding/step', { step: idx }).catch(() => {});
  }, [idx, loading]);

  const step = STEPS[idx];
  const isLast = idx === STEPS.length - 1;
  // Public tour: no gating (just click through). Telegram: core steps need confirm.
  const canNext = IS_PUBLIC || step.kind !== 'core' || !!confirmed[step.id];

  // «Далее» unlocks only after the user has scrolled the step to the bottom — so long
  // pages (with an install button / more info below) are actually read to the end.
  const bodyRef = useRef(null);
  const [atBottom, setAtBottom] = useState(false);
  // «Далее» must not unlock on a still-blank page (slow first paint) — otherwise the
  // user swipes past content they never saw. Gate on the body having actually rendered
  // text. Sticky per step (only ever flips ON) so a later scroll can't re-lock it.
  const [contentReady, setContentReady] = useState(false);
  const checkScrollBottom = useCallback(() => {
    const el = bodyRef.current;
    if (!el) { setAtBottom(true); return; }
    const hasContent = (el.textContent || '').trim().length > 0 && el.scrollHeight > 24;
    if (hasContent) setContentReady(true);
    const fits = el.scrollHeight <= el.clientHeight + 6;
    const scrolledEnd = el.scrollTop + el.clientHeight >= el.scrollHeight - 24;
    setAtBottom(fits || scrolledEnd);
  }, []);
  useEffect(() => {
    setAtBottom(false);
    setContentReady(false);
    const r = requestAnimationFrame(checkScrollBottom);
    const t1 = setTimeout(checkScrollBottom, 350);
    const t2 = setTimeout(checkScrollBottom, 900);
    // Safety net: never leave «Далее» permanently blocked if the check ever misfires.
    const tSafe = setTimeout(() => setContentReady(true), 4000);
    return () => { cancelAnimationFrame(r); clearTimeout(t1); clearTimeout(t2); clearTimeout(tSafe); };
  }, [idx, checkScrollBottom]);

  const goNext = useCallback(async () => {
    if (!canNext) return;
    try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
    if (!isLast) { setIdx((i) => Math.min(i + 1, STEPS.length - 1)); return; }
    // Finale. Standalone PWA → viewer already has the bot: just close the tour and
    // return to the app. Public browser tour → send the viewer to install the bot.
    // In Telegram → complete + open the main Mini-App.
    if (IS_PWA_TOUR) {
      try {
        if (window.history.length > 1) window.history.back();
        else window.location.href = '/';
      } catch (_e) { try { window.location.href = '/'; } catch (_e2) { /* ignore */ } }
      return;
    }
    if (IS_PUBLIC) {
      if (botUrl) { try { window.location.href = botUrl; } catch (_e) { /* ignore */ } }
      return;
    }
    setFinishing(true);
    try {
      await api('/api/webapp/onboarding/complete');
      setDone(true);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      // Single «Перейти в приложение» endpoint (moved here from the Shortcut screen):
      // open the main Mini-App. If the deep link isn't ready, fall back to closing to chat.
      setTimeout(() => {
        const url = botUrl ? `${botUrl}?startapp=webapp` : '';
        try {
          if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
          else tg?.close?.();
        } catch (_e) { try { tg?.close?.(); } catch (_e2) { /* ignore */ } }
      }, 900);
    } catch (_e) {
      setFinishing(false);
    }
  }, [canNext, isLast, botUrl]);

  const goBack = useCallback(() => {
    setIdx((i) => Math.max(i - 1, 0));
  }, []);

  // «Настроить сейчас» on the Shortcut step (the last content step): complete
  // onboarding and hand off to the full Shortcut setup screen.
  const openShortcutSetup = useCallback(async () => {
    if (shortcutOpening) return;
    setShortcutOpening(true);
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
    // Complete onboarding in the BACKGROUND — don't block the open on it.
    api('/api/webapp/onboarding/complete').catch(() => {});
    // The deeplink needs bot_url; it loads async on mount, so on an early tap it may
    // still be empty (this is why it used to need several taps). Fetch it inline if so.
    let base = botUrl;
    if (!base) {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        base = d.bot_url || '';
        if (base) setBotUrl(base);
      } catch (_e) { /* noop */ }
    }
    const url = base ? `${base}?startapp=shortcut` : '';
    try {
      if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
      else if (url) window.location.href = url;
    } catch (_e) { /* noop */ }
    setShortcutOpening(false);
  }, [botUrl, shortcutOpening]);

  // «Оформить Pro» on the plans step. Stars can only be charged INSIDE Telegram, and the
  // onboarding (esp. the standalone PWA) has no initData to open a payment itself — so we
  // hand off to the bot. `startapp=buypro` lands in the Mini-App AND auto-opens the Pro
  // Stars sheet immediately (no «Подписка» screen, no second «купить» tap) — the shortest
  // path Stars allows. Inside Telegram it's an in-place jump.
  const openSubscription = useCallback(async () => {
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
    let base = botUrl;
    if (!base) {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        base = d.bot_url || '';
        if (base) setBotUrl(base);
      } catch (_e) { /* noop */ }
    }
    const url = base ? `${base}?startapp=buypro` : '';
    try {
      if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
      else if (url) window.location.href = url;
    } catch (_e) { /* noop */ }
  }, [botUrl]);

  // Pull the LIVE Pro price (Stripe-configured, not hard-coded) when the user reaches
  // the plans step, so the tariff card never shows a stale amount. Failure is fine —
  // the price just stays hidden and the CTA still leads to the up-to-date paywall.
  useEffect(() => {
    if (loading || step.id !== 'plans' || proPrice) return;
    let off = false;
    (async () => {
      try {
        const r = await fetch('/api/billing/plans');
        const d = await r.json().catch(() => ({}));
        const pro = (Array.isArray(d.plans) ? d.plans : []).find((p) => p.plan_code === 'pro');
        const amount = pro && pro.amount_value != null ? Number(pro.amount_value) : null;
        if (!off && amount != null && !Number.isNaN(amount)) {
          const per = pro.recurring_interval === 'year' ? t('год', 'Jahr')
            : pro.recurring_interval === 'week' ? t('неделя', 'Woche')
            : t('месяц', 'Monat');
          // Pro is paid in Telegram Stars now → show the ⭐ price.
          // (Rate mirrors backend STARS_PER_EUR=50 / STARS_MARKUP=1.30.)
          const stars = Math.max(1, Math.ceil(amount * 1.30 * 50));
          setProPrice(`${stars} ⭐ / ${per}`);
        }
      } catch (_e) { /* price hidden; CTA still opens the live paywall */ }
    })();
    return () => { off = true; };
  }, [step.id, loading, proPrice]);

  const confirmStep = useCallback(async () => {
    setStepErr('');
    // Steps that persist something confirm asynchronously (save → then unlock).
    if (step.id === 'language') {
      setBusy(true);
      try {
        // German-only mode → fixed pair; this upserts the profile row.
        await api('/api/user/language-profile', { native_language: 'ru', learning_language: 'de' });
      } catch (_e) {
        setStepErr('Не удалось сохранить. Попробуй ещё раз.');
        setBusy(false);
        return;
      }
      setBusy(false);
    }
    setConfirmed((c) => ({ ...c, [step.id]: true }));
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
  }, [step.id]);

  // Load the starter-dictionary offer when the user reaches that step.
  useEffect(() => {
    if (loading || step.id !== 'dictionary' || dictOffer) return;
    let off = false;
    (async () => {
      try {
        const d = await api('/api/webapp/starter-dictionary/status');
        if (!off) setDictOffer(d.offer || {});
      } catch (_e) { /* offer optional — connect/skip still work */ }
    })();
    return () => { off = true; };
  }, [step.id, loading, dictOffer]);

  // Battle readiness defaults ON: opt the user in the first time they reach the step.
  useEffect(() => {
    if (loading || step.id !== 'battles' || selBattle !== null) return;
    setSelBattle('yes');
    api('/api/webapp/onboarding/battles', { opt_in: true }).catch(() => {});
  }, [step.id, loading, selBattle]);

  // Intensity/window are [R] (optional, default-accept): pick = optimistic + save.
  const pickPreset = useCallback((code) => {
    setSelPreset(code);
    api('/api/webapp/onboarding/preset', { preset: code }).catch(() => {});
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
  }, []);
  const pickWindow = useCallback((key) => {
    setSelWindow(key);
    api('/api/webapp/onboarding/window', { window: key }).catch(() => {});
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
  }, []);
  const pickBattle = useCallback((optIn) => {
    setSelBattle(optIn ? 'yes' : 'no');
    api('/api/webapp/onboarding/battles', { opt_in: !!optIn }).catch(() => {});
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
  }, []);

  // Connect / skip the base dictionary (accept starts a background import job).
  const dictAction = useCallback(async (action, full = false) => {
    setStepErr('');
    if (action === 'keep') {
      // Base dictionary already connected — just confirm the step (no API) so «Далее»
      // unlocks. Used by «Пропустить» in the partial state (base kept, full skipped).
      setConfirmed((c) => ({ ...c, dictionary: true }));
      setDictChoice('keep');
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      return;
    }
    setDictBusy(action === 'decline' ? 'decline' : (full ? 'full' : 'quick'));
    setBusy(true);
    try {
      await api('/api/webapp/starter-dictionary/apply', { action, full: !!full });
      setConfirmed((c) => ({ ...c, dictionary: true }));
      setDictChoice(action === 'decline' ? 'decline' : (full ? 'full' : 'quick'));
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
    } catch (_e) {
      setStepErr('Не удалось. Попробуй ещё раз.');
    } finally {
      setBusy(false);
      setDictBusy('');
    }
  }, []);

  const pct = useMemo(() => Math.round(((idx + 1) / STEPS.length) * 100), [idx]);

  if (loading) {
    return <div className="ob-root"><div className="ob-loading">{t('Загрузка…', 'Lädt…')}</div></div>;
  }

  return (
    <div className="ob-root">
      <div className="ob-card">
        <header className="ob-head">
          <div className="ob-topbar">
            <button type="button" className="ob-lang" onClick={switchLang}>
              {LANG === 'de' ? '🌐 RU' : '🌐 DE'}
            </button>
          </div>
          <div className="ob-progress">
            <span className="ob-step-label">{t('Шаг', 'Schritt')} {idx + 1} {t('из', 'von')} {STEPS.length}</span>
            <div className="ob-bar"><div className="ob-bar-fill" style={{ width: `${pct}%` }} /></div>
          </div>
          <h1 className="ob-title">{step.title}</h1>
        </header>

        <main className="ob-body" ref={bodyRef} onScroll={checkScrollBottom}>
          <StepBody
            step={step}
            isPro={isPro}
            confirmed={!!confirmed[step.id]}
            busy={busy}
            dictBusy={dictBusy}
            dictChoice={dictChoice}
            stepErr={stepErr}
            onConfirm={confirmStep}
            dictOffer={dictOffer}
            onDictAction={dictAction}
            selPreset={selPreset}
            selWindow={selWindow}
            onPickPreset={pickPreset}
            onPickWindow={pickWindow}
            selBattle={selBattle}
            onPickBattle={pickBattle}
            onShortcutSetup={openShortcutSetup}
            shortcutOpening={shortcutOpening}
            proPrice={proPrice}
            onOpenSubscription={openSubscription}
          />
        </main>

        <footer className="ob-nav">
          <button type="button" className="ob-btn ob-back" onClick={goBack} disabled={idx === 0 || finishing}>
            ← {t('Назад', 'Zurück')}
          </button>
          <button
            type="button"
            className="ob-btn ob-next"
            onClick={goNext}
            disabled={!canNext || finishing || done || !contentReady || !atBottom}
          >
            {done ? t('✅ Готово', '✅ Fertig')
              : !contentReady ? t('⏳ Загрузка…', '⏳ Lädt…')
              : !atBottom ? t('↓ Прокрути вниз', '↓ Nach unten scrollen')
              : isLast ? (IS_PWA_TOUR ? t('Закрыть', 'Schließen') : IS_PUBLIC ? t('🚀 Установить бота', '🚀 Bot installieren') : t('🎯 Перейти в приложение', '🎯 Zur App'))
              : t('Далее →', 'Weiter →')}
          </button>
        </footer>
      </div>
    </div>
  );
}
