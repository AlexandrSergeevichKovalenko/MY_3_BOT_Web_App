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

// The durable APP token: minted once per account and carried by the home-screen app (and by
// any browser tab launched with ?aqt=). Outside Telegram there is no initData, but this token
// identifies the SAME account — so the tour can really SAVE what the user picks here.
const APP_TOKEN = (() => {
  if (typeof window === 'undefined') return '';
  try {
    const q = String(new URLSearchParams(window.location.search).get('aqt') || '').trim();
    if (q) return q;
  } catch (_e) { /* noop */ }
  try {
    const m = String(window.location.pathname || '').match(/^\/webapp\/t\/([^/?#]+)/);
    if (m && m[1]) return decodeURIComponent(m[1]);
  } catch (_e) { /* noop */ }
  try { return String(localStorage.getItem('app_browser_token_v1') || '').trim(); }
  catch (_e) { return ''; }
})();

// Does the viewer have an ACCOUNT we can save to? Telegram (initData) or the standalone app
// (durable token). This — NOT «are we inside Telegram» — is what decides whether a step shows
// real controls or an informational stub. Previously the two were conflated, so the installed
// home-screen app degraded to text-only even though it was fully authenticated.
const HAS_ACCOUNT = !IS_PUBLIC || !!APP_TOKEN;

// Тур открыт ПОВЕРХ текущего окна приложения (standalone PWA, ИЛИ ?ob_inplace=1 —
// планшетный тур из Guide, который перезагрузил широкое окно вместо запуска отдельного
// мини-аппа). В этом случае финал = «Закрыть» и возврат назад в приложение, а не
// «Перейти в приложение» (иначе открылось бы второе окно поверх уже открытого).
const OB_INPLACE = (() => {
  try { return new URLSearchParams(window.location.search).get('ob_inplace') === '1'; } catch (_e) { return false; }
})();
const TOUR_IN_PLACE = IS_STANDALONE_PWA || OB_INPLACE;

// A ✕ close affordance so the tour can be abandoned at ANY step (not only by reaching
// the finale). Hidden only in the pure public web tour (plain browser, no bot), where
// there's no app to return to. In the standalone PWA the tour opened IN PLACE over the
// app, so leaving = go back to the app; in Telegram it's a sheet → tg.close().
const CAN_CLOSE = IS_STANDALONE_PWA || HAS_ACCOUNT;

// An existing user viewing their OWN finale (Telegram sheet OR their installed PWA) — as
// opposed to a cold public-web visitor. Only they get the «share the tour with a friend»
// button (a public visitor has no bot / no referral code yet).
const CAN_SHARE = HAS_ACCOUNT || IS_STANDALONE_PWA;

// A guest viewing the shared tour in a plain browser — they DON'T have the bot yet (no
// account). The install-the-icon / dictionary-icon / Shortcut / buy-Pro actions all need
// an account (they open the app/dict → the «войдите через Telegram» gate, which is a
// dead end for a guest). So in the guest tour those steps stay as INFO but their action
// buttons are replaced with a note: install the bot from the finale first, then the real
// onboarding (with the bot) offers working buttons. Anyone with an account — Telegram OR the
// installed app carrying its durable token — is NOT a guest and keeps the real buttons. The
// installed icon is never a guest either, even if its token went missing (it HAS the bot —
// telling it to «install the bot» would be nonsense; only saving degrades, see HAS_ACCOUNT).
const IS_GUEST_TOUR = !HAS_ACCOUNT && !IS_STANDALONE_PWA;

// Тур открыт как ОТДЕЛЬНЫЙ мини-апп из личка (кнопка «Как пользоваться»), а не поверх
// уже открытого приложения. Здесь у финала ДВЕ кнопки: «Закрыть» (вернуться в переписку,
// где человек и был) и «Закрыть и открыть приложение» (сразу пойти заниматься).
const TG_SHEET_TOUR = !TOUR_IN_PLACE && !IS_PUBLIC;

// A friend-shared tour is opened as /tour?ref=<referrer id>. Thread that code into the
// finale install CTA (→ t.me/<bot>?start=ref_<code>) so the referral is attributed when
// the friend installs the bot — the existing streak-triggered payout then rewards both.
const REF_PARAM = (() => {
  try { return String(new URLSearchParams(window.location.search).get('ref') || '').trim(); }
  catch (_e) { return ''; }
})();

// Onboarding language — ALWAYS opens in Russian. Our audience is Russian-speaking
// learners, so EVERY fresh open is RU regardless of the app-wide ui_lang (a user whose
// main app is German still gets the RU onboarding). The language is therefore NOT read
// from ui_lang; it's driven by a ?oblang= URL param that ONLY the in-wizard 🌐 toggle
// sets (see switchLang). That param survives the one reload the toggle triggers, so a
// switch to German holds while you read — but the next fresh open has no param → RU again.
const OBLANG_PARAM = (() => {
  try {
    const p = (new URLSearchParams(window.location.search).get('oblang') || '').toLowerCase();
    if (p === 'de' || p === 'ru') return p;
  } catch (_e) { /* noop */ }
  return '';
})();
const LANG = OBLANG_PARAM || 'ru';
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
  skill_map_photo: `${R2}/skill_map/skill_map.png`,                                   // skill_map — карта навыков (скрин)
  skill_training:  `${R2}/skill_map/skill_training.mp4`,                              // skill_map — точечная тренировка слабого навыка
  interactives: [                                                                     // howto_interactives
    { src: `${R2}/interactives/interactive_1.mp4`, caption: t('🎥 Артикли der/die/das — Artikelquiz', '🎥 Artikel der/die/das — Artikelquiz') },
    { src: `${R2}/interactives/interactive_2.mp4`, caption: t('🎥 Вопросы Wo-Fragen', '🎥 Wo-Fragen') },
    { src: `${R2}/interactives/interactive_3.mp4`, caption: t('🎥 Окончания прилагательных', '🎥 Adjektivendungen') },
    { src: `${R2}/interactives/interactive_4.mp4`, caption: t('🎥 Анаграммы — собери слово (Anagramm)', '🎥 Anagramm — Wort zusammensetzen') },
    { src: `${R2}/interactives/interactive_5.mp4`, caption: t('🎥 Artikel Sprint — der/die/das на скорость', '🎥 Artikel Sprint — der/die/das auf Zeit') },
  ],
};

// Switch the wizard language. Reload (so the module-level strings re-read) with a
// ?oblang= param that pins the choice through THIS reload only — a later fresh open has
// no param and defaults back to RU. Mirror into ui_lang too so the main app keeps the
// chosen language after onboarding ends. The URL hash (Telegram initData) is preserved.
function switchLang() {
  const target = LANG === 'de' ? 'ru' : 'de';
  try { localStorage.setItem('ui_lang', target); } catch (_e) { /* noop */ }
  try {
    const url = new URL(window.location.href);
    url.searchParams.set('oblang', target);
    window.location.replace(url.toString());
  } catch (_e) {
    try { window.location.reload(); } catch (_e2) { /* noop */ }
  }
}

async function api(path, extra) {
  const initData = tg?.initData || '';
  const headers = { 'Content-Type': 'application/json' };
  // Carry the durable app token explicitly (not only via the global fetch shim): outside
  // Telegram it IS the authentication, and the tour must work even when the shim never ran
  // (e.g. the wizard opened directly at /tour on a cold PWA launch).
  if (APP_TOKEN) headers['X-App-Token'] = APP_TOKEN;
  const payload = { initData, ...(extra || {}) };
  if (APP_TOKEN && !payload.aqt) payload.aqt = APP_TOKEN;
  const res = await fetch(path, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

// A guest (no bot yet) still gets the real pickers — we just can't save server-side. Park the
// choice locally and replay it on the first open WITH an account, so «выбери сколько слов» in
// the shared tour isn't a dead end and the answer isn't asked twice.
const PENDING_KEY = 'ob_pending_choices_v1';
function readPendingChoices() {
  try { return JSON.parse(localStorage.getItem(PENDING_KEY) || '{}') || {}; }
  catch (_e) { return {}; }
}
function rememberChoice(key, value) {
  try {
    const cur = readPendingChoices();
    cur[key] = value;
    localStorage.setItem(PENDING_KEY, JSON.stringify(cur));
  } catch (_e) { /* a guest without storage simply re-picks later */ }
}
function clearPendingChoices() {
  try { localStorage.removeItem(PENDING_KEY); } catch (_e) { /* noop */ }
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
  { id: 'groups',     title: t('Учись с близкими в группе 👨‍👩‍👧', 'Lerne mit deinen Liebsten in einer Gruppe 👨‍👩‍👧'), kind: 'opt' },
  { id: 'howto_words',        title: t('Твой словарь пополняется сам 📖', 'Dein Wörterbuch füllt sich von selbst 📖'), kind: 'info' },
  { id: 'howto_interactives', title: t('Игры-тренировки приходят сами 🎮', 'Übungsspiele kommen von selbst 🎮'), kind: 'info' },
  { id: 'howto_translations', title: t('Переводы с разбором ✍️', 'Übersetzungen mit Analyse ✍️'),          kind: 'info' },
  { id: 'skill_map',          title: t('Карта твоих навыков 🗺', 'Karte deiner Skills 🗺'),        kind: 'info' },
  { id: 'howto_tools',        title: t('Ещё инструменты 🧰', 'Weitere Werkzeuge 🧰'),              kind: 'info' },
  { id: 'howto_youtube',      title: t('Смотри YouTube в приложении ▶️', 'Schau YouTube in der App ▶️'), kind: 'info' },
  { id: 'keyboard',   title: t('Меню бота — где нажимать ⌨️', 'Bot-Menü — wo man tippt ⌨️'), kind: 'info' },
  { id: 'shortcut',   title: t('Захват слов со скриншотов (iPhone) 📲', 'Wörter aus Screenshots (iPhone) 📲'), kind: 'opt' },
  { id: 'howto_morning', title: t('Утром твои слова уже ждут 🌅', 'Morgens warten deine Wörter 🌅'), kind: 'info' },
  { id: 'howto_learn',   title: t('А теперь — как их учить 🎓', 'Und jetzt — wie du sie lernst 🎓'), kind: 'info' },
  { id: 'plans',      title: t('Тарифы: Free и Полный доступ 💎', 'Tarife: Free und Voller Zugang 💎'), kind: 'info' },
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

// ПОВТОРНЫЙ вход (онбординг уже пройден) = справочник, а не мастер настройки. Тогда тур
// открывается оглавлением: сверху то, что можно ИЗМЕНИТЬ (с текущими значениями), ниже —
// объяснения «что где находится». Так человек попадает в нужный раздел одним тапом, а не
// листает 19 экранов и не переподтверждает то, что уже настроил.
const TOC_GROUPS = [
  {
    title: t('⚙️ Настройки', '⚙️ Einstellungen'),
    ids: ['language', 'dictionary', 'intensity', 'windows', 'battles', 'groups', 'shortcut'],
  },
  {
    title: t('📖 Как это работает', '📖 Wie es funktioniert'),
    ids: ['install_app', 'howto_words', 'howto_interactives', 'howto_translations', 'skill_map',
      'howto_tools', 'howto_youtube', 'keyboard', 'howto_morning', 'howto_learn', 'plans'],
  },
];

// Текущее значение шага для оглавления — показываем РЕАЛЬНОЕ состояние, а не дефолт.
function tocValue(id, s) {
  const label = (list, code) => (list.find((o) => o[0] === code) || [])[1] || '';
  switch (id) {
    case 'language':
      return '🇩🇪 ↔ 🇷🇺';
    case 'dictionary':
      if (s.dictTier === 'full') return t('✅ весь словарь', '✅ ganzes Wörterbuch');
      if (s.dictTier === 'base') return t('✅ подключён', '✅ verbunden');
      return t('не подключён', 'nicht verbunden');
    case 'intensity':
      return s.isPro ? label(PRESETS, s.preset) : t('🔒 Полный доступ', '🔒 Voller Zugang');
    case 'windows':
      return s.isPro ? label(WINDOWS, s.window) : t('🔒 Полный доступ', '🔒 Voller Zugang');
    case 'battles':
      return s.battleReady ? t('⚔️ да', '⚔️ ja') : t('нет', 'nein');
    default:
      return '';
  }
}

function TableOfContents({ state, onPick }) {
  return (
    <div className="ob-toc">
      <p className="ob-lead">
        {t('Ты это уже настраивал — так что выбери, куда зайти. Ничего подтверждать заново не нужно.',
           'Das hast du schon eingerichtet — wähle einfach, wohin du willst. Du musst nichts erneut bestätigen.')}
      </p>
      {TOC_GROUPS.map((group) => (
        <section className="ob-toc-group" key={group.title}>
          <h3 className="ob-toc-title">{group.title}</h3>
          {group.ids.map((id) => {
            const at = STEPS.findIndex((s) => s.id === id);
            if (at < 0) return null;
            const value = tocValue(id, state);
            return (
              <button type="button" className="ob-toc-row" key={id} onClick={() => onPick(at)}>
                <span className="ob-toc-row-title">{STEPS[at].title}</span>
                {value ? <span className="ob-toc-row-value">{value}</span> : null}
              </button>
            );
          })}
        </section>
      ))}
    </div>
  );
}

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
  // Already outside Telegram (home-screen app) → we can't mint a token (that needs initData),
  // but we HAVE one: reuse it, so the Safari tab opens logged in instead of at the login gate.
  if (!initData && APP_TOKEN) url = `${origin}/webapp?aqt=${encodeURIComponent(APP_TOKEN)}`;
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
         'Kostenlos — eine Auswahl an Aufgaben pro Tag. Mit ')}
      <b>{t('«Полном доступе»', '«Vollem Zugang»')}</b>
      {t(' можно настроить количество и время доставки.',
         ' kannst du Menge und Uhrzeit einstellen.')}
    </p>
    <span className="ob-lock">{t('🔒 Только в «Полном доступе»', '🔒 Nur mit vollem Zugang')}</span>
  </div>
);

// Replaces account-requiring action buttons in the guest tour (viewer has no bot yet).
const GUEST_NOTE = (
  <span className="ob-muted-note">
    {t('📌 Это станет доступно, когда установишь бота. В конце тура нажми «🚀 Установить бота».',
       '📌 Das wird verfügbar, sobald du den Bot installierst. Tippe am Ende auf «🚀 Bot installieren».')}
  </span>
);

// Body per step. Real controls land case-by-case (Stage 1); the rest stay stubs.
const REAL_STEPS = new Set(['install_app', 'language', 'dictionary', 'intensity', 'windows',
  'battles', 'groups', 'shortcut', 'howto_words', 'howto_interactives', 'howto_translations',
  'skill_map', 'howto_tools', 'howto_youtube', 'keyboard', 'howto_morning', 'howto_learn', 'plans']);
function StepBody(props) {
  const { step, isPro, confirmed, busy, dictBusy, dictChoice, stepErr, onConfirm, dictOffer, onDictAction,
    selPreset, selWindow, onPickPreset, onPickWindow, selBattle, onPickBattle,
    onShortcutSetup, shortcutOpening, proPrice, onOpenSubscription, onShareTour, shareHint } = props;
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
            {IS_GUEST_TOUR ? GUEST_NOTE : (
              <button type="button" className="ob-confirm" onClick={openAppInBrowser}>
                {t('🌐 Открыть приложение в Safari', '🌐 App in Safari öffnen')}
              </button>
            )}
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
      return !HAS_ACCOUNT ? (
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
          {HAS_ACCOUNT && (
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
      // «Весь словарь» is now a LIVE subscription (no bulk copy), so `have` stays 0 — detect it
      // from the subscription flag, not from the imported-word count.
      const subscribed = !!(dictOffer?.state?.live_subscription);
      const hasFull = subscribed || (total > 0 && have >= total);
      const partial = have > 0 && !hasFull;          // small starter connected, full still available
      const done = (confirmed && !partial) || hasFull;
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Чтобы не начинать с пустого приложения, подключим готовый набор слов для тренировок и повторений. Дальше ты добавляешь свои слова — а этот стартовый набор можно отключить когда угодно. Выбери размер:',
               'Damit du nicht mit einer leeren App startest, verbinden wir ein fertiges Wörter-Set für Training und Wiederholung. Später fügst du deine eigenen Wörter hinzu — dieses Starter-Set kannst du jederzeit abschalten. Wähle die Größe:')}
          </p>
          {done && !HAS_ACCOUNT ? (
            // Guest tour: nothing was connected yet — the pick is parked until the bot is installed.
            <span className="ob-lock ob-ok">
              {dictChoice === 'decline'
                ? t('⏭ Понятно — базовый словарь не подключаем.', '⏭ Alles klar — kein Basis-Wörterbuch.')
                : IS_GUEST_TOUR
                  ? t('✅ Запомнили твой выбор — подключим сразу, как установишь бота.', '✅ Deine Wahl ist gemerkt — wir verbinden sie, sobald du den Bot installierst.')
                  : t('✅ Запомнили твой выбор — подключим при следующем входе.', '✅ Deine Wahl ist gemerkt — wir verbinden sie beim nächsten Start.')}
            </span>
          ) : done ? (
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
          ) : (
            <div className="ob-actions ob-actions-col">
              <button
                type="button"
                className="ob-confirm"
                onClick={() => onDictAction('accept', false)}
                disabled={busy}
              >
                {dictBusy === 'quick' ? t('Подключаю…', 'Verbinde…') : `${t('📚 Быстрый старт', '📚 Schnellstart')}${n ? ` — ~${n} ${t('слов', 'Wörter')}` : ''}`}
              </button>
              {/* Guest: the offer endpoint needs an account, so there are no counts — still show
                  BOTH sizes (unlabelled) so the choice itself isn't lost. */}
              {total > n || !HAS_ACCOUNT ? (
                <button
                  type="button"
                  className="ob-confirm ob-alt"
                  onClick={() => onDictAction('accept', true)}
                  disabled={busy}
                >
                  {dictBusy === 'full' ? t('Подключаю…', 'Verbinde…') : `${t('🔓 Весь словарь', '🔓 Ganzes Wörterbuch')}${total ? ` — ~${total} ${t('слов', 'Wörter')}` : ''}`}
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
          <p className="ob-muted-note">{t('📚 Быстрый старт — компактный набор базовых слов, чтобы не потеряться в начале. 🔓 Весь словарь — сразу весь набор. Оба — это только СТАРТ. Главное в приложении — слова, которые ты сам добавишь из видео, текстов и переводов: именно их бот будет давать тебе в повторениях. Стартовый набор можно в любой момент отключить в ⚙️ Настройках и учить только свои слова.', '📚 Schnellstart — ein kompaktes Set an Grundwörtern, um am Anfang nicht den Überblick zu verlieren. 🔓 Ganzes Wörterbuch — gleich der volle Satz. Beides ist nur der START. Das Wichtigste sind die Wörter, die du selbst aus Videos, Texten und Übersetzungen hinzufügst — die gibt dir der Bot in den Wiederholungen. Das Starter-Set kannst du jederzeit in ⚙️ Einstellungen abschalten und nur deine eigenen Wörter lernen.')}</p>
          {!HAS_ACCOUNT ? (
            <p className="ob-muted-note">
              {IS_GUEST_TOUR
                ? t('📌 Бота у тебя ещё нет — выбор мы запомним и применим сразу, как установишь его.',
                     '📌 Du hast den Bot noch nicht — wir merken uns die Wahl und wenden sie an, sobald du ihn installierst.')
                : t('📌 Сейчас не получается сохранить на сервере — выбор запомним и применим при следующем входе.',
                     '📌 Gerade lässt es sich nicht am Server speichern — wir merken uns die Wahl und wenden sie beim nächsten Start an.')}
            </p>
          ) : null}
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
    case 'groups':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Можно учиться не в одиночку, а вместе с близкими — семьёй или друзьями. Создаёшь свою группу в Telegram, добавляешь туда бота — и вы занимаетесь вместе и видите успехи друг друга.',
               'Du kannst nicht allein lernen, sondern zusammen mit deinen Liebsten — Familie oder Freunden. Du erstellst deine eigene Telegram-Gruppe, fügst den Bot hinzu — und ihr lernt gemeinsam und seht die Fortschritte der anderen.')}
          </p>
          <ul className="ob-list">
            <li>
              <b>{t('🚀 Как завести', '🚀 So legst du los')}</b>
              {t(' — создай группу в Telegram и добавь в неё нашего бота. Он сам подхватит всех участников — никому ничего подтверждать или нажимать не нужно. Задания у каждого свои и приходят в личный чат с ботом, а итоги со сравнением всех — прямо в группу.',
                 ' — erstelle eine Telegram-Gruppe und füge unseren Bot hinzu. Er erfasst alle Teilnehmer automatisch — niemand muss etwas bestätigen oder antippen. Aufgaben sind für jeden persönlich und kommen in den privaten Chat mit dem Bot, die Ergebnisse mit dem Vergleich aller — direkt in die Gruppe.')}
            </li>
            <li>
              <b>{t('🏅 Итоги дня и недели — прямо в чат', '🏅 Tages- und Wochenergebnisse — direkt in den Chat')}</b>
              {t(' — каждый вечер и в конце недели бот присылает в группу таблицу с медалями и диаграмму сравнения: кто сколько прошёл, чей балл выше. Отличная мотивация подтянуться за своими.',
                 ' — jeden Abend und am Ende der Woche schickt der Bot eine Tabelle mit Medaillen und ein Vergleichsdiagramm in die Gruppe: wer wie viel geschafft hat, wessen Punktzahl höher ist. Eine super Motivation, mit den anderen mitzuhalten.')}
            </li>
            <li>
              <b>{t('📊 Сравнение по группе в приложении', '📊 Gruppenvergleich in der App')}</b>
              {t(' — в приложении можно переключиться на свою группу и посмотреть, кто как занимается и какое у тебя место среди близких — отдельно от общего потока всех учеников.',
                 ' — in der App kannst du auf deine Gruppe umschalten und sehen, wer wie lernt und welchen Platz du unter deinen Liebsten hast — getrennt vom allgemeinen Strom aller Lernenden.')}
            </li>
          </ul>
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
          {IS_GUEST_TOUR ? GUEST_NOTE : (
            <button type="button" className="ob-confirm" onClick={onShortcutSetup} disabled={shortcutOpening}>
              {shortcutOpening ? t('⏳ Открываю…', '⏳ Öffne…') : t('📲 Настроить сейчас', '📲 Jetzt einrichten')}
            </button>
          )}
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
            <li>
              <b>{t('🎮 Участвуй в интерактивах', '🎮 Mach bei den Interaktiven mit')}</b>
              {t(' — играя в игры и проходя задания, ты прямо оттуда одним нажатием сохраняешь понравившееся слово или словосочетание в свой личный словарик, чтобы выучить его позже.',
                 ' — beim Spielen und beim Lösen der Aufgaben speicherst du direkt von dort mit einem Tippen ein Wort oder eine Wortverbindung in dein persönliches Wörterbuch, um es später zu lernen.')}
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
            {IS_GUEST_TOUR ? GUEST_NOTE : (
              <button type="button" className="ob-confirm" onClick={openDictInBrowser}>
                {t('🌐 Открыть словарь в Safari', '🌐 Wörterbuch in Safari öffnen')}
              </button>
            )}
            <MediaTile src="https://pub-6ebcbf6d9aec43d488d3f6a3ba222c14.r2.dev/shortcuts/%D0%98%D0%BA%D0%BE%D0%BD%D0%BA%D0%B0%20%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8C%20%D0%BD%D0%B0%20%D1%80%D0%B0%D0%B1%D0%BE%D1%87%D0%B8%D0%B9%20%D1%81%D1%82%D0%BE%D0%BB/translator_icon.mp4" type="video" caption={t('🎥 Как вынести иконку на экран', '🎥 Wie man das Symbol anlegt')} />
          </div>
          <p className="ob-lead">
            {t('Все интерактивы и вся твоя деятельность в приложении синхронизированы: ',
               'Alle Interaktiven und deine gesamte Aktivität in der App sind synchronisiert: ')}
            <b>{t('одним нажатием ты всегда отправляешь слово или словосочетание в словарь',
                  'mit einem Tippen schickst du jederzeit ein Wort oder eine Wortverbindung ins Wörterbuch')}</b>
            {t(' — чтобы потом выучить его в режиме интервального повторения.',
               ' — um es später im Modus der spaced repetition zu lernen.')}
          </p>
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
          { f: t('Быстрый перевод + разборы из библиотеки', 'Schnellübersetzung + Analysen aus der Bibliothek'), free: INF, pro: INF },
          { f: t('Разбор новых слов (GPT)', 'Analyse neuer Wörter (GPT)'), free: D(3), pro: INF },
          { f: t('Сохранение слов', 'Wörter speichern'), free: D(20), pro: INF },
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
          { f: t('Статьи из интернета (DW, Tagesschau…)', 'Web-Artikel (DW, Tagesschau…)'), free: D(1), pro: INF },
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
            {t('Ты увидел, что умеет бот. Вот что входит в каждый тариф. «Полный доступ» открывает все функции — отдельных лимитов по разделам нет. Озвучка книг оплачивается отдельно.',
               'Du hast gesehen, was der Bot kann. Das steckt in jedem Tarif. «Voller Zugang» schaltet alle Funktionen frei — ohne eigene Limits je Bereich. Buch-Vertonung wird separat bezahlt.')}
          </p>

          <div className="ob-cmp">
            <div className="ob-cmp-head">
              <div className="h-feat">{t('Возможность', 'Funktion')}</div>
              <div className="h-free"><span className="n">Free</span><span className="tag">{t('бесплатно', 'gratis')}</span></div>
              <div className="h-pro"><span className="n">{t('Полный доступ', 'Voller Zugang')}</span><span className="tag">💎</span></div>
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
            <span><b style={{ color: '#1b7a53' }}>∞</b>{t('без отдельного лимита', 'ohne eigenes Limit')}</span>
          </div>

          {/* Честная рамка «полного доступа»: у функций нет своих квот, но общий дневной
              запас есть. Показываем его НЕ счётчиком одинаковых операций («10 разборов» —
              человеку это ничего не говорит), а ДНЁМ ЦЕЛИКОМ: комплексом, в котором он
              узнаёт свой сценарий. Три разных дня — чтобы каждый нашёл близкий себе.
              Единицы человеческие: предложения, а не «наборы»; и разбираем НЕ все
              предложения подряд (никому не нужно 21 разбор), а треть — как в жизни.
              Числа посчитаны от замеренной себестоимости при потолке €0.25 и занижены
              (0.194 / 0.213 / 0.208), чтобы обещание выполнялось с запасом.
              ⚠️ Пересчитать при смене потолка или моделей. */}
          <div className="ob-info is-fairuse">
            <div className="ic">⚡</div>
            <div>
              <h4>{t('Один общий запас на день — вместо лимитов на каждую кнопку',
                     'Ein gemeinsames Tagesbudget — statt Limits pro Funktion')}</h4>
              <p>{t('Все функции открыты. Есть только общий дневной запас на самый тяжёлый ИИ — чтобы один человек не занял мощности всех. Вот дни, которые в него спокойно укладываются:',
                    'Alle Funktionen sind frei. Es gibt nur ein gemeinsames Tagesbudget für die schwersten KI-Aufgaben — damit nicht einer die Kapazität aller belegt. Solche Tage passen locker hinein:')}</p>
              <ul className="ob-fairuse-list">
                <li>{t('21 предложение перевода с проверкой, подробный разбор ошибок в семи из них, русские субтитры к пяти видео и коллокации к словам',
                       '21 Sätze übersetzen und prüfen lassen, davon sieben mit ausführlicher Fehleranalyse, russische Untertitel zu fünf Videos und Kollokationen zu Wörtern')}</li>
                <li>{t('или 9 совершенно новых слов с полным разбором и набор переводов на 7 предложений',
                       'oder 9 völlig neue Wörter mit voller Analyse und ein Übersetzungsset mit 7 Sätzen')}</li>
                <li>{t('или 7 тренировок по твоим слабым местам, набор переводов и пара новых слов',
                       'oder 7 Trainings zu deinen Schwachstellen, ein Übersetzungsset und ein paar neue Wörter')}</li>
              </ul>
              <p className="ob-fairuse-note">
                {t('«Совершенно новое» — это слово, которого ещё нет в нашем общем словаре, а там уже больше 14 000 слов. Знакомые слова, повторение карточек, игры, дуэли, чтение и озвучка «Классики» запас не тратят вообще. Обновляется каждый день в 00:00.',
                   '«Völlig neu» heißt: ein Wort, das noch nicht in unserem gemeinsamen Wörterbuch steht — und dort sind schon über 14 000. Bekannte Wörter, Karten-Wiederholungen, Spiele, Duelle sowie Lesen und Vertonung der «Klassiker» kosten gar nichts vom Budget. Es erneuert sich täglich um 00:00.')}
              </p>
            </div>
          </div>

          <div className="ob-info is-articles">
            <div className="ic">📰</div>
            <div>
              <h4>{t('Статьи из интернета — кликабельные', 'Web-Artikel — antippbar')}</h4>
              <p>{t('Свежие статьи из немецких изданий (DW, Tagesschau…) открываются прямо в Читалке. Тут ',
                    'Frische Artikel deutscher Medien (DW, Tagesschau…) öffnen sich direkt im Reader. Hier ist ')}
                <b>{t('каждое слово и предложение кликабельно', 'jedes Wort und jeder Satz antippbar')}</b>{' '}
                {t('— нажми, чтобы увидеть перевод, и сразу сохрани нужные слова в свой словарь для изучения. ',
                   '— tippe drauf für die Übersetzung und speichere passende Wörter direkt in dein Wörterbuch zum Lernen. ')}
                <b>{t('На Free — 1 статья в день, в «Полном доступе» — без дневного лимита на статьи.', 'Free — 1 Artikel pro Tag, voller Zugang — kein Tageslimit für Artikel.')}</b></p>
            </div>
          </div>

          <div className="ob-info is-trial">
            <div className="ic">🎁</div>
            <div>
              <h4>{t('7 дней полного доступа — бесплатно каждому', '7 Tage voller Zugang — für jeden gratis')}</h4>
              <p>{t('Каждый новый пользователь сразу получает полный доступ на 7 дней, чтобы попробовать всё. Без карты, автоматически.',
                    'Jeder neue Nutzer erhält sofort 7 Tage vollen Zugang, um alles auszuprobieren. Ohne Karte, automatisch.')}</p>
            </div>
          </div>

          <div className="ob-info is-audio">
            <div className="ic">🔊</div>
            <div>
              <h4>{t('Озвучка книг — отдельно', 'Buch-Vertonung — separat')}</h4>
              <p>{t('Не входит ни в Free, ни в «Полный доступ». Оплачивается за каждую книгу отдельно звёздами: 3 голоса и 25 / 50 / 100 % книги. ',
                    'Weder in Free noch im vollen Zugang enthalten. Wird pro Buch separat mit Sternen bezahlt: 3 Stimmen und 25 / 50 / 100 % des Buches. ')}
                <b>{t('Классика озвучена бесплатно для всех.', 'Klassiker sind für alle gratis vertont.')}</b>{' '}
                {t('Озвучить книгу можно прямо в Читалке: открываешь книгу → кнопка ▶.',
                   'Ein Buch vertonst du direkt im Reader: Buch öffnen → ▶-Knopf.')}</p>
            </div>
          </div>

          <div className="ob-info is-stars">
            <div className="ic">⭐</div>
            <div>
              <h4>{t('Что такое звёзды Telegram', 'Was sind Telegram-Sterne')}</h4>
              <p>{t('Звёзды — официальная валюта Telegram. Покупаются в пару тапов прямо в приложении, безопасно, без карт на сторонних сайтах. Ими оплачивается «Полный доступ» и озвучка книг.',
                    'Sterne sind die offizielle Telegram-Währung. In wenigen Taps direkt in der App gekauft — sicher, ohne Karte auf fremden Seiten. Damit zahlst du vollen Zugang und Buch-Vertonung.')}</p>
            </div>
          </div>

          <div className="ob-info is-cancel">
            <div className="ic">🔁</div>
            <div>
              <h4>{t('Как отменить «Полный доступ»', 'Vollen Zugang kündigen')}</h4>
              <p>{t('«Полный доступ» продлевается автоматически раз в месяц. Отменить можно в любой момент прямо в Telegram: Настройки → Telegram Stars и подписки → выбери бота → «Отменить». Доступ сохранится до конца оплаченного периода.',
                    'Der volle Zugang verlängert sich automatisch monatlich. Jederzeit kündbar direkt in Telegram: Einstellungen → Telegram Stars und Abos → Bot wählen → „Kündigen“. Der Zugang bleibt bis zum Ende des bezahlten Zeitraums.')}</p>
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
            <p className="ob-plan-title">💎 <b>{t('Полный доступ', 'Voller Zugang')}</b>{proPrice ? <> — <span className="ob-price">{proPrice}</span></> : <> — <span className="ob-price">325 ⭐ / {t('мес', 'Mon.')}</span></>}</p>
            {IS_GUEST_TOUR ? GUEST_NOTE : (
              <button type="button" className="ob-confirm" onClick={onOpenSubscription}>
                {t('✨ Оформить полный доступ', '✨ Vollen Zugang holen')}
              </button>
            )}
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
    case 'skill_map':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Когда ты переводишь предложения, бот под капотом оценивает каждую твою ошибку — и по ним строит ',
               'Wenn du Sätze übersetzt, bewertet der Bot im Hintergrund jeden deiner Fehler — und baut daraus ')}
            <b>{t('карту твоих навыков', 'eine Karte deiner Skills')}</b>
            {t(': где ты уже силён, а где слабые места. Эту карту ты видишь прямо в приложении.',
               ': wo du schon stark bist und wo die Schwächen liegen. Diese Karte siehst du direkt in der App.')}
          </p>
          <MediaTile
            src={MEDIA.skill_map_photo}
            type="image"
            caption={t('🗺 Карта навыков: сильные и слабые темы с процентами', '🗺 Skill-Karte: starke und schwache Themen mit Prozenten')}
          />
          <p className="ob-lead">
            {t('Дальше выбираешь любой слабый навык и прокачиваешь его точечно: короткая ',
               'Dann wählst du eine schwache Fähigkeit und trainierst sie gezielt: kurze ')}
            <b>{t('теория', 'Theorie')}</b>{t(', ', ', ')}
            <b>{t('видео с объяснением', 'ein Erklärvideo')}</b>{t(', ', ', ')}
            <b>{t('ссылка на материал', 'ein Link zum Nachlesen')}</b>
            {t(' — и короткое упражнение, чтобы сразу закрепить.',
               ' — und eine kurze Übung, um es gleich zu festigen.')}
          </p>
          <MediaTile
            src={MEDIA.skill_training}
            type="video"
            caption={t('🎥 Выбираешь слабый навык → теория, видео, материал и короткое упражнение',
                       '🎥 Du wählst eine schwache Fähigkeit → Theorie, Video, Material und eine kurze Übung')}
          />
          <p className="ob-lead ob-muted-note">
            {t('Так ты работаешь не «вообще», а именно над тем, что проседает — и слабые темы постепенно переходят в сильные.',
               'So arbeitest du nicht «allgemein», sondern genau an dem, was hakt — und schwache Themen werden nach und nach zu starken.')}
          </p>
          <span className="ob-lock">{t('🔒 Карта навыков и точечная тренировка — в «Полном доступе»', '🔒 Skill-Karte und gezieltes Training — mit vollem Zugang')}</span>
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
    case 'howto_youtube':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            {t('Смотри видео с YouTube прямо у нас — так удобнее, чем на самом YouTube.',
               'Schau YouTube-Videos direkt bei uns — bequemer als auf YouTube selbst.')}
          </p>
          <ul className="ob-list">
            <li>
              <b>{t('🔎 Любое видео + словарик под рукой', '🔎 Jedes Video + Wörterbuch griffbereit')}</b>
              {t(' — вставь ссылку на любой ролик с YouTube или найди его по названию прямо в приложении. Внизу есть маленький словарик: поставил на паузу, вписал непонятное слово — сразу перевод, и сохраняешь в свой словарь в один тап. Никуда переходить не нужно — в этом и смысл смотреть у нас, а не на YouTube.',
                 ' — füge den Link zu einem beliebigen YouTube-Video ein oder finde es per Titel direkt in der App. Unten gibt es ein kleines Wörterbuch: pausiere, tippe ein unbekanntes Wort ein — sofort die Übersetzung, und du speicherst es mit einem Tipp in dein Wörterbuch. Du musst nirgendwohin wechseln — genau darum lohnt es sich, bei uns statt auf YouTube zu schauen.')}
              {MEDIA.youtube_browser ? <MediaTile src={MEDIA.youtube_browser} type="video" caption={t('🎥 Так же удобно и в браузере', '🎥 Genauso bequem im Browser')} /> : null}
            </li>
            <li>
              <b>{t('🎬 Раздел «Фильмы» — с двойными субтитрами', '🎬 Bereich «Filme» — mit doppelten Untertiteln')}</b>
              {t(' — здесь мы собрали видео, которые идут с субтитрами на двух языках сразу (немецкий и русский). Любое слово в субтитрах переводится прямо по нажатию. Такие двойные субтитры есть только у этой коллекции: субтитры на YouTube — чужая собственность, поэтому произвольные ролики так показывать нельзя, только наши проверенные.',
                 ' — hier haben wir Videos gesammelt, die Untertitel in zwei Sprachen gleichzeitig haben (Deutsch und Russisch). Jedes Wort in den Untertiteln wird direkt per Tipp übersetzt. Solche doppelten Untertitel gibt es nur für diese Sammlung: Untertitel auf YouTube sind fremdes Eigentum, deshalb können beliebige Videos so nicht gezeigt werden — nur unsere geprüften.')}
              {MEDIA.youtube_dual_subs ? <MediaTile src={MEDIA.youtube_dual_subs} type="video" caption={t('🎥 «Фильмы» с двойными субтитрами', '🎥 «Filme» mit doppelten Untertiteln')} /> : null}
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
              {t(' — одна кнопка открывает страницу настроек: автосейв, готовность к батлам и расписание (Полный доступ). Всё в одном месте, меняется в любой момент.',
                 ' — eine Taste öffnet die Einstellungsseite: Auto-Speichern, Duell-Bereitschaft und Zeitplan (voller Zugang). Alles an einem Ort, jederzeit änderbar.')}
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
              <b>{t('🗓 Расписание (Полный доступ)', '🗓 Zeitplan (voller Zugang)')}</b>
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
        <p className="ob-lead">Здесь будет выбор (Полный доступ). Пока — заглушка каркаса.</p>
      ) : (
        <div className="ob-teaser">
          <p className="ob-lead">
            На бесплатном — подборка заданий в день. В <b>Полном доступе</b> можно настроить количество
            и время. Пока — заглушка каркаса.
          </p>
          <span className="ob-lock">🔒 Только в «Полном доступе»</span>
        </div>
      );
    case 'opt':
      return <p className="ob-lead">Необязательный шаг. Здесь будет выбор. Пока — заглушка каркаса.</p>;
    case 'info':
      return <p className="ob-lead">Короткое объяснение + медиа. Пока — заглушка каркаса.</p>;
    case 'finale': {
      const finaleText = TOUR_IN_PLACE ? (
        <p className="ob-lead">
          {t('Вот и весь обзор 🎉 Это был краткий тур по возможностям бота. Закрой его — и продолжай пользоваться приложением.',
             'Das war der Überblick 🎉 Ein kurzer Rundgang durch die Möglichkeiten des Bots. Schließe ihn — und nutze die App weiter.')}
        </p>
      ) : !HAS_ACCOUNT ? (
        <p className="ob-lead">
          {t('Вот и всё, что умеет бот 🎉 Понравилось? Установи его и начни учить немецкий по-настоящему — каждый день. Жми кнопку ниже 👇',
             'Das war alles, was der Bot kann 🎉 Gefällt es dir? Installiere ihn und lerne Deutsch richtig — jeden Tag. Tippe auf die Taste unten 👇')}
        </p>
      ) : (
        <p className="ob-lead">
          {t('Всё настроено! Задания уже ждут в чате. Захочешь что-то поменять — всё здесь же, под кнопкой ',
             'Alles eingerichtet! Die Aufgaben warten schon im Chat. Willst du etwas ändern — alles ist hier, unter der Taste ')}
          <b>{t('🎬 Как пользоваться', '🎬 Wie man es benutzt')}</b>.
          {' '}
          {t('Можно просто закрыть тур и остаться в переписке — или сразу открыть приложение и начать заниматься.',
             'Du kannst den Rundgang einfach schließen und im Chat bleiben — oder gleich die App öffnen und loslegen.')}
        </p>
      );
      return (
        <>
          {finaleText}
          {CAN_SHARE ? (
            <div className="ob-share">
              <p className="ob-lead">
                {t('Понравилось, что умеет бот? Поделись этим туром с другом 👇',
                   'Gefällt dir, was der Bot kann? Teile diesen Rundgang mit einem Freund 👇')}
              </p>
              <button type="button" className="ob-confirm ob-alt" onClick={onShareTour}>
                {t('📤 Поделиться с другом', '📤 Mit einem Freund teilen')}
              </button>
              {shareHint ? (
                <span className="ob-lock ob-ok">{shareHint}</span>
              ) : (
                <span className="ob-muted-note">
                  {t('Друг пройдёт короткий тур, а когда начнёт заниматься — вы оба получите бонусные дни полного доступа.',
                     'Dein Freund macht den kurzen Rundgang, und sobald er startet, bekommt ihr beide Bonustage für vollen Zugang.')}
                </span>
              )}
            </div>
          ) : null}
        </>
      );
    }
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
  const [shareHint, setShareHint] = useState('');                // «Поделиться» feedback (copied)
  const [proPrice, setProPrice] = useState('');           // live Pro price label (Stripe-configured)
  const [review, setReview] = useState(false);            // онбординг уже пройден → режим справочника
  const [showToc, setShowToc] = useState(false);          // оглавление (только в режиме справочника)
  const [dictTier, setDictTier] = useState('');           // none|base|full — реальное состояние словаря
  const [settingsReady, setSettingsReady] = useState(false); // текущие настройки подгружены
  // Что уже лежит на сервере: тап по ТОМУ ЖЕ значению не должен слать запрос.
  const savedRef = useRef({ preset: '', window: '', battleReady: null });
  const [langHint, setLangHint] = useState(false);        // one-time «switch to German» toast + globe glow

  // On EVERY fresh RU open, point out the 🌐 globe: the wizard is in Russian, but a learner
  // who wants the German UI can tap here. Glow the globe and float a gentle 6s toast. Not
  // one-time — the user asked to see it each time. Skipped only right after a manual switch
  // (?oblang= present) — they just used the toggle, no need to re-explain it.
  useEffect(() => {
    if (LANG !== 'ru' || OBLANG_PARAM) return;
    const show = setTimeout(() => setLangHint(true), 900); // let the wizard settle in first
    return () => clearTimeout(show);
  }, []);

  useEffect(() => {
    if (!langHint) return;
    const hide = setTimeout(() => setLangHint(false), 6000);
    return () => clearTimeout(hide);
  }, [langHint]);

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

  // Load state (resume point + tier). A failed status used to silently mean isPro=false, so a
  // paying Pro user saw «🔒 Только в Pro» on the schedule steps — retry once before believing it.
  useEffect(() => {
    let off = false;
    (async () => {
      let d = null;
      for (let attempt = 0; attempt < 2 && !d; attempt += 1) {
        try {
          d = await api('/api/webapp/onboarding/status');
        } catch (_e) {
          if (attempt === 0) await new Promise((r) => setTimeout(r, 800));
        }
      }
      if (off) { return; }
      if (d) {
        setIsPro(!!d.is_pro);
        const resume = d.completed ? 0 : Math.max(0, Math.min(Number(d.current_step) || 0, STEPS.length - 1));
        setIdx(resume);
        // Уже проходил → открываем оглавлением и снимаем обязательные подтверждения.
        if (d.completed) { setReview(true); setShowToc(true); }
      }
      /* else: unreachable → start from the top */
      setLoading(false);
    })();
    return () => { off = true; };
  }, []);

  // ТЕКУЩИЕ настройки пользователя. Без этого тур показывал зашитые дефолты («Обычно», «Весь
  // день») как будто это твой выбор — то есть врал, и изменить одну настройку было нельзя:
  // можно было только переназначить всё заново.
  useEffect(() => {
    if (!HAS_ACCOUNT) { setSettingsReady(true); return; }
    let off = false;
    (async () => {
      try {
        const d = await api('/api/webapp/settings');
        if (off) return;
        const preset = String(d.preset || 'normal');
        const win = String(d.window || 'allday');
        const battleReady = !!d.battle_ready;
        setSelPreset(preset);
        setSelWindow(win);
        setSelBattle(battleReady ? 'yes' : 'no');
        setDictTier(String(d.dict_tier || 'none'));
        setIsPro(!!d.is_pro);
        savedRef.current = { preset, window: win, battleReady };
      } catch (_e) {
        /* остаёмся на визуальных дефолтах — тур всё равно проходим */
      } finally {
        if (!off) setSettingsReady(true);
      }
    })();
    return () => { off = true; };
  }, []);

  // Replay what was picked in the guest tour (before the bot existed): the first open WITH an
  // account applies it for real, then the parked choice is dropped.
  useEffect(() => {
    if (loading || !HAS_ACCOUNT) return;
    const pending = readPendingChoices();
    if (!pending || !Object.keys(pending).length) return;
    clearPendingChoices();
    (async () => {
      if (pending.dict === 'quick' || pending.dict === 'full') {
        try { await api('/api/webapp/starter-dictionary/apply', { action: 'accept', full: pending.dict === 'full' }); }
        catch (_e) { /* the step still offers the buttons */ }
      }
      if (typeof pending.battles === 'boolean') {
        api('/api/webapp/onboarding/battles', { opt_in: pending.battles }).catch(() => {});
      }
    })();
  }, [loading]);

  // Persist the resume point when the step changes (fire-and-forget). В режиме справочника
  // это не прогресс, а листание — сохранять точку возобновления нечего (и незачем грузить
  // сервер записью на каждый экран).
  useEffect(() => {
    if (loading) return;
    setStepErr('');
    setBusy(false);
    if (review) return;
    api('/api/webapp/onboarding/step', { step: idx }).catch(() => {});
  }, [idx, loading, review]);

  const step = STEPS[idx];
  const isLast = idx === STEPS.length - 1;
  // Guest tour: no gating (just click through — nothing to save). With an account (Telegram OR
  // the standalone app): core steps must be confirmed before «Далее».
  // В режиме справочника тоже не гейтим: человек уже всё подтвердил, второй раз не надо.
  const canNext = !HAS_ACCOUNT || review || step.kind !== 'core' || !!confirmed[step.id];

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
    // The body is ONE persistent element across steps, so its scrollTop carries over.
    // Because «Далее» only unlocks after scrolling to the bottom, arriving on the next
    // (shorter) step left it scrolled past the content → the step looked blank until the
    // user went back+forward. Reset to the top so every step starts with content in view.
    const el = bodyRef.current;
    if (el) el.scrollTop = 0;
    const r = requestAnimationFrame(checkScrollBottom);
    const t1 = setTimeout(checkScrollBottom, 350);
    const t2 = setTimeout(checkScrollBottom, 900);
    // Safety net: never leave «Далее» permanently blocked if the check ever misfires.
    const tSafe = setTimeout(() => setContentReady(true), 4000);
    // Async media (YouTube embed, images) grow the page AFTER the timed checks — the early
    // "fits" check fired while the page was still short, wrongly unlocking «Далее». Re-run the
    // check whenever the content box resizes so a now-overflowing step re-locks until the user
    // actually scrolls to the new bottom. (checkScrollBottom is stable — useCallback([]).)
    let ro = null;
    const content = el && el.firstElementChild ? el.firstElementChild : el;
    if (content && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(() => checkScrollBottom());
      ro.observe(content);
    }
    // Belt-and-suspenders: ResizeObserver proved unreliable inside the Telegram WebView —
    // a media step (MediaTile does an async HEAD, then mounts <video>) could grow the page
    // AFTER the timed checks without RO firing, leaving «Далее» wrongly unlocked at the top.
    // A short poll re-evaluates the gate for the first few seconds regardless of RO, so a
    // late-loading video always re-locks the button until the user actually scrolls down.
    // (checkScrollBottom only reads the DOM + setState; unchanged value → React no-op.)
    const poll = setInterval(checkScrollBottom, 250);
    const tPollStop = setTimeout(() => clearInterval(poll), 6000);
    return () => {
      cancelAnimationFrame(r); clearTimeout(t1); clearTimeout(t2); clearTimeout(tSafe);
      clearInterval(poll); clearTimeout(tPollStop); if (ro) ro.disconnect();
    };
  }, [idx, checkScrollBottom]);

  const goNext = useCallback(async () => {
    if (!canNext) return;
    try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
    if (!isLast) { setIdx((i) => Math.min(i + 1, STEPS.length - 1)); return; }
    // Finale. Standalone PWA → viewer already has the bot: just close the tour and
    // return to the app. Public browser tour → send the viewer to install the bot.
    // In Telegram → complete + open the main Mini-App.
    if (TOUR_IN_PLACE) {
      // Открыто поверх приложения (PWA или планшетный in-place тур) → просто закрываем
      // тур и возвращаемся назад. В Telegram заодно отметим онбординг завершённым.
      if (HAS_ACCOUNT) { try { await api('/api/webapp/onboarding/complete'); } catch (_e) { /* noop */ } }
      try {
        if (window.history.length > 1) window.history.back();
        else window.location.href = '/';
      } catch (_e) { try { window.location.href = '/'; } catch (_e2) { /* ignore */ } }
      return;
    }
    if (HAS_ACCOUNT && IS_PUBLIC) {
      // Browser tab of the app (durable token, not an installed icon): the tour is done and
      // saved — go into the app itself, NOT to the «install the bot» CTA meant for guests.
      try { await api('/api/webapp/onboarding/complete'); } catch (_e) { /* noop */ }
      try { window.location.href = '/'; } catch (_e) { /* ignore */ }
      return;
    }
    if (IS_PUBLIC) {
      if (botUrl) {
        // Carry the referrer's code (if this tour was shared with ?ref=) into the bot
        // deeplink so the install is attributed to them.
        const dest = REF_PARAM ? `${botUrl}?start=ref_${encodeURIComponent(REF_PARAM)}` : botUrl;
        try { window.location.href = dest; } catch (_e) { /* ignore */ }
      }
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

  // Финал в отдельном мини-аппе из личка: «Закрыть» = отметить онбординг пройденным и
  // вернуться в переписку с ботом, откуда человек и пришёл (без открытия приложения).
  const finishAndClose = useCallback(async () => {
    try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
    setFinishing(true);
    try { await api('/api/webapp/onboarding/complete'); } catch (_e) { /* всё равно закрываем */ }
    try { tg?.close?.(); } catch (_e) { /* noop */ }
    setFinishing(false);
  }, []);

  const goBack = useCallback(() => {
    setIdx((i) => Math.max(i - 1, 0));
  }, []);

  // ✕ Close — leave the tour at any step. The resume point is already persisted, so
  // reopening later continues where it stopped.
  const onClose = useCallback(() => {
    try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
    // Тур открыли IN-PLACE поверх текущего окна (планшет: ?ob_inplace=1, или standalone
    // PWA) — просто возвращаемся назад в приложение, НЕ закрывая мини-апп.
    let openedInPlace = IS_STANDALONE_PWA;
    try { if (new URL(window.location.href).searchParams.get('ob_inplace') === '1') openedInPlace = true; } catch (_e) { /* noop */ }
    if (openedInPlace) {
      try {
        if (window.history.length > 1) window.history.back();
        else window.location.href = '/';
      } catch (_e) { try { window.location.href = '/'; } catch (_e2) { /* noop */ } }
      return;
    }
    // Inside Telegram: close the Mini-App sheet back to the chat.
    if (!IS_PUBLIC) {
      try { tg?.close?.(); } catch (_e) { /* noop */ }
      return;
    }
    // Plain public browser tour: nothing to return to — go to the app root.
    try { window.location.href = '/'; } catch (_e) { /* noop */ }
  }, []);

  // «📤 Поделиться с другом» on the finale: send the friend the PUBLIC tour link carrying
  // this user's referral code (/tour?ref=<id>). The friend walks the full presentation and
  // installs from its finale via ?start=ref_<id> — captured by the existing referral flow.
  const shareTour = useCallback(async () => {
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
    // Referral code = the acting user's id. In Telegram it's in initDataUnsafe; in the PWA
    // (no initData) ask the backend, which resolves us from the durable app token.
    let ref = '';
    try { ref = String(tg?.initDataUnsafe?.user?.id || '').trim(); } catch (_e) { /* noop */ }
    if (!ref) {
      try { const d = await api('/api/webapp/referral-link'); ref = String(d.ref_code || '').trim(); }
      catch (_e) { /* fall back to a bare tour link */ }
    }
    const origin = window.location.origin;
    const tourUrl = ref ? `${origin}/tour?ref=${encodeURIComponent(ref)}` : `${origin}/tour`;
    const text = t('Смотри, что умеет этот бот для немецкого — пройди короткий тур 👇',
                   'Schau, was dieser Deutsch-Bot kann — mach den kurzen Rundgang 👇');
    const tgShare = `https://t.me/share/url?url=${encodeURIComponent(tourUrl)}&text=${encodeURIComponent(text)}`;
    // Обычная отправка ссылки — общий запасной путь для всех веток ниже.
    const shareLinkOnly = () => {
      try {
        if (!IS_PUBLIC && tg?.openTelegramLink) { tg.openTelegramLink(tgShare); return true; }
      } catch (_e) { /* noop */ }
      try { window.open(tgShare, '_blank'); return true; } catch (_e) { /* noop */ }
      return false;
    };

    // Внутри Telegram сначала пробуем inline-режим: тогда другу уходит КАРТОЧКА
    // с Феликсом и рабочей кнопкой «Пройти тур». t.me/share/url так не умеет —
    // он шлёт только текст и ссылку, inline-кнопку может поставить лишь бот под
    // собственным сообщением.
    //
    // switchInlineQuery умеет отказать ДВУМЯ способами: бросить исключение (старый
    // клиент) и — хуже — молча ничего не сделать, если Mini App открыт не с той
    // кнопки. Во втором случае человек нажал бы «Поделиться» и не увидел ничего.
    // Поэтому: версия ≥ 6.7, try/catch и контрольная проверка — если через 900 мс
    // окно всё ещё перед нами, значит выбор чата не открылся, шлём просто ссылку.
    let inlineTried = false;
    try {
      const versionOk = typeof tg?.isVersionAtLeast === 'function' ? tg.isVersionAtLeast('6.7') : false;
      if (!IS_PUBLIC && versionOk && typeof tg.switchInlineQuery === 'function') {
        tg.switchInlineQuery('', ['users', 'groups']);
        inlineTried = true;
      }
    } catch (_e) { inlineTried = false; }
    if (inlineTried) {
      setTimeout(() => {
        const stillHere = document.visibilityState === 'visible'
          && (typeof document.hasFocus !== 'function' || document.hasFocus());
        if (stillHere) shareLinkOnly();
      }, 900);
      return;
    }
    // Otherwise → native «share to chat» picker, else Web Share sheet, else
    // open Telegram's share URL, else copy the link to the clipboard.
    try {
      if (!IS_PUBLIC && tg?.openTelegramLink) { tg.openTelegramLink(tgShare); return; }
    } catch (_e) { /* noop */ }
    try {
      if (navigator.share) { await navigator.share({ text, url: tourUrl }); return; }
    } catch (_e) { return; /* user cancelled the native sheet */ }
    try { window.open(tgShare, '_blank'); return; } catch (_e) { /* noop */ }
    try {
      await navigator.clipboard.writeText(tourUrl);
      setShareHint(t('✅ Ссылка скопирована', '✅ Link kopiert'));
      setTimeout(() => setShareHint(''), 2200);
    } catch (_e) { /* noop */ }
  }, []);

  // «Настроить сейчас» on the Shortcut step (the last content step): complete
  // onboarding and hand off to the full Shortcut setup screen.
  const openShortcutSetup = useCallback(async () => {
    if (shortcutOpening) return;
    setShortcutOpening(true);
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
    // Complete onboarding in the BACKGROUND — don't block the open on it.
    api('/api/webapp/onboarding/complete').catch(() => {});
    // Standalone PWA (app opened from its home-screen icon): open the Shortcut setup
    // screen IN PLACE via the SPA router (?startapp=shortcut) instead of bouncing the
    // user out to the Telegram client. The shortcut endpoints accept the durable app
    // token (same auth as the rest of the standalone app), so the pairing code works
    // here without initData. The launch hash is preserved by URL.searchParams, so this
    // is a same-window navigation, not a jump to Telegram.
    if (IS_STANDALONE_PWA) {
      try {
        const u = new URL(window.location.href);
        u.searchParams.set('startapp', 'shortcut');
        window.location.assign(u.toString());
      } catch (_e) {
        try { window.location.href = '/?startapp=shortcut'; } catch (_e2) { /* noop */ }
      }
      return;  // navigating away — leave shortcutOpening set
    }
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
        // Use the backend's authoritative amount_stars — the SAME figure the Telegram Stars
        // invoice charges (pro_price_stars(), env PRO_PRICE_EUR_MINOR). Recomputing from
        // amount_value pulled a retired Stripe €4.40 snapshot → showed 286⭐ while checkout
        // actually charges 325⭐. Show verbatim so «what you see» == «what you pay».
        const stars = pro && pro.amount_stars != null ? Number(pro.amount_stars) : null;
        if (!off && stars != null && !Number.isNaN(stars) && stars > 0) {
          const per = pro.recurring_interval === 'year' ? t('год', 'Jahr')
            : pro.recurring_interval === 'week' ? t('неделя', 'Woche')
            : t('месяц', 'Monat');
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

  // Battle readiness defaults ON — но ТОЛЬКО при первом прохождении. Раньше этот эффект
  // срабатывал на каждом заходе и молча возвращал «да, зовите на дуэли» человеку, который
  // однажды выбрал «нет»: открыл тур как шпаргалку — и незаметно снова подписался.
  const battlesDefaultedRef = useRef(false);
  useEffect(() => {
    if (loading || review || !settingsReady || step.id !== 'battles') return;
    if (battlesDefaultedRef.current || savedRef.current.battleReady) return;
    battlesDefaultedRef.current = true;
    setSelBattle('yes');
    if (HAS_ACCOUNT) {
      savedRef.current.battleReady = true;
      api('/api/webapp/onboarding/battles', { opt_in: true }).catch(() => {});
    } else {
      rememberChoice('battles', true);
    }
  }, [step.id, loading, review, settingsReady]);

  // Intensity/window are [R] (optional, default-accept): pick = optimistic + save.
  // Тап по УЖЕ выбранному значению ничего не меняет — значит и запрос слать не за что.
  const pickPreset = useCallback((code) => {
    setSelPreset(code);
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
    if (savedRef.current.preset === code) return;
    savedRef.current.preset = code;
    api('/api/webapp/onboarding/preset', { preset: code }).catch(() => {});
  }, []);
  const pickWindow = useCallback((key) => {
    setSelWindow(key);
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
    if (savedRef.current.window === key) return;
    savedRef.current.window = key;
    api('/api/webapp/onboarding/window', { window: key }).catch(() => {});
  }, []);
  const pickBattle = useCallback((optIn) => {
    setSelBattle(optIn ? 'yes' : 'no');
    try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
    if (savedRef.current.battleReady === !!optIn) return;
    savedRef.current.battleReady = !!optIn;
    if (HAS_ACCOUNT) api('/api/webapp/onboarding/battles', { opt_in: !!optIn }).catch(() => {});
    else rememberChoice('battles', !!optIn);
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
    const choice = action === 'decline' ? 'decline' : (full ? 'full' : 'quick');
    if (!HAS_ACCOUNT) {
      // Guest tour — nothing to save to yet. Park the pick; the first open with an account applies it.
      rememberChoice('dict', choice);
      setConfirmed((c) => ({ ...c, dictionary: true }));
      setDictChoice(choice);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      return;
    }
    setDictBusy(choice);
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
            {CAN_CLOSE ? (
              <button type="button" className="ob-close" onClick={onClose} aria-label={t('Закрыть', 'Schließen')}>✕</button>
            ) : <span />}
            <div className="ob-lang-wrap">
              <button
                type="button"
                className={`ob-lang${langHint ? ' is-hinted' : ''}`}
                onClick={() => { setLangHint(false); switchLang(); }}
              >
                {LANG === 'de' ? '🌐 RU' : '🌐 DE'}
              </button>
              {langHint && (
                <div className="ob-lang-hint" role="status">
                  <span className="ob-lang-hint-arrow" aria-hidden="true" />
                  <span aria-hidden="true">🌐</span>
                  <span>{t('Онбординг на русском. Хочешь на немецком — нажми здесь.', 'Onboarding auf Russisch. Lieber Deutsch? Tippe hier.')}</span>
                  <button
                    type="button"
                    className="ob-lang-hint-x"
                    onClick={() => setLangHint(false)}
                    aria-label={t('Закрыть', 'Schließen')}
                  >×</button>
                </div>
              )}
            </div>
          </div>
          {showToc ? null : (
            <div className="ob-progress">
              <span className="ob-step-label">
                {review
                  ? `${t('Раздел', 'Abschnitt')} ${idx + 1} ${t('из', 'von')} ${STEPS.length}`
                  : `${t('Шаг', 'Schritt')} ${idx + 1} ${t('из', 'von')} ${STEPS.length}`}
              </span>
              <div className="ob-bar"><div className="ob-bar-fill" style={{ width: `${pct}%` }} /></div>
            </div>
          )}
          <h1 className="ob-title">
            {showToc ? t('Как пользоваться 🎬', 'Wie man es benutzt 🎬') : step.title}
          </h1>
        </header>

        <main className="ob-body" ref={bodyRef} onScroll={checkScrollBottom}>
          {showToc ? (
            <TableOfContents
              state={{ preset: selPreset, window: selWindow, battleReady: selBattle === 'yes', dictTier, isPro }}
              onPick={(at) => { setShowToc(false); setIdx(at); }}
            />
          ) : (
          <StepBody
            step={step}
            isPro={isPro}
            /* В справочнике языковая пара уже подтверждена — показываем это, а не просим тапнуть
               ещё раз. Словарь НЕ подставляем: его реальное состояние приходит из dictOffer, и
               если он не подключён, кнопки подключения должны остаться. */
            confirmed={!!confirmed[step.id] || (review && step.id === 'language')}
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
            onShareTour={shareTour}
            shareHint={shareHint}
          />
          )}
        </main>

        {showToc ? null : (
        <footer className={`ob-nav${isLast && TG_SHEET_TOUR ? ' ob-nav-finale' : ''}`}>
          <div className="ob-nav-row">
            {review ? (
              // В режиме справочника оглавление — это хаб: возврат туда одним тапом.
              <button type="button" className="ob-btn ob-back" onClick={() => setShowToc(true)} disabled={finishing}>
                ☰ {t('Оглавление', 'Inhalt')}
              </button>
            ) : (
              <button type="button" className="ob-btn ob-back" onClick={goBack} disabled={idx === 0 || finishing}>
                ← {t('Назад', 'Zurück')}
              </button>
            )}
            {isLast && TG_SHEET_TOUR ? (
              // Двойной финал: остаться в переписке ИЛИ сразу перейти в приложение.
              <button
                type="button"
                className="ob-btn ob-finish-close"
                onClick={finishAndClose}
                disabled={finishing || done || !contentReady || !atBottom}
              >
                {t('Закрыть', 'Schließen')}
              </button>
            ) : null}
            <button
              type="button"
              className="ob-btn ob-next"
              onClick={goNext}
              disabled={!canNext || finishing || done || !contentReady || !atBottom}
            >
              {done ? t('✅ Готово', '✅ Fertig')
                : !contentReady ? t('⏳ Загрузка…', '⏳ Lädt…')
                : !atBottom ? t('↓ Прокрути вниз', '↓ Nach unten scrollen')
                : isLast ? (TOUR_IN_PLACE ? t('Закрыть', 'Schließen') : !HAS_ACCOUNT ? t('🚀 Установить бота', '🚀 Bot installieren') : IS_PUBLIC ? t('🎯 Открыть приложение', '🎯 App öffnen') : t('🎯 Закрыть и открыть приложение', '🎯 Schließen und App öffnen'))
                : t('Далее →', 'Weiter →')}
            </button>
          </div>
        </footer>
        )}
      </div>
    </div>
  );
}
