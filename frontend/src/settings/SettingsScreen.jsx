import React, { useCallback, useEffect, useState } from 'react';
import './settings.css';
import { requestTabletFullscreen } from '../utils/tabletFullscreen.js';

// Standalone Mini-App settings page — opened from the reply-keyboard «⚙️ Настройки»
// button (startapp=settings). Bot-native prefs that used to be scattered reply
// buttons: autosave, battle-readiness, schedule (Pro), theme-for-tomorrow (admin).
// Optimistic saves: flip locally, persist in the background, revert only on failure.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

function haptic() { try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ } }

async function api(path, body = {}) {
  const initData = tg?.initData || '';
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData, ...body }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

const PRESETS = [
  ['intensive', '🔥 Интенсивно', '~20 в день'],
  ['normal', '🙂 Обычно', '~12 в день'],
  ['rare', '🌙 Редко', '~8 в день'],
  ['silent', '🔕 Тишина', 'не слать'],
];
const WINDOWS = [
  ['allday', 'Весь день', 'любое время'],
  ['morning', 'Утро', '06–12'],
  ['evening', 'Вечер', '17:30–22:30'],
  ['morneve', 'Утро+вечер', '06–09 · 18–22:30'],
];

function Toggle({ on, onChange, disabled }) {
  return (
    <button
      type="button"
      className={`st-toggle ${on ? 'is-on' : ''} ${disabled ? 'is-disabled' : ''}`}
      onClick={() => { if (!disabled) onChange(!on); }}
      aria-pressed={!!on}
      disabled={disabled}
    >
      <span className="st-toggle-knob" />
    </button>
  );
}

function Segmented({ options, value, onPick, disabled }) {
  return (
    <div className={`st-seg ${disabled ? 'is-disabled' : ''}`}>
      {options.map(([key, label, sub]) => (
        <button
          key={key}
          type="button"
          className={`st-seg-item ${value === key ? 'is-sel' : ''}`}
          onClick={() => { if (!disabled) onPick(key); }}
        >
          <span className="st-seg-label">{label}</span>
          {sub ? <span className="st-seg-sub">{sub}</span> : null}
        </button>
      ))}
    </div>
  );
}

export default function SettingsScreen() {
  const [state, setState] = useState(null);
  const [botUrl, setBotUrl] = useState('');
  const [err, setErr] = useState('');
  const [dictBusy, setDictBusy] = useState(false);

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); requestTabletFullscreen(tg); } catch (_e) { /* noop */ }
    // Stop the Mini-App sheet from collapsing on any downward swipe/scroll.
    try { tg?.disableVerticalSwipes?.(); } catch (_e) { /* noop */ }
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* noop */ }
  }, []);

  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const d = await api('/api/webapp/settings');
        if (!off) setState(d);
      } catch (_e) { if (!off) setErr('Не удалось загрузить настройки. Попробуй ещё раз.'); }
    })();
    (async () => {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        if (!off && d.bot_url) setBotUrl(d.bot_url);
      } catch (_e) { /* theme row falls back to closing */ }
    })();
    return () => { off = true; };
  }, []);

  const patch = useCallback((next) => setState((s) => ({ ...(s || {}), ...next })), []);

  const persist = useCallback(async (key, path, value, payloadKey) => {
    const prev = state ? state[key] : undefined;
    patch({ [key]: value });
    try {
      await api(path, { [payloadKey]: value });
      haptic();
    } catch (_e) {
      patch({ [key]: prev });
      setErr('Не удалось сохранить. Попробуй ещё раз.');
      setTimeout(() => setErr(''), 2500);
    }
  }, [state, patch]);

  const openTheme = useCallback(() => {
    const url = botUrl ? `${botUrl}?startapp=ans_alf_0` : '';
    try {
      if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
      else if (url) window.location.href = url;
    } catch (_e) { /* noop */ }
  }, [botUrl]);

  // Base-dictionary tier: 'none' | 'base' (~1000) | 'full'. Mutually exclusive toggles.
  // Reuses the starter-dictionary apply endpoint (decline = remove, accept = (re)import).
  const setDictTier = useCallback(async (tier) => {
    if (dictBusy) return;
    const prev = state ? state.dict_tier : 'none';
    patch({ dict_tier: tier });
    setDictBusy(true);
    try {
      if (tier === 'none') {
        await api('/api/webapp/starter-dictionary/apply', { action: 'decline' });
      } else {
        await api('/api/webapp/starter-dictionary/apply', { action: 'accept', full: tier === 'full', force_reimport: true });
      }
      haptic();
    } catch (_e) {
      patch({ dict_tier: prev });
      setErr('Не удалось. Попробуй ещё раз.');
      setTimeout(() => setErr(''), 2500);
    } finally { setDictBusy(false); }
  }, [dictBusy, state, patch]);

  if (err && !state) {
    return <div className="st-root"><div className="st-card"><p className="st-err">{err}</p></div></div>;
  }
  if (!state) {
    return <div className="st-root"><div className="st-card"><p className="st-loading">Загрузка…</p></div></div>;
  }

  const isPro = !!state.is_pro;
  const isAdmin = !!state.is_admin;

  return (
    <div className="st-root">
      <div className="st-card">
        <header className="st-head">
          <h1 className="st-title">⚙️ Настройки</h1>
          <p className="st-sub">Всё в одном месте. Меняется мгновенно.</p>
        </header>

        {/* Autosave */}
        <section className="st-row">
          <div className="st-row-text">
            <div className="st-row-title">🌙 Ночной автосейв</div>
            <div className="st-row-desc">Слова, пойманные за день, приходят одним вечерним списком, а не по одному.</div>
          </div>
          <Toggle on={!!state.autosave} onChange={(v) => persist('autosave', '/api/webapp/settings/autosave', v, 'enabled')} />
        </section>

        {/* Battle readiness */}
        <section className="st-row">
          <div className="st-row-text">
            <div className="st-row-title">🛡 Готовность к батлам</div>
            <div className="st-row-desc">Зовут ли тебя другие ученики на дуэли по грамматике.</div>
          </div>
          <Toggle on={!!state.battle_ready} onChange={(v) => persist('battle_ready', '/api/webapp/settings/battle-ready', v, 'enabled')} />
        </section>

        {/* Schedule (Pro) */}
        <section className={`st-block ${isPro ? '' : 'is-locked'}`}>
          <div className="st-block-head">
            <div className="st-row-title">🗓 Расписание заданий {isPro ? '' : <span className="st-pro">Pro</span>}</div>
            <div className="st-row-desc">Сколько заданий в день и в какие часы их присылать.</div>
          </div>
          <div className="st-sub-label">Темп</div>
          <Segmented options={PRESETS} value={state.preset} disabled={!isPro}
            onPick={(k) => persist('preset', '/api/webapp/settings/preset', k, 'preset')} />
          <div className="st-sub-label">Часы</div>
          <Segmented options={WINDOWS} value={state.window} disabled={!isPro}
            onPick={(k) => persist('window', '/api/webapp/settings/window', k, 'window')} />
          {!isPro ? <p className="st-lock-note">🔓 Настройка расписания — в Pro. На бесплатном приходит подборка заданий в день.</p> : null}
        </section>

        {/* Base dictionaries — disable to keep only your own words */}
        <section className="st-block">
          <div className="st-block-head">
            <div className="st-row-title">📚 Базовый словарь</div>
            <div className="st-row-desc">Стартовые слова из общего словаря. Выключишь — останутся только твои слова.</div>
          </div>
          <div className="st-dict-row">
            <div className="st-row-title">📚 Быстрый старт (~1000)</div>
            <Toggle on={state.dict_tier === 'base'} onChange={(v) => setDictTier(v ? 'base' : 'none')} disabled={dictBusy} />
          </div>
          <div className="st-dict-row">
            <div className="st-row-title">🔓 Весь словарь (полный)</div>
            <Toggle on={state.dict_tier === 'full'} onChange={(v) => setDictTier(v ? 'full' : 'none')} disabled={dictBusy} />
          </div>
          <p className="st-lock-note">{dictBusy ? 'Применяю… (обновится через пару секунд)' : 'Включён только один. Выключишь оба — базовые слова удалятся, твои сохранятся.'}</p>
        </section>

        {/* Theme for tomorrow — admin only */}
        {isAdmin ? (
          <button type="button" className="st-link-row" onClick={openTheme}>
            <div className="st-row-text">
              <div className="st-row-title">🎯 Тема на завтра</div>
              <div className="st-row-desc">Выбрать тему, из которой ночью соберётся набор der/die/das.</div>
            </div>
            <span className="st-chevron">›</span>
          </button>
        ) : null}

        {err ? <p className="st-err">{err}</p> : null}
      </div>
    </div>
  );
}
