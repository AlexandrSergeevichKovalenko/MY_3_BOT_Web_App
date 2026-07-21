import React, { useCallback, useEffect, useState } from 'react';
import './battles.css';
import { requestTabletFullscreen } from '../utils/tabletFullscreen.js';

// Standalone «⚔️ Battles» hub — create a duel (type · opponents · themes) right in the
// app, plus links to the existing history/my-battles overlays. Create ENQUEUES on the
// backend; the bot drains the queue and runs the SAME invite/broadcast logic — nothing
// about invites, images, nudges or the digest changes.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
function haptic() { try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ } }

async function api(path, body = {}) {
  const initData = tg?.initData || '';
  const res = await fetch(path, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData, ...body }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

const KINDS = [['artikel', '🔵 Артикли'], ['adjektiv', '🟢 Прилагательные'], ['wofrage', '❓ Wo-Fragen']];
const MODES = [['all', 'Всех'], ['ind', 'Выбрать кого']];

function Segmented({ options, value, onPick, disabled }) {
  return (
    <div className={`bt-seg ${disabled ? 'is-disabled' : ''}`}>
      {options.map(([key, label]) => (
        <button key={key} type="button" className={`bt-seg-item ${value === key ? 'is-sel' : ''}`}
          onClick={() => { if (!disabled) { onPick(key); haptic(); } }}>{label}</button>
      ))}
    </div>
  );
}

export default function BattlesHub() {
  const [state, setState] = useState(null);
  const [people, setPeople] = useState([]);
  const [themes, setThemes] = useState([]);
  const [botUrl, setBotUrl] = useState('');
  const [kind, setKind] = useState('artikel');
  const [mode, setMode] = useState('all');
  const [selUsers, setSelUsers] = useState([]);
  const [selThemes, setSelThemes] = useState([]);
  const [themesOpen, setThemesOpen] = useState(false);
  const [sending, setSending] = useState(false);
  const [done, setDone] = useState(false);
  const [toast, setToast] = useState('');

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); requestTabletFullscreen(tg); } catch (_e) { /* noop */ }
    try { tg?.disableVerticalSwipes?.(); } catch (_e) { /* noop */ }
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* noop */ }
  }, []);

  useEffect(() => {
    let off = false;
    (async () => {
      try { const d = await api('/api/webapp/battles/state'); if (!off) setState(d); }
      catch (_e) { if (!off) setState({ is_pro: false }); }
      try { const d = await api('/api/webapp/battles/people'); if (!off) setPeople(d.people || []); } catch (_e) { /* noop */ }
      try { const d = await api('/api/webapp/battles/themes'); if (!off) setThemes(d.themes || []); } catch (_e) { /* noop */ }
      try { const r = await fetch('/api/public/tour-info'); const d = await r.json().catch(() => ({})); if (!off && d.bot_url) setBotUrl(d.bot_url); } catch (_e) { /* noop */ }
    })();
    return () => { off = true; };
  }, []);

  const toggleUser = (id) => setSelUsers((s) => (s.includes(id) ? s.filter((x) => x !== id) : [...s, id]));
  const toggleTheme = (k) => setSelThemes((s) => (s.includes(k) ? s.filter((x) => x !== k) : [...s, k]));

  const openLink = useCallback((param) => {
    const url = botUrl ? `${botUrl}?startapp=${param}` : '';
    try { if (url && tg?.openTelegramLink) tg.openTelegramLink(url); else if (url) window.location.href = url; } catch (_e) { /* noop */ }
  }, [botUrl]);

  const showToast = (m) => { setToast(m); setTimeout(() => setToast(''), 4000); };

  const send = useCallback(async () => {
    if (sending) return;
    if (mode === 'ind' && selUsers.length === 0) { showToast('Выбери хотя бы одного игрока.'); return; }
    setSending(true);
    try {
      const d = await api('/api/webapp/battles/create', {
        kind, mode, users: selUsers, themes: kind === 'artikel' ? selThemes : [],
      });
      if (d.ok) {
        // Clear confirmation, THEN close the Mini-App — so it's obvious the invite
        // was sent (the bot's status card lands in the chat right behind it).
        haptic();
        setDone(true);
        setTimeout(() => { try { tg?.close?.(); } catch (_e) { /* noop */ } }, 1600);
      } else {
        showToast(d.message || 'Не удалось.');
      }
    } catch (_e) { showToast('Не удалось. Попробуй ещё раз.'); }
    finally { setSending(false); }
  }, [sending, mode, selUsers, selThemes, kind]);

  if (!state) return <div className="bt-root"><div className="bt-card"><p className="bt-loading">Загрузка…</p></div></div>;
  if (done) return (
    <div className="bt-root">
      <div className="bt-card bt-done">
        <div className="bt-done-emoji">⚔️</div>
        <h1 className="bt-title">Приглашения отправлены!</h1>
        <p className="bt-sub">Соперники получат вызов в личке. Подтверждение — в чате с ботом.</p>
      </div>
    </div>
  );
  const isPro = !!state.is_pro;

  return (
    <div className="bt-root">
      <div className="bt-card">
        <header className="bt-head">
          <h1 className="bt-title">⚔️ Battles</h1>
          <p className="bt-sub">Короткие дуэли на грамматику с другими учениками.</p>
        </header>

        <div className="bt-create">
          <div className="bt-block-title">🚀 Начать батл</div>
          {!isPro ? <p className="bt-lock">🔓 Создавать батлы может Pro. Участвовать по приглашению — можно всем.</p> : null}

          <div className="bt-sub bt-sub-blue">
            <div className="bt-sub-title">🎯 Тема батла</div>
            <Segmented options={KINDS} value={kind} onPick={setKind} disabled={!isPro} />
          </div>

          <div className="bt-sub bt-sub-green">
            <div className="bt-sub-title">👥 Кого зовём</div>
            <Segmented options={MODES} value={mode} onPick={setMode} disabled={!isPro} />
            {mode === 'ind' ? (
              <div className="bt-people">
                {people.length === 0 ? (
                  <p className="bt-empty">Пока нет игроков в списке приглашаемых. Пусть друзья включат «Готовность к батлам» в ⚙️ Настройках.</p>
                ) : people.map((p) => (
                  <button key={p.user_id} type="button"
                    className={`bt-person ${selUsers.includes(p.user_id) ? 'is-sel' : ''}`}
                    onClick={() => { if (isPro) toggleUser(p.user_id); }} disabled={!isPro}>
                    <span className="bt-person-name">{p.name}</span>
                    <span className="bt-check">{selUsers.includes(p.user_id) ? '✓' : ''}</span>
                  </button>
                ))}
              </div>
            ) : null}
          </div>

          {kind === 'artikel' && themes.length > 0 ? (
            <div className="bt-sub bt-sub-amber">
              <button type="button" className="bt-collapse" onClick={() => setThemesOpen((o) => !o)}>
                <span className="bt-sub-title">🏷 Темы слов {selThemes.length ? `· выбрано ${selThemes.length}` : '(по желанию)'}</span>
                <span className="bt-caret">{themesOpen ? '▾' : '▸'}</span>
              </button>
              {themesOpen ? (
                <>
                  <div className="bt-chips">
                    {themes.map((th) => (
                      <button key={th.theme_key} type="button"
                        className={`bt-chip ${selThemes.includes(th.theme_key) ? 'is-sel' : ''}`}
                        onClick={() => { if (isPro) toggleTheme(th.theme_key); }} disabled={!isPro}>
                        {th.label}
                      </button>
                    ))}
                  </div>
                  <p className="bt-hint">Ничего не выбрал — будет микс по всем темам.</p>
                </>
              ) : null}
            </div>
          ) : null}

          <button type="button" className="bt-send" onClick={send} disabled={!isPro || sending}>
            {sending ? 'Отправляю…' : '🚀 Отправить приглашение'}
          </button>
        </div>

        <button type="button" className="bt-linkrow" onClick={() => openLink('ans_bh_0')}>
          <span>📜 История батлов</span><span className="bt-chev">›</span>
        </button>
        <button type="button" className="bt-linkrow" onClick={() => openLink('ans_asbl_0')}>
          <span>📋 Мои активные батлы</span><span className="bt-chev">›</span>
        </button>

        {toast ? <div className="bt-toast">{toast}</div> : null}
      </div>
    </div>
  );
}
