import React, { useCallback, useEffect, useState } from 'react';
import './plan.css';

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

const ICON = { sent: '✅', planned: '⏳', failed: '🔴' };
const REFRESH_MS = 25000;

export default function PlanTable() {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* ignore */ }
    const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
    try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const d = await api('/api/plan');
      setData(d); setError('');
    } catch (e) { setError(String(e.message || e)); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, REFRESH_MS);
    const onVis = () => { if (!document.hidden) load(); };
    document.addEventListener('visibilitychange', onVis);
    return () => { clearInterval(id); document.removeEventListener('visibilitychange', onVis); };
  }, [load]);

  const t = data?.totals || {};
  const rows = data?.rows || [];
  const dateStr = data?.date ? data.date.split('-').reverse().join('.') : '';

  return (
    <div className="pl-root">
      <div className="pl-head">
        <div className="pl-title">📊 План отправок{dateStr ? ` · ${dateStr}` : ''}</div>
        {data ? (
          <div className="pl-totals">
            <span className="pl-pill ok">✅ {t.sent || 0}</span>
            <span className="pl-pill wait">⏳ {t.planned || 0}</span>
            <span className="pl-pill bad">🔴 {t.failed || 0}</span>
            <span className="pl-pill mut">из {t.total || 0}</span>
          </div>
        ) : null}
        <div className="pl-sub">
          {data ? `Обновлено ${data.updated || ''}` : ''}
          <button className="pl-refresh" onClick={load} disabled={loading}>{loading ? '…' : '🔄'}</button>
        </div>
      </div>

      {error ? <div className="pl-error">{error}</div> : null}
      {!data && !error ? <div className="pl-skel-list">{[...Array(8)].map((_, i) => <div className="pl-skel" key={i} />)}</div> : null}

      <div className="pl-list">
        {rows.map((r, i) => (
          <div className={`pl-row ${r.status}`} key={`${r.minute}-${r.title}-${i}`}>
            <div className="pl-time">{r.time}</div>
            <div className="pl-name">
              <span className="pl-emoji">{r.emoji}</span> {r.title}
              {r.sub ? <span className="pl-tag">{r.sub}</span> : null}
            </div>
            <div className={`pl-status ${r.status}`}>{ICON[r.status] || '⏳'}</div>
          </div>
        ))}
      </div>

      <div className="pl-legend">✅ ушло · ⏳ ждём · 🔴 не ушло · обновляется автоматически</div>
    </div>
  );
}
