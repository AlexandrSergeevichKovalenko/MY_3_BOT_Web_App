import React, { useEffect, useMemo, useState } from 'react';
import './leaderboard.css';

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

async function api(path, body) {
  const initData = tg?.initData || '';
  const res = await fetch(path, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData, ...body }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || 'Fehler');
  return data;
}

function parseDays(startParam) {
  const m = /^lb(\d*)$/i.exec(String(startParam || '').trim());
  const n = m && m[1] ? Number(m[1]) : 7;
  return Math.max(1, Math.min(365, n || 7));
}

const medal = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i + 1}`);
const initials = (n) => (String(n || '?').trim()[0] || '?').toUpperCase();

function PodiumCol({ entry, rank, you }) {
  if (!entry) return <div className="lb-podium-col" />;
  const cls = rank === 1 ? 'gold' : rank === 2 ? 'silver' : 'bronze';
  const h = rank === 1 ? 130 : rank === 2 ? 92 : 70;
  return (
    <div className={`lb-podium-col ${cls}`}>
      <div className="lb-ava-wrap">
        <div className={`lb-ava ${cls}`}>{initials(entry.name)}</div>
        <span className="lb-ava-badge">{rank}</span>
      </div>
      <div className={`lb-podium-name${entry.user_id === you ? ' me' : ''}`}>{entry.name}</div>
      <div className="lb-bar" style={{ height: h }}>
        <div className="lb-bar-pts">{entry.points}</div>
      </div>
    </div>
  );
}

export default function Leaderboard({ startParam }) {
  const days = useMemo(() => parseDays(startParam), [startParam]);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* ignore */ }
    const scheme = (tg?.colorScheme === 'light') ? 'light' : 'dark';
    try { document.documentElement.setAttribute('data-scheme', scheme); } catch (_e) { /* ignore */ }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const d = await api('/api/leaderboard', { days });
        if (!cancelled) setData(d);
      } catch (e) {
        if (!cancelled) setError(String(e.message || e));
      }
    })();
    return () => { cancelled = true; };
  }, [days]);

  if (error) return <div className="lb-root"><div className="lb-card"><p className="lb-error">{error}</p></div></div>;
  if (!data) return <div className="lb-root"><div className="lb-card"><div className="lb-skel" /><div className="lb-skel sm" /></div></div>;

  const leaders = data.leaders || [];
  const top3 = leaders.slice(0, 3);
  const rest = leaders.slice(3);
  const you = data.you;
  const noms = [
    data.fastest && { icon: '⚡', label: 'Самый быстрый', who: data.fastest.name, val: data.fastest.avg_s != null ? `${data.fastest.avg_s} с` : '' },
    data.accurate && { icon: '🎯', label: 'Самый точный', who: data.accurate.name, val: `${Math.round(data.accurate.correct / data.accurate.answered * 100)}%` },
    data.active && { icon: '🔥', label: 'Самый активный', who: data.active.name, val: `${data.active.answered} зад.` },
  ].filter(Boolean);

  return (
    <div className="lb-root">
      <div className="lb-card">
        <div className="lb-head">
          <div className="lb-trophy">🏆</div>
          <h1 className="lb-title">🌍 Глобальный рейтинг</h1>
          <div className="lb-sub">{days === 7 ? 'неделя' : `${days} дн.`} · все игроки приложения · игроков {data.total_players} · заданий {data.total_tasks}</div>
          {data.min_for_prize ? (
            <div className="lb-sub">🏅 Для призовых мест нужно ответить ≥ {data.min_for_prize} (≥50% заданий)</div>
          ) : null}
        </div>

        <div className="lb-podium">
          <PodiumCol entry={top3[1]} rank={2} you={you} />
          <PodiumCol entry={top3[0]} rank={1} you={you} />
          <PodiumCol entry={top3[2]} rank={3} you={you} />
        </div>

        {rest.length ? (
          <div className="lb-list">
            {rest.map((l, i) => (
              <div className={`lb-row${l.user_id === you ? ' me' : ''}`} key={l.user_id}
                style={{ animationDelay: `${Math.min(i, 12) * 0.04}s` }}>
                <span className="lb-rank">{medal(i + 3)}</span>
                <span className="lb-ava sm">{initials(l.name)}</span>
                <span className="lb-name">{l.name}</span>
                <span className="lb-stat">{l.correct}✓</span>
                <span className="lb-pts">{l.points}</span>
              </div>
            ))}
          </div>
        ) : null}

        {noms.length ? (
          <div className="lb-noms">
            <div className="lb-noms-title">✨ Номинации</div>
            {noms.map((n) => (
              <div className="lb-nom" key={n.label}>
                <span className="lb-nom-ic">{n.icon}</span>
                <span className="lb-nom-label">{n.label}:</span>
                <span className="lb-nom-who">{n.who}</span>
                {n.val ? <span className="lb-nom-val">{n.val}</span> : null}
              </div>
            ))}
          </div>
        ) : null}

        <button className="lb-close" onClick={() => { try { tg?.close?.(); } catch (_e) { /* ignore */ } }}>Schließen</button>
      </div>
    </div>
  );
}
