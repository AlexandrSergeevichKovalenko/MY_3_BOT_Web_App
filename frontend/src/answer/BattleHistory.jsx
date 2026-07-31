import React, { useCallback, useEffect, useState } from 'react';

// Unified battle history: a section per game type (Artikel / Adjektiv / future),
// each battle showing the user's place + score; open ones are flagged "идёт".
const medal = (p) => (p === 1 ? '🥇' : p === 2 ? '🥈' : p === 3 ? '🥉' : p ? '🎖️' : '');

const SNAP_KEY = 'bh_snapshot_v1';
function readSnapshot() {
  try { const s = JSON.parse(localStorage.getItem(SNAP_KEY) || 'null'); return Array.isArray(s?.sections) ? s.sections : null; }
  catch (_e) { return null; }
}
function writeSnapshot(sections) {
  try { localStorage.setItem(SNAP_KEY, JSON.stringify({ sections, at: Date.now() })); } catch (_e) { /* ignore */ }
}

export default function BattleHistory({ api, onClose, onOpenBattle }) {
  const snap = readSnapshot();
  // Render the last known list INSTANTLY from a local snapshot (closed battles
  // never change), then refresh in the background — so the screen never shows a
  // 10 s spinner on reopen.
  const [phase, setPhase] = useState(snap ? 'ready' : 'loading'); // loading|ready|error
  const [sections, setSections] = useState(snap || []);
  const [refreshing, setRefreshing] = useState(true);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setRefreshing(true);
    try {
      const d = await api('/api/webapp/battles/history', {});
      if (!d.ok) {
        // Keep showing the snapshot if we have one; only hard-fail on a cold load.
        if (!readSnapshot()) { setError(d.error || 'Недоступно'); setPhase('error'); }
        return;
      }
      setSections(d.sections || []);
      writeSnapshot(d.sections || []);
      setPhase('ready');
    } catch (e) {
      if (!readSnapshot()) { setError((console.warn('[game] error', e), 'Не удалось загрузить. Попробуйте позже.')); setPhase('error'); }
    } finally {
      setRefreshing(false);
    }
  }, [api]);
  useEffect(() => { load(); }, [load]);

  let body;
  if (phase === 'loading') {
    body = (<><div className="ans-skel" /><div className="ans-skel sm" /><div className="ans-skel" /></>);
  } else if (phase === 'error') {
    body = (<>
      <div className="ans-head"><span className="ans-eyebrow">📜 История батлов</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  } else if (!sections.length) {
    body = (<>
      <div className="ans-head"><span className="ans-eyebrow">📜 История батлов</span></div>
      <p className="ans-sub">У тебя пока нет батлов. Прими вызов в личке или создай свой:
        ⚔️ «Artikel-батл» (артикли) или ⚔️ «Adjektiv-батл» (окончания).</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else {
    body = (<>
      <div className="ans-head">
        <h1 className="ans-title">📜 История батлов{refreshing ? <span className="bh-refresh"> · обновляю…</span> : null}</h1>
      </div>
      <div className="ans-body bh-list">
      {sections.map((sec) => (
        <div className="bh-section" key={sec.key}>
          <div className="bh-section-head">{sec.label}</div>
          {sec.battles.map((b) => {
            const played = b.your_place != null;
            const playable = !!b.play;
            const titleIcon = playable ? '⚔️' : (played ? medal(b.your_place) : '🏁');
            const sub = playable
              ? (played
                ? `идёт · ты уже сыграл · до 23:59 · от ${b.creator_name || '—'}`
                : `идёт · можно сыграть · до 23:59 · от ${b.creator_name || '—'}`)
              : (played
                ? `закрыт · ты сыграл · место ${b.your_place} из ${b.total} · ${b.your_count} верных`
                : `закрыт без результата · 🏆 ${b.winner || '—'}`);
            const handleOpen = () => {
              if (typeof onOpenBattle !== 'function') return;
              if (playable && !played) {
                onOpenBattle(sec.key, b.battle_id, { mode: 'play', battle: b });
                return;
              }
              onOpenBattle(sec.key, b.battle_id, { mode: 'summary', battle: b });
            };
            return (
              <button
                key={`${sec.key}-${b.battle_id}`}
                type="button"
                className={`bh-row bh-row-btn${playable ? ' open' : (played ? ' ok' : '')} can-open`}
                onClick={handleOpen}
              >
                <div className="bh-row-title">
                  {titleIcon} #{b.battle_id} · {b.label}
                </div>
                <div className="bh-row-sub">{sub}</div>
              </button>
            );
          })}
        </div>
      ))}
      </div>
      <p className="ans-sub" style={{ opacity: 0.7 }}>Открытые батлы играются из приглашения или «Мои батлы».</p>
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  }

  return <div className="ans-root"><div className="ans-card">{body}</div></div>;
}
