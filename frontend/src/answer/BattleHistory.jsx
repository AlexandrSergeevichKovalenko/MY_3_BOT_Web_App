import React, { useCallback, useEffect, useState } from 'react';

// Unified battle history: a section per game type (Artikel / Adjektiv / future),
// each battle showing the user's place + score; open ones are flagged "идёт".
const medal = (p) => (p === 1 ? '🥇' : p === 2 ? '🥈' : p === 3 ? '🥉' : p ? '🎖️' : '');

export default function BattleHistory({ api, onClose, onOpenBattle }) {
  const [phase, setPhase] = useState('loading'); // loading|ready|error
  const [sections, setSections] = useState([]);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    try {
      const d = await api('/api/webapp/battles/history', {});
      if (!d.ok) { setError(d.error || 'Недоступно'); setPhase('error'); return; }
      setSections(d.sections || []);
      setPhase('ready');
    } catch (e) { setError(String(e.message || e)); setPhase('error'); }
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
        ⚔️ «Вызвать на батл» (артикли) или ⚔️ «Adjektiv-батл» (окончания).</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>);
  } else {
    body = (<>
      <div className="ans-head"><h1 className="ans-title">📜 История батлов</h1></div>
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
      <p className="ans-sub" style={{ opacity: 0.7 }}>Открытые батлы играются из приглашения или «Мои батлы».</p>
      <button className="ans-btn-ghost" onClick={onClose}>Schließen</button>
    </>);
  }

  return <div className="ans-root"><div className="ans-card">{body}</div></div>;
}
