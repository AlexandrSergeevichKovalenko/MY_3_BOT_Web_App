import React, { useCallback, useEffect, useState } from 'react';
import './interactive.css';

// Standalone «📚 Интерактив» hub — one reply-keyboard button opens this page instead
// of scattering a learn/play button per topic. Each action opens the SAME existing
// Mini-App game via its startapp deeplink (ans_*), so nothing changes functionally.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

const TOPICS = [
  {
    emoji: '🔵', title: 'Артикли der/die/das',
    desc: 'Угадываешь род слова. База, которую надо держать всегда.',
    actions: [['📚 Учить', 'ans_al_0'], ['⚡ Играть', 'ans_as_0']],
  },
  {
    emoji: '🟢', title: 'Окончания прилагательных',
    desc: 'Самая частая ошибка в немецком.',
    actions: [['📚 Учить', 'ans_adl_0'], ['⚡ Играть', 'ans_ad_0']],
  },
  {
    emoji: '❓', title: 'Wo-Fragen',
    desc: 'Вопросы wo / wohin / woher — учишься правильно спрашивать.',
    actions: [['📚 Тренировать', 'ans_wfl_0'], ['⚡ Играть', 'ans_wf_0']],
  },
  {
    emoji: '🔢', title: 'Числа на слух',
    desc: 'Слушаешь немецкую сценку, ловишь номер и впечатываешь его.',
    actions: [['🎧 Открыть тренажёр', 'ans_np_0']],
  },
];

export default function InteractiveHub() {
  const [botUrl, setBotUrl] = useState('');

  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* noop */ }
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* noop */ }
  }, []);

  useEffect(() => {
    let off = false;
    (async () => {
      try {
        const r = await fetch('/api/public/tour-info');
        const d = await r.json().catch(() => ({}));
        if (!off && d.bot_url) setBotUrl(d.bot_url);
      } catch (_e) { /* falls back below */ }
    })();
    return () => { off = true; };
  }, []);

  const open = useCallback((param) => {
    const url = botUrl ? `${botUrl}?startapp=${param}` : '';
    try {
      if (url && tg?.openTelegramLink) tg.openTelegramLink(url);
      else if (url) window.location.href = url;
      try { tg?.HapticFeedback?.selectionChanged?.(); } catch (_e) { /* noop */ }
    } catch (_e) { /* noop */ }
  }, [botUrl]);

  return (
    <div className="ih-root">
      <div className="ih-card">
        <header className="ih-head">
          <h1 className="ih-title">📚 Интерактив</h1>
          <p className="ih-sub">Короткие игры на грамматику и слова. Выбери тему.</p>
        </header>
        {TOPICS.map((topic) => (
          <section className="ih-topic" key={topic.title}>
            <div className="ih-topic-head">
              <span className="ih-emoji">{topic.emoji}</span>
              <div className="ih-topic-text">
                <div className="ih-topic-title">{topic.title}</div>
                <div className="ih-topic-desc">{topic.desc}</div>
              </div>
            </div>
            <div className="ih-actions">
              {topic.actions.map(([label, param]) => (
                <button key={param} type="button" className="ih-btn" onClick={() => open(param)}>{label}</button>
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}
