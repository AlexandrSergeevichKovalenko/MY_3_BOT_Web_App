import React, { useCallback, useEffect, useMemo, useState } from 'react';
import './onboarding.css';

// Standalone Mini-App first-run onboarding. Opened from Telegram via
// startapp=onboarding (and later reused as the «🎬 Как пользоваться» hub).
// Stage 0 = SCAFFOLD: real step controls land in Stage 1; here we ship the
// frame — progress bar, ←/→ nav, the core-gate (language + dictionary are
// mandatory), tier-aware placeholders, and the resume/complete wiring.
const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

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
  { id: 'welcome',    title: 'Willkommen! 👋',            kind: 'welcome' },
  { id: 'language',   title: 'Твоя языковая пара 🌍',     kind: 'core' },
  { id: 'dictionary', title: 'Базовый словарь 📚',        kind: 'core' },
  { id: 'intensity',  title: 'Сколько заданий в день 🔥', kind: 'pro' },
  { id: 'windows',    title: 'Когда присылать задания ⏰', kind: 'pro' },
  { id: 'battles',    title: 'Батлы с другими ⚔️',        kind: 'opt' },
  { id: 'shortcut',   title: 'Захват слов (iPhone) 📲',   kind: 'opt' },
  { id: 'howto',      title: 'Как всё работает 🎬',       kind: 'info' },
  { id: 'keyboard',   title: 'Меню-клавиатура ⌨️',        kind: 'info' },
  { id: 'finale',     title: 'Готово! ✅',                kind: 'finale' },
];

// Placeholder body per step — Stage 1 replaces each case with the real control.
function StepBody({ step, isPro, confirmed, onConfirm }) {
  switch (step.kind) {
    case 'welcome':
      return (
        <p className="ob-lead">
          Давай за пару минут настроим бота под тебя — язык, словарь, темп заданий.
          Потом покажу, как всё работает. Всё это потом легко изменить в «🎬 Как пользоваться».
        </p>
      );
    case 'core':
      return (
        <div className="ob-stub">
          <p className="ob-lead">Здесь будет настройка (обязательный шаг). Пока — заглушка каркаса.</p>
          <button
            type="button"
            className={`ob-confirm ${confirmed ? 'is-done' : ''}`}
            onClick={onConfirm}
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
          <span className="ob-lock">🔓 перк Pro</span>
        </div>
      );
    case 'opt':
      return <p className="ob-lead">Необязательный шаг. Здесь будет выбор. Пока — заглушка каркаса.</p>;
    case 'info':
      return <p className="ob-lead">Короткое объяснение + медиа. Пока — заглушка каркаса.</p>;
    case 'finale':
      return (
        <p className="ob-lead">
          Всё настроено! Задания уже ждут в чате. Захочешь что-то поменять — всё здесь же,
          под кнопкой <b>🎬 Как пользоваться</b>.
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

  // Force LIGHT theme (owner: onboarding is always light, in the interactive style).
  useEffect(() => {
    try { tg?.ready?.(); tg?.expand?.(); } catch (_e) { /* ignore */ }
    try { document.documentElement.setAttribute('data-scheme', 'light'); } catch (_e) { /* ignore */ }
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
    api('/api/webapp/onboarding/step', { step: idx }).catch(() => {});
  }, [idx, loading]);

  const step = STEPS[idx];
  const isLast = idx === STEPS.length - 1;
  const canNext = step.kind !== 'core' || !!confirmed[step.id];

  const goNext = useCallback(async () => {
    if (!canNext) return;
    try { tg?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
    if (!isLast) { setIdx((i) => Math.min(i + 1, STEPS.length - 1)); return; }
    // Finale → complete.
    setFinishing(true);
    try {
      await api('/api/webapp/onboarding/complete');
      setDone(true);
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
      setTimeout(() => { try { tg?.close?.(); } catch (_e) { /* ignore */ } }, 1400);
    } catch (_e) {
      setFinishing(false);
    }
  }, [canNext, isLast]);

  const goBack = useCallback(() => {
    setIdx((i) => Math.max(i - 1, 0));
  }, []);

  const confirmStep = useCallback(() => {
    setConfirmed((c) => ({ ...c, [step.id]: true }));
    try { tg?.HapticFeedback?.impactOccurred?.('medium'); } catch (_e) { /* noop */ }
  }, [step.id]);

  const pct = useMemo(() => Math.round(((idx + 1) / STEPS.length) * 100), [idx]);

  if (loading) {
    return <div className="ob-root"><div className="ob-loading">Загрузка…</div></div>;
  }

  return (
    <div className="ob-root">
      <div className="ob-card">
        <header className="ob-head">
          <div className="ob-progress">
            <span className="ob-step-label">Шаг {idx + 1} из {STEPS.length}</span>
            <div className="ob-bar"><div className="ob-bar-fill" style={{ width: `${pct}%` }} /></div>
          </div>
          <h1 className="ob-title">{step.title}</h1>
        </header>

        <main className="ob-body">
          <StepBody
            step={step}
            isPro={isPro}
            confirmed={!!confirmed[step.id]}
            onConfirm={confirmStep}
          />
        </main>

        <footer className="ob-nav">
          <button type="button" className="ob-btn ob-back" onClick={goBack} disabled={idx === 0 || finishing}>
            ← Назад
          </button>
          <button
            type="button"
            className="ob-btn ob-next"
            onClick={goNext}
            disabled={!canNext || finishing || done}
          >
            {done ? '✅ Готово' : isLast ? '🎯 К заданиям' : 'Далее →'}
          </button>
        </footer>
      </div>
    </div>
  );
}
