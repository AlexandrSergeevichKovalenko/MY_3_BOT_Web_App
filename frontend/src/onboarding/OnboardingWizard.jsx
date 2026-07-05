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

// Pro delivery presets + active-hours windows (mirror the bot picker).
const PRESETS = [
  ['intensive', '🔥 Интенсивно', '~20 заданий в день'],
  ['normal',    '🙂 Обычно',     '~12 в день (по умолчанию)'],
  ['rare',      '🌙 Редко',      '~8 в день'],
  ['silent',    '🔕 Тишина',     'не присылать автоматически'],
];
const WINDOWS = [
  ['allday',  '🌗 Весь день',    'в любое время'],
  ['morning', '🌅 Утро',         '06–12'],
  ['evening', '🌆 Вечер',        '17:30–22:30'],
  ['morneve', '🌅🌆 Утро+вечер',  '06–09 · 18–22:30'],
];

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
      На бесплатном — подборка заданий в день. В <b>Pro</b> можно настроить количество
      и время доставки.
    </p>
    <span className="ob-lock">🔓 перк Pro</span>
  </div>
);

// Body per step. Real controls land case-by-case (Stage 1); the rest stay stubs.
const REAL_STEPS = new Set(['language', 'dictionary', 'intensity', 'windows',
  'battles', 'shortcut', 'howto', 'keyboard']);
function StepBody(props) {
  const { step, isPro, confirmed, busy, stepErr, onConfirm, dictOffer, onDictAction,
    selPreset, selWindow, onPickPreset, onPickWindow, selBattle, onPickBattle } = props;
  const bodyKey = REAL_STEPS.has(step.id) ? step.id : step.kind;
  switch (bodyKey) {
    case 'welcome':
      return (
        <p className="ob-lead">
          Давай за пару минут настроим бота под тебя — язык, словарь, темп заданий.
          Потом покажу, как всё работает. Всё это потом легко изменить в «🎬 Как пользоваться».
        </p>
      );
    case 'language':
      // German-only mode → the pair is fixed (Немецкий ← Русский); the step just
      // confirms it and upserts the profile row.
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            Ты учишь <b>немецкий</b>, а объяснять будем на <b>русском</b>. Подтверди — и поехали.
          </p>
          <div className="ob-pair">
            <span className="ob-pair-item"><b>🇩🇪 Немецкий</b><small>учишь</small></span>
            <span className="ob-pair-arrow">↔</span>
            <span className="ob-pair-item"><b>🗣 Русский</b><small>объясняем</small></span>
          </div>
          <button
            type="button"
            className={`ob-confirm ${confirmed ? 'is-done' : ''}`}
            onClick={onConfirm}
            disabled={busy || confirmed}
          >
            {confirmed ? '✅ Подтверждено' : busy ? 'Сохраняю…' : '✓ Подтвердить'}
          </button>
          {stepErr ? <p className="ob-err">{stepErr}</p> : null}
        </div>
      );
    case 'dictionary': {
      const connected = confirmed || (dictOffer && Number(dictOffer.starter_pair_total || 0) > 0);
      const n = Number(dictOffer?.suggested_count || dictOffer?.import_limit || 0);
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            Подключим стартовый набор слов{n ? <> — <b>~{n}</b></> : ''} под твой старт: с них
            начнём тренировки и повторения.
          </p>
          {connected ? (
            <span className="ob-lock ob-ok">✅ Базовый словарь подключён</span>
          ) : (
            <div className="ob-actions">
              <button
                type="button"
                className="ob-confirm"
                onClick={() => onDictAction('accept')}
                disabled={busy}
              >
                {busy ? 'Подключаю…' : '📚 Подключить'}
              </button>
              <button
                type="button"
                className="ob-skip"
                onClick={() => onDictAction('decline')}
                disabled={busy}
              >
                Пропустить
              </button>
            </div>
          )}
          {stepErr ? <p className="ob-err">{stepErr}</p> : null}
        </div>
      );
    }
    case 'intensity':
      return isPro ? (
        <div className="ob-stub">
          <p className="ob-lead">Выбери темп — можно поменять когда угодно.</p>
          <OptionList options={PRESETS} selected={selPreset} onPick={onPickPreset} />
        </div>
      ) : PRO_TEASER;
    case 'windows':
      return isPro ? (
        <div className="ob-stub">
          <p className="ob-lead">В какие часы присылать задания? Они придут равномерно внутри окна.</p>
          <OptionList options={WINDOWS} selected={selWindow} onPick={onPickWindow} />
        </div>
      ) : PRO_TEASER;
    case 'battles':
      return (
        <div className="ob-stub">
          <p className="ob-lead">
            Дуэли на артикли, прилагательные и W-вопросы с другими учениками. Хочешь
            получать приглашения?
          </p>
          <div className="ob-options">
            <button
              type="button"
              className={`ob-option ${selBattle === 'yes' ? 'is-sel' : ''}`}
              onClick={() => onPickBattle(true)}
            >
              <span className="ob-option-label">⚔️ Да, зовите меня</span>
              <span className="ob-option-sub">буду в списке приглашаемых</span>
            </button>
            <button
              type="button"
              className={`ob-option ${selBattle === 'no' ? 'is-sel' : ''}`}
              onClick={() => onPickBattle(false)}
            >
              <span className="ob-option-label">Пока нет</span>
              <span className="ob-option-sub">можно включить позже</span>
            </button>
          </div>
        </div>
      );
    case 'shortcut':
      return (
        <div className="ob-teaser">
          <p className="ob-lead">
            Только на iPhone: сохраняй незнакомые слова из любого приложения. Два
            способа — <b>моментальный</b> (скрин → перевод в чат) или <b>ночная пачка</b>
            (не отрывает от дел). Детальную установку пройдём в разделе «Захват слов».
          </p>
          <span className="ob-lock">📲 только iPhone</span>
        </div>
      );
    case 'howto':
      return (
        <div className="ob-stub">
          <p className="ob-lead">Коротко о главном — подробнее всегда в «🎬 Как пользоваться»:</p>
          <ul className="ob-list">
            <li><b>📋 Задания дня</b> — жми «Следующее задание», решай прямо в приложении.</li>
            <li><b>🗂 Словарь</b> — сохраняй слова, повторяй по интервальной системе.</li>
            <li><b>🧩 Тренажёры и ⚔️ батлы</b> — артикли, прилагательные, числа.</li>
            <li><b>🤖 AI-учитель</b> — спрашивай про грамматику в любой момент.</li>
          </ul>
        </div>
      );
    case 'keyboard':
      return (
        <div className="ob-stub">
          <p className="ob-lead">Внизу — меню-клавиатура. Коротко, что где:</p>
          <ul className="ob-list">
            <li><b>▶️ Следующее задание</b> — твои невыполненные на сегодня.</li>
            <li><b>Тренажёры и батлы</b> — быстрые игры на грамматику.</li>
            <li><b>Быстрый словарь · AI-учитель</b> — перевод и вопросы.</li>
            <li><b>🎬 Как пользоваться</b> — настройки и обучение в любой момент.</li>
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
  const [busy, setBusy] = useState(false);      // per-step async action in flight
  const [stepErr, setStepErr] = useState('');
  const [dictOffer, setDictOffer] = useState(null);  // starter-dictionary offer
  const [selPreset, setSelPreset] = useState('normal');   // intensity (visual default)
  const [selWindow, setSelWindow] = useState('allday');   // active window (visual default)
  const [selBattle, setSelBattle] = useState(null);       // battle readiness: null|'yes'|'no'

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
    setStepErr('');
    setBusy(false);
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
  const dictAction = useCallback(async (action) => {
    setStepErr('');
    setBusy(true);
    try {
      await api('/api/webapp/starter-dictionary/apply', { action });
      setConfirmed((c) => ({ ...c, dictionary: true }));
      try { tg?.HapticFeedback?.notificationOccurred?.('success'); } catch (_e) { /* noop */ }
    } catch (_e) {
      setStepErr('Не удалось. Попробуй ещё раз.');
    } finally {
      setBusy(false);
    }
  }, []);

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
            busy={busy}
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
