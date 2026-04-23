import React, { useCallback, useState } from 'react';
import './HomeDashboardTiles.css';

const TILE_DEFS = [
  { key: 'tr', labelRu: 'Переводы\nпредложений', labelDe: 'Satz-\nUebersetzungen', emoji: '🔤', cls: 'hdt-blue', sectionKey: 'translations', refKey: 'translationsRef' },
  { key: 'ca', labelRu: 'Карточки\nSpace Rep', labelDe: 'Karten\nSpace Rep', emoji: '🗂', cls: 'hdt-violet', sectionKey: 'flashcards', refKey: 'flashcardsRef' },
  { key: 're', labelRu: 'Чтение\nЧиталка', labelDe: 'Lesen\nReader', emoji: '📖', cls: 'hdt-emerald', sectionKey: 'reader', refKey: 'readerRef' },
  { key: 'sp', labelRu: 'Разговорная\nпрактика', labelDe: 'Sprech-\nuebung', emoji: '💬', cls: 'hdt-cyan', sectionKey: 'assistant', refKey: 'assistantRef' },
  { key: 'vi', labelRu: 'Видео\nYouTube', labelDe: 'YouTube\nVideos', emoji: '▶️', cls: 'hdt-red', sectionKey: 'youtube', refKey: 'youtubeRef' },
  { key: 'di', labelRu: 'Словарь\nи поиск', labelDe: 'Wörterbuch\nund Suche', emoji: '📚', cls: 'hdt-indigo', sectionKey: 'dictionary', refKey: 'dictionaryRef' },
  { key: 'an', labelRu: 'Аналитика\nпрогресса', labelDe: 'Analytics\nund Fortschritt', emoji: '📊', cls: 'hdt-green', sectionKey: 'analytics', refKey: 'analyticsRef' },
  { key: 'so', labelRu: 'Техподдержка\nи связь', labelDe: 'Support\nund Kontakt', emoji: '🛟', cls: 'hdt-amber', sectionKey: 'support', refKey: 'supportRef' },
  { key: 'gu', labelRu: 'Как\nпользоваться', labelDe: 'So\nbenutzt du es', emoji: '❓', cls: 'hdt-rose', sectionKey: 'guide', refKey: 'guideRef' },
];

function normalizeStatus(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeTaskType(item) {
  return String(item?.task_type || item?.type || '').trim().toLowerCase();
}

function getTodayBadge(todayPlan) {
  const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
  if (!items.length) return null;
  const done = items.filter((item) => normalizeStatus(item?.status) === 'done').length;
  return { text: `${done}/${items.length}`, done: done >= items.length };
}

function getTranslationBadge(todayPlan) {
  const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
  const translationItem = items.find((item) => normalizeTaskType(item).includes('translation'));
  if (!translationItem) return null;
  const payload = translationItem?.payload && typeof translationItem.payload === 'object' ? translationItem.payload : {};
  const done = Math.max(
    0,
    Number(
      payload.translation_completed_count
      ?? translationItem.completed_count
      ?? 0
    ) || 0,
  );
  const total = Math.max(
    0,
    Number(
      payload.translation_target_count
      ?? payload.sentences
      ?? translationItem.total_count
      ?? 0
    ) || 0,
  );
  if (!total) return null;
  return { text: `${done}/${total}`, done: done >= total };
}

function getCardsBadge(todayPlan, srsQueueInfo) {
  const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
  const cardsItem = items.find((item) => {
    const taskType = normalizeTaskType(item);
    return taskType.includes('card') || taskType.includes('flash');
  });
  if (!cardsItem) return null;
  const payload = cardsItem?.payload && typeof cardsItem.payload === 'object' ? cardsItem.payload : {};
  const limit = Math.max(0, Number(payload.limit || cardsItem.total_count || 0) || 0);
  if (!limit) return null;
  const mode = String(payload.mode || '').trim().toLowerCase();
  let done = Number(cardsItem.completed_count || 0) || 0;
  if (mode === 'fsrs_due') {
    const initialDue = Math.max(0, Number(payload.due_total || limit) || limit);
    const currentDue = Math.max(0, Number(srsQueueInfo?.due_count || 0) || 0);
    done = Math.max(0, Math.min(limit, initialDue - currentDue));
  } else if (mode === 'cards_new') {
    const remainingNew = Math.max(0, Number(srsQueueInfo?.new_remaining_today || 0) || 0);
    done = Math.max(0, Math.min(limit, limit - remainingNew));
  }
  return { text: `${done}/${limit}`, done: done >= limit };
}

function DashTile({ def, label, badge, onClick, showBadge = true }) {
  const [state, setState] = useState('idle');

  const handleDown = useCallback(() => setState('pressed'), []);
  const handleUp = useCallback(() => {
    setState('pop');
    onClick?.();
    window.setTimeout(() => setState('idle'), 380);
  }, [onClick]);
  const handleLeave = useCallback(() => {
    setState((prev) => (prev === 'pressed' ? 'idle' : prev));
  }, []);

  return (
    <button
      type="button"
      className={`hdt-tile ${def.cls} ${state === 'pressed' ? 'hdt-pressed' : ''} ${state === 'pop' ? 'hdt-pop' : ''}`}
      onPointerDown={handleDown}
      onPointerUp={handleUp}
      onPointerLeave={handleLeave}
      onPointerCancel={handleLeave}
      aria-label={label}
    >
      {showBadge && badge && (
        <span className={`hdt-badge ${badge.done ? 'hdt-badge-done' : ''}`}>
          {badge.text}
        </span>
      )}
      <span className="hdt-icon">{def.emoji}</span>
      <span className="hdt-label">{label}</span>
    </button>
  );
}

function TodayStrip({ tr, todayPlan, uiLang, showMetrics = true }) {
  const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
  const doneCount = items.filter((item) => normalizeStatus(item?.status) === 'done').length;
  const totalCount = items.length;
  const now = new Date();
  const dateLabel = now.toLocaleDateString(uiLang === 'de' ? 'de-DE' : 'ru-RU', {
    day: 'numeric',
    month: 'long',
    weekday: 'short',
  });
  const progressPct = totalCount > 0 ? Math.round((doneCount / totalCount) * 100) : 0;

  return (
    <div className="hdt-today-card">
      <div className="hdt-today-inner">
        <div className="hdt-today-row1">
          <span className="hdt-today-label">{tr('Сегодня', 'Heute')}</span>
          <span className="hdt-today-date">📅 {dateLabel}</span>
        </div>
        {showMetrics && (
          <div className="hdt-metrics">
            <div className="hdt-metric">
              <div className="hdt-metric-icon" style={{ background: 'rgba(16,185,129,0.2)' }}>✅</div>
              <div className="hdt-metric-val">{totalCount > 0 ? `${doneCount}/${totalCount}` : '—'}</div>
              <div className="hdt-metric-lbl">{tr('Задачи', 'Aufgaben')}</div>
            </div>
            <div className="hdt-metric">
              <div className="hdt-metric-icon" style={{ background: 'rgba(79,70,229,0.2)' }}>🎯</div>
              <div className="hdt-metric-val">{progressPct}%</div>
              <div className="hdt-metric-lbl">{tr('Прогресс', 'Fortschritt')}</div>
            </div>
            <div className="hdt-metric">
              <div className="hdt-metric-icon" style={{ background: 'rgba(59,130,246,0.2)' }}>📈</div>
              <div className="hdt-metric-val">{totalCount > 0 ? tr('В фокусе', 'Im Fokus') : tr('Пусто', 'Leer')}</div>
              <div className="hdt-metric-lbl">{tr('День', 'Tag')}</div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function HomeDashboardTiles({
  tr,
  uiLang,
  todayPlan,
  srsQueueInfo,
  openSection,
  onOpenMore = null,
  refs = {},
  showBadges = true,
  showMetrics = true,
  showQuickAccess = true,
}) {
  const currentUiLang = uiLang === 'de' ? 'de' : 'ru';
  const badges = {
    tr: getTranslationBadge(todayPlan),
    ca: getCardsBadge(todayPlan, srsQueueInfo),
  };

  const handleTile = useCallback((def) => {
    const ref = refs[def.refKey] || null;
    if (def.sectionKey && openSection) {
      openSection(def.sectionKey, ref);
      return;
    }
    if (ref?.current) {
      ref.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, [openSection, refs]);

  return (
    <div className="hdt-root">
      <TodayStrip tr={tr} todayPlan={todayPlan} uiLang={currentUiLang} showMetrics={showMetrics} />

      <div className="hdt-section-head">
        <span className="hdt-section-label">{tr('Разделы', 'Bereiche')}</span>
        <span className="hdt-section-hint">{tr('Нажмите для перехода', 'Antippen zum Oeffnen')}</span>
      </div>

      <div className="hdt-grid">
        {TILE_DEFS.map((def) => {
          const label = currentUiLang === 'de' ? def.labelDe : def.labelRu;
          return (
            <DashTile
              key={def.key}
              def={def}
              label={label}
              badge={badges[def.key] || null}
              onClick={() => handleTile(def)}
              showBadge={showBadges}
            />
          );
        })}
      </div>

      {showQuickAccess && (
        <button
          type="button"
          className="hdt-quick-bar"
          onClick={() => {
            if (onOpenMore) {
              onOpenMore();
              return;
            }
            openSection?.('dictionary', refs.dictionaryRef);
          }}
        >
          <span className="hdt-quick-card">
            <span className="hdt-quick-icon">⋯</span>
            <span className="hdt-quick-text">
              <span className="hdt-quick-title">{tr('Остальные функции', 'Weitere Funktionen')}</span>
              <span className="hdt-quick-sub">{tr('Словарь · Подписка · Поддержка · Аналитика', 'Woerterbuch · Abo · Support · Analytics')}</span>
            </span>
            <span className="hdt-quick-arrow">›</span>
          </span>
        </button>
      )}
    </div>
  );
}
