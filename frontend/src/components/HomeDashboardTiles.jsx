import React, { useCallback } from 'react';
import './HomeDashboardTiles.css';

const ICONS = {
  tr: '<path d="M4 6.5h8M8 4.7V6.5M11 6.5c0 4-3 7-7 8"/><path d="M5.4 9.7c1.2 1.4 2.8 2.3 4.6 2.7"/><path d="M12.4 19l3.8-8.6 3.8 8.6"/><path d="M13.8 15.9h5.2"/>',
  ca: '<rect x="3.2" y="7.5" width="13" height="11.5" rx="2.6"/><path d="M7.4 7.5V6a2 2 0 012-2h9a2 2 0 012 2v10a2 2 0 01-2 2h-1.6"/>',
  re: '<path d="M12 6.4C10.4 5.3 7.7 4.8 4.7 5.1v12.3c3-.3 5.7.2 7.3 1.3 1.6-1.1 4.3-1.6 7.3-1.3V5.1C16.3 4.8 13.6 5.3 12 6.4z"/><path d="M12 6.4V18.7"/>',
  sp: '<path d="M4.6 5.4h14.8a1.6 1.6 0 011.6 1.6v7a1.6 1.6 0 01-1.6 1.6h-8L7 20v-4H4.6A1.6 1.6 0 013 14V7a1.6 1.6 0 011.6-1.6z"/><circle cx="9" cy="10.6" r="1" fill="currentColor" stroke="none"/><circle cx="12" cy="10.6" r="1" fill="currentColor" stroke="none"/><circle cx="15" cy="10.6" r="1" fill="currentColor" stroke="none"/>',
  vi: '<rect x="3" y="6" width="18" height="12" rx="3.4"/><path d="M10.4 9.2l4.4 2.8-4.4 2.8z" fill="currentColor" stroke="none"/>',
  mv: '<rect x="3" y="9" width="18" height="10" rx="2.2"/><path d="M3.3 9l16.4-2.2.7 2.6M7.4 8.1l.9 2.4M11.8 7.5l.9 2.4M16.2 6.9l.9 2.4"/>',
  di: '<rect x="4.5" y="5" width="14" height="3.9" rx="1.1"/><rect x="4.5" y="10.1" width="14" height="3.9" rx="1.1"/><rect x="4.5" y="15.1" width="14" height="3.9" rx="1.1"/><path d="M7.7 5v3.9M7.7 10.1v3.9M7.7 15.1v3.9" stroke-width="1.1"/>',
  an: '<path d="M4 4v16h16"/><rect x="7" y="12" width="2.7" height="5" rx="0.6" fill="currentColor" stroke="none"/><rect x="11.6" y="9" width="2.7" height="8" rx="0.6" fill="currentColor" stroke="none"/><rect x="16.2" y="6" width="2.7" height="11" rx="0.6" fill="currentColor" stroke="none"/>',
  so: '<circle cx="12" cy="12" r="8.4"/><circle cx="12" cy="12" r="3.5"/><path d="M6.3 6.3l3.2 3.2M14.5 14.5l3.2 3.2M17.7 6.3l-3.2 3.2M9.5 14.5l-3.2 3.2"/>',
  gu: '<circle cx="12" cy="12" r="8.4"/><path d="M9.6 9.4a2.5 2.5 0 114.2 2.1c-1 .8-1.8 1.3-1.8 2.5"/><circle cx="12" cy="16.6" r="0.4" fill="currentColor" stroke="none"/>',
  su: '<rect x="3" y="6" width="18" height="12" rx="2.8"/><path d="M3 10h18"/><rect x="6" y="13.5" width="5.5" height="2.1" rx="1.05" fill="currentColor" stroke="none"/>',
  ec: '<path d="M4 4v16h16"/><path d="M7 15l3.6-3.6 3 3L20 7"/><path d="M16.2 7H20v3.8"/>',
};

function Icon({ name, size = 20 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
      dangerouslySetInnerHTML={{ __html: ICONS[name] || '' }} />
  );
}

// Bento config: key -> sectionKey/refKey/labels/hero. sectionKey + refKey match
// the values App.jsx passes to openSection(sectionKey, refs[refKey]).
const BENTO = [
  { k: 'tr', sectionKey: 'translations', refKey: 'translationsRef', ru: 'Переводы предложений', de: 'Satz-Übersetzungen', hero: true },
  { k: 'di', sectionKey: 'dictionary',   refKey: 'dictionaryRef',   ru: 'Словарь и поиск',     de: 'Wörterbuch & Suche' },
  { k: 'vi', sectionKey: 'youtube',      refKey: 'youtubeRef',      ru: 'Видео',               de: 'Videos' },
  { k: 'mv', sectionKey: 'movies',       refKey: 'moviesRef',       ru: 'Фильмы',              de: 'Filme' },
  { k: 'ca', sectionKey: 'flashcards',   refKey: 'flashcardsRef',   ru: 'Карточки Space Rep',  de: 'Karten Space Rep', hero: true },
  { k: 'an', sectionKey: 'analytics',    refKey: 'analyticsRef',    ru: 'Аналитика прогресса', de: 'Analytics' },
  { k: 'so', sectionKey: 'support',      refKey: 'supportRef',      ru: 'Поддержка',           de: 'Support' },
  { k: 'gu', sectionKey: 'guide',        refKey: 'guideRef',        ru: 'Гид',                 de: 'Guide' },
  { k: 're', sectionKey: 'reader',       refKey: 'readerRef',       ru: 'Чтение',              de: 'Lesen' },
  { k: 'sp', sectionKey: 'assistant',    refKey: 'assistantRef',    ru: 'Разговор',            de: 'Sprechen' },
  { k: 'su', sectionKey: 'subscription', refKey: 'billingRef',      ru: 'Подписка',            de: 'Abo' },
  { k: 'ec', sectionKey: 'economics',    refKey: 'economicsRef',    ru: 'Экономика',           de: 'Kosten' },
];

function pctFromBadge(b) {
  if (!b || !b.text) return 0;
  const m = String(b.text).split('/').map(Number);
  if (m.length === 2 && m[1] > 0) return Math.max(0, Math.min(100, Math.round((m[0] / m[1]) * 100)));
  return 0;
}

function Ring({ pct, size = 44 }) {
  const sw = 4.5, r = (size - sw * 2) / 2, c = 2 * Math.PI * r, f = (c * pct) / 100;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} style={{ transform: 'rotate(-90deg)' }}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="rgba(255,255,255,0.28)" strokeWidth={sw} />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#fff" strokeWidth={sw}
        strokeDasharray={`${f} ${c}`} strokeLinecap="round" />
    </svg>
  );
}

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

function getWeeklyPlanSummary(weeklyPlan) {
  const metrics = weeklyPlan?.metrics && typeof weeklyPlan.metrics === 'object'
    ? weeklyPlan.metrics
    : {};
  const metricKeys = ['translations', 'learned_words', 'agent_minutes', 'reading_minutes'];
  const rows = metricKeys
    .map((key) => metrics[key] || {})
    .map((item) => {
      const goal = Math.max(0, Number(item?.goal || 0) || 0);
      const actual = Math.max(0, Number(item?.actual || 0) || 0);
      const completion = goal > 0
        ? Math.max(0, Math.min(100, Number(item?.completion_percent || 0) || 0))
        : 0;
      return { goal, actual, completion };
    })
    .filter((item) => item.goal > 0);

  if (!rows.length) return null;

  const completedGoals = rows.filter((item) => item.completion >= 100).length;
  const percent = Math.round(rows.reduce((sum, item) => sum + item.completion, 0) / rows.length);

  return {
    percent,
    completedGoals,
    totalGoals: rows.length,
  };
}

function getSkillMasteryPct(skillReport) {
  const groups = Array.isArray(skillReport?.groups) ? skillReport.groups : [];
  const skills = groups.flatMap((g) => (Array.isArray(g?.skills) ? g.skills : []));
  const withData = skills.filter(
    (s) => Boolean(s?.has_data) && s?.mastery !== null && s?.mastery !== undefined,
  );
  if (!withData.length) return null;
  const avg = withData.reduce((sum, s) => sum + Number(s.mastery || 0), 0) / withData.length;
  return Math.round(avg);
}

function TodayStrip({ tr, todayPlan, weeklyPlan, skillReport, uiLang, showMetrics = true, openSection, refs = {} }) {
  const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
  const doneCount = items.filter((item) => normalizeStatus(item?.status) === 'done').length;
  const totalCount = items.length;
  const weeklyPlanSummary = getWeeklyPlanSummary(weeklyPlan);
  const skillMasteryPct = getSkillMasteryPct(skillReport);
  const now = new Date();
  const dateLabel = now.toLocaleDateString(uiLang === 'de' ? 'de-DE' : 'ru-RU', {
    day: 'numeric',
    month: 'long',
    weekday: 'short',
  });

  const handleMetric = useCallback((sectionKey, refKey) => {
    if (openSection && sectionKey) {
      openSection(sectionKey, refs[refKey] || null);
    }
  }, [openSection, refs]);

  return (
    <div className="hdt-today-card">
      <div className="hdt-today-inner">
        <div className="hdt-today-row1">
          <span className="hdt-today-label">{tr('Сегодня', 'Heute')}</span>
          <span className="hdt-today-date">📅 {dateLabel}</span>
        </div>
        {showMetrics && (
          <div className="hdt-metrics">
            <button
              type="button"
              className="hdt-metric hdt-metric-btn"
              onClick={() => handleMetric('home_today', 'todayRef')}
            >
              <div className="hdt-metric-icon" style={{ background: 'rgba(16,185,129,0.2)' }}>✅</div>
              <div className="hdt-metric-val">{totalCount > 0 ? `${doneCount}/${totalCount}` : '—'}</div>
              <div className="hdt-metric-lbl">{tr('Задачи', 'Aufgaben')}</div>
            </button>
            <button
              type="button"
              className="hdt-metric hdt-metric-btn"
              onClick={() => handleMetric('home_skills', 'skillsRef')}
            >
              <div className="hdt-metric-icon" style={{ background: 'rgba(79,70,229,0.2)' }}>🧠</div>
              <div className="hdt-metric-val">{skillMasteryPct !== null ? `${skillMasteryPct}%` : '—'}</div>
              <div className="hdt-metric-lbl">{tr('Карта навыков', 'Skill-Map')}</div>
            </button>
            <button
              type="button"
              className="hdt-metric hdt-metric-btn"
              onClick={() => handleMetric('home_weekly_plan', 'weeklyPlanRef')}
            >
              <div className="hdt-metric-icon" style={{ background: 'rgba(59,130,246,0.2)' }}>📅</div>
              <div className="hdt-metric-val">
                {weeklyPlanSummary ? `${weeklyPlanSummary.percent}%` : '—'}
              </div>
              {weeklyPlanSummary && (
                <div className="hdt-plan-bar" aria-hidden="true">
                  <span className="hdt-plan-bar-fill" style={{ width: `${weeklyPlanSummary.percent}%` }} />
                </div>
              )}
              <div className="hdt-metric-lbl">Wochenplan</div>
            </button>
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
  weeklyPlan,
  skillReport = null,
  srsQueueInfo,
  openSection,
  onOpenMore = null,
  refs = {},
  showBadges = true,
  showMetrics = true,
  canViewEconomics = false,
}) {
  const currentUiLang = uiLang === 'de' ? 'de' : 'ru';
  const badges = {
    tr: getTranslationBadge(todayPlan),
    ca: getCardsBadge(todayPlan, srsQueueInfo),
  };

  return (
    <div className="hdt-root">
      <TodayStrip
        tr={tr}
        todayPlan={todayPlan}
        weeklyPlan={weeklyPlan}
        skillReport={skillReport}
        uiLang={currentUiLang}
        showMetrics={showMetrics}
        openSection={openSection}
        refs={refs}
      />

      <div className="hdt-section-head">
        <span className="hdt-section-label">{tr('Разделы', 'Bereiche')}</span>
        <span className="hdt-section-hint">{tr('Нажмите для перехода', 'Antippen zum Oeffnen')}</span>
      </div>

      <div className="hdt-bento" {...(!canViewEconomics ? { 'data-no-eco': '' } : {})}>
        {BENTO.map((def) => {
          if (def.k === 'ec' && !canViewEconomics) return null;
          const label = currentUiLang === 'de' ? def.de : def.ru;
          const ref = refs[def.refKey] || null;
          const onClick = () => { if (def.sectionKey && openSection) openSection(def.sectionKey, ref); };

          if (def.hero) {
            const badge = badges[def.k];
            const pct = pctFromBadge(badge);
            return (
              <button key={def.k} type="button" onClick={onClick}
                className={`hdt-bt hero hdt-a-${def.k} hdt-c-${def.k}`} aria-label={label}>
                <div className="hdt-bt-top">
                  <div className="hdt-ic"><Icon name={def.k} size={24} /></div>
                  {badge && <div className="hdt-ring"><Ring pct={pct} /><div className="hdt-rp">{badge.text}</div></div>}
                </div>
                {def.k === 'ca' && (
                  <div className="hdt-bars">
                    {[42, 66, 52, 82, 60, 94].map((h, i) => <i key={i} style={{ height: `${h}%` }} />)}
                  </div>
                )}
                <div className="hdt-bl">{label}</div>
              </button>
            );
          }

          const badge = badges[def.k];
          return (
            <button key={def.k} type="button" onClick={onClick}
              className={`hdt-bt hdt-a-${def.k} hdt-c-${def.k}`} aria-label={label}>
              {showBadges && badge && <span className="hdt-badge">{badge.text}</span>}
              <div className="hdt-bt-top">
                <div className="hdt-ic"><Icon name={def.k} size={20} /></div>
              </div>
              <div className="hdt-bl">{label}</div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
