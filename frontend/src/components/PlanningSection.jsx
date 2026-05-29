import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { PlanningContext } from '../providers/PlanningProvider.jsx';

function parseIsoTimestampMs(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatSnapshotDateTime(value, locale = 'ru-RU') {
  const timestampMs = parseIsoTimestampMs(value);
  if (!Number.isFinite(timestampMs)) return '';
  return new Intl.DateTimeFormat(locale, {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(timestampMs));
}

function DefaultPerfProfiler({ children }) {
  return children;
}

export const PLANNING_BOUNDARY_METRICS = {
  provider_name: 'planning',
  rendering_lines_removed: 824,
  remaining_references: [
    'planning controller bridge',
    'dashboard badges',
    'weekly analytics props',
    'today task callbacks',
    'skill practice callbacks',
  ],
};

export function PlanningSection({
  tr,
  uiLang,
  sectionRefs = {},
  visiblePanels = {},
  todayItemLoading,
  todayTimerNowMs,
  saveWeeklyPlan,
  regenerateTodayPlan,
  startTodayTask,
  submitTodayVideoFeedback,
  getTodayItemDisplayElapsedSeconds,
  getTodayItemProgressPercent,
  getTodayTranslationProgress,
  getTodayItemTitle,
  formatCompactTimer,
  skillPracticeLoading,
  startSkillPractice,
  resumeSkillPractice,
  getStoredSkillTrainingSnapshot,
  getLocalizedSkillDisplayName,
  getLocalizedSkillTopicLabel,
  PerfProfilerComponent = DefaultPerfProfiler,
}) {
  const planning = useContext(PlanningContext);
  if (!planning) {
    throw new Error('PlanningSection requires PlanningContext');
  }
  const requiredCallbacks = {
    saveWeeklyPlan,
    regenerateTodayPlan,
    startTodayTask,
    submitTodayVideoFeedback,
    getTodayItemDisplayElapsedSeconds,
    getTodayItemProgressPercent,
    getTodayTranslationProgress,
    getTodayItemTitle,
    formatCompactTimer,
    startSkillPractice,
    resumeSkillPractice,
    getStoredSkillTrainingSnapshot,
    getLocalizedSkillDisplayName,
    getLocalizedSkillTopicLabel,
  };
  for (const [name, value] of Object.entries(requiredCallbacks)) {
    if (typeof value !== 'function') {
      throw new Error(`PlanningSection missing required callback: ${name}`);
    }
  }

  const {
    planAnalyticsPeriod,
    setPlanAnalyticsPeriod,
    planAnalyticsLoading,
    weeklyPlan,
    weeklyPlanCollapsed,
    setWeeklyPlanCollapsed,
    weeklyPlanDraft,
    setWeeklyPlanDraft,
    weeklyPlanSaving,
    weeklyPlanLoading,
    weeklyPlanError,
    weeklyPlanSnapshotTone,
    planAnalyticsMetrics,
    planAnalyticsRange,
    planAnalyticsError,
    weeklyMetricExpanded,
    setWeeklyMetricExpanded,
    todayPlan,
    todayPlanLoadedOnce,
    todayPlanLoading,
    todayPlanError,
    todayPlanSnapshotTone,
    skillReport,
    skillReportLoadedOnce,
    skillReportLoading,
    skillReportError,
    skillReportSnapshotTone,
    loadTodayPlan,
    loadSkillReport,
    loadWeeklyPlan,
    homeSkillTrainingStatusMap,
  } = planning;

  useEffect(() => {
    console.info('planning_section_mount', { provider_name: 'planning' });
    console.info('frontend_provider_boundary_completed', PLANNING_BOUNDARY_METRICS);
    return () => {
      console.info('planning_section_unmount', { provider_name: 'planning' });
    };
  }, []);

  const {
    weeklyPlanRef = null,
    todayRef = null,
    skillsRef = null,
  } = sectionRefs || {};
  const showWeeklyPlanPanel = visiblePanels.weeklyPlan !== false;
  const showTodayPlanPanel = visiblePanels.todayPlan !== false;
  const showSkillReportPanel = visiblePanels.skillReport !== false;

  const ringPalette = ['#ff5d7a', '#ff9d57', '#ffd84d', '#46dca0', '#53c7ff', '#7c9dff'];
  const ringSize = 264;
  const ringCenter = ringSize / 2;
  const ringStartRadius = 118;
  const ringStep = 14;
  const weeklyPlanUsesDeferredAnalytics = planAnalyticsPeriod !== 'week';
  const hasPlanAnalyticsMetrics = Object.keys(planAnalyticsMetrics || {}).length > 0;
  const useLivePlanAnalyticsMetrics = hasPlanAnalyticsMetrics && !planAnalyticsError;
  const weeklyMetrics = weeklyPlanUsesDeferredAnalytics
    ? (useLivePlanAnalyticsMetrics ? planAnalyticsMetrics : {})
    : (useLivePlanAnalyticsMetrics ? planAnalyticsMetrics : (weeklyPlan?.metrics || {}));
  const hasWeeklyPlanSnapshot = Boolean(
    weeklyPlan?.week
    || (weeklyPlan?.metrics && Object.keys(weeklyPlan.metrics).length > 0)
  );
  const hasTodayPlanItems = Array.isArray(todayPlan?.items) && todayPlan.items.length > 0;
  const hasTodayPlanSnapshot = Boolean(
    todayPlan?.date
    || Number(todayPlan?.total_minutes || 0) > 0
    || hasTodayPlanItems
  );
  const showTodayPlanSkeleton = (
    (!todayPlanLoadedOnce && !todayPlanError && !hasTodayPlanSnapshot)
    || (todayPlanLoading && !hasTodayPlanSnapshot)
  );
  const weeklyMetricRows = useMemo(() => ([
    { key: 'translations', title: tr('Переводы', 'Uebersetzungen'), unit: tr('шт', 'Stk'), data: weeklyMetrics.translations || {} },
    { key: 'learned_words', title: tr('Слова SRS', 'Vokabeln SRS'), unit: tr('слов', 'Woerter'), data: weeklyMetrics.learned_words || {} },
    { key: 'agent_minutes', title: tr('Разговор', 'Gespraech'), unit: tr('мин', 'Min'), data: weeklyMetrics.agent_minutes || {} },
    { key: 'reading_minutes', title: tr('Чтение', 'Lesen'), unit: tr('мин', 'Min'), data: weeklyMetrics.reading_minutes || {} },
  ]), [tr, weeklyMetrics]);
  const hasWeeklyMetricRows = weeklyMetricRows.some((item) => Object.keys(item.data || {}).length > 0);
  const canRenderWeeklyMetrics = weeklyPlanUsesDeferredAnalytics
    ? useLivePlanAnalyticsMetrics
    : (useLivePlanAnalyticsMetrics || hasWeeklyMetricRows);
  const showWeeklyPlanSkeleton = !weeklyPlanError && !canRenderWeeklyMetrics;
  const uiLocale = uiLang === 'de' ? 'de-DE' : 'ru-RU';
  const weeklyPlanUpdatedLabel = formatSnapshotDateTime(weeklyPlan?.snapshot_saved_at, uiLocale);
  const todayPlanUpdatedLabel = formatSnapshotDateTime(todayPlan?.snapshot_saved_at, uiLocale);
  const skillReportUpdatedLabel = formatSnapshotDateTime(skillReport?.snapshot_saved_at, uiLocale);
  const weeklyPlanSnapshotLabel = weeklyPlanUpdatedLabel
    ? tr(`Данные на ${weeklyPlanUpdatedLabel}`, `Stand: ${weeklyPlanUpdatedLabel}`)
    : tr('Данные обновятся при первом запросе', 'Daten werden beim ersten Abruf geladen');
  const todayPlanSnapshotLabel = todayPlanUpdatedLabel
    ? tr(`Данные на ${todayPlanUpdatedLabel}`, `Stand: ${todayPlanUpdatedLabel}`)
    : tr('Данные обновятся при первом запросе', 'Daten werden beim ersten Abruf geladen');
  const skillReportSnapshotLabel = skillReportUpdatedLabel
    ? tr(`Данные на ${skillReportUpdatedLabel}`, `Stand: ${skillReportUpdatedLabel}`)
    : tr('Данные обновятся при первом запросе', 'Daten werden beim ersten Abruf geladen');
  const weeklyWeekLabel = (weeklyPlanUsesDeferredAnalytics || useLivePlanAnalyticsMetrics)
    ? (planAnalyticsRange?.start_date && planAnalyticsRange?.end_date ? `${planAnalyticsRange.start_date} - ${planAnalyticsRange.end_date}` : '')
    : (weeklyPlan?.week?.start_date && weeklyPlan?.week?.end_date ? `${weeklyPlan.week.start_date} - ${weeklyPlan.week.end_date}` : '');
  const periodDaysElapsed = Math.max(0, Number(
    (weeklyPlanUsesDeferredAnalytics || useLivePlanAnalyticsMetrics)
      ? (planAnalyticsRange?.days_elapsed ?? 0)
      : (weeklyPlan?.week?.days_elapsed ?? 0)
  ) || 0);
  const periodDaysTotal = Math.max(1, Number(
    (weeklyPlanUsesDeferredAnalytics || useLivePlanAnalyticsMetrics)
      ? (planAnalyticsRange?.days_total ?? 7)
      : (weeklyPlan?.week?.days_total ?? 7)
  ) || 1);
  const expectedProgressPercent = Math.max(0, Math.min(100, (periodDaysElapsed / periodDaysTotal) * 100));
  const weeklyMetricToneClass = (key) => {
    if (key === 'translations') return 'is-translations';
    if (key === 'learned_words') return 'is-words';
    if (key === 'agent_minutes') return 'is-agent';
    if (key === 'reading_minutes') return 'is-reading';
    return '';
  };
  const formatWeeklyValue = (value, digits = 0) => {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '0';
    if (digits <= 0) return String(Math.round(num));
    return num.toFixed(digits);
  };
  const uniqueSkills = useMemo(() => {
    const flat = (Array.isArray(skillReport?.groups) ? skillReport.groups : [])
      .flatMap((group) => (Array.isArray(group?.skills) ? group.skills : []));
    const byId = new Map();
    for (const skill of flat) {
      const id = String(skill?.skill_id || '').trim();
      if (!id || byId.has(id)) continue;
      byId.set(id, skill);
    }
    return Array.from(byId.values());
  }, [skillReport?.groups]);
  const getDisplaySkillName = useCallback(
    (skillLike) => getLocalizedSkillDisplayName(skillLike, uiLang),
    [getLocalizedSkillDisplayName, uiLang],
  );
  const ringSkills = useMemo(() => {
    const skilledWithData = uniqueSkills.filter((item) => Boolean(item?.has_data) && item?.mastery !== null && item?.mastery !== undefined);
    const weakestSkills = [...skilledWithData]
      .sort((a, b) => (Number(a?.mastery || 0) - Number(b?.mastery || 0)) || (Number(b?.errors_7d || 0) - Number(a?.errors_7d || 0)))
      .slice(0, 3)
      .map((item) => ({ ...item, ring_type: 'weak' }));
    const strongestSkills = [...skilledWithData]
      .sort((a, b) => (Number(b?.mastery || 0) - Number(a?.errors_7d || 0)))
      .filter((item) => !weakestSkills.some((weak) => String(weak?.skill_id || '') === String(item?.skill_id || '')))
      .slice(0, 3)
      .map((item) => ({ ...item, ring_type: 'best' }));
    return [...weakestSkills, ...strongestSkills];
  }, [uniqueSkills]);
  const hasSkillReportSnapshot = Boolean(
    (Array.isArray(skillReport?.groups) && skillReport.groups.length > 0)
    || Number(skillReport?.total_skills || 0) > 0
    || ringSkills.length > 0
  );
  const showSkillReportSkeleton = (
    (!skillReportLoadedOnce && !skillReportError && !hasSkillReportSnapshot)
    || (skillReportLoading && !hasSkillReportSnapshot)
  );
  const getSkillTrainingStatus = useCallback((skillId) => {
    const normalized = String(skillId || '').trim();
    if (!normalized) return null;
    const value = homeSkillTrainingStatusMap?.[normalized];
    if (!value || typeof value !== 'object') return null;
    return {
      state: String(value?.state || '').trim().toLowerCase(),
      is_complete: Boolean(value?.is_complete),
      opened_count: Number(value?.opened_count || 0),
      required_count: Number(value?.required_count || 0),
      practice_submitted: Boolean(value?.practice_submitted),
    };
  }, [homeSkillTrainingStatusMap]);

  const [weeklyMetricModalKey, setWeeklyMetricModalKey] = useState(null);
  const PerfWrapper = PerfProfilerComponent || DefaultPerfProfiler;

  return (
    <PerfWrapper id="section.home">
      <>
        {showWeeklyPlanPanel && (
          <section className="weekly-plan-panel" ref={weeklyPlanRef}>
            <div className="weekly-plan-head">
              <div className="home-panel-head-main">
                <div className="home-panel-head-copy">
                  <div className="home-panel-title-row">
                    <h2>{tr('План на неделю', 'Wochenplan')}</h2>
                    <div className="home-panel-head-actions">
                      <button
                        type="button"
                        className="home-panel-action-btn"
                        onClick={() => loadWeeklyPlan({ manual: true, syncFacts: true })}
                        disabled={weeklyPlanLoading || planAnalyticsLoading}
                        title={tr('Актуализировать данные', 'Daten aktualisieren')}
                        aria-label={tr('Актуализировать данные', 'Daten aktualisieren')}
                      >
                        <svg viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" width="14" height="14" aria-hidden="true" className={(weeklyPlanLoading || planAnalyticsLoading) ? 'is-spinning' : ''}>
                          <path d="M16 2v4h-4"/><path d="M2 11a7 7 0 0 0 12.9 2.9L16 6"/><path d="M2 16v-4h4"/><path d="M16 7a7 7 0 0 0-12.9-2.9L2 12"/>
                        </svg>
                        {tr('Обновить', 'Aktualisieren')}
                      </button>
                      <button
                        type="button"
                        className="home-panel-action-btn"
                        onClick={() => setWeeklyPlanCollapsed((prev) => !prev)}
                        title={weeklyPlanCollapsed ? tr('Развернуть', 'Aufklappen') : tr('Свернуть', 'Einklappen')}
                        aria-label={weeklyPlanCollapsed ? tr('Развернуть', 'Aufklappen') : tr('Свернуть', 'Einklappen')}
                        aria-pressed={!weeklyPlanCollapsed}
                      >
                        <svg viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" width="14" height="14" aria-hidden="true" style={{ transform: weeklyPlanCollapsed ? 'rotate(0deg)' : 'rotate(180deg)', transition: 'transform 0.2s ease' }}>
                          <polyline points="4 7 9 12 14 7"/>
                        </svg>
                        {weeklyPlanCollapsed ? tr('Развернуть', 'Aufklappen') : tr('Свернуть', 'Einklappen')}
                      </button>
                    </div>
                  </div>
                  <small className={`home-panel-snapshot-meta is-${weeklyPlanSnapshotTone === 'manual' ? 'fresh' : 'stale'}`}>{weeklyPlanSnapshotLabel}</small>
                </div>
              </div>
              <div className="weekly-plan-head-toolbar">
                <select
                  className="weekly-plan-period-select-inline"
                  value={planAnalyticsPeriod}
                  onChange={(event) => setPlanAnalyticsPeriod(event.target.value)}
                  disabled={planAnalyticsLoading}
                >
                  <option value="week">{tr('Неделя', 'Woche')}</option>
                  <option value="month">{tr('Месяц', 'Monat')}</option>
                  <option value="quarter">{tr('Квартал', 'Quartal')}</option>
                  <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
                  <option value="year">{tr('Год', 'Jahr')}</option>
                </select>
                {weeklyWeekLabel && <span className="weekly-plan-period">{weeklyWeekLabel}</span>}
              </div>
            </div>

            {!weeklyPlanCollapsed && (
              <div className="weekly-plan-form">
                <label className="webapp-field">
                  <span>{tr('Количество переводов', 'Anzahl Uebersetzungen')}</span>
                  <input type="number" min="0" inputMode="numeric" value={weeklyPlanDraft.translations_goal} onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, translations_goal: event.target.value }))} disabled={weeklyPlanSaving} placeholder="0" />
                </label>
                <label className="webapp-field">
                  <span>{tr('Количество выученных слов', 'Anzahl gelernter Woerter')}</span>
                  <input type="number" min="0" inputMode="numeric" value={weeklyPlanDraft.learned_words_goal} onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, learned_words_goal: event.target.value }))} disabled={weeklyPlanSaving} placeholder="0" />
                </label>
                <label className="webapp-field">
                  <span>{tr('Минуты разговора с агентом', 'Gespraechsminuten mit Assistent')}</span>
                  <input type="number" min="0" inputMode="numeric" value={weeklyPlanDraft.agent_minutes_goal} onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, agent_minutes_goal: event.target.value }))} disabled={weeklyPlanSaving} placeholder="0" />
                </label>
                <label className="webapp-field">
                  <span>{tr('Минуты чтения', 'Leseminuten')}</span>
                  <input type="number" min="0" inputMode="numeric" value={weeklyPlanDraft.reading_minutes_goal} onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, reading_minutes_goal: event.target.value }))} disabled={weeklyPlanSaving} placeholder="0" />
                </label>
                <button type="button" className="primary-button weekly-plan-save-btn" onClick={saveWeeklyPlan} disabled={weeklyPlanSaving || weeklyPlanLoading}>
                  {weeklyPlanSaving ? tr('Сохраняем...', 'Speichern...') : tr('Сохранить план', 'Plan speichern')}
                </button>
              </div>
            )}

            {weeklyPlanLoading && !hasWeeklyPlanSnapshot && <div className="webapp-muted">{tr('Считаем недельные показатели...', 'Wochenwerte werden berechnet...')}</div>}
            {weeklyPlanLoading && hasWeeklyPlanSnapshot && <div className="webapp-muted">{tr('Обновляем недельный план в фоне...', 'Wochenplan wird im Hintergrund aktualisiert...')}</div>}
            {weeklyPlanError && <div className="webapp-error">{weeklyPlanError}</div>}
            {weeklyPlanUsesDeferredAnalytics && planAnalyticsLoading && <div className="webapp-muted">{tr('Считаем показатели плана...', 'Planwerte werden berechnet...')}</div>}
            {weeklyPlanUsesDeferredAnalytics && planAnalyticsError && <div className="webapp-error">{planAnalyticsError}</div>}

            {(showWeeklyPlanSkeleton || canRenderWeeklyMetrics) && (
              <div className="weekly-plan-metrics weekly-plan-metrics-grid">
                {weeklyMetricRows.map((item) => {
                  const goal = Number(item.data?.goal || 0);
                  const actual = Number(item.data?.actual || 0);
                  const completion = Number(item.data?.completion_percent || 0);
                  const completionClamped = Math.max(0, Math.min(100, completion));
                  const ringGradient = showWeeklyPlanSkeleton
                    ? 'conic-gradient(rgba(94, 117, 159, 0.35) 0% 100%)'
                    : `conic-gradient(#7bf1b3 0% ${completionClamped}%, rgba(94, 117, 159, 0.35) ${completionClamped}% 100%)`;
                  const completionRingStyle = {
                    background: `radial-gradient(circle at center, rgba(8, 16, 34, 0.96) 56%, transparent 57%), ${ringGradient}`,
                  };
                  return (
                    <article
                      className={`weekly-plan-metric-card ${weeklyMetricToneClass(item.key)}`}
                      key={item.key}
                      onClick={() => !showWeeklyPlanSkeleton && setWeeklyMetricModalKey(item.key)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(e) => { if (!showWeeklyPlanSkeleton && (e.key === 'Enter' || e.key === ' ')) setWeeklyMetricModalKey(item.key); }}
                    >
                      <div className="weekly-plan-metric-compact-top">
                        <h4>{item.title}</h4>
                        <div className="weekly-plan-progress-ring" style={completionRingStyle}>
                          <span>{showWeeklyPlanSkeleton ? '...' : `${Math.round(completionClamped)}%`}</span>
                        </div>
                      </div>
                      <div className="weekly-plan-values-compact">
                        <span>{tr('Факт', 'Ist')}: <strong>{showWeeklyPlanSkeleton ? '...' : `${formatWeeklyValue(actual)} ${item.unit}`}</strong></span>
                        <span>{tr('План', 'Plan')}: <strong>{showWeeklyPlanSkeleton ? '...' : `${formatWeeklyValue(goal)} ${item.unit}`}</strong></span>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
            {weeklyMetricModalKey && (() => {
              const modalItem = weeklyMetricRows.find((r) => r.key === weeklyMetricModalKey);
              if (!modalItem) return null;
              const mGoal = Number(modalItem.data?.goal || 0);
              const mActual = Number(modalItem.data?.actual || 0);
              const mForecast = Number(modalItem.data?.forecast || 0);
              const mForecastDelta = Number(modalItem.data?.forecast_delta_vs_goal || 0);
              return (
                <div className="weekly-metric-modal-overlay" onClick={() => setWeeklyMetricModalKey(null)} role="dialog" aria-modal="true">
                  <div className="weekly-metric-modal" onClick={(e) => e.stopPropagation()}>
                    <button type="button" className="weekly-metric-modal-close" onClick={() => setWeeklyMetricModalKey(null)} aria-label={tr('Закрыть', 'Schliessen')}>x</button>
                    <div className="weekly-metric-modal-head"><h3>{modalItem.title}</h3></div>
                    <div className="weekly-plan-values">
                      <div><span>{tr('План', 'Plan')}</span><strong>{formatWeeklyValue(mGoal)} {modalItem.unit}</strong></div>
                      <div><span>{tr('Факт', 'Ist')}</span><strong>{formatWeeklyValue(mActual)} {modalItem.unit}</strong></div>
                      <div><span>{tr('Прогноз', 'Prognose')}</span><strong>{formatWeeklyValue(mForecast, 1)} {modalItem.unit}</strong></div>
                      <div className={mForecastDelta >= 0 ? 'is-good' : 'is-bad'}><span>{tr('Abw. Prognose', 'Abw. Prognose')}</span><strong>{mForecastDelta >= 0 ? '+' : ''}{formatWeeklyValue(mForecastDelta, 1)} {modalItem.unit}</strong></div>
                    </div>
                  </div>
                </div>
              );
            })()}
          </section>
        )}

        {showTodayPlanPanel && (
          <section className="today-plan-panel" ref={todayRef}>
            <div className="today-plan-head">
              <div className="home-panel-head-main">
                <div className="today-plan-title-wrap">
                  <div className="home-panel-title-row">
                    <h2>{tr('Задачи на сегодня', 'Aufgaben fuer heute')}</h2>
                    <div className="home-panel-head-actions today-plan-head-actions">
                      <button type="button" className="home-panel-action-btn is-accent" onClick={regenerateTodayPlan} disabled={todayPlanLoading}>
                        {tr('Новый план', 'Neu erstellen')}
                      </button>
                      <button type="button" className="home-panel-action-btn" onClick={() => loadTodayPlan({ manual: true, syncFacts: true })} disabled={todayPlanLoading}>
                        {tr('Синхронизировать', 'Synchronisieren')}
                      </button>
                    </div>
                  </div>
                  <p>{tr('Короткий персональный маршрут на день', 'Dein kurzer persoenlicher Plan fuer heute')}</p>
                  <small className={`home-panel-snapshot-meta is-${todayPlanSnapshotTone === 'manual' ? 'fresh' : 'stale'}`}>{todayPlanSnapshotLabel}</small>
                </div>
              </div>
            </div>
            {showTodayPlanSkeleton && <div className="today-plan-skeleton" aria-hidden="true">{[0, 1, 2].map((index) => <div className="today-plan-skeleton-card" key={`today-plan-skeleton-${index}`} />)}</div>}
            {todayPlanLoading && hasTodayPlanSnapshot && <div className="webapp-muted">{tr('Синхронизируем выполнение в фоне...', 'Fortschritt wird im Hintergrund synchronisiert...')}</div>}
            {todayPlanError && <div className="webapp-error">{todayPlanError}</div>}
            {!showTodayPlanSkeleton && !todayPlanLoading && !todayPlanError && !hasTodayPlanItems && <div className="webapp-muted">{tr('План на сегодня пуст.', 'Tagesplan ist leer.')}</div>}
            {!showTodayPlanSkeleton && !todayPlanError && hasTodayPlanItems && (
              <div className="today-plan-items">
                {todayPlan.items.map((item) => {
                  const loadingAction = todayItemLoading[item.id];
                  const taskType = String(item?.task_type || '').toLowerCase();
                  const isTranslationTask = taskType === 'translation';
                  const isVideoTask = taskType === 'video' || taskType === 'youtube';
                  const elapsedSeconds = getTodayItemDisplayElapsedSeconds(item, todayTimerNowMs);
                  const progressPercent = getTodayItemProgressPercent(item, todayTimerNowMs);
                  const translationProgress = isTranslationTask ? getTodayTranslationProgress(item) : null;
                  const doneByProgress = progressPercent >= 100;
                  const done = String(item?.status || '').toLowerCase() === 'done' || doneByProgress;
                  const itemStatusClass = done ? 'done' : (item.status || 'todo');
                  const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
                  const videoTopic = getLocalizedSkillTopicLabel({
                    skill_id: payload?.skill_id,
                    skill_title: payload?.skill_title,
                    sub_category: payload?.sub_category,
                    main_category: payload?.main_category,
                  }, uiLang);
                  const progressBadgeText = done ? 'OK' : (isTranslationTask ? `${translationProgress?.completedCount || 0}/${translationProgress?.targetCount || 0}` : `${Math.round(progressPercent)}%`);
                  return (
                    <div className={`today-plan-item is-${itemStatusClass}`} key={item.id}>
                      <div className="today-plan-item-row">
                        <div className="today-plan-item-title">{getTodayItemTitle(item)}</div>
                        <div className={`today-plan-item-actions ${isVideoTask ? 'is-video-task' : ''}`}>
                          <button type="button" className={`secondary-button today-plan-start-btn ${isVideoTask ? 'today-video-start-btn' : ''}`} onClick={() => startTodayTask(item)} disabled={Boolean(loadingAction) || (!isVideoTask && done)}>
                            {loadingAction === 'start' ? tr('Старт...', 'Start...') : tr('Начать', 'Starten')}
                          </button>
                          <div className={`today-task-progress-badge ${isVideoTask ? 'today-video-progress-badge' : ''} ${done ? 'is-done' : ''}`}>{progressBadgeText}</div>
                          {isVideoTask && (
                            <>
                              <button type="button" className="secondary-button today-video-vote" onClick={() => submitTodayVideoFeedback(item, 'like')} disabled={Boolean(loadingAction)}>+</button>
                              <button type="button" className="secondary-button today-video-vote" onClick={() => submitTodayVideoFeedback(item, 'dislike')} disabled={Boolean(loadingAction)}>-</button>
                            </>
                          )}
                        </div>
                      </div>
                      <div className="today-plan-item-meta">
                        {!isVideoTask && <span>{item.estimated_minutes || 0} {tr('мин', 'Min')}</span>}
                        <span>{done ? 'DONE' : String(item.status || 'todo').toUpperCase()}</span>
                        <span>{formatCompactTimer(elapsedSeconds)}</span>
                      </div>
                      {isVideoTask && videoTopic && <div className="today-video-hint">{videoTopic}</div>}
                    </div>
                  );
                })}
              </div>
            )}
          </section>
        )}

        {showSkillReportPanel && (
          <section className="skill-report-panel" ref={skillsRef}>
            <div className="skill-report-head">
              <div className="home-panel-head-main">
                <div className="home-panel-head-copy">
                  <div className="home-panel-title-row">
                    <h3>{tr('Карта навыков', 'Skill-Ringe')}</h3>
                    <div className="home-panel-head-actions">
                      <button type="button" className="home-panel-action-btn" onClick={() => loadSkillReport({ manual: true, syncFacts: true })} disabled={skillReportLoading}>
                        {tr('Обновить', 'Aktualisieren')}
                      </button>
                    </div>
                  </div>
                  <small className={`home-panel-snapshot-meta is-${skillReportSnapshotTone === 'manual' ? 'fresh' : 'stale'}`}>{skillReportSnapshotLabel}</small>
                </div>
              </div>
            </div>
            {showSkillReportSkeleton && <div className="skill-rings-layout skill-rings-layout-skeleton" aria-hidden="true" />}
            {skillReportLoading && hasSkillReportSnapshot && <div className="webapp-muted">{tr('Обновляем карту навыков...', 'Skill-Karte wird aktualisiert...')}</div>}
            {skillReportError && <div className="webapp-error">{skillReportError}</div>}
            {!showSkillReportSkeleton && !skillReportError && (
              <div className="skill-rings-layout">
                <div className="skill-rings-canvas">
                  <svg width={ringSize} height={ringSize} viewBox={`0 0 ${ringSize} ${ringSize}`} role="img" aria-label="Skill rings">
                    {ringSkills.map((skill, index) => {
                      const radius = ringStartRadius - index * ringStep;
                      const circumference = 2 * Math.PI * radius;
                      const progress = Math.max(0, Math.min(1, Number(skill?.mastery || 0) / 100));
                      const offset = circumference * (1 - progress);
                      const color = ringPalette[index % ringPalette.length];
                      return (
                        <g key={skill.skill_id}>
                          <circle cx={ringCenter} cy={ringCenter} r={radius} fill="none" stroke="rgba(143, 167, 206, 0.22)" strokeWidth="10" />
                          <circle cx={ringCenter} cy={ringCenter} r={radius} fill="none" stroke={color} strokeWidth="10" strokeLinecap="round" strokeDasharray={`${circumference} ${circumference}`} strokeDashoffset={offset} transform={`rotate(-90 ${ringCenter} ${ringCenter})`} />
                        </g>
                      );
                    })}
                  </svg>
                  <div className="skill-rings-center">
                    <div className="skill-rings-center-title">{tr('Фокус', 'Fokus')}</div>
                    <div className="skill-rings-center-value">{ringSkills.length}</div>
                    <div className="skill-rings-center-sub">{tr('3 слабых + 3 сильных', '3 schwache + 3 starke')}</div>
                  </div>
                </div>
                <div className="skill-rings-grid">
                  {ringSkills.map((skill, index) => {
                    const color = ringPalette[index % ringPalette.length];
                    const trainingStatus = getSkillTrainingStatus(skill?.skill_id);
                    const isSkillComplete = Boolean(trainingStatus?.is_complete);
                    const showSkillInProgress = Boolean(trainingStatus && !isSkillComplete);
                    const canResumeSkillTraining = Boolean(getStoredSkillTrainingSnapshot(skill?.skill_id));
                    const isSkillBusy = Boolean(skillPracticeLoading[String(skill.skill_id || '')]);
                    const masteryDisplay = skill?.mastery !== null && skill?.mastery !== undefined ? `${Math.round(Number(skill.mastery || 0))}%` : '-';
                    return (
                      <button
                        type="button"
                        className={`skill-rings-grid-item ${skill.ring_type === 'weak' ? 'is-weak' : 'is-strong'} ${isSkillComplete ? 'is-complete' : ''} ${isSkillBusy ? 'is-busy' : ''}`}
                        key={`grid-${skill.skill_id}`}
                        onClick={() => canResumeSkillTraining ? resumeSkillPractice(skill) : startSkillPractice(skill, { forceRefresh: false })}
                        disabled={isSkillBusy}
                      >
                        <div className="skill-rings-grid-item-top">
                          <span className="skill-rings-dot" style={{ backgroundColor: color }} />
                          {isSkillComplete && <span className="skill-grid-badge is-complete">OK</span>}
                          {showSkillInProgress && <span className="skill-grid-badge is-progress">...</span>}
                          {isSkillBusy && <span className="skill-grid-badge is-loading">...</span>}
                        </div>
                        <div className="skill-rings-grid-name">{getDisplaySkillName(skill)}</div>
                        <div className="skill-rings-grid-stats">
                          <span className="skill-grid-score">{masteryDisplay}</span>
                          <span className="skill-grid-errors">{Number(skill.errors_7d || 0)}F</span>
                        </div>
                      </button>
                    );
                  })}
                  {ringSkills.length === 0 && <div className="webapp-muted skill-rings-empty">{tr('Пока нет данных по навыкам.', 'Noch keine Skill-Daten.')}</div>}
                </div>
              </div>
            )}
          </section>
        )}
      </>
    </PerfWrapper>
  );
}

export default React.memo(PlanningSection);
