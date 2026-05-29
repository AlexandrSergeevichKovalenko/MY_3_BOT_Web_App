import React, { createContext, useCallback, useEffect, useRef, useState } from 'react';

// --- module-level utilities ---

const HOME_SNAPSHOT_AUTO_REFRESH_MAX_AGE_MS = 60 * 60 * 1000;
const TODAY_PLAN_AUTO_REFRESH_MAX_AGE_MS = 60 * 60 * 1000;

function beginAsyncGuard(ref) {
  const nextToken = Number(ref?.current || 0) + 1;
  ref.current = nextToken;
  return nextToken;
}

function isAsyncGuardCurrent(ref, token) {
  return Number(ref?.current || 0) === Number(token || 0);
}

function parseIsoTimestampMs(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function isSnapshotRefreshDue(value, maxAgeMs = HOME_SNAPSHOT_AUTO_REFRESH_MAX_AGE_MS) {
  const ts = parseIsoTimestampMs(value);
  if (!Number.isFinite(ts)) return true;
  return (Date.now() - ts) >= maxAgeMs;
}

export function getLocalDateKey() {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function planStorageGet(key) {
  try { return window.localStorage.getItem(key); } catch (_) { return null; }
}
function planStorageSet(key, value) {
  try { window.localStorage.setItem(key, value); } catch (_) {}
}

export function formatWeeklyValueFn(value, digits = 0) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return '0';
  if (digits <= 0) return String(Math.round(num));
  return num.toFixed(digits);
}

export const PlanningContext = createContext(null);

export const PLANNING_PROVIDER_EXTRACTION_METRICS = {
  provider_name: 'planning',
  state_removed: 23,
  refs_removed: 11,
  effects_removed: 9,
  callbacks_removed: 14,
  memos_removed: 5,
};

export function usePlanningController({
  initData,
  isWebAppMode,
  isLightweightFreeMode,
  startupEffectiveMode,
  stableWebappUserId,
  getWebappLanguagePairHint,
  fetchGetWithRetry,
  readApiError,
  normalizeNetworkErrorMessage,
  tr,
  startupPhase2Ready,
  startupPhase3Ready,
  pageVisible,
  isHomeRouteActive,
  homeSnapshotResumeTick,
  activeHomeSubsectionKey,
  planningApiRef,
}) {
  // --- today plan state ---
  const [todayPlan, setTodayPlan] = useState(null);
  const [todayPlanLoadedOnce, setTodayPlanLoadedOnce] = useState(false);
  const [todayPlanLoading, setTodayPlanLoading] = useState(false);
  const [todayPlanError, setTodayPlanError] = useState('');
  const [todayPlanSnapshotTone, setTodayPlanSnapshotTone] = useState('snapshot');

  // --- skill report state ---
  const [skillReport, setSkillReport] = useState(null);
  const [skillReportLoadedOnce, setSkillReportLoadedOnce] = useState(false);
  const [skillReportLoading, setSkillReportLoading] = useState(false);
  const [skillReportError, setSkillReportError] = useState('');
  const [skillReportSnapshotTone, setSkillReportSnapshotTone] = useState('snapshot');

  // --- weekly plan state ---
  const [weeklyPlan, setWeeklyPlan] = useState(null);
  const [weeklyPlanLoading, setWeeklyPlanLoading] = useState(false);
  const [weeklyPlanSaving, setWeeklyPlanSaving] = useState(false);
  const [weeklyPlanError, setWeeklyPlanError] = useState('');
  const [weeklyPlanSnapshotTone, setWeeklyPlanSnapshotTone] = useState('snapshot');
  const [weeklyPlanDraft, setWeeklyPlanDraft] = useState({
    translations_goal: '',
    learned_words_goal: '',
    agent_minutes_goal: '',
    reading_minutes_goal: '',
  });
  const [weeklyPlanCollapsed, setWeeklyPlanCollapsed] = useState(false);
  const [weeklyMetricExpanded, setWeeklyMetricExpanded] = useState({
    translations: false,
    learned_words: false,
    agent_minutes: false,
    reading_minutes: false,
  });

  // --- plan analytics state ---
  const [planAnalyticsPeriod, setPlanAnalyticsPeriod] = useState('week');
  const [planAnalyticsMetrics, setPlanAnalyticsMetrics] = useState({});
  const [planAnalyticsRange, setPlanAnalyticsRange] = useState(null);
  const [planAnalyticsLoading, setPlanAnalyticsLoading] = useState(false);
  const [planAnalyticsError, setPlanAnalyticsError] = useState('');

  // --- storage keys (stable - derived from stableWebappUserId + date) ---
  const currentLocalDateKey = getLocalDateKey();
  const weeklyPlanSnapshotStorageKey = `weekly_plan_snapshot_${stableWebappUserId}`;
  const todayPlanSnapshotStorageKey = `today_plan_snapshot_${stableWebappUserId}_${currentLocalDateKey}`;
  const skillReportSnapshotStorageKey = `skill_report_snapshot_${stableWebappUserId}_${currentLocalDateKey}`;

  // --- async guard refs ---
  const todayPlanRequestIdRef = useRef(0);
  const skillReportRequestIdRef = useRef(0);
  const weeklyPlanRequestIdRef = useRef(0);

  // --- startup refresh done flags ---
  const todayPlanStartupRefreshDoneRef = useRef(false);
  const skillReportStartupRefreshDoneRef = useRef(false);
  const weeklyPlanStartupRefreshDoneRef = useRef(false);

  // --- stable loader refs for effects that close over them ---
  const loadWeeklyPlanRef = useRef(null);
  const loadSkillReportRef = useRef(null);
  const weeklyPlanLoadingRef = useRef(weeklyPlanLoading);
  weeklyPlanLoadingRef.current = weeklyPlanLoading;
  const skillReportLoadingRef = useRef(skillReportLoading);
  skillReportLoadingRef.current = skillReportLoading;

  // --- snapshot helpers ---

  const buildWeeklyPlanDraftFromPlan = useCallback((plan) => ({
    translations_goal: String(Number(plan?.plan?.translations_goal || 0)),
    learned_words_goal: String(Number(plan?.plan?.learned_words_goal || 0)),
    agent_minutes_goal: String(Number(plan?.plan?.agent_minutes_goal || 0)),
    reading_minutes_goal: String(Number(plan?.plan?.reading_minutes_goal || 0)),
  }), []);

  const persistWeeklyPlanSnapshot = useCallback((plan) => {
    if (!isWebAppMode || !plan) return;
    planStorageSet(weeklyPlanSnapshotStorageKey, JSON.stringify({
      saved_at: new Date().toISOString(),
      plan,
      draft: buildWeeklyPlanDraftFromPlan(plan),
    }));
  }, [buildWeeklyPlanDraftFromPlan, isWebAppMode, weeklyPlanSnapshotStorageKey]);

  const readWeeklyPlanSnapshot = useCallback(() => {
    if (!isWebAppMode) return null;
    const raw = planStorageGet(weeklyPlanSnapshotStorageKey);
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw);
      const plan = parsed?.plan && typeof parsed.plan === 'object' ? parsed.plan : null;
      const draft = parsed?.draft && typeof parsed.draft === 'object' ? parsed.draft : null;
      const startDate = String(plan?.week?.start_date || '').trim();
      const endDate = String(plan?.week?.end_date || '').trim();
      const todayKey = getLocalDateKey();
      if (!plan || !startDate || !endDate || todayKey < startDate || todayKey > endDate) return null;
      return {
        plan: {
          ...plan,
          snapshot_saved_at: String(parsed?.saved_at || plan?.snapshot_saved_at || '').trim() || null,
        },
        draft: draft || buildWeeklyPlanDraftFromPlan(plan),
      };
    } catch (_) { return null; }
  }, [buildWeeklyPlanDraftFromPlan, isWebAppMode, weeklyPlanSnapshotStorageKey]);

  const normalizeTodayPlanSnapshot = useCallback((payload) => {
    if (!payload || typeof payload !== 'object') return null;
    return {
      date: payload?.date || null,
      total_minutes: Number(payload?.total_minutes || 0),
      items: Array.isArray(payload?.items) ? payload.items : [],
    };
  }, []);

  const persistTodayPlanSnapshot = useCallback((plan) => {
    if (!isWebAppMode || !plan) return;
    const normalized = normalizeTodayPlanSnapshot(plan);
    if (!normalized) return;
    planStorageSet(todayPlanSnapshotStorageKey, JSON.stringify({
      saved_at: new Date().toISOString(),
      date_key: currentLocalDateKey,
      plan: normalized,
    }));
  }, [currentLocalDateKey, isWebAppMode, normalizeTodayPlanSnapshot, todayPlanSnapshotStorageKey]);

  const readTodayPlanSnapshot = useCallback(() => {
    if (!isWebAppMode) return null;
    const raw = planStorageGet(todayPlanSnapshotStorageKey);
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw);
      const snapshotDateKey = String(parsed?.date_key || '').trim();
      if (snapshotDateKey && snapshotDateKey !== currentLocalDateKey) return null;
      const plan = normalizeTodayPlanSnapshot(parsed?.plan);
      if (!plan) return null;
      if (String(plan?.date || '').trim() && String(plan.date).trim() !== currentLocalDateKey) return null;
      return {
        ...plan,
        snapshot_saved_at: String(parsed?.saved_at || plan?.snapshot_saved_at || '').trim() || null,
      };
    } catch (_) { return null; }
  }, [currentLocalDateKey, isWebAppMode, normalizeTodayPlanSnapshot, todayPlanSnapshotStorageKey]);

  const normalizeSkillReportSnapshot = useCallback((payload) => {
    if (!payload || typeof payload !== 'object') return null;
    return {
      updated_at: payload?.updated_at || null,
      top_weak: Array.isArray(payload?.top_weak) ? payload.top_weak : [],
      groups: Array.isArray(payload?.groups) ? payload.groups : [],
      total_skills: Number(payload?.total_skills || 0),
      skill_training_status: payload?.skill_training_status && typeof payload.skill_training_status === 'object'
        ? payload.skill_training_status
        : {},
    };
  }, []);

  const persistSkillReportSnapshot = useCallback((report) => {
    if (!isWebAppMode || !report) return;
    const normalized = normalizeSkillReportSnapshot(report);
    if (!normalized) return;
    planStorageSet(skillReportSnapshotStorageKey, JSON.stringify({
      saved_at: new Date().toISOString(),
      date_key: currentLocalDateKey,
      report: normalized,
    }));
  }, [currentLocalDateKey, isWebAppMode, normalizeSkillReportSnapshot, skillReportSnapshotStorageKey]);

  const readSkillReportSnapshot = useCallback(() => {
    if (!isWebAppMode) return null;
    const raw = planStorageGet(skillReportSnapshotStorageKey);
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw);
      const snapshotDateKey = String(parsed?.date_key || '').trim();
      if (snapshotDateKey && snapshotDateKey !== currentLocalDateKey) return null;
      const report = normalizeSkillReportSnapshot(parsed?.report);
      if (!report) return null;
      return {
        ...report,
        snapshot_saved_at: String(parsed?.saved_at || report?.snapshot_saved_at || '').trim() || null,
      };
    } catch (_) { return null; }
  }, [currentLocalDateKey, isWebAppMode, normalizeSkillReportSnapshot, skillReportSnapshotStorageKey]);

  // --- loaders ---

  const loadTodayPlan = useCallback(async (options = {}) => {
    if (!initData) return;
    if (isLightweightFreeMode) {
      console.info('startup_heavy_block_skipped', { feature: 'today', startupEffectiveMode });
      setTodayPlanLoadedOnce(true);
      setTodayPlanLoading(false);
      return;
    }
    const requestId = beginAsyncGuard(todayPlanRequestIdRef);
    const tone = options?.manual ? 'manual' : 'snapshot';
    const syncFacts = Boolean(options?.syncFacts);
    try {
      setTodayPlanLoading(true);
      setTodayPlanError('');
      const pairHint = getWebappLanguagePairHint();
      const query = new URLSearchParams({ initData });
      if (pairHint?.source_lang) query.set('source_lang', pairHint.source_lang);
      if (pairHint?.target_lang) query.set('target_lang', pairHint.target_lang);
      const response = syncFacts
        ? await fetch('/api/today/sync', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, language_pair: pairHint || undefined }),
        })
        : await fetchGetWithRetry(`/api/today?${query.toString()}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(
          response,
          syncFacts ? 'Ошибка синхронизации выполнения' : 'Ошибка загрузки плана на сегодня',
          syncFacts ? 'Fehler bei der Fortschritt-Synchronisierung' : 'Fehler beim Laden des Tagesplans',
        ));
      }
      const data = await response.json();
      if (!isAsyncGuardCurrent(todayPlanRequestIdRef, requestId)) return;
      const nextPlan = {
        date: data?.date || null,
        total_minutes: data?.total_minutes || 0,
        items: Array.isArray(data?.items) ? data.items : [],
        snapshot_saved_at: new Date().toISOString(),
      };
      setTodayPlan((prevPlan) => {
        const prevItems = Array.isArray(prevPlan?.items) ? prevPlan.items : [];
        const prevById = Object.fromEntries(prevItems.map((it) => [String(it?.id ?? ''), it]));
        const mergedItems = nextPlan.items.map((newItem) => {
          const newPayload = newItem?.payload && typeof newItem.payload === 'object' ? newItem.payload : null;
          const hasTimerData = newPayload !== null && (
            newPayload.timer_running !== undefined || newPayload.timer_seconds !== undefined
          );
          if (!hasTimerData) {
            const prevItem = prevById[String(newItem?.id ?? '')];
            if (prevItem?.payload && typeof prevItem.payload === 'object') {
              return { ...newItem, payload: prevItem.payload };
            }
          }
          return newItem;
        });
        return { ...nextPlan, items: mergedItems };
      });
      setTodayPlanSnapshotTone(tone);
      persistTodayPlanSnapshot(nextPlan);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        syncFacts ? 'Не удалось синхронизировать выполнение на сегодня.' : 'Не удалось загрузить задачи на сегодня.',
        syncFacts ? 'Fortschritt fuer heute konnte nicht synchronisiert werden.' : 'Tagesaufgaben konnten nicht geladen werden.',
      );
      if (!isAsyncGuardCurrent(todayPlanRequestIdRef, requestId)) return;
      setTodayPlanError(friendly);
    } finally {
      if (isAsyncGuardCurrent(todayPlanRequestIdRef, requestId)) {
        setTodayPlanLoadedOnce(true);
        setTodayPlanLoading(false);
      }
    }
  }, [
    initData, isLightweightFreeMode, startupEffectiveMode,
    fetchGetWithRetry, readApiError, normalizeNetworkErrorMessage,
    getWebappLanguagePairHint, persistTodayPlanSnapshot,
  ]);

  const loadSkillReport = useCallback(async (options = {}) => {
    if (!initData) return;
    if (isLightweightFreeMode) {
      console.info('startup_heavy_block_skipped', { feature: 'skills', startupEffectiveMode });
      setSkillReportLoadedOnce(true);
      setSkillReportLoading(false);
      return;
    }
    const requestId = beginAsyncGuard(skillReportRequestIdRef);
    const tone = options?.manual ? 'manual' : 'snapshot';
    const syncFacts = Boolean(options?.syncFacts);
    try {
      setSkillReportLoading(true);
      setSkillReportError('');
      const pairHint = getWebappLanguagePairHint();
      const query = new URLSearchParams({ period: '7d', initData });
      if (pairHint?.source_lang) query.set('source_lang', pairHint.source_lang);
      if (pairHint?.target_lang) query.set('target_lang', pairHint.target_lang);
      const response = syncFacts
        ? await fetch('/api/progress/skills/sync', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, period: '7d', language_pair: pairHint || undefined }),
        })
        : await fetchGetWithRetry(`/api/progress/skills?${query.toString()}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(
          response,
          syncFacts ? 'Ошибка синхронизации навыков' : 'Ошибка загрузки отчета по навыкам',
          syncFacts ? 'Fehler bei der Skill-Synchronisierung' : 'Fehler beim Laden des Skills-Reports',
        ));
      }
      const data = await response.json();
      if (!isAsyncGuardCurrent(skillReportRequestIdRef, requestId)) return;
      const nextReport = {
        updated_at: data?.updated_at || null,
        top_weak: Array.isArray(data?.top_weak) ? data.top_weak : [],
        groups: Array.isArray(data?.groups) ? data.groups : [],
        total_skills: Number(data?.total_skills || 0),
        skill_training_status: data?.skill_training_status && typeof data.skill_training_status === 'object'
          ? data.skill_training_status
          : {},
        snapshot_saved_at: new Date().toISOString(),
      };
      setSkillReport(nextReport);
      setSkillReportSnapshotTone(tone);
      persistSkillReportSnapshot(nextReport);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        syncFacts ? 'Не удалось синхронизировать прогресс навыков.' : 'Не удалось загрузить прогресс навыков.',
        syncFacts ? 'Skill-Fortschritt konnte nicht synchronisiert werden.' : 'Skill-Fortschritt konnte nicht geladen werden.',
      );
      if (!isAsyncGuardCurrent(skillReportRequestIdRef, requestId)) return;
      setSkillReportError(friendly);
    } finally {
      if (isAsyncGuardCurrent(skillReportRequestIdRef, requestId)) {
        setSkillReportLoadedOnce(true);
        setSkillReportLoading(false);
      }
    }
  }, [
    initData, isLightweightFreeMode, startupEffectiveMode,
    fetchGetWithRetry, readApiError, normalizeNetworkErrorMessage,
    getWebappLanguagePairHint, persistSkillReportSnapshot,
  ]);

  const loadWeeklyPlan = useCallback(async (options = {}) => {
    if (!initData) return;
    if (isLightweightFreeMode) {
      console.info('startup_heavy_block_skipped', { feature: 'weekly_plan', startupEffectiveMode });
      setWeeklyPlanLoading(false);
      return;
    }
    const requestId = beginAsyncGuard(weeklyPlanRequestIdRef);
    const tone = options?.manual ? 'manual' : 'snapshot';
    const syncFacts = Boolean(options?.syncFacts);
    const silent = Boolean(options?.silent);
    try {
      if (!silent) setWeeklyPlanLoading(true);
      setWeeklyPlanError('');
      const response = syncFacts
        ? await fetch('/api/progress/weekly-plan/sync', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData }),
        })
        : await fetchGetWithRetry(`/api/progress/weekly-plan?initData=${encodeURIComponent(initData)}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(
          response,
          syncFacts ? 'Ошибка синхронизации недельного плана' : 'Ошибка загрузки недельного плана',
          syncFacts ? 'Fehler bei der Wochenplan-Synchronisierung' : 'Fehler beim Laden des Wochenplans',
        ));
      }
      const data = await response.json();
      if (!isAsyncGuardCurrent(weeklyPlanRequestIdRef, requestId)) return;
      const snapshotPending = Boolean(data?.snapshot_pending);
      const cachedSnapshotPlan = readWeeklyPlanSnapshot()?.plan || null;
      // weeklyPlan read from state via closure — use functional setter to get latest
      setWeeklyPlan((preservedPlan) => {
        const effectivePreserved = preservedPlan || cachedSnapshotPlan || null;
        const plan = {
          week: data?.week || effectivePreserved?.week || null,
          plan: data?.plan || effectivePreserved?.plan || {
            translations_goal: 0, learned_words_goal: 0,
            agent_minutes_goal: 0, reading_minutes_goal: 0,
          },
          metrics: snapshotPending
            ? (effectivePreserved?.metrics || data?.metrics || {})
            : (data?.metrics || {}),
          snapshot_saved_at: snapshotPending
            ? String(effectivePreserved?.snapshot_saved_at || '').trim() || new Date().toISOString()
            : new Date().toISOString(),
          snapshot_pending: snapshotPending,
        };
        setWeeklyPlanSnapshotTone(tone);
        setWeeklyPlanDraft(buildWeeklyPlanDraftFromPlan(plan));
        if (!snapshotPending) persistWeeklyPlanSnapshot(plan);
        return plan;
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        syncFacts ? 'Не удалось синхронизировать недельный план.' : 'Не удалось загрузить недельный план.',
        syncFacts ? 'Wochenplan konnte nicht synchronisiert werden.' : 'Wochenplan konnte nicht geladen werden.',
      );
      if (!isAsyncGuardCurrent(weeklyPlanRequestIdRef, requestId)) return;
      setWeeklyPlanError(friendly);
    } finally {
      if (isAsyncGuardCurrent(weeklyPlanRequestIdRef, requestId)) {
        if (!silent) setWeeklyPlanLoading(false);
      }
    }
  }, [
    initData, isLightweightFreeMode, startupEffectiveMode,
    fetchGetWithRetry, readApiError, normalizeNetworkErrorMessage,
    buildWeeklyPlanDraftFromPlan, persistWeeklyPlanSnapshot, readWeeklyPlanSnapshot,
  ]);

  const loadPlanAnalytics = useCallback(async (periodOverride) => {
    if (!initData) return;
    const period = periodOverride || planAnalyticsPeriod;
    try {
      setPlanAnalyticsLoading(true);
      setPlanAnalyticsError('');
      const response = await fetchGetWithRetry(
        `/api/progress/plan-analytics?initData=${encodeURIComponent(initData)}&period=${encodeURIComponent(period)}`,
        45000,
      );
      if (!response.ok) {
        throw new Error(await readApiError(
          response,
          'Ошибка загрузки аналитики планов',
          'Fehler beim Laden der Plan-Analytik',
        ));
      }
      const data = await response.json();
      setPlanAnalyticsMetrics(data?.metrics || {});
      setPlanAnalyticsRange(data?.range || null);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить аналитику планов.',
        'Plan-Analytik konnte nicht geladen werden.',
      );
      setPlanAnalyticsError(friendly);
    } finally {
      setPlanAnalyticsLoading(false);
    }
  }, [initData, planAnalyticsPeriod, fetchGetWithRetry, readApiError, normalizeNetworkErrorMessage]);

  // Keep stable loader refs up-to-date (used by effects that have stale closure constraints)
  loadWeeklyPlanRef.current = loadWeeklyPlan;
  loadSkillReportRef.current = loadSkillReport;

  // --- fill planningApiRef synchronously on every render so callers always get latest ---
  const todayPlanRef = useRef(todayPlan);
  todayPlanRef.current = todayPlan;

  if (planningApiRef) {
    planningApiRef.current = {
      loadTodayPlan,
      loadSkillReport,
      loadWeeklyPlan,
      loadPlanAnalytics,
      getTodayTaskForSection: (sectionKey) => {
        const normalized = String(sectionKey || '').toLowerCase();
        const items = Array.isArray(todayPlanRef.current?.items) ? todayPlanRef.current.items : [];
        const findByTypes = (types) => {
          const typeSet = new Set(types.map((t) => String(t || '').toLowerCase()));
          const active = items.find((e) => typeSet.has(String(e?.task_type || '').toLowerCase()) && String(e?.status || '').toLowerCase() !== 'done');
          return active || items.find((e) => typeSet.has(String(e?.task_type || '').toLowerCase())) || null;
        };
        if (normalized === 'translations') return findByTypes(['translation']);
        if (normalized === 'theory') return findByTypes(['theory']);
        if (normalized === 'youtube') return findByTypes(['video', 'youtube']);
        if (normalized === 'flashcards') return findByTypes(['cards']);
        return null;
      },
      setTodayPlan,
      setTodayPlanError,
    };
  }

  // --- startup effects: reset on no initData ---

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setTodayPlan(null);
      setTodayPlanLoadedOnce(false);
      setTodayPlanError('');
    }
  }, [isWebAppMode, initData]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setSkillReport(null);
      setSkillReportLoadedOnce(false);
      setSkillReportError('');
    }
  }, [isWebAppMode, initData, startupPhase2Ready]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      todayPlanRequestIdRef.current += 1;
      skillReportRequestIdRef.current += 1;
      weeklyPlanRequestIdRef.current += 1;
      todayPlanStartupRefreshDoneRef.current = false;
      skillReportStartupRefreshDoneRef.current = false;
      weeklyPlanStartupRefreshDoneRef.current = false;
      return;
    }
    if (!pageVisible) {
      todayPlanStartupRefreshDoneRef.current = false;
      skillReportStartupRefreshDoneRef.current = false;
      weeklyPlanStartupRefreshDoneRef.current = false;
    }
  }, [initData, isWebAppMode, pageVisible]);

  // --- snapshot hydration effects ---

  useEffect(() => {
    if (!isWebAppMode || !initData || weeklyPlan) return;
    const snapshot = readWeeklyPlanSnapshot();
    if (!snapshot?.plan) return;
    setWeeklyPlan(snapshot.plan);
    setWeeklyPlanSnapshotTone('snapshot');
    setWeeklyPlanDraft(snapshot.draft || buildWeeklyPlanDraftFromPlan(snapshot.plan));
  }, [buildWeeklyPlanDraftFromPlan, initData, isWebAppMode, readWeeklyPlanSnapshot, weeklyPlan]);

  useEffect(() => {
    if (!isWebAppMode || !initData || todayPlan) return;
    const snapshot = readTodayPlanSnapshot();
    if (!snapshot) return;
    setTodayPlan(snapshot);
    setTodayPlanSnapshotTone('snapshot');
    setTodayPlanLoadedOnce(true);
  }, [initData, isWebAppMode, readTodayPlanSnapshot, todayPlan]);

  useEffect(() => {
    if (!isWebAppMode || !initData || skillReport) return;
    const snapshot = readSkillReportSnapshot();
    if (!snapshot) return;
    setSkillReport(snapshot);
    setSkillReportSnapshotTone('snapshot');
    setSkillReportLoadedOnce(true);
  }, [initData, isWebAppMode, readSkillReportSnapshot, skillReport]);

  // --- startup auto-refresh: today plan ---

  useEffect(() => {
    const shouldHydrateTodayPanel = Boolean(isHomeRouteActive);
    if (
      !isWebAppMode
      || !initData
      || !pageVisible
      || !startupPhase2Ready
      || !shouldHydrateTodayPanel
      || todayPlanStartupRefreshDoneRef.current
    ) {
      return undefined;
    }
    const snapshot = readTodayPlanSnapshot();
    const shouldRefresh = (
      !snapshot
      || isSnapshotRefreshDue(snapshot?.snapshot_saved_at, TODAY_PLAN_AUTO_REFRESH_MAX_AGE_MS)
      || !(snapshot?.items?.length > 0)
    );
    if (!shouldRefresh) {
      todayPlanStartupRefreshDoneRef.current = true;
      return undefined;
    }
    const delayMs = snapshot ? 1600 : 350;
    const timerId = window.setTimeout(() => {
      todayPlanStartupRefreshDoneRef.current = true;
      void loadTodayPlan({ syncFacts: true });
    }, delayMs);
    return () => window.clearTimeout(timerId);
  }, [
    homeSnapshotResumeTick,
    initData,
    isHomeRouteActive,
    isWebAppMode,
    loadTodayPlan,
    pageVisible,
    readTodayPlanSnapshot,
    startupPhase2Ready,
    startupEffectiveMode,
  ]);

  // --- startup auto-refresh: weekly plan and skill report on subsection enter ---

  useEffect(() => {
    if (!isWebAppMode || !initData || !pageVisible || !startupPhase3Ready) return undefined;
    if (activeHomeSubsectionKey !== 'home_skills') {
      skillReportStartupRefreshDoneRef.current = false;
    }
    if (activeHomeSubsectionKey !== 'home_weekly_plan') {
      weeklyPlanStartupRefreshDoneRef.current = false;
    }
    if (activeHomeSubsectionKey === 'home_weekly_plan') {
      if (weeklyPlanLoadingRef.current || weeklyPlanStartupRefreshDoneRef.current) return undefined;
      const snapshot = readWeeklyPlanSnapshot();
      const shouldRefresh = !snapshot || isSnapshotRefreshDue(snapshot?.snapshot_saved_at);
      if (!shouldRefresh) {
        weeklyPlanStartupRefreshDoneRef.current = true;
        return undefined;
      }
      const delayMs = snapshot ? 1200 : 250;
      const timerId = window.setTimeout(() => {
        weeklyPlanStartupRefreshDoneRef.current = true;
        void loadWeeklyPlanRef.current();
      }, delayMs);
      return () => window.clearTimeout(timerId);
    }
    if (activeHomeSubsectionKey === 'home_skills') {
      if (skillReportLoadingRef.current || skillReportStartupRefreshDoneRef.current) return undefined;
      const snapshot = readSkillReportSnapshot();
      const shouldRefresh = !snapshot || isSnapshotRefreshDue(snapshot?.snapshot_saved_at);
      if (!shouldRefresh) {
        skillReportStartupRefreshDoneRef.current = true;
        return undefined;
      }
      const delayMs = snapshot ? 1200 : 250;
      const timerId = window.setTimeout(() => {
        skillReportStartupRefreshDoneRef.current = true;
        void loadSkillReportRef.current();
      }, delayMs);
      return () => window.clearTimeout(timerId);
    }
    return undefined;
  }, [
    activeHomeSubsectionKey,
    homeSnapshotResumeTick,
    initData,
    isWebAppMode,
    pageVisible,
    readSkillReportSnapshot,
    readWeeklyPlanSnapshot,
    startupPhase3Ready,
    startupEffectiveMode,
  ]);

  // --- auto-load plan analytics when period changes away from 'week' ---

  useEffect(() => {
    if (!isWebAppMode || !initData || !startupPhase3Ready || planAnalyticsPeriod === 'week') return;
    void loadPlanAnalytics(planAnalyticsPeriod);
  }, [planAnalyticsPeriod, isWebAppMode, initData, startupPhase3Ready]); // eslint-disable-line react-hooks/exhaustive-deps

  // --- provider observability ---

  useEffect(() => {
    console.info('planning_controller_mount', {
      provider_name: 'planning',
      effective_mode: startupEffectiveMode,
    });
    return () => {
      console.info('planning_controller_unmount', {
        provider_name: 'planning',
        effective_mode: startupEffectiveMode,
      });
    };
  }, []);

  useEffect(() => {
    console.info('frontend_provider_extracted', PLANNING_PROVIDER_EXTRACTION_METRICS);
  }, []);

  // --- derived values ---

  const weeklyPlanUsesDeferredAnalytics = planAnalyticsPeriod !== 'week';
  const hasPlanAnalyticsMetrics = Object.keys(planAnalyticsMetrics || {}).length > 0;
  const useLivePlanAnalyticsMetrics = hasPlanAnalyticsMetrics && !planAnalyticsError;
  const weeklyMetrics = weeklyPlanUsesDeferredAnalytics
    ? (useLivePlanAnalyticsMetrics ? planAnalyticsMetrics : {})
    : (useLivePlanAnalyticsMetrics ? planAnalyticsMetrics : (weeklyPlan?.metrics || {}));

  const weeklyMetricRows = [
    {
      key: 'translations',
      title: tr('Переводы предложений', 'Satz-Uebersetzungen'),
      unit: tr('шт', 'Stk'),
      data: weeklyMetrics.translations || {},
    },
    {
      key: 'learned_words',
      title: tr('Выученные слова (Space Repetition)', 'Gelernte Woerter (Space Repetition)'),
      unit: tr('слов', 'Woerter'),
      data: weeklyMetrics.learned_words || {},
    },
    {
      key: 'agent_minutes',
      title: tr('Минуты разговора с агентом', 'Gespraechsminuten mit Assistent'),
      unit: tr('мин', 'Min'),
      data: weeklyMetrics.agent_minutes || {},
    },
    {
      key: 'reading_minutes',
      title: tr('Чтение (минуты)', 'Lesen (Minuten)'),
      unit: tr('мин', 'Min'),
      data: weeklyMetrics.reading_minutes || {},
    },
  ];

  const homeSkillTrainingStatusMap = skillReport?.skill_training_status && typeof skillReport.skill_training_status === 'object'
    ? skillReport.skill_training_status
    : {};

  // --- context value ---

  const contextValue = {
    // today plan
    todayPlan, setTodayPlan,
    todayPlanLoadedOnce,
    todayPlanLoading,
    todayPlanError, setTodayPlanError,
    todayPlanSnapshotTone,
    // skill report
    skillReport, setSkillReport,
    skillReportLoadedOnce,
    skillReportLoading,
    skillReportError, setSkillReportError,
    skillReportSnapshotTone,
    // weekly plan
    weeklyPlan, setWeeklyPlan,
    weeklyPlanLoading,
    weeklyPlanSaving, setWeeklyPlanSaving,
    weeklyPlanError, setWeeklyPlanError,
    weeklyPlanSnapshotTone, setWeeklyPlanSnapshotTone,
    weeklyPlanDraft, setWeeklyPlanDraft,
    weeklyPlanCollapsed, setWeeklyPlanCollapsed,
    weeklyMetricExpanded, setWeeklyMetricExpanded,
    // plan analytics
    planAnalyticsPeriod, setPlanAnalyticsPeriod,
    planAnalyticsMetrics,
    planAnalyticsRange,
    planAnalyticsLoading,
    planAnalyticsError,
    // loaders
    loadTodayPlan,
    loadSkillReport,
    loadWeeklyPlan,
    loadPlanAnalytics,
    // snapshot helpers (App.jsx functions still need these)
    buildWeeklyPlanDraftFromPlan,
    persistWeeklyPlanSnapshot,
    readWeeklyPlanSnapshot,
    normalizeTodayPlanSnapshot,
    persistTodayPlanSnapshot,
    readTodayPlanSnapshot,
    normalizeSkillReportSnapshot,
    persistSkillReportSnapshot,
    readSkillReportSnapshot,
    // derived
    weeklyMetrics,
    weeklyMetricRows,
    formatWeeklyValue: formatWeeklyValueFn,
    homeSkillTrainingStatusMap,
    weeklyPlanUsesDeferredAnalytics,
    useLivePlanAnalyticsMetrics,
    hasPlanAnalyticsMetrics,
  };

  return contextValue;
}

export function PlanningProvider(props) {
  const contextValue = usePlanningController(props);

  useEffect(() => {
    console.info('provider_mount', {
      provider_name: 'planning',
      effective_mode: props?.startupEffectiveMode,
    });
    return () => {
      console.info('provider_unmount', {
        provider_name: 'planning',
        effective_mode: props?.startupEffectiveMode,
      });
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <PlanningContext.Provider value={contextValue}>
      {props.children}
    </PlanningContext.Provider>
  );
}
