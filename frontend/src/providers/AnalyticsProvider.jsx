import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';

function formatDateInputValue(value) {
  const date = value instanceof Date ? new Date(value) : new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return '';
  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function addCalendarDays(value, days) {
  const date = new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return formatDateInputValue(new Date());
  date.setDate(date.getDate() + Number(days || 0));
  return formatDateInputValue(date);
}

export function buildDefaultAnalyticsCalendarRange() {
  const today = formatDateInputValue(new Date());
  return { startDate: addCalendarDays(today, -29), endDate: today };
}

function parseIsoDateParts(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return { year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
}

export function formatAnalyticsCalendarDisplayDate(value, locale = 'ru-RU') {
  const parts = parseIsoDateParts(value);
  if (!parts) return '';
  const safeDate = new Date(parts.year, parts.month - 1, parts.day);
  if (Number.isNaN(safeDate.getTime())) return '';
  return new Intl.DateTimeFormat(locale, { day: '2-digit', month: '2-digit', year: 'numeric' }).format(safeDate);
}

export function parseAnalyticsScopeKey(rawValue) {
  const value = String(rawValue || '').trim();
  if (!value || value === 'personal') {
    return { scope_key: 'personal', scope_kind: 'personal', scope_chat_id: null };
  }
  if (value.startsWith('group:')) {
    const parsedId = Number.parseInt(value.slice('group:'.length), 10);
    if (Number.isFinite(parsedId)) {
      return { scope_key: `group:${parsedId}`, scope_kind: 'group', scope_chat_id: parsedId };
    }
  }
  return { scope_key: 'personal', scope_kind: 'personal', scope_chat_id: null };
}

export const AnalyticsContext = createContext(null);

export function AnalyticsProvider({
  initData,
  isWebAppMode,
  selectedSections,
  flashcardsOnly,
  tr,
  uiLang,
  readApiError,
  webappUser,
  themeMode,
  webappChatType,
  languageProfile,
  postJsonWithRetry,
  normalizeNetworkErrorMessage,
  initDataMissingMsg,
  telegramApp,
  onProgressResetApplied,
  children,
}) {
  const [analyticsPeriod, setAnalyticsPeriod] = useState('week');
  const [analyticsPeriodSelectVersion, setAnalyticsPeriodSelectVersion] = useState(0);
  const [analyticsCalendarOpen, setAnalyticsCalendarOpen] = useState(false);
  const [analyticsCustomStartDate, setAnalyticsCustomStartDate] = useState('');
  const [analyticsCustomEndDate, setAnalyticsCustomEndDate] = useState('');
  const [analyticsCalendarDraftStartDate, setAnalyticsCalendarDraftStartDate] = useState('');
  const [analyticsCalendarDraftEndDate, setAnalyticsCalendarDraftEndDate] = useState('');
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState('');
  const [analyticsSummary, setAnalyticsSummary] = useState(null);
  const [analyticsPoints, setAnalyticsPoints] = useState([]);
  const [analyticsCompare, setAnalyticsCompare] = useState([]);
  const [analyticsRank, setAnalyticsRank] = useState(null);
  const [analyticsScopeData, setAnalyticsScopeData] = useState(null);
  const [analyticsScopeKey, setAnalyticsScopeKey] = useState('personal');
  const [analyticsBootstrapReady, setAnalyticsBootstrapReady] = useState(false);
  const [analyticsScopeLoading, setAnalyticsScopeLoading] = useState(false);
  const [analyticsScopeSaving, setAnalyticsScopeSaving] = useState(false);
  const [analyticsScopeError, setAnalyticsScopeError] = useState('');
  const [analyticsTrendVisible, setAnalyticsTrendVisible] = useState(false);
  const [analyticsCompareVisible, setAnalyticsCompareVisible] = useState(false);
  const [progressResetInfo, setProgressResetInfo] = useState(null);
  const [progressResetLoading, setProgressResetLoading] = useState(false);
  const [progressResetSaving, setProgressResetSaving] = useState(false);
  const [progressResetError, setProgressResetError] = useState('');
  const [progressResetModalOpen, setProgressResetModalOpen] = useState(false);
  const [progressResetDraftDate, setProgressResetDraftDate] = useState('');

  const analyticsScopeRequestRef = useRef(null);
  const analyticsSummaryRequestIdRef = useRef(0);
  const analyticsTimeseriesRequestIdRef = useRef(0);
  const analyticsCompareRequestIdRef = useRef(0);
  const analyticsCalendarRef = useRef(null);
  const analyticsPeriodSelectRef = useRef(null);
  const analyticsTrendRef = useRef(null);
  const analyticsCompareRef = useRef(null);
  const analyticsCompareWordsRef = useRef(null);
  const analyticsCompareErrorsRef = useRef(null);

  useEffect(() => {
    console.info('provider_mount', { provider: 'analytics' });
    return () => {
      console.info('provider_unmount', { provider: 'analytics' });
    };
  }, []);

  const analyticsCalendarRangeValid = Boolean(
    analyticsCustomStartDate
    && analyticsCustomEndDate
    && analyticsCustomStartDate <= analyticsCustomEndDate
  );
  const analyticsCalendarDraftValid = Boolean(
    analyticsCalendarDraftStartDate
    && analyticsCalendarDraftEndDate
    && analyticsCalendarDraftStartDate <= analyticsCalendarDraftEndDate
  );

  const analyticsCalendarLabel = useMemo(() => {
    if (!analyticsCalendarRangeValid) {
      return tr('Период не выбран', 'Zeitraum nicht gewaehlt');
    }
    const locale = uiLang === 'de' ? 'de-AT' : 'ru-RU';
    return `${formatAnalyticsCalendarDisplayDate(analyticsCustomStartDate, locale)} - ${formatAnalyticsCalendarDisplayDate(analyticsCustomEndDate, locale)}`;
  }, [analyticsCalendarRangeValid, analyticsCustomEndDate, analyticsCustomStartDate, tr, uiLang]);

  const progressResetDateLabel = useMemo(() => {
    const locale = uiLang === 'de' ? 'de-AT' : 'ru-RU';
    const resetDate = String(progressResetInfo?.reset?.reset_date || '').trim();
    if (!resetDate) return tr('Без точки отсчета', 'Ohne Neustart-Datum');
    return formatAnalyticsCalendarDisplayDate(resetDate, locale) || resetDate;
  }, [progressResetInfo, tr, uiLang]);

  const progressResetMaxDate = String(progressResetInfo?.date_bounds?.max_date || '').trim();

  const buildAnalyticsScopeContextPayload = useCallback(() => {
    const unsafeChat = telegramApp?.initDataUnsafe?.chat || {};
    const chatType = String(
      unsafeChat?.type || telegramApp?.initDataUnsafe?.chat_type || webappChatType || ''
    ).trim().toLowerCase();
    const rawChatId = unsafeChat?.id ?? unsafeChat?.chat_id;
    const parsedChatId = Number.parseInt(String(rawChatId ?? ''), 10);
    const chatId = Number.isFinite(parsedChatId) ? parsedChatId : null;
    const chatTitle = String(unsafeChat?.title || unsafeChat?.username || '').trim();
    const context = {};
    if (chatType) context.chat_type = chatType;
    if (chatId !== null) context.chat_id = chatId;
    if (chatTitle) context.chat_title = chatTitle;
    return context;
  }, [telegramApp, webappChatType]);

  const normalizeAnalyticsScopeKeyFromPayload = useCallback((payload) => {
    const data = payload && typeof payload === 'object' ? payload : {};
    const effective = data.effective_scope && typeof data.effective_scope === 'object' ? data.effective_scope : {};
    const saved = data.saved_scope && typeof data.saved_scope === 'object' ? data.saved_scope : {};

    const fromEffectiveKey = parseAnalyticsScopeKey(effective.scope_key);
    if (fromEffectiveKey.scope_key !== 'personal' || String(effective.scope_kind || '').toLowerCase() === 'personal') {
      return fromEffectiveKey.scope_key;
    }
    const effectiveKind = String(effective.scope_kind || '').toLowerCase();
    const effectiveChatId = Number.parseInt(String(effective.scope_chat_id ?? ''), 10);
    if (effectiveKind === 'group' && Number.isFinite(effectiveChatId)) {
      return `group:${effectiveChatId}`;
    }
    const fromSavedKey = parseAnalyticsScopeKey(saved.scope_key);
    if (fromSavedKey.scope_key !== 'personal' || String(saved.scope_kind || '').toLowerCase() === 'personal') {
      return fromSavedKey.scope_key;
    }
    const savedKind = String(saved.scope_kind || '').toLowerCase();
    const savedChatId = Number.parseInt(String(saved.scope_chat_id ?? ''), 10);
    if (savedKind === 'group' && Number.isFinite(savedChatId)) {
      return `group:${savedChatId}`;
    }
    return 'personal';
  }, []);

  const applyAnalyticsScopePayload = useCallback((payload) => {
    const data = payload && typeof payload === 'object' ? payload : {};
    const availableGroups = Array.isArray(data.available_groups) ? data.available_groups : [];
    const normalizedPayload = { ...data, available_groups: availableGroups };
    setAnalyticsScopeData(normalizedPayload);
    setAnalyticsScopeKey(normalizeAnalyticsScopeKeyFromPayload(normalizedPayload));
  }, [normalizeAnalyticsScopeKeyFromPayload]);

  const loadAnalyticsScope = useCallback(async ({ silent = false } = {}) => {
    if (!initData) {
      if (!silent) setAnalyticsScopeError(initDataMissingMsg);
      return null;
    }
    if (analyticsScopeRequestRef.current) {
      return analyticsScopeRequestRef.current;
    }
    if (!silent) {
      setAnalyticsScopeLoading(true);
      setAnalyticsScopeError('');
    }
    const requestPromise = (async () => {
      try {
        const scopeContext = buildAnalyticsScopeContextPayload();
        const body = { initData };
        if (Object.keys(scopeContext).length > 0) body.scope_context = scopeContext;
        const response = await postJsonWithRetry('/api/webapp/analytics/scope', body);
        if (!response.ok) {
          throw new Error(await readApiError(response, 'Ошибка загрузки режима участия', 'Fehler beim Laden des Teilnahme-Modus'));
        }
        const data = await response.json();
        applyAnalyticsScopePayload(data);
        return data;
      } catch (error) {
        if (!silent) {
          const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить режим участия.', 'Teilnahme-Modus konnte nicht geladen werden.');
          setAnalyticsScopeError(`${tr('Ошибка режима участия', 'Fehler beim Teilnahme-Modus')}: ${friendly}`);
        }
        return null;
      } finally {
        analyticsScopeRequestRef.current = null;
        if (!silent) setAnalyticsScopeLoading(false);
      }
    })();
    analyticsScopeRequestRef.current = requestPromise;
    return requestPromise;
  }, [applyAnalyticsScopePayload, buildAnalyticsScopeContextPayload, initData, initDataMissingMsg, normalizeNetworkErrorMessage, postJsonWithRetry, readApiError, tr]);

  const loadProgressResetStatus = useCallback(async ({ silent = false } = {}) => {
    if (!initData) {
      if (!silent) setProgressResetError(initDataMissingMsg);
      return null;
    }
    if (!silent) {
      setProgressResetLoading(true);
      setProgressResetError('');
    }
    try {
      const response = await postJsonWithRetry('/api/webapp/progress-reset/status', { initData });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки reset progress', 'Fehler beim Laden des Reset-Status'));
      }
      const data = await response.json();
      setProgressResetInfo(data || null);
      return data;
    } catch (error) {
      if (!silent) {
        const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить reset progress.', 'Reset-Status konnte nicht geladen werden.');
        setProgressResetError(`${tr('Ошибка reset progress', 'Fehler beim Reset-Status')}: ${friendly}`);
      }
      return null;
    } finally {
      if (!silent) setProgressResetLoading(false);
    }
  }, [initData, initDataMissingMsg, normalizeNetworkErrorMessage, postJsonWithRetry, readApiError, tr]);

  const openProgressResetModal = useCallback(() => {
    const fallbackDate = String(progressResetInfo?.date_bounds?.today || '').trim();
    const currentResetDate = String(progressResetInfo?.reset?.reset_date || '').trim();
    setProgressResetDraftDate(currentResetDate || fallbackDate);
    setProgressResetError('');
    setProgressResetModalOpen(true);
  }, [progressResetInfo]);

  const resolveAnalyticsGranularity = useCallback((periodValue, rangeOverride = null) => {
    const resolvedRange = rangeOverride && typeof rangeOverride === 'object'
      ? { startDate: String(rangeOverride.startDate || '').trim(), endDate: String(rangeOverride.endDate || '').trim() }
      : { startDate: analyticsCustomStartDate, endDate: analyticsCustomEndDate };
    if (
      periodValue === 'calendar'
      && resolvedRange.startDate
      && resolvedRange.endDate
      && resolvedRange.startDate <= resolvedRange.endDate
    ) {
      const start = new Date(resolvedRange.startDate);
      const end = new Date(resolvedRange.endDate);
      const diffDays = Math.max(1, Math.round((end.getTime() - start.getTime()) / 86400000) + 1);
      if (diffDays <= 31) return 'day';
      if (diffDays <= 180) return 'week';
      return 'month';
    }
    switch (periodValue) {
      case 'day':
      case 'week':
        return 'day';
      case 'month':
        return 'week';
      case 'quarter':
      case 'half-year':
      case 'year':
      case 'all':
        return 'month';
      default:
        return 'day';
    }
  }, [analyticsCustomEndDate, analyticsCustomStartDate]);

  const ensureAnalyticsCalendarDraftRange = useCallback(() => {
    const fallback = buildDefaultAnalyticsCalendarRange();
    setAnalyticsCalendarDraftStartDate(analyticsCustomStartDate || fallback.startDate);
    setAnalyticsCalendarDraftEndDate(analyticsCustomEndDate || fallback.endDate);
  }, [analyticsCustomEndDate, analyticsCustomStartDate]);

  const openAnalyticsCalendar = useCallback(() => {
    ensureAnalyticsCalendarDraftRange();
    try {
      analyticsPeriodSelectRef.current?.blur?.();
      if (typeof document !== 'undefined' && document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
      }
    } catch (_error) {
      // ignore focus cleanup issues on mobile clients
    }
    window.setTimeout(() => {
      try {
        analyticsPeriodSelectRef.current?.blur?.();
        if (typeof document !== 'undefined' && document.activeElement instanceof HTMLElement) {
          document.activeElement.blur();
        }
      } catch (_error) {
        // ignore delayed focus cleanup issues on mobile clients
      }
      setAnalyticsPeriodSelectVersion((value) => value + 1);
    }, 0);
    setAnalyticsCalendarOpen(true);
  }, [ensureAnalyticsCalendarDraftRange]);

  const resolveAnalyticsLoadContext = useCallback((overridePeriod, overrideScopeKey, overrideRange = null) => {
    const period = overridePeriod || analyticsPeriod;
    const effectiveRange = overrideRange && typeof overrideRange === 'object'
      ? { startDate: String(overrideRange.startDate || '').trim(), endDate: String(overrideRange.endDate || '').trim() }
      : { startDate: analyticsCustomStartDate, endDate: analyticsCustomEndDate };
    const useCalendarRange = period === 'calendar';
    if (useCalendarRange && (!effectiveRange.startDate || !effectiveRange.endDate || effectiveRange.startDate > effectiveRange.endDate)) {
      throw new Error(tr('Выберите корректный диапазон дат для аналитики.', 'Waehle einen gueltigen Datumsbereich fuer die Analytik.'));
    }
    const granularity = resolveAnalyticsGranularity(period, effectiveRange);
    const scope = parseAnalyticsScopeKey(overrideScopeKey || analyticsScopeKey);
    const scopeContext = buildAnalyticsScopeContextPayload();
    const payloadBase = {
      initData,
      period,
      scope: scope.scope_key,
      scope_kind: scope.scope_kind,
      scope_chat_id: scope.scope_chat_id,
    };
    if (useCalendarRange) {
      payloadBase.start_date = effectiveRange.startDate;
      payloadBase.end_date = effectiveRange.endDate;
    }
    if (Object.keys(scopeContext).length > 0) payloadBase.scope_context = scopeContext;
    return {
      granularity,
      payloadBase,
      personalPayloadBase: { ...payloadBase, scope: 'personal', scope_kind: 'personal', scope_chat_id: null },
      scope,
    };
  }, [
    analyticsCustomEndDate,
    analyticsCustomStartDate,
    analyticsPeriod,
    analyticsScopeKey,
    buildAnalyticsScopeContextPayload,
    initData,
    resolveAnalyticsGranularity,
    tr,
  ]);

  const loadAnalyticsSummary = useCallback(async (overridePeriod, overrideScopeKey, overrideRange = null) => {
    if (!initData) {
      setAnalyticsError(initDataMissingMsg);
      return;
    }
    const requestId = analyticsSummaryRequestIdRef.current + 1;
    analyticsSummaryRequestIdRef.current = requestId;
    setAnalyticsLoading(true);
    setAnalyticsError('');
    try {
      const { personalPayloadBase } = resolveAnalyticsLoadContext(overridePeriod, undefined, overrideRange);
      const summaryResponse = await postJsonWithRetry('/api/webapp/analytics/summary', personalPayloadBase);
      if (!summaryResponse.ok) {
        throw new Error(await readApiError(summaryResponse, 'Ошибка загрузки аналитики', 'Fehler beim Laden der Analytik'));
      }
      const summaryData = await summaryResponse.json();
      if (analyticsSummaryRequestIdRef.current !== requestId) return;
      setAnalyticsSummary(summaryData.summary || null);
      setAnalyticsError('');
    } catch (error) {
      if (analyticsSummaryRequestIdRef.current !== requestId) return;
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить аналитику.', 'Analytik konnte nicht geladen werden.');
      setAnalyticsError(`${tr('Ошибка аналитики', 'Analytikfehler')}: ${friendly}`);
    } finally {
      if (analyticsSummaryRequestIdRef.current === requestId) setAnalyticsLoading(false);
    }
  }, [
    initData,
    initDataMissingMsg,
    normalizeNetworkErrorMessage,
    postJsonWithRetry,
    readApiError,
    resolveAnalyticsLoadContext,
    tr,
  ]);

  const loadAnalyticsTimeseries = useCallback(async (overridePeriod, overrideScopeKey, overrideRange = null) => {
    if (!initData) return null;
    const requestId = analyticsTimeseriesRequestIdRef.current + 1;
    analyticsTimeseriesRequestIdRef.current = requestId;
    try {
      const { granularity, personalPayloadBase } = resolveAnalyticsLoadContext(overridePeriod, overrideScopeKey, overrideRange);
      const response = await postJsonWithRetry('/api/webapp/analytics/timeseries', { ...personalPayloadBase, granularity });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки динамики', 'Fehler beim Laden des Verlaufs'));
      }
      const data = await response.json();
      if (analyticsTimeseriesRequestIdRef.current !== requestId) return null;
      setAnalyticsPoints(data.points || []);
      setAnalyticsError('');
      return data;
    } catch (error) {
      if (analyticsTimeseriesRequestIdRef.current !== requestId) return null;
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить динамику.', 'Verlauf konnte nicht geladen werden.');
      setAnalyticsError(`${tr('Ошибка аналитики', 'Analytikfehler')}: ${friendly}`);
      return null;
    }
  }, [initData, normalizeNetworkErrorMessage, postJsonWithRetry, readApiError, resolveAnalyticsLoadContext, tr]);

  const loadAnalyticsCompare = useCallback(async (overridePeriod, overrideScopeKey, overrideRange = null) => {
    if (!initData) return null;
    const requestId = analyticsCompareRequestIdRef.current + 1;
    analyticsCompareRequestIdRef.current = requestId;
    try {
      const { payloadBase, scope } = resolveAnalyticsLoadContext(overridePeriod, overrideScopeKey, overrideRange);
      const response = await postJsonWithRetry('/api/webapp/analytics/compare', { ...payloadBase, limit: 8 });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки сравнения', 'Fehler beim Laden des Vergleichs'));
      }
      const data = await response.json();
      if (analyticsCompareRequestIdRef.current !== requestId) return null;
      setAnalyticsCompare(data.items || []);
      setAnalyticsRank(scope.scope_kind === 'group' ? (data.self?.rank ?? null) : null);
      setAnalyticsError('');
      return data;
    } catch (error) {
      if (analyticsCompareRequestIdRef.current !== requestId) return null;
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить сравнение.', 'Vergleich konnte nicht geladen werden.');
      setAnalyticsError(`${tr('Ошибка аналитики', 'Analytikfehler')}: ${friendly}`);
      return null;
    }
  }, [initData, normalizeNetworkErrorMessage, postJsonWithRetry, readApiError, resolveAnalyticsLoadContext, tr]);

  const reloadVisibleAnalytics = useCallback(async (overridePeriod, overrideScopeKey, overrideRange = null) => {
    const tasks = [loadAnalyticsSummary(overridePeriod, overrideScopeKey, overrideRange)];
    if (analyticsTrendVisible) tasks.push(loadAnalyticsTimeseries(overridePeriod, overrideScopeKey, overrideRange));
    if (analyticsCompareVisible) tasks.push(loadAnalyticsCompare(overridePeriod, overrideScopeKey, overrideRange));
    await Promise.all(tasks);
  }, [analyticsCompareVisible, analyticsTrendVisible, loadAnalyticsCompare, loadAnalyticsSummary, loadAnalyticsTimeseries]);

  const applyAnalyticsCalendarRange = useCallback(() => {
    if (!analyticsCalendarDraftValid) {
      setAnalyticsError(tr('Выберите корректный диапазон дат для аналитики.', 'Waehle einen gueltigen Datumsbereich fuer die Analytik.'));
      return;
    }
    const nextRange = { startDate: analyticsCalendarDraftStartDate, endDate: analyticsCalendarDraftEndDate };
    setAnalyticsError('');
    setAnalyticsCustomStartDate(nextRange.startDate);
    setAnalyticsCustomEndDate(nextRange.endDate);
    setAnalyticsCalendarOpen(false);
    try {
      analyticsPeriodSelectRef.current?.blur?.();
      if (typeof document !== 'undefined' && document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
      }
    } catch (_error) {
      // ignore focus cleanup issues on mobile clients
    }
    window.setTimeout(() => {
      try {
        analyticsPeriodSelectRef.current?.blur?.();
        if (typeof document !== 'undefined' && document.activeElement instanceof HTMLElement) {
          document.activeElement.blur();
        }
      } catch (_error) {
        // ignore delayed focus cleanup issues on mobile clients
      }
      setAnalyticsPeriodSelectVersion((value) => value + 1);
    }, 0);
    void reloadVisibleAnalytics('calendar', analyticsScopeKey, nextRange);
  }, [
    analyticsCalendarDraftEndDate,
    analyticsCalendarDraftStartDate,
    analyticsCalendarDraftValid,
    analyticsScopeKey,
    reloadVisibleAnalytics,
    tr,
  ]);

  const handleAnalyticsScopeSelect = useCallback(async (nextScopeRaw) => {
    if (!initData) {
      setAnalyticsScopeError(initDataMissingMsg);
      return;
    }
    const nextScope = parseAnalyticsScopeKey(nextScopeRaw);
    if (nextScope.scope_key === analyticsScopeKey) return;
    const previousScopeKey = analyticsScopeKey;
    setAnalyticsScopeKey(nextScope.scope_key);
    setAnalyticsScopeSaving(true);
    setAnalyticsScopeError('');
    try {
      const scopeContext = buildAnalyticsScopeContextPayload();
      const body = {
        initData,
        scope_kind: nextScope.scope_kind,
        scope_chat_id: nextScope.scope_chat_id,
        scope: nextScope.scope_key,
      };
      if (Object.keys(scopeContext).length > 0) body.scope_context = scopeContext;
      const response = await postJsonWithRetry('/api/webapp/analytics/scope/select', body);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения режима участия', 'Fehler beim Speichern des Teilnahme-Modus'));
      }
      await loadAnalyticsScope({ silent: true });
    } catch (error) {
      setAnalyticsScopeKey(previousScopeKey);
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось сохранить режим участия.', 'Teilnahme-Modus konnte nicht gespeichert werden.');
      setAnalyticsScopeError(`${tr('Ошибка режима участия', 'Fehler beim Teilnahme-Modus')}: ${friendly}`);
    } finally {
      setAnalyticsScopeSaving(false);
    }
  }, [
    analyticsScopeKey,
    buildAnalyticsScopeContextPayload,
    initData,
    initDataMissingMsg,
    loadAnalyticsScope,
    normalizeNetworkErrorMessage,
    postJsonWithRetry,
    readApiError,
    tr,
  ]);

  const applyProgressReset = useCallback(async () => {
    if (!initData) {
      setProgressResetError(initDataMissingMsg);
      return;
    }
    const resetDate = String(progressResetDraftDate || '').trim();
    if (!resetDate) {
      setProgressResetError(tr('Выберите дату для новой точки отсчета.', 'Waehle ein Datum fuer den Neustart.'));
      return;
    }
    setProgressResetSaving(true);
    setProgressResetError('');
    try {
      const response = await postJsonWithRetry('/api/webapp/progress-reset/apply', {
        initData,
        reset_date: resetDate,
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения reset progress', 'Fehler beim Speichern des Reset-Status'));
      }
      const data = await response.json();
      setProgressResetInfo((prev) => ({
        ...(prev && typeof prev === 'object' ? prev : {}),
        ...(data && typeof data === 'object' ? data : {}),
      }));
      setProgressResetModalOpen(false);
      if (onProgressResetApplied) await onProgressResetApplied();
      await reloadVisibleAnalytics(undefined, analyticsScopeKey);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось сохранить reset progress.', 'Reset-Status konnte nicht gespeichert werden.');
      setProgressResetError(`${tr('Ошибка reset progress', 'Fehler beim Reset-Status')}: ${friendly}`);
    } finally {
      setProgressResetSaving(false);
    }
  }, [
    analyticsScopeKey,
    initData,
    initDataMissingMsg,
    normalizeNetworkErrorMessage,
    onProgressResetApplied,
    postJsonWithRetry,
    progressResetDraftDate,
    readApiError,
    reloadVisibleAnalytics,
    tr,
  ]);

  const analyticsScopeOptions = useMemo(() => {
    const options = [{ key: 'personal', label: tr('Только я', 'Nur ich') }];
    const groups = Array.isArray(analyticsScopeData?.available_groups) ? analyticsScopeData.available_groups : [];
    groups.forEach((item) => {
      const parsedId = Number.parseInt(String(item?.chat_id ?? ''), 10);
      if (!Number.isFinite(parsedId)) return;
      const title = String(item?.chat_title || '').trim();
      options.push({
        key: `group:${parsedId}`,
        label: title ? tr(`Группа: ${title}`, `Gruppe: ${title}`) : tr(`Группа #${parsedId}`, `Gruppe #${parsedId}`),
      });
    });
    if (String(analyticsScopeKey || '').startsWith('group:') && !options.some((item) => item.key === analyticsScopeKey)) {
      const chatId = String(analyticsScopeKey).slice('group:'.length).trim();
      const fallbackId = Number.parseInt(chatId, 10);
      if (Number.isFinite(fallbackId)) {
        options.push({ key: `group:${fallbackId}`, label: tr(`Группа #${fallbackId}`, `Gruppe #${fallbackId}`) });
      }
    }
    return options;
  }, [analyticsScopeData, analyticsScopeKey, tr]);

  const analyticsScopeStatusText = useMemo(() => {
    if (String(analyticsScopeKey || '').startsWith('group:')) {
      const selectedOption = analyticsScopeOptions.find((item) => item.key === analyticsScopeKey);
      const selectedLabel = String(selectedOption?.label || '')
        .replace(/^Группа:\s*/i, '')
        .replace(/^Gruppe:\s*/i, '')
        .trim();
      return selectedLabel
        ? tr(
          `Верхняя динамика показывает только ваши попытки. Нижнее сравнение сейчас по группе: ${selectedLabel}`,
          `Die obere Dynamik zeigt nur deine Versuche. Der untere Vergleich ist aktuell fuer die Gruppe: ${selectedLabel}`
        )
        : tr(
          'Верхняя динамика показывает только ваши попытки. Нижнее сравнение сейчас по группе.',
          'Die obere Dynamik zeigt nur deine Versuche. Der untere Vergleich ist aktuell fuer die Gruppe.'
        );
    }
    return tr(
      'Верхняя динамика показывает только ваши попытки. Нижнее сравнение сейчас тоже персональное.',
      'Die obere Dynamik zeigt nur deine Versuche. Der untere Vergleich ist aktuell ebenfalls persoenlich.'
    );
  }, [analyticsScopeKey, analyticsScopeOptions, tr]);

  const analyticsScopeSelectorRequired = Boolean(analyticsScopeData?.selector?.required);

  const analyticsCompareInsight = useMemo(() => {
    const items = Array.isArray(analyticsCompare) ? analyticsCompare : [];
    const scopeKey = String(analyticsScopeKey || '').trim().toLowerCase();
    const hasNonZeroFinalScore = items.some((item) => Number(item?.final_score || 0) !== 0);
    const maxFinalScore = items.reduce((maxValue, item) => Math.max(maxValue, Number(item?.final_score || 0)), 0);
    if (scopeKey === 'personal') {
      return tr(
        'Нижняя диаграмма сравнивает участников выбранного режима. Сейчас выбран персональный режим, поэтому здесь только ваши данные.',
        'Das untere Diagramm vergleicht die Teilnehmenden des gewaehlten Modus. Aktuell ist der persoenliche Modus aktiv, deshalb siehst du hier nur deine Daten.'
      );
    }
    if (items.length <= 1) {
      return tr(
        'Для выбранной группы пока недостаточно участников или данных для сравнения.',
        'Fuer die gewaehlte Gruppe gibt es aktuell noch zu wenige Teilnehmende oder Daten fuer einen Vergleich.'
      );
    }
    if (!hasNonZeroFinalScore) {
      return tr(
        'Сравнение есть, но итоговые баллы сейчас равны 0, поэтому столбцы почти не видны.',
        'Der Vergleich ist vorhanden, aber die Gesamtscores liegen aktuell bei 0, deshalb sind die Balken fast unsichtbar.'
      );
    }
    if (maxFinalScore <= 0) {
      return tr(
        'Сравнение есть, но итоговые баллы в этом периоде сейчас отрицательные, поэтому часть столбцов уходит влево от нуля.',
        'Der Vergleich ist vorhanden, aber die Gesamtscores sind in diesem Zeitraum aktuell negativ, deshalb laufen einige Balken links von der Nullachse.'
      );
    }
    return tr(
      'Нижняя диаграмма показывает сравнение участников по итоговому баллу за выбранный период. Под именем участника мелким шрифтом указано, с какой даты его данные реально учитываются в сравнении.',
      'Das untere Diagramm zeigt den Vergleich der Teilnehmenden nach Gesamtscore fuer den gewaehlten Zeitraum. Unter dem Namen steht in kleiner Schrift, ab welchem Datum die Daten dieser Person im Vergleich tatsaechlich beruecksichtigt werden.'
    );
  }, [analyticsCompare, analyticsScopeKey, tr]);

  const analyticsFinalScoreFormulaText = useMemo(() => {
    const formula = analyticsSummary?.final_score_formula;
    if (formula?.expression) {
      return tr(
        'Формула итогового результата: средний балл - среднее время на перевод × 0.5 - дни без практики × 0.5. Значение не ограничивается ни снизу, ни сверху: итог может быть отрицательным или выше 100.',
        'Formel fuer den Gesamtscore: Durchschnittsbewertung - durchschnittliche Zeit pro Uebersetzung × 0.5 - Tage ohne Praxis × 0.5. Der Wert ist weder nach unten noch nach oben begrenzt: Er kann negativ sein oder ueber 100 liegen.'
      );
    }
    return tr(
      'Формула итогового результата: средний балл - среднее время на перевод × 0.5 - дни без практики × 0.5. Значение не ограничивается ни снизу, ни сверху: итог может быть отрицательным или выше 100.',
      'Formel fuer den Gesamtscore: Durchschnittsbewertung - durchschnittliche Zeit pro Uebersetzung × 0.5 - Tage ohne Praxis × 0.5. Der Wert ist weder nach unten noch nach oben begrenzt: Er kann negativ sein oder ueber 100 liegen.'
    );
  }, [analyticsSummary, tr]);

  const analyticsCardGroups = useMemo(() => {
    if (!analyticsSummary) return [];
    const coveredSentences = Number(analyticsSummary.covered_sentences ?? analyticsSummary.total_translations ?? 0);
    const assignedSentences = Number(analyticsSummary.assigned_sentences ?? 0);
    const successRate = Number(analyticsSummary.success_rate ?? 0);
    const completionRate = Number(analyticsSummary.completion_rate ?? 0);
    const avgScore = Number(analyticsSummary.avg_score ?? 0);
    const totalTimeMin = Math.round(Number(analyticsSummary.total_time_min ?? 0) * 10) / 10;
    const avgTimeMin = Number(analyticsSummary.avg_time_min ?? 0);
    const missedSentences = Number(analyticsSummary.missed_sentences ?? 0);
    const missedDays = Number(analyticsSummary.missed_days ?? 0);
    const finalScore = Number(analyticsSummary.final_score ?? 0);
    return [
      {
        key: 'volume',
        title: tr('Что сделано', 'Was erledigt wurde'),
        items: [
          { key: 'translations', label: tr('Переведено предложений', 'Uebersetzte Saetze'), value: `${coveredSentences}`, hint: tr('Сколько предложений вы реально закрыли за период.', 'Wie viele Saetze du im Zeitraum wirklich abgeschlossen hast.') },
          { key: 'assigned', label: tr('Назначено предложений', 'Zugewiesene Saetze'), value: `${assignedSentences}`, hint: tr('Сколько предложений было запланировано на выбранный период аналитики.', 'Wie viele Saetze fuer den gewaehlten Analysezeitraum geplant waren.') },
          { key: 'completion', label: tr('Закрыто по плану', 'Vom Plan erledigt'), value: `${completionRate}%`, hint: assignedSentences > 0 ? tr(`${coveredSentences} из ${assignedSentences} запланированных предложений.`, `${coveredSentences} von ${assignedSentences} geplanten Saetzen.`) : tr('Запланированных предложений в этом периоде не было.', 'In diesem Zeitraum gab es keine geplanten Saetze.') },
          { key: 'missed', label: tr('Пропущено заданий', 'Verpasste Aufgaben'), value: `${missedSentences}`, hint: tr('Сколько запланированных предложений осталось незакрытыми.', 'Wie viele geplante Saetze offen geblieben sind.') },
        ],
      },
      {
        key: 'quality',
        title: tr('Качество ответов', 'Qualitaet der Antworten'),
        items: [
          { key: 'success', label: tr('Удачных переводов', 'Gute Uebersetzungen'), value: `${successRate}%`, hint: tr('Доля переводов, которые прошли успешно.', 'Anteil der Uebersetzungen, die erfolgreich waren.') },
          { key: 'avg-score', label: tr('Средняя оценка ответа', 'Durchschnittliche Bewertung'), value: `${avgScore}`, hint: tr('Средний балл ваших ответов по шкале до 100.', 'Durchschnittspunktzahl deiner Antworten auf einer Skala bis 100.') },
          { key: 'total-time', label: tr('Общее время на переводы', 'Gesamtzeit fuer Uebersetzungen'), value: `${totalTimeMin} ${tr('мин', 'Min')}`, hint: tr('Сколько минут суммарно ушло на переводы за выбранный период.', 'Wie viele Minuten insgesamt fuer Uebersetzungen im gewaehlten Zeitraum draufgegangen sind.') },
          { key: 'final-score', label: tr('Общий результат', 'Gesamtergebnis'), value: `${finalScore}`, hint: analyticsFinalScoreFormulaText },
        ],
      },
      {
        key: 'rhythm',
        title: tr('Регулярность', 'Regelmaessigkeit'),
        items: [
          { key: 'avg-time', label: tr('Среднее время на перевод', 'Durchschnittszeit pro Uebersetzung'), value: `${avgTimeMin} ${tr('мин', 'Min')}`, hint: tr('Сколько в среднем уходило времени на одно выполненное предложение.', 'Wie viel Zeit du im Schnitt pro erledigtem Satz gebraucht hast.') },
          { key: 'missed-days', label: tr('Дней без практики', 'Tage ohne Praxis'), value: `${missedDays}`, hint: tr('Сколько активных дней прошло без переводов.', 'Wie viele aktive Tage ohne Uebersetzungen vergangen sind.') },
        ],
      },
    ];
  }, [analyticsFinalScoreFormulaText, analyticsSummary, tr]);

  // Bootstrap: set ready + load scope/progress on analytics section entry
  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setAnalyticsBootstrapReady(false);
      return;
    }
    if (!flashcardsOnly && selectedSections.has('analytics')) {
      setAnalyticsBootstrapReady(true);
      void loadAnalyticsScope();
      void loadProgressResetStatus();
      return undefined;
    }
    setAnalyticsBootstrapReady(false);
    return undefined;
  }, [
    initData,
    isWebAppMode,
    selectedSections,
    flashcardsOnly,
    webappChatType,
    languageProfile?.learning_language,
    languageProfile?.native_language,
  ]);

  // Load summary when bootstrap ready + period + calendar valid
  useEffect(() => {
    if (!isWebAppMode || !initData || !analyticsBootstrapReady) return;
    if (!flashcardsOnly && selectedSections.has('analytics')) {
      if (analyticsPeriod === 'calendar' && !analyticsCalendarRangeValid) return;
      void loadAnalyticsSummary(undefined, undefined);
    }
  }, [
    initData,
    isWebAppMode,
    analyticsBootstrapReady,
    analyticsPeriod,
    analyticsCalendarRangeValid,
    selectedSections,
    flashcardsOnly,
    loadAnalyticsSummary,
  ]);

  // IntersectionObserver for trend/compare visibility
  useEffect(() => {
    const analyticsSectionVisible = !flashcardsOnly && selectedSections.has('analytics');
    if (!analyticsSectionVisible || !analyticsBootstrapReady || typeof window === 'undefined') {
      setAnalyticsTrendVisible(false);
      setAnalyticsCompareVisible(false);
      return undefined;
    }
    const trendNode = analyticsTrendRef.current;
    const compareNode = analyticsCompareRef.current;
    if (!trendNode && !compareNode) return undefined;
    const observer = new window.IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        if (trendNode && entry.target === trendNode) setAnalyticsTrendVisible(true);
        if (compareNode && entry.target === compareNode) setAnalyticsCompareVisible(true);
      });
    }, { threshold: 0.6 });
    if (trendNode) observer.observe(trendNode);
    if (compareNode) observer.observe(compareNode);
    return () => { observer.disconnect(); };
  }, [analyticsBootstrapReady, flashcardsOnly, selectedSections]);

  // Load timeseries when trend visible + summary ready
  useEffect(() => {
    if (!isWebAppMode || !initData || !analyticsBootstrapReady || !analyticsTrendVisible || !analyticsSummary) return;
    if (!flashcardsOnly && selectedSections.has('analytics')) {
      if (analyticsPeriod === 'calendar' && !analyticsCalendarRangeValid) return;
      void loadAnalyticsTimeseries(undefined, analyticsScopeKey);
    }
  }, [
    initData,
    isWebAppMode,
    analyticsBootstrapReady,
    analyticsTrendVisible,
    analyticsSummary,
    analyticsPeriod,
    analyticsScopeKey,
    analyticsCalendarRangeValid,
    selectedSections,
    flashcardsOnly,
    loadAnalyticsTimeseries,
  ]);

  // Load compare when compare visible + summary ready
  useEffect(() => {
    if (!isWebAppMode || !initData || !analyticsBootstrapReady || !analyticsCompareVisible || !analyticsSummary) return;
    if (!flashcardsOnly && selectedSections.has('analytics')) {
      if (analyticsPeriod === 'calendar' && !analyticsCalendarRangeValid) return;
      void loadAnalyticsCompare(undefined, analyticsScopeKey);
    }
  }, [
    initData,
    isWebAppMode,
    analyticsBootstrapReady,
    analyticsCompareVisible,
    analyticsSummary,
    analyticsPeriod,
    analyticsScopeKey,
    analyticsCalendarRangeValid,
    selectedSections,
    flashcardsOnly,
    loadAnalyticsCompare,
  ]);

  // Calendar click-outside / Escape handler
  useEffect(() => {
    if (!analyticsCalendarOpen) return undefined;
    const handlePointerDown = (event) => {
      const panel = analyticsCalendarRef.current;
      if (!panel || panel.contains(event.target)) return;
      setAnalyticsCalendarOpen(false);
    };
    const handleKeyDown = (event) => {
      if (event.key === 'Escape') setAnalyticsCalendarOpen(false);
    };
    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('touchstart', handlePointerDown, { passive: true });
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('touchstart', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [analyticsCalendarOpen]);

  // Reset progressReset state when not in webapp mode
  useEffect(() => {
    if (isWebAppMode && initData) return;
    setProgressResetInfo(null);
    setProgressResetError('');
    setProgressResetLoading(false);
    setProgressResetSaving(false);
    setProgressResetModalOpen(false);
    setProgressResetDraftDate('');
  }, [initData, isWebAppMode]);

  // ECharts: trend chart
  useEffect(() => {
    if (!analyticsTrendRef.current) return;
    let disposed = false;
    let chart = null;
    const isLightTheme = themeMode === 'light';
    const chartTextColor = isLightTheme ? '#5a4a39' : '#c7d2f1';
    const chartLegendColor = isLightTheme ? '#2f271f' : '#dbe7ff';
    const chartAxisColor = isLightTheme ? 'rgba(130, 101, 67, 0.5)' : '#2f3f5f';
    const chartSplitLineColor = isLightTheme ? 'rgba(130, 101, 67, 0.18)' : 'rgba(255,255,255,0.08)';
    const tooltipBackground = isLightTheme ? 'rgba(255, 248, 238, 0.96)' : 'rgba(15, 23, 42, 0.92)';
    const tooltipBorder = isLightTheme ? 'rgba(171, 126, 72, 0.5)' : 'rgba(148, 163, 184, 0.28)';
    const tooltipTextColor = isLightTheme ? '#1f1a14' : '#e2e8f0';
    const labels = analyticsPoints.map((item) => item.period_start);
    const success = analyticsPoints.map((item) => item.successful_translations || 0);
    const fail = analyticsPoints.map((item) => item.unsuccessful_translations || 0);
    const avgScore = analyticsPoints.map((item) => item.avg_score || 0);
    const avgTime = analyticsPoints.map((item) => item.avg_time_min || 0);
    const option = {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow',
          shadowStyle: { color: isLightTheme ? 'rgba(207, 157, 99, 0.16)' : 'rgba(148, 163, 184, 0.14)' },
        },
        backgroundColor: tooltipBackground,
        borderColor: tooltipBorder,
        borderWidth: 1,
        textStyle: { color: tooltipTextColor },
        formatter: (params) => {
          const map = {};
          params.forEach((entry) => { map[entry.seriesName] = entry.value; });
          const index = params[0]?.dataIndex ?? 0;
          const timeValue = avgTime[index] ?? 0;
          return `
            <strong>${labels[index] || ''}</strong><br/>
            ${tr('Удачных переводов', 'Gute Uebersetzungen')}: ${map[tr('Успешно', 'Erfolgreich')] ?? 0}<br/>
            ${tr('Нужно исправить', 'Zu verbessern')}: ${map[tr('Нужно доработать', 'Verbessern')] ?? 0}<br/>
            ${tr('Средняя оценка', 'Durchschnittliche Bewertung')}: ${map[tr('Средний балл', 'Durchschnitt')] ?? 0}<br/>
            ${tr('Среднее время', 'Durchschnittszeit')}: ${timeValue} ${tr('мин', 'Min')}
          `;
        },
      },
      legend: {
        data: [tr('Успешно', 'Erfolgreich'), tr('Нужно доработать', 'Verbessern'), tr('Средний балл', 'Durchschnitt')],
        textStyle: { color: chartLegendColor },
      },
      grid: { left: 32, right: 32, top: 40, bottom: 40 },
      xAxis: {
        type: 'category',
        data: labels,
        axisLine: { lineStyle: { color: chartAxisColor } },
        axisLabel: { color: chartTextColor },
      },
      yAxis: [
        { type: 'value', name: tr('Переводы', 'Uebersetzungen'), nameTextStyle: { color: chartTextColor }, axisLabel: { color: chartTextColor }, splitLine: { lineStyle: { color: chartSplitLineColor } } },
        { type: 'value', name: tr('Баллы', 'Punkte'), min: 0, max: 100, nameTextStyle: { color: chartTextColor }, axisLabel: { color: chartTextColor }, splitLine: { show: false } },
      ],
      series: [
        { name: tr('Успешно', 'Erfolgreich'), type: 'bar', stack: 'total', data: success, itemStyle: { color: '#06d6a0' }, barWidth: 22 },
        { name: tr('Нужно доработать', 'Verbessern'), type: 'bar', stack: 'total', data: fail, itemStyle: { color: '#ff6b6b' }, barWidth: 22 },
        { name: tr('Средний балл', 'Durchschnitt'), type: 'line', yAxisIndex: 1, data: avgScore, smooth: true, symbol: 'circle', symbolSize: 8, itemStyle: { color: '#ffd166' }, lineStyle: { width: 3 } },
      ],
    };
    (async () => {
      const { echarts: echartsModule } = await import('../utils/echartsRuntime');
      if (disposed || !analyticsTrendRef.current) return;
      chart = echartsModule.init(analyticsTrendRef.current);
      chart.setOption(option);
    })().catch(() => {
      // analytics cards remain usable without chart bootstrap
    });
    return () => {
      disposed = true;
      if (chart) chart.dispose();
    };
  }, [analyticsPoints, analyticsPeriod, themeMode]);

  // ECharts: compare chart
  useEffect(() => {
    if (!analyticsCompareRef.current || !analyticsCompareWordsRef.current || !analyticsCompareErrorsRef.current) return;
    let disposed = false;
    const charts = [];
    const isLightTheme = themeMode === 'light';
    const chartTextColor = isLightTheme ? '#5a4a39' : '#c7d2f1';
    const chartSplitLineColor = isLightTheme ? 'rgba(130, 101, 67, 0.18)' : 'rgba(255,255,255,0.08)';
    const tooltipBackground = isLightTheme ? 'rgba(255, 248, 238, 0.96)' : 'rgba(15, 23, 42, 0.92)';
    const tooltipBorder = isLightTheme ? 'rgba(171, 126, 72, 0.5)' : 'rgba(148, 163, 184, 0.28)';
    const tooltipTextColor = isLightTheme ? '#1f1a14' : '#e2e8f0';
    const secondaryLabelColor = isLightTheme ? 'rgba(90, 74, 57, 0.72)' : 'rgba(199, 210, 241, 0.72)';
    const selfId = webappUser?.id;
    const names = analyticsCompare.map((item) => item.username);
    const formatCompareStartDateLabel = (rawValue) => {
      const normalized = String(rawValue || '').trim();
      if (!normalized) return '';
      const formatted = formatAnalyticsCalendarDisplayDate(normalized, uiLang === 'de' ? 'de-AT' : 'ru-RU') || normalized;
      return uiLang === 'de' ? `seit ${formatted}` : `с ${formatted}`;
    };
    const formatMetricValue = (metricKey, value) => {
      const numeric = Number(value || 0);
      if (!Number.isFinite(numeric)) return '0';
      if (metricKey === 'learned_words') return `${Math.round(numeric)}`;
      if (metricKey === 'errors_per_sentence') return numeric.toFixed(2);
      return `${Math.round(numeric * 100) / 100}`;
    };
    const buildCompareData = (metricKey, selfColor, peerColor) => analyticsCompare.map((item) => ({
      value: Number(item?.[metricKey] || 0),
      itemStyle: { color: item.user_id === selfId ? selfColor : peerColor },
    }));
    const buildCompareTooltip = (metricKey, label) => (params) => {
      const item = analyticsCompare[params.dataIndex];
      if (!item) return '';
      return `
        <strong>${item.username}</strong><br/>
        ${tr('Данные в сравнении с', 'Vergleichsdaten seit')}: ${formatCompareStartDateLabel(item.effective_compare_start_date) || '—'}<br/>
        ${label}: ${formatMetricValue(metricKey, item?.[metricKey])}<br/>
        ${tr('Общий результат', 'Gesamtergebnis')}: ${formatMetricValue('final_score', item?.final_score)}<br/>
        ${tr('Слова', 'Woerter')}: ${formatMetricValue('learned_words', item?.learned_words)}<br/>
        ${tr('Ошибок на 1 предложение', 'Fehler pro Satz')}: ${formatMetricValue('errors_per_sentence', item?.errors_per_sentence)}<br/>
        ${tr('Переведено', 'Uebersetzt')}: ${item.total_translations}<br/>
        ${tr('Попытки', 'Versuche')}: ${item.translation_attempts ?? item.total_translations ?? 0}
      `;
    };
    const buildCompareOption = ({ metricKey, label, selfColor, peerColor }) => ({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'item',
        backgroundColor: tooltipBackground,
        borderColor: tooltipBorder,
        borderWidth: 1,
        textStyle: { color: tooltipTextColor },
        formatter: buildCompareTooltip(metricKey, label),
      },
      grid: { left: 20, right: 20, top: 20, bottom: 20, containLabel: true },
      xAxis: {
        type: 'value',
        axisLabel: { color: chartTextColor },
        splitLine: { lineStyle: { color: chartSplitLineColor } },
      },
      yAxis: {
        type: 'category',
        data: names,
        axisLabel: {
          color: chartTextColor,
          formatter: (_value, index) => {
            const item = analyticsCompare[index];
            const username = String(item?.username || '').trim() || 'Unknown';
            const compareStartLabel = formatCompareStartDateLabel(item?.effective_compare_start_date);
            if (!compareStartLabel) return `{name|${username}}`;
            return `{name|${username}}\n{meta|${compareStartLabel}}`;
          },
          rich: {
            name: { color: chartTextColor, fontSize: 13, fontWeight: 600, lineHeight: 18 },
            meta: { color: secondaryLabelColor, fontSize: 11, lineHeight: 14 },
          },
        },
        inverse: true,
      },
      series: [{
        type: 'bar',
        data: buildCompareData(metricKey, selfColor, peerColor),
        barWidth: 18,
        borderRadius: [8, 8, 8, 8],
        label: {
          show: true,
          position: 'right',
          color: chartTextColor,
          formatter: (params) => {
            const item = analyticsCompare[params.dataIndex];
            return formatMetricValue(metricKey, item?.[metricKey]);
          },
        },
      }],
    });
    (async () => {
      const { echarts: echartsModule } = await import('../utils/echartsRuntime');
      if (disposed) return;
      const chartConfigs = [
        { ref: analyticsCompareRef, option: buildCompareOption({ metricKey: 'final_score', label: tr('Общий результат', 'Gesamtergebnis'), selfColor: '#ffd166', peerColor: '#5ddcff' }) },
        { ref: analyticsCompareWordsRef, option: buildCompareOption({ metricKey: 'learned_words', label: tr('Слова', 'Woerter'), selfColor: '#ff9f1c', peerColor: '#2ec4b6' }) },
        { ref: analyticsCompareErrorsRef, option: buildCompareOption({ metricKey: 'errors_per_sentence', label: tr('Ошибок на 1 предложение', 'Fehler pro Satz'), selfColor: '#9b5de5', peerColor: '#f15bb5' }) },
      ];
      chartConfigs.forEach(({ ref, option }) => {
        if (!ref.current) return;
        const chart = echartsModule.init(ref.current);
        chart.setOption(option);
        charts.push(chart);
      });
    })().catch(() => {
      // comparison cards remain usable even if chart import fails
    });
    return () => {
      disposed = true;
      charts.forEach((chart) => {
        try { chart.dispose(); } catch (_error) { /* ignore */ }
      });
    };
  }, [analyticsCompare, webappUser, themeMode, tr, uiLang]);

  const contextValue = {
    // state
    analyticsPeriod, setAnalyticsPeriod,
    analyticsPeriodSelectVersion,
    analyticsCalendarOpen, setAnalyticsCalendarOpen,
    analyticsCustomStartDate, setAnalyticsCustomStartDate,
    analyticsCustomEndDate, setAnalyticsCustomEndDate,
    analyticsCalendarDraftStartDate, setAnalyticsCalendarDraftStartDate,
    analyticsCalendarDraftEndDate, setAnalyticsCalendarDraftEndDate,
    analyticsLoading,
    analyticsError,
    analyticsSummary,
    analyticsPoints,
    analyticsCompare,
    analyticsRank,
    analyticsScopeData,
    analyticsScopeKey,
    analyticsBootstrapReady,
    analyticsScopeLoading,
    analyticsScopeSaving,
    analyticsScopeError,
    analyticsTrendVisible,
    analyticsCompareVisible,
    progressResetInfo, setProgressResetInfo,
    progressResetLoading,
    progressResetSaving,
    progressResetError,
    progressResetModalOpen, setProgressResetModalOpen,
    progressResetDraftDate, setProgressResetDraftDate,
    // computed
    analyticsCalendarRangeValid,
    analyticsCalendarDraftValid,
    analyticsScopeSelectorRequired,
    progressResetMaxDate,
    // memos
    analyticsCalendarLabel,
    progressResetDateLabel,
    analyticsScopeOptions,
    analyticsScopeStatusText,
    analyticsCompareInsight,
    analyticsFinalScoreFormulaText,
    analyticsCardGroups,
    // refs (for DOM attachment in AnalyticsSection)
    analyticsCalendarRef,
    analyticsPeriodSelectRef,
    analyticsTrendRef,
    analyticsCompareRef,
    analyticsCompareWordsRef,
    analyticsCompareErrorsRef,
    // functions
    buildAnalyticsScopeContextPayload,
    parseAnalyticsScopeKey,
    normalizeAnalyticsScopeKeyFromPayload,
    applyAnalyticsScopePayload,
    openAnalyticsCalendar,
    applyAnalyticsCalendarRange,
    handleAnalyticsScopeSelect,
    reloadVisibleAnalytics,
    loadAnalyticsScope,
    loadProgressResetStatus,
    openProgressResetModal,
    applyProgressReset,
  };

  return (
    <AnalyticsContext.Provider value={contextValue}>
      {children}
    </AnalyticsContext.Provider>
  );
}
