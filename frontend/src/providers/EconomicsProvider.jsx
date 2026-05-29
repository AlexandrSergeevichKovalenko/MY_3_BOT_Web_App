import { createContext, useContext, useEffect, useMemo, useState } from 'react';

const ECONOMICS_PERIOD_STORAGE_KEY = 'dds_economics_period_v2';
const ECONOMICS_PROVIDER_STORAGE_KEY = 'dds_economics_provider_v2';
const ECONOMICS_FORECAST_USERS_STORAGE_KEY = 'dds_economics_forecast_users_v1';
const ECONOMICS_RAILWAY_APP_RAM_STORAGE_KEY = 'dds_economics_railway_app_ram_v1';
const ECONOMICS_RAILWAY_APP_CPU_STORAGE_KEY = 'dds_economics_railway_app_cpu_v1';
const ECONOMICS_RAILWAY_POSTGRES_RAM_STORAGE_KEY = 'dds_economics_railway_postgres_ram_v1';
const ECONOMICS_RAILWAY_POSTGRES_VOLUME_STORAGE_KEY = 'dds_economics_railway_postgres_volume_v1';
const ECONOMICS_RAILWAY_REDIS_RAM_STORAGE_KEY = 'dds_economics_railway_redis_ram_v1';
const ECONOMICS_RAILWAY_EGRESS_STORAGE_KEY = 'dds_economics_railway_egress_v1';
const ECONOMICS_PERIOD_OPTIONS = new Set(['day', 'week', 'month', 'quarter', 'half-year', 'year', 'all']);

function readStoredDraftValue(key, fallback = '') {
  if (typeof window === 'undefined') return fallback;
  try {
    const raw = String(window.localStorage.getItem(key) || '').trim();
    return raw || fallback;
  } catch (_error) {
    return fallback;
  }
}

function writeStoredValue(key, value) {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(key, String(value || ''));
  } catch (_error) {
    // ignore storage failures
  }
}

function readStoredEconomicsPeriod() {
  if (typeof window === 'undefined') return 'month';
  try {
    const raw = String(window.localStorage.getItem(ECONOMICS_PERIOD_STORAGE_KEY) || '').trim().toLowerCase();
    return ECONOMICS_PERIOD_OPTIONS.has(raw) ? raw : 'month';
  } catch (_error) {
    return 'month';
  }
}

function readStoredEconomicsProvider() {
  if (typeof window === 'undefined') return 'all';
  try {
    const raw = String(window.localStorage.getItem(ECONOMICS_PROVIDER_STORAGE_KEY) || '').trim().toLowerCase();
    return raw || 'all';
  } catch (_error) {
    return 'all';
  }
}

function readStoredEconomicsForecastUsers() {
  if (typeof window === 'undefined') return '100';
  try {
    const raw = String(window.localStorage.getItem(ECONOMICS_FORECAST_USERS_STORAGE_KEY) || '').trim();
    return raw || '100';
  } catch (_error) {
    return '100';
  }
}

export function formatEconomicsProviderLabel(provider) {
  const value = String(provider || '').trim().toLowerCase();
  const labels = {
    all: 'All Providers',
    openai: 'OpenAI',
    google_tts: 'Google TTS',
    google_translate: 'Google Translate',
    deepl_free: 'DeepL',
    azure_translator: 'Azure Translator',
    livekit: 'LiveKit',
    cloudflare_r2_class_a: 'Cloudflare R2 Class A',
    cloudflare_r2_class_b: 'Cloudflare R2 Class B',
    cloudflare_r2_storage: 'Cloudflare R2 Storage',
    youtube_api: 'YouTube API',
    youtube_proxy: 'YouTube Proxy',
    perplexity: 'Perplexity',
    stripe: 'Stripe',
    agent_tts: 'Agent TTS',
    offline_tts: 'Offline TTS',
    app_internal: 'App Internal',
    libretranslate: 'LibreTranslate',
    mymemory: 'MyMemory',
    argos_offline: 'Argos Offline',
  };
  if (labels[value]) return labels[value];
  if (!value) return 'Unassigned';
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function formatEconomicsUnitsLabel(unitsType, uiLang) {
  const value = String(unitsType || '').trim().toLowerCase();
  const map = {
    tokens_in: uiLang === 'de' ? 'Input-Tokens' : 'tokens in',
    tokens_out: uiLang === 'de' ? 'Output-Tokens' : 'tokens out',
    chars: uiLang === 'de' ? 'Zeichen' : 'chars',
    requests: uiLang === 'de' ? 'Anfragen' : 'requests',
    audio_minutes: uiLang === 'de' ? 'Audiominuten' : 'audio min',
    youtube_quota_units: uiLang === 'de' ? 'YouTube-Quota' : 'yt quota',
    operations: uiLang === 'de' ? 'Operationen' : 'ops',
    mb_month: uiLang === 'de' ? 'MB/Monat' : 'MB/month',
  };
  return map[value] || value || 'units';
}

export function formatEconomicsCompactNumber(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return '0';
  if (Math.abs(numeric) >= 100) return numeric.toFixed(0);
  if (Math.abs(numeric) >= 10) return numeric.toFixed(1);
  if (Math.abs(numeric) >= 1) return numeric.toFixed(2);
  return numeric.toFixed(3);
}

export const EconomicsContext = createContext(null);

export function EconomicsProvider({
  initData,
  isWebAppMode,
  selectedSections,
  flashcardsOnly,
  tr,
  readApiError,
  children,
}) {
  const [economicsPeriod, setEconomicsPeriod] = useState(() => readStoredEconomicsPeriod());
  const [economicsProvider, setEconomicsProvider] = useState(() => readStoredEconomicsProvider());
  const [economicsForecastUsersDraft, setEconomicsForecastUsersDraft] = useState(() => readStoredEconomicsForecastUsers());
  const [economicsRailwayAppRamDraft, setEconomicsRailwayAppRamDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_APP_RAM_STORAGE_KEY, readStoredDraftValue('dds_economics_railway_ram_v1', '')));
  const [economicsRailwayAppCpuDraft, setEconomicsRailwayAppCpuDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_APP_CPU_STORAGE_KEY, readStoredDraftValue('dds_economics_railway_cpu_v1', '')));
  const [economicsRailwayPostgresRamDraft, setEconomicsRailwayPostgresRamDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_POSTGRES_RAM_STORAGE_KEY, ''));
  const [economicsRailwayPostgresVolumeDraft, setEconomicsRailwayPostgresVolumeDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_POSTGRES_VOLUME_STORAGE_KEY, readStoredDraftValue('dds_economics_railway_volume_v1', '')));
  const [economicsRailwayRedisRamDraft, setEconomicsRailwayRedisRamDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_REDIS_RAM_STORAGE_KEY, ''));
  const [economicsRailwayEgressDraft, setEconomicsRailwayEgressDraft] = useState(() => readStoredDraftValue(ECONOMICS_RAILWAY_EGRESS_STORAGE_KEY, ''));
  const [economicsLoading, setEconomicsLoading] = useState(false);
  const [economicsError, setEconomicsError] = useState('');
  const [economicsSummary, setEconomicsSummary] = useState(null);

  const loadEconomics = async (overridePeriod, overrideProvider) => {
    if (!initData) {
      setEconomicsError(tr('initData не найдено. Откройте Web App внутри Telegram.', 'initData nicht gefunden. Oeffne die Web App in Telegram.'));
      return;
    }
    const period = overridePeriod || economicsPeriod;
    const provider = overrideProvider || economicsProvider || 'all';
    setEconomicsLoading(true);
    setEconomicsError('');
    try {
      const response = await fetch(
        `/api/economics/summary?initData=${encodeURIComponent(initData)}&period=${encodeURIComponent(period)}&provider=${encodeURIComponent(provider)}&sync_fixed=1`,
      );
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки экономики', 'Fehler beim Laden der Kosten'));
      }
      const data = await response.json();
      setEconomicsSummary(data?.summary || null);
    } catch (error) {
      setEconomicsSummary(null);
      setEconomicsError(`${tr('Ошибка экономики', 'Kostenfehler')}: ${error.message}`);
    } finally {
      setEconomicsLoading(false);
    }
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) return;
    if (!flashcardsOnly && selectedSections.has('economics')) {
      loadEconomics(); // eslint-disable-line react-hooks/exhaustive-deps
    }
  }, [initData, isWebAppMode, economicsPeriod, economicsProvider, selectedSections, flashcardsOnly]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_PERIOD_STORAGE_KEY, economicsPeriod);
  }, [economicsPeriod]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_PROVIDER_STORAGE_KEY, economicsProvider);
  }, [economicsProvider]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_FORECAST_USERS_STORAGE_KEY, economicsForecastUsersDraft);
  }, [economicsForecastUsersDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_APP_RAM_STORAGE_KEY, economicsRailwayAppRamDraft);
  }, [economicsRailwayAppRamDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_APP_CPU_STORAGE_KEY, economicsRailwayAppCpuDraft);
  }, [economicsRailwayAppCpuDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_POSTGRES_RAM_STORAGE_KEY, economicsRailwayPostgresRamDraft);
  }, [economicsRailwayPostgresRamDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_POSTGRES_VOLUME_STORAGE_KEY, economicsRailwayPostgresVolumeDraft);
  }, [economicsRailwayPostgresVolumeDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_REDIS_RAM_STORAGE_KEY, economicsRailwayRedisRamDraft);
  }, [economicsRailwayRedisRamDraft]);

  useEffect(() => {
    writeStoredValue(ECONOMICS_RAILWAY_EGRESS_STORAGE_KEY, economicsRailwayEgressDraft);
  }, [economicsRailwayEgressDraft]);

  useEffect(() => {
    const postgresVolumeGb = Number(economicsSummary?.railway_infra?.live?.postgres?.db_size_gb || 0);
    if (!economicsRailwayPostgresVolumeDraft && postgresVolumeGb > 0) {
      setEconomicsRailwayPostgresVolumeDraft(String(Math.max(0.1, Number(postgresVolumeGb.toFixed(3)))));
    }
  }, [economicsSummary, economicsRailwayPostgresVolumeDraft]);

  useEffect(() => {
    const redisMemoryGb = Number(economicsSummary?.railway_infra?.live?.redis?.memory_used_mb || 0) / 1024;
    if (!economicsRailwayRedisRamDraft && redisMemoryGb > 0) {
      setEconomicsRailwayRedisRamDraft(String(Math.max(0.05, Number(redisMemoryGb.toFixed(3)))));
    }
  }, [economicsSummary, economicsRailwayRedisRamDraft]);

  const economicsProviderOptions = useMemo(() => {
    const knownOrder = [
      'openai',
      'google_tts',
      'google_translate',
      'deepl_free',
      'azure_translator',
      'livekit',
      'cloudflare_r2_storage',
      'cloudflare_r2_class_a',
      'cloudflare_r2_class_b',
      'youtube_api',
      'youtube_proxy',
      'perplexity',
      'stripe',
      'agent_tts',
      'offline_tts',
      'app_internal',
      'libretranslate',
      'mymemory',
      'argos_offline',
    ];
    const orderMap = new Map(knownOrder.map((item, index) => [item, index]));
    const dynamicProviders = Array.isArray(economicsSummary?.providers) ? economicsSummary.providers : [];
    const merged = new Set([
      'all',
      ...knownOrder,
      ...dynamicProviders.map((item) => String(item || '').trim().toLowerCase()).filter(Boolean),
    ]);
    if (economicsProvider && economicsProvider !== 'all') {
      merged.add(String(economicsProvider).trim().toLowerCase());
    }
    return Array.from(merged).sort((a, b) => {
      if (a === 'all') return -1;
      if (b === 'all') return 1;
      const orderA = orderMap.has(a) ? orderMap.get(a) : 10_000;
      const orderB = orderMap.has(b) ? orderMap.get(b) : 10_000;
      if (orderA !== orderB) return orderA - orderB;
      return a.localeCompare(b);
    });
  }, [economicsSummary, economicsProvider]);

  const economicsBudgetRows = useMemo(() => {
    const rows = Array.isArray(economicsSummary?.provider_budgets) ? economicsSummary.provider_budgets : [];
    if (!rows.length) return [];
    if (!economicsProvider || economicsProvider === 'all') return rows;
    return rows.filter((item) => String(item?.provider || '').trim().toLowerCase() === economicsProvider);
  }, [economicsSummary, economicsProvider]);

  const economicsPerUserResourceRows = useMemo(() => {
    const rows = Array.isArray(economicsSummary?.breakdown?.by_provider) ? economicsSummary.breakdown.by_provider : [];
    return rows
      .filter((item) => Number(item?.active_users || 0) > 0 || Number(item?.avg_total_cost_per_active_user || 0) > 0)
      .slice()
      .sort((a, b) => Number(b?.avg_total_cost_per_active_user || 0) - Number(a?.avg_total_cost_per_active_user || 0))
      .slice(0, 10);
  }, [economicsSummary]);

  const economicsFreeTierCapacityRows = useMemo(() => {
    return economicsBudgetRows
      .map((row) => {
        const limitUnits = row?.effective_limit_units == null ? null : Number(row.effective_limit_units || 0);
        const usedUnits = Number(row?.used_units || 0);
        const avgUnitsPerActiveUser = Number(row?.avg_units_per_active_user || 0);
        const budgetKind = String(row?.metadata?.budget_kind || '').trim().toLowerCase();
        if (limitUnits == null || limitUnits <= 0 || avgUnitsPerActiveUser <= 0) {
          return null;
        }
        const totalUsersCapacity = Math.floor(limitUnits / avgUnitsPerActiveUser);
        const remainingUnits = Math.max(0, limitUnits - usedUnits);
        const remainingUsersCapacity = Math.floor(remainingUnits / avgUnitsPerActiveUser);
        return {
          provider: String(row?.provider || '').trim(),
          unitsType: String(row?.units_type || row?.unit || '').trim(),
          avgUnitsPerActiveUser,
          limitUnits,
          usedUnits,
          totalUsersCapacity,
          remainingUsersCapacity,
          budgetKind,
          budgetLabel: String(row?.period_label || '').trim() || String(row?.period_month || '').slice(0, 7),
        };
      })
      .filter(Boolean)
      .sort((a, b) => Number(a?.totalUsersCapacity || 0) - Number(b?.totalUsersCapacity || 0));
  }, [economicsBudgetRows]);

  const economicsFreeTierBottleneck = economicsFreeTierCapacityRows.length > 0 ? economicsFreeTierCapacityRows[0] : null;

  const economicsForecastUsers = useMemo(() => {
    const parsed = parseInt(String(economicsForecastUsersDraft || '').trim(), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return 0;
    return parsed;
  }, [economicsForecastUsersDraft]);

  const economicsForecastResourceRows = useMemo(() => {
    const rows = Array.isArray(economicsSummary?.breakdown?.by_provider) ? economicsSummary.breakdown.by_provider : [];
    const freeTierCapacityByProvider = new Map(
      economicsFreeTierCapacityRows.map((item) => [String(item?.provider || '').trim().toLowerCase(), item]),
    );
    if (economicsForecastUsers <= 0) return [];
    return rows
      .filter((item) => Number(item?.active_users || 0) > 0 || Number(item?.avg_total_cost_per_active_user || 0) > 0)
      .map((item) => {
        const providerKey = String(item?.provider || '').trim().toLowerCase();
        const capacity = freeTierCapacityByProvider.get(providerKey);
        return {
          provider: String(item?.provider || '').trim(),
          activeUsers: Number(item?.active_users || 0),
          forecastUsers: economicsForecastUsers,
          forecastVariableCost: Number(item?.avg_variable_cost_per_active_user || 0) * economicsForecastUsers,
          forecastFixedCost: Number(item?.avg_fixed_cost_per_active_user || 0) * economicsForecastUsers,
          forecastTotalCost: Number(item?.avg_total_cost_per_active_user || 0) * economicsForecastUsers,
          forecastEvents: Number(item?.avg_events_per_active_user || 0) * economicsForecastUsers,
          forecastUnitsByType: Array.isArray(item?.avg_units_by_type_per_active_user)
            ? item.avg_units_by_type_per_active_user.map((unitRow) => ({
              unitsType: String(unitRow?.units_type || '').trim(),
              forecastUnits: Number(unitRow?.avg_units_per_active_user || 0) * economicsForecastUsers,
            }))
            : [],
          freeTierTotalUsersCapacity: Number(capacity?.totalUsersCapacity || 0),
          freeTierRemainingUsersCapacity: Number(capacity?.remainingUsersCapacity || 0),
        };
      })
      .sort((a, b) => b.forecastTotalCost - a.forecastTotalCost);
  }, [economicsSummary, economicsFreeTierCapacityRows, economicsForecastUsers]);

  const economicsForecastTotals = useMemo(() => {
    if (economicsForecastUsers <= 0) {
      return {
        users: 0,
        forecastVariableCost: 0,
        forecastFixedCost: 0,
        forecastTotalCost: 0,
        freeTierBottleneckProvider: '',
        freeTierBottleneckUsers: 0,
      };
    }
    return {
      users: economicsForecastUsers,
      forecastVariableCost: Number(economicsSummary?.totals?.avg_variable_cost_per_active_user || 0) * economicsForecastUsers,
      forecastFixedCost: Number(economicsSummary?.totals?.avg_fixed_cost_per_active_user || 0) * economicsForecastUsers,
      forecastTotalCost: Number(economicsSummary?.totals?.avg_cost_per_active_user || 0) * economicsForecastUsers,
      freeTierBottleneckProvider: String(economicsFreeTierBottleneck?.provider || '').trim(),
      freeTierBottleneckUsers: Number(economicsFreeTierBottleneck?.totalUsersCapacity || 0),
    };
  }, [economicsSummary, economicsForecastUsers, economicsFreeTierBottleneck]);

  const economicsRailwayInfra = economicsProvider === 'all' ? (economicsSummary?.railway_infra || null) : null;
  const economicsRailwayPricing = economicsRailwayInfra?.pricing || null;
  const economicsRailwayLivePostgres = economicsRailwayInfra?.live?.postgres || null;
  const economicsRailwayLiveRedis = economicsRailwayInfra?.live?.redis || null;
  const economicsRailwayFixedComponents = Array.isArray(economicsRailwayInfra?.tracked_fixed_components)
    ? economicsRailwayInfra.tracked_fixed_components
    : [];
  const economicsRailwayTrackedBaselineUsd = Number(economicsRailwayInfra?.tracked_fixed_baseline_month_usd || 0);

  const economicsRailwayForecast = useMemo(() => {
    const parseDraft = (value) => {
      const parsed = Number.parseFloat(String(value || '').trim().replace(',', '.'));
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
    };
    const appRamGb = parseDraft(economicsRailwayAppRamDraft);
    const appCpuVcpu = parseDraft(economicsRailwayAppCpuDraft);
    const postgresRamGb = parseDraft(economicsRailwayPostgresRamDraft);
    const postgresVolumeGb = parseDraft(economicsRailwayPostgresVolumeDraft);
    const redisRamGb = parseDraft(economicsRailwayRedisRamDraft);
    const egressGb = parseDraft(economicsRailwayEgressDraft);
    const ramRate = Number(economicsRailwayPricing?.ram_per_gb_month || 0);
    const cpuRate = Number(economicsRailwayPricing?.cpu_per_vcpu_month || 0);
    const volumeRate = Number(economicsRailwayPricing?.volume_per_gb_month || 0);
    const appRamCost = appRamGb * ramRate;
    const appCpuCost = appCpuVcpu * cpuRate;
    const postgresRamCost = postgresRamGb * ramRate;
    const postgresVolumeCost = postgresVolumeGb * volumeRate;
    const redisRamCost = redisRamGb * ramRate;
    const egressCost = egressGb * Number(economicsRailwayPricing?.egress_per_gb || 0);
    const totalRamGb = appRamGb + postgresRamGb + redisRamGb;
    const variableCost = appRamCost + appCpuCost + postgresRamCost + postgresVolumeCost + redisRamCost + egressCost;
    return {
      appRamGb,
      appCpuVcpu,
      postgresRamGb,
      postgresVolumeGb,
      redisRamGb,
      egressGb,
      totalRamGb,
      baselineFixedCost: economicsRailwayTrackedBaselineUsd,
      appRamCost,
      appCpuCost,
      postgresRamCost,
      postgresVolumeCost,
      redisRamCost,
      egressCost,
      variableCost,
      totalCost: economicsRailwayTrackedBaselineUsd + variableCost,
    };
  }, [
    economicsRailwayPricing,
    economicsRailwayTrackedBaselineUsd,
    economicsRailwayAppRamDraft,
    economicsRailwayAppCpuDraft,
    economicsRailwayPostgresRamDraft,
    economicsRailwayPostgresVolumeDraft,
    economicsRailwayRedisRamDraft,
    economicsRailwayEgressDraft,
  ]);

  const economicsLedgerEventsCount = Number(economicsSummary?.totals?.events_count || 0);
  const economicsLedgerIsEmpty = !!economicsSummary && economicsLedgerEventsCount === 0;
  const selectedEconomicsProviderLabel = economicsProvider === 'all'
    ? tr('Все провайдеры', 'Alle Provider')
    : formatEconomicsProviderLabel(economicsProvider);

  useEffect(() => {
    console.info('provider_mount', { provider_name: 'EconomicsProvider', ts: Date.now() });
    return () => {
      console.info('provider_unmount', { provider_name: 'EconomicsProvider', ts: Date.now() });
    };
  }, []);

  const value = {
    economicsPeriod, setEconomicsPeriod,
    economicsProvider, setEconomicsProvider,
    economicsForecastUsersDraft, setEconomicsForecastUsersDraft,
    economicsRailwayAppRamDraft, setEconomicsRailwayAppRamDraft,
    economicsRailwayAppCpuDraft, setEconomicsRailwayAppCpuDraft,
    economicsRailwayPostgresRamDraft, setEconomicsRailwayPostgresRamDraft,
    economicsRailwayPostgresVolumeDraft, setEconomicsRailwayPostgresVolumeDraft,
    economicsRailwayRedisRamDraft, setEconomicsRailwayRedisRamDraft,
    economicsRailwayEgressDraft, setEconomicsRailwayEgressDraft,
    economicsLoading,
    economicsError,
    economicsSummary,
    loadEconomics,
    economicsProviderOptions,
    economicsBudgetRows,
    economicsPerUserResourceRows,
    economicsFreeTierCapacityRows,
    economicsFreeTierBottleneck,
    economicsForecastUsers,
    economicsForecastResourceRows,
    economicsForecastTotals,
    economicsRailwayInfra,
    economicsRailwayPricing,
    economicsRailwayLivePostgres,
    economicsRailwayLiveRedis,
    economicsRailwayFixedComponents,
    economicsRailwayTrackedBaselineUsd,
    economicsRailwayForecast,
    economicsLedgerEventsCount,
    economicsLedgerIsEmpty,
    selectedEconomicsProviderLabel,
  };

  return (
    <EconomicsContext.Provider value={value}>
      {children}
    </EconomicsContext.Provider>
  );
}

export function useEconomics() {
  return useContext(EconomicsContext);
}
