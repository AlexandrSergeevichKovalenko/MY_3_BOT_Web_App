const WEEKDAY_KINDS = {
  1: 'monday',
  3: 'wednesday',
  5: 'friday',
};

function startOfLocalDay(value) {
  const date = new Date(value);
  date.setHours(0, 0, 0, 0);
  return date;
}

function addDays(value, days) {
  const date = startOfLocalDay(value);
  date.setDate(date.getDate() + days);
  return date;
}

function getMondayOfWeek(value) {
  const date = startOfLocalDay(value);
  const offset = (date.getDay() + 6) % 7;
  return addDays(date, -offset);
}

function toLocalDateKey(value) {
  const date = startOfLocalDay(value);
  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function formatExactDate(value, locale) {
  return new Intl.DateTimeFormat(locale || 'ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  }).format(value);
}

function formatExactRange(startDate, endDate, locale) {
  return `${formatExactDate(startDate, locale)} - ${formatExactDate(endDate, locale)}`;
}

export function buildWeeklySummaryVisitConfig({ now = new Date(), locale = 'ru-RU', labels = {} } = {}) {
  const visitDate = startOfLocalDay(now);
  const weekday = visitDate.getDay();
  const kind = WEEKDAY_KINDS[weekday];

  if (!kind) {
    return null;
  }

  const currentWeekMonday = getMondayOfWeek(visitDate);
  let currentStart = currentWeekMonday;
  let currentEnd = currentWeekMonday;
  let previousStart = currentWeekMonday;
  let previousEnd = currentWeekMonday;
  let title = '';
  let comparisonLabel = '';

  if (kind === 'monday') {
    currentStart = addDays(currentWeekMonday, -7);
    currentEnd = addDays(currentWeekMonday, -1);
    previousStart = addDays(currentWeekMonday, -14);
    previousEnd = addDays(currentWeekMonday, -8);
    title = labels.mondayTitle || '';
    comparisonLabel = labels.mondayComparisonLabel || '';
  } else if (kind === 'wednesday') {
    currentStart = currentWeekMonday;
    currentEnd = addDays(currentWeekMonday, 1);
    previousStart = addDays(currentWeekMonday, -7);
    previousEnd = addDays(currentWeekMonday, -6);
    title = labels.wednesdayTitle || '';
    comparisonLabel = labels.wednesdayComparisonLabel || '';
  } else if (kind === 'friday') {
    currentStart = currentWeekMonday;
    currentEnd = addDays(currentWeekMonday, 3);
    previousStart = addDays(currentWeekMonday, -7);
    previousEnd = addDays(currentWeekMonday, -4);
    title = labels.fridayTitle || '';
    comparisonLabel = labels.fridayComparisonLabel || '';
  }

  return {
    kind,
    title,
    comparisonLabel,
    visitDateKey: toLocalDateKey(visitDate),
    subtitle: formatExactRange(currentStart, currentEnd, locale),
    currentPeriod: {
      startDate: toLocalDateKey(currentStart),
      endDate: toLocalDateKey(currentEnd),
      label: formatExactRange(currentStart, currentEnd, locale),
    },
    previousPeriod: {
      startDate: toLocalDateKey(previousStart),
      endDate: toLocalDateKey(previousEnd),
      label: formatExactRange(previousStart, previousEnd, locale),
    },
  };
}

export function buildWeeklySummaryHeroFacts({
  currentMetrics = {},
  previousMetrics = {},
  metricPriority = [],
} = {}) {
  const keys = Array.isArray(metricPriority) && metricPriority.length
    ? metricPriority
    : Object.keys(currentMetrics || {});

  const normalized = keys.map((key) => {
    const current = currentMetrics?.[key] && typeof currentMetrics[key] === 'object' ? currentMetrics[key] : {};
    const previous = previousMetrics?.[key] && typeof previousMetrics[key] === 'object' ? previousMetrics[key] : {};
    const goal = Math.max(0, Number(current.goal || 0));
    const actual = Math.max(0, Number(current.actual || 0));
    const previousActual = Math.max(0, Number(previous.actual || 0));
    const completionPercent = goal > 0 ? Number(current.completion_percent || 0) : 0;
    const previousGoal = Math.max(0, Number(previous.goal || 0));
    const previousCompletionPercent = previousGoal > 0 ? Number(previous.completion_percent || 0) : 0;
    return {
      key,
      goal,
      actual,
      previousActual,
      deltaActual: actual - previousActual,
      completionPercent,
      previousCompletionPercent,
      activityScore: goal > 0 ? completionPercent : actual,
    };
  });

  const planned = normalized.filter((item) => item.goal > 0);
  const active = normalized.filter((item) => item.goal > 0 || item.actual > 0);
  const basis = planned.length ? planned : active;
  const strongestCount = basis.length <= 2 ? Math.min(1, basis.length) : 2;
  const weakestCount = basis.length <= 2 ? Math.min(1, Math.max(0, basis.length - strongestCount)) : 2;
  const hasComparablePreviousActivity = basis.some((item) => item.previousActual > 0);

  let strongest = [];
  let weakest = [];

  if (hasComparablePreviousActivity) {
    strongest = [...basis]
      .filter((item) => item.deltaActual > 0)
      .sort((a, b) => b.deltaActual - a.deltaActual)
      .slice(0, strongestCount)
      .map((item) => item.key);
    weakest = [...basis]
      .filter((item) => item.deltaActual < 0 && !strongest.includes(item.key))
      .sort((a, b) => a.deltaActual - b.deltaActual)
      .slice(0, weakestCount)
      .map((item) => item.key);
  }

  if (!strongest.length) {
    strongest = [...basis]
      .sort((a, b) => b.activityScore - a.activityScore)
      .slice(0, strongestCount)
      .map((item) => item.key);
  }

  if (!weakest.length) {
    weakest = [...basis]
      .sort((a, b) => a.activityScore - b.activityScore)
      .filter((item) => !strongest.includes(item.key))
      .slice(0, weakestCount)
      .map((item) => item.key);
  }

  const currentAverageCompletion = planned.length
    ? planned.reduce((sum, item) => sum + item.completionPercent, 0) / planned.length
    : 0;
  const previousAverageCompletion = planned.length
    ? planned.reduce((sum, item) => sum + item.previousCompletionPercent, 0) / planned.length
    : 0;

  return {
    hasPlan: planned.length > 0,
    hasActivity: active.length > 0,
    planCompletedPercent: Math.round(currentAverageCompletion),
    trendDeltaPercent: Math.round(currentAverageCompletion - previousAverageCompletion),
    strongestKeys: strongest,
    weakestKeys: weakest,
  };
}
