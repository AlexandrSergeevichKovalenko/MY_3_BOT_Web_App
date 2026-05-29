import React, { useContext } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  getLocalDateKey,
  formatWeeklyValueFn,
  PlanningContext,
  PlanningProvider,
  PLANNING_PROVIDER_EXTRACTION_METRICS,
} from '../providers/PlanningProvider.jsx';

const basePlanningProps = {
  initData: 'query_id=test&user=%7B%22id%22%3A123%7D',
  isWebAppMode: true,
  isLightweightFreeMode: false,
  startupEffectiveMode: 'pro',
  stableWebappUserId: '123',
  getWebappLanguagePairHint: () => ({ source_lang: 'ru', target_lang: 'de' }),
  fetchGetWithRetry: vi.fn(),
  readApiError: vi.fn(async (_response, fallbackRu) => fallbackRu),
  normalizeNetworkErrorMessage: vi.fn((_error, fallbackRu) => fallbackRu),
  tr: (ru) => ru,
  startupPhase2Ready: false,
  startupPhase3Ready: false,
  pageVisible: true,
  isHomeRouteActive: false,
  homeSnapshotResumeTick: 0,
  activeHomeSubsectionKey: '',
  planningApiRef: { current: null },
};

function capturePlanningContext(props = {}) {
  let observed = null;
  function Consumer() {
    observed = useContext(PlanningContext);
    return null;
  }
  renderToStaticMarkup(React.createElement(
    PlanningProvider,
    { ...basePlanningProps, ...props },
    React.createElement(Consumer),
  ));
  return observed;
}

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('planningProvider — getLocalDateKey', () => {
  it('returns a YYYY-MM-DD formatted string', () => {
    const key = getLocalDateKey();
    expect(key).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });

  it('matches today\'s date', () => {
    const key = getLocalDateKey();
    const now = new Date();
    const yyyy = String(now.getFullYear());
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    expect(key).toBe(`${yyyy}-${mm}-${dd}`);
  });
});

describe('planningProvider — formatWeeklyValueFn', () => {
  it('returns "0" for falsy input', () => {
    expect(formatWeeklyValueFn(null)).toBe('0');
    expect(formatWeeklyValueFn(undefined)).toBe('0');
    expect(formatWeeklyValueFn('')).toBe('0');
  });

  it('rounds to integer by default', () => {
    expect(formatWeeklyValueFn(3.7)).toBe('4');
    expect(formatWeeklyValueFn(2.3)).toBe('2');
  });

  it('respects digits parameter', () => {
    const result = formatWeeklyValueFn(3.14159, 2);
    expect(result).toBe('3.14');
  });

  it('returns "0" for non-finite input', () => {
    expect(formatWeeklyValueFn(Infinity)).toBe('0');
    expect(formatWeeklyValueFn(NaN)).toBe('0');
  });
});

describe('planningProvider — PlanningContext', () => {
  it('PlanningContext is not null (was properly created)', () => {
    expect(PlanningContext).not.toBeNull();
    expect(typeof PlanningContext).toBe('object');
  });

  it('provides planning context when mounted for entitled users', () => {
    const ctx = capturePlanningContext();
    expect(ctx).not.toBeNull();
    expect(typeof ctx.loadTodayPlan).toBe('function');
    expect(typeof ctx.loadWeeklyPlan).toBe('function');
    expect(typeof ctx.loadSkillReport).toBe('function');
  });

  it('does not execute planning loaders in lightweight free mode', async () => {
    const fetchGetWithRetry = vi.fn();
    const ctx = capturePlanningContext({
      isLightweightFreeMode: true,
      startupEffectiveMode: 'free',
      fetchGetWithRetry,
    });
    await ctx.loadTodayPlan({ manual: true });
    await ctx.loadWeeklyPlan({ manual: true });
    await ctx.loadSkillReport({ manual: true });
    expect(fetchGetWithRetry).not.toHaveBeenCalled();
  });

  it('executes planning loader for entitled users when explicitly requested', async () => {
    const fetchGetWithRetry = vi.fn(async () => ({
      ok: true,
      json: async () => ({ date: '2026-05-29', total_minutes: 0, items: [] }),
    }));
    const ctx = capturePlanningContext({ fetchGetWithRetry });
    await ctx.loadTodayPlan({ manual: true });
    expect(fetchGetWithRetry).toHaveBeenCalledTimes(1);
    expect(String(fetchGetWithRetry.mock.calls[0][0])).toContain('/api/today?');
  });

  it('exposes extraction metrics for startup observability', () => {
    expect(PLANNING_PROVIDER_EXTRACTION_METRICS).toEqual(expect.objectContaining({
      provider_name: 'planning',
      state_removed: expect.any(Number),
      refs_removed: expect.any(Number),
      effects_removed: expect.any(Number),
      callbacks_removed: expect.any(Number),
      memos_removed: expect.any(Number),
    }));
  });
});
