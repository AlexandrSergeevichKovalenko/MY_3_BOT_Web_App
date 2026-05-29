import { describe, it, expect } from 'vitest';
import {
  buildDefaultAnalyticsCalendarRange,
  formatAnalyticsCalendarDisplayDate,
  parseAnalyticsScopeKey,
  AnalyticsContext,
} from '../providers/AnalyticsProvider.jsx';

describe('analyticsProvider — buildDefaultAnalyticsCalendarRange', () => {
  it('returns object with startDate and endDate strings', () => {
    const range = buildDefaultAnalyticsCalendarRange();
    expect(typeof range.startDate).toBe('string');
    expect(typeof range.endDate).toBe('string');
    expect(range.startDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(range.endDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });

  it('startDate is 29 days before endDate', () => {
    const range = buildDefaultAnalyticsCalendarRange();
    const start = new Date(range.startDate);
    const end = new Date(range.endDate);
    const diffDays = Math.round((end.getTime() - start.getTime()) / 86400000);
    expect(diffDays).toBe(29);
  });
});

describe('analyticsProvider — formatAnalyticsCalendarDisplayDate', () => {
  it('returns empty string for empty input', () => {
    expect(formatAnalyticsCalendarDisplayDate('')).toBe('');
  });

  it('returns empty string for invalid date string', () => {
    expect(formatAnalyticsCalendarDisplayDate('not-a-date')).toBe('');
  });

  it('formats a valid ISO date without throwing', () => {
    const result = formatAnalyticsCalendarDisplayDate('2024-03-15', 'ru-RU');
    expect(typeof result).toBe('string');
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('analyticsProvider — parseAnalyticsScopeKey', () => {
  it('returns personal scope for empty string', () => {
    const result = parseAnalyticsScopeKey('');
    expect(result).toEqual({ scope_key: 'personal', scope_kind: 'personal', scope_chat_id: null });
  });

  it('returns personal scope for "personal"', () => {
    const result = parseAnalyticsScopeKey('personal');
    expect(result).toEqual({ scope_key: 'personal', scope_kind: 'personal', scope_chat_id: null });
  });

  it('parses group scope key correctly', () => {
    const result = parseAnalyticsScopeKey('group:12345');
    expect(result).toEqual({ scope_key: 'group:12345', scope_kind: 'group', scope_chat_id: 12345 });
  });

  it('falls back to personal for malformed group key', () => {
    const result = parseAnalyticsScopeKey('group:notanumber');
    expect(result).toEqual({ scope_key: 'personal', scope_kind: 'personal', scope_chat_id: null });
  });
});

describe('analyticsProvider — AnalyticsContext', () => {
  it('AnalyticsContext is not null (was properly created)', () => {
    expect(AnalyticsContext).not.toBeNull();
    expect(typeof AnalyticsContext).toBe('object');
  });
});
