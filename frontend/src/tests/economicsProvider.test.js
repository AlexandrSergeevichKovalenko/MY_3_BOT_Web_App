import { describe, it, expect } from 'vitest';
import {
  formatEconomicsProviderLabel,
  formatEconomicsUnitsLabel,
  formatEconomicsCompactNumber,
  EconomicsContext,
} from '../providers/EconomicsProvider.jsx';

describe('economicsProvider — formatEconomicsProviderLabel', () => {
  it('returns known provider label for openai', () => {
    expect(formatEconomicsProviderLabel('openai')).toBe('OpenAI');
  });

  it('returns "All Providers" for "all"', () => {
    expect(formatEconomicsProviderLabel('all')).toBe('All Providers');
  });

  it('returns "Unassigned" for empty string', () => {
    expect(formatEconomicsProviderLabel('')).toBe('Unassigned');
  });

  it('capitalizes unknown snake_case provider', () => {
    expect(formatEconomicsProviderLabel('my_custom_provider')).toBe('My Custom Provider');
  });
});

describe('economicsProvider — formatEconomicsUnitsLabel', () => {
  it('returns German label for tokens_in when uiLang=de', () => {
    expect(formatEconomicsUnitsLabel('tokens_in', 'de')).toBe('Input-Tokens');
  });

  it('returns Russian/English label for tokens_in when uiLang is not de', () => {
    expect(formatEconomicsUnitsLabel('tokens_in', 'ru')).toBe('tokens in');
  });

  it('returns "units" for unknown unit type', () => {
    expect(formatEconomicsUnitsLabel('', 'ru')).toBe('units');
  });
});

describe('economicsProvider — formatEconomicsCompactNumber', () => {
  it('formats large number without decimals', () => {
    expect(formatEconomicsCompactNumber(1234)).toBe('1234');
  });

  it('formats value between 10-100 with 1 decimal', () => {
    expect(formatEconomicsCompactNumber(12.5678)).toBe('12.6');
  });

  it('formats zero correctly', () => {
    expect(formatEconomicsCompactNumber(0)).toBe('0.000');
  });
});

describe('economicsProvider — EconomicsContext', () => {
  it('EconomicsContext is not null (was properly created)', () => {
    expect(EconomicsContext).not.toBeNull();
    expect(typeof EconomicsContext).toBe('object');
  });
});
