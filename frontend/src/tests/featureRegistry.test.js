import { describe, it, expect } from 'vitest';
import {
  FREE_CORE_FEATURES,
  HEAVY_PREMIUM_FEATURES,
  FREE_CORE_ROUTES,
  HEAVY_PREMIUM_ROUTES,
  HEAVY_PREMIUM_MODULES,
  isHeavyModulePermitted,
  isFreeCoreOnlyMode,
} from '../featureRegistry.js';

describe('featureRegistry — FREE_CORE_FEATURES', () => {
  it('contains translations', () => {
    expect(FREE_CORE_FEATURES.translations).toBe('translations');
  });

  it('contains dictionary', () => {
    expect(FREE_CORE_FEATURES.dictionary).toBe('dictionary');
  });

  it('contains fsrs', () => {
    expect(FREE_CORE_FEATURES.fsrs).toBe('fsrs');
  });

  it('contains quizzes', () => {
    expect(FREE_CORE_FEATURES.quizzes).toBe('quizzes');
  });

  it('contains shortcut_ingest', () => {
    expect(FREE_CORE_FEATURES.shortcut_ingest).toBe('shortcut_ingest');
  });

  it('is frozen — cannot add new keys', () => {
    expect(() => { FREE_CORE_FEATURES.injected = 'bad'; }).toThrow();
  });
});

describe('featureRegistry — HEAVY_PREMIUM_FEATURES', () => {
  it('contains skills', () => {
    expect(HEAVY_PREMIUM_FEATURES.skills).toBe('skills');
  });

  it('contains analytics', () => {
    expect(HEAVY_PREMIUM_FEATURES.analytics).toBe('analytics');
  });

  it('contains livekit_assistant', () => {
    expect(HEAVY_PREMIUM_FEATURES.livekit_assistant).toBe('livekit_assistant');
  });

  it('contains youtube', () => {
    expect(HEAVY_PREMIUM_FEATURES.youtube).toBe('youtube');
  });

  it('is frozen — cannot add new keys', () => {
    expect(() => { HEAVY_PREMIUM_FEATURES.injected = 'bad'; }).toThrow();
  });

  it('does NOT overlap with FREE_CORE_FEATURES values', () => {
    const freeValues = new Set(Object.values(FREE_CORE_FEATURES));
    for (const value of Object.values(HEAVY_PREMIUM_FEATURES)) {
      expect(freeValues.has(value)).toBe(false);
    }
  });
});

describe('featureRegistry — routes', () => {
  it('translations is in FREE_CORE_ROUTES', () => {
    expect(FREE_CORE_ROUTES.has('translations')).toBe(true);
  });

  it('dictionary is in FREE_CORE_ROUTES', () => {
    expect(FREE_CORE_ROUTES.has('dictionary')).toBe(true);
  });

  it('flashcards is in FREE_CORE_ROUTES', () => {
    expect(FREE_CORE_ROUTES.has('flashcards')).toBe(true);
  });

  it('analytics is in HEAVY_PREMIUM_ROUTES', () => {
    expect(HEAVY_PREMIUM_ROUTES.has('analytics')).toBe(true);
  });

  it('assistant is in HEAVY_PREMIUM_ROUTES', () => {
    expect(HEAVY_PREMIUM_ROUTES.has('assistant')).toBe(true);
  });

  it('home_today is in HEAVY_PREMIUM_ROUTES', () => {
    expect(HEAVY_PREMIUM_ROUTES.has('home_today')).toBe(true);
  });

  it('FREE_CORE_ROUTES and HEAVY_PREMIUM_ROUTES do not overlap', () => {
    for (const route of FREE_CORE_ROUTES) {
      expect(HEAVY_PREMIUM_ROUTES.has(route)).toBe(false);
    }
  });
});

describe('featureRegistry — isFreeCoreOnlyMode', () => {
  it('returns true for "free"', () => {
    expect(isFreeCoreOnlyMode('free')).toBe(true);
  });

  it('returns true for empty string (entitlement not yet known)', () => {
    expect(isFreeCoreOnlyMode('')).toBe(true);
  });

  it('returns true for null', () => {
    expect(isFreeCoreOnlyMode(null)).toBe(true);
  });

  it('returns true for undefined', () => {
    expect(isFreeCoreOnlyMode(undefined)).toBe(true);
  });

  it('returns false for "trial"', () => {
    expect(isFreeCoreOnlyMode('trial')).toBe(false);
  });

  it('returns false for "pro"', () => {
    expect(isFreeCoreOnlyMode('pro')).toBe(false);
  });
});

describe('featureRegistry — isHeavyModulePermitted', () => {
  it('livekit is NOT permitted for free mode', () => {
    expect(isHeavyModulePermitted('free', 'livekit')).toBe(false);
  });

  it('livekit is permitted for trial mode', () => {
    expect(isHeavyModulePermitted('trial', 'livekit')).toBe(true);
  });

  it('livekit is permitted for pro mode', () => {
    expect(isHeavyModulePermitted('pro', 'livekit')).toBe(true);
  });

  it('unknown module returns false', () => {
    expect(isHeavyModulePermitted('pro', 'nonexistent_module')).toBe(false);
  });

  it('echarts is permitted even for free mode (used in analytics visible to free users)', () => {
    expect(isHeavyModulePermitted('free', 'echarts')).toBe(true);
  });

  it('HEAVY_PREMIUM_MODULES list is non-empty', () => {
    expect(HEAVY_PREMIUM_MODULES.length).toBeGreaterThan(0);
  });
});
