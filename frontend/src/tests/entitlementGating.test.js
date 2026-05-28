import { describe, it, expect } from 'vitest';
import {
  isFreeCoreOnlyMode,
  isHeavyModulePermitted,
  FREE_CORE_ROUTES,
  HEAVY_PREMIUM_ROUTES,
  FREE_CORE_FEATURES,
  HEAVY_PREMIUM_FEATURES,
} from '../featureRegistry.js';

// Simulate what App.jsx computes: billingEffectiveMode → isLightweightFreeMode
function deriveIsLightweightFreeMode(billingStatus) {
  const effectiveMode = String(billingStatus?.effective_mode || '').trim().toLowerCase();
  return effectiveMode === 'free' || effectiveMode === '';
}

// Simulate lockedFreeStartupSections derivation
function deriveLockedFreeStartupSections(isLightweightFreeMode) {
  return new Set(isLightweightFreeMode ? ['home_today', 'home_skills', 'home_weekly_plan'] : []);
}

// Simulate LiveKit prefetch gate
function shouldPrefetchLiveKit(isLightweightFreeMode, appMode, selectedSections, assistantToken, token) {
  if (isLightweightFreeMode) return false;
  return (appMode === 'lesson' || selectedSections.has('assistant') || assistantToken || token);
}

describe('entitlementGating — free user initialization', () => {
  it('free user has isLightweightFreeMode=true', () => {
    expect(deriveIsLightweightFreeMode({ effective_mode: 'free' })).toBe(true);
  });

  it('null billing status treats user as free (conservative)', () => {
    expect(deriveIsLightweightFreeMode(null)).toBe(true);
  });

  it('pro user has isLightweightFreeMode=false', () => {
    expect(deriveIsLightweightFreeMode({ effective_mode: 'pro' })).toBe(false);
  });

  it('trial user has isLightweightFreeMode=false', () => {
    expect(deriveIsLightweightFreeMode({ effective_mode: 'trial' })).toBe(false);
  });
});

describe('entitlementGating — free user does not initialize heavy modules', () => {
  it('free user: LiveKit NOT prefetched even if assistant section selected', () => {
    const isFreeFree = true;
    const sections = new Set(['assistant']);
    expect(shouldPrefetchLiveKit(isFreeFree, 'webapp', sections, null, null)).toBe(false);
  });

  it('free user: LiveKit NOT prefetched even in lesson mode', () => {
    const isFreeFree = true;
    expect(shouldPrefetchLiveKit(isFreeFree, 'lesson', new Set(), null, null)).toBe(false);
  });

  it('paid user: LiveKit prefetched when assistant selected', () => {
    const isFreeFree = false;
    const sections = new Set(['assistant']);
    expect(shouldPrefetchLiveKit(isFreeFree, 'webapp', sections, null, null)).toBe(true);
  });

  it('paid user: LiveKit prefetched in lesson mode', () => {
    const isFreeFree = false;
    expect(shouldPrefetchLiveKit(isFreeFree, 'lesson', new Set(), null, null)).toBe(true);
  });

  it('paid user: LiveKit NOT prefetched if no trigger', () => {
    const isFreeFree = false;
    expect(shouldPrefetchLiveKit(isFreeFree, 'webapp', new Set(), null, null)).toBeFalsy();
  });
});

describe('entitlementGating — free user locked sections', () => {
  it('free user: today/skills/weekly_plan are locked', () => {
    const locked = deriveLockedFreeStartupSections(true);
    expect(locked.has('home_today')).toBe(true);
    expect(locked.has('home_skills')).toBe(true);
    expect(locked.has('home_weekly_plan')).toBe(true);
  });

  it('paid user: no sections locked', () => {
    const locked = deriveLockedFreeStartupSections(false);
    expect(locked.size).toBe(0);
  });
});

describe('entitlementGating — heavy premium routes are not in free-core', () => {
  it('assistant route is heavy premium', () => {
    expect(HEAVY_PREMIUM_ROUTES.has('assistant')).toBe(true);
    expect(FREE_CORE_ROUTES.has('assistant')).toBe(false);
  });

  it('analytics route is heavy premium', () => {
    expect(HEAVY_PREMIUM_ROUTES.has('analytics')).toBe(true);
    expect(FREE_CORE_ROUTES.has('analytics')).toBe(false);
  });

  it('flashcards route is free-core', () => {
    expect(FREE_CORE_ROUTES.has('flashcards')).toBe(true);
    expect(HEAVY_PREMIUM_ROUTES.has('flashcards')).toBe(false);
  });

  it('translations route is free-core', () => {
    expect(FREE_CORE_ROUTES.has('translations')).toBe(true);
    expect(HEAVY_PREMIUM_ROUTES.has('translations')).toBe(false);
  });
});

describe('entitlementGating — feature registry gating correctness', () => {
  it('all FREE_CORE_FEATURES values are strings', () => {
    for (const v of Object.values(FREE_CORE_FEATURES)) {
      expect(typeof v).toBe('string');
    }
  });

  it('all HEAVY_PREMIUM_FEATURES values are strings', () => {
    for (const v of Object.values(HEAVY_PREMIUM_FEATURES)) {
      expect(typeof v).toBe('string');
    }
  });

  it('isFreeCoreOnlyMode returns true for all edge-case free inputs', () => {
    for (const input of ['free', '', null, undefined, '  ']) {
      expect(isFreeCoreOnlyMode(input)).toBe(true);
    }
  });

  it('isHeavyModulePermitted blocks livekit for unknown/free mode', () => {
    for (const mode of ['', 'free', null, undefined]) {
      expect(isHeavyModulePermitted(mode, 'livekit')).toBe(false);
    }
  });
});

describe('entitlementGating — paid users still initialize premium modules', () => {
  it('pro user: livekit module is permitted', () => {
    expect(isHeavyModulePermitted('pro', 'livekit')).toBe(true);
  });

  it('trial user: livekit module is permitted', () => {
    expect(isHeavyModulePermitted('trial', 'livekit')).toBe(true);
  });
});
