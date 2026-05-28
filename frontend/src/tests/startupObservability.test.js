import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  emitStartupPhase,
  emitEntitlementResolved,
  emitFreeCoreBootReady,
  emitFreeCoreReady,
  emitHeavyModuleLoaded,
  emitHeavyModuleSkipped,
  emitLazyChunkLoaded,
} from '../startup/startupObservability.js';

describe('startupObservability — emitStartupPhase', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_startup_phase with the phase name', () => {
    emitStartupPhase('phase2', { effective_mode: 'free' });
    expect(console.info).toHaveBeenCalledWith(
      'frontend_startup_phase',
      expect.objectContaining({ phase: 'phase2', effective_mode: 'free' }),
    );
  });

  it('includes a ts timestamp', () => {
    emitStartupPhase('phase3');
    const payload = console.info.mock.calls[0][1];
    expect(typeof payload.ts).toBe('number');
    expect(payload.ts).toBeGreaterThan(0);
  });
});

describe('startupObservability — emitEntitlementResolved', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_entitlement_resolved', () => {
    emitEntitlementResolved('free');
    expect(console.info).toHaveBeenCalledWith(
      'frontend_entitlement_resolved',
      expect.objectContaining({ effective_mode: 'free', is_free: true }),
    );
  });

  it('sets is_free=false for trial mode', () => {
    emitEntitlementResolved('trial');
    const payload = console.info.mock.calls[0][1];
    expect(payload.is_free).toBe(false);
  });

  it('sets is_free=true for null (unknown entitlement)', () => {
    emitEntitlementResolved(null);
    const payload = console.info.mock.calls[0][1];
    expect(payload.is_free).toBe(true);
  });

  it('passes through extra meta fields', () => {
    emitEntitlementResolved('pro', { user_id: 42 });
    const payload = console.info.mock.calls[0][1];
    expect(payload.user_id).toBe(42);
  });
});

describe('startupObservability — emitFreeCoreBootReady', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_free_core_boot', () => {
    emitFreeCoreBootReady();
    expect(console.info).toHaveBeenCalledWith('frontend_free_core_boot', expect.any(Object));
  });
});

describe('startupObservability — emitFreeCoreReady', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_free_core_ready', () => {
    emitFreeCoreReady({ effective_mode: 'pro' });
    expect(console.info).toHaveBeenCalledWith(
      'frontend_free_core_ready',
      expect.objectContaining({ effective_mode: 'pro' }),
    );
  });
});

describe('startupObservability — emitHeavyModuleLoaded', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_heavy_module_loaded with module name', () => {
    emitHeavyModuleLoaded('livekit', { trigger: 'assistant_open' });
    expect(console.info).toHaveBeenCalledWith(
      'frontend_heavy_module_loaded',
      expect.objectContaining({ module: 'livekit', trigger: 'assistant_open' }),
    );
  });
});

describe('startupObservability — emitHeavyModuleSkipped', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_heavy_module_skipped with module and reason', () => {
    emitHeavyModuleSkipped('livekit', 'free_mode', { effective_mode: 'free' });
    expect(console.info).toHaveBeenCalledWith(
      'frontend_heavy_module_skipped',
      expect.objectContaining({ module: 'livekit', reason: 'free_mode', effective_mode: 'free' }),
    );
  });
});

describe('startupObservability — emitLazyChunkLoaded', () => {
  beforeEach(() => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });

  it('emits frontend_lazy_chunk_loaded with chunk name', () => {
    emitLazyChunkLoaded('home-feature');
    expect(console.info).toHaveBeenCalledWith(
      'frontend_lazy_chunk_loaded',
      expect.objectContaining({ chunk: 'home-feature' }),
    );
  });
});
