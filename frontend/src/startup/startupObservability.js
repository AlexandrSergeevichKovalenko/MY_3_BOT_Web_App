/**
 * Startup observability — structured console events for frontend boot instrumentation.
 *
 * All events are console.info calls with a structured payload.
 * The event name is the first argument; the payload is the second.
 *
 * Events:
 *   frontend_startup_phase         — a named startup phase completed
 *   frontend_entitlement_resolved  — billing status known, mode determined
 *   frontend_free_core_boot        — free-core is ready (entitlement=free confirmed)
 *   frontend_heavy_module_loaded   — a heavy module was permitted and loaded
 *   frontend_heavy_module_skipped  — a heavy module was blocked (free mode)
 *   frontend_lazy_chunk_loaded     — a React.lazy() chunk finished loading
 *   frontend_free_core_ready       — free-core initialization sequence complete
 */

const _emit = (event, payload) => {
  console.info(event, payload);
};

export function emitStartupPhase(phase, meta = {}) {
  _emit('frontend_startup_phase', {
    phase,
    ts: Date.now(),
    ...meta,
  });
}

export function emitEntitlementResolved(effectiveMode, meta = {}) {
  _emit('frontend_entitlement_resolved', {
    effective_mode: String(effectiveMode || 'free'),
    is_free: String(effectiveMode || '') === 'free' || effectiveMode == null,
    ts: Date.now(),
    ...meta,
  });
}

export function emitFreeCoreBootReady(meta = {}) {
  _emit('frontend_free_core_boot', {
    ts: Date.now(),
    ...meta,
  });
}

export function emitFreeCoreReady(meta = {}) {
  _emit('frontend_free_core_ready', {
    ts: Date.now(),
    ...meta,
  });
}

export function emitHeavyModuleLoaded(moduleName, meta = {}) {
  _emit('frontend_heavy_module_loaded', {
    module: moduleName,
    ts: Date.now(),
    ...meta,
  });
}

export function emitHeavyModuleSkipped(moduleName, reason, meta = {}) {
  _emit('frontend_heavy_module_skipped', {
    module: moduleName,
    reason,
    ts: Date.now(),
    ...meta,
  });
}

export function emitLazyChunkLoaded(chunkName, meta = {}) {
  _emit('frontend_lazy_chunk_loaded', {
    chunk: chunkName,
    ts: Date.now(),
    ...meta,
  });
}
