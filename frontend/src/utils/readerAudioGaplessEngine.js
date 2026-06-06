/**
 * ReaderAudioGaplessEngine — Web Audio API gapless playback for the reader TTS.
 *
 * Why: the legacy <audio>-element + ping-pong approach has an audible gap at
 * every clip boundary (especially on iOS, which won't buffer a background
 * <audio> element). The Web Audio API schedules the next AudioBufferSource to
 * start exactly at the end of the current one on the audio-context clock, so
 * transitions are sample-accurate and gapless on both iOS and Android.
 *
 * This module is self-contained and framework-agnostic. It is added ALONGSIDE
 * the existing engine behind a feature flag — the legacy path stays as a
 * fallback. Nothing here touches the network beyond fetching the same MP3 URLs
 * the backend already produces; all decoding/playback happens on the device, so
 * there is zero additional server/model load.
 *
 * Lifecycle:
 *   const eng = new ReaderAudioGaplessEngine({ onActiveClipChange, onEnded, onError });
 *   await eng.ensureContext();           // MUST be called from a user gesture
 *   await eng.loadClip('keyA', urlA);    // fetch + decode → cached AudioBuffer
 *   eng.start('keyA', { offsetSec: 0, rate: 1 });
 *   await eng.loadClip('keyB', urlB);
 *   eng.enqueueNext('keyB');             // scheduled to play gapless after A
 *   // ...on boundary, onActiveClipChange('keyB') fires → caller enqueues C
 *   eng.getPositionMs();                 // position within the active clip
 *   eng.pause(); eng.resume(); eng.stop();
 */

/**
 * Feature flag: which reader-audio engine to use.
 *   'webaudio' → new gapless Web Audio engine
 *   'legacy'   → existing <audio>-element + ping-pong engine (default)
 * Stored in localStorage so it can be toggled live (no redeploy), e.g. from a
 * settings switch or the browser console:
 *   localStorage.setItem('readerAudioEngine', 'webaudio')
 */
export const READER_AUDIO_ENGINE_STORAGE_KEY = 'readerAudioEngine';

export function getReaderAudioEnginePreference() {
  try {
    const raw = String(window.localStorage.getItem(READER_AUDIO_ENGINE_STORAGE_KEY) || '').trim().toLowerCase();
    if (raw === 'webaudio' || raw === 'legacy') return raw;
  } catch (_e) { /* ignore */ }
  return 'legacy';
}

export function isWebAudioEngineEnabled() {
  return getReaderAudioEnginePreference() === 'webaudio' && isWebAudioSupported();
}

export function isWebAudioSupported() {
  return typeof window !== 'undefined'
    && (typeof window.AudioContext === 'function'
        || typeof window.webkitAudioContext === 'function');
}

export class ReaderAudioGaplessEngine {
  constructor({ onActiveClipChange = null, onEnded = null, onError = null } = {}) {
    this._onActiveClipChange = onActiveClipChange;
    this._onEnded = onEnded;
    this._onError = onError;

    this._ctx = null;
    this._gain = null;
    this._rate = 1;

    // Decoded buffers cached by clip key.
    this._buffers = new Map(); // key -> AudioBuffer
    // In-flight decode promises so we never decode the same key twice.
    this._decoding = new Map(); // key -> Promise<AudioBuffer>

    // Active clip currently sounding.
    this._active = null; // { key, source, startCtxTime, offsetSec, duration }
    // Next clip scheduled to start gapless after the active one.
    this._scheduled = null; // { key, source, startCtxTime, duration }

    this._stopped = true;
    this._generation = 0; // bumped on stop() to invalidate stale source.onended
  }

  isSupported() {
    return isWebAudioSupported();
  }

  /** Create (or resume) the AudioContext. Call from a user gesture on iOS. */
  async ensureContext() {
    if (!this.isSupported()) {
      throw new Error('Web Audio API is not supported in this browser');
    }
    if (!this._ctx) {
      const Ctor = window.AudioContext || window.webkitAudioContext;
      this._ctx = new Ctor();
      this._gain = this._ctx.createGain();
      this._gain.gain.value = 1;
      this._gain.connect(this._ctx.destination);
    }
    if (this._ctx.state === 'suspended') {
      try { await this._ctx.resume(); } catch (_e) { /* ignore */ }
    }
    return this._ctx;
  }

  /** Fetch + decode an MP3 URL into an AudioBuffer, cached by key. */
  async loadClip(key, url) {
    const k = String(key || '').trim();
    const u = String(url || '').trim();
    if (!k || !u) throw new Error('loadClip requires key and url');
    if (this._buffers.has(k)) return this._buffers.get(k);
    if (this._decoding.has(k)) return this._decoding.get(k);

    const decodePromise = (async () => {
      await this.ensureContext();
      const resp = await fetch(u);
      if (!resp.ok) throw new Error(`audio fetch HTTP ${resp.status}`);
      const arrayBuf = await resp.arrayBuffer();
      // decodeAudioData: use the promise form; Safari historically needed the
      // callback form, but modern iOS supports the promise form.
      const audioBuf = await this._ctx.decodeAudioData(arrayBuf.slice(0));
      this._buffers.set(k, audioBuf);
      this._decoding.delete(k);
      return audioBuf;
    })();

    this._decoding.set(k, decodePromise);
    try {
      return await decodePromise;
    } catch (err) {
      this._decoding.delete(k);
      throw err;
    }
  }

  hasClip(key) {
    return this._buffers.has(String(key || '').trim());
  }

  _makeSource(buffer) {
    const source = this._ctx.createBufferSource();
    source.buffer = buffer;
    source.playbackRate.value = this._rate;
    source.connect(this._gain);
    return source;
  }

  /** Begin playback from a loaded clip. */
  async start(clipKey, { offsetSec = 0, rate = 1 } = {}) {
    const k = String(clipKey || '').trim();
    const buffer = this._buffers.get(k);
    if (!buffer) throw new Error(`start: clip not loaded: ${k}`);
    await this.ensureContext();

    this._stopAllSources();
    this._rate = Number(rate) > 0 ? Number(rate) : 1;
    this._stopped = false;
    const gen = this._generation;

    const source = this._makeSource(buffer);
    const startAt = this._ctx.currentTime;
    const safeOffset = Math.max(0, Math.min(offsetSec || 0, buffer.duration - 0.02));
    source.start(startAt, safeOffset);
    source.onended = () => this._handleSourceEnded(k, gen);

    this._active = {
      key: k,
      source,
      startCtxTime: startAt,
      offsetSec: safeOffset,
      duration: buffer.duration,
    };
    this._scheduled = null;
  }

  /**
   * Schedule the next clip to start exactly when the active clip ends — the
   * gapless seam. Safe to call once the next clip's buffer is loaded.
   */
  enqueueNext(clipKey) {
    const k = String(clipKey || '').trim();
    const buffer = this._buffers.get(k);
    if (!buffer || !this._active || this._stopped) return false;
    if (this._scheduled && this._scheduled.key === k) return true; // already queued

    // When the active clip finishes (accounting for its play offset and rate).
    const remaining = (this._active.duration - this._active.offsetSec) / this._rate;
    const seamAt = this._active.startCtxTime + remaining;
    const source = this._makeSource(buffer);
    // If we're already past the computed seam (e.g. enqueued late), start ASAP.
    const startAt = Math.max(seamAt, this._ctx.currentTime);
    source.start(startAt, 0);
    const gen = this._generation;
    source.onended = () => this._handleSourceEnded(k, gen);

    this._scheduled = { key: k, source, startCtxTime: startAt, duration: buffer.duration };
    return true;
  }

  _handleSourceEnded(endedKey, gen) {
    if (gen !== this._generation || this._stopped) return; // stale/stopped
    // Only react to the ACTIVE clip ending (scheduled clip becomes active).
    if (!this._active || this._active.key !== endedKey) return;

    if (this._scheduled) {
      // Promote the scheduled clip to active — gapless, it's already sounding.
      this._active = {
        key: this._scheduled.key,
        source: this._scheduled.source,
        startCtxTime: this._scheduled.startCtxTime,
        offsetSec: 0,
        duration: this._scheduled.duration,
      };
      this._scheduled = null;
      if (typeof this._onActiveClipChange === 'function') {
        try { this._onActiveClipChange(this._active.key); } catch (_e) { /* ignore */ }
      }
    } else {
      // Nothing queued → playback finished.
      this._active = null;
      this._stopped = true;
      if (typeof this._onEnded === 'function') {
        try { this._onEnded(); } catch (_e) { /* ignore */ }
      }
    }
  }

  /** Position (ms) within the active clip, accounting for offset and rate. */
  getPositionMs() {
    if (!this._active || !this._ctx) return 0;
    const elapsedCtx = this._ctx.currentTime - this._active.startCtxTime;
    const pos = this._active.offsetSec + Math.max(0, elapsedCtx) * this._rate;
    return Math.max(0, Math.min(pos, this._active.duration)) * 1000;
  }

  getActiveClipKey() {
    return this._active ? this._active.key : null;
  }

  isPlaying() {
    return !this._stopped && this._ctx != null && this._ctx.state === 'running';
  }

  /** Pause the whole timeline (suspends the context — preserves scheduling). */
  async pause() {
    if (this._ctx && this._ctx.state === 'running') {
      try { await this._ctx.suspend(); } catch (_e) { /* ignore */ }
    }
  }

  async resume() {
    if (this._ctx && this._ctx.state === 'suspended') {
      try { await this._ctx.resume(); } catch (_e) { /* ignore */ }
    }
  }

  /** Restart the active clip at a new rate from the current position. */
  async setRate(rate) {
    const next = Number(rate) > 0 ? Number(rate) : 1;
    if (next === this._rate || !this._active) { this._rate = next; return; }
    const key = this._active.key;
    const offsetSec = this.getPositionMs() / 1000;
    await this.start(key, { offsetSec, rate: next });
  }

  /** Seek within the active clip. */
  async seek(offsetSec) {
    if (!this._active) return;
    await this.start(this._active.key, { offsetSec, rate: this._rate });
  }

  _stopAllSources() {
    for (const holder of [this._active, this._scheduled]) {
      if (holder && holder.source) {
        try { holder.source.onended = null; } catch (_e) { /* ignore */ }
        try { holder.source.stop(); } catch (_e) { /* ignore */ }
        try { holder.source.disconnect(); } catch (_e) { /* ignore */ }
      }
    }
    this._active = null;
    this._scheduled = null;
  }

  stop() {
    this._generation += 1; // invalidate any pending onended callbacks
    this._stopped = true;
    this._stopAllSources();
  }

  /** Free everything. The instance should not be reused after destroy(). */
  destroy() {
    this.stop();
    this._buffers.clear();
    this._decoding.clear();
    if (this._ctx) {
      try { this._ctx.close(); } catch (_e) { /* ignore */ }
      this._ctx = null;
      this._gain = null;
    }
  }
}

export default ReaderAudioGaplessEngine;
