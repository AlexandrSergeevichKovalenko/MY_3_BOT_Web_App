import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

export const YouTubePlaybackContext = createContext(null);

function requirePlaybackValue(name, value) {
  if (value === undefined || value === null) {
    throw new Error(`YouTubePlaybackProvider requires ${name}`);
  }
  return value;
}

export const YOUTUBE_PLAYBACK_PROVIDER_SCAFFOLD = {
  provider_name: 'youtube_playback',
  refs_owned_by_app: true,
  scaffold_only: false,
  state_removed: 5,
  refs_removed: 0,
  effects_removed: 0,
};

export function useYouTubePlaybackController({
  initData,
  resumeStorageKey,
  youtubeInputValueRef,
  youtubeCurrentTimeRef,
  safeStorageSet,
  extractYoutubeId,
}) {
  if (!youtubeInputValueRef || typeof youtubeInputValueRef !== 'object') {
    throw new Error('useYouTubePlaybackController requires youtubeInputValueRef');
  }
  if (!youtubeCurrentTimeRef || typeof youtubeCurrentTimeRef !== 'object') {
    throw new Error('useYouTubePlaybackController requires youtubeCurrentTimeRef');
  }
  if (!resumeStorageKey) {
    throw new Error('useYouTubePlaybackController requires resumeStorageKey');
  }
  if (typeof safeStorageSet !== 'function') {
    throw new Error('useYouTubePlaybackController requires safeStorageSet');
  }
  if (typeof extractYoutubeId !== 'function') {
    throw new Error('useYouTubePlaybackController requires extractYoutubeId');
  }

  const [youtubeId, setYoutubeId] = useState('');
  const [youtubePlayerReady, setYoutubePlayerReady] = useState(false);
  const [youtubeCurrentTime, setYoutubeCurrentTime] = useState(0);
  const [youtubeIsPaused, setYoutubeIsPaused] = useState(false);
  const [youtubePlaybackStarted, setYoutubePlaybackStarted] = useState(false);

  const writeYoutubeResumeToLocalCache = useCallback((payload) => {
    if (!payload || typeof payload !== 'object') return;
    const serialized = JSON.stringify(payload);
    safeStorageSet(resumeStorageKey, serialized);
    safeStorageSet('webapp_youtube', serialized);
    console.info('youtube_resume_state_saved', {
      provider_name: 'youtube_playback',
      youtube_id_present: Boolean(String(payload?.id || '').trim()),
      local_only: true,
    });
  }, [resumeStorageKey, safeStorageSet]);

  const persistYoutubeResumeState = useCallback((timeValue) => {
    const trimmed = String(youtubeInputValueRef.current || '').trim();
    const resolvedId = String(youtubeId || extractYoutubeId(trimmed) || '').trim();
    if (!trimmed || !resolvedId) return;
    const sourceTime = timeValue ?? youtubeCurrentTimeRef.current;
    const safeTime = Math.max(0, Math.floor(Number(sourceTime || 0)));
    writeYoutubeResumeToLocalCache({
      input: trimmed,
      id: resolvedId,
      currentTime: safeTime,
      updatedAt: Date.now(),
    });
  }, [extractYoutubeId, writeYoutubeResumeToLocalCache, youtubeCurrentTimeRef, youtubeId, youtubeInputValueRef]);

  const syncYoutubeResumeState = useCallback(async (timeValue, options = {}) => {
    const trimmed = String(youtubeInputValueRef.current || '').trim();
    const resolvedId = String(youtubeId || extractYoutubeId(trimmed) || '').trim();
    if (!trimmed || !resolvedId || !initData) return;
    const sourceTime = timeValue ?? youtubeCurrentTimeRef.current;
    const safeTime = Math.max(0, Math.floor(Number(sourceTime || 0)));
    persistYoutubeResumeState(safeTime);
    try {
      await fetch('/api/webapp/youtube/state', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          videoId: resolvedId,
          input: trimmed,
          current_time_seconds: safeTime,
        }),
        keepalive: Boolean(options?.keepalive),
      });
      console.info('youtube_resume_state_saved', {
        provider_name: 'youtube_playback',
        youtube_id_present: true,
        local_only: false,
      });
    } catch (_error) {
      // ignore sync errors; local cache already has the latest position
    }
  }, [
    extractYoutubeId,
    initData,
    persistYoutubeResumeState,
    youtubeCurrentTimeRef,
    youtubeId,
    youtubeInputValueRef,
  ]);

  useEffect(() => {
    console.info('youtube_playback_state_initialized', {
      provider_name: 'youtube_playback',
      youtube_id_present: Boolean(youtubeId),
    });
    console.info('frontend_provider_extracted', {
      provider_name: 'youtube_playback',
      state_removed: 5,
      refs_removed: 0,
      effects_removed: 0,
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    console.info('youtube_playback_state_updated', {
      provider_name: 'youtube_playback',
      youtube_id_present: Boolean(youtubeId),
      player_ready: Boolean(youtubePlayerReady),
      playback_started: Boolean(youtubePlaybackStarted),
      paused: Boolean(youtubeIsPaused),
      current_time_bucket: Math.floor(Number(youtubeCurrentTime || 0)),
    });
  }, [youtubeCurrentTime, youtubeId, youtubeIsPaused, youtubePlaybackStarted, youtubePlayerReady]);

  return {
    youtubeId,
    setYoutubeId,
    youtubePlayerReady,
    setYoutubePlayerReady,
    youtubeCurrentTime,
    setYoutubeCurrentTime,
    youtubeIsPaused,
    setYoutubeIsPaused,
    youtubePlaybackStarted,
    setYoutubePlaybackStarted,
    youtubeResumeStorageKey: resumeStorageKey,
    writeYoutubeResumeToLocalCache,
    persistYoutubeResumeState,
    syncYoutubeResumeState,
  };
}

export function YouTubePlaybackProvider({
  isLightweightFreeMode,
  startupEffectiveMode,
  youtubeId,
  youtubePlayerReady,
  youtubeCurrentTime,
  youtubeIsPaused,
  youtubePlaybackStarted,
  youtubeTranscriptLength,
  youtubeResumeStorageKey,
  persistYoutubeResumeState,
  syncYoutubeResumeState,
  refsOwnedByApp,
  value,
  children,
}) {
  if (isLightweightFreeMode) {
    console.info('provider_skipped_free_mode', {
      provider_name: 'youtube_playback',
      effective_mode: startupEffectiveMode,
      refs_owned_by_app: true,
    });
    return null;
  }
  if (refsOwnedByApp !== true) {
    throw new Error('YouTubePlaybackProvider requires refsOwnedByApp=true during scaffold phase');
  }
  if (!value || typeof value !== 'object') {
    throw new Error('YouTubePlaybackProvider requires explicit context value');
  }

  const normalizedYoutubeId = String(requirePlaybackValue('youtubeId', youtubeId) || '');
  const contextValue = useMemo(() => ({
    ...value,
    youtubeId: normalizedYoutubeId,
    youtubePlayerReady: Boolean(requirePlaybackValue('youtubePlayerReady', youtubePlayerReady)),
    youtubeCurrentTime: Number(requirePlaybackValue('youtubeCurrentTime', youtubeCurrentTime) || 0),
    youtubeIsPaused: Boolean(requirePlaybackValue('youtubeIsPaused', youtubeIsPaused)),
    youtubePlaybackStarted: Boolean(requirePlaybackValue('youtubePlaybackStarted', youtubePlaybackStarted)),
    youtubeTranscriptLength: Number(requirePlaybackValue('youtubeTranscriptLength', youtubeTranscriptLength) || 0),
    youtubeResumeStorageKey: requirePlaybackValue('youtubeResumeStorageKey', youtubeResumeStorageKey),
    persistYoutubeResumeState: requirePlaybackValue('persistYoutubeResumeState', persistYoutubeResumeState),
    syncYoutubeResumeState: requirePlaybackValue('syncYoutubeResumeState', syncYoutubeResumeState),
    refsOwnedByApp: true,
  }), [
    normalizedYoutubeId,
    persistYoutubeResumeState,
    syncYoutubeResumeState,
    value,
    youtubeCurrentTime,
    youtubeIsPaused,
    youtubePlaybackStarted,
    youtubePlayerReady,
    youtubeResumeStorageKey,
    youtubeTranscriptLength,
  ]);

  useEffect(() => {
    const payload = {
      provider_name: 'youtube_playback',
      effective_mode: startupEffectiveMode,
      youtube_id_present: Boolean(normalizedYoutubeId),
      refs_owned_by_app: true,
    };
    console.info('provider_mount', payload);
    console.info('youtube_playback_provider_scaffold_created', payload);
    console.info('youtube_playback_boundary_mount', payload);
    return () => {
      console.info('provider_unmount', payload);
      console.info('youtube_playback_boundary_unmount', payload);
    };
  }, [normalizedYoutubeId, startupEffectiveMode]);

  return (
    <YouTubePlaybackContext.Provider value={contextValue}>
      {children}
    </YouTubePlaybackContext.Provider>
  );
}

export function useYouTubePlaybackContext() {
  const context = useContext(YouTubePlaybackContext);
  if (!context) {
    throw new Error('YouTubePlaybackContext is required');
  }
  return context;
}

export default YouTubePlaybackProvider;
