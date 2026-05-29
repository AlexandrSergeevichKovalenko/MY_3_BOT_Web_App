import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';

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
  state_removed: 8,
  refs_removed: 3,
  effects_removed: 9,
};

export function useYouTubePlaybackController({
  isLightweightFreeMode,
  startupEffectiveMode,
  initData,
  resumeStorageKey,
  youtubeInputValueRef,
  youtubePlayerRef,
  youtubeSubtitlesRef,
  youtubeSectionVisible,
  youtubeTranscriptLength,
  youtubeTranslationEnabled,
  safeStorageSet,
  safeStorageGet,
  extractYoutubeId,
  setYoutubeForceShowPanel,
  onYoutubePlaybackStarted,
}) {
  if (typeof isLightweightFreeMode !== 'boolean') {
    throw new Error('useYouTubePlaybackController requires isLightweightFreeMode');
  }
  if (!startupEffectiveMode) {
    throw new Error('useYouTubePlaybackController requires startupEffectiveMode');
  }
  if (!youtubeInputValueRef || typeof youtubeInputValueRef !== 'object') {
    throw new Error('useYouTubePlaybackController requires youtubeInputValueRef');
  }
  if (!youtubePlayerRef || typeof youtubePlayerRef !== 'object') {
    throw new Error('useYouTubePlaybackController requires youtubePlayerRef');
  }
  if (!youtubeSubtitlesRef || typeof youtubeSubtitlesRef !== 'object') {
    throw new Error('useYouTubePlaybackController requires youtubeSubtitlesRef');
  }
  if (!resumeStorageKey) {
    throw new Error('useYouTubePlaybackController requires resumeStorageKey');
  }
  if (typeof safeStorageSet !== 'function') {
    throw new Error('useYouTubePlaybackController requires safeStorageSet');
  }
  if (typeof safeStorageGet !== 'function') {
    throw new Error('useYouTubePlaybackController requires safeStorageGet');
  }
  if (typeof extractYoutubeId !== 'function') {
    throw new Error('useYouTubePlaybackController requires extractYoutubeId');
  }
  if (typeof setYoutubeForceShowPanel !== 'function') {
    throw new Error('useYouTubePlaybackController requires setYoutubeForceShowPanel');
  }
  if (typeof onYoutubePlaybackStarted !== 'function') {
    throw new Error('useYouTubePlaybackController requires onYoutubePlaybackStarted');
  }

  const [youtubeId, setYoutubeId] = useState('');
  const [youtubePlayerReady, setYoutubePlayerReady] = useState(false);
  const [youtubeCurrentTime, setYoutubeCurrentTime] = useState(0);
  const [youtubeIsPaused, setYoutubeIsPaused] = useState(false);
  const [youtubePlaybackStarted, setYoutubePlaybackStarted] = useState(false);
  const [youtubeOverlayEnabled, setYoutubeOverlayEnabled] = useState(false);
  const [youtubeAppFullscreen, setYoutubeAppFullscreen] = useState(false);
  const [youtubeTranscriptHasTiming, setYoutubeTranscriptHasTiming] = useState(true);
  const youtubeCurrentTimeRef = useRef(0);
  const youtubeTimeIntervalRef = useRef(null);
  const youtubeResumeAppliedForVideoRef = useRef('');
  const youtubeResumeLastSavedSecondRef = useRef(-1);
  const youtubeResumeLastSyncedSecondRef = useRef(-1);

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

  const resetYoutubeResumeTracking = useCallback(() => {
    youtubeResumeAppliedForVideoRef.current = '';
    youtubeResumeLastSavedSecondRef.current = -1;
    youtubeResumeLastSyncedSecondRef.current = -1;
  }, []);

  useEffect(() => {
    if (isLightweightFreeMode) {
      console.info('provider_skipped_free_mode', {
        provider_name: 'youtube_playback_lifecycle',
        effective_mode: startupEffectiveMode,
      });
      return;
    }
    console.info('youtube_playback_state_initialized', {
      provider_name: 'youtube_playback',
      youtube_id_present: Boolean(youtubeId),
    });
    console.info('frontend_provider_extracted', {
      provider_name: 'youtube_playback_lifecycle',
      state_removed: 8,
      refs_removed: 3,
      effects_removed: 9,
    });
  }, [isLightweightFreeMode, startupEffectiveMode, youtubeId]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    console.info('youtube_playback_state_updated', {
      provider_name: 'youtube_playback',
      youtube_id_present: Boolean(youtubeId),
      player_ready: Boolean(youtubePlayerReady),
      playback_started: Boolean(youtubePlaybackStarted),
      paused: Boolean(youtubeIsPaused),
      current_time_bucket: Math.floor(Number(youtubeCurrentTime || 0)),
    });
  }, [isLightweightFreeMode, youtubeCurrentTime, youtubeId, youtubeIsPaused, youtubePlaybackStarted, youtubePlayerReady]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    youtubeCurrentTimeRef.current = Number(youtubeCurrentTime || 0);
  }, [isLightweightFreeMode, youtubeCurrentTime]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    resetYoutubeResumeTracking();
  }, [isLightweightFreeMode, resetYoutubeResumeTracking, youtubeId]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    const currentSecond = Math.max(0, Math.floor(Number(youtubeCurrentTime || 0)));
    if (youtubeId && currentSecond >= 0 && currentSecond % 3 === 0 && youtubeResumeLastSavedSecondRef.current !== currentSecond) {
      youtubeResumeLastSavedSecondRef.current = currentSecond;
      persistYoutubeResumeState(currentSecond);
    }
    if (youtubeId && currentSecond >= 0 && currentSecond % 9 === 0 && youtubeResumeLastSyncedSecondRef.current !== currentSecond) {
      youtubeResumeLastSyncedSecondRef.current = currentSecond;
      void syncYoutubeResumeState(currentSecond);
    }

    if (!youtubeSectionVisible && youtubeId) {
      void syncYoutubeResumeState();
    }
  }, [isLightweightFreeMode, persistYoutubeResumeState, syncYoutubeResumeState, youtubeCurrentTime, youtubeId, youtubeSectionVisible]);

  useEffect(() => {
    if (isLightweightFreeMode) return undefined;
    const onPageHide = () => {
      void syncYoutubeResumeState(undefined, { keepalive: true });
    };
    window.addEventListener('pagehide', onPageHide);
    return () => {
      window.removeEventListener('pagehide', onPageHide);
    };
  }, [isLightweightFreeMode, syncYoutubeResumeState]);

  useEffect(() => {
    if (isLightweightFreeMode) return undefined;
    if (!youtubeId) {
      setYoutubePlayerReady(false);
      setYoutubeCurrentTime(0);
      setYoutubePlaybackStarted(false);
      setYoutubeIsPaused(true);
      setYoutubeForceShowPanel(false);
      youtubeResumeAppliedForVideoRef.current = '';
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
        console.info('youtube_playback_polling_stopped', {
          provider_name: 'youtube_playback_lifecycle',
          youtube_id_present: false,
        });
      }
      if (youtubePlayerRef.current && youtubePlayerRef.current.destroy) {
        youtubePlayerRef.current.destroy();
        youtubePlayerRef.current = null;
      }
      return undefined;
    }
    if (!youtubeSectionVisible) {
      setYoutubeIsPaused(true);
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
        console.info('youtube_playback_polling_stopped', {
          provider_name: 'youtube_playback_lifecycle',
          youtube_id_present: true,
        });
      }
      return undefined;
    }

    const ensureApiReady = () => new Promise((resolve) => {
      if (window.YT && window.YT.Player) {
        resolve();
        return;
      }
      const existing = document.querySelector('script[data-youtube-iframe]');
      if (existing) {
        const handler = () => resolve();
        window.onYouTubeIframeAPIReady = handler;
        return;
      }
      const script = document.createElement('script');
      script.src = 'https://www.youtube.com/iframe_api';
      script.async = true;
      script.dataset.youtubeIframe = '1';
      window.onYouTubeIframeAPIReady = () => resolve();
      document.body.appendChild(script);
      console.info('youtube_player_initialized', {
        provider_name: 'youtube_playback_lifecycle',
        youtube_id_present: true,
      });
    });

    let cancelled = false;
    ensureApiReady().then(() => {
      if (cancelled) return;
      if (!window.YT || !window.YT.Player) return;
      const hostNode = document.getElementById('youtube-player');
      if (!hostNode) return;
      if (youtubePlayerRef.current && youtubePlayerRef.current.destroy) {
        youtubePlayerRef.current.destroy();
      }
      youtubePlayerRef.current = new window.YT.Player(hostNode, {
        videoId: youtubeId,
        playerVars: {
          rel: 0,
          modestbranding: 1,
          fs: 0,
          disablekb: 1,
          playsinline: 1,
        },
        events: {
          onReady: () => {
            setYoutubePlayerReady(true);
            setYoutubeIsPaused(true);
            setYoutubePlaybackStarted(false);
            console.info('youtube_player_ready', {
              provider_name: 'youtube_playback_lifecycle',
              youtube_id_present: true,
            });
            try {
              const stored = safeStorageGet(resumeStorageKey) || safeStorageGet('webapp_youtube');
              if (stored) {
                const parsed = JSON.parse(stored);
                const savedId = String(parsed?.id || '').trim();
                const savedTime = Math.max(0, Number(parsed?.currentTime || 0));
                if (
                  savedId === youtubeId
                  && savedTime >= 2
                  && youtubeResumeAppliedForVideoRef.current !== youtubeId
                ) {
                  youtubePlayerRef.current?.seekTo?.(savedTime, true);
                  setYoutubeCurrentTime(savedTime);
                  youtubeResumeAppliedForVideoRef.current = youtubeId;
                }
              }
            } catch (_error) {
              // ignore
            }
            if (youtubeTimeIntervalRef.current) {
              clearInterval(youtubeTimeIntervalRef.current);
            }
            youtubeTimeIntervalRef.current = setInterval(() => {
              try {
                const time = youtubePlayerRef.current?.getCurrentTime?.();
                if (typeof time === 'number' && !Number.isNaN(time)) {
                  setYoutubeCurrentTime(time);
                }
              } catch (_error) {
                // ignore
              }
            }, 400);
            console.info('youtube_playback_polling_started', {
              provider_name: 'youtube_playback_lifecycle',
              youtube_id_present: true,
            });
          },
          onStateChange: (stateEvent) => {
            const state = stateEvent?.data;
            if (state === 2) {
              setYoutubeIsPaused(true);
              console.info('youtube_playback_paused', {
                provider_name: 'youtube_playback_lifecycle',
                youtube_id_present: true,
              });
              try {
                const time = youtubePlayerRef.current?.getCurrentTime?.();
                if (typeof time === 'number' && !Number.isNaN(time)) {
                  setYoutubeCurrentTime(time);
                  void syncYoutubeResumeState(time);
                }
              } catch (_error) {
                // ignore
              }
            } else if (state === 1) {
              onYoutubePlaybackStarted();
              setYoutubeIsPaused(false);
              setYoutubePlaybackStarted(true);
              setYoutubeForceShowPanel(false);
              console.info('youtube_playback_started', {
                provider_name: 'youtube_playback_lifecycle',
                youtube_id_present: true,
              });
              console.info('youtube_playback_resumed', {
                provider_name: 'youtube_playback_lifecycle',
                youtube_id_present: true,
              });
            } else if (state === 3) {
              setYoutubeIsPaused(false);
              console.info('youtube_playback_resumed', {
                provider_name: 'youtube_playback_lifecycle',
                youtube_id_present: true,
              });
            } else if (state === 0 || state === 5 || state === -1) {
              setYoutubeIsPaused(true);
            }
          },
        },
      });
    });

    return () => {
      cancelled = true;
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
        console.info('youtube_playback_polling_stopped', {
          provider_name: 'youtube_playback_lifecycle',
          youtube_id_present: Boolean(youtubeId),
        });
      }
    };
  }, [
    resumeStorageKey,
    onYoutubePlaybackStarted,
    safeStorageGet,
    setYoutubeForceShowPanel,
    syncYoutubeResumeState,
    isLightweightFreeMode,
    youtubeId,
    youtubePlayerRef,
    youtubeSectionVisible,
  ]);

  useEffect(() => {
    if (isLightweightFreeMode) return undefined;
    if (!youtubePlayerReady || !youtubeId || !initData || !youtubePlayerRef.current?.seekTo) return undefined;
    let cancelled = false;
    fetch('/api/webapp/youtube/state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ initData, videoId: youtubeId }),
    })
      .then(async (response) => {
        if (!response.ok) throw new Error(await response.text());
        return response.json();
      })
      .then((data) => {
        if (cancelled) return;
        const state = data?.state;
        const savedId = String(state?.video_id || '').trim();
        const savedTime = Math.max(0, Number(state?.current_time_seconds || 0));
        if (savedId !== youtubeId || savedTime < 2) return;
        const localRaw = safeStorageGet(resumeStorageKey) || safeStorageGet('webapp_youtube');
        let localTime = 0;
        try {
          const parsed = localRaw ? JSON.parse(localRaw) : null;
          if (String(parsed?.id || '').trim() === youtubeId) {
            localTime = Math.max(0, Number(parsed?.currentTime || 0));
          }
        } catch (_error) {
          localTime = 0;
        }
        writeYoutubeResumeToLocalCache({
          input: String(state?.input_text || '').trim() || `https://youtu.be/${youtubeId}`,
          id: youtubeId,
          currentTime: Math.max(localTime, savedTime),
          updatedAt: Date.now(),
        });
        if (savedTime > (youtubeCurrentTimeRef.current + 1)) {
          youtubePlayerRef.current?.seekTo?.(savedTime, true);
          setYoutubeCurrentTime(savedTime);
          youtubeResumeAppliedForVideoRef.current = youtubeId;
        }
      })
      .catch(() => {
        // ignore server resume lookup errors
      });
    return () => {
      cancelled = true;
    };
  }, [
    initData,
    isLightweightFreeMode,
    resumeStorageKey,
    safeStorageGet,
    writeYoutubeResumeToLocalCache,
    youtubeId,
    youtubePlayerReady,
    youtubePlayerRef,
  ]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    if (youtubeTranscriptLength > 0 && youtubeSubtitlesRef.current) {
      youtubeSubtitlesRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, [isLightweightFreeMode, youtubeSubtitlesRef, youtubeTranscriptLength]);

  useEffect(() => {
    if (isLightweightFreeMode) return undefined;
    if (!youtubeAppFullscreen) return undefined;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prevOverflow;
    };
  }, [isLightweightFreeMode, youtubeAppFullscreen]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    if (!youtubeAppFullscreen) {
      setYoutubeIsPaused(false);
    }
  }, [isLightweightFreeMode, youtubeAppFullscreen]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    if (!youtubeSubtitlesRef.current) return;
    const listEl = youtubeSubtitlesRef.current.querySelector('.webapp-subtitles-list');
    const activeEl = youtubeSubtitlesRef.current.querySelector('.webapp-subtitles-list .is-active');
    if (listEl && activeEl) {
      const listRect = listEl.getBoundingClientRect();
      const activeRect = activeEl.getBoundingClientRect();
      const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
      listEl.scrollTop += offset;
    }
  }, [isLightweightFreeMode, youtubeCurrentTime, youtubeSubtitlesRef, youtubeTranscriptLength]);

  useEffect(() => {
    if (isLightweightFreeMode) return undefined;
    if (!youtubeSectionVisible) return undefined;
    if (!youtubeSubtitlesRef.current || !youtubeTranscriptLength) return undefined;
    const raf = requestAnimationFrame(() => {
      const listEl = youtubeSubtitlesRef.current?.querySelector('.webapp-subtitles-list');
      const activeEl = youtubeSubtitlesRef.current?.querySelector('.webapp-subtitles-list .is-active');
      if (listEl && activeEl) {
        const listRect = listEl.getBoundingClientRect();
        const activeRect = activeEl.getBoundingClientRect();
        const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
        listEl.scrollTop += offset;
      }
    });
    return () => cancelAnimationFrame(raf);
  }, [isLightweightFreeMode, youtubeOverlayEnabled, youtubeSectionVisible, youtubeSubtitlesRef, youtubeTranscriptLength, youtubeTranslationEnabled]);

  useEffect(() => {
    if (isLightweightFreeMode) return;
    const translationListRef = document.querySelector('.webapp-subtitles.is-translation .webapp-subtitles-list');
    if (!translationListRef) return;
    const activeEl = translationListRef.querySelector('.is-active');
    if (activeEl) {
      const listRect = translationListRef.getBoundingClientRect();
      const activeRect = activeEl.getBoundingClientRect();
      const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
      translationListRef.scrollTop += offset;
    }
  }, [isLightweightFreeMode, youtubeCurrentTime, youtubeTranscriptLength, youtubeTranslationEnabled]);

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
    youtubeOverlayEnabled,
    setYoutubeOverlayEnabled,
    youtubeAppFullscreen,
    setYoutubeAppFullscreen,
    youtubeTranscriptHasTiming,
    setYoutubeTranscriptHasTiming,
    youtubeResumeStorageKey: resumeStorageKey,
    youtubeCurrentTimeRef,
    resetYoutubeResumeTracking,
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
  youtubeOverlayEnabled,
  youtubeAppFullscreen,
  youtubeTranscriptHasTiming,
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
    youtubeOverlayEnabled: Boolean(requirePlaybackValue('youtubeOverlayEnabled', youtubeOverlayEnabled)),
    youtubeAppFullscreen: Boolean(requirePlaybackValue('youtubeAppFullscreen', youtubeAppFullscreen)),
    youtubeTranscriptHasTiming: Boolean(requirePlaybackValue('youtubeTranscriptHasTiming', youtubeTranscriptHasTiming)),
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
    youtubeOverlayEnabled,
    youtubeAppFullscreen,
    youtubeTranscriptHasTiming,
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
