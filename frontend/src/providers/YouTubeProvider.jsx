import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';

export const YouTubeContext = createContext(null);

export const YOUTUBE_PROVIDER_EXTRACTION_METRICS = {
  provider_name: 'youtube_search',
  state_removed: 8,
  refs_removed: 1,
  effects_removed: 0,
  callbacks_removed: 12,
  memos_removed: 0,
  extraction_mode: 'search_state',
};

function requireProviderInput(name, value) {
  if (value === undefined || value === null) {
    throw new Error(`YouTubeProvider requires ${name}`);
  }
  return value;
}

function parseTranscriptInput(value) {
  const lines = value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !/^\d+$/.test(line))
    .filter((line) => !/^\d{2}:\d{2}:\d{2}/.test(line))
    .filter((line) => !/^\d{2}:\d{2}/.test(line))
    .filter((line) => !/-->/.test(line))
    .filter((line) => !/^WEBVTT/i.test(line));
  return lines.map((line, index) => ({
    text: line,
    start: index,
    duration: 0,
  }));
}

function parseTimedTranscript(value) {
  const lines = value.split(/\r?\n/);
  const items = [];
  let currentStart = null;
  let buffer = [];
  const flush = () => {
    if (currentStart !== null && buffer.length) {
      items.push({ start: currentStart, text: buffer.join(' ').trim() });
    }
    buffer = [];
  };
  const timeToSeconds = (stamp) => {
    const clean = stamp.replace(',', '.');
    const parts = clean.split(':').map(Number);
    if (parts.some((p) => Number.isNaN(p))) return null;
    if (parts.length === 3) {
      return parts[0] * 3600 + parts[1] * 60 + parts[2];
    }
    if (parts.length === 2) {
      return parts[0] * 60 + parts[1];
    }
    return null;
  };
  let hasTiming = false;
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      flush();
      currentStart = null;
      continue;
    }
    if (/^WEBVTT/i.test(line) || /^\d+$/.test(line)) {
      continue;
    }
    if (line.includes('-->')) {
      flush();
      const startPart = line.split('-->')[0].trim();
      const seconds = timeToSeconds(startPart);
      if (seconds !== null) {
        currentStart = seconds;
        hasTiming = true;
      } else {
        currentStart = null;
      }
      continue;
    }
    buffer.push(line);
  }
  flush();
  return { items, hasTiming };
}

function parseSimpleTimestampTranscript(value) {
  const lines = value.split(/\r?\n/).map((line) => line.trim());
  const items = [];
  let hasTiming = false;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    if (/^\d{1,2}:\d{2}$/.test(line)) {
      const parts = line.split(':').map(Number);
      if (parts.length === 2 && !parts.some((p) => Number.isNaN(p))) {
        const start = parts[0] * 60 + parts[1];
        const text = (lines[i + 1] || '').trim();
        if (text) {
          items.push({ start, text });
          hasTiming = true;
        }
      }
    }
  }
  return { items, hasTiming };
}

function detectTranscriptLanguage(items) {
  const sample = items.map((item) => String(item?.text || '')).join(' ');
  if (!sample) return null;
  const hasCyrillic = /[А-Яа-яЁё]/.test(sample);
  const hasLatin = /[A-Za-z]/.test(sample);
  if (hasCyrillic && !hasLatin) return 'ru';
  if (hasLatin && !hasCyrillic) return 'de';
  if (hasCyrillic && hasLatin) return 'en';
  return null;
}

export function YouTubeProvider({
  isLightweightFreeMode,
  startupEffectiveMode,
  initData,
  initDataMissingMsg,
  youtubeId,
  languageProfile,
  normalizeLangCode,
  extractYoutubeId,
  getWebappLanguagePairHint,
  tr,
  setYoutubeId,
  setYoutubeTranscript,
  setYoutubeTranslations,
  setYoutubeManualOverride,
  setYoutubeTranscriptError,
  setYoutubeTranscriptLoading,
  setYoutubeTranscriptHasTiming,
  onYoutubeInputChanged,
  onYoutubeInputCleared,
  onYoutubeVideoResolved,
  onManualTranscriptSaved,
  pendingCommand,
  onPendingCommandHandled,
  value,
  children,
}) {
  if (isLightweightFreeMode) {
    console.info('provider_skipped_free_mode', {
      provider_name: 'youtube',
      effective_mode: startupEffectiveMode,
    });
    return null;
  }
  if (!value || typeof value !== 'object') {
    throw new Error('YouTubeProvider requires explicit context value');
  }
  const translate = requireProviderInput('tr', tr);
  const normalizeLanguageCode = requireProviderInput('normalizeLangCode', normalizeLangCode);
  const resolveYoutubeId = requireProviderInput('extractYoutubeId', extractYoutubeId);
  const resolveLanguagePairHint = requireProviderInput('getWebappLanguagePairHint', getWebappLanguagePairHint);
  const updateYoutubeId = requireProviderInput('setYoutubeId', setYoutubeId);
  const updateYoutubeTranscript = requireProviderInput('setYoutubeTranscript', setYoutubeTranscript);
  const updateYoutubeTranslations = requireProviderInput('setYoutubeTranslations', setYoutubeTranslations);
  const updateYoutubeManualOverride = requireProviderInput('setYoutubeManualOverride', setYoutubeManualOverride);
  const updateYoutubeTranscriptError = requireProviderInput('setYoutubeTranscriptError', setYoutubeTranscriptError);
  const updateYoutubeTranscriptLoading = requireProviderInput('setYoutubeTranscriptLoading', setYoutubeTranscriptLoading);
  const updateYoutubeTranscriptHasTiming = requireProviderInput('setYoutubeTranscriptHasTiming', setYoutubeTranscriptHasTiming);
  const notifyYoutubeInputChanged = requireProviderInput('onYoutubeInputChanged', onYoutubeInputChanged);
  const notifyYoutubeInputCleared = requireProviderInput('onYoutubeInputCleared', onYoutubeInputCleared);
  const notifyYoutubeVideoResolved = requireProviderInput('onYoutubeVideoResolved', onYoutubeVideoResolved);
  const notifyManualTranscriptSaved = requireProviderInput('onManualTranscriptSaved', onManualTranscriptSaved);
  const markPendingCommandHandled = requireProviderInput('onPendingCommandHandled', onPendingCommandHandled);

  const [youtubeInput, setYoutubeInput] = useState('');
  const [youtubeError, setYoutubeError] = useState('');
  const [youtubeEmptyState, setYoutubeEmptyState] = useState(null);
  const [youtubeSearchLoading, setYoutubeSearchLoading] = useState(false);
  const [youtubeSearchResults, setYoutubeSearchResults] = useState([]);
  const [youtubeSearchError, setYoutubeSearchError] = useState('');
  const [youtubeRecommendationLoading, setYoutubeRecommendationLoading] = useState(false);
  const [showManualTranscript, setShowManualTranscript] = useState(false);
  const [manualTranscript, setManualTranscript] = useState('');
  const youtubeInputDraftRef = useRef('');
  const handledPendingCommandRef = useRef('');

  const commitYoutubeInputDraft = useCallback((nextValue = youtubeInputDraftRef.current) => {
    const normalized = String(nextValue ?? '');
    youtubeInputDraftRef.current = normalized;
    setYoutubeInput((previous) => (previous === normalized ? previous : normalized));
    return normalized;
  }, []);

  const setYoutubeInputDraft = useCallback((nextValue) => {
    youtubeInputDraftRef.current = String(nextValue ?? '');
  }, []);

  useEffect(() => {
    youtubeInputDraftRef.current = String(youtubeInput || '');
    notifyYoutubeInputChanged(String(youtubeInput || ''));
  }, [notifyYoutubeInputChanged, youtubeInput]);

  useEffect(() => {
    const trimmed = youtubeInput.trim();
    if (!trimmed) {
      updateYoutubeId('');
      setYoutubeError('');
      setYoutubeSearchError('');
      setYoutubeSearchResults([]);
      notifyYoutubeInputCleared();
      return;
    }
    const id = resolveYoutubeId(trimmed);
    if (id) {
      updateYoutubeId(id);
      setYoutubeError('');
      setYoutubeEmptyState(null);
      notifyYoutubeVideoResolved({ input: trimmed, id });
    } else if (/(youtube\.com|youtu\.be|^https?:\/\/)/i.test(trimmed)) {
      setYoutubeError(translate('Не удалось распознать ссылку или ID видео.', 'Video-Link oder ID konnte nicht erkannt werden.'));
      updateYoutubeId('');
    } else {
      setYoutubeError('');
      updateYoutubeId('');
    }
  }, [
    notifyYoutubeInputCleared,
    notifyYoutubeVideoResolved,
    resolveYoutubeId,
    translate,
    updateYoutubeId,
    youtubeInput,
  ]);

  const searchYoutubeVideos = useCallback(async (overrideQuery = null) => {
    const committedInput = commitYoutubeInputDraft(
      overrideQuery == null ? youtubeInputDraftRef.current : overrideQuery
    );
    const query = committedInput.trim();
    if (!query) return;
    if (!initData) {
      setYoutubeSearchError(initDataMissingMsg);
      return;
    }

    const directId = resolveYoutubeId(query);
    if (directId) {
      setYoutubeSearchError('');
      setYoutubeSearchResults([]);
      return;
    }

    setYoutubeSearchLoading(true);
    setYoutubeSearchError('');
    console.info('youtube_search_started', {
      provider_name: 'youtube',
      effective_mode: startupEffectiveMode,
    });
    try {
      const response = await fetch('/api/webapp/youtube/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          query,
          limit: 8,
          language_pair: resolveLanguagePairHint() || undefined,
        }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      const items = Array.isArray(data.items) ? data.items : [];
      setYoutubeSearchResults(items);
      if (!items.length) {
        setYoutubeSearchError(translate('По вашему запросу ничего не найдено.', 'Keine Ergebnisse fuer diese Suche.'));
      }
      console.info('youtube_search_completed', {
        provider_name: 'youtube',
        effective_mode: startupEffectiveMode,
        result_count: items.length,
      });
    } catch (error) {
      setYoutubeSearchResults([]);
      setYoutubeSearchError(`${translate('Ошибка поиска YouTube', 'YouTube-Suchfehler')}: ${error.message}`);
      console.info('youtube_search_failed', {
        provider_name: 'youtube',
        effective_mode: startupEffectiveMode,
        error: error.message,
      });
    } finally {
      setYoutubeSearchLoading(false);
    }
  }, [
    commitYoutubeInputDraft,
    initData,
    initDataMissingMsg,
    resolveLanguagePairHint,
    resolveYoutubeId,
    startupEffectiveMode,
    translate,
  ]);

  const selectYoutubeSearchResult = useCallback((item) => {
    const videoId = String(item?.video_id || '').trim();
    const videoUrl = String(item?.video_url || '').trim();
    if (!videoId && !videoUrl) {
      throw new Error('selectYoutubeSearchResult requires video_id or video_url');
    }
    setYoutubeInput(videoUrl || `https://youtu.be/${videoId}`);
    setYoutubeSearchResults([]);
    setYoutubeSearchError('');
  }, []);

  const executeYoutubeCommand = useCallback((command) => {
    if (!command || typeof command !== 'object') {
      throw new Error('YouTube command bridge requires command object');
    }
    const normalizedCommand = String(command?.command_name || '').trim();
    const sourceFeature = String(command?.source_feature || '').trim();
    const requestId = String(command?.request_id || '').trim();
    if (!normalizedCommand) {
      throw new Error('YouTube command bridge requires command_name');
    }
    if (!sourceFeature) {
      throw new Error(`YouTube command bridge requires source_feature for ${normalizedCommand}`);
    }
    if (!requestId) {
      throw new Error(`YouTube command bridge requires request_id for ${normalizedCommand}`);
    }
    const logBase = {
      source_feature: sourceFeature,
      command_name: normalizedCommand,
      effective_mode: startupEffectiveMode,
      request_id: requestId,
    };
    console.info('youtube_command_bridge_request', logBase);
    const resolveVideoInput = () => {
      const videoUrl = String(command?.video_url || '').trim();
      const videoId = String(command?.video_id || '').trim();
      if (videoUrl) return videoUrl;
      if (videoId) return `https://youtu.be/${videoId}`;
      return '';
    };
    try {
      if (normalizedCommand === 'request_video_task_start') {
        setYoutubeSearchResults([]);
        setYoutubeSearchError('');
        setYoutubeError('');
        setYoutubeEmptyState(null);
        setYoutubeRecommendationLoading(true);
        setYoutubeInput(resolveVideoInput());
      } else if (normalizedCommand === 'request_video_selection') {
        const nextInput = resolveVideoInput();
        if (!nextInput) {
          throw new Error('request_video_selection requires video_url or video_id');
        }
        setYoutubeError('');
        setYoutubeEmptyState(null);
        setYoutubeInput(nextInput);
      } else if (normalizedCommand === 'request_video_empty_state') {
        const emptyState = command?.empty_state && typeof command.empty_state === 'object' ? command.empty_state : null;
        if (!emptyState) {
          throw new Error('request_video_empty_state requires empty_state');
        }
        setYoutubeError('');
        setYoutubeEmptyState(emptyState);
      } else if (normalizedCommand === 'request_video_error') {
        const message = String(command?.message || '').trim();
        if (!message) {
          throw new Error('request_video_error requires message');
        }
        setYoutubeEmptyState(null);
        setYoutubeError(message);
      } else if (normalizedCommand === 'finish_video_recommendation') {
        setYoutubeRecommendationLoading(false);
      } else {
        throw new Error(`Unknown YouTube command: ${normalizedCommand}`);
      }
      console.info('youtube_command_bridge_success', logBase);
      return { ok: true, request_id: requestId };
    } catch (error) {
      console.info('youtube_command_bridge_failure', {
        ...logBase,
        error: error.message,
      });
      throw error;
    }
  }, [startupEffectiveMode]);

  useEffect(() => {
    if (!pendingCommand) return;
    const requestId = String(pendingCommand?.request_id || '').trim();
    if (!requestId) {
      throw new Error('YouTubeProvider pendingCommand requires request_id');
    }
    if (handledPendingCommandRef.current === requestId) return;
    handledPendingCommandRef.current = requestId;
    try {
      executeYoutubeCommand(pendingCommand);
    } finally {
      markPendingCommandHandled(requestId);
    }
  }, [executeYoutubeCommand, markPendingCommandHandled, pendingCommand]);

  const applyYoutubeTranscriptPayload = useCallback((data) => {
    const items = data?.items || [];
    updateYoutubeTranscript(items);
    updateYoutubeTranslations(data?.translations || {});
    const hasTiming = items.some((item) => Number(item?.start) > 0);
    updateYoutubeTranscriptHasTiming(hasTiming);
    setManualTranscript('');
  }, [updateYoutubeTranscript, updateYoutubeTranscriptHasTiming, updateYoutubeTranslations]);

  const pollYoutubeTranscriptStatus = useCallback(async ({ videoId, lang }) => {
    const maxAttempts = 25;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, attempt === 0 ? 800 : 1200));
      const response = await fetch('/api/webapp/youtube/transcript/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          videoId,
          lang,
        }),
      });
      if (response.status === 202) {
        continue;
      }
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (_error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      return await response.json();
    }
    throw new Error(translate('Субтитры всё ещё подготавливаются. Попробуйте ещё раз.', 'Untertitel werden noch vorbereitet. Bitte erneut versuchen.'));
  }, [initData, translate]);

  const fetchTranscript = useCallback(async (manualOverride) => {
    if (!youtubeId) return;
    if (!initData) {
      updateYoutubeTranscriptError(initDataMissingMsg);
      return;
    }
    if (manualOverride) return;
    updateYoutubeTranscriptLoading(true);
    updateYoutubeTranscriptError('');
    console.info('youtube_transcript_fetch_started', {
      provider_name: 'youtube',
      video_id: youtubeId,
      effective_mode: startupEffectiveMode,
    });
    try {
      const requestedLang = normalizeLanguageCode(languageProfile?.learning_language) || 'de';
      const response = await fetch('/api/webapp/youtube/transcript', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          videoId: youtubeId,
          lang: requestedLang,
        }),
      });
      if (response.status === 202) {
        const data = await pollYoutubeTranscriptStatus({ videoId: youtubeId, lang: requestedLang });
        applyYoutubeTranscriptPayload(data);
        console.info('youtube_transcript_fetch_completed', {
          provider_name: 'youtube',
          video_id: youtubeId,
          status: 202,
          effective_mode: startupEffectiveMode,
        });
        return;
      }
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      applyYoutubeTranscriptPayload(data);
      console.info('youtube_transcript_fetch_completed', {
        provider_name: 'youtube',
        video_id: youtubeId,
        status: response.status,
        effective_mode: startupEffectiveMode,
      });
    } catch (error) {
      updateYoutubeTranscript([]);
      updateYoutubeTranscriptError(`${translate('Авто-субтитры недоступны', 'Auto-Untertitel nicht verfuegbar')}: ${error.message}`);
      console.info('youtube_transcript_fetch_failed', {
        provider_name: 'youtube',
        video_id: youtubeId,
        effective_mode: startupEffectiveMode,
        error: error.message,
      });
    } finally {
      updateYoutubeTranscriptLoading(false);
    }
  }, [
    applyYoutubeTranscriptPayload,
    initData,
    initDataMissingMsg,
    languageProfile,
    normalizeLanguageCode,
    pollYoutubeTranscriptStatus,
    startupEffectiveMode,
    translate,
    updateYoutubeTranscript,
    updateYoutubeTranscriptError,
    updateYoutubeTranscriptLoading,
    youtubeId,
  ]);

  const saveManualTranscriptToDb = useCallback(async (items) => {
    if (!initData || !youtubeId || !items?.length) return;
    try {
      const language = detectTranscriptLanguage(items);
      const response = await fetch('/api/webapp/youtube/manual', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, videoId: youtubeId, items, language }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      notifyManualTranscriptSaved();
    } catch (error) {
      updateYoutubeTranscriptError(`${translate('Ошибка сохранения субтитров', 'Fehler beim Speichern der Untertitel')}: ${error.message}`);
    }
  }, [initData, notifyManualTranscriptSaved, translate, updateYoutubeTranscriptError, youtubeId]);

  const resetManualTranscriptToAuto = useCallback(() => {
    updateYoutubeManualOverride(false);
    setManualTranscript('');
    updateYoutubeTranscriptHasTiming(true);
    setShowManualTranscript(false);
  }, [updateYoutubeManualOverride, updateYoutubeTranscriptHasTiming]);

  const handleManualTranscript = useCallback(async () => {
    const raw = manualTranscript.trim();
    if (!raw) {
      updateYoutubeManualOverride(false);
      updateYoutubeTranscript([]);
      updateYoutubeTranscriptHasTiming(true);
      return;
    }
    const parsed = parseTimedTranscript(raw);
    if (parsed.items.length) {
      updateYoutubeTranscript(parsed.items);
      updateYoutubeTranscriptHasTiming(parsed.hasTiming);
      updateYoutubeManualOverride(true);
      updateYoutubeTranscriptError('');
      setShowManualTranscript(false);
      await saveManualTranscriptToDb(parsed.items);
      return;
    }
    const simple = parseSimpleTimestampTranscript(raw);
    if (simple.items.length) {
      updateYoutubeTranscript(simple.items);
      updateYoutubeTranscriptHasTiming(simple.hasTiming);
      updateYoutubeManualOverride(true);
      updateYoutubeTranscriptError('');
      setShowManualTranscript(false);
      await saveManualTranscriptToDb(simple.items);
      return;
    }
    const fallback = parseTranscriptInput(raw);
    updateYoutubeTranscript(fallback);
    updateYoutubeTranscriptHasTiming(false);
    updateYoutubeManualOverride(true);
    updateYoutubeTranscriptError('');
    setShowManualTranscript(false);
    await saveManualTranscriptToDb(fallback);
  }, [
    manualTranscript,
    saveManualTranscriptToDb,
    updateYoutubeManualOverride,
    updateYoutubeTranscript,
    updateYoutubeTranscriptError,
    updateYoutubeTranscriptHasTiming,
  ]);

  const contextValue = useMemo(() => ({
    ...value,
    youtubeInput,
    setYoutubeInput,
    youtubeError,
    youtubeEmptyState,
    youtubeSearchLoading,
    youtubeSearchResults,
    setYoutubeSearchResults,
    youtubeSearchError,
    setYoutubeSearchError,
    youtubeRecommendationLoading,
    setYoutubeInputDraft,
    commitYoutubeInputDraft,
    searchYoutubeVideos,
    selectYoutubeSearchResult,
    executeYoutubeCommand,
    showManualTranscript,
    setShowManualTranscript,
    manualTranscript,
    setManualTranscript,
    handleManualTranscript,
    resetManualTranscriptToAuto,
    fetchTranscript,
  }), [
    commitYoutubeInputDraft,
    executeYoutubeCommand,
    fetchTranscript,
    handleManualTranscript,
    manualTranscript,
    resetManualTranscriptToAuto,
    searchYoutubeVideos,
    selectYoutubeSearchResult,
    setYoutubeInputDraft,
    showManualTranscript,
    value,
    youtubeEmptyState,
    youtubeError,
    youtubeInput,
    youtubeRecommendationLoading,
    youtubeSearchError,
    youtubeSearchLoading,
    youtubeSearchResults,
  ]);

  useEffect(() => {
    console.info('provider_mount', {
      provider_name: 'youtube',
      effective_mode: startupEffectiveMode,
    });
    console.info('frontend_provider_extracted', YOUTUBE_PROVIDER_EXTRACTION_METRICS);
    console.info('youtube_transcript_provider_state_initialized', {
      provider_name: 'youtube',
      feature: 'manual_transcript',
      effective_mode: startupEffectiveMode,
    });
    console.info('youtube_search_provider_state_initialized', {
      provider_name: 'youtube',
      effective_mode: startupEffectiveMode,
    });
    return () => {
      console.info('provider_unmount', {
        provider_name: 'youtube',
        effective_mode: startupEffectiveMode,
      });
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <YouTubeContext.Provider value={contextValue}>
      {children}
    </YouTubeContext.Provider>
  );
}

export function useYouTubeContext() {
  const context = useContext(YouTubeContext);
  if (!context) {
    throw new Error('YouTubeContext is required');
  }
  return context;
}
