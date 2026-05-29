import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

export const YouTubeMoviesContext = createContext(null);

export const YOUTUBE_MOVIES_PROVIDER_EXTRACTION_METRICS = {
  provider_name: 'youtube_movies',
  state_removed: 5,
  refs_removed: 0,
  effects_removed: 2,
  callbacks_removed: 1,
  memos_removed: 2,
};

function requireMoviesInput(name, value) {
  if (value === undefined || value === null) {
    throw new Error(`YouTubeMoviesProvider requires ${name}`);
  }
  return value;
}

export function YouTubeMoviesProvider({
  isLightweightFreeMode,
  startupEffectiveMode,
  isWebAppMode,
  initData,
  initDataMissingMsg,
  languageProfile,
  normalizeLangCode,
  tr,
  onMovieSelected,
  onCatalogUpdated,
  registerInvalidateCatalog,
  children,
}) {
  if (isLightweightFreeMode) {
    console.info('provider_skipped_free_mode', {
      provider_name: 'youtube_movies',
      effective_mode: startupEffectiveMode,
    });
    return null;
  }
  const translate = requireMoviesInput('tr', tr);
  const normalizeLanguageCode = requireMoviesInput('normalizeLangCode', normalizeLangCode);
  const handleMovieSelected = requireMoviesInput('onMovieSelected', onMovieSelected);
  const handleCatalogUpdated = requireMoviesInput('onCatalogUpdated', onCatalogUpdated);
  const handleRegisterInvalidateCatalog = requireMoviesInput('registerInvalidateCatalog', registerInvalidateCatalog);

  const [movies, setMovies] = useState([]);
  const [moviesLoading, setMoviesLoading] = useState(false);
  const [moviesError, setMoviesError] = useState('');
  const [moviesCollapsed, setMoviesCollapsed] = useState(false);
  const [moviesLanguageFilter, setMoviesLanguageFilter] = useState('all');

  const getMovieLanguageCode = useCallback((item) => (
    normalizeLanguageCode(item?.language || '').slice(0, 2) || 'unknown'
  ), [normalizeLanguageCode]);

  const movieLanguageOptions = useMemo(() => {
    const set = new Set();
    movies.forEach((item) => set.add(getMovieLanguageCode(item)));
    return Array.from(set)
      .filter(Boolean)
      .sort();
  }, [getMovieLanguageCode, movies]);

  const moviesFiltered = useMemo(() => {
    if (moviesLanguageFilter === 'all') return movies;
    return movies.filter((item) => getMovieLanguageCode(item) === moviesLanguageFilter);
  }, [getMovieLanguageCode, movies, moviesLanguageFilter]);

  const invalidateMoviesCatalog = useCallback(() => {
    setMovies([]);
    handleCatalogUpdated([]);
  }, [handleCatalogUpdated]);

  const selectMovie = useCallback((item) => {
    const videoId = String(item?.video_id || '').trim();
    if (!videoId) {
      throw new Error('selectMovie requires movie.video_id');
    }
    console.info('youtube_movie_selected', {
      provider_name: 'youtube_movies',
      video_id_present: true,
      language: String(item?.language || '').trim(),
    });
    handleMovieSelected(item);
    setMoviesCollapsed(true);
  }, [handleMovieSelected]);

  useEffect(() => {
    const payload = {
      provider_name: 'youtube_movies',
      effective_mode: startupEffectiveMode,
    };
    console.info('youtube_movies_provider_mount', payload);
    console.info('frontend_provider_extracted', YOUTUBE_MOVIES_PROVIDER_EXTRACTION_METRICS);
    return () => {
      console.info('youtube_movies_provider_unmount', payload);
    };
  }, [startupEffectiveMode]);

  useEffect(() => {
    handleRegisterInvalidateCatalog(invalidateMoviesCatalog);
    return () => {
      handleRegisterInvalidateCatalog(null);
    };
  }, [handleRegisterInvalidateCatalog, invalidateMoviesCatalog]);

  useEffect(() => {
    const learning = normalizeLanguageCode(languageProfile?.learning_language);
    if (!learning) return;
    if (movieLanguageOptions.includes(learning)) {
      setMoviesLanguageFilter(learning);
    } else {
      setMoviesLanguageFilter('all');
    }
  }, [languageProfile?.learning_language, movieLanguageOptions, normalizeLanguageCode]);

  useEffect(() => {
    if (movies.length > 0) return undefined;
    if (!isWebAppMode || !initData) {
      setMoviesError(initDataMissingMsg || translate('Не удалось загрузить каталог: нет данных авторизации.', 'Katalog konnte nicht geladen werden: Auth-Daten fehlen.'));
      console.info('youtube_movies_load_failed', {
        provider_name: 'youtube_movies',
        reason: 'missing_auth',
      });
      return undefined;
    }
    let cancelled = false;
    setMoviesLoading(true);
    setMoviesError('');
    fetch('/api/webapp/youtube/catalog', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ initData, limit: 60 }),
    })
      .then(async (res) => {
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      })
      .then((data) => {
        if (cancelled) return;
        const items = Array.isArray(data.items) ? data.items : [];
        setMovies(items);
        handleCatalogUpdated(items);
        console.info('youtube_movies_loaded', {
          provider_name: 'youtube_movies',
          count: items.length,
        });
      })
      .catch((err) => {
        if (cancelled) return;
        setMoviesError(`${translate('Ошибка каталога', 'Katalogfehler')}: ${err.message}`);
        console.info('youtube_movies_load_failed', {
          provider_name: 'youtube_movies',
          reason: 'request_failed',
        });
      })
      .finally(() => {
        if (!cancelled) setMoviesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [handleCatalogUpdated, initData, initDataMissingMsg, isWebAppMode, movies.length, translate]);

  const value = useMemo(() => ({
    movies,
    moviesLoading,
    moviesError,
    moviesCollapsed,
    moviesLanguageFilter,
    movieLanguageOptions,
    moviesFiltered,
    setMoviesLanguageFilter,
    selectMovie,
  }), [
    movieLanguageOptions,
    movies,
    moviesCollapsed,
    moviesError,
    moviesFiltered,
    moviesLanguageFilter,
    moviesLoading,
    selectMovie,
  ]);

  return (
    <YouTubeMoviesContext.Provider value={value}>
      {children}
    </YouTubeMoviesContext.Provider>
  );
}

export function useYouTubeMoviesContext() {
  const context = useContext(YouTubeMoviesContext);
  if (!context) {
    throw new Error('YouTubeMoviesContext is required');
  }
  return context;
}

export default YouTubeMoviesProvider;
