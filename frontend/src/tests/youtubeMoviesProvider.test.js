import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import {
  useYouTubeMoviesContext,
  YouTubeMoviesProvider,
  YOUTUBE_MOVIES_PROVIDER_EXTRACTION_METRICS,
} from '../providers/YouTubeMoviesProvider.jsx';

function providerProps(overrides = {}) {
  return {
    isLightweightFreeMode: false,
    startupEffectiveMode: 'pro',
    isWebAppMode: true,
    initData: 'init-data',
    initDataMissingMsg: 'missing auth',
    languageProfile: { learning_language: 'de' },
    normalizeLangCode: (value) => String(value || '').trim().toLowerCase(),
    tr: (ru) => ru,
    onMovieSelected: vi.fn(),
    onCatalogUpdated: vi.fn(),
    registerInvalidateCatalog: vi.fn(),
    ...overrides,
  };
}

function MoviesProbe() {
  const context = useYouTubeMoviesContext();
  return React.createElement('span', null, [
    String(context.movies.length),
    String(context.moviesLoading),
    String(context.moviesLanguageFilter),
    String(typeof context.selectMovie),
  ].join('|'));
}

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('YouTubeMoviesProvider', () => {
  it('does not mount for free users', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeMoviesProvider,
      providerProps({
        isLightweightFreeMode: true,
        startupEffectiveMode: 'free',
      }),
      React.createElement(MoviesProbe),
    ));
    expect(html).toBe('');
  });

  it('mounts for entitled users and exposes movie context', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeMoviesProvider,
      providerProps(),
      React.createElement(MoviesProbe),
    ));
    expect(html).toContain('0|false|all|function');
  });

  it('fails explicitly when required inputs are missing', () => {
    const props = providerProps();
    delete props.onMovieSelected;
    expect(() => renderToStaticMarkup(React.createElement(
      YouTubeMoviesProvider,
      props,
      React.createElement(MoviesProbe),
    ))).toThrow('YouTubeMoviesProvider requires onMovieSelected');
  });

  it('documents extraction metrics', () => {
    expect(YOUTUBE_MOVIES_PROVIDER_EXTRACTION_METRICS).toEqual(expect.objectContaining({
      provider_name: 'youtube_movies',
      state_removed: 5,
      effects_removed: 2,
      callbacks_removed: 1,
      memos_removed: 2,
    }));
  });

  it('App.jsx no longer owns moved movie state symbols', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).not.toContain('const [movies, setMovies] = useState');
    expect(appSource).not.toContain('const [moviesLoading, setMoviesLoading] = useState');
    expect(appSource).not.toContain('const [moviesError, setMoviesError] = useState');
    expect(appSource).not.toContain('const [moviesCollapsed, setMoviesCollapsed] = useState');
    expect(appSource).not.toContain('const [moviesLanguageFilter, setMoviesLanguageFilter] = useState');
    expect(appSource).not.toContain('const movieLanguageOptions = useMemo');
    expect(appSource).not.toContain('const moviesFiltered = useMemo');
  });

  it('provider owns loading, filtering, selection, and observability', () => {
    const providerSource = fs.readFileSync(path.resolve(process.cwd(), 'src/providers/YouTubeMoviesProvider.jsx'), 'utf8');
    expect(providerSource).toContain('const [movies, setMovies] = useState');
    expect(providerSource).toContain("fetch('/api/webapp/youtube/catalog'");
    expect(providerSource).toContain('const movieLanguageOptions = useMemo');
    expect(providerSource).toContain('const moviesFiltered = useMemo');
    expect(providerSource).toContain('youtube_movies_loaded');
    expect(providerSource).toContain('youtube_movie_selected');
    expect(providerSource).toContain('frontend_provider_extracted');
  });

  it('movie selection uses explicit external callback and does not move dictionary/player bridges', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).toContain('onMovieSelected={handleYoutubeMovieSelected}');
    expect(appSource).toContain('youtubeCommands.requestVideoSelection');
    expect(appSource).toContain('const youtubePlayerRef = useRef(null)');
    expect(appSource).toContain('const youtubeDictWidgetRef = useRef(null)');
  });
});
