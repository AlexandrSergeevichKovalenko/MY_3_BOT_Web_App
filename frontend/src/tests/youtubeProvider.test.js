import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { YouTubeProvider, useYouTubeContext, YOUTUBE_PROVIDER_EXTRACTION_METRICS } from '../providers/YouTubeProvider.jsx';
import YouTubeSection from '../components/YouTubeSection.jsx';

function ContextProbe() {
  const ctx = useYouTubeContext();
  return React.createElement('span', null, ctx.marker);
}

function providerProps(overrides = {}) {
  return {
    isLightweightFreeMode: false,
    startupEffectiveMode: 'pro',
    initData: 'init-data',
    initDataMissingMsg: 'missing init',
    youtubeId: 'abc123def45',
    languageProfile: { learning_language: 'de' },
    normalizeLangCode: (value) => value,
    extractYoutubeId: (value) => (/^[a-zA-Z0-9_-]{11}$/.test(String(value || '').trim()) ? String(value).trim() : ''),
    getWebappLanguagePairHint: () => 'ru-de',
    tr: (ru, de) => ru || de,
    setYoutubeId: vi.fn(),
    setYoutubeTranscript: vi.fn(),
    setYoutubeTranslations: vi.fn(),
    setYoutubeManualOverride: vi.fn(),
    setYoutubeTranscriptError: vi.fn(),
    setYoutubeTranscriptLoading: vi.fn(),
    setYoutubeTranscriptHasTiming: vi.fn(),
    onYoutubeInputChanged: vi.fn(),
    onYoutubeInputCleared: vi.fn(),
    onYoutubeVideoResolved: vi.fn(),
    onManualTranscriptSaved: vi.fn(),
    pendingCommand: null,
    onPendingCommandHandled: vi.fn(),
    value: { marker: 'youtube' },
    ...overrides,
  };
}

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('YouTubeProvider boundary', () => {
  it('does not mount for free users', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps({
        isLightweightFreeMode: true,
        startupEffectiveMode: 'free',
        value: { marker: 'youtube' },
      }),
      React.createElement(ContextProbe),
    ));
    expect(html).toBe('');
  });

  it('mounts for entitled users and exposes context', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps(),
      React.createElement(ContextProbe),
    ));
    expect(html).toContain('youtube');
  });

  it('does not execute transcript loader in free mode because children are not rendered', () => {
    const transcriptLoader = vi.fn();
    renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps({
        isLightweightFreeMode: true,
        startupEffectiveMode: 'free',
        value: { marker: 'youtube', transcriptLoader },
      }),
      React.createElement(() => {
        transcriptLoader();
        return null;
      }),
    ));
    expect(transcriptLoader).not.toHaveBeenCalled();
  });

  it('renders YouTubeSection through explicit render contract', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps({
        value: { render: () => React.createElement('section', { className: 'webapp-video' }, 'YouTube') },
      }),
      React.createElement(YouTubeSection),
    ));
    expect(html).toContain('webapp-video');
    expect(html).toContain('YouTube');
  });

  it('fails explicitly if section render contract is missing', () => {
    expect(() => renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps({
        value: { marker: 'youtube' },
      }),
      React.createElement(YouTubeSection),
    ))).toThrow('YouTubeSection requires explicit render function');
  });

  it('fails explicitly when required transcript extraction config is missing for entitled users', () => {
    const props = providerProps();
    delete props.setYoutubeTranscript;
    expect(() => renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      props,
      React.createElement(ContextProbe),
    ))).toThrow('YouTubeProvider requires setYoutubeTranscript');
  });

  it('exposes manual transcript state and fetch handler through context', () => {
    function TranscriptProbe() {
      const ctx = useYouTubeContext();
      return React.createElement(
        'span',
        null,
        [
          typeof ctx.setShowManualTranscript,
          typeof ctx.setManualTranscript,
          typeof ctx.handleManualTranscript,
          typeof ctx.resetManualTranscriptToAuto,
          typeof ctx.fetchTranscript,
          typeof ctx.searchYoutubeVideos,
          typeof ctx.commitYoutubeInputDraft,
          typeof ctx.selectYoutubeSearchResult,
          String(ctx.showManualTranscript),
          ctx.manualTranscript,
        ].join('|'),
      );
    }
    const html = renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps(),
      React.createElement(TranscriptProbe),
    ));
    expect(html).toContain('function|function|function|function|function|function|function|function|false|');
  });

  it('passes provider context into YouTubeSection render contract', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubeProvider,
      providerProps({
        value: {
          render: ({ context }) => React.createElement(
            'section',
            null,
            typeof context.handleManualTranscript,
          ),
        },
      }),
      React.createElement(YouTubeSection),
    ));
    expect(html).toContain('function');
  });

  it('App.jsx no longer owns moved manual transcript state or handlers', () => {
    const appSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/App.jsx'),
      'utf8',
    );
    expect(appSource).not.toContain('const [showManualTranscript, setShowManualTranscript] = useState');
    expect(appSource).not.toContain('const [manualTranscript, setManualTranscript] = useState');
    expect(appSource).not.toContain('const parseTranscriptInput =');
    expect(appSource).not.toContain('const handleManualTranscript = async');
  });

  it('defines an explicit YouTube command bridge contract', () => {
    const appSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/App.jsx'),
      'utf8',
    );
    const providerSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/providers/YouTubeProvider.jsx'),
      'utf8',
    );
    expect(appSource).toContain('const executeYoutubeCommand = useCallback');
    expect(appSource).toContain('const youtubeCommands = useMemo');
    expect(appSource).toContain('requestVideoTaskStart');
    expect(appSource).toContain('requestVideoSelection');
    expect(appSource).toContain('requestVideoError');
    expect(providerSource).toContain('youtube_command_bridge_request');
    expect(providerSource).toContain('youtube_command_bridge_success');
    expect(providerSource).toContain('youtube_command_bridge_failure');
  });

  it('Today video task flow uses the command bridge instead of direct YouTube search mutations', () => {
    const appSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/App.jsx'),
      'utf8',
    );
    const startTodayTaskBlock = appSource.slice(
      appSource.indexOf('const startTodayTask = async'),
      appSource.indexOf('const getTodayItemTitle =', appSource.indexOf('const startTodayTask = async')),
    );
    expect(startTodayTaskBlock).toContain('youtubeCommands.requestVideoTaskStart');
    expect(startTodayTaskBlock).toContain('youtubeCommands.requestVideoSelection');
    expect(startTodayTaskBlock).toContain('youtubeCommands.requestVideoError');
    expect(startTodayTaskBlock).not.toContain('setYoutubeSearchResults');
    expect(startTodayTaskBlock).not.toContain('setYoutubeSearchError');
    expect(startTodayTaskBlock).not.toContain('setYoutubeInput');
    expect(startTodayTaskBlock).not.toContain('setYoutubeEmptyState');
    expect(startTodayTaskBlock).not.toContain('setYoutubeRecommendationLoading');
  });

  it('search state moved out of App.jsx for this slice', () => {
    const appSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/App.jsx'),
      'utf8',
    );
    expect(appSource).not.toContain('const [youtubeInput, setYoutubeInput] = useState');
    expect(appSource).not.toContain('const [youtubeError, setYoutubeError] = useState');
    expect(appSource).not.toContain('const [youtubeEmptyState, setYoutubeEmptyState] = useState');
    expect(appSource).not.toContain('const [youtubeSearchLoading, setYoutubeSearchLoading] = useState(false)');
    expect(appSource).not.toContain('const [youtubeSearchResults, setYoutubeSearchResults] = useState([])');
    expect(appSource).not.toContain('const [youtubeSearchError, setYoutubeSearchError] = useState');
    expect(appSource).not.toContain('const [youtubeRecommendationLoading, setYoutubeRecommendationLoading] = useState');
    expect(appSource).not.toContain('const searchYoutubeVideos = async');
  });

  it('YouTubeProvider owns search state and command execution', () => {
    const providerSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/providers/YouTubeProvider.jsx'),
      'utf8',
    );
    expect(providerSource).toContain('const [youtubeInput, setYoutubeInput] = useState');
    expect(providerSource).toContain('const [youtubeSearchLoading, setYoutubeSearchLoading] = useState(false)');
    expect(providerSource).toContain('const searchYoutubeVideos = useCallback');
    expect(providerSource).toContain('const executeYoutubeCommand = useCallback');
    expect(providerSource).toContain('youtube_search_provider_state_initialized');
    expect(providerSource).toContain('youtube_search_started');
    expect(providerSource).toContain('youtube_search_completed');
    expect(providerSource).toContain('youtube_search_failed');
  });

  it('unknown YouTube commands fail explicitly in provider', () => {
    const providerSource = fs.readFileSync(
      path.resolve(process.cwd(), 'src/providers/YouTubeProvider.jsx'),
      'utf8',
    );
    expect(providerSource).toContain('Unknown YouTube command');
    expect(providerSource).toContain('YouTube command bridge requires command_name');
    expect(providerSource).toContain('YouTube command bridge requires source_feature');
    expect(providerSource).toContain('YouTube command bridge requires request_id');
  });

  it('exposes extraction metrics', () => {
    expect(YOUTUBE_PROVIDER_EXTRACTION_METRICS).toEqual(expect.objectContaining({
      provider_name: 'youtube_search',
      state_removed: expect.any(Number),
      refs_removed: expect.any(Number),
      effects_removed: expect.any(Number),
      callbacks_removed: expect.any(Number),
      memos_removed: expect.any(Number),
    }));
  });
});
