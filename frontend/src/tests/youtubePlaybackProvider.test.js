import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import {
  useYouTubePlaybackContext,
  YouTubePlaybackProvider,
  YOUTUBE_PLAYBACK_PROVIDER_SCAFFOLD,
} from '../providers/YouTubePlaybackProvider.jsx';

function providerProps(overrides = {}) {
  return {
    isLightweightFreeMode: false,
    startupEffectiveMode: 'pro',
    youtubeId: 'abc123def45',
    youtubePlayerReady: true,
    youtubeCurrentTime: 12,
    youtubeIsPaused: false,
    youtubePlaybackStarted: true,
    youtubeOverlayEnabled: true,
    youtubeAppFullscreen: false,
    youtubeTranscriptHasTiming: true,
    youtubeTranscriptLength: 3,
    youtubeResumeStorageKey: 'webapp_youtube_resume_test',
    persistYoutubeResumeState: vi.fn(),
    syncYoutubeResumeState: vi.fn(),
    refsOwnedByApp: true,
    value: { marker: 'playback' },
    ...overrides,
  };
}

function PlaybackProbe() {
  const context = useYouTubePlaybackContext();
  return React.createElement(
    'span',
    null,
    [
      context.marker,
      context.youtubeId,
      String(context.youtubePlayerReady),
      String(context.youtubePlaybackStarted),
      String(context.youtubeOverlayEnabled),
      String(context.refsOwnedByApp),
    ].join('|'),
  );
}

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('YouTubePlaybackProvider scaffold', () => {
  it('does not mount for free users', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubePlaybackProvider,
      providerProps({
        isLightweightFreeMode: true,
        startupEffectiveMode: 'free',
      }),
      React.createElement(PlaybackProbe),
    ));
    expect(html).toBe('');
  });

  it('mounts for entitled users and exposes explicit playback context', () => {
    const html = renderToStaticMarkup(React.createElement(
      YouTubePlaybackProvider,
      providerProps(),
      React.createElement(PlaybackProbe),
    ));
    expect(html).toContain('playback|abc123def45|true|true|true|true');
  });

  it('fails explicitly when required values are missing', () => {
    const props = providerProps();
    delete props.youtubePlayerReady;
    expect(() => renderToStaticMarkup(React.createElement(
      YouTubePlaybackProvider,
      props,
      React.createElement(PlaybackProbe),
    ))).toThrow('YouTubePlaybackProvider requires youtubePlayerReady');
  });

  it('fails explicitly if refs are not still owned by App during scaffold phase', () => {
    expect(() => renderToStaticMarkup(React.createElement(
      YouTubePlaybackProvider,
      providerProps({ refsOwnedByApp: false }),
      React.createElement(PlaybackProbe),
    ))).toThrow('YouTubePlaybackProvider requires refsOwnedByApp=true during scaffold phase');
  });

  it('documents scaffold metadata', () => {
    expect(YOUTUBE_PLAYBACK_PROVIDER_SCAFFOLD).toEqual(expect.objectContaining({
      provider_name: 'youtube_playback',
      refs_owned_by_app: true,
      scaffold_only: false,
      state_removed: 8,
      refs_removed: 3,
      effects_removed: 9,
    }));
  });

  it('App.jsx no longer owns moved playback state directly', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).not.toContain('const [youtubeId, setYoutubeId] = useState');
    expect(appSource).not.toContain('const [youtubePlayerReady, setYoutubePlayerReady] = useState');
    expect(appSource).not.toContain('const [youtubeCurrentTime, setYoutubeCurrentTime] = useState');
    expect(appSource).not.toContain('const [youtubeIsPaused, setYoutubeIsPaused] = useState');
    expect(appSource).not.toContain('const [youtubePlaybackStarted, setYoutubePlaybackStarted] = useState');
    expect(appSource).toContain('useYouTubePlaybackController');
  });

  it('App.jsx keeps cross-feature refs but no longer owns moved lifecycle refs', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).toContain('const youtubePlayerRef = useRef(null)');
    expect(appSource).not.toContain('const youtubeTimeIntervalRef = useRef(null)');
    expect(appSource).not.toContain('const youtubeCurrentTimeRef = useRef(0)');
    expect(appSource).not.toContain('youtubeResumeAppliedForVideoRef');
    expect(appSource).toContain('const youtubeTodayTimerSyncInFlightRef = useRef(false)');
  });

  it('App.jsx wraps YouTube section with playback provider without owning player lifecycle', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).toContain('<YouTubePlaybackProvider');
    expect(appSource).toContain('refsOwnedByApp={true}');
    expect(appSource).not.toContain('new window.YT.Player');
    expect(appSource).not.toContain('youtubePlayerRef.current = new window.YT.Player');
  });

  it('playback controller owns state and resume callbacks', () => {
    const providerSource = fs.readFileSync(path.resolve(process.cwd(), 'src/providers/YouTubePlaybackProvider.jsx'), 'utf8');
    expect(providerSource).toContain('export function useYouTubePlaybackController');
    expect(providerSource).toContain('const [youtubeId, setYoutubeId] = useState');
    expect(providerSource).toContain('const [youtubePlayerReady, setYoutubePlayerReady] = useState');
    expect(providerSource).toContain('const [youtubeCurrentTime, setYoutubeCurrentTime] = useState');
    expect(providerSource).toContain('const youtubeTimeIntervalRef = useRef(null)');
    expect(providerSource).toContain('youtubePlayerRef.current = new window.YT.Player');
    expect(providerSource).toContain('const persistYoutubeResumeState = useCallback');
    expect(providerSource).toContain('const syncYoutubeResumeState = useCallback');
    expect(providerSource).toContain('youtube_resume_state_saved');
    expect(providerSource).toContain('youtube_playback_state_initialized');
    expect(providerSource).toContain('youtube_playback_polling_started');
    expect(providerSource).toContain('youtube_player_ready');
  });
});
