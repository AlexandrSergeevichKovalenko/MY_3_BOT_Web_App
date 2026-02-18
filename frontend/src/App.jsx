import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  LiveKitRoom,
  ControlBar,
  ConnectionStateToast,
  RoomAudioRenderer,
} from '@livekit/components-react';
import '@livekit/components-styles';
import './App.css';
import * as echarts from 'echarts';
import BlocksTrainer from './components/BlocksTrainer';

// URL вашего сервера LiveKit
const livekitUrl = "wss://implemrntingvoicetobot-vhsnc86g.livekit.cloud";

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error('WebApp crashed:', error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="webapp-page">
          <div className="webapp-card">
            <header className="webapp-header">
              <span className="pill">Telegram Web App</span>
              <h1>Ошибка загрузки</h1>
              <p>Произошла ошибка при запуске приложения. Попробуйте перезагрузить.</p>
            </header>
            <div className="webapp-error">
              {this.state.error?.message || 'Неизвестная ошибка'}
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

function AppInner() {
  const telegramApp = useMemo(() => window.Telegram?.WebApp, []);
  const isWebAppMode = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const isWebappPath =
      window.location.pathname === '/webapp' ||
      window.location.pathname === '/webapp/review';
    return Boolean(telegramApp?.initData) || params.get('mode') === 'webapp' || isWebappPath;
  }, [telegramApp]);

  const [initData, setInitData] = useState(telegramApp?.initData || '');
  const [sessionId, setSessionId] = useState(null);
  const [webappUser, setWebappUser] = useState(null);
  const [webappChatType, setWebappChatType] = useState('');
  const [results, setResults] = useState([]);
  const [sentences, setSentences] = useState([]);
  const [webappError, setWebappError] = useState('');
  const [webappLoading, setWebappLoading] = useState(false);
  const [translationDrafts, setTranslationDrafts] = useState({});
  const [finishMessage, setFinishMessage] = useState('');
  const [dictionaryWord, setDictionaryWord] = useState('');
  const [dictionaryResult, setDictionaryResult] = useState(null);
  const [dictionaryError, setDictionaryError] = useState('');
  const [dictionaryLoading, setDictionaryLoading] = useState(false);
  const [dictionarySaved, setDictionarySaved] = useState('');
  const [dictionaryDirection, setDictionaryDirection] = useState('ru-de');
  const [collocationsVisible, setCollocationsVisible] = useState(false);
  const [collocationsLoading, setCollocationsLoading] = useState(false);
  const [collocationsError, setCollocationsError] = useState('');
  const [collocationOptions, setCollocationOptions] = useState([]);
  const [selectedCollocation, setSelectedCollocation] = useState(null);
  const [flashcardExitSummary, setFlashcardExitSummary] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [youtubeInput, setYoutubeInput] = useState('');
  const [youtubeId, setYoutubeId] = useState('');
  const [youtubeError, setYoutubeError] = useState('');
  const [videoExpanded, setVideoExpanded] = useState(false);
  const [youtubeTranscript, setYoutubeTranscript] = useState([]);
  const [youtubeTranscriptError, setYoutubeTranscriptError] = useState('');
  const [youtubeTranscriptLoading, setYoutubeTranscriptLoading] = useState(false);
  const [youtubePlayerReady, setYoutubePlayerReady] = useState(false);
  const [youtubeCurrentTime, setYoutubeCurrentTime] = useState(0);
  const [youtubeTranslations, setYoutubeTranslations] = useState({});
  const [youtubeRuEnabled, setYoutubeRuEnabled] = useState(false);
  const [youtubeManualOverride, setYoutubeManualOverride] = useState(false);
  const [youtubeTranscriptHasTiming, setYoutubeTranscriptHasTiming] = useState(true);
  const [movies, setMovies] = useState([]);
  const [moviesLoading, setMoviesLoading] = useState(false);
  const [moviesError, setMoviesError] = useState('');
  const [moviesCollapsed, setMoviesCollapsed] = useState(false);
  const [showManualTranscript, setShowManualTranscript] = useState(false);
  const [manualTranscript, setManualTranscript] = useState('');
  const [selectionText, setSelectionText] = useState('');
  const [selectionPos, setSelectionPos] = useState(null);
  const [lastLookupScrollY, setLastLookupScrollY] = useState(null);
  const [historyItems, setHistoryItems] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [historyVisible, setHistoryVisible] = useState(false);
  const [finishStatus, setFinishStatus] = useState('idle');
  const [explanations, setExplanations] = useState({});
  const [explanationLoading, setExplanationLoading] = useState({});
  const [flashcardsVisible, setFlashcardsVisible] = useState(false);
  const [flashcardsOnly, setFlashcardsOnly] = useState(false);
  const [flashcardsLoading, setFlashcardsLoading] = useState(false);
  const [flashcardsError, setFlashcardsError] = useState('');
  const [flashcards, setFlashcards] = useState([]);
  const [flashcardPool, setFlashcardPool] = useState([]);
  const [flashcardIndex, setFlashcardIndex] = useState(0);
  const [flashcardSelection, setFlashcardSelection] = useState(null);
  const [flashcardOptions, setFlashcardOptions] = useState([]);
  const [flashcardSetSize, setFlashcardSetSize] = useState(15);
  const [flashcardDurationSec, setFlashcardDurationSec] = useState(10);
  const [flashcardTrainingMode, setFlashcardTrainingMode] = useState('quiz');
  const [blocksTimerMode, setBlocksTimerMode] = useState('fixed');
  const [flashcardSessionActive, setFlashcardSessionActive] = useState(false);
  const [flashcardPreviewActive, setFlashcardPreviewActive] = useState(false);
  const [flashcardPreviewIndex, setFlashcardPreviewIndex] = useState(0);
  const [srsCard, setSrsCard] = useState(null);
  const [srsState, setSrsState] = useState(null);
  const [srsQueueInfo, setSrsQueueInfo] = useState({ due_count: 0, new_remaining_today: 0 });
  const [srsLoading, setSrsLoading] = useState(false);
  const [srsSubmitting, setSrsSubmitting] = useState(false);
  const [srsRevealAnswer, setSrsRevealAnswer] = useState(false);
  const [flashcardFeelMap, setFlashcardFeelMap] = useState({});
  const [flashcardFeelVisibleMap, setFlashcardFeelVisibleMap] = useState({});
  const [flashcardFeelLoadingMap, setFlashcardFeelLoadingMap] = useState({});
  const [flashcardFeelFeedbackLoading, setFlashcardFeelFeedbackLoading] = useState({});
  const [previewAudioReady, setPreviewAudioReady] = useState(false);
  const [previewAudioPlaying, setPreviewAudioPlaying] = useState(false);
  const [flashcardTimerKey, setFlashcardTimerKey] = useState(0);
  const [topics, setTopics] = useState([
    '🧩 ЗАГАДОЧНАЯ ИСТОРИЯ',
    '💼 Business',
    '🏥 Medicine',
    '🎨 Hobbies',
    '✈️ Travel',
    '🔬 Science',
    '💻 Technology',
    '🖼️ Art',
    '🎓 Education',
    '🍽️ Food',
    '⚽ Sports',
    '🌿 Nature',
    '🎵 Music',
    '📚 Literature',
    '🧠 Psychology',
    '🏛️ History',
    '📰 News',
  ]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState('');
  const [selectedTopic, setSelectedTopic] = useState('💼 Business');
  const [selectedLevel, setSelectedLevel] = useState('c1');
  const STORY_TOPIC = '🧩 ЗАГАДОЧНАЯ ИСТОРИЯ';
  const isStoryTopic = (value) => (value || '').includes('ЗАГАДОЧНАЯ ИСТОРИЯ');
  const [storyMode, setStoryMode] = useState('new');
  const [storyType, setStoryType] = useState('знаменитая личность');
  const [storyDifficulty, setStoryDifficulty] = useState('средний');
  const [storyHistory, setStoryHistory] = useState([]);
  const [storyHistoryLoading, setStoryHistoryLoading] = useState(false);
  const [storyHistoryError, setStoryHistoryError] = useState('');
  const [selectedStoryId, setSelectedStoryId] = useState('');
  const [storyGuess, setStoryGuess] = useState('');
  const [storyResult, setStoryResult] = useState(null);
  const [sessionType, setSessionType] = useState('none');
  const [selectedSections, setSelectedSections] = useState(new Set());
  const [flashcardSetComplete, setFlashcardSetComplete] = useState(false);
  const [flashcardStats, setFlashcardStats] = useState({ total: 0, correct: 0, wrong: 0 });
  const [flashcardTimedOut, setFlashcardTimedOut] = useState(false);
  const [flashcardOutcome, setFlashcardOutcome] = useState(null);
  const [blocksResetNonce, setBlocksResetNonce] = useState(0);
  const [blocksMenuOpen, setBlocksMenuOpen] = useState(false);
  const [blocksMenuSettingsOpen, setBlocksMenuSettingsOpen] = useState(false);
  const [blocksFinishConfirmOpen, setBlocksFinishConfirmOpen] = useState(false);
  const [folders, setFolders] = useState([]);
  const [foldersLoading, setFoldersLoading] = useState(false);
  const [foldersError, setFoldersError] = useState('');
  const [dictionaryFolderId, setDictionaryFolderId] = useState('none');
  const [flashcardFolderMode, setFlashcardFolderMode] = useState('all');
  const [flashcardFolderId, setFlashcardFolderId] = useState('');
  const [flashcardAutoAdvance, setFlashcardAutoAdvance] = useState(true);
  const [showNewFolderForm, setShowNewFolderForm] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [newFolderColor, setNewFolderColor] = useState('#5ddcff');
  const [newFolderIcon, setNewFolderIcon] = useState('book');
  const [userAvatar, setUserAvatar] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const [menuMultiSelect, setMenuMultiSelect] = useState(true);
  const [analyticsPeriod, setAnalyticsPeriod] = useState('week');
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState('');
  const [analyticsSummary, setAnalyticsSummary] = useState(null);
  const [analyticsPoints, setAnalyticsPoints] = useState([]);
  const [analyticsCompare, setAnalyticsCompare] = useState([]);
  const [analyticsRank, setAnalyticsRank] = useState(null);
  const isStorySession = sessionType === 'story' || isStoryTopic(selectedTopic);
  const isStoryResultMode = Boolean(storyResult && isStorySession);

  const dictionaryRef = useRef(null);
  const flashcardsRef = useRef(null);
  const translationsRef = useRef(null);
  const youtubeRef = useRef(null);
  const moviesRef = useRef(null);
  const youtubeSubtitlesRef = useRef(null);
  const youtubePlayerRef = useRef(null);
  const youtubeTimeIntervalRef = useRef(null);
  const youtubeTranslateInFlightRef = useRef(false);
  const youtubeTranslateIndexRef = useRef(-1);
  const autoAdvanceTimeoutRef = useRef(null);
  const revealTimeoutRef = useRef(null);
  const flashcardIndexRef = useRef(0);
  const flashcardSelectionRef = useRef(null);
  const flashcardRoundStartRef = useRef(Date.now());
  const blocksMenuRef = useRef(null);
  const srsShownAtRef = useRef(null);
  const ttsCacheRef = useRef(new Map());
  const ttsLastRef = useRef({ key: '', ts: 0 });
  const ttsCurrentAudioRef = useRef(null);
  const audioContextRef = useRef(null);
  const positiveAudioRef = useRef(null);
  const negativeAudioRef = useRef(null);
  const timeoutAudioRef = useRef(null);
  const avatarInputRef = useRef(null);
  const analyticsRef = useRef(null);
  const assistantRef = useRef(null);
  const analyticsTrendRef = useRef(null);
  const analyticsCompareRef = useRef(null);
  const assetBaseUrl = import.meta.env.BASE_URL || '/';
  const heroMascotSrc = `${assetBaseUrl}hero_original.jpg`;
  const heroStickerSrc = `${assetBaseUrl}hero_sticker.webp`;
  const heroCrySrc = `${assetBaseUrl}hero_cry.webp`;

  const safeStorageGet = (key) => {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      console.warn('localStorage unavailable', error);
      return null;
    }
  };

  const safeStorageSet = (key, value) => {
    try {
      window.localStorage.setItem(key, value);
    } catch (error) {
      console.warn('localStorage unavailable', error);
    }
  };

  const safeStorageRemove = (key) => {
    try {
      window.localStorage.removeItem(key);
    } catch (error) {
      console.warn('localStorage unavailable', error);
    }
  };

  useEffect(() => {
    const positiveUrl = `${assetBaseUrl}sounds/correct.wav`;
    const negativeUrl = `${assetBaseUrl}sounds/wrong.wav`;
    const timeoutUrl = `${assetBaseUrl}sounds/timeout.wav`;
    positiveAudioRef.current = new Audio(positiveUrl);
    negativeAudioRef.current = new Audio(negativeUrl);
    timeoutAudioRef.current = new Audio(timeoutUrl);
    [positiveAudioRef.current, negativeAudioRef.current, timeoutAudioRef.current].forEach((audio) => {
      if (!audio) return;
      audio.volume = 0.8;
      audio.preload = 'auto';
    });
  }, [assetBaseUrl]);


  const getAudioContext = () => {
    if (!audioContextRef.current) {
      const Context = window.AudioContext || window.webkitAudioContext;
      if (!Context) return null;
      audioContextRef.current = new Context();
    }
    if (audioContextRef.current.state === 'suspended') {
      audioContextRef.current.resume();
    }
    return audioContextRef.current;
  };

  const unlockAudio = () => {
    const ctx = getAudioContext();
    if (!ctx) return;
    const buffer = ctx.createBuffer(1, 1, 22050);
    const source = ctx.createBufferSource();
    source.buffer = buffer;
    source.connect(ctx.destination);
    try {
      source.start(0);
    } catch (error) {
      // ignore playback errors
    }
    [positiveAudioRef.current, negativeAudioRef.current, timeoutAudioRef.current].forEach((audio) => {
      if (!audio) return;
      const prevVolume = audio.volume;
      audio.volume = 0;
      audio.play().catch(() => {});
      audio.pause();
      audio.currentTime = 0;
      audio.volume = prevVolume;
    });
  };

  const playFeedbackSound = (type) => {
    const audio = type === 'positive'
      ? positiveAudioRef.current
      : type === 'negative'
        ? negativeAudioRef.current
        : timeoutAudioRef.current;
    [positiveAudioRef.current, negativeAudioRef.current, timeoutAudioRef.current].forEach((item) => {
      if (item && item !== audio) {
        item.pause();
        item.currentTime = 0;
      }
    });
    if (audio) {
      try {
        audio.currentTime = 0;
        const playPromise = audio.play();
        if (playPromise && typeof playPromise.catch === 'function') {
          playPromise.catch(() => {});
        }
        return;
      } catch (error) {
        // fall back to WebAudio
      }
    }
    const ctx = getAudioContext();
    if (!ctx) return;
    const now = ctx.currentTime;
    const gain = ctx.createGain();
    gain.gain.setValueAtTime(0, now);
    gain.gain.linearRampToValueAtTime(0.12, now + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.9);
    gain.connect(ctx.destination);

    const osc = ctx.createOscillator();
    osc.type = type === 'positive' ? 'sine' : 'triangle';
    if (type === 'positive') {
      osc.frequency.setValueAtTime(660, now);
      osc.frequency.exponentialRampToValueAtTime(980, now + 0.35);
    } else if (type === 'negative') {
      osc.frequency.setValueAtTime(220, now);
      osc.frequency.exponentialRampToValueAtTime(140, now + 0.3);
    } else {
      osc.frequency.setValueAtTime(330, now);
      osc.frequency.exponentialRampToValueAtTime(220, now + 0.3);
    }
    osc.connect(gain);
    osc.start(now);
    osc.stop(now + 0.92);
  };

  const playTts = async (text, language = 'de-DE') => {
    if (!initData || !text) return Promise.resolve();
    const key = `${language}:${text}`;
    const now = Date.now();
    if (ttsLastRef.current.key === key && now - ttsLastRef.current.ts < 1200) {
      return Promise.resolve();
    }
    ttsLastRef.current = { key, ts: now };
    if (ttsCurrentAudioRef.current) {
      ttsCurrentAudioRef.current.pause();
      ttsCurrentAudioRef.current.currentTime = 0;
      ttsCurrentAudioRef.current = null;
    }
    const playAudio = (audio) => new Promise((resolve) => {
      ttsCurrentAudioRef.current = audio;
      audio.currentTime = 0;
      audio.onended = () => resolve();
      audio.onerror = () => resolve();
      audio.play().catch(() => resolve());
    });
    const playWebSpeech = () => new Promise((resolve) => {
      if (!('speechSynthesis' in window)) {
        resolve();
        return;
      }
      try {
        const utterance = new SpeechSynthesisUtterance(text);
        utterance.lang = language;
        utterance.rate = 0.95;
        utterance.onend = () => resolve();
        utterance.onerror = () => resolve();
        window.speechSynthesis.cancel();
        window.speechSynthesis.speak(utterance);
      } catch (error) {
        resolve();
      }
    });

    if (ttsCacheRef.current.has(key)) {
      const audio = ttsCacheRef.current.get(key);
      return playAudio(audio);
    }
    try {
      const response = await fetch('/api/webapp/tts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, text, language }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const audio = new Audio(url);
      ttsCacheRef.current.set(key, audio);
      return playAudio(audio);
    } catch (error) {
      console.warn('TTS error', error);
      return playWebSpeech();
    }
  };

  const coerceResponseJson = (value) => {
    if (!value) return null;
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch (error) {
        return null;
      }
    }
    if (typeof value === 'object') return value;
    return null;
  };

  const formatFeelLines = (text) => {
    const raw = String(text || '').trim();
    if (!raw) return [];
    const byNewline = raw
      .split(/\n+/)
      .map((line) => line.trim())
      .filter(Boolean);
    if (byNewline.length > 1) return byNewline;
    return raw
      .split(/(?<=[.!?])\s+(?=[A-ZА-ЯЁ])/)
      .map((line) => line.trim())
      .filter(Boolean);
  };

  const resolveFlashcardGerman = (entry) => {
    if (!entry) return '';
    const responseJson = entry.response_json || {};
    return (
      entry.word_de
      || responseJson.word_de
      || entry.translation_de
      || responseJson.translation_de
      || ''
    );
  };

  const loadSrsNextCard = async () => {
    if (!initData) return;
    try {
      setSrsLoading(true);
      const response = await fetch(`/api/cards/next?initData=${encodeURIComponent(initData)}`);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setSrsCard(data.card || null);
      setSrsState(data.srs || null);
      setSrsQueueInfo(data.queue_info || { due_count: 0, new_remaining_today: 0 });
      setSrsRevealAnswer(false);
      srsShownAtRef.current = Date.now();
    } catch (error) {
      setWebappError(`Ошибка загрузки SRS карточки: ${error.message}`);
    } finally {
      setSrsLoading(false);
    }
  };

  const submitSrsReview = async (ratingValue) => {
    if (!initData || !srsCard?.id) return;
    const startedAt = srsShownAtRef.current || Date.now();
    const responseMs = Math.max(0, Date.now() - startedAt);
    try {
      setSrsSubmitting(true);
      const response = await fetch('/api/cards/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          card_id: srsCard.id,
          rating: ratingValue,
          response_ms: responseMs,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await loadSrsNextCard();
    } catch (error) {
      setWebappError(`Ошибка SRS review: ${error.message}`);
    } finally {
      setSrsSubmitting(false);
    }
  };

  const scrollToDictionary = () => {
    if (dictionaryRef.current) {
      dictionaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const scrollToFlashcards = () => {
    if (flashcardsRef.current) {
      flashcardsRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const folderColorOptions = ['#5ddcff', '#7c5cff', '#ff6b6b', '#ffd166', '#06d6a0', '#f78c6b'];
  const folderIconOptions = ['book', 'bolt', 'star', 'target', 'flag', 'check'];

  const isSectionVisible = (key) => {
    if (flashcardsOnly) {
      return key === 'flashcards';
    }
    return selectedSections.has(key);
  };

  const showHero = !flashcardsOnly && selectedSections.size === 0;

  const toggleSection = (key) => {
    setSelectedSections((prev) => {
      if (!menuMultiSelect) {
        return new Set([key]);
      }
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const ensureSectionVisible = (key) => {
    setSelectedSections((prev) => {
      const next = new Set(prev);
      next.add(key);
      return next;
    });
  };

  const scrollToRef = (ref, options = {}) => {
    if (!ref?.current) return;
    const { center = false, block = 'start' } = options;
    if (center) {
      const rect = ref.current.getBoundingClientRect();
      const absoluteTop = window.scrollY + rect.top;
      const target = absoluteTop - (window.innerHeight - rect.height) / 2;
      window.scrollTo({ top: Math.max(0, target), behavior: 'smooth' });
      return;
    }
    ref.current.scrollIntoView({ behavior: 'smooth', block });
  };

  const openSectionAndScroll = (key, ref) => {
    ensureSectionVisible(key);
    setTimeout(() => {
      scrollToRef(ref, { center: key === 'flashcards', block: 'start' });
    }, 80);
  };

  const jumpToDictionaryFromSentence = () => {
    setLastLookupScrollY(window.scrollY);
    ensureSectionVisible('dictionary');
    setTimeout(() => {
      scrollToRef(dictionaryRef, { block: 'start' });
    }, 120);
  };

  const openFlashcardsSetup = (ref) => {
    setFlashcardsVisible(true);
    setFlashcardsOnly(false);
    setFlashcardSessionActive(false);
    setFlashcardPreviewActive(false);
    setFlashcardExitSummary(false);
    ensureSectionVisible('flashcards');
    if (ref?.current) {
      setTimeout(() => {
        scrollToRef(ref, { center: true });
      }, 120);
    }
  };

  const showAllSections = () => {
    setSelectedSections(new Set(['translations', 'youtube', 'movies', 'dictionary', 'flashcards', 'assistant', 'analytics']));
    setMoviesCollapsed(false);
  };

  const hideAllSections = () => {
    setSelectedSections(new Set());
    setMoviesCollapsed(false);
  };

  const handleMenuSelection = (key, ref) => {
    setSelectedSections((prev) => {
      if (!menuMultiSelect) {
        return new Set([key]);
      }
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
    if (key === 'flashcards') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(false);
      setFlashcardSessionActive(false);
      setFlashcardExitSummary(false);
    }
    if (key === 'movies') {
      setMoviesCollapsed(false);
    }
    if (!menuMultiSelect) {
      setMenuOpen(false);
    }
    if (ref?.current) {
      setTimeout(() => {
        scrollToRef(ref, { center: key === 'flashcards', block: 'start' });
      }, 120);
    }
  };

  const resolveFolderIconLabel = (icon) => {
    const map = {
      book: 'Book',
      bolt: 'Bolt',
      star: 'Star',
      target: 'Target',
      flag: 'Flag',
      check: 'Check',
    };
    return map[icon] || 'Folder';
  };

  const renderFolderIcon = (icon, color) => {
    const fill = color || '#5ddcff';
    const stroke = '#0b1020';
    if (icon === 'bolt') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M13 2L4 14h6l-1 8 9-12h-6l1-8z" fill={fill} stroke={stroke} strokeWidth="1.4" />
        </svg>
      );
    }
    if (icon === 'star') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 2l2.7 5.6 6.2.9-4.5 4.4 1 6.2L12 16.8 6.6 19.1l1-6.2-4.5-4.4 6.2-.9L12 2z" fill={fill} stroke={stroke} strokeWidth="1.2" />
        </svg>
      );
    }
    if (icon === 'target') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="9" fill="none" stroke={fill} strokeWidth="2.4" />
          <circle cx="12" cy="12" r="5" fill="none" stroke={fill} strokeWidth="2.4" />
          <circle cx="12" cy="12" r="2" fill={fill} />
        </svg>
      );
    }
    if (icon === 'flag') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M6 3v18M6 4h11l-2 3 2 3H6" fill={fill} stroke={stroke} strokeWidth="1.4" />
        </svg>
      );
    }
    if (icon === 'check') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M5 12l4 4 10-10" fill="none" stroke={fill} strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    }
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 6h8l2 2h6v10a2 2 0 0 1-2 2H4z" fill={fill} stroke={stroke} strokeWidth="1.2" />
      </svg>
    );
  };

  // Состояние для хранения токена доступа. Изначально его нет.
  // Мы говорим React'у: "Создай ячейку памяти. Изначально положи туда null (пустоту)".
  // Когда мы захотим обновить эту ячейку, мы будем использовать функцию setToken.
  // Каждый раз, когда мы вызываем setToken с новым значением, React "замечает" это изменение
  // и перерисовывает компонент App с новым значением token.
  // Аналогично: const [username, setUsername] = useState(''); — создали память для имени пользователя, изначально пустая строка.
  // Итог: useState — это способ "создать память" внутри функционального компонента React.
  const [token, setToken] = useState(null);
  const [assistantToken, setAssistantToken] = useState(null);
  const [assistantConnecting, setAssistantConnecting] = useState(false);
  const [assistantError, setAssistantError] = useState('');

  // LiveKit login state
  const [telegramID, setTelegramID] = useState('');
  const [username, setUsername] = useState('');

  const assistantIdentity = useMemo(() => {
    const userId = webappUser?.id ? String(webappUser.id) : '';
    const fullName = [webappUser?.first_name, webappUser?.last_name]
      .filter(Boolean)
      .join(' ')
      .trim();
    const displayName = fullName || webappUser?.username || (userId ? `user_${userId}` : '');
    return { userId, displayName };
  }, [webappUser]);

  const handleConnect = async (e) => {
    e.preventDefault();
    if (!telegramID || !username) {
      alert('Пожалуйста, введите ваше имя');
      return;
    }

    try {
      const response = await fetch(
        `/api/token?user_id=${encodeURIComponent(telegramID)}&username=${encodeURIComponent(username)}`
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Ошибка получения токена: ${errorText}`);
      }

      const data = await response.json();
      setToken(data.token);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  };

  const connectAssistant = async () => {
    const userId = assistantIdentity.userId;
    const displayName = assistantIdentity.displayName;
    if (!userId || !displayName) {
      setAssistantError('Не удалось определить пользователя Telegram. Обновите страницу.');
      return;
    }
    try {
      setAssistantConnecting(true);
      setAssistantError('');
      const response = await fetch(
        `/api/token?user_id=${encodeURIComponent(userId)}&username=${encodeURIComponent(displayName)}`
      );
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setAssistantToken(data.token);
    } catch (error) {
      setAssistantError(`Ошибка подключения ассистента: ${error.message}`);
    } finally {
      setAssistantConnecting(false);
    }
  };

  const disconnectAssistant = () => {
    setAssistantToken(null);
    setAssistantError('');
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }

    const bootstrap = async () => {
      try {
        setWebappError('');
        const response = await fetch('/api/webapp/bootstrap', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData }),
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const data = await response.json();
        setSessionId(data.session_id);
        setWebappUser(data.user);
        const unsafeChatType = telegramApp?.initDataUnsafe?.chat?.type || telegramApp?.initDataUnsafe?.chat_type;
        setWebappChatType(data.chat_type || unsafeChatType || '');
      } catch (error) {
        setWebappError(`Ошибка инициализации: ${error.message}`);
      }
    };

    bootstrap();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    if (flashcardsOnly || !selectedSections.has('assistant')) {
      setAssistantToken(null);
    }
  }, [flashcardsOnly, selectedSections]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('review') === '1') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
    const startParam = telegramApp?.initDataUnsafe?.start_param;
    if (startParam === 'review' || startParam === 'flashcards') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
    if (window.location.pathname === '/webapp/review') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
  }, []);

  useEffect(() => {
    const storedAvatar = safeStorageGet('webapp_avatar');
    if (storedAvatar) {
      setUserAvatar(storedAvatar);
    }
  }, []);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    loadFolders();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    if (!initData || flashcardsOnly || !isSectionVisible('flashcards')) {
      return;
    }
    loadSrsNextCard();
  }, [initData, flashcardsOnly, selectedSections]);

  useEffect(() => {
    if (!flashcardSessionActive) {
      return;
    }
    if (flashcards.length === 0) {
      loadFlashcards();
    }
    scrollToFlashcards();
  }, [flashcardSessionActive, initData, flashcardFolderMode, flashcardFolderId, flashcardSetSize, flashcards.length]);

  useEffect(() => {
    if (!flashcards.length) {
      setFlashcardOptions([]);
      return;
    }
    const entry = flashcards[flashcardIndex];
    setFlashcardOptions(buildFlashcardOptions(entry, flashcards));
    setFlashcardSelection(null);
    setFlashcardOutcome(null);
    flashcardRoundStartRef.current = Date.now();
  }, [flashcards, flashcardIndex]);

  useEffect(() => {
    if (!flashcardPreviewActive || !flashcardsOnly) {
      if (ttsCurrentAudioRef.current) {
        ttsCurrentAudioRef.current.pause();
        ttsCurrentAudioRef.current.currentTime = 0;
        ttsCurrentAudioRef.current = null;
      }
      setPreviewAudioReady(false);
      setPreviewAudioPlaying(false);
      return;
    }
    const entry = flashcards[flashcardPreviewIndex];
    if (!entry) {
      setPreviewAudioReady(false);
      setPreviewAudioPlaying(false);
      return;
    }
    const text = resolveFlashcardGerman(entry);
    let cancelled = false;
    const run = async () => {
      if (!text) {
        setPreviewAudioReady(true);
        setPreviewAudioPlaying(false);
        return;
      }
      setPreviewAudioReady(false);
      setPreviewAudioPlaying(true);
      await playTts(text, 'de-DE');
      if (cancelled) return;
      setPreviewAudioPlaying(false);
      setPreviewAudioReady(true);
    };
    run();
    return () => {
      cancelled = true;
    };
  }, [flashcardPreviewActive, flashcardsOnly, flashcards, flashcardPreviewIndex]);

  useEffect(() => {
    if (!flashcardPreviewActive || !flashcardsOnly) return;
    const entry = flashcards[flashcardPreviewIndex];
    if (!entry?.id) return;
    setFlashcardFeelVisibleMap((prev) => ({
      ...prev,
      [entry.id]: false,
    }));
  }, [flashcardPreviewActive, flashcardsOnly, flashcards, flashcardPreviewIndex]);

  useEffect(() => {
    flashcardIndexRef.current = flashcardIndex;
  }, [flashcardIndex]);

  useEffect(() => {
    flashcardSelectionRef.current = flashcardSelection;
  }, [flashcardSelection]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcards.length === 0) {
      return;
    }
    if (flashcardTrainingMode !== 'quiz') {
      return;
    }
    setFlashcardTimerKey((prev) => prev + 1);
  }, [flashcardIndex, flashcardSessionActive, flashcards.length, flashcardDurationSec, flashcardTrainingMode]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcardSetComplete || flashcardExitSummary || !flashcards.length) {
      return;
    }
    if (flashcardTrainingMode !== 'quiz') {
      return;
    }
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
    }
    setFlashcardTimedOut(false);
    autoAdvanceTimeoutRef.current = setTimeout(() => {
      const currentIndex = flashcardIndexRef.current;
      const currentSelection = flashcardSelectionRef.current;
      if (currentSelection !== null) return;
      const entry = flashcards[currentIndex];
      if (!entry) return;
      const responseJson = entry.response_json || {};
      const correct = entry.translation_de
        || responseJson.translation_de
        || entry.translation_ru
        || responseJson.translation_ru
        || '—';
      const timeSpentMs = Math.max(0, Date.now() - flashcardRoundStartRef.current);
      recordFlashcardAnswer(entry.id, false, {
        mode: 'quiz',
        timeSpentMs,
        hintsUsed: 0,
      });
      setFlashcardStats((prev) => ({
        ...prev,
        wrong: prev.wrong + 1,
      }));
      setFlashcardTimedOut(true);
      playFeedbackSound('timeout');
      setFlashcardSelection(-1);
      setFlashcardOutcome('timeout');
      (async () => {
        const german = resolveFlashcardGerman(entry);
        if (german) {
          await playTts(german, 'de-DE');
        }
        if (flashcardAutoAdvance) {
          revealTimeoutRef.current = setTimeout(() => {
            advanceFlashcard();
          }, 3000);
        }
      })();
    }, flashcardDurationSec * 1000);

    return () => {
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
    };
  }, [
    flashcardSessionActive,
    flashcardSetComplete,
    flashcardExitSummary,
    flashcards,
    flashcardIndex,
    flashcardDurationSec,
    flashcardTrainingMode,
  ]);

  useEffect(() => {
    if (!flashcardSessionActive && autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
    if (!flashcardSessionActive && revealTimeoutRef.current) {
      clearTimeout(revealTimeoutRef.current);
      revealTimeoutRef.current = null;
    }
  }, [flashcardSessionActive]);

  useEffect(() => {
    if (!blocksMenuOpen) return;
    const onPointerDown = (event) => {
      if (!blocksMenuRef.current?.contains(event.target)) {
        setBlocksMenuOpen(false);
        setBlocksMenuSettingsOpen(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setBlocksMenuOpen(false);
        setBlocksMenuSettingsOpen(false);
      }
    };
    document.addEventListener('pointerdown', onPointerDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [blocksMenuOpen]);

  useEffect(() => {
    setBlocksMenuOpen(false);
    setBlocksMenuSettingsOpen(false);
    setBlocksFinishConfirmOpen(false);
  }, [flashcardIndex, flashcardPreviewActive, flashcardTrainingMode]);

  useEffect(() => {
    if (!blocksFinishConfirmOpen) return;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setBlocksFinishConfirmOpen(false);
      }
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [blocksFinishConfirmOpen]);

  const loadSentences = async () => {
    if (!initData) {
      return;
    }
    try {
      setFinishMessage('');
      const response = await fetch('/api/webapp/sentences', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 7 }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setSentences(data.items || []);
      setResults([]);
      setFinishStatus('idle');
    } catch (error) {
      setWebappError(`Ошибка загрузки предложений: ${error.message}`);
    }
  };

  const loadTopics = async () => {
    setTopicsLoading(true);
    setTopicsError('');
    try {
      const response = await fetch('/api/webapp/topics');
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const items = Array.isArray(data.items) ? data.items : [];
      setTopics(items);
      if (items.length > 0 && !items.includes(selectedTopic)) {
        setSelectedTopic(items[0]);
      }
    } catch (error) {
      setTopicsError(`Ошибка тем: ${error.message}`);
    } finally {
      setTopicsLoading(false);
    }
  };

  useEffect(() => {
    if (!webappUser?.id || sentences.length === 0) {
      return;
    }
    const storageKey = `webappDrafts_${webappUser.id}_${sessionId || 'nosession'}`;
    const stored = safeStorageGet(storageKey);
    const sentenceIds = sentences.map((item) => String(item.id_for_mistake_table));
    let initial = sentenceIds.reduce((acc, id) => ({ ...acc, [id]: '' }), {});
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        if (parsed && typeof parsed === 'object') {
          initial = sentenceIds.reduce((acc, id) => ({
            ...acc,
            [id]: parsed[id] || '',
          }), {});
        }
      } catch (error) {
        console.warn('Failed to parse saved drafts', error);
      }
    }
    setTranslationDrafts(initial);
  }, [sentences, webappUser?.id, sessionId]);

  useEffect(() => {
    if (!webappUser?.id || Object.keys(translationDrafts).length === 0) {
      return;
    }
    const storageKey = `webappDrafts_${webappUser.id}_${sessionId || 'nosession'}`;
    safeStorageSet(storageKey, JSON.stringify(translationDrafts));
  }, [translationDrafts, webappUser?.id, sessionId]);

  useEffect(() => {
    if (isWebAppMode && initData) {
      loadTopics();
      loadSentences();
      loadSessionInfo();
    }
  }, [initData, isWebAppMode]);

  useEffect(() => {
    if (isStoryTopic(selectedTopic) && initData) {
      loadStoryHistory();
    }
  }, [selectedTopic, initData]);

  useEffect(() => {
    if (!isStoryTopic(selectedTopic)) {
      setStoryResult(null);
      setStoryGuess('');
    }
  }, [selectedTopic]);

  useEffect(() => {
    const stored = safeStorageGet('webapp_youtube');
    if (!stored) return;
    try {
      const parsed = JSON.parse(stored);
      if (parsed?.input) {
        setYoutubeInput(parsed.input);
      }
      if (parsed?.id) {
        setYoutubeId(parsed.id);
      }
    } catch (error) {
      // ignore
    }
  }, []);

  const handleWebappSubmit = async (event) => {
    event.preventDefault();
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (sentences.length === 0) {
      setWebappError('Нет предложений для перевода.');
      return;
    }
    if (Object.values(translationDrafts).every((text) => !text.trim())) {
      setWebappError('Заполните хотя бы один перевод.');
      return;
    }

    const numberedOriginal = sentences
      .map((item) => `${item.unique_id ?? item.id_for_mistake_table}. ${item.sentence}`)
      .join('\n');
    const numberedTranslations = sentences
      .map((item) => {
        const translation = translationDrafts[String(item.id_for_mistake_table)] || '';
        return `${item.unique_id ?? item.id_for_mistake_table}. ${translation}`;
      })
      .join('\n');

    setWebappLoading(true);
    setWebappError('');
    setResults([]);
    setExplanations({});
    setExplanationLoading({});

    try {
      const submittedIds = Object.entries(translationDrafts)
        .filter(([, text]) => text && text.trim())
        .map(([id]) => Number(id));
      const response = await fetch('/api/message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          session_id: sessionId,
          translations: Object.entries(translationDrafts).map(([id, translation]) => ({
            id_for_mistake_table: Number(id),
            translation,
          })),
          original_text: numberedOriginal,
          user_translation: numberedTranslations,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setResults(data.results || []);
      if (submittedIds.length > 0) {
        setSentences((prev) => prev.filter((item) => !submittedIds.includes(Number(item.id_for_mistake_table))));
        setTranslationDrafts((prev) => {
          const next = { ...prev };
          submittedIds.forEach((id) => {
            delete next[String(id)];
          });
          const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
          if (Object.keys(next).length === 0) {
            safeStorageRemove(storageKey);
          } else {
            safeStorageSet(storageKey, JSON.stringify(next));
          }
          return next;
        });
      }
    } catch (error) {
      setWebappError(`Ошибка проверки: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleTranslationSubmit = (event) => {
    if (sessionType === 'story' || isStoryTopic(selectedTopic)) {
      event.preventDefault();
      handleStorySubmit();
      return;
    }
    handleWebappSubmit(event);
  };

  const handleStartTranslation = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    try {
      const response = await fetch('/api/webapp/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          topic: selectedTopic,
          level: selectedLevel,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      if (data.blocked) {
        setFinishMessage('Есть активная сессия. Завершите текущий перевод, чтобы получить новый сет.');
      }
      await loadSessionInfo();
      await loadSentences();
    } catch (error) {
      setWebappError(`Ошибка старта: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const loadStoryHistory = async () => {
    if (!initData) return;
    setStoryHistoryLoading(true);
    setStoryHistoryError('');
    try {
      const response = await fetch('/api/webapp/story/history', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 10 }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setStoryHistory(Array.isArray(data.items) ? data.items : []);
    } catch (error) {
      setStoryHistoryError(`Ошибка истории: ${error.message}`);
    } finally {
      setStoryHistoryLoading(false);
    }
  };

  const loadSessionInfo = async () => {
    if (!initData) return;
    try {
      const response = await fetch('/api/webapp/session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const type = data.type || 'none';
      setSessionType(type);
      if (type === 'story') {
        setSelectedTopic(STORY_TOPIC);
      }
    } catch (error) {
      // silent
    }
  };

  const handleStartStory = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    setStoryResult(null);
    setResults([]);
    setExplanations({});
    try {
      const response = await fetch('/api/webapp/story/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          mode: storyMode,
          story_type: storyType,
          difficulty: storyDifficulty,
          story_id: storyMode === 'repeat' && selectedStoryId ? Number(selectedStoryId) : null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      if (data.blocked) {
        setFinishMessage('Есть активная сессия. Завершите текущий перевод, чтобы получить новый сет.');
      }
      await loadSessionInfo();
      await loadSentences();
    } catch (error) {
      setWebappError(`Ошибка старта истории: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleStorySubmit = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    const missing = sentences.filter((item) => {
      const value = (translationDrafts[String(item.id_for_mistake_table)] || '').trim();
      return !value;
    });
    if (missing.length > 0) {
      setWebappError('Для истории нужно перевести все 7 предложений.');
      return;
    }
    if (!storyGuess.trim()) {
      setWebappError('Введите ваш ответ: о ком/чем была история.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setResults([]);
    setExplanations({});
    try {
      const response = await fetch('/api/webapp/story/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          translations: Object.entries(translationDrafts).map(([id, translation]) => ({
            id_for_mistake_table: Number(id),
            translation,
          })),
          guess: storyGuess,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setStoryResult(data);
      setSentences([]);
      await loadSessionInfo();
    } catch (error) {
      setWebappError(`Ошибка истории: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleDraftChange = (sentenceId, value) => {
    setTranslationDrafts((prev) => ({
      ...prev,
      [String(sentenceId)]: value,
    }));
  };

  const normalizeSelectionText = (value) => {
    if (!value) return '';
    return value.replace(/\s+/g, ' ').trim();
  };

  const resolveDictionaryDirection = (item) => {
    if (!item) return 'ru-de';
    if (item.translation_de) return 'ru-de';
    if (item.translation_ru) return 'de-ru';
    if (item.word_de) return 'de-ru';
    return 'ru-de';
  };

  const hasCyrillic = (value) => /[А-Яа-яЁё]/.test(value || '');

  const handleSelection = (event, overrideText = '') => {
    const text = overrideText || normalizeSelectionText(window.getSelection()?.toString() || '');
    if (!text) {
      setSelectionText('');
      setSelectionPos(null);
      return;
    }
    const clientX = event?.clientX ?? event?.touches?.[0]?.clientX ?? window.innerWidth / 2;
    const clientY = event?.clientY ?? event?.touches?.[0]?.clientY ?? window.innerHeight / 3;
    setSelectionText(text);
    setSelectionPos({ x: clientX, y: clientY });
  };

  const clearSelection = () => {
    setSelectionText('');
    setSelectionPos(null);
  };

  const handleQuickAddToDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    let normalized = cleaned;
    if (!hasCyrillic(cleaned)) {
      try {
        const normalizeResponse = await fetch('/api/webapp/normalize/de', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, text: cleaned }),
        });
        if (normalizeResponse.ok) {
          const data = await normalizeResponse.json();
          normalized = data.normalized || cleaned;
        }
      } catch (error) {
        // ignore normalization errors
      }
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized }),
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
      setDictionaryResult(data.item || null);
      const detectedDirection = data.direction || resolveDictionaryDirection(data.item);
      setDictionaryDirection(detectedDirection);
      scrollToDictionary();

      const saveResponse = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: detectedDirection === 'ru-de' ? normalized : '',
          word_de: detectedDirection === 'de-ru' ? normalized : '',
          translation_de: data.item?.translation_de || '',
          translation_ru: data.item?.translation_ru || '',
          response_json: data.item || {},
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        }),
      });
      if (!saveResponse.ok) {
        let message = await saveResponse.text();
        try {
          const payload = JSON.parse(message);
          message = payload.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      setDictionarySaved('Добавлено в словарь ✅');
      clearSelection();
    } catch (error) {
      setDictionaryError(`Ошибка сохранения: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleQuickLookupDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    let normalized = cleaned;
    if (!hasCyrillic(cleaned)) {
      try {
        const normalizeResponse = await fetch('/api/webapp/normalize/de', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, text: cleaned }),
        });
        if (normalizeResponse.ok) {
          const data = await normalizeResponse.json();
          normalized = data.normalized || cleaned;
        }
      } catch (error) {
        // ignore normalization errors
      }
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized }),
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
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
      scrollToDictionary();
      clearSelection();
    } catch (error) {
      setDictionaryError(`Ошибка словаря: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const loadFlashcards = async () => {
    if (!initData) {
      setFlashcardsError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setFlashcardsLoading(true);
    setFlashcardsError('');
    try {
      const response = await fetch('/api/webapp/flashcards/set', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          set_size: flashcardSetSize,
          wrong_size: 5,
          folder_mode: flashcardFolderMode,
          folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const items = (data.items || []).map((item) => ({
        ...item,
        response_json: coerceResponseJson(item.response_json),
      }));
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
      if (revealTimeoutRef.current) {
        clearTimeout(revealTimeoutRef.current);
        revealTimeoutRef.current = null;
      }
      setFlashcards(items);
      setFlashcardIndex(0);
      setFlashcardSelection(null);
      setFlashcardSetComplete(false);
      setFlashcardTimedOut(false);
      setFlashcardExitSummary(false);
      setFlashcardTimerKey((prev) => prev + 1);
      setFlashcardPreviewIndex(0);
      setFlashcardStats({
        total: items.length,
        correct: 0,
        wrong: 0,
      });
    } catch (error) {
      setFlashcardsError(`Ошибка карточек: ${error.message}`);
    } finally {
      setFlashcardsLoading(false);
    }

    (async () => {
      try {
        const poolResponse = await fetch('/api/webapp/dictionary/cards', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            limit: 60,
            folder_mode: flashcardFolderMode,
            folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
          }),
        });
        if (poolResponse.ok) {
          const poolData = await poolResponse.json();
          const poolItems = (poolData.items || []).map((item) => ({
            ...item,
            response_json: coerceResponseJson(item.response_json),
          }));
          setFlashcardPool(poolItems);
        }
      } catch (error) {
        // ignore pool errors
      }
    })();
  };

  const loadFolders = async () => {
    if (!initData) {
      return;
    }
    setFoldersLoading(true);
    setFoldersError('');
    try {
      const response = await fetch('/api/webapp/dictionary/folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setFolders(data.items || []);
    } catch (error) {
      setFoldersError(`Ошибка папок: ${error.message}`);
    } finally {
      setFoldersLoading(false);
    }
  };

  const handleCreateFolder = async () => {
    if (!initData) {
      setFoldersError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!newFolderName.trim()) {
      setFoldersError('Введите название папки.');
      return;
    }
    setFoldersLoading(true);
    setFoldersError('');
    try {
      const response = await fetch('/api/webapp/dictionary/folders/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          name: newFolderName.trim(),
          color: newFolderColor,
          icon: newFolderIcon,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const created = data.item;
      setFolders((prev) => [created, ...prev]);
      setDictionaryFolderId(String(created.id));
      setShowNewFolderForm(false);
      setNewFolderName('');
    } catch (error) {
      setFoldersError(`Ошибка создания папки: ${error.message}`);
    } finally {
      setFoldersLoading(false);
    }
  };

  const shuffleArray = (items) => {
    const array = [...items];
    const rand = () => {
      if (window.crypto?.getRandomValues) {
        const buf = new Uint32Array(1);
        window.crypto.getRandomValues(buf);
        return buf[0] / 2 ** 32;
      }
      return Math.random();
    };
    for (let i = array.length - 1; i > 0; i -= 1) {
      const j = Math.floor(rand() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
  };

  const buildFlashcardOptions = (entry, allEntries) => {
    const responseJson = entry?.response_json || {};
    const correct = entry?.translation_de
      || responseJson.translation_de
      || entry?.translation_ru
      || responseJson.translation_ru
      || '';
    if (!correct) return [];
    const pool = [...allEntries, ...flashcardPool]
      .filter((item) => item && item.id !== entry?.id);
    const values = Array.from(new Set(
      pool
        .map((item) => (
          item.translation_de
            || item.response_json?.translation_de
            || item.translation_ru
            || item.response_json?.translation_ru
            || ''
        ))
        .filter(Boolean)
        .filter((value) => value !== correct)
    ));

    const distractors = [];
    while (distractors.length < 3 && values.length > 0) {
      const index = Math.floor(Math.random() * values.length);
      distractors.push(values.splice(index, 1)[0]);
    }

    const options = Array.from(new Set([correct, ...distractors]));
    while (options.length < 4 && values.length > 0) {
      options.push(values.shift());
    }
    while (options.length < 4) {
      options.push(correct);
    }
    return shuffleArray(options).slice(0, 4);
  };

  const resolveBlocksAnswer = (entry) => {
    const responseJson = entry?.response_json || {};
    const translationDe = entry?.translation_de || responseJson.translation_de || '';
    const translationArray = Array.isArray(responseJson.translations)
      ? responseJson.translations.filter(Boolean)
      : [];
    const raw = translationArray[0]
      || translationDe
      || entry?.word_de
      || responseJson.word_de
      || '';
    if (!raw) return '';
    const normalized = String(raw).replace(/\s+/g, ' ').trim();
    // Keep commas inside phrases. Split by semicolon/slash as alternative list separators.
    if (normalized.includes(';') || normalized.includes('/')) {
      return normalized.split(/[;/]/)[0]?.trim() || normalized;
    }
    return normalized;
  };

  const resolveBlocksPrompt = (entry) => {
    const responseJson = entry?.response_json || {};
    return entry?.word_ru
      || responseJson.word_ru
      || entry?.translation_ru
      || responseJson.translation_ru
      || 'Соберите ответ';
  };

  const resolveBlocksType = (entry, answer) => {
    const responseJson = entry?.response_json || {};
    const explicit = String(entry?.type || responseJson.type || '').toUpperCase();
    const tokens = String(answer || '').trim().split(/\s+/).filter(Boolean);
    if (tokens.length === 2 && ['DER', 'DIE', 'DAS', 'DEN', 'DEM', 'DES', 'EIN', 'EINE', 'EINEN', 'EINEM', 'EINER', 'EINES'].includes(tokens[0].toUpperCase())) {
      return 'WORD';
    }
    if (explicit === 'WORD' || explicit === 'PHRASE') return explicit;
    return /\s+/.test(String(answer || '').trim()) ? 'PHRASE' : 'WORD';
  };

  const recordFlashcardAnswer = async (entryId, isCorrect, meta = {}) => {
    if (!initData || !entryId) {
      return;
    }
    try {
      await fetch('/api/webapp/flashcards/answer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          entry_id: entryId,
          is_correct: isCorrect,
          mode: meta.mode || flashcardTrainingMode || 'quiz',
          time_spent_ms: typeof meta.timeSpentMs === 'number' ? Math.max(0, Math.round(meta.timeSpentMs)) : null,
          hints_used: typeof meta.hintsUsed === 'number' ? Math.max(0, Math.round(meta.hintsUsed)) : 0,
        }),
      });
    } catch (error) {
      // ignore answer tracking errors
    }
  };

  const advanceFlashcard = () => {
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
    const nextIndex = flashcardIndex + 1;
    if (nextIndex >= flashcards.length) {
      setFlashcardSetComplete(true);
      return;
    }
    setFlashcardIndex(nextIndex);
    setFlashcardSelection(null);
  };

  const resetCurrentBlocksCard = () => {
    setBlocksResetNonce((prev) => prev + 1);
    setFlashcardTimedOut(false);
    setFlashcardOutcome(null);
  };

  const requestFinishFlashcardSession = () => {
    const tg = window.Telegram?.WebApp;
    if (tg?.showPopup) {
      try {
        tg.showPopup(
          {
            title: 'Завершить повтор?',
            message: 'Текущий прогресс будет завершён.',
            buttons: [
              { id: 'continue', type: 'default', text: 'Продолжить' },
              { id: 'finish', type: 'destructive', text: 'Завершить' },
            ],
          },
          (buttonId) => {
            if (buttonId === 'finish') {
              setFlashcardExitSummary(true);
            }
          }
        );
        return;
      } catch (error) {
        // fallback to custom dialog below
      }
    }
    setBlocksFinishConfirmOpen(true);
  };

  const handleAvatarUpload = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      if (!result) return;
      setUserAvatar(result);
      safeStorageSet('webapp_avatar', result);
    };
    reader.readAsDataURL(file);
  };

  const normalizeSubtitleText = (text) => {
    if (!text) return '';
    return text
      .replace(/&gt;/g, '>')
      .replace(/&lt;/g, '<')
      .replace(/&amp;/g, '&')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\b\d{2}:\d{2}:\d{2}\.\d{3}\b/g, ' ')
      .replace(/\b\d{2}:\d{2}\.\d{3}\b/g, ' ')
      .replace(/\u00a0/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  };

  const renderClickableText = (text) => {
    if (!text) return null;
    return text.split(/\s+/).map((word, index) => {
      const cleaned = word.replace(/[^A-Za-zÄÖÜäöüßÀ-ÿ'’-]/g, '');
      if (!cleaned) {
        return <span key={`w-${index}`}>{word} </span>;
      }
      return (
        <span
          key={`w-${index}`}
          className="clickable-word"
          onClick={(event) => handleSelection(event, cleaned)}
        >
          {word}{' '}
        </span>
      );
    });
  };

  const renderSubtitleText = (text) => renderClickableText(normalizeSubtitleText(text));

  const getActiveSubtitleIndex = () => {
    const hasTiming = youtubeTranscriptHasTiming || youtubeTranscript.some((item) => Number(item?.start) > 0);
    if (!hasTiming) return -1;
    if (!youtubeTranscript.length) return -1;
    const time = youtubeCurrentTime || 0;
    let activeIndex = -1;
    for (let i = 0; i < youtubeTranscript.length; i += 1) {
      const start = Number(youtubeTranscript[i]?.start ?? 0);
      const nextStart = Number(youtubeTranscript[i + 1]?.start ?? Number.POSITIVE_INFINITY);
      if (time >= start && time < nextStart) {
        activeIndex = i;
        break;
      }
    }
    return activeIndex;
  };

  const parseTranscriptInput = (value) => {
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
  };

  const parseTimedTranscript = (value) => {
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
  };

  const parseSimpleTimestampTranscript = (value) => {
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
  };

  const detectTranscriptLanguage = (items) => {
    const sample = items.map((item) => String(item?.text || '')).join(' ');
    if (!sample) return null;
    const hasCyrillic = /[А-Яа-яЁё]/.test(sample);
    const hasLatin = /[A-Za-z]/.test(sample);
    if (hasCyrillic && !hasLatin) return 'ru';
    if (hasLatin && !hasCyrillic) return 'de';
    if (hasCyrillic && hasLatin) return 'en';
    return null;
  };

  const saveManualTranscriptToDb = async (items) => {
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
      setMovies([]);
    } catch (error) {
      setYoutubeTranscriptError(`Ошибка сохранения субтитров: ${error.message}`);
    }
  };

  const handleManualTranscript = async () => {
    const raw = manualTranscript.trim();
    if (!raw) {
      setYoutubeManualOverride(false);
      setYoutubeTranscript([]);
      setYoutubeTranscriptHasTiming(true);
      return;
    }
    const parsed = parseTimedTranscript(raw);
    if (parsed.items.length) {
      setYoutubeTranscript(parsed.items);
      setYoutubeTranscriptHasTiming(parsed.hasTiming);
      setYoutubeManualOverride(true);
      setYoutubeTranscriptError('');
      setShowManualTranscript(false);
      await saveManualTranscriptToDb(parsed.items);
      return;
    }
    const simple = parseSimpleTimestampTranscript(raw);
    if (simple.items.length) {
      setYoutubeTranscript(simple.items);
      setYoutubeTranscriptHasTiming(simple.hasTiming);
      setYoutubeManualOverride(true);
      setYoutubeTranscriptError('');
      setShowManualTranscript(false);
      await saveManualTranscriptToDb(simple.items);
      return;
    }
    const fallback = parseTranscriptInput(raw);
    setYoutubeTranscript(fallback);
    setYoutubeTranscriptHasTiming(false);
    setYoutubeManualOverride(true);
    setYoutubeTranscriptError('');
    setShowManualTranscript(false);
    await saveManualTranscriptToDb(fallback);
  };

  const handleFinishTranslation = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    try {
      const response = await fetch('/api/webapp/finish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
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
      setFinishMessage(data.message || 'Перевод завершён.');
      setFinishStatus('done');
      const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
      safeStorageRemove(storageKey);
      setTranslationDrafts({});
      setSessionType('none');
      setStoryGuess('');
      setStoryResult(null);
      setResults([]);
      setSelectedTopic('💼 Business');
      await loadSentences();
    } catch (error) {
      setWebappError(`Ошибка завершения: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleExplainTranslation = async (item) => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    const key = String(item.sentence_number ?? item.original_text);
    setExplanationLoading((prev) => ({ ...prev, [key]: true }));
    try {
      const response = await fetch('/api/webapp/explain', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          original_text: item.original_text,
          user_translation: item.user_translation,
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
      setExplanations((prev) => ({ ...prev, [key]: data.explanation }));
    } catch (error) {
      setWebappError(`Ошибка объяснения: ${error.message}`);
    } finally {
      setExplanationLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  const renderRichText = (text) => {
    if (!text) return '';
    const escaped = text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    return escaped
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.+?)\*/g, '<strong>$1</strong>');
  };

  const renderFeedback = (feedback) => {
    if (!feedback) return null;
    const lines = feedback.split('\n').map((line) => line.trim()).filter(Boolean);
    return lines.map((line, index) => {
      const match = line.match(/Correct Translation:\*?\s*(.+)$/i);
      if (match) {
        return (
          <div
            key={`fb-${index}`}
            className="webapp-feedback-line"
            onMouseUp={handleSelection}
          >
            <span className="webapp-feedback-label">🟣 Correct Translation:</span>
            <span className="webapp-feedback-value">{renderClickableText(match[1])}</span>
          </div>
        );
      }
      return (
        <div
          key={`fb-${index}`}
          className="webapp-feedback-line"
          dangerouslySetInnerHTML={{ __html: renderRichText(line) }}
        />
      );
    });
  };


  const handleDictionaryLookup = async (event) => {
    event.preventDefault();
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!dictionaryWord.trim()) {
      setDictionaryError('Введите слово или фразу для словаря.');
      return;
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionaryResult(null);
    setDictionarySaved('');
    setLastLookupScrollY(null);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: dictionaryWord.trim() }),
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
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
    } catch (error) {
      setDictionaryError(`Ошибка словаря: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleDictionarySave = async () => {
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!dictionaryResult) {
      setDictionaryError('Сначала выполните перевод в словаре.');
      return;
    }
    setCollocationsVisible(true);
    setCollocationsLoading(true);
    setCollocationsError('');
    setCollocationOptions([]);
    setSelectedCollocation(null);
    try {
      const response = await fetch('/api/webapp/dictionary/collocations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word: dictionaryWord.trim() || dictionaryResult.word_ru || dictionaryResult.word_de,
          translation: dictionaryDirection === 'ru-de'
            ? (dictionaryResult.translation_de || '')
            : (dictionaryResult.translation_ru || ''),
          direction: dictionaryDirection,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const baseSource = dictionaryDirection === 'ru-de'
        ? (dictionaryResult.word_ru || dictionaryWord.trim())
        : (dictionaryResult.word_de || dictionaryWord.trim());
      const baseTarget = dictionaryDirection === 'ru-de'
        ? (dictionaryResult.translation_de || '')
        : (dictionaryResult.translation_ru || '');
      const options = [
        { source: baseSource, target: baseTarget, isBase: true },
        ...(data.items || []).map((item) => ({
          source: item.source,
          target: item.target,
          isBase: false,
        })),
      ].filter((item) => item.source && item.target);
      setCollocationOptions(options);
      setSelectedCollocation(options[0] || null);
    } catch (error) {
      setCollocationsError(`Ошибка связок: ${error.message}`);
    } finally {
      setCollocationsLoading(false);
    }
  };

  const handleConfirmSaveCollocation = async () => {
    if (!selectedCollocation) {
      setCollocationsError('Выберите вариант для сохранения.');
      return;
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    try {
      const response = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: dictionaryDirection === 'ru-de' ? selectedCollocation.source : '',
          word_de: dictionaryDirection === 'de-ru' ? selectedCollocation.source : '',
          translation_de: dictionaryDirection === 'ru-de' ? selectedCollocation.target : '',
          translation_ru: dictionaryDirection === 'de-ru' ? selectedCollocation.target : '',
          response_json: dictionaryResult,
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
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
      setDictionarySaved('Добавлено в словарь ✅');
      setCollocationsVisible(false);
    } catch (error) {
      setDictionaryError(`Ошибка сохранения: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleExportDictionaryPdf = async () => {
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setExportLoading(true);
    setDictionaryError('');
    try {
      const response = await fetch('/api/webapp/dictionary/export/pdf', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          folder_mode: dictionaryFolderId !== 'none' ? 'folder' : 'all',
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'dictionary.pdf';
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      setDictionaryError(`Ошибка выгрузки PDF: ${error.message}`);
    } finally {
      setExportLoading(false);
    }
  };

  const selectedDictionaryFolder = folders.find(
    (folder) => String(folder.id) === dictionaryFolderId
  );

  const extractYoutubeId = (value) => {
    if (!value) return '';
    const trimmed = value.trim();
    if (/^[a-zA-Z0-9_-]{11}$/.test(trimmed)) {
      return trimmed;
    }
    const patterns = [
      /v=([a-zA-Z0-9_-]{11})/,
      /youtu\.be\/([a-zA-Z0-9_-]{11})/,
      /embed\/([a-zA-Z0-9_-]{11})/,
      /shorts\/([a-zA-Z0-9_-]{11})/,
    ];
    for (const pattern of patterns) {
      const match = trimmed.match(pattern);
      if (match) return match[1];
    }
    return '';
  };

  useEffect(() => {
    const trimmed = youtubeInput.trim();
    if (!trimmed) {
      setYoutubeId('');
      setYoutubeError('');
      safeStorageRemove('webapp_youtube');
      return;
    }
    const id = extractYoutubeId(trimmed);
    if (id) {
      setYoutubeId(id);
      setYoutubeError('');
      safeStorageSet('webapp_youtube', JSON.stringify({ input: trimmed, id }));
    } else if (trimmed.length > 8) {
      setYoutubeError('Не удалось распознать ссылку или ID видео.');
      setYoutubeId('');
    } else {
      setYoutubeError('');
    }
  }, [youtubeInput]);

  const fetchTranscript = async () => {
    if (!youtubeId || !initData) return;
    if (youtubeManualOverride) return;
    setYoutubeTranscriptLoading(true);
    setYoutubeTranscriptError('');
    try {
      const response = await fetch('/api/webapp/youtube/transcript', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, videoId: youtubeId }),
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
      const items = data.items || [];
      setYoutubeTranscript(items);
      setYoutubeTranslations(data.translations || {});
      const hasTiming = items.some((item) => Number(item?.start) > 0);
      setYoutubeTranscriptHasTiming(hasTiming);
      setManualTranscript('');
    } catch (error) {
      setYoutubeTranscript([]);
      setYoutubeTranscriptError(`Авто-субтитры недоступны: ${error.message}`);
    } finally {
      setYoutubeTranscriptLoading(false);
    }
  };

  useEffect(() => {
    if (!youtubeId || !initData) {
      setYoutubeTranscript([]);
      setYoutubeTranscriptError('');
      setYoutubeTranslations({});
      setYoutubeRuEnabled(false);
      setYoutubeManualOverride(false);
      setYoutubeTranscriptHasTiming(true);
    }
  }, [youtubeId, initData]);

  useEffect(() => {
    if (!isWebAppMode || flashcardsOnly || !initData) return;
    if (!isSectionVisible('movies')) return;
    if (movies.length > 0) return;
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
        setMovies(Array.isArray(data.items) ? data.items : []);
      })
      .catch((err) => {
        if (cancelled) return;
        setMoviesError(`Ошибка каталога: ${err.message}`);
      })
      .finally(() => {
        if (!cancelled) setMoviesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isWebAppMode, flashcardsOnly, initData, selectedSections, movies.length]);

  useEffect(() => {
    if (!youtubeId) {
      setYoutubePlayerReady(false);
      setYoutubeCurrentTime(0);
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
      }
      if (youtubePlayerRef.current && youtubePlayerRef.current.destroy) {
        youtubePlayerRef.current.destroy();
        youtubePlayerRef.current = null;
      }
      return;
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
    });

    ensureApiReady().then(() => {
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
        },
        events: {
          onReady: () => {
            setYoutubePlayerReady(true);
            if (youtubeTimeIntervalRef.current) {
              clearInterval(youtubeTimeIntervalRef.current);
            }
            youtubeTimeIntervalRef.current = setInterval(() => {
              try {
                const time = youtubePlayerRef.current?.getCurrentTime?.();
                if (typeof time === 'number' && !Number.isNaN(time)) {
                  setYoutubeCurrentTime(time);
                }
              } catch (error) {
                // ignore
              }
            }, 400);
          },
        },
      });
    });

    return () => {
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
      }
    };
  }, [youtubeId]);

  useEffect(() => {
    if (youtubeTranscript.length > 0 && youtubeSubtitlesRef.current) {
      youtubeSubtitlesRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, [youtubeTranscript.length]);

  useEffect(() => {
    if (!youtubeSubtitlesRef.current) return;
    const listEl = youtubeSubtitlesRef.current.querySelector('.webapp-subtitles-list');
    const activeEl = youtubeSubtitlesRef.current.querySelector('.webapp-subtitles-list .is-active');
    if (listEl && activeEl) {
      const listRect = listEl.getBoundingClientRect();
      const activeRect = activeEl.getBoundingClientRect();
      const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
      listEl.scrollTop += offset;
    }
  }, [youtubeCurrentTime, youtubeTranscript.length]);

  useEffect(() => {
    const ruRef = document.querySelector('.webapp-subtitles.is-translation .webapp-subtitles-list');
    if (!ruRef) return;
    const activeEl = ruRef.querySelector('.is-active');
    if (activeEl) {
      const listRect = ruRef.getBoundingClientRect();
      const activeRect = activeEl.getBoundingClientRect();
      const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
      ruRef.scrollTop += offset;
    }
  }, [youtubeCurrentTime, youtubeTranscript.length, youtubeRuEnabled]);

  useEffect(() => {
    if (!youtubeRuEnabled) return;
    if (!youtubeTranscript.length || !youtubeId || !initData) return;
    const activeIndex = getActiveSubtitleIndex();
    if (activeIndex < 0) return;
    if (youtubeTranslateInFlightRef.current) return;
    const aheadLimit = 50;
    const minBuffer = 25;
    let available = 0;
    for (let i = activeIndex; i < youtubeTranscript.length; i += 1) {
      const idx = String(i);
      if (!youtubeTranslations[idx]) break;
      available += 1;
    }
    if (available >= minBuffer) return;
    const startIndex = activeIndex + available;
    if (youtubeTranslateIndexRef.current === startIndex) return;

    const batch = youtubeTranscript.slice(startIndex, startIndex + aheadLimit);
    const lines = batch.map((item) => normalizeSubtitleText(item.text || ''));
    const hasText = lines.some((line) => line);
    if (!hasText) return;

    youtubeTranslateInFlightRef.current = true;
    youtubeTranslateIndexRef.current = startIndex;

    fetch('/api/webapp/youtube/translate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        initData,
        videoId: youtubeId,
        start_index: startIndex,
        lines,
      }),
    })
      .then((res) => res.ok ? res.json() : Promise.reject(res))
      .then((data) => {
        const translations = data.translations || [];
        if (!translations.length) return;
        setYoutubeTranslations((prev) => {
          const next = { ...prev };
          translations.forEach((text, offset) => {
            const idx = String(startIndex + offset);
            if (text) {
              next[idx] = text;
            }
          });
          return next;
        });
      })
      .catch(() => {})
      .finally(() => {
        youtubeTranslateInFlightRef.current = false;
      });
  }, [youtubeCurrentTime, youtubeTranscript.length, youtubeId, initData, youtubeTranslations, youtubeRuEnabled]);

  const handleLoadDailyHistory = async () => {
    if (!initData) {
      setHistoryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setHistoryLoading(true);
    setHistoryError('');
    try {
      const response = await fetch('/api/webapp/history/daily', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 50 }),
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
      setHistoryItems(data.items || []);
      setHistoryVisible(true);
    } catch (error) {
      setHistoryError(`Ошибка загрузки истории: ${error.message}`);
    } finally {
      setHistoryLoading(false);
    }
  };

  const resolveAnalyticsGranularity = (periodValue) => {
    switch (periodValue) {
      case 'day':
      case 'week':
        return 'day';
      case 'month':
        return 'week';
      case 'quarter':
      case 'half-year':
      case 'year':
      case 'all':
        return 'month';
      default:
        return 'day';
    }
  };

  const loadAnalytics = async (overridePeriod) => {
    if (!initData) {
      setAnalyticsError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    const period = overridePeriod || analyticsPeriod;
    const granularity = resolveAnalyticsGranularity(period);
    setAnalyticsLoading(true);
    setAnalyticsError('');
    try {
      const summaryResponse = await fetch('/api/webapp/analytics/summary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period }),
      });
      if (!summaryResponse.ok) {
        throw new Error(await summaryResponse.text());
      }
      const summaryData = await summaryResponse.json();
      setAnalyticsSummary(summaryData.summary || null);

      const seriesResponse = await fetch('/api/webapp/analytics/timeseries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period, granularity }),
      });
      if (!seriesResponse.ok) {
        throw new Error(await seriesResponse.text());
      }
      const seriesData = await seriesResponse.json();
      setAnalyticsPoints(seriesData.points || []);

      const compareResponse = await fetch('/api/webapp/analytics/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period, limit: 8 }),
      });
      if (!compareResponse.ok) {
        throw new Error(await compareResponse.text());
      }
      const compareData = await compareResponse.json();
      setAnalyticsCompare(compareData.items || []);
      setAnalyticsRank(compareData.self?.rank ?? null);
    } catch (error) {
      setAnalyticsError(`Ошибка аналитики: ${error.message}`);
    } finally {
      setAnalyticsLoading(false);
    }
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('analytics')) {
      loadAnalytics();
    }
  }, [initData, isWebAppMode, analyticsPeriod, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!analyticsTrendRef.current) {
      return;
    }
    const chart = echarts.init(analyticsTrendRef.current);
    const labels = analyticsPoints.map((item) => item.period_start);
    const success = analyticsPoints.map((item) => item.successful_translations || 0);
    const fail = analyticsPoints.map((item) => item.unsuccessful_translations || 0);
    const avgScore = analyticsPoints.map((item) => item.avg_score || 0);
    const avgTime = analyticsPoints.map((item) => item.avg_time_min || 0);

    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        formatter: (params) => {
          const map = {};
          params.forEach((entry) => {
            map[entry.seriesName] = entry.value;
          });
          const index = params[0]?.dataIndex ?? 0;
          const timeValue = avgTime[index] ?? 0;
          return `
            <strong>${labels[index] || ''}</strong><br/>
            Успешно: ${map['Успешно'] ?? 0}<br/>
            Ошибки: ${map['Нужно доработать'] ?? 0}<br/>
            Ср. балл: ${map['Средний балл'] ?? 0}<br/>
            Ср. время: ${timeValue} мин
          `;
        },
      },
      legend: {
        data: ['Успешно', 'Нужно доработать', 'Средний балл'],
        textStyle: { color: '#dbe7ff' },
      },
      grid: { left: 32, right: 32, top: 40, bottom: 40 },
      xAxis: {
        type: 'category',
        data: labels,
        axisLine: { lineStyle: { color: '#2f3f5f' } },
        axisLabel: { color: '#c7d2f1' },
      },
      yAxis: [
        {
          type: 'value',
          name: 'Переводы',
          axisLabel: { color: '#c7d2f1' },
          splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        {
          type: 'value',
          name: 'Баллы',
          min: 0,
          max: 100,
          axisLabel: { color: '#c7d2f1' },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: 'Успешно',
          type: 'bar',
          stack: 'total',
          data: success,
          itemStyle: { color: '#06d6a0' },
          barWidth: 22,
        },
        {
          name: 'Нужно доработать',
          type: 'bar',
          stack: 'total',
          data: fail,
          itemStyle: { color: '#ff6b6b' },
          barWidth: 22,
        },
        {
          name: 'Средний балл',
          type: 'line',
          yAxisIndex: 1,
          data: avgScore,
          smooth: true,
          symbol: 'circle',
          symbolSize: 8,
          itemStyle: { color: '#ffd166' },
          lineStyle: { width: 3 },
        },
      ],
    });

    return () => {
      chart.dispose();
    };
  }, [analyticsPoints, analyticsPeriod]);

  useEffect(() => {
    if (!analyticsCompareRef.current) {
      return;
    }
    const chart = echarts.init(analyticsCompareRef.current);
    const selfId = webappUser?.id;
    const names = analyticsCompare.map((item) => item.username);
    const data = analyticsCompare.map((item) => ({
      value: item.final_score || 0,
      itemStyle: {
        color: item.user_id === selfId ? '#ffd166' : '#5ddcff',
      },
    }));

    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'item',
        formatter: (params) => {
          const item = analyticsCompare[params.dataIndex];
          if (!item) return '';
          return `
            <strong>${item.username}</strong><br/>
            Итоговый балл: ${item.final_score}<br/>
            Успех: ${item.success_rate}%<br/>
            Ср. балл: ${item.avg_score}<br/>
            Переводы: ${item.total_translations}<br/>
            Пропущено: ${item.missed_sentences}<br/>
            Пропущено дней: ${item.missed_days ?? 0}
          `;
        },
      },
      grid: { left: 20, right: 20, top: 20, bottom: 20, containLabel: true },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#c7d2f1' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      yAxis: {
        type: 'category',
        data: names,
        axisLabel: { color: '#c7d2f1' },
        inverse: true,
      },
      series: [
        {
          type: 'bar',
          data,
          barWidth: 18,
          borderRadius: [8, 8, 8, 8],
        },
      ],
    });

    return () => {
      chart.dispose();
    };
  }, [analyticsCompare, webappUser]);

  if (isWebAppMode) {
    return (
      <div className={`webapp-page ${flashcardsOnly ? 'is-flashcards' : ''}`}>
        <div className="webapp-shell">
          <aside className="webapp-sidebar">
            <div className="webapp-brand">
              <div className="brand-mark">DF</div>
              <div>
                <div className="brand-title">DeutschFlow</div>
                <div className="brand-subtitle">Переводы • Видео • Словарь</div>
              </div>
            </div>
            <div className="webapp-menu">
              <button
                type="button"
                className={`menu-item menu-item-translations ${selectedSections.has('translations') ? 'is-active' : ''}`}
                onClick={() => toggleSection('translations')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M7 4h7l3 3v13a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z" fill="#1d4ed8" opacity="0.9" />
                    <path d="M14 4v4h4" fill="#60a5fa" />
                    <path d="M8 10h8M8 13h8M8 16h6" stroke="#ffffff" strokeWidth="1.7" strokeLinecap="round" />
                    <path d="M4.5 12.5h2.2l1-1.6 1 3 1.1-2h2.2" stroke="#fbbf24" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" fill="none" />
                  </svg>
                </span>
                <span>Переводы</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-youtube ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                onClick={() => toggleSection('youtube')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 28 20" aria-hidden="true">
                    <rect x="0" y="0" width="28" height="20" rx="4" fill="#ff0000" />
                    <path d="M11 5l8 5-8 5V5z" fill="#ffffff" />
                  </svg>
                </span>
                <span>YouTube</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-movies ${selectedSections.has('movies') ? 'is-active' : ''}`}
                onClick={() => toggleSection('movies')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <rect x="3" y="6" width="18" height="12" rx="2" fill="#111827" />
                    <path d="M6 6l2-3m4 3l2-3m4 3l2-3" stroke="#f59e0b" strokeWidth="2" strokeLinecap="round" />
                    <path d="M9 10l6 4-6 4v-8z" fill="#f59e0b" />
                  </svg>
                </span>
                <span>Фильмы</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-dictionary ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                onClick={() => toggleSection('dictionary')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M4 6a2 2 0 0 1 2-2h11a3 3 0 0 1 3 3v11a2 2 0 0 1-2 2H7a3 3 0 0 0-3 3V6z" fill="#0ea5e9" />
                    <path d="M7 8h9M7 12h9M7 16h6" stroke="#ffffff" strokeWidth="1.7" strokeLinecap="round" />
                    <path d="M6 6h10a2 2 0 0 1 2 2v10" stroke="#0284c7" strokeWidth="2" fill="none" />
                  </svg>
                </span>
                <span>Словарь</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-flashcards ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                onClick={() => {
                  toggleSection('flashcards');
                  setFlashcardsVisible(true);
                  setFlashcardsOnly(false);
                  setFlashcardSessionActive(false);
                  setFlashcardExitSummary(false);
                }}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M4 6a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6z" fill="#8b5cf6" />
                    <path d="M9 8h6M9 12h4" stroke="#ffffff" strokeWidth="1.6" strokeLinecap="round" />
                    <path d="M13 7l7 2v9l-7-2V7z" fill="#c4b5fd" />
                    <circle cx="16" cy="12" r="1.2" fill="#7c3aed" />
                  </svg>
                </span>
                <span>Карточки</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-assistant ${selectedSections.has('assistant') ? 'is-active' : ''}`}
                onClick={() => toggleSection('assistant')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <rect x="3" y="4" width="18" height="16" rx="4" fill="#d97706" />
                    <circle cx="9" cy="11" r="1.5" fill="#fff7ed" />
                    <circle cx="15" cy="11" r="1.5" fill="#fff7ed" />
                    <path d="M8 15h8" stroke="#fff7ed" strokeWidth="1.7" strokeLinecap="round" />
                    <path d="M12 2v3" stroke="#fef3c7" strokeWidth="1.8" strokeLinecap="round" />
                  </svg>
                </span>
                <span>Ассистент</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                onClick={() => toggleSection('analytics')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <rect x="3" y="11" width="4" height="8" rx="1.2" fill="#22c55e" />
                    <rect x="10" y="7" width="4" height="12" rx="1.2" fill="#16a34a" />
                    <rect x="17" y="4" width="4" height="15" rx="1.2" fill="#15803d" />
                    <path d="M3 19h18" stroke="#064e3b" strokeWidth="1.2" strokeLinecap="round" />
                  </svg>
                </span>
                <span>Аналитика</span>
              </button>
            </div>
            <div className="webapp-menu-actions">
              <label className="menu-toggle-row">
                <input
                  type="checkbox"
                  checked={menuMultiSelect}
                  onChange={(event) => setMenuMultiSelect(event.target.checked)}
                />
                <span>Мультивыбор</span>
              </label>
              <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                Показать все
              </button>
              <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                Скрыть всё
              </button>
            </div>
            {flashcardsOnly && (
              <div className="webapp-menu-note">Режим повторения активен</div>
            )}
          </aside>

          <div className="webapp-main">
            <div className="webapp-topbar">
              <button
                type="button"
                className="menu-toggle"
                onClick={() => setMenuOpen(true)}
              >
                <span />
                <span />
                <span />
              </button>
              <div className="topbar-title">DeutschFlow</div>
              <div className="topbar-profile">
                <input
                  ref={avatarInputRef}
                  type="file"
                  accept="image/*"
                  className="avatar-input"
                  onChange={handleAvatarUpload}
                />
                <button
                  type="button"
                  className="avatar-button topbar-avatar"
                  onClick={() => avatarInputRef.current?.click()}
                >
                  {userAvatar ? <img src={userAvatar} alt="User avatar" /> : <span className="avatar-placeholder" />}
                </button>
                <div className="topbar-user-meta">
                  <div className="topbar-user-name">{webappUser?.first_name || 'Гость'}</div>
                  <div className="topbar-user-line">ID: {webappUser?.id || '—'}</div>
                  <div className="topbar-user-line">Chat: {webappChatType || '—'}</div>
                </div>
              </div>
            </div>

            {menuOpen && (
              <div className="webapp-overlay">
                <div className="overlay-backdrop" onClick={() => setMenuOpen(false)} />
                <div className="overlay-panel">
                  <div className="overlay-header">
                    <div className="brand-title">DeutschFlow</div>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setMenuOpen(false)}
                    >
                      Закрыть
                    </button>
                  </div>
                <div className="overlay-menu">
                    <label className="menu-toggle-row">
                      <input
                        type="checkbox"
                        checked={menuMultiSelect}
                        onChange={(event) => setMenuMultiSelect(event.target.checked)}
                      />
                      <span>Мультивыбор</span>
                    </label>
                    <button
                      type="button"
                      className={`menu-item menu-item-translations ${selectedSections.has('translations') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('translations', translationsRef)}
                      disabled={flashcardsOnly}
                    >
                    <span className="menu-icon">
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path d="M7 4h7l3 3v13a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z" fill="#1d4ed8" opacity="0.9" />
                        <path d="M14 4v4h4" fill="#60a5fa" />
                        <path d="M8 10h8M8 13h8M8 16h6" stroke="#ffffff" strokeWidth="1.7" strokeLinecap="round" />
                        <path d="M4.5 12.5h2.2l1-1.6 1 3 1.1-2h2.2" stroke="#fbbf24" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" fill="none" />
                      </svg>
                    </span>
                    <span>Переводы</span>
                  </button>
                <button
                  type="button"
                  className={`menu-item menu-item-youtube ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('youtube', youtubeRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon">
                    <svg viewBox="0 0 28 20" aria-hidden="true">
                      <rect x="0" y="0" width="28" height="20" rx="4" fill="#ff0000" />
                      <path d="M11 5l8 5-8 5V5z" fill="#ffffff" />
                    </svg>
                  </span>
                  <span>YouTube</span>
                </button>
                <button
                  type="button"
                  className={`menu-item menu-item-movies ${selectedSections.has('movies') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('movies', moviesRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon">
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                      <rect x="3" y="6" width="18" height="12" rx="2" fill="#111827" />
                      <path d="M6 6l2-3m4 3l2-3m4 3l2-3" stroke="#f59e0b" strokeWidth="2" strokeLinecap="round" />
                      <path d="M9 10l6 4-6 4v-8z" fill="#f59e0b" />
                    </svg>
                  </span>
                  <span>Фильмы</span>
                </button>
                <button
                  type="button"
                  className={`menu-item menu-item-dictionary ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('dictionary', dictionaryRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon">
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                      <path d="M4 6a2 2 0 0 1 2-2h11a3 3 0 0 1 3 3v11a2 2 0 0 1-2 2H7a3 3 0 0 0-3 3V6z" fill="#0ea5e9" />
                      <path d="M7 8h9M7 12h9M7 16h6" stroke="#ffffff" strokeWidth="1.7" strokeLinecap="round" />
                      <path d="M6 6h10a2 2 0 0 1 2 2v10" stroke="#0284c7" strokeWidth="2" fill="none" />
                    </svg>
                  </span>
                      <span>Словарь</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-flashcards ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('flashcards', flashcardsRef)}
                    >
                      <span className="menu-icon">
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                          <path d="M4 6a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6z" fill="#8b5cf6" />
                          <path d="M9 8h6M9 12h4" stroke="#ffffff" strokeWidth="1.6" strokeLinecap="round" />
                          <path d="M13 7l7 2v9l-7-2V7z" fill="#c4b5fd" />
                          <circle cx="16" cy="12" r="1.2" fill="#7c3aed" />
                        </svg>
                      </span>
                      <span>Карточки</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-assistant ${selectedSections.has('assistant') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('assistant', assistantRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon">
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                          <rect x="3" y="4" width="18" height="16" rx="4" fill="#d97706" />
                          <circle cx="9" cy="11" r="1.5" fill="#fff7ed" />
                          <circle cx="15" cy="11" r="1.5" fill="#fff7ed" />
                          <path d="M8 15h8" stroke="#fff7ed" strokeWidth="1.7" strokeLinecap="round" />
                          <path d="M12 2v3" stroke="#fef3c7" strokeWidth="1.8" strokeLinecap="round" />
                        </svg>
                      </span>
                      <span>Ассистент</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('analytics', analyticsRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon">
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                          <rect x="3" y="11" width="4" height="8" rx="1.2" fill="#22c55e" />
                          <rect x="10" y="7" width="4" height="12" rx="1.2" fill="#16a34a" />
                          <rect x="17" y="4" width="4" height="15" rx="1.2" fill="#15803d" />
                          <path d="M3 19h18" stroke="#064e3b" strokeWidth="1.2" strokeLinecap="round" />
                        </svg>
                      </span>
                      <span>Аналитика</span>
                    </button>
                  </div>
                  <div className="overlay-actions">
                    <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                      Показать все
                    </button>
                    <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                      Скрыть всё
                    </button>
                  </div>
                </div>
              </div>
            )}

            {showHero && (
            <header className="webapp-hero">
              <div className="webapp-hero-copy webapp-hero-copy-landing">
                <span className="pill">Telegram Web App</span>
                <h1>Осваивайте немецкий легко и уверенно</h1>
              </div>
              <div className="webapp-hero-mascot-flat" aria-hidden="true">
                <img src={heroStickerSrc} alt="Deutsch mascot" className="hero-flat-image" />
              </div>
            </header>
            )}

            {showHero && (
            <section className="webapp-hero-cards">
              <div className="hero-card">
                <div className="hero-card-head is-translate">Переводите</div>
                <p>Напишите перевод, получите оценку и объяснения ошибок.</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-save">Сохраняйте</div>
                <p>Добавляйте слова в словарь и группируйте по папкам.</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-train">Тренируйтесь</div>
                <p>Повторяйте слова сетами по 15 карточек с прогрессом.</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-watch">Смотрите и слушайте</div>
                <p>Смотрите фильмы и слушайте песни с двойными субтитрами, сохраняйте новые слова для дальнейшего повторения и изучения.</p>
              </div>
            </section>
            )}

            

            {!telegramApp?.initData && (
              <label className="webapp-field">
                <span>initData (для локального теста)</span>
                <textarea
                  rows={3}
                  value={initData}
                  onChange={(event) => setInitData(event.target.value)}
                  placeholder="Вставьте initData из Telegram"
                />
              </label>
            )}

            {!flashcardsOnly && isSectionVisible('translations') && (
              <section className="webapp-section" ref={translationsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>Ваши переводы</h2>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="webapp-translation-start">
                  <label className="webapp-field">
                    <span>Тема</span>
                    <select
                      value={selectedTopic}
                      onChange={(event) => setSelectedTopic(event.target.value)}
                      disabled={topicsLoading || webappLoading}
                    >
                      {topics.map((topic) => (
                        <option key={topic} value={topic}>
                          {topic}
                        </option>
                      ))}
                    </select>
                  </label>
                  {!isStoryTopic(selectedTopic) && (
                    <label className="webapp-field">
                      <span>Уровень</span>
                      <select
                        value={selectedLevel}
                        onChange={(event) => setSelectedLevel(event.target.value)}
                        disabled={webappLoading}
                      >
                        <option value="a2">A2</option>
                        <option value="b1">B1</option>
                        <option value="b2">B2</option>
                        <option value="c1">C1</option>
                        <option value="c2">C2</option>
                      </select>
                    </label>
                  )}
                  {isStoryTopic(selectedTopic) && (
                    <>
                      <label className="webapp-field">
                        <span>История</span>
                        <select
                          value={storyMode}
                          onChange={(event) => setStoryMode(event.target.value)}
                          disabled={webappLoading}
                        >
                          <option value="new">Новая</option>
                          <option value="repeat">Повторить старую</option>
                        </select>
                      </label>
                      {storyMode === 'repeat' && (
                        <label className="webapp-field">
                          <span>Выберите историю</span>
                          <select
                            value={selectedStoryId}
                            onChange={(event) => setSelectedStoryId(event.target.value)}
                            disabled={webappLoading || storyHistoryLoading}
                          >
                            <option value="">Последняя</option>
                            {storyHistory.map((item) => (
                              <option key={item.story_id} value={item.story_id}>
                                {item.title || `История #${item.story_id}`}
                              </option>
                            ))}
                          </select>
                        </label>
                      )}
                      <label className="webapp-field">
                        <span>Тип истории</span>
                        <select
                          value={storyType}
                          onChange={(event) => setStoryType(event.target.value)}
                          disabled={webappLoading}
                        >
                          <option value="знаменитая личность">Знаменитая личность</option>
                          <option value="историческое событие">Историческое событие</option>
                          <option value="выдающееся открытие">Выдающееся открытие</option>
                          <option value="выдающееся изобретение">Выдающееся изобретение</option>
                          <option value="география">География</option>
                          <option value="космос">Космос</option>
                          <option value="культура">Культура</option>
                          <option value="спорт">Спорт</option>
                          <option value="политика">Политика</option>
                        </select>
                      </label>
                      <label className="webapp-field">
                        <span>Сложность</span>
                        <select
                          value={storyDifficulty}
                          onChange={(event) => setStoryDifficulty(event.target.value)}
                          disabled={webappLoading}
                        >
                          <option value="начальный">Начальный</option>
                          <option value="средний">Средний</option>
                          <option value="продвинутый">Продвинутый</option>
                        </select>
                      </label>
                    </>
                  )}
                  <button
                    type="button"
                    className="primary-button"
                    onClick={isStoryTopic(selectedTopic) ? handleStartStory : handleStartTranslation}
                    disabled={webappLoading || topicsLoading}
                  >
                    {webappLoading ? 'Запускаем...' : '🚀 Начать перевод'}
                  </button>
                </div>
                {topicsError && <div className="webapp-error">{topicsError}</div>}
                {storyHistoryError && <div className="webapp-error">{storyHistoryError}</div>}
                {!isStoryResultMode && (
                <form className="webapp-form" onSubmit={handleTranslationSubmit}>
                  <section className="webapp-translation-list">
                    {sentences.length === 0 ? (
                      <p className="webapp-muted">
                        Нет активных предложений. Нажмите «🚀 Начать перевод», чтобы получить новые.
                      </p>
                    ) : (
                      sentences.map((item, index) => {
                        const draft = translationDrafts[String(item.id_for_mistake_table)] || '';
                        return (
                          <label key={item.id_for_mistake_table} className="webapp-translation-item">
                            <span className="translation-sentence">
                              {item.unique_id ?? index + 1}. {item.sentence}
                            </span>
                            <textarea
                              rows={5}
                              value={draft}
                              onChange={(event) => handleDraftChange(item.id_for_mistake_table, event.target.value)}
                              placeholder="Введите перевод..."
                            />
                            <div className="translation-actions">
                              <button
                                type="button"
                                className="translation-dict-jump"
                                onClick={jumpToDictionaryFromSentence}
                                aria-label="Перейти в словарь"
                              >
                                ↗
                              </button>
                            </div>
                          </label>
                        );
                      })
                    )}
                  </section>

                  {isStorySession && (
                    <label className="webapp-field">
                      <span>А теперь угадай, о ком / чем шла речь</span>
                      <input
                        type="text"
                        value={storyGuess}
                        onChange={(event) => setStoryGuess(event.target.value)}
                        placeholder="Ваш ответ..."
                      />
                    </label>
                  )}

                  <button className="primary-button" type="submit" disabled={webappLoading}>
                    {webappLoading
                      ? 'Проверяем...'
                      : isStorySession
                        ? 'Проверить историю'
                        : 'Проверить перевод'}
                  </button>
                </form>
                )}

                {webappError && <div className="webapp-error">{webappError}</div>}
                {finishMessage && <div className="webapp-success">{finishMessage}</div>}

                {isStoryResultMode && (
                  <section className="webapp-result">
                    <h3>Результат истории</h3>
                    <div className="webapp-result-card story-result">
                      <div className="story-result-head">
                        <strong>⭐ Итоговый балл:</strong> {storyResult.score ?? '—'} / 100
                      </div>

                      {storyResult.feedback && (
                        <div
                          className="webapp-result-text story-result-feedback"
                          dangerouslySetInnerHTML={{ __html: renderRichText(storyResult.feedback) }}
                        />
                      )}

                      <div className="webapp-result-text story-result-answer">
                        <div><strong>🎯 Ответ пользователя:</strong> {storyResult.guess_correct ? 'верно по смыслу' : 'неверно по смыслу'}</div>
                        <div><strong>✅ Эталон:</strong> {storyResult.answer || '—'}</div>
                        {storyResult.guess_reason && (
                          <div><strong>📝 Пояснение:</strong> {storyResult.guess_reason}</div>
                        )}
                      </div>

                      {storyResult.extra_de && (
                        <div className="webapp-result-text story-result-extra">
                          <strong>📌 Дополнительно (DE):</strong> {storyResult.extra_de}
                        </div>
                      )}
                      {Array.isArray(storyResult.source_links) && storyResult.source_links.length > 0 && (
                        <div className="webapp-result-text story-result-links">
                          <strong>🔗 Официальные источники:</strong>
                          <ul>
                            {storyResult.source_links.map((item, idx) => (
                              <li key={`${item.url}-${idx}`}>
                                <a href={item.url} target="_blank" rel="noreferrer">{item.lang}: {item.url}</a>
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  </section>
                )}

                {results.length > 0 && (
                  <section className="webapp-result">
                    <h3>Результат проверки</h3>
                    <div className="webapp-result-list">
                      {results.map((item, index) => (
                        <div key={`${item.sentence_number ?? index}`} className="webapp-result-card">
                          {item.error ? (
                            <div className="webapp-error">{item.error}</div>
                          ) : (
                            <>
                              <div
                                className="webapp-result-text"
                                onMouseUp={handleSelection}
                                onTouchEnd={handleSelection}
                              >
                                {renderFeedback(item.feedback)}
                              </div>
                              <button
                                type="button"
                                className="secondary-button explanation-button"
                                onClick={() => handleExplainTranslation(item)}
                                disabled={explanationLoading[String(item.sentence_number ?? item.original_text)]}
                              >
                                {explanationLoading[String(item.sentence_number ?? item.original_text)]
                                  ? 'Запрашиваем объяснение...'
                                  : 'Объяснить ошибки'}
                              </button>
                              {explanations[String(item.sentence_number ?? item.original_text)] && (
                                <div
                                  className="webapp-explanation"
                                  dangerouslySetInnerHTML={{
                                    __html: renderRichText(
                                      explanations[String(item.sentence_number ?? item.original_text)]
                                    ),
                                  }}
                                />
                              )}
                            </>
                          )}
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                <div className="webapp-actions webapp-actions-footer">
                  {sentences.length === 0 && !webappLoading && (
                    <div className="webapp-muted">
                      Если сессия зависла, можно завершить её вручную.
                    </div>
                  )}
                  <button
                    type="button"
                    onClick={handleFinishTranslation}
                    className={`primary-button finish-button ${finishStatus === 'done' ? 'status-done' : ''}`}
                    disabled={webappLoading || ((results.length === 0 && !storyResult) && sentences.length > 0)}
                  >
                    {finishStatus === 'done' ? 'Завершено' : 'Завершить перевод'}
                  </button>
                  <button
                    type="button"
                    onClick={handleLoadDailyHistory}
                    className="secondary-button"
                    disabled={webappLoading || historyLoading}
                  >
                    {historyLoading ? 'Загружаем...' : 'Посмотреть результат за сегодня'}
                  </button>
                  {results.length === 0 && !storyResult && !webappLoading && (
                    <div className="webapp-muted">Сначала проверьте перевод, чтобы завершить.</div>
                  )}
                </div>

                {historyError && <div className="webapp-error">{historyError}</div>}

                {historyVisible && (
                  <section className="webapp-result">
                    <h3>История переводов за сегодня</h3>
                    {historyItems.length === 0 ? (
                      <p className="webapp-muted">Сегодня пока нет завершённых переводов.</p>
                    ) : (
                      <div className="webapp-result-list">
                        {historyItems.map((item, index) => (
                          <div key={item.id ?? index} className="webapp-result-card">
                            <pre className="webapp-result-text">
                              {`Sentence number: ${item.sentence_number ?? '—'}\nScore: ${
                                item.score ?? '—'
                              }/100\nOriginal: ${item.original_text ?? '—'}\nTranslation: ${
                                item.user_translation ?? '—'
                              }\nCorrect: ${item.correct_translation ?? '—'}`}
                            </pre>
                          </div>
                        ))}
                      </div>
                    )}
                  </section>
                )}
              </section>
            )}

            {!flashcardsOnly && (isSectionVisible('youtube') || isSectionVisible('dictionary')) && (
              <div className={`webapp-video-dictionary ${videoExpanded ? 'is-split' : ''}`}>
                {isSectionVisible('youtube') && (
                  <section className="webapp-video" ref={youtubeRef}>
                    <div className="webapp-local-section-head">
                      <h3>Видео YouTube</h3>
                      <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                    </div>
                    <div className="webapp-video-form">
                      <label className="webapp-field">
                        <span>Ссылка или ID видео</span>
                        <div className="input-clear-wrap">
                          <input
                            type="text"
                            className="input-clear-field"
                            value={youtubeInput}
                            onChange={(event) => setYoutubeInput(event.target.value)}
                            placeholder="https://youtu.be/VIDEO_ID"
                          />
                          {youtubeInput && (
                            <button
                              type="button"
                              className="input-clear-btn"
                              onClick={() => setYoutubeInput('')}
                              aria-label="Очистить"
                            >
                              ×
                            </button>
                          )}
                        </div>
                      </label>
                    <div className="webapp-video-actions">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setVideoExpanded((prev) => !prev)}
                        style={{ display: 'none' }}
                        >
                          {videoExpanded ? 'Обычный режим' : 'Словарь рядом'}
                        </button>
                      </div>
                    </div>
                    {youtubeError && <div className="webapp-error">{youtubeError}</div>}
                    {youtubeId ? (
                      <div className={`webapp-video-frame ${videoExpanded ? 'is-expanded' : ''}`}>
                        <div
                          id="youtube-player"
                          className="youtube-player-host"
                          data-video-id={youtubeId}
                        />
                        {!youtubePlayerReady && (
                          <iframe
                            title="YouTube player"
                            src={`https://www.youtube.com/embed/${youtubeId}`}
                            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                            allowFullScreen
                          />
                        )}
                      </div>
                    ) : (
                      <p className="webapp-muted">Вставьте ссылку на видео, чтобы смотреть прямо здесь.</p>
                    )}
                    <div className="webapp-video-actions">
                      <button
                        type="button"
                        className={`secondary-button ${youtubeRuEnabled ? 'is-active' : ''}`}
                        onClick={() => setYoutubeRuEnabled((prev) => !prev)}
                        disabled={!youtubeTranscript.length}
                      >
                        {youtubeRuEnabled ? 'Скрыть RU субтитры' : 'Показать RU субтитры'}
                      </button>
                      <button
                        type="button"
                        className="primary-button"
                        onClick={() => fetchTranscript()}
                        disabled={!youtubeId || youtubeTranscriptLoading || youtubeManualOverride}
                      >
                        {youtubeTranscriptLoading ? 'Загружаем...' : 'Загрузить субтитры'}
                      </button>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setShowManualTranscript((prev) => !prev)}
                      >
                        {showManualTranscript ? 'Скрыть транскрипцию' : 'Вставить транскрипцию'}
                      </button>
                    </div>
                    {showManualTranscript && (
                      <div className="webapp-subtitles-manual">
                        <textarea
                          rows={6}
                          value={manualTranscript}
                          onChange={(event) => setManualTranscript(event.target.value)}
                          placeholder="Вставьте .srt/.vtt с таймкодами. Если таймкодов нет — покажем статично."
                        />
                        <div className="webapp-video-actions">
                          <button
                            type="button"
                            className="primary-button"
                            onClick={() => handleManualTranscript()}
                          >
                            Использовать транскрипцию
                          </button>
                          {youtubeManualOverride && (
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => {
                                setYoutubeManualOverride(false);
                                setManualTranscript('');
                                setYoutubeTranscriptHasTiming(true);
                                setShowManualTranscript(false);
                              }}
                            >
                              Вернуться к авто
                            </button>
                          )}
                        </div>
                      </div>
                    )}
                    {youtubeTranscript.length > 0 && (
                      <div className="webapp-subtitles" ref={youtubeSubtitlesRef}>
                        <div className="webapp-subtitles-header">
                          <h4>Субтитры</h4>
                        </div>
                        <div className="webapp-subtitles-list" onMouseUp={handleSelection}>
                          {(() => {
                            const activeIndex = getActiveSubtitleIndex();
                            return youtubeTranscript.map((item, index) => (
                              <p
                                key={`${item.start}-${index}`}
                                className={index === activeIndex ? 'is-active' : ''}
                              >
                                {renderSubtitleText(item.text)}
                              </p>
                            ));
                          })()}
                        </div>
                      </div>
                    )}
                    {youtubeTranscript.length > 0 && youtubeRuEnabled && (
                      <div className="webapp-subtitles is-translation">
                        <div className="webapp-subtitles-header">
                          <h4>Перевод (RU)</h4>
                        </div>
                        <div className="webapp-subtitles-list">
                          {(() => {
                            const activeIndex = getActiveSubtitleIndex();
                            return youtubeTranscript.map((item, index) => {
                              const translation = youtubeTranslations[String(index)] || '…';
                              return (
                                <p
                                  key={`ru-${item.start}-${index}`}
                                  className={index === activeIndex ? 'is-active' : ''}
                                >
                                  {translation}
                                </p>
                              );
                            });
                          })()}
                        </div>
                      </div>
                    )}
                  </section>
                )}

                {isSectionVisible('dictionary') && (
                  <section className="webapp-dictionary" ref={dictionaryRef}>
                    <div className="webapp-local-section-head">
                      <h3>Словарь</h3>
                      <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                    </div>
                    <form className="webapp-dictionary-form" onSubmit={handleDictionaryLookup}>
                      <label className="webapp-field">
                        <span>Слово или фраза (русский / немецкий)</span>
                        <div className="dictionary-input-wrap">
                          <input
                            className="dictionary-input"
                            type="text"
                            value={dictionaryWord}
                            onChange={(event) => setDictionaryWord(event.target.value)}
                            placeholder="Например: отказаться, уважение, несмотря на / verzichten, Respekt"
                          />
                          {dictionaryWord.trim() && (
                            <button
                              type="button"
                              className="dictionary-clear"
                              onClick={() => setDictionaryWord('')}
                              aria-label="Очистить слово"
                            >
                              ×
                            </button>
                          )}
                        </div>
                      </label>
                      <div className="dictionary-actions">
                        <button className="secondary-button dictionary-button" type="submit" disabled={dictionaryLoading}>
                          {dictionaryLoading ? 'Ищем...' : 'Перевести'}
                        </button>
                        {lastLookupScrollY !== null && (
                          <button
                            type="button"
                            className="secondary-button dictionary-back-icon"
                            onClick={() => window.scrollTo({ top: lastLookupScrollY, behavior: 'smooth' })}
                          >
                            ↙ к предложению
                          </button>
                        )}
                        <button
                          className="secondary-button dictionary-save-button"
                          type="button"
                          onClick={handleDictionarySave}
                          disabled={dictionaryLoading || !dictionaryResult}
                        >
                          Добавить в словарь
                        </button>
                      </div>
                    </form>

                    <div className="folder-panel">
                      <div className="folder-row">
                        <label className="webapp-field folder-select">
                          <span>Папка для сохранения</span>
                          <select
                            value={dictionaryFolderId}
                            onChange={(event) => setDictionaryFolderId(event.target.value)}
                          >
                            <option value="none">Без папки</option>
                            {folders.map((folder) => (
                              <option key={folder.id} value={folder.id}>
                                {resolveFolderIconLabel(folder.icon)} • {folder.name}
                              </option>
                            ))}
                          </select>
                        </label>
                        {dictionaryFolderId !== 'none' && selectedDictionaryFolder && (
                          <div className="folder-preview">
                            {renderFolderIcon(
                              selectedDictionaryFolder.icon,
                              selectedDictionaryFolder.color
                            )}
                            <span>{selectedDictionaryFolder.name}</span>
                          </div>
                        )}
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => setShowNewFolderForm((prev) => !prev)}
                        >
                          Новая папка
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={handleExportDictionaryPdf}
                          disabled={exportLoading}
                        >
                          {exportLoading ? 'Готовим PDF...' : 'Выгрузить PDF'}
                        </button>
                      </div>
                      {showNewFolderForm && (
                        <div className="folder-create">
                          <label className="webapp-field">
                            <span>Название</span>
                            <input
                              type="text"
                              value={newFolderName}
                              onChange={(event) => setNewFolderName(event.target.value)}
                              placeholder="Например: Путешествия"
                            />
                          </label>
                          <div className="folder-pickers">
                            <div className="folder-picker">
                              <span>Цвет</span>
                              <div className="folder-color-options">
                                {folderColorOptions.map((color) => (
                                  <button
                                    key={color}
                                    type="button"
                                    className={`color-dot ${newFolderColor === color ? 'is-active' : ''}`}
                                    style={{ background: color }}
                                    onClick={() => setNewFolderColor(color)}
                                  />
                                ))}
                              </div>
                            </div>
                            <div className="folder-picker">
                              <span>Иконка</span>
                              <div className="folder-icon-options">
                                {folderIconOptions.map((icon) => (
                                  <button
                                    key={icon}
                                    type="button"
                                    className={`icon-dot ${newFolderIcon === icon ? 'is-active' : ''}`}
                                    onClick={() => setNewFolderIcon(icon)}
                                  >
                                    {renderFolderIcon(icon, newFolderColor)}
                                  </button>
                                ))}
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="primary-button"
                            onClick={handleCreateFolder}
                            disabled={foldersLoading}
                          >
                            {foldersLoading ? 'Создаём...' : 'Создать папку'}
                          </button>
                          {foldersError && <div className="webapp-error">{foldersError}</div>}
                        </div>
                      )}
                      {!showNewFolderForm && foldersError && <div className="webapp-error">{foldersError}</div>}
                    </div>

                    {dictionaryError && <div className="webapp-error">{dictionaryError}</div>}
                    {dictionarySaved && <div className="webapp-success">{dictionarySaved}</div>}

                    {dictionaryResult && (
                      <div className="webapp-dictionary-result">
                        {lastLookupScrollY !== null && (
                          <button
                            type="button"
                            className="dictionary-back-button"
                            onClick={() => window.scrollTo({ top: lastLookupScrollY, behavior: 'smooth' })}
                          >
                            ← вернуться назад
                          </button>
                        )}
                        <div className="dictionary-row">
                          <span className="dictionary-label">Перевод:</span>
                          <span className="dictionary-translation">
                            {dictionaryDirection === 'ru-de'
                              ? (dictionaryResult.translation_de || '—')
                              : (dictionaryResult.translation_ru || '—')}
                          </span>
                          <span className="dictionary-direction">
                            {dictionaryDirection === 'ru-de' ? 'RU → DE' : 'DE → RU'}
                          </span>
                        </div>
                        <div className="dictionary-row">
                          <strong>Часть речи:</strong> {dictionaryResult.part_of_speech || '—'}
                        </div>
                        {dictionaryResult.article && (
                          <div className="dictionary-row">
                            <strong>Артикль:</strong> {dictionaryResult.article}
                          </div>
                        )}
                        {dictionaryResult.forms && (
                          <div className="dictionary-forms">
                            <div><strong>Plural:</strong> {dictionaryResult.forms.plural || '—'}</div>
                            <div><strong>Präteritum:</strong> {dictionaryResult.forms.praeteritum || '—'}</div>
                            <div><strong>Perfekt:</strong> {dictionaryResult.forms.perfekt || '—'}</div>
                            <div><strong>Konjunktiv I:</strong> {dictionaryResult.forms.konjunktiv1 || '—'}</div>
                            <div><strong>Konjunktiv II:</strong> {dictionaryResult.forms.konjunktiv2 || '—'}</div>
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.prefixes) && dictionaryResult.prefixes.length > 0 && (
                          <div className="dictionary-prefixes">
                            <strong>Префиксы/варианты:</strong>
                            <ul>
                              {dictionaryResult.prefixes.map((item, index) => (
                                <li key={`${item.variant}-${index}`}>
                                  <div><strong>{item.variant}:</strong> {item.translation_de || '—'}</div>
                                  {item.explanation && <div>{item.explanation}</div>}
                                  {item.example_de && <div><em>{item.example_de}</em></div>}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.usage_examples) && dictionaryResult.usage_examples.length > 0 && (
                          <div className="dictionary-examples">
                            <strong>Примеры:</strong>
                            <ul>
                              {dictionaryResult.usage_examples.map((example, index) => (
                                <li key={`${index}-${example}`}>{example}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                    {collocationsVisible && (
                      <div className="dictionary-collocations">
                        <h4>Выберите связку для словаря</h4>
                        {collocationsLoading && <div className="webapp-muted">Генерируем варианты...</div>}
                        {collocationsError && <div className="webapp-error">{collocationsError}</div>}
                        {!collocationsLoading && collocationOptions.length > 0 && (
                          <div className="collocation-list">
                            {collocationOptions.map((option, index) => (
                              <label key={`${option.source}-${index}`} className="collocation-item">
                                <input
                                  type="radio"
                                  name="collocation"
                                  checked={selectedCollocation?.source === option.source && selectedCollocation?.target === option.target}
                                  onChange={() => setSelectedCollocation(option)}
                                />
                                <div>
                                  <div className="collocation-source">{option.source}</div>
                                  <div className="collocation-target">{option.target}</div>
                                  {option.isBase && <span className="collocation-tag">Исходное</span>}
                                </div>
                              </label>
                            ))}
                          </div>
                        )}
                        <div className="collocation-actions">
                          <button
                            type="button"
                            className="primary-button"
                            onClick={handleConfirmSaveCollocation}
                            disabled={dictionaryLoading}
                          >
                            {dictionaryLoading ? 'Сохраняем...' : 'Добавить выбранное'}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => setCollocationsVisible(false)}
                          >
                            Отмена
                          </button>
                        </div>
                      </div>
                    )}
                    <div className="dictionary-flashcards">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          openFlashcardsSetup(flashcardsRef);
                        }}
                      >
                        Повторить слова
                      </button>
                    </div>
                  </section>
                )}
              </div>
            )}

            {!flashcardsOnly && isSectionVisible('movies') && !moviesCollapsed && (
              <section className="webapp-movies" ref={moviesRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>Фильмы</h2>
                  <p>Видео с доступными субтитрами, сохранённые в каталоге.</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                {moviesLoading && <div className="webapp-muted">Загружаем каталог...</div>}
                {moviesError && <div className="webapp-error">{moviesError}</div>}
                {!moviesLoading && !moviesError && movies.length === 0 && (
                  <div className="webapp-muted">Пока нет сохранённых видео.</div>
                )}
                {!moviesLoading && movies.length > 0 && (
                  <div className="movies-grid">
                    {movies.map((item) => (
                      <button
                        type="button"
                        key={item.video_id}
                        className="movie-card"
                        onClick={() => {
                          setYoutubeInput(`https://youtu.be/${item.video_id}`);
                          setMoviesCollapsed(true);
                          ensureSectionVisible('youtube');
                          setTimeout(() => scrollToRef(youtubeRef, { block: 'start' }), 120);
                        }}
                      >
                        <div className="movie-thumb">
                          <img src={item.thumbnail} alt={item.title} loading="lazy" />
                        </div>
                        <div className="movie-meta">
                          <div className="movie-title">{item.title}</div>
                          <div className="movie-subtitle">
                            {item.author ? `${item.author} • ` : ''}
                            {item.items_count ? `${item.items_count} строк` : 'Субтитры'}
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </section>
            )}

            {isSectionVisible('flashcards') && (
              <section className="webapp-flashcards" ref={flashcardsRef}>
                {!flashcardsOnly && (
                  <div className="srs-panel">
                    <div className="srs-panel-head">
                      <h3>Интервальное повторение (FSRS)</h3>
                      <div className="srs-queue">
                        <span>Due: {srsQueueInfo?.due_count ?? 0}</span>
                        <span>New today: {srsQueueInfo?.new_remaining_today ?? 0}</span>
                      </div>
                    </div>
                    {srsLoading && <div className="webapp-muted">Загружаем следующую карточку…</div>}
                    {!srsLoading && !srsCard && (
                      <div className="webapp-muted">На сейчас нет карточек для повторения.</div>
                    )}
                    {!srsLoading && srsCard && (
                      <div className="srs-card">
                        <div className="srs-card-front">{srsCard.word_ru || srsCard.word_de || '—'}</div>
                        {srsRevealAnswer && (
                          <div className="srs-card-back">
                            {srsCard.translation_de || srsCard.translation_ru || srsCard.word_de || '—'}
                          </div>
                        )}
                        <div className="srs-state-line">
                          <span>Status: {srsState?.status || 'new'}</span>
                          <span>Interval: {srsState?.interval_days ?? 0} дн</span>
                          {srsState?.is_mature && <span className="srs-mature">Освоено</span>}
                        </div>
                        {!srsRevealAnswer ? (
                          <div className="srs-actions">
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => setSrsRevealAnswer(true)}
                              disabled={srsSubmitting}
                            >
                              Показать ответ
                            </button>
                          </div>
                        ) : (
                          <div className="srs-rating-grid">
                            <button type="button" className="srs-rate again" onClick={() => submitSrsReview('AGAIN')} disabled={srsSubmitting}>
                              AGAIN
                            </button>
                            <button type="button" className="srs-rate hard" onClick={() => submitSrsReview('HARD')} disabled={srsSubmitting}>
                              HARD
                            </button>
                            <button type="button" className="srs-rate good" onClick={() => submitSrsReview('GOOD')} disabled={srsSubmitting}>
                              GOOD
                            </button>
                            <button type="button" className="srs-rate easy" onClick={() => submitSrsReview('EASY')} disabled={srsSubmitting}>
                              EASY
                            </button>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
                {!flashcardsVisible && (
                  <div className="webapp-muted">
                    Нажмите «Повторить слова», чтобы начать тренировку.
                  </div>
                )}
                {flashcardsVisible && (
                  <div className={`flashcards-panel ${flashcardsOnly ? 'is-session' : 'is-setup'}`}>
                    {!flashcardsOnly && (
                      <div className="flashcard-stage is-setup">
                        <div className="flashcards-setup">
                          <div className="setup-hero">
                            <div className="setup-ring">
                              <img src={heroStickerSrc} alt="Deutsch mascot" className="setup-mascot-flat" />
                            </div>
                            <div className="setup-title">Тренировка карточек</div>
                            <div className="setup-subtitle">Выберите параметры и стартуйте сет.</div>
                          </div>
                          <div className="setup-grid">
                            <div className="setup-group">
                              <div className="setup-label">Режим тренировки</div>
                              <div className="setup-options">
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardTrainingMode === 'quiz' ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardTrainingMode('quiz')}
                                >
                                  Quiz (4 варианта)
                                </button>
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardTrainingMode === 'blocks' ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardTrainingMode('blocks')}
                                >
                                  Blocks (сборка)
                                </button>
                              </div>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">Размер сета</div>
                              <div className="setup-options">
                                {[5, 10, 15].map((size) => (
                                  <button
                                    key={`set-${size}`}
                                    type="button"
                                    className={`option-pill ${flashcardSetSize === size ? 'is-active' : ''}`}
                                    onClick={() => setFlashcardSetSize(size)}
                                  >
                                    {size} карточек
                                  </button>
                                ))}
                              </div>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">Папка для тренировки</div>
                              <label className="webapp-field">
                                <select
                                  value={flashcardFolderMode === 'folder' ? flashcardFolderId : flashcardFolderMode}
                                  onChange={(event) => {
                                    const value = event.target.value;
                                    if (value === 'all') {
                                      setFlashcardFolderMode('all');
                                      setFlashcardFolderId('');
                                    } else if (value === 'none') {
                                      setFlashcardFolderMode('none');
                                      setFlashcardFolderId('');
                                    } else {
                                      setFlashcardFolderMode('folder');
                                      setFlashcardFolderId(value);
                                    }
                                  }}
                                >
                                  <option value="all">Все папки</option>
                                  <option value="none">Без папки</option>
                                  {folders.map((folder) => (
                                    <option key={folder.id} value={folder.id}>
                                      {resolveFolderIconLabel(folder.icon)} • {folder.name}
                                    </option>
                                  ))}
                                </select>
                              </label>
                            </div>
                            {flashcardTrainingMode === 'quiz' ? (
                              <div className="setup-group">
                                <div className="setup-label">Скорость</div>
                                <div className="setup-options">
                                  {[5, 10, 15].map((seconds) => (
                                    <button
                                      key={`speed-${seconds}`}
                                      type="button"
                                      className={`option-pill ${flashcardDurationSec === seconds ? 'is-active' : ''}`}
                                      onClick={() => setFlashcardDurationSec(seconds)}
                                    >
                                      {seconds} сек
                                    </button>
                                  ))}
                                </div>
                              </div>
                            ) : (
                              <div className="setup-group">
                                <div className="setup-label">Таймер Blocks</div>
                                <div className="setup-options">
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'adaptive' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('adaptive')}
                                  >
                                    Адаптивный
                                  </button>
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'fixed' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('fixed')}
                                  >
                                    10 сек
                                  </button>
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'none' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('none')}
                                  >
                                    Без таймера
                                  </button>
                                </div>
                              </div>
                            )}
                            <div className="setup-group">
                              <div className="setup-label">Переход</div>
                              <div className="setup-options">
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardAutoAdvance ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardAutoAdvance(true)}
                                >
                                  Автоматически
                                </button>
                                <button
                                  type="button"
                                  className={`option-pill ${!flashcardAutoAdvance ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardAutoAdvance(false)}
                                >
                                  Вручную
                                </button>
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="primary-button flashcards-start"
                            onClick={() => {
                              unlockAudio();
                              loadFlashcards();
                              setFlashcardPreviewActive(true);
                              setFlashcardsOnly(true);
                              setFlashcardExitSummary(false);
                            }}
                          >
                            Начать тренировку
                          </button>
                        </div>
                      </div>
                    )}

                    {flashcardsOnly && (
                      <>
                        {flashcardsLoading && <div className="webapp-muted">Загружаем карточки...</div>}
                        {flashcardsError && <div className="webapp-error">{flashcardsError}</div>}
                        {!flashcardsLoading && !flashcardsError && flashcards.length === 0 && (
                          <div className="webapp-muted">Словарь пуст. Сначала добавьте слова.</div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && flashcardPreviewActive && (
                          <div className="flashcard-stage is-session is-preview">
                            {(() => {
                              const entry = flashcards[flashcardPreviewIndex] || {};
                              const responseJson = entry.response_json || {};
                              const translations = responseJson.translations
                                || responseJson.translation_de
                                || entry.translation_de
                                || responseJson.translation_ru
                                || entry.translation_ru
                                || '';
                              const translationList = Array.isArray(translations)
                                ? translations
                                : String(translations).split(/[,;/]/).map((t) => t.trim()).filter(Boolean);
                              const feel = flashcardFeelMap[entry.id]
                                || responseJson.feel_explanation
                                || '';
                              const feelVisible = !!flashcardFeelVisibleMap[entry.id];
                              const feelLines = formatFeelLines(feel);
                              const previewNavLocked = previewAudioPlaying || !previewAudioReady;

                              return (
                                <div className="flashcard flashcard-preview">
                                  <div className="flashcard-header">
                                    <span className="flashcard-counter">
                                      {flashcardPreviewIndex + 1} / {flashcards.length}
                                    </span>
                                  </div>
                                  <div className="flashcard-word-row">
                                    <div className="flashcard-word">{entry.word_ru || entry.word_de || '—'}</div>
                                    <button
                                      type="button"
                                      className="flashcard-audio-replay"
                                      onClick={async () => {
                                        const text = resolveFlashcardGerman(entry);
                                        if (!text || previewAudioPlaying) return;
                                        try {
                                          setPreviewAudioPlaying(true);
                                          await playTts(text, 'de-DE');
                                        } finally {
                                          setPreviewAudioPlaying(false);
                                          setPreviewAudioReady(true);
                                        }
                                      }}
                                      aria-label="Повторить аудио"
                                      title="Повторить аудио"
                                      disabled={previewAudioPlaying}
                                    >
                                      🔊
                                    </button>
                                  </div>
                                  <div className="flashcard-details">
                                    {translationList.length > 0 && (
                                      <div className="flashcard-section">
                                        <div className="flashcard-section-title">Переводы</div>
                                        <div className="flashcard-translation-list">
                                          {translationList.map((item, idx) => (
                                            <span key={`${item}-${idx}`} className="flashcard-chip">
                                              {item}
                                            </span>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    {(responseJson.article || responseJson.part_of_speech || responseJson.is_separable !== undefined) && (
                                      <div className="flashcard-section">
                                        <div className="flashcard-section-title">Грамматика</div>
                                        <div className="flashcard-meta-grid">
                                          {responseJson.article && (
                                            <div className="flashcard-meta-item">
                                              <span>Артикль</span>
                                              <strong>{responseJson.article}</strong>
                                            </div>
                                          )}
                                          {responseJson.part_of_speech && (
                                            <div className="flashcard-meta-item">
                                              <span>Часть речи</span>
                                              <strong>{responseJson.part_of_speech}</strong>
                                            </div>
                                          )}
                                          {responseJson.is_separable !== undefined && (
                                            <div className="flashcard-meta-item">
                                              <span>Trennbar</span>
                                              <strong>{responseJson.is_separable ? 'да' : 'нет'}</strong>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    )}
                                    {feelVisible && feel && (
                                      <div className="flashcard-feel">
                                        <strong>Почувствовать слово</strong>
                                        <div className="flashcard-feel-content">
                                          {feelLines.map((line, idx) => (
                                            <p key={`${entry.id}-feel-${idx}`}>{line}</p>
                                          ))}
                                        </div>
                                        {entry.id && (
                                          <div className="flashcard-feel-feedback">
                                            <button
                                              type="button"
                                              className="secondary-button"
                                              disabled={!!flashcardFeelFeedbackLoading[entry.id]}
                                              onClick={async () => {
                                                try {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: true }));
                                                  const response = await fetch('/api/webapp/flashcards/feel/feedback', {
                                                    method: 'POST',
                                                    headers: { 'Content-Type': 'application/json' },
                                                    body: JSON.stringify({
                                                      initData,
                                                      entry_id: entry.id,
                                                      liked: true,
                                                    }),
                                                  });
                                                  if (!response.ok) {
                                                    throw new Error(await response.text());
                                                  }
                                                } catch (error) {
                                                  setWebappError(`Ошибка feedback: ${error.message}`);
                                                } finally {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: false }));
                                                }
                                              }}
                                            >
                                              👍 Нравится
                                            </button>
                                            <button
                                              type="button"
                                              className="secondary-button"
                                              disabled={!!flashcardFeelFeedbackLoading[entry.id]}
                                              onClick={async () => {
                                                try {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: true }));
                                                  const response = await fetch('/api/webapp/flashcards/feel/feedback', {
                                                    method: 'POST',
                                                    headers: { 'Content-Type': 'application/json' },
                                                    body: JSON.stringify({
                                                      initData,
                                                      entry_id: entry.id,
                                                      liked: false,
                                                    }),
                                                  });
                                                  if (!response.ok) {
                                                    throw new Error(await response.text());
                                                  }
                                                  setFlashcardFeelMap((prev) => {
                                                    const next = { ...prev };
                                                    delete next[entry.id];
                                                    return next;
                                                  });
                                                  setFlashcardFeelVisibleMap((prev) => ({
                                                    ...prev,
                                                    [entry.id]: false,
                                                  }));
                                                  setFlashcards((prev) => prev.map((item) => {
                                                    if (item.id !== entry.id) return item;
                                                    const nextResponse = { ...(item.response_json || {}) };
                                                    delete nextResponse.feel_explanation;
                                                    nextResponse.feel_feedback = 'dislike';
                                                    return { ...item, response_json: nextResponse };
                                                  }));
                                                } catch (error) {
                                                  setWebappError(`Ошибка feedback: ${error.message}`);
                                                } finally {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: false }));
                                                }
                                              }}
                                            >
                                              👎 Не нравится
                                            </button>
                                          </div>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                  <div className="flashcard-actions-row">
                                    <button
                                      type="button"
                                      className="secondary-button"
                                      disabled={!!flashcardFeelLoadingMap[entry.id]}
                                      onClick={async () => {
                                        if (!entry.id) return;
                                        try {
                                          setFlashcardFeelLoadingMap((prev) => ({
                                            ...prev,
                                            [entry.id]: true,
                                          }));
                                          const response = await fetch('/api/webapp/flashcards/feel', {
                                            method: 'POST',
                                            headers: { 'Content-Type': 'application/json' },
                                            body: JSON.stringify({
                                              initData,
                                              entry_id: entry.id,
                                              word_ru: entry.word_ru,
                                              word_de: entry.word_de,
                                            }),
                                          });
                                          if (!response.ok) {
                                            throw new Error(await response.text());
                                          }
                                          const data = await response.json();
                                          if (data.feel_explanation) {
                                            setFlashcardFeelMap((prev) => ({
                                              ...prev,
                                              [entry.id]: data.feel_explanation,
                                            }));
                                            setFlashcardFeelVisibleMap((prev) => ({
                                              ...prev,
                                              [entry.id]: true,
                                            }));
                                          }
                                        } catch (error) {
                                          setWebappError(`Ошибка feel: ${error.message}`);
                                        } finally {
                                          setFlashcardFeelLoadingMap((prev) => ({
                                            ...prev,
                                            [entry.id]: false,
                                          }));
                                        }
                                      }}
                                    >
                                      {flashcardFeelLoadingMap[entry.id] ? 'Загружаем...' : 'Почувствовать слово'}
                                    </button>
                                  </div>
                                  <div className="flashcard-actions-row">
                                    <button
                                      type="button"
                                      className="secondary-button"
                                      onClick={() => {
                                        setPreviewAudioReady(false);
                                        setPreviewAudioPlaying(true);
                                        const nextIndex = Math.max(flashcardPreviewIndex - 1, 0);
                                        setFlashcardPreviewIndex(nextIndex);
                                      }}
                                      disabled={flashcardPreviewIndex === 0 || previewAudioPlaying}
                                    >
                                      Назад
                                    </button>
                                    {flashcardPreviewIndex < flashcards.length - 1 ? (
                                      <button
                                        type="button"
                                        className="primary-button"
                                        onClick={() => {
                                          setPreviewAudioReady(false);
                                          setPreviewAudioPlaying(true);
                                          const nextIndex = Math.min(flashcardPreviewIndex + 1, flashcards.length - 1);
                                          setFlashcardPreviewIndex(nextIndex);
                                        }}
                                        disabled={previewNavLocked}
                                      >
                                        {previewAudioPlaying ? 'Слушаем...' : 'Далее'}
                                      </button>
                                    ) : (
                                      <button
                                        type="button"
                                        className="primary-button"
                                        onClick={() => {
                                          setFlashcardPreviewActive(false);
                                          setFlashcardSessionActive(true);
                                          setFlashcardIndex(0);
                                          setFlashcardSelection(null);
                                          setFlashcardTimerKey((prev) => prev + 1);
                                        }}
                                        disabled={previewNavLocked}
                                      >
                                        {previewAudioPlaying ? 'Слушаем...' : 'Начать тренировку'}
                                      </button>
                                    )}
                                  </div>
                                </div>
                              );
                            })()}
                          </div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && !flashcardPreviewActive && (
                          <div className="flashcard-stage is-session">
                            {(flashcardSetComplete || flashcardExitSummary) ? (
                              <div className="flashcard-summary">
                                <h4>{flashcardSetComplete ? 'Сет завершён' : 'Повтор завершён'}</h4>
                                <div className="summary-grid">
                                  <div>
                                    <span>Итого слов</span>
                                    <strong>{flashcardStats.total}</strong>
                                  </div>
                                  <div>
                                    <span>Верно</span>
                                    <strong>{flashcardStats.correct}</strong>
                                  </div>
                                  <div>
                                    <span>Неверно</span>
                                    <strong>{flashcardStats.wrong}</strong>
                                  </div>
                                </div>
                                <div className="summary-actions">
                                  <button
                                    type="button"
                                    className="primary-button"
                                    onClick={loadFlashcards}
                                  >
                                    Да, следующий сет
                                  </button>
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() => {
                                      setFlashcardsVisible(false);
                                      setFlashcardsOnly(false);
                                      setFlashcardSessionActive(false);
                                      setFlashcardExitSummary(false);
                                    }}
                                  >
                                    Нет, завершить
                                  </button>
                                </div>
                              </div>
                            ) : (
                              (() => {
                                const entry = flashcards[flashcardIndex] || {};
                                const responseJson = entry.response_json || {};
                                const correct = entry.translation_de
                                  || responseJson.translation_de
                                  || entry.translation_ru
                                  || responseJson.translation_ru
                                  || '—';
                                const questionWord = entry.word_ru
                                  || responseJson.word_ru
                                  || entry.word_de
                                  || responseJson.word_de
                                  || '—';
                                const context = Array.isArray(responseJson.usage_examples)
                                  ? responseJson.usage_examples[0]
                                  : '';
                                const blocksAnswer = resolveBlocksAnswer(entry);
                                const blocksPrompt = resolveBlocksPrompt(entry);
                                const blocksType = resolveBlocksType(entry, blocksAnswer);

                                if (flashcardTrainingMode === 'blocks') {
                                  return (
                                    <div className="flashcard flashcard-blocks">
                                      <div className="flashcard-header">
                                        <span className="flashcard-counter">
                                          {flashcardIndex + 1} / {flashcards.length}
                                        </span>
                                        <div className="flashcard-actions-row blocks-header-actions" ref={blocksMenuRef}>
                                          <button
                                            type="button"
                                            className="blocks-overflow-trigger"
                                            aria-haspopup="menu"
                                            aria-expanded={blocksMenuOpen}
                                            aria-label="Открыть меню"
                                            onClick={() => {
                                              setBlocksMenuOpen((prev) => !prev);
                                              if (blocksMenuOpen) {
                                                setBlocksMenuSettingsOpen(false);
                                              }
                                            }}
                                          >
                                            ⋯
                                          </button>
                                          {blocksMenuOpen && (
                                            <div className="blocks-overflow-menu" role="menu">
                                              <button
                                                type="button"
                                                className="blocks-overflow-item"
                                                onClick={() => {
                                                  resetCurrentBlocksCard();
                                                  setBlocksMenuOpen(false);
                                                  setBlocksMenuSettingsOpen(false);
                                                }}
                                              >
                                                Сбросить текущую карточку
                                              </button>
                                              <button
                                                type="button"
                                                className="blocks-overflow-item"
                                                onClick={() => {
                                                  setBlocksMenuSettingsOpen((prev) => !prev);
                                                }}
                                              >
                                                Настройки скорости / таймера
                                              </button>
                                              {blocksMenuSettingsOpen && (
                                                <div className="blocks-overflow-settings">
                                                  <div className="blocks-overflow-settings-label">Таймер</div>
                                                  <div className="blocks-overflow-pills">
                                                    <button
                                                      type="button"
                                                      className={`blocks-overflow-pill ${blocksTimerMode === 'adaptive' ? 'is-active' : ''}`}
                                                      onClick={() => {
                                                        setBlocksTimerMode('adaptive');
                                                        setBlocksMenuOpen(false);
                                                        setBlocksMenuSettingsOpen(false);
                                                      }}
                                                    >
                                                      Адаптивный
                                                    </button>
                                                    <button
                                                      type="button"
                                                      className={`blocks-overflow-pill ${blocksTimerMode === 'fixed' ? 'is-active' : ''}`}
                                                      onClick={() => {
                                                        setBlocksTimerMode('fixed');
                                                        setBlocksMenuOpen(false);
                                                        setBlocksMenuSettingsOpen(false);
                                                      }}
                                                    >
                                                      10 сек
                                                    </button>
                                                    <button
                                                      type="button"
                                                      className={`blocks-overflow-pill ${blocksTimerMode === 'none' ? 'is-active' : ''}`}
                                                      onClick={() => {
                                                        setBlocksTimerMode('none');
                                                        setBlocksMenuOpen(false);
                                                        setBlocksMenuSettingsOpen(false);
                                                      }}
                                                    >
                                                      Без таймера
                                                    </button>
                                                  </div>
                                                </div>
                                              )}
                                              <button
                                                type="button"
                                                className="blocks-overflow-item is-danger"
                                                onClick={() => {
                                                  setBlocksMenuOpen(false);
                                                  setBlocksMenuSettingsOpen(false);
                                                  requestFinishFlashcardSession();
                                                }}
                                              >
                                                ⚠️ Закончить повтор
                                              </button>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                      <BlocksTrainer
                                        card={entry}
                                        prompt={blocksPrompt}
                                        answer={blocksAnswer}
                                        cardType={blocksType}
                                        resetSignal={blocksResetNonce}
                                        timerMode={blocksTimerMode}
                                        autoAdvance={flashcardAutoAdvance}
                                        onRoundResult={({ isCorrect, timeSpentMs, hintsUsed, status }) => {
                                          setFlashcardTimedOut(status === 'timeout');
                                          setFlashcardOutcome(isCorrect ? 'correct' : (status === 'timeout' ? 'timeout' : 'wrong'));
                                          unlockAudio();
                                          playFeedbackSound(status === 'timeout' ? 'timeout' : (isCorrect ? 'positive' : 'negative'));
                                          setFlashcardStats((prev) => ({
                                            ...prev,
                                            correct: prev.correct + (isCorrect ? 1 : 0),
                                            wrong: prev.wrong + (isCorrect ? 0 : 1),
                                          }));
                                          recordFlashcardAnswer(entry.id, isCorrect, {
                                            mode: 'blocks',
                                            timeSpentMs,
                                            hintsUsed,
                                          });
                                          const german = resolveFlashcardGerman(entry) || blocksAnswer;
                                          if (german) {
                                            void playTts(german, 'de-DE');
                                          }
                                        }}
                                        onNext={() => advanceFlashcard()}
                                      />
                                      {blocksFinishConfirmOpen && (
                                        <div className="blocks-confirm-backdrop" onClick={() => setBlocksFinishConfirmOpen(false)}>
                                          <div className="blocks-confirm" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
                                            <h4>Завершить повтор?</h4>
                                            <p>Текущий прогресс будет завершён.</p>
                                            <div className="blocks-confirm-actions">
                                              <button
                                                type="button"
                                                className="secondary-button"
                                                onClick={() => setBlocksFinishConfirmOpen(false)}
                                              >
                                                Продолжить
                                              </button>
                                              <button
                                                type="button"
                                                className="danger-button"
                                                onClick={() => {
                                                  setBlocksFinishConfirmOpen(false);
                                                  setFlashcardExitSummary(true);
                                                }}
                                              >
                                                Завершить
                                              </button>
                                            </div>
                                          </div>
                                        </div>
                                      )}
                                    </div>
                                  );
                                }

                                return (
                                  <div className="flashcard">
                                    <div className="flashcard-header">
                                      <span className="flashcard-counter">
                                        {flashcardIndex + 1} / {flashcards.length}
                                      </span>
                                      <div className="flashcard-actions-row">
                                        <button
                                          type="button"
                                          className="flashcard-refresh"
                                          onClick={loadFlashcards}
                                        >
                                          Обновить
                                        </button>
                                      </div>
                                    </div>
                                    <div className="flashcard-hero">
                                      <div
                                        key={`timer-${flashcardTimerKey}`}
                                        className={`flashcard-timer ${flashcardSelection !== null ? 'is-paused' : ''}`}
                                        style={{ '--duration': `${flashcardDurationSec}s` }}
                                      >
                                        <svg viewBox="0 0 120 120" aria-hidden="true">
                                          <circle className="timer-track" cx="60" cy="60" r="54" />
                                          <circle className="timer-progress" cx="60" cy="60" r="54" />
                                        </svg>
                                        {flashcardOutcome === 'correct' && (
                                          <div className="flashcard-party" aria-hidden="true">
                                            <div className="flashcard-confetti">
                                              {Array.from({ length: 18 }).map((_, i) => (
                                                <span key={`conf-${flashcardTimerKey}-${i}`} />
                                              ))}
                                            </div>
                                            <div className="flashcard-sparkler">
                                              {Array.from({ length: 10 }).map((_, i) => (
                                                <span key={`spark-${flashcardTimerKey}-${i}`} />
                                              ))}
                                            </div>
                                          </div>
                                        )}
                                        <div className={`flashcard-character ${flashcardOutcome ? `is-${flashcardOutcome}` : ''}`}>
                                          <img
                                            src={flashcardOutcome === 'wrong' ? heroCrySrc : heroStickerSrc}
                                            alt="Deutsch mascot"
                                            className="flashcard-mascot-flat"
                                          />
                                          {(flashcardOutcome === 'wrong' || flashcardOutcome === 'timeout') && (
                                            <div className="flashcard-poop-shot" aria-hidden="true">
                                              <span className="flashcard-poop-throw">💩</span>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    </div>
                                    <div className="flashcard-word">{questionWord}</div>
                                    {flashcardSelection !== null && context && (
                                      <div className="flashcard-context flashcard-context-visible">{context}</div>
                                    )}
                                    <div className="flashcard-options">
                                      {flashcardOptions.map((option, idx) => {
                                        const isSelected = flashcardSelection === idx;
                                        const isCorrect = option === correct;
                                        const showResult = flashcardSelection !== null;
                                        const className = [
                                          'flashcard-option',
                                          showResult && isCorrect ? 'is-correct' : '',
                                          showResult && isSelected && !isCorrect ? 'is-wrong' : '',
                                        ]
                                          .filter(Boolean)
                                          .join(' ');
                                        return (
                                          <div key={`${option}-${idx}`} className="flashcard-option-row">
                                            <button
                                              type="button"
                                              className={className}
                                              onClick={async () => {
                                                if (flashcardSelection !== null) return;
                                                setFlashcardSelection(idx);
                                                setFlashcardTimedOut(false);
                                                setFlashcardOutcome(option === correct ? 'correct' : 'wrong');
                                                unlockAudio();
                                                playFeedbackSound(option === correct ? 'positive' : 'negative');
                                                const timeSpentMs = Math.max(0, Date.now() - flashcardRoundStartRef.current);
                                                recordFlashcardAnswer(entry.id, option === correct, {
                                                  mode: 'quiz',
                                                  timeSpentMs,
                                                  hintsUsed: 0,
                                                });
                                                setFlashcardStats((prev) => ({
                                                  ...prev,
                                                  correct: prev.correct + (option === correct ? 1 : 0),
                                                  wrong: prev.wrong + (option === correct ? 0 : 1),
                                                }));
                                                if (autoAdvanceTimeoutRef.current) {
                                                  clearTimeout(autoAdvanceTimeoutRef.current);
                                                  autoAdvanceTimeoutRef.current = null;
                                                }
                                                if (revealTimeoutRef.current) {
                                                  clearTimeout(revealTimeoutRef.current);
                                                }
                                                const german = resolveFlashcardGerman(entry);
                                                if (german) {
                                                  await playTts(german, 'de-DE');
                                                }
                                                if (flashcardAutoAdvance) {
                                                  revealTimeoutRef.current = setTimeout(() => {
                                                    advanceFlashcard();
                                                  }, 3000);
                                                }
                                              }}
                                            >
                                              {option}
                                            </button>
                                          </div>
                                        );
                                      })}
                                    </div>
                                    {flashcardTimedOut && (
                                      <div className="flashcard-timeout">die Zeit ist um</div>
                                    )}
                                    {flashcardSelection !== null && flashcardAutoAdvance && (
                                      <div className="flashcard-hint">
                                        Следующая карточка через 3 секунды
                                      </div>
                                    )}
                                    {flashcardSelection !== null && !flashcardAutoAdvance && (
                                      <div className="flashcard-actions">
                                        <button
                                          type="button"
                                          className="primary-button"
                                          onClick={() => advanceFlashcard()}
                                        >
                                          Следующая карточка
                                        </button>
                                      </div>
                                    )}
                                    {!(flashcardSetComplete || flashcardExitSummary) && (
                                      <div className="flashcard-end flashcard-end-session">
                                        <button
                                          type="button"
                                          className="secondary-button"
                                          onClick={() => {
                                            setFlashcardExitSummary(true);
                                          }}
                                        >
                                          Закончить повтор
                                        </button>
                                      </div>
                                    )}
                                  </div>
                                );
                              })()
                            )}
                          </div>
                        )}
                        {!flashcardsOnly && !(flashcardSetComplete || flashcardExitSummary) && (
                          <div className="flashcard-end">
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => {
                                setFlashcardExitSummary(true);
                              }}
                            >
                              Закончить повтор
                            </button>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('assistant') && (
              <section className="webapp-section voice-assistant-section" ref={assistantRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>Голосовой ассистент</h2>
                  <p>Практика разговорного немецкого в реальном времени.</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>

                {!assistantToken ? (
                  <div className="voice-assistant-join">
                    <div className="voice-assistant-meta">
                      <span>Пользователь: {assistantIdentity.displayName || '—'}</span>
                      <span>ID: {assistantIdentity.userId || '—'}</span>
                    </div>
                    {assistantError && <div className="webapp-error">{assistantError}</div>}
                    <button
                      type="button"
                      className="primary-button"
                      onClick={connectAssistant}
                      disabled={assistantConnecting || !webappUser?.id}
                    >
                      {assistantConnecting ? 'Подключаем...' : 'Подключить ассистента'}
                    </button>
                  </div>
                ) : (
                  <div className="voice-assistant-room-wrap" data-lk-theme="default">
                    <LiveKitRoom
                      serverUrl={livekitUrl}
                      token={assistantToken}
                      connect={true}
                      audio={true}
                      video={false}
                      onDisconnected={() => setAssistantToken(null)}
                      onError={(e) => setAssistantError(`LiveKit error: ${e?.message || e}`)}
                      className="voice-assistant-room"
                    >
                      <div className="voice-assistant-room-head">
                        <div>
                          <span className="pill">Учитель онлайн</span>
                          <h3>Живая практика немецкого</h3>
                        </div>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={disconnectAssistant}
                        >
                          Отключить
                        </button>
                      </div>
                      <p className="voice-assistant-hint">
                        Нажмите на микрофон в панели управления и начинайте диалог.
                      </p>
                      <div className="voice-assistant-controls">
                        <ControlBar />
                      </div>
                      <RoomAudioRenderer />
                      <ConnectionStateToast />
                    </LiveKitRoom>
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('analytics') && (
              <section className="webapp-section webapp-analytics" ref={analyticsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>Аналитика</h2>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="analytics-controls">
                  <label className="webapp-field">
                    <span>Период</span>
                    <select
                      value={analyticsPeriod}
                      onChange={(event) => setAnalyticsPeriod(event.target.value)}
                    >
                      <option value="day">День</option>
                      <option value="week">Неделя</option>
                      <option value="month">Месяц</option>
                      <option value="quarter">Квартал</option>
                      <option value="half-year">Полугодие</option>
                      <option value="year">Год</option>
                      <option value="all">Все время</option>
                    </select>
                  </label>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => loadAnalytics()}
                    disabled={analyticsLoading}
                  >
                    {analyticsLoading ? 'Считаем...' : 'Обновить'}
                  </button>
                  {analyticsRank && (
                    <div className="analytics-rank">Ваше место: #{analyticsRank}</div>
                  )}
                </div>

                {analyticsError && <div className="webapp-error">{analyticsError}</div>}

                {analyticsSummary && (
                  <div className="analytics-cards">
                    <div className="analytics-card">
                      <span>Переводы</span>
                      <strong>{analyticsSummary.total_translations}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Успех</span>
                      <strong>{analyticsSummary.success_rate}%</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Средний балл</span>
                      <strong>{analyticsSummary.avg_score}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Среднее время</span>
                      <strong>{analyticsSummary.avg_time_min} мин</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Пропущено дней</span>
                      <strong>{analyticsSummary.missed_days}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Пропущено</span>
                      <strong>{analyticsSummary.missed_sentences}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Итоговый балл</span>
                      <strong>{analyticsSummary.final_score}</strong>
                    </div>
                  </div>
                )}

                <div className="analytics-chart" ref={analyticsTrendRef} />
                <div className="analytics-chart analytics-compare" ref={analyticsCompareRef} />
              </section>
            )}

            {selectionText && selectionPos && (isSectionVisible('youtube') || isSectionVisible('dictionary')) && (
              <div
                className="webapp-selection-menu"
                style={{ left: `${selectionPos.x + 8}px`, top: `${selectionPos.y + 8}px` }}
                onMouseLeave={clearSelection}
              >
                <div className="webapp-selection-text">{selectionText}</div>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => handleQuickLookupDictionary(selectionText)}
                >
                  Перевести
                </button>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => handleQuickAddToDictionary(selectionText)}
                >
                  Добавить в словарь
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  // Если токена еще нет, показываем форму для входа
  // <form>: Это HTML-тег для сбора данных. Его особенность: он умеет реагировать на нажатие клавиши Enter на клавиатуре.
  // Когда пользователь нажимает Enter, форма автоматически вызывает функцию, указанную в onSubmit.
  // В нашем случае это handleConnect.
  // Таким образом, пользователь может либо нажать кнопку "Войти",
  // либо просто нажать Enter после ввода имени, и форма все равно сработает.
  // onSubmit — это событие "Отправка формы" (когда нажали кнопку submit или Enter).
  // e.preventDefault() внутри handleConnect предотвращает стандартное поведение формы — перезагрузку страницы.
  // {handleConnect} — мы говорим: "Когда случится отправка, НЕ перезагружай страницу (как делают старые сайты), а запусти нашу функцию handleConnect".
  // <h2>: Header 2. Заголовок второго уровня (жирный, крупный текст). Просто надпись.
  // Поле ввода <input> (Связь с памятью):
  // Это самая сложная концепция React, называется "Управляемый компонент" (Controlled Component).
  // Идея в том, что значение поля ввода (input) "связывается" с состоянием React (переменная username).
  // Когда пользователь вводит текст, срабатывает событие onChange.
  // Мы ловим это событие и вызываем setUsername с новым значением e.target.value.
  // Это обновляет состояние username в React.
  // Поскольку состояние изменилось, React перерисовывает компонент App,
  // и новое значение username снова "попадает" в поле ввода через атрибут value={username}.
  // Таким образом, поле ввода всегда "отражает" текущее состояние username.
  // Итог: Поле ввода и состояние username "связаны" друг с другом.
  // Любое изменение в поле ввода обновляет состояние,
  // а любое изменение состояния обновляет отображаемое значение в поле ввода.
  // Это позволяет нам точно контролировать, что находится в поле ввода в любой момент времени.
  // Кнопка <button type="submit">: Кнопка для отправки формы. При нажатии запускается событие onSubmit формы, вызывая handleConnect.
if (!token) {
    return (
      <div className="lesson-page lesson-login" data-lk-theme="default">
        <div className="lesson-bg" aria-hidden="true" />
        <div className="login-card">
          <div className="login-header">
            <span className="pill">Deutsch Tutor</span>
            <h2>Вход в урок</h2>
            <p>Подключитесь к разговорной практике и начните диалог с учителем.</p>
          </div>
          <form onSubmit={handleConnect} className="login-form">
            <label className="field">
              <span>Telegram ID</span>
              <input
                type="text"
                placeholder="Ваш Telegram ID (цифры)"
                value={telegramID}
                onChange={(e) => setTelegramID(e.target.value)}
              />
            </label>

            <label className="field">
              <span>Ваше имя</span>
              <input
                type="text"
                placeholder="Как вас называть? (Имя)"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </label>

            <button type="submit" className="primary-button">
              Начать урок
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <LiveKitRoom
      serverUrl={livekitUrl}
      token={token}
      connect={true}
      audio={true}
      video={false}
      onDisconnected={() => setToken(null)}
      onError={(e) => console.error("LiveKit error:", e)}
      className="lesson-page lesson-room"
      data-lk-theme="default"
    >
      <div className="lesson-bg" aria-hidden="true" />
      <div className="lesson-shell">
        <header className="lesson-header">
          <div>
            <span className="pill">Учитель онлайн</span>
            <h1>Живая практика немецкого</h1>
            <p>Говорите свободно, а помощник ведет диалог, исправляет и поддерживает.</p>
          </div>
          <div className="lesson-meta">
            <span>Пользователь: {username}</span>
            <span>ID: {telegramID}</span>
          </div>
        </header>

        <main className="lesson-main">
          <section className="lesson-hero">
            <div className="lesson-illustration" aria-hidden="true">
              <img src={heroMascotSrc} alt="" aria-hidden="true" className="lesson-hero-image" />
            </div>
            <div className="lesson-copy">
              <h2>Сфокусируйтесь на голосе</h2>
              <p>Нажмите на микрофон, чтобы включить речь, и нажмите выход, когда урок завершен.</p>
              <div className="lesson-tips">
                <div className="tip">Четко формулируйте ответы, чтобы учитель слышал интонацию.</div>
                <div className="tip">Если нужно подумать, просто сделайте паузу — связь сохранится.</div>
              </div>
            </div>
          </section>

          <section className="lesson-controls">
            <h3>Управление уроком</h3>
            <p>Все основные действия собраны в центре: микрофон, выход и настройки.</p>
            <div className="lesson-control-bar">
              <ControlBar />
            </div>
            <div className="lesson-hint">Совет: держите окно открытым, чтобы учитель не прерывал сессию.</div>
          </section>
        </main>
      </div>

      <RoomAudioRenderer />
      <ConnectionStateToast />
    </LiveKitRoom>
  );
}

export default function App() {
  return (
    <ErrorBoundary>
      <AppInner />
    </ErrorBoundary>
  );
}
