import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
import WeeklySummaryModal from './components/WeeklySummaryModal';
import { createTranslator, getPreferredLanguage, normalizeLanguage } from './i18n';
import { buildWeeklySummaryHeroFacts, buildWeeklySummaryVisitConfig } from './utils/weeklySummary';
import { detectAppMode } from './utils/appMode';

// URL вашего сервера LiveKit
const livekitUrl = "wss://implemrntingvoicetobot-vhsnc86g.livekit.cloud";
const SINGLE_INSTANCE_LOCK_KEY = 'dds_single_instance_lock_v1';
const SINGLE_INSTANCE_HEARTBEAT_MS = 2000;
const SINGLE_INSTANCE_STALE_MS = 12000;
const TTS_CACHE_MAX_ENTRIES = 60;
const READER_IDLE_TIMEOUT_MS = 60000;
const ALLOW_MANUAL_INITDATA_FALLBACK = Boolean(import.meta.env.DEV);

function isEditableElement(element) {
  if (!element || typeof element !== 'object') return false;
  const tagName = String(element.tagName || '').toUpperCase();
  if (tagName === 'TEXTAREA') return true;
  if (tagName !== 'INPUT') {
    return Boolean(element.isContentEditable);
  }
  const type = String(element.type || '').toLowerCase();
  return !['button', 'checkbox', 'file', 'hidden', 'image', 'radio', 'range', 'reset', 'submit'].includes(type);
}

function hasFocusedEditableElement() {
  if (typeof document === 'undefined') return false;
  return isEditableElement(document.activeElement);
}

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
      const isDe = typeof navigator !== 'undefined' && String(navigator.language || '').toLowerCase().startsWith('de');
      return (
        <div className="webapp-page">
          <div className="webapp-card">
            <header className="webapp-header">
              <span className="pill">Telegram Web App</span>
              <h1>{isDe ? 'Ladefehler' : 'Ошибка загрузки'}</h1>
              <p>{isDe ? 'Beim Starten der App ist ein Fehler aufgetreten. Bitte neu laden.' : 'Произошла ошибка при запуске приложения. Попробуйте перезагрузить.'}</p>
            </header>
            <div className="webapp-error">
              {this.state.error?.message || (isDe ? 'Unbekannter Fehler' : 'Неизвестная ошибка')}
            </div>
            {this.state.error?.stack && (
              <pre className="webapp-error webapp-error-stack">
                {String(this.state.error.stack).slice(0, 1200)}
              </pre>
            )}
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

const TranslationDraftField = React.memo(function TranslationDraftField({
  sentenceId,
  sentenceNumber,
  sentenceText,
  initialValue,
  placeholder,
  dictionaryLabel,
  isAndroidClient,
  onLiveChange,
  onCommit,
  onJumpToDictionary,
}) {
  const textareaRef = useRef(null);
  const syncTimeoutRef = useRef(null);
  const valueRef = useRef(String(initialValue || ''));

  const commitDelayMs = isAndroidClient ? 1200 : 240;

  useEffect(() => {
    const normalizedValue = String(initialValue || '');
    const node = textareaRef.current;
    if (!node) {
      valueRef.current = normalizedValue;
      return;
    }
    const isFocused = typeof document !== 'undefined' && document.activeElement === node;
    if (isFocused && String(node.value || '') !== normalizedValue) {
      valueRef.current = String(node.value || '');
      return;
    }
    if (String(node.value || '') !== normalizedValue) {
      node.value = normalizedValue;
    }
    valueRef.current = normalizedValue;
  }, [initialValue, sentenceId]);

  const flushValue = useCallback((value, reason = 'idle') => {
    onCommit(sentenceId, value, { reason });
  }, [onCommit, sentenceId]);

  const clearPendingFlush = useCallback(() => {
    if (syncTimeoutRef.current) {
      window.clearTimeout(syncTimeoutRef.current);
      syncTimeoutRef.current = null;
    }
  }, []);

  const scheduleFlush = useCallback((value) => {
    clearPendingFlush();
    syncTimeoutRef.current = window.setTimeout(() => {
      syncTimeoutRef.current = null;
      flushValue(valueRef.current, 'idle');
    }, commitDelayMs);
  }, [clearPendingFlush, commitDelayMs, flushValue]);

  useEffect(() => {
    return () => {
      clearPendingFlush();
    };
  }, [clearPendingFlush]);

  const handleInput = (event) => {
    const nextValue = event.target.value;
    valueRef.current = nextValue;
    onLiveChange(sentenceId, nextValue);
    scheduleFlush(nextValue);
  };

  const handleBlur = () => {
    const nextValue = textareaRef.current ? String(textareaRef.current.value || '') : valueRef.current;
    valueRef.current = nextValue;
    onLiveChange(sentenceId, nextValue);
    clearPendingFlush();
    flushValue(nextValue, 'blur');
  };

  return (
    <label className="webapp-translation-item">
      <span className="translation-sentence">
        {sentenceNumber}. {sentenceText}
      </span>
      <textarea
        ref={textareaRef}
        rows={5}
        defaultValue={String(initialValue || '')}
        onInput={handleInput}
        onBlur={handleBlur}
        placeholder={placeholder}
      />
      <div className="translation-actions">
        <button
          type="button"
          className="translation-dict-jump"
          onClick={onJumpToDictionary}
          aria-label={dictionaryLabel}
        >
          {dictionaryLabel}
        </button>
      </div>
    </label>
  );
});

function AppInner() {
  const telegramApp = useMemo(() => window.Telegram?.WebApp, []);
  const [appMode, setAppMode] = useState(() => detectAppMode());
  const [singleInstanceBlocked, setSingleInstanceBlocked] = useState(false);
  const [initData, setInitData] = useState(telegramApp?.initData || '');
  const isWebAppMode = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const isWebappPath =
      window.location.pathname === '/webapp' ||
      window.location.pathname === '/webapp/review';
    return Boolean(telegramApp?.initData || initData) || params.get('mode') === 'webapp' || isWebappPath;
  }, [telegramApp, initData]);
  const billingReturnContext = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const startParam = String(telegramApp?.initDataUnsafe?.start_param || '').trim().toLowerCase();
    let kind = String(params.get('billing') || '').trim().toLowerCase();
    const sessionId = String(params.get('session_id') || '').trim();
    let section = String(params.get('section') || '').trim().toLowerCase();
    if (!kind && startParam.startsWith('billing_')) {
      kind = startParam.slice('billing_'.length);
    }
    if (!section && kind) {
      section = 'subscription';
    }
    return {
      kind,
      sessionId,
      section,
      shouldHandle: ['success', 'cancel', 'portal'].includes(kind),
    };
  }, [telegramApp]);
  const isAndroidTelegramClient = useMemo(() => {
    const tgPlatform = String(telegramApp?.platform || '').toLowerCase();
    if (tgPlatform.includes('android')) return true;
    const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
    return /Android/i.test(userAgent);
  }, [telegramApp]);
  const needsContainedWebappScroll = useMemo(() => {
    if (!isWebAppMode) return false;
    const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
    const maxTouchPoints = typeof navigator !== 'undefined' ? Number(navigator.maxTouchPoints || 0) : 0;
    const isIPadDesktopUA = /Macintosh/i.test(userAgent) && maxTouchPoints > 1;
    const isIOS = /iPad|iPhone|iPod/i.test(userAgent) || isIPadDesktopUA;
    const isChromium = /Chrome|Chromium|CriOS|EdgA?|OPR|SamsungBrowser/i.test(userAgent);
    return isAndroidTelegramClient || (isChromium && !isIOS);
  }, [isAndroidTelegramClient, isWebAppMode]);

  const [browserAuthLoading, setBrowserAuthLoading] = useState(false);
  const [browserAuthError, setBrowserAuthError] = useState('');
  const [browserAuthBotUsername, setBrowserAuthBotUsername] = useState('');
  const [sessionId, setSessionId] = useState(null);
  const [webappUser, setWebappUser] = useState(null);
  const [webappChatType, setWebappChatType] = useState('');
  const [results, setResults] = useState([]);
  const [sentences, setSentences] = useState([]);
  const [webappError, setWebappError] = useState('');
  const [webappLoading, setWebappLoading] = useState(false);
  const [translationCheckProgress, setTranslationCheckProgress] = useState({ active: false, done: 0, total: 0 });
  const [translationDrafts, setTranslationDrafts] = useState({});
  const [finishMessage, setFinishMessage] = useState('');
  const [dictionaryWord, setDictionaryWord] = useState('');
  const [dictionaryResult, setDictionaryResult] = useState(null);
  const [dictionaryError, setDictionaryError] = useState('');
  const [dictionaryLoading, setDictionaryLoading] = useState(false);
  const [dictionaryLookupMode, setDictionaryLookupMode] = useState('');
  const [dictionarySaved, setDictionarySaved] = useState('');
  const [dictionaryDirection, setDictionaryDirection] = useState('ru-de');
  const [dictionaryLanguagePair, setDictionaryLanguagePair] = useState(null);
  const [collocationsVisible, setCollocationsVisible] = useState(false);
  const [collocationsLoading, setCollocationsLoading] = useState(false);
  const [collocationsError, setCollocationsError] = useState('');
  const [collocationOptions, setCollocationOptions] = useState([]);
  const [selectedCollocations, setSelectedCollocations] = useState([]);
  const [flashcardExitSummary, setFlashcardExitSummary] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [dictionaryPdfUrl, setDictionaryPdfUrl] = useState('');
  const [dictionaryPdfName, setDictionaryPdfName] = useState('dictionary.pdf');
  const [youtubeInput, setYoutubeInput] = useState('');
  const [youtubeId, setYoutubeId] = useState('');
  const [youtubeError, setYoutubeError] = useState('');
  const [youtubeSearchLoading, setYoutubeSearchLoading] = useState(false);
  const [youtubeSearchResults, setYoutubeSearchResults] = useState([]);
  const [youtubeSearchError, setYoutubeSearchError] = useState('');
  const [videoExpanded, setVideoExpanded] = useState(false);
  const [youtubeTranscript, setYoutubeTranscript] = useState([]);
  const [youtubeTranscriptError, setYoutubeTranscriptError] = useState('');
  const [youtubeTranscriptLoading, setYoutubeTranscriptLoading] = useState(false);
  const [youtubePlayerReady, setYoutubePlayerReady] = useState(false);
  const [youtubeCurrentTime, setYoutubeCurrentTime] = useState(0);
  const [youtubeTranslations, setYoutubeTranslations] = useState({});
  const [youtubeTranslationEnabled, setYoutubeTranslationEnabled] = useState(false);
  const [youtubeOverlayEnabled, setYoutubeOverlayEnabled] = useState(false);
  const [youtubeAppFullscreen, setYoutubeAppFullscreen] = useState(false);
  const [youtubeIsPaused, setYoutubeIsPaused] = useState(false);
  const [youtubePlaybackStarted, setYoutubePlaybackStarted] = useState(false);
  const [youtubeForceShowPanel, setYoutubeForceShowPanel] = useState(false);
  const [youtubeManualOverride, setYoutubeManualOverride] = useState(false);
  const [youtubeTranscriptHasTiming, setYoutubeTranscriptHasTiming] = useState(true);
  const [youtubeBackSection, setYoutubeBackSection] = useState('');
  const [movies, setMovies] = useState([]);
  const [moviesLoading, setMoviesLoading] = useState(false);
  const [moviesError, setMoviesError] = useState('');
  const [moviesCollapsed, setMoviesCollapsed] = useState(false);
  const [moviesLanguageFilter, setMoviesLanguageFilter] = useState('all');
  const [showManualTranscript, setShowManualTranscript] = useState(false);
  const [youtubeSettingsOpen, setYoutubeSettingsOpen] = useState(false);
  const [manualTranscript, setManualTranscript] = useState('');
  const [readerInput, setReaderInput] = useState('');
  const [readerSelectedFile, setReaderSelectedFile] = useState(null);
  const [readerLoading, setReaderLoading] = useState(false);
  const [readerError, setReaderError] = useState('');
  const [readerErrorCode, setReaderErrorCode] = useState('');
  const [readerContent, setReaderContent] = useState('');
  const [readerTitle, setReaderTitle] = useState('');
  const [readerSourceType, setReaderSourceType] = useState('');
  const [readerSourceUrl, setReaderSourceUrl] = useState('');
  const [readerDetectedLanguage, setReaderDetectedLanguage] = useState('');
  const [readerDocumentId, setReaderDocumentId] = useState(null);
  const [readerDocuments, setReaderDocuments] = useState([]);
  const [readerLibraryLoading, setReaderLibraryLoading] = useState(false);
  const [readerLibraryError, setReaderLibraryError] = useState('');
  const [readerIncludeArchived, setReaderIncludeArchived] = useState(false);
  const [readerPages, setReaderPages] = useState([]);
  const [readerCurrentPage, setReaderCurrentPage] = useState(1);
  const [readerAudioFromPage, setReaderAudioFromPage] = useState('');
  const [readerAudioToPage, setReaderAudioToPage] = useState('');
  const [readerAudioLoading, setReaderAudioLoading] = useState(false);
  const [readerAudioError, setReaderAudioError] = useState('');
  const [readerAudioPreviewUrl, setReaderAudioPreviewUrl] = useState('');
  const [readerAudioPreviewName, setReaderAudioPreviewName] = useState('');
  const [readerProgressPercent, setReaderProgressPercent] = useState(0);
  const [readerBookmarkPercent, setReaderBookmarkPercent] = useState(0);
  const [readerReadingMode, setReaderReadingMode] = useState('vertical');
  const [readerSessionStartedAt, setReaderSessionStartedAt] = useState('');
  const [readerLiveSeconds, setReaderLiveSeconds] = useState(0);
  const [readerAccumulatedSeconds, setReaderAccumulatedSeconds] = useState(0);
  const [readerTimerPaused, setReaderTimerPaused] = useState(false);
  const [readerSwipeSensitivity, setReaderSwipeSensitivity] = useState('medium');
  const [readerImmersive, setReaderImmersive] = useState(false);
  const [readerArchiveOpen, setReaderArchiveOpen] = useState(false);
  const [readerSettingsOpen, setReaderSettingsOpen] = useState(false);
  const [readerTopbarCollapsed, setReaderTopbarCollapsed] = useState(false);
  const [readerLibrarySearch, setReaderLibrarySearch] = useState('');
  const [readerFontSize, setReaderFontSize] = useState(18);
  const [readerFontWeight, setReaderFontWeight] = useState(500);
  const [selectionText, setSelectionText] = useState('');
  const [selectionPos, setSelectionPos] = useState(null);
  const [selectionType, setSelectionType] = useState('');
  const [selectedMeta, setSelectedMeta] = useState(null);
  const [selectionCompact, setSelectionCompact] = useState(false);
  const [selectionLookupLang, setSelectionLookupLang] = useState('');
  const [selectionInlineMode, setSelectionInlineMode] = useState(false);
  const [selectionInlineLookup, setSelectionInlineLookup] = useState({ loading: false, word: '', translation: '', direction: '', provider: '' });
  const [selectionLookupLoading, setSelectionLookupLoading] = useState(false);
  const [selectionGptOpen, setSelectionGptOpen] = useState(false);
  const [selectionGptLoading, setSelectionGptLoading] = useState(false);
  const [selectionGptError, setSelectionGptError] = useState('');
  const [selectionGptData, setSelectionGptData] = useState({ translation: '', notes: '', examples: [] });
  const [selectionGptSaveOriginalChecked, setSelectionGptSaveOriginalChecked] = useState(true);
  const [selectionGptSaveExamplesChecked, setSelectionGptSaveExamplesChecked] = useState({});
  const [selectionGptSaveLoading, setSelectionGptSaveLoading] = useState(false);
  const [selectionGptSaveError, setSelectionGptSaveError] = useState('');
  const [selectionGptSaveMessage, setSelectionGptSaveMessage] = useState('');
  const [ttsPendingMap, setTtsPendingMap] = useState({});
  const [inlineToast, setInlineToast] = useState('');
  const [inlineToastDurationMs, setInlineToastDurationMs] = useState(3000);
  const [lastLookupScrollY, setLastLookupScrollY] = useState(null);
  const [telegramFullscreenMode, setTelegramFullscreenMode] = useState(false);
  const [telegramTabletLike, setTelegramTabletLike] = useState(false);
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
  const [flashcardActiveMode, setFlashcardActiveMode] = useState(null); // 'fsrs' | 'quiz' | 'blocks' | 'sentence' | null
  const [flashcardSettingsModalMode, setFlashcardSettingsModalMode] = useState(null); // same domain as flashcardActiveMode
  const [flashcardSessionActive, setFlashcardSessionActive] = useState(false);
  const [flashcardPreviewActive, setFlashcardPreviewActive] = useState(false);
  const [flashcardPreviewIndex, setFlashcardPreviewIndex] = useState(0);
  const [srsCard, setSrsCard] = useState(null);
  const [srsState, setSrsState] = useState(null);
  const [srsQueueInfo, setSrsQueueInfo] = useState({ due_count: 0, new_remaining_today: 0 });
  const [srsPreview, setSrsPreview] = useState(null);
  const [srsPrefetchQueue, setSrsPrefetchQueue] = useState([]);
  const [todayPlan, setTodayPlan] = useState(null);
  const [todayPlanLoading, setTodayPlanLoading] = useState(false);
  const [todayPlanError, setTodayPlanError] = useState('');
  const [todayTestSending, setTodayTestSending] = useState(false);
  const [todayItemLoading, setTodayItemLoading] = useState({});
  const [todayTimerNowMs, setTodayTimerNowMs] = useState(Date.now());
  const [theoryLoading, setTheoryLoading] = useState(false);
  const [theoryError, setTheoryError] = useState('');
  const [theoryPackage, setTheoryPackage] = useState(null);
  const [theoryPracticeOpen, setTheoryPracticeOpen] = useState(false);
  const [theoryPracticeAnswers, setTheoryPracticeAnswers] = useState(['', '', '', '', '']);
  const [theoryChecking, setTheoryChecking] = useState(false);
  const [theoryFeedback, setTheoryFeedback] = useState(null);
  const [theoryItemId, setTheoryItemId] = useState(null);
  const [translationPrivateGrammarTextOptIn, setTranslationPrivateGrammarTextOptIn] = useState(false);
  const [translationAudioGrammarOptIn, setTranslationAudioGrammarOptIn] = useState({});
  const [translationAudioGrammarSaving, setTranslationAudioGrammarSaving] = useState({});
  const [skillReport, setSkillReport] = useState(null);
  const [skillReportLoading, setSkillReportLoading] = useState(false);
  const [skillReportError, setSkillReportError] = useState('');
  const [skillPracticeLoading, setSkillPracticeLoading] = useState({});
  const [skillTrainingLoading, setSkillTrainingLoading] = useState(false);
  const [skillTrainingError, setSkillTrainingError] = useState('');
  const [skillTrainingData, setSkillTrainingData] = useState(null);
  const [skillTrainingAnswers, setSkillTrainingAnswers] = useState(['', '', '', '', '']);
  const [skillTrainingChecking, setSkillTrainingChecking] = useState(false);
  const [skillTrainingFeedback, setSkillTrainingFeedback] = useState(null);
  const [skillTrainingVideoLoading, setSkillTrainingVideoLoading] = useState(false);
  const [skillTrainingDraftMap, setSkillTrainingDraftMap] = useState({});
  const [weeklyPlan, setWeeklyPlan] = useState(null);
  const [weeklyPlanLoading, setWeeklyPlanLoading] = useState(false);
  const [weeklyPlanSaving, setWeeklyPlanSaving] = useState(false);
  const [weeklyPlanError, setWeeklyPlanError] = useState('');
  const [weeklyPlanDraft, setWeeklyPlanDraft] = useState({ translations_goal: '', learned_words_goal: '', agent_minutes_goal: '', reading_minutes_goal: '' });
  const [weeklyPlanCollapsed, setWeeklyPlanCollapsed] = useState(false);
  const [planAnalyticsPeriod, setPlanAnalyticsPeriod] = useState('week');
  const [planAnalyticsMetrics, setPlanAnalyticsMetrics] = useState({});
  const [planAnalyticsRange, setPlanAnalyticsRange] = useState(null);
  const [planAnalyticsLoading, setPlanAnalyticsLoading] = useState(false);
  const [planAnalyticsError, setPlanAnalyticsError] = useState('');
  const [weeklyMetricExpanded, setWeeklyMetricExpanded] = useState({
    translations: false,
    learned_words: false,
    agent_minutes: false,
    reading_minutes: false,
  });
  const [srsLoading, setSrsLoading] = useState(false);
  const [srsSubmitting, setSrsSubmitting] = useState(false);
  const [srsSubmittingRating, setSrsSubmittingRating] = useState(null);
  const [srsRevealAnswer, setSrsRevealAnswer] = useState(false);
  const [srsRevealStartedAt, setSrsRevealStartedAt] = useState(0);
  const [srsRevealElapsedSec, setSrsRevealElapsedSec] = useState(0);
  const [srsError, setSrsError] = useState('');
  const [flashcardFeelQueuedMap, setFlashcardFeelQueuedMap] = useState({});
  const [flashcardFeelStatusMap, setFlashcardFeelStatusMap] = useState({});
  const [flashcardFeelDispatching, setFlashcardFeelDispatching] = useState(false);
  const [previewAudioReady, setPreviewAudioReady] = useState(false);
  const [previewAudioPlaying, setPreviewAudioPlaying] = useState(false);
  const [flashcardTimerKey, setFlashcardTimerKey] = useState(0);
  const [topics, setTopics] = useState([
    '🧩 ЗАГАДОЧНАЯ ИСТОРИЯ',
    '🧱 V2 в главном предложении',
    '🔗 Порядок слов в придаточном',
    '🪢 weil / dass / wenn / obwohl',
    '🧭 Akkusativ и Dativ',
    '📍 Wechselpraepositionen',
    '👑 Артикли: der / die / das / ein / eine / kein',
    '🎨 Склонение прилагательных',
    '🔁 Отделяемые глаголы',
    '🛠️ Модальные глаголы',
    '🪞 Возвратные глаголы',
    '⏳ Perfekt и Praeteritum',
    '💭 Konjunktiv II',
    '🧮 Passiv',
    '🔍 Relativsaetze',
    '⚙️ zu + Infinitiv / um ... zu',
    '✍️ Свой грамматический фокус',
  ]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState('');
  const [selectedTopic, setSelectedTopic] = useState('🧱 V2 в главном предложении');
  const [customTopicInput, setCustomTopicInput] = useState('');
  const [selectedLevel, setSelectedLevel] = useState('c1');
  const STORY_TOPIC = '🧩 ЗАГАДОЧНАЯ ИСТОРИЯ';
  const CUSTOM_TOPIC = '✍️ Свой грамматический фокус';
  const isStoryTopic = (value) => (value || '').includes('ЗАГАДОЧНАЯ ИСТОРИЯ');
  const isCustomTopic = (value) => String(value || '').trim() === CUSTOM_TOPIC;
  const normalizeSelectedTopicValue = useCallback((value) => {
    const rawValue = String(value || '').trim();
    if (isStoryTopic(rawValue)) {
      return STORY_TOPIC;
    }
    if (isCustomTopic(rawValue)) {
      return CUSTOM_TOPIC;
    }
    const exactMatch = topics.find((topic) => String(topic || '').trim() === rawValue);
    if (exactMatch) {
      return exactMatch;
    }
    return rawValue;
  }, [topics]);
  const handleTopicChange = useCallback((event) => {
    setSelectedTopic(normalizeSelectedTopicValue(event?.target?.value));
  }, [normalizeSelectedTopicValue]);
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
  const [globalTimerSuspended, setGlobalTimerSuspended] = useState(false);
  const [, setGlobalPauseReason] = useState('');
  const [blocksResetNonce, setBlocksResetNonce] = useState(0);
  const [blocksMenuOpen, setBlocksMenuOpen] = useState(false);
  const [blocksFinishConfirmOpen, setBlocksFinishConfirmOpen] = useState(false);
  const [folders, setFolders] = useState([]);
  const [foldersLoading, setFoldersLoading] = useState(false);
  const [foldersError, setFoldersError] = useState('');
  const [dictionaryFolderId, setDictionaryFolderId] = useState('none');
  const [flashcardFolderMode, setFlashcardFolderMode] = useState('all');
  const [flashcardFolderId, setFlashcardFolderId] = useState('');
  const [flashcardAutoAdvance, setFlashcardAutoAdvance] = useState(true);
  const [sentenceDifficulty, setSentenceDifficulty] = useState('medium');
  const [showNewFolderForm, setShowNewFolderForm] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [newFolderColor, setNewFolderColor] = useState('#5ddcff');
  const [newFolderIcon, setNewFolderIcon] = useState('book');
  const [userAvatar, setUserAvatar] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const [menuMultiSelect, setMenuMultiSelect] = useState(false);
  const [analyticsPeriod, setAnalyticsPeriod] = useState('week');
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState('');
  const [analyticsSummary, setAnalyticsSummary] = useState(null);
  const [analyticsPoints, setAnalyticsPoints] = useState([]);
  const [analyticsCompare, setAnalyticsCompare] = useState([]);
  const [analyticsRank, setAnalyticsRank] = useState(null);
  const [analyticsScopeData, setAnalyticsScopeData] = useState(null);
  const [analyticsScopeKey, setAnalyticsScopeKey] = useState('personal');
  const [analyticsScopeLoading, setAnalyticsScopeLoading] = useState(false);
  const [analyticsScopeSaving, setAnalyticsScopeSaving] = useState(false);
  const [analyticsScopeError, setAnalyticsScopeError] = useState('');
  const [economicsPeriod, setEconomicsPeriod] = useState('month');
  const [economicsAllocation, setEconomicsAllocation] = useState('weighted');
  const [economicsLoading, setEconomicsLoading] = useState(false);
  const [economicsError, setEconomicsError] = useState('');
  const [economicsSummary, setEconomicsSummary] = useState(null);
  const [billingStatusLoading, setBillingStatusLoading] = useState(false);
  const [billingStatusError, setBillingStatusError] = useState('');
  const [billingStatus, setBillingStatus] = useState(null);
  const [billingPlansLoading, setBillingPlansLoading] = useState(false);
  const [billingPlansError, setBillingPlansError] = useState('');
  const [billingPlans, setBillingPlans] = useState([]);
  const [billingActionLoading, setBillingActionLoading] = useState(false);
  const [billingPlanDetailsOpenFor, setBillingPlanDetailsOpenFor] = useState('');
  const [languageProfile, setLanguageProfile] = useState(null);
  const [languageProfileDraft, setLanguageProfileDraft] = useState({ learning_language: 'de', native_language: 'ru' });
  const [languageProfileLoading, setLanguageProfileLoading] = useState(false);
  const [languageProfileSaving, setLanguageProfileSaving] = useState(false);
  const [languageProfileError, setLanguageProfileError] = useState('');
  const [languageProfileModalOpen, setLanguageProfileModalOpen] = useState(false);
  const [starterDictionaryOffer, setStarterDictionaryOffer] = useState(null);
  const [starterDictionaryPromptOpen, setStarterDictionaryPromptOpen] = useState(false);
  const [starterDictionaryActionLoading, setStarterDictionaryActionLoading] = useState(false);
  const [starterDictionaryActionError, setStarterDictionaryActionError] = useState('');
  const [starterDictionaryActionMessage, setStarterDictionaryActionMessage] = useState('');
  const [supportMessages, setSupportMessages] = useState([]);
  const [supportFailedMessages, setSupportFailedMessages] = useState([]);
  const [supportUnreadCount, setSupportUnreadCount] = useState(0);
  const [supportLoading, setSupportLoading] = useState(false);
  const [supportSending, setSupportSending] = useState(false);
  const [supportError, setSupportError] = useState('');
  const [supportDraft, setSupportDraft] = useState('');
  const [supportAttachment, setSupportAttachment] = useState(null);
  const [guideQuickCardDismissed, setGuideQuickCardDismissed] = useState(false);
  const [onboardingOpen, setOnboardingOpen] = useState(false);
  const [onboardingStep, setOnboardingStep] = useState(0);
  const [weeklySummaryModalOpen, setWeeklySummaryModalOpen] = useState(false);
  const [weeklySummaryHeroLoading, setWeeklySummaryHeroLoading] = useState(false);
  const [weeklySummaryHeroError, setWeeklySummaryHeroError] = useState('');
  const [weeklySummaryHeroFacts, setWeeklySummaryHeroFacts] = useState(null);
  const [weeklySummaryCurrentMetrics, setWeeklySummaryCurrentMetrics] = useState(null);
  const [weeklySummaryPreviousMetrics, setWeeklySummaryPreviousMetrics] = useState(null);
  const [weeklySummarySocialSignal, setWeeklySummarySocialSignal] = useState(null);
  const [guideStepOpenKey, setGuideStepOpenKey] = useState('start_setup');
  const isStorySession = sessionType === 'story' || isStoryTopic(selectedTopic);
  const isStoryResultMode = Boolean(storyResult && isStorySession);
  const hasActiveTranslationSentences = sentences.length > 0;
  const showTranslationStartConfigurator = !hasActiveTranslationSentences;
  const BLOCKS_SINGLE_WORD_MAX_LEN = 10;
  const FLASHCARDS_LOAD_DEDUP_WINDOW_MS = 900;
  const SRS_NEXT_DEDUP_WINDOW_MS = 900;
  const SRS_EASY_LOCK_AFTER_SEC = 5;
  const SRS_GOOD_LOCK_AFTER_SEC = 7;
  const srsEasyLocked = srsRevealAnswer && srsRevealElapsedSec >= SRS_EASY_LOCK_AFTER_SEC;
  const srsGoodLocked = srsRevealAnswer && srsRevealElapsedSec >= SRS_GOOD_LOCK_AFTER_SEC;

  const dictionaryRef = useRef(null);
  const theoryRef = useRef(null);
  const skillTrainingRef = useRef(null);
  const readerRef = useRef(null);
  const readerArticleRef = useRef(null);
  const flashcardsRef = useRef(null);
  const translationsRef = useRef(null);
  const youtubeRef = useRef(null);
  const moviesRef = useRef(null);
  const youtubeSubtitlesRef = useRef(null);
  const youtubePlayerRef = useRef(null);
  const youtubePausedBySelectionRef = useRef(false);
  const youtubePlayerShellRef = useRef(null);
  const youtubeTimeIntervalRef = useRef(null);
  const youtubeCurrentTimeRef = useRef(0);
  const youtubeResumeAppliedForVideoRef = useRef('');
  const youtubeResumeLastSavedSecondRef = useRef(-1);
  const youtubeResumeLastSyncedSecondRef = useRef(-1);
  const youtubeTranslateInFlightRef = useRef(false);
  const youtubeTranslateIndexRef = useRef(-1);
  const autoAdvanceTimeoutRef = useRef(null);
  const revealTimeoutRef = useRef(null);
  const flashcardIndexRef = useRef(0);
  const flashcardSelectionRef = useRef(null);
  const flashcardTrainingModeRef = useRef('quiz');
  const flashcardRoundStartRef = useRef(Date.now());
  const blocksMenuRef = useRef(null);
  const srsShownAtRef = useRef(null);
  const srsAutoTtsPlayedRef = useRef('');
  const srsTtsPrefetchSignatureRef = useRef('');
  const srsCardRef = useRef(null);
  const srsPrefetchQueueRef = useRef([]);
  const srsPrefetchInFlightRef = useRef(false);
  const srsReviewBufferRef = useRef([]);
  const srsReviewDrainInFlightRef = useRef(false);
  const srsReviewRetryTimerRef = useRef(null);
  const srsInitSignatureRef = useRef('');
  const srsNextLoadInFlightRef = useRef(false);
  const srsNextLoadPendingRef = useRef(false);
  const srsNextLoadLastSignatureRef = useRef('');
  const srsNextLoadLastStartedAtRef = useRef(0);
  const flashcardsLoadInFlightRef = useRef(false);
  const flashcardsLoadInFlightSignatureRef = useRef('');
  const flashcardsLoadPendingRef = useRef(false);
  const flashcardsLoadLastSignatureRef = useRef('');
  const flashcardsLoadLastStartedAtRef = useRef(0);
  const ttsCacheRef = useRef(new Map());
  const ttsInFlightRef = useRef(new Map());
  const ttsPrefetchInFlightRef = useRef(new Map());
  const ttsPendingCacheRef = useRef(new Map());
  const ttsLastRef = useRef({ key: '', ts: 0 });
  const ttsCurrentAudioRef = useRef(null);
  const ttsPlaybackSeqRef = useRef(0);
  const audioContextRef = useRef(null);
  const positiveAudioRef = useRef(null);
  const negativeAudioRef = useRef(null);
  const timeoutAudioRef = useRef(null);
  const avatarInputRef = useRef(null);
  const analyticsRef = useRef(null);
  const economicsRef = useRef(null);
  const billingRef = useRef(null);
  const assistantRef = useRef(null);
  const supportRef = useRef(null);
  const supportAttachmentInputRef = useRef(null);
  const guideRef = useRef(null);
  const analyticsTrendRef = useRef(null);
  const analyticsCompareRef = useRef(null);
  const selectionMenuRef = useRef(null);
  const telegramLoginWidgetRef = useRef(null);
  const youtubeAutoFolderCacheRef = useRef(new Map());
  const youtubeAutoFolderPendingRef = useRef(new Map());
  const ttsPendingKeysRef = useRef(new Set());
  const flashcardFeelQueueRef = useRef(new Map());
  const flashcardFeelDispatchInFlightRef = useRef(false);
  const singleInstanceTokenRef = useRef(`inst_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`);
  const singleInstanceOwnsLockRef = useRef(false);
  const singleInstanceHeartbeatRef = useRef(null);
  const inlineToastTimeoutRef = useRef(null);
  const readerSessionStartingRef = useRef(false);
  const readerStateSaveTimeoutRef = useRef(null);
  const readerTimerIntervalRef = useRef(null);
  const readerIdleTimeoutRef = useRef(null);
  const readerLastInteractionAtRef = useRef(0);
  const readerSwipeStartRef = useRef(null);
  const readerPageNavLockRef = useRef(false);
  const todayTimerCompletionLockRef = useRef(new Set());
  const globalTimerAutoPauseInFlightRef = useRef(false);
  const globalTimerAutoResumeInFlightRef = useRef(false);
  const sectionVisibilitySnapshotRef = useRef(null);
  const autoPausedTodayTimerIdsRef = useRef(new Set());
  const youtubeTodayTimerSyncInFlightRef = useRef(false);
  const readerAutoPausedByNavigationRef = useRef(false);
  const readerAutoPausedByIdleRef = useRef(false);
  const translationCheckPollTokenRef = useRef(0);
  const translationCheckUnmountedRef = useRef(false);
  const translationSubmitInFlightRef = useRef(false);
  const translationStartInFlightRef = useRef(false);
  const translationFinishInFlightRef = useRef(false);
  const translationDraftsRef = useRef({});
  const translationDraftSyncTimeoutRef = useRef(null);
  const translationDraftStorageTimeoutRef = useRef(null);
  const translationDraftHydrationKeyRef = useRef('');
  const translationDraftSentenceIdsRef = useRef(new Set());
  const explanationInFlightKeysRef = useRef(new Set());
  const supportBottomRef = useRef(null);
  const assetBaseUrl = import.meta.env.BASE_URL || '/';
  const heroMascotSrc = `${assetBaseUrl}hero_original.webp`;
  const heroStickerSrc = `${assetBaseUrl}hero_sticker.webp`;
  const heroCrySrc = `${assetBaseUrl}hero_cry.webp`;
  const heroThinkSrc = `${assetBaseUrl}hero_think.webp`;

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
  const getLocalDateKey = () => {
    const now = new Date();
    const yyyy = String(now.getFullYear());
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  };
  function getInitDataUserId(rawInitData) {
    const value = String(rawInitData || '').trim();
    if (!value) return '';
    try {
      const params = new URLSearchParams(value);
      const userRaw = params.get('user');
      if (!userRaw) return '';
      const parsedUser = JSON.parse(userRaw);
      const candidate = String(parsedUser?.id || '').trim();
      return candidate;
    } catch (_error) {
      return '';
    }
  }
  const guideQuickCardStorageKey = useMemo(() => {
    const stableId = String(webappUser?.id || getInitDataUserId(initData) || 'anon').trim() || 'anon';
    return `guide_quick_card_dismissed_${stableId}`;
  }, [webappUser?.id, initData]);
  const weeklySummaryStableUserId = useMemo(() => {
    return String(webappUser?.id || getInitDataUserId(initData) || 'anon').trim() || 'anon';
  }, [webappUser?.id, initData]);
  const onboardingSeenStorageKey = useMemo(() => {
    const stableId = String(webappUser?.id || getInitDataUserId(initData) || 'anon').trim() || 'anon';
    return `webapp_onboarding_seen_${stableId}`;
  }, [webappUser?.id, initData]);
  const youtubeResumeStorageKey = useMemo(() => {
    const stableId = String(webappUser?.id || getInitDataUserId(initData) || 'anon').trim() || 'anon';
    return `webapp_youtube_resume_${stableId}`;
  }, [webappUser?.id, initData]);
  const translationDraftScopeKey = useMemo(() => {
    for (const item of sentences) {
      const candidate = String(item?.source_session_id || '').trim();
      if (candidate) return candidate;
    }
    return '';
  }, [sentences]);
  const translationDraftStorageKey = useMemo(() => {
    const stableId = String(webappUser?.id || getInitDataUserId(initData) || 'anon').trim() || 'anon';
    const scopeKey = translationDraftScopeKey || 'nosession';
    return `webappDrafts_${stableId}_${scopeKey}`;
  }, [webappUser?.id, initData, translationDraftScopeKey]);
  const writeYoutubeResumeToLocalCache = useCallback((payload) => {
    if (!payload || typeof payload !== 'object') return;
    const serialized = JSON.stringify(payload);
    safeStorageSet(youtubeResumeStorageKey, serialized);
    safeStorageSet('webapp_youtube', serialized);
  }, [youtubeResumeStorageKey]);
  const buildTranslationDraftPayload = useCallback((draftMap) => {
    const sentenceIds = Array.from(translationDraftSentenceIdsRef.current);
    if (!sentenceIds.length) return [];
    return sentenceIds.map((sentenceId) => ({
      id_for_mistake_table: sentenceId,
      translation: String(draftMap?.[String(sentenceId)] ?? ''),
    }));
  }, []);
  const persistTranslationDraftsToServer = useCallback(async (draftMap, options = {}) => {
    if (!initData || !translationDraftScopeKey) return null;
    try {
      const response = await fetch('/api/webapp/translation/drafts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          drafts: buildTranslationDraftPayload(draftMap),
        }),
        keepalive: Boolean(options?.keepalive),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return await response.json();
    } catch (error) {
      if (!options?.silent) {
        console.warn('Failed to persist translation drafts', error);
      }
      return null;
    }
  }, [buildTranslationDraftPayload, initData, translationDraftScopeKey]);
  const getActiveTranslationDraftMap = useCallback(() => {
    const next = {};
    translationDraftSentenceIdsRef.current.forEach((sentenceId) => {
      const key = String(sentenceId);
      next[key] = String(translationDraftsRef.current?.[key] ?? '');
    });
    return next;
  }, []);
  const persistTranslationDraftsToLocalCache = useCallback((draftMap) => {
    if (!translationDraftStorageKey) return;
    const hasNonEmptyValue = Object.values(draftMap || {}).some((value) => String(value ?? '').trim() !== '');
    if (!hasNonEmptyValue) {
      safeStorageRemove(translationDraftStorageKey);
      return;
    }
    safeStorageSet(translationDraftStorageKey, JSON.stringify(draftMap));
  }, [translationDraftStorageKey]);
  const flushTranslationDraftsToLocalCache = useCallback(() => {
    persistTranslationDraftsToLocalCache(getActiveTranslationDraftMap());
  }, [getActiveTranslationDraftMap, persistTranslationDraftsToLocalCache]);
  const flushTranslationDraftsToServer = useCallback((options = {}) => {
    const hydrationKey = `${translationDraftStorageKey}:${translationDraftScopeKey || 'nosession'}`;
    if (!initData || !translationDraftScopeKey || translationDraftSentenceIdsRef.current.size === 0) {
      return null;
    }
    if (translationDraftHydrationKeyRef.current !== hydrationKey) {
      return null;
    }
    return persistTranslationDraftsToServer(getActiveTranslationDraftMap(), options);
  }, [
    getActiveTranslationDraftMap,
    initData,
    persistTranslationDraftsToServer,
    translationDraftScopeKey,
    translationDraftStorageKey,
  ]);
  const scheduleTranslationDraftPersistence = useCallback((options = {}) => {
    const localDelayMs = Number.isFinite(options?.localDelayMs)
      ? Math.max(0, Number(options.localDelayMs))
      : (isAndroidTelegramClient ? 700 : 220);
    const serverDelayMs = Number.isFinite(options?.serverDelayMs)
      ? Math.max(0, Number(options.serverDelayMs))
      : (isAndroidTelegramClient ? 1800 : 350);
    const shouldPersistLocal = options?.local !== false;
    const shouldPersistServer = options?.server === true || (!isAndroidTelegramClient && options?.server !== false);
    if (translationDraftStorageTimeoutRef.current) {
      clearTimeout(translationDraftStorageTimeoutRef.current);
      translationDraftStorageTimeoutRef.current = null;
    }
    if (translationDraftSyncTimeoutRef.current) {
      clearTimeout(translationDraftSyncTimeoutRef.current);
      translationDraftSyncTimeoutRef.current = null;
    }
    if (shouldPersistLocal) {
      if (options?.immediateLocal) {
        flushTranslationDraftsToLocalCache();
      } else {
        translationDraftStorageTimeoutRef.current = setTimeout(() => {
          translationDraftStorageTimeoutRef.current = null;
          flushTranslationDraftsToLocalCache();
        }, localDelayMs);
      }
    }
    if (shouldPersistServer) {
      if (options?.immediateServer) {
        void flushTranslationDraftsToServer({ silent: true, keepalive: Boolean(options?.keepalive) });
      } else {
        translationDraftSyncTimeoutRef.current = setTimeout(() => {
          translationDraftSyncTimeoutRef.current = null;
          void flushTranslationDraftsToServer({ silent: true });
        }, serverDelayMs);
      }
    }
  }, [
    flushTranslationDraftsToLocalCache,
    flushTranslationDraftsToServer,
    isAndroidTelegramClient,
  ]);
  const clearTranslationDraftsOnServer = useCallback(async (sentenceIds = [], options = {}) => {
    if (!initData || !translationDraftScopeKey) return null;
    const clearAll = Boolean(options?.clearAll);
    const resolvedSentenceIds = Array.from(new Set(
      (sentenceIds || [])
        .map((item) => Number(item))
        .filter((item) => Number.isFinite(item) && item > 0)
    ));
    if (!clearAll && resolvedSentenceIds.length === 0) {
      return null;
    }
    try {
      const response = await fetch('/api/webapp/translation/drafts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          clear_all: clearAll,
          clear_sentence_ids: clearAll ? [] : resolvedSentenceIds,
        }),
        keepalive: Boolean(options?.keepalive),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return await response.json();
    } catch (error) {
      if (!options?.silent) {
        console.warn('Failed to clear translation drafts', error);
      }
      return null;
    }
  }, [initData, translationDraftScopeKey]);
  const persistYoutubeResumeState = useCallback((timeValue) => {
    const trimmed = String(youtubeInput || '').trim();
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
  }, [writeYoutubeResumeToLocalCache, youtubeId, youtubeInput]);
  const syncYoutubeResumeState = useCallback(async (timeValue, options = {}) => {
    const trimmed = String(youtubeInput || '').trim();
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
    } catch (_error) {
      // ignore sync errors; local cache already has the latest position
    }
  }, [initData, persistYoutubeResumeState, youtubeId, youtubeInput]);
  const normalizeSkillTrainingSnapshot = (value) => {
    if (!value || typeof value !== 'object') return null;
    const pack = value?.package && typeof value.package === 'object' ? value.package : null;
    const skillId = String(
      value?.skill?.skill_id
      || pack?.focus?.skill_id
      || value?.skill_id
      || ''
    ).trim();
    if (!skillId || !pack) return null;
    const answersRaw = Array.isArray(value?.answers) ? value.answers : [];
    const answers = Array.from({ length: 5 }, (_, index) => String(answersRaw[index] || ''));
    return {
      skill: value?.skill && typeof value.skill === 'object'
        ? {
          skill_id: skillId,
          title: String(value.skill.title || pack?.focus?.skill_name || '').trim(),
          mastery: value.skill.mastery ?? null,
        }
        : {
          skill_id: skillId,
          title: String(pack?.focus?.skill_name || '').trim(),
          mastery: null,
        },
      package: pack,
      video: value?.video && typeof value.video === 'object' ? value.video : null,
      answers,
      feedback: value?.feedback && typeof value.feedback === 'object' ? value.feedback : null,
      saved_at: String(value?.saved_at || '').trim() || new Date().toISOString(),
    };
  };
  const normalizeSkillTrainingDraftMap = (value) => {
    const payload = value && typeof value === 'object' ? value : {};
    const normalized = {};
    Object.entries(payload).forEach(([skillId, item]) => {
      const snapshot = normalizeSkillTrainingSnapshot(item);
      if (!snapshot) return;
      normalized[String(skillId).trim()] = snapshot;
    });
    return normalized;
  };

  const [uiLang, setUiLang] = useState('ru');
  const [themeMode, setThemeMode] = useState('dark');
  const t = useMemo(() => createTranslator(uiLang), [uiLang]);
  const tr = useCallback((ru, de) => (uiLang === 'de' ? de : ru), [uiLang]);
  const weeklySummaryMetricTitles = useMemo(() => ({
    translations: tr('переводы', 'Uebersetzungen'),
    learned_words: tr('слова и FSRS', 'Woerter und FSRS'),
    reading_minutes: tr('чтение', 'Lesen'),
    youtube_minutes: tr('видео и YouTube', 'Video und YouTube'),
    agent_minutes: tr('разговорная практика', 'Sprechpraxis'),
  }), [tr]);
  const weeklySummaryKpiMeta = useMemo(() => ([
    {
      key: 'translations',
      title: tr('Переводы предложений', 'Satz-Uebersetzungen'),
      unit: tr('шт', 'Stk'),
      digits: 0,
    },
    {
      key: 'learned_words',
      title: tr('Слова / FSRS', 'Woerter / FSRS'),
      unit: tr('слов', 'Woerter'),
      digits: 0,
    },
    {
      key: 'reading_minutes',
      title: tr('Чтение (мин)', 'Lesen (Min)'),
      unit: tr('мин', 'Min'),
      digits: 0,
    },
    {
      key: 'youtube_minutes',
      title: tr('Видео / YouTube (мин)', 'Video / YouTube (Min)'),
      unit: tr('мин', 'Min'),
      digits: 0,
    },
  ]), [tr]);
  const weeklySummaryVisitConfig = useMemo(() => buildWeeklySummaryVisitConfig({
    now: new Date(),
    locale: uiLang === 'de' ? 'de-AT' : 'ru-RU',
    labels: {
      mondayTitle: tr('Посмотри свои результаты за прошлую неделю', 'Schau dir deine Ergebnisse der letzten Woche an'),
      wednesdayTitle: tr('Промежуточные итоги недели', 'Zwischenstand der Woche'),
      fridayTitle: tr('Итоги недели на текущий момент', 'Dein Wochenstand bis jetzt'),
      mondayComparisonLabel: tr('Прошлая неделя vs неделя до неё', 'Letzte Woche vs Woche davor'),
      wednesdayComparisonLabel: tr('Пн-Вт этой недели vs Пн-Вт прошлой недели', 'Mo-Di dieser Woche vs Mo-Di der letzten Woche'),
      fridayComparisonLabel: tr('Пн-Чт этой недели vs Пн-Чт прошлой недели', 'Mo-Do dieser Woche vs Mo-Do der letzten Woche'),
    },
  }), [tr, uiLang]);
  const weeklySummaryDismissStorageKey = useMemo(() => {
    if (!weeklySummaryVisitConfig?.visitDateKey) return '';
    return `weekly_summary_modal_dismissed_${weeklySummaryStableUserId}_${weeklySummaryVisitConfig.visitDateKey}`;
  }, [weeklySummaryStableUserId, weeklySummaryVisitConfig]);
  const weeklySummaryHeroLines = useMemo(() => {
    if (!weeklySummaryHeroFacts) {
      return [];
    }
    const strongest = (weeklySummaryHeroFacts.strongestKeys || [])
      .map((key) => weeklySummaryMetricTitles[key])
      .filter(Boolean)
      .slice(0, 2);
    const weakest = (weeklySummaryHeroFacts.weakestKeys || [])
      .map((key) => weeklySummaryMetricTitles[key])
      .filter(Boolean)
      .slice(0, 2);
    const trendDelta = Number(weeklySummaryHeroFacts.trendDeltaPercent || 0);
    let trendLine = tr(
      'Сравнение с прошлым периодом появится после заполнения недельного плана.',
      'Der Vergleich erscheint, sobald dein Wochenplan gesetzt ist.'
    );
    if (weeklySummaryHeroFacts.hasPlan) {
      if (trendDelta > 0) {
        trendLine = tr(
          `По сравнению с прошлым периодом ты идёшь на ${Math.abs(trendDelta)}% лучше.`,
          `Im Vergleich zum letzten Zeitraum liegst du um ${Math.abs(trendDelta)}% besser.`
        );
      } else if (trendDelta < 0) {
        trendLine = tr(
          `По сравнению с прошлым периодом ты сейчас на ${Math.abs(trendDelta)}% ниже.`,
          `Im Vergleich zum letzten Zeitraum liegst du aktuell um ${Math.abs(trendDelta)}% niedriger.`
        );
      } else {
        trendLine = tr(
          'Ты идёшь примерно на уровне прошлого сравнимого периода.',
          'Du liegst ungefaehr auf dem Niveau des letzten Vergleichszeitraums.'
        );
      }
    }

    if (!weeklySummaryHeroFacts.hasPlan && !weeklySummaryHeroFacts.hasActivity) {
      return [
        tr(
          'Пока нет данных для weekly summary. Открой приложение позже, когда появится активность.',
          'Es gibt noch keine Daten fuer diese Weekly Summary. Oeffne die App spaeter erneut.'
        ),
      ];
    }

    return [
      weeklySummaryHeroFacts.hasPlan
        ? tr(
          `Ты выполнил ${weeklySummaryHeroFacts.planCompletedPercent}% недельного плана.`,
          `Du hast ${weeklySummaryHeroFacts.planCompletedPercent}% deines Wochenplans geschafft.`
        )
        : tr(
          'Недельный план ещё не задан, поэтому процент выполнения пока недоступен.',
          'Dein Wochenplan ist noch nicht gesetzt, deshalb gibt es noch keinen Erfuellungswert.'
        ),
      strongest.length
        ? tr(
          `Лучше всего шли: ${strongest.join(' и ')}.`,
          `Am staerksten liefen: ${strongest.join(' und ')}.`
        )
        : tr(
          'Сильные стороны появятся после первых действий за период.',
          'Staerken erscheinen nach den ersten Aktivitaeten in diesem Zeitraum.'
        ),
      weakest.length
        ? tr(
          `Просадка: ${weakest.join(' и ')}.`,
          `Rueckstand: ${weakest.join(' und ')}.`
        )
        : tr(
          'Слабые зоны определятся после появления прогресса.',
          'Schwaechere Bereiche erscheinen, sobald Fortschritt sichtbar ist.'
        ),
      trendLine,
    ];
  }, [tr, weeklySummaryHeroFacts, weeklySummaryMetricTitles]);
  const weeklySummaryRecommendation = useMemo(() => {
    if (!weeklySummaryCurrentMetrics || !weeklySummaryPreviousMetrics) {
      return null;
    }
    const entries = weeklySummaryKpiMeta.map((meta) => {
      const current = weeklySummaryCurrentMetrics?.[meta.key] && typeof weeklySummaryCurrentMetrics[meta.key] === 'object'
        ? weeklySummaryCurrentMetrics[meta.key]
        : {};
      const previous = weeklySummaryPreviousMetrics?.[meta.key] && typeof weeklySummaryPreviousMetrics[meta.key] === 'object'
        ? weeklySummaryPreviousMetrics[meta.key]
        : {};
      const actual = Math.max(0, Number(current.actual || 0));
      const goal = Math.max(0, Number(current.goal || 0));
      const delta = actual - Math.max(0, Number(previous.actual || 0));
      const completionPercent = goal > 0 ? Number(current.completion_percent || 0) : 0;
      const remaining = Math.max(0, goal - actual);
      return {
        key: meta.key,
        title: meta.title,
        actual,
        goal,
        delta,
        completionPercent,
        remaining,
      };
    });

    const improved = [...entries]
      .filter((item) => item.delta > 0)
      .sort((a, b) => b.delta - a.delta)
      .slice(0, 2);
    const lagging = [...entries]
      .filter((item) => item.goal > 0 || item.actual > 0)
      .sort((a, b) => {
        const aScore = a.goal > 0 ? a.completionPercent : a.delta;
        const bScore = b.goal > 0 ? b.completionPercent : b.delta;
        return aScore - bScore;
      })[0] || null;

    const improvedTitles = improved.map((item) => item.title);
    let nextAction = tr(
      'Продолжай в том же темпе и открой аналитику для деталей.',
      'Halte das Tempo und oeffne die Analytik fuer die Details.'
    );
    if (lagging) {
      if (lagging.key === 'translations') {
        const amount = Math.max(3, Math.min(10, Math.ceil(lagging.remaining || 3)));
        nextAction = tr(
          `Чтобы сократить отставание, сегодня лучше сделать ещё ${amount} переводов.`,
          `Um den Rueckstand zu verringern, mach heute am besten noch ${amount} Uebersetzungen.`
        );
      } else if (lagging.key === 'learned_words') {
        const amount = Math.max(5, Math.min(15, Math.ceil(lagging.remaining || 5)));
        nextAction = tr(
          `Чтобы выйти на план, сегодня лучше пройти ещё ${amount} слов в FSRS.`,
          `Um in den Plan zu kommen, geh heute am besten noch ${amount} FSRS-Woerter durch.`
        );
      } else if (lagging.key === 'reading_minutes') {
        const amount = Math.max(10, Math.min(25, Math.ceil(lagging.remaining || 10)));
        nextAction = tr(
          `Чтобы подтянуть чтение, сегодня лучше добавить ещё ${amount} минут.`,
          `Um Lesen aufzuholen, fuege heute am besten noch ${amount} Minuten hinzu.`
        );
      } else if (lagging.key === 'youtube_minutes') {
        const amount = Math.max(5, Math.min(15, Math.ceil(lagging.remaining || 5)));
        nextAction = tr(
          `Чтобы добрать видео-блок, сегодня лучше посмотреть ещё ${amount} минут или 1 короткое видео.`,
          `Um den Video-Block aufzuholen, schau heute am besten noch ${amount} Minuten oder 1 kurzes Video.`
        );
      }
    }

    return {
      leadLine: improvedTitles.length
        ? tr(
          `Ты идёшь лучше прошлого периода по ${improvedTitles.join(' и ')}.`,
          `Du liegst besser als im letzten Zeitraum bei ${improvedTitles.join(' und ')}.`
        )
        : tr(
          'Пока нет явного роста против прошлого периода, но базовый темп уже виден.',
          'Es gibt noch keinen klaren Vorsprung zum letzten Zeitraum, aber das Grundtempo ist schon sichtbar.'
        ),
      lagLine: lagging
        ? tr(
          `${lagging.title} сейчас отстаёт сильнее всего.`,
          `${lagging.title} liegt aktuell am meisten zurueck.`
        )
        : tr(
          'Явной просадки пока нет.',
          'Es gibt aktuell keinen klaren Rueckstand.'
        ),
      nextLine: nextAction,
    };
  }, [tr, weeklySummaryCurrentMetrics, weeklySummaryKpiMeta, weeklySummaryPreviousMetrics]);
  const weeklySummaryKpiCards = useMemo(() => {
    return weeklySummaryKpiMeta.map((meta) => {
      const current = weeklySummaryCurrentMetrics?.[meta.key] && typeof weeklySummaryCurrentMetrics[meta.key] === 'object'
        ? weeklySummaryCurrentMetrics[meta.key]
        : {};
      const previous = weeklySummaryPreviousMetrics?.[meta.key] && typeof weeklySummaryPreviousMetrics[meta.key] === 'object'
        ? weeklySummaryPreviousMetrics[meta.key]
        : {};
      const actual = Number(current.actual || 0);
      const delta = actual - Number(previous.actual || 0);
      const formatValue = (value) => {
        const normalized = Number(value || 0);
        if (!Number.isFinite(normalized)) return '0';
        return meta.digits > 0
          ? normalized.toFixed(meta.digits)
          : String(Math.round(normalized));
      };
      const deltaLabel = delta > 0
        ? `+${formatValue(delta)}`
        : delta < 0
          ? `-${formatValue(Math.abs(delta))}`
          : `0`;
      return {
        ...meta,
        actualLabel: formatValue(actual),
        previousLabel: formatValue(Number(previous.actual || 0)),
        deltaLabel,
        deltaClass: delta > 0 ? 'is-positive' : delta < 0 ? 'is-negative' : 'is-neutral',
      };
    });
  }, [weeklySummaryCurrentMetrics, weeklySummaryKpiMeta, weeklySummaryPreviousMetrics]);
  const weeklySummaryComparisonRows = useMemo(() => {
    return weeklySummaryKpiMeta.map((meta) => {
      const current = weeklySummaryCurrentMetrics?.[meta.key] && typeof weeklySummaryCurrentMetrics[meta.key] === 'object'
        ? weeklySummaryCurrentMetrics[meta.key]
        : {};
      const previous = weeklySummaryPreviousMetrics?.[meta.key] && typeof weeklySummaryPreviousMetrics[meta.key] === 'object'
        ? weeklySummaryPreviousMetrics[meta.key]
        : {};
      const currentValue = Math.max(0, Number(current.actual || 0));
      const previousValue = Math.max(0, Number(previous.actual || 0));
      const scaleMax = Math.max(currentValue, previousValue, 1);
      const formatValue = (value) => {
        const normalized = Number(value || 0);
        if (!Number.isFinite(normalized)) return '0';
        return meta.digits > 0
          ? normalized.toFixed(meta.digits)
          : String(Math.round(normalized));
      };
      return {
        key: meta.key,
        title: meta.title,
        unit: meta.unit,
        currentLabel: formatValue(currentValue),
        previousLabel: formatValue(previousValue),
        currentWidth: currentValue > 0 ? `${Math.max(6, Math.round((currentValue / scaleMax) * 100))}%` : '0%',
        previousWidth: previousValue > 0 ? `${Math.max(6, Math.round((previousValue / scaleMax) * 100))}%` : '0%',
      };
    });
  }, [weeklySummaryCurrentMetrics, weeklySummaryKpiMeta, weeklySummaryPreviousMetrics]);
  const forceSingleInstanceTakeover = useCallback(() => {
    if (!isWebAppMode) return;
    const now = Date.now();
    safeStorageSet(SINGLE_INSTANCE_LOCK_KEY, JSON.stringify({
      owner: singleInstanceTokenRef.current,
      lastSeen: now,
      openedAt: now,
      href: window.location.href,
    }));
    singleInstanceOwnsLockRef.current = true;
    setSingleInstanceBlocked(false);
  }, [isWebAppMode]);
  useEffect(() => {
    if (!isWebAppMode) {
      setSingleInstanceBlocked(false);
      singleInstanceOwnsLockRef.current = false;
      if (singleInstanceHeartbeatRef.current) {
        clearInterval(singleInstanceHeartbeatRef.current);
        singleInstanceHeartbeatRef.current = null;
      }
      return undefined;
    }

    const parseLock = () => {
      const raw = safeStorageGet(SINGLE_INSTANCE_LOCK_KEY);
      if (!raw) return null;
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') return null;
        return {
          owner: String(parsed.owner || '').trim(),
          lastSeen: Number(parsed.lastSeen || 0),
        };
      } catch (_error) {
        return null;
      }
    };

    const isFreshLock = (lock) => {
      if (!lock?.owner) return false;
      if (!Number.isFinite(lock?.lastSeen)) return false;
      return (Date.now() - Number(lock.lastSeen)) <= SINGLE_INSTANCE_STALE_MS;
    };

    const writeOwnLock = () => {
      const now = Date.now();
      safeStorageSet(SINGLE_INSTANCE_LOCK_KEY, JSON.stringify({
        owner: singleInstanceTokenRef.current,
        lastSeen: now,
        openedAt: now,
        href: window.location.href,
      }));
    };

    const releaseOwnLock = () => {
      const lock = parseLock();
      if (lock?.owner === singleInstanceTokenRef.current) {
        safeStorageRemove(SINGLE_INSTANCE_LOCK_KEY);
      }
      singleInstanceOwnsLockRef.current = false;
    };

    const acquireOrBlock = (force = false) => {
      const lock = parseLock();
      const occupiedByOther = Boolean(
        lock?.owner
        && lock.owner !== singleInstanceTokenRef.current
        && isFreshLock(lock)
      );
      if (!force && occupiedByOther) {
        singleInstanceOwnsLockRef.current = false;
        setSingleInstanceBlocked(true);
        return false;
      }
      writeOwnLock();
      singleInstanceOwnsLockRef.current = true;
      setSingleInstanceBlocked(false);
      return true;
    };

    const heartbeat = () => {
      const lock = parseLock();
      if (singleInstanceOwnsLockRef.current) {
        const otherFresh = Boolean(
          lock?.owner
          && lock.owner !== singleInstanceTokenRef.current
          && isFreshLock(lock)
        );
        if (otherFresh) {
          singleInstanceOwnsLockRef.current = false;
          setSingleInstanceBlocked(true);
          return;
        }
        writeOwnLock();
        return;
      }
      if (!lock || !isFreshLock(lock)) {
        acquireOrBlock(false);
      } else if (lock.owner !== singleInstanceTokenRef.current) {
        setSingleInstanceBlocked(true);
      }
    };

    const onStorage = (event) => {
      if (event.key !== SINGLE_INSTANCE_LOCK_KEY) return;
      heartbeat();
    };

    const onVisible = () => {
      if (document.visibilityState !== 'visible') return;
      heartbeat();
    };

    const onPageHide = () => {
      releaseOwnLock();
    };

    const onBeforeUnload = () => {
      releaseOwnLock();
    };

    acquireOrBlock(false);
    singleInstanceHeartbeatRef.current = window.setInterval(heartbeat, SINGLE_INSTANCE_HEARTBEAT_MS);
    window.addEventListener('storage', onStorage);
    document.addEventListener('visibilitychange', onVisible);
    window.addEventListener('pagehide', onPageHide);
    window.addEventListener('beforeunload', onBeforeUnload);

    return () => {
      if (singleInstanceHeartbeatRef.current) {
        clearInterval(singleInstanceHeartbeatRef.current);
        singleInstanceHeartbeatRef.current = null;
      }
      window.removeEventListener('storage', onStorage);
      document.removeEventListener('visibilitychange', onVisible);
      window.removeEventListener('pagehide', onPageHide);
      window.removeEventListener('beforeunload', onBeforeUnload);
      releaseOwnLock();
    };
  }, [isWebAppMode]);
  const billingReturnMessage = useMemo(() => {
    if (billingReturnContext.kind === 'success') {
      return tr('Оплата прошла успешно. Проверяю подписку и обновляю статус.', 'Zahlung erfolgreich. Ich pruefe jetzt dein Abo und aktualisiere den Status.');
    }
    if (billingReturnContext.kind === 'cancel') {
      return tr('Оплата была отменена. Ты можешь попробовать еще раз в этом разделе.', 'Die Zahlung wurde abgebrochen. Du kannst es in diesem Bereich erneut versuchen.');
    }
    if (billingReturnContext.kind === 'portal') {
      return tr('Возврат из Stripe Portal. Обновляю текущий статус подписки.', 'Rueckkehr aus dem Stripe-Portal. Ich aktualisiere den aktuellen Abo-Status.');
    }
    return '';
  }, [billingReturnContext.kind, tr]);
  const billingPlanMeta = useMemo(() => ({
    free: {
      eyebrow: tr('Базовый', 'Basis'),
      title: tr('Free', 'Free'),
      blurb: tr('Базовый бесплатный план.', 'Basisplan ohne Kosten.'),
      priceLabel: '0 EUR',
      priceLabelDe: '0 EUR',
    },
    pro: {
      eyebrow: tr('Текущий флагман', 'Aktuelles Flaggschiff'),
      title: tr('Pro', 'Pro'),
      blurb: tr('Полный доступ без дневного лимита.', 'Voller Zugang ohne Tageslimit.'),
      priceLabel: '3.50 EUR / месяц',
      priceLabelDe: '3.50 EUR / Monat',
    },
    support_coffee: {
      eyebrow: tr('Лёгкая поддержка', 'Leichte Unterstuetzung'),
      title: tr('Поддержать разработчика: кофе ☕️', 'Entwickler unterstuetzen: Kaffee ☕️'),
      blurb: tr(
        'Я делал это приложение 1 год и 3 месяца и продолжаю улучшать его каждый день.',
        'Ich habe diese App 1 Jahr und 3 Monate lang gebaut und verbessere sie weiterhin jeden Tag.'
      ),
      priceLabel: '3.50 EUR / месяц',
      priceLabelDe: '3.50 EUR / Monat',
    },
    support_cheesecake: {
      eyebrow: tr('Расширенная поддержка', 'Erweiterte Unterstuetzung'),
      title: tr('Поддержать разработчика: кофе ☕️ и чизкейк 🍰', 'Entwickler unterstuetzen: Kaffee ☕️ und Cheesecake 🍰'),
      blurb: tr(
        'Если хочешь поддержать проект сильнее, этот тариф помогает оплачивать развитие и инфраструктуру.',
        'Wenn du das Projekt staerker unterstuetzen willst, hilft dieser Tarif bei Weiterentwicklung und Infrastruktur.'
      ),
      priceLabel: '4.99 EUR / месяц',
      priceLabelDe: '4.99 EUR / Monat',
    },
  }), [tr]);
  const billingPlanCards = useMemo(() => {
    const order = {
      free: 10,
      pro: 20,
      support_coffee: 30,
      support_cheesecake: 40,
    };
    const rows = Array.isArray(billingPlans) ? billingPlans : [];
    return rows
      .filter((item) => item && String(item.plan_code || '').trim())
      .filter((item) => item.is_active !== false)
      .map((item) => {
        const planCode = String(item.plan_code || '').trim().toLowerCase();
        const meta = billingPlanMeta[planCode] || {};
        return {
          ...item,
          planCode,
          title: String(meta.title || item.name || planCode).trim(),
          eyebrow: String(meta.eyebrow || tr('Тариф', 'Tarif')).trim(),
          blurb: String(meta.blurb || '').trim(),
          priceLabel: uiLang === 'de'
            ? String(meta.priceLabelDe || meta.priceLabel || '').trim()
            : String(meta.priceLabel || '').trim(),
          sortOrder: Number.isFinite(order[planCode]) ? order[planCode] : 500,
        };
      })
      .sort((a, b) => {
        if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
        return String(a.planCode || '').localeCompare(String(b.planCode || ''));
      });
  }, [billingPlans, billingPlanMeta, uiLang, tr]);
  const billingPlanLimitDetails = useMemo(() => {
    const paidCommon = [
      tr('Переводы: безлимитно.', 'Uebersetzungen: unbegrenzt.'),
      tr('Читалка: безлимитно.', 'Reader: unbegrenzt.'),
      tr('Скачивание аудио из читалки: до 10 страниц за 7 дней.', 'Audio-Export aus dem Reader: bis zu 10 Seiten in 7 Tagen.'),
      tr('Карточки: безлимитно.', 'Karteikarten: unbegrenzt.'),
      tr('«Почувствуй слово»: безлимитно.', '„Wort fuehlen“: unbegrenzt.'),
      tr('Разговорная практика: 15 минут в день.', 'Sprechpraxis: 15 Minuten pro Tag.'),
      tr('Прокачка навыков: безлимитно.', 'Skill-Training: unbegrenzt.'),
      tr(
        'По запросу можно обсудить индивидуальную доработку/персональную тренировку для этого пользователя (по технической возможности).',
        'Auf Wunsch kann eine individuelle technische Anpassung/persoenliches Training besprochen werden (sofern technisch machbar).'
      ),
    ];
    return {
      free: {
        title: tr('Лимиты тарифа Free', 'Limits des Free-Tarifs'),
        items: [
          tr('Переводы: безлимитно.', 'Uebersetzungen: unbegrenzt.'),
          tr('Читалка: 1 книга/документ в архиве (период хранения до 30 дней).', 'Reader: 1 Buch/Dokument im Archiv (Speicherzeit bis 30 Tage).'),
          tr('Чтобы добавить новую книгу, нужно удалить предыдущую.', 'Um ein neues Buch hinzuzufuegen, muss das vorherige geloescht werden.'),
          tr('Скачивание аудио из читалки: недоступно.', 'Audio-Export aus dem Reader: nicht verfuegbar.'),
          tr('Карточки: в каждом виде тренировки по 5 слов в день.', 'Karteikarten: in jedem Trainingsmodus 5 Woerter pro Tag.'),
          tr('«Почувствуй слово»: до 3 раз в день (общий лимит).', '„Wort fuehlen“: bis zu 3-mal pro Tag (globales Limit).'),
          tr('Разговорная практика: 3 минуты в день.', 'Sprechpraxis: 3 Minuten pro Tag.'),
          tr('Прокачка навыков: 1 навык.', 'Skill-Training: 1 Skill.'),
        ],
      },
      pro: {
        title: tr('Лимиты тарифа Pro', 'Limits des Pro-Tarifs'),
        items: paidCommon,
      },
      support_coffee: {
        title: tr('Лимиты тарифа Coffee', 'Limits des Coffee-Tarifs'),
        items: paidCommon,
      },
      support_cheesecake: {
        title: tr('Лимиты тарифа Cheesecake', 'Limits des Cheesecake-Tarifs'),
        items: paidCommon,
      },
    };
  }, [tr]);
  const activeBillingPlanDetails = useMemo(() => {
    if (!billingPlanDetailsOpenFor) return null;
    return billingPlanLimitDetails[billingPlanDetailsOpenFor] || {
      title: tr('Лимиты тарифа', 'Tariflimits'),
      items: [tr('Лимиты для этого тарифа обновляются. Попробуйте позже.', 'Die Limits fuer diesen Tarif werden aktualisiert. Bitte spaeter erneut pruefen.')],
    };
  }, [billingPlanDetailsOpenFor, billingPlanLimitDetails, tr]);
  const readApiError = useCallback(async (response, fallbackRu, fallbackDe) => {
    const fallback = tr(fallbackRu, fallbackDe);
    const formatBillingLimitError = (payload) => {
      if (!payload || typeof payload !== 'object') return '';
      const errorCode = String(payload.error || '').trim();
      if (errorCode === 'cost_cap_exceeded') {
        const spent = Number(payload.spent_eur || 0);
        const cap = Number(payload.cap_eur || 0);
        const resetAt = String(payload.reset_at || '');
        return tr(
          `Дневной лимит расходов исчерпан: ${spent.toFixed(2)} / ${cap.toFixed(2)} EUR. Сброс: ${resetAt}`,
          `Taegliches Kostenlimit erreicht: ${spent.toFixed(2)} / ${cap.toFixed(2)} EUR. Reset: ${resetAt}`
        );
      }
      if (errorCode === 'feature_limit_exceeded') {
        const feature = String(payload.feature || '').trim();
        const used = Number(payload.used || 0);
        const limit = Number(payload.limit || 0);
        const unit = String(payload.unit || '').trim();
        const resetAt = String(payload.reset_at || '');
        return tr(
          `Лимит функции исчерпан (${feature}): ${used} / ${limit} ${unit}. Сброс: ${resetAt}`,
          `Funktionslimit erreicht (${feature}): ${used} / ${limit} ${unit}. Reset: ${resetAt}`
        );
      }
      return '';
    };
    try {
      const raw = await response.text();
      if (!raw) return `${fallback} (HTTP ${response.status})`;
      try {
        const parsed = JSON.parse(raw);
        const billingLimitMessage = formatBillingLimitError(parsed);
        if (billingLimitMessage) return billingLimitMessage;
        const message = String(parsed?.error || parsed?.message || '').trim();
        return message || `${fallback} (HTTP ${response.status})`;
      } catch (_jsonError) {
        const compact = String(raw).replace(/\s+/g, ' ').trim();
        return compact || `${fallback} (HTTP ${response.status})`;
      }
    } catch (_readError) {
      return `${fallback} (HTTP ${response.status})`;
    }
  }, [tr]);
  const normalizeNetworkErrorMessage = useCallback((error, fallbackRu, fallbackDe) => {
    const fallback = tr(fallbackRu, fallbackDe);
    const name = String(error?.name || '').trim().toLowerCase();
    const raw = String(error?.message || '').trim().toLowerCase();
    if (!raw) return fallback;
    if (
      name === 'aborterror'
      || name === 'timeouterror'
      || raw.includes('aborted')
      || raw.includes('timeout')
      || raw.includes('timed out')
    ) {
      return tr(
        'Сервер отвечает слишком долго. Попробуйте ещё раз.',
        'Der Server antwortet zu langsam. Bitte erneut versuchen.'
      );
    }
    if (
      raw.includes('application failed to respond')
      || raw.includes('upstream request timeout')
      || raw.includes('gateway timeout')
      || raw.includes('service unavailable')
    ) {
      return tr(
        'Сервер перегружен или временно недоступен. Повторите через несколько секунд.',
        'Der Server ist ueberlastet oder voruebergehend nicht verfuegbar. Bitte in ein paar Sekunden erneut versuchen.'
      );
    }
    if (
      raw.includes('load failed')
      || raw.includes('failed to fetch')
      || raw.includes('networkerror')
      || raw.includes('network request failed')
    ) {
      return tr(
        'Сетевой сбой. Проверьте интернет и повторите.',
        'Netzwerkfehler. Bitte Internet pruefen und erneut versuchen.'
      );
    }
    return String(error?.message || fallback);
  }, [tr]);
  const formatSupportTime = (isoValue) => {
    if (!isoValue) return '';
    const timestamp = Date.parse(String(isoValue));
    if (!Number.isFinite(timestamp)) return '';
    const locale = uiLang === 'de' ? 'de-DE' : 'ru-RU';
    try {
      return new Date(timestamp).toLocaleTimeString(locale, { hour: '2-digit', minute: '2-digit' });
    } catch (_error) {
      return '';
    }
  };
  const initDataMissingMsg = tr(
    'initData не найдено. Откройте Web App внутри Telegram.',
    'initData nicht gefunden. Oeffne die Web App in Telegram.'
  );
  const postSupportApi = useCallback(async (path, body = {}) => {
    if (!initData) {
      throw new Error(initDataMissingMsg);
    }
    const response = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ initData, ...body }),
    });
    if (!response.ok) {
      let message = '';
      try {
        const payload = await response.json();
        message = String(payload?.error || payload?.message || '').trim();
      } catch (_jsonError) {
        try {
          message = (await response.text()).trim();
        } catch (_readError) {
          message = '';
        }
      }
      throw new Error(message || tr('Ошибка техподдержки', 'Support-Fehler'));
    }
    return response.json();
  }, [initData, initDataMissingMsg, tr]);
  const fetchWithTimeout = useCallback(async (url, options = {}, timeoutMs = 15000) => {
    const method = String(options?.method || 'GET').trim().toUpperCase();
    const maxAttempts = method === 'GET' || method === 'HEAD' ? 2 : 1;
    const isTelegramWebApp = Boolean(window.Telegram?.WebApp);
    const baseTimeout = Math.max(1000, Number(timeoutMs || 15000));
    const effectiveTimeout = isTelegramWebApp ? Math.max(baseTimeout, 30000) : baseTimeout;
    let lastError;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const controller = new AbortController();
      let didTimeout = false;
      const timerId = window.setTimeout(() => {
        didTimeout = true;
        controller.abort();
      }, effectiveTimeout);
      try {
        return await fetch(url, { ...options, signal: controller.signal });
      } catch (error) {
        if (didTimeout) {
          const timeoutError = new Error('Request timed out');
          timeoutError.name = 'TimeoutError';
          lastError = timeoutError;
        } else {
          lastError = error;
        }
      } finally {
        window.clearTimeout(timerId);
      }
      if (attempt < maxAttempts) {
        await new Promise((resolve) => window.setTimeout(resolve, 250));
      }
    }
    throw lastError || new Error('Request failed');
  }, []);
  const fetchGetWithRetry = useCallback(async (url, timeoutMs = 45000) => {
    let response = await fetchWithTimeout(url, {}, timeoutMs);
    if (!response.ok && response.status >= 500) {
      await new Promise((resolve) => window.setTimeout(resolve, 280));
      response = await fetchWithTimeout(url, {}, timeoutMs);
    }
    return response;
  }, [fetchWithTimeout]);
  const learningLanguageOptions = [
    { value: 'de', label: tr('Немецкий', 'Deutsch') },
    { value: 'en', label: tr('Английский', 'Englisch') },
    { value: 'es', label: tr('Испанский', 'Spanisch') },
    { value: 'it', label: tr('Итальянский', 'Italienisch') },
  ];
  const nativeLanguageOptions = [
    { value: 'ru', label: tr('Русский', 'Russisch') },
    { value: 'en', label: tr('Английский', 'Englisch') },
    { value: 'de', label: tr('Немецкий', 'Deutsch') },
  ];
  const needsLanguageProfileChoice = Boolean(isWebAppMode && initData && !languageProfileLoading && !languageProfile?.has_profile);
  const languageProfileGateOpen = needsLanguageProfileChoice || languageProfileModalOpen;
  const solvedTodayStorageKeyByMode = useMemo(() => {
    const uid = webappUser?.id ? String(webappUser.id) : 'anon';
    const dateKey = getLocalDateKey();
    return {
      blocks: `cards_solved_today_core_${uid}_${dateKey}`,
      quiz: `cards_solved_today_core_${uid}_${dateKey}`,
      sentence: `cards_solved_today_sentence_${uid}_${dateKey}`,
    };
  }, [webappUser?.id]);
  const stableWebappUserId = useMemo(() => {
    const fromWebappUser = String(webappUser?.id || '').trim();
    if (fromWebappUser) return fromWebappUser;
    const fromInitData = getInitDataUserId(initData);
    if (fromInitData) return fromInitData;
    const fromTelegramUnsafe = String(telegramApp?.initDataUnsafe?.user?.id || '').trim();
    if (fromTelegramUnsafe) return fromTelegramUnsafe;
    return 'anon';
  }, [initData, telegramApp, webappUser?.id]);
  const canViewEconomics = stableWebappUserId === '117649764';
  const skillTrainingStorageKey = useMemo(() => {
    return `skill_training_sessions_${stableWebappUserId}_${getLocalDateKey()}`;
  }, [stableWebappUserId]);
  const skillTrainingLegacyStorageKeys = useMemo(() => {
    const dateKey = getLocalDateKey();
    const candidates = [
      stableWebappUserId,
      String(webappUser?.id || '').trim(),
      getInitDataUserId(initData),
      String(telegramApp?.initDataUnsafe?.user?.id || '').trim(),
      'anon',
    ];
    return Array.from(new Set(
      candidates
        .map((item) => String(item || '').trim())
        .filter(Boolean)
        .map((item) => `skill_training_sessions_${item}_${dateKey}`)
    ));
  }, [initData, stableWebappUserId, telegramApp, webappUser?.id]);
  const skillTrainingDraftMapRef = useRef({});

  useEffect(() => {
    skillTrainingDraftMapRef.current = skillTrainingDraftMap;
  }, [skillTrainingDraftMap]);

  const persistSkillTrainingDraftMap = useCallback((nextMap) => {
    const normalized = normalizeSkillTrainingDraftMap(nextMap);
    skillTrainingDraftMapRef.current = normalized;
    setSkillTrainingDraftMap(normalized);
    if (Object.keys(normalized).length > 0) {
      safeStorageSet(skillTrainingStorageKey, JSON.stringify(normalized));
    } else {
      safeStorageRemove(skillTrainingStorageKey);
    }
  }, [skillTrainingStorageKey]);

  const removeSkillTrainingSnapshot = useCallback((skillId) => {
    const normalizedSkillId = String(skillId || '').trim();
    if (!normalizedSkillId) return;
    const current = skillTrainingDraftMapRef.current || {};
    if (!current[normalizedSkillId]) return;
    const next = { ...current };
    delete next[normalizedSkillId];
    persistSkillTrainingDraftMap(next);
  }, [persistSkillTrainingDraftMap]);

  const getStoredSkillTrainingSnapshot = useCallback((skillId) => {
    const normalizedSkillId = String(skillId || '').trim();
    if (!normalizedSkillId) return null;
    return normalizeSkillTrainingSnapshot((skillTrainingDraftMapRef.current || {})[normalizedSkillId]);
  }, []);

  const readSolvedTodaySet = useCallback((mode) => {
    const modeKey = String(mode || '').toLowerCase();
    const storageKey = solvedTodayStorageKeyByMode[modeKey];
    if (!storageKey) return new Set();
    try {
      const raw = safeStorageGet(storageKey);
      if (!raw) return new Set();
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return new Set();
      return new Set(parsed.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0));
    } catch (_error) {
      return new Set();
    }
  }, [solvedTodayStorageKeyByMode]);

  const markSolvedTodayByMode = useCallback((mode, entryId) => {
    const modeKey = String(mode || '').toLowerCase();
    const storageKey = solvedTodayStorageKeyByMode[modeKey];
    if (!storageKey) return;
    const id = Number(entryId);
    if (!Number.isFinite(id) || id <= 0) return;
    const next = readSolvedTodaySet(modeKey);
    next.add(id);
    safeStorageSet(storageKey, JSON.stringify(Array.from(next)));
  }, [readSolvedTodaySet, solvedTodayStorageKeyByMode]);

  const toggleLanguage = () => {
    setUiLang((prev) => (prev === 'ru' ? 'de' : 'ru'));
  };

  const toggleThemeMode = () => {
    setThemeMode((prev) => (prev === 'light' ? 'dark' : 'light'));
  };

  useEffect(() => {
    const stored = safeStorageGet('ui_lang');
    if (stored) {
      setUiLang(normalizeLanguage(stored));
      return;
    }
    const nextLang = getPreferredLanguage(telegramApp);
    setUiLang(nextLang);
  }, [telegramApp]);

  useEffect(() => {
    safeStorageSet('ui_lang', uiLang);
  }, [uiLang]);

  useEffect(() => {
    const stored = String(safeStorageGet('ui_theme_mode') || '').trim().toLowerCase();
    if (stored === 'light' || stored === 'dark') {
      setThemeMode(stored);
    }
  }, []);

  useEffect(() => {
    safeStorageSet('ui_theme_mode', themeMode);
  }, [themeMode]);

  useEffect(() => {
    const stored = safeStorageGet(guideQuickCardStorageKey);
    setGuideQuickCardDismissed(stored === '1');
  }, [guideQuickCardStorageKey]);

  useEffect(() => {
    if (!initData) return;
    if (needsLanguageProfileChoice || languageProfileGateOpen) return;
    const seen = safeStorageGet(onboardingSeenStorageKey) === '1';
    if (!seen) {
      setOnboardingStep(0);
      setOnboardingOpen(true);
    }
  }, [initData, needsLanguageProfileChoice, languageProfileGateOpen, onboardingSeenStorageKey]);

  useEffect(() => {
    if (!isWebAppMode || !initData || flashcardsOnly || onboardingOpen) {
      setWeeklySummaryModalOpen(false);
      return;
    }
    if (!weeklySummaryVisitConfig || !weeklySummaryDismissStorageKey) {
      setWeeklySummaryModalOpen(false);
      return;
    }
    const dismissed = safeStorageGet(weeklySummaryDismissStorageKey) === '1';
    setWeeklySummaryModalOpen(!dismissed);
  }, [
    flashcardsOnly,
    initData,
    isWebAppMode,
    onboardingOpen,
    weeklySummaryDismissStorageKey,
    weeklySummaryVisitConfig,
  ]);

  useEffect(() => {
    const refreshMode = () => {
      const nextMode = detectAppMode();
      setAppMode(nextMode);
      if (import.meta.env.DEV) {
        console.log('[app-mode]', nextMode);
      }
    };
    refreshMode();
    const media = typeof window.matchMedia === 'function'
      ? window.matchMedia('(display-mode: standalone)')
      : null;
    const onChange = () => refreshMode();
    if (media && typeof media.addEventListener === 'function') {
      media.addEventListener('change', onChange);
    } else if (media && typeof media.addListener === 'function') {
      media.addListener(onChange);
    }
    window.addEventListener('focus', refreshMode);
    return () => {
      if (media && typeof media.removeEventListener === 'function') {
        media.removeEventListener('change', onChange);
      } else if (media && typeof media.removeListener === 'function') {
        media.removeListener(onChange);
      }
      window.removeEventListener('focus', refreshMode);
    };
  }, []);

  const handleBrowserLogout = () => {
    safeStorageRemove('browser_init_data');
    setInitData('');
    setWebappUser(null);
    setWebappChatType('');
    setBrowserAuthError('');
    setWebappError('');
  };

  const handleBrowserTelegramAuth = async (authUser) => {
    if (!authUser || typeof authUser !== 'object') {
      setBrowserAuthError(tr('Не удалось получить данные Telegram Login.', 'Telegram-Login-Daten konnten nicht gelesen werden.'));
      return;
    }
    try {
      setBrowserAuthLoading(true);
      setBrowserAuthError('');
      const response = await fetch('/api/web/auth/telegram', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(authUser),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      if (!data?.initData) {
        throw new Error(tr('Сервер не вернул initData', 'Server hat initData nicht zurueckgegeben'));
      }
      setInitData(data.initData);
      safeStorageSet('browser_init_data', data.initData);
      if (data.user) {
        setWebappUser(data.user);
      }
      setWebappChatType(data.chat_type || 'browser');
    } catch (error) {
      setBrowserAuthError(`${tr('Ошибка входа', 'Login-Fehler')}: ${error.message}`);
    } finally {
      setBrowserAuthLoading(false);
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

  const fetchTtsUrlStatus = useCallback(async (text, language = 'de-DE', voice = '') => {
    const normalizedText = String(text || '').trim();
    if (!initData || !normalizedText) return null;
    const params = new URLSearchParams();
    params.set('text', normalizedText);
    params.set('language', String(language || 'de-DE').trim() || 'de-DE');
    const normalizedVoice = String(voice || '').trim();
    if (normalizedVoice) params.set('voice', normalizedVoice);
    const response = await fetch(`/api/webapp/tts/url?${params.toString()}`, {
      method: 'GET',
      headers: {
        'X-Telegram-InitData': initData,
      },
    });
    if (!response.ok) {
      throw new Error(await readApiError(response, 'Ошибка запроса статуса TTS', 'Fehler beim TTS-Status'));
    }
    return (await response.json()) || null;
  }, [initData, readApiError]);

  const requestTtsGenerate = useCallback(async (text, language = 'de-DE', voice = '') => {
    const normalizedText = String(text || '').trim();
    if (!initData || !normalizedText) return null;
    const body = {
      initData,
      text: normalizedText,
      language: String(language || 'de-DE').trim() || 'de-DE',
    };
    const normalizedVoice = String(voice || '').trim();
    if (normalizedVoice) {
      body.voice = normalizedVoice;
    }
    const response = await fetch('/api/webapp/tts/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(await readApiError(response, 'Ошибка генерации TTS', 'Fehler bei der TTS-Generierung'));
    }
    return (await response.json()) || null;
  }, [initData, readApiError]);

  const stopTtsPlayback = useCallback((options = {}) => {
    const invalidatePending = options?.invalidatePending !== false;
    if (invalidatePending) {
      ttsPlaybackSeqRef.current += 1;
    }
    if (ttsCurrentAudioRef.current) {
      ttsCurrentAudioRef.current.pause();
      ttsCurrentAudioRef.current.currentTime = 0;
      ttsCurrentAudioRef.current = null;
    }
    if ('speechSynthesis' in window) {
      try {
        window.speechSynthesis.cancel();
      } catch (_error) {
        // no-op
      }
    }
  }, []);

  const getTtsCacheValue = useCallback((key) => {
    const normalizedKey = String(key || '').trim();
    if (!normalizedKey) return null;
    const cache = ttsCacheRef.current;
    if (!cache.has(normalizedKey)) return null;
    const cachedValue = cache.get(normalizedKey);
    cache.delete(normalizedKey);
    cache.set(normalizedKey, cachedValue);
    return cachedValue || null;
  }, []);

  const setTtsCacheValue = useCallback((key, audioUrl) => {
    const normalizedKey = String(key || '').trim();
    const normalizedUrl = String(audioUrl || '').trim();
    if (!normalizedKey || !normalizedUrl) return;
    const cache = ttsCacheRef.current;
    if (cache.has(normalizedKey)) {
      cache.delete(normalizedKey);
    }
    cache.set(normalizedKey, normalizedUrl);
    while (cache.size > TTS_CACHE_MAX_ENTRIES) {
      const oldestKey = cache.keys().next().value;
      if (oldestKey === undefined) break;
      cache.delete(oldestKey);
    }
  }, []);

  const playTts = useCallback(async (text, language = 'de-DE', voice = '') => {
    if (!initData || !text) return Promise.resolve();
    const normalizedText = String(text || '').trim();
    if (!normalizedText) return Promise.resolve();
    const normalizedLang = String(language || 'de-DE').trim() || 'de-DE';
    const normalizedVoice = String(voice || '').trim();
    const key = `${normalizedLang}:${normalizedVoice}:${normalizedText}`;
    const now = Date.now();
    if (ttsLastRef.current.key === key && now - ttsLastRef.current.ts < 1200) {
      return Promise.resolve();
    }
    const playbackSeq = ++ttsPlaybackSeqRef.current;
    const isStalePlayback = () => ttsPlaybackSeqRef.current !== playbackSeq;
    ttsLastRef.current = { key, ts: now };
    stopTtsPlayback({ invalidatePending: false });
    const playAudioUrl = (audioUrl) => new Promise((resolve) => {
      if (!audioUrl || isStalePlayback()) {
        resolve();
        return;
      }
      const audio = new Audio(audioUrl);
      audio.preload = 'auto';
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        if (ttsCurrentAudioRef.current === audio) {
          ttsCurrentAudioRef.current = null;
        }
        resolve();
      };
      audio.onended = finish;
      audio.onerror = finish;
      ttsCurrentAudioRef.current = audio;
      audio.currentTime = 0;
      if (isStalePlayback()) {
        finish();
        return;
      }
      audio.play().catch(() => finish());
    });
    const playWebSpeech = () => new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        resolve();
      };
      const watchdog = window.setTimeout(finish, 4000);
      if (!('speechSynthesis' in window) || isStalePlayback()) {
        window.clearTimeout(watchdog);
        finish();
        return;
      }
      try {
        const utterance = new SpeechSynthesisUtterance(normalizedText);
        utterance.lang = language;
        utterance.rate = 0.95;
        utterance.onend = () => {
          window.clearTimeout(watchdog);
          finish();
        };
        utterance.onerror = () => {
          window.clearTimeout(watchdog);
          finish();
        };
        if (isStalePlayback()) {
          window.clearTimeout(watchdog);
          finish();
          return;
        }
        window.speechSynthesis.cancel();
        window.speechSynthesis.speak(utterance);
      } catch (error) {
        window.clearTimeout(watchdog);
        finish();
      }
    });

    const cachedAudioUrl = getTtsCacheValue(key);
    if (cachedAudioUrl) {
      const audioUrl = cachedAudioUrl;
      return playAudioUrl(audioUrl);
    }

    const ensureTtsReadyUrl = async () => {
      const readyCachedAudioUrl = getTtsCacheValue(key);
      if (readyCachedAudioUrl) {
        return readyCachedAudioUrl;
      }
      if (ttsInFlightRef.current.has(key)) {
        return ttsInFlightRef.current.get(key);
      }
      if (ttsPrefetchInFlightRef.current.has(key)) {
        try {
          const prefetched = await ttsPrefetchInFlightRef.current.get(key);
          if (prefetched) {
            setTtsCacheValue(key, prefetched);
            return prefetched;
          }
        } catch (error) {
          // continue with active request flow below
        }
      }
      const requestPromise = (async () => {
        const startedAt = Date.now();
        let generationStarted = Boolean(ttsPendingCacheRef.current.get(key)?.generateStarted);
        while (Date.now() - startedAt < 2500) {
          let statusPayload;
          try {
            statusPayload = await fetchTtsUrlStatus(normalizedText, normalizedLang, normalizedVoice);
          } catch (error) {
            console.warn('TTS status request failed', error);
            return null;
          }
          const status = String(statusPayload?.status || '').trim().toLowerCase();
          const audioUrl = String(statusPayload?.audio_url || '').trim();
          if (status === 'ready' && audioUrl) {
            ttsPendingCacheRef.current.delete(key);
            setTtsCacheValue(key, audioUrl);
            return audioUrl;
          }
          if (status === 'failed') {
            ttsPendingCacheRef.current.delete(key);
            return null;
          }

          if (!generationStarted) {
            generationStarted = true;
            ttsPendingCacheRef.current.set(key, { startedAt: Date.now(), generateStarted: true });
            try {
              const generatePayload = await requestTtsGenerate(normalizedText, normalizedLang, normalizedVoice);
              const generateStatus = String(generatePayload?.status || '').trim().toLowerCase();
              const generatedAudioUrl = String(generatePayload?.audio_url || '').trim();
              if (generateStatus === 'ready' && generatedAudioUrl) {
                ttsPendingCacheRef.current.delete(key);
                setTtsCacheValue(key, generatedAudioUrl);
                return generatedAudioUrl;
              }
              if (generateStatus === 'failed') {
                ttsPendingCacheRef.current.delete(key);
                return null;
              }
            } catch (error) {
              console.warn('TTS generate request failed', error);
              return null;
            }
          }

          const retryAfterMs = Number(statusPayload?.retry_after_ms || 400);
          const pollDelay = Math.max(250, Math.min(700, retryAfterMs));
          await new Promise((resolve) => window.setTimeout(resolve, pollDelay));
        }
        return null;
      })()
        .finally(() => {
          ttsInFlightRef.current.delete(key);
        });
      ttsInFlightRef.current.set(key, requestPromise);
      return requestPromise;
    };

    try {
      const readyUrl = await ensureTtsReadyUrl();
      if (isStalePlayback()) {
        return;
      }
      if (readyUrl) {
        return playAudioUrl(readyUrl);
      }
      console.info('TTS fallback: timeout_or_failed', { key });
      return playWebSpeech();
    } catch (error) {
      console.warn('TTS error', error);
      return playWebSpeech();
    }
  }, [initData, fetchTtsUrlStatus, getTtsCacheValue, requestTtsGenerate, setTtsCacheValue, stopTtsPlayback]);

  const isTtsPending = useCallback((key) => Boolean(ttsPendingMap[String(key || '')]), [ttsPendingMap]);

  const playTtsWithUi = useCallback(async (key, text, language = 'de-DE') => {
    const pendingKey = String(key || '').trim();
    const normalizedText = String(text || '').trim();
    if (!pendingKey || !normalizedText) return;
    if (ttsPendingKeysRef.current.has(pendingKey)) return;
    ttsPendingKeysRef.current.add(pendingKey);
    setTtsPendingMap((prev) => ({ ...prev, [pendingKey]: true }));
    try {
      await playTts(normalizedText, language);
    } finally {
      ttsPendingKeysRef.current.delete(pendingKey);
      setTtsPendingMap((prev) => {
        const next = { ...prev };
        delete next[pendingKey];
        return next;
      });
    }
  }, [playTts]);

  const renderTtsButtonContent = useCallback((loading) => (
    loading
      ? <span className="tts-mini-spinner" aria-hidden="true" />
      : '🔊'
  ), []);

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

  const getLearningTtsLocale = () => {
    const lang = normalizeLangCode(languageProfile?.learning_language);
    if (lang === 'en') return 'en-US';
    if (lang === 'es') return 'es-ES';
    if (lang === 'it') return 'it-IT';
    if (lang === 'ru') return 'ru-RU';
    return 'de-DE';
  };
  const getTtsLocaleForLang = (langCode) => {
    const lang = normalizeLangCode(langCode);
    if (lang === 'en') return 'en-US';
    if (lang === 'es') return 'es-ES';
    if (lang === 'it') return 'it-IT';
    if (lang === 'ru') return 'ru-RU';
    if (lang === 'de') return 'de-DE';
    return getLearningTtsLocale();
  };
  const detectTtsLangFromText = (rawText) => {
    const text = String(rawText || '').trim();
    if (!text) return normalizeLangCode(languageProfile?.learning_language) || 'de';
    if (/[А-Яа-яЁё]/.test(text)) return 'ru';
    if (/[ÄÖÜäöüß]/.test(text)) return 'de';
    if (/[A-Za-z]/.test(text)) {
      const learning = normalizeLangCode(languageProfile?.learning_language) || 'de';
      if (['de', 'en', 'es', 'it'].includes(learning)) return learning;
      return 'de';
    }
    return normalizeLangCode(languageProfile?.learning_language) || 'de';
  };

  const resolveDictionaryTargetTts = useCallback((item, directionValue = dictionaryDirection) => {
    if (!item) {
      return { text: '', locale: getLearningTtsLocale(), learningLang: normalizeLangCode(languageProfile?.learning_language) || 'de' };
    }
    const learningLang = normalizeLangCode(languageProfile?.learning_language) || 'de';
    const nativeLang = normalizeLangCode(languageProfile?.native_language) || 'ru';
    const dir = String(directionValue || '').trim().toLowerCase();
    let sourceLang = nativeLang;
    let targetLang = learningLang;
    if (dir.includes('-')) {
      const [from, to] = dir.split('-', 2);
      sourceLang = normalizeLangCode(from || sourceLang || nativeLang || 'ru');
      targetLang = normalizeLangCode(to || targetLang || learningLang || 'de');
    }
    const sourceText = String(
      item?.source_text
      || item?.word_de
      || item?.word_ru
      || item?.translation_de
      || item?.translation_ru
      || ''
    ).trim();
    const targetText = String(
      item?.target_text
      || item?.translation_de
      || item?.translation_ru
      || item?.word_de
      || item?.word_ru
      || ''
    ).trim();
    if (sourceLang === learningLang && sourceText) {
      return { text: sourceText, locale: getTtsLocaleForLang(learningLang), learningLang };
    }
    if (targetLang === learningLang && targetText) {
      return { text: targetText, locale: getTtsLocaleForLang(learningLang), learningLang };
    }
    if (sourceText && detectTtsLangFromText(sourceText) === learningLang) {
      return { text: sourceText, locale: getTtsLocaleForLang(learningLang), learningLang };
    }
    if (targetText && detectTtsLangFromText(targetText) === learningLang) {
      return { text: targetText, locale: getTtsLocaleForLang(learningLang), learningLang };
    }
    return { text: '', locale: getTtsLocaleForLang(learningLang), learningLang };
  }, [
    dictionaryDirection,
    detectTtsLangFromText,
    getLearningTtsLocale,
    getTtsLocaleForLang,
    languageProfile?.learning_language,
    languageProfile?.native_language,
  ]);

  const resolveFlashcardTexts = (entry) => {
    const responseJson = entry?.response_json || {};
    const sourceText = String(
      responseJson.source_text
      || entry?.word_ru
      || responseJson.word_ru
      || entry?.translation_ru
      || responseJson.translation_ru
      || entry?.word_de
      || responseJson.word_de
      || ''
    ).trim();
    const targetText = String(
      responseJson.target_text
      || entry?.translation_de
      || responseJson.translation_de
      || entry?.word_de
      || responseJson.word_de
      || entry?.translation_ru
      || responseJson.translation_ru
      || ''
    ).trim();
    return { sourceText, targetText };
  };

  const resolveFlashcardGerman = (entry) => {
    const responseJson = entry?.response_json || {};
    const quizType = String(responseJson?.quiz_type || '').trim();
    if (quizType === 'separable_prefix_verb_gap') {
      return String(responseJson?.correct_full_sentence || '').trim() || resolveFlashcardTexts(entry).targetText;
    }
    return resolveFlashcardTexts(entry).targetText;
  };

  const resolveFlashcardFeelEntryId = useCallback((entry) => {
    const raw = entry?.entry_id ?? entry?.id ?? entry?.response_json?.entry_id ?? 0;
    const normalized = Number(raw || 0);
    if (!Number.isFinite(normalized) || normalized <= 0) return 0;
    return Math.trunc(normalized);
  }, []);

  const queueFlashcardFeel = useCallback((entry) => {
    const entryId = resolveFlashcardFeelEntryId(entry);
    if (!entryId) return;
    if (flashcardFeelQueueRef.current.has(entryId)) {
      setFlashcardFeelStatusMap((prev) => ({
        ...prev,
        [entryId]: tr(
          'Уже в очереди: отправим в личку после завершения тренировки.',
          'Bereits in der Warteschlange: wird nach dem Training per Privatnachricht gesendet.'
        ),
      }));
      return;
    }
    flashcardFeelQueueRef.current.set(entryId, true);
    setFlashcardFeelQueuedMap((prev) => ({ ...prev, [entryId]: true }));
    setFlashcardFeelStatusMap((prev) => ({
      ...prev,
      [entryId]: tr(
        'Принято. Отправим объяснение в личку после завершения тренировки.',
        'Verstanden. Wir senden die Erklaerung nach dem Training per Privatnachricht.'
      ),
    }));
  }, [resolveFlashcardFeelEntryId, tr]);

  const dispatchQueuedFlashcardFeel = useCallback(async (trigger = 'manual') => {
    if (!initData) {
      return 0;
    }
    if (flashcardFeelDispatchInFlightRef.current) {
      return 0;
    }
    const entryIds = Array.from(flashcardFeelQueueRef.current.keys())
      .map((value) => Number(value || 0))
      .filter((value) => Number.isInteger(value) && value > 0);
    if (!entryIds.length) {
      return 0;
    }

    flashcardFeelDispatchInFlightRef.current = true;
    setFlashcardFeelDispatching(true);
    try {
      const response = await fetch('/api/webapp/flashcards/feel/dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          entry_ids: entryIds,
          trigger,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const queued = Math.max(0, Number(data?.queued || entryIds.length));
      for (const entryId of entryIds) {
        flashcardFeelQueueRef.current.delete(entryId);
      }
      setFlashcardFeelQueuedMap((prev) => {
        const next = { ...prev };
        for (const entryId of entryIds) {
          delete next[entryId];
        }
        return next;
      });
      setFlashcardFeelStatusMap((prev) => {
        const next = { ...prev };
        for (const entryId of entryIds) {
          next[entryId] = tr(
            'Отправлено в обработку. Сообщение придёт в личку.',
            'Zur Verarbeitung gesendet. Die Nachricht kommt in die Privatnachricht.'
          );
        }
        return next;
      });
      return queued;
    } catch (error) {
      const message = normalizeNetworkErrorMessage(error, 'Не удалось отправить Feel в очередь.', 'Feel konnte nicht in die Warteschlange gestellt werden.');
      setWebappError(`${tr('Ошибка Feel', 'Feel-Fehler')}: ${message}`);
      return 0;
    } finally {
      flashcardFeelDispatchInFlightRef.current = false;
      setFlashcardFeelDispatching(false);
    }
  }, [initData, normalizeNetworkErrorMessage, tr]);

  const preloadTts = useCallback((text, language = 'de-DE', voice = '') => {
    if (!initData) return;
    const normalizedText = String(text || '').trim();
    if (!normalizedText) return;
    const normalizedLang = String(language || 'de-DE').trim() || 'de-DE';
    const normalizedVoice = String(voice || '').trim();
    const key = `${normalizedLang}:${normalizedVoice}:${normalizedText}`;
    if (
      Boolean(getTtsCacheValue(key))
      || ttsInFlightRef.current.has(key)
      || ttsPrefetchInFlightRef.current.has(key)
    ) return;
    const requestPromise = (async () => {
      try {
        const statusPayload = await fetchTtsUrlStatus(normalizedText, normalizedLang, normalizedVoice);
        const status = String(statusPayload?.status || '').trim().toLowerCase();
        const audioUrl = String(statusPayload?.audio_url || '').trim();
        if (status === 'ready' && audioUrl) {
          ttsPendingCacheRef.current.delete(key);
          setTtsCacheValue(key, audioUrl);
          return audioUrl;
        }
        if (status === 'failed') {
          ttsPendingCacheRef.current.delete(key);
          return null;
        }
        if (!ttsPendingCacheRef.current.has(key)) {
          ttsPendingCacheRef.current.set(key, { startedAt: Date.now(), generateStarted: true });
          const generated = await requestTtsGenerate(normalizedText, normalizedLang, normalizedVoice);
          const generatedStatus = String(generated?.status || '').trim().toLowerCase();
          const generatedUrl = String(generated?.audio_url || '').trim();
          if (generatedStatus === 'ready' && generatedUrl) {
            ttsPendingCacheRef.current.delete(key);
            setTtsCacheValue(key, generatedUrl);
            return generatedUrl;
          }
          if (generatedStatus === 'failed') {
            ttsPendingCacheRef.current.delete(key);
            return null;
          }
        }
      } catch (error) {
        return null;
      }
      return null;
    })()
      .finally(() => {
        ttsPrefetchInFlightRef.current.delete(key);
      });
    ttsPrefetchInFlightRef.current.set(key, requestPromise);
  }, [initData, fetchTtsUrlStatus, getTtsCacheValue, requestTtsGenerate, setTtsCacheValue]);

  useEffect(() => {
    const locale = getLearningTtsLocale();
    flashcards.slice(0, 8).forEach((item) => {
      const german = resolveFlashcardGerman(item);
      if (german) preloadTts(german, locale);
    });
  }, [flashcards, languageProfile?.learning_language, preloadTts]);

  useEffect(() => {
    const targetTts = resolveDictionaryTargetTts(dictionaryResult, dictionaryDirection);
    if (!targetTts.text) return undefined;
    const timerId = window.setTimeout(() => {
      preloadTts(targetTts.text, targetTts.locale);
    }, 0);
    return () => window.clearTimeout(timerId);
  }, [dictionaryDirection, dictionaryResult, preloadTts, resolveDictionaryTargetTts]);

  useEffect(() => {
    const locale = getLearningTtsLocale();
    results.slice(0, 6).forEach((item) => {
      const correct = extractCorrectTranslationText(item);
      if (correct) preloadTts(correct, locale);
    });
  }, [results, languageProfile?.learning_language, preloadTts]);

  const getSrsCardId = useCallback((card) => String(card?.id || card?.entry_id || '').trim(), []);

  const updateSrsPrefetchQueue = useCallback((updater) => {
    setSrsPrefetchQueue((prev) => {
      const rawNext = typeof updater === 'function' ? updater(prev) : updater;
      const next = Array.isArray(rawNext) ? rawNext : [];
      srsPrefetchQueueRef.current = next;
      return next;
    });
  }, []);

  const clearSrsReviewRetryTimer = useCallback(() => {
    if (srsReviewRetryTimerRef.current) {
      window.clearTimeout(srsReviewRetryTimerRef.current);
      srsReviewRetryTimerRef.current = null;
    }
  }, []);

  useEffect(() => () => {
    clearSrsReviewRetryTimer();
  }, [clearSrsReviewRetryTimer]);

  const appendToSrsPrefetchQueue = useCallback((items = []) => {
    const incoming = Array.isArray(items) ? items : [];
    if (incoming.length === 0) return;
    updateSrsPrefetchQueue((prev) => {
      const activeCardId = getSrsCardId(srsCardRef.current);
      const seen = new Set(activeCardId ? [activeCardId] : []);
      const next = [];
      for (const item of prev) {
        const itemId = getSrsCardId(item);
        if (itemId && seen.has(itemId)) continue;
        if (itemId) seen.add(itemId);
        next.push(item);
      }
      for (const item of incoming) {
        const itemId = getSrsCardId(item);
        if (itemId && seen.has(itemId)) continue;
        if (itemId) seen.add(itemId);
        next.push(item);
      }
      return next.slice(0, 20);
    });
  }, [getSrsCardId, updateSrsPrefetchQueue]);

  const takeFromSrsPrefetchQueue = useCallback(() => {
    const queue = srsPrefetchQueueRef.current;
    if (!Array.isArray(queue) || queue.length === 0) return null;
    const [nextCard, ...rest] = queue;
    srsPrefetchQueueRef.current = rest;
    setSrsPrefetchQueue(rest);
    return nextCard || null;
  }, []);

  const applySrsPayload = useCallback((data) => {
    stopTtsPlayback();
    const nextCard = data?.card || null;
    srsCardRef.current = nextCard;
    setSrsCard(nextCard);
    setSrsState(data?.srs || null);
    setSrsPreview(data?.srs_preview && typeof data.srs_preview === 'object' ? data.srs_preview : null);
    const incomingQueue = data?.queue_info && typeof data.queue_info === 'object'
      ? data.queue_info
      : null;
    const nextDueCount = Number(incomingQueue?.due_count);
    const nextNewRemaining = Number(incomingQueue?.new_remaining_today);
    if (Number.isFinite(nextDueCount) || Number.isFinite(nextNewRemaining)) {
      setSrsQueueInfo((prev) => ({
        due_count: Number.isFinite(nextDueCount)
          ? Math.max(0, Math.trunc(nextDueCount))
          : Math.max(0, Math.trunc(Number(prev?.due_count || 0))),
        new_remaining_today: Number.isFinite(nextNewRemaining)
          ? Math.max(0, Math.trunc(nextNewRemaining))
          : Math.max(0, Math.trunc(Number(prev?.new_remaining_today || 0))),
      }));
    }
    const activeCardId = getSrsCardId(nextCard);
    if (activeCardId) {
      updateSrsPrefetchQueue((prev) => prev.filter((item) => getSrsCardId(item) !== activeCardId));
    }
    setSrsRevealAnswer(false);
    srsShownAtRef.current = Date.now();
  }, [getSrsCardId, updateSrsPrefetchQueue, stopTtsPlayback]);

  const decrementSrsQueueInfoLocal = () => {
    setSrsQueueInfo((prev) => {
      const dueCount = Math.max(0, Math.trunc(Number(prev?.due_count || 0)));
      const newRemaining = Math.max(0, Math.trunc(Number(prev?.new_remaining_today || 0)));
      if (dueCount > 0) {
        return { due_count: dueCount - 1, new_remaining_today: newRemaining };
      }
      if (newRemaining > 0) {
        return { due_count: dueCount, new_remaining_today: newRemaining - 1 };
      }
      return { due_count: dueCount, new_remaining_today: newRemaining };
    });
  };

  const loadSrsNextCard = useCallback(async () => {
    if (!initData) return;
    const fsrsContextActive = flashcardActiveMode === 'fsrs'
      && flashcardsVisible
      && (flashcardsOnly || selectedSections.has('flashcards'));
    if (!fsrsContextActive) return;
    const requestSignature = `${String(initData || '')}|${String(languageProfile?.native_language || '')}|${String(languageProfile?.learning_language || '')}`;
    const nowTs = Date.now();
    if (srsNextLoadInFlightRef.current) {
      if (srsNextLoadLastSignatureRef.current !== requestSignature) {
        srsNextLoadPendingRef.current = true;
      }
      return;
    }
    if (
      srsNextLoadLastSignatureRef.current === requestSignature
      && (nowTs - srsNextLoadLastStartedAtRef.current) < SRS_NEXT_DEDUP_WINDOW_MS
    ) {
      return;
    }
    srsNextLoadInFlightRef.current = true;
    srsNextLoadLastSignatureRef.current = requestSignature;
    srsNextLoadLastStartedAtRef.current = nowTs;
    const FSRS_LOAD_TIMEOUT_MS = 60000;
    try {
      setSrsLoading(true);
      setSrsError('');
      let response = await fetchWithTimeout(`/api/cards/next?initData=${encodeURIComponent(initData)}`, {}, FSRS_LOAD_TIMEOUT_MS);
      if (!response.ok && response.status >= 500) {
        await new Promise((resolve) => setTimeout(resolve, 220));
        response = await fetchWithTimeout(`/api/cards/next?initData=${encodeURIComponent(initData)}`, {}, FSRS_LOAD_TIMEOUT_MS);
      }
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки SRS карточки', 'Fehler beim Laden der SRS-Karte'));
      }
      const data = await response.json();
      applySrsPayload(data);
    } catch (error) {
      const rawName = String(error?.name || '').toLowerCase();
      const rawMessage = String(error?.message || '').toLowerCase();
      const isTimeoutError = rawName === 'timeouterror'
        || rawName === 'aborterror'
        || rawMessage.includes('timeout')
        || rawMessage.includes('timed out')
        || rawMessage.includes('aborted');
      if (isTimeoutError) {
        try {
          const probe = await fetchWithTimeout('/api/webapp/dictionary/cards', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ initData, limit: 1 }),
          }, 12000);
          if (probe.ok) {
            const probeData = await probe.json();
            const probeItems = Array.isArray(probeData?.items) ? probeData.items : [];
            if (probeItems.length === 0) {
              srsCardRef.current = null;
              setSrsCard(null);
              setSrsState(null);
              setSrsPreview(null);
              setSrsQueueInfo({ due_count: 0, new_remaining_today: 0 });
              setSrsError('');
              return;
            }
          }
        } catch (_probeError) {
          // ignore probe errors and show original timeout message
        }
      }
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить FSRS карточку.', 'FSRS-Karte konnte nicht geladen werden.');
      setSrsError(friendly);
      setWebappError(`${tr('Ошибка загрузки SRS карточки', 'Fehler beim Laden der SRS-Karte')}: ${friendly}`);
    } finally {
      setSrsLoading(false);
      srsNextLoadInFlightRef.current = false;
      if (srsNextLoadPendingRef.current) {
        srsNextLoadPendingRef.current = false;
        window.setTimeout(() => {
          void loadSrsNextCard();
        }, 0);
      }
    }
  }, [
    applySrsPayload,
    fetchWithTimeout,
    initData,
    normalizeNetworkErrorMessage,
    readApiError,
    tr,
    flashcardActiveMode,
    flashcardsVisible,
    flashcardsOnly,
    selectedSections,
    languageProfile?.native_language,
    languageProfile?.learning_language,
  ]);

  const prefetchSrsCards = useCallback(async () => {
    if (!initData || srsPrefetchInFlightRef.current) return;
    srsPrefetchInFlightRef.current = true;
    try {
      const response = await fetchWithTimeout(`/api/cards/prefetch?initData=${encodeURIComponent(initData)}`, {}, 12000);
      if (!response.ok) return;
      const data = await response.json();
      const queueInfo = data?.queue_info && typeof data.queue_info === 'object' ? data.queue_info : null;
      if (queueInfo) {
        const nextDueCount = Number(queueInfo?.due_count);
        const nextNewRemaining = Number(queueInfo?.new_remaining_today);
        if (Number.isFinite(nextDueCount) || Number.isFinite(nextNewRemaining)) {
          setSrsQueueInfo((prev) => ({
            due_count: Number.isFinite(nextDueCount)
              ? Math.max(0, Math.trunc(nextDueCount))
              : Math.max(0, Math.trunc(Number(prev?.due_count || 0))),
            new_remaining_today: Number.isFinite(nextNewRemaining)
              ? Math.max(0, Math.trunc(nextNewRemaining))
              : Math.max(0, Math.trunc(Number(prev?.new_remaining_today || 0))),
          }));
        }
      }
      const items = Array.isArray(data?.items) ? data.items : [];
      appendToSrsPrefetchQueue(items);
    } catch (error) {
      console.warn('FSRS card prefetch failed', error);
    } finally {
      srsPrefetchInFlightRef.current = false;
    }
  }, [appendToSrsPrefetchQueue, fetchWithTimeout, initData]);

  const loadTodayPlan = async () => {
    if (!initData) return;
    try {
      setTodayPlanLoading(true);
      setTodayPlanError('');
      const response = await fetchGetWithRetry(`/api/today?initData=${encodeURIComponent(initData)}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки плана на сегодня', 'Fehler beim Laden des Tagesplans'));
      }
      const data = await response.json();
      setTodayPlan({
        date: data?.date || null,
        total_minutes: data?.total_minutes || 0,
        items: Array.isArray(data?.items) ? data.items : [],
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить задачи на сегодня.',
        'Tagesaufgaben konnten nicht geladen werden.'
      );
      setTodayPlanError(friendly);
    } finally {
      setTodayPlanLoading(false);
    }
  };

  const loadSkillReport = async () => {
    if (!initData) return;
    try {
      setSkillReportLoading(true);
      setSkillReportError('');
      const response = await fetchGetWithRetry(`/api/progress/skills?period=7d&initData=${encodeURIComponent(initData)}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки отчета по навыкам', 'Fehler beim Laden des Skills-Reports'));
      }
      const data = await response.json();
      setSkillReport({
        updated_at: data?.updated_at || null,
        top_weak: Array.isArray(data?.top_weak) ? data.top_weak : [],
        groups: Array.isArray(data?.groups) ? data.groups : [],
        total_skills: Number(data?.total_skills || 0),
        skill_training_status: data?.skill_training_status && typeof data.skill_training_status === 'object'
          ? data.skill_training_status
          : {},
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить прогресс навыков.',
        'Skill-Fortschritt konnte nicht geladen werden.'
      );
      setSkillReportError(friendly);
    } finally {
      setSkillReportLoading(false);
    }
  };

  const loadWeeklyPlan = async () => {
    if (!initData) return;
    try {
      setWeeklyPlanLoading(true);
      setWeeklyPlanError('');
      const response = await fetchGetWithRetry(`/api/progress/weekly-plan?initData=${encodeURIComponent(initData)}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки недельного плана', 'Fehler beim Laden des Wochenplans'));
      }
      const data = await response.json();
      const plan = {
        week: data?.week || null,
        plan: data?.plan || { translations_goal: 0, learned_words_goal: 0, agent_minutes_goal: 0, reading_minutes_goal: 0 },
        metrics: data?.metrics || {},
      };
      setWeeklyPlan(plan);
      setWeeklyPlanDraft({
        translations_goal: String(Number(plan?.plan?.translations_goal || 0)),
        learned_words_goal: String(Number(plan?.plan?.learned_words_goal || 0)),
        agent_minutes_goal: String(Number(plan?.plan?.agent_minutes_goal || 0)),
        reading_minutes_goal: String(Number(plan?.plan?.reading_minutes_goal || 0)),
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить недельный план.',
        'Wochenplan konnte nicht geladen werden.'
      );
      setWeeklyPlanError(friendly);
    } finally {
      setWeeklyPlanLoading(false);
    }
  };

  const loadPlanAnalytics = async (periodOverride) => {
    if (!initData) return;
    const period = periodOverride || planAnalyticsPeriod;
    try {
      setPlanAnalyticsLoading(true);
      setPlanAnalyticsError('');
      const response = await fetchGetWithRetry(`/api/progress/plan-analytics?initData=${encodeURIComponent(initData)}&period=${encodeURIComponent(period)}`, 45000);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки аналитики планов', 'Fehler beim Laden der Plan-Analytik'));
      }
      const data = await response.json();
      setPlanAnalyticsMetrics(data?.metrics || {});
      setPlanAnalyticsRange(data?.range || null);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить аналитику планов.',
        'Plan-Analytik konnte nicht geladen werden.'
      );
      setPlanAnalyticsError(friendly);
    } finally {
      setPlanAnalyticsLoading(false);
    }
  };

  const loadWeeklySummaryHero = useCallback(async () => {
    if (!initData || !weeklySummaryVisitConfig) {
      setWeeklySummaryHeroFacts(null);
      setWeeklySummaryCurrentMetrics(null);
      setWeeklySummaryPreviousMetrics(null);
      return;
    }
    const buildUrl = (periodConfig) => {
      const params = new URLSearchParams({
        initData,
        period: 'week',
        week_start: String(periodConfig?.startDate || ''),
        as_of_date: String(periodConfig?.endDate || ''),
      });
      return `/api/progress/plan-analytics?${params.toString()}`;
    };
    try {
      setWeeklySummaryHeroLoading(true);
      setWeeklySummaryHeroError('');
      const [currentResponse, previousResponse] = await Promise.all([
        fetchGetWithRetry(buildUrl(weeklySummaryVisitConfig.currentPeriod), 45000),
        fetchGetWithRetry(buildUrl(weeklySummaryVisitConfig.previousPeriod), 45000),
      ]);
      if (!currentResponse.ok) {
        throw new Error(await readApiError(currentResponse, 'Ошибка загрузки weekly summary', 'Fehler beim Laden der Weekly Summary'));
      }
      if (!previousResponse.ok) {
        throw new Error(await readApiError(previousResponse, 'Ошибка загрузки weekly summary', 'Fehler beim Laden der Weekly Summary'));
      }
      const [currentData, previousData] = await Promise.all([
        currentResponse.json(),
        previousResponse.json(),
      ]);
      setWeeklySummaryCurrentMetrics(currentData?.metrics || {});
      setWeeklySummaryPreviousMetrics(previousData?.metrics || {});
      const facts = buildWeeklySummaryHeroFacts({
        currentMetrics: currentData?.metrics || {},
        previousMetrics: previousData?.metrics || {},
        metricPriority: ['translations', 'learned_words', 'reading_minutes', 'youtube_minutes'],
      });
      setWeeklySummaryHeroFacts(facts);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить summary по неделе.',
        'Die Wochenzusammenfassung konnte nicht geladen werden.'
      );
      setWeeklySummaryHeroError(friendly);
      setWeeklySummaryHeroFacts(null);
      setWeeklySummaryCurrentMetrics(null);
      setWeeklySummaryPreviousMetrics(null);
    } finally {
      setWeeklySummaryHeroLoading(false);
    }
  }, [
    fetchGetWithRetry,
    initData,
    normalizeNetworkErrorMessage,
    readApiError,
    weeklySummaryVisitConfig,
  ]);

  useEffect(() => {
    if (!weeklySummaryModalOpen || !weeklySummaryVisitConfig) {
      return;
    }
    void loadWeeklySummaryHero();
  }, [loadWeeklySummaryHero, weeklySummaryModalOpen, weeklySummaryVisitConfig]);

  const saveWeeklyPlan = async () => {
    if (!initData) return;
    const translationsGoal = Math.max(0, Number.parseInt(String(weeklyPlanDraft.translations_goal || '0'), 10) || 0);
    const learnedWordsGoal = Math.max(0, Number.parseInt(String(weeklyPlanDraft.learned_words_goal || '0'), 10) || 0);
    const agentMinutesGoal = Math.max(0, Number.parseInt(String(weeklyPlanDraft.agent_minutes_goal || '0'), 10) || 0);
    const readingMinutesGoal = Math.max(0, Number.parseInt(String(weeklyPlanDraft.reading_minutes_goal || '0'), 10) || 0);
    try {
      setWeeklyPlanSaving(true);
      setWeeklyPlanError('');
      const response = await fetch('/api/progress/weekly-plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          translations_goal: translationsGoal,
          learned_words_goal: learnedWordsGoal,
          agent_minutes_goal: agentMinutesGoal,
          reading_minutes_goal: readingMinutesGoal,
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения недельного плана', 'Fehler beim Speichern des Wochenplans'));
      }
      const data = await response.json();
      const plan = {
        week: data?.week || null,
        plan: data?.plan || { translations_goal: 0, learned_words_goal: 0, agent_minutes_goal: 0, reading_minutes_goal: 0 },
        metrics: data?.metrics || {},
      };
      setWeeklyPlan(plan);
      setWeeklyPlanDraft({
        translations_goal: String(Number(plan?.plan?.translations_goal || 0)),
        learned_words_goal: String(Number(plan?.plan?.learned_words_goal || 0)),
        agent_minutes_goal: String(Number(plan?.plan?.agent_minutes_goal || 0)),
        reading_minutes_goal: String(Number(plan?.plan?.reading_minutes_goal || 0)),
      });
      setWeeklyPlanCollapsed(true);
      setWeeklyMetricExpanded({
        translations: false,
        learned_words: false,
        agent_minutes: false,
        reading_minutes: false,
      });
      loadPlanAnalytics();
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось сохранить недельный план.',
        'Wochenplan konnte nicht gespeichert werden.'
      );
      setWeeklyPlanError(friendly);
    } finally {
      setWeeklyPlanSaving(false);
    }
  };

  const startSkillPractice = async (skill, options = {}) => {
    if (!initData || !skill?.skill_id) return;
    const skillId = String(skill.skill_id);
    const forceRefresh = Boolean(options?.forceRefresh);
    try {
      setSkillPracticeLoading((prev) => ({ ...prev, [skillId]: true }));
      setSkillReportError('');
      setSkillTrainingLoading(true);
      setSkillTrainingVideoLoading(false);
      setSkillTrainingError('');
      setSkillTrainingFeedback(null);
      setSkillTrainingAnswers(['', '', '', '', '']);
      openSingleSectionAndScroll('skill_training', skillTrainingRef);

      const prepareResponse = await fetch('/api/today/theory/prepare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          skill_id: skillId,
          skill_title: String(skill?.name || skill?.title || '').trim() || null,
          lookback_days: 14,
          force_refresh: forceRefresh,
        }),
      });
      if (!prepareResponse.ok) {
        throw new Error(await readApiError(prepareResponse, 'Ошибка запуска прокачки', 'Fehler beim Start der Skill-Uebung'));
      }
      const prepareData = await prepareResponse.json();
      const pack = prepareData?.package && typeof prepareData.package === 'object' ? prepareData.package : null;
      if (!pack) {
        throw new Error(tr('Не удалось подготовить тренировку навыка.', 'Skill-Training konnte nicht vorbereitet werden.'));
      }

      const focus = pack?.focus && typeof pack.focus === 'object' ? pack.focus : {};
      setSkillTrainingData({
        skill: {
          skill_id: skillId,
          title: String(skill?.name || skill?.title || '').trim() || String(pack?.focus?.skill_name || '').trim(),
          mastery: skill?.mastery,
        },
        package: pack,
        video: null,
      });
      loadSkillReport();

      setSkillTrainingVideoLoading(true);
      void (async () => {
        try {
          const videoResponse = await fetch('/api/today/video/recommend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              initData,
              skill_id: focus?.skill_id || skillId,
              skill_title: focus?.skill_name || String(skill?.name || skill?.title || '').trim() || null,
              main_category: focus?.error_category || null,
              sub_category: focus?.error_subcategory || null,
              examples: Array.isArray(pack?.examples_used) ? pack.examples_used.slice(0, 5) : [],
              lookback_days: 14,
            }),
          });
          if (!videoResponse.ok) return;
          const videoData = await videoResponse.json();
          const video = videoData?.video && typeof videoData.video === 'object' ? videoData.video : null;
          setSkillTrainingData((prev) => {
            const prevSkillId = String(prev?.skill?.skill_id || '').trim();
            if (!prev || prevSkillId !== skillId) return prev;
            return { ...prev, video };
          });
        } catch (error) {
          // Keep skill training flow functional even if video recommendation fails.
        } finally {
          setSkillTrainingVideoLoading(false);
        }
      })();
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось запустить тренировку навыка.',
        'Skill-Uebung konnte nicht gestartet werden.'
      );
      setSkillTrainingError(friendly);
      setSkillTrainingData(null);
      setSkillTrainingVideoLoading(false);
      setSelectedSections(new Set());
    } finally {
      setSkillTrainingLoading(false);
      setSkillPracticeLoading((prev) => {
        const next = { ...prev };
        delete next[skillId];
        return next;
      });
    }
  };

  const resumeSkillPractice = (skill) => {
    const skillId = String(skill?.skill_id || '').trim();
    if (!skillId) return;
    const snapshot = getStoredSkillTrainingSnapshot(skillId);
    if (!snapshot) return;
    setSkillTrainingLoading(false);
    setSkillTrainingVideoLoading(false);
    setSkillTrainingError('');
    setSkillTrainingFeedback(snapshot.feedback || null);
    setSkillTrainingAnswers(
      Array.from({ length: 5 }, (_, index) => String(snapshot.answers?.[index] || ''))
    );
    setSkillTrainingData({
      skill: snapshot.skill,
      package: snapshot.package,
      video: snapshot.video || null,
    });
    openSingleSectionAndScroll('skill_training', skillTrainingRef);
  };

  const checkSkillTraining = async () => {
    if (!initData || !skillTrainingData?.package) return;
    const sentences = Array.isArray(skillTrainingData?.package?.practice_sentences)
      ? skillTrainingData.package.practice_sentences
      : [];
    if (!sentences.length) {
      setSkillTrainingError(tr('Нет предложений для проверки.', 'Keine Saetze zur Pruefung.'));
      return;
    }
    if (skillTrainingAnswers.some((item, index) => index < sentences.length && !String(item || '').trim())) {
      setSkillTrainingError(tr('Заполните переводы для всех предложений.', 'Bitte alle Uebersetzungen ausfuellen.'));
      return;
    }
    try {
      setSkillTrainingChecking(true);
      setSkillTrainingError('');
      const response = await fetch('/api/today/theory/check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          focus: skillTrainingData?.package?.focus || {},
          native_sentences: sentences,
          translations: skillTrainingAnswers.slice(0, sentences.length),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка проверки тренировки', 'Fehler beim Pruefen des Trainings'));
      }
      const data = await response.json();
      setSkillTrainingFeedback(data?.feedback || null);
      loadSkillReport();
    } catch (error) {
      setSkillTrainingError(normalizeNetworkErrorMessage(
        error,
        'Не удалось проверить тренировку.',
        'Training konnte nicht geprueft werden.'
      ));
    } finally {
      setSkillTrainingChecking(false);
    }
  };

  const openSkillTrainingVideo = () => {
    const video = skillTrainingData?.video && typeof skillTrainingData.video === 'object' ? skillTrainingData.video : null;
    const videoUrl = String(video?.video_url || '').trim();
    const videoId = String(video?.video_id || '').trim();
    if (!videoUrl && !videoId) return;
    setYoutubeInput(videoUrl || `https://youtu.be/${videoId}`);
    setYoutubeForceShowPanel(true);
    setYoutubeBackSection('skill_training');
    openSingleSectionAndScroll('youtube', youtubeRef);
  };

  const finishSkillTraining = () => {
    const activeSkillId = String(
      skillTrainingData?.skill?.skill_id
      || skillTrainingData?.package?.focus?.skill_id
      || ''
    ).trim();
    if (activeSkillId) {
      removeSkillTrainingSnapshot(activeSkillId);
    }
    setSkillTrainingLoading(false);
    setSkillTrainingVideoLoading(false);
    setSkillTrainingError('');
    setSkillTrainingFeedback(null);
    setSkillTrainingData(null);
    setSkillTrainingAnswers(['', '', '', '', '']);
    goHomeScreen();
  };

  const trackSkillPracticeEvent = useCallback(async (skillId, event, resourceUrl = '') => {
    const normalizedSkillId = String(skillId || '').trim();
    if (!initData || !normalizedSkillId || !event) return;
    try {
      await fetch(`/api/progress/skills/${encodeURIComponent(normalizedSkillId)}/practice/event`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          event,
          resource_url: resourceUrl || null,
        }),
      });
      loadSkillReport();
    } catch (error) {
      // no-op: UI should not break if event tracking failed
    }
  }, [initData, loadSkillReport]);

  const regenerateTodayPlan = async () => {
    if (!initData) return;
    try {
      setTodayPlanLoading(true);
      setTodayPlanError('');
      const response = await fetch('/api/today/regenerate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка пересборки плана', 'Fehler beim Aktualisieren des Plans'));
      }
      const data = await response.json();
      setTodayPlan({
        date: data?.date || null,
        total_minutes: data?.total_minutes || 0,
        items: Array.isArray(data?.items) ? data.items : [],
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось пересобрать задачи на сегодня.',
        'Tagesaufgaben konnten nicht aktualisiert werden.'
      );
      setTodayPlanError(friendly);
    } finally {
      setTodayPlanLoading(false);
    }
  };

  const sendTodayReminderTest = async () => {
    if (!initData) return;
    try {
      setTodayTestSending(true);
      setTodayPlanError('');
      const response = await fetch('/api/today/reminders/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка тестовой отправки в личку', 'Fehler beim Testversand'));
      }
      setTodayPlanError(tr('Тест отправлен в личку. Проверьте диалог с ботом.', 'Test wurde in den privaten Chat gesendet.'));
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось отправить тест в личку.',
        'Test in den privaten Chat konnte nicht gesendet werden.'
      );
      setTodayPlanError(friendly);
    } finally {
      setTodayTestSending(false);
    }
  };

  const updateTodayItemStatus = async (itemId, action) => {
    if (!initData || !itemId) return null;
    try {
      setTodayItemLoading((prev) => ({ ...prev, [itemId]: action }));
      const response = await fetch(`/api/today/items/${itemId}/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка обновления статуса задачи', 'Fehler beim Aktualisieren des Aufgabenstatus'));
      }
      const data = await response.json();
      const updated = data?.item || null;
      if (updated) {
        setTodayPlan((prev) => {
          if (!prev || !Array.isArray(prev.items)) return prev;
          return {
            ...prev,
            items: prev.items.map((item) => (item.id === updated.id ? { ...item, ...updated } : item)),
          };
        });
      }
      return updated;
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось обновить задачу.',
        'Aufgabe konnte nicht aktualisiert werden.'
      );
      setTodayPlanError(friendly);
      return null;
    } finally {
      setTodayItemLoading((prev) => {
        const next = { ...prev };
        delete next[itemId];
        return next;
      });
    }
  };

  const getTodayItemTimerPayload = (item) => (
    item?.payload && typeof item.payload === 'object' ? item.payload : {}
  );

  const getTodayItemGoalSeconds = (item) => {
    const payload = getTodayItemTimerPayload(item);
    const explicitGoal = Number(payload?.timer_goal_seconds || 0);
    if (explicitGoal > 0) return Math.max(0, Math.floor(explicitGoal));
    return Math.max(0, Math.floor(Number(item?.estimated_minutes || 0) * 60));
  };

  const getTodayItemElapsedSeconds = (item, nowMs = Date.now()) => {
    const payload = getTodayItemTimerPayload(item);
    const baseSeconds = Math.max(0, Math.floor(Number(payload?.timer_seconds || 0)));
    const isDone = String(item?.status || '').toLowerCase() === 'done';
    if (isDone) return baseSeconds;
    const timerRunning = Boolean(payload?.timer_running);
    const startedAtRaw = String(payload?.timer_started_at || '').trim();
    if (!timerRunning || !startedAtRaw) return baseSeconds;
    const startedMs = Date.parse(startedAtRaw);
    if (!Number.isFinite(startedMs)) return baseSeconds;
    const liveExtra = Math.max(0, Math.floor((nowMs - startedMs) / 1000));
    return baseSeconds + liveExtra;
  };

  const getTodayTranslationProgress = (item) => {
    const payload = getTodayItemTimerPayload(item);
    const targetCount = Math.max(0, Math.floor(Number(
      payload?.translation_target_count
      || payload?.sentences
      || 0
    )));
    const completedCount = Math.max(0, Math.floor(Number(payload?.translation_completed_count || 0)));
    const progressPercent = targetCount > 0
      ? Math.max(0, Math.min(100, (completedCount / targetCount) * 100))
      : 0;
    return {
      completedCount,
      targetCount,
      progressPercent,
    };
  };

  const getTodayItemProgressPercent = (item, nowMs = Date.now()) => {
    const taskType = String(item?.task_type || '').toLowerCase();
    if (taskType === 'translation') {
      return getTodayTranslationProgress(item).progressPercent;
    }
    const goalSeconds = getTodayItemGoalSeconds(item);
    const elapsed = getTodayItemElapsedSeconds(item, nowMs);
    if (goalSeconds <= 0) return elapsed > 0 ? 100 : 0;
    return Math.max(0, Math.min(100, (elapsed / goalSeconds) * 100));
  };

  const isTodayItemTimerRunning = (item) => {
    const payload = getTodayItemTimerPayload(item);
    return Boolean(payload?.timer_running) && String(item?.status || '').toLowerCase() !== 'done';
  };

  const syncTodayItemTimer = async (item, action, options = {}) => {
    if (!initData || !item?.id) return null;
    const elapsedSeconds = options?.elapsedSeconds;
    const running = options?.running;
    const keepalive = Boolean(options?.keepalive);
    try {
      setTodayItemLoading((prev) => ({ ...prev, [item.id]: action === 'sync' ? 'timer_sync' : `timer_${action}` }));
      const response = await fetch(`/api/today/items/${item.id}/timer`, {
        method: 'POST',
        keepalive,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          action,
          elapsed_seconds: Number.isFinite(Number(elapsedSeconds)) ? Math.max(0, Math.floor(Number(elapsedSeconds))) : undefined,
          running: running === undefined ? undefined : Boolean(running),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка синхронизации таймера задачи', 'Fehler bei der Aufgaben-Timer-Synchronisierung'));
      }
      const data = await response.json();
      const updated = data?.item || null;
      if (updated) {
        setTodayPlan((prev) => {
          if (!prev || !Array.isArray(prev.items)) return prev;
          return {
            ...prev,
            items: prev.items.map((entry) => (entry.id === updated.id ? { ...entry, ...updated } : entry)),
          };
        });
      }
      return updated;
    } catch (error) {
      setTodayPlanError(normalizeNetworkErrorMessage(
        error,
        'Не удалось синхронизировать таймер задачи.',
        'Aufgaben-Timer konnte nicht synchronisiert werden.'
      ));
      return null;
    } finally {
      setTodayItemLoading((prev) => {
        const next = { ...prev };
        delete next[item.id];
        return next;
      });
    }
  };

  const findTodayTaskByTypes = (types = []) => {
    const normalizedTypes = new Set(types.map((entry) => String(entry || '').toLowerCase()));
    const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
    const active = items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase()) && String(entry?.status || '').toLowerCase() !== 'done');
    if (active) return active;
    return items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase())) || null;
  };

  const getTodayTaskForSection = (sectionKey) => {
    const normalized = String(sectionKey || '').toLowerCase();
    if (normalized === 'translations') return findTodayTaskByTypes(['translation']);
    if (normalized === 'theory') return findTodayTaskByTypes(['theory']);
    if (normalized === 'youtube') return findTodayTaskByTypes(['video', 'youtube']);
    if (normalized === 'flashcards') return findTodayTaskByTypes(['cards']);
    return null;
  };

  const formatCompactTimer = (seconds) => {
    const safe = Math.max(0, Math.floor(Number(seconds || 0)));
    const mins = Math.floor(safe / 60);
    const secs = safe % 60;
    return `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  };

  const formatSrsIntervalHint = (seconds) => {
    const safeSeconds = Number(seconds);
    if (!Number.isFinite(safeSeconds) || safeSeconds < 0) return '';
    if (safeSeconds < 3600) {
      const mins = Math.max(1, Math.ceil(safeSeconds / 60));
      return tr(`${mins} мин`, `${mins} Min`);
    }
    if (safeSeconds < 86400) {
      const hours = Math.max(1, Math.ceil(safeSeconds / 3600));
      return tr(`${hours} ч`, `${hours} Std`);
    }
    const days = Math.max(1, Math.ceil(safeSeconds / 86400));
    return tr(`${days} дн`, `${days} Tg`);
  };

  const getSrsRatingHint = (ratingKey) => {
    const previewSeconds = Number(srsPreview?.[ratingKey]?.seconds);
    if (Number.isFinite(previewSeconds) && previewSeconds >= 0) {
      return formatSrsIntervalHint(previewSeconds);
    }
    if (ratingKey === 'AGAIN') return tr('1 мин', '1 Min');
    if (ratingKey === 'HARD') return tr('5 мин', '5 Min');
    if (ratingKey === 'GOOD') return tr('3 дн', '3 Tg');
    if (ratingKey === 'EASY') return tr('7 дн', '7 Tg');
    return '';
  };

  const toggleTodaySectionTaskTimer = async (sectionKey) => {
    const item = getTodayTaskForSection(sectionKey);
    if (!item || String(item?.status || '').toLowerCase() === 'done') return;
    const nowElapsed = getTodayItemElapsedSeconds(item, Date.now());
    if (isTodayItemTimerRunning(item)) {
      await syncTodayItemTimer(item, 'pause', { elapsedSeconds: nowElapsed, running: false });
      return;
    }
    const hasStartedBefore = nowElapsed > 0 || String(item?.status || '').toLowerCase() === 'doing';
    await syncTodayItemTimer(
      item,
      hasStartedBefore ? 'resume' : 'start',
      { elapsedSeconds: nowElapsed, running: true }
    );
  };

  const renderTodaySectionTaskHud = (sectionKey, options = {}) => {
    const item = getTodayTaskForSection(sectionKey);
    if (!item) return null;
    const inline = Boolean(options?.inline);
    const elapsed = getTodayItemElapsedSeconds(item, todayTimerNowMs);
    const progress = getTodayItemProgressPercent(item, todayTimerNowMs);
    const done = String(item?.status || '').toLowerCase() === 'done' || progress >= 100;
    const running = isTodayItemTimerRunning(item);
    return (
      <div className={`today-section-task-hud ${inline ? 'is-inline' : ''}`.trim()}>
        {done ? (
          <span className="today-section-task-done" title={tr('Задача выполнена', 'Aufgabe erledigt')}>✅</span>
        ) : (
          <button
            type="button"
            className={`reader-timer-pill today-section-timer-pill ${!running ? 'is-paused' : ''}`}
            onClick={() => toggleTodaySectionTaskTimer(sectionKey)}
            title={tr('Пауза/продолжение таймера задачи', 'Aufgaben-Timer pausieren/fortsetzen')}
          >
            {running ? `⏱ ${formatCompactTimer(elapsed)}` : `⏸ ${formatCompactTimer(elapsed)}`}
          </button>
        )}
      </div>
    );
  };

  const ensureFlashcardsTaskTimerRunning = async () => {
    const item = getTodayTaskForSection('flashcards');
    if (!item || String(item?.status || '').toLowerCase() === 'done') return;
    if (isTodayItemTimerRunning(item)) return;
    const elapsedSeconds = getTodayItemElapsedSeconds(item, Date.now());
    const hasStartedBefore = elapsedSeconds > 0 || String(item?.status || '').toLowerCase() === 'doing';
    await syncTodayItemTimer(
      item,
      hasStartedBefore ? 'resume' : 'start',
      { elapsedSeconds, running: true }
    );
  };

  const pauseFlashcardsTaskTimer = async () => {
    const item = getTodayTaskForSection('flashcards');
    if (!item || String(item?.status || '').toLowerCase() === 'done') return;
    if (!isTodayItemTimerRunning(item)) return;
    const elapsedSeconds = getTodayItemElapsedSeconds(item, Date.now());
    await syncTodayItemTimer(item, 'pause', { elapsedSeconds, running: false });
  };

  const prepareTodayTheory = async (item, options = {}) => {
    if (!initData || !item?.id) return;
    const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
    try {
      setTheoryLoading(true);
      setTheoryError('');
      setTheoryFeedback(null);
      setTheoryPracticeOpen(false);
      const response = await fetch('/api/today/theory/prepare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          item_id: item.id,
          item_payload: payload,
          lookback_days: payload.lookback_days || 14,
          force_refresh: Boolean(options?.forceRefresh),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка подготовки теории', 'Fehler beim Vorbereiten der Theorie'));
      }
      const data = await response.json();
      const pack = data?.package && typeof data.package === 'object' ? data.package : null;
      const updatedItem = data?.updated_item && typeof data.updated_item === 'object' ? data.updated_item : null;
      setTheoryPackage(pack);
      setTheoryItemId(Number(item.id));
      setTheoryPracticeAnswers(['', '', '', '', '']);
      if (updatedItem) {
        setTodayPlan((prev) => {
          if (!prev || !Array.isArray(prev.items)) return prev;
          return {
            ...prev,
            items: prev.items.map((planItem) => (planItem.id === updatedItem.id ? { ...planItem, ...updatedItem } : planItem)),
          };
        });
      }
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось подготовить теорию.',
        'Theorie konnte nicht vorbereitet werden.'
      );
      setTheoryError(friendly);
    } finally {
      setTheoryLoading(false);
    }
  };

  const checkTodayTheory = async () => {
    if (!initData || !theoryPackage) return;
    const sentences = Array.isArray(theoryPackage?.practice_sentences) ? theoryPackage.practice_sentences : [];
    if (!sentences.length) {
      setTheoryError(tr('Нет предложений для проверки.', 'Keine Saetze zur Pruefung.'));
      return;
    }
    if (theoryPracticeAnswers.some((item, index) => index < sentences.length && !String(item || '').trim())) {
      setTheoryError(tr('Заполните переводы для всех предложений.', 'Bitte alle Uebersetzungen ausfuellen.'));
      return;
    }
    try {
      setTheoryChecking(true);
      setTheoryError('');
      const response = await fetch('/api/today/theory/check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          item_id: theoryItemId,
          focus: theoryPackage?.focus || {},
          native_sentences: sentences,
          translations: theoryPracticeAnswers.slice(0, sentences.length),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка проверки теории', 'Fehler beim Pruefen der Theorie'));
      }
      const data = await response.json();
      setTheoryFeedback(data?.feedback || null);
      const updatedItem = data?.updated_item && typeof data.updated_item === 'object' ? data.updated_item : null;
      if (updatedItem) {
        setTodayPlan((prev) => {
          if (!prev || !Array.isArray(prev.items)) return prev;
          return {
            ...prev,
            items: prev.items.map((planItem) => (planItem.id === updatedItem.id ? { ...planItem, ...updatedItem } : planItem)),
          };
        });
      }
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось проверить теорию.',
        'Theorie konnte nicht geprueft werden.'
      );
      setTheoryError(friendly);
    } finally {
      setTheoryChecking(false);
    }
  };

  const startTodayTask = async (item) => {
    if (!item?.id) return;
    const startedItem = await updateTodayItemStatus(item.id, 'start');
    const effectiveItem = startedItem || item;
    const taskType = String(effectiveItem?.task_type || '').toLowerCase();
    const shouldStartTimerRunning = taskType !== 'video' && taskType !== 'youtube';
    await syncTodayItemTimer(effectiveItem, 'start', {
      elapsedSeconds: getTodayItemElapsedSeconds(effectiveItem, Date.now()),
      running: shouldStartTimerRunning,
    });
    if (taskType === 'cards') {
      setSelectedSections(new Set(['flashcards']));
      openFlashcardsSetup(flashcardsRef);
      return;
    }
    if (taskType === 'translation') {
      openSingleSectionAndScroll('translations', translationsRef);
      const taskPayload = effectiveItem?.payload && typeof effectiveItem.payload === 'object' ? effectiveItem.payload : {};
      try {
        setWebappLoading(true);
        setWebappError('');
        setFinishMessage('');
        setFinishStatus('idle');
        setTranslationCheckProgress({ active: false, done: 0, total: 0 });
        const response = await fetch(`/api/today/items/${effectiveItem.id}/translation/start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            level: String(taskPayload?.level || selectedLevel || 'c1').toLowerCase(),
          }),
        });
        if (!response.ok) {
          throw new Error(await readApiError(
            response,
            'Ошибка запуска переводов по задаче дня',
            'Fehler beim Start der Tagesaufgabe (Uebersetzung)'
          ));
        }
        const data = await response.json();
        const updatedItem = data?.item && typeof data.item === 'object' ? data.item : null;
        if (updatedItem) {
          setTodayPlan((prev) => {
            if (!prev || !Array.isArray(prev.items)) return prev;
            return {
              ...prev,
              items: prev.items.map((planItem) => (planItem.id === updatedItem.id ? { ...planItem, ...updatedItem } : planItem)),
            };
          });
        }
        const practice = data?.practice || {};
        const levelLabel = String(practice?.level || '').trim().toLowerCase();
        if (levelLabel) {
          setSelectedLevel(levelLabel);
        }
        if (data?.blocked) {
          setFinishMessage(tr(
            'Есть активная сессия. Завершите текущий перевод, чтобы получить новый сет.',
            'Es gibt eine aktive Session. Beende die aktuelle Uebersetzung, um ein neues Set zu erhalten.'
          ));
        }
        await loadSessionInfo();
        await loadSentences();
      } catch (error) {
        setWebappError(`${tr('Ошибка старта', 'Startfehler')}: ${error.message}`);
      } finally {
        setWebappLoading(false);
      }
      return;
    }
    if (taskType === 'theory') {
      openSingleSectionAndScroll('theory', theoryRef);
      prepareTodayTheory(effectiveItem);
      return;
    }
    if (taskType === 'video' || taskType === 'youtube') {
      const payload = effectiveItem?.payload && typeof effectiveItem.payload === 'object' ? effectiveItem.payload : {};
      let videoUrl = String(payload.video_url || '').trim();
      let videoId = String(payload.video_id || '').trim();
      let videoTitle = String(payload.video_title || '').trim();
      try {
        if (initData) {
          const response = await fetch('/api/today/video/recommend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              initData,
              item_id: effectiveItem.id,
              skill_id: payload.skill_id || null,
              skill_title: payload.skill_title || null,
              main_category: payload.main_category || null,
              sub_category: payload.sub_category || null,
              examples: Array.isArray(payload.examples) ? payload.examples.slice(0, 5) : [],
              lookback_days: payload.lookback_days || 7,
            }),
          });
          if (response.ok) {
            const data = await response.json();
            const recommended = data?.video && typeof data.video === 'object' ? data.video : {};
            const updatedItem = data?.updated_item && typeof data.updated_item === 'object' ? data.updated_item : null;
            videoUrl = String(recommended.video_url || videoUrl).trim();
            videoId = String(recommended.video_id || videoId).trim();
            videoTitle = String(recommended.title || videoTitle).trim();
            setTodayPlan((prev) => {
              if (!prev || !Array.isArray(prev.items)) return prev;
              return {
                ...prev,
                items: prev.items.map((planItem) => {
                  if (planItem.id !== effectiveItem.id) return planItem;
                  if (updatedItem) {
                    return { ...planItem, ...updatedItem };
                  }
                  const planPayload = planItem?.payload && typeof planItem.payload === 'object' ? planItem.payload : {};
                  return {
                    ...planItem,
                    payload: {
                      ...planPayload,
                      video_url: videoUrl || planPayload.video_url || null,
                      video_id: videoId || planPayload.video_id || null,
                      video_title: videoTitle || planPayload.video_title || null,
                    },
                  };
                }),
              };
            });
          }
        }
      } catch (error) {
        console.warn('today video recommendation failed', error);
      }
      if (videoUrl) {
        setYoutubeError('');
        setYoutubeInput(videoUrl);
      } else if (videoId) {
        setYoutubeError('');
        setYoutubeInput(`https://youtu.be/${videoId}`);
      } else {
        setYoutubeError(tr('Видео по текущему слабому навыку не найдено. Попробуйте обновить план.', 'Kein passendes Video fuer den aktuellen schwachen Skill gefunden. Bitte Plan aktualisieren.'));
      }
      const backCandidate = Array.from(selectedSections).find((key) => key && key !== 'youtube') || '';
      setYoutubeBackSection(backCandidate);
      openSingleSectionAndScroll('youtube', youtubeRef);
    }
  };

  const submitTodayVideoFeedback = async (item, vote) => {
    if (!initData || !item?.id) return;
    const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
    const recommendationId = Number(payload?.recommendation_id || 0);
    if (!recommendationId) {
      setTodayPlanError(tr(
        'Сначала нажмите "Начать", чтобы подобрать видео для оценки.',
        'Bitte zuerst "Starten" druecken, damit ein Video zur Bewertung gewaehlt wird.'
      ));
      return;
    }
    const actionKey = vote === 'like' ? 'vote_like' : 'vote_dislike';
    try {
      setTodayItemLoading((prev) => ({ ...prev, [item.id]: actionKey }));
      setTodayPlanError('');
      const response = await fetch('/api/today/video/feedback', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          item_id: item.id,
          recommendation_id: recommendationId,
          vote,
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения оценки видео', 'Fehler beim Speichern der Videobewertung'));
      }
      const data = await response.json();
      const updated = data?.updated_item || null;
      const recommendation = data?.recommendation || null;
      setTodayPlan((prev) => {
        if (!prev || !Array.isArray(prev.items)) return prev;
        return {
          ...prev,
          items: prev.items.map((planItem) => {
            if (planItem.id !== item.id) return planItem;
            if (updated) {
              return { ...planItem, ...updated };
            }
            const planPayload = planItem?.payload && typeof planItem.payload === 'object' ? planItem.payload : {};
            return {
              ...planItem,
              payload: {
                ...planPayload,
                video_likes: Number(recommendation?.like_count || planPayload.video_likes || 0),
                video_dislikes: Number(recommendation?.dislike_count || planPayload.video_dislikes || 0),
                video_score: Number(recommendation?.score || planPayload.video_score || 0),
                video_user_vote: vote === 'like' ? 1 : -1,
              },
            };
          }),
        };
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось сохранить оценку видео.',
        'Videobewertung konnte nicht gespeichert werden.'
      );
      setTodayPlanError(friendly);
    } finally {
      setTodayItemLoading((prev) => {
        const next = { ...prev };
        delete next[item.id];
        return next;
      });
    }
  };

  const getTodayItemTitle = (item) => {
    const taskType = String(item?.task_type || '').toLowerCase();
    const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
    const mode = String(payload?.mode || '').toLowerCase();

    if (taskType === 'cards') {
      if (mode === 'cards_new') return tr('Карточки: новые слова', 'Karten: neue Woerter');
      return tr('Карточки: повторение', 'Karten: Wiederholung');
    }
    if (taskType === 'translation') {
      const sentences = Number(payload?.sentences || 5);
      return tr(`Перевод: ${sentences} предложений`, `Uebersetzung: ${sentences} Saetze`);
    }
    if (taskType === 'theory') {
      const topic = String(payload?.sub_category || payload?.skill_title || payload?.main_category || '').trim();
      return topic ? tr(`Теория: ${topic}`, `Theorie: ${topic}`) : tr('Теория', 'Theorie');
    }
    if (taskType === 'video' || taskType === 'youtube') {
      const focusTopic = String(
        payload?.sub_category
        || payload?.skill_title
        || payload?.main_category
        || ''
      ).trim();
      if (focusTopic) {
        return tr(`Видео`, `Video`);
      }
      return tr(`Видео`, `Video`);
    }
    if (taskType === 'dialogue') {
      const minutes = Number(item?.estimated_minutes || 3);
      return tr(`Диалог: ${minutes} минут`, `Dialog: ${minutes} Minuten`);
    }
    return item?.title || tr('Задача', 'Aufgabe');
  };

  useEffect(() => {
    const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
    const hasRunning = items.some((item) => isTodayItemTimerRunning(item));
    if (!hasRunning) return undefined;
    const intervalId = window.setInterval(() => {
      setTodayTimerNowMs(Date.now());
    }, 1000);
    return () => window.clearInterval(intervalId);
  }, [todayPlan]);

  useEffect(() => {
    const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
    items.forEach((item) => {
      if (!item?.id) return;
      const status = String(item?.status || '').toLowerCase();
      if (status === 'done') {
        todayTimerCompletionLockRef.current.delete(item.id);
        return;
      }
      if (!isTodayItemTimerRunning(item)) {
        return;
      }
      const goal = getTodayItemGoalSeconds(item);
      if (goal <= 0) return;
      const elapsed = getTodayItemElapsedSeconds(item, todayTimerNowMs);
      if (elapsed < goal) return;
      if (todayTimerCompletionLockRef.current.has(item.id)) return;
      todayTimerCompletionLockRef.current.add(item.id);
      syncTodayItemTimer(item, 'sync', { elapsedSeconds: elapsed, running: false })
        .finally(() => {
          todayTimerCompletionLockRef.current.delete(item.id);
        });
    });
  }, [todayPlan, todayTimerNowMs]);

  const drainSrsReviewBuffer = useCallback(async () => {
    if (!initData || srsReviewDrainInFlightRef.current) return;
    srsReviewDrainInFlightRef.current = true;
    try {
      while (srsReviewBufferRef.current.length > 0) {
        const job = srsReviewBufferRef.current[0];
        const FSRS_REVIEW_TIMEOUT_MS = 60000;
        try {
          let response = await fetchWithTimeout('/api/cards/review', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              initData,
              card_id: job.cardId,
              rating: job.rating,
              response_ms: job.responseMs,
            }),
          }, FSRS_REVIEW_TIMEOUT_MS);
          if (!response.ok && response.status >= 500) {
            await new Promise((resolve) => setTimeout(resolve, 220));
            response = await fetchWithTimeout('/api/cards/review', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                initData,
                card_id: job.cardId,
                rating: job.rating,
                response_ms: job.responseMs,
              }),
            }, FSRS_REVIEW_TIMEOUT_MS);
          }
          if (!response.ok) {
            throw new Error(await readApiError(response, 'Ошибка SRS review', 'Fehler bei SRS-Review'));
          }
          srsReviewBufferRef.current.shift();
        } catch (error) {
          job.attempt = Number(job.attempt || 0) + 1;
          const friendly = normalizeNetworkErrorMessage(error, 'Не удалось сохранить оценку.', 'Bewertung konnte nicht gespeichert werden.');
          setSrsError(friendly);
          setWebappError(`${tr('Ошибка SRS review', 'Fehler bei SRS-Review')}: ${friendly}`);
          clearSrsReviewRetryTimer();
          const delayMs = Math.min(15000, 1200 * (2 ** Math.max(0, job.attempt - 1)));
          srsReviewRetryTimerRef.current = window.setTimeout(() => {
            srsReviewRetryTimerRef.current = null;
            void drainSrsReviewBuffer();
          }, delayMs);
          break;
        }
      }
    } finally {
      srsReviewDrainInFlightRef.current = false;
    }
  }, [
    clearSrsReviewRetryTimer,
    fetchWithTimeout,
    initData,
    normalizeNetworkErrorMessage,
    readApiError,
    tr,
  ]);

  const submitSrsReview = async (ratingValue) => {
    if (!initData) {
      setSrsError(tr('Сессия Telegram не найдена. Откройте mini app через Telegram.', 'Telegram-Sitzung nicht gefunden. Bitte ueber Telegram oeffnen.'));
      return;
    }
    const cardId = srsCard?.id || srsCard?.entry_id;
    if (!cardId) {
      setSrsError(tr('У карточки нет идентификатора. Обновите экран.', 'Karten-ID fehlt. Bitte Bildschirm aktualisieren.'));
      return;
    }
    const startedAt = srsShownAtRef.current || Date.now();
    const responseMs = Math.max(0, Date.now() - startedAt);
    if (ratingValue === 'EASY' && srsEasyLocked) return;
    if (ratingValue === 'GOOD' && srsGoodLocked) return;
    try {
      stopTtsPlayback();
      setSrsError('');
      setSrsSubmitting(true);
      setSrsSubmittingRating(ratingValue);
      const optimisticCard = takeFromSrsPrefetchQueue();
      setSrsRevealAnswer(false);
      setSrsRevealStartedAt(0);
      setSrsRevealElapsedSec(0);
      decrementSrsQueueInfoLocal();
      if (optimisticCard) {
        applySrsPayload({
          card: optimisticCard,
          srs: null,
          srs_preview: null,
          queue_info: null,
        });
        srsReviewBufferRef.current.push({
          cardId,
          rating: ratingValue,
          responseMs,
          attempt: 0,
        });
        void drainSrsReviewBuffer();
        if (srsPrefetchQueueRef.current.length <= 2) {
          void prefetchSrsCards();
        }
        return;
      }

      const FSRS_REVIEW_TIMEOUT_MS = 60000;
      let response = await fetchWithTimeout('/api/cards/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          card_id: cardId,
          rating: ratingValue,
          response_ms: responseMs,
        }),
      }, FSRS_REVIEW_TIMEOUT_MS);
      if (!response.ok && response.status >= 500) {
        await new Promise((resolve) => setTimeout(resolve, 220));
        response = await fetchWithTimeout('/api/cards/review', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            card_id: cardId,
            rating: ratingValue,
            response_ms: responseMs,
          }),
        }, FSRS_REVIEW_TIMEOUT_MS);
      }
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка SRS review', 'Fehler bei SRS-Review'));
      }
      const data = await response.json();
      if (data?.next && typeof data.next === 'object') {
        applySrsPayload(data.next);
      } else {
        await loadSrsNextCard();
      }
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось сохранить оценку.', 'Bewertung konnte nicht gespeichert werden.');
      setSrsError(friendly);
      setWebappError(`${tr('Ошибка SRS review', 'Fehler bei SRS-Review')}: ${friendly}`);
      try {
        await loadSrsNextCard();
      } catch (_) {
        // noop: preserve original review error message
      }
    } finally {
      setSrsSubmitting(false);
      setSrsSubmittingRating(null);
    }
  };

  useEffect(() => {
    if (!srsRevealAnswer || !srsCard) {
      setSrsRevealElapsedSec(0);
      return undefined;
    }
    const startedAt = srsRevealStartedAt || Date.now();
    if (!srsRevealStartedAt) {
      setSrsRevealStartedAt(startedAt);
    }
    setSrsRevealElapsedSec(Math.max(0, Math.floor((Date.now() - startedAt) / 1000)));
    const intervalId = window.setInterval(() => {
      setSrsRevealElapsedSec(Math.max(0, Math.floor((Date.now() - startedAt) / 1000)));
    }, 250);
    return () => {
      window.clearInterval(intervalId);
    };
  }, [srsRevealAnswer, srsCard, srsRevealStartedAt]);

  useEffect(() => {
    if (!srsRevealAnswer || !srsCard) return;
    const key = String(srsCard?.id || srsCard?.entry_id || '');
    if (!key) return;
    if (srsAutoTtsPlayedRef.current === key) return;
    const answerText = getDictionarySourceTarget(
      srsCard,
      (srsCard?.source_lang || 'ru') === 'de' ? 'de-ru' : 'ru-de'
    ).targetText || '';
    const langCode = detectTtsLangFromText(answerText);
    const locale = getTtsLocaleForLang(langCode);
    srsAutoTtsPlayedRef.current = key;
    let timerId = null;
    const rafId = window.requestAnimationFrame(() => {
      timerId = window.setTimeout(() => {
        void playTts(answerText, locale);
      }, 0);
    });
    return () => {
      window.cancelAnimationFrame(rafId);
      if (timerId) {
        window.clearTimeout(timerId);
      }
    };
  }, [srsRevealAnswer, srsCard, dictionaryDirection, languageProfile?.learning_language, languageProfile?.native_language]);

  const normalizeStarterDictionaryOffer = useCallback((value) => {
    const payload = value && typeof value === 'object' ? value : {};
    const stateRaw = payload.state && typeof payload.state === 'object' ? payload.state : {};
    const state = {
      decision_status: String(stateRaw.decision_status || 'pending').trim().toLowerCase() || 'pending',
      source_user_id: Number(stateRaw.source_user_id || 0) || null,
      template_version: String(stateRaw.template_version || '').trim() || null,
      source_lang: String(stateRaw.source_lang || '').trim().toLowerCase() || null,
      target_lang: String(stateRaw.target_lang || '').trim().toLowerCase() || null,
      last_imported_count: Math.max(0, Number(stateRaw.last_imported_count || 0) || 0),
      decided_at: String(stateRaw.decided_at || '').trim() || null,
      last_imported_at: String(stateRaw.last_imported_at || '').trim() || null,
      updated_at: String(stateRaw.updated_at || '').trim() || null,
    };
    return {
      enabled: Boolean(payload.enabled),
      source_user_id: Number(payload.source_user_id || 0) || 0,
      template_version: String(payload.template_version || '').trim() || '',
      import_limit: Math.max(1, Number(payload.import_limit || 0) || 1000),
      folder_name: String(payload.folder_name || '').trim() || 'Базовый словарь',
      source_lang: String(payload.source_lang || '').trim().toLowerCase() || '',
      target_lang: String(payload.target_lang || '').trim().toLowerCase() || '',
      has_profile: Boolean(payload.has_profile),
      user_pair_total: Math.max(0, Number(payload.user_pair_total || 0) || 0),
      template_total: Math.max(0, Number(payload.template_total || 0) || 0),
      suggested_count: Math.max(0, Number(payload.suggested_count || 0) || 0),
      should_prompt: Boolean(payload.should_prompt),
      can_reconnect: Boolean(payload.can_reconnect),
      state,
    };
  }, []);

  const loadStarterDictionaryStatus = useCallback(async () => {
    if (!initData) return;
    try {
      const response = await fetch('/api/webapp/starter-dictionary/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки базового словаря', 'Fehler beim Laden des Basiswoerterbuchs'));
      }
      const data = await response.json();
      const offer = normalizeStarterDictionaryOffer(data?.offer);
      setStarterDictionaryOffer(offer);
      setStarterDictionaryPromptOpen(Boolean(offer?.should_prompt));
    } catch (_error) {
      // Silent: starter dictionary is optional and should not block app bootstrap.
    }
  }, [initData, normalizeStarterDictionaryOffer, readApiError]);

  const applyStarterDictionaryDecision = useCallback(async (accept, { forceReimport = false, closePromptOnSuccess = true } = {}) => {
    if (!initData) {
      setStarterDictionaryActionError(initDataMissingMsg);
      return;
    }
    try {
      setStarterDictionaryActionLoading(true);
      setStarterDictionaryActionError('');
      setStarterDictionaryActionMessage('');
      const response = await fetch('/api/webapp/starter-dictionary/apply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          action: accept ? 'accept' : 'decline',
          force_reimport: Boolean(forceReimport),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка базового словаря', 'Fehler beim Basiswoerterbuch'));
      }
      const data = await response.json();
      const offer = normalizeStarterDictionaryOffer(data?.offer);
      setStarterDictionaryOffer(offer);
      if (accept) {
        const inserted = Math.max(0, Number(data?.import_result?.inserted_count || 0) || 0);
        const folderName = String(data?.import_result?.folder?.name || offer?.folder_name || '').trim();
        const message = inserted > 0
          ? tr(`Базовый словарь подключён: +${inserted} записей${folderName ? ` (${folderName})` : ''}.`, `Basiswoerterbuch verbunden: +${inserted} Eintraege${folderName ? ` (${folderName})` : ''}.`)
          : tr('Базовый словарь уже подключён. Новых записей не найдено.', 'Basiswoerterbuch ist bereits verbunden. Keine neuen Eintraege gefunden.');
        setStarterDictionaryActionMessage(message);
      } else {
        setStarterDictionaryActionMessage(tr('Ок, начинаем с пустого словаря.', 'Alles klar, wir starten mit einem leeren Woerterbuch.'));
      }
      if (closePromptOnSuccess) {
        setStarterDictionaryPromptOpen(false);
      }
    } catch (error) {
      setStarterDictionaryActionError(String(error?.message || tr('Не удалось применить действие по базовому словарю.', 'Aktion fuer Basiswoerterbuch fehlgeschlagen.')));
    } finally {
      setStarterDictionaryActionLoading(false);
    }
  }, [initData, initDataMissingMsg, normalizeStarterDictionaryOffer, readApiError, tr]);

  const loadLanguageProfile = async () => {
    if (!initData) return;
    try {
      setLanguageProfileLoading(true);
      setLanguageProfileError('');
      const response = await fetch('/api/user/language-profile', {
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
          // ignore parse errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      const profile = data?.profile || null;
      setLanguageProfile(profile);
      if (profile) {
        setLanguageProfileDraft({
          learning_language: profile.learning_language || 'de',
          native_language: profile.native_language || 'ru',
        });
      }
    } catch (error) {
      setLanguageProfileError(`${tr('Ошибка профиля языка', 'Sprachprofil-Fehler')}: ${error.message}`);
    } finally {
      setLanguageProfileLoading(false);
    }
  };

  const saveLanguageProfile = async () => {
    if (!initData) return;
    try {
      setLanguageProfileSaving(true);
      setLanguageProfileError('');
      const response = await fetch('/api/user/language-profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          learning_language: languageProfileDraft.learning_language,
          native_language: languageProfileDraft.native_language,
        }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parse errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      if (data?.profile) {
        const prevPair = `${languageProfile?.native_language || 'ru'}-${languageProfile?.learning_language || 'de'}`;
        setLanguageProfile(data.profile);
        const nextPair = `${data.profile.native_language || 'ru'}-${data.profile.learning_language || 'de'}`;
        const pairChanged = prevPair !== nextPair || Boolean(data.reset_sessions);
        if (pairChanged) {
          setSrsCard(null);
          setSrsState(null);
          setSrsQueueInfo({ due_count: 0, new_remaining_today: 0 });
          setSrsRevealAnswer(false);
          setSrsError('');
          setSessionType('none');
          setSentences([]);
          setResults([]);
          setTranslationAudioGrammarOptIn({});
          setTranslationAudioGrammarSaving({});
          setExplanations({});
          if (translationDraftStorageTimeoutRef.current) {
            clearTimeout(translationDraftStorageTimeoutRef.current);
            translationDraftStorageTimeoutRef.current = null;
          }
          if (translationDraftSyncTimeoutRef.current) {
            clearTimeout(translationDraftSyncTimeoutRef.current);
            translationDraftSyncTimeoutRef.current = null;
          }
          translationDraftsRef.current = {};
          setTranslationDrafts({});
          safeStorageRemove(translationDraftStorageKey);
          void clearTranslationDraftsOnServer([], { clearAll: true, silent: true });
          await loadSessionInfo();
          await loadSentences();
          if (!flashcardsOnly && isSectionVisible('flashcards')) {
            await loadSrsNextCard();
          }
        }
        await loadStarterDictionaryStatus();
        if (!needsLanguageProfileChoice) {
          setLanguageProfileModalOpen(false);
        }
      }
    } catch (error) {
      setLanguageProfileError(`${tr('Ошибка сохранения профиля', 'Fehler beim Speichern des Profils')}: ${error.message}`);
    } finally {
      setLanguageProfileSaving(false);
    }
  };

  const openLanguageProfileModal = () => {
    setLanguageProfileError('');
    setLanguageProfileDraft({
      learning_language: languageProfile?.learning_language || 'de',
      native_language: languageProfile?.native_language || 'ru',
    });
    setLanguageProfileModalOpen(true);
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
  const youtubeSectionVisible = isSectionVisible('youtube');
  const dictionarySectionVisible = isSectionVisible('dictionary');
  const supportSectionVisible = !flashcardsOnly && isSectionVisible('support');
  const isSkillTrainingReady = Boolean(skillTrainingData?.package);

  const isHomeScreen = !flashcardsOnly && selectedSections.size === 0;
  const isGuideScreen = !flashcardsOnly && selectedSections.size === 1 && selectedSections.has('guide');
  const showHomeGuideQuickCard = isHomeScreen && !guideQuickCardDismissed;
  const onboardingSlides = useMemo(() => ([
    {
      eyebrow: tr('Шаг 1 из 6', 'Schritt 1 von 6'),
      title: tr('Начните с переводов', 'Starte mit Uebersetzungen'),
      body: tr(
        'Выберите тему и уровень, переведите предложения и сразу читайте разбор ошибок. Это главный вход в грамматику и структуру языка.',
        'Waehle Thema und Niveau, uebersetze Saetze und lies sofort die Fehleranalyse. Das ist dein Haupteinstieg in Grammatik und Satzstruktur.'
      ),
      bullets: [
        tr('Тема задаёт контекст упражнений.', 'Das Thema gibt den Kontext der Uebungen vor.'),
        tr('Уровень влияет на сложность предложений.', 'Das Niveau steuert die Schwierigkeit der Saetze.'),
        tr('После проверки разбирайте ошибки, а не только балл.', 'Nach dem Pruefen zaehlt nicht nur der Score, sondern die Fehleranalyse.'),
      ],
    },
    {
      eyebrow: tr('Шаг 2 из 6', 'Schritt 2 von 6'),
      title: tr('Сохраняйте слова и повторяйте их', 'Speichere Woerter und wiederhole sie'),
      body: tr(
        'Полезные слова и выражения отправляйте в словарь, а затем закрепляйте их через карточки и FSRS-повторение.',
        'Speichere nuetzliche Woerter und Ausdruecke im Woerterbuch und festige sie dann mit Karten und FSRS-Wiederholung.'
      ),
      bullets: [
        tr('Словарь хранит лексику и формы слова.', 'Das Woerterbuch speichert Wortschatz und Wortformen.'),
        tr('Карточки закрепляют слова в памяти.', 'Karten verankern Woerter im Gedaechtnis.'),
        tr('FSRS сам рассчитывает интервалы повторения.', 'FSRS berechnet die Wiederholungsabstaende automatisch.'),
      ],
    },
    {
      eyebrow: tr('Шаг 3 из 6', 'Schritt 3 von 6'),
      title: tr('Переходите к живому немецкому', 'Wechsle zu echtem Deutsch'),
      body: tr(
        'YouTube и Reader нужны, чтобы видеть язык в реальном контексте: видео, субтитры, тексты, аудио и чтение.',
        'YouTube und Reader zeigen dir die Sprache im echten Kontext: Videos, Untertitel, Texte, Audio und Lesen.'
      ),
      bullets: [
        tr('В YouTube можно искать видео и включать субтитры.', 'In YouTube kannst du Videos suchen und Untertitel aktivieren.'),
        tr('По словам из субтитров и текста можно делать быстрый lookup.', 'Woerter aus Untertiteln und Texten kannst du direkt nachschlagen.'),
        tr('Reader подходит для спокойного чтения и аудио.', 'Der Reader eignet sich fuer ruhiges Lesen und Audio.'),
      ],
    },
    {
      eyebrow: tr('Шаг 4 из 6', 'Schritt 4 von 6'),
      title: tr('Прокачивайте слабые места точечно', 'Trainiere gezielt deine Schwachstellen'),
      body: tr(
        'Голосовой ассистент, теория и тренировка навыка помогают закрепить именно те темы, где у вас больше всего ошибок.',
        'Sprachassistent, Theorie und Skill-Training helfen dir genau bei den Themen, in denen du die meisten Fehler machst.'
      ),
      bullets: [
        tr('Голосовой ассистент нужен для разговорной практики.', 'Der Sprachassistent ist fuer muendliche Praxis da.'),
        tr('Skill-Training собирает теорию, видео и упражнения в одном месте.', 'Skill-Training kombiniert Theorie, Video und Uebungen an einem Ort.'),
        tr('Даже 10–15 минут в день дают стабильный прогресс.', 'Schon 10 bis 15 Minuten pro Tag bringen stabilen Fortschritt.'),
      ],
    },
    {
      eyebrow: tr('Шаг 5 из 6', 'Schritt 5 von 6'),
      title: tr('Подписка и управление тарифом', 'Abo und Tarifverwaltung'),
      body: tr(
        'В разделе «Подписка» видны текущий тариф, лимиты и все доступные планы. Там же открывается Stripe Portal для управления оплатой.',
        'Im Bereich „Abo“ siehst du deinen aktuellen Tarif, Limits und alle verfuegbaren Plaene. Dort oeffnest du auch das Stripe-Portal zur Zahlungsverwaltung.'
      ),
      bullets: [
        tr('Free — базовый бесплатный режим с лимитами, Pro — полный режим без дневного лимита; в Pro также можно отправить персональный запрос на доработку под себя.', 'Free ist der kostenlose Modus mit Limits, Pro ist der volle Modus ohne Tageslimit; in Pro kannst du zusaetzlich einen persoenlichen Anpassungswunsch einreichen.'),
        tr('«Кофе» и «Кофе + чизкейк» сейчас работают как альтернативные платные планы (не параллельные add-on).', '„Kaffee“ und „Kaffee + Cheesecake“ sind aktuell alternative bezahlte Plaene (keine parallelen Add-ons).'),
        tr('Кнопка «Управлять подпиской» открывает Stripe Portal: там смена карты, отмена/возобновление и счета; затем возвращайтесь в Mini App.', '„Abo verwalten“ oeffnet das Stripe-Portal: Karte wechseln, kuendigen/reaktivieren und Rechnungen; danach zur Mini App zurueckkehren.'),
      ],
    },
    {
      eyebrow: tr('Шаг 6 из 6', 'Schritt 6 von 6'),
      title: tr('Поддержка и связь с разработчиком', 'Support und direkter Kontakt'),
      body: tr(
        'Если что-то непонятно или возникают технические сложности, пишите в раздел «Поддержка» в меню. Разработчик отвечает максимально быстро.',
        'Wenn etwas unklar ist oder technische Probleme auftreten, schreibe im Menue in den Bereich „Support“. Der Entwickler antwortet so schnell wie moeglich.'
      ),
      bullets: [
        tr('В «Поддержке» можно описать баг, вопрос по функционалу или идею улучшения.', 'Im Bereich „Support“ kannst du Bugs, Funktionsfragen oder Verbesserungsideen schicken.'),
        tr('Чем точнее описание (скриншот, шаги, где именно), тем быстрее решение.', 'Je genauer die Beschreibung (Screenshot, Schritte, konkreter Bereich), desto schneller die Loesung.'),
        tr('Поддержка доступна прямо внутри Mini App, без перехода во внешние сервисы.', 'Support ist direkt in der Mini App verfuegbar, ohne externe Dienste.'),
      ],
    },
  ]), [tr]);
  const guideStepItems = useMemo(() => {
    if (uiLang === 'de') {
      return [
        {
          key: 'platform_access',
          number: '1',
          accent: true,
          title: 'Telegram Mini App oder Website: beides ist vollwertig',
          summary: 'Du kannst dieselbe App mit demselben Funktionsumfang in Telegram oder als Website nutzen.',
          sections: [
            {
              title: 'Ein Produkt, zwei Zugänge',
              items: [
                'Die App funktioniert als Telegram Mini App und als Website mit dem gleichen Kern-Funktionsumfang.',
                'Du verlierst beim Wechsel zwischen Telegram und Web keine Lernlogik: Workflows, Inhalte und Fortschritt bleiben konsistent.',
                'Du entscheidest je nach Situation selbst, welche Oberfläche gerade praktischer ist.',
              ],
            },
            {
              title: 'Wann Telegram praktisch ist',
              items: [
                'Für schnellen Einstieg vom Smartphone: Bot öffnen, Aufgabe starten, Ergebnis sofort sehen.',
                'Für Gruppenmodus, Erinnerungen und schnelle Interaktion direkt im Chat.',
                'Für kurze tägliche Sessions unterwegs.',
              ],
            },
            {
              title: 'Wann Website/Browser praktischer ist',
              items: [
                'Auf Tablet oder Computer ist das Lesen, Analysieren und Video-Lernen oft komfortabler wegen größerem Bildschirm.',
                'Für längere YouTube-/Reader-Sessions und detailreiche Infos (Analytik, Theorie, große Textblöcke).',
                'Wenn du ruhig und fokussiert mit viel Inhalt auf einmal arbeiten willst.',
              ],
            },
          ],
        },
        {
          key: 'start_setup',
          number: '1',
          title: 'Start-Einstellungen',
          summary: 'Einmal sauber einrichten: allein lernen oder mit Freunden im Gruppenmodus.',
          sections: [
            {
              title: 'Zwei Lernformate',
              items: [
                'Du kannst die App allein nutzen oder gemeinsam mit Freunden im Gruppenmodus.',
                'Im Modus „Nur ich“ lernst du individuell ohne Gruppen-Ranking.',
                'Im Gruppenmodus vergleichst du Ergebnisse mit Freunden und siehst gemeinsame Statistik.',
              ],
            },
            {
              title: 'So bereitest du den Gruppenmodus vor',
              items: [
                'Erstelle eine Telegram-Gruppe fuer euer Lernen (oder nutze eine bestehende Gruppe).',
                'Fuege den Bot in diese Gruppe hinzu.',
                'Empfohlen: Gib dem Bot Admin-Rechte mit der Berechtigung „Nachrichten anheften“, damit die Bestaetigungsnachricht oben fixiert bleibt.',
                'Nach dem Hinzufuegen sendet der Bot eine Nachricht „Teilnahme bestaetigen“ mit der Taste „✅ Teilnahme bestaetigen“ und versucht, diese Nachricht in der Gruppe anzupinnen.',
              ],
            },
            {
              title: 'Was jeder Teilnehmer machen muss',
              items: [
                'Jeder Teilnehmer tippt in der Gruppe einmal auf „✅ Teilnahme bestaetigen“.',
                'Wer nicht bestaetigt, bleibt zwar in der Gruppe, wird aber nicht im Wettbewerb und Gruppen-Ranking beruecksichtigt.',
                'Zusatz: Jeder Teilnehmer sollte den Bot auch privat einmal starten, damit alle Funktionen im Mini App korrekt verfuegbar sind.',
                'Danach erscheint im Mini App bei Analytics der Gruppenmodus zur Auswahl.',
              ],
            },
          ],
        },
        {
          key: 'translations',
          number: '2',
          title: 'Übersetzungen',
          summary: 'Trainiere Satzbau, Grammatik und Fehleranalyse Schritt für Schritt.',
          sections: [
            {
              title: 'So startest du',
              items: [
                'Wähle zuerst Thema und Niveau und tippe danach auf „Übersetzung starten“.',
                'Danach öffnet die App deinen aktuellen Satzsatz und du gibst deine Übersetzungen direkt in die Felder ein.',
                'Im normalen Modus besteht ein Standardsatz aus 7 Sätzen.',
              ],
            },
            {
              title: 'Teilnahme-Modus: allein oder Gruppe',
              items: [
                'Waehle unbedingt deinen Teilnahme-Modus: „Nur ich“ oder eine konkrete Gruppe.',
                'Wenn die Mini App direkt aus einer Telegram-Gruppe geoeffnet wird, kann die App diese Gruppe automatisch als Kontext nehmen.',
                'Wenn du aus Privat-Chat, Home-Screen-Shortcut oder Browser startest, waehle den Modus bitte bewusst im Analytics-Selector.',
                'Fuer Wettbewerbe mit Freunden: Erstelle eine Lerngruppe (oder nutze eine bestehende) und fuege den Bot in diese Gruppe ein.',
                'Wichtig: Jeder Teilnehmer sollte den Bot auch individuell privat starten, sonst nutzt am Ende nur ein Teil der Gruppe die Funktionen korrekt.',
              ],
            },
            {
              title: 'Während der Übersetzung',
              items: [
                'Wenn dir ein Wort fehlt, kannst du den Wörterbuch-Sprung über den kleinen Pfeil neben dem Satzfeld nutzen.',
                'Du kannst den Bereich „Wörterbuch“ auch parallel geöffnet lassen und dort Wörter nachschlagen, während du weiterübersetzt.',
                'Wenn alle Felder fertig sind, tippst du auf „Prüfen“, um Score, Korrekturen und Feedback zu bekommen.',
              ],
            },
            {
              title: 'Nach dem Prüfen',
              items: [
                'Vor dem Prüfen kannst du die Checkbox aktivieren, damit textuelle Grammatik-Erklärungen zusätzlich in den privaten Bot-Chat gesendet werden.',
                'Nach dem Prüfen siehst du Score, richtige Variante, Erklärungen und später auch den Verlauf über den Button für die Tagesergebnisse.',
                'Am nächsten Tag kann der Bot dir zusätzlich Audio-Erklärungen zu Fehlern schicken, damit du dich besser auf die nächste Runde vorbereitest.',
              ],
            },
            {
              title: 'Wie Sätze, Fehlerdatenbank und Skill-Map funktionieren',
              items: [
                'Standardmäßig werden bis zu 5 Sätze aus deiner persönlichen Fehlerdatenbank und die restlichen Sätze als neue Sätze kombiniert.',
                'Ein Satz landet in der Fehlerdatenbank, wenn dein Ergebnis unter 80 Punkten bleibt.',
                'Ein Satz wird aus der Fehlerdatenbank entfernt, wenn er bereits dort war und du ihn später mit 85 Punkten oder mehr korrekt schaffst.',
                'Die Skill-Map arbeitet nach Fehlerkategorien: Fehler geben in der Regel minus 3 Punkte, korrekte Treffer plus 2 Punkte in der passenden Kategorie.',
              ],
            },
          ],
        },
        {
          key: 'dictionary',
          number: '3',
          title: 'Wörterbuch',
          summary: 'Suche Wörter in beide Richtungen und speichere sie für später.',
          sections: [
            {
              title: 'Was du eingeben kannst',
              items: [
                'Du kannst ein Wort oder eine Phrase sowohl in deiner Muttersprache als auch in der Zielsprache eingeben.',
                'Nach dem Klick auf „Übersetzen“ braucht die App kurz Zeit, um Ergebnis und Zusatzinfos zu laden.',
              ],
            },
            {
              title: 'Was du als Ergebnis bekommst',
              items: [
                'Du bekommst nicht nur die Übersetzung, sondern oft auch Wortformen, Varianten und nützliche Zusatzinformationen.',
                'Viele Einträge lassen sich direkt anhören, damit du Aussprache und Rhythmus mitlernst.',
              ],
            },
            {
              title: 'So speicherst du sinnvoll',
              items: [
                'Nach dem Übersetzen kannst du auf „Speichern“ tippen.',
                'Die App zeigt dir dazu häufige Kombinationen, passende Kollokationen oder Beispielvarianten zum Wort bzw. zur Phrase.',
                'Du kannst eine oder mehrere davon auswählen und in einem Ordner speichern, damit sie später im Training auftauchen.',
              ],
            },
            {
              title: 'Wofür das im Alltag gut ist',
              items: [
                'Das Wörterbuch ist besonders praktisch parallel zu Übersetzungen, YouTube und Reader.',
                'Alles, was du dort speicherst, kann später in Karten und Wiederholung übernommen werden.',
              ],
            },
          ],
        },
        {
          key: 'flashcards',
          number: '4',
          title: 'Karten',
          summary: 'Vier Trainingsmodi für Wiederholung, Erkennen, Bauen und Ergänzen.',
          sections: [
            {
              title: 'Die 4 Modi',
              items: [
                'FSRS: intelligentes spaced repetition für langfristiges Behalten.',
                'Quiz: 4 Antwortoptionen, damit du schnell Bedeutung und Form erkennst.',
                'Blocks: du baust die richtige Antwort aus Teilen auf.',
                'Sentence: ergänzende Kontextpraxis; dieser Modus bleibt bewusst supplemental.',
              ],
            },
            {
              title: 'Welche Einstellungen es gibt',
              items: [
                'Für Quiz, Blocks und Sentence kannst du Set-Größe, Ordner, Geschwindigkeit und automatischen oder manuellen Übergang wählen.',
                'Im Blocks-Modus stellst du zusätzlich den Timer ein: adaptiv, fest oder ohne Timer.',
                'Im Sentence-Modus kannst du die Schwierigkeit wählen: easy, medium oder hard.',
                'Im FSRS-Modus siehst du vor allem Queue-Infos wie „Due“ und „New Today“.',
              ],
            },
            {
              title: 'Wie Karten entstehen',
              items: [
                'Quiz und Blocks ziehen Karten aus derselben FSRS-Kernwarteschlange: due zuerst, danach neue Karten.',
                'Sentence nutzt ergänzendes Material aus Wörterbuch und GPT-Seed-Sätzen und bleibt ein supplemental mode.',
                'Vor dem eigentlichen Training zeigt die App zuerst Karten zur kurzen Orientierung, danach startet die Session.',
                'FSRS nutzt dieselbe Kernwarteschlange direkt als Review-Modus.',
              ],
            },
            {
              title: 'So benutzt du FSRS richtig',
              items: [
                'In FSRS siehst du zuerst die Karte in deiner Muttersprache. Versuche die Übersetzung zuerst laut oder innerlich selbst abzurufen.',
                'Dann drehst du die Karte um und bewertest, wie gut du die Antwort wirklich aus dem Kopf holen konntest.',
                'Again: du konntest nicht antworten oder lagst daneben. Die Karte kommt sehr bald wieder.',
                'Hard: du hast es geschafft, aber mit viel Mühe oder Unsicherheit. Die Wiederholung kommt früher.',
                'Good: du hast korrekt erinnert. Das ist der Standard-Schritt für normales Lernen.',
                'Easy: du wusstest es sofort und sicher. Dann legt FSRS ein längeres Intervall fest.',
              ],
            },
          ],
        },
        {
          key: 'media',
          number: '5',
          title: 'Video, Lesen und Sprechen',
          summary: 'YouTube, Reader und Sprachassistent für echtes Sprachmaterial.',
          sections: [
            {
              title: 'YouTube: Einstieg und Modi',
              items: [
                'Du kannst entweder einen YouTube-Link einfügen oder eine Suchanfrage schreiben. Bei der Suche braucht die App etwas Zeit.',
                'Nach dem Öffnen des Videos drückst du Play und lädst dann die Untertitel.',
                'Der Fullscreen-Button vergrößert den Videobereich, damit Video, Overlay und Untertitel angenehmer lesbar werden.',
                'Im Overlay-Modus liegen die Untertitel direkt auf dem Video. Dort kannst du die Originalsprache und – wenn verfügbar – auch deine Muttersprache sehen.',
                'Beim Tippen auf ein Wort pausiert das Video: Du siehst die Übersetzung, bekommst Zusatzinfos, speicherst das Wort bei Bedarf ins Wörterbuch und setzt mit einem Tap auf den Bildschirm fort.',
              ],
            },
            {
              title: 'Wenn Untertitel nicht automatisch kommen',
              items: [
                'Wenn nach etwa 2 Minuten keine Untertitel erscheinen, kannst du das Transcript auf YouTube manuell kopieren.',
                'Am besten kopierst du die Version mit Zeitmarken und fügst sie in den speziellen Transcript-Block der App ein.',
                'Danach können die Zeilen mit dem Video synchronisiert werden und du kannst wieder die Übersetzung in deiner Muttersprache einschalten.',
              ],
            },
            {
              title: 'Reader: lesen, übersetzen, Audio',
              items: [
                'Im Reader kannst du Text oder Link einfügen oder eine Datei laden und danach ein Dokument aus der Bibliothek öffnen.',
                'Beim Lesen kannst du Wörter oder ganze Sätze antippen und sofort Kontext-Hilfe nutzen.',
                'Die obere Leiste lässt sich einklappen und später wieder aufklappen, wenn du ohne Ablenkung lesen willst.',
                'Zusätzlich kannst du Audio für das ganze Dokument oder für bestimmte Seiten erzeugen.',
              ],
            },
            {
              title: 'Sprachassistent',
              items: [
                'Im Bereich Sprachpraxis verbindest du zuerst den Assistenten und erlaubst Mikrofon-Zugriff.',
                'Danach sprichst du in kurzen Phrasen oder ganzen Antworten und trainierst freies Sprechen in Echtzeit.',
                'Der Assistent ist gut für Aussprache, Reaktionsgeschwindigkeit, freies Formulieren und mündliche Routine.',
              ],
            },
          ],
        },
        {
          key: 'subscription',
          number: '6',
          title: 'Abo und Stripe-Portal',
          summary: 'Verstehe Free/Pro, Support-Tarife und wie der Tarifwechsel wirklich funktioniert.',
          sections: [
            {
              title: 'Was der Bereich „Abo“ zeigt',
              items: [
                'Hier siehst du deinen aktuellen Plan, den Status, den heutigen Verbrauch und dein Tageslimit.',
                'Außerdem sind alle verfügbaren Tarife sichtbar, damit du direkt auswählen kannst, ohne erst das Portal zu öffnen.',
                'Wenn ein Tarif als „nicht verbunden in Stripe“ erscheint, fehlt für diesen Plan die Stripe-Preis-Konfiguration auf dem Server.',
              ],
            },
            {
              title: 'Welche Tarife es gibt',
              items: [
                'Free: kostenloser Basis-Modus mit Limits.',
                'Pro: voller Modus ohne Tageslimit; zusaetzlich kannst du einen persoenlichen Wunsch fuer eine individuelle Funktionsanpassung anfragen.',
                'Support „Kaffee“ und „Kaffee + Cheesecake“ sind aktuell alternative bezahlte Tarife und ersetzen den aktiven bezahlten Tarif, statt parallel als Add-on zu laufen.',
              ],
            },
            {
              title: 'Was passiert bei Tarifwechsel',
              items: [
                'Wenn bereits bezahlte Abos aktiv sind, werden alle auf „läuft bis Periodenende“ gestellt und der neue Tarif startet zum naechsten Abrechnungszeitpunkt.',
                'Dadurch gibt es keine doppelte Sofortbelastung mitten im laufenden Zyklus.',
                'Wenn du denselben aktiven Tarif noch einmal auswählst, bekommst du eine Meldung und verwaltest ihn stattdessen im Portal.',
              ],
            },
            {
              title: 'Button „Abo im Stripe-Portal verwalten“',
              items: [
                'Der Button öffnet Stripe Billing Portal (Domain: billing.stripe.com) für deinen Customer-Kontext.',
                'Dort kannst du Karte wechseln, Abo kündigen/reaktivieren und Rechnungen einsehen.',
                'Zurück: im Portal auf Rückkehr zur App tippen oder Browser/WebView schließen und die Mini App erneut öffnen, dann wird der Status im Abo-Bereich aktualisiert.',
              ],
            },
            {
              title: 'Wichtige Hinweise',
              items: [
                'Nach Checkout oder Portal-Rückkehr immer kurz 1–3 Sekunden warten und den Abo-Bereich aktualisieren.',
                'Wenn ein Klick auf Tarif nichts macht, zuerst prüfen: Plan ist in Stripe aktiv, Button ist nicht disabled, und es gibt keine Netzwerk-/InitData-Fehler.',
                'Wenn du alle Tarife für bestehende Pro-Nutzer sichtbar halten willst, muss die Tarifliste immer aus `/api/billing/plans` geladen werden und nicht nur aus Stripe-Portal-UI.',
              ],
            },
          ],
        },
        {
          key: 'bot_translator_share',
          number: '7',
          title: 'Bot als Schnell-Übersetzer',
          summary: 'Markiere Text in jeder App, teile ihn an den Bot und hole dir später die Auswertung an einem Ort.',
          sections: [
            {
              title: 'Was der Bot hier zusätzlich kann',
              items: [
                'Der Bot ist nicht nur Teil der Mini App, sondern auch ein direkter Übersetzer im Alltag.',
                'Du kannst einzelne Wörter, Phrasen oder ganze Sätze aus jedem beliebigen Text markieren und an den Bot teilen.',
                'Das funktioniert wie normales Weiterleiten an einen Freund: einfach „Teilen“ und den Bot auswählen.',
              ],
            },
            {
              title: 'Wie der Ablauf praktisch ist',
              items: [
                'Du kannst mehrere Phrasen nacheinander an den Bot schicken, ohne jedes Mal sofort alles zu bearbeiten.',
                'Danach öffnest du den Bot einmal und wählst die Übersetzungsrichtung: zum Beispiel DE→RU, DE→EN oder RU→DE.',
                'So bleibt es flexibel, falls du nicht immer in dieselbe Zielsprache übersetzen willst.',
              ],
            },
            {
              title: 'Was du als Ergebnis bekommst',
              items: [
                'Der Bot gibt dir die Übersetzung der Phrase oder des Satzes.',
                'Zusätzlich zeigt er passende Begleit-Ausdrücke und nützliche Varianten zum Kontext.',
                'Ein Teil dieser Varianten kann direkt in dein Wörterbuch übernommen werden, damit du sie später gezielt trainierst.',
              ],
            },
          ],
        },
        {
          key: 'extra_features',
          number: '8',
          title: 'Zusatzfunktionen (angenehme Extras)',
          summary: 'Quiz, tiefe Analytik, Audio-Fehlerfeedback, Ziele, Empfehlungen und Ranking auf einen Blick.',
          sections: [
            {
              title: 'Quiz im Chat',
              items: [
                'Der Bot sendet Quizfragen direkt in den Chat/Gruppen-Thread, damit du schnell trainierst, ohne die Mini App jedes Mal zu öffnen.',
                'In der Regel ist die Aufgabe in deiner Basissprache, und die Antwortoptionen sind in der Zielsprache.',
                'Quiz-Ergebnisse fließen in Fortschritt, Fehleranalyse und spätere Skill-Trainingsvorschläge ein.',
              ],
            },
            {
              title: 'Detaillierte Analytik + Erinnerungen',
              items: [
                'Es gibt Analytik sowohl in der Mini App als auch im Bot-Chat: tägliche/wochentliche Metriken, Erfolgsquote, Zeit und Fortschritt.',
                'Erinnerungen kommen in den Chat (und bei Gruppenmodus auch in die Gruppe): was heute noch offen ist, was bereits erledigt wurde und was Priorität hat.',
                'Die Tagesplan-Karten in der Mini App sind mit diesen Erinnerungen synchron: Status „todo/doing/done“ wird gemeinsam aktualisiert.',
              ],
            },
            {
              title: 'Audio-Fehleranalyse',
              items: [
                'Nach abgeschlossenen Aufgaben kann der Bot Audio-Erklärungen zu typischen Fehlern senden, damit du die Schwachstellen schneller schließt.',
                'Diese Audio-Hinweise ergänzen die Text-Erklärungen und helfen besonders bei Aussprache, Intonation und Sprachgefühl.',
                'Die gleichen Fehlerkategorien werden anschließend in Skill-Map und Trainingsaufgaben berücksichtigt.',
              ],
            },
            {
              title: 'YouTube-Empfehlungen + persönliche Ziele',
              items: [
                'Die App kann relevante YouTube-Videos nach deinem aktuellen Skill/Thema empfehlen und den Fortschritt zu Video-Aufgaben verfolgen.',
                'Persönliche Ziele stellst du im Bereich Analytics im Block Wochenplan ein: Übersetzungen, Wörter, Agent-Minuten und Lese-Minuten.',
                'Danach priorisiert der Tagesplan Aufgaben in Richtung deiner Ziele und zeigt Abweichung Soll/Ist.',
              ],
            },
            {
              title: '„Fühle das Wort“ + Turnier-Tabelle',
              items: [
                'Die Funktion „Fühle das Wort“ stärkt Sprachgefühl: Kontext, Nuancen, typische Kollokationen und natürliche Formulierungen.',
                'Im Gruppenmodus gibt es eine Turnier-/Ranking-Ansicht mit Vergleich zwischen Teilnehmern nach Score und Aktivität.',
                'In der Gruppenstatistik erscheinen nur Nutzer, die ihre Teilnahme in der Gruppe bestätigt haben.',
              ],
            },
            {
              title: 'Was oft vergessen wird (wichtig)',
              items: [
                'Jeder Gruppen-Teilnehmer sollte den Bot einmal im Privat-Chat starten, sonst funktionieren einzelne Features bei manchen Nutzern unvollständig.',
                'Nach Zahlung, Portal-Rückkehr oder Tarifwechsel immer kurz warten (1–3 Sekunden) und Status neu laden.',
                'Wenn eine Funktion „still“ wirkt (kein Klick-Effekt), zuerst Netzwerk, initData und aktuellen Bereichskontext prüfen (Privat vs. Gruppe).',
              ],
            },
          ],
        },
        {
          key: 'support_help',
          number: '9',
          title: 'Support und Hilfe vom Entwickler',
          summary: 'Wenn etwas technisch unklar ist: nutze den Support-Bereich direkt im Menue.',
          sections: [
            {
              title: 'Wann du Support nutzen solltest',
              items: [
                'Wenn ein Block unklar beschrieben ist oder sich anders verhaelt als erwartet.',
                'Wenn bei dir ein technisches Problem auftritt (Button reagiert nicht, Anzeige ist falsch, Funktion startet nicht).',
                'Wenn du zu einem konkreten Workflow eine zusaetzliche Erklaerung brauchst.',
              ],
            },
            {
              title: 'So bekommst du schneller eine praezise Antwort',
              items: [
                'Gehe im Menue in den Bereich „Support“ und sende dort deine Nachricht direkt an den Entwickler.',
                'Beschreibe kurz: welches Geraet, welcher Bereich, was genau du geklickt hast und was stattdessen passiert ist.',
                'Falls moeglich, haenge Screenshot/Video an: damit kann die Ursache deutlich schneller lokalisiert werden.',
                'Der Entwickler antwortet so schnell wie moeglich und gibt dir entweder sofort eine Loesung oder den naechsten klaren Schritt.',
              ],
            },
          ],
        },
      ];
    }

    return [
      {
        key: 'platform_access',
        number: '1',
        accent: true,
        title: 'Telegram Mini App и сайт: оба варианта полноценные',
        summary: 'Приложение можно использовать и внутри Telegram, и как сайт — с тем же ключевым функционалом.',
        sections: [
          {
            title: 'Один продукт, два формата доступа',
            items: [
              'Вы можете работать через Telegram Mini App или через веб-версию в браузере — логика и основные возможности одинаковые.',
              'При смене формата вы не теряете смысл процесса: тренировки, разделы и прогресс остаются согласованными.',
              'Формат выбирается по удобству пользователя, а не по ограничениям функционала.',
            ],
          },
          {
            title: 'Когда удобнее Telegram',
            items: [
              'Для быстрого старта с телефона: открыть бота, запустить задачу и сразу получить результат.',
              'Для группового режима, напоминаний и быстрой коммуникации прямо в чате.',
              'Для коротких ежедневных сессий «на ходу».',
            ],
          },
          {
            title: 'Когда удобнее сайт / браузер',
            items: [
              'На планшете и компьютере комфортнее смотреть видео, читать длинные тексты и анализировать детали из-за большего экрана.',
              'Для длительных сессий в YouTube/Reader и работы с насыщенными блоками информации (аналитика, теория, подробные разборы).',
              'Если вам важна спокойная, сфокусированная работа с большим объемом контента за один подход.',
            ],
          },
        ],
      },
      {
        key: 'start_setup',
        number: '1',
        title: 'Стартовые настройки',
        summary: 'Один раз настройте формат участия: индивидуально или в группе с друзьями.',
        sections: [
          {
            title: 'Два формата обучения',
            items: [
              'Вы можете учиться в двух режимах: лично («Только я») или вместе с друзьями в группе.',
              'Режим «Только я» нужен для индивидуального обучения без группового рейтинга.',
              'Групповой режим нужен для соревнования, общей статистики и сравнения результатов с друзьями.',
            ],
          },
          {
            title: 'Как правильно подготовить групповой режим',
            items: [
              'Создайте учебную Telegram-группу (или используйте уже существующую) и добавьте туда бота.',
              'Рекомендуется сделать бота администратором с правом «Закреплять сообщения», чтобы важная кнопка всегда была видна вверху группы.',
              'После добавления бот отправит сообщение «Подтвердите участие» с кнопкой «✅ Подтвердить участие» и попытается автоматически закрепить это сообщение в группе.',
            ],
          },
          {
            title: 'Что должен сделать каждый участник',
            items: [
              'Каждый участник группы должен один раз нажать кнопку «✅ Подтвердить участие».',
              'Кто не подтвердит участие, останется в группе, но не будет участвовать в рейтинге и групповой статистике.',
              'Дополнительно: каждому участнику нужно хотя бы один раз открыть бота в личке, чтобы все функции Mini App работали корректно.',
              'После подтверждения в Mini App в аналитике появится доступный групповой режим.',
            ],
          },
          {
            title: 'Как упростить доступ к приложению',
            items: [
              'Чтобы открывать Mini App быстрее, установите иконку приложения на главный экран телефона.',
              'На iPhone это делается через Safari и кнопку Share -> Add to Home Screen; на Android обычно через меню браузера -> Add to Home Screen или Install App.',
              'После этого приложение будет запускаться почти как обычное мобильное приложение, прямо с иконки.',
              'Точный путь лучше посмотреть на видео-инструкции внутри приложения: там это показано наглядно для iPhone и Android.',
            ],
          },
        ],
      },
      {
        key: 'translations',
        number: '2',
        title: 'Переводы',
        summary: 'Тренируйте построение предложений и сразу смотрите разбор ошибок.',
        sections: [
          {
            title: 'Как начать',
            items: [
              'Сначала выберите тему и уровень, затем нажмите «Начать перевод».',
              'После запуска приложение откроет ваш текущий набор предложений, и вы будете вводить переводы прямо в поля.',
              'В обычном режиме стандартный набор состоит из 7 предложений.',
            ],
          },
          {
            title: 'Режим участия: лично или в группе',
            items: [
              'Обязательно выберите режим участия: «Только я» или конкретную группу.',
              'Если Mini App открыт прямо из Telegram-группы, приложение обычно может автоматически взять эту группу как контекст.',
              'Если вы открываете Mini App из лички, с иконки на рабочем столе или из браузера, режим участия нужно выбирать осознанно в selector аналитики.',
              'Чтобы соревноваться и видеть результаты друзей, создайте учебную группу (или используйте уже существующую) и добавьте туда бота.',
              'Важно: каждый друг должен также получить индивидуальный доступ к боту и открыть его в личке, иначе полноценно пользоваться сможет не вся группа.',
            ],
          },
          {
            title: 'Во время перевода',
            items: [
              'Если вы не знаете слово, можно открыть словарь через маленькую стрелку рядом с полем предложения.',
              'Можно держать раздел «Словарь» открытым параллельно и смотреть слова прямо по ходу перевода.',
              'Когда все поля заполнены, нажмите «Проверить перевод», чтобы получить балл, исправления и разбор.',
            ],
          },
          {
            title: 'После проверки',
            items: [
              'Перед проверкой можно поставить галочку, чтобы текстовые объяснения грамматики дополнительно приходили в личку по каждому предложению.',
              'После проверки вы увидите балл, эталонный вариант, объяснения и сможете открыть историю результатов за сегодня той же кнопкой внизу.',
              'На следующий день бот может прислать аудиосообщения с разбором ошибок, чтобы вам было легче подготовиться к следующему переводу.',
            ],
          },
          {
            title: 'Как формируются предложения, ошибки и skill',
            items: [
              'Обычно набор собирается так: до 5 предложений из вашей личной базы ошибок и оставшиеся предложения как новые.',
              'Предложение попадает в базу ошибок, если результат за него ниже 80 баллов.',
              'Предложение удаляется из базы ошибок, если оно уже было там и позже вы перевели его на 85 баллов или выше.',
              'Skill-карта работает по категориям ошибок: за ошибку категория обычно получает −3, а за успешный результат +2.',
            ],
          },
        ],
      },
      {
        key: 'dictionary',
        number: '3',
        title: 'Словарь',
        summary: 'Сохраняйте новые слова, выражения и свои полезные находки.',
        sections: [
          {
            title: 'Что можно вводить',
            items: [
              'Можно вводить как слово или фразу на родном языке, так и слово на языке, который вы учите.',
              'После нажатия на перевод подождите немного: приложению нужно время, чтобы получить результат и дополнительные данные.',
            ],
          },
          {
            title: 'Что показывает словарь',
            items: [
              'Система показывает не только перевод, но и формы слова, варианты употребления и дополнительные подсказки.',
              'Для многих слов можно сразу прослушать произношение.',
            ],
          },
          {
            title: 'Как сохранять',
            items: [
              'После перевода нажмите «Сохранить».',
              'Программа может предложить частые сочетания, коллокации и полезные варианты с этим словом или выражением.',
              'Вы можете выбрать один или несколько вариантов и записать их в словарь, при желании сразу в нужную папку.',
            ],
          },
          {
            title: 'Как использовать дальше',
            items: [
              'Словарь удобно держать рядом с переводами, YouTube и Reader, когда попадаются незнакомые слова.',
              'Всё сохранённое затем можно повторять в карточках и FSRS.',
              'Если словарь в самом начале ещё пустой, но вы хотите сразу запустить Quiz, карточки и тренировку выражений, можно подключить базовый стартовый набор.',
              'Это примерно 1000 слов и выражений из заранее подготовленной разработчиком полезной повседневной выборки.',
              'Важно: набор удобен именно для старта, но контекст и состав не всегда идеально совпадают с личными задачами конкретного пользователя.',
              'Поэтому стартовый словарь лучше воспринимать как базу для разгона, а дальше постепенно расширять его своими словами.',
              'Новые слова потом удобно добавлять из YouTube, Reader, лички с ботом, обычного словаря и блока переводов.',
              'Когда вы наберёте свой словарь, именно эти ваши слова и выражения дальше будут использоваться в карточках, FSRS и quiz-режимах.',
            ],
          },
        ],
      },
      {
        key: 'flashcards',
        number: '4',
        title: 'Карточки',
        summary: 'Закрепляйте лексику через повторение, FSRS и быстрые режимы тренировки.',
        sections: [
          {
            title: '4 режима тренировки',
            items: [
              'FSRS: интервальное повторение для долгой памяти.',
              'Quiz: 4 варианта ответа для быстрой проверки знания слова.',
              'Blocks: сборка правильного ответа из частей.',
              'Sentence: дополнительная контекстная практика; этот режим остаётся supplemental.',
            ],
          },
          {
            title: 'Какие настройки есть',
            items: [
              'Для Quiz, Blocks и Sentence можно выбрать размер набора, папку, скорость и режим перехода: автоматический или ручной.',
              'В режиме Blocks дополнительно настраивается таймер: adaptive, fixed или без таймера.',
              'В режиме Sentence можно выбрать сложность: easy, medium или hard.',
              'В FSRS-режиме показывается очередь карточек: сколько due и сколько new today.',
            ],
          },
          {
            title: 'Как формируются карточки',
            items: [
              'Quiz и Blocks берут карточки из одной общей FSRS-очереди: сначала due, потом новые.',
              'Sentence использует дополнительный материал из словаря и GPT-seed предложений и остаётся supplemental mode.',
              'Перед самой тренировкой приложение сначала показывает карточки для быстрого ознакомления, а затем уже запускает quiz, blocks или sentence session.',
              'FSRS использует ту же общую очередь напрямую как основной review-режим.',
            ],
          },
          {
            title: 'Как работать в FSRS',
            items: [
              'В FSRS вы сначала видите карточку на родном языке. Сначала попробуйте сами произнести перевод вслух или про себя.',
              'Потом переверните карточку и честно оцените, насколько легко вы достали ответ из памяти.',
              'Снова / Again: не смогли ответить или ответили неверно. Карточка вернётся очень скоро.',
              'Сложно / Hard: вспомнили, но с большим усилием или ошибками. Повтор придёт раньше обычного.',
              'Хорошо / Good: вспомнили правильно. Это стандартный шаг нормального обучения.',
              'Легко / Easy: ответ появился сразу и уверенно. Тогда интервал до следующего повтора будет длиннее.',
            ],
          },
        ],
      },
      {
        key: 'media',
        number: '5',
        title: 'Видео, чтение и голос',
        summary: 'Переходите к реальному немецкому: YouTube, Reader и живой разговорный ассистент.',
        sections: [
          {
            title: 'YouTube: как начать',
            items: [
              'Можно вставить ссылку на видео с YouTube или написать поисковый запрос в строке. Если вы ищете по запросу, нужно немного подождать.',
              'После открытия видео нажмите Play, затем загрузите субтитры.',
              'Кнопка разворота на весь экран увеличивает рабочую область, чтобы удобнее смотреть видео, overlay и субтитры.',
              'В режиме Overlay субтитры показываются прямо поверх видео: оригинал и, при наличии, перевод на родной язык.',
              'При нажатии на слово видео ставится на паузу: вы видите перевод, получаете доп. информацию, можете сохранить слово в словарь и продолжить, просто тапнув по любому месту экрана.',
            ],
          },
          {
            title: 'Если субтитры не загрузились',
            items: [
              'Если субтитры не появились примерно за 2 минуты, можно взять транскрипт вручную на странице YouTube.',
              'Лучше копировать вариант с метками времени и вставить его в специальный блок для субтитров в приложении.',
              'После этого строки можно синхронизировать с видео и затем включить отображение субтитров на родном языке.',
            ],
          },
          {
            title: 'Reader: как пользоваться',
            items: [
              'В Reader можно вставить текст или ссылку, загрузить файл и потом открыть документ из библиотеки.',
              'Во время чтения можно нажимать на слова и предложения и получать контекстный перевод или подсказку.',
              'Верхняя панель в Reader сворачивается и разворачивается отдельной кнопкой, если вы хотите читать без лишнего шума.',
              'Также можно создавать аудио для всего текста или для выбранных страниц.',
            ],
          },
          {
            title: 'Разговорная практика',
            items: [
              'В блоке агента сначала подключите ассистента и разрешите микрофон.',
              'Дальше говорите короткими фразами или полными ответами и тренируйте устную речь в реальном времени.',
              'Этот режим нужен для спонтанной речи, произношения, скорости реакции и уверенности в разговоре.',
            ],
          },
          ],
        },
      {
        key: 'subscription',
        number: '6',
        title: 'Подписка и Stripe Portal',
        summary: 'Раздел, где видно все тарифы и как правильно менять план без путаницы.',
        sections: [
          {
            title: 'Что показывает блок «Подписка»',
            items: [
              'Текущий план, статус, расход за сегодня и дневной лимит.',
              'Полный список доступных тарифов внутри Mini App, чтобы не искать их в Stripe вручную.',
              'Если у плана написано «тариф не подключён в Stripe», значит для него не задан или неактивен Stripe Price ID на сервере.',
            ],
          },
          {
            title: 'Какие варианты тарифов есть',
            items: [
              'Free: бесплатный базовый режим с лимитами.',
              'Pro: полный режим без дневного лимита; в Pro также можно отправить персональный запрос на индивидуальную доработку функции.',
              '«Поддержать разработчика: кофе» и «кофе + чизкейк» сейчас работают как альтернативные платные планы, а не как параллельные add-on поверх Pro.',
            ],
          },
          {
            title: 'Как работает смена тарифа',
            items: [
              'Если у вас уже активны платные тарифы, система ставит их на завершение в конце периода и включает новый тариф со следующего биллингового цикла.',
              'Это сделано, чтобы не было двойного списания в середине текущего периода.',
              'Если выбрать тот же активный тариф, сервер вернёт подсказку управлять им через Stripe Portal.',
            ],
          },
          {
            title: 'Кнопка «Управлять подпиской в Stripe Portal»',
            items: [
              'Кнопка открывает Stripe Billing Portal (домен `billing.stripe.com`) для вашего аккаунта.',
              'Там можно: сменить карту, отменить/возобновить подписку, посмотреть счета и платежи.',
              'Чтобы вернуться: нажмите возврат в портале или закройте браузер/WebView и снова откройте Mini App; раздел подписки подтянет актуальный статус.',
            ],
          },
          {
            title: 'Что ещё важно пользователю',
            items: [
              'После оплаты/возврата из портала дайте системе 1–3 секунды и обновите блок подписки.',
              'Если кнопка «Выбрать тариф» не реагирует, проверьте интернет, наличие initData и что тариф реально активен в `/api/billing/plans`.',
              'Даже при активном Pro стоит показывать остальные тарифы в Mini App, иначе пользователь не узнает о доступных вариантах смены.',
            ],
          },
        ],
      },
      {
        key: 'bot_translator_share',
        number: '7',
        title: 'Бот как быстрый переводчик',
        summary: 'Выделяйте текст где угодно, пересылайте боту и разбирайте всё в одном месте.',
        sections: [
          {
            title: 'Что умеет бот дополнительно',
            items: [
              'Сам бот работает не только как часть Mini App, но и как отдельный повседневный переводчик.',
              'Вы можете выделить слово, фразу или целое предложение в любом приложении и переслать это боту.',
              'Это делается так же, как обычная пересылка другу: нажали «Поделиться» и выбрали бота.',
            ],
          },
          {
            title: 'Как это удобно использовать',
            items: [
              'Можно отправить боту сразу несколько фраз подряд, а потом открыть его один раз и обработать всё вместе.',
              'Дальше выберите направление перевода: например DE→RU, DE→EN или RU→DE.',
              'Это важно, потому что перевод нужен не всегда только на русский: иногда удобнее сразу на английский или другой язык.',
            ],
          },
          {
            title: 'Что вы получите на выходе',
            items: [
              'Бот вернёт перевод фразы или предложения.',
              'Дополнительно он покажет несколько полезных сопутствующих выражений и вариантов по контексту.',
              'Часть этих вариантов можно сразу добавить в ваш словарь для дальнейшего изучения в карточках и FSRS.',
            ],
          },
        ],
      },
      {
        key: 'extra_features',
        number: '8',
        title: 'Доп. приятные фишки',
        summary: 'Квизы, детальная аналитика, аудио-разбор ошибок, цели, рекомендации и турнирная таблица в одном месте.',
        sections: [
          {
            title: 'Квизы в чате',
            items: [
              'Бот отправляет квизы прямо в чат/групповой тред, чтобы можно было тренироваться быстро, не открывая каждый раз Mini App.',
              'Обычно условие идёт на вашем базовом языке, а варианты ответа на целевом языке обучения.',
              'Результаты квизов учитываются в прогрессе, ошибках и дальнейших рекомендациях по skill-тренировкам.',
            ],
          },
          {
            title: 'Детальная аналитика + напоминания',
            items: [
              'Аналитика доступна и в Mini App, и в чате бота: дневные/недельные метрики, успешность, время, динамика.',
              'Напоминания приходят в чат (а в групповом режиме также в группу): что по плану ещё не закрыто, что уже сделано и что сейчас приоритетно.',
              'Карточки плана на сегодня внутри Mini App синхронизированы с этими напоминаниями по статусам todo/doing/done.',
            ],
          },
          {
            title: 'Аудио ошибок',
            items: [
              'После завершения задач бот может присылать аудио-разборы типичных ошибок, чтобы быстрее закрывать слабые места.',
              'Это дополняет текстовые объяснения и особенно полезно для произношения, интонации и языкового слуха.',
              'Те же категории ошибок дальше попадают в skill-карту и тренировки.',
            ],
          },
          {
            title: 'Рекомендации YouTube + анализ личных целей',
            items: [
              'Система подбирает релевантные YouTube-видео под ваш текущий skill/тему и отслеживает прогресс видео-задач.',
              'Личные цели настраиваются в разделе Аналитика в блоке «План на неделю»: переводы, слова, минуты агента и минуты чтения.',
              'После установки целей план на сегодня приоритизирует задачи под эти цели и показывает отклонение факт/план.',
            ],
          },
          {
            title: '«Почувствуй слово» + турнирная таблица',
            items: [
              'Функция «Почувствуй слово» помогает прокачивать языковую интуицию: контекст, оттенки, коллокации и естественные формулировки.',
              'В групповом режиме доступна турнирная таблица/рейтинг со сравнением участников по результатам и активности.',
              'В общий рейтинг попадают только пользователи, которые подтвердили участие в группе.',
            ],
          },
          {
            title: 'Что ещё важно и часто забывают',
            items: [
              'Каждому участнику группы нужно хотя бы один раз открыть бота в личке, иначе часть функций у некоторых пользователей может работать неполно.',
              'После оплаты, возврата из Stripe Portal или смены тарифа дайте системе 1–3 секунды и обновите статус.',
              'Если функция визуально не срабатывает, сначала проверьте сеть, initData и контекст запуска (личка или группа).',
            ],
          },
        ],
      },
      {
        key: 'support_help',
        number: '9',
        title: 'Поддержка и помощь разработчика',
        summary: 'Если что-то непонятно технически, пишите в раздел «Поддержка» прямо в меню.',
        sections: [
          {
            title: 'Когда стоит обращаться в поддержку',
            items: [
              'Если в описании блока или функции остались вопросы и нужна дополнительная расшифровка «как это работает».',
              'Если заметили техническую проблему: кнопка не срабатывает, отображение ломается или процесс зависает.',
              'Если хотите уточнить логику конкретного сценария под свой кейс обучения.',
            ],
          },
          {
            title: 'Как быстрее получить точный ответ',
            items: [
              'Откройте в меню раздел «Поддержка» и отправьте сообщение прямо разработчику.',
              'Кратко укажите: устройство, раздел, что именно нажали и какой результат получили.',
              'По возможности добавьте скриншот/видео: так причина находится и исправляется заметно быстрее.',
              'Разработчик отвечает максимально быстро и даёт либо решение сразу, либо чёткий следующий шаг.',
            ],
          },
        ],
      },
    ];
  }, [uiLang]);
  const isYoutubeSelectionMenu = String(selectionType || '').startsWith('youtube_');
  const isTranslationResultSelectionMenu = String(selectionType || '').startsWith('translation_result_');
  const isInlineSelectionMenu = isYoutubeSelectionMenu || isTranslationResultSelectionMenu;
  const isLightTheme = themeMode === 'light';
  const readerHasContent = Boolean(String(readerContent || '').trim());
  const readerDisplayPages = useMemo(() => {
    if (Array.isArray(readerPages) && readerPages.length > 0) {
      return readerPages
        .map((item, index) => ({
          page_number: Number(item?.page_number || index + 1),
          text: String(item?.text || '').trim(),
        }));
    }
    const raw = String(readerContent || '').trim();
    if (!raw) return [];
    const paragraphs = raw.split(/\n{2,}/).map((item) => item.trim()).filter(Boolean);
    const pages = [];
    let buffer = '';
    const limit = 1650;
    for (const paragraph of paragraphs) {
      const candidate = buffer ? `${buffer}\n\n${paragraph}` : paragraph;
      if (candidate.length <= limit) {
        buffer = candidate;
      } else {
        if (buffer) pages.push(buffer);
        if (paragraph.length <= limit) {
          buffer = paragraph;
        } else {
          let rest = paragraph;
          while (rest.length > limit) {
            pages.push(rest.slice(0, limit));
            rest = rest.slice(limit);
          }
          buffer = rest;
        }
      }
    }
    if (buffer) pages.push(buffer);
    return pages.map((text, index) => ({ page_number: index + 1, text }));
  }, [readerPages, readerContent]);
  const readerPageCount = readerDisplayPages.length;
  const supportTimelineItems = useMemo(() => {
    const remote = Array.isArray(supportMessages) ? supportMessages : [];
    const failed = Array.isArray(supportFailedMessages) ? supportFailedMessages : [];
    return [...remote, ...failed].sort((a, b) => {
      const aTime = Date.parse(String(a?.created_at || '')) || 0;
      const bTime = Date.parse(String(b?.created_at || '')) || 0;
      return aTime - bTime;
    });
  }, [supportMessages, supportFailedMessages]);
  const analyticsScopeOptions = useMemo(() => {
    const options = [
      {
        key: 'personal',
        label: tr('Только я', 'Nur ich'),
      },
    ];
    const groups = Array.isArray(analyticsScopeData?.available_groups) ? analyticsScopeData.available_groups : [];
    groups.forEach((item) => {
      const parsedId = Number.parseInt(String(item?.chat_id ?? ''), 10);
      if (!Number.isFinite(parsedId)) return;
      const title = String(item?.chat_title || '').trim();
      options.push({
        key: `group:${parsedId}`,
        label: title ? tr(`Группа: ${title}`, `Gruppe: ${title}`) : tr(`Группа #${parsedId}`, `Gruppe #${parsedId}`),
      });
    });
    if (String(analyticsScopeKey || '').startsWith('group:') && !options.some((item) => item.key === analyticsScopeKey)) {
      const chatId = String(analyticsScopeKey).slice('group:'.length).trim();
      const fallbackId = Number.parseInt(chatId, 10);
      if (Number.isFinite(fallbackId)) {
        options.push({
          key: `group:${fallbackId}`,
          label: tr(`Группа #${fallbackId}`, `Gruppe #${fallbackId}`),
        });
      }
    }
    return options;
  }, [analyticsScopeData, analyticsScopeKey, tr]);
  const analyticsScopeStatusText = useMemo(() => {
    if (String(analyticsScopeKey || '').startsWith('group:')) {
      const selectedOption = analyticsScopeOptions.find((item) => item.key === analyticsScopeKey);
      const selectedLabel = String(selectedOption?.label || '')
        .replace(/^Группа:\s*/i, '')
        .replace(/^Gruppe:\s*/i, '')
        .trim();
      return selectedLabel
        ? tr(`Сейчас показана статистика группы: ${selectedLabel}`, `Aktuell wird Gruppen-Statistik gezeigt: ${selectedLabel}`)
        : tr('Сейчас показана статистика группы.', 'Aktuell wird Gruppen-Statistik gezeigt.');
    }
    return tr('Сейчас показана статистика: только ваша.', 'Aktuell wird nur deine Statistik angezeigt.');
  }, [analyticsScopeKey, analyticsScopeOptions, tr]);
  const analyticsScopeSelectorRequired = Boolean(analyticsScopeData?.selector?.required);
  const readerVisibleText = useMemo(() => {
    if (readerPageCount > 0) {
      return String(readerDisplayPages[Math.max(0, Number(readerCurrentPage || 1) - 1)]?.text || '');
    }
    return String(readerContent || '');
  }, [readerPageCount, readerDisplayPages, readerCurrentPage, readerContent]);
  const readerSegmentationHash = useMemo(() => {
    const value = String(readerVisibleText || '');
    let hash = 0;
    for (let index = 0; index < value.length; index += 1) {
      hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
    }
    return `${String(readerDocumentId || 'no-doc')}:${value.length}:${hash}`;
  }, [readerVisibleText, readerDocumentId]);
  const readerSegmentationLang = useMemo(
    () => normalizeLangCode(readerDetectedLanguage || '') || 'de',
    [readerDetectedLanguage]
  );
  const readerSentencesModel = useMemo(
    () => segmentText(readerVisibleText, readerSegmentationLang),
    [readerVisibleText, readerSegmentationLang, readerSegmentationHash]
  );
  const readerSentenceMap = useMemo(() => {
    const map = new Map();
    readerSentencesModel.forEach((sentence) => {
      map.set(sentence.sid, sentence);
    });
    return map;
  }, [readerSentencesModel]);
  const readerWordMap = useMemo(() => {
    const map = new Map();
    readerSentencesModel.forEach((sentence) => {
      sentence.tokens
        .filter((token) => token.kind === 'word' && token.wid)
        .forEach((token) => {
          map.set(token.wid, { ...token, sid: sentence.sid });
        });
    });
    return map;
  }, [readerSentencesModel]);
  const selectedSentenceIds = useMemo(
    () => new Set(Array.isArray(selectedMeta?.sids) ? selectedMeta.sids : []),
    [selectedMeta]
  );
  const selectedWordIds = useMemo(
    () => new Set(Array.isArray(selectedMeta?.wids) ? selectedMeta.wids : []),
    [selectedMeta]
  );
  const readerBookmarkPage = readerPageCount > 0
    ? Math.max(1, Math.min(readerPageCount, Math.round((Math.max(0, Math.min(100, Number(readerBookmarkPercent || 0))) / 100) * readerPageCount) || 1))
    : 0;
  const isCurrentReaderPageBookmarked = readerPageCount > 0 && readerBookmarkPage === Math.max(1, Math.min(readerPageCount, Number(readerCurrentPage || 1)));
  const readerElapsedTotalSeconds = Math.max(0, Number(readerAccumulatedSeconds || 0) + Number(readerLiveSeconds || 0));
  const readerSwipeThreshold = readerSwipeSensitivity === 'high' ? 24 : readerSwipeSensitivity === 'low' ? 52 : 36;
  const readerSwipeLockMs = readerSwipeSensitivity === 'high' ? 180 : readerSwipeSensitivity === 'low' ? 340 : 260;
  const readerTrackingVisible = Boolean(
    isWebAppMode
    && initData
    && !flashcardsOnly
    && selectedSections.has('reader')
    && readerImmersive
    && !readerArchiveOpen
    && !readerSettingsOpen
    && readerHasContent
  );
  const youtubeTodayTask = getTodayTaskForSection('youtube');
  const youtubeTaskProgress = youtubeTodayTask ? getTodayItemProgressPercent(youtubeTodayTask, todayTimerNowMs) : 0;
  const youtubeTaskDone = Boolean(
    youtubeTodayTask
    && (
      String(youtubeTodayTask?.status || '').toLowerCase() === 'done'
      || youtubeTaskProgress >= 100
    )
  );
  const youtubeWatchFocusMode = Boolean(
    !flashcardsOnly
    && selectedSections.size === 1
    && selectedSections.has('youtube')
    && youtubeId
    && youtubePlaybackStarted
    && !youtubeForceShowPanel
    && !youtubeOverlayEnabled
    && !youtubeAppFullscreen
  );
  const youtubeSubtitlesReady = youtubeTranscript.length > 0;
  const youtubeLearningMode = Boolean((youtubePlaybackStarted || youtubeAppFullscreen) && !youtubeForceShowPanel);
  const youtubeSearchExpanded = !youtubeId || !youtubeLearningMode;
  const youtubeLoadDisabled = !youtubeId || youtubeTranscriptLoading || youtubeManualOverride;
  const youtubeSubtitleStatusClass = youtubeTranscriptLoading
    ? 'is-loading'
    : youtubeSubtitlesReady
      ? 'is-ready'
      : 'is-empty';
  const youtubeSubtitleStatusLabel = youtubeTranscriptLoading
    ? tr('Субтитры: загрузка', 'Untertitel: Laden')
    : youtubeSubtitlesReady
      ? tr('Субтитры: готово', 'Untertitel: Bereit')
      : tr('Субтитры: не загружены', 'Untertitel: Nicht geladen');
  const showHero = false;
  const isFocusedSection = (key) => !flashcardsOnly && selectedSections.size === 1 && selectedSections.has(key);
  const uniqueSkills = (() => {
    const flat = (Array.isArray(skillReport?.groups) ? skillReport.groups : [])
      .flatMap((group) => (Array.isArray(group?.skills) ? group.skills : []));
    const byId = new Map();
    for (const skill of flat) {
      const id = String(skill?.skill_id || '').trim();
      if (!id || byId.has(id)) continue;
      byId.set(id, skill);
    }
    return Array.from(byId.values());
  })();
  const skilledWithData = uniqueSkills.filter((item) => Boolean(item?.has_data) && item?.mastery !== null && item?.mastery !== undefined);
  const weakestSkills = [...skilledWithData]
    .sort((a, b) => (Number(a?.mastery || 0) - Number(b?.mastery || 0)) || (Number(b?.errors_7d || 0) - Number(a?.errors_7d || 0)))
    .slice(0, 3)
    .map((item) => ({ ...item, ring_type: 'weak' }));
  const strongestSkills = [...skilledWithData]
    .sort((a, b) => (Number(b?.mastery || 0) - Number(a?.mastery || 0)) || (Number(a?.errors_7d || 0) - Number(b?.errors_7d || 0)))
    .filter((item) => !weakestSkills.some((weak) => String(weak?.skill_id || '') === String(item?.skill_id || '')))
    .slice(0, 3)
    .map((item) => ({ ...item, ring_type: 'best' }));
  const ringSkills = [...weakestSkills, ...strongestSkills];
  const skillTrainingStatusMap = useMemo(
    () => (skillReport?.skill_training_status && typeof skillReport.skill_training_status === 'object'
      ? skillReport.skill_training_status
      : {}),
    [skillReport]
  );
  const getSkillTrainingStatus = useCallback((skillId) => {
    const normalized = String(skillId || '').trim();
    if (!normalized) return null;
    const value = skillTrainingStatusMap[normalized];
    if (!value || typeof value !== 'object') return null;
    return {
      state: String(value?.state || '').trim().toLowerCase(),
      is_complete: Boolean(value?.is_complete),
      opened_count: Number(value?.opened_count || 0),
      required_count: Number(value?.required_count || 0),
      practice_submitted: Boolean(value?.practice_submitted),
    };
  }, [skillTrainingStatusMap]);
  const ringPalette = ['#ff5d7a', '#ff9d57', '#ffd84d', '#46dca0', '#53c7ff', '#7c9dff'];
  const economicsActionMap = useMemo(() => {
    const rows = Array.isArray(economicsSummary?.breakdown?.by_action_type)
      ? economicsSummary.breakdown.by_action_type
      : [];
    const map = new Map();
    rows.forEach((item) => {
      const key = String(item?.action_type || '').trim();
      if (!key) return;
      map.set(key, item);
    });
    return map;
  }, [economicsSummary]);
  const economicsVoiceRows = useMemo(() => ([
    {
      key: 'voice_stt_whisper',
      title: 'Whisper STT',
      item: economicsActionMap.get('voice_stt_whisper'),
      unit: tr('мин', 'Min'),
    },
    {
      key: 'voice_tts_agent',
      title: 'Agent TTS',
      item: economicsActionMap.get('voice_tts_agent'),
      unit: tr('мин', 'Min'),
    },
    {
      key: 'livekit_room_minutes',
      title: 'LiveKit',
      item: economicsActionMap.get('livekit_room_minutes'),
      unit: tr('мин', 'Min'),
    },
  ]), [economicsActionMap, uiLang]);
  const livekitStatusColor = String(economicsSummary?.livekit_status?.color || '').toLowerCase();
  const ringSize = 264;
  const ringCenter = ringSize / 2;
  const ringStartRadius = 118;
  const ringStep = 14;
  const weeklyMetrics = Object.keys(planAnalyticsMetrics || {}).length > 0
    ? planAnalyticsMetrics
    : (weeklyPlan?.metrics || {});
  const weeklyMetricRows = [
    {
      key: 'translations',
      title: tr('Переводы предложений', 'Satz-Uebersetzungen'),
      unit: tr('шт', 'Stk'),
      data: weeklyMetrics.translations || {},
    },
    {
      key: 'learned_words',
      title: tr('Выученные слова (FSRS)', 'Gelernte Woerter (FSRS)'),
      unit: tr('слов', 'Woerter'),
      data: weeklyMetrics.learned_words || {},
    },
    {
      key: 'agent_minutes',
      title: tr('Минуты разговора с агентом', 'Gesprächsminuten mit Assistent'),
      unit: tr('мин', 'Min'),
      data: weeklyMetrics.agent_minutes || {},
    },
    {
      key: 'reading_minutes',
      title: tr('Чтение (минуты)', 'Lesen (Minuten)'),
      unit: tr('мин', 'Min'),
      data: weeklyMetrics.reading_minutes || {},
    },
  ];
  const weeklyWeekLabel = planAnalyticsRange?.start_date && planAnalyticsRange?.end_date
    ? `${planAnalyticsRange.start_date} — ${planAnalyticsRange.end_date}`
    : (weeklyPlan?.week?.start_date && weeklyPlan?.week?.end_date
      ? `${weeklyPlan.week.start_date} — ${weeklyPlan.week.end_date}`
      : '');
  const periodDaysElapsed = Math.max(
    0,
    Number(planAnalyticsRange?.days_elapsed ?? weeklyPlan?.week?.days_elapsed ?? 0) || 0
  );
  const periodDaysTotal = Math.max(
    1,
    Number(planAnalyticsRange?.days_total ?? weeklyPlan?.week?.days_total ?? 7) || 1
  );
  const expectedProgressPercent = Math.max(0, Math.min(100, (periodDaysElapsed / periodDaysTotal) * 100));
  const planPeriodLabel = {
    week: tr('Неделя', 'Woche'),
    month: tr('Месяц', 'Monat'),
    quarter: tr('Квартал', 'Quartal'),
    'half-year': tr('Полугодие', 'Halbjahr'),
    year: tr('Год', 'Jahr'),
  }[planAnalyticsPeriod] || tr('Неделя', 'Woche');
  const weeklyMetricToneClass = (key) => {
    if (key === 'translations') return 'is-translations';
    if (key === 'learned_words') return 'is-words';
    if (key === 'agent_minutes') return 'is-agent';
    if (key === 'reading_minutes') return 'is-reading';
    return '';
  };
  const formatWeeklyValue = (value, digits = 0) => {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '0';
    if (digits <= 0) return String(Math.round(num));
    return num.toFixed(digits);
  };
  const weeklyPlanCollapseStorageKey = useMemo(() => {
    const uid = webappUser?.id ? String(webappUser.id) : 'anon';
    return `weekly_plan_collapsed_${uid}`;
  }, [webappUser]);
  const defaultWeeklyMetricExpanded = useMemo(() => ({
    translations: false,
    learned_words: false,
    agent_minutes: false,
    reading_minutes: false,
  }), []);
  const weeklyMetricExpandedStorageKey = useMemo(() => {
    const uid = webappUser?.id ? String(webappUser.id) : 'anon';
    return `weekly_metric_expanded_${uid}`;
  }, [webappUser]);

  const toggleSection = (key) => {
    if (key === 'economics' && !canViewEconomics) return;
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

  const ensureSectionVisible = useCallback((key) => {
    setSelectedSections((prev) => {
      const next = new Set(prev);
      next.add(key);
      return next;
    });
  }, []);

  const scrollToRef = useCallback((ref, options = {}) => {
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
  }, []);

  const openSectionAndScroll = (key, ref) => {
    ensureSectionVisible(key);
    setTimeout(() => {
      scrollToRef(ref, { center: key === 'flashcards', block: 'start' });
    }, 80);
  };

  const openSingleSectionAndScroll = (key, ref) => {
    setSelectedSections(new Set([key]));
    setTimeout(() => {
      scrollToRef(ref, { center: key === 'flashcards', block: 'start' });
    }, 80);
  };

  const goHomeScreen = () => {
    setFlashcardsOnly(false);
    setFlashcardSessionActive(false);
    setSelectedSections(new Set());
    setYoutubeBackSection('');
    setGlobalPauseReason('');
    setGlobalTimerSuspended(false);
    setTimeout(() => window.scrollTo({ top: 0, behavior: 'smooth' }), 60);
  };

  const getSectionRefByKey = (key) => {
    if (key === 'guide') return guideRef;
    if (key === 'translations') return translationsRef;
    if (key === 'youtube') return youtubeRef;
    if (key === 'movies') return moviesRef;
    if (key === 'dictionary') return dictionaryRef;
    if (key === 'reader') return readerRef;
    if (key === 'flashcards') return flashcardsRef;
    if (key === 'assistant') return assistantRef;
    if (key === 'support') return supportRef;
    if (key === 'analytics') return analyticsRef;
    if (key === 'economics') return economicsRef;
    if (key === 'subscription') return billingRef;
    if (key === 'theory') return theoryRef;
    if (key === 'skill_training') return skillTrainingRef;
    return null;
  };

  const goBackFromYoutube = () => {
    const backKey = String(youtubeBackSection || '').trim();
    if (!backKey) {
      goHomeScreen();
      return;
    }
    const backRef = getSectionRefByKey(backKey);
    openSingleSectionAndScroll(backKey, backRef);
  };

  const jumpToDictionaryFromSentence = useCallback(() => {
    setLastLookupScrollY(window.scrollY);
    ensureSectionVisible('dictionary');
    setTimeout(() => {
      scrollToRef(dictionaryRef, { block: 'start' });
    }, 120);
  }, [ensureSectionVisible, scrollToRef]);

  const openFlashcardsSetup = (ref) => {
    stopTtsPlayback();
    setFlashcardsVisible(true);
    setFlashcardsOnly(false);
    setFlashcardActiveMode(null);
    setFlashcardSettingsModalMode(null);
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

  const exitFlashcardsTraining = async () => {
    stopTtsPlayback();
    void dispatchQueuedFlashcardFeel('exit_session');
    setFlashcardsOnly(false);
    setFlashcardActiveMode(null);
    setFlashcardSettingsModalMode(null);
    setFlashcardSessionActive(false);
    setFlashcardPreviewActive(false);
    setFlashcardExitSummary(false);
    await pauseFlashcardsTaskTimer();
  };

  const startFlashcardsMode = async (mode) => {
    const normalizedMode = String(mode || '').toLowerCase();
    if (!['fsrs', 'quiz', 'blocks', 'sentence'].includes(normalizedMode)) return;
    stopTtsPlayback();
    setFlashcardsVisible(true);
    setFlashcardSettingsModalMode(null);
    setFlashcardActiveMode(normalizedMode);
    setFlashcardsOnly(true);
    setFlashcardExitSummary(false);
    await ensureFlashcardsTaskTimerRunning();

    if (normalizedMode === 'fsrs') {
      setFlashcardSessionActive(false);
      setFlashcardPreviewActive(false);
      setSrsError('');
      return;
    }

    flashcardTrainingModeRef.current = normalizedMode;
    setFlashcardTrainingMode(normalizedMode);
    setFlashcardSessionActive(false);
    setFlashcardPreviewActive(true);
    unlockAudio();
    await loadFlashcards();
  };

  const showAllSections = () => {
    const next = ['guide', 'translations', 'youtube', 'movies', 'dictionary', 'reader', 'flashcards', 'assistant', 'support', 'analytics', 'subscription', 'theory'];
    if (canViewEconomics) {
      next.push('economics');
    }
    if (isSkillTrainingReady) {
      next.push('skill_training');
    }
    setSelectedSections(new Set(next));
    setMoviesCollapsed(false);
  };

  const hideAllSections = () => {
    setSelectedSections(new Set());
    setMoviesCollapsed(false);
  };

  const dismissWeeklySummaryModal = useCallback(() => {
    if (weeklySummaryDismissStorageKey) {
      safeStorageSet(weeklySummaryDismissStorageKey, '1');
    }
    setWeeklySummaryModalOpen(false);
  }, [weeklySummaryDismissStorageKey]);

  const openAnalyticsFromWeeklySummary = useCallback(() => {
    dismissWeeklySummaryModal();
    setSelectedSections((prev) => {
      if (!menuMultiSelect) {
        return new Set(['analytics']);
      }
      const next = new Set(prev);
      next.add('analytics');
      return next;
    });
    setTimeout(() => {
      scrollToRef(analyticsRef, { block: 'start' });
    }, 120);
  }, [dismissWeeklySummaryModal, menuMultiSelect]);

  const handleMenuSelection = (key, ref) => {
    if (key === 'economics' && !canViewEconomics) return;
    if (key === 'youtube' && !menuMultiSelect) {
      const backCandidate = Array.from(selectedSections).find((item) => item && item !== 'youtube') || '';
      setYoutubeBackSection(backCandidate);
    }
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
      setFlashcardActiveMode(null);
      setFlashcardSettingsModalMode(null);
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

  const loadSupportUnread = useCallback(async () => {
    try {
      const data = await postSupportApi('/api/webapp/support/unread');
      setSupportUnreadCount(Math.max(0, Number(data?.unread || 0)));
    } catch (_error) {
      // Keep UX silent for badge polling failures.
    }
  }, [postSupportApi]);

  const loadSupportMessages = useCallback(async () => {
    if (!initData) return;
    setSupportLoading(true);
    setSupportError('');
    try {
      const data = await postSupportApi('/api/webapp/support/messages/list', { limit: 120 });
      const items = Array.isArray(data?.items) ? data.items : [];
      const filtered = items.filter((item) => ['user', 'admin'].includes(String(item?.from_role || '').toLowerCase()));
      setSupportMessages(filtered);
    } catch (error) {
      setSupportError(normalizeNetworkErrorMessage(error, 'Не удалось загрузить переписку', 'Chat konnte nicht geladen werden'));
    } finally {
      setSupportLoading(false);
    }
  }, [initData, normalizeNetworkErrorMessage, postSupportApi]);

  const markSupportMessagesRead = useCallback(async () => {
    try {
      await postSupportApi('/api/webapp/support/messages/read');
      setSupportUnreadCount(0);
    } catch (_error) {
      // Silent: badge will sync on next poll.
    }
  }, [postSupportApi]);

  const clearSupportAttachment = useCallback(() => {
    setSupportAttachment((prev) => {
      const previewUrl = String(prev?.preview_url || '');
      if (previewUrl.startsWith('blob:')) {
        try {
          URL.revokeObjectURL(previewUrl);
        } catch (_error) {
          // ignore
        }
      }
      return null;
    });
    if (supportAttachmentInputRef.current) {
      supportAttachmentInputRef.current.value = '';
    }
  }, []);

  const handleSupportAttachmentSelect = useCallback(async (event) => {
    const file = event?.target?.files?.[0] || null;
    if (!file) {
      clearSupportAttachment();
      return;
    }
    const mimeType = String(file.type || '').toLowerCase();
    if (!['image/jpeg', 'image/png', 'image/webp'].includes(mimeType)) {
      setSupportError(tr('Можно прикрепить только JPG, PNG или WEBP.', 'Es koennen nur JPG, PNG oder WEBP angehaengt werden.'));
      clearSupportAttachment();
      return;
    }
    if (Number(file.size || 0) > 8 * 1024 * 1024) {
      setSupportError(tr('Фото слишком большое. Максимум 8 МБ.', 'Das Bild ist zu gross. Maximal 8 MB.'));
      clearSupportAttachment();
      return;
    }
    try {
      const imageBase64 = await readFileAsBase64(file);
      setSupportError('');
      setSupportAttachment((prev) => {
        const previousUrl = String(prev?.preview_url || '');
        if (previousUrl.startsWith('blob:')) {
          try {
            URL.revokeObjectURL(previousUrl);
          } catch (_error) {
            // ignore
          }
        }
        return {
          file_name: String(file.name || 'support-image'),
          mime_type: mimeType,
          image_base64: imageBase64,
          preview_url: URL.createObjectURL(file),
        };
      });
    } catch (error) {
      setSupportError(normalizeNetworkErrorMessage(error, 'Не удалось прочитать фото', 'Bild konnte nicht gelesen werden'));
      clearSupportAttachment();
    }
  }, [clearSupportAttachment, normalizeNetworkErrorMessage, tr]);

  const sendSupportMessage = useCallback(async (payload, retryId = '') => {
    const rawText = typeof payload === 'string' ? payload : payload?.text;
    const clean = String(rawText || '').trim();
    const attachment = typeof payload === 'string' ? null : (payload?.attachment || null);
    if ((!clean && !attachment?.image_base64) || supportSending) return;
    setSupportSending(true);
    setSupportError('');
    try {
      const requestPayload = { text: clean };
      if (attachment?.image_base64) {
        requestPayload.image_base64 = attachment.image_base64;
        requestPayload.image_mime_type = attachment.mime_type;
        requestPayload.image_file_name = attachment.file_name;
      }
      const data = await postSupportApi('/api/webapp/support/messages/send', requestPayload);
      const item = data?.item;
      if (item && typeof item === 'object') {
        setSupportMessages((prev) => [...prev, item]);
      } else {
        await loadSupportMessages();
      }
      setSupportDraft('');
      clearSupportAttachment();
      if (retryId) {
        setSupportFailedMessages((prev) => prev.filter((entry) => entry.temp_id !== retryId));
      }
    } catch (error) {
      const message = normalizeNetworkErrorMessage(error, 'Не удалось отправить сообщение', 'Nachricht konnte nicht gesendet werden');
      setSupportError(message);
      if (!retryId) {
        const tempId = `support_failed_${Date.now()}_${Math.random().toString(16).slice(2, 7)}`;
        setSupportFailedMessages((prev) => [
          ...prev,
          {
            temp_id: tempId,
            from_role: 'user',
            message_text: clean,
            attachment_url: attachment?.preview_url || null,
            attachment_kind: attachment?.image_base64 ? 'image' : '',
            attachment_mime_type: attachment?.mime_type || null,
            attachment_file_name: attachment?.file_name || null,
            attachment_payload: attachment ? { ...attachment } : null,
            created_at: new Date().toISOString(),
            is_failed: true,
          },
        ]);
      }
    } finally {
      setSupportSending(false);
    }
  }, [clearSupportAttachment, loadSupportMessages, normalizeNetworkErrorMessage, postSupportApi, supportSending]);

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

  const renderMenuIcon = (kind) => {
    if (kind === 'guide') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <circle cx="12" cy="12" r="8.2" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M9.7 9.2a2.4 2.4 0 1 1 4.1 1.7c-.8.8-1.5 1.2-1.5 2.3" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" />
          <circle cx="12" cy="16.8" r="1.1" fill="currentColor" />
        </svg>
      );
    }
    if (kind === 'today') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M4 8h16v12H4z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8 4v4M16 4v4M4 11h16" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <circle cx="12" cy="16" r="2.2" fill="currentColor" />
        </svg>
      );
    }
    if (kind === 'translations') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M4 6h16v12H4z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8 10h8M8 14h6" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M6.5 8.5l2-2M15.5 17.5l2-2" fill="none" stroke="currentColor" strokeWidth="1.9" />
        </svg>
      );
    }
    if (kind === 'youtube') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <rect x="3" y="6" width="18" height="12" rx="3.2" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M10 9.2l6 2.8-6 2.8V9.2z" fill="currentColor" />
        </svg>
      );
    }
    if (kind === 'movies') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <rect x="3" y="6" width="18" height="12" rx="2.8" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M7 6l2-3M12 6l2-3M17 6l2-3" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M10 10l5 2-5 2v-4z" fill="currentColor" />
        </svg>
      );
    }
    if (kind === 'dictionary') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M6 4h11a2 2 0 0 1 2 2v14H8a2 2 0 0 0-2 2V4z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8.5 9h7M8.5 12.5h7M8.5 16h5" fill="none" stroke="currentColor" strokeWidth="1.9" />
        </svg>
      );
    }
    if (kind === 'reader') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M4 6a2 2 0 0 1 2-2h6v15H6a2 2 0 0 0-2 2V6z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M20 6a2 2 0 0 0-2-2h-6v15h6a2 2 0 0 1 2 2V6z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8 9h2.6M13.4 9H16M8 12h8M8 15h8" fill="none" stroke="currentColor" strokeWidth="1.9" />
        </svg>
      );
    }
    if (kind === 'flashcards') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <rect x="4" y="5" width="12" height="14" rx="2.3" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M10 8h4M10 11.5h3" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M14 7l6 2.2v9L14 16V7z" fill="none" stroke="currentColor" strokeWidth="1.9" />
        </svg>
      );
    }
    if (kind === 'assistant') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <rect x="4" y="5" width="16" height="13" rx="3.2" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <circle cx="9" cy="11" r="1.2" fill="currentColor" />
          <circle cx="15" cy="11" r="1.2" fill="currentColor" />
          <path d="M9 14.5h6M12 2.5v2.2" fill="none" stroke="currentColor" strokeWidth="1.9" />
        </svg>
      );
    }
    if (kind === 'support') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M4 6.5a3 3 0 0 1 3-3h10a3 3 0 0 1 3 3v7a3 3 0 0 1-3 3H11l-4.5 3v-3H7a3 3 0 0 1-3-3v-7z" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinejoin="round" />
          <path d="M8.2 9.7h7.6M8.2 12.5h5.2" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" />
        </svg>
      );
    }
    if (kind === 'economics') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <circle cx="12" cy="12" r="8.2" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M9.2 9.3h4.4a1.7 1.7 0 0 1 0 3.4H10.7a1.7 1.7 0 0 0 0 3.4h4.2M12 7v10" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" />
        </svg>
      );
    }
    if (kind === 'subscription') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M5 8.5h14v9a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2v-9z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8.5 8.5V7a3.5 3.5 0 0 1 7 0v1.5" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <circle cx="12" cy="13.3" r="1.1" fill="currentColor" />
        </svg>
      );
    }
    if (kind === 'skill_training') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
          <path d="M4.5 7.2h15v9.6a2 2 0 0 1-2 2h-11a2 2 0 0 1-2-2V7.2z" fill="none" stroke="currentColor" strokeWidth="1.9" />
          <path d="M8.2 12.2l2.2 2.2 5.4-5.4" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" />
          <path d="M8 4.8h8" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" />
        </svg>
      );
    }
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="menu-icon-svg">
        <path d="M4 18h16M7 18V12M12 18V8M17 18V5" fill="none" stroke="currentColor" strokeWidth="1.9" />
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
  const [assistantSessionId, setAssistantSessionId] = useState(null);
  const [readerSessionId, setReaderSessionId] = useState(null);

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

  const openGuideSection = useCallback(() => {
    setGuideQuickCardDismissed(true);
    safeStorageSet(guideQuickCardStorageKey, '1');
    openSingleSectionAndScroll('guide', guideRef);
    setMenuOpen(false);
  }, [guideQuickCardStorageKey]);

  const startOnboardingTour = useCallback(() => {
    setOnboardingStep(0);
    setOnboardingOpen(true);
    setMenuOpen(false);
  }, []);

  const dismissOnboarding = useCallback(() => {
    safeStorageSet(onboardingSeenStorageKey, '1');
    setOnboardingOpen(false);
    setOnboardingStep(0);
  }, [onboardingSeenStorageKey]);

  const finishOnboarding = useCallback((target = '') => {
    safeStorageSet(onboardingSeenStorageKey, '1');
    setGuideQuickCardDismissed(true);
    safeStorageSet(guideQuickCardStorageKey, '1');
    setOnboardingOpen(false);
    setOnboardingStep(0);
    if (target === 'translations') {
      openSingleSectionAndScroll('translations', translationsRef);
      return;
    }
    if (target === 'guide') {
      openSingleSectionAndScroll('guide', guideRef);
    }
  }, [guideQuickCardStorageKey, onboardingSeenStorageKey]);

  const handleConnect = async (e) => {
    e.preventDefault();
    if (!telegramID || !username) {
      alert(tr('Пожалуйста, введите ваше имя', 'Bitte gib deinen Namen ein'));
      return;
    }

    try {
      const response = await fetch(
        `/api/token?user_id=${encodeURIComponent(telegramID)}&username=${encodeURIComponent(username)}`
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`${tr('Ошибка получения токена', 'Token-Fehler')}: ${errorText}`);
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
      setAssistantError(tr('Не удалось определить пользователя Telegram. Обновите страницу.', 'Telegram-Nutzer konnte nicht bestimmt werden. Bitte Seite aktualisieren.'));
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
      setAssistantSessionId(null);
    } catch (error) {
      setAssistantError(`${tr('Ошибка подключения ассистента', 'Assistent-Verbindungsfehler')}: ${error.message}`);
    } finally {
      setAssistantConnecting(false);
    }
  };

  const startAssistantSessionTracking = async () => {
    if (!initData || assistantSessionId) return;
    try {
      const response = await fetch('/api/assistant/session/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const nextSessionId = data?.session?.session_id;
      if (nextSessionId !== undefined && nextSessionId !== null) {
        setAssistantSessionId(Number(nextSessionId));
      }
    } catch (error) {
      setAssistantError(`${tr('Ошибка старта сессии ассистента', 'Fehler beim Start der Assistent-Session')}: ${error.message}`);
    }
  };

  const stopAssistantSessionTracking = async (sessionIdOverride = null, options = {}) => {
    if (!initData) return;
    const sid = sessionIdOverride ?? assistantSessionId;
    const shouldUseKeepalive = Boolean(options?.keepalive);
    const shouldSkipRefresh = Boolean(options?.skipRefresh);
    setAssistantSessionId(null);
    try {
      const response = await fetch('/api/assistant/session/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        keepalive: shouldUseKeepalive,
        body: JSON.stringify(sid ? { initData, session_id: sid } : { initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      if (!shouldSkipRefresh) {
        await loadWeeklyPlan();
      }
    } catch (error) {
      console.warn('assistant session stop error', error);
    }
  };

  const disconnectAssistant = () => {
    const sid = assistantSessionId;
    if (sid) {
      stopAssistantSessionTracking(sid);
    }
    setAssistantToken(null);
    setAssistantError('');
  };

  const startReaderSessionTracking = async () => {
    if (!initData || readerSessionId || readerSessionStartingRef.current || readerTimerPaused) return;
    readerSessionStartingRef.current = true;
    try {
      const response = await fetch('/api/reader/session/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const nextSessionId = data?.session?.session_id;
      if (nextSessionId !== undefined && nextSessionId !== null) {
        setReaderSessionId(Number(nextSessionId));
      }
      const startedAt = String(data?.session?.started_at || '').trim();
      setReaderSessionStartedAt(startedAt);
      setReaderLiveSeconds(0);
      readerLastInteractionAtRef.current = Date.now();
    } catch (error) {
      console.warn('reader session start error', error);
    } finally {
      readerSessionStartingRef.current = false;
    }
  };

  const stopReaderSessionTracking = async (sessionIdOverride = null, options = {}) => {
    if (!initData) return;
    const sid = sessionIdOverride ?? readerSessionId;
    const shouldUseKeepalive = Boolean(options?.keepalive);
    const shouldSkipRefresh = Boolean(options?.skipRefresh);
    const shouldSkipReaderStateSync = Boolean(options?.skipReaderStateSync);
    const latestProgress = computeReaderProgressPercent();
    setReaderProgressPercent(latestProgress);
    if (readerDocumentId && !shouldSkipReaderStateSync) {
      await syncReaderState({ progress_percent: Number(latestProgress.toFixed(2)) });
    }
    setReaderSessionId(null);
    setReaderSessionStartedAt('');
    setReaderLiveSeconds(0);
    try {
      const response = await fetch('/api/reader/session/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        keepalive: shouldUseKeepalive,
        body: JSON.stringify(sid ? { initData, session_id: sid } : { initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      if (!shouldSkipRefresh) {
        await loadWeeklyPlan();
        await loadPlanAnalytics();
      }
    } catch (error) {
      console.warn('reader session stop error', error);
    }
  };

  const pauseReaderForIdle = useCallback(() => {
    if (!readerTrackingVisible || readerTimerPaused || !readerHasContent) return;
    const nowMs = Date.now();
    const startedTs = Date.parse(String(readerSessionStartedAt || ''));
    const segmentSeconds = Number.isFinite(startedTs)
      ? Math.max(0, Math.floor((nowMs - startedTs) / 1000))
      : 0;
    if (segmentSeconds > 0) {
      setReaderAccumulatedSeconds((prev) => prev + segmentSeconds);
    }
    setReaderTimerPaused(true);
    setGlobalPauseReason(tr('Поставлено на паузу: нет активности в читалке', 'Pausiert: keine Aktivitaet im Leser'));
    readerAutoPausedByIdleRef.current = true;
    if (readerSessionId) {
      void stopReaderSessionTracking(readerSessionId);
    }
  }, [
    readerTrackingVisible,
    readerTimerPaused,
    readerHasContent,
    readerSessionStartedAt,
    readerSessionId,
    stopReaderSessionTracking,
    tr,
  ]);

  const markReaderInteraction = useCallback(() => {
    readerLastInteractionAtRef.current = Date.now();
    if (readerAutoPausedByIdleRef.current && readerTrackingVisible && readerTimerPaused) {
      readerAutoPausedByIdleRef.current = false;
      setReaderTimerPaused(false);
      setGlobalPauseReason('');
      void startReaderSessionTracking();
    }
  }, [
    readerTrackingVisible,
    readerTimerPaused,
    startReaderSessionTracking,
  ]);

  const toggleReaderTimerPause = async () => {
    if (!readerHasContent) return;
    if (readerTimerPaused) {
      readerAutoPausedByIdleRef.current = false;
      readerLastInteractionAtRef.current = Date.now();
      setReaderTimerPaused(false);
      await startReaderSessionTracking();
      return;
    }
    const segmentSeconds = readerSessionStartedAt
      ? Math.max(0, Math.floor((Date.now() - new Date(readerSessionStartedAt).getTime()) / 1000))
      : 0;
    setReaderAccumulatedSeconds((prev) => prev + segmentSeconds);
    setReaderTimerPaused(true);
    readerAutoPausedByIdleRef.current = false;
    if (readerSessionId) {
      await stopReaderSessionTracking(readerSessionId);
    }
  };

  const pauseAllActiveTimers = useCallback(async (reason = 'auto') => {
    if (globalTimerAutoPauseInFlightRef.current) return;
    globalTimerAutoPauseInFlightRef.current = true;
    setGlobalTimerSuspended(true);
    setGlobalPauseReason(
      reason === 'lifecycle'
        ? tr('Поставлено на паузу: приложение не в фокусе', 'Pausiert: App ist nicht im Fokus')
        : tr('Поставлено на паузу: приложение закрывается', 'Pausiert: App wird geschlossen')
    );
    try {
      const nowMs = Date.now();
      const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
      const runningTodayTimers = items.filter((item) => (
        String(item?.status || '').toLowerCase() !== 'done'
        && isTodayItemTimerRunning(item)
      ));
      if (runningTodayTimers.length > 0) {
        runningTodayTimers.forEach((item) => {
          const taskType = String(item?.task_type || '').toLowerCase();
          if (taskType !== 'video' && taskType !== 'youtube') {
            autoPausedTodayTimerIdsRef.current.add(item.id);
          }
        });
        await Promise.all(runningTodayTimers.map((item) => (
          syncTodayItemTimer(item, 'pause', {
            elapsedSeconds: getTodayItemElapsedSeconds(item, nowMs),
            running: false,
            keepalive: reason === 'pagehide' || reason === 'beforeunload',
          })
        )));
      }

      if (readerHasContent && !readerTimerPaused) {
        const startedTs = Date.parse(String(readerSessionStartedAt || ''));
        const segmentSeconds = Number.isFinite(startedTs)
          ? Math.max(0, Math.floor((nowMs - startedTs) / 1000))
          : 0;
        if (segmentSeconds > 0) {
          setReaderAccumulatedSeconds((prev) => prev + segmentSeconds);
        }
        setReaderTimerPaused(true);
        readerAutoPausedByIdleRef.current = false;
        if (readerSessionId) {
          await stopReaderSessionTracking(readerSessionId, {
            keepalive: reason === 'pagehide' || reason === 'beforeunload',
            skipRefresh: reason === 'pagehide' || reason === 'beforeunload',
            skipReaderStateSync: reason === 'pagehide' || reason === 'beforeunload',
          });
        }
        readerAutoPausedByNavigationRef.current = true;
      }

      if (assistantSessionId) {
        await stopAssistantSessionTracking(assistantSessionId, {
          keepalive: reason === 'pagehide' || reason === 'beforeunload',
          skipRefresh: reason === 'pagehide' || reason === 'beforeunload',
        });
      }
    } catch (error) {
      console.warn(`auto timer pause failed (${reason})`, error);
    } finally {
      globalTimerAutoPauseInFlightRef.current = false;
    }
  }, [
    todayPlan,
    readerHasContent,
    readerTimerPaused,
    readerSessionStartedAt,
    readerSessionId,
    assistantSessionId,
    syncTodayItemTimer,
    getTodayItemElapsedSeconds,
    isTodayItemTimerRunning,
    stopReaderSessionTracking,
    stopAssistantSessionTracking,
  ]);

  const resumeAutoPausedVisibleTimers = useCallback(async () => {
    if (globalTimerAutoResumeInFlightRef.current) return;
    globalTimerAutoResumeInFlightRef.current = true;
    try {
      const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
      if (items.length === 0) return;

      const sectionVisibility = {
        flashcards: flashcardsOnly || selectedSections.has('flashcards'),
        translations: !flashcardsOnly && selectedSections.has('translations'),
        theory: !flashcardsOnly && selectedSections.has('theory'),
        reader: !flashcardsOnly
          && selectedSections.has('reader')
          && readerImmersive
          && !readerArchiveOpen
          && !readerSettingsOpen,
      };
      const findTodayTaskByTypesLocal = (types = []) => {
        const normalizedTypes = new Set(types.map((entry) => String(entry || '').toLowerCase()));
        const active = items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase()) && String(entry?.status || '').toLowerCase() !== 'done');
        if (active) return active;
        return items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase())) || null;
      };
      const getSectionTask = (key) => {
        if (key === 'flashcards') return findTodayTaskByTypesLocal(['cards']);
        if (key === 'translations') return findTodayTaskByTypesLocal(['translation']);
        if (key === 'theory') return findTodayTaskByTypesLocal(['theory']);
        return null;
      };

      const nowMs = Date.now();
      const timerSyncCalls = [];
      ['flashcards', 'translations', 'theory'].forEach((sectionKey) => {
        if (!sectionVisibility[sectionKey]) return;
        const item = getSectionTask(sectionKey);
        if (!item?.id) return;
        const status = String(item?.status || '').toLowerCase();
        if (status === 'done') {
          autoPausedTodayTimerIdsRef.current.delete(item.id);
          return;
        }
        if (!autoPausedTodayTimerIdsRef.current.has(item.id)) return;
        if (isTodayItemTimerRunning(item)) {
          autoPausedTodayTimerIdsRef.current.delete(item.id);
          return;
        }
        const elapsedSeconds = getTodayItemElapsedSeconds(item, nowMs);
        const hasStartedBefore = elapsedSeconds > 0 || status === 'doing';
        timerSyncCalls.push(
          syncTodayItemTimer(
            item,
            hasStartedBefore ? 'resume' : 'start',
            { elapsedSeconds, running: true }
          ).then(() => {
            autoPausedTodayTimerIdsRef.current.delete(item.id);
          })
        );
      });

      let readerResumed = false;
      if (sectionVisibility.reader && readerHasContent && readerTimerPaused && readerAutoPausedByNavigationRef.current) {
        readerAutoPausedByIdleRef.current = false;
        readerLastInteractionAtRef.current = Date.now();
        setReaderTimerPaused(false);
        void startReaderSessionTracking();
        readerAutoPausedByNavigationRef.current = false;
        readerResumed = true;
      }

      if (timerSyncCalls.length > 0) {
        await Promise.allSettled(timerSyncCalls);
      }
      if (timerSyncCalls.length > 0 || readerResumed) {
        setGlobalPauseReason('');
      }
    } finally {
      globalTimerAutoResumeInFlightRef.current = false;
    }
  }, [
    todayPlan,
    flashcardsOnly,
    selectedSections,
    readerHasContent,
    readerImmersive,
    readerArchiveOpen,
    readerSettingsOpen,
    readerTimerPaused,
    getTodayItemElapsedSeconds,
    isTodayItemTimerRunning,
    syncTodayItemTimer,
    startReaderSessionTracking,
  ]);

  const requestTelegramFullscreen = useCallback(() => {
    if (!telegramApp) return;
    try {
      telegramApp.ready?.();
      telegramApp.expand?.();
      if (typeof telegramApp.requestFullscreen === 'function') {
        Promise.resolve(telegramApp.requestFullscreen())
          .then(() => {
            const isFullscreen = typeof telegramApp.isFullscreen === 'boolean' ? telegramApp.isFullscreen : true;
            setTelegramFullscreenMode(Boolean(isFullscreen));
          })
          .catch(() => {
            // ignore: unsupported Telegram clients may reject fullscreen
          });
      }
    } catch (error) {
      // ignore
    }
  }, [telegramApp]);

  useEffect(() => {
    if (!telegramApp) return;
    let fullscreenRetryCount = 0;
    let fullscreenRetryTimer = null;
    let expandPulseTimer = null;
    let stopped = false;
    let viewportSyncFrame = null;

    const isHandsetDevice = () => {
      const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
      return /iPhone|iPod|Windows Phone|Android.*Mobile/i.test(userAgent);
    };

    const detectTabletLikeViewport = () => {
      const viewportWidth = window.innerWidth || 0;
      const viewportHeight = window.innerHeight || 0;
      const minSide = Math.min(viewportWidth, viewportHeight);
      const maxSide = Math.max(viewportWidth, viewportHeight);
      const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
      const platform = typeof navigator !== 'undefined' ? String(navigator.platform || '') : '';
      const maxTouchPoints = typeof navigator !== 'undefined' ? Number(navigator.maxTouchPoints || 0) : 0;
      const isIPadDesktopUA = platform === 'MacIntel' && maxTouchPoints > 1;
      const isTabletUserAgent = /iPad|Tablet|PlayBook|Silk|Android(?!.*Mobile)/i.test(userAgent) || isIPadDesktopUA;
      if (isHandsetDevice()) return false;
      return isTabletUserAgent || viewportWidth >= 700 || (maxSide >= 1000 && minSide >= 600);
    };

    const isTelegramIOSClient = () => {
      const tgPlatform = String(telegramApp?.platform || '').toLowerCase();
      if (tgPlatform === 'ios') return true;
      const userAgent = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
      const platform = typeof navigator !== 'undefined' ? String(navigator.platform || '') : '';
      const maxTouchPoints = typeof navigator !== 'undefined' ? Number(navigator.maxTouchPoints || 0) : 0;
      const isIPadDesktopUA = platform === 'MacIntel' && maxTouchPoints > 1;
      return /iPad|iPhone|iPod/i.test(userAgent) || isIPadDesktopUA;
    };

    const tryEnterTelegramFullscreen = () => {
      if (stopped) return;
      if (typeof telegramApp.requestFullscreen !== 'function') {
        setTelegramFullscreenMode(false);
        telegramApp.expand?.();
        return;
      }
      Promise.resolve(telegramApp.requestFullscreen())
        .then(() => {
          const isFullscreen = typeof telegramApp.isFullscreen === 'boolean' ? telegramApp.isFullscreen : true;
          setTelegramFullscreenMode(Boolean(isFullscreen));
          if (!isFullscreen) {
            telegramApp.expand?.();
            if (fullscreenRetryCount < 6) {
              fullscreenRetryCount += 1;
              fullscreenRetryTimer = window.setTimeout(tryEnterTelegramFullscreen, 320);
            }
          }
        })
        .catch(() => {
          setTelegramFullscreenMode(false);
          telegramApp.expand?.();
          if (fullscreenRetryCount < 6) {
            fullscreenRetryCount += 1;
            fullscreenRetryTimer = window.setTimeout(tryEnterTelegramFullscreen, 320);
          }
        });
    };

    const syncViewportMode = (options = {}) => {
      const force = Boolean(options?.force);
      if (!force && isAndroidTelegramClient && hasFocusedEditableElement()) {
        return;
      }
      try {
        telegramApp.ready?.();
        telegramApp.expand?.();
        const shouldUseFullscreen = detectTabletLikeViewport();
        setTelegramTabletLike(shouldUseFullscreen);
        if (shouldUseFullscreen) {
          tryEnterTelegramFullscreen();
        } else {
          setTelegramFullscreenMode(false);
          try {
            telegramApp.exitFullscreen?.();
          } catch (error) {
            // ignore
          }
          telegramApp.expand?.();
        }
        if (isTelegramIOSClient()) {
          telegramApp.disableVerticalSwipes?.();
        } else {
          telegramApp.enableVerticalSwipes?.();
        }
      } catch (error) {
        setTelegramFullscreenMode(false);
        setTelegramTabletLike(false);
        try {
          telegramApp.expand?.();
        } catch (expandError) {
          // ignore
        }
        // Telegram API may be partially unavailable in browser mode.
      }
    };

    const requestViewportSync = (options = {}) => {
      if (viewportSyncFrame !== null) {
        window.cancelAnimationFrame(viewportSyncFrame);
        viewportSyncFrame = null;
      }
      viewportSyncFrame = window.requestAnimationFrame(() => {
        viewportSyncFrame = null;
        syncViewportMode(options);
      });
    };

    const onFirstUserGesture = () => {
      if (!detectTabletLikeViewport()) return;
      if (isAndroidTelegramClient && hasFocusedEditableElement()) return;
      try {
        fullscreenRetryCount = 0;
        tryEnterTelegramFullscreen();
      } catch (error) {
        // ignore
      }
    };

    try {
      syncViewportMode({ force: true });
    } catch (error) {
      // Telegram API may be partially unavailable in browser mode.
    }

    const onResize = () => {
      fullscreenRetryCount = 0;
      requestViewportSync();
    };
    const onFocus = () => {
      fullscreenRetryCount = 0;
      requestViewportSync();
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        fullscreenRetryCount = 0;
        requestViewportSync();
      }
    };
    window.addEventListener('resize', onResize);
    window.addEventListener('focus', onFocus);
    document.addEventListener('visibilitychange', onVisibilityChange);
    window.addEventListener('pointerdown', onFirstUserGesture, { passive: true });
    window.addEventListener('touchstart', onFirstUserGesture, { passive: true });
    window.addEventListener('click', onFirstUserGesture, { passive: true });
    if (typeof telegramApp.onEvent === 'function') {
      telegramApp.onEvent('viewportChanged', requestViewportSync);
    }
    expandPulseTimer = window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode({ force: true });
    }, 180);
    window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode({ force: true });
    }, 620);
    window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode({ force: true });
    }, 1200);

    return () => {
      stopped = true;
      if (fullscreenRetryTimer) window.clearTimeout(fullscreenRetryTimer);
      if (expandPulseTimer) window.clearTimeout(expandPulseTimer);
      if (viewportSyncFrame !== null) {
        window.cancelAnimationFrame(viewportSyncFrame);
        viewportSyncFrame = null;
      }
      window.removeEventListener('resize', onResize);
      window.removeEventListener('focus', onFocus);
      document.removeEventListener('visibilitychange', onVisibilityChange);
      window.removeEventListener('pointerdown', onFirstUserGesture);
      window.removeEventListener('touchstart', onFirstUserGesture);
      window.removeEventListener('click', onFirstUserGesture);
      if (typeof telegramApp.offEvent === 'function') {
        telegramApp.offEvent('viewportChanged', requestViewportSync);
      }
    };
  }, [telegramApp, isAndroidTelegramClient]);

  useEffect(() => {
    const pauseNow = () => {
      void pauseAllActiveTimers('lifecycle');
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        pauseNow();
      } else if (document.visibilityState === 'visible') {
        setGlobalTimerSuspended(false);
        setGlobalPauseReason('');
        void resumeAutoPausedVisibleTimers();
      }
    };
    const onWindowBlur = () => {
      pauseNow();
    };
    const onWindowFocus = () => {
      setGlobalTimerSuspended(false);
      setGlobalPauseReason('');
      void resumeAutoPausedVisibleTimers();
    };
    const onPageHide = () => {
      void pauseAllActiveTimers('pagehide');
    };
    const onBeforeUnload = () => {
      void pauseAllActiveTimers('beforeunload');
    };

    document.addEventListener('visibilitychange', onVisibilityChange);
    window.addEventListener('blur', onWindowBlur);
    window.addEventListener('focus', onWindowFocus);
    window.addEventListener('pagehide', onPageHide);
    window.addEventListener('beforeunload', onBeforeUnload);

    return () => {
      document.removeEventListener('visibilitychange', onVisibilityChange);
      window.removeEventListener('blur', onWindowBlur);
      window.removeEventListener('focus', onWindowFocus);
      window.removeEventListener('pagehide', onPageHide);
      window.removeEventListener('beforeunload', onBeforeUnload);
    };
  }, [pauseAllActiveTimers, resumeAutoPausedVisibleTimers]);

  useEffect(() => {
    if (!flashcardSessionActive) return;
    const flashcardsVisible = flashcardsOnly || selectedSections.has('flashcards');
    if (!flashcardsVisible) {
      setGlobalTimerSuspended(true);
      setGlobalPauseReason(tr('Поставлено на паузу: переход в другой раздел', 'Pausiert: Wechsel in einen anderen Bereich'));
      return;
    }
    setGlobalTimerSuspended(false);
    setGlobalPauseReason('');
  }, [flashcardSessionActive, flashcardsOnly, selectedSections]);

  useEffect(() => {
    const sectionVisibility = {
      flashcards: flashcardsOnly || selectedSections.has('flashcards'),
      translations: !flashcardsOnly && selectedSections.has('translations'),
      theory: !flashcardsOnly && selectedSections.has('theory'),
      youtube: !flashcardsOnly && selectedSections.has('youtube'),
      // Reader timer is active only while an opened book is actually being read:
      // immersive mode + reader section visible + no archive/settings overlays.
      reader: !flashcardsOnly
        && selectedSections.has('reader')
        && readerImmersive
        && !readerArchiveOpen
        && !readerSettingsOpen,
    };

    const prevVisibility = sectionVisibilitySnapshotRef.current;
    sectionVisibilitySnapshotRef.current = sectionVisibility;
    if (!prevVisibility) return;

    const items = Array.isArray(todayPlan?.items) ? todayPlan.items : [];
    const findTodayTaskByTypesLocal = (types = []) => {
      const normalizedTypes = new Set(types.map((entry) => String(entry || '').toLowerCase()));
      const active = items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase()) && String(entry?.status || '').toLowerCase() !== 'done');
      if (active) return active;
      return items.find((entry) => normalizedTypes.has(String(entry?.task_type || '').toLowerCase())) || null;
    };
    const getSectionTask = (key) => {
      if (key === 'flashcards') return findTodayTaskByTypesLocal(['cards']);
      if (key === 'translations') return findTodayTaskByTypesLocal(['translation']);
      if (key === 'theory') return findTodayTaskByTypesLocal(['theory']);
      if (key === 'youtube') return findTodayTaskByTypesLocal(['video', 'youtube']);
      return null;
    };

    const nowMs = Date.now();
    const timerSyncCalls = [];
    const managedSections = ['flashcards', 'translations', 'theory', 'youtube'];
    let pausedByNavigation = false;
    let resumedByNavigation = false;

    managedSections.forEach((sectionKey) => {
      const wasVisible = Boolean(prevVisibility[sectionKey]);
      const isVisible = Boolean(sectionVisibility[sectionKey]);
      if (wasVisible === isVisible) return;
      const item = getSectionTask(sectionKey);
      if (!item?.id) return;
      const status = String(item?.status || '').toLowerCase();
      if (status === 'done') {
        autoPausedTodayTimerIdsRef.current.delete(item.id);
        return;
      }
      const elapsedSeconds = getTodayItemElapsedSeconds(item, nowMs);
      if (wasVisible && !isVisible && isTodayItemTimerRunning(item)) {
        pausedByNavigation = true;
        if (sectionKey !== 'youtube') {
          autoPausedTodayTimerIdsRef.current.add(item.id);
        }
        timerSyncCalls.push(
          syncTodayItemTimer(item, 'pause', { elapsedSeconds, running: false })
        );
        return;
      }
      if (
        sectionKey !== 'youtube'
        && !wasVisible
        && isVisible
        && !isTodayItemTimerRunning(item)
      ) {
        resumedByNavigation = true;
        const hasStartedBefore = elapsedSeconds > 0 || status === 'doing';
        timerSyncCalls.push(
          syncTodayItemTimer(
            item,
            hasStartedBefore ? 'resume' : 'start',
            { elapsedSeconds, running: true }
          ).then(() => {
            autoPausedTodayTimerIdsRef.current.delete(item.id);
          })
        );
      }
    });

    if (timerSyncCalls.length > 0) {
      void Promise.allSettled(timerSyncCalls);
    }
    if (pausedByNavigation) {
      setGlobalPauseReason(tr('Поставлено на паузу: переход в другой раздел', 'Pausiert: Wechsel in einen anderen Bereich'));
    } else if (resumedByNavigation) {
      setGlobalPauseReason('');
    }

    const readerWasVisible = Boolean(prevVisibility.reader);
    const readerIsVisible = Boolean(sectionVisibility.reader);
    if (readerWasVisible && !readerIsVisible && readerHasContent && !readerTimerPaused) {
      const startedTs = Date.parse(String(readerSessionStartedAt || ''));
      const segmentSeconds = Number.isFinite(startedTs)
        ? Math.max(0, Math.floor((nowMs - startedTs) / 1000))
        : 0;
      if (segmentSeconds > 0) {
        setReaderAccumulatedSeconds((prev) => prev + segmentSeconds);
      }
      setReaderTimerPaused(true);
      setGlobalPauseReason(tr('Поставлено на паузу: переход в другой раздел', 'Pausiert: Wechsel in einen anderen Bereich'));
      readerAutoPausedByNavigationRef.current = true;
      readerAutoPausedByIdleRef.current = false;
      if (readerSessionId) {
        void stopReaderSessionTracking(readerSessionId);
      }
      return;
    }
    if (
      !readerWasVisible
      && readerIsVisible
      && readerHasContent
      && readerTimerPaused
      && readerImmersive
      && !readerArchiveOpen
      && !readerSettingsOpen
    ) {
      readerAutoPausedByIdleRef.current = false;
      readerLastInteractionAtRef.current = Date.now();
      setReaderTimerPaused(false);
      void startReaderSessionTracking();
      setGlobalPauseReason('');
      readerAutoPausedByNavigationRef.current = false;
    }
  }, [
    flashcardsOnly,
    selectedSections,
    todayPlan,
    readerHasContent,
    readerImmersive,
    readerArchiveOpen,
    readerSettingsOpen,
    readerTimerPaused,
    readerSessionStartedAt,
    readerSessionId,
    getTodayItemElapsedSeconds,
    isTodayItemTimerRunning,
    syncTodayItemTimer,
    stopReaderSessionTracking,
    startReaderSessionTracking,
  ]);

  useEffect(() => {
    const youtubeTask = getTodayTaskForSection('youtube');
    if (!youtubeTask?.id) return;
    if (youtubeTodayTimerSyncInFlightRef.current) return;
    const status = String(youtubeTask.status || '').toLowerCase();
    if (status === 'done') return;

    const shouldRunByPlayback = Boolean(
      youtubeSectionVisible
      && youtubePlayerReady
      && youtubePlaybackStarted
      && !youtubeIsPaused
    );
    const isRunningNow = isTodayItemTimerRunning(youtubeTask);
    if (shouldRunByPlayback === isRunningNow) return;

    const elapsedSeconds = getTodayItemElapsedSeconds(youtubeTask, Date.now());
    const hasStartedBefore = elapsedSeconds > 0 || status === 'doing';
    youtubeTodayTimerSyncInFlightRef.current = true;
    void syncTodayItemTimer(
      youtubeTask,
      shouldRunByPlayback ? (hasStartedBefore ? 'resume' : 'start') : 'pause',
      { elapsedSeconds, running: shouldRunByPlayback }
    ).finally(() => {
      youtubeTodayTimerSyncInFlightRef.current = false;
    });
  }, [
    todayPlan,
    youtubeSectionVisible,
    youtubePlayerReady,
    youtubePlaybackStarted,
    youtubeIsPaused,
    getTodayTaskForSection,
    getTodayItemElapsedSeconds,
    isTodayItemTimerRunning,
    syncTodayItemTimer,
  ]);

  useEffect(() => {
    if (telegramApp?.initData) return;
    const stored = safeStorageGet('browser_init_data');
    if (stored && !initData) {
      setInitData(stored);
    }
  }, [telegramApp, initData]);

  useEffect(() => {
    if (!isWebAppMode) return;
    let lockFrame = null;
    const lockHorizontalScroll = () => {
      if (isAndroidTelegramClient && hasFocusedEditableElement()) {
        return;
      }
      const scrollingElement = document.scrollingElement || document.documentElement;
      const pageElement = document.querySelector('.webapp-page');
      if (Math.abs(window.scrollX) > 0) {
        window.scrollTo(0, window.scrollY);
      }
      if (scrollingElement && Math.abs(scrollingElement.scrollLeft) > 0) {
        scrollingElement.scrollLeft = 0;
      }
      if (document.body && Math.abs(document.body.scrollLeft) > 0) {
        document.body.scrollLeft = 0;
      }
      if (pageElement && Math.abs(pageElement.scrollLeft) > 0) {
        pageElement.scrollLeft = 0;
      }
    };
    const requestHorizontalLock = () => {
      if (lockFrame !== null) return;
      lockFrame = window.requestAnimationFrame(() => {
        lockFrame = null;
        lockHorizontalScroll();
      });
    };
    const viewport = window.visualViewport;
    window.addEventListener('scroll', requestHorizontalLock, { passive: true });
    window.addEventListener('resize', requestHorizontalLock);
    viewport?.addEventListener('resize', requestHorizontalLock);
    viewport?.addEventListener('scroll', requestHorizontalLock);
    requestHorizontalLock();
    return () => {
      if (lockFrame !== null) {
        window.cancelAnimationFrame(lockFrame);
        lockFrame = null;
      }
      window.removeEventListener('scroll', requestHorizontalLock);
      window.removeEventListener('resize', requestHorizontalLock);
      viewport?.removeEventListener('resize', requestHorizontalLock);
      viewport?.removeEventListener('scroll', requestHorizontalLock);
    };
  }, [isWebAppMode, isAndroidTelegramClient]);

  useEffect(() => {
    if (telegramApp?.initData) return;
    let cancelled = false;
    fetch('/api/web/auth/config')
      .then(async (res) => {
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      })
      .then((data) => {
        if (cancelled) return;
        setBrowserAuthBotUsername(String(data.telegram_bot_username || '').trim());
      })
      .catch(() => {
        if (!cancelled) setBrowserAuthBotUsername('');
      });
    return () => {
      cancelled = true;
    };
  }, [telegramApp]);

  useEffect(() => {
    if (telegramApp?.initData) return undefined;
    if (!browserAuthBotUsername) return undefined;
    if (!telegramLoginWidgetRef.current) return undefined;
    if (initData) return undefined;

    window.onTelegramAuth = (user) => {
      void handleBrowserTelegramAuth(user);
    };

    const container = telegramLoginWidgetRef.current;
    container.innerHTML = '';
    const script = document.createElement('script');
    script.async = true;
    script.src = 'https://telegram.org/js/telegram-widget.js?22';
    script.setAttribute('data-telegram-login', browserAuthBotUsername);
    script.setAttribute('data-size', 'large');
    script.setAttribute('data-radius', '10');
    script.setAttribute('data-userpic', 'false');
    script.setAttribute('data-request-access', 'write');
    script.setAttribute('data-onauth', 'onTelegramAuth(user)');
    container.appendChild(script);

    return () => {
      delete window.onTelegramAuth;
      if (container) {
        container.innerHTML = '';
      }
    };
  }, [telegramApp, browserAuthBotUsername, initData]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }

    const bootstrap = async () => {
      try {
        setWebappError('');
        setBrowserAuthError('');
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
        const starterOffer = normalizeStarterDictionaryOffer(data?.starter_dictionary);
        setStarterDictionaryOffer(starterOffer);
        setStarterDictionaryPromptOpen(Boolean(starterOffer?.should_prompt));
      } catch (error) {
        if (!telegramApp?.initData && String(error?.message || '').includes('initData не прошёл')) {
          safeStorageRemove('browser_init_data');
          setInitData('');
        }
        setWebappError(`${tr('Ошибка инициализации', 'Initialisierungsfehler')}: ${error.message}`);
      }
    };

    bootstrap();
  }, [initData, isWebAppMode, normalizeStarterDictionaryOffer]);

  useEffect(() => {
    if (isWebAppMode && initData) {
      return;
    }
    setStarterDictionaryOffer(null);
    setStarterDictionaryPromptOpen(false);
    setStarterDictionaryActionLoading(false);
    setStarterDictionaryActionError('');
    setStarterDictionaryActionMessage('');
  }, [isWebAppMode, initData]);

  useEffect(() => {
    if (!isWebAppMode || !initData) return;
    loadLanguageProfile();
  }, [isWebAppMode, initData]);

  useEffect(() => {
    if (!isWebAppMode || !initData || !languageProfile?.has_profile) {
      return;
    }
    void loadStarterDictionaryStatus();
  }, [
    isWebAppMode,
    initData,
    languageProfile?.has_profile,
    languageProfile?.native_language,
    languageProfile?.learning_language,
    loadStarterDictionaryStatus,
  ]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setTodayPlan(null);
      setTodayPlanError('');
      return;
    }
    const timer = window.setTimeout(() => {
      loadTodayPlan();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setSkillReport(null);
      setSkillReportError('');
      return;
    }
    const timer = window.setTimeout(() => {
      loadSkillReport();
    }, 150);
    return () => window.clearTimeout(timer);
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

  useEffect(() => {
    if (!isWebAppMode) {
      skillTrainingDraftMapRef.current = {};
      setSkillTrainingDraftMap({});
      return;
    }
    try {
      const entries = skillTrainingLegacyStorageKeys.map((key) => [key, safeStorageGet(key)]);
      const currentRaw = entries.find(([key]) => key === skillTrainingStorageKey)?.[1] || null;
      const fallbackEntry = entries.find(([, value]) => Boolean(value));
      const activeRaw = currentRaw || fallbackEntry?.[1] || null;
      if (!activeRaw) {
        skillTrainingDraftMapRef.current = {};
        setSkillTrainingDraftMap({});
        return;
      }
      const parsed = JSON.parse(activeRaw);
      const normalized = normalizeSkillTrainingDraftMap(parsed);
      skillTrainingDraftMapRef.current = normalized;
      setSkillTrainingDraftMap(normalized);
      if (!currentRaw && Object.keys(normalized).length > 0) {
        safeStorageSet(skillTrainingStorageKey, JSON.stringify(normalized));
      }
    } catch (_error) {
      skillTrainingDraftMapRef.current = {};
      setSkillTrainingDraftMap({});
    }
  }, [isWebAppMode, skillTrainingLegacyStorageKeys, skillTrainingStorageKey]);

  useEffect(() => {
    const activeSkillId = String(
      skillTrainingData?.skill?.skill_id
      || skillTrainingData?.package?.focus?.skill_id
      || ''
    ).trim();
    if (!activeSkillId || !skillTrainingData?.package) return;
    const snapshot = normalizeSkillTrainingSnapshot({
      skill: skillTrainingData?.skill || {},
      package: skillTrainingData?.package,
      video: skillTrainingData?.video || null,
      answers: skillTrainingAnswers,
      feedback: skillTrainingFeedback,
      saved_at: new Date().toISOString(),
    });
    if (!snapshot) return;
    const current = skillTrainingDraftMapRef.current || {};
    const currentSerialized = JSON.stringify(current[activeSkillId] || null);
    const nextSerialized = JSON.stringify(snapshot);
    if (currentSerialized === nextSerialized) return;
    persistSkillTrainingDraftMap({
      ...current,
      [activeSkillId]: snapshot,
    });
  }, [
    persistSkillTrainingDraftMap,
    skillTrainingAnswers,
    skillTrainingData,
    skillTrainingFeedback,
  ]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setSupportUnreadCount(0);
      setSupportMessages([]);
      setSupportFailedMessages([]);
      return;
    }
    void loadSupportUnread();
    const timer = window.setInterval(() => {
      void loadSupportUnread();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [isWebAppMode, initData, loadSupportUnread]);

  useEffect(() => {
    if (!supportSectionVisible || !initData) return;
    void loadSupportMessages();
    void markSupportMessagesRead();
    const timer = window.setInterval(() => {
      void loadSupportMessages();
      void loadSupportUnread();
    }, 10000);
    return () => window.clearInterval(timer);
  }, [
    supportSectionVisible,
    initData,
    loadSupportMessages,
    markSupportMessagesRead,
    loadSupportUnread,
  ]);

  useEffect(() => {
    if (!supportSectionVisible) return;
    window.requestAnimationFrame(() => {
      supportBottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    });
  }, [supportSectionVisible, supportTimelineItems.length, supportSending]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setWeeklyPlan(null);
      setWeeklyPlanError('');
      setWeeklyPlanDraft({ translations_goal: '', learned_words_goal: '', agent_minutes_goal: '', reading_minutes_goal: '' });
      setWeeklyPlanCollapsed(false);
      setPlanAnalyticsMetrics({});
      setPlanAnalyticsRange(null);
      setPlanAnalyticsError('');
      setWeeklyMetricExpanded({
        translations: true,
        learned_words: true,
        agent_minutes: true,
        reading_minutes: true,
      });
      return;
    }
    const timer = window.setTimeout(() => {
      loadWeeklyPlan();
    }, 300);
    return () => window.clearTimeout(timer);
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setReaderDocuments([]);
      setReaderLibraryError('');
      return;
    }
    loadReaderLibrary();
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language, readerIncludeArchived]);

  useEffect(() => {
    if (!isWebAppMode || !initData) return;
    loadPlanAnalytics(planAnalyticsPeriod);
  }, [planAnalyticsPeriod, isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

  useEffect(() => {
    if (!isWebAppMode) return;
    const saved = safeStorageGet(weeklyPlanCollapseStorageKey);
    setWeeklyPlanCollapsed(saved === '1');
  }, [isWebAppMode, weeklyPlanCollapseStorageKey]);

  useEffect(() => {
    if (!isWebAppMode) return;
    safeStorageSet(weeklyPlanCollapseStorageKey, weeklyPlanCollapsed ? '1' : '0');
  }, [isWebAppMode, weeklyPlanCollapseStorageKey, weeklyPlanCollapsed]);

  useEffect(() => {
    if (!isWebAppMode) return;
    const saved = safeStorageGet(weeklyMetricExpandedStorageKey);
    if (!saved) {
      setWeeklyMetricExpanded(defaultWeeklyMetricExpanded);
      return;
    }
    try {
      const parsed = JSON.parse(saved);
      setWeeklyMetricExpanded({
        ...defaultWeeklyMetricExpanded,
        ...(parsed && typeof parsed === 'object'
          ? Object.fromEntries(
              Object.entries(parsed).map(([key, value]) => [key, Boolean(value)])
            )
          : {}),
      });
    } catch (_error) {
      setWeeklyMetricExpanded(defaultWeeklyMetricExpanded);
    }
  }, [defaultWeeklyMetricExpanded, isWebAppMode, weeklyMetricExpandedStorageKey]);

  useEffect(() => {
    if (!isWebAppMode) return;
    safeStorageSet(weeklyMetricExpandedStorageKey, JSON.stringify({
      translations: Boolean(weeklyMetricExpanded.translations),
      learned_words: Boolean(weeklyMetricExpanded.learned_words),
      agent_minutes: Boolean(weeklyMetricExpanded.agent_minutes),
      reading_minutes: Boolean(weeklyMetricExpanded.reading_minutes),
    }));
  }, [isWebAppMode, weeklyMetricExpanded, weeklyMetricExpandedStorageKey]);

  useEffect(() => {
    if (flashcardsOnly || !selectedSections.has('assistant')) {
      if (assistantSessionId) {
        stopAssistantSessionTracking(assistantSessionId);
      }
      setAssistantToken(null);
    }
  }, [flashcardsOnly, selectedSections]);

  useEffect(() => {
    const shouldTrackReader = Boolean(readerTrackingVisible && !readerTimerPaused);
    if (shouldTrackReader) {
      startReaderSessionTracking();
      return;
    }
    if (readerSessionId) {
      stopReaderSessionTracking(readerSessionId);
    }
  }, [
    readerTrackingVisible,
    readerTimerPaused,
  ]);

  useEffect(() => {
    return () => {
      if (readerSessionId) {
        stopReaderSessionTracking(readerSessionId);
      }
    };
  }, [readerSessionId]);

  useEffect(() => {
    if (readerTimerIntervalRef.current) {
      clearInterval(readerTimerIntervalRef.current);
      readerTimerIntervalRef.current = null;
    }
    if (!readerSessionStartedAt) return;
    const baseTs = new Date(readerSessionStartedAt).getTime();
    if (!Number.isFinite(baseTs) || baseTs <= 0) return;
    setReaderLiveSeconds(Math.max(0, Math.floor((Date.now() - baseTs) / 1000)));
    readerTimerIntervalRef.current = setInterval(() => {
      setReaderLiveSeconds(Math.max(0, Math.floor((Date.now() - baseTs) / 1000)));
    }, 1000);
    return () => {
      if (readerTimerIntervalRef.current) {
        clearInterval(readerTimerIntervalRef.current);
        readerTimerIntervalRef.current = null;
      }
    };
  }, [readerSessionStartedAt]);

  useEffect(() => {
    if (readerIdleTimeoutRef.current) {
      clearTimeout(readerIdleTimeoutRef.current);
      readerIdleTimeoutRef.current = null;
    }
    if (!readerTrackingVisible || readerTimerPaused || !readerHasContent) {
      return undefined;
    }
    if (!readerLastInteractionAtRef.current) {
      readerLastInteractionAtRef.current = Date.now();
    }
    const scheduleIdleCheck = () => {
      const idleForMs = Date.now() - readerLastInteractionAtRef.current;
      const timeoutMs = Math.max(250, READER_IDLE_TIMEOUT_MS - idleForMs);
      readerIdleTimeoutRef.current = window.setTimeout(() => {
        const idleNowMs = Date.now() - readerLastInteractionAtRef.current;
        if (idleNowMs >= READER_IDLE_TIMEOUT_MS) {
          pauseReaderForIdle();
          return;
        }
        scheduleIdleCheck();
      }, timeoutMs);
    };
    scheduleIdleCheck();
    return () => {
      if (readerIdleTimeoutRef.current) {
        clearTimeout(readerIdleTimeoutRef.current);
        readerIdleTimeoutRef.current = null;
      }
    };
  }, [readerTrackingVisible, readerTimerPaused, readerHasContent, pauseReaderForIdle]);

  useEffect(() => {
    if (!isWebAppMode) return;
    const saved = safeStorageGet('reader_swipe_sensitivity');
    if (saved === 'high' || saved === 'medium' || saved === 'low') {
      setReaderSwipeSensitivity(saved);
    }
  }, [isWebAppMode]);

  useEffect(() => {
    if (!isWebAppMode) return;
    safeStorageSet('reader_swipe_sensitivity', readerSwipeSensitivity);
  }, [isWebAppMode, readerSwipeSensitivity]);

  useEffect(() => {
    if (readerHasContent) return;
    setReaderImmersive(false);
    setReaderTimerPaused(false);
    setReaderAccumulatedSeconds(0);
    setReaderLiveSeconds(0);
    setReaderSessionStartedAt('');
    readerAutoPausedByIdleRef.current = false;
    readerLastInteractionAtRef.current = 0;
  }, [readerHasContent]);

  useEffect(() => {
    const node = readerArticleRef.current;
    if (!node || !readerDocumentId || !readerContent) return undefined;
    if (readerPageCount > 0) return undefined;
    const handleScroll = () => {
      markReaderInteraction();
      const nextPercent = computeReaderProgressPercent();
      setReaderProgressPercent(nextPercent);
      if (readerStateSaveTimeoutRef.current) {
        clearTimeout(readerStateSaveTimeoutRef.current);
      }
      readerStateSaveTimeoutRef.current = setTimeout(() => {
        syncReaderState({ progress_percent: Number(nextPercent.toFixed(2)) });
      }, 900);
    };
    node.addEventListener('scroll', handleScroll, { passive: true });
    return () => {
      node.removeEventListener('scroll', handleScroll);
      if (readerStateSaveTimeoutRef.current) {
        clearTimeout(readerStateSaveTimeoutRef.current);
        readerStateSaveTimeoutRef.current = null;
      }
    };
  }, [readerDocumentId, readerReadingMode, readerContent, readerPageCount, markReaderInteraction]);

  useEffect(() => {
    if (!readerDocumentId || readerPageCount === 0) return;
    const nextPercent = computeReaderProgressPercent();
    setReaderProgressPercent(nextPercent);
    syncReaderState({ progress_percent: Number(nextPercent.toFixed(2)) });
  }, [readerCurrentPage, readerDocumentId, readerPageCount]);

  useEffect(() => {
    if (readerPageCount <= 0) {
      setReaderCurrentPage(1);
      return;
    }
    setReaderCurrentPage((prev) => Math.max(1, Math.min(readerPageCount, Number(prev || 1))));
  }, [readerPageCount]);

  useEffect(() => {
    if (!readerDocumentId || !readerContent) return;
    const targetPercent = readerBookmarkPercent > 0 ? readerBookmarkPercent : readerProgressPercent;
    const timer = setTimeout(() => applyReaderProgressPercent(targetPercent), 90);
    return () => clearTimeout(timer);
  }, [readerReadingMode, readerDocumentId, readerContent]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const startParam = String(telegramApp?.initDataUnsafe?.start_param || '').trim().toLowerCase();
    if (params.get('review') === '1') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardActiveMode('fsrs');
      setFlashcardSessionActive(false);
    }
    if (startParam === 'review' || startParam === 'flashcards') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardActiveMode('fsrs');
      setFlashcardSessionActive(false);
    }
    if (startParam === 'analytics') {
      setFlashcardsOnly(false);
      setFlashcardSessionActive(false);
      setSelectedSections(new Set(['analytics']));
      const timer = setTimeout(() => {
        scrollToRef(analyticsRef, { block: 'start' });
      }, 120);
      return () => clearTimeout(timer);
    }
    if (window.location.pathname === '/webapp/review') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardActiveMode('fsrs');
      setFlashcardSessionActive(false);
    }
  }, []);

  useEffect(() => {
    if (!billingReturnContext.shouldHandle && billingReturnContext.section !== 'subscription') {
      return;
    }
    setFlashcardsOnly(false);
    setFlashcardSessionActive(false);
    setSelectedSections(new Set(['subscription']));
    const timer = setTimeout(() => {
      scrollToRef(billingRef, { block: 'start' });
    }, 120);
    return () => clearTimeout(timer);
  }, [billingReturnContext.section, billingReturnContext.shouldHandle]);

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

  const fsrsSectionActive = Boolean(
    initData
    && isSectionVisible('flashcards')
    && flashcardsVisible
    && flashcardActiveMode === 'fsrs'
  );
  const fsrsInitSignature = `${String(initData || '')}|${flashcardsVisible ? 1 : 0}|${String(flashcardActiveMode || '')}|${String(languageProfile?.native_language || '')}|${String(languageProfile?.learning_language || '')}|${isSectionVisible('flashcards') ? 1 : 0}`;

  useEffect(() => {
    if (!fsrsSectionActive) {
      srsInitSignatureRef.current = '';
      return;
    }
    if (srsInitSignatureRef.current === fsrsInitSignature) {
      return;
    }
    srsInitSignatureRef.current = fsrsInitSignature;
    clearSrsReviewRetryTimer();
    srsReviewBufferRef.current = [];
    srsReviewDrainInFlightRef.current = false;
    updateSrsPrefetchQueue([]);
    srsCardRef.current = null;
    setSrsCard(null);
    setSrsState(null);
    setSrsQueueInfo({ due_count: 0, new_remaining_today: 0 });
    setSrsRevealAnswer(false);
    setSrsError('');
    void loadSrsNextCard().then(() => {
      void prefetchSrsCards();
    });
    return () => {
      clearSrsReviewRetryTimer();
    };
  }, [
    fsrsSectionActive,
    fsrsInitSignature,
    clearSrsReviewRetryTimer,
    loadSrsNextCard,
    prefetchSrsCards,
    updateSrsPrefetchQueue,
  ]);

  useEffect(() => {
    if (!flashcardsOnly || !flashcardActiveMode || !isSectionVisible('flashcards')) return;
    void ensureFlashcardsTaskTimerRunning();
  }, [flashcardsOnly, flashcardActiveMode, selectedSections]);

  useEffect(() => {
    if (!initData || !isSectionVisible('flashcards') || !flashcardsVisible || flashcardActiveMode !== 'fsrs') return;
    const activeCardId = getSrsCardId(srsCard);
    const pendingTotal = Math.max(0, Number(srsQueueInfo?.due_count || 0)) + Math.max(0, Number(srsQueueInfo?.new_remaining_today || 0));
    if (!activeCardId && pendingTotal <= 0) return;
    if (srsPrefetchQueue.length > 2) return;
    const signature = `${activeCardId}:${pendingTotal}:${srsPrefetchQueue.length}`;
    if (srsTtsPrefetchSignatureRef.current === signature) return;
    srsTtsPrefetchSignatureRef.current = signature;
    void prefetchSrsCards();
  }, [
    getSrsCardId,
    initData,
    selectedSections,
    flashcardsVisible,
    flashcardActiveMode,
    srsQueueInfo?.due_count,
    srsQueueInfo?.new_remaining_today,
    srsCard?.id,
    srsCard?.entry_id,
    srsPrefetchQueue.length,
    prefetchSrsCards,
  ]);

  useEffect(() => {
    if (!initData || !isSectionVisible('flashcards') || !flashcardsVisible || flashcardActiveMode !== 'fsrs') return;
    const cardsToWarm = [srsCard, ...srsPrefetchQueue.slice(0, 4)];
    cardsToWarm.forEach((card) => {
      if (!card) return;
      const direction = (card?.source_lang || 'ru') === 'de' ? 'de-ru' : 'ru-de';
      const { targetText } = getDictionarySourceTarget(card, direction);
      const text = String(targetText || '').trim();
      if (!text) return;
      const locale = getTtsLocaleForLang(detectTtsLangFromText(text));
      preloadTts(text, locale);
    });
  }, [
    initData,
    flashcardsOnly,
    selectedSections,
    flashcardsVisible,
    flashcardActiveMode,
    languageProfile?.learning_language,
    srsCard,
    srsPrefetchQueue,
    preloadTts,
    getDictionarySourceTarget,
  ]);

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
      stopTtsPlayback();
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
      await playTts(text, getLearningTtsLocale());
      if (cancelled) return;
      setPreviewAudioPlaying(false);
      setPreviewAudioReady(true);
    };
    run();
    return () => {
      cancelled = true;
    };
  }, [flashcardPreviewActive, flashcardsOnly, flashcards, flashcardPreviewIndex, stopTtsPlayback]);

  useEffect(() => {
    flashcardIndexRef.current = flashcardIndex;
  }, [flashcardIndex]);

  useEffect(() => {
    srsCardRef.current = srsCard;
  }, [srsCard]);

  useEffect(() => {
    flashcardSelectionRef.current = flashcardSelection;
  }, [flashcardSelection]);

  useEffect(() => {
    flashcardTrainingModeRef.current = flashcardTrainingMode;
  }, [flashcardTrainingMode]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcards.length === 0) {
      return;
    }
    if (!['quiz', 'sentence'].includes(flashcardTrainingMode)) {
      return;
    }
    setFlashcardTimerKey((prev) => prev + 1);
  }, [flashcardIndex, flashcardSessionActive, flashcards.length, flashcardDurationSec, flashcardTrainingMode]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcardSetComplete || flashcardExitSummary || !flashcards.length) {
      return;
    }
    if (!['quiz', 'sentence'].includes(flashcardTrainingMode)) {
      return;
    }
    if (globalTimerSuspended) {
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
      const timeSpentMs = Math.max(0, Date.now() - flashcardRoundStartRef.current);
      recordFlashcardAnswer(entry.id, false, {
        mode: flashcardTrainingModeRef.current || 'quiz',
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
          await playTts(german, getLearningTtsLocale());
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
    globalTimerSuspended,
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
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setBlocksMenuOpen(false);
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
        throw new Error(await readApiError(response, 'Ошибка загрузки предложений', 'Fehler beim Laden der Saetze'));
      }
      const data = await response.json();
      setSentences(data.items || []);
      setResults([]);
      setTranslationAudioGrammarOptIn({});
      setTranslationAudioGrammarSaving({});
      setFinishStatus('idle');
    } catch (error) {
      setWebappError(`${tr('Ошибка загрузки предложений', 'Fehler beim Laden der Saetze')}: ${error.message}`);
    }
  };

  const loadTopics = async () => {
    setTopicsLoading(true);
    setTopicsError('');
    try {
      const response = await fetch('/api/webapp/topics');
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки тем', 'Fehler beim Laden der Themen'));
      }
      const data = await response.json();
      const items = Array.isArray(data.items) ? data.items : [];
      setTopics(items);
      if (items.length > 0 && !items.includes(selectedTopic)) {
        setSelectedTopic(items.find((item) => !isStoryTopic(item) && !isCustomTopic(item)) || items[0]);
      }
    } catch (error) {
      setTopicsError(`${tr('Ошибка тем', 'Themenfehler')}: ${error.message}`);
    } finally {
      setTopicsLoading(false);
    }
  };

  useEffect(() => {
    translationDraftsRef.current = translationDrafts;
  }, [translationDrafts]);

  useEffect(() => {
    translationDraftSentenceIdsRef.current = new Set(
      sentences
        .map((item) => Number(item?.id_for_mistake_table || 0))
        .filter((value) => Number.isFinite(value) && value > 0)
    );
  }, [sentences]);

  useEffect(() => {
    const stableUserId = String(webappUser?.id || getInitDataUserId(initData) || '').trim();
    if (!stableUserId || sentences.length === 0) {
      translationDraftHydrationKeyRef.current = '';
      if (translationDraftSyncTimeoutRef.current) {
        clearTimeout(translationDraftSyncTimeoutRef.current);
        translationDraftSyncTimeoutRef.current = null;
      }
      if (translationDraftStorageTimeoutRef.current) {
        clearTimeout(translationDraftStorageTimeoutRef.current);
        translationDraftStorageTimeoutRef.current = null;
      }
      translationDraftsRef.current = {};
      setTranslationDrafts({});
      return;
    }
    const sentenceIds = sentences
      .map((item) => String(item.id_for_mistake_table || '').trim())
      .filter(Boolean);
    const emptyDrafts = {};
    sentenceIds.forEach((id) => {
      emptyDrafts[id] = '';
    });
    const stored = translationDraftStorageKey ? safeStorageGet(translationDraftStorageKey) : null;
    let initialLocal = { ...emptyDrafts };
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        if (parsed && typeof parsed === 'object') {
          initialLocal = { ...emptyDrafts };
          sentenceIds.forEach((id) => {
            initialLocal[id] = String(parsed[id] ?? '');
          });
        }
      } catch (error) {
        console.warn('Failed to parse saved drafts', error);
      }
    }
    translationDraftsRef.current = { ...initialLocal };
    setTranslationDrafts(initialLocal);
    const hydrationKey = `${translationDraftStorageKey}:${translationDraftScopeKey || 'nosession'}`;
    translationDraftHydrationKeyRef.current = '';
    let cancelled = false;
    const hydrate = async () => {
      if (!initData || !translationDraftScopeKey) {
        if (!cancelled) {
          translationDraftHydrationKeyRef.current = hydrationKey;
        }
        return;
      }
      try {
        const response = await fetch('/api/webapp/translation/drafts', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData }),
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const data = await response.json();
        const serverDrafts = data?.drafts && typeof data.drafts === 'object' ? data.drafts : {};
        if (cancelled) return;
        setTranslationDrafts((prev) => {
          const next = {};
          sentenceIds.forEach((id) => {
            const baseline = String(initialLocal[id] ?? '');
            const current = String(prev?.[id] ?? '');
            if (current !== baseline) {
              next[id] = current;
              return;
            }
            next[id] = String(serverDrafts[id] ?? baseline);
          });
          translationDraftsRef.current = { ...next };
          return next;
        });
      } catch (error) {
        console.warn('Failed to load translation drafts', error);
      } finally {
        if (!cancelled) {
          translationDraftHydrationKeyRef.current = hydrationKey;
        }
      }
    };
    void hydrate();
    return () => {
      cancelled = true;
    };
  }, [sentences, webappUser?.id, initData, translationDraftScopeKey, translationDraftStorageKey]);

  useEffect(() => {
    if (!initData || !translationDraftScopeKey || sentences.length === 0) {
      return undefined;
    }
    const hydrationKey = `${translationDraftStorageKey}:${translationDraftScopeKey || 'nosession'}`;
    const flushDrafts = () => {
      if (translationDraftHydrationKeyRef.current !== hydrationKey) {
        return;
      }
      if (translationDraftSyncTimeoutRef.current) {
        clearTimeout(translationDraftSyncTimeoutRef.current);
        translationDraftSyncTimeoutRef.current = null;
      }
      if (translationDraftStorageTimeoutRef.current) {
        clearTimeout(translationDraftStorageTimeoutRef.current);
        translationDraftStorageTimeoutRef.current = null;
      }
      flushTranslationDraftsToLocalCache();
      void persistTranslationDraftsToServer(translationDraftsRef.current, {
        keepalive: true,
        silent: true,
      });
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        flushDrafts();
      }
    };
    window.addEventListener('pagehide', flushDrafts);
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      window.removeEventListener('pagehide', flushDrafts);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [
    initData,
    flushTranslationDraftsToLocalCache,
    translationDraftScopeKey,
    translationDraftStorageKey,
    sentences.length,
    persistTranslationDraftsToServer,
  ]);

  useEffect(() => {
    if (isWebAppMode && initData) {
      loadTopics();
      loadSentences();
      loadSessionInfo();
    }
  }, [initData, isWebAppMode]);

  useEffect(() => {
    translationCheckUnmountedRef.current = false;
    return () => {
      translationCheckUnmountedRef.current = true;
      translationCheckPollTokenRef.current += 1;
    };
  }, []);

  useEffect(() => {
    if (isStoryTopic(selectedTopic) && initData) {
      loadStoryHistory();
    }
  }, [selectedTopic, initData]);

  useEffect(() => {
    if (!initData || flashcardsOnly || !selectedSections.has('translations') || isStorySession) {
      return undefined;
    }
    let cancelled = false;
    const run = async () => {
      try {
        await resumeActiveTranslationCheck({ silent: true });
      } catch (_error) {
        if (cancelled) return;
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [initData, flashcardsOnly, selectedSections, isStorySession]);

  useEffect(() => {
    if (!isStoryTopic(selectedTopic)) {
      setStoryResult(null);
      setStoryGuess('');
    }
  }, [selectedTopic]);

  useEffect(() => {
    const stored = safeStorageGet(youtubeResumeStorageKey) || safeStorageGet('webapp_youtube');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        if (parsed?.input) {
          setYoutubeInput(parsed.input);
        }
        if (parsed?.id) {
          setYoutubeId(parsed.id);
        }
        return;
      } catch (_error) {
        // ignore and continue with server fallback
      }
    }
    if (!initData) return;
    let cancelled = false;
    fetch('/api/webapp/youtube/state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ initData }),
    })
      .then(async (response) => {
        if (!response.ok) throw new Error(await response.text());
        return response.json();
      })
      .then((data) => {
        if (cancelled) return;
        const state = data?.state;
        const savedId = String(state?.video_id || '').trim();
        const savedInput = String(state?.input_text || '').trim();
        const savedTime = Math.max(0, Number(state?.current_time_seconds || 0));
        if (!savedId) return;
        writeYoutubeResumeToLocalCache({
          input: savedInput || `https://youtu.be/${savedId}`,
          id: savedId,
          currentTime: savedTime,
          updatedAt: Date.now(),
        });
        setYoutubeInput(savedInput || `https://youtu.be/${savedId}`);
        setYoutubeId(savedId);
      })
      .catch(() => {
        // ignore server resume bootstrap errors
      });
    return () => {
      cancelled = true;
    };
  }, [initData, writeYoutubeResumeToLocalCache, youtubeResumeStorageKey]);

  const buildTranslationResultFromCheckItem = (item) => {
    if (!item || typeof item !== 'object') return null;
    if (item.result_json && typeof item.result_json === 'object') {
      return {
        ...item.result_json,
        check_item_id: item.id ?? null,
        item_order: item.item_order ?? null,
        sentence_id_for_mistake_table: item.sentence_id_for_mistake_table ?? null,
      };
    }
    const errorText = String(item.error_text || '').trim();
    if (!errorText) return null;
    return {
      check_item_id: item.id ?? null,
      item_order: item.item_order ?? null,
      sentence_number: item.sentence_number ?? null,
      sentence_id_for_mistake_table: item.sentence_id_for_mistake_table ?? null,
      original_text: item.original_text || '',
      user_translation: item.user_translation || '',
      error: errorText,
    };
  };

  const applyTranslationCheckStatusPayload = (payload) => {
    const checkSession = payload?.check_session && typeof payload.check_session === 'object'
      ? payload.check_session
      : null;
    const checkItems = Array.isArray(payload?.items) ? payload.items : [];
    const mappedResults = checkItems
      .map((item) => buildTranslationResultFromCheckItem(item))
      .filter(Boolean)
      .sort((a, b) => {
        const orderA = Number.isFinite(Number(a?.item_order)) ? Number(a.item_order) : Number(a?.sentence_number || 0);
        const orderB = Number.isFinite(Number(b?.item_order)) ? Number(b.item_order) : Number(b?.sentence_number || 0);
        return orderA - orderB;
      });
    const audioOptInMap = {};
    mappedResults.forEach((item) => {
      const translationId = Number(item?.translation_id || 0);
      if (translationId > 0) {
        audioOptInMap[translationId] = Boolean(item?.audio_grammar_opt_in);
      }
    });

    setResults(mappedResults);
    setTranslationAudioGrammarOptIn(audioOptInMap);
    const processedSentenceIds = new Set(
      checkItems
        .filter((item) => {
          const status = String(item?.status || '').trim().toLowerCase();
          return status === 'done' || status === 'failed';
        })
        .map((item) => Number(item?.sentence_id_for_mistake_table || 0))
        .filter((value) => Number.isFinite(value) && value > 0)
    );
    if (processedSentenceIds.size > 0) {
      setSentences((prev) => prev.filter((item) => !processedSentenceIds.has(Number(item?.id_for_mistake_table || 0))));
      if (translationDraftStorageTimeoutRef.current) {
        clearTimeout(translationDraftStorageTimeoutRef.current);
        translationDraftStorageTimeoutRef.current = null;
      }
      if (translationDraftSyncTimeoutRef.current) {
        clearTimeout(translationDraftSyncTimeoutRef.current);
        translationDraftSyncTimeoutRef.current = null;
      }
      const nextDraftRefMap = { ...translationDraftsRef.current };
      processedSentenceIds.forEach((sentenceId) => {
        delete nextDraftRefMap[String(sentenceId)];
      });
      translationDraftsRef.current = nextDraftRefMap;
      setTranslationDrafts((prev) => {
        const next = { ...prev };
        processedSentenceIds.forEach((sentenceId) => {
          delete next[String(sentenceId)];
        });
        return next;
      });
      void clearTranslationDraftsOnServer(Array.from(processedSentenceIds), { silent: true });
    }

    const progress = payload?.progress && typeof payload.progress === 'object' ? payload.progress : {};
    const total = Math.max(0, Number(progress.total || checkSession?.total_items || checkItems.length || 0));
    const completed = Math.max(0, Number(progress.completed || checkSession?.completed_items || 0));
    const failed = Math.max(0, Number(progress.failed || checkSession?.failed_items || 0));
    const done = Math.min(total, completed + failed);
    const status = String(checkSession?.status || '').trim().toLowerCase();
    const active = status === 'queued' || status === 'running';
    setTranslationCheckProgress({ active, done, total });

    return {
      status,
      done,
      total,
      sessionId: checkSession?.id ?? null,
    };
  };

  const pollTranslationCheckStatus = async ({ checkSessionId: checkSessionIdParam, pollToken }) => {
    let attempt = 0;
    while (!translationCheckUnmountedRef.current && translationCheckPollTokenRef.current === pollToken) {
      if (attempt > 0) {
        await new Promise((resolve) => setTimeout(resolve, 900));
      }
      attempt += 1;
      let response;
      try {
        response = await fetch('/api/webapp/check/status', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            check_session_id: checkSessionIdParam,
          }),
        });
      } catch (error) {
        if (attempt >= 3) {
          throw error;
        }
        continue;
      }

      if (!response.ok) {
        const apiMessage = await readApiError(
          response,
          'Не удалось получить статус проверки.',
          'Pruefungsstatus konnte nicht geladen werden.'
        );
        throw new Error(apiMessage);
      }

      const data = await response.json();
      const nextState = applyTranslationCheckStatusPayload(data);
      if (!nextState.sessionId) {
        throw new Error(tr('Сессия проверки не найдена.', 'Pruefungssession wurde nicht gefunden.'));
      }
      if (nextState.status === 'done' || nextState.status === 'failed' || nextState.status === 'canceled') {
        void loadTodayPlan();
        return data;
      }
    }
    return null;
  };

  const resumeActiveTranslationCheck = async ({ silent = false } = {}) => {
    if (!initData || isStorySession) return false;
    if (webappLoading && translationCheckProgress.active) return true;
    try {
      const response = await fetch('/api/webapp/check/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          active_only: true,
        }),
      });
      if (!response.ok) {
        const apiMessage = await readApiError(
          response,
          'Не удалось восстановить проверку переводов.',
          'Uebersetzungspruefung konnte nicht wiederhergestellt werden.'
        );
        throw new Error(apiMessage);
      }

      const data = await response.json();
      const activeSession = data?.check_session && typeof data.check_session === 'object'
        ? data.check_session
        : null;
      if (!activeSession?.id) {
        return false;
      }

      const pollToken = translationCheckPollTokenRef.current + 1;
      translationCheckPollTokenRef.current = pollToken;
      if (!silent) {
        setWebappError('');
      }
      setWebappLoading(true);
      setTranslationAudioGrammarSaving({});
      setExplanations({});
      setExplanationLoading({});
      const nextState = applyTranslationCheckStatusPayload(data);
      await pollTranslationCheckStatus({
        checkSessionId: nextState.sessionId,
        pollToken,
      });
      return true;
    } finally {
      setWebappLoading(false);
    }
  };

  const handleWebappSubmit = async (event) => {
    event.preventDefault();
    const liveDrafts = translationDraftsRef.current && typeof translationDraftsRef.current === 'object'
      ? translationDraftsRef.current
      : {};
    if (translationSubmitInFlightRef.current) {
      return;
    }
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    if (sentences.length === 0) {
      setWebappError(tr('Нет предложений для перевода.', 'Keine Saetze zur Uebersetzung vorhanden.'));
      return;
    }
    if (Object.values(liveDrafts).every((text) => !String(text || '').trim())) {
      setWebappError(tr('Заполните хотя бы один перевод.', 'Bitte fuelle mindestens eine Uebersetzung aus.'));
      return;
    }

    setWebappLoading(true);
    translationSubmitInFlightRef.current = true;
    setWebappError('');
    setResults([]);
    setTranslationAudioGrammarOptIn({});
    setTranslationAudioGrammarSaving({});
    setExplanations({});
    setExplanationLoading({});
    setTranslationCheckProgress({ active: false, done: 0, total: 0 });

    try {
      const currentSentenceIds = new Set(
        sentences
          .map((item) => Number(item?.id_for_mistake_table || 0))
          .filter((value) => Number.isFinite(value) && value > 0)
      );
      const submittedEntries = Object.entries(liveDrafts)
        .map(([id, translation]) => ({ id: Number(id), translation: String(translation || '').trim() }))
        .filter((item) => currentSentenceIds.has(item.id) && item.translation);
      if (!submittedEntries.length) {
        throw new Error(tr('Нет переводов для проверки.', 'Keine Uebersetzungen zur Pruefung.'));
      }
      const pollToken = translationCheckPollTokenRef.current + 1;
      translationCheckPollTokenRef.current = pollToken;
      setTranslationCheckProgress({ active: true, done: 0, total: submittedEntries.length });

      const startResponse = await fetch('/api/webapp/check/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          session_id: sessionId,
          translations: submittedEntries.map((item) => ({
            id_for_mistake_table: item.id,
            translation: item.translation,
          })),
          send_private_grammar_text: translationPrivateGrammarTextOptIn,
        }),
      });
      if (!startResponse.ok) {
        const apiMessage = await readApiError(
          startResponse,
          'Не удалось запустить проверку переводов.',
          'Uebersetzungspruefung konnte nicht gestartet werden.'
        );
        throw new Error(apiMessage);
      }
      const startData = await startResponse.json();
      const startState = applyTranslationCheckStatusPayload(startData);
      if (!startState.sessionId) {
        throw new Error(tr('Не удалось создать сессию проверки.', 'Pruefungssession konnte nicht erstellt werden.'));
      }
      showInlineToast(
        tr(
          'Можно подождать здесь или вернуться позже. Проверка продолжится в фоновом режиме.',
          'Du kannst hier warten oder spaeter zurueckkommen. Die Pruefung laeuft im Hintergrund weiter.'
        ),
        3000,
      );
      await pollTranslationCheckStatus({
        checkSessionId: startState.sessionId,
        pollToken,
      });
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось проверить переводы.',
        'Uebersetzungen konnten nicht geprueft werden.'
      );
      setWebappError(`${tr('Ошибка проверки', 'Pruefungsfehler')}: ${friendly}`);
    } finally {
      translationSubmitInFlightRef.current = false;
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
    if (translationStartInFlightRef.current) {
      return;
    }
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    if (isCustomTopic(selectedTopic) && !customTopicInput.trim()) {
      setWebappError(tr('Введите свой грамматический фокус.', 'Gib deinen eigenen Grammatikfokus ein.'));
      return;
    }
    translationStartInFlightRef.current = true;
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    setTranslationCheckProgress({ active: false, done: 0, total: 0 });
    try {
      const response = await fetch('/api/webapp/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          topic: selectedTopic,
          custom_focus: isCustomTopic(selectedTopic) ? customTopicInput.trim() : '',
          level: selectedLevel,
          force_new_session: false,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      if (data.blocked) {
        setFinishMessage(tr('Есть активная сессия. Завершите текущий перевод, чтобы получить новый сет.', 'Es gibt eine aktive Session. Beende die aktuelle Uebersetzung, um ein neues Set zu erhalten.'));
      }
      await loadSessionInfo();
      await loadSentences();
    } catch (error) {
      setWebappError(`${tr('Ошибка старта', 'Startfehler')}: ${error.message}`);
    } finally {
      translationStartInFlightRef.current = false;
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
      setStoryHistoryError(`${tr('Ошибка истории', 'Story-Fehler')}: ${error.message}`);
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
      return data;
    } catch (error) {
      // silent
      return null;
    }
  };

  const handleStartStory = async () => {
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    setTranslationCheckProgress({ active: false, done: 0, total: 0 });
    setStoryResult(null);
    setResults([]);
    setTranslationAudioGrammarOptIn({});
    setTranslationAudioGrammarSaving({});
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
        setFinishMessage(tr('Есть активная сессия. Завершите текущий перевод, чтобы получить новый сет.', 'Es gibt eine aktive Session. Beende die aktuelle Uebersetzung, um ein neues Set zu erhalten.'));
        const sessionInfo = await loadSessionInfo();
        if (String(sessionInfo?.type || '') === 'story') {
          await loadSentences();
        } else {
          setSentences([]);
        }
        return;
      }
      await loadSessionInfo();
      await loadSentences();
    } catch (error) {
      setWebappError(`${tr('Ошибка старта истории', 'Story-Startfehler')}: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleStorySubmit = async () => {
    const liveDrafts = translationDraftsRef.current && typeof translationDraftsRef.current === 'object'
      ? translationDraftsRef.current
      : {};
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    const missing = sentences.filter((item) => {
      const value = (liveDrafts[String(item.id_for_mistake_table)] || '').trim();
      return !value;
    });
    if (missing.length > 0) {
      setWebappError(tr('Для истории нужно перевести все 7 предложений.', 'Fuer die Story muessen alle 7 Saetze uebersetzt werden.'));
      return;
    }
    if (!storyGuess.trim()) {
      setWebappError(tr('Введите ваш ответ: о ком/чем была история.', 'Gib deine Antwort ein: Worum oder um wen ging die Story?'));
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setTranslationCheckProgress({ active: false, done: 0, total: 0 });
    setResults([]);
    setTranslationAudioGrammarOptIn({});
    setTranslationAudioGrammarSaving({});
    setExplanations({});
    try {
      const response = await fetch('/api/webapp/story/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          translations: Object.entries(liveDrafts).map(([id, translation]) => ({
            id_for_mistake_table: Number(id),
            translation,
          })),
          guess: storyGuess,
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка проверки истории', 'Fehler beim Pruefen der Story'));
      }
      const data = await response.json();
      setStoryResult(data);
      setSentences([]);
      await loadSessionInfo();
    } catch (error) {
      setWebappError(`${tr('Ошибка истории', 'Story-Fehler')}: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleDraftLiveChange = useCallback((sentenceId, value) => {
    const key = String(sentenceId);
    translationDraftsRef.current[key] = value;
    scheduleTranslationDraftPersistence({ local: true, server: false });
  }, [scheduleTranslationDraftPersistence]);

  const handleDraftCommit = useCallback((sentenceId, value, options = {}) => {
    const key = String(sentenceId);
    translationDraftsRef.current[key] = value;
    if (options?.reason !== 'blur') {
      return;
    }
    const applyStateUpdate = () => {
      setTranslationDrafts((prev) => {
        if ((prev[key] || '') === value) {
          return prev;
        }
        return {
          ...prev,
          [key]: value,
        };
      });
    };
    if (typeof React.startTransition === 'function') {
      React.startTransition(applyStateUpdate);
    } else {
      applyStateUpdate();
    }
    scheduleTranslationDraftPersistence({
      immediateLocal: true,
      immediateServer: true,
      local: true,
      server: true,
    });
  }, [scheduleTranslationDraftPersistence]);

  const normalizeSelectionText = (value) => {
    if (!value) return '';
    return value.replace(/\s+/g, ' ').trim();
  };

  function splitNonWordToken(value) {
    const chunks = [];
    if (!value) return chunks;
    const regex = /(\s+|[^\s]+)/g;
    let match = regex.exec(value);
    while (match) {
      const piece = String(match[0] || '');
      if (piece) {
        chunks.push({
          kind: /^\s+$/u.test(piece) ? 'space' : 'punct',
          value: piece,
          relativeStart: Number(match.index || 0),
          relativeEnd: Number(match.index || 0) + piece.length,
        });
      }
      match = regex.exec(value);
    }
    return chunks;
  }

  function segmentText(rawText, langHint) {
    const text = String(rawText || '');
    if (!text) return [];
    const safeLang = normalizeLangCode(langHint || '') || 'de';
    const wordRegex = /[A-Za-z0-9À-ÿА-Яа-яЁё'’-]/u;

    const tokenizeSentence = (sentenceText, sentenceStart, sid) => {
      const tokens = [];
      let wordIndex = 0;
      if (typeof Intl !== 'undefined' && typeof Intl.Segmenter === 'function') {
        try {
          const wordSegmenter = new Intl.Segmenter(safeLang, { granularity: 'word' });
          const segmented = wordSegmenter.segment(sentenceText);
          for (const part of segmented) {
            const value = String(part?.segment || '');
            if (!value) continue;
            const tokenStart = Number(sentenceStart) + Number(part?.index || 0);
            const tokenEnd = tokenStart + value.length;
            if (part?.isWordLike) {
              const wid = `${sid}-w-${wordIndex}-${tokenStart}-${tokenEnd}`;
              wordIndex += 1;
              tokens.push({ kind: 'word', wid, value, start: tokenStart, end: tokenEnd });
              continue;
            }
            const chunks = splitNonWordToken(value);
            if (!chunks.length) {
              tokens.push({
                kind: /^\s+$/u.test(value) ? 'space' : 'punct',
                value,
                start: tokenStart,
                end: tokenEnd,
              });
              continue;
            }
            chunks.forEach((chunk) => {
              tokens.push({
                kind: chunk.kind,
                value: chunk.value,
                start: tokenStart + chunk.relativeStart,
                end: tokenStart + chunk.relativeEnd,
              });
            });
          }
          return tokens;
        } catch (_intlWordError) {
          // fallback below
        }
      }

      const fallbackWordTokenRegex = /(\s+|[A-Za-z0-9À-ÿА-Яа-яЁё'’-]+|[^A-Za-z0-9À-ÿА-Яа-яЁё'’-\s]+)/g;
      let match = fallbackWordTokenRegex.exec(sentenceText);
      while (match) {
        const value = String(match[0] || '');
        const tokenStart = Number(sentenceStart) + Number(match.index || 0);
        const tokenEnd = tokenStart + value.length;
        const isWord = wordRegex.test(value);
        if (isWord && !/^\s+$/u.test(value)) {
          const wid = `${sid}-w-${wordIndex}-${tokenStart}-${tokenEnd}`;
          wordIndex += 1;
          tokens.push({ kind: 'word', wid, value, start: tokenStart, end: tokenEnd });
        } else {
          tokens.push({
            kind: /^\s+$/u.test(value) ? 'space' : 'punct',
            value,
            start: tokenStart,
            end: tokenEnd,
          });
        }
        match = fallbackWordTokenRegex.exec(sentenceText);
      }
      return tokens;
    };

    const buildSentence = (sentenceText, startIndex, endIndex, sidIndex) => {
      const sid = `s-${sidIndex}-${startIndex}-${endIndex}`;
      return {
        sid,
        text: sentenceText,
        start: startIndex,
        end: endIndex,
        tokens: tokenizeSentence(sentenceText, startIndex, sid),
      };
    };

    const sentences = [];
    if (typeof Intl !== 'undefined' && typeof Intl.Segmenter === 'function') {
      try {
        const sentenceSegmenter = new Intl.Segmenter(safeLang, { granularity: 'sentence' });
        const segmented = sentenceSegmenter.segment(text);
        let sidIndex = 0;
        for (const part of segmented) {
          const sentenceText = String(part?.segment || '');
          if (!sentenceText) continue;
          const startIndex = Number(part?.index || 0);
          const endIndex = startIndex + sentenceText.length;
          sentences.push(buildSentence(sentenceText, startIndex, endIndex, sidIndex));
          sidIndex += 1;
        }
        if (sentences.length > 0) return sentences;
      } catch (_intlSentenceError) {
        // fallback below
      }
    }

    const fallbackRegex = /([.!?]+|\n+)/g;
    let sidIndex = 0;
    let cursor = 0;
    let match = fallbackRegex.exec(text);
    while (match) {
      const end = Number(match.index || 0) + String(match[0] || '').length;
      const segment = text.slice(cursor, end);
      if (segment) {
        sentences.push(buildSentence(segment, cursor, end, sidIndex));
        sidIndex += 1;
      }
      cursor = end;
      match = fallbackRegex.exec(text);
    }
    if (cursor < text.length) {
      const segment = text.slice(cursor);
      const end = text.length;
      if (segment) {
        sentences.push(buildSentence(segment, cursor, end, sidIndex));
      }
    }
    return sentences;
  }

  const normalizeFolderKey = (value) => normalizeSelectionText(value).toLocaleLowerCase();

  const showInlineToast = (text, durationMs = 3000) => {
    const value = normalizeSelectionText(text);
    if (!value) return;
    if (inlineToastTimeoutRef.current) {
      clearTimeout(inlineToastTimeoutRef.current);
      inlineToastTimeoutRef.current = null;
    }
    const safeDurationMs = Math.max(1000, Number(durationMs || 3000));
    setInlineToastDurationMs(safeDurationMs);
    setInlineToast(value);
    inlineToastTimeoutRef.current = setTimeout(() => {
      setInlineToast('');
      inlineToastTimeoutRef.current = null;
    }, safeDurationMs);
  };

  const resolveYoutubeAutoFolderName = () => {
    const fromPlayer = normalizeSelectionText(
      youtubePlayerRef.current?.getVideoData?.()?.title || ''
    );
    const fromCatalog = normalizeSelectionText(
      movies.find((item) => item.video_id === youtubeId)?.title || ''
    );
    const rawTitle = fromPlayer || fromCatalog;
    if (!rawTitle) {
      return youtubeId ? `YouTube ${youtubeId.slice(0, 6)}` : 'YouTube';
    }
    const tokens = rawTitle
      .replace(/[|/\\()[\]{}"'`.,!?;:]+/g, ' ')
      .split(/\s+/)
      .map((token) => token.trim())
      .filter(Boolean);
    if (tokens.length === 0) {
      return youtubeId ? `YouTube ${youtubeId.slice(0, 6)}` : 'YouTube';
    }
    return tokens.slice(0, 2).join(' ');
  };

  const ensureYoutubeAutoFolderId = async () => {
    if (!initData) return null;
    const folderName = resolveYoutubeAutoFolderName();
    const folderKey = normalizeFolderKey(folderName);
    if (!folderKey) return null;
    if (youtubeAutoFolderCacheRef.current.has(folderKey)) {
      return youtubeAutoFolderCacheRef.current.get(folderKey);
    }
    const pending = youtubeAutoFolderPendingRef.current.get(folderKey);
    if (pending) {
      return pending;
    }

    const task = (async () => {
      const localMatch = folders.find((item) => normalizeFolderKey(item?.name || '') === folderKey);
      if (localMatch?.id) {
        const result = { id: localMatch.id, name: localMatch.name || folderName };
        youtubeAutoFolderCacheRef.current.set(folderKey, result);
        return result;
      }

      const listResponse = await fetch('/api/webapp/dictionary/folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!listResponse.ok) {
        throw new Error(await listResponse.text());
      }
      const listData = await listResponse.json();
      const items = Array.isArray(listData.items) ? listData.items : [];
      setFolders(items);
      const existing = items.find((item) => normalizeFolderKey(item?.name || '') === folderKey);
      if (existing?.id) {
        const result = { id: existing.id, name: existing.name || folderName };
        youtubeAutoFolderCacheRef.current.set(folderKey, result);
        return result;
      }

      const createResponse = await fetch('/api/webapp/dictionary/folders/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          name: folderName,
          color: '#5ddcff',
          icon: 'book',
        }),
      });
      if (!createResponse.ok) {
        throw new Error(await createResponse.text());
      }
      const createData = await createResponse.json();
      const created = createData.item;
      if (created?.id) {
        youtubeAutoFolderCacheRef.current.set(folderKey, { id: created.id, name: created.name || folderName });
      }
      setFolders((prev) => {
        const exists = prev.some((item) => item.id === created?.id);
        return exists ? prev : [created, ...prev];
      });
      return created?.id ? { id: created.id, name: created.name || folderName } : null;
    })()
      .finally(() => {
        youtubeAutoFolderPendingRef.current.delete(folderKey);
      });

    youtubeAutoFolderPendingRef.current.set(folderKey, task);
    return task;
  };

  const resolveDictionaryDirection = (item) => {
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const forward = `${pair.source_lang}-${pair.target_lang}`;
    const reverse = `${pair.target_lang}-${pair.source_lang}`;
    if (!item) return forward;
    if (item.source_text || item.target_text) return forward;
    if (item.translation_de) return 'ru-de';
    if (item.translation_ru) return 'de-ru';
    if (item.word_de) return 'de-ru';
    return forward;
  };

  function normalizeLangCode(value) {
    return String(value || '').trim().toLowerCase();
  }
  const prependArticleIfNeeded = (text, article, partOfSpeech) => {
    const base = String(text || '').trim();
    const articleText = String(article || '').trim();
    if (!base || !articleText) return base;
    const pos = String(partOfSpeech || '').trim().toLowerCase();
    if (pos && !/(noun|substantiv|nomen|sostantiv|sustantiv)/.test(pos)) {
      return base;
    }
    const firstToken = String(base.split(/\s+/, 1)[0] || '')
      .toLowerCase()
      .replace(/[.,;:!?]+$/g, '');
    const normalizedArticle = articleText.toLowerCase().replace(/[.,;:!?]+$/g, '');
    if (firstToken === normalizedArticle) return base;
    return `${articleText} ${base}`.replace(/\s+/g, ' ').trim();
  };
  const applyArticleForDirection = (sourceText, targetText, direction, item) => {
    let source = String(sourceText || '').trim();
    let target = String(targetText || '').trim();
    const article = String(item?.article || '').trim();
    if (!article) return { sourceText: source, targetText: target };
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const normalizedDirection = String(direction || '').trim().toLowerCase();
    let sourceLang = pair.source_lang;
    let targetLang = pair.target_lang;
    if (normalizedDirection.includes('-')) {
      const [src, tgt] = normalizedDirection.split('-', 2);
      sourceLang = src || sourceLang;
      targetLang = tgt || targetLang;
    }
    const langsWithArticles = new Set(['de', 'it', 'es', 'en']);
    if (langsWithArticles.has(sourceLang)) {
      source = prependArticleIfNeeded(source, article, item?.part_of_speech);
    }
    if (langsWithArticles.has(targetLang)) {
      target = prependArticleIfNeeded(target, article, item?.part_of_speech);
    }
    return { sourceText: source, targetText: target };
  };
  const getMovieLanguageCode = (item) => normalizeLangCode(item?.language || '').slice(0, 2) || 'unknown';
  const resolveLanguagePairForUI = (pair) => {
    const source = normalizeLangCode(pair?.source_lang);
    const target = normalizeLangCode(pair?.target_lang);
    if (source && target) return { source_lang: source, target_lang: target };
    const profileSource = normalizeLangCode(languageProfile?.native_language) || 'ru';
    const profileTarget = normalizeLangCode(languageProfile?.learning_language) || 'de';
    return { source_lang: profileSource, target_lang: profileTarget };
  };
  const getDictionaryDirectionLabel = () => {
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const source = String(pair.source_lang || '').toUpperCase();
    const target = String(pair.target_lang || '').toUpperCase();
    return `${source} → ${target}`;
  };
  const getLookupDirectionLabel = () => {
    const dir = String(dictionaryDirection || '').trim().toLowerCase();
    if (dir && dir.includes('-')) {
      const [from, to] = dir.split('-', 2);
      if (from && to) return `${from.toUpperCase()} → ${to.toUpperCase()}`;
    }
    return getDictionaryDirectionLabel();
  };
  const extractCorrectTranslationText = (item) => {
    const direct = String(item?.correct_translation || '').trim();
    if (direct && direct !== '—') return direct;
    const feedback = String(item?.feedback || '').trim();
    if (!feedback) return '';
    const patterns = [
      /Correct Translation:\*?\s*([^\n]+)/i,
      /Korrigierte Version:\*?\s*([^\n]+)/i,
      /Исправленный вариант:\*?\s*([^\n]+)/i,
    ];
    for (const pattern of patterns) {
      const match = feedback.match(pattern);
      if (match?.[1]) {
        const cleaned = String(match[1]).replace(/[*`_]/g, '').trim();
        if (cleaned && cleaned !== '—') return cleaned;
      }
    }
    return '';
  };
  const getActiveLanguagePairLabel = () => {
    const source = String(languageProfile?.native_language || 'ru').toUpperCase();
    const target = String(languageProfile?.learning_language || 'de').toUpperCase();
    return `${source} → ${target}`;
  };
  const getNativeSubtitleCode = () => {
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    return String(pair.source_lang || languageProfile?.native_language || 'ru').toUpperCase();
  };
  const movieLanguageOptions = useMemo(() => {
    const set = new Set();
    movies.forEach((item) => set.add(getMovieLanguageCode(item)));
    return Array.from(set)
      .filter(Boolean)
      .sort();
  }, [movies]);
  const moviesFiltered = useMemo(() => {
    if (moviesLanguageFilter === 'all') return movies;
    return movies.filter((item) => getMovieLanguageCode(item) === moviesLanguageFilter);
  }, [movies, moviesLanguageFilter]);
  function getDictionarySourceTarget(item, direction = dictionaryDirection) {
    if (!item) return { sourceText: '', targetText: '' };
    const sourceTextRaw = String(
      item.source_text
      || (direction === 'de-ru'
        ? (item.word_de || item.translation_de || '')
        : (item.word_ru || item.translation_ru || ''))
      || ''
    ).trim();
    const targetTextRaw = String(
      item.target_text
      || (direction === 'de-ru'
        ? (item.translation_ru || item.word_ru || '')
        : (item.translation_de || item.word_de || ''))
      || ''
    ).trim();
    return applyArticleForDirection(sourceTextRaw, targetTextRaw, direction, item);
  }
  function getDictionaryDisplayedTranslation(item, direction = dictionaryDirection) {
    const { sourceText, targetText } = getDictionarySourceTarget(item, direction);
    return targetText || sourceText || '';
  }
  const getFormValue = (forms, keys) => {
    if (!forms || typeof forms !== 'object') return '';
    const candidates = Array.isArray(keys) ? keys : [keys];
    for (const key of candidates) {
      const value = String(forms?.[key] ?? '').trim();
      if (value && value !== '-' && value !== '—') return value;
    }
    return '';
  };
  const getDictionaryFormRows = (item) => {
    const forms = item?.forms;
    if (!forms || typeof forms !== 'object') return [];
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const learningLang = normalizeLangCode(pair?.target_lang || languageProfile?.learning_language || 'de');

    const byLang = {
      de: [
        { label: 'Plural', keys: ['plural'] },
        { label: 'Präteritum', keys: ['praeteritum'] },
        { label: 'Perfekt', keys: ['perfekt'] },
        { label: 'Konjunktiv I', keys: ['konjunktiv1'] },
        { label: 'Konjunktiv II', keys: ['konjunktiv2'] },
      ],
      it: [
        { label: 'Plurale', keys: ['plural'] },
        { label: 'Presente', keys: ['presente'] },
        { label: 'Passato prossimo', keys: ['passato_prossimo', 'past_perfect', 'perfekt'] },
        { label: 'Imperfetto', keys: ['imperfetto'] },
        { label: 'Congiuntivo', keys: ['congiuntivo'] },
      ],
      es: [
        { label: 'Plural', keys: ['plural'] },
        { label: 'Presente', keys: ['presente'] },
        { label: 'Pretérito', keys: ['preterito', 'past', 'praeteritum'] },
        { label: 'Pretérito perfecto', keys: ['preterito_perfecto', 'perfekt'] },
        { label: 'Subjuntivo', keys: ['subjuntivo', 'konjunktiv1', 'konjunktiv2'] },
      ],
      en: [
        { label: 'Plural', keys: ['plural'] },
        { label: 'Past', keys: ['past', 'praeteritum'] },
        { label: 'Past participle', keys: ['past_participle', 'perfekt'] },
        { label: 'Gerund', keys: ['gerund'] },
      ],
    };

    const schema = byLang[learningLang] || byLang.en;
    return schema
      .map((field) => ({ label: field.label, value: getFormValue(forms, field.keys) }))
      .filter((row) => row.value);
  };

  const isLegacyRuDeDirection = (direction) => direction === 'ru-de' || direction === 'de-ru';
  const getNormalizeLookupLang = () => {
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    return pair.target_lang || languageProfile?.learning_language || 'de';
  };

  const hasCyrillic = (value) => /[А-Яа-яЁё]/.test(value || '');
  const hasLatin = (value) => /[A-Za-zÄÖÜäöüßÀ-ÿ]/.test(value || '');
  const cyrillicLangs = new Set(['ru', 'uk', 'be', 'bg', 'sr', 'mk']);
  const isCyrillicLang = (lang) => cyrillicLangs.has(String(lang || '').toLowerCase());

  const handleSelection = (event, overrideText = '', options = {}) => {
    const text = overrideText || normalizeSelectionText(window.getSelection()?.toString() || '');
    if (!text) {
      clearSelection();
      return;
    }
    const nextSelectionType = String(options?.selectionType || '');
    const isYoutubeSelection = nextSelectionType.startsWith('youtube_');
    if (isYoutubeSelection) {
      try {
        const playerState = youtubePlayerRef.current?.getPlayerState?.();
        if (playerState === 1) {
          youtubePlayerRef.current?.pauseVideo?.();
          youtubePausedBySelectionRef.current = true;
        } else {
          youtubePausedBySelectionRef.current = false;
        }
      } catch (_playerStateError) {
        youtubePausedBySelectionRef.current = false;
      }
    }
    const clientX = event?.clientX ?? event?.touches?.[0]?.clientX ?? window.innerWidth / 2;
    const clientY = event?.clientY ?? event?.touches?.[0]?.clientY ?? window.innerHeight / 3;
    const menuWidth = youtubeAppFullscreen ? Math.min(220, window.innerWidth * 0.58) : 210;
    const menuHeight = youtubeAppFullscreen ? 128 : (options?.compact ? 116 : 144);
    const margin = 10;
    const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
    const safeX = clamp(clientX + 8, margin, Math.max(margin, window.innerWidth - menuWidth - margin));
    const rawY = youtubeAppFullscreen
      ? clientY - menuHeight - 12
      : clientY + 8;
    const safeY = clamp(rawY, margin, Math.max(margin, window.innerHeight - menuHeight - margin));
    setSelectionText(text);
    setSelectionPos({ x: safeX, y: safeY });
    setSelectionType(nextSelectionType);
    setSelectedMeta(options?.selectedMeta || null);
    setSelectionCompact(Boolean(options?.compact));
    setSelectionLookupLang(normalizeLangCode(options?.lookupLang || ''));
    setSelectionInlineMode(Boolean(options?.inlineLookup));
    if (options?.inlineLookup) {
      void loadSelectionInlineLookup(text);
    }
  };

  const clearSelection = () => {
    const shouldResumeYoutube = youtubePausedBySelectionRef.current
      && String(selectionType || '').startsWith('youtube_');
    youtubePausedBySelectionRef.current = false;
    if (shouldResumeYoutube) {
      try {
        if (youtubeSectionVisible && youtubePlayerRef.current?.playVideo) {
          youtubePlayerRef.current.playVideo();
        }
      } catch (_resumeError) {
        // ignore resume errors
      }
    }
    setSelectionText('');
    setSelectionPos(null);
    setSelectionType('');
    setSelectedMeta(null);
    setSelectionCompact(false);
    setSelectionLookupLang('');
    setSelectionInlineMode(false);
    setSelectionLookupLoading(false);
    setSelectionInlineLookup({ loading: false, word: '', translation: '', direction: '', provider: '' });
  };

  const normalizeForLookup = async (rawText) => {
    const cleaned = normalizeSelectionText(rawText);
    if (!cleaned) return '';
    if (hasCyrillic(cleaned)) return cleaned;
    try {
      const effectiveLang = normalizeLangCode(selectionLookupLang || getNormalizeLookupLang()) || getNormalizeLookupLang();
      const normalizeLang = encodeURIComponent(effectiveLang);
      const normalizeResponse = await fetch(`/api/webapp/normalize/${normalizeLang}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, text: cleaned }),
      });
      if (normalizeResponse.ok) {
        const data = await normalizeResponse.json();
        return data.normalized || cleaned;
      }
    } catch (error) {
      // ignore normalization errors
    }
    return cleaned;
  };

  const resolveQuickTranslateParams = (rawText) => {
    const cleaned = normalizeSelectionText(rawText);
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const nativeLang = normalizeLangCode(pair.source_lang || languageProfile?.native_language || 'ru') || 'ru';
    const learningLang = normalizeLangCode(pair.target_lang || languageProfile?.learning_language || 'de') || 'de';
    let detectedByScript = '';
    if (hasCyrillic(cleaned) && !hasLatin(cleaned)) {
      detectedByScript = nativeLang;
    } else if (hasLatin(cleaned) && !hasCyrillic(cleaned)) {
      if (isCyrillicLang(nativeLang) && !isCyrillicLang(learningLang)) {
        detectedByScript = learningLang;
      } else if (!isCyrillicLang(nativeLang) && isCyrillicLang(learningLang)) {
        detectedByScript = nativeLang;
      } else {
        detectedByScript = learningLang;
      }
    }
    const sourceLangHint = normalizeLangCode(
      detectedByScript || selectionLookupLang || (hasCyrillic(cleaned) ? nativeLang : learningLang)
    ) || null;
    const targetLang = sourceLangHint === learningLang ? nativeLang : learningLang;
    return { cleaned, sourceLangHint, targetLang };
  };

  const requestQuickTranslation = async (rawText) => {
    const { cleaned, sourceLangHint, targetLang } = resolveQuickTranslateParams(rawText);
    const response = await fetch('/api/translate/quick', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        initData: initData || undefined,
        text: cleaned,
        source_lang: sourceLangHint || null,
        target_lang: targetLang,
      }),
    });
    if (!response.ok) {
      throw new Error(await readApiError(response, 'Ошибка быстрого перевода', 'Fehler bei Schnelluebersetzung'));
    }
    const data = await response.json();
    const translation = String(data?.translation || '').trim();
    const provider = String(data?.provider || '').trim();
    const detectedSource = normalizeLangCode(data?.detected_source_lang || sourceLangHint || '') || sourceLangHint || '';
    return {
      cleaned,
      translation,
      provider,
      sourceLangHint,
      targetLang,
      detectedSource,
    };
  };

  const loadSelectionInlineLookup = async (rawText) => {
    const cleaned = normalizeSelectionText(rawText);
    if (!cleaned) return;
    setSelectionInlineLookup({ loading: true, word: cleaned, translation: '', direction: '', provider: '' });
    try {
      const quick = await requestQuickTranslation(cleaned);
      const direction = `${String(quick.detectedSource || quick.sourceLangHint || 'auto').toLowerCase()}-${String(quick.targetLang).toLowerCase()}`;
      setSelectionInlineLookup({
        loading: false,
        word: cleaned,
        translation: quick.translation,
        direction,
        provider: quick.provider,
      });
    } catch (error) {
      setSelectionInlineLookup({
        loading: false,
        word: cleaned,
        translation: String(error?.message || tr('Ошибка перевода', 'Uebersetzungsfehler')),
        direction: '',
        provider: '',
      });
    }
  };

  const handleQuickAddToDictionary = async (text, options = {}) => {
    const inlineMode = Boolean(options?.inlineMode);
    const inlineOrigin = String(options?.inlineOrigin || '').trim().toLowerCase();
    const isYoutubeInline = inlineMode && (
      inlineOrigin === 'youtube'
      || youtubeAppFullscreen
    );
    const isTranslationsInline = inlineMode && inlineOrigin === 'translations';
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    const normalized = await normalizeForLookup(cleaned);
    const lookupParams = resolveQuickTranslateParams(normalized);
    const lookupLangHint = normalizeLangCode(lookupParams.sourceLangHint || '');
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    if (!inlineMode) {
      setDictionaryWord(normalized);
      setLastLookupScrollY(window.scrollY);
    }
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized, lookup_lang: lookupLangHint || undefined }),
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
      const detectedDirection = data.direction || resolveDictionaryDirection(data.item);
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      const pair = resolveLanguagePairForUI(data.language_pair);
      const directionPair = String(detectedDirection || '').includes('-')
        ? String(detectedDirection).toLowerCase().split('-', 2)
        : [];
      const saveSourceLang = normalizeLangCode(directionPair[0] || pair.source_lang);
      const saveTargetLang = normalizeLangCode(directionPair[1] || pair.target_lang);
      const isLegacyPair = pair.source_lang === 'ru' && pair.target_lang === 'de' && isLegacyRuDeDirection(detectedDirection);
      const canonicalWordDe = normalizeSelectionText(data.item?.word_de || '');
      const canonicalWordRu = normalizeSelectionText(data.item?.word_ru || '');
      const saveWordDe = isLegacyPair && detectedDirection === 'de-ru'
        ? (canonicalWordDe || normalized)
        : '';
      const saveWordRu = isLegacyPair && detectedDirection === 'ru-de'
        ? (canonicalWordRu || normalized)
        : '';
      const autoFolder = isYoutubeInline
        ? await ensureYoutubeAutoFolderId()
        : null;
      const autoFolderId = autoFolder?.id || null;
      if (!inlineMode) {
        setDictionaryResult(data.item || null);
        setDictionaryDirection(detectedDirection);
        scrollToDictionary();
      }
      const saveOriginProcess = inlineMode
        ? (isYoutubeInline ? 'youtube' : (isTranslationsInline ? 'translations' : 'reader'))
        : 'webapp_dictionary_save';
      const saveOriginMeta = {
        endpoint: '/api/webapp/dictionary/save',
        flow: inlineMode ? 'quick_add_inline' : 'quick_add',
        from: inlineMode
          ? (
            isYoutubeInline
              ? 'youtube_selection'
              : (isTranslationsInline ? 'translations_result_selection' : 'reader_selection')
          )
          : 'dictionary_lookup',
      };

      const saveResponse = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: saveWordRu,
          word_de: saveWordDe,
          translation_de: data.item?.translation_de || '',
          translation_ru: data.item?.translation_ru || '',
          source_text: getDictionarySourceTarget(data.item, detectedDirection).sourceText || normalized,
          target_text: getDictionarySourceTarget(data.item, detectedDirection).targetText || '',
          source_lang: saveSourceLang || undefined,
          target_lang: saveTargetLang || undefined,
          direction: detectedDirection || undefined,
          response_json: data.item || {},
          folder_id: autoFolderId ?? (dictionaryFolderId !== 'none' ? dictionaryFolderId : null),
          origin_process: saveOriginProcess,
          origin_meta: saveOriginMeta,
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
      const savePayload = await saveResponse.json();
      setDictionaryLanguagePair(resolveLanguagePairForUI(savePayload.language_pair));
      if (inlineMode) {
      setSelectionInlineLookup((prev) => ({
        ...prev,
        translation: prev.translation ? `${prev.translation} • ${tr('Сохранено ✅', 'Gespeichert ✅')}` : tr('Сохранено ✅', 'Gespeichert ✅'),
      }));
        if (isYoutubeInline) {
          showInlineToast(`${tr('Сохранено в папку', 'In Ordner gespeichert')}: ${autoFolder?.name || 'YouTube'}`);
        }
      } else {
        setDictionarySaved(tr('Добавлено в словарь ✅', 'Zum Woerterbuch hinzugefuegt ✅'));
      }
      clearSelection();
    } catch (error) {
      setDictionaryError(`${tr('Ошибка сохранения', 'Speicherfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleSelectionOpenDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    const normalized = await normalizeForLookup(cleaned);
    const lookupParams = resolveQuickTranslateParams(normalized);
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setCollocationsVisible(false);
    setCollocationsError('');
    setCollocationOptions([]);
    setSelectedCollocations([]);
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized, lookup_lang: lookupParams.sourceLangHint || undefined }),
      });
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
      const data = await response.json();
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      setSelectedSections((prev) => {
        const next = new Set(prev);
        next.add('dictionary');
        return next;
      });
      ensureSectionVisible('dictionary');
      setTimeout(() => {
        scrollToDictionary();
      }, 90);
      clearSelection();
    } catch (error) {
      setDictionaryError(`${tr('Ошибка словаря', 'Woerterbuchfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleQuickLookupDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    setSelectionInlineMode(true);
    setSelectionLookupLoading(true);
    try {
      await loadSelectionInlineLookup(cleaned);
    } catch (error) {
      setSelectionInlineLookup({
        loading: false,
        word: cleaned,
        translation: String(error?.message || tr('Ошибка перевода', 'Uebersetzungsfehler')),
        direction: '',
        provider: '',
      });
    } finally {
      setSelectionLookupLoading(false);
    }
  };

  const handleSelectionSave = async (text) => {
    const inReaderSection = Boolean(isSectionVisible('reader') && readerHasContent && !readerArchiveOpen);
    const inYoutubeSelectionSection = Boolean(
      isSectionVisible('youtube')
      && (
        String(selectionType || '').startsWith('youtube_')
        || selectionInlineMode
      )
    );
    const inTranslationResultSelection = String(selectionType || '').startsWith('translation_result_');
    if (inReaderSection || inYoutubeSelectionSection || inTranslationResultSelection) {
      const snapshot = normalizeSelectionText(text);
      clearSelection();
      if (!snapshot) return;
      void handleQuickAddToDictionary(snapshot, {
        inlineMode: true,
        inlineOrigin: inYoutubeSelectionSection
          ? 'youtube'
          : (inTranslationResultSelection ? 'translations' : 'reader'),
      });
      return;
    }
    await handleSelectionOpenDictionary(text);
  };

  const parseSelectionGptPayload = (explanationRaw, quickTranslationRaw) => {
    const quickTranslation = String(quickTranslationRaw || '').trim();
    const explanation = String(explanationRaw || '').trim();
    const lines = explanation
      .split(/\r?\n/)
      .map((line) => String(line || '').trim())
      .filter(Boolean);
    const explanationLinesRaw = explanation.split(/\r?\n/);
    const exampleSectionTitles = new Set([
      'примеры:',
      'beispiele:',
      'examples:',
      'ejemplos:',
      'esempi:',
    ]);
    const examples = [];
    const exampleSectionStart = explanationLinesRaw.findIndex((line) => (
      exampleSectionTitles.has(String(line || '').trim().toLowerCase())
    ));
    if (exampleSectionStart >= 0) {
      for (let index = exampleSectionStart + 1; index < explanationLinesRaw.length; index += 1) {
        const rawLine = String(explanationLinesRaw[index] || '');
        const trimmed = rawLine.trim();
        if (!trimmed) {
          if (examples.length > 0) break;
          continue;
        }
        const isExampleItem = /^[-•*]\s+/.test(trimmed) || /^\d+\.\s+/.test(trimmed);
        if (!isExampleItem) {
          if (examples.length > 0) break;
          continue;
        }
        const normalized = trimmed
          .replace(/^[-•*]\s+/, '')
          .replace(/^\d+\.\s+/, '')
          .trim();
        if (!normalized) continue;
        examples.push(normalized);
        if (examples.length >= 4) break;
      }
    }
    return {
      translation: quickTranslation || lines[0] || '',
      notes: explanation,
      examples,
    };
  };

  const resetSelectionGptSaveState = () => {
    setSelectionGptSaveOriginalChecked(true);
    setSelectionGptSaveExamplesChecked({});
    setSelectionGptSaveLoading(false);
    setSelectionGptSaveError('');
    setSelectionGptSaveMessage('');
  };

  const saveSelectionGptDictionaryEntry = async ({
    sourceText,
    targetText,
    sourceLang,
    targetLang,
    direction,
    responseJson,
    originMeta,
  }) => {
    const source = String(sourceText || '').trim();
    const target = String(targetText || '').trim();
    if (!source || !target) {
      throw new Error(tr('Пустые данные для сохранения', 'Leere Daten zum Speichern'));
    }
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const resolvedSourceLang = normalizeLangCode(sourceLang || pair.source_lang) || pair.source_lang;
    const resolvedTargetLang = normalizeLangCode(targetLang || pair.target_lang) || pair.target_lang;
    const resolvedDirection = String(direction || `${resolvedSourceLang}-${resolvedTargetLang}`).trim().toLowerCase();
    const isLegacyPair = pair.source_lang === 'ru' && pair.target_lang === 'de' && isLegacyRuDeDirection(resolvedDirection);
    const response = await fetch('/api/webapp/dictionary/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        initData,
        word_ru: isLegacyPair && resolvedDirection === 'ru-de' ? source : '',
        word_de: isLegacyPair && resolvedDirection === 'de-ru' ? source : '',
        translation_de: isLegacyPair && resolvedDirection === 'ru-de' ? target : '',
        translation_ru: isLegacyPair && resolvedDirection === 'de-ru' ? target : '',
        source_text: source,
        target_text: target,
        response_json: {
          ...(responseJson && typeof responseJson === 'object' ? responseJson : {}),
          source_text: source,
          target_text: target,
          source_lang: resolvedSourceLang,
          target_lang: resolvedTargetLang,
          direction: resolvedDirection,
          language_pair: {
            source_lang: resolvedSourceLang,
            target_lang: resolvedTargetLang,
          },
        },
        source_lang: resolvedSourceLang || undefined,
        target_lang: resolvedTargetLang || undefined,
        direction: resolvedDirection || undefined,
        folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        origin_process: 'reader_selection_gpt_save',
        origin_meta: {
          endpoint: '/api/webapp/dictionary/save',
          flow: 'reader_gpt_sheet',
          from: 'reader_gpt_sheet',
          ...(originMeta && typeof originMeta === 'object' ? originMeta : {}),
        },
      }),
    });
    if (!response.ok) {
      throw new Error(await readApiError(response, 'Ошибка сохранения', 'Speicherfehler'));
    }
    const payload = await response.json();
    setDictionaryLanguagePair(resolveLanguagePairForUI(payload.language_pair));
  };

  const saveSelectionGptOriginalWord = async (rawText) => {
    const cleaned = normalizeSelectionText(rawText);
    if (!cleaned) return false;
    const normalized = await normalizeForLookup(cleaned);
    const lookupParams = resolveQuickTranslateParams(normalized);
    const lookupLangHint = normalizeLangCode(lookupParams.sourceLangHint || '');
    const lookupResponse = await fetch('/api/webapp/dictionary', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        initData,
        word: normalized,
        lookup_lang: lookupLangHint || undefined,
      }),
    });
    if (!lookupResponse.ok) {
      throw new Error(await readApiError(lookupResponse, 'Ошибка словаря', 'Woerterbuchfehler'));
    }
    const data = await lookupResponse.json();
    const item = data?.item || {};
    const fallbackPair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const direction = String(data?.direction || resolveDictionaryDirection(item) || `${fallbackPair.source_lang}-${fallbackPair.target_lang}`)
      .trim()
      .toLowerCase();
    const [directionSourceLang = fallbackPair.source_lang, directionTargetLang = fallbackPair.target_lang] = direction.includes('-')
      ? direction.split('-', 2)
      : [fallbackPair.source_lang, fallbackPair.target_lang];
    const sourceTarget = getDictionarySourceTarget(item, direction);
    let sourceText = String(sourceTarget.sourceText || normalized).trim();
    let targetText = String(sourceTarget.targetText || '').trim();
    if (!targetText) {
      const quick = await requestQuickTranslation(normalized);
      targetText = String(quick.translation || '').trim();
    }
    if (!targetText) {
      throw new Error(tr('Не удалось получить перевод для сохранения', 'Uebersetzung fuer das Speichern konnte nicht ermittelt werden'));
    }
    const normalizedPair = applyArticleForDirection(sourceText, targetText, direction, item);
    sourceText = String(normalizedPair.sourceText || sourceText).trim();
    targetText = String(normalizedPair.targetText || targetText).trim();
    const article = String(item?.article || '').trim();
    const partOfSpeech = String(item?.part_of_speech || '').trim();
    const responseJson = {
      ...(item && typeof item === 'object' ? item : {}),
      source_text: sourceText,
      target_text: targetText,
      source_lang: directionSourceLang,
      target_lang: directionTargetLang,
      direction,
      language_pair: {
        source_lang: directionSourceLang,
        target_lang: directionTargetLang,
      },
    };
    if (article) {
      const deFromSource = directionSourceLang === 'de' ? sourceText : '';
      const deFromTarget = directionTargetLang === 'de' ? targetText : '';
      const responseWordDe = String(responseJson.word_de || deFromSource || deFromTarget || '').trim();
      const responseTranslationDe = String(responseJson.translation_de || deFromTarget || deFromSource || '').trim();
      if (responseWordDe) {
        responseJson.word_de = prependArticleIfNeeded(responseWordDe, article, partOfSpeech);
      }
      if (responseTranslationDe) {
        responseJson.translation_de = prependArticleIfNeeded(responseTranslationDe, article, partOfSpeech);
      }
    }
    await saveSelectionGptDictionaryEntry({
      sourceText,
      targetText,
      sourceLang: directionSourceLang,
      targetLang: directionTargetLang,
      direction,
      responseJson,
      originMeta: {
        source_kind: 'original_word',
      },
    });
    return true;
  };

  const saveSelectionGptExample = async (exampleText, exampleIndex) => {
    const cleaned = normalizeSelectionText(exampleText)
      .replace(/^["“”„«»]+/, '')
      .replace(/["“”„«»]+$/, '')
      .trim();
    if (!cleaned) return false;
    const quick = await requestQuickTranslation(cleaned);
    const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
    const sourceLang = normalizeLangCode(
      quick.detectedSource
      || quick.sourceLangHint
      || pair.source_lang
    ) || pair.source_lang;
    const targetLang = normalizeLangCode(
      quick.targetLang
      || (sourceLang === pair.source_lang ? pair.target_lang : pair.source_lang)
    ) || pair.target_lang;
    const sourceText = String(quick.cleaned || cleaned).trim();
    const targetText = String(quick.translation || '').trim();
    if (!targetText) {
      throw new Error(tr('Перевод примера не получен', 'Beispieluebersetzung fehlt'));
    }
    await saveSelectionGptDictionaryEntry({
      sourceText,
      targetText,
      sourceLang,
      targetLang,
      direction: `${sourceLang}-${targetLang}`,
      responseJson: {
        source_text: sourceText,
        target_text: targetText,
      },
      originMeta: {
        source_kind: 'gpt_example',
        source_word: normalizeSelectionText(selectionText),
        example_index: exampleIndex + 1,
      },
    });
    return true;
  };

  const handleSelectionGptSaveToDictionary = async () => {
    if (!initData) {
      setSelectionGptSaveError(initDataMissingMsg);
      return;
    }
    const selectedExamples = (Array.isArray(selectionGptData.examples) ? selectionGptData.examples : [])
      .map((item, index) => ({ text: String(item || '').trim(), index }))
      .filter((item) => Boolean(selectionGptSaveExamplesChecked[item.index]) && Boolean(item.text));
    const shouldSaveOriginal = Boolean(selectionGptSaveOriginalChecked && normalizeSelectionText(selectionText));
    if (!shouldSaveOriginal && selectedExamples.length === 0) {
      setSelectionGptSaveError(tr('Выберите слово или минимум один пример.', 'Waehle ein Wort oder mindestens ein Beispiel.'));
      setSelectionGptSaveMessage('');
      return;
    }
    setSelectionGptSaveLoading(true);
    setSelectionGptSaveError('');
    setSelectionGptSaveMessage('');
    let savedCount = 0;
    const failedItems = [];
    if (shouldSaveOriginal) {
      try {
        const saved = await saveSelectionGptOriginalWord(selectionText);
        if (saved) savedCount += 1;
      } catch (error) {
        failedItems.push(tr(
          `Оригинальное слово: ${String(error?.message || '').trim()}`,
          `Originalwort: ${String(error?.message || '').trim()}`
        ));
      }
    }
    for (const item of selectedExamples) {
      try {
        const saved = await saveSelectionGptExample(item.text, item.index);
        if (saved) savedCount += 1;
      } catch (error) {
        failedItems.push(tr(
          `Пример ${item.index + 1}: ${String(error?.message || '').trim()}`,
          `Beispiel ${item.index + 1}: ${String(error?.message || '').trim()}`
        ));
      }
    }
    if (savedCount > 0) {
      const successMessage = tr(
        `Добавлено в словарь: ${savedCount}`,
        `Zum Woerterbuch hinzugefuegt: ${savedCount}`
      );
      setSelectionGptSaveMessage(successMessage);
      showInlineToast(successMessage);
    }
    if (failedItems.length > 0) {
      setSelectionGptSaveError(failedItems.join('\n'));
    } else {
      setSelectionGptSaveError('');
    }
    setSelectionGptSaveLoading(false);
  };

  const handleSelectionGptLookup = async () => {
    const cleaned = normalizeSelectionText(selectionText);
    if (!cleaned) return;
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    setSelectionGptOpen(true);
    setSelectionGptLoading(true);
    setSelectionGptError('');
    resetSelectionGptSaveState();
    setSelectionGptData({ translation: '', notes: '', examples: [] });
    try {
      const quick = await requestQuickTranslation(cleaned);
      const explainResponse = await fetch('/api/webapp/explain', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          mode: 'selection_context',
          original_text: cleaned,
          user_translation: quick.translation || cleaned,
        }),
      });
      if (!explainResponse.ok) {
        throw new Error(await readApiError(explainResponse, 'Ошибка GPT-объяснения', 'Fehler bei GPT-Erklaerung'));
      }
      const explainData = await explainResponse.json();
      setSelectionGptData(parseSelectionGptPayload(explainData?.explanation, quick.translation));
      setSelectionInlineLookup({
        loading: false,
        word: cleaned,
        translation: quick.translation,
        direction: `${String(quick.detectedSource || quick.sourceLangHint || 'auto').toLowerCase()}-${String(quick.targetLang).toLowerCase()}`,
        provider: quick.provider || '',
      });
    } catch (error) {
      setSelectionGptError(String(error?.message || tr('Ошибка GPT-объяснения', 'GPT-Erklaerungsfehler')));
    } finally {
      setSelectionGptLoading(false);
    }
  };

  const closeSelectionGptSheet = () => {
    setSelectionGptOpen(false);
    setSelectionGptLoading(false);
    setSelectionGptError('');
    resetSelectionGptSaveState();
  };

  function formatReaderTimer(seconds) {
    const value = Math.max(0, Number(seconds || 0));
    const hours = Math.floor(value / 3600);
    const minutes = Math.floor((value % 3600) / 60);
    const secs = Math.floor(value % 60);
    const pad = (num) => String(num).padStart(2, '0');
    if (hours > 0) return `${pad(hours)}:${pad(minutes)}:${pad(secs)}`;
    return `${pad(minutes)}:${pad(secs)}`;
  }

  function computeReaderProgressPercent() {
    if (readerPageCount > 0) {
      const page = Math.max(1, Math.min(readerPageCount, Number(readerCurrentPage || 1)));
      return Math.max(0, Math.min(100, (page / readerPageCount) * 100));
    }
    const node = readerArticleRef.current;
    if (!node) return 0;
    if (readerReadingMode === 'horizontal') {
      const max = Math.max(1, node.scrollWidth - node.clientWidth);
      return Math.max(0, Math.min(100, (node.scrollLeft / max) * 100));
    }
    const max = Math.max(1, node.scrollHeight - node.clientHeight);
    return Math.max(0, Math.min(100, (node.scrollTop / max) * 100));
  }

  function applyReaderProgressPercent(percent) {
    if (readerPageCount > 0) {
      const safe = Math.max(0, Math.min(100, Number(percent || 0)));
      const resolved = Math.max(1, Math.min(readerPageCount, Math.round((safe / 100) * readerPageCount) || 1));
      setReaderCurrentPage(resolved);
      return;
    }
    const node = readerArticleRef.current;
    if (!node) return;
    const safe = Math.max(0, Math.min(100, Number(percent || 0)));
    if (readerReadingMode === 'horizontal') {
      const max = Math.max(1, node.scrollWidth - node.clientWidth);
      node.scrollLeft = (safe / 100) * max;
      return;
    }
    const max = Math.max(1, node.scrollHeight - node.clientHeight);
    node.scrollTop = (safe / 100) * max;
  }

  const handleReaderStructuredClick = (event) => {
    markReaderInteraction();
    const root = readerArticleRef.current;
    if (!root) return;
    const target = event?.target;
    if (!(target instanceof Element)) return;

    const wordEl = target.closest('[data-wid]');
    if (wordEl && root.contains(wordEl)) {
      const wid = String(wordEl.getAttribute('data-wid') || '').trim();
      const sid = String(wordEl.getAttribute('data-sid') || '').trim();
      const metaWord = readerWordMap.get(wid);
      if (!metaWord || !sid) return;
      handleSelection(event, metaWord.value, {
        compact: true,
        inlineLookup: true,
        lookupLang: getNormalizeLookupLang(),
        selectionType: 'word',
        selectedMeta: {
          sids: [sid],
          wids: [wid],
          start: Number(metaWord.start || 0),
          end: Number(metaWord.end || 0),
        },
      });
      return;
    }

    const sentenceEl = target.closest('[data-sid]');
    if (sentenceEl && root.contains(sentenceEl)) {
      const sid = String(sentenceEl.getAttribute('data-sid') || '').trim();
      const sentence = readerSentenceMap.get(sid);
      if (!sentence) return;
      handleSelection(event, String(sentence.text || ''), {
        compact: true,
        inlineLookup: true,
        lookupLang: getNormalizeLookupLang(),
        selectionType: 'sentence',
        selectedMeta: {
          sids: [sid],
          start: Number(sentence.start || 0),
          end: Number(sentence.end || 0),
        },
      });
    }
  };

  const handleReaderStructuredSelectionEnd = (event) => {
    const root = readerArticleRef.current;
    if (!root) return;
    const selection = window.getSelection?.();
    if (!selection || selection.rangeCount === 0 || selection.isCollapsed) return;
    const range = selection.getRangeAt(0);
    const commonNode = range.commonAncestorContainer;
    const anchorNode = selection.anchorNode;
    const focusNode = selection.focusNode;
    if (commonNode && !root.contains(commonNode)) return;
    if (anchorNode && !root.contains(anchorNode)) return;
    if (focusNode && !root.contains(focusNode)) return;

    const words = Array.from(root.querySelectorAll('[data-wid][data-sid]'));
    const pickedWords = [];
    for (const node of words) {
      try {
        if (range.intersectsNode(node)) {
          pickedWords.push(node);
        }
      } catch (_rangeError) {
        // ignore invalid nodes
      }
    }
    if (pickedWords.length === 0) return;

    const sentenceIds = [];
    const sentenceIdSet = new Set();
    const wordIds = [];
    pickedWords.forEach((node) => {
      const sid = String(node.getAttribute('data-sid') || '').trim();
      const wid = String(node.getAttribute('data-wid') || '').trim();
      if (sid && !sentenceIdSet.has(sid)) {
        sentenceIdSet.add(sid);
        sentenceIds.push(sid);
      }
      if (wid) wordIds.push(wid);
    });
    if (!sentenceIds.length) return;

    const selectedSentences = readerSentencesModel.filter((sentence) => sentenceIdSet.has(sentence.sid));
    if (!selectedSentences.length) return;
    const selectedText = selectedSentences.map((sentence) => String(sentence.text || '')).join('');
    if (!selectedText) return;

    const selectionKind = selectedSentences.length > 1 ? 'multi_sentence' : 'sentence';
    const start = Number(selectedSentences[0]?.start || 0);
    const end = Number(selectedSentences[selectedSentences.length - 1]?.end || start);
    handleSelection(event, selectedText, {
      compact: true,
      inlineLookup: true,
      lookupLang: getNormalizeLookupLang(),
      selectionType: selectionKind,
      selectedMeta: {
        sids: sentenceIds,
        wids: wordIds,
        start,
        end,
      },
    });
    try {
      selection.removeAllRanges();
    } catch (_clearError) {
      // ignore cleanup errors
    }
  };

  const handleReaderArticleMouseUp = (event) => {
    markReaderInteraction();
    handleReaderStructuredSelectionEnd(event);
  };

  const handleReaderArticleTouchEnd = (event) => {
    markReaderInteraction();
    handleReaderPageTouchEnd(event);
    handleReaderStructuredSelectionEnd(event);
  };

  const renderReaderStructuredText = () => (
    <div className="reader-container">
      {readerSentencesModel.map((sentence) => (
        <span
          key={sentence.sid}
          className={`reader-sentence ${selectedSentenceIds.has(sentence.sid) ? 'is-selected' : ''}`}
          data-sid={sentence.sid}
          data-start={sentence.start}
          data-end={sentence.end}
        >
          {sentence.tokens.map((token, tokenIndex) => {
            if (token.kind === 'word') {
              const wordId = String(token.wid || '');
              return (
                <span
                  key={wordId || `${sentence.sid}-word-${tokenIndex}`}
                  className={`reader-word ${selectedWordIds.has(wordId) ? 'is-selected' : ''}`}
                  data-wid={wordId}
                  data-sid={sentence.sid}
                  data-start={token.start}
                  data-end={token.end}
                >
                  {token.value}
                </span>
              );
            }
            return (
              <span
                key={`${sentence.sid}-${token.kind}-${token.start}-${token.end}-${tokenIndex}`}
                className="reader-token"
                aria-hidden="true"
              >
                {token.value}
              </span>
            );
          })}
        </span>
      ))}
    </div>
  );

  const getReaderCoverInitials = (title) => {
    const value = String(title || '').trim();
    if (!value) return 'BK';
    const parts = value.split(/\s+/).filter(Boolean);
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return `${parts[0][0] || ''}${parts[1][0] || ''}`.toUpperCase();
  };

  const getReaderCoverGradient = (item) => {
    const seedRaw = `${item?.id || ''}:${item?.title || ''}:${item?.target_lang || ''}`;
    let hash = 0;
    for (let i = 0; i < seedRaw.length; i += 1) {
      hash = (hash << 5) - hash + seedRaw.charCodeAt(i);
      hash |= 0;
    }
    const themes = [
      ['#1d4ed8', '#0f172a'],
      ['#0f766e', '#1e293b'],
      ['#7c3aed', '#0f172a'],
      ['#be123c', '#1f2937'],
      ['#065f46', '#111827'],
      ['#334155', '#0f172a'],
    ];
    return themes[Math.abs(hash) % themes.length];
  };

  const getReaderCoverUrl = (item) => {
    const candidates = [
      item?.cover_url,
      item?.thumbnail,
      item?.image_url,
      item?.poster_url,
    ];
    for (const raw of candidates) {
      const value = String(raw || '').trim();
      if (!value) continue;
      if (/^https?:\/\//i.test(value) && /\.(png|jpe?g|webp|avif|gif)(\?.*)?$/i.test(value)) {
        return value;
      }
    }
    return '';
  };

  const formatReaderArchiveDate = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const ts = Date.parse(raw);
    if (!Number.isFinite(ts)) return '';
    return new Date(ts).toLocaleDateString();
  };

  const buildReaderArchiveMeta = (item) => {
    const bits = [];
    const lang = String(item?.target_lang || '').toUpperCase();
    const sourceType = String(item?.source_type || '').toUpperCase();
    const dateLabel = formatReaderArchiveDate(item?.updated_at || item?.last_opened_at || item?.created_at);
    if (lang) bits.push(lang);
    if (sourceType) bits.push(sourceType);
    if (dateLabel) bits.push(dateLabel);
    return bits.join(' • ');
  };

  async function loadReaderLibrary(includeArchivedOverride = readerIncludeArchived) {
    if (!initData) return;
    try {
      setReaderLibraryLoading(true);
      setReaderLibraryError('');
      const response = await fetch('/api/webapp/reader/library', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 120, include_archived: includeArchivedOverride }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки библиотеки', 'Fehler beim Laden der Bibliothek'));
      }
      const data = await response.json();
      setReaderDocuments(Array.isArray(data?.items) ? data.items : []);
    } catch (error) {
      setReaderLibraryError(normalizeNetworkErrorMessage(error, 'Не удалось загрузить библиотеку.', 'Bibliothek konnte nicht geladen werden.'));
    } finally {
      setReaderLibraryLoading(false);
    }
  }

  const openReaderArchive = async () => {
    setReaderArchiveOpen(true);
    setReaderImmersive(false);
    setReaderSettingsOpen(false);
    setReaderIncludeArchived(true);
    await loadReaderLibrary(true);
  };

  const handleReaderFileSelect = (event) => {
    const file = event?.target?.files?.[0] || null;
    setReaderSelectedFile(file);
    if (file?.name && !readerInput.trim()) {
      setReaderInput(file.name);
    }
  };

  const goReaderPage = (delta) => {
    if (readerPageCount === 0) return;
    const step = delta > 0 ? 1 : -1;
    setReaderCurrentPage((prev) => {
      const next = prev + step;
      return Math.max(1, Math.min(readerPageCount, next));
    });
  };

  const handleReaderPageWheel = (event) => {
    if (readerPageCount === 0) return;
    markReaderInteraction();
    event.preventDefault();
    if (readerPageNavLockRef.current) return;
    const deltaRaw = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
    if (!deltaRaw) return;
    readerPageNavLockRef.current = true;
    goReaderPage(deltaRaw > 0 ? 1 : -1);
    setTimeout(() => {
      readerPageNavLockRef.current = false;
    }, readerSwipeLockMs);
  };

  const handleReaderPageTouchStart = (event) => {
    if (readerPageCount === 0) return;
    markReaderInteraction();
    const touch = event?.touches?.[0];
    if (!touch) return;
    readerSwipeStartRef.current = {
      x: touch.clientX,
      y: touch.clientY,
      ts: Date.now(),
    };
  };

  const handleReaderPageTouchEnd = (event) => {
    if (readerPageCount === 0) return;
    const start = readerSwipeStartRef.current;
    readerSwipeStartRef.current = null;
    if (!start || readerPageNavLockRef.current) return;
    const touch = event?.changedTouches?.[0];
    if (!touch) return;
    const dx = touch.clientX - start.x;
    const dy = touch.clientY - start.y;
    const elapsed = Date.now() - start.ts;
    if (elapsed > 900) return;
    const threshold = readerSwipeThreshold;
    if (readerReadingMode === 'horizontal') {
      if (Math.abs(dx) < threshold || Math.abs(dx) < Math.abs(dy)) return;
      readerPageNavLockRef.current = true;
      goReaderPage(dx < 0 ? 1 : -1);
    } else {
      if (Math.abs(dy) < threshold || Math.abs(dy) < Math.abs(dx)) return;
      readerPageNavLockRef.current = true;
      goReaderPage(dy < 0 ? 1 : -1);
    }
    setTimeout(() => {
      readerPageNavLockRef.current = false;
    }, readerSwipeLockMs);
  };

  const readFileAsBase64 = (file) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || '');
      const marker = 'base64,';
      const idx = result.indexOf(marker);
      if (idx < 0) {
        reject(new Error('Не удалось прочитать файл'));
        return;
      }
      resolve(result.slice(idx + marker.length));
    };
    reader.onerror = () => reject(new Error('Ошибка чтения файла'));
    reader.readAsDataURL(file);
  });

  async function syncReaderState(patch = {}) {
    if (!initData || !readerDocumentId) return;
    try {
      await fetch('/api/webapp/reader/library/state', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          document_id: readerDocumentId,
          ...patch,
        }),
      });
    } catch (_error) {
      // ignore transient sync errors
    }
    setReaderDocuments((prev) => prev.map((item) => (
      Number(item?.id) === Number(readerDocumentId)
        ? { ...item, ...patch }
        : item
    )));
  }

  async function openReaderDocument(documentId) {
    if (!initData || !documentId) return;
    try {
      setReaderLoading(true);
      setReaderError('');
      const response = await fetch('/api/webapp/reader/library/open', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, document_id: documentId }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка открытия книги', 'Fehler beim Oeffnen des Dokuments'));
      }
      const data = await response.json();
      const doc = data?.document || {};
      const progress = Number(doc?.progress_percent || 0);
      const bookmark = Number(doc?.bookmark_percent || 0);
      const pages = Array.isArray(doc?.content_pages) ? doc.content_pages : [];
      setReaderDocumentId(Number(doc?.id || documentId));
      setReaderTitle(String(data?.title || doc?.title || ''));
      setReaderContent(String(data?.text || doc?.content_text || '').trim());
      setReaderPages(pages);
      setReaderSourceType(String(data?.source_type || doc?.source_type || 'text'));
      setReaderSourceUrl(String(data?.source_url || doc?.source_url || ''));
      setReaderDetectedLanguage(normalizeLangCode(data?.detected_language || ''));
      setReaderReadingMode(String(doc?.reading_mode || 'vertical'));
      setReaderProgressPercent(progress);
      setReaderBookmarkPercent(bookmark);
      const pageFromProgress = pages.length > 0
        ? Math.max(1, Math.min(pages.length, Math.round(((bookmark > 0 ? bookmark : progress) / 100) * pages.length) || 1))
        : 1;
      setReaderCurrentPage(pageFromProgress);
      setReaderAudioFromPage(pages.length > 0 ? '1' : '');
      setReaderAudioToPage(pages.length > 0 ? String(pages.length) : '');
      setReaderAudioError('');
      setReaderLiveSeconds(0);
      setReaderTimerPaused(false);
      setReaderImmersive(true);
      setReaderTopbarCollapsed(false);
      setReaderArchiveOpen(false);
      setReaderSettingsOpen(false);
      setSelectedSections(new Set(['reader']));
      ensureSectionVisible('reader');
      setTimeout(() => {
        scrollToRef(readerRef, { block: 'start' });
        const target = bookmark > 0 ? bookmark : progress;
        applyReaderProgressPercent(target);
      }, 100);
      loadReaderLibrary();
    } catch (error) {
      setReaderError(normalizeNetworkErrorMessage(error, 'Не удалось открыть книгу.', 'Dokument konnte nicht geoeffnet werden.'));
    } finally {
      setReaderLoading(false);
    }
  }

  const renameReaderDocument = async (documentId, currentTitle) => {
    if (!initData || !documentId) return;
    const nextTitleRaw = window.prompt(
      tr('Введите новое название книги', 'Neuen Titel eingeben'),
      String(currentTitle || '')
    );
    if (nextTitleRaw === null) return;
    const nextTitle = String(nextTitleRaw || '').trim();
    if (!nextTitle) return;
    try {
      const response = await fetch('/api/webapp/reader/library/rename', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, document_id: documentId, title: nextTitle }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка переименования', 'Fehler beim Umbenennen'));
      }
      const data = await response.json();
      const doc = data?.document || {};
      setReaderDocuments((prev) => prev.map((item) => (Number(item?.id) === Number(documentId) ? { ...item, ...doc } : item)));
      if (Number(readerDocumentId) === Number(documentId)) {
        setReaderTitle(String(doc?.title || nextTitle));
      }
    } catch (error) {
      setReaderLibraryError(normalizeNetworkErrorMessage(error, 'Не удалось переименовать книгу.', 'Dokument konnte nicht umbenannt werden.'));
    }
  };

  const archiveReaderDocument = async (documentId, archived = true) => {
    if (!initData || !documentId) return;
    try {
      const response = await fetch('/api/webapp/reader/library/archive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, document_id: documentId, archived }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка архивации', 'Fehler bei der Archivierung'));
      }
      if (!readerIncludeArchived || archived) {
        setReaderDocuments((prev) => prev.filter((item) => Number(item?.id) !== Number(documentId)));
      } else {
        await loadReaderLibrary();
      }
      if (Number(readerDocumentId) === Number(documentId) && archived) {
        setReaderDocumentId(null);
        setReaderContent('');
        setReaderPages([]);
      }
    } catch (error) {
      setReaderLibraryError(normalizeNetworkErrorMessage(error, 'Не удалось архивировать книгу.', 'Dokument konnte nicht archiviert werden.'));
    }
  };

  const deleteReaderDocument = async (documentId) => {
    if (!initData || !documentId) return;
    const confirmed = window.confirm(tr('Удалить книгу из библиотеки?', 'Dokument aus Bibliothek loeschen?'));
    if (!confirmed) return;
    try {
      const response = await fetch('/api/webapp/reader/library/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, document_id: documentId }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка удаления', 'Fehler beim Loeschen'));
      }
      setReaderDocuments((prev) => prev.filter((item) => Number(item?.id) !== Number(documentId)));
      if (Number(readerDocumentId) === Number(documentId)) {
        setReaderDocumentId(null);
        setReaderContent('');
        setReaderPages([]);
      }
    } catch (error) {
      setReaderLibraryError(normalizeNetworkErrorMessage(error, 'Не удалось удалить книгу.', 'Dokument konnte nicht geloescht werden.'));
    }
  };

  const downloadReaderAudio = async (fullDocument = false) => {
    if (!initData || !readerDocumentId) return;
    try {
      setReaderAudioLoading(true);
      setReaderAudioError('');
      const hasPages = Array.isArray(readerPages) && readerPages.length > 0;
      const fromPage = hasPages && !fullDocument
        ? Math.max(1, Number.parseInt(String(readerAudioFromPage || '1'), 10) || 1)
        : null;
      const toPage = hasPages && !fullDocument
        ? Math.max(fromPage || 1, Number.parseInt(String(readerAudioToPage || String(readerPages.length)), 10) || readerPages.length)
        : null;
      const response = await fetch('/api/webapp/reader/audio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          document_id: readerDocumentId,
          page_from: fromPage,
          page_to: toPage,
          language: readerDetectedLanguage || undefined,
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка аудио-конвертации', 'Fehler bei Audio-Konvertierung'));
      }
      const blob = await response.blob();
      const contentDisposition = String(response.headers.get('content-disposition') || '');
      const fileNameMatch = contentDisposition.match(/filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?/i);
      const decodedNameRaw = fileNameMatch?.[1] || fileNameMatch?.[2] || '';
      const decodedName = decodedNameRaw ? decodeURIComponent(decodedNameRaw) : '';
      const contentType = String(response.headers.get('content-type') || '').toLowerCase();
      const ext = contentType.includes('mpeg') ? 'mp3' : 'wav';
      const fileName = decodedName || `${String(readerTitle || 'reader').replace(/[^\w.-]+/g, '_')}_${fullDocument ? 'full' : 'range'}.${ext}`;
      const url = URL.createObjectURL(blob);
      const ua = typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '';
      const isIOS = /iPad|iPhone|iPod/i.test(ua)
        || (typeof navigator !== 'undefined'
          && navigator.platform === 'MacIntel'
          && Number(navigator.maxTouchPoints || 0) > 1);
      if (isIOS) {
        setReaderAudioPreviewUrl((prev) => {
          if (prev && prev !== url) {
            try {
              URL.revokeObjectURL(prev);
            } catch (error) {
              // ignore
            }
          }
          return url;
        });
        setReaderAudioPreviewName(fileName);
        return;
      }
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (error) {
      setReaderAudioError(normalizeNetworkErrorMessage(error, 'Не удалось создать аудио.', 'Audio konnte nicht erstellt werden.'));
    } finally {
      setReaderAudioLoading(false);
    }
  };

  const closeReaderAudioPreview = () => {
    setReaderAudioPreviewUrl((prev) => {
      if (prev) {
        try {
          URL.revokeObjectURL(prev);
        } catch (error) {
          // ignore
        }
      }
      return '';
    });
    setReaderAudioPreviewName('');
  };

  useEffect(() => () => {
    if (!readerAudioPreviewUrl) return;
    try {
      URL.revokeObjectURL(readerAudioPreviewUrl);
    } catch (error) {
      // ignore
    }
  }, [readerAudioPreviewUrl]);

  async function handleReaderIngest(event) {
    event?.preventDefault?.();
    const rawInput = String(readerInput || '').trim();
    if (!rawInput && !readerSelectedFile) {
      setReaderError(tr('Вставьте ссылку или текст.', 'Fuege einen Link oder Text ein.'));
      return;
    }
    if (!initData) {
      setReaderError(initDataMissingMsg);
      return;
    }
    setReaderLoading(true);
    setReaderError('');
    setReaderErrorCode('');
    try {
      const looksLikeUrl = /^https?:\/\//i.test(rawInput) || /^[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/i.test(rawInput);
      let filePayload = {};
      if (readerSelectedFile) {
        const fileBase64 = await readFileAsBase64(readerSelectedFile);
        filePayload = {
          file_name: readerSelectedFile.name,
          file_mime: readerSelectedFile.type,
          file_content_base64: fileBase64,
        };
      }
      const response = await fetch('/api/webapp/reader/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          url: readerSelectedFile ? '' : (looksLikeUrl ? rawInput : ''),
          text: readerSelectedFile ? '' : (looksLikeUrl ? '' : rawInput),
          ...filePayload,
        }),
      });
      if (!response.ok) {
        const fallback = tr('Ошибка загрузки читалки', 'Fehler beim Laden des Leser-Modus');
        let message = fallback;
        let errorCode = '';
        try {
          const raw = await response.text();
          if (raw) {
            try {
              const parsed = JSON.parse(raw);
              message = String(parsed?.error || parsed?.message || '').trim() || fallback;
              errorCode = String(parsed?.error_code || '').trim();
            } catch (_jsonError) {
              message = String(raw).replace(/\s+/g, ' ').trim() || `${fallback} (HTTP ${response.status})`;
            }
          } else {
            message = `${fallback} (HTTP ${response.status})`;
          }
        } catch (_readError) {
          message = `${fallback} (HTTP ${response.status})`;
        }
        const apiError = new Error(message);
        apiError.code = errorCode;
        throw apiError;
      }
      const data = await response.json();
      const doc = data?.document || {};
      const docId = Number(doc?.id || 0) || null;
      const pages = Array.isArray(doc?.content_pages) ? doc.content_pages : [];
      setReaderContent(String(data?.text || '').trim());
      setReaderTitle(String(data?.title || doc?.title || rawInput.slice(0, 80)));
      setReaderPages(pages);
      setReaderSourceType(String(data?.source_type || 'text'));
      setReaderSourceUrl(String(data?.source_url || rawInput));
      setReaderDetectedLanguage(normalizeLangCode(data?.detected_language || ''));
      setReaderDocumentId(docId);
      setReaderReadingMode(String(doc?.reading_mode || 'vertical'));
      setReaderProgressPercent(Number(doc?.progress_percent || 0));
      setReaderBookmarkPercent(Number(doc?.bookmark_percent || 0));
      const pageFromProgress = pages.length > 0
        ? Math.max(1, Math.min(pages.length, Math.round((Number(doc?.bookmark_percent || doc?.progress_percent || 0) / 100) * pages.length) || 1))
        : 1;
      setReaderCurrentPage(pageFromProgress);
      setReaderAudioFromPage(pages.length > 0 ? '1' : '');
      setReaderAudioToPage(pages.length > 0 ? String(pages.length) : '');
      setReaderAudioError('');
      setReaderLiveSeconds(0);
      setReaderTimerPaused(false);
      setReaderImmersive(true);
      setReaderTopbarCollapsed(false);
      setReaderArchiveOpen(false);
      setReaderSettingsOpen(false);
      setSelectedSections(new Set(['reader']));
      ensureSectionVisible('reader');
      setTimeout(() => {
        scrollToRef(readerRef, { block: 'start' });
        const target = Number(doc?.bookmark_percent || doc?.progress_percent || 0);
        applyReaderProgressPercent(target);
      }, 80);
      loadReaderLibrary();
      setReaderSelectedFile(null);
      setReaderErrorCode('');
    } catch (error) {
      const code = String(error?.code || '').trim();
      setReaderErrorCode(code);
      if (code === 'LIMIT_FREE_PLAN_1_BOOK') {
        setReaderError(tr(
          'На бесплатном плане можно хранить только 1 книгу/документ. Удалите текущий документ или перейдите на Pro.',
          'Im Free-Plan kannst du nur 1 Buch/Dokument speichern. Loesche das aktuelle Dokument oder wechsle zu Pro.'
        ));
      } else {
        setReaderError(normalizeNetworkErrorMessage(error, 'Не удалось загрузить текст в читалку.', 'Text konnte nicht in den Leser geladen werden.'));
      }
    } finally {
      setReaderLoading(false);
    }
  }

  const loadFlashcards = async () => {
    if (!initData) {
      setFlashcardsError(initDataMissingMsg);
      return;
    }
    const requestedMode = String(flashcardTrainingModeRef.current || flashcardTrainingMode || 'quiz').toLowerCase();
    const requestedSetSize = requestedMode === 'blocks'
      ? Math.max(flashcardSetSize * 5, 40)
      : flashcardSetSize;
    const requestSignature = [
      requestedMode,
      String(flashcardSetSize),
      String(requestedSetSize),
      String(flashcardFolderMode || 'all'),
      flashcardFolderMode === 'folder' ? String(flashcardFolderId || '') : '-',
    ].join('|');
    const nowTs = Date.now();

    if (flashcardsLoadInFlightRef.current) {
      if (flashcardsLoadInFlightSignatureRef.current !== requestSignature) {
        flashcardsLoadPendingRef.current = true;
      }
      return;
    }
    if (
      flashcardsLoadLastSignatureRef.current === requestSignature
      && (nowTs - flashcardsLoadLastStartedAtRef.current) < FLASHCARDS_LOAD_DEDUP_WINDOW_MS
    ) {
      return;
    }

    flashcardsLoadInFlightRef.current = true;
    flashcardsLoadInFlightSignatureRef.current = requestSignature;
    flashcardsLoadLastSignatureRef.current = requestSignature;
    flashcardsLoadLastStartedAtRef.current = nowTs;
    setFlashcardsLoading(true);
    setFlashcardsError('');
    try {
      const requestPayload = {
        initData,
        training_mode: requestedMode,
        set_size: requestedSetSize,
        wrong_size: 5,
        folder_mode: flashcardFolderMode,
        folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
      };
      const requestOptions = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestPayload),
      };
      const FLASHCARDS_SET_TIMEOUT_MS = 60000;
      let response;
      try {
        response = await fetchWithTimeout('/api/webapp/flashcards/set', requestOptions, FLASHCARDS_SET_TIMEOUT_MS);
      } catch (error) {
        const raw = String(error?.message || '').toLowerCase();
        const name = String(error?.name || '').toLowerCase();
        const shouldRetry = name === 'timeouterror'
          || name === 'aborterror'
          || raw.includes('load failed')
          || raw.includes('failed to fetch')
          || raw.includes('networkerror')
          || raw.includes('timeout')
          || raw.includes('timed out')
          || raw.includes('aborted');
        if (!shouldRetry) throw error;
        await new Promise((resolve) => window.setTimeout(resolve, 350));
        response = await fetchWithTimeout('/api/webapp/flashcards/set', requestOptions, FLASHCARDS_SET_TIMEOUT_MS);
      }
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      const items = (data.items || []).map((item) => ({
        ...item,
        response_json: coerceResponseJson(item.response_json),
      }));
      const solvedFilteredItems = requestedMode === 'sentence'
        ? items.filter((item) => !readSolvedTodaySet(requestedMode).has(Number(item?.id || 0)))
        : items;
      const isBlocksSingleWordEligible = (entry) => {
        const raw = String(resolveBlocksAnswer(entry) || '').replace(/\s+/g, ' ').trim();
        if (!raw) return false;
        return raw.length <= BLOCKS_SINGLE_WORD_MAX_LEN;
      };
      const filteredItems = requestedMode === 'blocks'
        ? solvedFilteredItems.filter(isBlocksSingleWordEligible)
        : solvedFilteredItems;
      const sessionItems = requestedMode === 'blocks'
        ? filteredItems.slice(0, flashcardSetSize)
        : filteredItems;
      try {
        console.info('[flashcards-client-profile]', {
          mode: requestedMode,
          requested_set_size: flashcardSetSize,
          requested_fetch_size: requestedSetSize,
          server_items: items.length,
          after_solved_today: solvedFilteredItems.length,
          blocks_eligible_len10_client: requestedMode === 'blocks' ? filteredItems.length : null,
          session_items: sessionItems.length,
          server_profile: data?.profile || null,
        });
      } catch (_error) {
        // no-op: profiling must never break training flow
      }
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
      if (revealTimeoutRef.current) {
        clearTimeout(revealTimeoutRef.current);
        revealTimeoutRef.current = null;
      }
      stopTtsPlayback();
      setFlashcards(sessionItems);
      setFlashcardIndex(0);
      setFlashcardSelection(null);
      setFlashcardSetComplete(false);
      setFlashcardTimedOut(false);
      setFlashcardExitSummary(false);
      setFlashcardTimerKey((prev) => prev + 1);
      setFlashcardPreviewIndex(0);
      setFlashcardStats({
        total: sessionItems.length,
        correct: 0,
        wrong: 0,
      });
      if (requestedMode === 'blocks' && solvedFilteredItems.length > 0 && sessionItems.length === 0) {
        setFlashcardsError(
          tr(
            `Для режима Blocks сейчас нет подходящих карточек: используем варианты длиной до ${BLOCKS_SINGLE_WORD_MAX_LEN} символов (включая пробелы).`,
            `Fuer den Blocks-Modus gibt es aktuell keine passenden Karten: Es werden nur Varianten bis ${BLOCKS_SINGLE_WORD_MAX_LEN} Zeichen (inklusive Leerzeichen) verwendet.`
          )
        );
      } else if (requestedMode === 'sentence' && items.length > 0 && solvedFilteredItems.length === 0) {
        const modeLabel = tr('Satz ergänzen', 'Satz ergaenzen');
        setFlashcardsError(
          tr(
            `На сегодня в режиме ${modeLabel} всё выполнено: карточки с верным ответом больше не показываются.`,
            `Fuer heute ist der Modus ${modeLabel} erledigt: korrekt beantwortete Karten werden nicht erneut gezeigt.`
          )
        );
      }
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось загрузить карточки.',
        'Karten konnten nicht geladen werden.'
      );
      setFlashcardsError(`${tr('Ошибка карточек', 'Kartenfehler')}: ${friendly}`);
    } finally {
      setFlashcardsLoading(false);
      flashcardsLoadInFlightRef.current = false;
      flashcardsLoadInFlightSignatureRef.current = '';
      if (flashcardsLoadPendingRef.current) {
        flashcardsLoadPendingRef.current = false;
        window.setTimeout(() => {
          void loadFlashcards();
        }, 0);
      }
    }

    (async () => {
      try {
        const poolResponse = await fetchWithTimeout('/api/webapp/dictionary/cards', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            limit: 60,
            folder_mode: flashcardFolderMode,
            folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
          }),
        }, 12000);
        if (poolResponse.ok) {
          const poolData = await poolResponse.json();
          setDictionaryLanguagePair(resolveLanguagePairForUI(poolData.language_pair));
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
      setFoldersError(`${tr('Ошибка папок', 'Ordnerfehler')}: ${error.message}`);
    } finally {
      setFoldersLoading(false);
    }
  };

  const handleCreateFolder = async () => {
    if (!initData) {
      setFoldersError(initDataMissingMsg);
      return;
    }
    if (!newFolderName.trim()) {
      setFoldersError(tr('Введите название папки.', 'Bitte gib einen Ordnernamen ein.'));
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
      setFoldersError(`${tr('Ошибка создания папки', 'Fehler beim Erstellen des Ordners')}: ${error.message}`);
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
    const quizType = String(responseJson?.quiz_type || '').trim();
    if (quizType === 'separable_prefix_verb_gap' || quizType === 'sentence_gap_context') {
      const options = Array.isArray(responseJson?.options)
        ? responseJson.options.map((item) => String(item || '').trim()).filter(Boolean)
        : [];
      if (options.length === 4) {
        return options;
      }
    }
    const correct = resolveFlashcardTexts(entry).targetText || '';
    if (!correct) return [];
    const pool = [...allEntries, ...flashcardPool]
      .filter((item) => item && item.id !== entry?.id);
    const values = Array.from(new Set(
      pool
        .map((item) => resolveFlashcardTexts(item).targetText || '')
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

  const resolveQuizCorrectOption = (entry, options = []) => {
    const responseJson = entry?.response_json || {};
    const quizType = String(responseJson?.quiz_type || '').trim();
    const normalizedOptions = Array.isArray(options)
      ? options.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const oneBasedCorrectIndex = Number(responseJson?.correct_index);
    const zeroBasedCorrectIndex = Number.isFinite(oneBasedCorrectIndex)
      ? Math.max(0, Math.min(normalizedOptions.length - 1, Math.trunc(oneBasedCorrectIndex) - 1))
      : -1;
    const indexed = normalizedOptions[zeroBasedCorrectIndex] || '';
    if (indexed) return indexed;
    if (quizType === 'sentence_gap_context') {
      return String(responseJson?.correct_word || '').trim() || resolveFlashcardTexts(entry).targetText || '—';
    }
    if (quizType === 'separable_prefix_verb_gap') {
      return String(responseJson?.correct_infinitive || '').trim() || resolveFlashcardTexts(entry).targetText || '—';
    }
    return resolveFlashcardTexts(entry).targetText || '—';
  };

  const renderSentenceWithGapAnswer = (sentenceWithGap, answer, tone = 'neutral') => {
    const sentence = String(sentenceWithGap || '').trim();
    if (!sentence) return '—';
    if (!sentence.includes('___') || !answer) return sentence;
    const answerToneClass = tone === 'correct'
      ? 'flashcard-gap-answer is-correct'
      : tone === 'wrong'
        ? 'flashcard-gap-answer is-wrong'
        : 'flashcard-gap-answer';
    const [left, right] = sentence.split('___');
    return (
      <>
        {left}
        <span className={answerToneClass}>{answer}</span>
        {right}
      </>
    );
  };

  const resolveBlocksAnswer = (entry) => {
    const responseJson = entry?.response_json || {};
    const translationDe = resolveFlashcardTexts(entry).targetText || '';
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
    return resolveFlashcardTexts(entry).sourceText || t('blocks_build_answer');
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
      const resolvedRating = (() => {
        const raw = String(meta.rating || '').trim().toUpperCase();
        if (raw === 'AGAIN' || raw === 'HARD' || raw === 'GOOD' || raw === 'EASY') return raw;
        if (!isCorrect) return 'AGAIN';
        const timeSpentMs = Number(meta.timeSpentMs);
        if (!Number.isFinite(timeSpentMs) || timeSpentMs < 0) return 'GOOD';
        const seconds = timeSpentMs / 1000;
        if (seconds <= 5) return 'EASY';
        if (seconds <= 8) return 'GOOD';
        return 'HARD';
      })();

      await fetch('/api/cards/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          card_id: entryId,
          rating: resolvedRating,
          response_ms: typeof meta.timeSpentMs === 'number' ? Math.max(0, Math.round(meta.timeSpentMs)) : null,
          // legacy analytics fields are kept for backward-compatible payload shape on the client side.
          mode: meta.mode || flashcardTrainingMode || 'quiz',
          hints_used: typeof meta.hintsUsed === 'number' ? Math.max(0, Math.round(meta.hintsUsed)) : 0,
        }),
      });
    } catch (error) {
      // ignore answer tracking errors
    }
  };

  const advanceFlashcard = () => {
    stopTtsPlayback();
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
    const nextIndex = flashcardIndex + 1;
    if (nextIndex >= flashcards.length) {
      setFlashcardSetComplete(true);
      void dispatchQueuedFlashcardFeel('set_complete');
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
            title: tr('Завершить повтор?', 'Wiederholung beenden?'),
            message: tr('Текущий прогресс будет завершён.', 'Der aktuelle Fortschritt wird beendet.'),
            buttons: [
              { id: 'continue', type: 'default', text: tr('Продолжить', 'Weiter') },
              { id: 'finish', type: 'destructive', text: tr('Завершить', 'Beenden') },
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

  const renderClickableText = (text, options = {}) => {
    const className = options.className || 'clickable-word';
    const compact = Boolean(options.compact);
    const inlineLookup = Boolean(options.inlineLookup);
    const lookupLang = options.lookupLang || '';
    const selectionTypeOption = String(options.selectionType || '').trim();
    const stopPropagation = Boolean(options.stopPropagation);
    if (!text) return null;
    return text.split(/\s+/).map((word, index) => {
      const cleaned = word.replace(/[^A-Za-zÄÖÜäöüßÀ-ÿА-Яа-яЁё'’-]/g, '');
      if (!cleaned) {
        return <span key={`w-${index}`}>{word} </span>;
      }
      return (
        <span
          key={`w-${index}`}
          className={className}
          onClick={(event) => {
            if (stopPropagation) {
              event.stopPropagation();
            }
            handleSelection(event, cleaned, {
              compact,
              inlineLookup,
              lookupLang,
              selectionType: selectionTypeOption,
            });
          }}
        >
          {word}{' '}
        </span>
      );
    });
  };

  const openYoutubeSentenceSelection = (event, text, selectionType = 'youtube_sentence') => {
    const normalized = normalizeSubtitleText(text);
    if (!normalized) return;
    handleSelection(event, normalized, {
      compact: true,
      inlineLookup: true,
      lookupLang: getNormalizeLookupLang(),
      selectionType,
    });
  };

  const renderSubtitleText = (text, selectionType = (youtubeOverlayEnabled ? 'youtube_overlay_word' : 'youtube_word')) => renderClickableText(
    normalizeSubtitleText(text),
    {
      lookupLang: getNormalizeLookupLang(),
      compact: true,
      inlineLookup: true,
      selectionType,
      stopPropagation: true,
    }
  );

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

  const jumpYoutubeBySubtitle = (direction) => {
    const step = Number(direction);
    if (!step || !youtubeTranscript.length || !youtubePlayerRef.current?.seekTo) return;
    const activeIndex = getActiveSubtitleIndex();
    const fallbackIndex = step > 0 ? -1 : 0;
    const baseIndex = activeIndex >= 0 ? activeIndex : fallbackIndex;
    const nextIndex = Math.min(
      youtubeTranscript.length - 1,
      Math.max(0, baseIndex + step)
    );
    const nextStart = Math.max(0, Number(youtubeTranscript[nextIndex]?.start ?? 0));
    try {
      youtubePlayerRef.current.seekTo(nextStart, true);
      setYoutubeCurrentTime(nextStart);
      persistYoutubeResumeState(nextStart);
    } catch (_seekError) {
      // ignore seek errors
    }
  };

  const toggleYoutubePlayback = () => {
    if (!youtubeId || !youtubePlayerRef.current) return;
    youtubePausedBySelectionRef.current = false;
    try {
      const state = youtubePlayerRef.current?.getPlayerState?.();
      if (state === 1 || state === 3) {
        youtubePlayerRef.current.pauseVideo?.();
        return;
      }
      youtubePlayerRef.current.playVideo?.();
      setYoutubePlaybackStarted(true);
      setYoutubeForceShowPanel(false);
    } catch (_toggleError) {
      // ignore player toggle errors
    }
  };

  const renderYoutubeSentenceJumpBar = ({ inline = false } = {}) => {
    const activeSubtitleIndex = getActiveSubtitleIndex();
    const canControlPlayback = Boolean(youtubeId && youtubePlayerRef.current);
    const canJump = Boolean(youtubeTranscript.length && youtubePlayerRef.current?.seekTo);
    const canJumpPrev = canJump && activeSubtitleIndex > 0;
    const canJumpNext = canJump && activeSubtitleIndex < youtubeTranscript.length - 1;
    return (
      <div
        className={`youtube-sentence-jump-bar ${inline ? 'is-inline' : 'is-floating'}`}
        aria-label={tr('Навигация по предложениям', 'Navigation zwischen Saetzen')}
      >
        <button
          type="button"
          className="youtube-sentence-jump-btn is-prev"
          onClick={() => jumpYoutubeBySubtitle(-1)}
          disabled={!canJumpPrev}
          aria-label={tr('Предыдущее предложение', 'Vorheriger Satz')}
        >
          <span className="youtube-sentence-jump-icon" aria-hidden="true">←</span>
        </button>
        <button
          type="button"
          className={`youtube-sentence-jump-btn is-toggle ${youtubeIsPaused ? 'is-paused' : 'is-playing'}`}
          onClick={toggleYoutubePlayback}
          disabled={!canControlPlayback}
          aria-label={youtubeIsPaused
            ? tr('Продолжить видео и субтитры', 'Video und Untertitel fortsetzen')
            : tr('Поставить видео и субтитры на паузу', 'Video und Untertitel pausieren')}
        >
          <span className="youtube-sentence-jump-icon" aria-hidden="true">
            {youtubeIsPaused ? '▶' : '❚❚'}
          </span>
        </button>
        <button
          type="button"
          className="youtube-sentence-jump-btn is-next"
          onClick={() => jumpYoutubeBySubtitle(1)}
          disabled={!canJumpNext}
          aria-label={tr('Следующее предложение', 'Naechster Satz')}
        >
          <span className="youtube-sentence-jump-icon" aria-hidden="true">→</span>
        </button>
      </div>
    );
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
      setYoutubeTranscriptError(`${tr('Ошибка сохранения субтитров', 'Fehler beim Speichern der Untertitel')}: ${error.message}`);
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
    if (translationFinishInFlightRef.current) {
      return;
    }
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    translationFinishInFlightRef.current = true;
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
      setFinishMessage(data.message || tr('Перевод завершён.', 'Uebersetzung abgeschlossen.'));
      setFinishStatus('done');
      if (translationDraftStorageTimeoutRef.current) {
        clearTimeout(translationDraftStorageTimeoutRef.current);
        translationDraftStorageTimeoutRef.current = null;
      }
      if (translationDraftSyncTimeoutRef.current) {
        clearTimeout(translationDraftSyncTimeoutRef.current);
        translationDraftSyncTimeoutRef.current = null;
      }
      translationDraftsRef.current = {};
      safeStorageRemove(translationDraftStorageKey);
      setTranslationDrafts({});
      setSessionType('none');
      setStoryGuess('');
      setStoryResult(null);
      setResults([]);
      setSentences([]);
      setTranslationAudioGrammarOptIn({});
      setTranslationAudioGrammarSaving({});
      translationCheckPollTokenRef.current += 1;
      setTranslationCheckProgress({ active: false, done: 0, total: 0 });
      await loadSessionInfo();
    } catch (error) {
      setWebappError(`${tr('Ошибка завершения', 'Abschlussfehler')}: ${error.message}`);
    } finally {
      translationFinishInFlightRef.current = false;
      setWebappLoading(false);
    }
  };

  const handleExplainTranslation = async (item) => {
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    const key = String(item.sentence_number ?? item.original_text);
    if (explanationInFlightKeysRef.current.has(key)) {
      return;
    }
    explanationInFlightKeysRef.current.add(key);
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
      setWebappError(`${tr('Ошибка объяснения', 'Erklaerungsfehler')}: ${error.message}`);
    } finally {
      explanationInFlightKeysRef.current.delete(key);
      setExplanationLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  const handleToggleResultAudioGrammar = async (item, enabled) => {
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    const translationId = Number(item?.translation_id || 0);
    if (!translationId) {
      return;
    }
    setTranslationAudioGrammarSaving((prev) => ({ ...prev, [translationId]: true }));
    try {
      const response = await fetch('/api/audio/grammar-optin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          translation_id: translationId,
          enabled: Boolean(enabled),
        }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения аудио-галочки', 'Fehler beim Speichern der Audio-Markierung'));
      }
      setTranslationAudioGrammarOptIn((prev) => ({ ...prev, [translationId]: Boolean(enabled) }));
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось сохранить настройку аудио-объяснения.',
        'Audio-Erklaerungsoption konnte nicht gespeichert werden.'
      );
      setWebappError(friendly);
    } finally {
      setTranslationAudioGrammarSaving((prev) => ({ ...prev, [translationId]: false }));
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
      .replace(/\*(.+?)\*/g, '<strong>$1</strong>')
      .replace(/\n/g, '<br />');
  };

  const normalizeStoryFeedbackLine = (line) => String(line || '')
    .replace(/\u00a0/g, ' ')
    .replace(/^\s+/, '')
    .replace(/\s+$/, '');

  const stripStoryListMarker = (line) => normalizeStoryFeedbackLine(line)
    .replace(/^[-•▪◦●■▸▹▶►]+\s*/, '');

  const parseStoryFeedback = (feedback) => {
    if (!feedback) return null;
    const sections = {
      intro: [],
      summary: [],
      sentence: [],
      grammar: [],
      extra: [],
    };
    let currentSection = 'intro';

    String(feedback)
      .replace(/\r/g, '')
      .split('\n')
      .forEach((rawLine) => {
        const line = normalizeStoryFeedbackLine(rawLine);
        if (!line) return;
        if (/^Score:/i.test(line) || /^Feedback:/i.test(line)) return;
        if (/^🟨\s*ОБЩАЯ ОЦЕНКА/i.test(line)) {
          currentSection = 'summary';
          return;
        }
        if (/^🧠\s*РАЗБОР ПО ПРЕДЛОЖЕНИЯМ/i.test(line)) {
          currentSection = 'sentence';
          return;
        }
        if (/^📚\s*ГРАММАТИКА ДЛЯ ПРОРАБОТКИ/i.test(line)) {
          currentSection = 'grammar';
          return;
        }
        if (/^🔎\s*ДОПОЛНИТЕЛЬНО/i.test(line)) {
          currentSection = 'extra';
          return;
        }
        sections[currentSection].push(line);
      });

    const sentenceBlocks = [];
    let currentBlock = [];
    let currentItem = null;

    const flushSentenceBlock = () => {
      if (!currentBlock.length) return;
      sentenceBlocks.push(currentBlock);
      currentBlock = [];
      currentItem = null;
    };

    sections.sentence.forEach((line) => {
      if (/^1\)\s*Оригинал\b/i.test(line) && currentBlock.length) {
        flushSentenceBlock();
      }
      const match = line.match(/^(\d+)\)\s*([^:]+):\s*(.*)$/u);
      if (match) {
        currentItem = {
          sourceIndex: Number(match[1] || 0),
          label: String(match[2] || '').trim(),
          contentLines: String(match[3] || '').trim() ? [String(match[3] || '').trim()] : [],
        };
        currentBlock.push(currentItem);
        return;
      }
      if (currentItem) {
        currentItem.contentLines.push(line);
        return;
      }
      sections.intro.push(line);
    });
    flushSentenceBlock();

    return {
      intro: sections.intro.map(stripStoryListMarker).filter(Boolean),
      summary: sections.summary.map(stripStoryListMarker).filter(Boolean),
      sentenceLines: sections.sentence.map(normalizeStoryFeedbackLine).filter(Boolean),
      sentenceBlocks,
      grammar: sections.grammar.map(stripStoryListMarker).filter(Boolean),
      extra: sections.extra.map(stripStoryListMarker).filter(Boolean),
    };
  };

  const renderStoryFeedbackContent = (contentLines, keyPrefix) => {
    const lines = (Array.isArray(contentLines) ? contentLines : [contentLines])
      .map(normalizeStoryFeedbackLine)
      .filter(Boolean);
    if (!lines.length) {
      return <span className="story-feedback-item-content">—</span>;
    }
    if (lines.length === 1 && !/^[-•▪◦●■▸▹▶►]\s*/.test(lines[0])) {
      return <span className="story-feedback-item-content">{lines[0]}</span>;
    }
    return (
      <ul className="story-feedback-sublist">
        {lines.map((line, index) => (
          <li key={`${keyPrefix}-${index}`} className="story-feedback-subitem">
            {stripStoryListMarker(line)}
          </li>
        ))}
      </ul>
    );
  };

  const renderStoryFeedbackList = (items, keyPrefix) => {
    if (!Array.isArray(items) || items.length === 0) return null;
    return (
      <ul className="story-feedback-list">
        {items.map((item, index) => (
          <li key={`${keyPrefix}-${index}`} className="story-feedback-list-item">
            {item}
          </li>
        ))}
      </ul>
    );
  };

  const renderStoryFeedback = (feedback) => {
    if (!feedback) return null;
    const parsed = parseStoryFeedback(feedback);
    const hasStructuredContent = parsed
      && (
        parsed.intro.length > 0
        || parsed.summary.length > 0
        || parsed.sentenceBlocks.length > 0
        || parsed.grammar.length > 0
        || parsed.extra.length > 0
      );

    if (!hasStructuredContent) {
      return (
        <div
          className="webapp-result-text story-result-feedback"
          dangerouslySetInnerHTML={{ __html: renderRichText(feedback) }}
        />
      );
    }

    return (
      <div className="webapp-result-text story-result-feedback">
        {parsed.intro.length > 0 && (
          <section className="story-feedback-section">
            {renderStoryFeedbackList(parsed.intro, 'story-intro')}
          </section>
        )}

        {parsed.summary.length > 0 && (
          <section className="story-feedback-section">
            <div className="story-feedback-section-title">
              {tr('Общая оценка', 'Gesamtbewertung')}
            </div>
            {renderStoryFeedbackList(parsed.summary, 'story-summary')}
          </section>
        )}

        {parsed.sentenceBlocks.length > 0 && (
          <section className="story-feedback-section">
            <div className="story-feedback-section-title">
              {tr('Разбор по предложениям', 'Satzanalyse')}
            </div>
            <div className="story-feedback-sentences">
              {parsed.sentenceBlocks.map((block, sentenceIndex) => (
                <section
                  key={`story-sentence-${sentenceIndex}`}
                  className="story-feedback-sentence-card"
                >
                  <div className="story-feedback-sentence-title">
                    {sentenceIndex + 1}) {tr('Предложение', 'Satz')} {sentenceIndex + 1}
                  </div>
                  <div className="story-feedback-sentence-items">
                    {block.map((item, itemIndex) => (
                      <div
                        key={`story-sentence-${sentenceIndex}-item-${item.sourceIndex || itemIndex}`}
                        className="story-feedback-sentence-item"
                      >
                        <div className="story-feedback-item-label">{item.label}</div>
                        {renderStoryFeedbackContent(
                          item.contentLines,
                          `story-sentence-${sentenceIndex}-item-${itemIndex}`
                        )}
                      </div>
                    ))}
                  </div>
                </section>
              ))}
            </div>
          </section>
        )}

        {parsed.sentenceBlocks.length === 0 && parsed.sentenceLines.length > 0 && (
          <section className="story-feedback-section">
            <div className="story-feedback-section-title">
              {tr('Разбор по предложениям', 'Satzanalyse')}
            </div>
            {renderStoryFeedbackList(parsed.sentenceLines.map(stripStoryListMarker), 'story-sentences-fallback')}
          </section>
        )}

        {parsed.grammar.length > 0 && (
          <section className="story-feedback-section">
            <div className="story-feedback-section-title">
              {tr('Грамматика для проработки', 'Grammatik zum Ueben')}
            </div>
            {renderStoryFeedbackList(parsed.grammar, 'story-grammar')}
          </section>
        )}

        {parsed.extra.length > 0 && (
          <section className="story-feedback-section">
            <div className="story-feedback-section-title">
              {tr('Дополнительно', 'Zusaetzlich')}
            </div>
            {renderStoryFeedbackList(parsed.extra, 'story-extra')}
          </section>
        )}
      </div>
    );
  };

  const renderExplanationRichText = (text) => {
    if (!text) return '';
    const escaped = text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    const withSections = escaped
      .replace(/^Error\s+(\d+):/gim, '<strong>🔴 Error $1:</strong>')
      .replace(/^Correct Translation:/gim, '<strong>🟣 Correct Translation:</strong>')
      .replace(/^Grammar Explanation:/gim, '<strong>🟡 Grammar Explanation:</strong>')
      .replace(/^Alternative Sentence Construction:/gim, '<strong>🔵 Alternative Sentence Construction:</strong>')
      .replace(/^Alternative Construction:/gim, '<strong>🔵 Alternative Sentence Construction:</strong>')
      .replace(/^Synonyms:/gim, '<strong>➡️ Synonyms:</strong>')
      .replace(/^Original Word:/gim, '<strong>• Original Word:</strong>')
      .replace(/^Possible Synonyms:/gim, '<strong>• Possible Synonyms:</strong>');

    return withSections
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.+?)\*/g, '<strong>$1</strong>')
      .replace(/\n/g, '<br />');
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
            <span className="webapp-feedback-value">{renderClickableText(match[1], {
              compact: true,
              inlineLookup: true,
              lookupLang: getNormalizeLookupLang(),
              selectionType: 'translation_result_word',
            })}</span>
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
      setDictionaryError(initDataMissingMsg);
      return;
    }
    if (!dictionaryWord.trim()) {
      setDictionaryError(tr('Введите слово или фразу для словаря.', 'Bitte gib ein Wort oder eine Phrase fuers Woerterbuch ein.'));
      return;
    }
    setDictionaryLoading(true);
    setDictionaryLookupMode('gpt');
    setDictionaryError('');
    setDictionaryResult(null);
    setDictionarySaved('');
    setLastLookupScrollY(null);
    try {
      const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
      const guessedLookupLang = normalizeLangCode(detectTtsLangFromText(dictionaryWord));
      const lookupLang = guessedLookupLang && (guessedLookupLang === pair.source_lang || guessedLookupLang === pair.target_lang)
        ? guessedLookupLang
        : '';
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word: dictionaryWord.trim(),
          lookup_lang: lookupLang || undefined,
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
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
    } catch (error) {
      setDictionaryError(`${tr('Ошибка словаря', 'Woerterbuchfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
      setDictionaryLookupMode('');
    }
  };

  const handleDictionaryQuickLookup = async () => {
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    const sourceWord = String(dictionaryWord || '').trim();
    if (!sourceWord) {
      setDictionaryError(tr('Введите слово или фразу для словаря.', 'Bitte gib ein Wort oder eine Phrase fuers Woerterbuch ein.'));
      return;
    }
    setDictionaryLoading(true);
    setDictionaryLookupMode('quick');
    setDictionaryError('');
    setDictionaryResult(null);
    setDictionarySaved('');
    setLastLookupScrollY(null);
    try {
      const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
      const quick = await requestQuickTranslation(sourceWord);
      const sourceLang = normalizeLangCode(
        quick.detectedSource
        || quick.sourceLangHint
        || pair.source_lang
      ) || pair.source_lang;
      const targetLang = normalizeLangCode(
        quick.targetLang
        || (sourceLang === pair.source_lang ? pair.target_lang : pair.source_lang)
      ) || pair.target_lang;
      const sourceText = String(quick.cleaned || sourceWord).trim();
      const targetText = String(quick.translation || '').trim();
      if (!targetText) {
        throw new Error(tr('Быстрый перевод не вернул результат', 'Schnelluebersetzung hat kein Ergebnis geliefert'));
      }
      setDictionaryResult({
        word_ru: sourceText,
        translation_de: targetText,
        word_de: targetText,
        translation_ru: sourceText,
        source_text: sourceText,
        target_text: targetText,
        source_lang: sourceLang,
        target_lang: targetLang,
        part_of_speech: '',
        article: '',
        forms: {},
        usage_examples: [],
        provider: quick.provider || '',
        quick_mode: true,
      });
      setDictionaryDirection(`${sourceLang}-${targetLang}`);
      setDictionaryLanguagePair(resolveLanguagePairForUI({ source_lang: sourceLang, target_lang: targetLang }));
    } catch (error) {
      setDictionaryError(`${tr('Ошибка быстрого перевода', 'Fehler bei Schnelluebersetzung')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
      setDictionaryLookupMode('');
    }
  };

  const handleDictionarySave = async () => {
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    if (!dictionaryResult) {
      setDictionaryError(tr('Сначала выполните перевод в словаре.', 'Fuehre zuerst eine Uebersetzung im Woerterbuch aus.'));
      return;
    }
    setCollocationsVisible(true);
    setCollocationsLoading(true);
    setCollocationsError('');
    setCollocationOptions([]);
    setSelectedCollocations([]);
    try {
      const response = await fetch('/api/webapp/dictionary/collocations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word: dictionaryWord.trim() || getDictionarySourceTarget(dictionaryResult).sourceText || dictionaryResult.word_ru || dictionaryResult.word_de,
          translation: getDictionarySourceTarget(dictionaryResult).targetText,
          direction: dictionaryDirection,
          source_lang: resolveLanguagePairForUI(dictionaryLanguagePair).source_lang,
          target_lang: resolveLanguagePairForUI(dictionaryLanguagePair).target_lang,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      const sourceTarget = getDictionarySourceTarget(dictionaryResult);
      const baseSource = sourceTarget.sourceText || dictionaryWord.trim();
      const baseTarget = sourceTarget.targetText || '';
      const options = [
        { source: baseSource, target: baseTarget, isBase: true },
        ...(data.items || []).map((item) => ({
          ...(() => {
            const normalized = applyArticleForDirection(
              item.source,
              item.target,
              dictionaryDirection,
              dictionaryResult,
            );
            return { source: normalized.sourceText, target: normalized.targetText };
          })(),
          isBase: false,
        })),
      ].filter((item) => item.source && item.target);
      setCollocationOptions(options);
      setSelectedCollocations(options.length > 0
        ? [`${String(options[0].source)}|||${String(options[0].target)}`]
        : []);
    } catch (error) {
      setCollocationsError(`${tr('Ошибка связок', 'Kollokationsfehler')}: ${error.message}`);
    } finally {
      setCollocationsLoading(false);
    }
  };

  const handleConfirmSaveCollocation = async () => {
    const selectedOptions = collocationOptions.filter((option) => (
      selectedCollocations.includes(`${String(option.source)}|||${String(option.target)}`)
    ));
    if (selectedOptions.length === 0) {
      setCollocationsError(tr('Выберите минимум один вариант для сохранения.', 'Waehle mindestens eine Option zum Speichern.'));
      return;
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    try {
      const pair = resolveLanguagePairForUI(dictionaryLanguagePair);
      const directionPair = String(dictionaryDirection || '').includes('-')
        ? String(dictionaryDirection).toLowerCase().split('-', 2)
        : [];
      const saveSourceLang = normalizeLangCode(directionPair[0] || pair.source_lang);
      const saveTargetLang = normalizeLangCode(directionPair[1] || pair.target_lang);
      const isLegacyPair = pair.source_lang === 'ru' && pair.target_lang === 'de' && isLegacyRuDeDirection(dictionaryDirection);
      for (const selectedCollocation of selectedOptions) {
        const response = await fetch('/api/webapp/dictionary/save', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            word_ru: isLegacyPair && dictionaryDirection === 'ru-de' ? selectedCollocation.source : '',
            word_de: isLegacyPair && dictionaryDirection === 'de-ru' ? selectedCollocation.source : '',
            translation_de: isLegacyPair && dictionaryDirection === 'ru-de' ? selectedCollocation.target : '',
            translation_ru: isLegacyPair && dictionaryDirection === 'de-ru' ? selectedCollocation.target : '',
            source_text: selectedCollocation.source,
            target_text: selectedCollocation.target,
            response_json: {
              ...(dictionaryResult || {}),
              source_text: selectedCollocation.source,
              target_text: selectedCollocation.target,
              source_lang: saveSourceLang || pair.source_lang,
              target_lang: saveTargetLang || pair.target_lang,
              language_pair: {
                source_lang: saveSourceLang || pair.source_lang,
                target_lang: saveTargetLang || pair.target_lang,
              },
            },
            source_lang: saveSourceLang || undefined,
            target_lang: saveTargetLang || undefined,
            direction: dictionaryDirection || undefined,
            folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
            origin_process: 'webapp_dictionary_save',
            origin_meta: {
              endpoint: '/api/webapp/dictionary/save',
              flow: 'dictionary_collocations',
              from: 'dictionary_manual',
            },
          }),
        });
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
        const payload = await response.json();
        setDictionaryLanguagePair(resolveLanguagePairForUI(payload.language_pair));
      }
      setDictionarySaved(tr('Добавлено в словарь ✅', 'Zum Woerterbuch hinzugefuegt ✅'));
      setCollocationsVisible(false);
    } catch (error) {
      setDictionaryError(`${tr('Ошибка сохранения', 'Speicherfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleExportDictionaryPdf = async () => {
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
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
      setDictionaryPdfUrl((prev) => {
        if (prev) {
          try {
            window.URL.revokeObjectURL(prev);
          } catch (_error) {
            // ignore revoke errors
          }
        }
        return url;
      });
      setDictionaryPdfName('dictionary.pdf');
      setDictionarySaved(tr('PDF готов. Выберите: скачать или открыть.', 'PDF ist bereit. Waehle: herunterladen oder oeffnen.'));
    } catch (error) {
      setDictionaryError(`${tr('Ошибка выгрузки PDF', 'PDF-Exportfehler')}: ${error.message}`);
    } finally {
      setExportLoading(false);
    }
  };

  const handleCloseDictionaryPdf = useCallback(() => {
    setDictionaryPdfUrl((prev) => {
      if (prev) {
        try {
          window.URL.revokeObjectURL(prev);
        } catch (_error) {
          // ignore revoke errors
        }
      }
      return '';
    });
    setDictionaryPdfName('dictionary.pdf');
  }, []);

  const handleOpenDictionaryPdf = useCallback(() => {
    if (!dictionaryPdfUrl) return;
    const opened = window.open(dictionaryPdfUrl, '_blank', 'noopener,noreferrer');
    if (opened) return;
    const link = document.createElement('a');
    link.href = dictionaryPdfUrl;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    document.body.appendChild(link);
    link.click();
    link.remove();
  }, [dictionaryPdfUrl]);

  useEffect(() => () => {
    if (!dictionaryPdfUrl) return;
    try {
      window.URL.revokeObjectURL(dictionaryPdfUrl);
    } catch (_error) {
      // ignore revoke errors
    }
  }, [dictionaryPdfUrl]);

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
      setYoutubeSearchError('');
      setYoutubeSearchResults([]);
      safeStorageRemove(youtubeResumeStorageKey);
      safeStorageRemove('webapp_youtube');
      youtubeResumeAppliedForVideoRef.current = '';
      youtubeResumeLastSavedSecondRef.current = -1;
      youtubeResumeLastSyncedSecondRef.current = -1;
      return;
    }
    const id = extractYoutubeId(trimmed);
    if (id) {
      setYoutubeId(id);
      setYoutubeError('');
      const existingRaw = safeStorageGet(youtubeResumeStorageKey) || safeStorageGet('webapp_youtube');
      let existingTime = 0;
      try {
        const parsed = existingRaw ? JSON.parse(existingRaw) : null;
        if (parsed?.id === id) {
          existingTime = Math.max(0, Number(parsed?.currentTime || 0));
        }
      } catch (_error) {
        existingTime = 0;
      }
      writeYoutubeResumeToLocalCache({
        input: trimmed,
        id,
        currentTime: existingTime,
        updatedAt: Date.now(),
      });
    } else if (/(youtube\.com|youtu\.be|^https?:\/\/)/i.test(trimmed)) {
      setYoutubeError(tr('Не удалось распознать ссылку или ID видео.', 'Video-Link oder ID konnte nicht erkannt werden.'));
      setYoutubeId('');
    } else {
      setYoutubeError('');
      setYoutubeId('');
    }
  }, [tr, writeYoutubeResumeToLocalCache, youtubeInput, youtubeResumeStorageKey]);

  const searchYoutubeVideos = async () => {
    const query = youtubeInput.trim();
    if (!query) return;
    if (!initData) {
      setYoutubeSearchError(initDataMissingMsg);
      return;
    }

    const directId = extractYoutubeId(query);
    if (directId) {
      setYoutubeSearchError('');
      setYoutubeSearchResults([]);
      return;
    }

    setYoutubeSearchLoading(true);
    setYoutubeSearchError('');
    try {
      const response = await fetch('/api/webapp/youtube/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, query, limit: 8 }),
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
        setYoutubeSearchError(tr('По вашему запросу ничего не найдено.', 'Keine Ergebnisse fuer diese Suche.'));
      }
    } catch (error) {
      setYoutubeSearchResults([]);
      setYoutubeSearchError(`${tr('Ошибка поиска YouTube', 'YouTube-Suchfehler')}: ${error.message}`);
    } finally {
      setYoutubeSearchLoading(false);
    }
  };

  const fetchTranscript = async () => {
    if (!youtubeId) return;
    if (!initData) {
      setYoutubeTranscriptError(initDataMissingMsg);
      return;
    }
    if (youtubeManualOverride) return;
    setYoutubeTranscriptLoading(true);
    setYoutubeTranscriptError('');
    try {
      const response = await fetch('/api/webapp/youtube/transcript', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          videoId: youtubeId,
          lang: normalizeLangCode(languageProfile?.learning_language) || 'de',
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
      const items = data.items || [];
      setYoutubeTranscript(items);
      setYoutubeTranslations(data.translations || {});
      const hasTiming = items.some((item) => Number(item?.start) > 0);
      setYoutubeTranscriptHasTiming(hasTiming);
      setManualTranscript('');
    } catch (error) {
      setYoutubeTranscript([]);
      setYoutubeTranscriptError(`${tr('Авто-субтитры недоступны', 'Auto-Untertitel nicht verfuegbar')}: ${error.message}`);
    } finally {
      setYoutubeTranscriptLoading(false);
    }
  };

  useEffect(() => {
    if (!youtubeId || !initData) {
      setYoutubeTranscript([]);
      setYoutubeTranscriptError('');
      setYoutubeTranslations({});
      setYoutubeTranslationEnabled(false);
      setYoutubeManualOverride(false);
      setYoutubeTranscriptHasTiming(true);
      setYoutubeIsPaused(false);
    }
  }, [youtubeId, initData]);

  useEffect(() => {
    if (!youtubeSectionVisible) {
      setYoutubeSettingsOpen(false);
    }
  }, [youtubeSectionVisible]);

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
        setMoviesError(`${tr('Ошибка каталога', 'Katalogfehler')}: ${err.message}`);
      })
      .finally(() => {
        if (!cancelled) setMoviesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isWebAppMode, flashcardsOnly, initData, selectedSections, movies.length]);

  useEffect(() => {
    const learning = normalizeLangCode(languageProfile?.learning_language);
    if (!learning) return;
    if (movieLanguageOptions.includes(learning)) {
      setMoviesLanguageFilter(learning);
    } else {
      setMoviesLanguageFilter('all');
    }
  }, [languageProfile?.learning_language, movieLanguageOptions]);

  useEffect(() => {
    youtubeCurrentTimeRef.current = Number(youtubeCurrentTime || 0);
  }, [youtubeCurrentTime]);

  useEffect(() => {
    youtubeResumeLastSavedSecondRef.current = -1;
    youtubeResumeLastSyncedSecondRef.current = -1;
    youtubeResumeAppliedForVideoRef.current = '';
  }, [youtubeId]);

  useEffect(() => {
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
  }, [persistYoutubeResumeState, syncYoutubeResumeState, youtubeCurrentTime, youtubeId, youtubeSectionVisible]);

  useEffect(() => {
    const onPageHide = () => {
      void syncYoutubeResumeState(undefined, { keepalive: true });
    };
    window.addEventListener('pagehide', onPageHide);
    return () => {
      window.removeEventListener('pagehide', onPageHide);
    };
  }, [syncYoutubeResumeState]);

  useEffect(() => {
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
      }
      if (youtubePlayerRef.current && youtubePlayerRef.current.destroy) {
        youtubePlayerRef.current.destroy();
        youtubePlayerRef.current = null;
      }
      return;
    }
    if (!youtubeSectionVisible) {
      setYoutubeIsPaused(true);
      if (youtubeTimeIntervalRef.current) {
        clearInterval(youtubeTimeIntervalRef.current);
        youtubeTimeIntervalRef.current = null;
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
          fs: 0,
          disablekb: 1,
          playsinline: 1,
        },
        events: {
          onReady: () => {
            setYoutubePlayerReady(true);
            setYoutubeIsPaused(true);
            setYoutubePlaybackStarted(false);
            try {
              const stored = safeStorageGet(youtubeResumeStorageKey) || safeStorageGet('webapp_youtube');
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
              } catch (error) {
                // ignore
              }
            }, 400);
          },
          onStateChange: (stateEvent) => {
            const state = stateEvent?.data;
            // YT.PlayerState.PAUSED === 2
            if (state === 2) {
              setYoutubeIsPaused(true);
              try {
                const time = youtubePlayerRef.current?.getCurrentTime?.();
                if (typeof time === 'number' && !Number.isNaN(time)) {
                  setYoutubeCurrentTime(time);
                  void syncYoutubeResumeState(time);
                }
              } catch (error) {
                // ignore
              }
            } else if (state === 1) {
              youtubePausedBySelectionRef.current = false;
              setYoutubeIsPaused(false);
              setYoutubePlaybackStarted(true);
              setYoutubeForceShowPanel(false);
            } else if (state === 3) {
              setYoutubeIsPaused(false);
            } else if (state === 0 || state === 5 || state === -1) {
              setYoutubeIsPaused(true);
            }
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
  }, [persistYoutubeResumeState, syncYoutubeResumeState, youtubeId, youtubeResumeStorageKey, youtubeSectionVisible]);

  useEffect(() => {
    if (!youtubePlayerReady || !youtubeId || !initData || !youtubePlayerRef.current?.seekTo) return;
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
        const localRaw = safeStorageGet(youtubeResumeStorageKey) || safeStorageGet('webapp_youtube');
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
  }, [initData, writeYoutubeResumeToLocalCache, youtubeId, youtubePlayerReady, youtubeResumeStorageKey]);

  useEffect(() => {
    if (youtubeTranscript.length > 0 && youtubeSubtitlesRef.current) {
      youtubeSubtitlesRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, [youtubeTranscript.length]);

  useEffect(() => {
    if (!youtubeAppFullscreen) return undefined;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prevOverflow;
    };
  }, [youtubeAppFullscreen]);

  useEffect(() => {
    if (youtubeAppFullscreen) return;
    clearSelection();
  }, [youtubeAppFullscreen]);

  useEffect(() => {
    if (!selectionText || !selectionPos) {
      return undefined;
    }
    const onPointerDown = (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        clearSelection();
        return;
      }
      if (selectionMenuRef.current?.contains(target)) return;
      clearSelection();
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        clearSelection();
      }
    };
    document.addEventListener('pointerdown', onPointerDown, true);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown, true);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [selectionText, selectionPos]);

  useEffect(() => {
    if (!selectionGptOpen) return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        closeSelectionGptSheet();
      }
    };
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [selectionGptOpen]);

  useEffect(() => {
    if (!youtubeAppFullscreen) {
      setYoutubeIsPaused(false);
    }
  }, [youtubeAppFullscreen]);

  useEffect(() => () => {
    if (inlineToastTimeoutRef.current) {
      clearTimeout(inlineToastTimeoutRef.current);
      inlineToastTimeoutRef.current = null;
    }
  }, []);

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
    if (!youtubeSectionVisible) return;
    if (!youtubeSubtitlesRef.current || !youtubeTranscript.length) return;
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
  }, [youtubeSectionVisible, youtubeTranscript.length, youtubeTranslationEnabled, youtubeOverlayEnabled]);

  useEffect(() => {
    const translationListRef = document.querySelector('.webapp-subtitles.is-translation .webapp-subtitles-list');
    if (!translationListRef) return;
    const activeEl = translationListRef.querySelector('.is-active');
    if (activeEl) {
      const listRect = translationListRef.getBoundingClientRect();
      const activeRect = activeEl.getBoundingClientRect();
      const offset = activeRect.top - listRect.top - listRect.height / 2 + activeRect.height / 2;
      translationListRef.scrollTop += offset;
    }
  }, [youtubeCurrentTime, youtubeTranscript.length, youtubeTranslationEnabled]);

  useEffect(() => {
    if (!youtubeTranslationEnabled) return;
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
  }, [youtubeCurrentTime, youtubeTranscript.length, youtubeId, initData, youtubeTranslations, youtubeTranslationEnabled]);

  const handleLoadDailyHistory = async () => {
    if (historyVisible) {
      setHistoryVisible(false);
      setHistoryError('');
      return;
    }
    if (!initData) {
      setHistoryError(initDataMissingMsg);
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
      setHistoryError(`${tr('Ошибка загрузки истории', 'Fehler beim Laden der Historie')}: ${error.message}`);
    } finally {
      setHistoryLoading(false);
    }
  };

  const buildAnalyticsScopeContextPayload = () => {
    const unsafeChat = telegramApp?.initDataUnsafe?.chat || {};
    const chatType = String(
      unsafeChat?.type || telegramApp?.initDataUnsafe?.chat_type || webappChatType || ''
    ).trim().toLowerCase();
    const rawChatId = unsafeChat?.id ?? unsafeChat?.chat_id;
    const parsedChatId = Number.parseInt(String(rawChatId ?? ''), 10);
    const chatId = Number.isFinite(parsedChatId) ? parsedChatId : null;
    const chatTitle = String(unsafeChat?.title || unsafeChat?.username || '').trim();
    const context = {};
    if (chatType) context.chat_type = chatType;
    if (chatId !== null) context.chat_id = chatId;
    if (chatTitle) context.chat_title = chatTitle;
    return context;
  };

  const parseAnalyticsScopeKey = (rawValue) => {
    const value = String(rawValue || '').trim();
    if (!value || value === 'personal') {
      return {
        scope_key: 'personal',
        scope_kind: 'personal',
        scope_chat_id: null,
      };
    }
    if (value.startsWith('group:')) {
      const parsedId = Number.parseInt(value.slice('group:'.length), 10);
      if (Number.isFinite(parsedId)) {
        return {
          scope_key: `group:${parsedId}`,
          scope_kind: 'group',
          scope_chat_id: parsedId,
        };
      }
    }
    return {
      scope_key: 'personal',
      scope_kind: 'personal',
      scope_chat_id: null,
    };
  };

  const normalizeAnalyticsScopeKeyFromPayload = (payload) => {
    const data = payload && typeof payload === 'object' ? payload : {};
    const effective = data.effective_scope && typeof data.effective_scope === 'object' ? data.effective_scope : {};
    const saved = data.saved_scope && typeof data.saved_scope === 'object' ? data.saved_scope : {};

    const fromEffectiveKey = parseAnalyticsScopeKey(effective.scope_key);
    if (fromEffectiveKey.scope_key !== 'personal' || String(effective.scope_kind || '').toLowerCase() === 'personal') {
      return fromEffectiveKey.scope_key;
    }

    const effectiveKind = String(effective.scope_kind || '').toLowerCase();
    const effectiveChatId = Number.parseInt(String(effective.scope_chat_id ?? ''), 10);
    if (effectiveKind === 'group' && Number.isFinite(effectiveChatId)) {
      return `group:${effectiveChatId}`;
    }

    const fromSavedKey = parseAnalyticsScopeKey(saved.scope_key);
    if (fromSavedKey.scope_key !== 'personal' || String(saved.scope_kind || '').toLowerCase() === 'personal') {
      return fromSavedKey.scope_key;
    }

    const savedKind = String(saved.scope_kind || '').toLowerCase();
    const savedChatId = Number.parseInt(String(saved.scope_chat_id ?? ''), 10);
    if (savedKind === 'group' && Number.isFinite(savedChatId)) {
      return `group:${savedChatId}`;
    }
    return 'personal';
  };

  const applyAnalyticsScopePayload = (payload) => {
    const data = payload && typeof payload === 'object' ? payload : {};
    const availableGroups = Array.isArray(data.available_groups) ? data.available_groups : [];
    const normalizedPayload = { ...data, available_groups: availableGroups };
    setAnalyticsScopeData(normalizedPayload);
    setAnalyticsScopeKey(normalizeAnalyticsScopeKeyFromPayload(normalizedPayload));
  };

  const loadAnalyticsScope = async ({ silent = false } = {}) => {
    if (!initData) {
      if (!silent) {
        setAnalyticsScopeError(initDataMissingMsg);
      }
      return null;
    }

    if (!silent) {
      setAnalyticsScopeLoading(true);
      setAnalyticsScopeError('');
    }

    try {
      const scopeContext = buildAnalyticsScopeContextPayload();
      const body = { initData };
      if (Object.keys(scopeContext).length > 0) {
        body.scope_context = scopeContext;
      }
      const response = await fetch('/api/webapp/analytics/scope', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки режима участия', 'Fehler beim Laden des Teilnahme-Modus'));
      }
      const data = await response.json();
      applyAnalyticsScopePayload(data);
      return data;
    } catch (error) {
      if (!silent) {
        setAnalyticsScopeError(`${tr('Ошибка режима участия', 'Fehler beim Teilnahme-Modus')}: ${error.message}`);
      }
      return null;
    } finally {
      if (!silent) {
        setAnalyticsScopeLoading(false);
      }
    }
  };

  const loadWeeklySummarySocialSignal = useCallback(async () => {
    if (!initData || !weeklySummaryVisitConfig) {
      setWeeklySummarySocialSignal(null);
      return;
    }
    try {
      let scopePayload = analyticsScopeData;
      if (!scopePayload) {
        const scopeContext = buildAnalyticsScopeContextPayload();
        const scopeBody = { initData };
        if (Object.keys(scopeContext).length > 0) {
          scopeBody.scope_context = scopeContext;
        }
        const scopeResponse = await fetch('/api/webapp/analytics/scope', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(scopeBody),
        });
        if (scopeResponse.ok) {
          scopePayload = await scopeResponse.json();
          applyAnalyticsScopePayload(scopePayload);
        }
      }
      const resolvedScopeKey = normalizeAnalyticsScopeKeyFromPayload(scopePayload || analyticsScopeData || {});
      const scope = parseAnalyticsScopeKey(resolvedScopeKey || analyticsScopeKey);
      if (scope.scope_kind !== 'group') {
        setWeeklySummarySocialSignal(null);
        return;
      }
      const scopeContext = buildAnalyticsScopeContextPayload();
      const body = {
        initData,
        period: 'week',
        start_date: weeklySummaryVisitConfig.currentPeriod.startDate,
        end_date: weeklySummaryVisitConfig.currentPeriod.endDate,
        limit: 200,
        scope: scope.scope_key,
        scope_kind: scope.scope_kind,
        scope_chat_id: scope.scope_chat_id,
      };
      if (Object.keys(scopeContext).length > 0) {
        body.scope_context = scopeContext;
      }
      const response = await fetch('/api/webapp/analytics/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки social signal', 'Fehler beim Laden des Social Signals'));
      }
      const data = await response.json();
      const items = Array.isArray(data?.items) ? data.items : [];
      const selfRank = Math.max(0, Number(data?.self?.rank || 0));
      const effectiveScopeKind = String(data?.scope?.scope_kind || scope.scope_kind || '').trim().toLowerCase();
      if (effectiveScopeKind !== 'group' || items.length <= 1 || selfRank <= 0) {
        setWeeklySummarySocialSignal(null);
        return;
      }
      const total = items.length;
      const percentile = total > 1 ? Math.round(((total - selfRank) / (total - 1)) * 100) : 0;
      let text = tr(`Твоё место в группе: #${selfRank}`, `Dein Platz in der Gruppe: #${selfRank}`);
      if (selfRank > 3 && percentile > 0) {
        text = tr(`Ты выше ${percentile}% участников`, `Du liegst vor ${percentile}% der Teilnehmenden`);
      }
      setWeeklySummarySocialSignal({ text });
    } catch (_error) {
      setWeeklySummarySocialSignal(null);
    }
  }, [
    analyticsScopeData,
    analyticsScopeKey,
    initData,
    readApiError,
    tr,
    weeklySummaryVisitConfig,
  ]);

  const handleAnalyticsScopeSelect = async (nextScopeRaw) => {
    if (!initData) {
      setAnalyticsScopeError(initDataMissingMsg);
      return;
    }
    const nextScope = parseAnalyticsScopeKey(nextScopeRaw);
    if (nextScope.scope_key === analyticsScopeKey) {
      return;
    }
    const previousScopeKey = analyticsScopeKey;
    setAnalyticsScopeKey(nextScope.scope_key);
    setAnalyticsScopeSaving(true);
    setAnalyticsScopeError('');
    try {
      const scopeContext = buildAnalyticsScopeContextPayload();
      const body = {
        initData,
        scope_kind: nextScope.scope_kind,
        scope_chat_id: nextScope.scope_chat_id,
        scope: nextScope.scope_key,
      };
      if (Object.keys(scopeContext).length > 0) {
        body.scope_context = scopeContext;
      }
      const response = await fetch('/api/webapp/analytics/scope/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка сохранения режима участия', 'Fehler beim Speichern des Teilnahme-Modus'));
      }
      await loadAnalyticsScope({ silent: true });
    } catch (error) {
      setAnalyticsScopeKey(previousScopeKey);
      setAnalyticsScopeError(`${tr('Ошибка режима участия', 'Fehler beim Teilnahme-Modus')}: ${error.message}`);
    } finally {
      setAnalyticsScopeSaving(false);
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

  const loadAnalytics = async (overridePeriod, overrideScopeKey) => {
    if (!initData) {
      setAnalyticsError(initDataMissingMsg);
      return;
    }
    const period = overridePeriod || analyticsPeriod;
    const granularity = resolveAnalyticsGranularity(period);
    const scope = parseAnalyticsScopeKey(overrideScopeKey || analyticsScopeKey);
    const scopeContext = buildAnalyticsScopeContextPayload();
    const payloadBase = {
      initData,
      period,
      scope: scope.scope_key,
      scope_kind: scope.scope_kind,
      scope_chat_id: scope.scope_chat_id,
    };
    if (Object.keys(scopeContext).length > 0) {
      payloadBase.scope_context = scopeContext;
    }
    setAnalyticsLoading(true);
    setAnalyticsError('');
    try {
      const summaryResponse = await fetch('/api/webapp/analytics/summary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payloadBase),
      });
      if (!summaryResponse.ok) {
        throw new Error(await readApiError(summaryResponse, 'Ошибка загрузки аналитики', 'Fehler beim Laden der Analytik'));
      }
      const summaryData = await summaryResponse.json();
      setAnalyticsSummary(summaryData.summary || null);

      const seriesResponse = await fetch('/api/webapp/analytics/timeseries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...payloadBase, granularity }),
      });
      if (!seriesResponse.ok) {
        throw new Error(await readApiError(seriesResponse, 'Ошибка загрузки динамики', 'Fehler beim Laden des Verlaufs'));
      }
      const seriesData = await seriesResponse.json();
      setAnalyticsPoints(seriesData.points || []);

      const compareResponse = await fetch('/api/webapp/analytics/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...payloadBase, limit: 8 }),
      });
      if (!compareResponse.ok) {
        throw new Error(await readApiError(compareResponse, 'Ошибка загрузки сравнения', 'Fehler beim Laden des Vergleichs'));
      }
      const compareData = await compareResponse.json();
      setAnalyticsCompare(compareData.items || []);
      setAnalyticsRank(compareData.self?.rank ?? null);
    } catch (error) {
      setAnalyticsError(`${tr('Ошибка аналитики', 'Analytikfehler')}: ${error.message}`);
    } finally {
      setAnalyticsLoading(false);
    }
  };

  const loadEconomics = async (overridePeriod, overrideAllocation) => {
    if (!initData) {
      setEconomicsError(initDataMissingMsg);
      return;
    }
    const period = overridePeriod || economicsPeriod;
    const allocation = overrideAllocation || economicsAllocation;
    setEconomicsLoading(true);
    setEconomicsError('');
    try {
      const response = await fetch(
        `/api/economics/summary?initData=${encodeURIComponent(initData)}&period=${encodeURIComponent(period)}&allocation=${encodeURIComponent(allocation)}&sync_fixed=1`,
      );
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки экономики', 'Fehler beim Laden der Kosten'));
      }
      const data = await response.json();
      setEconomicsSummary(data?.summary || null);
    } catch (error) {
      setEconomicsError(`${tr('Ошибка экономики', 'Kostenfehler')}: ${error.message}`);
    } finally {
      setEconomicsLoading(false);
    }
  };

  const loadBillingStatus = async () => {
    if (!initData) {
      setBillingStatusError(initDataMissingMsg);
      return;
    }
    setBillingStatusLoading(true);
    setBillingStatusError('');
    try {
      const response = await fetch('/api/billing/status', {
        headers: { 'X-Telegram-InitData': initData },
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки статуса подписки', 'Fehler beim Laden des Abo-Status'));
      }
      const data = await response.json();
      setBillingStatus(data || null);
    } catch (error) {
      setBillingStatusError(`${tr('Ошибка подписки', 'Abo-Fehler')}: ${error.message}`);
    } finally {
      setBillingStatusLoading(false);
    }
  };

  const loadBillingPlans = async () => {
    setBillingPlansLoading(true);
    setBillingPlansError('');
    try {
      const response = await fetch(`/api/billing/plans?ts=${Date.now()}`, {
        cache: 'no-store',
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки тарифов', 'Fehler beim Laden der Tarife'));
      }
      const data = await response.json();
      const normalizedPlans = Array.isArray(data?.plans)
        ? data.plans.map((plan) => ({
          ...plan,
          plan_code: String(plan?.plan_code || '').trim().toLowerCase(),
          stripe_price_id: String(plan?.stripe_price_id || '').trim() || null,
        }))
        : [];
      setBillingPlans(normalizedPlans);
    } catch (error) {
      setBillingPlansError(`${tr('Ошибка тарифов', 'Tarif-Fehler')}: ${error.message}`);
    } finally {
      setBillingPlansLoading(false);
    }
  };

  const openBillingUrl = (url) => {
    const target = String(url || '').trim();
    if (!target) return;
    if (telegramApp?.openLink) {
      try {
        telegramApp.openLink(target);
        return;
      } catch (_error) {
        // fall through to browser redirect
      }
    }
    window.location.href = target;
  };

  const handleBillingUpgrade = async (planCode) => {
    if (!initData) {
      setBillingStatusError(initDataMissingMsg);
      return;
    }
    setBillingActionLoading(true);
    try {
      const response = await fetch('/api/billing/create-checkout-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, plan_code: planCode }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка создания checkout', 'Fehler beim Erstellen von Checkout'));
      }
      const data = await response.json();
      const url = String(data?.url || '').trim();
      if (!url) {
        throw new Error(tr('Сервер не вернул URL checkout', 'Server hat keine Checkout-URL geliefert'));
      }
      openBillingUrl(url);
    } catch (error) {
      setBillingStatusError(`${tr('Ошибка оплаты', 'Zahlungsfehler')}: ${error.message}`);
    } finally {
      setBillingActionLoading(false);
    }
  };

  const handleBillingManage = async () => {
    if (!initData) {
      setBillingStatusError(initDataMissingMsg);
      return;
    }
    setBillingActionLoading(true);
    try {
      const response = await fetch('/api/billing/create-portal-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка открытия портала', 'Fehler beim Oeffnen des Portals'));
      }
      const data = await response.json();
      const url = String(data?.url || '').trim();
      if (!url) {
        throw new Error(tr('Сервер не вернул URL портала', 'Server hat keine Portal-URL geliefert'));
      }
      openBillingUrl(url);
    } catch (error) {
      setBillingStatusError(`${tr('Ошибка управления подпиской', 'Fehler bei der Abo-Verwaltung')}: ${error.message}`);
    } finally {
      setBillingActionLoading(false);
    }
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('analytics')) {
      loadAnalyticsScope();
    }
  }, [initData, isWebAppMode, selectedSections, flashcardsOnly, webappChatType]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('analytics')) {
      loadAnalytics(undefined, analyticsScopeKey);
    }
  }, [initData, isWebAppMode, analyticsPeriod, analyticsScopeKey, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (canViewEconomics && !flashcardsOnly && isSectionVisible('economics')) {
      loadEconomics();
    }
  }, [initData, isWebAppMode, canViewEconomics, economicsPeriod, economicsAllocation, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('subscription')) {
      loadBillingStatus();
      loadBillingPlans();
    }
  }, [initData, isWebAppMode, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!weeklySummaryModalOpen || !weeklySummaryVisitConfig) {
      setWeeklySummarySocialSignal(null);
      return;
    }
    void loadWeeklySummarySocialSignal();
  }, [loadWeeklySummarySocialSignal, weeklySummaryModalOpen, weeklySummaryVisitConfig]);

  useEffect(() => {
    if (canViewEconomics) return;
    setSelectedSections((prev) => {
      if (!prev.has('economics')) return prev;
      const next = new Set(prev);
      next.delete('economics');
      return next;
    });
  }, [canViewEconomics]);

  useEffect(() => {
    if (!billingPlanDetailsOpenFor) return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setBillingPlanDetailsOpenFor('');
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [billingPlanDetailsOpenFor]);

  useEffect(() => {
    if (!analyticsTrendRef.current) {
      return;
    }
    const chart = echarts.init(analyticsTrendRef.current);
    const isLightTheme = themeMode === 'light';
    const chartTextColor = isLightTheme ? '#5a4a39' : '#c7d2f1';
    const chartLegendColor = isLightTheme ? '#2f271f' : '#dbe7ff';
    const chartAxisColor = isLightTheme ? 'rgba(130, 101, 67, 0.5)' : '#2f3f5f';
    const chartSplitLineColor = isLightTheme ? 'rgba(130, 101, 67, 0.18)' : 'rgba(255,255,255,0.08)';
    const tooltipBackground = isLightTheme ? 'rgba(255, 248, 238, 0.96)' : 'rgba(15, 23, 42, 0.92)';
    const tooltipBorder = isLightTheme ? 'rgba(171, 126, 72, 0.5)' : 'rgba(148, 163, 184, 0.28)';
    const tooltipTextColor = isLightTheme ? '#1f1a14' : '#e2e8f0';
    const labels = analyticsPoints.map((item) => item.period_start);
    const success = analyticsPoints.map((item) => item.successful_translations || 0);
    const fail = analyticsPoints.map((item) => item.unsuccessful_translations || 0);
    const avgScore = analyticsPoints.map((item) => item.avg_score || 0);
    const avgTime = analyticsPoints.map((item) => item.avg_time_min || 0);

    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow',
          shadowStyle: {
            color: isLightTheme ? 'rgba(207, 157, 99, 0.16)' : 'rgba(148, 163, 184, 0.14)',
          },
        },
        backgroundColor: tooltipBackground,
        borderColor: tooltipBorder,
        borderWidth: 1,
        textStyle: { color: tooltipTextColor },
        formatter: (params) => {
          const map = {};
          params.forEach((entry) => {
            map[entry.seriesName] = entry.value;
          });
          const index = params[0]?.dataIndex ?? 0;
          const timeValue = avgTime[index] ?? 0;
          return `
            <strong>${labels[index] || ''}</strong><br/>
            ${tr('Успешно', 'Erfolgreich')}: ${map[tr('Успешно', 'Erfolgreich')] ?? 0}<br/>
            ${tr('Ошибки', 'Fehler')}: ${map[tr('Нужно доработать', 'Verbessern')] ?? 0}<br/>
            ${tr('Ср. балл', 'Durchschnitt')}: ${map[tr('Средний балл', 'Durchschnitt')] ?? 0}<br/>
            ${tr('Ср. время', 'Durchschn. Zeit')}: ${timeValue} ${tr('мин', 'Min')}
          `;
        },
      },
      legend: {
        data: [tr('Успешно', 'Erfolgreich'), tr('Нужно доработать', 'Verbessern'), tr('Средний балл', 'Durchschnitt')],
        textStyle: { color: chartLegendColor },
      },
      grid: { left: 32, right: 32, top: 40, bottom: 40 },
      xAxis: {
        type: 'category',
        data: labels,
        axisLine: { lineStyle: { color: chartAxisColor } },
        axisLabel: { color: chartTextColor },
      },
      yAxis: [
        {
          type: 'value',
          name: tr('Переводы', 'Uebersetzungen'),
          nameTextStyle: { color: chartTextColor },
          axisLabel: { color: chartTextColor },
          splitLine: { lineStyle: { color: chartSplitLineColor } },
        },
        {
          type: 'value',
          name: tr('Баллы', 'Punkte'),
          min: 0,
          max: 100,
          nameTextStyle: { color: chartTextColor },
          axisLabel: { color: chartTextColor },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: tr('Успешно', 'Erfolgreich'),
          type: 'bar',
          stack: 'total',
          data: success,
          itemStyle: { color: '#06d6a0' },
          barWidth: 22,
        },
        {
          name: tr('Нужно доработать', 'Verbessern'),
          type: 'bar',
          stack: 'total',
          data: fail,
          itemStyle: { color: '#ff6b6b' },
          barWidth: 22,
        },
        {
          name: tr('Средний балл', 'Durchschnitt'),
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
  }, [analyticsPoints, analyticsPeriod, themeMode]);

  useEffect(() => {
    if (!analyticsCompareRef.current) {
      return;
    }
    const chart = echarts.init(analyticsCompareRef.current);
    const isLightTheme = themeMode === 'light';
    const chartTextColor = isLightTheme ? '#5a4a39' : '#c7d2f1';
    const chartSplitLineColor = isLightTheme ? 'rgba(130, 101, 67, 0.18)' : 'rgba(255,255,255,0.08)';
    const tooltipBackground = isLightTheme ? 'rgba(255, 248, 238, 0.96)' : 'rgba(15, 23, 42, 0.92)';
    const tooltipBorder = isLightTheme ? 'rgba(171, 126, 72, 0.5)' : 'rgba(148, 163, 184, 0.28)';
    const tooltipTextColor = isLightTheme ? '#1f1a14' : '#e2e8f0';
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
        backgroundColor: tooltipBackground,
        borderColor: tooltipBorder,
        borderWidth: 1,
        textStyle: { color: tooltipTextColor },
        formatter: (params) => {
          const item = analyticsCompare[params.dataIndex];
          if (!item) return '';
          return `
            <strong>${item.username}</strong><br/>
            ${tr('Итоговый балл', 'Gesamtscore')}: ${item.final_score}<br/>
            ${tr('Успех', 'Erfolg')}: ${item.success_rate}%<br/>
            ${tr('Ср. балл', 'Durchschnitt')}: ${item.avg_score}<br/>
            ${tr('Переводы', 'Uebersetzungen')}: ${item.total_translations}<br/>
            ${tr('Пропущено', 'Verpasst')}: ${item.missed_sentences}<br/>
            ${tr('Пропущено дней', 'Verpasste Tage')}: ${item.missed_days ?? 0}
          `;
        },
      },
      grid: { left: 20, right: 20, top: 20, bottom: 20, containLabel: true },
      xAxis: {
        type: 'value',
        axisLabel: { color: chartTextColor },
        splitLine: { lineStyle: { color: chartSplitLineColor } },
      },
      yAxis: {
        type: 'category',
        data: names,
        axisLabel: { color: chartTextColor },
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
  }, [analyticsCompare, webappUser, themeMode]);

  if (isWebAppMode) {
    if (singleInstanceBlocked) {
      return (
        <div className={`webapp-page ${themeMode === 'light' ? 'is-theme-light' : ''}`}>
          <div className="webapp-card">
            <header className="webapp-header">
              <span className="pill">Telegram Web App</span>
              <h1>{tr('Уже открыто в другом окне', 'Bereits in einem anderen Fenster offen')}</h1>
              <p>
                {tr(
                  'Сейчас активна другая копия приложения. Это защита от параллельных сессий и дублирования действий.',
                  'Aktuell ist bereits eine andere App-Kopie aktiv. Das ist ein Schutz gegen parallele Sessions und doppelte Aktionen.'
                )}
              </p>
            </header>
            <div className="webapp-actions">
              <button type="button" className="primary-button" onClick={forceSingleInstanceTakeover}>
                {tr('Открыть эту копию', 'Diese Kopie öffnen')}
              </button>
              <button type="button" className="secondary-button" onClick={() => window.location.reload()}>
                {tr('Проверить снова', 'Erneut prüfen')}
              </button>
            </div>
          </div>
        </div>
      );
    }
    return (
      <div className={`webapp-page ${themeMode === 'light' ? 'is-theme-light' : ''} ${flashcardsOnly ? 'is-flashcards' : ''} ${readerHasContent && readerImmersive ? 'is-reader-immersive' : ''} ${youtubeWatchFocusMode ? 'is-youtube-watch-focus' : ''} ${telegramFullscreenMode ? 'is-telegram-fullscreen' : ''} ${telegramTabletLike ? 'is-telegram-tablet' : ''} ${needsContainedWebappScroll ? 'is-contained-scroll' : ''} ${isAndroidTelegramClient ? 'is-android-client' : ''} ${isGuideScreen ? 'is-guide-screen' : ''} ${!flashcardsOnly && dictionarySectionVisible ? 'is-dictionary-layout' : ''}`}>
        <div className="webapp-shell">
          <aside className="webapp-sidebar">
            <div className="webapp-brand">
              <div className="brand-mark">DF</div>
                <div>
                  <div className="brand-title">DeutschFlow</div>
                  <div className="brand-subtitle">{t('brand_subtitle')}</div>
                </div>
              </div>
            <div className="webapp-sidebar-top-controls">
              <div className="language-toggle-wrap">
                <span className="language-toggle-label">{t('language_toggle_label')}</span>
                <button type="button" className="language-toggle" onClick={toggleLanguage} aria-label={t('language_toggle_label')}>
                  <span className={`language-chip ${uiLang === 'ru' ? 'is-active' : ''}`}>{t('language_ru')}</span>
                  <span className={`language-chip ${uiLang === 'de' ? 'is-active' : ''}`}>{t('language_de')}</span>
                </button>
              </div>
              <div className="language-toggle-wrap">
                <span className="language-toggle-label">{tr('Тема', 'Thema')}</span>
                <button
                  type="button"
                  className="language-toggle theme-toggle-compact theme-toggle-sidebar"
                  onClick={toggleThemeMode}
                  title={themeMode === 'light' ? tr('Светлая тема', 'Helles Thema') : tr('Тёмная тема', 'Dunkles Thema')}
                  aria-label={tr('Переключить тему', 'Theme wechseln')}
                >
                  <span className={`language-chip theme-chip ${themeMode === 'dark' ? 'is-active' : ''}`}>DARK</span>
                  <span className={`language-chip theme-chip ${themeMode === 'light' ? 'is-active' : ''}`}>LIGHT</span>
                </button>
              </div>
              <label className="menu-toggle-row">
                <input
                  type="checkbox"
                  checked={menuMultiSelect}
                  onChange={(event) => setMenuMultiSelect(event.target.checked)}
                />
                <span>{t('menu_multi_select')}</span>
              </label>
            </div>
            <div className="webapp-menu">
              <button
                type="button"
                className={`menu-item menu-item-subscription ${selectedSections.has('subscription') ? 'is-active' : ''}`}
                onClick={() => toggleSection('subscription')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-subscription">{renderMenuIcon('subscription')}</span>
                <span>{t('menu_billing')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-today ${isHomeScreen ? 'is-active' : ''}`}
                onClick={goHomeScreen}
              >
                <span className="menu-icon menu-icon-today">{renderMenuIcon('today')}</span>
                <span>{tr('Сегодня', 'Heute')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-guide ${selectedSections.has('guide') ? 'is-active' : ''}`}
                onClick={openGuideSection}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-guide">{renderMenuIcon('guide')}</span>
                <span>{t('menu_guide')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-translations ${selectedSections.has('translations') ? 'is-active' : ''}`}
                onClick={() => toggleSection('translations')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-translations">{renderMenuIcon('translations')}</span>
                <span>{t('menu_translations')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-youtube ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                onClick={() => toggleSection('youtube')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-youtube">{renderMenuIcon('youtube')}</span>
                <span>YouTube</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-movies ${selectedSections.has('movies') ? 'is-active' : ''}`}
                onClick={() => toggleSection('movies')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-movies">{renderMenuIcon('movies')}</span>
                <span>{t('menu_movies')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-dictionary ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                onClick={() => toggleSection('dictionary')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-dictionary">{renderMenuIcon('dictionary')}</span>
                <span>{t('menu_dictionary')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-reader ${selectedSections.has('reader') ? 'is-active' : ''}`}
                onClick={() => toggleSection('reader')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-reader">{renderMenuIcon('reader')}</span>
                <span>{tr('Читалка', 'Leser')}</span>
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
                <span className="menu-icon menu-icon-flashcards">{renderMenuIcon('flashcards')}</span>
                <span>{t('menu_flashcards')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-assistant ${selectedSections.has('assistant') ? 'is-active' : ''}`}
                onClick={() => toggleSection('assistant')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-assistant">{renderMenuIcon('assistant')}</span>
                <span>{t('menu_assistant')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-support ${selectedSections.has('support') ? 'is-active' : ''}`}
                onClick={() => toggleSection('support')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-support">{renderMenuIcon('support')}</span>
                <span className="menu-item-label-with-dot">
                  <span>{tr('Техподдержка', 'Support')}</span>
                  {supportUnreadCount > 0 && <span className="menu-item-unread-dot" />}
                </span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                onClick={() => toggleSection('analytics')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-analytics">{renderMenuIcon('analytics')}</span>
                <span>{t('menu_analytics')}</span>
              </button>
              {canViewEconomics && (
                <button
                  type="button"
                  className={`menu-item menu-item-economics ${selectedSections.has('economics') ? 'is-active' : ''}`}
                  onClick={() => toggleSection('economics')}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon menu-icon-economics">{renderMenuIcon('economics')}</span>
                  <span>{t('menu_economics')}</span>
                </button>
              )}
              <button
                type="button"
                className={`menu-item menu-item-skill-training ${selectedSections.has('skill_training') ? 'is-active' : ''}`}
                onClick={() => toggleSection('skill_training')}
                disabled={flashcardsOnly || (!isSkillTrainingReady && !selectedSections.has('skill_training'))}
              >
                <span className="menu-icon menu-icon-skill-training">{renderMenuIcon('skill_training')}</span>
                <span>{tr('Тренировка навыка', 'Skill-Training')}</span>
              </button>
            </div>
            <div className="webapp-menu-actions">
              <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                {t('menu_show_all')}
              </button>
              <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                {t('menu_hide_all')}
              </button>
            </div>
            {flashcardsOnly && (
              <div className="webapp-menu-note">{t('review_mode_active')}</div>
            )}
          </aside>

          <div className="webapp-main">
            <div className="webapp-topbar">
              <div className="topbar-row topbar-row-main">
                <button
                  type="button"
                  className="menu-toggle"
                  onClick={() => setMenuOpen(true)}
                >
                  <span />
                  <span />
                  <span />
                </button>
                <div className="topbar-title">Das Deutsche Schlümpfchen</div>
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
                </div>
              </div>
              <div className="topbar-row topbar-row-controls">
                <div className="topbar-controls">
                  {telegramTabletLike && !telegramFullscreenMode && typeof telegramApp?.requestFullscreen === 'function' && (
                    <button
                      type="button"
                      className="telegram-fullscreen-button"
                      onClick={requestTelegramFullscreen}
                      title={tr('Развернуть на весь экран', 'Vollbild aktivieren')}
                      aria-label={tr('Развернуть на весь экран', 'Vollbild aktivieren')}
                    >
                      ⤢
                    </button>
                  )}
                  <button type="button" className="language-toggle language-toggle-compact" onClick={toggleLanguage} aria-label={t('language_toggle_label')}>
                    <span className={`language-chip ${uiLang === 'ru' ? 'is-active' : ''}`}>{t('language_ru')}</span>
                    <span className={`language-chip ${uiLang === 'de' ? 'is-active' : ''}`}>{t('language_de')}</span>
                  </button>
                  <button
                    type="button"
                    className="language-toggle language-toggle-compact theme-toggle-compact"
                    onClick={toggleThemeMode}
                    title={themeMode === 'light' ? tr('Светлая тема', 'Helles Thema') : tr('Тёмная тема', 'Dunkles Thema')}
                    aria-label={tr('Переключить тему', 'Theme wechseln')}
                  >
                    <span className={`language-chip theme-chip ${themeMode === 'dark' ? 'is-active' : ''}`}>DARK</span>
                    <span className={`language-chip theme-chip ${themeMode === 'light' ? 'is-active' : ''}`}>LIGHT</span>
                  </button>
                  <button
                    type="button"
                    className="language-pair-button"
                    onClick={openLanguageProfileModal}
                    title={tr('Изменить языковую пару обучения', 'Sprachpaar aendern')}
                  >
                    {getActiveLanguagePairLabel()}
                  </button>
                  <button
                    type="button"
                    className="topbar-help-button"
                    onClick={openGuideSection}
                    title={t('guide_topbar_button')}
                    aria-label={t('guide_topbar_button')}
                  >
                    ?
                  </button>
                </div>
              </div>
            </div>

            {menuOpen && (
              <div className="webapp-overlay">
                <div className="overlay-backdrop" onClick={() => setMenuOpen(false)} />
                <div className="overlay-panel">
                  <div className="overlay-header">
                    <button
                      type="button"
                      className="overlay-back-button"
                      onClick={() => setMenuOpen(false)}
                    >
                      {tr('Back', 'Back')}
                    </button>
                    <div className="brand-title overlay-brand-title">DeutschFlow</div>
                    <button
                      type="button"
                      className="overlay-close-button"
                      onClick={() => setMenuOpen(false)}
                    >
                      {t('menu_close')}
                    </button>
                  </div>
                  <div className="overlay-menu">
                    <label className="menu-toggle-row">
                      <input
                        type="checkbox"
                        checked={menuMultiSelect}
                        onChange={(event) => setMenuMultiSelect(event.target.checked)}
                      />
                      <span>{t('menu_multi_select')}</span>
                    </label>
                    <button
                      type="button"
                      className={`menu-item menu-item-subscription ${selectedSections.has('subscription') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('subscription', billingRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-subscription">{renderMenuIcon('subscription')}</span>
                      <span>{t('menu_billing')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-today ${isHomeScreen ? 'is-active' : ''}`}
                      onClick={() => {
                        goHomeScreen();
                        setMenuOpen(false);
                      }}
                    >
                      <span className="menu-icon menu-icon-today">{renderMenuIcon('today')}</span>
                      <span>{tr('Сегодня', 'Heute')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-guide ${selectedSections.has('guide') ? 'is-active' : ''}`}
                      onClick={openGuideSection}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-guide">{renderMenuIcon('guide')}</span>
                      <span>{t('menu_guide')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-translations ${selectedSections.has('translations') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('translations', translationsRef)}
                      disabled={flashcardsOnly}
                    >
                    <span className="menu-icon menu-icon-translations">{renderMenuIcon('translations')}</span>
                    <span>{t('menu_translations')}</span>
                  </button>
                <button
                  type="button"
                  className={`menu-item menu-item-youtube ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('youtube', youtubeRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon menu-icon-youtube">{renderMenuIcon('youtube')}</span>
                  <span>YouTube</span>
                </button>
                <button
                  type="button"
                  className={`menu-item menu-item-movies ${selectedSections.has('movies') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('movies', moviesRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon menu-icon-movies">{renderMenuIcon('movies')}</span>
                  <span>{t('menu_movies')}</span>
                </button>
                <button
                  type="button"
                  className={`menu-item menu-item-dictionary ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                  onClick={() => handleMenuSelection('dictionary', dictionaryRef)}
                  disabled={flashcardsOnly}
                >
                  <span className="menu-icon menu-icon-dictionary">{renderMenuIcon('dictionary')}</span>
                      <span>{t('menu_dictionary')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-reader ${selectedSections.has('reader') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('reader', readerRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-reader">{renderMenuIcon('reader')}</span>
                      <span>{tr('Читалка', 'Leser')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-flashcards ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('flashcards', flashcardsRef)}
                    >
                      <span className="menu-icon menu-icon-flashcards">{renderMenuIcon('flashcards')}</span>
                      <span>{t('menu_flashcards')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-assistant ${selectedSections.has('assistant') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('assistant', assistantRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-assistant">{renderMenuIcon('assistant')}</span>
                      <span>{t('menu_assistant')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-support ${selectedSections.has('support') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('support', supportRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-support">{renderMenuIcon('support')}</span>
                      <span className="menu-item-label-with-dot">
                        <span>{tr('Техподдержка', 'Support')}</span>
                        {supportUnreadCount > 0 && <span className="menu-item-unread-dot" />}
                      </span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('analytics', analyticsRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-analytics">{renderMenuIcon('analytics')}</span>
                      <span>{t('menu_analytics')}</span>
                    </button>
                    {canViewEconomics && (
                      <button
                        type="button"
                        className={`menu-item menu-item-economics ${selectedSections.has('economics') ? 'is-active' : ''}`}
                        onClick={() => handleMenuSelection('economics', economicsRef)}
                        disabled={flashcardsOnly}
                      >
                        <span className="menu-icon menu-icon-economics">{renderMenuIcon('economics')}</span>
                        <span>{t('menu_economics')}</span>
                      </button>
                    )}
                    <button
                      type="button"
                      className={`menu-item menu-item-skill-training ${selectedSections.has('skill_training') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('skill_training', skillTrainingRef)}
                      disabled={flashcardsOnly || (!isSkillTrainingReady && !selectedSections.has('skill_training'))}
                    >
                      <span className="menu-icon menu-icon-skill-training">{renderMenuIcon('skill_training')}</span>
                      <span>{tr('Тренировка навыка', 'Skill-Training')}</span>
                    </button>
                  </div>
                  <div className="overlay-actions">
                    <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                      {t('menu_show_all')}
                    </button>
                    <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                      {t('menu_hide_all')}
                    </button>
                  </div>
                </div>
              </div>
            )}

            {onboardingOpen && onboardingSlides[onboardingStep] && (
              <div className="onboarding-overlay" role="dialog" aria-modal="true" aria-label={tr('Быстрый старт', 'Schnellstart')}>
                <button
                  type="button"
                  className="onboarding-backdrop"
                  aria-label={tr('Пропустить быстрый старт', 'Schnellstart ueberspringen')}
                  onClick={dismissOnboarding}
                />
                <div className="onboarding-modal">
                  <div className="onboarding-modal-head">
                    <div>
                      <div className="onboarding-eyebrow">{onboardingSlides[onboardingStep].eyebrow}</div>
                      <h3>{onboardingSlides[onboardingStep].title}</h3>
                    </div>
                    <button
                      type="button"
                      className="secondary-button onboarding-close"
                      onClick={dismissOnboarding}
                    >
                      {tr('Пропустить', 'Ueberspringen')}
                    </button>
                  </div>
                  <div className="onboarding-modal-body">
                    <img src={heroStickerSrc} alt="" aria-hidden="true" className="onboarding-mascot" />
                    <p>{onboardingSlides[onboardingStep].body}</p>
                    <div className="onboarding-bullets">
                      {onboardingSlides[onboardingStep].bullets.map((item, index) => (
                        <div key={`onboarding-bullet-${onboardingStep}-${index}`} className="onboarding-bullet">
                          <span className="onboarding-bullet-mark">•</span>
                          <span>{item}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="onboarding-progress" aria-hidden="true">
                    {onboardingSlides.map((_, index) => (
                      <span
                        key={`onboarding-dot-${index}`}
                        className={`onboarding-dot ${index === onboardingStep ? 'is-active' : ''}`}
                      />
                    ))}
                  </div>
                  <div className="onboarding-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setOnboardingStep((prev) => Math.max(0, prev - 1))}
                      disabled={onboardingStep === 0}
                    >
                      {tr('Назад', 'Zurueck')}
                    </button>
                    {onboardingStep < onboardingSlides.length - 1 ? (
                      <button
                        type="button"
                        className="primary-button"
                        onClick={() => setOnboardingStep((prev) => Math.min(onboardingSlides.length - 1, prev + 1))}
                      >
                        {tr('Далее', 'Weiter')}
                      </button>
                    ) : (
                      <>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => finishOnboarding('guide')}
                        >
                          {tr('Открыть гид', 'Guide oeffnen')}
                        </button>
                        <button
                          type="button"
                          className="primary-button"
                          onClick={() => finishOnboarding('translations')}
                        >
                          {tr('Начать с переводов', 'Mit Uebersetzungen starten')}
                        </button>
                      </>
                    )}
                  </div>
                </div>
              </div>
            )}

            {weeklySummaryVisitConfig && !onboardingOpen && (
              <WeeklySummaryModal
                isOpen={weeklySummaryModalOpen}
                title={weeklySummaryVisitConfig.title}
                subtitle={weeklySummaryVisitConfig.subtitle}
                closeLabel={tr('Закрыть weekly summary', 'Weekly Summary schliessen')}
                openAnalyticsLabel={tr('Открыть аналитику', 'Analytik oeffnen')}
                onClose={dismissWeeklySummaryModal}
                onOpenAnalytics={openAnalyticsFromWeeklySummary}
              >
                <section className="weekly-summary-hero" aria-live="polite">
                  <div className="weekly-summary-hero-label">
                    {tr('Краткий итог', 'Kurzfazit')}
                  </div>
                  {weeklySummaryHeroLoading ? (
                    <div className="weekly-summary-hero-loading">
                      {tr('Собираю краткую сводку по неделе...', 'Ich erstelle gerade die Wochenkurzzusammenfassung...')}
                    </div>
                  ) : weeklySummaryHeroError ? (
                    <div className="weekly-summary-hero-error">{weeklySummaryHeroError}</div>
                  ) : weeklySummaryHeroLines.length === 0 ? (
                    <div className="weekly-summary-hero-loading">
                      {tr('Пока нет данных для краткого weekly summary.', 'Es gibt noch keine Daten fuer diese kurze Weekly Summary.')}
                    </div>
                  ) : (
                    <div className="weekly-summary-hero-lines">
                      {weeklySummaryHeroLines.map((line, index) => (
                        <p key={`weekly-summary-hero-line-${index}`}>{line}</p>
                      ))}
                    </div>
                  )}
                </section>
                <section className="weekly-summary-kpi" aria-label={tr('Ключевые показатели', 'Kernmetriken')}>
                  <div className="weekly-summary-hero-label">
                    {tr('Ключевые показатели', 'Kernmetriken')}
                  </div>
                  {weeklySummaryHeroLoading && !weeklySummaryCurrentMetrics ? (
                    <div className="weekly-summary-kpi-empty">
                      {tr('Загружаю KPI для weekly summary...', 'Ich lade die KPI fuer die Weekly Summary...')}
                    </div>
                  ) : weeklySummaryHeroError ? (
                    <div className="weekly-summary-kpi-empty">{weeklySummaryHeroError}</div>
                  ) : (
                    <div className="weekly-summary-kpi-grid">
                      {weeklySummaryKpiCards.map((card) => (
                        <article key={`weekly-summary-kpi-${card.key}`} className="weekly-summary-kpi-card">
                          <span>{card.title}</span>
                          <strong>{card.actualLabel} / {card.previousLabel}</strong>
                          <small className={`weekly-summary-kpi-delta ${card.deltaClass}`}>
                            {card.deltaLabel} {tr('vs прошлый период', 'vs letzter Zeitraum')}
                          </small>
                        </article>
                      ))}
                    </div>
                  )}
                </section>
                <section className="weekly-summary-compare" aria-label={tr('Сравнение с прошлым периодом', 'Vergleich mit dem letzten Zeitraum')}>
                  <div className="weekly-summary-hero-label">
                    {weeklySummaryVisitConfig.comparisonLabel || tr('Сравнение с прошлым периодом', 'Vergleich mit dem letzten Zeitraum')}
                  </div>
                  {weeklySummaryHeroLoading && !weeklySummaryCurrentMetrics ? (
                    <div className="weekly-summary-kpi-empty">
                      {tr('Загружаю сравнение периодов...', 'Ich lade den Periodenvergleich...')}
                    </div>
                  ) : weeklySummaryHeroError ? (
                    <div className="weekly-summary-kpi-empty">{weeklySummaryHeroError}</div>
                  ) : (
                    <div className="weekly-summary-compare-rows">
                      {weeklySummaryComparisonRows.map((row) => (
                        <div key={`weekly-summary-compare-${row.key}`} className="weekly-summary-compare-row">
                          <div className="weekly-summary-compare-topline">
                            <span className="weekly-summary-compare-title">{row.title}</span>
                            <span className="weekly-summary-compare-values">
                              {row.currentLabel} / {row.previousLabel} {row.unit}
                            </span>
                          </div>
                          <div className="weekly-summary-compare-bars" aria-hidden="true">
                            <div className="weekly-summary-compare-bar-row">
                              <div className="weekly-summary-compare-bar-meta">
                                <span className="weekly-summary-compare-bar-label">
                                  <span className="weekly-summary-compare-dot is-current" />
                                  {tr('Сейчас', 'Aktuell')}
                                </span>
                                <span className="weekly-summary-compare-bar-value">
                                  {row.currentLabel} {row.unit}
                                </span>
                              </div>
                              <div className="weekly-summary-compare-track">
                                <div
                                  className="weekly-summary-compare-fill is-current"
                                  style={{ width: row.currentWidth }}
                                />
                              </div>
                            </div>
                            <div className="weekly-summary-compare-bar-row">
                              <div className="weekly-summary-compare-bar-meta">
                                <span className="weekly-summary-compare-bar-label">
                                  <span className="weekly-summary-compare-dot is-previous" />
                                  {tr('Прошлый период', 'Letzter Zeitraum')}
                                </span>
                                <span className="weekly-summary-compare-bar-value">
                                  {row.previousLabel} {row.unit}
                                </span>
                              </div>
                              <div className="weekly-summary-compare-track">
                                <div
                                  className="weekly-summary-compare-fill is-previous"
                                  style={{ width: row.previousWidth }}
                                />
                              </div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </section>
                {weeklySummarySocialSignal?.text && (
                  <section className="weekly-summary-social" aria-label={tr('Позиция в группе', 'Gruppensignal')}>
                    <div className="weekly-summary-social-chip">
                      {weeklySummarySocialSignal.text}
                    </div>
                  </section>
                )}
                <section className="weekly-summary-recommendation" aria-label={tr('Следующий шаг', 'Naechster Schritt')}>
                  <div className="weekly-summary-hero-label">
                    {tr('Следующий шаг', 'Naechster Schritt')}
                  </div>
                  {weeklySummaryHeroLoading && !weeklySummaryRecommendation ? (
                    <div className="weekly-summary-kpi-empty">
                      {tr('Готовлю короткую рекомендацию...', 'Ich bereite eine kurze Empfehlung vor...')}
                    </div>
                  ) : weeklySummaryHeroError ? (
                    <div className="weekly-summary-kpi-empty">{weeklySummaryHeroError}</div>
                  ) : weeklySummaryRecommendation ? (
                    <div className="weekly-summary-recommendation-card">
                      <p>{weeklySummaryRecommendation.leadLine}</p>
                      <p>{weeklySummaryRecommendation.lagLine}</p>
                      <p>{weeklySummaryRecommendation.nextLine}</p>
                    </div>
                  ) : (
                    <div className="weekly-summary-kpi-empty">
                      {tr('Пока нет данных для рекомендации.', 'Es gibt noch keine Daten fuer eine Empfehlung.')}
                    </div>
                  )}
                </section>
              </WeeklySummaryModal>
            )}

            {showHero && (
            <header className="webapp-hero">
              <div className="webapp-hero-copy webapp-hero-copy-landing">
                <span className="pill">Telegram Web App</span>
                <h1>{t('hero_title')}</h1>
              </div>
              <div className="webapp-hero-mascot-flat" aria-hidden="true">
                <img src={heroStickerSrc} alt="Deutsch mascot" className="hero-flat-image" />
              </div>
            </header>
            )}

            {showHero && (
            <section className="webapp-hero-cards">
              <div className="hero-card">
                <div className="hero-card-head is-translate">{t('card_translate_title')}</div>
                <p>{t('card_translate_body')}</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-save">{t('card_save_title')}</div>
                <p>{t('card_save_body')}</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-train">{t('card_train_title')}</div>
                <p>{t('card_train_body')}</p>
              </div>
              <div className="hero-card">
                <div className="hero-card-head is-watch">{t('card_watch_title')}</div>
                <p>{t('card_watch_body')}</p>
              </div>
            </section>
            )}

            {showHomeGuideQuickCard && (
              <section className="hero-guide-card">
                <div className="hero-guide-card-copy">
                  <div className="hero-guide-card-title">{t('guide_quick_title')}</div>
                  <p>{t('guide_quick_body')}</p>
                </div>
                <div className="hero-guide-card-actions">
                  <button type="button" className="primary-button" onClick={openGuideSection}>
                    {t('guide_open_button')}
                  </button>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => {
                      setGuideQuickCardDismissed(true);
                      safeStorageSet(guideQuickCardStorageKey, '1');
                    }}
                  >
                    {tr('Скрыть', 'Ausblenden')}
                  </button>
                </div>
              </section>
            )}

            

            {!telegramApp?.initData && isHomeScreen && (
              <section className={`webapp-browser-auth ${initData ? 'is-compact' : ''}`}>
                <div className="webapp-browser-auth-head">
                  <strong>{t('browser_login_title')}</strong>
                  {initData ? (
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={handleBrowserLogout}
                    >
                      {t('logout')}
                    </button>
                  ) : null}
                </div>
                {!initData && (
                  <>
                    <p className="webapp-muted">
                      {t('browser_login_hint')}
                    </p>
                    <div className="webapp-telegram-login-slot" ref={telegramLoginWidgetRef} />
                    {browserAuthLoading && <div className="webapp-muted">{t('auth_loading')}</div>}
                    {browserAuthError && <div className="webapp-error">{browserAuthError}</div>}
                    {!browserAuthBotUsername && (
                      <div className="webapp-error">
                        {t('bot_username_missing')}
                      </div>
                    )}
                    {ALLOW_MANUAL_INITDATA_FALLBACK && (
                      <label className="webapp-field">
                        <span>{t('init_data_fallback')}</span>
                        <textarea
                          rows={3}
                          value={initData}
                          onChange={(event) => setInitData(event.target.value)}
                          placeholder={t('init_data_placeholder')}
                        />
                      </label>
                    )}
                  </>
                )}
              </section>
            )}

            {languageProfileGateOpen && (
              <div className="language-profile-gate" role="dialog" aria-modal="true">
                <div className="language-profile-card">
                  <h3>{tr('Выберите языки для обучения', 'Waehle deine Lernsprachen')}</h3>
                  <p className="webapp-muted">
                    {tr(
                      'Укажите язык, который изучаете, и родной язык. Это нужно для переводов, словаря, карточек и аналитики.',
                      'Waehle Lernsprache und Muttersprache. Das wird fuer Uebersetzung, Woerterbuch, Karten und Analytik genutzt.'
                    )}
                  </p>
                  <div className="language-profile-fields">
                    <label className="webapp-field">
                      <span>{tr('Язык изучения', 'Lernsprache')}</span>
                      <select
                        value={languageProfileDraft.learning_language}
                        onChange={(event) => setLanguageProfileDraft((prev) => ({ ...prev, learning_language: event.target.value }))}
                        disabled={languageProfileSaving}
                      >
                        {learningLanguageOptions.map((item) => (
                          <option key={item.value} value={item.value}>{item.label}</option>
                        ))}
                      </select>
                    </label>
                    <label className="webapp-field">
                      <span>{tr('Родной язык', 'Muttersprache')}</span>
                      <select
                        value={languageProfileDraft.native_language}
                        onChange={(event) => setLanguageProfileDraft((prev) => ({ ...prev, native_language: event.target.value }))}
                        disabled={languageProfileSaving}
                      >
                        {nativeLanguageOptions.map((item) => (
                          <option key={item.value} value={item.value}>{item.label}</option>
                        ))}
                      </select>
                    </label>
                  </div>
                  {languageProfileError && <div className="webapp-error">{languageProfileError}</div>}
                  <div className="language-profile-actions">
                    <button
                      type="button"
                      className="primary-button language-profile-save-btn"
                      onClick={saveLanguageProfile}
                      disabled={languageProfileSaving}
                    >
                      {languageProfileSaving ? tr('Сохраняем...', 'Speichern...') : tr('Сохранить и продолжить', 'Speichern und fortsetzen')}
                    </button>
                    {languageProfile?.has_profile && starterDictionaryOffer?.enabled && (
                      <button
                        type="button"
                        className="secondary-button language-profile-starter-btn"
                        onClick={() => void applyStarterDictionaryDecision(true, { forceReimport: true, closePromptOnSuccess: false })}
                        disabled={languageProfileSaving || starterDictionaryActionLoading || !starterDictionaryOffer?.can_reconnect}
                      >
                        {starterDictionaryActionLoading
                          ? tr('Подключаем...', 'Wird verbunden...')
                          : !starterDictionaryOffer?.can_reconnect
                            ? tr('Базовый словарь пока пуст', 'Basiswoerterbuch ist noch leer')
                            : starterDictionaryOffer?.state?.decision_status === 'accepted'
                              ? tr('Переподключить базовый словарь', 'Basiswoerterbuch neu verbinden')
                              : tr('Подключить базовый словарь', 'Basiswoerterbuch verbinden')}
                      </button>
                    )}
                    {!needsLanguageProfileChoice && (
                      <button
                        type="button"
                        className="secondary-button language-profile-close-btn"
                        onClick={() => setLanguageProfileModalOpen(false)}
                        disabled={languageProfileSaving}
                      >
                        {tr('Закрыть', 'Schliessen')}
                      </button>
                    )}
                  </div>
                  {starterDictionaryActionError && <div className="webapp-error">{starterDictionaryActionError}</div>}
                  {starterDictionaryActionMessage && <div className="webapp-success">{starterDictionaryActionMessage}</div>}
                </div>
              </div>
            )}

            {starterDictionaryPromptOpen && !languageProfileGateOpen && starterDictionaryOffer?.enabled && (
              <div className="language-profile-gate starter-dictionary-gate" role="dialog" aria-modal="true">
                <div className="language-profile-card starter-dictionary-card">
                  <h3>{tr('Быстрый старт словаря', 'Schnellstart Woerterbuch')}</h3>
                  <p className="webapp-muted">
                    {tr(
                      `Подключить базовый словарь (${Math.max(0, Number(starterDictionaryOffer?.suggested_count || starterDictionaryOffer?.import_limit || 0))} слов/фраз) для быстрого старта карточек, quiz и выражений?`,
                      `Basiswoerterbuch (${Math.max(0, Number(starterDictionaryOffer?.suggested_count || starterDictionaryOffer?.import_limit || 0))} Woerter/Phrasen) fuer schnellen Start mit Karten, Quiz und Ausdruecken verbinden?`
                    )}
                  </p>
                  <p className="webapp-muted">
                    {tr(
                      'Это одноразовый импорт копии стартового набора. Он полезен как база на старте, а дальше словарь можно расширять уже своими словами из YouTube, Reader, переводов, словаря и лички с ботом.',
                      'Das ist ein einmaliger Import einer Starter-Kopie. Sie hilft beim Einstieg, danach kannst du dein Woerterbuch mit eigenen Woertern aus YouTube, Reader, Uebersetzungen, Woerterbuch und Bot-Chat erweitern.'
                    )}
                  </p>
                  {starterDictionaryActionError && <div className="webapp-error">{starterDictionaryActionError}</div>}
                  {starterDictionaryActionMessage && <div className="webapp-success">{starterDictionaryActionMessage}</div>}
                  <div className="language-profile-actions starter-dictionary-actions">
                    <button
                      type="button"
                      className="primary-button language-profile-save-btn"
                      onClick={() => void applyStarterDictionaryDecision(true)}
                      disabled={starterDictionaryActionLoading}
                    >
                      {starterDictionaryActionLoading
                        ? tr('Подключаем...', 'Wird verbunden...')
                        : tr('Да, подключить', 'Ja, verbinden')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button language-profile-close-btn"
                      onClick={() => void applyStarterDictionaryDecision(false)}
                      disabled={starterDictionaryActionLoading}
                    >
                      {tr('Нет, начать с нуля', 'Nein, leer starten')}
                    </button>
                  </div>
                </div>
              </div>
            )}

            {isHomeScreen && initData && (
              <section className="weekly-plan-panel">
                <div className="weekly-plan-head">
                  <div>
                    <h2>{tr('План на неделю', 'Wochenplan')}</h2>
                    <p>{tr('Личные цели и факт с прогнозом до конца недели', 'Persoenliche Ziele mit Ist-Werten und Prognose bis Wochenende')}</p>
                  </div>
                  <div className="weekly-plan-head-actions">
                    <label className="weekly-plan-period-select">
                      <span>{tr('Период', 'Zeitraum')}</span>
                      <select
                        value={planAnalyticsPeriod}
                        onChange={(event) => setPlanAnalyticsPeriod(event.target.value)}
                        disabled={planAnalyticsLoading}
                      >
                        <option value="week">{tr('Неделя', 'Woche')}</option>
                        <option value="month">{tr('Месяц', 'Monat')}</option>
                        <option value="quarter">{tr('Квартал', 'Quartal')}</option>
                        <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
                        <option value="year">{tr('Год', 'Jahr')}</option>
                      </select>
                    </label>
                    {weeklyWeekLabel && (
                      <span className="weekly-plan-period">{planPeriodLabel}: {weeklyWeekLabel}</span>
                    )}
                    <button
                      type="button"
                      className="secondary-button weekly-plan-collapse-btn"
                      onClick={() => setWeeklyPlanCollapsed((prev) => !prev)}
                    >
                      {weeklyPlanCollapsed ? tr('Развернуть', 'Aufklappen') : tr('Свернуть', 'Einklappen')}
                    </button>
                  </div>
                </div>

                {!weeklyPlanCollapsed && (
                <div className="weekly-plan-form">
                  <label className="webapp-field">
                    <span>{tr('Количество переводов', 'Anzahl Uebersetzungen')}</span>
                    <input
                      type="number"
                      min="0"
                      inputMode="numeric"
                      value={weeklyPlanDraft.translations_goal}
                      onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, translations_goal: event.target.value }))}
                      disabled={weeklyPlanSaving}
                      placeholder="0"
                    />
                  </label>
                  <label className="webapp-field">
                    <span>{tr('Количество выученных слов', 'Anzahl gelernter Woerter')}</span>
                    <input
                      type="number"
                      min="0"
                      inputMode="numeric"
                      value={weeklyPlanDraft.learned_words_goal}
                      onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, learned_words_goal: event.target.value }))}
                      disabled={weeklyPlanSaving}
                      placeholder="0"
                    />
                  </label>
                  <label className="webapp-field">
                    <span>{tr('Минуты разговора с агентом', 'Gesprächsminuten mit Assistent')}</span>
                    <input
                      type="number"
                      min="0"
                      inputMode="numeric"
                      value={weeklyPlanDraft.agent_minutes_goal}
                      onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, agent_minutes_goal: event.target.value }))}
                      disabled={weeklyPlanSaving}
                      placeholder="0"
                    />
                  </label>
                  <label className="webapp-field">
                    <span>{tr('Минуты чтения', 'Leseminuten')}</span>
                    <input
                      type="number"
                      min="0"
                      inputMode="numeric"
                      value={weeklyPlanDraft.reading_minutes_goal}
                      onChange={(event) => setWeeklyPlanDraft((prev) => ({ ...prev, reading_minutes_goal: event.target.value }))}
                      disabled={weeklyPlanSaving}
                      placeholder="0"
                    />
                  </label>
                  <button
                    type="button"
                    className="primary-button weekly-plan-save-btn"
                    onClick={saveWeeklyPlan}
                    disabled={weeklyPlanSaving || weeklyPlanLoading}
                  >
                    {weeklyPlanSaving ? tr('Сохраняем...', 'Speichern...') : tr('Сохранить план', 'Plan speichern')}
                  </button>
                </div>
                )}

                {weeklyPlanLoading && <div className="webapp-muted">{tr('Считаем недельные показатели...', 'Wochenwerte werden berechnet...')}</div>}
                {weeklyPlanError && <div className="webapp-error">{weeklyPlanError}</div>}
                {planAnalyticsLoading && <div className="webapp-muted">{tr('Считаем показатели плана...', 'Planwerte werden berechnet...')}</div>}
                {planAnalyticsError && <div className="webapp-error">{planAnalyticsError}</div>}

                {!weeklyPlanLoading && !weeklyPlanError && !planAnalyticsLoading && !planAnalyticsError && (
                  <div className="weekly-plan-metrics">
                    {weeklyMetricRows.map((item) => {
                      const goal = Number(item.data?.goal || 0);
                      const actual = Number(item.data?.actual || 0);
                      const forecast = Number(item.data?.forecast || 0);
                      const completion = Number(item.data?.completion_percent || 0);
                      const completionClamped = Math.max(0, Math.min(100, completion));
                      const ringExpected = expectedProgressPercent;
                      const forecastDelta = Number(item.data?.forecast_delta_vs_goal || 0);
                      const deficit = Math.max(0, ringExpected - completionClamped);
                      const ahead = Math.max(0, completionClamped - ringExpected);
                      const expectedPart = Math.min(completionClamped, ringExpected);
                      const remainder = Math.max(0, 100 - Math.max(completionClamped, ringExpected));
                      let ringGradient = '';
                      if (deficit > 0.01) {
                        ringGradient = `conic-gradient(#7bf1b3 0% ${expectedPart}%, #ff6b6b ${expectedPart}% ${ringExpected}%, rgba(94, 117, 159, 0.35) ${ringExpected}% 100%)`;
                      } else if (ahead > 0.01) {
                        ringGradient = `conic-gradient(#7bf1b3 0% ${ringExpected}%, #60a5fa ${ringExpected}% ${completionClamped}%, rgba(94, 117, 159, 0.35) ${completionClamped}% 100%)`;
                      } else {
                        ringGradient = `conic-gradient(#7bf1b3 0% ${completionClamped}%, rgba(94, 117, 159, 0.35) ${completionClamped}% 100%)`;
                      }
                      const completionRingStyle = {
                        background: `radial-gradient(circle at center, rgba(8, 16, 34, 0.96) 56%, transparent 57%), ${ringGradient}`,
                      };
                      const forecastClass = forecastDelta >= 0 ? 'is-good' : 'is-bad';
                      const expanded = Boolean(weeklyMetricExpanded[item.key]);
                      return (
                        <article className={`weekly-plan-metric-card ${weeklyMetricToneClass(item.key)}`} key={item.key}>
                          <div className="weekly-plan-metric-top">
                            <div>
                              <h4>{item.title}</h4>
                              <p>{tr('План/Факт/Прогноз', 'Plan/Ist/Prognose')}</p>
                            </div>
                            <div className="weekly-plan-metric-actions">
                              <div className="weekly-plan-progress-ring" style={completionRingStyle} title={`${tr('Факт', 'Ist')}: ${Math.round(completionClamped)}% • ${tr('Должно быть к текущему дню', 'Soll bis heute sein')}: ${Math.round(ringExpected)}%`}>
                                <span>{Math.round(completionClamped)}%</span>
                              </div>
                              <button
                                type="button"
                                className="secondary-button weekly-plan-card-toggle"
                                onClick={() => setWeeklyMetricExpanded((prev) => ({ ...prev, [item.key]: !prev[item.key] }))}
                              >
                                {expanded ? tr('Свернуть', 'Einklappen') : tr('Развернуть', 'Aufklappen')}
                              </button>
                            </div>
                          </div>
                          {expanded ? (
                          <div className="weekly-plan-values">
                            <div>
                              <span>{tr('План', 'Plan')}</span>
                              <strong>{formatWeeklyValue(goal)} {item.unit}</strong>
                            </div>
                            <div>
                              <span>{tr('Факт', 'Ist')}</span>
                              <strong>{formatWeeklyValue(actual)} {item.unit}</strong>
                            </div>
                            <div>
                              <span>{tr('Прогноз', 'Prognose')}</span>
                              <strong>{formatWeeklyValue(forecast, 1)} {item.unit}</strong>
                            </div>
                            <div className={forecastClass}>
                              <span>{tr('Отклонение прогноза', 'Abweichung Prognose')}</span>
                              <strong>{forecastDelta >= 0 ? '+' : ''}{formatWeeklyValue(forecastDelta, 1)} {item.unit}</strong>
                            </div>
                          </div>
                          ) : (
                            <div className="weekly-plan-values-compact">
                              <span>{tr('Факт', 'Ist')}: <strong>{formatWeeklyValue(actual)} {item.unit}</strong></span>
                              <span>{tr('План', 'Plan')}: <strong>{formatWeeklyValue(goal)} {item.unit}</strong></span>
                            </div>
                          )}
                        </article>
                      );
                    })}
                  </div>
                )}
              </section>
            )}

            {isHomeScreen && initData && (
              <section className="today-plan-panel">
                <div className="today-plan-head">
                  <div className="today-plan-title-wrap">
                    <h2>{tr('Задачи на сегодня', 'Aufgaben fuer heute')}</h2>
                    <p>{tr('Короткий персональный маршрут на день', 'Dein kurzer persoenlicher Plan fuer heute')}</p>
                  </div>
                  <span className="today-plan-total">
                    {tr('Всего', 'Gesamt')}: {todayPlan?.total_minutes || 0} {tr('мин', 'Min')}
                  </span>
                </div>
                <div className="today-plan-toolbar">
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={regenerateTodayPlan}
                    disabled={todayPlanLoading}
                  >
                    {todayPlanLoading ? tr('Обновляем...', 'Aktualisieren...') : tr('Обновить план', 'Plan aktualisieren')}
                  </button>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => { void loadTodayPlan(); }}
                    disabled={todayPlanLoading}
                  >
                    {todayPlanLoading ? tr('Загружаем...', 'Laden...') : tr('Показать план', 'Plan zeigen')}
                  </button>
                  <button
                    type="button"
                    className="secondary-button today-plan-mail-check-btn"
                    onClick={sendTodayReminderTest}
                    disabled={todayTestSending}
                  >
                    {todayTestSending ? tr('Отправка...', 'Senden...') : tr('Проверить личку', 'Privat testen')}
                  </button>
                </div>
                {todayPlanLoading && <div className="webapp-muted">{tr('Загружаем план...', 'Plan wird geladen...')}</div>}
                {todayPlanError && <div className="webapp-error">{todayPlanError}</div>}
                {!todayPlanLoading && !todayPlanError && (!todayPlan?.items || todayPlan.items.length === 0) && (
                  <div className="webapp-muted">{tr('План на сегодня пуст.', 'Tagesplan ist leer.')}</div>
                )}
                {!todayPlanLoading && !todayPlanError && Array.isArray(todayPlan?.items) && todayPlan.items.length > 0 && (
                  <div className="today-plan-items">
                    {todayPlan.items.map((item) => {
                      const loadingAction = todayItemLoading[item.id];
                      const taskType = String(item?.task_type || '').toLowerCase();
                      const isTranslationTask = taskType === 'translation';
                      const isVideoTask = taskType === 'video' || taskType === 'youtube';
                      const elapsedSeconds = getTodayItemElapsedSeconds(item, todayTimerNowMs);
                      const progressPercent = getTodayItemProgressPercent(item, todayTimerNowMs);
                      const translationProgress = isTranslationTask ? getTodayTranslationProgress(item) : null;
                      const doneByProgress = progressPercent >= 100;
                      const done = String(item?.status || '').toLowerCase() === 'done' || doneByProgress;
                      const itemStatusClass = done ? 'done' : (item.status || 'todo');
                      const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
                      const videoTopic = String(payload?.sub_category || payload?.skill_title || payload?.main_category || '').trim();
                      const videoLikes = Number(payload?.video_likes || 0);
                      const videoDislikes = Number(payload?.video_dislikes || 0);
                      const videoScore = Number(payload?.video_score || 0);
                      const userVote = Number(payload?.video_user_vote || 0);
                      const progressBadgeTitle = done
                        ? tr('Задача выполнена', 'Aufgabe erledigt')
                        : (
                          isTranslationTask
                            ? `${translationProgress?.completedCount || 0}/${translationProgress?.targetCount || 0}`
                            : `${Math.round(progressPercent)}%`
                        );
                      const progressBadgeText = done
                        ? '✅'
                        : (
                          isTranslationTask
                            ? `⭕ ${translationProgress?.completedCount || 0}/${translationProgress?.targetCount || 0}`
                            : `⭕ ${Math.round(progressPercent)}%`
                        );
                      return (
                        <div className={`today-plan-item is-${itemStatusClass}`} key={item.id}>
                          <div className="today-plan-item-main">
                            <div className="today-plan-item-title">{getTodayItemTitle(item)}</div>
                            <div className="today-plan-item-meta">
                              {!isVideoTask && <span>{item.estimated_minutes || 0} {tr('мин', 'Min')}</span>}
                              <span>{done ? 'DONE' : String(item.status || 'todo').toUpperCase()}</span>
                              <span>⏱ {formatCompactTimer(elapsedSeconds)}</span>
                            </div>
                            {isVideoTask && (
                              <div className="today-video-hint">
                                <div className="today-video-topic-line">
                                  <span>{tr('Тема для тренировки:', 'Thema fuer Training:')}</span>{' '}
                                  <span className="today-video-topic-value">{videoTopic || tr('не определена', 'nicht definiert')}</span>
                                </div>
                                {tr(
                                  'Если видео полезно по теме - поставьте 👍. Если не по теме - 👎.',
                                  'Wenn das Video zum Thema passt - 👍. Wenn nicht - 👎.'
                                )}
                                {' '}
                                <span>{tr('Рейтинг', 'Bewertung')}: {videoLikes}/{videoDislikes} ({videoScore >= 0 ? '+' : ''}{videoScore})</span>
                              </div>
                            )}
                          </div>
                          <div className={`today-plan-item-actions ${isVideoTask ? 'is-video-task' : ''}`}>
                            <button
                              type="button"
                              className={`secondary-button ${isVideoTask ? 'today-video-start-btn' : ''}`}
                              onClick={() => startTodayTask(item)}
                              disabled={Boolean(loadingAction) || (!isVideoTask && done)}
                            >
                              {loadingAction === 'start' ? tr('Старт...', 'Start...') : tr('Начать', 'Starten')}
                            </button>
                            <div className={`today-task-progress-badge ${isVideoTask ? 'today-video-progress-badge' : ''} ${done ? 'is-done' : ''}`} title={progressBadgeTitle}>
                              {progressBadgeText}
                            </div>
                            {isVideoTask && (
                              <>
                                <button
                                  type="button"
                                  className={`secondary-button today-video-vote ${userVote === 1 ? 'is-active' : ''}`}
                                  onClick={() => submitTodayVideoFeedback(item, 'like')}
                                  disabled={Boolean(loadingAction)}
                                  title={tr('Лайк: видео полезно по теме', 'Like: Video passt zum Thema')}
                                >
                                  {loadingAction === 'vote_like' ? '…' : '👍'}
                                </button>
                                <button
                                  type="button"
                                  className={`secondary-button today-video-vote ${userVote === -1 ? 'is-active is-negative' : ''}`}
                                  onClick={() => submitTodayVideoFeedback(item, 'dislike')}
                                  disabled={Boolean(loadingAction)}
                                  title={tr('Дизлайк: видео не по теме', 'Dislike: Video passt nicht zum Thema')}
                                >
                                  {loadingAction === 'vote_dislike' ? '…' : '👎'}
                                </button>
                              </>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>
            )}

            {isHomeScreen && initData && (
              <section className="skill-report-panel">
                <div className="skill-report-head">
                  <h3>{tr('Карта навыков', 'Skill-Ringe')}</h3>
                  <div className="skill-report-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={loadSkillReport}
                      disabled={skillReportLoading}
                    >
                      {skillReportLoading ? tr('Обновляем...', 'Aktualisieren...') : tr('Обновить', 'Aktualisieren')}
                    </button>
                  </div>
                </div>
                {skillReportLoading && <div className="webapp-muted">{tr('Загружаем прогресс...', 'Fortschritt wird geladen...')}</div>}
                {skillReportError && <div className="webapp-error">{skillReportError}</div>}
                {!skillReportLoading && !skillReportError && (
                  <div className="skill-rings-layout">
                    <div className="skill-rings-canvas">
                      <svg width={ringSize} height={ringSize} viewBox={`0 0 ${ringSize} ${ringSize}`} role="img" aria-label="Skill rings">
                        {ringSkills.map((skill, index) => {
                          const radius = ringStartRadius - index * ringStep;
                          const circumference = 2 * Math.PI * radius;
                          const progress = Math.max(0, Math.min(1, Number(skill?.mastery || 0) / 100));
                          const offset = circumference * (1 - progress);
                          const color = ringPalette[index % ringPalette.length];
                          return (
                            <g key={skill.skill_id}>
                              <circle
                                cx={ringCenter}
                                cy={ringCenter}
                                r={radius}
                                fill="none"
                                stroke="rgba(143, 167, 206, 0.22)"
                                strokeWidth="10"
                              />
                              <circle
                                cx={ringCenter}
                                cy={ringCenter}
                                r={radius}
                                fill="none"
                                stroke={color}
                                strokeWidth="10"
                                strokeLinecap="round"
                                strokeDasharray={`${circumference} ${circumference}`}
                                strokeDashoffset={offset}
                                transform={`rotate(-90 ${ringCenter} ${ringCenter})`}
                              />
                            </g>
                          );
                        })}
                      </svg>
                      <div className="skill-rings-center">
                        <div className="skill-rings-center-title">{tr('Фокус', 'Fokus')}</div>
                        <div className="skill-rings-center-value">{ringSkills.length}</div>
                        <div className="skill-rings-center-sub">{tr('3 слабых + 3 сильных', '3 schwache + 3 starke')}</div>
                      </div>
                    </div>
                    <div className="skill-rings-legend">
                      {ringSkills.map((skill, index) => {
                        const color = ringPalette[index % ringPalette.length];
                        const trainingStatus = getSkillTrainingStatus(skill?.skill_id);
                        const isSkillComplete = Boolean(trainingStatus?.is_complete);
                        const showSkillInProgress = Boolean(trainingStatus && !isSkillComplete);
                        const storedTrainingSnapshot = getStoredSkillTrainingSnapshot(skill?.skill_id);
                        const canResumeSkillTraining = Boolean(storedTrainingSnapshot);
                        const isSkillBusy = Boolean(skillPracticeLoading[String(skill.skill_id || '')]);
                        return (
                          <div
                            className={`skill-rings-legend-item ${skill.ring_type === 'weak' ? 'is-weak' : 'is-strong'}`}
                            key={`legend-${skill.skill_id}`}
                          >
                            <span className="skill-rings-dot" style={{ backgroundColor: color }} />
                            <div className="skill-rings-text">
                              <div className="skill-rings-name">
                                {skill.name}
                                {isSkillComplete && (
                                  <span className="skill-train-status-badge is-complete">✅ {tr('Готово', 'Fertig')}</span>
                                )}
                                {showSkillInProgress && (
                                  <span className="skill-train-status-badge is-progress">
                                    {tr('в процессе', 'in Arbeit')}
                                  </span>
                                )}
                              </div>
                              <div className="skill-rings-meta">
                                <span>{skill.ring_type === 'weak' ? tr('Слабый', 'Schwach') : tr('Сильный', 'Stark')}</span>
                                <span>
                                  {tr('Оценка', 'Score')}: {skill?.mastery === null || skill?.mastery === undefined
                                    ? tr('нет данных', 'keine Daten')
                                    : `${Math.round(Number(skill.mastery || 0))}%`}
                                </span>
                                <span>{tr('Ошибки 7д', 'Fehler 7T')}: {Number(skill.errors_7d || 0)}</span>
                                {trainingStatus && (
                                  <span>
                                    {tr('Ссылки', 'Links')}: {Math.max(0, Number(trainingStatus.opened_count || 0))}/{Math.max(0, Number(trainingStatus.required_count || 0))}
                                  </span>
                                )}
                              </div>
                            </div>
                            <div className="skill-rings-actions">
                              <button
                                type="button"
                                className="secondary-button skill-rings-train-btn"
                                onClick={() => startSkillPractice(skill, { forceRefresh: false })}
                                disabled={isSkillBusy}
                              >
                                {isSkillBusy
                                  ? tr('Запуск...', 'Start...')
                                  : tr('Прокачать', 'Trainieren')}
                              </button>
                              {canResumeSkillTraining && (
                                <button
                                  type="button"
                                  className="secondary-button skill-rings-resume-btn"
                                  onClick={() => resumeSkillPractice(skill)}
                                  disabled={isSkillBusy}
                                >
                                  {tr('Вернуться к тренировке', 'Zum Training zurueck')}
                                </button>
                              )}
                            </div>
                          </div>
                        );
                      })}
                      {ringSkills.length === 0 && (
                        <div className="webapp-muted">{tr('Пока нет данных по навыкам.', 'Noch keine Skill-Daten.')}</div>
                      )}
                    </div>
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('guide') && (
              <section className="webapp-section webapp-guide" ref={guideRef}>
                <div className="guide-hero-card">
                  <div className="guide-hero-copy">
                    <h2>{tr('Как пользоваться', 'So benutzt du DeutschFlow')}</h2>
                    <p className="guide-hero-subtitle">
                      {tr(
                        'Короткий маршрут по приложению: с чего начать, куда нажимать и какой раздел за что отвечает.',
                        'Kurzer Startpfad durch die App: womit du beginnst, wohin du tippst und welcher Bereich wofuer da ist.'
                      )}
                    </p>
                  </div>
                  <div className="guide-hero-mascot-wrap" aria-hidden="true">
                    <img src={heroStickerSrc} alt="" className="guide-hero-mascot" />
                  </div>
                </div>

                <div className="guide-step-grid">
                  {guideStepItems.map((item, index) => {
                    const isOpen = guideStepOpenKey === item.key;
                    const displayNumber = String(index + 1);
                    return (
                      <article
                        key={item.key}
                        className={`guide-step-card ${isOpen ? 'is-open' : ''} ${item.accent ? 'is-highlight' : ''}`}
                        onClick={() => setGuideStepOpenKey((prev) => (prev === item.key ? '' : item.key))}
                      >
                        <button
                          type="button"
                          className="guide-step-question"
                          aria-expanded={isOpen}
                          aria-controls={`guide-step-answer-${item.key}`}
                        >
                          <div className="guide-step-leading">
                            <div className="guide-step-number">{displayNumber}</div>
                            <div>
                              <div className="guide-step-title">{item.title}</div>
                              <p className="guide-step-text">{item.summary}</p>
                            </div>
                          </div>
                          <span className={`guide-step-chevron ${isOpen ? 'is-open' : ''}`} aria-hidden="true">⌄</span>
                        </button>
                        {isOpen && (
                          <div className="guide-step-answer" id={`guide-step-answer-${item.key}`}>
                            <div className="guide-step-details">
                              {item.sections.map((section) => (
                                <div key={`${item.key}-${section.title}`} className="guide-step-detail-block">
                                  <div className="guide-step-detail-title">{section.title}</div>
                                  <ul className="guide-step-detail-list">
                                    {section.items.map((detail, index) => (
                                      <li key={`${item.key}-${section.title}-${index}`}>{detail}</li>
                                    ))}
                                  </ul>
                                </div>
                              ))}
                            </div>
                            <div className="guide-step-answer-actions">
                              <button
                                type="button"
                                className="secondary-button guide-step-close-btn"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setGuideStepOpenKey('');
                                }}
                              >
                                {tr('Свернуть раздел', 'Abschnitt einklappen')}
                              </button>
                            </div>
                          </div>
                        )}
                      </article>
                    );
                  })}
                </div>

                <div className="guide-actions">
                  <button type="button" className="primary-button guide-primary-cta" onClick={() => openSingleSectionAndScroll('translations', translationsRef)}>
                    {tr('Начать с переводов', 'Mit Uebersetzungen starten')}
                  </button>
                  <button type="button" className="secondary-button guide-tour-button" onClick={startOnboardingTour}>
                    {tr('Показать быстрый тур', 'Schnellstart zeigen')}
                  </button>
                  <div className="guide-quick-actions-grid">
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('dictionary', dictionaryRef)}>
                      {tr('Открыть словарь', 'Woerterbuch oeffnen')}
                    </button>
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('flashcards', flashcardsRef)}>
                      {tr('Перейти к карточкам', 'Zu Karten gehen')}
                    </button>
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('youtube', youtubeRef)}>
                      {tr('Смотреть YouTube', 'YouTube ansehen')}
                    </button>
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('reader', readerRef)}>
                      {tr('Открыть Reader', 'Reader oeffnen')}
                    </button>
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('assistant', assistantRef)}>
                      {tr('Поговорить с ассистентом', 'Mit dem Assistenten sprechen')}
                    </button>
                    <button type="button" className="secondary-button" onClick={() => openSingleSectionAndScroll('subscription', billingRef)}>
                      {tr('Открыть подписку', 'Abo oeffnen')}
                    </button>
                  </div>
                </div>
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('skill_training') && (
              <section className="webapp-section theory-section skill-training-section" ref={skillTrainingRef}>
                <div className="skill-training-top-card">
                  <div className="skill-training-top-head">
                    <div className="skill-training-top-copy">
                      <h2>
                        {tr('Тренируем навык', 'Skill-Training')}: {String(
                          skillTrainingData?.package?.focus?.skill_name
                          || skillTrainingData?.skill?.title
                          || tr('Навык', 'Skill')
                        )}
                      </h2>
                    </div>
                    <img src={heroStickerSrc} alt="" aria-hidden="true" className="skill-training-mascot" />
                  </div>

                  <div className="skill-training-action-row">
                    <button type="button" className="section-home-back skill-training-back-btn" onClick={goHomeScreen}>
                      {tr('← Назад', '← Zurueck')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button skill-training-refresh-btn"
                      onClick={() => startSkillPractice(skillTrainingData?.skill, { forceRefresh: true })}
                      disabled={skillTrainingLoading || !skillTrainingData?.skill?.skill_id}
                    >
                      {skillTrainingLoading ? tr('Обновляем...', 'Aktualisieren...') : tr('Обновить', 'Aktualisieren')}
                    </button>
                    <button type="button" className="primary-button skill-training-finish-btn" onClick={finishSkillTraining}>
                      {tr('Закончить тренировку навыка', 'Skill-Training beenden')}
                    </button>
                  </div>

                  {skillTrainingLoading && (
                    <div className="skill-training-status-block" role="status" aria-live="polite">
                      <div className="skill-training-status-row">
                        <span className="skill-training-status-indicator" aria-hidden="true" />
                        <span>{tr('Обновляем...', 'Aktualisieren...')}</span>
                      </div>
                      <div className="skill-training-status-row">
                        <span className="skill-training-status-indicator is-subtle" aria-hidden="true" />
                        <span>{tr('Готовим тренировку навыка...', 'Skill-Training wird vorbereitet...')}</span>
                      </div>
                    </div>
                  )}
                </div>

                {skillTrainingError && <div className="webapp-error">{skillTrainingError}</div>}

                {!skillTrainingLoading && !skillTrainingError && skillTrainingData?.package && (
                  <div className="theory-card">
                    <div className="theory-focus-line">
                      <strong>{tr('Фокус', 'Fokus')}:</strong>{' '}
                      {String(skillTrainingData?.package?.focus?.skill_name || '')}
                      {skillTrainingData?.package?.focus?.error_subcategory
                        ? ` • ${String(skillTrainingData.package.focus.error_subcategory)}`
                        : ''}
                    </div>

                    <div className="theory-practice-block skill-training-theory-block">
                      <h4>{tr('1) Теория по теме', '1) Theorie zum Thema')}</h4>
                      {skillTrainingData?.package?.theory?.title && (
                        <h3 className="skill-training-theory-title">
                          {String(skillTrainingData.package.theory.title)}
                        </h3>
                      )}
                      {skillTrainingData?.package?.theory?.core_explanation && (
                        <p className="skill-training-theory-lead">
                          {String(skillTrainingData.package.theory.core_explanation)}
                        </p>
                      )}
                      {skillTrainingData?.package?.theory?.what_this_topic_is && (
                        <div className="skill-training-theory-section">
                          <div className="skill-training-theory-section-title">
                            {tr('Что это за тема', 'Was ist dieses Thema')}
                          </div>
                          <p>{String(skillTrainingData.package.theory.what_this_topic_is)}</p>
                        </div>
                      )}
                      {skillTrainingData?.package?.theory?.why_mistake_happens && (
                        <div className="skill-training-theory-section">
                          <div className="skill-training-theory-section-title">
                            {tr('Почему возникает ошибка', 'Warum der Fehler entsteht')}
                          </div>
                          <p>{String(skillTrainingData.package.theory.why_mistake_happens)}</p>
                        </div>
                      )}
                      {skillTrainingData?.package?.theory?.error_connection && (
                        <div className="skill-training-theory-section">
                          <div className="skill-training-theory-section-title">
                            {tr('Связь с ошибками ученика', 'Bezug zu den Fehlern')}
                          </div>
                          <p>{String(skillTrainingData.package.theory.error_connection)}</p>
                        </div>
                      )}
                      {Array.isArray(skillTrainingData?.package?.theory?.core_rules)
                        && skillTrainingData.package.theory.core_rules.length > 0 && (
                          <div className="skill-training-theory-section">
                            <div className="skill-training-theory-section-title">
                              {tr('Ключевые правила', 'Kernregeln')}
                            </div>
                            <ul className="skill-training-theory-list">
                              {skillTrainingData.package.theory.core_rules.map((item, index) => (
                                <li key={`skill-training-rule-${index}`}>{String(item || '')}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      {Array.isArray(skillTrainingData?.package?.theory?.step_by_step)
                        && skillTrainingData.package.theory.step_by_step.length > 0 && (
                          <div className="skill-training-theory-section">
                            <div className="skill-training-theory-section-title">
                              {tr('Быстрый алгоритм', 'Schneller Ablauf')}
                            </div>
                            <ol className="skill-training-theory-list is-ordered">
                              {skillTrainingData.package.theory.step_by_step.map((item, index) => (
                                <li key={`skill-training-step-${index}`}>{String(item || '')}</li>
                              ))}
                            </ol>
                          </div>
                        )}
                      {Array.isArray(skillTrainingData?.package?.theory?.construction_recipe)
                        && skillTrainingData.package.theory.construction_recipe.length > 0 && (
                          <div className="skill-training-theory-section">
                            <div className="skill-training-theory-section-title">
                              {tr('Пошаговая сборка предложения', 'Schritt-fuer-Schritt Aufbau')}
                            </div>
                            <ol className="skill-training-theory-list is-ordered">
                              {skillTrainingData.package.theory.construction_recipe.map((item, index) => (
                                <li key={`skill-training-recipe-${index}`}>{String(item || '')}</li>
                              ))}
                            </ol>
                          </div>
                        )}
                      {skillTrainingData?.package?.theory?.key_rule && (
                        <div className="theory-key-rule">
                          <strong>{tr('Главное правило', 'Wichtigste Regel')}:</strong>{' '}
                          {String(skillTrainingData.package.theory.key_rule)}
                        </div>
                      )}
                      {Array.isArray(skillTrainingData?.package?.theory?.minimal_pairs)
                        && skillTrainingData.package.theory.minimal_pairs.length > 0 && (
                          <div className="skill-training-theory-section">
                            <div className="skill-training-theory-section-title">
                              {tr('Минимальные пары и контрасты', 'Kontrastpaare')}
                            </div>
                            <div className="skill-training-pairs">
                              {skillTrainingData.package.theory.minimal_pairs.map((pair, index) => (
                                <div className="skill-training-pair-card" key={`skill-training-pair-${index}`}>
                                  <div className="skill-training-pair-sentence">
                                    A: {String(pair?.sentence_a || '')}
                                  </div>
                                  <div className="skill-training-pair-sentence">
                                    B: {String(pair?.sentence_b || '')}
                                  </div>
                                  {pair?.explanation && (
                                    <div className="skill-training-pair-explanation">
                                      {String(pair.explanation)}
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      {Array.isArray(skillTrainingData?.package?.theory?.examples)
                        && skillTrainingData.package.theory.examples.length > 0 && (
                          <div className="theory-examples">
                            <h4>{tr('Примеры', 'Beispiele')}</h4>
                            {skillTrainingData.package.theory.examples.map((example, index) => (
                              <div key={`skill-training-example-${index}`} className="theory-example-item">
                                <div><strong>{String(example?.sentence || '')}</strong></div>
                                {example?.translation && (
                                  <div className="skill-training-example-translation">
                                    {String(example.translation)}
                                  </div>
                                )}
                                {example?.explanation && <div>{String(example.explanation)}</div>}
                              </div>
                            ))}
                          </div>
                        )}
                      {skillTrainingData?.package?.theory?.memory_trick && (
                        <div className="theory-memory-trick">
                          <strong>{tr('Лайфхак', 'Merkhilfe')}:</strong>{' '}
                          {String(skillTrainingData.package.theory.memory_trick)}
                        </div>
                      )}
                      {Array.isArray(skillTrainingData?.package?.theory?.self_check)
                        && skillTrainingData.package.theory.self_check.length > 0 && (
                          <div className="skill-training-theory-section">
                            <div className="skill-training-theory-section-title">
                              {tr('Быстрая самопроверка', 'Selbst-Check')}
                            </div>
                            <ul className="skill-training-theory-list">
                              {skillTrainingData.package.theory.self_check.map((item, index) => (
                                <li key={`skill-training-self-check-${index}`}>{String(item || '')}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                    </div>

                    <div className="theory-resources">
                      <h4>{tr('2) Источники по теме', '2) Quellen zum Thema')}</h4>
                      {Array.isArray(skillTrainingData?.package?.theory?.resources)
                        && skillTrainingData.package.theory.resources.slice(0, 2).map((item, index) => {
                          const title = String(item?.title || '').trim();
                          const url = String(item?.url || '').trim();
                          const why = String(item?.why || '').trim();
                          if (!title || !url) return null;
                          return (
                            <a
                              key={`skill-training-resource-${index}`}
                              className="theory-resource-item"
                              href={url}
                              target="_blank"
                              rel="noreferrer"
                              onClick={() => {
                                const focusSkillId = String(skillTrainingData?.package?.focus?.skill_id || skillTrainingData?.skill?.skill_id || '').trim();
                                if (!focusSkillId) return;
                                trackSkillPracticeEvent(focusSkillId, 'open_resource', url);
                              }}
                            >
                              <div className="theory-resource-title">🔗 {title}</div>
                              {why && <div className="theory-resource-why">{why}</div>}
                              <div className="theory-resource-url">{url}</div>
                            </a>
                          );
                        })}
                      {(!Array.isArray(skillTrainingData?.package?.theory?.resources)
                        || skillTrainingData.package.theory.resources.length === 0) && (
                          <div className="webapp-muted">
                            {tr(
                              'Источники по теме пока не подобраны. Обновите тренировку позже.',
                              'Quellen zum Thema wurden noch nicht gefunden. Bitte spaeter aktualisieren.'
                            )}
                          </div>
                        )}
                    </div>

                    <div className="theory-practice-block skill-training-video-block">
                      <h4>{tr('3) Релевантное видео YouTube', '3) Relevantes YouTube-Video')}</h4>
                      {skillTrainingVideoLoading ? (
                        <div className="webapp-muted">
                          {tr('Подбираем видео по теме...', 'Video zum Thema wird geladen...')}
                        </div>
                      ) : skillTrainingData?.video?.video_url || skillTrainingData?.video?.video_id ? (
                        <>
                          <div className="webapp-muted">
                            {String(skillTrainingData?.video?.title || tr('Видео по теме найдено', 'Video zum Thema gefunden'))}
                          </div>
                          <button type="button" className="primary-button" onClick={openSkillTrainingVideo}>
                            {tr('Открыть видео', 'Video oeffnen')}
                          </button>
                        </>
                      ) : (
                        <div className="webapp-muted">
                          {tr('Видео по теме пока не найдено. Нажмите «Обновить».', 'Video zum Thema wurde noch nicht gefunden. Klicke auf „Aktualisieren“.')}
                        </div>
                      )}
                    </div>

                    <div className="theory-practice-block">
                      <h4>{tr('4) Практика: 5 предложений', '4) Uebung: 5 Saetze')}</h4>
                      {(skillTrainingData?.package?.practice_sentences || []).map((sentence, index) => (
                        <label key={`skill-training-practice-${index}`} className="webapp-field">
                          <span>{index + 1}. {String(sentence || '')}</span>
                          <textarea
                            rows={2}
                            value={skillTrainingAnswers[index] || ''}
                            onChange={(event) => {
                              const value = event.target.value;
                              setSkillTrainingAnswers((prev) => prev.map((item, idx) => (idx === index ? value : item)));
                            }}
                            placeholder={tr('Введите перевод...', 'Uebersetzung eingeben...')}
                          />
                        </label>
                      ))}
                      <button type="button" className="primary-button" onClick={checkSkillTraining} disabled={skillTrainingChecking}>
                        {skillTrainingChecking ? tr('Проверяем...', 'Pruefen...') : tr('Проверить', 'Pruefen')}
                      </button>
                    </div>

                    {skillTrainingFeedback && (
                      <div className="theory-feedback">
                        <h4>{tr('Обратная связь', 'Feedback')}</h4>
                        {(skillTrainingFeedback?.items || []).map((row, index) => (
                          <div className="theory-feedback-item" key={`skill-training-feedback-${index}`}>
                            <div><strong>{index + 1}. {String(row?.native_sentence || '')}</strong></div>
                            <div>{tr('Ваш перевод', 'Deine Uebersetzung')}: {String(row?.learner_translation || '')}</div>
                            <div>{tr('Статус', 'Status')}: {row?.is_correct ? tr('Верно', 'Korrekt') : tr('Нужно исправить', 'Korrigieren')}</div>
                            {row?.corrected_translation && <div>{tr('Исправленный вариант', 'Korrigierte Version')}: {String(row.corrected_translation)}</div>}
                            {row?.what_is_good && <div>{tr('Что хорошо', 'Was gut ist')}: {String(row.what_is_good)}</div>}
                            {row?.what_is_wrong && <div>{tr('Что не так', 'Was falsch ist')}: {String(row.what_is_wrong)}</div>}
                            {row?.missed_rule && <div>{tr('Правило', 'Regel')}: {String(row.missed_rule)}</div>}
                            {row?.tip && <div>{tr('Совет', 'Tipp')}: {String(row.tip)}</div>}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('theory') && (
              <section className="webapp-section theory-section" ref={theoryRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Теория', 'Theorie')}</h2>
                  {isFocusedSection('theory') && (
                    <button type="button" className="section-home-back" onClick={goHomeScreen}>
                      {tr('На главную', 'Startseite')}
                    </button>
                  )}
                  {renderTodaySectionTaskHud('theory')}
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="theory-actions-top">
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => {
                      const theoryItem = (todayPlan?.items || []).find((entry) => String(entry?.task_type || '').toLowerCase() === 'theory');
                      if (theoryItem) {
                        prepareTodayTheory(theoryItem, { forceRefresh: true });
                      }
                    }}
                  >
                    {tr('Обновить', 'Aktualisieren')}
                  </button>
                  <button
                    type="button"
                    className="primary-button"
                    onClick={goHomeScreen}
                  >
                    {tr('Закончить теорию', 'Theorie beenden')}
                  </button>
                </div>
                {theoryLoading && <div className="webapp-muted">{tr('Готовим теорию...', 'Theorie wird vorbereitet...')}</div>}
                {theoryError && <div className="webapp-error">{theoryError}</div>}
                {!theoryLoading && !theoryError && theoryPackage && (
                  <div className="theory-card">
                    <div className="theory-focus-line">
                      <strong>{tr('Фокус', 'Fokus')}:</strong>{' '}
                      {String(theoryPackage?.focus?.skill_name || '')}
                      {theoryPackage?.focus?.error_subcategory ? ` • ${String(theoryPackage.focus.error_subcategory)}` : ''}
                    </div>
                    <h3>{String(theoryPackage?.theory?.title || tr('Теория', 'Theorie'))}</h3>
                    {theoryPackage?.theory?.core_explanation && <p>{String(theoryPackage.theory.core_explanation)}</p>}
                    {theoryPackage?.theory?.why_mistake_happens && (
                      <p><strong>{tr('Почему возникает ошибка', 'Warum der Fehler entsteht')}:</strong> {String(theoryPackage.theory.why_mistake_happens)}</p>
                    )}
                    {Array.isArray(theoryPackage?.theory?.step_by_step) && theoryPackage.theory.step_by_step.length > 0 && (
                      <ol className="theory-steps">
                        {theoryPackage.theory.step_by_step.map((item, index) => (
                          <li key={`theory-step-${index}`}>{String(item || '')}</li>
                        ))}
                      </ol>
                    )}
                    {theoryPackage?.theory?.key_rule && (
                      <div className="theory-key-rule">
                        <strong>{tr('Главное правило', 'Wichtigste Regel')}:</strong> {String(theoryPackage.theory.key_rule)}
                      </div>
                    )}
                    {Array.isArray(theoryPackage?.theory?.examples) && theoryPackage.theory.examples.length > 0 && (
                      <div className="theory-examples">
                        <h4>{tr('Примеры', 'Beispiele')}</h4>
                        {theoryPackage.theory.examples.map((example, index) => (
                          <div key={`theory-example-${index}`} className="theory-example-item">
                            <div><strong>{String(example?.sentence || '')}</strong></div>
                            <div>{String(example?.explanation || '')}</div>
                          </div>
                        ))}
                      </div>
                    )}
                    {theoryPackage?.theory?.memory_trick && (
                      <div className="theory-memory-trick">
                        <strong>{tr('Лайфхак', 'Merkhilfe')}:</strong> {String(theoryPackage.theory.memory_trick)}
                      </div>
                    )}
                    {Array.isArray(theoryPackage?.theory?.resources) && theoryPackage.theory.resources.length > 0 && (
                      <div className="theory-resources">
                        <h4>{tr('Полезные источники', 'Nuetzliche Quellen')}</h4>
                        {theoryPackage.theory.resources.map((item, index) => {
                          const title = String(item?.title || '').trim();
                          const url = String(item?.url || '').trim();
                          const why = String(item?.why || '').trim();
                          const type = String(item?.type || '').trim().toLowerCase();
                          if (!title || !url) return null;
                          return (
                            <a
                              key={`theory-resource-${index}`}
                              className="theory-resource-item"
                              href={url}
                              target="_blank"
                              rel="noreferrer"
                            >
                              <div className="theory-resource-title">
                                🔗 {title}
                                {type === 'video' ? ` (${tr('видео', 'Video')})` : ` (${tr('статья', 'Artikel')})`}
                              </div>
                              {why && <div className="theory-resource-why">{why}</div>}
                              <div className="theory-resource-url">{url}</div>
                            </a>
                          );
                        })}
                      </div>
                    )}

                    <div className="theory-practice-toggle">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setTheoryPracticeOpen((prev) => !prev)}
                      >
                        {theoryPracticeOpen ? tr('Скрыть закрепление', 'Uebung verbergen') : tr('Закрепить теорию', 'Theorie festigen')}
                      </button>
                    </div>

                    {theoryPracticeOpen && (
                      <div className="theory-practice-block">
                        <h4>{tr('Закрепление теории', 'Theorie-Festigung')}</h4>
                        {(theoryPackage?.practice_sentences || []).map((sentence, index) => (
                          <label key={`theory-practice-${index}`} className="webapp-field">
                            <span>{index + 1}. {String(sentence || '')}</span>
                            <textarea
                              rows={2}
                              value={theoryPracticeAnswers[index] || ''}
                              onChange={(event) => {
                                const value = event.target.value;
                                setTheoryPracticeAnswers((prev) => prev.map((item, idx) => (idx === index ? value : item)));
                              }}
                              placeholder={tr('Введите перевод...', 'Uebersetzung eingeben...')}
                            />
                          </label>
                        ))}
                        <button type="button" className="primary-button" onClick={checkTodayTheory} disabled={theoryChecking}>
                          {theoryChecking ? tr('Проверяем...', 'Pruefen...') : tr('Проверить', 'Pruefen')}
                        </button>
                      </div>
                    )}

                    {theoryFeedback && (
                      <div className="theory-feedback">
                        <h4>{tr('Обратная связь', 'Feedback')}</h4>
                        {(theoryFeedback?.items || []).map((row, index) => (
                          <div className="theory-feedback-item" key={`theory-feedback-${index}`}>
                            <div><strong>{index + 1}. {String(row?.native_sentence || '')}</strong></div>
                            <div>{tr('Ваш перевод', 'Deine Uebersetzung')}: {String(row?.learner_translation || '')}</div>
                            <div>{tr('Статус', 'Status')}: {row?.is_correct ? tr('Верно', 'Korrekt') : tr('Нужно исправить', 'Korrigieren')}</div>
                            {row?.corrected_translation && <div>{tr('Исправленный вариант', 'Korrigierte Version')}: {String(row.corrected_translation)}</div>}
                            {row?.what_is_good && <div>{tr('Что хорошо', 'Was gut ist')}: {String(row.what_is_good)}</div>}
                            {row?.what_is_wrong && <div>{tr('Что не так', 'Was falsch ist')}: {String(row.what_is_wrong)}</div>}
                            {row?.missed_rule && <div>{tr('Правило', 'Regel')}: {String(row.missed_rule)}</div>}
                            {row?.tip && <div>{tr('Совет', 'Tipp')}: {String(row.tip)}</div>}
                          </div>
                        ))}
                        {theoryFeedback?.summary_good && <div><strong>{tr('Вы уже умеете', 'Schon gut')}:</strong> {String(theoryFeedback.summary_good)}</div>}
                        {theoryFeedback?.summary_improve && <div><strong>{tr('Нужно подтянуть', 'Verbessern')}:</strong> {String(theoryFeedback.summary_improve)}</div>}
                        {theoryFeedback?.memory_secret && <div><strong>{tr('Секрет запоминания', 'Merk-Geheimnis')}:</strong> {String(theoryFeedback.memory_secret)}</div>}
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('translations') && (
              <section className="webapp-section webapp-section-translations" ref={translationsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo translations-title-row">
                  <div className="translations-title-main">
                    <h2>{tr('Ваши переводы', 'Ihre Uebersetzungen')}</h2>
                    {isFocusedSection('translations') && (
                      <button type="button" className="section-home-back" onClick={goHomeScreen}>
                        {tr('На главную', 'Startseite')}
                      </button>
                    )}
                  </div>
                  <div className="translations-title-side">
                    {renderTodaySectionTaskHud('translations')}
                    <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo translations-corner-logo" />
                  </div>
                </div>
                {showTranslationStartConfigurator && (
                  <div className="webapp-translation-start">
                    <label className="webapp-field">
                      <span>{tr('Грамматический фокус тренировки', 'Grammatikfokus fuer das Training')}</span>
      <select
        value={selectedTopic}
        onChange={handleTopicChange}
        disabled={topicsLoading || webappLoading}
      >
                        {topics.map((topic) => (
                          <option key={topic} value={topic}>
                            {topic}
                          </option>
                        ))}
                      </select>
                    </label>
                    {!isStoryTopic(selectedTopic) && isCustomTopic(selectedTopic) && (
                      <label className="webapp-field">
                        <span>{tr('Свой грамматический фокус', 'Eigener Grammatikfokus')}</span>
                        <input
                          type="text"
                          value={customTopicInput}
                          onChange={(event) => setCustomTopicInput(event.target.value)}
                          placeholder={tr('Например: Genitiv, Passiv mit Modalverben, nicht vs kein', 'Zum Beispiel: Genitiv, Passiv mit Modalverben, nicht vs kein')}
                          disabled={webappLoading}
                        />
                      </label>
                    )}
                    {!isStoryTopic(selectedTopic) && (
                      <label className="webapp-field">
                        <span>{tr('Уровень', 'Niveau')}</span>
                        <select
                          value={selectedLevel}
                          onChange={(event) => setSelectedLevel(event.target.value)}
                          disabled={webappLoading}
                        >
                          <option value="a1">A1</option>
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
                          <span>{tr('История', 'Story')}</span>
                          <select
                            value={storyMode}
                            onChange={(event) => setStoryMode(event.target.value)}
                            disabled={webappLoading}
                          >
                            <option value="new">{tr('Новая', 'Neu')}</option>
                            <option value="repeat">{tr('Повторить старую', 'Alte wiederholen')}</option>
                          </select>
                        </label>
                        {storyMode === 'repeat' && (
                          <label className="webapp-field">
                            <span>{tr('Выберите историю', 'Story auswaehlen')}</span>
                            <select
                              value={selectedStoryId}
                              onChange={(event) => setSelectedStoryId(event.target.value)}
                              disabled={webappLoading || storyHistoryLoading}
                            >
                              <option value="">{tr('Последняя', 'Letzte')}</option>
                              {storyHistory.map((item) => (
                                <option key={item.story_id} value={item.story_id}>
                                  {item.title || tr(`История #${item.story_id}`, `Story #${item.story_id}`)}
                                </option>
                              ))}
                            </select>
                          </label>
                        )}
                        <label className="webapp-field">
                          <span>{tr('Тип истории', 'Story-Typ')}</span>
                          <select
                            value={storyType}
                            onChange={(event) => setStoryType(event.target.value)}
                            disabled={webappLoading}
                          >
                            <option value="знаменитая личность">{tr('Знаменитая личность', 'Beruehmte Persoenlichkeit')}</option>
                            <option value="историческое событие">{tr('Историческое событие', 'Historisches Ereignis')}</option>
                            <option value="выдающееся открытие">{tr('Выдающееся открытие', 'Bedeutende Entdeckung')}</option>
                            <option value="выдающееся изобретение">{tr('Выдающееся изобретение', 'Bedeutende Erfindung')}</option>
                            <option value="география">{tr('География', 'Geografie')}</option>
                            <option value="космос">{tr('Космос', 'Weltraum')}</option>
                            <option value="культура">{tr('Культура', 'Kultur')}</option>
                            <option value="спорт">{tr('Спорт', 'Sport')}</option>
                            <option value="политика">{tr('Политика', 'Politik')}</option>
                          </select>
                        </label>
                        <label className="webapp-field">
                          <span>{tr('Сложность', 'Schwierigkeit')}</span>
                          <select
                            value={storyDifficulty}
                            onChange={(event) => setStoryDifficulty(event.target.value)}
                            disabled={webappLoading}
                          >
                            <option value="начальный">{tr('Начальный', 'Anfaenger')}</option>
                            <option value="средний">{tr('Средний', 'Mittel')}</option>
                            <option value="продвинутый">{tr('Продвинутый', 'Fortgeschritten')}</option>
                          </select>
                        </label>
                      </>
                    )}
                    <button
                      type="button"
                      className="primary-button translation-start-cta"
                      onClick={isStoryTopic(selectedTopic) ? handleStartStory : handleStartTranslation}
                      disabled={webappLoading || topicsLoading || (isCustomTopic(selectedTopic) && !customTopicInput.trim())}
                    >
                      {webappLoading ? tr('Запускаем...', 'Starten...') : tr('🚀 Начать перевод', '🚀 Uebersetzung starten')}
                    </button>
                  </div>
                )}
                {topicsError && <div className="webapp-error">{topicsError}</div>}
                {storyHistoryError && <div className="webapp-error">{storyHistoryError}</div>}
                {!isStoryResultMode && (
                <form className="webapp-form translation-form" onSubmit={handleTranslationSubmit}>
                  <section className="webapp-translation-list">
                    {sentences.length === 0 ? (
                      <p className="webapp-muted translation-empty-state">
                        {tr('Нет активных предложений. Нажмите «🚀 Начать перевод», чтобы получить новые.', 'Keine aktiven Saetze. Druecke «🚀 Uebersetzung starten», um neue zu laden.')}
                      </p>
                    ) : (
                      sentences.map((item, index) => {
                        const draft = translationDrafts[String(item.id_for_mistake_table)] || '';
                        return (
                          <TranslationDraftField
                            key={`${item.id_for_mistake_table}-${item.unique_id ?? index}`}
                            sentenceId={item.id_for_mistake_table}
                            sentenceNumber={item.unique_id ?? index + 1}
                            sentenceText={item.sentence}
                            initialValue={draft}
                            placeholder={tr('Введите перевод...', 'Uebersetzung eingeben...')}
                            dictionaryLabel={tr('Открыть словарь', 'Woerterbuch')}
                            isAndroidClient={isAndroidTelegramClient}
                            onLiveChange={handleDraftLiveChange}
                            onCommit={handleDraftCommit}
                            onJumpToDictionary={jumpToDictionaryFromSentence}
                          />
                        );
                      })
                    )}
                  </section>

                  {isStorySession && hasActiveTranslationSentences && (
                    <label className="webapp-field">
                      <span>{tr('А теперь угадай, о ком / чем шла речь', 'Rate jetzt, worum oder um wen es ging')}</span>
                      <input
                        type="text"
                        value={storyGuess}
                        onChange={(event) => setStoryGuess(event.target.value)}
                        placeholder={tr('Ваш ответ...', 'Deine Antwort...')}
                      />
                    </label>
                  )}
                  {!isStorySession && hasActiveTranslationSentences && (
                    <label className="translation-private-grammar-optin">
                      <div className="translation-private-grammar-optin-copy">
                        <span>
                          {tr(
                            'Отправлять текстовое объяснение грамматики в личку сразу после проверки',
                            'Textuelle Grammatikerklaerung sofort nach der Pruefung in den privaten Chat senden'
                          )}
                        </span>
                        <small>
                          {tr(
                            'Разбор приходит отдельным сообщением по каждому проверенному предложению.',
                            'Die Analyse kommt als separate Nachricht fuer jeden geprueften Satz.'
                          )}
                        </small>
                      </div>
                      <span className="translation-private-grammar-optin-check">
                        <input
                          type="checkbox"
                          checked={translationPrivateGrammarTextOptIn}
                          onChange={(event) => setTranslationPrivateGrammarTextOptIn(event.target.checked)}
                          disabled={webappLoading}
                        />
                      </span>
                    </label>
                  )}

                  {hasActiveTranslationSentences && (
                    <>
                      <button
                        className={`primary-button translation-check-cta ${sentences.length === 0 && !webappLoading ? 'is-disabled-empty' : ''}`}
                        type="submit"
                        disabled={webappLoading || sentences.length === 0}
                      >
                        {webappLoading
                          ? (translationCheckProgress.total > 0
                              ? tr(
                                  `Проверяем... ${translationCheckProgress.done}/${translationCheckProgress.total}`,
                                  `Pruefen... ${translationCheckProgress.done}/${translationCheckProgress.total}`
                                )
                              : tr('Проверяем...', 'Pruefen...'))
                          : isStorySession
                            ? tr('Проверить историю', 'Story pruefen')
                            : tr('Проверить перевод', 'Uebersetzung pruefen')}
                      </button>
                      {webappLoading && translationCheckProgress.total > 0 && (
                        <div className="webapp-muted">
                          {tr(
                            `Готово ${translationCheckProgress.done} из ${translationCheckProgress.total}`,
                            `${translationCheckProgress.done} von ${translationCheckProgress.total} fertig`
                          )}
                        </div>
                      )}
                    </>
                  )}
                </form>
                )}

                {webappError && <div className="webapp-error">{webappError}</div>}
                {finishMessage && <div className="webapp-success">{finishMessage}</div>}

                {isStoryResultMode && (
                  <section className="webapp-result">
                    <h3>{tr('Результат истории', 'Story-Ergebnis')}</h3>
                    <div className="webapp-result-card story-result">
                      <div className="story-result-head">
                        <strong>{tr('⭐ Итоговый балл:', '⭐ Gesamtscore:')}</strong> {storyResult.score ?? '—'} / 100
                      </div>

                      {storyResult.feedback && renderStoryFeedback(storyResult.feedback)}

                      <div className="webapp-result-text story-result-answer">
                        <div><strong>{tr('🎯 Ответ пользователя:', '🎯 Antwort des Nutzers:')}</strong> {storyResult.guess_correct ? tr('верно по смыслу', 'inhaltlich richtig') : tr('неверно по смыслу', 'inhaltlich falsch')}</div>
                        <div><strong>{tr('✅ Эталон:', '✅ Referenz:')}</strong> {storyResult.answer || '—'}</div>
                        {storyResult.guess_reason && (
                          <div><strong>{tr('📝 Пояснение:', '📝 Erklaerung:')}</strong> {storyResult.guess_reason}</div>
                        )}
                      </div>

                      {storyResult.extra_de && (
                        <div className="webapp-result-text story-result-extra">
                          <strong>{tr('📌 Дополнительно (DE):', '📌 Zusaetzlich (DE):')}</strong> {storyResult.extra_de}
                        </div>
                      )}
                      {Array.isArray(storyResult.source_links) && storyResult.source_links.length > 0 && (
                        <div className="webapp-result-text story-result-links">
                          <strong>{tr('🔗 Официальные источники:', '🔗 Offizielle Quellen:')}</strong>
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
                    <h3>{tr('Результат проверки', 'Pruefungsergebnis')}</h3>
                    <div className="webapp-result-list">
                      {results.map((item, index) => {
                        const correctTextForTts = extractCorrectTranslationText(item);
                        return (
                        <div key={`${item.check_item_id ?? item.translation_id ?? item.sentence_id_for_mistake_table ?? item.sentence_number ?? index}-${index}`} className="webapp-result-card">
                          {item.error ? (
                            <div className="webapp-error">{item.error}</div>
                          ) : (
                            <>
                              {Number(item?.translation_id || 0) > 0 && (
                                <>
                                  {/*
                                  <label className="result-audio-optin">
                                    <span>{tr('Аудио-объяснение для этого предложения', 'Audio-Erklaerung fuer diesen Satz')}</span>
                                    <input
                                      type="checkbox"
                                      checked={Boolean(
                                        translationAudioGrammarOptIn[Number(item.translation_id)] ?? item?.audio_grammar_opt_in
                                      )}
                                      onChange={(event) => handleToggleResultAudioGrammar(item, event.target.checked)}
                                      disabled={Boolean(translationAudioGrammarSaving[Number(item.translation_id)])}
                                    />
                                  </label>
                                  */}
                                </>
                              )}
                              <div
                                className="webapp-result-text"
                                onMouseUp={handleSelection}
                                onTouchEnd={handleSelection}
                              >
                                {renderFeedback(item.feedback)}
                              </div>
                              {correctTextForTts && (
                                <div className="result-inline-audio-row">
                                  {(() => {
                                    const ttsKey = `result-correct-${item.translation_id || item.sentence_number || index}`;
                                    const loading = isTtsPending(ttsKey);
                                    return (
                                      <>
                                  <span className="webapp-muted">
                                    {tr('Озвучить корректный вариант', 'Korrekte Version vorlesen')}
                                  </span>
                                  <button
                                    type="button"
                                    className={`inline-tts-button ${loading ? 'is-loading' : ''}`}
                                    onClick={() => {
                                      void playTtsWithUi(ttsKey, correctTextForTts, getLearningTtsLocale());
                                    }}
                                    aria-label={tr('Озвучить корректный вариант', 'Korrekte Version vorlesen')}
                                    title={tr('Озвучить корректный вариант', 'Korrekte Version vorlesen')}
                                    disabled={loading}
                                  >
                                    {renderTtsButtonContent(loading)}
                                  </button>
                                  {loading && (
                                    <span className="tts-loading-note">
                                      {tr('Озвучиваем...', 'Wird vorgelesen...')}
                                    </span>
                                  )}
                                      </>
                                    );
                                  })()}
                                </div>
                              )}
                              <button
                                type="button"
                                className="secondary-button explanation-button"
                                onClick={() => handleExplainTranslation(item)}
                                disabled={explanationLoading[String(item.sentence_number ?? item.original_text)]}
                              >
                                {explanationLoading[String(item.sentence_number ?? item.original_text)]
                                  ? tr('Запрашиваем объяснение...', 'Erklaerung wird angefragt...')
                                  : tr('Объяснить ошибки', 'Fehler erklaeren')}
                              </button>
                              {explanations[String(item.sentence_number ?? item.original_text)] && (
                                <div
                                  className="webapp-explanation"
                                  dangerouslySetInnerHTML={{
                                    __html: renderExplanationRichText(
                                      explanations[String(item.sentence_number ?? item.original_text)]
                                    ),
                                  }}
                                />
                              )}
                            </>
                          )}
                        </div>
                      )})}
                    </div>
                  </section>
                )}

                <div className="webapp-actions webapp-actions-footer">
                  {sentences.length === 0 && !webappLoading && (
                    <div className="webapp-muted">
                      {tr('Если сессия зависла, можно завершить её вручную.', 'Wenn die Session haengt, kann sie manuell beendet werden.')}
                    </div>
                  )}
                  <button
                    type="button"
                    onClick={handleFinishTranslation}
                    className={`primary-button finish-button ${finishStatus === 'done' ? 'status-done' : ''}`}
                    disabled={webappLoading || ((results.length === 0 && !storyResult) && sentences.length > 0)}
                  >
                    {finishStatus === 'done' ? tr('Завершено', 'Abgeschlossen') : tr('Завершить перевод', 'Uebersetzung beenden')}
                  </button>
                  <button
                    type="button"
                    onClick={handleLoadDailyHistory}
                    className="secondary-button"
                    disabled={webappLoading || historyLoading}
                  >
                    {historyLoading
                      ? tr('Загружаем...', 'Laden...')
                      : historyVisible
                        ? tr('Скрыть результаты', 'Ergebnisse ausblenden')
                        : tr('Посмотреть результат за сегодня', 'Ergebnis fuer heute anzeigen')}
                  </button>
                  {results.length === 0 && !storyResult && !webappLoading && (
                    <div className="webapp-muted">{tr('Сначала проверьте перевод, чтобы завершить.', 'Bitte erst pruefen, dann beenden.')}</div>
                  )}
                </div>

                {historyError && <div className="webapp-error">{historyError}</div>}

                {historyVisible && (
                  <section className="webapp-result">
                    <h3>{tr('История переводов за сегодня', 'Uebersetzungsverlauf fuer heute')}</h3>
                    <p className="webapp-muted">{tr('Языковая пара', 'Sprachpaar')}: {getActiveLanguagePairLabel()}</p>
                    {historyItems.length === 0 ? (
                      <p className="webapp-muted">{tr('Сегодня пока нет завершённых переводов.', 'Heute gibt es noch keine abgeschlossenen Uebersetzungen.')}</p>
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
                  <section className={`webapp-video youtube-player-first ${youtubeLearningMode ? 'is-learning' : 'is-setup'} ${youtubeAppFullscreen ? 'is-app-fullscreen-active' : ''}`} ref={youtubeRef}>
                    <div className="webapp-local-section-head youtube-player-first-head">
                      <div className="youtube-desktop-command-bar">
                        <div className="youtube-desktop-command-row youtube-desktop-command-row-top">
                          <div className="youtube-desktop-mainline">
                            <div className="youtube-player-first-title-wrap">
                              <h3>{tr('Видео YouTube', 'YouTube Video')}</h3>
                              {youtubeTaskDone && (
                                <span className="youtube-inline-done" title={tr('Задача выполнена', 'Aufgabe erledigt')}>✅</span>
                              )}
                            </div>
                            {!youtubeTaskDone && renderTodaySectionTaskHud('youtube', { inline: true })}
                            <button type="button" className="youtube-command-action youtube-command-home-btn" onClick={goHomeScreen}>
                              {tr('← На главную', '← Startseite')}
                            </button>
                          </div>
                          <div className="youtube-desktop-status-chips" aria-live="polite">
                            <span className={`youtube-status-chip ${youtubeSubtitleStatusClass}`}>{youtubeSubtitleStatusLabel}</span>
                            <span className={`youtube-status-chip ${youtubeTranslationEnabled ? 'is-ready' : 'is-empty'}`}>
                              {youtubeTranslationEnabled ? tr('RU: ON', 'RU: ON') : tr('RU: OFF', 'RU: OFF')}
                            </span>
                            <span className={`youtube-status-chip ${youtubeOverlayEnabled ? 'is-ready' : 'is-empty'}`}>
                              {youtubeOverlayEnabled ? 'Overlay: ON' : 'Overlay: OFF'}
                            </span>
                          </div>
                        </div>
                        <div className="youtube-desktop-command-row youtube-desktop-command-row-controls">
                          <div className="youtube-desktop-control-group youtube-desktop-control-group-source">
                            <button
                              type="button"
                              className="youtube-command-action"
                              onClick={() => setYoutubeForceShowPanel(true)}
                            >
                              {tr('Сменить видео', 'Change video')}
                            </button>
                            <button
                              type="button"
                              className="youtube-command-action"
                              onClick={() => {
                                setYoutubeForceShowPanel(true);
                                if (youtubeInput.trim()) searchYoutubeVideos();
                              }}
                              disabled={youtubeSearchLoading}
                            >
                              {youtubeSearchLoading ? tr('Ищем...', 'Searching...') : tr('Искать в YouTube', 'Search on YouTube')}
                            </button>
                            <button
                              type="button"
                              className={`youtube-command-action ${showManualTranscript ? 'is-active' : ''}`}
                              onClick={() => setShowManualTranscript((prev) => !prev)}
                            >
                              {tr('Вставить транскрипцию', 'Paste transcript')}
                            </button>
                            <button
                              type="button"
                              className="youtube-command-action"
                              onClick={() => fetchTranscript()}
                              disabled={youtubeLoadDisabled}
                            >
                              {youtubeTranscriptLoading
                                ? tr('Загрузка...', 'Loading...')
                                : youtubeSubtitlesReady
                                  ? tr('Перезагрузить субтитры', 'Reload subtitles')
                                  : tr('Загрузить субтитры', 'Load subtitles')}
                            </button>
                          </div>
                          <div className="youtube-desktop-control-group youtube-desktop-control-group-view">
                            <button
                              type="button"
                              className={`youtube-command-toggle ${youtubeTranslationEnabled ? 'is-active' : ''}`}
                              onClick={() => setYoutubeTranslationEnabled((prev) => !prev)}
                              disabled={!youtubeSubtitlesReady}
                            >
                              {tr('Показать RU', 'Show RU')}
                            </button>
                            <button
                              type="button"
                              className={`youtube-command-toggle ${youtubeOverlayEnabled ? 'is-active' : ''}`}
                              onClick={() => setYoutubeOverlayEnabled((prev) => !prev)}
                              disabled={!youtubeSubtitlesReady}
                            >
                              Overlay
                            </button>
                            <button
                              type="button"
                              className={`youtube-command-toggle ${youtubeAppFullscreen ? 'is-active' : ''}`}
                              onClick={() => setYoutubeAppFullscreen((prev) => !prev)}
                              disabled={!youtubeId}
                            >
                              {tr('Развернуть во весь экран', 'Full screen')}
                            </button>
                          </div>
                        </div>
                        {showManualTranscript && (
                          <div className="webapp-subtitles-manual youtube-sheet-manual youtube-sheet-manual-desktop">
                            <textarea
                              rows={6}
                              value={manualTranscript}
                              onChange={(event) => setManualTranscript(event.target.value)}
                              placeholder={tr('Вставьте .srt/.vtt с таймкодами. Если таймкодов нет, покажем статично.', 'Paste .srt/.vtt with timecodes. Without timecodes we show static lines.')}
                            />
                            <div className="webapp-video-actions">
                              <button
                                type="button"
                                className="secondary-button"
                                onClick={() => handleManualTranscript()}
                              >
                                {tr('Использовать транскрипцию', 'Use transcript')}
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
                                  {tr('Вернуться к авто', 'Back to auto')}
                                </button>
                              )}
                            </div>
                          </div>
                        )}
                      </div>

                      <div className="youtube-mobile-head">
                        <div className="youtube-player-first-title-row">
                          <div className="youtube-player-first-title-wrap">
                            <h3>{tr('Видео YouTube', 'YouTube Video')}</h3>
                            {youtubeTaskDone && (
                              <span className="youtube-inline-done" title={tr('Задача выполнена', 'Aufgabe erledigt')}>✅</span>
                            )}
                          </div>
                          <div className="youtube-player-first-title-actions">
                            {!youtubeTaskDone && renderTodaySectionTaskHud('youtube', { inline: true })}
                            <button
                              type="button"
                              className="youtube-dock-icon-btn youtube-head-settings-btn"
                              onClick={() => setYoutubeSettingsOpen(true)}
                              aria-label={tr('Настройки', 'Settings')}
                              title={tr('Настройки', 'Settings')}
                            >
                              <span aria-hidden="true">&#9881;</span>
                            </button>
                          </div>
                        </div>
                        {isFocusedSection('youtube') && (
                          <div className="section-head-nav">
                            <button
                              type="button"
                              className="section-home-back is-compact-arrow"
                              onClick={goBackFromYoutube}
                              title={tr('Назад', 'Zurueck')}
                              aria-label={tr('Назад', 'Zurueck')}
                            >
                              ⏪
                            </button>
                            <button type="button" className="section-home-back" onClick={goHomeScreen}>
                              {tr('На главную', 'Startseite')}
                            </button>
                          </div>
                        )}
                        <div className="youtube-player-first-head-controls">
                          <button
                            type="button"
                            className={`youtube-status-action-btn ${youtubeOverlayEnabled ? 'is-active' : ''}`}
                            onClick={() => setYoutubeOverlayEnabled((prev) => !prev)}
                            disabled={!youtubeSubtitlesReady}
                          >
                            {youtubeOverlayEnabled ? 'Overlay: ON' : 'Overlay: OFF'}
                          </button>
                          <button
                            type="button"
                            className={`youtube-status-action-btn ${youtubeTranslationEnabled ? 'is-active' : ''}`}
                            onClick={() => setYoutubeTranslationEnabled((prev) => !prev)}
                            disabled={!youtubeSubtitlesReady}
                          >
                            {youtubeTranslationEnabled ? tr('RU: ON', 'RU: ON') : tr('RU: OFF', 'RU: OFF')}
                          </button>
                          <button
                            type="button"
                            className="youtube-status-action-btn youtube-status-load-btn"
                            onClick={() => fetchTranscript()}
                            disabled={youtubeLoadDisabled}
                          >
                            {youtubeTranscriptLoading
                              ? tr('Загружаем...', 'Loading...')
                              : tr('Загрузить субтитры', 'Load subtitles')}
                          </button>
                          <button
                            type="button"
                            className={`youtube-status-action-btn ${youtubeAppFullscreen ? 'is-active' : ''}`}
                            onClick={() => {
                              setYoutubeAppFullscreen((prev) => !prev);
                              setYoutubeSettingsOpen(false);
                            }}
                            disabled={!youtubeId}
                          >
                            {youtubeAppFullscreen
                              ? tr('Full screen: ON', 'Full screen: ON')
                              : tr('Full screen: OFF', 'Full screen: OFF')}
                          </button>
                        </div>
                      </div>
                    </div>
                    <div className="youtube-player-card">
                      <div
                        ref={youtubePlayerShellRef}
                        className={`webapp-video-player-shell ${youtubeAppFullscreen ? 'is-app-fullscreen' : ''}`}
                      >
                        {youtubeAppFullscreen && (
                          <button
                            type="button"
                            className="youtube-app-fullscreen-close"
                            onClick={() => setYoutubeAppFullscreen(false)}
                          >
                            {tr('Свернуть', 'Minimieren')}
                          </button>
                        )}
                        <div className={`webapp-video-frame youtube-player-frame ${videoExpanded ? 'is-expanded' : ''} ${youtubeOverlayEnabled ? 'has-overlay' : ''}`}>
                          <div
                            id="youtube-player"
                            className="youtube-player-host"
                            data-video-id={youtubeId}
                          />
                          {!youtubeId && (
                            <div className="youtube-player-placeholder">
                              <strong>{tr('Плеер YouTube', 'YouTube Player')}</strong>
                              <span>{tr('Выберите видео, чтобы начать обучение.', 'Waehle ein Video, um zu starten.')}</span>
                            </div>
                          )}
                          {!youtubePlayerReady && youtubeId && (
                            <iframe
                              title="YouTube player"
                              src={`https://www.youtube.com/embed/${youtubeId}?rel=0&modestbranding=1&fs=0&disablekb=1&playsinline=1`}
                              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                            />
                          )}
                          {youtubeOverlayEnabled && youtubeTranscript.length > 0 && (() => {
                            const activeIndex = getActiveSubtitleIndex();
                            const resolvedIndex = activeIndex >= 0 ? activeIndex : 0;
                            const showPausedHistory = youtubeAppFullscreen && youtubeIsPaused;
                            const overlayIndexes = showPausedHistory
                              ? [resolvedIndex - 2, resolvedIndex - 1, resolvedIndex].filter((idx) => idx >= 0)
                              : [resolvedIndex];
                            return (
                              <div className={`youtube-subtitles-overlay ${showPausedHistory ? 'is-paused-history' : ''}`} aria-hidden="true">
                                {overlayIndexes.map((idx) => {
                                  const item = youtubeTranscript[idx];
                                  const overlayDeText = normalizeSubtitleText(item?.text || '');
                                  const overlayTranslationText = (youtubeTranslations[String(idx)] || '').trim();
                                  if (!overlayDeText && !(youtubeTranslationEnabled && overlayTranslationText)) {
                                    return null;
                                  }
                                  const isCurrent = idx === resolvedIndex;
                                  return (
                                    <div key={`overlay-line-${idx}`} className={`youtube-subtitles-overlay-row ${isCurrent ? 'is-current' : 'is-history'}`}>
                                      {overlayDeText && (
                                        <p
                                          className="youtube-subtitles-overlay-line is-target-language"
                                          onClick={(event) => openYoutubeSentenceSelection(event, overlayDeText, 'youtube_overlay_sentence')}
                                        >
                                          {renderClickableText(overlayDeText, {
                                            className: 'overlay-clickable-word',
                                            compact: true,
                                            inlineLookup: true,
                                            lookupLang: getNormalizeLookupLang(),
                                            selectionType: 'youtube_overlay_word',
                                            stopPropagation: true,
                                          })}
                                        </p>
                                      )}
                                      {youtubeTranslationEnabled && overlayTranslationText && (
                                        <p className="youtube-subtitles-overlay-line is-user-language">{overlayTranslationText}</p>
                                      )}
                                    </div>
                                  );
                                })}
                              </div>
                            );
                          })()}
                        </div>
                      </div>
                    </div>
                    {(youtubeOverlayEnabled || !youtubeSubtitlesReady) && renderYoutubeSentenceJumpBar()}
                    {youtubeError && <div className="webapp-error">{youtubeError}</div>}
                    {youtubeTranscriptError && <div className="webapp-error">{youtubeTranscriptError}</div>}
                    {youtubeSearchExpanded && (
                      <div className="webapp-video-form youtube-setup-form">
                        <label className="webapp-field">
                          <span>{tr('Ссылка, ID или поисковый запрос', 'Link, Video-ID oder Suchanfrage')}</span>
                          <div className="input-clear-wrap">
                            <input
                              type="text"
                              className="input-clear-field"
                              value={youtubeInput}
                              onChange={(event) => setYoutubeInput(event.target.value)}
                              onKeyDown={(event) => {
                                if (event.key === 'Enter') {
                                  event.preventDefault();
                                  searchYoutubeVideos();
                                }
                              }}
                              placeholder={tr('https://youtu.be/VIDEO_ID или Deutsch Grammatik B1', 'https://youtu.be/VIDEO_ID oder Deutsch Grammatik B1')}
                            />
                            {youtubeInput && (
                              <button
                                type="button"
                                className="input-clear-btn"
                                onClick={() => setYoutubeInput('')}
                                aria-label={tr('Очистить', 'Loeschen')}
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
                            onClick={searchYoutubeVideos}
                            disabled={!youtubeInput.trim() || youtubeSearchLoading}
                          >
                            {youtubeSearchLoading
                              ? tr('Ищем в YouTube...', 'Suche auf YouTube...')
                              : tr('Искать в YouTube', 'Auf YouTube suchen')}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => setVideoExpanded((prev) => !prev)}
                            style={{ display: 'none' }}
                          >
                            {videoExpanded ? tr('Обычный режим', 'Normalmodus') : tr('Словарь рядом', 'Woerterbuch daneben')}
                          </button>
                        </div>
                        {youtubeSearchError && <div className="webapp-error">{youtubeSearchError}</div>}
                        {youtubeSearchResults.length > 0 && (
                          <div className="youtube-search-results">
                            {youtubeSearchResults.map((item) => (
                              <button
                                type="button"
                                key={item.video_id}
                                className="youtube-search-item"
                                onClick={() => {
                                  setYoutubeInput(item.video_url || `https://youtu.be/${item.video_id}`);
                                  setYoutubeSearchResults([]);
                                  setYoutubeSearchError('');
                                }}
                              >
                                <img
                                  src={item.thumbnail || `https://i.ytimg.com/vi/${item.video_id}/mqdefault.jpg`}
                                  alt=""
                                  loading="lazy"
                                />
                                <span>{item.title || item.video_id}</span>
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                    {!youtubeOverlayEnabled && youtubeSubtitlesReady && (
                      <div className="youtube-subtitles-card youtube-subtitles-panel">
                        <div className="youtube-subtitles-panel-head">
                          <div className="youtube-subtitles-panel-copy">
                            <strong>{tr('Субтитры', 'Subtitles')}</strong>
                            <span>{String(languageProfile?.learning_language || 'de').toUpperCase()} · {tr('всегда ON', 'always ON')}</span>
                          </div>
                        </div>
                        <div className={`youtube-subtitles-panel-content ${youtubeTranslationEnabled ? 'is-dual' : 'is-single'}`}>
                          <div className="youtube-subtitles-block youtube-subtitles-block-de">
                            <div className="youtube-subtitles-card-head youtube-subtitles-card-head-with-nav">
                              <div className="youtube-subtitles-card-badge">
                                <span>{String(languageProfile?.learning_language || 'de').toUpperCase()}</span>
                              </div>
                              <div className="youtube-subtitles-card-nav">
                                {renderYoutubeSentenceJumpBar({ inline: true })}
                              </div>
                              <div className="youtube-subtitles-card-head-spacer" aria-hidden="true" />
                            </div>
                            <div className="webapp-subtitles" ref={youtubeSubtitlesRef}>
                              <div className="webapp-subtitles-list" onMouseUp={handleSelection}>
                                {(() => {
                                  const activeIndex = getActiveSubtitleIndex();
                                  return youtubeTranscript.map((item, index) => (
                                    <p
                                      key={`${item.start}-${index}`}
                                      className={index === activeIndex ? 'is-active' : ''}
                                      onClick={(event) => openYoutubeSentenceSelection(event, item.text, 'youtube_sentence')}
                                    >
                                      {renderSubtitleText(item.text)}
                                    </p>
                                  ));
                                })()}
                              </div>
                            </div>
                          </div>
                          {youtubeTranslationEnabled && (
                            <div className="youtube-subtitles-block youtube-subtitles-block-ru">
                              <div className="youtube-subtitles-card-head">
                                <span>{getNativeSubtitleCode()}</span>
                              </div>
                              <div className="webapp-subtitles is-translation">
                                <div className="webapp-subtitles-list">
                                  {(() => {
                                    const activeIndex = getActiveSubtitleIndex();
                                    return youtubeTranscript.map((item, index) => {
                                      const translation = youtubeTranslations[String(index)] || '…';
                                      return (
                                        <p
                                          key={`translation-${item.start}-${index}`}
                                          className={index === activeIndex ? 'is-active' : ''}
                                        >
                                          {translation}
                                        </p>
                                      );
                                    });
                                  })()}
                                </div>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                    {youtubeSettingsOpen && (
                      <div className="youtube-settings-overlay" onClick={() => setYoutubeSettingsOpen(false)}>
                        <div
                          className="youtube-settings-sheet"
                          role="dialog"
                          aria-modal="true"
                          aria-label={tr('Настройки YouTube', 'YouTube settings')}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <div className="youtube-settings-sheet-head">
                            <strong>{tr('Настройки YouTube', 'YouTube settings')}</strong>
                            <button
                              type="button"
                              className="youtube-dock-icon-btn youtube-settings-close"
                              onClick={() => setYoutubeSettingsOpen(false)}
                              aria-label={tr('Закрыть', 'Close')}
                            >
                              ×
                            </button>
                          </div>
                          <div className="youtube-settings-sheet-list">
                            <button
                              type="button"
                              className="youtube-settings-row"
                              onClick={() => {
                                setYoutubeForceShowPanel(true);
                                setYoutubeSettingsOpen(false);
                              }}
                            >
                              <span>{tr('Сменить видео', 'Change video')}</span>
                              <span>{tr('Открыть поиск', 'Open search')}</span>
                            </button>
                            <button
                              type="button"
                              className="youtube-settings-row"
                              onClick={searchYoutubeVideos}
                              disabled={!youtubeInput.trim() || youtubeSearchLoading}
                            >
                              <span>{tr('Искать в YouTube', 'Search on YouTube')}</span>
                              <span>{youtubeSearchLoading ? tr('Загрузка...', 'Loading...') : tr('Запустить', 'Run')}</span>
                            </button>
                            <button
                              type="button"
                              className={`youtube-settings-row ${showManualTranscript ? 'is-active' : ''}`}
                              onClick={() => setShowManualTranscript((prev) => !prev)}
                            >
                              <span>{tr('Вставить транскрипцию', 'Paste transcript')}</span>
                              <span>{showManualTranscript ? tr('ON', 'ON') : tr('OFF', 'OFF')}</span>
                            </button>
                            <button
                              type="button"
                              className={`youtube-settings-row ${youtubeTranslationEnabled ? 'is-active' : ''}`}
                              onClick={() => setYoutubeTranslationEnabled((prev) => !prev)}
                              disabled={!youtubeSubtitlesReady}
                            >
                              <span>{tr('Показать RU', 'Show RU')}</span>
                              <span>{youtubeTranslationEnabled ? tr('ON', 'ON') : tr('OFF', 'OFF')}</span>
                            </button>
                            <button
                              type="button"
                              className={`youtube-settings-row ${youtubeOverlayEnabled ? 'is-active' : ''}`}
                              onClick={() => setYoutubeOverlayEnabled((prev) => !prev)}
                              disabled={!youtubeSubtitlesReady}
                            >
                              <span>Overlay</span>
                              <span>{youtubeOverlayEnabled ? 'ON' : 'OFF'}</span>
                            </button>
                            <button
                              type="button"
                              className={`youtube-settings-row ${youtubeAppFullscreen ? 'is-active' : ''}`}
                              onClick={() => {
                                setYoutubeAppFullscreen((prev) => !prev);
                                setYoutubeSettingsOpen(false);
                              }}
                              disabled={!youtubeId}
                            >
                              <span>{tr('Развернуть во весь экран', 'Full screen')}</span>
                              <span>{youtubeAppFullscreen ? tr('ON', 'ON') : tr('OFF', 'OFF')}</span>
                            </button>
                            <button
                              type="button"
                              className="youtube-settings-row"
                              onClick={() => fetchTranscript()}
                              disabled={youtubeLoadDisabled}
                            >
                              <span>{youtubeSubtitlesReady ? tr('Перезагрузить субтитры', 'Reload subtitles') : tr('Загрузить оригинальные субтитры', 'Load original subtitles')}</span>
                              <span>{youtubeSubtitlesReady ? tr('Готово', 'Ready') : tr('Не загружено', 'Not loaded')}</span>
                            </button>
                          </div>
                          {showManualTranscript && (
                            <div className="webapp-subtitles-manual youtube-sheet-manual">
                              <textarea
                                rows={6}
                                value={manualTranscript}
                                onChange={(event) => setManualTranscript(event.target.value)}
                                placeholder={tr('Вставьте .srt/.vtt с таймкодами. Если таймкодов нет, покажем статично.', 'Paste .srt/.vtt with timecodes. Without timecodes we show static lines.')}
                              />
                              <div className="webapp-video-actions">
                                <button
                                  type="button"
                                  className="secondary-button"
                                  onClick={() => handleManualTranscript()}
                                >
                                  {tr('Использовать транскрипцию', 'Use transcript')}
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
                                    {tr('Вернуться к авто', 'Back to auto')}
                                  </button>
                                )}
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </section>
                )}

                {isSectionVisible('dictionary') && (
                  <section className="webapp-dictionary" ref={dictionaryRef}>
                    <div className="webapp-local-section-head">
                      <h3>{tr('Словарь', 'Woerterbuch')}</h3>
                      <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                    </div>
                    <form className="webapp-dictionary-form" onSubmit={handleDictionaryLookup}>
                      <label className="webapp-field">
                        <span>{tr('Слово или фраза', 'Wort oder Phrase')}</span>
                        <div className="dictionary-input-wrap">
                          <input
                            className="dictionary-input"
                            type="text"
                            value={dictionaryWord}
                            onChange={(event) => setDictionaryWord(event.target.value)}
                            placeholder={tr('Например: слово, фраза или выражение', 'Zum Beispiel: Wort, Phrase oder Ausdruck')}
                          />
                          {dictionaryWord.trim() && (
                            <div className="dictionary-input-tools">
                              <button
                                type="button"
                                className="dictionary-clear"
                                onClick={() => setDictionaryWord('')}
                                aria-label={tr('Очистить слово', 'Wort loeschen')}
                              >
                                ×
                              </button>
                            </div>
                          )}
                        </div>
                      </label>
                      <div className="dictionary-actions">
                        <button
                          className="secondary-button dictionary-fast-button"
                          type="button"
                          onClick={handleDictionaryQuickLookup}
                          disabled={dictionaryLoading}
                        >
                          {dictionaryLoading && dictionaryLookupMode === 'quick'
                            ? tr('Быстро...', 'Schnell...')
                            : tr('Быстрый перевод', 'Schnell')}
                        </button>
                        <button className="secondary-button dictionary-button" type="submit" disabled={dictionaryLoading}>
                          {dictionaryLoading && dictionaryLookupMode === 'gpt'
                            ? tr('GPT...', 'GPT...')
                            : tr('GPT-разбор', 'Mit GPT')}
                        </button>
                        {lastLookupScrollY !== null && (
                          <button
                            type="button"
                            className="secondary-button dictionary-back-icon"
                            onClick={() => window.scrollTo({ top: lastLookupScrollY, behavior: 'smooth' })}
                          >
                            {tr('↙ к предложению', '↙ zum Satz')}
                          </button>
                        )}
                        <button
                          className="secondary-button dictionary-save-button"
                          type="button"
                          onClick={handleDictionarySave}
                          disabled={dictionaryLoading || !dictionaryResult}
                        >
                          {tr('В словарь', 'Speichern')}
                        </button>
                      </div>
                    </form>

                    <div className="folder-panel">
                      <div className="folder-row">
                        <label className="webapp-field folder-select">
                          <span>{tr('Папка для сохранения', 'Speicherordner')}</span>
                          <select
                            value={dictionaryFolderId}
                            onChange={(event) => setDictionaryFolderId(event.target.value)}
                          >
                            <option value="none">{tr('Без папки', 'Ohne Ordner')}</option>
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
                          {tr('Новая папка', 'Neuer Ordner')}
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={handleExportDictionaryPdf}
                          disabled={exportLoading}
                        >
                          {exportLoading ? tr('Готовим PDF...', 'PDF wird erstellt...') : tr('Выгрузить PDF', 'PDF exportieren')}
                        </button>
                      </div>
                      {dictionaryPdfUrl && (
                        <div className="dictionary-pdf-panel">
                          <div className="dictionary-pdf-title">
                            {tr('PDF готов', 'PDF ist bereit')}
                          </div>
                          <div className="dictionary-pdf-actions">
                            <a
                              href={dictionaryPdfUrl}
                              download={dictionaryPdfName || 'dictionary.pdf'}
                              className="secondary-button"
                            >
                              {tr('Скачать PDF', 'PDF herunterladen')}
                            </a>
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={handleOpenDictionaryPdf}
                            >
                              {tr('Открыть PDF', 'PDF oeffnen')}
                            </button>
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={handleCloseDictionaryPdf}
                            >
                              {tr('← Назад', '← Zurueck')}
                            </button>
                          </div>
                        </div>
                      )}
                      {showNewFolderForm && (
                        <div className="folder-create">
                          <label className="webapp-field">
                            <span>{tr('Название', 'Name')}</span>
                            <input
                              type="text"
                              value={newFolderName}
                              onChange={(event) => setNewFolderName(event.target.value)}
                              placeholder={tr('Например: Путешествия', 'Zum Beispiel: Reisen')}
                            />
                          </label>
                          <div className="folder-pickers">
                            <div className="folder-picker">
                              <span>{tr('Цвет', 'Farbe')}</span>
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
                              <span>{tr('Иконка', 'Icon')}</span>
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
                            {foldersLoading ? tr('Создаём...', 'Erstellen...') : tr('Создать папку', 'Ordner erstellen')}
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
                            {tr('← вернуться назад', '← zurueck')}
                          </button>
                        )}
                        <div className="dictionary-row">
                          <span className="dictionary-label">{tr('Перевод:', 'Uebersetzung:')}</span>
                          <span className="dictionary-translation">
                            {getDictionaryDisplayedTranslation(dictionaryResult) || '—'}
                          </span>
                          {(() => {
                            const targetTts = resolveDictionaryTargetTts(dictionaryResult, dictionaryDirection);
                            if (!targetTts.text) return null;
                            const ttsKey = `dictionary-target-${String(dictionaryDirection || 'default')}`;
                            const loading = isTtsPending(ttsKey);
                            return (
                              <button
                                type="button"
                                className={`inline-tts-button ${loading ? 'is-loading' : ''}`}
                                onClick={() => {
                                  void playTtsWithUi(ttsKey, targetTts.text, targetTts.locale);
                                }}
                                aria-label={tr('Озвучить на изучаемом языке', 'In der Zielsprache vorlesen')}
                                title={tr('Озвучить на изучаемом языке', 'In der Zielsprache vorlesen')}
                                disabled={loading}
                              >
                                {renderTtsButtonContent(loading)}
                              </button>
                            );
                          })()}
                          <span className="dictionary-direction">
                            {getLookupDirectionLabel()}
                          </span>
                          {!!String(dictionaryResult?.provider || '').trim() && (
                            <span className="dictionary-direction">
                              {String(dictionaryResult.provider).toUpperCase()}
                            </span>
                          )}
                        </div>
                        <div className="dictionary-row">
                          <strong>{tr('Часть речи:', 'Wortart:')}</strong> {dictionaryResult.part_of_speech || '—'}
                        </div>
                        {dictionaryResult.article && (
                          <div className="dictionary-row">
                            <strong>{tr('Артикль:', 'Artikel:')}</strong> {dictionaryResult.article}
                          </div>
                        )}
                        {getDictionaryFormRows(dictionaryResult).length > 0 && (
                          <div className="dictionary-forms">
                            {getDictionaryFormRows(dictionaryResult).map((row) => (
                              <div key={row.label}><strong>{row.label}:</strong> {row.value}</div>
                            ))}
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.prefixes) && dictionaryResult.prefixes.length > 0 && (
                          <div className="dictionary-prefixes">
                            <strong>{tr('Префиксы/варианты:', 'Praefixe/Varianten:')}</strong>
                            <ul>
                              {dictionaryResult.prefixes.map((item, index) => (
                                <li key={`${item.variant}-${index}`}>
                                  <div>
                                    <strong>{item.variant}:</strong> {item.translation_target || item.translation_de || item.translation_ru || '—'}
                                  </div>
                                  {item.explanation && <div>{item.explanation}</div>}
                                  {(item.example_target || item.example_de) && <div><em>{item.example_target || item.example_de}</em></div>}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.usage_examples) && dictionaryResult.usage_examples.length > 0 && (
                          <div className="dictionary-examples">
                            <strong>{tr('Примеры:', 'Beispiele:')}</strong>
                            <ul>
                              {dictionaryResult.usage_examples.map((example, index) => {
                                let exampleText = '';
                                if (typeof example === 'string') {
                                  exampleText = example.trim();
                                } else if (example && typeof example === 'object') {
                                  const source = String(example.source || '').trim();
                                  const target = String(example.target || '').trim();
                                  if (source && target) {
                                    exampleText = `${source} → ${target}`;
                                  } else {
                                    exampleText = source || target;
                                  }
                                }
                                if (!exampleText) return null;
                                return <li key={`example-${index}-${exampleText}`}>{exampleText}</li>;
                              })}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                    {collocationsVisible && (
                      <div className="dictionary-collocations">
                        <h4>{tr('Выберите связку для словаря', 'Waehle eine Kollokation')}</h4>
                        {collocationsLoading && <div className="webapp-muted">{tr('Генерируем варианты...', 'Varianten werden generiert...')}</div>}
                        {collocationsError && <div className="webapp-error">{collocationsError}</div>}
                        {!collocationsLoading && collocationOptions.length > 0 && (
                          <div className="collocation-list">
                            {collocationOptions.map((option, index) => (
                              <label key={`${option.source}-${index}`} className="collocation-item">
                                <input
                                  type="checkbox"
                                  checked={selectedCollocations.includes(`${String(option.source)}|||${String(option.target)}`)}
                                  onChange={() => {
                                    const optionKey = `${String(option.source)}|||${String(option.target)}`;
                                    setSelectedCollocations((prev) => (
                                      prev.includes(optionKey)
                                        ? prev.filter((key) => key !== optionKey)
                                        : [...prev, optionKey]
                                    ));
                                  }}
                                />
                                <div>
                                  <div className="collocation-source">{option.source}</div>
                                  <div className="collocation-target">{option.target}</div>
                                  {option.isBase && <span className="collocation-tag">{tr('Исходное', 'Basis')}</span>}
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
                            {dictionaryLoading ? tr('Сохраняем...', 'Speichern...') : tr('Добавить выбранное', 'Ausgewaehltes hinzufuegen')}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => setCollocationsVisible(false)}
                          >
                            {tr('Отмена', 'Abbrechen')}
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
                        {tr('Повторить слова', 'Woerter wiederholen')}
                      </button>
                    </div>
                  </section>
                )}
              </div>
            )}

            {!flashcardsOnly && isSectionVisible('reader') && (
              <section className={`webapp-section webapp-reader ${readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : ''} ${readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : ''}`} ref={readerRef}>
                {(() => {
                  const showLibraryMode = !readerHasContent || readerArchiveOpen || !readerImmersive;
                  const searchRaw = String(readerLibrarySearch || '').trim().toLowerCase();
                  const visibleLibraryItems = readerDocuments.filter((item) => {
                    const isArchived = Boolean(item?.is_archived);
                    if (!readerIncludeArchived && isArchived) return false;
                    if (!searchRaw) return true;
                    const haystack = `${item?.title || ''} ${item?.source_type || ''} ${item?.target_lang || ''}`.toLowerCase();
                    return haystack.includes(searchRaw);
                  });

                  if (showLibraryMode) {
                    return (
                      <div className="reader-library-mode">
                        <div className="reader-topbar">
                          <div className="reader-topbar-left">
                            {isFocusedSection('reader') && (
                              <button type="button" className="section-home-back" onClick={goHomeScreen}>
                                {tr('← Назад', '← Zurueck')}
                              </button>
                            )}
                          </div>
                          <div className="reader-topbar-center">
                            <div className="reader-topbar-title">{tr('Библиотека', 'Bibliothek')}</div>
                            <div className="webapp-muted reader-topbar-meta">
                              {tr('Книги и тексты для чтения', 'Buecher und Texte zum Lesen')}
                            </div>
                          </div>
                          <div className="reader-topbar-right">
                            <button type="button" className="secondary-button" onClick={() => loadReaderLibrary()}>
                              {readerLibraryLoading ? tr('Обновляем...', 'Aktualisieren...') : tr('Обновить', 'Aktualisieren')}
                            </button>
                          </div>
                        </div>

                        <form className="webapp-reader-form" onSubmit={handleReaderIngest}>
                          <label className="webapp-field">
                            <span>{tr('URL или текст', 'URL oder Text')}</span>
                            <textarea
                              rows={4}
                              value={readerInput}
                              onChange={(event) => setReaderInput(event.target.value)}
                              placeholder={tr(
                                'Вставьте URL статьи/книги (включая PDF) или сам текст.',
                                'Fuege URL eines Artikels/Buchs (auch PDF) oder den Text selbst ein.'
                              )}
                            />
                          </label>
                          <label className="webapp-field">
                            <span>{tr('Файл с телефона', 'Datei vom Telefon')}</span>
                            <input
                              type="file"
                              accept=".txt,.md,.pdf,text/plain,application/pdf"
                              onChange={handleReaderFileSelect}
                            />
                            {readerSelectedFile && (
                              <small className="webapp-muted">
                                {tr('Выбран файл', 'Datei gewaehlt')}: {readerSelectedFile.name}
                              </small>
                            )}
                          </label>
                          <div className="webapp-actions">
                            <button type="submit" className="primary-button" disabled={readerLoading}>
                              {readerLoading ? tr('Загружаем...', 'Laden...') : tr('Открыть в читалке', 'Im Leser oeffnen')}
                            </button>
                          </div>
                        </form>

                        {readerError && (
                          <div className="webapp-error">
                            <span>{readerError}</span>
                            {readerErrorCode === 'LIMIT_FREE_PLAN_1_BOOK' && (
                              <div>
                                <button
                                  type="button"
                                  className="secondary-button"
                                  onClick={handleBillingUpgrade}
                                  disabled={billingActionLoading}
                                >
                                  {billingActionLoading ? tr('Открываем...', 'Oeffnen...') : tr('Upgrade', 'Upgrade')}
                                </button>
                              </div>
                            )}
                          </div>
                        )}

                        <section className="reader-library">
                          <div className="reader-library-head">
                            <h4>{readerArchiveOpen ? tr('Архив', 'Archiv') : tr('Моя библиотека', 'Meine Bibliothek')}</h4>
                            <div className="reader-library-head-actions">
                              <label className="menu-toggle-row">
                                <input
                                  type="checkbox"
                                  checked={readerIncludeArchived}
                                  onChange={(event) => setReaderIncludeArchived(event.target.checked)}
                                />
                                <span>{tr('Показывать архив', 'Archiv zeigen')}</span>
                              </label>
                            </div>
                          </div>
                          <label className="webapp-field">
                            <span>{tr('Поиск по библиотеке', 'Suche in Bibliothek')}</span>
                            <input
                              type="text"
                              value={readerLibrarySearch}
                              onChange={(event) => setReaderLibrarySearch(event.target.value)}
                              placeholder={tr('Название книги...', 'Buchtitel...')}
                            />
                          </label>
                          {readerLibraryError && <div className="webapp-error">{readerLibraryError}</div>}
                          {!readerLibraryError && visibleLibraryItems.length === 0 && (
                            <div className="webapp-muted">{tr('Библиотека пока пуста.', 'Bibliothek ist noch leer.')}</div>
                          )}
                          {visibleLibraryItems.length > 0 && (
                            <div className="reader-library-grid">
                              {visibleLibraryItems.map((item) => {
                                const progress = Math.max(0, Math.min(100, Number(item?.progress_percent || 0)));
                                const coverUrl = getReaderCoverUrl(item);
                                const initials = getReaderCoverInitials(item?.title);
                                const meta = buildReaderArchiveMeta(item);
                                return (
                                  <button
                                    type="button"
                                    key={`reader-doc-${item.id}`}
                                    className={`reader-library-card ${Number(readerDocumentId) === Number(item.id) ? 'is-active' : ''}`}
                                    onClick={() => openReaderDocument(item.id)}
                                  >
                                    <div
                                      className="reader-library-cover"
                                      style={coverUrl ? undefined : { background: '#111827' }}
                                    >
                                      {coverUrl ? (
                                        <img src={coverUrl} alt="" loading="lazy" className="reader-archive-cover-img" />
                                      ) : (
                                        <span className="reader-archive-cover-fallback">{initials}</span>
                                      )}
                                    </div>
                                    <div className="reader-library-title">{item.title || tr('Без названия', 'Ohne Titel')}</div>
                                    <div className="reader-library-meta">
                                      <span>{tr('Прогресс', 'Fortschritt')}: {Math.round(progress)}%</span>
                                      {meta && <span>{meta}</span>}
                                    </div>
                                    <div className="reader-library-actions" onClick={(event) => event.stopPropagation()}>
                                      <button
                                        type="button"
                                        className="reader-lib-action"
                                        onClick={() => renameReaderDocument(item.id, item.title)}
                                        title={tr('Переименовать', 'Umbenennen')}
                                      >
                                        ✎
                                      </button>
                                      <button
                                        type="button"
                                        className="reader-lib-action"
                                        onClick={() => archiveReaderDocument(item.id, !Boolean(item?.is_archived))}
                                        title={Boolean(item?.is_archived) ? tr('Разархивировать', 'Wiederherstellen') : tr('В архив', 'Archivieren')}
                                      >
                                        {Boolean(item?.is_archived) ? '↺' : '⤓'}
                                      </button>
                                      <button
                                        type="button"
                                        className="reader-lib-action is-danger"
                                        onClick={() => deleteReaderDocument(item.id)}
                                        title={tr('Удалить', 'Loeschen')}
                                      >
                                        ×
                                      </button>
                                    </div>
                                  </button>
                                );
                              })}
                            </div>
                          )}
                        </section>

                        {readerDocumentId && (
                          <section className="reader-audio-panel">
                            <div className="reader-audio-head">
                              <strong>{tr('Оффлайн-аудио документа', 'Offline-Audio des Dokuments')}</strong>
                            </div>
                            <div className="reader-audio-actions">
                              <button
                                type="button"
                                className="secondary-button"
                                onClick={() => downloadReaderAudio(true)}
                                disabled={readerAudioLoading}
                              >
                                {readerAudioLoading ? tr('Готовим...', 'Erstellen...') : tr('Скачать весь документ', 'Ganzes Dokument herunterladen')}
                              </button>
                            </div>
                            {readerAudioError && <div className="webapp-error">{readerAudioError}</div>}
                            {readerAudioPreviewUrl && (
                              <div className="reader-audio-preview">
                                <audio controls preload="metadata" src={readerAudioPreviewUrl} className="reader-audio-player" />
                                <div className="reader-audio-preview-actions">
                                  <a
                                    href={readerAudioPreviewUrl}
                                    download={readerAudioPreviewName || 'reader_audio.wav'}
                                    className="secondary-button"
                                  >
                                    {tr('Скачать файл', 'Datei herunterladen')}
                                  </a>
                                  <button type="button" className="secondary-button" onClick={closeReaderAudioPreview}>
                                    {tr('Назад', 'Zurueck')}
                                  </button>
                                </div>
                              </div>
                            )}
                          </section>
                        )}
                      </div>
                    );
                  }

                  return (
                    <>
                      {!readerTopbarCollapsed && (
                      <div className="reader-topbar reader-immersive-topbar">
                        <div className="reader-immersive-head">
                          <div className="reader-immersive-title-wrap">
                            <div className="reader-topbar-title">
                              {readerTitle || tr('Читалка', 'Leser')}
                            </div>
                            <div className="webapp-muted reader-topbar-meta">
                              {tr('Прогресс', 'Fortschritt')}: {Math.round(readerProgressPercent)}%
                              {readerPageCount > 0 ? ` • ${tr('Страница', 'Seite')} ${readerCurrentPage}/${readerPageCount}` : ''}
                            </div>
                          </div>
                          <button
                            type="button"
                            className={`reader-timer-pill ${readerTimerPaused ? 'is-paused' : ''}`}
                            onClick={toggleReaderTimerPause}
                            disabled={!readerHasContent}
                          >
                            {readerTimerPaused
                              ? `⏸ ${formatReaderTimer(readerElapsedTotalSeconds)}`
                              : `⏱ ${formatReaderTimer(readerElapsedTotalSeconds)}`}
                          </button>
                        </div>
                        <div className="reader-immersive-dock">
                          <button
                            type="button"
                            className="section-home-back"
                            onClick={() => {
                              setReaderArchiveOpen(true);
                              setReaderImmersive(false);
                              setReaderTopbarCollapsed(false);
                              setReaderSettingsOpen(false);
                            }}
                          >
                            {tr('← Архив', '← Archiv')}
                          </button>
                          <button
                            type="button"
                            className={`reader-bookmark-btn ${isCurrentReaderPageBookmarked ? 'is-active' : ''}`}
                            onClick={() => {
                              const mark = computeReaderProgressPercent();
                              setReaderBookmarkPercent(mark);
                              if (readerDocumentId) {
                                syncReaderState({ bookmark_percent: Number(mark.toFixed(2)), progress_percent: Number(mark.toFixed(2)) });
                              }
                            }}
                            disabled={!readerContent || !readerDocumentId}
                          >
                            🔖
                          </button>
                          <button
                            type="button"
                            className={`secondary-button ${readerReadingMode === 'horizontal' ? 'is-active' : ''}`}
                            onClick={() => {
                              const nextMode = readerReadingMode === 'vertical' ? 'horizontal' : 'vertical';
                              setReaderReadingMode(nextMode);
                              if (readerDocumentId) {
                                syncReaderState({ reading_mode: nextMode });
                              }
                            }}
                            disabled={!readerContent}
                            title={tr('Направление прокрутки', 'Scroll-Richtung')}
                          >
                            {readerReadingMode === 'vertical' ? '↕︎' : '↔︎'}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => setReaderSettingsOpen(true)}
                            title={tr('Настройки чтения', 'Leseeinstellungen')}
                          >
                            ⋯
                          </button>
                          <button
                            type="button"
                            className="secondary-button reader-topbar-collapse-btn reader-topbar-toggle-chip"
                            onClick={() => {
                              const next = !readerTopbarCollapsed;
                              setReaderTopbarCollapsed(next);
                              if (next) {
                                setReaderSettingsOpen(false);
                              }
                            }}
                            title={readerTopbarCollapsed
                              ? tr('Развернуть панель', 'Leiste aufklappen')
                              : tr('Свернуть панель', 'Leiste einklappen')}
                          >
                            {tr('Свернуть', 'Einklappen')}
                          </button>
                        </div>
                      </div>
                      )}

                      {readerTopbarCollapsed && (
                        <div className="reader-topbar-peek">
                          <button
                            type="button"
                            className="secondary-button reader-topbar-peek-btn reader-topbar-toggle-chip"
                            onClick={() => setReaderTopbarCollapsed(false)}
                            title={tr('Показать панель чтения', 'Leseleiste anzeigen')}
                          >
                            {tr('Развернуть', 'Aufklappen')}
                          </button>
                        </div>
                      )}

                      {readerContent && (
                        <article
                          ref={readerArticleRef}
                          className={`reader-article ${readerReadingMode === 'horizontal' ? 'is-horizontal' : 'is-vertical'} ${readerPageCount > 0 ? 'has-pages' : ''}`}
                          onClick={handleReaderStructuredClick}
                          onMouseUp={handleReaderArticleMouseUp}
                          onWheel={handleReaderPageWheel}
                          onTouchStart={handleReaderPageTouchStart}
                          onTouchEnd={handleReaderArticleTouchEnd}
                        >
                          {readerPageCount > 0 ? (
                            <div className="reader-pages-layout">
                              <div
                                className="reader-page-sheet"
                                style={{
                                  '--reader-font-size': `${readerFontSize}px`,
                                  '--reader-font-weight': readerFontWeight,
                                }}
                              >
                                {isCurrentReaderPageBookmarked && (
                                  <span className="reader-page-bookmark-indicator" aria-hidden="true" />
                                )}
                                <div className="reader-page-sheet-inner">
                                  {renderReaderStructuredText()}
                                </div>
                                <div className="reader-page-num">
                                  {tr('Стр.', 'S.')}{' '}{readerCurrentPage}{readerPageCount > 0 ? ` / ${readerPageCount}` : ''}
                                </div>
                              </div>
                            </div>
                          ) : (
                            renderReaderStructuredText()
                          )}
                        </article>
                      )}

                      {readerSettingsOpen && (
                        <div className="reader-settings-sheet-wrap" role="dialog" aria-modal="true">
                          <button
                            type="button"
                            className="reader-settings-sheet-backdrop"
                            aria-label={tr('Закрыть', 'Schliessen')}
                            onClick={() => setReaderSettingsOpen(false)}
                          />
                          <div className="reader-settings-sheet">
                            <div className="reader-settings-sheet-head">
                              <strong>{tr('Настройки чтения', 'Leseeinstellungen')}</strong>
                              <button
                                type="button"
                                className="secondary-button"
                                onClick={() => setReaderSettingsOpen(false)}
                              >
                                ×
                              </button>
                            </div>
                            <label className="webapp-field">
                              <span>{tr('Размер шрифта', 'Schriftgroesse')}</span>
                              <input
                                type="range"
                                min="14"
                                max="28"
                                step="1"
                                value={readerFontSize}
                                onChange={(event) => setReaderFontSize(Number(event.target.value))}
                              />
                              <small className="webapp-muted">{readerFontSize}px</small>
                            </label>
                            <label className="webapp-field">
                              <span>{tr('Жирность текста', 'Schriftstaerke')}</span>
                              <input
                                type="range"
                                min="400"
                                max="700"
                                step="50"
                                value={readerFontWeight}
                                onChange={(event) => setReaderFontWeight(Number(event.target.value))}
                              />
                              <small className="webapp-muted">{readerFontWeight}</small>
                            </label>
                            <label className="webapp-field">
                              <span>{tr('Чувствительность свайпа', 'Swipe-Empfindlichkeit')}</span>
                              <select
                                value={readerSwipeSensitivity}
                                onChange={(event) => setReaderSwipeSensitivity(event.target.value)}
                              >
                                <option value="high">{tr('Высокая', 'Hoch')}</option>
                                <option value="medium">{tr('Средняя', 'Mittel')}</option>
                                <option value="low">{tr('Низкая', 'Niedrig')}</option>
                              </select>
                            </label>
                            <div className="reader-immersive-indicator is-on">{tr('Immersive: ON', 'Immersive: ON')}</div>
                          </div>
                        </div>
                      )}
                    </>
                  );
                })()}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('movies') && !moviesCollapsed && (
              <section className="webapp-movies" ref={moviesRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Фильмы', 'Filme')}</h2>
                  <p>{tr('Видео с доступными субтитрами, сохранённые в каталоге.', 'Videos mit verfuegbaren Untertiteln im Katalog.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                {moviesLoading && <div className="webapp-muted">{tr('Загружаем каталог...', 'Katalog wird geladen...')}</div>}
                {moviesError && <div className="webapp-error">{moviesError}</div>}
                {!moviesLoading && !moviesError && movies.length === 0 && (
                  <div className="webapp-muted">{tr('Пока нет сохранённых видео.', 'Noch keine gespeicherten Videos.')}</div>
                )}
                {!moviesLoading && movieLanguageOptions.length > 0 && (
                  <div className="movies-language-filter">
                    <button
                      type="button"
                      className={`movies-filter-chip ${moviesLanguageFilter === 'all' ? 'is-active' : ''}`}
                      onClick={() => setMoviesLanguageFilter('all')}
                    >
                      {tr('Все', 'Alle')}
                    </button>
                    {movieLanguageOptions.map((code) => (
                      <button
                        type="button"
                        key={code}
                        className={`movies-filter-chip ${moviesLanguageFilter === code ? 'is-active' : ''}`}
                        onClick={() => setMoviesLanguageFilter(code)}
                      >
                        {code.toUpperCase()}
                      </button>
                    ))}
                  </div>
                )}
                {!moviesLoading && moviesFiltered.length > 0 && (
                  <div className="movies-grid">
                    {moviesFiltered.map((item) => (
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
                            {(item.language ? `${String(item.language).toUpperCase()} • ` : '')}
                            {item.items_count ? `${item.items_count} ${tr('строк', 'Zeilen')}` : tr('Субтитры', 'Untertitel')}
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
                {!moviesLoading && movies.length > 0 && moviesFiltered.length === 0 && (
                  <div className="webapp-muted">{tr('Для выбранного языка пока нет фильмов.', 'Fuer die gewaehlt Sprache gibt es noch keine Videos.')}</div>
                )}
              </section>
            )}

            {isSectionVisible('flashcards') && (
              <section className="webapp-flashcards" ref={flashcardsRef}>
                {flashcardActiveMode && !flashcardsOnly && (isFocusedSection('flashcards') || Boolean(getTodayTaskForSection('flashcards'))) && (
                  <div className="section-inline-actions section-inline-actions-task">
                    {isFocusedSection('flashcards') && (
                      <button type="button" className="section-home-back" onClick={goHomeScreen}>
                        {tr('На главную', 'Startseite')}
                      </button>
                    )}
                    {renderTodaySectionTaskHud('flashcards')}
                  </div>
                )}
                {!flashcardsVisible && (
                  <div className="webapp-muted">
                    {t('flashcards_start_hint')}
                  </div>
                )}
                {flashcardsVisible && (
                  <div className={`flashcards-panel ${flashcardsOnly ? 'is-session' : 'is-setup'}`}>
                    {!flashcardsOnly && !flashcardActiveMode && (
                      <div className="flashcard-mode-menu">
                        <div className="flashcard-mode-title-wrap">
                          <h3>{tr('Choose Training Mode', 'Choose Training Mode')}</h3>
                          <p>{tr('Select how you want to train', 'Select how you want to train')}</p>
                        </div>
                        <div className="flashcard-mode-list">
                          {[
                            { mode: 'fsrs', title: 'FSRS', subtitle: 'Smart spaced repetition' },
                            { mode: 'quiz', title: 'Quiz', subtitle: 'Quiz - test +4 options' },
                            { mode: 'blocks', title: 'Blocks', subtitle: 'Blocks - assemble the word' },
                            { mode: 'sentence', title: 'Sentence', subtitle: 'Sentence - supplemental context practice' },
                          ].map((entry) => (
                            <div className="flashcard-mode-item" key={`mode-${entry.mode}`}>
                              <button
                                type="button"
                                className="flashcard-mode-button"
                                onClick={() => {
                                  void startFlashcardsMode(entry.mode);
                                }}
                              >
                                <span className="flashcard-mode-button-title">{entry.title}</span>
                                <span className="flashcard-mode-button-subtitle">{entry.subtitle}</span>
                              </button>
                              <button
                                type="button"
                                className="flashcard-mode-settings"
                                aria-label={tr('Настройки режима', 'Modus-Einstellungen')}
                                onClick={() => setFlashcardSettingsModalMode(entry.mode)}
                              >
                                ⚙
                              </button>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {flashcardsOnly && flashcardActiveMode === 'fsrs' && (
                      <div className="flashcard-mode-screen">
                        <div className="flashcard-mode-topbar">
                          <button type="button" className="secondary-button" onClick={() => void exitFlashcardsTraining()}>
                            {tr('Назад', 'Zurueck')}
                          </button>
                          {renderTodaySectionTaskHud('flashcards')}
                        </div>
                        <div className="fsrs-study-screen">
                          <div className="fsrs-study-header">
                            <div className="fsrs-study-title">FSRS Study</div>
                            <div className="fsrs-study-queue">
                              {t('due')}: {srsQueueInfo?.due_count ?? 0} · {t('new_today')}: {srsQueueInfo?.new_remaining_today ?? 0}
                            </div>
                          </div>

                          {!srsLoading && !srsCard && !srsError && (srsQueueInfo?.due_count ?? 0) === 0 && (srsQueueInfo?.new_remaining_today ?? 0) === 0 && (
                            <div className="fsrs-empty-note">
                              {tr('Сегодня по FSRS всё повторено. Можно отдыхать.', 'Heute ist alles in FSRS wiederholt. Du kannst entspannen.')}
                            </div>
                          )}
                          {!srsLoading && !srsCard && srsError && <div className="webapp-error">{srsError}</div>}
                          {!srsLoading && !srsCard && !srsError && (
                            <div className="fsrs-empty-note">{t('no_cards_now')}</div>
                          )}

                          {srsLoading && (
                            <div className="fsrs-study-card fsrs-loading-card" aria-live="polite">
                              <div className="fsrs-hourglass">⌛</div>
                              <div className="fsrs-loading-title">Preparing next card…</div>
                              <div className="fsrs-divider" />
                              <div className="fsrs-loading-subtitle">Optimizing repetition interval (FSRS)</div>
                            </div>
                          )}

                          {!srsLoading && srsCard && (() => {
                            const direction = (srsCard?.source_lang || 'ru') === 'de' ? 'de-ru' : 'ru-de';
                            const cardTexts = getDictionarySourceTarget(srsCard, direction);
                            const sourceText = cardTexts?.sourceText || '—';
                            const targetText = cardTexts?.targetText || '—';
                            const srsReplayTtsKey = `srs-replay-${srsCard?.id || srsCard?.entry_id || 'current'}`;
                            const srsReplayTtsLoading = isTtsPending(srsReplayTtsKey);
                            const srsReplayLang = getTtsLocaleForLang(detectTtsLangFromText(targetText));
                            const srsFeelEntryId = resolveFlashcardFeelEntryId(srsCard);
                            const srsFeelQueued = srsFeelEntryId ? !!flashcardFeelQueuedMap[srsFeelEntryId] : false;
                            const srsFeelStatus = srsFeelEntryId ? String(flashcardFeelStatusMap[srsFeelEntryId] || '').trim() : '';
                            return (
                              <>
                                <div className={`fsrs-study-card ${srsRevealAnswer ? 'is-revealed' : ''}`}>
                                  <div className="fsrs-card-source is-muted-top">{sourceText}</div>
                                  <div className="fsrs-divider" />
                                  {srsRevealAnswer && (
                                    <div className="fsrs-card-target">{targetText}</div>
                                  )}
                                  {!srsRevealAnswer && (
                                    <div className="fsrs-card-meta">
                                      {tr('Status', 'Status')}: {String(srsState?.status || 'new')} · {tr('Interval', 'Intervall')}: {srsState?.interval_days ?? 0} {t('days_short')}
                                    </div>
                                  )}
                                  {!srsRevealAnswer ? (
                                    <button
                                      type="button"
                                      className="fsrs-show-answer-btn"
                                      onClick={() => {
                                        setSrsRevealStartedAt(Date.now());
                                        setSrsRevealElapsedSec(0);
                                        setSrsRevealAnswer(true);
                                      }}
                                      disabled={srsSubmitting}
                                    >
                                      Show Answer
                                    </button>
                                  ) : (
                                    <>
                                      <div className="fsrs-divider" />
                                      <div className="fsrs-card-meta fsrs-card-meta-answer">
                                        {tr('Response time', 'Response time')}: {srsRevealElapsedSec}s
                                      </div>
                                      <button
                                        type="button"
                                        className={`flashcard-audio-replay ${srsReplayTtsLoading ? 'is-loading' : ''}`}
                                        onClick={() => {
                                          void playTtsWithUi(srsReplayTtsKey, targetText, srsReplayLang);
                                        }}
                                        aria-label={tr('Повторить аудио', 'Audio wiederholen')}
                                        title={tr('Повторить аудио', 'Audio wiederholen')}
                                        disabled={srsReplayTtsLoading || srsSubmitting}
                                      >
                                        {renderTtsButtonContent(srsReplayTtsLoading)}
                                      </button>
                                      <div className="flashcard-actions-row flashcard-preview-feel-row">
                                        <button
                                          type="button"
                                          className="secondary-button flashcard-preview-feel-btn"
                                          disabled={!srsFeelEntryId || srsFeelQueued || flashcardFeelDispatching || srsSubmitting}
                                          onClick={() => {
                                            queueFlashcardFeel(srsCard);
                                          }}
                                        >
                                          {srsFeelQueued
                                            ? tr('В очереди', 'In Warteschlange')
                                            : tr('Почувствовать слово', 'Feel the Word')}
                                        </button>
                                      </div>
                                      {srsFeelStatus && (
                                        <div className="flashcard-preview-feel-status">
                                          {srsFeelStatus}
                                        </div>
                                      )}
                                    </>
                                  )}
                                </div>

                                {srsRevealAnswer && (
                                  <div className="fsrs-rating-wrap">
                                    <div className="fsrs-rating-grid">
                                      <div className="fsrs-rate-cell">
                                        <button type="button" className="fsrs-rate-btn again" onClick={() => submitSrsReview('AGAIN')} disabled={srsSubmitting}>
                                          <span>Again</span>
                                        </button>
                                        <small className="fsrs-rate-hint">{getSrsRatingHint('AGAIN')}</small>
                                      </div>
                                      <div className="fsrs-rate-cell">
                                        <button type="button" className="fsrs-rate-btn hard" onClick={() => submitSrsReview('HARD')} disabled={srsSubmitting}>
                                          <span>Hard</span>
                                        </button>
                                        <small className="fsrs-rate-hint">{getSrsRatingHint('HARD')}</small>
                                      </div>
                                      <div className="fsrs-rate-cell">
                                        <button type="button" className="fsrs-rate-btn good" onClick={() => submitSrsReview('GOOD')} disabled={srsSubmitting || srsGoodLocked}>
                                          <span>Good</span>
                                        </button>
                                        <small className="fsrs-rate-hint">{getSrsRatingHint('GOOD')}</small>
                                      </div>
                                      <div className="fsrs-rate-cell">
                                        <button type="button" className="fsrs-rate-btn easy" onClick={() => submitSrsReview('EASY')} disabled={srsSubmitting || srsEasyLocked}>
                                          <span>Easy</span>
                                        </button>
                                        <small className="fsrs-rate-hint">{getSrsRatingHint('EASY')}</small>
                                      </div>
                                    </div>
                                  </div>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      </div>
                    )}

                    {flashcardsOnly && (flashcardActiveMode === 'quiz' || flashcardActiveMode === 'blocks' || flashcardActiveMode === 'sentence') && (
                      <>
                        <div className="flashcard-mode-topbar">
                          <button type="button" className="secondary-button" onClick={() => void exitFlashcardsTraining()}>
                            {tr('Назад', 'Zurueck')}
                          </button>
                          {renderTodaySectionTaskHud('flashcards')}
                        </div>
                        {flashcardsLoading && <div className="webapp-muted">{t('loading_cards')}</div>}
                        {flashcardsError && <div className="webapp-error">{flashcardsError}</div>}
                        {!flashcardsLoading && !flashcardsError && flashcards.length === 0 && (
                          <div className="webapp-muted">{t('dictionary_empty')}</div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && flashcardPreviewActive && (
                          <div className="flashcard-stage is-session is-preview">
                            {(() => {
                              const entry = flashcards[flashcardPreviewIndex] || {};
                              const cardTexts = resolveFlashcardTexts(entry);
                              const previewLearningText = cardTexts.targetText || cardTexts.sourceText || '—';
                              const previewNativeText = cardTexts.sourceText || cardTexts.targetText || '—';
                              const previewQuizType = String(entry?.response_json?.quiz_type || '').trim();
                              const sentencePreviewRuHint = previewQuizType === 'sentence_gap_context'
                                ? String(entry?.response_json?.translation_ru || entry?.translation_ru || '').trim()
                                : '';
                              const learningCode = String(languageProfile?.learning_language || 'de').toUpperCase();
                              const nativeCode = String(languageProfile?.native_language || 'ru').toUpperCase();
                              const previewModeLabel = flashcardActiveMode === 'blocks'
                                ? 'Blocks Mode'
                                : flashcardActiveMode === 'sentence'
                                  ? 'Satz Ergaenzen Mode'
                                  : 'Quiz 4 Options Mode';
                              const feelEntryId = resolveFlashcardFeelEntryId(entry);
                              const feelQueued = feelEntryId ? !!flashcardFeelQueuedMap[feelEntryId] : false;
                              const feelStatus = feelEntryId ? String(flashcardFeelStatusMap[feelEntryId] || '').trim() : '';
                              const previewNavLocked = previewAudioPlaying;
                              const previewTtsKey = `flashcard-preview-${entry.id}`;
                              const previewTtsLoading = previewAudioPlaying || isTtsPending(previewTtsKey);

                              return (
                                <div className="flashcard flashcard-preview flashcard-preview-shell">
                                  <div className="flashcard-preview-topline">
                                    <span className="flashcard-preview-title">Preview</span>
                                    <span className="flashcard-preview-mode">{previewModeLabel}</span>
                                  </div>
                                  <div className="flashcard-preview-card">
                                    <div className="flashcard-preview-lang-row">
                                      <span className="flashcard-lang-tag">{learningCode}</span>
                                      <span className="flashcard-lang-tag is-native">{nativeCode}</span>
                                    </div>
                                    <div className="flashcard-preview-word">{previewLearningText}</div>
                                    <div className="flashcard-preview-divider" />
                                    <div className="flashcard-preview-native-box">
                                      <div className="flashcard-native-translation">{previewNativeText}</div>
                                    </div>
                                    {sentencePreviewRuHint && (
                                      <div className="flashcard-preview-ru-hint">{sentencePreviewRuHint}</div>
                                    )}
                                    <button
                                      type="button"
                                      className={`flashcard-audio-replay flashcard-preview-listen ${previewTtsLoading ? 'is-loading' : ''}`}
                                      onClick={async () => {
                                        const text = resolveFlashcardGerman(entry);
                                        if (!text || previewTtsLoading) return;
                                        try {
                                          setPreviewAudioPlaying(true);
                                          await playTtsWithUi(previewTtsKey, text, getLearningTtsLocale());
                                        } finally {
                                          setPreviewAudioPlaying(false);
                                          setPreviewAudioReady(true);
                                        }
                                      }}
                                      aria-label={tr('Повторить аудио', 'Audio wiederholen')}
                                      title={tr('Повторить аудио', 'Audio wiederholen')}
                                      disabled={previewTtsLoading}
                                    >
                                      {renderTtsButtonContent(previewTtsLoading)}
                                    </button>
                                    <div className="flashcard-preview-listen-label">
                                      {tr('Слушать', 'Listen')}
                                    </div>
                                    <div className="flashcard-preview-divider" />
                                    <div className="flashcard-actions-row flashcard-preview-feel-row">
                                      <button
                                        type="button"
                                        className="secondary-button flashcard-preview-feel-btn"
                                        disabled={!feelEntryId || feelQueued || flashcardFeelDispatching}
                                        onClick={() => {
                                          queueFlashcardFeel(entry);
                                        }}
                                      >
                                        {feelQueued
                                          ? tr('В очереди', 'In Warteschlange')
                                          : tr('Почувствовать слово', 'Feel the Word')}
                                      </button>
                                    </div>
                                    {feelStatus && (
                                      <div className="flashcard-preview-feel-status">
                                        {feelStatus}
                                      </div>
                                    )}
                                  </div>
                                  <div className="flashcard-actions-row flashcard-preview-footer-row">
                                    <button
                                      type="button"
                                      className="secondary-button flashcard-preview-footer-btn"
                                      onClick={() => {
                                        stopTtsPlayback();
                                        setPreviewAudioReady(false);
                                        setPreviewAudioPlaying(true);
                                        const nextIndex = Math.max(flashcardPreviewIndex - 1, 0);
                                        setFlashcardPreviewIndex(nextIndex);
                                      }}
                                      disabled={flashcardPreviewIndex === 0 || previewAudioPlaying}
                                    >
                                      {tr('Назад', 'Zurueck')}
                                    </button>
                                    {flashcardPreviewIndex < flashcards.length - 1 ? (
                                      <button
                                        type="button"
                                        className="primary-button flashcard-preview-footer-btn is-next"
                                        onClick={() => {
                                          stopTtsPlayback();
                                          setPreviewAudioReady(false);
                                          setPreviewAudioPlaying(true);
                                          const nextIndex = Math.min(flashcardPreviewIndex + 1, flashcards.length - 1);
                                          setFlashcardPreviewIndex(nextIndex);
                                        }}
                                        disabled={previewNavLocked}
                                      >
                                        {previewAudioPlaying ? tr('Слушаем...', 'Hoeren...') : tr('Далее', 'Weiter')}
                                      </button>
                                    ) : (
                                      <button
                                        type="button"
                                        className="primary-button flashcard-preview-footer-btn is-start"
                                        onClick={() => {
                                          stopTtsPlayback();
                                          setFlashcardPreviewActive(false);
                                          setFlashcardSessionActive(true);
                                          setFlashcardIndex(0);
                                          setFlashcardSelection(null);
                                          setFlashcardTimerKey((prev) => prev + 1);
                                        }}
                                        disabled={previewNavLocked}
                                      >
                                        {previewAudioPlaying ? tr('Слушаем...', 'Hoeren...') : tr('Начать тренировку', 'Training starten')}
                                      </button>
                                    )}
                                  </div>
                                </div>
                              );
                            })()}
                          </div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && !flashcardPreviewActive && (
                          <div className={`flashcard-stage is-session ${flashcardTrainingMode === 'blocks' ? '' : 'is-quiz-layout'}`}>
                            {(flashcardSetComplete || flashcardExitSummary) ? (
                              <div className="flashcard-summary flashcard-summary-card">
                                <div className="summary-trophy-wrap" aria-hidden="true">
                                  <div className="summary-trophy-ring">
                                    <div className="summary-trophy">🏆</div>
                                  </div>
                                </div>
                                <h4>{tr('Set Completed', 'Set Completed')}</h4>
                                <div className="summary-grid summary-grid-2x2">
                                  <div>
                                    <span>{tr('Total Words', 'Total Words')}</span>
                                    <strong>{flashcardStats.total}</strong>
                                  </div>
                                  <div>
                                    <span>{tr('Correct', 'Correct')}</span>
                                    <strong>{flashcardStats.correct}</strong>
                                  </div>
                                  <div>
                                    <span>{tr('Learned', 'Learned')}</span>
                                    <strong>{flashcardStats.correct}</strong>
                                  </div>
                                  <div>
                                    <span>{tr('Incorrect', 'Incorrect')}</span>
                                    <strong className="summary-bad">{flashcardStats.wrong}</strong>
                                  </div>
                                </div>
                                <div className="summary-actions summary-actions-vertical">
                                  <button
                                    type="button"
                                    className="primary-button"
                                    onClick={async () => {
                                      await dispatchQueuedFlashcardFeel('next_set');
                                      await loadFlashcards();
                                    }}
                                  >
                                    {tr('Next Set', 'Next Set')}
                                  </button>
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() => void exitFlashcardsTraining()}
                                  >
                                    {tr('End Session', 'End Session')}
                                  </button>
                                </div>
                              </div>
                            ) : (
                              (() => {
                                const entry = flashcards[flashcardIndex] || {};
                                const responseJson = entry.response_json || {};
                                const cardTexts = resolveFlashcardTexts(entry);
                                const quizType = String(responseJson?.quiz_type || '').trim();
                                const isSentenceTrainingMode = flashcardTrainingMode === 'sentence';
                                const isSentenceGapQuiz = quizType === 'separable_prefix_verb_gap';
                                const questionWord = (isSentenceTrainingMode || isSentenceGapQuiz)
                                  ? (String(responseJson?.sentence_with_gap || '').trim() || cardTexts.sourceText || '—')
                                  : (cardTexts.sourceText || '—');
                                const normalizedQuestionWord = String(questionWord || '').replace(/\s+/g, ' ').trim();
                                const sentenceTranslation = String(responseJson?.translation_ru || '').trim();
                                const context = !(isSentenceTrainingMode || isSentenceGapQuiz) && Array.isArray(responseJson.usage_examples)
                                  ? responseJson.usage_examples[0]
                                  : '';
                                const correct = resolveQuizCorrectOption(entry, flashcardOptions);
                                const blocksAnswer = resolveBlocksAnswer(entry);
                                const blocksPrompt = resolveBlocksPrompt(entry);
                                const blocksType = resolveBlocksType(entry, blocksAnswer);

                                if (flashcardTrainingMode === 'blocks') {
                                  return (
                                    <div className="flashcard flashcard-blocks">
                                      <div className="blocks-session-topline">
                                        <span>Block Mode</span>
                                        <span className="flashcard-counter">
                                          {flashcardIndex + 1} / {flashcards.length}
                                        </span>
                                      </div>
                                      <BlocksTrainer
                                        key={`blocks-${entry.id || 'na'}-${flashcardIndex}`}
                                        card={entry}
                                        prompt={blocksPrompt}
                                        answer={blocksAnswer}
                                        cardType={blocksType}
                                        resetSignal={blocksResetNonce}
                                        timerMode={blocksTimerMode}
                                        isExternallyPaused={globalTimerSuspended}
                                        autoAdvance={flashcardAutoAdvance}
                                        labels={{
                                          promptFallback: t('blocks_build_answer'),
                                          hint: t('blocks_hint'),
                                          check: t('blocks_check'),
                                          next: t('blocks_next'),
                                          correct: t('blocks_correct'),
                                          wrong: t('blocks_wrong'),
                                          timeout: t('blocks_timeout'),
                                          correctAnswer: t('blocks_correct_answer'),
                                          hintsUsed: t('blocks_hints_used'),
                                        }}
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
                                            void playTts(german, getLearningTtsLocale());
                                          }
                                        }}
                                        onNext={() => advanceFlashcard()}
                                      />
                                    </div>
                                  );
                                }

                                const isAnswered = flashcardSelection !== null;
                                const isCorrectAnswer = flashcardOutcome === 'correct';
                                const quizMascotSrc = !isAnswered
                                  ? heroThinkSrc
                                  : (isCorrectAnswer ? heroMascotSrc : heroCrySrc);
                                const screenModeLabel = isSentenceTrainingMode || isSentenceGapQuiz ? 'Satz Ergaenzen Mode' : 'Quiz Mode';
                                const longestOptionLen = Array.isArray(flashcardOptions)
                                  ? flashcardOptions.reduce((maxLen, optionText) => {
                                    const normalized = String(optionText || '').replace(/\s+/g, ' ').trim();
                                    return Math.max(maxLen, normalized.length);
                                  }, 0)
                                  : 0;
                                const quizNeedsCompact = isAnswered || normalizedQuestionWord.length > 90 || longestOptionLen > 90;
                                const quizNeedsUltraCompact = normalizedQuestionWord.length > 150 || longestOptionLen > 130;
                                const quizLayoutClassName = [
                                  'flashcard flashcard-quiz-layout',
                                  quizNeedsCompact ? 'is-compact' : '',
                                  quizNeedsUltraCompact ? 'is-ultra-compact' : '',
                                ].filter(Boolean).join(' ');
                                const quizStudyCardClassName = [
                                  'quiz-study-card',
                                  quizNeedsCompact ? 'is-compact' : '',
                                  quizNeedsUltraCompact ? 'is-ultra-compact' : '',
                                ].filter(Boolean).join(' ');
                                return (
                                  <div className={quizLayoutClassName}>
                                    <div className="quiz-layout-head">
                                      <span>{screenModeLabel}</span>
                                      <span className="flashcard-counter">{flashcardIndex + 1} / {flashcards.length}</span>
                                    </div>

                                    <div className={quizStudyCardClassName}>
                                      <div className="quiz-study-mode-title">{isSentenceTrainingMode || isSentenceGapQuiz ? 'Satz Ergaenzen' : 'Quiz Mode'}</div>
                                      <div className={`quiz-study-question ${(isSentenceTrainingMode || isSentenceGapQuiz) ? 'is-sentence-gap' : ''}`}>
                                        {(isSentenceTrainingMode || isSentenceGapQuiz) && isAnswered
                                          ? renderSentenceWithGapAnswer(
                                            questionWord,
                                            correct,
                                            isCorrectAnswer ? 'correct' : 'wrong'
                                          )
                                          : questionWord}
                                      </div>

                                      {(isSentenceTrainingMode || isSentenceGapQuiz) && sentenceTranslation && (
                                        <div className="quiz-study-translation">{sentenceTranslation}</div>
                                      )}

                                      <div className={`quiz-mascot-circle ${isAnswered ? (isCorrectAnswer ? 'is-correct' : 'is-wrong') : ''}`}>
                                        <img
                                          src={quizMascotSrc}
                                          alt="Deutsch mascot"
                                          className="quiz-mascot-image"
                                        />
                                      </div>

                                      {isAnswered && (
                                        <div className={`quiz-result ${isCorrectAnswer ? 'is-correct' : 'is-wrong'}`}>
                                          <div className="quiz-result-title">{isCorrectAnswer ? 'Correct!' : 'Incorrect'}</div>
                                          <div className="quiz-result-subtitle">{isCorrectAnswer ? 'Great job - keep going' : 'Review and try again'}</div>
                                        </div>
                                      )}

                                      <div className="flashcard-options quiz-options">
                                        {flashcardOptions.map((option, idx) => {
                                          const isSelected = flashcardSelection === idx;
                                          const isCorrect = option === correct;
                                          const showResult = flashcardSelection !== null;
                                          const className = [
                                            'flashcard-option quiz-option',
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
                                                    mode: flashcardTrainingMode,
                                                    timeSpentMs,
                                                    hintsUsed: 0,
                                                  });
                                                  setFlashcardStats((prev) => ({
                                                    ...prev,
                                                    correct: prev.correct + (option === correct ? 1 : 0),
                                                    wrong: prev.wrong + (option === correct ? 0 : 1),
                                                  }));
                                                  if (option === correct) {
                                                    const solvedMode = String(flashcardTrainingMode || '').toLowerCase() === 'sentence'
                                                      ? 'sentence'
                                                      : 'quiz';
                                                    if (solvedMode === 'sentence') {
                                                      markSolvedTodayByMode(solvedMode, entry.id);
                                                    }
                                                  }
                                                  if (autoAdvanceTimeoutRef.current) {
                                                    clearTimeout(autoAdvanceTimeoutRef.current);
                                                    autoAdvanceTimeoutRef.current = null;
                                                  }
                                                  if (revealTimeoutRef.current) {
                                                    clearTimeout(revealTimeoutRef.current);
                                                  }
                                                  const german = resolveFlashcardGerman(entry);
                                                  if (german) {
                                                    await playTts(german, getLearningTtsLocale());
                                                  }
                                                  if (flashcardAutoAdvance) {
                                                    revealTimeoutRef.current = setTimeout(() => {
                                                      advanceFlashcard();
                                                    }, 3000);
                                                  }
                                                }}
                                              >
                                                <span>{option}</span>
                                                {isAnswered && (option === correct || isSelected) && (
                                                  <span className="quiz-option-mark">
                                                    {option === correct ? '✓' : (isSelected ? '✕' : '')}
                                                  </span>
                                                )}
                                              </button>
                                            </div>
                                          );
                                        })}
                                      </div>

                                      {isAnswered && (
                                        <div className="quiz-actions">
                                          <button
                                            type="button"
                                            className={`quiz-continue-btn ${isCorrectAnswer ? 'is-correct' : 'is-wrong'}`}
                                            onClick={() => {
                                              if (revealTimeoutRef.current) {
                                                clearTimeout(revealTimeoutRef.current);
                                                revealTimeoutRef.current = null;
                                              }
                                              advanceFlashcard();
                                            }}
                                          >
                                            Continue
                                          </button>
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                );
                              })()
                            )}
                          </div>
                        )}
                      </>
                    )}
                    {flashcardSettingsModalMode && (
                      <div className="flashcard-settings-overlay" onClick={() => setFlashcardSettingsModalMode(null)}>
                        <div className="flashcard-settings-modal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
                          <div className="flashcard-settings-head">
                            <button type="button" className="flashcard-settings-back" onClick={() => setFlashcardSettingsModalMode(null)}>
                              {tr('Back', 'Back')}
                            </button>
                            <h4>
                              {flashcardSettingsModalMode === 'fsrs'
                                ? 'FSRS Settings'
                                : flashcardSettingsModalMode === 'quiz'
                                  ? 'Quiz 4 Options Settings'
                                  : flashcardSettingsModalMode === 'blocks'
                                    ? 'Blocks Settings'
                                    : 'Sentence Completion Settings'}
                            </h4>
                          </div>
                          <div className="flashcard-settings-divider" />
                          <div className="setup-grid">
                            {flashcardSettingsModalMode === 'fsrs' ? (
                              <>
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Language Pair', 'Language Pair')}</div>
                                  <div className="flashcard-settings-badge">
                                    {String(getActiveLanguagePairLabel() || '').replace('-', ' → ')}
                                  </div>
                                </div>
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Card Queue', 'Card Queue')}</div>
                                  <div className="flashcard-settings-queue">
                                    <span>{tr('Due', 'Due')}: {srsQueueInfo?.due_count ?? 0}</span>
                                    <span>{tr('New Today', 'New Today')}: {srsQueueInfo?.new_remaining_today ?? 0}</span>
                                  </div>
                                  <button type="button" className="flashcard-settings-update-btn" onClick={() => void loadSrsNextCard()}>
                                    {tr('Update Queue', 'Update Queue')}
                                  </button>
                                </div>
                              </>
                            ) : (
                              <>
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Set Size', 'Set Size')}</div>
                                  <div className="setup-options">
                                    {[5, 10, 15].map((size) => (
                                      <button
                                        key={`modal-set-${size}`}
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${flashcardSetSize === size ? 'is-active' : ''}`}
                                        onClick={() => setFlashcardSetSize(size)}
                                      >
                                        {size} {tr('Cards', 'Cards')}
                                      </button>
                                    ))}
                                  </div>
                                </div>
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Folder', 'Folder')}</div>
                                  <label className="webapp-field">
                                    <select
                                      className="flashcard-settings-select"
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
                                      <option value="all">{tr('All Folders', 'All Folders')}</option>
                                      <option value="none">{t('setup_without_folder')}</option>
                                      {folders.map((folder) => (
                                        <option key={folder.id} value={folder.id}>
                                          {resolveFolderIconLabel(folder.icon)} • {folder.name}
                                        </option>
                                      ))}
                                    </select>
                                  </label>
                                </div>
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Speed', 'Speed')}</div>
                                  <div className="setup-options">
                                    {[5, 10, 15].map((seconds) => (
                                      <button
                                        key={`modal-speed-${seconds}`}
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${flashcardDurationSec === seconds ? 'is-active' : ''}`}
                                        onClick={() => setFlashcardDurationSec(seconds)}
                                      >
                                        {seconds} sec
                                      </button>
                                    ))}
                                  </div>
                                </div>
                                {flashcardSettingsModalMode === 'blocks' && (
                                  <div className="setup-group">
                                    <div className="setup-label">{tr('Blocks Timer', 'Blocks Timer')}</div>
                                    <div className="setup-options">
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${blocksTimerMode === 'adaptive' ? 'is-active' : ''}`}
                                        onClick={() => setBlocksTimerMode('adaptive')}
                                      >
                                        {tr('Adaptive', 'Adaptive')}
                                      </button>
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${blocksTimerMode === 'fixed' ? 'is-active' : ''}`}
                                        onClick={() => setBlocksTimerMode('fixed')}
                                      >
                                        {tr('10 sec', '10 sec')}
                                      </button>
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${blocksTimerMode === 'none' ? 'is-active' : ''}`}
                                        onClick={() => setBlocksTimerMode('none')}
                                      >
                                        {tr('No Timer', 'No Timer')}
                                      </button>
                                    </div>
                                  </div>
                                )}
                                {flashcardSettingsModalMode === 'sentence' && (
                                  <div className="setup-group">
                                    <div className="setup-label">{tr('Difficulty', 'Difficulty')}</div>
                                    <div className="setup-options">
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${sentenceDifficulty === 'easy' ? 'is-active' : ''}`}
                                        onClick={() => setSentenceDifficulty('easy')}
                                      >
                                        {tr('Easy', 'Easy')}
                                      </button>
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${sentenceDifficulty === 'medium' ? 'is-active' : ''}`}
                                        onClick={() => setSentenceDifficulty('medium')}
                                      >
                                        {tr('Medium', 'Medium')}
                                      </button>
                                      <button
                                        type="button"
                                        className={`option-pill flashcard-settings-pill ${sentenceDifficulty === 'hard' ? 'is-active' : ''}`}
                                        onClick={() => setSentenceDifficulty('hard')}
                                      >
                                        {tr('Hard', 'Hard')}
                                      </button>
                                    </div>
                                  </div>
                                )}
                                <div className="setup-group">
                                  <div className="setup-label">{tr('Transition', 'Transition')}</div>
                                  <div className="setup-options">
                                    <button
                                      type="button"
                                      className={`option-pill flashcard-settings-pill ${flashcardAutoAdvance ? 'is-active' : ''}`}
                                      onClick={() => setFlashcardAutoAdvance(true)}
                                    >
                                      {tr('Automatic', 'Automatic')}
                                    </button>
                                    <button
                                      type="button"
                                      className={`option-pill flashcard-settings-pill ${!flashcardAutoAdvance ? 'is-active' : ''}`}
                                      onClick={() => setFlashcardAutoAdvance(false)}
                                    >
                                      {tr('Manual', 'Manual')}
                                    </button>
                                  </div>
                                </div>
                              </>
                            )}
                          </div>
                          <div className="flashcard-settings-footer">
                            <button
                              type="button"
                              className="flashcard-settings-save-start"
                              onClick={() => {
                                const mode = flashcardSettingsModalMode;
                                setFlashcardSettingsModalMode(null);
                                if (mode) {
                                  void startFlashcardsMode(mode);
                                }
                              }}
                            >
                              {tr('Save & Start', 'Save & Start')}
                            </button>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('assistant') && (
              <section className="webapp-section voice-assistant-section" ref={assistantRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Голосовой ассистент', 'Sprachassistent')}</h2>
                  <p>{tr('Практика разговорного немецкого в реальном времени.', 'Sprechtraining Deutsch in Echtzeit.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>

                {!assistantToken ? (
                  <div className="voice-assistant-join">
                    <div className="voice-assistant-meta">
                      <span>{tr('Пользователь', 'Nutzer')}: {assistantIdentity.displayName || '—'}</span>
                      <span>ID: {assistantIdentity.userId || '—'}</span>
                    </div>
                    {assistantError && <div className="webapp-error">{assistantError}</div>}
                    <button
                      type="button"
                      className="primary-button"
                      onClick={connectAssistant}
                      disabled={assistantConnecting || !webappUser?.id}
                    >
                      {assistantConnecting ? tr('Подключаем...', 'Verbinden...') : tr('Подключить ассистента', 'Assistent verbinden')}
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
                      onConnected={() => {
                        setAssistantError('');
                        startAssistantSessionTracking();
                      }}
                      onDisconnected={() => {
                        const sid = assistantSessionId;
                        if (sid) {
                          stopAssistantSessionTracking(sid);
                        }
                        setAssistantToken(null);
                      }}
                      onError={(e) => setAssistantError(`LiveKit error: ${e?.message || e}`)}
                      className="voice-assistant-room"
                    >
                      <div className="voice-assistant-room-head">
                        <div>
                          <span className="pill">{tr('Учитель онлайн', 'Lehrer online')}</span>
                          <h3>{tr('Живая практика немецкого', 'Live-Deutschpraxis')}</h3>
                        </div>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={disconnectAssistant}
                        >
                          {tr('Отключить', 'Trennen')}
                        </button>
                      </div>
                      <p className="voice-assistant-hint">
                        {tr('Нажмите на микрофон в панели управления и начинайте диалог.', 'Druecke auf das Mikrofon im Steuerfeld und starte den Dialog.')}
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

            {!flashcardsOnly && isSectionVisible('support') && (
              <section className="webapp-section support-section" ref={supportRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Техподдержка', 'Support')}</h2>
                  <p>{tr('Напишите вопрос администратору прямо здесь.', 'Schreibe deine Frage direkt an den Administrator.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>

                {supportError && <div className="webapp-error">{supportError}</div>}

                <div className="support-chat-shell">
                  <div className="support-chat-list">
                    {supportLoading && supportTimelineItems.length === 0 && (
                      <div className="webapp-muted">{tr('Загружаем переписку...', 'Chat wird geladen...')}</div>
                    )}
                    {!supportLoading && supportTimelineItems.length === 0 && (
                      <div className="webapp-muted">{tr('У вас пока нет сообщений. Напишите нам.', 'Du hast noch keine Nachrichten. Schreib uns.')}</div>
                    )}
                    {supportTimelineItems.map((item) => {
                      const role = String(item?.from_role || '').toLowerCase();
                      const isAdmin = role === 'admin';
                      const isFailed = Boolean(item?.is_failed);
                      const key = String(item?.id || item?.temp_id || `${item?.created_at || ''}:${item?.message_text || ''}`);
                      const attachmentUrl = String(item?.attachment_url || item?.preview_url || '').trim();
                      const hasImageAttachment = String(item?.attachment_kind || '').toLowerCase() === 'image' && attachmentUrl;
                      return (
                        <div key={key} className={`support-row ${isAdmin ? 'is-admin' : 'is-user'}`}>
                          <div className={`support-bubble ${isAdmin ? 'is-admin' : 'is-user'} ${isFailed ? 'is-failed' : ''}`}>
                            {hasImageAttachment && (
                              <a href={attachmentUrl} target="_blank" rel="noreferrer" className="support-bubble-image-link">
                                <img
                                  src={attachmentUrl}
                                  alt={String(item?.attachment_file_name || 'support image')}
                                  className="support-bubble-image"
                                  loading="lazy"
                                />
                              </a>
                            )}
                            {String(item?.message_text || '').trim() && (
                              <div className="support-bubble-text">{String(item?.message_text || '')}</div>
                            )}
                            <div className="support-bubble-meta">
                              <span>{formatSupportTime(item?.created_at)}</span>
                              {isFailed && (
                                <>
                                  <span className="support-failed-label">{tr('Не отправлено', 'Nicht gesendet')}</span>
                                  <button
                                    type="button"
                                    className="support-retry-button"
                                    onClick={() => sendSupportMessage({
                                      text: item?.message_text || '',
                                      attachment: item?.attachment_payload || null,
                                    }, String(item?.temp_id || ''))}
                                    disabled={supportSending}
                                  >
                                    {tr('Повторить', 'Erneut senden')}
                                  </button>
                                </>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                    <div ref={supportBottomRef} />
                  </div>

                  <form
                    className="support-composer"
                    onSubmit={(event) => {
                      event.preventDefault();
                      void sendSupportMessage({ text: supportDraft, attachment: supportAttachment });
                    }}
                  >
                    <div className="support-composer-fields">
                      {supportAttachment?.preview_url && (
                        <div className="support-attachment-preview">
                          <img src={supportAttachment.preview_url} alt={supportAttachment.file_name || 'support attachment'} className="support-attachment-preview-image" />
                          <button type="button" className="support-attachment-remove" onClick={clearSupportAttachment} disabled={supportSending}>
                            {tr('Убрать фото', 'Bild entfernen')}
                          </button>
                        </div>
                      )}
                      <textarea
                        value={supportDraft}
                        onChange={(event) => setSupportDraft(event.target.value)}
                        placeholder={tr('Напишите сообщение...', 'Nachricht schreiben...')}
                        rows={2}
                        disabled={supportSending}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' && !event.shiftKey) {
                            event.preventDefault();
                            void sendSupportMessage({ text: supportDraft, attachment: supportAttachment });
                          }
                        }}
                      />
                    </div>
                    <input
                      ref={supportAttachmentInputRef}
                      type="file"
                      accept="image/jpeg,image/png,image/webp"
                      onChange={handleSupportAttachmentSelect}
                      disabled={supportSending}
                      hidden
                    />
                    <button
                      type="button"
                      className="secondary-button support-attach-button"
                      onClick={() => supportAttachmentInputRef.current?.click()}
                      disabled={supportSending}
                    >
                      {supportAttachment?.preview_url ? tr('Фото выбрано', 'Bild gewaehlt') : tr('Фото', 'Bild')}
                    </button>
                    <button
                      type="submit"
                      className="primary-button"
                      disabled={supportSending || (!String(supportDraft || '').trim() && !supportAttachment?.image_base64)}
                    >
                      {supportSending ? tr('Отправка...', 'Senden...') : tr('Отправить', 'Senden')}
                    </button>
                  </form>
                </div>
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('analytics') && (
              <section className="webapp-section webapp-analytics" ref={analyticsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Аналитика', 'Analytik')}</h2>
                  <p className="webapp-muted">{tr('Языковая пара', 'Sprachpaar')}: {getActiveLanguagePairLabel()}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="analytics-controls">
                  <label className="webapp-field">
                    <span>{tr('Период', 'Zeitraum')}</span>
                    <select
                      value={analyticsPeriod}
                      onChange={(event) => setAnalyticsPeriod(event.target.value)}
                    >
                      <option value="day">{tr('День', 'Tag')}</option>
                      <option value="week">{tr('Неделя', 'Woche')}</option>
                      <option value="month">{tr('Месяц', 'Monat')}</option>
                      <option value="quarter">{tr('Квартал', 'Quartal')}</option>
                      <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
                      <option value="year">{tr('Год', 'Jahr')}</option>
                      <option value="all">{tr('Все время', 'Gesamt')}</option>
                    </select>
                  </label>
                  <label className="webapp-field analytics-scope-field">
                    <span>{tr('Режим участия', 'Teilnahme-Modus')}</span>
                    <select
                      value={analyticsScopeKey}
                      onChange={(event) => {
                        void handleAnalyticsScopeSelect(event.target.value);
                      }}
                      disabled={analyticsScopeLoading || analyticsScopeSaving}
                    >
                      {analyticsScopeOptions.map((item) => (
                        <option key={item.key} value={item.key}>{item.label}</option>
                      ))}
                    </select>
                  </label>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => loadAnalytics(undefined, analyticsScopeKey)}
                    disabled={analyticsLoading || analyticsScopeLoading || analyticsScopeSaving}
                  >
                    {analyticsLoading ? tr('Считаем...', 'Berechnen...') : tr('Обновить', 'Aktualisieren')}
                  </button>
                  {analyticsRank && (
                    <div className="analytics-rank">{tr('Ваше место', 'Dein Rang')}: #{analyticsRank}</div>
                  )}
                </div>
                <div className={`webapp-muted analytics-scope-hint ${analyticsScopeSelectorRequired ? 'is-warning' : ''}`}>
                  {analyticsScopeStatusText}
                  {analyticsScopeSelectorRequired ? ` ${tr('У вас несколько групп: выберите нужный режим участия.', 'Du hast mehrere Gruppen: waehle den passenden Teilnahme-Modus.')}` : ''}
                  {analyticsScopeLoading ? ` ${tr('Определяем контекст...', 'Kontext wird ermittelt...')}` : ''}
                  {analyticsScopeSaving ? ` ${tr('Сохраняем режим...', 'Modus wird gespeichert...')}` : ''}
                </div>

                {analyticsScopeError && <div className="webapp-error">{analyticsScopeError}</div>}
                {analyticsError && <div className="webapp-error">{analyticsError}</div>}

                {analyticsSummary && (
                  <div className="analytics-cards">
                    <div className="analytics-card">
                      <span>{tr('Переводы', 'Uebersetzungen')}</span>
                      <strong>{analyticsSummary.total_translations}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Успех', 'Erfolg')}</span>
                      <strong>{analyticsSummary.success_rate}%</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Средний балл', 'Durchschnitt')}</span>
                      <strong>{analyticsSummary.avg_score}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Среднее время', 'Durchschnittszeit')}</span>
                      <strong>{analyticsSummary.avg_time_min} {tr('мин', 'Min')}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Пропущено дней', 'Verpasste Tage')}</span>
                      <strong>{analyticsSummary.missed_days}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Пропущено', 'Verpasst')}</span>
                      <strong>{analyticsSummary.missed_sentences}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>{tr('Итоговый балл', 'Gesamtscore')}</span>
                      <strong>{analyticsSummary.final_score}</strong>
                    </div>
                  </div>
                )}

                {weeklyPlan && (
                  <div className="analytics-goals-grid">
                    {weeklyMetricRows.map((item) => (
                      <div className="analytics-goal-card" key={`analytics-goal-${item.key}`}>
                        <span>{item.title}</span>
                        <strong>
                          {formatWeeklyValue(item.data?.actual)} / {formatWeeklyValue(item.data?.goal)} {item.unit}
                        </strong>
                        <small>
                          {tr('Прогноз', 'Prognose')}: {formatWeeklyValue(item.data?.forecast, 1)} {item.unit}
                          {' • '}
                          {tr('% выполнения', '% Erfuellung')}: {formatWeeklyValue(item.data?.completion_percent, 1)}%
                        </small>
                      </div>
                    ))}
                  </div>
                )}

                <div className="analytics-chart" ref={analyticsTrendRef} />
                <div className="analytics-chart analytics-compare" ref={analyticsCompareRef} />
              </section>
            )}

            {canViewEconomics && !flashcardsOnly && isSectionVisible('economics') && (
              <section className="webapp-section webapp-economics" ref={economicsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Экономика', 'Kosten')}</h2>
                  <p className="webapp-muted">{tr('Учёт переменных и фиксированных затрат по пользователю.', 'Tracking von variablen und fixen Kosten pro Nutzer.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="analytics-controls economics-controls">
                  <label className="webapp-field">
                    <span>{tr('Период', 'Zeitraum')}</span>
                    <select value={economicsPeriod} onChange={(event) => setEconomicsPeriod(event.target.value)}>
                      <option value="week">{tr('Неделя', 'Woche')}</option>
                      <option value="month">{tr('Месяц', 'Monat')}</option>
                      <option value="quarter">{tr('Квартал', 'Quartal')}</option>
                      <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
                      <option value="year">{tr('Год', 'Jahr')}</option>
                      <option value="all">{tr('Все время', 'Gesamt')}</option>
                    </select>
                  </label>
                  <label className="webapp-field">
                    <span>{tr('Аллокация fixed', 'Fixed-Allokation')}</span>
                    <select value={economicsAllocation} onChange={(event) => setEconomicsAllocation(event.target.value)}>
                      <option value="weighted">{tr('По весу активности', 'Gewichtet')}</option>
                      <option value="equal">{tr('Поровну на активных', 'Gleich verteilt')}</option>
                    </select>
                  </label>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => loadEconomics(economicsPeriod, economicsAllocation)}
                    disabled={economicsLoading}
                  >
                    {economicsLoading ? tr('Считаем...', 'Berechnen...') : tr('Обновить', 'Aktualisieren')}
                  </button>
                </div>

                {economicsError && <div className="webapp-error">{economicsError}</div>}
                {!economicsError && economicsLoading && <div className="webapp-muted">{tr('Считаем расходы...', 'Kosten werden berechnet...')}</div>}

                {economicsSummary && (
                  <>
                    <div className="analytics-cards economics-cards">
                      <div className="analytics-card">
                        <span>{tr('Переменные', 'Variabel')}</span>
                        <strong>{Number(economicsSummary?.totals?.user_variable_cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Fixed (аллокация)', 'Fixed (allokiert)')}</span>
                        <strong>{Number(economicsSummary?.totals?.user_fixed_allocated_cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Итого', 'Gesamt')}</span>
                        <strong>{Number(economicsSummary?.totals?.user_total_cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('События', 'Ereignisse')}</span>
                        <strong>{Number(economicsSummary?.totals?.user_events_count || 0)}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Без цены (events)', 'Ohne Preis (Events)')}</span>
                        <strong>{Number(economicsSummary?.totals?.user_unpriced_events || 0)}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Ср. цена события', 'Ø Kosten/Ereignis')}</span>
                        <strong>{Number(economicsSummary?.totals?.avg_cost_per_user_event || 0).toFixed(4)} {economicsSummary?.currency || 'USD'}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Активных пользователей', 'Aktive Nutzer')}</span>
                        <strong>{Number(economicsSummary?.totals?.period_active_users || 0)}</strong>
                      </div>
                    </div>

                    <div className="economics-meta-row">
                      <span>{tr('Диапазон', 'Zeitraum')}: {economicsSummary?.range?.start_date} — {economicsSummary?.range?.end_date}</span>
                      <span>{tr('Метод аллокации', 'Allokation')}: {economicsSummary?.allocation_method === 'weighted' ? tr('взвешенный', 'gewichtet') : tr('равный', 'gleich')}</span>
                    </div>

                    <div className="analytics-cards economics-voice-cards">
                      {economicsVoiceRows.map((row) => {
                        const cost = Number(row?.item?.cost || 0);
                        const units = Number(row?.item?.units || 0);
                        const cardTone =
                          row.key === 'livekit_room_minutes'
                            ? `is-${livekitStatusColor || 'green'}`
                            : (cost > 0 ? 'is-red' : 'is-green');
                        return (
                          <div className={`analytics-card economics-voice-card ${cardTone}`} key={`voice-cost-${row.key}`}>
                            <span>{row.title}</span>
                            <strong>{cost.toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                            <small className="webapp-muted">
                              {units.toFixed(2)} {row.unit}
                              {row.key === 'livekit_room_minutes' && economicsSummary?.livekit_status?.free_minutes_month > 0 ? (
                                ` • ${Number(economicsSummary.livekit_status.user_month_minutes || 0).toFixed(1)} / `
                                + `${Number(economicsSummary.livekit_status.free_minutes_month).toFixed(1)} ${tr('мин', 'Min')}`
                                + ` (${Math.round(Number(economicsSummary.livekit_status.ratio_to_free_tier || 0) * 100)}%)`
                              ) : ''}
                            </small>
                          </div>
                        );
                      })}
                    </div>

                    <div className="economics-breakdown-grid">
                      <div className="economics-breakdown-card">
                        <h4>{tr('По провайдерам', 'Nach Providern')}</h4>
                        {(economicsSummary?.breakdown?.by_provider || []).slice(0, 8).map((item) => (
                          <div className="economics-breakdown-row" key={`provider-${item.provider}`}>
                            <span>{item.provider || 'n/a'}</span>
                            <strong>{Number(item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                          </div>
                        ))}
                        {(!economicsSummary?.breakdown?.by_provider || economicsSummary.breakdown.by_provider.length === 0) && (
                          <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
                        )}
                      </div>
                      <div className="economics-breakdown-card">
                        <h4>{tr('По действиям', 'Nach Aktionen')}</h4>
                        {(economicsSummary?.breakdown?.by_action_type || []).slice(0, 8).map((item) => (
                          <div className="economics-breakdown-row" key={`action-${item.action_type}`}>
                            <span>{item.action_type || 'n/a'}</span>
                            <strong>{Number(item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                          </div>
                        ))}
                        {(!economicsSummary?.breakdown?.by_action_type || economicsSummary.breakdown.by_action_type.length === 0) && (
                          <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
                        )}
                      </div>
                    </div>
                  </>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('subscription') && (
              <section className="webapp-section webapp-billing" ref={billingRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Подписка', 'Abo')}</h2>
                  <p className="webapp-muted">{tr('Текущий тариф, лимиты и Stripe Portal: отмена подписки, смена карты и счета.', 'Aktueller Tarif, Limits und Stripe-Portal: Kuendigung, Kartenwechsel und Rechnungen.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>

                {billingReturnMessage && (
                  <div className={billingReturnContext.kind === 'cancel' ? 'webapp-muted' : 'webapp-success'}>
                    {billingReturnMessage}
                  </div>
                )}
                {billingStatusError && <div className="webapp-error">{billingStatusError}</div>}
                {billingPlansError && <div className="webapp-error">{billingPlansError}</div>}
                {(billingStatusLoading || billingPlansLoading) && <div className="webapp-muted">{tr('Загружаем статус подписки...', 'Abo-Status wird geladen...')}</div>}

                {billingStatus && (
                  <>
                    <div className="analytics-cards economics-cards">
                      <div className="analytics-card">
                        <span>{tr('План', 'Plan')}</span>
                        <strong>{String(billingStatus?.plan_name || billingStatus?.effective_mode || 'free')}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Статус', 'Status')}</span>
                        <strong>{String(billingStatus?.status || 'inactive')}</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Расход сегодня', 'Heute verbraucht')}</span>
                        <strong>{Number(billingStatus?.spent_today_eur || 0).toFixed(2)} EUR</strong>
                      </div>
                      <div className="analytics-card">
                        <span>{tr('Дневной cap', 'Tages-Cap')}</span>
                        <strong>{billingStatus?.cap_today_eur == null ? tr('Без лимита', 'Unbegrenzt') : `${Number(billingStatus?.cap_today_eur || 0).toFixed(2)} EUR`}</strong>
                      </div>
                    </div>

                    {billingStatus?.trial_ends_at && (
                      <div className="webapp-muted">
                        {tr('Trial активен до', 'Trial aktiv bis')}: {new Date(billingStatus.trial_ends_at).toLocaleString()}
                      </div>
                    )}
                    <div className="webapp-muted">
                      {tr('Сброс лимитов', 'Limits-Reset')}: {billingStatus?.reset_at ? new Date(billingStatus.reset_at).toLocaleString() : '—'}
                    </div>

                    <div className="billing-trial-banner">
                      <strong>{tr('Первые 3 дня: Trial Pro для всех новых пользователей.', 'Die ersten 3 Tage: Trial Pro fuer alle neuen Nutzer.')}</strong>
                      <p>
                        {tr(
                          'В этот период работает расширенный доступ, но действует внутренний технический cap расходов (он невидим пользователю).',
                          'In diesem Zeitraum gilt erweiterter Zugriff, aber mit internem technischem Kosten-Cap (fuer Nutzer nicht sichtbar).'
                        )}
                      </p>
                    </div>
                    <div className="billing-policy-grid">
                      <article className="billing-policy-card">
                        <h4>{tr('Free после trial', 'Free nach Trial')}</h4>
                        <ul>
                          <li>{tr('Переводы: безлимитно.', 'Uebersetzungen: unbegrenzt.')}</li>
                          <li>{tr('Читалка: 1 книга (до 30 дней), новая только после удаления старой.', 'Reader: 1 Buch (bis 30 Tage), neues nur nach Loeschen des alten.')}</li>
                          <li>{tr('Аудио из читалки: недоступно.', 'Audio aus Reader: nicht verfuegbar.')}</li>
                          <li>{tr('Карточки: по 5 слов в день на каждый вид тренировки.', 'Karteikarten: 5 Woerter pro Tag je Trainingsmodus.')}</li>
                          <li>{tr('«Почувствуй слово»: 3 раза в день.', '„Wort fuehlen“: 3-mal pro Tag.')}</li>
                          <li>{tr('Разговорная практика: 3 минуты в день.', 'Sprechpraxis: 3 Minuten pro Tag.')}</li>
                          <li>{tr('Прокачка навыков: 1 навык.', 'Skill-Training: 1 Skill.')}</li>
                        </ul>
                      </article>
                      <article className="billing-policy-card">
                        <h4>{tr('Pro / Coffee / Cheesecake', 'Pro / Coffee / Cheesecake')}</h4>
                        <ul>
                          <li>{tr('Переводы, читалка, карточки, «почувствуй слово»: безлимитно.', 'Uebersetzungen, Reader, Karteikarten, „Wort fuehlen“: unbegrenzt.')}</li>
                          <li>{tr('Аудио из читалки: до 10 страниц за 7 дней.', 'Audio aus Reader: bis zu 10 Seiten in 7 Tagen.')}</li>
                          <li>{tr('Разговорная практика: 15 минут в день.', 'Sprechpraxis: 15 Minuten pro Tag.')}</li>
                          <li>{tr('Прокачка навыков: безлимитно.', 'Skill-Training: unbegrenzt.')}</li>
                          <li>{tr('Можно обсудить индивидуальную доработку/тренировку (по технической возможности).', 'Individuelle Anpassung/Training kann besprochen werden (sofern technisch moeglich).')}</li>
                        </ul>
                      </article>
                    </div>
                    <div className="billing-support-grid">
                      {billingPlanCards.map((offer) => {
                        const hasBillingPlansCatalog = !billingPlansLoading
                          && !billingPlansError
                          && Array.isArray(billingPlans)
                          && billingPlans.length > 0;
                        const planMeta = Array.isArray(billingPlans)
                          ? billingPlans.find((plan) => String(plan?.plan_code || '').trim().toLowerCase() === offer.planCode)
                          : null;
                        const selectedPlanCode = String(billingStatus?.plan_code || '').trim().toLowerCase();
                        const selectedEffectiveMode = String(billingStatus?.effective_mode || '').trim().toLowerCase();
                        const isCurrentPlan = selectedPlanCode === offer.planCode
                          || (selectedEffectiveMode === 'pro' && offer.planCode === 'pro');
                        const isPaidPlan = Boolean(planMeta?.is_paid);
                        const isInactivePlan = Boolean(planMeta && planMeta.is_active === false);
                        const canSelect = !isCurrentPlan && isPaidPlan && !isInactivePlan;
                        let buttonText = tr('Выбрать тариф', 'Tarif waehlen');
                        if (isCurrentPlan) {
                          buttonText = tr('Текущий тариф', 'Aktueller Tarif');
                        } else if (!isPaidPlan) {
                          buttonText = tr('Бесплатный план', 'Kostenloser Tarif');
                        } else if (billingActionLoading) {
                          buttonText = tr('Открываем...', 'Oeffnen...');
                        }
                        return (
                          <article className="billing-support-card" key={offer.planCode}>
                            <div className="billing-support-card__eyebrow">{offer.eyebrow}</div>
                            <div className="billing-support-card__title-row">
                              <h3>{offer.title}</h3>
                              <button
                                type="button"
                                className="billing-plan-info-button"
                                onClick={() => setBillingPlanDetailsOpenFor(offer.planCode)}
                                aria-label={tr('Показать лимиты тарифа', 'Tariflimits anzeigen')}
                                title={tr('Показать лимиты тарифа', 'Tariflimits anzeigen')}
                              >
                                i
                              </button>
                            </div>
                            {offer.blurb && <p className="billing-support-card__blurb">{offer.blurb}</p>}
                            {offer.priceLabel && <div className="billing-support-card__price">{offer.priceLabel}</div>}
                            <button
                              type="button"
                              className="secondary-button billing-support-card__button"
                              onClick={() => canSelect && handleBillingUpgrade(offer.planCode)}
                              disabled={billingActionLoading || !canSelect}
                            >
                              {buttonText}
                            </button>
                            {hasBillingPlansCatalog && isInactivePlan && (
                              <div className="webapp-muted">
                                {tr('Тариф временно неактивен.', 'Der Tarif ist voruebergehend inaktiv.')}
                              </div>
                            )}
                          </article>
                        );
                      })}
                    </div>
                    {activeBillingPlanDetails && (
                      <div
                        role="presentation"
                        className="billing-plan-details-modal"
                        onClick={() => setBillingPlanDetailsOpenFor('')}
                      >
                        <div
                          className="billing-plan-details-modal__panel"
                          role="dialog"
                          aria-modal="true"
                          aria-label={activeBillingPlanDetails.title}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <div className="billing-plan-details-modal__head">
                            <h3>{activeBillingPlanDetails.title}</h3>
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => setBillingPlanDetailsOpenFor('')}
                            >
                              {tr('Закрыть', 'Schliessen')}
                            </button>
                          </div>
                          <ul>
                            {activeBillingPlanDetails.items.map((item, index) => (
                              <li key={`${billingPlanDetailsOpenFor}_limit_${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    )}

                    {billingStatus?.manage?.available && (
                      <div className="webapp-section-actions">
                        <button type="button" className="secondary-button" onClick={handleBillingManage} disabled={billingActionLoading}>
                          {billingActionLoading ? tr('Открываем...', 'Oeffnen...') : tr('Управлять подпиской в Stripe Portal', 'Abo im Stripe-Portal verwalten')}
                        </button>
                      </div>
                    )}
                  </>
                )}

                {appMode !== 'telegram' && (
                  <div className="economics-breakdown-grid">
                    <div className="economics-breakdown-card">
                      <h4>{tr('Установить приложение', 'App installieren')}</h4>
                      {appMode === 'browser' ? (
                        <ol className="webapp-muted" style={{ margin: 0, paddingLeft: '18px' }}>
                          <li>{tr('Откройте сайт в Safari', 'Seite in Safari oeffnen')}</li>
                          <li>{tr('Нажмите Share (квадрат со стрелкой)', 'Auf Share tippen (Quadrat mit Pfeil)')}</li>
                          <li>{tr('Выберите Add to Home Screen', 'Add to Home Screen waehlen')}</li>
                          <li>{tr('Запустите приложение с иконки', 'App vom Homescreen starten')}</li>
                        </ol>
                      ) : (
                        <div className="webapp-muted">
                          {tr('Уже установлено. Вы открыли приложение в standalone режиме.', 'Bereits installiert. Die App laeuft im Standalone-Modus.')}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </section>
            )}

            {selectionText && selectionPos && (isSectionVisible('youtube') || isSectionVisible('dictionary') || isSectionVisible('translations') || isSectionVisible('reader')) && (
              <div
                ref={selectionMenuRef}
                className={`webapp-selection-menu ${selectionCompact ? 'is-compact' : ''} ${(youtubeAppFullscreen || selectionInlineMode) ? 'is-overlay-mode' : ''} ${selectionType ? `is-${selectionType}` : ''}`}
                style={{
                  left: `${selectionPos.x}px`,
                  top: `${selectionPos.y}px`,
                  minWidth: 180,
                  maxWidth: 280,
                  borderRadius: 10,
                  padding: '8px 9px',
                  boxShadow: '0 14px 28px rgba(8, 11, 26, 0.45)',
                  gap: 6,
                }}
                onMouseLeave={clearSelection}
              >
                <div
                  className="webapp-selection-text"
                  style={{
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    maxWidth: 248,
                    fontSize: 12,
                    lineHeight: 1.2,
                  }}
                  title={selectionText}
                >
                  {selectionText}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: isInlineSelectionMenu ? '1fr' : '1fr 1fr', gap: 6 }}>
                  {!isInlineSelectionMenu && (
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => handleQuickLookupDictionary(selectionText)}
                    disabled={selectionLookupLoading || selectionInlineLookup.loading}
                    style={{ minHeight: 32, padding: '6px 8px', fontSize: 12 }}
                  >
                    {selectionLookupLoading || selectionInlineLookup.loading ? tr('Quick...', 'Quick...') : 'Quick'}
                  </button>
                  )}
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => { void handleSelectionSave(selectionText); }}
                    disabled={dictionaryLoading}
                    style={{ minHeight: 32, padding: '6px 8px', fontSize: 12 }}
                  >
                    {dictionaryLoading ? tr('Словарь...', 'Woerterbuch...') : tr('Сохранить', 'Speichern')}
                  </button>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 6 }}>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={handleSelectionGptLookup}
                    disabled={selectionGptLoading}
                    style={{ minHeight: 32, padding: '6px 8px', fontSize: 12 }}
                  >
                    {selectionGptLoading ? 'GPT...' : 'GPT'}
                  </button>
                </div>
                {(selectionInlineLookup.loading || selectionInlineLookup.translation) && (
                  <div className="webapp-selection-translation" style={{ fontSize: 11, padding: '5px 7px' }}>
                    {selectionInlineLookup.provider && (
                      <div className="webapp-selection-provider">
                        {String(selectionInlineLookup.provider).toUpperCase()}
                      </div>
                    )}
                    {selectionInlineLookup.loading
                      ? tr('Переводим...', 'Uebersetzen...')
                      : (selectionInlineLookup.translation || '—')}
                  </div>
                )}
              </div>
            )}
            {selectionGptOpen && (
              <div
                role="presentation"
                onClick={(event) => {
                  if (event.target === event.currentTarget) {
                    closeSelectionGptSheet();
                  }
                }}
                style={{
                  position: 'fixed',
                  inset: 0,
                  zIndex: 180,
                  background: isLightTheme ? 'rgba(96, 72, 44, 0.26)' : 'rgba(2, 6, 23, 0.56)',
                  display: 'flex',
                  alignItems: 'flex-end',
                  justifyContent: 'center',
                }}
              >
                <section
                  style={{
                    width: 'min(100%, 760px)',
                    maxHeight: '82vh',
                    overflow: 'auto',
                    background: isLightTheme ? 'rgba(253, 247, 236, 0.98)' : 'rgba(10, 18, 36, 0.98)',
                    borderTopLeftRadius: 16,
                    borderTopRightRadius: 16,
                    border: isLightTheme
                      ? '1px solid rgba(171, 139, 98, 0.34)'
                      : '1px solid rgba(148, 163, 184, 0.35)',
                    boxShadow: isLightTheme
                      ? '0 -12px 34px rgba(105, 78, 47, 0.28)'
                      : '0 -12px 34px rgba(2, 6, 23, 0.58)',
                    padding: '14px 14px 18px',
                    display: 'grid',
                    gap: 10,
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                    <strong style={{ fontSize: 14 }}>{tr('GPT Объяснение', 'GPT-Erklaerung')}</strong>
                    <button type="button" className="secondary-button" onClick={closeSelectionGptSheet}>
                      {tr('Закрыть', 'Schliessen')}
                    </button>
                  </div>
                  <div className="webapp-muted" style={{ fontSize: 12 }}>
                    {selectionText}
                  </div>
                  {selectionGptLoading && <div className="webapp-muted">{tr('Готовим объяснение...', 'Erklaerung wird vorbereitet...')}</div>}
                  {selectionGptError && <div className="webapp-error">{selectionGptError}</div>}
                  {!selectionGptLoading && !selectionGptError && (
                    <>
                      <div className="webapp-selection-translation">
                        <div style={{ fontWeight: 700, marginBottom: 4 }}>{tr('Перевод', 'Uebersetzung')}</div>
                        <div>{selectionGptData.translation || '—'}</div>
                      </div>
                  <div className="webapp-selection-translation">
                    <div style={{ fontWeight: 700, marginBottom: 4 }}>{tr('Смысл / заметки', 'Bedeutung / Hinweise')}</div>
                    <div
                      style={{ whiteSpace: 'pre-wrap' }}
                      dangerouslySetInnerHTML={{ __html: renderRichText(selectionGptData.notes || '—') }}
                    />
                  </div>
                      <div className="webapp-selection-translation">
                        <div style={{ fontWeight: 700, marginBottom: 4 }}>{tr('Примеры', 'Beispiele')}</div>
                        {Array.isArray(selectionGptData.examples) && selectionGptData.examples.length > 0 ? (
                          <div style={{ display: 'grid', gap: 6 }}>
                            {selectionGptData.examples.map((item, index) => (
                              <label key={`gpt-example-${index}`} className="webapp-gpt-save-option">
                                <input
                                  type="checkbox"
                                  checked={Boolean(selectionGptSaveExamplesChecked[index])}
                                  onChange={(event) => {
                                    const checked = event.target.checked;
                                    setSelectionGptSaveExamplesChecked((prev) => ({
                                      ...prev,
                                      [index]: checked,
                                    }));
                                    setSelectionGptSaveError('');
                                    setSelectionGptSaveMessage('');
                                  }}
                                />
                                <span>{item}</span>
                              </label>
                            ))}
                          </div>
                        ) : (
                          <div>—</div>
                        )}
                      </div>
                      <div className="webapp-selection-translation webapp-gpt-save-block">
                        <div style={{ fontWeight: 700, marginBottom: 6 }}>{tr('Сохранить в словарь', 'Im Woerterbuch speichern')}</div>
                        <label className="webapp-gpt-save-option">
                          <input
                            type="checkbox"
                            checked={selectionGptSaveOriginalChecked}
                            onChange={(event) => {
                              setSelectionGptSaveOriginalChecked(event.target.checked);
                              setSelectionGptSaveError('');
                              setSelectionGptSaveMessage('');
                            }}
                          />
                          <span>
                            {tr('Оригинальное слово (с артиклем, если это существительное)', 'Originalwort (mit Artikel, falls es ein Nomen ist)')}
                          </span>
                        </label>
                        <div className="webapp-gpt-save-actions">
                          <button
                            type="button"
                            className="secondary-button webapp-gpt-save-button"
                            onClick={handleSelectionGptSaveToDictionary}
                            disabled={selectionGptSaveLoading}
                          >
                            {selectionGptSaveLoading
                              ? tr('Сохраняем...', 'Speichern...')
                              : tr('Сохранить в словарь', 'Ins Woerterbuch speichern')}
                          </button>
                          <span className="webapp-muted" style={{ fontSize: 11 }}>
                            {tr('Можно выбрать одно или несколько значений', 'Du kannst einen oder mehrere Eintraege waehlen')}
                          </span>
                        </div>
                        {selectionGptSaveMessage && (
                          <div className="webapp-gpt-save-status">
                            {selectionGptSaveMessage}
                          </div>
                        )}
                        {selectionGptSaveError && (
                          <div className="webapp-gpt-save-status is-error">
                            {selectionGptSaveError}
                          </div>
                        )}
                      </div>
                    </>
                  )}
                </section>
              </div>
            )}
            {inlineToast && (
              <div
                className="webapp-inline-toast"
                style={{ '--toast-duration': `${inlineToastDurationMs}ms` }}
              >
                {inlineToast}
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
            <h2>{tr('Вход в урок', 'Unterricht betreten')}</h2>
            <p>{tr('Подключитесь к разговорной практике и начните диалог с учителем.', 'Verbinde dich fuer Sprachpraxis und starte den Dialog mit dem Tutor.')}</p>
          </div>
          <form onSubmit={handleConnect} className="login-form">
            <label className="field">
              <span>Telegram ID</span>
              <input
                type="text"
                placeholder={tr('Ваш Telegram ID (цифры)', 'Deine Telegram-ID (Ziffern)')}
                value={telegramID}
                onChange={(e) => setTelegramID(e.target.value)}
              />
            </label>

            <label className="field">
              <span>{tr('Ваше имя', 'Dein Name')}</span>
              <input
                type="text"
                placeholder={tr('Как вас называть? (Имя)', 'Wie sollen wir dich nennen?')}
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </label>

            <button type="submit" className="primary-button">
              {tr('Начать урок', 'Unterricht starten')}
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
            <span className="pill">{tr('Учитель онлайн', 'Lehrer online')}</span>
            <h1>{tr('Живая практика немецкого', 'Live-Deutschpraxis')}</h1>
            <p>{tr('Говорите свободно, а помощник ведет диалог, исправляет и поддерживает.', 'Sprich frei, der Assistent fuehrt den Dialog, korrigiert und unterstuetzt dich.')}</p>
          </div>
          <div className="lesson-meta">
            <span>{tr('Пользователь', 'Nutzer')}: {username}</span>
            <span>ID: {telegramID}</span>
          </div>
        </header>

        <main className="lesson-main">
          <section className="lesson-hero">
            <div className="lesson-illustration" aria-hidden="true">
              <img src={heroMascotSrc} alt="" aria-hidden="true" className="lesson-hero-image" />
            </div>
            <div className="lesson-copy">
              <h2>{tr('Сфокусируйтесь на голосе', 'Fokussiere dich auf die Stimme')}</h2>
              <p>{tr('Нажмите на микрофон, чтобы включить речь, и нажмите выход, когда урок завершен.', 'Druecke auf das Mikrofon, um zu sprechen, und beende danach die Sitzung.')}</p>
              <div className="lesson-tips">
                <div className="tip">{tr('Четко формулируйте ответы, чтобы учитель слышал интонацию.', 'Formuliere klar, damit der Tutor die Intonation hoert.')}</div>
                <div className="tip">{tr('Если нужно подумать, просто сделайте паузу — связь сохранится.', 'Wenn du nachdenken willst, pausier kurz - die Verbindung bleibt bestehen.')}</div>
              </div>
            </div>
          </section>

          <section className="lesson-controls">
            <h3>{tr('Управление уроком', 'Unterrichtssteuerung')}</h3>
            <p>{tr('Все основные действия собраны в центре: микрофон, выход и настройки.', 'Alle Hauptaktionen sind zentral: Mikrofon, Verlassen und Einstellungen.')}</p>
            <div className="lesson-control-bar">
              <ControlBar />
            </div>
            <div className="lesson-hint">{tr('Совет: держите окно открытым, чтобы учитель не прерывал сессию.', 'Tipp: Lass das Fenster offen, damit die Sitzung nicht unterbrochen wird.')}</div>
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
