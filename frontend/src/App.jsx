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
import { createTranslator, getPreferredLanguage, normalizeLanguage } from './i18n';
import { detectAppMode } from './utils/appMode';

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
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

function AppInner() {
  const telegramApp = useMemo(() => window.Telegram?.WebApp, []);
  const [appMode, setAppMode] = useState(() => detectAppMode());
  const isWebAppMode = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const isWebappPath =
      window.location.pathname === '/webapp' ||
      window.location.pathname === '/webapp/review';
    return Boolean(telegramApp?.initData) || params.get('mode') === 'webapp' || isWebappPath;
  }, [telegramApp]);

  const [initData, setInitData] = useState(telegramApp?.initData || '');
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
  const [dictionarySaved, setDictionarySaved] = useState('');
  const [dictionaryDirection, setDictionaryDirection] = useState('ru-de');
  const [dictionaryLanguagePair, setDictionaryLanguagePair] = useState(null);
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
  const [youtubeTranslationEnabled, setYoutubeTranslationEnabled] = useState(false);
  const [youtubeOverlayEnabled, setYoutubeOverlayEnabled] = useState(false);
  const [youtubeAppFullscreen, setYoutubeAppFullscreen] = useState(false);
  const [youtubeIsPaused, setYoutubeIsPaused] = useState(false);
  const [youtubePlaybackStarted, setYoutubePlaybackStarted] = useState(false);
  const [youtubeForceShowPanel, setYoutubeForceShowPanel] = useState(false);
  const [youtubeManualOverride, setYoutubeManualOverride] = useState(false);
  const [youtubeTranscriptHasTiming, setYoutubeTranscriptHasTiming] = useState(true);
  const [movies, setMovies] = useState([]);
  const [moviesLoading, setMoviesLoading] = useState(false);
  const [moviesError, setMoviesError] = useState('');
  const [moviesCollapsed, setMoviesCollapsed] = useState(false);
  const [moviesLanguageFilter, setMoviesLanguageFilter] = useState('all');
  const [showManualTranscript, setShowManualTranscript] = useState(false);
  const [manualTranscript, setManualTranscript] = useState('');
  const [readerInput, setReaderInput] = useState('');
  const [readerSelectedFile, setReaderSelectedFile] = useState(null);
  const [readerLoading, setReaderLoading] = useState(false);
  const [readerError, setReaderError] = useState('');
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
  const [readerFontSize, setReaderFontSize] = useState(18);
  const [readerFontWeight, setReaderFontWeight] = useState(500);
  const [selectionText, setSelectionText] = useState('');
  const [selectionPos, setSelectionPos] = useState(null);
  const [selectionCompact, setSelectionCompact] = useState(false);
  const [selectionLookupLang, setSelectionLookupLang] = useState('');
  const [selectionInlineMode, setSelectionInlineMode] = useState(false);
  const [selectionInlineLookup, setSelectionInlineLookup] = useState({ loading: false, word: '', translation: '', direction: '' });
  const [selectionLookupLoading, setSelectionLookupLoading] = useState(false);
  const [inlineToast, setInlineToast] = useState('');
  const [lastLookupScrollY, setLastLookupScrollY] = useState(null);
  const [telegramFullscreenMode, setTelegramFullscreenMode] = useState(false);
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
  const [translationAudioGrammarOptIn, setTranslationAudioGrammarOptIn] = useState({});
  const [translationAudioGrammarSaving, setTranslationAudioGrammarSaving] = useState({});
  const [skillReport, setSkillReport] = useState(null);
  const [skillReportLoading, setSkillReportLoading] = useState(false);
  const [skillReportError, setSkillReportError] = useState('');
  const [skillPracticeLoading, setSkillPracticeLoading] = useState({});
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
    translations: true,
    learned_words: true,
    agent_minutes: true,
    reading_minutes: true,
  });
  const [srsLoading, setSrsLoading] = useState(false);
  const [srsSubmitting, setSrsSubmitting] = useState(false);
  const [srsSubmittingRating, setSrsSubmittingRating] = useState(null);
  const [srsRevealAnswer, setSrsRevealAnswer] = useState(false);
  const [srsRevealStartedAt, setSrsRevealStartedAt] = useState(0);
  const [srsRevealElapsedSec, setSrsRevealElapsedSec] = useState(0);
  const [srsError, setSrsError] = useState('');
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
  const [globalTimerSuspended, setGlobalTimerSuspended] = useState(false);
  const [globalPauseReason, setGlobalPauseReason] = useState('');
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
  const [languageProfile, setLanguageProfile] = useState(null);
  const [languageProfileDraft, setLanguageProfileDraft] = useState({ learning_language: 'de', native_language: 'ru' });
  const [languageProfileLoading, setLanguageProfileLoading] = useState(false);
  const [languageProfileSaving, setLanguageProfileSaving] = useState(false);
  const [languageProfileError, setLanguageProfileError] = useState('');
  const [languageProfileModalOpen, setLanguageProfileModalOpen] = useState(false);
  const isStorySession = sessionType === 'story' || isStoryTopic(selectedTopic);
  const isStoryResultMode = Boolean(storyResult && isStorySession);
  const SRS_EASY_LOCK_AFTER_SEC = 5;
  const SRS_GOOD_LOCK_AFTER_SEC = 7;
  const srsEasyLocked = srsRevealAnswer && srsRevealElapsedSec >= SRS_EASY_LOCK_AFTER_SEC;
  const srsGoodLocked = srsRevealAnswer && srsRevealElapsedSec >= SRS_GOOD_LOCK_AFTER_SEC;

  const dictionaryRef = useRef(null);
  const theoryRef = useRef(null);
  const readerRef = useRef(null);
  const readerArticleRef = useRef(null);
  const flashcardsRef = useRef(null);
  const translationsRef = useRef(null);
  const youtubeRef = useRef(null);
  const moviesRef = useRef(null);
  const youtubeSubtitlesRef = useRef(null);
  const youtubePlayerRef = useRef(null);
  const youtubePlayerShellRef = useRef(null);
  const youtubeTimeIntervalRef = useRef(null);
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
  const ttsCacheRef = useRef(new Map());
  const ttsLastRef = useRef({ key: '', ts: 0 });
  const ttsCurrentAudioRef = useRef(null);
  const audioContextRef = useRef(null);
  const positiveAudioRef = useRef(null);
  const negativeAudioRef = useRef(null);
  const timeoutAudioRef = useRef(null);
  const avatarInputRef = useRef(null);
  const analyticsRef = useRef(null);
  const economicsRef = useRef(null);
  const billingRef = useRef(null);
  const assistantRef = useRef(null);
  const analyticsTrendRef = useRef(null);
  const analyticsCompareRef = useRef(null);
  const selectionMenuRef = useRef(null);
  const telegramLoginWidgetRef = useRef(null);
  const youtubeAutoFolderCacheRef = useRef(new Map());
  const youtubeAutoFolderPendingRef = useRef(new Map());
  const inlineToastTimeoutRef = useRef(null);
  const readerSessionStartingRef = useRef(false);
  const readerStateSaveTimeoutRef = useRef(null);
  const readerTimerIntervalRef = useRef(null);
  const readerSwipeStartRef = useRef(null);
  const readerPageNavLockRef = useRef(false);
  const todayTimerCompletionLockRef = useRef(new Set());
  const globalTimerAutoPauseInFlightRef = useRef(false);
  const sectionVisibilitySnapshotRef = useRef(null);
  const autoPausedTodayTimerIdsRef = useRef(new Set());
  const readerAutoPausedByNavigationRef = useRef(false);
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

  const [uiLang, setUiLang] = useState('ru');
  const t = useMemo(() => createTranslator(uiLang), [uiLang]);
  const tr = (ru, de) => (uiLang === 'de' ? de : ru);
  const readApiError = async (response, fallbackRu, fallbackDe) => {
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
  };
  const normalizeNetworkErrorMessage = (error, fallbackRu, fallbackDe) => {
    const fallback = tr(fallbackRu, fallbackDe);
    const raw = String(error?.message || '').trim().toLowerCase();
    if (!raw) return fallback;
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
  };
  const initDataMissingMsg = tr(
    'initData не найдено. Откройте Web App внутри Telegram.',
    'initData nicht gefunden. Oeffne die Web App in Telegram.'
  );
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

  const toggleLanguage = () => {
    setUiLang((prev) => (prev === 'ru' ? 'de' : 'ru'));
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
    return resolveFlashcardTexts(entry).targetText;
  };

  const loadSrsNextCard = async () => {
    if (!initData) return;
    try {
      setSrsLoading(true);
      setSrsError('');
      let response = await fetch(`/api/cards/next?initData=${encodeURIComponent(initData)}`);
      if (!response.ok && response.status >= 500) {
        await new Promise((resolve) => setTimeout(resolve, 220));
        response = await fetch(`/api/cards/next?initData=${encodeURIComponent(initData)}`);
      }
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки SRS карточки', 'Fehler beim Laden der SRS-Karte'));
      }
      const data = await response.json();
      setSrsCard(data.card || null);
      setSrsState(data.srs || null);
      setSrsQueueInfo(data.queue_info || { due_count: 0, new_remaining_today: 0 });
      setSrsRevealAnswer(false);
      srsShownAtRef.current = Date.now();
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось загрузить FSRS карточку.', 'FSRS-Karte konnte nicht geladen werden.');
      setSrsError(friendly);
      setWebappError(`${tr('Ошибка загрузки SRS карточки', 'Fehler beim Laden der SRS-Karte')}: ${friendly}`);
    } finally {
      setSrsLoading(false);
    }
  };

  const loadTodayPlan = async () => {
    if (!initData) return;
    try {
      setTodayPlanLoading(true);
      setTodayPlanError('');
      const response = await fetch(`/api/today?initData=${encodeURIComponent(initData)}`);
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
      const response = await fetch(`/api/progress/skills?period=7d&initData=${encodeURIComponent(initData)}`);
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки отчета по навыкам', 'Fehler beim Laden des Skills-Reports'));
      }
      const data = await response.json();
      setSkillReport({
        updated_at: data?.updated_at || null,
        top_weak: Array.isArray(data?.top_weak) ? data.top_weak : [],
        groups: Array.isArray(data?.groups) ? data.groups : [],
        total_skills: Number(data?.total_skills || 0),
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
      const response = await fetch(`/api/progress/weekly-plan?initData=${encodeURIComponent(initData)}`);
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
      const response = await fetch(`/api/progress/plan-analytics?initData=${encodeURIComponent(initData)}&period=${encodeURIComponent(period)}`);
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

  const startSkillPractice = async (skill) => {
    if (!initData || !skill?.skill_id) return;
    const skillId = String(skill.skill_id);
    try {
      setSkillPracticeLoading((prev) => ({ ...prev, [skillId]: true }));
      setSkillReportError('');
      const response = await fetch(`/api/progress/skills/${encodeURIComponent(skillId)}/practice/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, level: selectedLevel }),
      });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка запуска прокачки', 'Fehler beim Start der Skill-Uebung'));
      }
      const data = await response.json();
      if (data?.blocked) {
        setFinishMessage(
          tr(
            'Есть активная сессия. Завершите текущую, чтобы начать новую тренировку.',
            'Es gibt eine aktive Session. Beende die aktuelle, um eine neue Uebung zu starten.'
          )
        );
      }
      await loadSessionInfo();
      await loadSentences();
      openSingleSectionAndScroll('translations', translationsRef);
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось запустить тренировку навыка.',
        'Skill-Uebung konnte nicht gestartet werden.'
      );
      setSkillReportError(friendly);
    } finally {
      setSkillPracticeLoading((prev) => {
        const next = { ...prev };
        delete next[skillId];
        return next;
      });
    }
  };

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
    const timerRunning = Boolean(payload?.timer_running);
    const startedAtRaw = String(payload?.timer_started_at || '').trim();
    if (!timerRunning || !startedAtRaw) return baseSeconds;
    const startedMs = Date.parse(startedAtRaw);
    if (!Number.isFinite(startedMs)) return baseSeconds;
    const liveExtra = Math.max(0, Math.floor((nowMs - startedMs) / 1000));
    return baseSeconds + liveExtra;
  };

  const getTodayItemProgressPercent = (item, nowMs = Date.now()) => {
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

  const renderTodaySectionTaskHud = (sectionKey) => {
    const item = getTodayTaskForSection(sectionKey);
    if (!item) return null;
    const elapsed = getTodayItemElapsedSeconds(item, todayTimerNowMs);
    const progress = getTodayItemProgressPercent(item, todayTimerNowMs);
    const done = String(item?.status || '').toLowerCase() === 'done' || progress >= 100;
    const running = isTodayItemTimerRunning(item);
    return (
      <div className="today-section-task-hud">
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
    await syncTodayItemTimer(effectiveItem, 'start', {
      elapsedSeconds: getTodayItemElapsedSeconds(effectiveItem, Date.now()),
      running: true,
    });
    const taskType = String(effectiveItem?.task_type || '').toLowerCase();
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
      setSrsError('');
      setSrsSubmitting(true);
      setSrsSubmittingRating(ratingValue);
      let response = await fetch('/api/cards/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          card_id: cardId,
          rating: ratingValue,
          response_ms: responseMs,
        }),
      });
      if (!response.ok && response.status >= 500) {
        await new Promise((resolve) => setTimeout(resolve, 220));
        response = await fetch('/api/cards/review', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            initData,
            card_id: cardId,
            rating: ratingValue,
            response_ms: responseMs,
          }),
        });
      }
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка SRS review', 'Fehler bei SRS-Review'));
      }
      const data = await response.json();
      setSrsRevealAnswer(false);
      setSrsRevealStartedAt(0);
      setSrsRevealElapsedSec(0);
      if (data?.next && typeof data.next === 'object') {
        setSrsCard(data.next.card || null);
        setSrsState(data.next.srs || null);
        setSrsQueueInfo(data.next.queue_info || { due_count: 0, new_remaining_today: 0 });
        srsShownAtRef.current = Date.now();
      } else {
        await loadSrsNextCard();
      }
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(error, 'Не удалось сохранить оценку.', 'Bewertung konnte nicht gespeichert werden.');
      setSrsError(friendly);
      setWebappError(`${tr('Ошибка SRS review', 'Fehler bei SRS-Review')}: ${friendly}`);
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
          setTranslationDrafts({});
          if (webappUser?.id) {
            const oldStorageKey = `webappDrafts_${webappUser.id}_${sessionId || 'nosession'}`;
            safeStorageRemove(oldStorageKey);
          }
          await loadSessionInfo();
          await loadSentences();
          if (!flashcardsOnly && isSectionVisible('flashcards')) {
            await loadSrsNextCard();
          }
        }
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

  const isHomeScreen = !flashcardsOnly && selectedSections.size === 0;
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
  const readerBookmarkPage = readerPageCount > 0
    ? Math.max(1, Math.min(readerPageCount, Math.round((Math.max(0, Math.min(100, Number(readerBookmarkPercent || 0))) / 100) * readerPageCount) || 1))
    : 0;
  const isCurrentReaderPageBookmarked = readerPageCount > 0 && readerBookmarkPage === Math.max(1, Math.min(readerPageCount, Number(readerCurrentPage || 1)));
  const readerElapsedTotalSeconds = Math.max(0, Number(readerAccumulatedSeconds || 0) + Number(readerLiveSeconds || 0));
  const readerSwipeThreshold = readerSwipeSensitivity === 'high' ? 24 : readerSwipeSensitivity === 'low' ? 52 : 36;
  const readerSwipeLockMs = readerSwipeSensitivity === 'high' ? 180 : readerSwipeSensitivity === 'low' ? 340 : 260;
  const youtubeWatchFocusMode = Boolean(
    youtubeId
    && youtubePlaybackStarted
    && !youtubeForceShowPanel
    && !youtubeIsPaused
    && !youtubeOverlayEnabled
    && !youtubeAppFullscreen
  );
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
    setTimeout(() => window.scrollTo({ top: 0, behavior: 'smooth' }), 60);
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
    setSelectedSections(new Set(['translations', 'youtube', 'movies', 'dictionary', 'reader', 'flashcards', 'assistant', 'analytics', 'economics', 'subscription', 'theory']));
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

  const renderMenuIcon = (kind) => {
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

  const stopAssistantSessionTracking = async (sessionIdOverride = null) => {
    if (!initData) return;
    const sid = sessionIdOverride ?? assistantSessionId;
    setAssistantSessionId(null);
    try {
      const response = await fetch('/api/assistant/session/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(sid ? { initData, session_id: sid } : { initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await loadWeeklyPlan();
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
    } catch (error) {
      console.warn('reader session start error', error);
    } finally {
      readerSessionStartingRef.current = false;
    }
  };

  const stopReaderSessionTracking = async (sessionIdOverride = null) => {
    if (!initData) return;
    const sid = sessionIdOverride ?? readerSessionId;
    const latestProgress = computeReaderProgressPercent();
    setReaderProgressPercent(latestProgress);
    if (readerDocumentId) {
      await syncReaderState({ progress_percent: Number(latestProgress.toFixed(2)) });
    }
    setReaderSessionId(null);
    setReaderSessionStartedAt('');
    setReaderLiveSeconds(0);
    try {
      const response = await fetch('/api/reader/session/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(sid ? { initData, session_id: sid } : { initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await loadWeeklyPlan();
      await loadPlanAnalytics();
    } catch (error) {
      console.warn('reader session stop error', error);
    }
  };

  const toggleReaderTimerPause = async () => {
    if (!readerHasContent) return;
    if (readerTimerPaused) {
      setReaderTimerPaused(false);
      await startReaderSessionTracking();
      return;
    }
    const segmentSeconds = readerSessionStartedAt
      ? Math.max(0, Math.floor((Date.now() - new Date(readerSessionStartedAt).getTime()) / 1000))
      : 0;
    setReaderAccumulatedSeconds((prev) => prev + segmentSeconds);
    setReaderTimerPaused(true);
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
        if (readerSessionId) {
          await stopReaderSessionTracking(readerSessionId);
        }
      }

      if (assistantSessionId) {
        await stopAssistantSessionTracking(assistantSessionId);
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

  useEffect(() => {
    if (!telegramApp) return;
    let fullscreenRetryCount = 0;
    let fullscreenRetryTimer = null;
    let expandPulseTimer = null;
    let stopped = false;

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
      const isTabletUserAgent = /iPad|Tablet|PlayBook|Silk|Android(?!.*Mobile)/i.test(userAgent);
      if (isHandsetDevice()) return false;
      return isTabletUserAgent || viewportWidth >= 700 || (maxSide >= 1000 && minSide >= 600);
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

    const syncViewportMode = () => {
      try {
        telegramApp.ready?.();
        telegramApp.expand?.();
        const shouldUseFullscreen = detectTabletLikeViewport();
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
        telegramApp.disableVerticalSwipes?.();
      } catch (error) {
        setTelegramFullscreenMode(false);
        try {
          telegramApp.expand?.();
        } catch (expandError) {
          // ignore
        }
        // Telegram API may be partially unavailable in browser mode.
      }
    };

    const onFirstUserGesture = () => {
      if (!detectTabletLikeViewport()) return;
      try {
        fullscreenRetryCount = 0;
        tryEnterTelegramFullscreen();
      } catch (error) {
        // ignore
      }
    };

    try {
      syncViewportMode();
    } catch (error) {
      // Telegram API may be partially unavailable in browser mode.
    }

    const onResize = () => {
      fullscreenRetryCount = 0;
      syncViewportMode();
    };
    const onFocus = () => {
      fullscreenRetryCount = 0;
      syncViewportMode();
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        fullscreenRetryCount = 0;
        syncViewportMode();
      }
    };
    window.addEventListener('resize', onResize);
    window.addEventListener('focus', onFocus);
    document.addEventListener('visibilitychange', onVisibilityChange);
    window.addEventListener('pointerdown', onFirstUserGesture, { passive: true });
    window.addEventListener('touchstart', onFirstUserGesture, { passive: true });
    window.addEventListener('click', onFirstUserGesture, { passive: true });
    if (typeof telegramApp.onEvent === 'function') {
      telegramApp.onEvent('viewportChanged', syncViewportMode);
    }
    expandPulseTimer = window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode();
    }, 180);
    window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode();
    }, 620);
    window.setTimeout(() => {
      if (stopped) return;
      syncViewportMode();
    }, 1200);

    return () => {
      stopped = true;
      if (fullscreenRetryTimer) window.clearTimeout(fullscreenRetryTimer);
      if (expandPulseTimer) window.clearTimeout(expandPulseTimer);
      window.removeEventListener('resize', onResize);
      window.removeEventListener('focus', onFocus);
      document.removeEventListener('visibilitychange', onVisibilityChange);
      window.removeEventListener('pointerdown', onFirstUserGesture);
      window.removeEventListener('touchstart', onFirstUserGesture);
      window.removeEventListener('click', onFirstUserGesture);
      if (typeof telegramApp.offEvent === 'function') {
        telegramApp.offEvent('viewportChanged', syncViewportMode);
      }
    };
  }, [telegramApp]);

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
      }
    };
    const onWindowBlur = () => {
      pauseNow();
    };
    const onWindowFocus = () => {
      setGlobalTimerSuspended(false);
      setGlobalPauseReason('');
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
  }, [pauseAllActiveTimers]);

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
      reader: !flashcardsOnly && selectedSections.has('reader'),
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
        autoPausedTodayTimerIdsRef.current.add(item.id);
        timerSyncCalls.push(
          syncTodayItemTimer(item, 'pause', { elapsedSeconds, running: false })
        );
        return;
      }
      if (!wasVisible && isVisible && autoPausedTodayTimerIdsRef.current.has(item.id) && !isTodayItemTimerRunning(item)) {
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
      && readerAutoPausedByNavigationRef.current
    ) {
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
    if (telegramApp?.initData) return;
    const stored = safeStorageGet('browser_init_data');
    if (stored && !initData) {
      setInitData(stored);
    }
  }, [telegramApp, initData]);

  useEffect(() => {
    if (!isWebAppMode) return;
    const lockHorizontalScroll = () => {
      if (Math.abs(window.scrollX) > 0) {
        window.scrollTo(0, window.scrollY);
      }
    };
    const onScroll = () => {
      lockHorizontalScroll();
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', lockHorizontalScroll);
    lockHorizontalScroll();
    return () => {
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('resize', lockHorizontalScroll);
    };
  }, [isWebAppMode]);

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
      } catch (error) {
        if (!telegramApp?.initData && String(error?.message || '').includes('initData не прошёл')) {
          safeStorageRemove('browser_init_data');
          setInitData('');
        }
        setWebappError(`${tr('Ошибка инициализации', 'Initialisierungsfehler')}: ${error.message}`);
      }
    };

    bootstrap();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    if (!isWebAppMode || !initData) return;
    loadLanguageProfile();
  }, [isWebAppMode, initData]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setTodayPlan(null);
      setTodayPlanError('');
      return;
    }
    loadTodayPlan();
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      setSkillReport(null);
      setSkillReportError('');
      return;
    }
    loadSkillReport();
  }, [isWebAppMode, initData, languageProfile?.native_language, languageProfile?.learning_language]);

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
    loadWeeklyPlan();
    loadPlanAnalytics();
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
  }, [planAnalyticsPeriod]);

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
    if (flashcardsOnly || !selectedSections.has('assistant')) {
      if (assistantSessionId) {
        stopAssistantSessionTracking(assistantSessionId);
      }
      setAssistantToken(null);
    }
  }, [flashcardsOnly, selectedSections]);

  useEffect(() => {
    const shouldTrackReader = Boolean(
      isWebAppMode
      && initData
      && !flashcardsOnly
      && selectedSections.has('reader')
      && String(readerContent || '').trim()
      && !readerTimerPaused
    );
    if (shouldTrackReader) {
      startReaderSessionTracking();
      return;
    }
    if (readerSessionId) {
      stopReaderSessionTracking(readerSessionId);
    }
  }, [isWebAppMode, initData, flashcardsOnly, selectedSections, readerContent, readerTimerPaused]);

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
  }, [readerHasContent]);

  useEffect(() => {
    const node = readerArticleRef.current;
    if (!node || !readerDocumentId || !readerContent) return undefined;
    if (readerPageCount > 0) return undefined;
    const handleScroll = () => {
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
  }, [readerDocumentId, readerReadingMode, readerContent, readerPageCount]);

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
    setSrsCard(null);
    setSrsState(null);
    setSrsQueueInfo({ due_count: 0, new_remaining_today: 0 });
    setSrsRevealAnswer(false);
    setSrsError('');
    loadSrsNextCard();
  }, [initData, flashcardsOnly, selectedSections, languageProfile?.native_language, languageProfile?.learning_language]);

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
      await playTts(text, getLearningTtsLocale());
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
    flashcardTrainingModeRef.current = flashcardTrainingMode;
  }, [flashcardTrainingMode]);

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
      const correct = resolveFlashcardTexts(entry).targetText || '—';
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
        setSelectedTopic(items[0]);
      }
    } catch (error) {
      setTopicsError(`${tr('Ошибка тем', 'Themenfehler')}: ${error.message}`);
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
      setWebappError(initDataMissingMsg);
      return;
    }
    if (sentences.length === 0) {
      setWebappError(tr('Нет предложений для перевода.', 'Keine Saetze zur Uebersetzung vorhanden.'));
      return;
    }
    if (Object.values(translationDrafts).every((text) => !text.trim())) {
      setWebappError(tr('Заполните хотя бы один перевод.', 'Bitte fuelle mindestens eine Uebersetzung aus.'));
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
    setTranslationAudioGrammarOptIn({});
    setTranslationAudioGrammarSaving({});
    setExplanations({});
    setExplanationLoading({});
    setTranslationCheckProgress({ active: false, done: 0, total: 0 });

    try {
      const submittedEntries = Object.entries(translationDrafts)
        .map(([id, translation]) => ({ id: Number(id), translation: String(translation || '').trim() }))
        .filter((item) => item.translation);
      if (!submittedEntries.length) {
        throw new Error(tr('Нет переводов для проверки.', 'Keine Uebersetzungen zur Pruefung.'));
      }
      setTranslationCheckProgress({ active: true, done: 0, total: submittedEntries.length });

      const sentenceNumberById = new Map(
        sentences.map((item, idx) => [Number(item.id_for_mistake_table), Number(item.unique_id ?? idx + 1)])
      );
      const upsertResultItem = (item) => {
        const translationId = Number(item?.translation_id || 0);
        if (translationId > 0) {
          setTranslationAudioGrammarOptIn((prev) => ({
            ...prev,
            [translationId]: Boolean(item?.audio_grammar_opt_in),
          }));
        }
        setResults((prev) => {
          const indexByKey = new Map();
          const merged = [...prev];
          merged.forEach((entry, idx) => {
            const key = String(entry?.sentence_number ?? entry?.id_for_mistake_table ?? entry?.original_text ?? idx);
            indexByKey.set(key, idx);
          });
          const key = String(item?.sentence_number ?? item?.id_for_mistake_table ?? item?.original_text ?? `new-${merged.length}`);
          if (indexByKey.has(key)) {
            merged[indexByKey.get(key)] = item;
          } else {
            merged.push(item);
          }
          merged.sort((a, b) => (Number(a?.sentence_number || 0) - Number(b?.sentence_number || 0)));
          return merged;
        });
      };

      let nextIndex = 0;
      const concurrency = Math.min(2, submittedEntries.length);
      const worker = async () => {
        while (nextIndex < submittedEntries.length) {
          const current = submittedEntries[nextIndex];
          nextIndex += 1;
          const sentenceNumber = Number(sentenceNumberById.get(Number(current.id)) || 0) || null;
          try {
            const requestBody = {
              initData,
              session_id: sessionId,
              translations: [
                {
                  id_for_mistake_table: current.id,
                  translation: current.translation,
                },
              ],
              original_text: numberedOriginal,
              user_translation: numberedTranslations,
            };
            let response = null;
            let lastError = null;
            for (let attempt = 0; attempt < 2; attempt += 1) {
              try {
                response = await fetch('/api/message', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(requestBody),
                });
                if (response.status >= 500 && attempt === 0) {
                  await new Promise((resolve) => setTimeout(resolve, 400));
                  continue;
                }
                break;
              } catch (fetchError) {
                lastError = fetchError;
                if (attempt === 0) {
                  await new Promise((resolve) => setTimeout(resolve, 400));
                  continue;
                }
                throw fetchError;
              }
            }
            if (!response && lastError) {
              throw lastError;
            }
            if (!response.ok) {
              const apiMessage = await readApiError(
                response,
                'Не удалось проверить перевод.',
                'Uebersetzung konnte nicht geprueft werden.'
              );
              throw new Error(apiMessage);
            }
            const data = await response.json();
            const resultItem = Array.isArray(data?.results) && data.results.length > 0
              ? data.results[0]
              : {
                  sentence_number: sentenceNumber,
                  error: tr('Пустой ответ проверки.', 'Leere Pruefungsantwort.'),
                };
            upsertResultItem(resultItem);
          } catch (error) {
            const friendly = normalizeNetworkErrorMessage(
              error,
              'Не удалось проверить перевод. Повторите позже.',
              'Uebersetzung konnte nicht geprueft werden. Bitte spaeter erneut versuchen.'
            );
            upsertResultItem({
              sentence_number: sentenceNumber,
              error: `${tr('Ошибка проверки', 'Pruefungsfehler')}: ${friendly}`,
            });
          } finally {
            setTranslationCheckProgress((prev) => ({
              active: true,
              done: Math.min(prev.total || submittedEntries.length, (prev.done || 0) + 1),
              total: prev.total || submittedEntries.length,
            }));
          }
        }
      };

      await Promise.all(Array.from({ length: concurrency }, () => worker()));
    } catch (error) {
      const friendly = normalizeNetworkErrorMessage(
        error,
        'Не удалось проверить переводы.',
        'Uebersetzungen konnten nicht geprueft werden.'
      );
      setWebappError(`${tr('Ошибка проверки', 'Pruefungsfehler')}: ${friendly}`);
    } finally {
      setWebappLoading(false);
      setTranslationCheckProgress((prev) => ({ ...prev, active: false }));
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
      setWebappError(initDataMissingMsg);
      return;
    }
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
          level: selectedLevel,
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
    } catch (error) {
      // silent
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
    if (!initData) {
      setWebappError(initDataMissingMsg);
      return;
    }
    const missing = sentences.filter((item) => {
      const value = (translationDrafts[String(item.id_for_mistake_table)] || '').trim();
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
      setWebappError(`${tr('Ошибка истории', 'Story-Fehler')}: ${error.message}`);
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

  const normalizeFolderKey = (value) => normalizeSelectionText(value).toLocaleLowerCase();

  const showInlineToast = (text) => {
    const value = normalizeSelectionText(text);
    if (!value) return;
    if (inlineToastTimeoutRef.current) {
      clearTimeout(inlineToastTimeoutRef.current);
      inlineToastTimeoutRef.current = null;
    }
    setInlineToast(value);
    inlineToastTimeoutRef.current = setTimeout(() => {
      setInlineToast('');
      inlineToastTimeoutRef.current = null;
    }, 1000);
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

  const normalizeLangCode = (value) => String(value || '').trim().toLowerCase();
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
  const getDictionarySourceTarget = (item, direction = dictionaryDirection) => {
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
  };
  const getDictionaryDisplayedTranslation = (item, direction = dictionaryDirection) => {
    const { sourceText, targetText } = getDictionarySourceTarget(item, direction);
    return targetText || sourceText || '';
  };
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

  const handleSelection = (event, overrideText = '', options = {}) => {
    const text = overrideText || normalizeSelectionText(window.getSelection()?.toString() || '');
    if (!text) {
      setSelectionText('');
      setSelectionPos(null);
      setSelectionCompact(false);
      setSelectionLookupLang('');
      setSelectionInlineMode(false);
      return;
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
    setSelectionCompact(Boolean(options?.compact));
    setSelectionLookupLang(normalizeLangCode(options?.lookupLang || ''));
    setSelectionInlineMode(Boolean(options?.inlineLookup));
    if (options?.inlineLookup) {
      void loadSelectionInlineLookup(text);
    }
  };

  const clearSelection = () => {
    setSelectionText('');
    setSelectionPos(null);
    setSelectionCompact(false);
    setSelectionLookupLang('');
    setSelectionInlineMode(false);
    setSelectionLookupLoading(false);
    setSelectionInlineLookup({ loading: false, word: '', translation: '', direction: '' });
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

  const loadSelectionInlineLookup = async (rawText) => {
    if (!initData) return;
    const normalized = await normalizeForLookup(rawText);
    if (!normalized) return;
    const lookupLangHint = normalizeLangCode(
      selectionLookupLang || (hasCyrillic(normalized) ? 'ru' : getNormalizeLookupLang())
    );
    setSelectionInlineLookup({ loading: true, word: normalized, translation: '', direction: '' });
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized, lookup_lang: lookupLangHint || undefined }),
      });
      if (!response.ok) throw new Error('lookup failed');
      const data = await response.json();
      const direction = data.direction || resolveDictionaryDirection(data.item);
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      const translation = getDictionarySourceTarget(data.item, direction).targetText || '';
      setSelectionInlineLookup({
        loading: false,
        word: normalized,
        translation: String(translation || '').trim(),
        direction,
      });
    } catch (error) {
      setSelectionInlineLookup({ loading: false, word: normalized, translation: tr('Ошибка перевода', 'Uebersetzungsfehler'), direction: '' });
    }
  };

  const handleQuickAddToDictionary = async (text, options = {}) => {
    const inlineMode = Boolean(options?.inlineMode);
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    const normalized = await normalizeForLookup(cleaned);
    const lookupLangHint = normalizeLangCode(
      selectionLookupLang || (hasCyrillic(normalized) ? 'ru' : getNormalizeLookupLang())
    );
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
      const autoFolder = inlineMode && youtubeAppFullscreen
        ? await ensureYoutubeAutoFolderId()
        : null;
      const autoFolderId = autoFolder?.id || null;
      if (!inlineMode) {
        setDictionaryResult(data.item || null);
        setDictionaryDirection(detectedDirection);
        scrollToDictionary();
      }

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
        if (youtubeAppFullscreen) {
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

  const handleQuickLookupDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError(initDataMissingMsg);
      return;
    }
    let normalized = cleaned;
    if (!hasCyrillic(cleaned)) {
      try {
        const normalizeLang = encodeURIComponent(getNormalizeLookupLang());
        const normalizeResponse = await fetch(`/api/webapp/normalize/${normalizeLang}`, {
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
    setSelectionLookupLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    const lookupLangHint = normalizeLangCode(
      selectionLookupLang || (hasCyrillic(normalized) ? 'ru' : getNormalizeLookupLang())
    );
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
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
      scrollToDictionary();
      clearSelection();
    } catch (error) {
      setDictionaryError(`${tr('Ошибка словаря', 'Woerterbuchfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
      setSelectionLookupLoading(false);
    }
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

  async function loadReaderLibrary() {
    if (!initData) return;
    try {
      setReaderLibraryLoading(true);
      setReaderLibraryError('');
      const response = await fetch('/api/webapp/reader/library', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 120, include_archived: readerIncludeArchived }),
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
      setReaderAccumulatedSeconds(0);
      setReaderLiveSeconds(0);
      setReaderTimerPaused(false);
      setReaderImmersive(true);
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
        throw new Error(await readApiError(response, 'Ошибка загрузки читалки', 'Fehler beim Laden des Leser-Modus'));
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
      setReaderAccumulatedSeconds(0);
      setReaderLiveSeconds(0);
      setReaderTimerPaused(false);
      setReaderImmersive(true);
      setSelectedSections(new Set(['reader']));
      ensureSectionVisible('reader');
      setTimeout(() => {
        scrollToRef(readerRef, { block: 'start' });
        const target = Number(doc?.bookmark_percent || doc?.progress_percent || 0);
        applyReaderProgressPercent(target);
      }, 80);
      loadReaderLibrary();
      setReaderSelectedFile(null);
    } catch (error) {
      setReaderError(normalizeNetworkErrorMessage(error, 'Не удалось загрузить текст в читалку.', 'Text konnte nicht in den Leser geladen werden.'));
    } finally {
      setReaderLoading(false);
    }
  }

  const loadFlashcards = async () => {
    if (!initData) {
      setFlashcardsError(initDataMissingMsg);
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
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
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
      setFlashcardsError(`${tr('Ошибка карточек', 'Kartenfehler')}: ${error.message}`);
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
        return isCorrect ? 'GOOD' : 'AGAIN';
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
          onClick={(event) => handleSelection(event, cleaned, { compact, inlineLookup, lookupLang })}
        >
          {word}{' '}
        </span>
      );
    });
  };

  const renderSubtitleText = (text) => renderClickableText(
    normalizeSubtitleText(text),
    { lookupLang: getNormalizeLookupLang(), compact: true, inlineLookup: youtubeAppFullscreen }
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
    if (!initData) {
      setWebappError(initDataMissingMsg);
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
      setFinishMessage(data.message || tr('Перевод завершён.', 'Uebersetzung abgeschlossen.'));
      setFinishStatus('done');
      const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
      safeStorageRemove(storageKey);
      setTranslationDrafts({});
      setSessionType('none');
      setStoryGuess('');
      setStoryResult(null);
      setResults([]);
      setTranslationAudioGrammarOptIn({});
      setTranslationAudioGrammarSaving({});
      setSelectedTopic('💼 Business');
      await loadSentences();
    } catch (error) {
      setWebappError(`${tr('Ошибка завершения', 'Abschlussfehler')}: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleExplainTranslation = async (item) => {
    if (!initData) {
      setWebappError(initDataMissingMsg);
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
      setWebappError(`${tr('Ошибка объяснения', 'Erklaerungsfehler')}: ${error.message}`);
    } finally {
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
      .replace(/\*(.+?)\*/g, '<strong>$1</strong>');
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

    return withSections.replace(/\n/g, '<br />');
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
      setDictionaryError(initDataMissingMsg);
      return;
    }
    if (!dictionaryWord.trim()) {
      setDictionaryError(tr('Введите слово или фразу для словаря.', 'Bitte gib ein Wort oder eine Phrase fuers Woerterbuch ein.'));
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
      setDictionaryLanguagePair(resolveLanguagePairForUI(data.language_pair));
    } catch (error) {
      setDictionaryError(`${tr('Ошибка словаря', 'Woerterbuchfehler')}: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
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
    setSelectedCollocation(null);
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
      setSelectedCollocation(options[0] || null);
    } catch (error) {
      setCollocationsError(`${tr('Ошибка связок', 'Kollokationsfehler')}: ${error.message}`);
    } finally {
      setCollocationsLoading(false);
    }
  };

  const handleConfirmSaveCollocation = async () => {
    if (!selectedCollocation) {
      setCollocationsError(tr('Выберите вариант для сохранения.', 'Waehle eine Option zum Speichern.'));
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
      const payload = await response.json();
      setDictionaryLanguagePair(resolveLanguagePairForUI(payload.language_pair));
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
      const link = document.createElement('a');
      link.href = url;
      link.download = 'dictionary.pdf';
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      setDictionaryError(`${tr('Ошибка выгрузки PDF', 'PDF-Exportfehler')}: ${error.message}`);
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
      setYoutubeError(tr('Не удалось распознать ссылку или ID видео.', 'Video-Link oder ID konnte nicht erkannt werden.'));
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
    if (!youtubeId) {
      setYoutubePlayerReady(false);
      setYoutubeCurrentTime(0);
      setYoutubePlaybackStarted(false);
      setYoutubeIsPaused(true);
      setYoutubeForceShowPanel(false);
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
        },
        events: {
          onReady: () => {
            setYoutubePlayerReady(true);
            setYoutubeIsPaused(true);
            setYoutubePlaybackStarted(false);
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
                }
              } catch (error) {
                // ignore
              }
            } else if (state === 1) {
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
  }, [youtubeId, youtubeSectionVisible]);

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
    if (!youtubeAppFullscreen || !selectionText || !selectionPos) {
      return undefined;
    }
    const onPointerDown = (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      if (selectionMenuRef.current?.contains(target)) return;
      if (target.closest('.overlay-clickable-word')) return;
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
  }, [youtubeAppFullscreen, selectionText, selectionPos]);

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
      setAnalyticsError(initDataMissingMsg);
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
      const response = await fetch('/api/billing/plans');
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Ошибка загрузки тарифов', 'Fehler beim Laden der Tarife'));
      }
      const data = await response.json();
      setBillingPlans(Array.isArray(data?.plans) ? data.plans : []);
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

  const handleBillingUpgrade = async () => {
    if (!initData) {
      setBillingStatusError(initDataMissingMsg);
      return;
    }
    setBillingActionLoading(true);
    try {
      const response = await fetch('/api/billing/create-checkout-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, plan_code: 'pro' }),
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
      loadAnalytics();
    }
  }, [initData, isWebAppMode, analyticsPeriod, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('economics')) {
      loadEconomics();
    }
  }, [initData, isWebAppMode, economicsPeriod, economicsAllocation, selectedSections, flashcardsOnly]);

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
            ${tr('Успешно', 'Erfolgreich')}: ${map[tr('Успешно', 'Erfolgreich')] ?? 0}<br/>
            ${tr('Ошибки', 'Fehler')}: ${map[tr('Нужно доработать', 'Verbessern')] ?? 0}<br/>
            ${tr('Ср. балл', 'Durchschnitt')}: ${map[tr('Средний балл', 'Durchschnitt')] ?? 0}<br/>
            ${tr('Ср. время', 'Durchschn. Zeit')}: ${timeValue} ${tr('мин', 'Min')}
          `;
        },
      },
      legend: {
        data: [tr('Успешно', 'Erfolgreich'), tr('Нужно доработать', 'Verbessern'), tr('Средний балл', 'Durchschnitt')],
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
          name: tr('Переводы', 'Uebersetzungen'),
          axisLabel: { color: '#c7d2f1' },
          splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        {
          type: 'value',
          name: tr('Баллы', 'Punkte'),
          min: 0,
          max: 100,
          axisLabel: { color: '#c7d2f1' },
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
      <div className={`webapp-page ${flashcardsOnly ? 'is-flashcards' : ''} ${readerHasContent && readerImmersive ? 'is-reader-immersive' : ''} ${youtubeWatchFocusMode ? 'is-youtube-watch-focus' : ''} ${telegramFullscreenMode ? 'is-telegram-fullscreen' : ''}`}>
        <div className="webapp-shell">
          <aside className="webapp-sidebar">
            <div className="webapp-brand">
              <div className="brand-mark">DF</div>
                <div>
                  <div className="brand-title">DeutschFlow</div>
                  <div className="brand-subtitle">{t('brand_subtitle')}</div>
                </div>
              </div>
            <div className="webapp-menu">
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
                className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                onClick={() => toggleSection('analytics')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-analytics">{renderMenuIcon('analytics')}</span>
                <span>{t('menu_analytics')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-economics ${selectedSections.has('economics') ? 'is-active' : ''}`}
                onClick={() => toggleSection('economics')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-economics">{renderMenuIcon('economics')}</span>
                <span>{t('menu_economics')}</span>
              </button>
              <button
                type="button"
                className={`menu-item menu-item-subscription ${selectedSections.has('subscription') ? 'is-active' : ''}`}
                onClick={() => toggleSection('subscription')}
                disabled={flashcardsOnly}
              >
                <span className="menu-icon menu-icon-subscription">{renderMenuIcon('subscription')}</span>
                <span>{t('menu_billing')}</span>
              </button>
            </div>
            <div className="webapp-menu-actions">
              <div className="language-toggle-wrap">
                <span className="language-toggle-label">{t('language_toggle_label')}</span>
                <button type="button" className="language-toggle" onClick={toggleLanguage} aria-label={t('language_toggle_label')}>
                  <span className={`language-chip ${uiLang === 'ru' ? 'is-active' : ''}`}>{t('language_ru')}</span>
                  <span className={`language-chip ${uiLang === 'de' ? 'is-active' : ''}`}>{t('language_de')}</span>
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
              <div className="topbar-left">
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
              </div>
              <div className="topbar-right">
                <div className="topbar-controls">
                  <button type="button" className="language-toggle language-toggle-compact" onClick={toggleLanguage} aria-label={t('language_toggle_label')}>
                    <span className={`language-chip ${uiLang === 'ru' ? 'is-active' : ''}`}>{t('language_ru')}</span>
                    <span className={`language-chip ${uiLang === 'de' ? 'is-active' : ''}`}>{t('language_de')}</span>
                  </button>
                  <button
                    type="button"
                    className="language-pair-button"
                    onClick={openLanguageProfileModal}
                    title={tr('Изменить языковую пару обучения', 'Sprachpaar aendern')}
                  >
                    {getActiveLanguagePairLabel()}
                  </button>
                </div>
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
                    <div className="topbar-user-name">{webappUser?.first_name || t('guest')}</div>
                    <div className="topbar-user-line topbar-user-extra">ID: {webappUser?.id || '—'}</div>
                    <div className="topbar-user-line topbar-user-extra">Chat: {webappChatType || '—'}</div>
                  </div>
                </div>
              </div>
            </div>

            {globalPauseReason && (
              <div className="timer-pause-reason-banner">
                {globalPauseReason}
              </div>
            )}

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
                      className={`menu-item menu-item-analytics ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('analytics', analyticsRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-analytics">{renderMenuIcon('analytics')}</span>
                      <span>{t('menu_analytics')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-economics ${selectedSections.has('economics') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('economics', economicsRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-economics">{renderMenuIcon('economics')}</span>
                      <span>{t('menu_economics')}</span>
                    </button>
                    <button
                      type="button"
                      className={`menu-item menu-item-subscription ${selectedSections.has('subscription') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('subscription', billingRef)}
                      disabled={flashcardsOnly}
                    >
                      <span className="menu-icon menu-icon-subscription">{renderMenuIcon('subscription')}</span>
                      <span>{t('menu_billing')}</span>
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

            

            {!telegramApp?.initData && (
              <section className="webapp-browser-auth">
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
                    <label className="webapp-field">
                      <span>{t('init_data_fallback')}</span>
                      <textarea
                        rows={3}
                        value={initData}
                        onChange={(event) => setInitData(event.target.value)}
                        placeholder={t('init_data_placeholder')}
                      />
                    </label>
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
                  {languageProfileError && <div className="webapp-error">{languageProfileError}</div>}
                  <div className="language-profile-actions">
                    <button
                      type="button"
                      className="primary-button"
                      onClick={saveLanguageProfile}
                      disabled={languageProfileSaving}
                    >
                      {languageProfileSaving ? tr('Сохраняем...', 'Speichern...') : tr('Сохранить и продолжить', 'Speichern und fortsetzen')}
                    </button>
                    {!needsLanguageProfileChoice && (
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setLanguageProfileModalOpen(false)}
                        disabled={languageProfileSaving}
                      >
                        {tr('Закрыть', 'Schliessen')}
                      </button>
                    )}
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
                      const elapsedSeconds = getTodayItemElapsedSeconds(item, todayTimerNowMs);
                      const progressPercent = getTodayItemProgressPercent(item, todayTimerNowMs);
                      const doneByProgress = progressPercent >= 100;
                      const done = String(item?.status || '').toLowerCase() === 'done' || doneByProgress;
                      const itemStatusClass = done ? 'done' : (item.status || 'todo');
                      const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
                      const videoTopic = String(payload?.sub_category || payload?.skill_title || payload?.main_category || '').trim();
                      const videoLikes = Number(payload?.video_likes || 0);
                      const videoDislikes = Number(payload?.video_dislikes || 0);
                      const videoScore = Number(payload?.video_score || 0);
                      const userVote = Number(payload?.video_user_vote || 0);
                      return (
                        <div className={`today-plan-item is-${itemStatusClass}`} key={item.id}>
                          <div className="today-plan-item-main">
                            <div className="today-plan-item-title">{getTodayItemTitle(item)}</div>
                            <div className="today-plan-item-meta">
                              {taskType !== 'video' && <span>{item.estimated_minutes || 0} {tr('мин', 'Min')}</span>}
                              <span>{done ? 'DONE' : String(item.status || 'todo').toUpperCase()}</span>
                              <span>⏱ {formatCompactTimer(elapsedSeconds)}</span>
                            </div>
                            {taskType === 'video' && (
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
                          <div className="today-plan-item-actions">
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => startTodayTask(item)}
                              disabled={Boolean(loadingAction) || done}
                            >
                              {loadingAction === 'start' ? tr('Старт...', 'Start...') : tr('Начать', 'Starten')}
                            </button>
                            <div className={`today-task-progress-badge ${done ? 'is-done' : ''}`} title={done ? tr('Задача выполнена', 'Aufgabe erledigt') : `${Math.round(progressPercent)}%`}>
                              {done ? '✅' : `⭕ ${Math.round(progressPercent)}%`}
                            </div>
                            {taskType === 'video' && (
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
                        return (
                          <div className="skill-rings-legend-item" key={`legend-${skill.skill_id}`}>
                            <span className="skill-rings-dot" style={{ backgroundColor: color }} />
                            <div className="skill-rings-text">
                              <div className="skill-rings-name">{skill.name}</div>
                              <div className="skill-rings-meta">
                                <span>{skill.ring_type === 'weak' ? tr('Слабый', 'Schwach') : tr('Сильный', 'Stark')}</span>
                                <span>
                                  {tr('Оценка', 'Score')}: {skill?.mastery === null || skill?.mastery === undefined
                                    ? tr('нет данных', 'keine Daten')
                                    : `${Math.round(Number(skill.mastery || 0))}%`}
                                </span>
                                <span>{tr('Ошибки 7д', 'Fehler 7T')}: {Number(skill.errors_7d || 0)}</span>
                              </div>
                            </div>
                            <button
                              type="button"
                              className="secondary-button skill-rings-train-btn"
                              onClick={() => startSkillPractice(skill)}
                              disabled={Boolean(skillPracticeLoading[String(skill.skill_id || '')])}
                            >
                              {skillPracticeLoading[String(skill.skill_id || '')]
                                ? tr('Запуск...', 'Start...')
                                : tr('Прокачать', 'Trainieren')}
                            </button>
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
              <section className="webapp-section" ref={translationsRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <h2>{tr('Ваши переводы', 'Ihre Uebersetzungen')}</h2>
                  {isFocusedSection('translations') && (
                    <button type="button" className="section-home-back" onClick={goHomeScreen}>
                      {tr('На главную', 'Startseite')}
                    </button>
                  )}
                  {renderTodaySectionTaskHud('translations')}
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>
                <div className="webapp-translation-start">
                  <label className="webapp-field">
                    <span>{tr('Тема', 'Thema')}</span>
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
                      <span>{tr('Уровень', 'Niveau')}</span>
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
                    className="primary-button"
                    onClick={isStoryTopic(selectedTopic) ? handleStartStory : handleStartTranslation}
                    disabled={webappLoading || topicsLoading}
                  >
                    {webappLoading ? tr('Запускаем...', 'Starten...') : tr('🚀 Начать перевод', '🚀 Uebersetzung starten')}
                  </button>
                </div>
                {topicsError && <div className="webapp-error">{topicsError}</div>}
                {storyHistoryError && <div className="webapp-error">{storyHistoryError}</div>}
                {!isStoryResultMode && (
                <form className="webapp-form" onSubmit={handleTranslationSubmit}>
                  <section className="webapp-translation-list">
                    {sentences.length === 0 ? (
                      <p className="webapp-muted">
                        {tr('Нет активных предложений. Нажмите «🚀 Начать перевод», чтобы получить новые.', 'Keine aktiven Saetze. Druecke «🚀 Uebersetzung starten», um neue zu laden.')}
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
                              placeholder={tr('Введите перевод...', 'Uebersetzung eingeben...')}
                            />
                            <div className="translation-actions">
                              <button
                                type="button"
                                className="translation-dict-jump"
                                onClick={jumpToDictionaryFromSentence}
                                aria-label={tr('Перейти в словарь', 'Zum Woerterbuch')}
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
                      <span>{tr('А теперь угадай, о ком / чем шла речь', 'Rate jetzt, worum oder um wen es ging')}</span>
                      <input
                        type="text"
                        value={storyGuess}
                        onChange={(event) => setStoryGuess(event.target.value)}
                        placeholder={tr('Ваш ответ...', 'Deine Antwort...')}
                      />
                    </label>
                  )}

                  <button className="primary-button" type="submit" disabled={webappLoading}>
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

                      {storyResult.feedback && (
                        <div
                          className="webapp-result-text story-result-feedback"
                          dangerouslySetInnerHTML={{ __html: renderRichText(storyResult.feedback) }}
                        />
                      )}

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
                      {results.map((item, index) => (
                        <div key={`${item.sentence_number ?? index}`} className="webapp-result-card">
                          {item.error ? (
                            <div className="webapp-error">{item.error}</div>
                          ) : (
                            <>
                              {Number(item?.translation_id || 0) > 0 && (
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
                              )}
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
                      ))}
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
                    {historyLoading ? tr('Загружаем...', 'Laden...') : tr('Посмотреть результат за сегодня', 'Ergebnis fuer heute anzeigen')}
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
                  <section className={`webapp-video ${youtubeWatchFocusMode ? 'is-watch-focus' : ''}`} ref={youtubeRef}>
                    {youtubeWatchFocusMode && (
                      <div className="youtube-focus-actions">
                        <button
                          type="button"
                          className="secondary-button youtube-restore-panel-btn"
                          onClick={() => setYoutubeForceShowPanel(true)}
                        >
                          {tr('Вернуть панель', 'Panel anzeigen')}
                        </button>
                      </div>
                    )}
                    {!youtubeWatchFocusMode && (
                    <div className="webapp-local-section-head">
                      <h3>{tr('Видео YouTube', 'YouTube Video')}</h3>
                      {isFocusedSection('youtube') && (
                        <button type="button" className="section-home-back" onClick={goHomeScreen}>
                          {tr('На главную', 'Startseite')}
                        </button>
                      )}
                      {renderTodaySectionTaskHud('youtube')}
                      <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                    </div>
                    )}
                    {!youtubeWatchFocusMode && (
                    <div className="webapp-video-form">
                      <label className="webapp-field">
                        <span>{tr('Ссылка или ID видео', 'Link oder Video-ID')}</span>
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
                        onClick={() => setVideoExpanded((prev) => !prev)}
                        style={{ display: 'none' }}
                        >
                          {videoExpanded ? tr('Обычный режим', 'Normalmodus') : tr('Словарь рядом', 'Woerterbuch daneben')}
                        </button>
                      </div>
                    </div>
                    )}
                    {!youtubeWatchFocusMode && (
                    <div className="webapp-video-actions is-subtitle-toolbar">
                      <button
                        type="button"
                        className={`secondary-button ${youtubeTranslationEnabled ? 'is-active' : ''}`}
                        onClick={() => setYoutubeTranslationEnabled((prev) => !prev)}
                        disabled={!youtubeTranscript.length}
                      >
                        {youtubeTranslationEnabled
                          ? `${tr('Скрыть', 'Ausblenden')} ${getNativeSubtitleCode()}`
                          : `${tr('Показать', 'Anzeigen')} ${getNativeSubtitleCode()}`}
                      </button>
                      <button
                        type="button"
                        className="primary-button"
                        onClick={() => fetchTranscript()}
                        disabled={!youtubeId || youtubeTranscriptLoading || youtubeManualOverride}
                      >
                        {youtubeTranscriptLoading ? tr('Загружаем...', 'Laden...') : tr('Загрузить', 'Laden')}
                      </button>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setShowManualTranscript((prev) => !prev)}
                      >
                        {showManualTranscript ? tr('Скрыть вставку', 'Einfuegen ausblenden') : tr('Вставить транскрипцию', 'Transkript einfuegen')}
                      </button>
                      <button
                        type="button"
                        className={`secondary-button ${youtubeOverlayEnabled ? 'is-active' : ''}`}
                        onClick={() => setYoutubeOverlayEnabled((prev) => !prev)}
                        disabled={!youtubeTranscript.length}
                      >
                        {youtubeOverlayEnabled ? 'Overlay: ON' : 'Overlay: OFF'}
                      </button>
                      <button
                        type="button"
                        className={`secondary-button ${youtubeAppFullscreen ? 'is-active' : ''}`}
                        onClick={() => setYoutubeAppFullscreen((prev) => !prev)}
                        disabled={!youtubeId}
                      >
                        {youtubeAppFullscreen ? tr('Свернуть', 'Minimieren') : tr('Развернуть', 'Erweitern')}
                      </button>
                    </div>
                    )}
                    {youtubeError && <div className="webapp-error">{youtubeError}</div>}
                    {youtubeId ? (
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
                        <div className={`webapp-video-frame ${videoExpanded ? 'is-expanded' : ''} ${youtubeOverlayEnabled ? 'has-overlay' : ''}`}>
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
                                      <p className="youtube-subtitles-overlay-line is-de">
                                        {renderClickableText(overlayDeText, {
                                          className: 'overlay-clickable-word',
                                          compact: true,
                                          inlineLookup: true,
                                          lookupLang: getNormalizeLookupLang(),
                                        })}
                                      </p>
                                    )}
                                    {youtubeTranslationEnabled && overlayTranslationText && (
                                      <p className="youtube-subtitles-overlay-line is-translation">{overlayTranslationText}</p>
                                    )}
                                  </div>
                                );
                              })}
                            </div>
                          );
                        })()}
                        </div>
                      </div>
                    ) : (
                      <p className="webapp-muted">{tr('Вставьте ссылку на видео, чтобы смотреть прямо здесь.', 'Fuege einen Videolink ein, um hier direkt zu schauen.')}</p>
                    )}
                    {showManualTranscript && !youtubeWatchFocusMode && (
                      <div className="webapp-subtitles-manual">
                        <textarea
                          rows={6}
                          value={manualTranscript}
                          onChange={(event) => setManualTranscript(event.target.value)}
                          placeholder={tr('Вставьте .srt/.vtt с таймкодами. Если таймкодов нет — покажем статично.', 'Fuege .srt/.vtt mit Zeitcodes ein. Ohne Zeitcodes zeigen wir statisch an.')}
                        />
                        <div className="webapp-video-actions">
                          <button
                            type="button"
                            className="primary-button"
                            onClick={() => handleManualTranscript()}
                          >
                            {tr('Использовать транскрипцию', 'Transkript verwenden')}
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
                              {tr('Вернуться к авто', 'Zu Auto zurueck')}
                            </button>
                          )}
                        </div>
                      </div>
                    )}
                    {youtubeTranscript.length > 0 && !youtubeOverlayEnabled && (
                      <div className="webapp-subtitles" ref={youtubeSubtitlesRef}>
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
                    {youtubeTranscript.length > 0 && youtubeTranslationEnabled && !youtubeOverlayEnabled && (
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
                            <button
                              type="button"
                              className="dictionary-clear"
                              onClick={() => setDictionaryWord('')}
                              aria-label={tr('Очистить слово', 'Wort loeschen')}
                            >
                              ×
                            </button>
                          )}
                        </div>
                      </label>
                      <div className="dictionary-actions">
                        <button className="secondary-button dictionary-button" type="submit" disabled={dictionaryLoading}>
                          {dictionaryLoading ? tr('Ищем...', 'Suche...') : tr('Перевести', 'Uebersetzen')}
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
                          {tr('Добавить в словарь', 'Zum Woerterbuch hinzufuegen')}
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
                          <span className="dictionary-direction">
                            {getLookupDirectionLabel()}
                          </span>
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
                        <h4>{tr('Выберите связку для словаря', 'Waehle eine Kollokation')}</h4>
                        {collocationsLoading && <div className="webapp-muted">{tr('Генерируем варианты...', 'Varianten werden generiert...')}</div>}
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
              <section className={`webapp-section webapp-reader ${readerHasContent && readerImmersive ? 'is-immersive' : ''}`} ref={readerRef}>
                <div className="webapp-section-title webapp-section-title-with-logo">
                  <div className="webapp-local-section-head">
                    <h3>{tr('Читалка', 'Leser')}</h3>
                    {!readerImmersive && isFocusedSection('reader') && (
                      <button type="button" className="section-home-back" onClick={goHomeScreen}>
                        {tr('На главную', 'Startseite')}
                      </button>
                    )}
                    {!readerImmersive && <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />}
                  </div>
                </div>
                {readerHasContent && readerImmersive && (
                  <div className="reader-immersive-topbar">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setReaderImmersive(false)}
                    >
                      {tr('Назад', 'Zurueck')}
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
                    >
                      {readerReadingMode === 'vertical' ? '↕︎' : '↔︎'}
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
                      title={tr('Поставить закладку', 'Lesezeichen setzen')}
                    >
                      🔖
                    </button>
                    <button
                      type="button"
                      className={`reader-timer-pill ${readerTimerPaused ? 'is-paused' : ''}`}
                      onClick={toggleReaderTimerPause}
                    >
                      {readerTimerPaused ? `⏸ ${formatReaderTimer(readerElapsedTotalSeconds)}` : `⏱ ${formatReaderTimer(readerElapsedTotalSeconds)}`}
                    </button>
                    <span className="reader-immersive-indicator is-on">
                      {tr('Иммерсивный: ON', 'Immersiv: ON')}
                    </span>
                  </div>
                )}

                {!readerImmersive && (
                <form className="webapp-reader-form" onSubmit={handleReaderIngest}>
                  <label className="webapp-field">
                    <span>{tr('Ссылка или текст', 'Link oder Text')}</span>
                    <textarea
                      rows={4}
                      value={readerInput}
                      onChange={(event) => setReaderInput(event.target.value)}
                      placeholder={tr(
                        'Вставьте URL статьи/книги (включая PDF) или сам текст.',
                        'Fuege die URL eines Artikels/Buchs (auch PDF) oder den Text selbst ein.'
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
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setReaderImmersive(true)}
                      disabled={!readerHasContent}
                    >
                      {tr('Иммерсивный ON', 'Immersiv ON')}
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
                    >
                      {readerReadingMode === 'vertical'
                        ? tr('Режим: вертикально', 'Modus: vertikal')
                        : tr('Режим: горизонтально', 'Modus: horizontal')}
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
                      title={tr('Поставить закладку на текущей позиции', 'Lesezeichen an der aktuellen Position setzen')}
                    >
                      {tr('Закладка', 'Lesezeichen')}
                    </button>
                    {readerDetectedLanguage && (
                      <span className="webapp-muted">
                        {tr('Язык текста', 'Textsprache')}: {readerDetectedLanguage.toUpperCase()}
                      </span>
                    )}
                    <button
                      type="button"
                      className={`reader-timer-pill ${readerTimerPaused ? 'is-paused' : ''}`}
                      onClick={toggleReaderTimerPause}
                      disabled={!readerHasContent}
                      title={tr('Пауза/продолжение таймера чтения', 'Lese-Timer pausieren/fortsetzen')}
                    >
                      {readerTimerPaused ? `⏸ ${formatReaderTimer(readerElapsedTotalSeconds)}` : `${tr('Чтение', 'Lesen')}: ${formatReaderTimer(readerElapsedTotalSeconds)}`}
                    </button>
                    <span className={`reader-immersive-indicator ${readerImmersive ? 'is-on' : 'is-off'}`}>
                      {readerImmersive
                        ? tr('Иммерсивный: ON', 'Immersiv: ON')
                        : tr('Иммерсивный: OFF', 'Immersiv: OFF')}
                    </span>
                  </div>
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
                </form>
                )}
                {!readerImmersive && readerDocumentId && (
                  <section className="reader-audio-panel">
                    <div className="reader-audio-head">
                      <strong>{tr('Оффлайн-аудио документа', 'Offline-Audio des Dokuments')}</strong>
                    </div>
                    <div className="reader-audio-actions">
                      {readerPageCount > 0 && (
                        <>
                          <label className="webapp-field">
                            <span>{tr('Страницы от', 'Seiten von')}</span>
                            <input
                              type="number"
                              min="1"
                              max={readerPageCount}
                              value={readerAudioFromPage}
                              onChange={(event) => setReaderAudioFromPage(event.target.value)}
                            />
                          </label>
                          <label className="webapp-field">
                            <span>{tr('до', 'bis')}</span>
                            <input
                              type="number"
                              min="1"
                              max={readerPageCount}
                              value={readerAudioToPage}
                              onChange={(event) => setReaderAudioToPage(event.target.value)}
                            />
                          </label>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => downloadReaderAudio(false)}
                            disabled={readerAudioLoading}
                          >
                            {readerAudioLoading ? tr('Готовим...', 'Erstellen...') : tr('Скачать диапазон', 'Bereich herunterladen')}
                          </button>
                        </>
                      )}
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
                {!readerImmersive && readerError && <div className="webapp-error">{readerError}</div>}
                {!readerImmersive && (
                <section className="reader-library">
                  <div className="reader-library-head">
                    <h4>{tr('Библиотека', 'Bibliothek')}</h4>
                    <div className="reader-library-head-actions">
                      <label className="menu-toggle-row">
                        <input
                          type="checkbox"
                          checked={readerIncludeArchived}
                          onChange={(event) => setReaderIncludeArchived(event.target.checked)}
                        />
                        <span>{tr('Показывать архив', 'Archiv zeigen')}</span>
                      </label>
                      <button type="button" className="secondary-button" onClick={loadReaderLibrary} disabled={readerLibraryLoading}>
                        {readerLibraryLoading ? tr('Обновляем...', 'Aktualisieren...') : tr('Обновить', 'Aktualisieren')}
                      </button>
                    </div>
                  </div>
                  {readerLibraryError && <div className="webapp-error">{readerLibraryError}</div>}
                  {!readerLibraryError && readerDocuments.length === 0 && (
                    <div className="webapp-muted">{tr('Пока библиотека пуста. Откройте текст или книгу, и она появится здесь.', 'Die Bibliothek ist noch leer. Oeffne Text oder Buch, dann erscheint es hier.')}</div>
                  )}
                  {readerDocuments.length > 0 && (
                    <div className="reader-library-grid">
                      {readerDocuments.map((item) => {
                        const progress = Math.max(0, Math.min(100, Number(item?.progress_percent || 0)));
                        const badgeStyle = { '--reader-progress': `${progress}%` };
                        return (
                          <button
                            type="button"
                            key={`reader-doc-${item.id}`}
                            className={`reader-library-card ${Number(readerDocumentId) === Number(item.id) ? 'is-active' : ''}`}
                            onClick={() => openReaderDocument(item.id)}
                          >
                            <div className="reader-library-title">{item.title || tr('Без названия', 'Ohne Titel')}</div>
                            <div className="reader-library-meta">
                              <span>{String(item.source_type || 'text').toUpperCase()}</span>
                              <span>{String(item.target_lang || '').toUpperCase()}</span>
                            </div>
                            <div className="reader-library-progress-ring" style={badgeStyle}>
                              <span>{Math.round(progress)}%</span>
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
                )}
                {readerImmersive && readerContent && (
                  <article
                    ref={readerArticleRef}
                    className={`reader-article ${readerReadingMode === 'horizontal' ? 'is-horizontal' : 'is-vertical'} ${readerPageCount > 0 ? 'has-pages' : ''}`}
                    onMouseUp={(event) => handleSelection(event, '', { lookupLang: getNormalizeLookupLang() })}
                    onWheel={handleReaderPageWheel}
                    onTouchStart={handleReaderPageTouchStart}
                    onTouchEnd={handleReaderPageTouchEnd}
                  >
                    {readerSourceType && !readerImmersive && (
                      <div className="reader-meta">
                        {readerTitle && <span>{tr('Книга', 'Buch')}: {readerTitle}</span>}
                        <span>{tr('Источник', 'Quelle')}: {readerSourceType.toUpperCase()}</span>
                        {readerSourceUrl && <span>{readerSourceUrl}</span>}
                        <span>{tr('Прогресс', 'Fortschritt')}: {Math.round(readerProgressPercent)}%</span>
                        <span>{tr('Закладка', 'Lesezeichen')}: {Math.round(readerBookmarkPercent)}%</span>
                      </div>
                    )}
                    {readerPageCount > 0 ? (
                      <div className="reader-pages-layout">
                        {!readerImmersive && (
                        <div className="reader-pages-controls">
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => goReaderPage(-1)}
                            disabled={readerCurrentPage <= 1}
                          >
                            {tr('Назад', 'Zurueck')}
                          </button>
                          <span>
                            {tr('Страница', 'Seite')} {readerCurrentPage} / {readerPageCount}
                          </span>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => goReaderPage(1)}
                            disabled={readerCurrentPage >= readerPageCount}
                          >
                            {tr('Вперёд', 'Weiter')}
                          </button>
                        </div>
                        )}
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
                            {renderClickableText(
                              String(readerDisplayPages[readerCurrentPage - 1]?.text || ''),
                              { className: 'reader-clickable-word', lookupLang: getNormalizeLookupLang(), inlineLookup: true, compact: true }
                            )}
                          </div>
                          <div className="reader-page-num">
                            {tr('Стр.', 'S.')}{' '}{readerCurrentPage}{readerPageCount > 0 ? ` / ${readerPageCount}` : ''}
                          </div>
                        </div>
                      </div>
                    ) : (
                      readerContent.split(/\n{2,}/).map((paragraph, index) => {
                        const value = String(paragraph || '').trim();
                        if (!value) return null;
                        return (
                          <p key={`reader-p-${index}`}>
                            {renderClickableText(value, { className: 'reader-clickable-word', lookupLang: getNormalizeLookupLang(), inlineLookup: true, compact: true })}
                          </p>
                        );
                      })
                    )}
                  </article>
                )}
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
                {(isFocusedSection('flashcards') || Boolean(getTodayTaskForSection('flashcards'))) && (
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
                    {!flashcardsOnly && (
                      <div className="flashcard-stage is-setup">
                        <div
                          className={`srs-panel srs-panel-setup ${
                            (srsQueueInfo?.due_count ?? 0) > 0 || srsCard ? 'is-urgent' : 'is-calm'
                          } ${
                            !srsLoading && !srsCard && (srsQueueInfo?.due_count ?? 0) === 0 ? 'is-collapsed' : ''
                          }`}
                        >
                          <div className="srs-panel-head">
                            <h3>
                              <span className="srs-head-icon" aria-hidden="true">
                                {(srsQueueInfo?.due_count ?? 0) > 0 || srsCard ? '‼️' : '🛋️'}
                              </span>
                              {t('srs_title')}
                            </h3>
                            <div className="srs-queue">
                              <span>{t('due')}: {srsQueueInfo?.due_count ?? 0}</span>
                              <span>{t('new_today')}: {srsQueueInfo?.new_remaining_today ?? 0}</span>
                            </div>
                          </div>
                          <div className="webapp-muted">{tr('Языковая пара', 'Sprachpaar')}: {getActiveLanguagePairLabel()}</div>
                          {!srsLoading && !srsCard && (srsQueueInfo?.due_count ?? 0) === 0 && (
                            <div className="srs-success-note">
                              {tr('Сегодня по FSRS всё повторено. Можно отдыхать.', 'Heute ist alles in FSRS wiederholt. Du kannst entspannen.')}
                            </div>
                          )}
                          {srsLoading && <div className="webapp-muted">{t('loading_next_card')}</div>}
                          {!srsLoading && !srsCard && (
                            <div className="webapp-muted">{t('no_cards_now')}</div>
                          )}
                          {srsError && <div className="webapp-error">{srsError}</div>}
                          {!srsLoading && srsCard && (
                            <div className={`srs-card ${srsRevealAnswer ? 'is-revealed' : ''}`}>
                              <div className="srs-card-front">
                                {getDictionarySourceTarget(
                                  srsCard,
                                  (srsCard?.source_lang || 'ru') === 'de' ? 'de-ru' : 'ru-de'
                                ).sourceText || '—'}
                              </div>
                              {srsRevealAnswer && (
                                <div className="srs-card-back">
                                  {getDictionarySourceTarget(
                                    srsCard,
                                    (srsCard?.source_lang || 'ru') === 'de' ? 'de-ru' : 'ru-de'
                                  ).targetText || '—'}
                                </div>
                              )}
                              <div className="srs-state-line">
                                <span>{t('status')}: {srsState?.status || 'new'}</span>
                                <span>{t('interval')}: {srsState?.interval_days ?? 0} {t('days_short')}</span>
                                {srsState?.is_mature && <span className="srs-mature">{t('mature')}</span>}
                              </div>
                              {!srsRevealAnswer ? (
                                <div className="srs-actions">
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() => {
                                      setSrsRevealStartedAt(Date.now());
                                      setSrsRevealElapsedSec(0);
                                      setSrsRevealAnswer(true);
                                    }}
                                    disabled={srsSubmitting}
                                  >
                                    {t('show_answer')}
                                  </button>
                                </div>
                              ) : (
                                <>
                                  <div className="srs-timer-line">
                                    {tr('Таймер', 'Timer')}: {srsRevealElapsedSec}s
                                  </div>
                                  <div className="srs-rating-grid">
                                    <button type="button" className="srs-rate again" onClick={() => submitSrsReview('AGAIN')} disabled={srsSubmitting}>
                                      {srsSubmitting && srsSubmittingRating === 'AGAIN' ? tr('Сохраняем...', 'Speichern...') : 'AGAIN'}
                                    </button>
                                    <button type="button" className="srs-rate hard" onClick={() => submitSrsReview('HARD')} disabled={srsSubmitting}>
                                      {srsSubmitting && srsSubmittingRating === 'HARD' ? tr('Сохраняем...', 'Speichern...') : 'HARD'}
                                    </button>
                                    <button type="button" className="srs-rate good" onClick={() => submitSrsReview('GOOD')} disabled={srsSubmitting || srsGoodLocked}>
                                      {srsSubmitting && srsSubmittingRating === 'GOOD' ? tr('Сохраняем...', 'Speichern...') : 'GOOD'}
                                    </button>
                                    <button type="button" className="srs-rate easy" onClick={() => submitSrsReview('EASY')} disabled={srsSubmitting || srsEasyLocked}>
                                      {srsSubmitting && srsSubmittingRating === 'EASY' ? tr('Сохраняем...', 'Speichern...') : 'EASY'}
                                    </button>
                                  </div>
                                </>
                              )}
                            </div>
                          )}
                        </div>
                        <div className="flashcards-setup">
                            <div className="setup-hero">
                              <div className="setup-ring">
                                <img src={heroStickerSrc} alt="Deutsch mascot" className="setup-mascot-flat" />
                              </div>
                              <div className="setup-title">{t('setup_training_title')}</div>
                              <div className="setup-subtitle">{t('setup_training_subtitle')}</div>
                            </div>
                            <div className="setup-grid">
                              <div className="setup-group">
                                <div className="setup-label">{t('setup_training_mode')}</div>
                                <div className="setup-options">
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardTrainingMode === 'quiz' ? 'is-active' : ''}`}
                                  onClick={() => {
                                    flashcardTrainingModeRef.current = 'quiz';
                                    setFlashcardTrainingMode('quiz');
                                  }}
                                >
                                  {t('setup_mode_quiz')}
                                </button>
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardTrainingMode === 'blocks' ? 'is-active' : ''}`}
                                  onClick={() => {
                                    flashcardTrainingModeRef.current = 'blocks';
                                    setFlashcardTrainingMode('blocks');
                                  }}
                                >
                                  {t('setup_mode_blocks')}
                                </button>
                              </div>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">{t('setup_set_size')}</div>
                              <div className="setup-options">
                                {[5, 10, 15].map((size) => (
                                  <button
                                    key={`set-${size}`}
                                    type="button"
                                    className={`option-pill ${flashcardSetSize === size ? 'is-active' : ''}`}
                                    onClick={() => setFlashcardSetSize(size)}
                                  >
                                    {t('setup_cards_count', { count: size })}
                                  </button>
                                ))}
                              </div>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">{t('setup_folder')}</div>
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
                                  <option value="all">{t('setup_all_folders')}</option>
                                  <option value="none">{t('setup_without_folder')}</option>
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
                                <div className="setup-label">{t('setup_speed')}</div>
                                <div className="setup-options">
                                  {[5, 10, 15].map((seconds) => (
                                    <button
                                      key={`speed-${seconds}`}
                                      type="button"
                                      className={`option-pill ${flashcardDurationSec === seconds ? 'is-active' : ''}`}
                                      onClick={() => setFlashcardDurationSec(seconds)}
                                    >
                                      {seconds} {uiLang === 'de' ? 'Sek' : 'сек'}
                                    </button>
                                  ))}
                                </div>
                              </div>
                            ) : (
                              <div className="setup-group">
                                <div className="setup-label">{t('setup_blocks_timer')}</div>
                                <div className="setup-options">
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'adaptive' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('adaptive')}
                                  >
                                    {t('timer_adaptive')}
                                  </button>
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'fixed' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('fixed')}
                                  >
                                    {t('timer_fixed_10')}
                                  </button>
                                  <button
                                    type="button"
                                    className={`option-pill ${blocksTimerMode === 'none' ? 'is-active' : ''}`}
                                    onClick={() => setBlocksTimerMode('none')}
                                  >
                                    {t('timer_none')}
                                  </button>
                                </div>
                              </div>
                            )}
                            <div className="setup-group">
                              <div className="setup-label">{t('setup_transition')}</div>
                              <div className="setup-options">
                                <button
                                  type="button"
                                  className={`option-pill ${flashcardAutoAdvance ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardAutoAdvance(true)}
                                >
                                  {t('transition_auto')}
                                </button>
                                <button
                                  type="button"
                                  className={`option-pill ${!flashcardAutoAdvance ? 'is-active' : ''}`}
                                  onClick={() => setFlashcardAutoAdvance(false)}
                                >
                                  {t('transition_manual')}
                                </button>
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="primary-button flashcards-start"
                            onClick={() => {
                              setFlashcardTrainingMode(flashcardTrainingModeRef.current || 'quiz');
                              unlockAudio();
                              loadFlashcards();
                              setFlashcardPreviewActive(true);
                              setFlashcardsOnly(true);
                              setFlashcardExitSummary(false);
                            }}
                          >
                            {t('start_training')}
                          </button>
                        </div>
                      </div>
                    )}

                    {flashcardsOnly && (
                      <>
                        {flashcardsLoading && <div className="webapp-muted">{t('loading_cards')}</div>}
                        {flashcardsError && <div className="webapp-error">{flashcardsError}</div>}
                        {!flashcardsLoading && !flashcardsError && flashcards.length === 0 && (
                          <div className="webapp-muted">{t('dictionary_empty')}</div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && flashcardPreviewActive && (
                          <div className="flashcard-stage is-session is-preview">
                            {(() => {
                              const entry = flashcards[flashcardPreviewIndex] || {};
                              const responseJson = entry.response_json || {};
                              const cardTexts = resolveFlashcardTexts(entry);
                              const previewLearningText = cardTexts.targetText || cardTexts.sourceText || '—';
                              const previewNativeText = cardTexts.sourceText || cardTexts.targetText || '—';
                              const learningCode = String(languageProfile?.learning_language || 'de').toUpperCase();
                              const nativeCode = String(languageProfile?.native_language || 'ru').toUpperCase();
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
                                    <div className="flashcard-word-block">
                                      <span className="flashcard-lang-tag">{learningCode}</span>
                                      <div className="flashcard-word">{previewLearningText}</div>
                                    </div>
                                    <button
                                      type="button"
                                      className="flashcard-audio-replay"
                                      onClick={async () => {
                                        const text = resolveFlashcardGerman(entry);
                                        if (!text || previewAudioPlaying) return;
                                        try {
                                          setPreviewAudioPlaying(true);
                                          await playTts(text, getLearningTtsLocale());
                                        } finally {
                                          setPreviewAudioPlaying(false);
                                          setPreviewAudioReady(true);
                                        }
                                      }}
                                      aria-label={tr('Повторить аудио', 'Audio wiederholen')}
                                      title={tr('Повторить аудио', 'Audio wiederholen')}
                                      disabled={previewAudioPlaying}
                                    >
                                      🔊
                                    </button>
                                  </div>
                                  <div className="flashcard-native-block">
                                    <span className="flashcard-lang-tag is-native">{nativeCode}</span>
                                    <div className="flashcard-native-translation">{previewNativeText}</div>
                                  </div>
                                  <div className="flashcard-details">
                                    {(responseJson.article || responseJson.part_of_speech || responseJson.is_separable !== undefined) && (
                                      <div className="flashcard-section">
                                        <div className="flashcard-section-title">{tr('Грамматика', 'Grammatik')}</div>
                                        <div className="flashcard-meta-grid">
                                          {responseJson.article && (
                                            <div className="flashcard-meta-item">
                                              <span>{tr('Артикль', 'Artikel')}</span>
                                              <strong>{responseJson.article}</strong>
                                            </div>
                                          )}
                                          {responseJson.part_of_speech && (
                                            <div className="flashcard-meta-item">
                                              <span>{tr('Часть речи', 'Wortart')}</span>
                                              <strong>{responseJson.part_of_speech}</strong>
                                            </div>
                                          )}
                                          {responseJson.is_separable !== undefined && (
                                            <div className="flashcard-meta-item">
                                              <span>Trennbar</span>
                                              <strong>{responseJson.is_separable ? tr('да', 'ja') : tr('нет', 'nein')}</strong>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    )}
                                    {feelVisible && feel && (
                                      <div className="flashcard-feel">
                                        <strong>{tr('Почувствовать слово', 'Wort fuehlen')}</strong>
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
                                                  setWebappError(`${tr('Ошибка feedback', 'Feedback-Fehler')}: ${error.message}`);
                                                } finally {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: false }));
                                                }
                                              }}
                                            >
                                              {tr('👍 Нравится', '👍 Gefaellt mir')}
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
                                                  setWebappError(`${tr('Ошибка feedback', 'Feedback-Fehler')}: ${error.message}`);
                                                } finally {
                                                  setFlashcardFeelFeedbackLoading((prev) => ({ ...prev, [entry.id]: false }));
                                                }
                                              }}
                                            >
                                              {tr('👎 Не нравится', '👎 Gefaellt mir nicht')}
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
                                              word_ru: cardTexts.sourceText,
                                              word_de: cardTexts.targetText,
                                              source_text: cardTexts.sourceText,
                                              target_text: cardTexts.targetText,
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
                                          setWebappError(`${tr('Ошибка feel', 'Feel-Fehler')}: ${error.message}`);
                                        } finally {
                                          setFlashcardFeelLoadingMap((prev) => ({
                                            ...prev,
                                            [entry.id]: false,
                                          }));
                                        }
                                      }}
                                    >
                                      {flashcardFeelLoadingMap[entry.id] ? tr('Загружаем...', 'Laden...') : tr('Почувствовать слово', 'Wort fuehlen')}
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
                                      {tr('Назад', 'Zurueck')}
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
                                        {previewAudioPlaying ? tr('Слушаем...', 'Hoeren...') : tr('Далее', 'Weiter')}
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
                          <div className="flashcard-stage is-session">
                            {(flashcardSetComplete || flashcardExitSummary) ? (
                              <div className="flashcard-summary">
                                <h4>{flashcardSetComplete ? tr('Сет завершён', 'Set abgeschlossen') : tr('Повтор завершён', 'Wiederholung beendet')}</h4>
                                <div className="summary-grid">
                                  <div>
                                    <span>{tr('Итого слов', 'Worte gesamt')}</span>
                                    <strong>{flashcardStats.total}</strong>
                                  </div>
                                  <div>
                                    <span>{tr('Верно', 'Richtig')}</span>
                                    <strong>{flashcardStats.correct}</strong>
                                  </div>
                                  <div>
                                    <span>{tr('Неверно', 'Falsch')}</span>
                                    <strong>{flashcardStats.wrong}</strong>
                                  </div>
                                </div>
                                <div className="summary-actions">
                                  <button
                                    type="button"
                                    className="primary-button"
                                    onClick={loadFlashcards}
                                  >
                                    {tr('Да, следующий сет', 'Ja, naechstes Set')}
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
                                    {tr('Нет, завершить', 'Nein, beenden')}
                                  </button>
                                </div>
                              </div>
                            ) : (
                              (() => {
                                const entry = flashcards[flashcardIndex] || {};
                                const responseJson = entry.response_json || {};
                                const cardTexts = resolveFlashcardTexts(entry);
                                const correct = cardTexts.targetText || '—';
                                const questionWord = cardTexts.sourceText || '—';
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
                                            aria-label={t('blocks_menu_open')}
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
                                                {t('blocks_reset_card')}
                                              </button>
                                              <button
                                                type="button"
                                                className="blocks-overflow-item"
                                                onClick={() => {
                                                  setBlocksMenuSettingsOpen((prev) => !prev);
                                                }}
                                              >
                                                {t('blocks_timer_settings')}
                                              </button>
                                              {blocksMenuSettingsOpen && (
                                                <div className="blocks-overflow-settings">
                                                  <div className="blocks-overflow-settings-label">{t('blocks_timer_label')}</div>
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
                                                      {t('timer_adaptive')}
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
                                                      {t('timer_fixed_10')}
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
                                                      {t('timer_none')}
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
                                                {t('blocks_finish_review')}
                                              </button>
                                            </div>
                                          )}
                                        </div>
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
                                      {blocksFinishConfirmOpen && (
                                        <div className="blocks-confirm-backdrop" onClick={() => setBlocksFinishConfirmOpen(false)}>
                                          <div className="blocks-confirm" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
                                            <h4>{t('blocks_finish_title')}</h4>
                                            <p>{t('blocks_finish_text')}</p>
                                            <div className="blocks-confirm-actions">
                                              <button
                                                type="button"
                                                className="secondary-button"
                                                onClick={() => setBlocksFinishConfirmOpen(false)}
                                              >
                                                {t('continue')}
                                              </button>
                                              <button
                                                type="button"
                                                className="danger-button"
                                                onClick={() => {
                                                  setBlocksFinishConfirmOpen(false);
                                                  setFlashcardExitSummary(true);
                                                }}
                                              >
                                                {t('finish')}
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
                                          {tr('Обновить', 'Aktualisieren')}
                                        </button>
                                      </div>
                                    </div>
                                    <div className="flashcard-hero">
                                      <div
                                        key={`timer-${flashcardTimerKey}`}
                                        className={`flashcard-timer ${(flashcardSelection !== null || globalTimerSuspended) ? 'is-paused' : ''}`}
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
                                                  await playTts(german, getLearningTtsLocale());
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
                                        {tr('Следующая карточка через 3 секунды', 'Naechste Karte in 3 Sekunden')}
                                      </div>
                                    )}
                                    {flashcardSelection !== null && !flashcardAutoAdvance && (
                                      <div className="flashcard-actions">
                                        <button
                                          type="button"
                                          className="primary-button"
                                          onClick={() => advanceFlashcard()}
                                        >
                                          {tr('Следующая карточка', 'Naechste Karte')}
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
                                          {tr('Закончить повтор', 'Wiederholung beenden')}
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
                              {tr('Закончить повтор', 'Wiederholung beenden')}
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
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => loadAnalytics()}
                    disabled={analyticsLoading}
                  >
                    {analyticsLoading ? tr('Считаем...', 'Berechnen...') : tr('Обновить', 'Aktualisieren')}
                  </button>
                  {analyticsRank && (
                    <div className="analytics-rank">{tr('Ваше место', 'Dein Rang')}: #{analyticsRank}</div>
                  )}
                </div>

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

            {!flashcardsOnly && isSectionVisible('economics') && (
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
                  <p className="webapp-muted">{tr('Текущий тариф, лимиты и управление подпиской.', 'Aktueller Tarif, Limits und Abo-Verwaltung.')}</p>
                  <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
                </div>

                {billingStatusError && <div className="webapp-error">{billingStatusError}</div>}
                {billingPlansError && <div className="webapp-error">{billingPlansError}</div>}
                {(billingStatusLoading || billingPlansLoading) && <div className="webapp-muted">{tr('Загружаем статус подписки...', 'Abo-Status wird geladen...')}</div>}

                {billingStatus && (
                  <>
                    <div className="analytics-cards economics-cards">
                      <div className="analytics-card">
                        <span>{tr('План', 'Plan')}</span>
                        <strong>{String(billingStatus?.effective_mode || 'free').toUpperCase()}</strong>
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

                    <div className="webapp-section-actions">
                      {billingStatus?.upgrade?.available && (
                        <button type="button" className="secondary-button" onClick={handleBillingUpgrade} disabled={billingActionLoading}>
                          {billingActionLoading ? tr('Открываем...', 'Oeffnen...') : tr('Перейти на Pro', 'Zu Pro wechseln')}
                        </button>
                      )}
                      {billingStatus?.manage?.available && (
                        <button type="button" className="secondary-button" onClick={handleBillingManage} disabled={billingActionLoading}>
                          {billingActionLoading ? tr('Открываем...', 'Oeffnen...') : tr('Управлять подпиской', 'Abo verwalten')}
                        </button>
                      )}
                    </div>
                  </>
                )}

                {Array.isArray(billingPlans) && billingPlans.length > 0 && (
                  <div className="economics-breakdown-grid">
                    {billingPlans.map((plan) => (
                      <div className="economics-breakdown-card" key={`plan-${plan.plan_code}`}>
                        <h4>{String(plan?.name || plan?.plan_code || '').trim() || 'Plan'}</h4>
                        <div className="economics-breakdown-row">
                          <span>{tr('Код', 'Code')}</span>
                          <strong>{String(plan?.plan_code || '')}</strong>
                        </div>
                        <div className="economics-breakdown-row">
                          <span>{tr('Тип', 'Typ')}</span>
                          <strong>{plan?.is_paid ? tr('Платный', 'Bezahlt') : tr('Бесплатный', 'Kostenlos')}</strong>
                        </div>
                        <div className="economics-breakdown-row">
                          <span>{tr('Cap / день', 'Cap / Tag')}</span>
                          <strong>{plan?.daily_cost_cap_eur == null ? tr('Без лимита', 'Unbegrenzt') : `${Number(plan.daily_cost_cap_eur).toFixed(2)} EUR`}</strong>
                        </div>
                      </div>
                    ))}
                  </div>
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
                className={`webapp-selection-menu ${selectionCompact ? 'is-compact' : ''} ${(youtubeAppFullscreen || selectionInlineMode) ? 'is-overlay-mode' : ''}`}
                style={{ left: `${selectionPos.x}px`, top: `${selectionPos.y}px` }}
                onMouseLeave={clearSelection}
              >
                <div className="webapp-selection-text">{selectionText}</div>
                {(youtubeAppFullscreen || selectionInlineMode) ? (
                  <>
                    <div className="webapp-selection-translation">
                      {selectionInlineLookup.loading
                        ? tr('Переводим...', 'Uebersetzen...')
                        : (selectionInlineLookup.translation || '—')}
                    </div>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => loadSelectionInlineLookup(selectionText)}
                      disabled={selectionInlineLookup.loading}
                    >
                      {selectionInlineLookup.loading ? tr('Переводим...', 'Uebersetzen...') : tr('Перевести', 'Uebersetzen')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => playTts(selectionText, getTtsLocaleForLang(selectionLookupLang || readerDetectedLanguage || getNormalizeLookupLang()))}
                      disabled={selectionInlineLookup.loading}
                    >
                      {tr('Прослушать', 'Anhoeren')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => handleQuickAddToDictionary(selectionText, { inlineMode: true })}
                      disabled={selectionInlineLookup.loading}
                    >
                      {tr('Сохранить', 'Speichern')}
                    </button>
                  </>
                ) : (
                  <>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => playTts(selectionText, getTtsLocaleForLang(selectionLookupLang || readerDetectedLanguage || getNormalizeLookupLang()))}
                    >
                      {tr('Прослушать', 'Anhoeren')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => handleQuickLookupDictionary(selectionText)}
                      disabled={selectionLookupLoading}
                    >
                      {selectionLookupLoading ? tr('Переводим...', 'Uebersetzen...') : tr('Перевести', 'Uebersetzen')}
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => handleQuickAddToDictionary(selectionText)}
                    >
                      {tr('Добавить в словарь', 'Zum Woerterbuch hinzufuegen')}
                    </button>
                  </>
                )}
              </div>
            )}
            {inlineToast && <div className="webapp-inline-toast">{inlineToast}</div>}
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
