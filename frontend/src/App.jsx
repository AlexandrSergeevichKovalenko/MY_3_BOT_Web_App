import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  LiveKitRoom,
  ControlBar,
  ConnectionStateToast,
  RoomAudioRenderer,
} from '@livekit/components-react';
import '@livekit/components-styles';
import './App.css';

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
  const [youtubeInput, setYoutubeInput] = useState('');
  const [youtubeId, setYoutubeId] = useState('');
  const [youtubeError, setYoutubeError] = useState('');
  const [videoExpanded, setVideoExpanded] = useState(false);
  const [youtubeTranscript, setYoutubeTranscript] = useState([]);
  const [youtubeTranscriptError, setYoutubeTranscriptError] = useState('');
  const [youtubeTranscriptLoading, setYoutubeTranscriptLoading] = useState(false);
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
  const [flashcardIndex, setFlashcardIndex] = useState(0);
  const [flashcardSelection, setFlashcardSelection] = useState(null);
  const [flashcardOptions, setFlashcardOptions] = useState([]);
  const [selectedSections, setSelectedSections] = useState(
    new Set(['translations', 'youtube', 'dictionary', 'flashcards'])
  );
  const [flashcardSetComplete, setFlashcardSetComplete] = useState(false);
  const [flashcardStats, setFlashcardStats] = useState({ total: 0, correct: 0, wrong: 0 });
  const [autoAdvancePaused, setAutoAdvancePaused] = useState(false);
  const [folders, setFolders] = useState([]);
  const [foldersLoading, setFoldersLoading] = useState(false);
  const [foldersError, setFoldersError] = useState('');
  const [dictionaryFolderId, setDictionaryFolderId] = useState('none');
  const [flashcardFolderMode, setFlashcardFolderMode] = useState('all');
  const [flashcardFolderId, setFlashcardFolderId] = useState('');
  const [showNewFolderForm, setShowNewFolderForm] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [newFolderColor, setNewFolderColor] = useState('#5ddcff');
  const [newFolderIcon, setNewFolderIcon] = useState('book');
  const [userAvatar, setUserAvatar] = useState('');

  const dictionaryRef = useRef(null);
  const flashcardsRef = useRef(null);
  const translationsRef = useRef(null);
  const autoAdvanceTimeoutRef = useRef(null);
  const avatarInputRef = useRef(null);

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

  const toggleSection = (key) => {
    setSelectedSections((prev) => {
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

  const openSectionAndScroll = (key, ref) => {
    ensureSectionVisible(key);
    setTimeout(() => {
      if (ref?.current) {
        ref.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }, 80);
  };

  const showAllSections = () => {
    setSelectedSections(new Set(['translations', 'youtube', 'dictionary', 'flashcards']));
  };

  const hideAllSections = () => {
    setSelectedSections(new Set());
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

  // LiveKit login state
  const [telegramID, setTelegramID] = useState('');
  const [username, setUsername] = useState('');

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
      } catch (error) {
        setWebappError(`Ошибка инициализации: ${error.message}`);
      }
    };

    bootstrap();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('review') === '1') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
    }
    if (window.location.pathname === '/webapp/review') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
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
    if (!flashcardsVisible) {
      return;
    }
    loadFlashcards();
    scrollToFlashcards();
  }, [flashcardsVisible, initData, flashcardFolderMode, flashcardFolderId]);

  useEffect(() => {
    if (!flashcards.length) {
      setFlashcardOptions([]);
      return;
    }
    const entry = flashcards[flashcardIndex];
    setFlashcardOptions(buildFlashcardOptions(entry, flashcards));
    setFlashcardSelection(null);
  }, [flashcards, flashcardIndex]);

  useEffect(() => {
    if (flashcardSelection === null) {
      return;
    }
    if (flashcardSetComplete) {
      return;
    }
    if (autoAdvancePaused) {
      return;
    }
    if (!flashcards.length) {
      return;
    }
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
    }
    autoAdvanceTimeoutRef.current = setTimeout(() => {
      advanceFlashcard();
    }, 10000);

    return () => {
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
    };
  }, [
    flashcardSelection,
    flashcardIndex,
    autoAdvancePaused,
    flashcardSetComplete,
    flashcards.length,
  ]);

  useEffect(() => {
    if (!flashcardsVisible && autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
  }, [flashcardsVisible]);

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
      loadSentences();
    }
  }, [initData, isWebAppMode]);

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

  const handleSelection = (event, overrideText = '') => {
    const text = overrideText || normalizeSelectionText(window.getSelection()?.toString() || '');
    if (!text) {
      setSelectionText('');
      setSelectionPos(null);
      return;
    }
    setSelectionText(text);
    setSelectionPos({ x: event.clientX, y: event.clientY });
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
      scrollToDictionary();

      const saveResponse = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: normalized,
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
          set_size: 15,
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
      setFlashcards(items);
      setFlashcardIndex(0);
      setFlashcardSelection(null);
      setFlashcardSetComplete(false);
      setAutoAdvancePaused(false);
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

  const buildFlashcardOptions = (entry, allEntries) => {
    const responseJson = entry?.response_json || {};
    const correct = entry?.translation_de || responseJson.translation_de || '';
    if (!correct) return [];
    const distractors = allEntries
      .filter((item) => item !== entry)
      .map((item) => item.translation_de || item.response_json?.translation_de || '')
      .filter(Boolean)
      .filter((value) => value !== correct)
      .slice(0, 6);
    const options = Array.from(new Set([correct, ...distractors])).filter(Boolean).slice(0, 4);
    while (options.length < 4) {
      options.push(correct);
    }
    for (let i = options.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [options[i], options[j]] = [options[j], options[i]];
    }
    return options;
  };

  const recordFlashcardAnswer = async (entryId, isCorrect) => {
    if (!initData || !entryId) {
      return;
    }
    try {
      await fetch('/api/webapp/flashcards/answer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, entry_id: entryId, is_correct: isCorrect }),
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

  const handleManualTranscript = () => {
    const parsed = parseTranscriptInput(manualTranscript);
    setYoutubeTranscript(parsed);
    setYoutubeTranscriptError('');
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
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    try {
      const response = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: dictionaryResult.word_ru || dictionaryWord.trim(),
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
    } catch (error) {
      setDictionaryError(`Ошибка сохранения: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
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

  useEffect(() => {
    const fetchTranscript = async () => {
      if (!youtubeId || !initData) {
        setYoutubeTranscript([]);
        setYoutubeTranscriptError('');
        return;
      }
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
        setYoutubeTranscript(data.items || []);
        setManualTranscript('');
      } catch (error) {
        setYoutubeTranscript([]);
        setYoutubeTranscriptError(`Авто-субтитры недоступны: ${error.message}`);
      } finally {
        setYoutubeTranscriptLoading(false);
      }
    };

    fetchTranscript();
  }, [youtubeId, initData]);

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

  if (isWebAppMode) {
    return (
      <div className="webapp-page">
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
                className={`menu-item ${selectedSections.has('translations') ? 'is-active' : ''}`}
                onClick={() => toggleSection('translations')}
                disabled={flashcardsOnly}
              >
                Переводы
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                onClick={() => toggleSection('youtube')}
                disabled={flashcardsOnly}
              >
                YouTube
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                onClick={() => toggleSection('dictionary')}
                disabled={flashcardsOnly}
              >
                Словарь
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                onClick={() => toggleSection('flashcards')}
              >
                Карточки
              </button>
            </div>
            <div className="webapp-menu-actions">
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
            <header className="webapp-hero">
              <div className="webapp-hero-copy">
                <span className="pill">Telegram Web App</span>
                <h1>Учите немецкий в потоке</h1>
                <p>
                  Переводы, словарь, видео и карточки — всё в одном месте. Короткие шаги,
                  быстрые проверки и понятный прогресс без перегруза.
                </p>
              </div>
              <div className="webapp-user-badge">
                <input
                  ref={avatarInputRef}
                  type="file"
                  accept="image/*"
                  className="avatar-input"
                  onChange={handleAvatarUpload}
                />
                <button
                  type="button"
                  className="avatar-button"
                  onClick={() => avatarInputRef.current?.click()}
                >
                  {userAvatar ? <img src={userAvatar} alt="User avatar" /> : <span className="avatar-placeholder" />}
                </button>
                <div className="user-name">{webappUser?.first_name || 'Гость'}</div>
              </div>
            </header>

            <section className="webapp-hero-cards">
              <div className="hero-card">
                <h3>Переводите</h3>
                <p>Напишите перевод, получите оценку и объяснения ошибок.</p>
              </div>
              <div className="hero-card">
                <h3>Сохраняйте</h3>
                <p>Добавляйте слова в словарь и группируйте по папкам.</p>
              </div>
              <div className="hero-card">
                <h3>Тренируйтесь</h3>
                <p>Повторяйте слова сетами по 15 карточек с прогрессом.</p>
              </div>
            </section>

            <section className="webapp-quickstart">
              <button
                type="button"
                className="primary-button"
                onClick={() => openSectionAndScroll('translations', translationsRef)}
                disabled={flashcardsOnly}
              >
                Начать перевод
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => openSectionAndScroll('dictionary', dictionaryRef)}
                disabled={flashcardsOnly}
              >
                Открыть словарь
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => {
                  setFlashcardsVisible(true);
                  setFlashcardsOnly(false);
                  openSectionAndScroll('flashcards', flashcardsRef);
                }}
              >
                Повторить слова
              </button>
            </section>

            {flashcardsOnly && <div className="webapp-mode-banner">Режим повторения</div>}

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
                <div className="webapp-section-title">
                  <h2>Ваши переводы</h2>
                </div>
                <form className="webapp-form" onSubmit={handleWebappSubmit}>
                  <section className="webapp-translation-list">
                    {sentences.length === 0 ? (
                      <p className="webapp-muted">Все предложения текущей сессии переведены. Запросите новые.</p>
                    ) : (
                      sentences.map((item, index) => {
                        const draft = translationDrafts[String(item.id_for_mistake_table)] || '';
                        return (
                          <label key={item.id_for_mistake_table} className="webapp-translation-item">
                            <span>
                              {item.unique_id ?? index + 1}. {item.sentence}
                            </span>
                            <textarea
                              rows={5}
                              value={draft}
                              onChange={(event) => handleDraftChange(item.id_for_mistake_table, event.target.value)}
                              placeholder="Введите перевод..."
                            />
                          </label>
                        );
                      })
                    )}
                  </section>

                  <button className="primary-button" type="submit" disabled={webappLoading}>
                    {webappLoading ? 'Проверяем...' : 'Проверить перевод'}
                  </button>
                </form>

                {webappError && <div className="webapp-error">{webappError}</div>}
                {finishMessage && <div className="webapp-success">{finishMessage}</div>}

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
                              <div className="webapp-result-text">{renderFeedback(item.feedback)}</div>
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
                  <button
                    type="button"
                    onClick={handleFinishTranslation}
                    className={`primary-button finish-button ${finishStatus === 'done' ? 'status-done' : ''}`}
                    disabled={webappLoading || results.length === 0}
                  >
                    {finishStatus === 'done' ? 'Завершено' : 'Завершить перевод'}
                  </button>
                  <button
                    type="button"
                    onClick={handleLoadDailyHistory}
                    className="secondary-button"
                    disabled={webappLoading || historyLoading}
                  >
                    {historyLoading ? 'Загружаем...' : 'История за сегодня'}
                  </button>
                  {results.length === 0 && !webappLoading && (
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
                  <section className="webapp-video">
                    <h3>Видео YouTube</h3>
                    <div className="webapp-video-form">
                      <label className="webapp-field">
                        <span>Ссылка или ID видео</span>
                        <input
                          type="text"
                          value={youtubeInput}
                          onChange={(event) => setYoutubeInput(event.target.value)}
                          placeholder="https://youtu.be/VIDEO_ID"
                        />
                      </label>
                      <div className="webapp-video-actions">
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => setVideoExpanded((prev) => !prev)}
                        >
                          {videoExpanded ? 'Обычный режим' : 'Словарь рядом'}
                        </button>
                        {youtubeId && (
                          <a
                            className="secondary-button"
                            href={`https://www.youtube.com/watch?v=${youtubeId}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            Открыть в YouTube
                          </a>
                        )}
                      </div>
                    </div>
                    {youtubeError && <div className="webapp-error">{youtubeError}</div>}
                    {youtubeId ? (
                      <div className={`webapp-video-frame ${videoExpanded ? 'is-expanded' : ''}`}>
                        <iframe
                          title="YouTube player"
                          src={`https://www.youtube.com/embed/${youtubeId}`}
                          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                          allowFullScreen
                        />
                      </div>
                    ) : (
                      <p className="webapp-muted">Вставьте ссылку на видео, чтобы смотреть прямо здесь.</p>
                    )}
                    <p className="webapp-muted">
                      Если видео не воспроизводится внутри Web App, используйте кнопку «Открыть в YouTube».
                    </p>

                    {/*
                    <div className="webapp-subtitles">
                      <div className="webapp-subtitles-header">
                        <h4>Субтитры</h4>
                        <div className="webapp-subtitles-actions">
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => handleManualTranscript()}
                            disabled={!manualTranscript.trim()}
                          >
                            Использовать вставленные
                          </button>
                        </div>
                      </div>

                      {youtubeTranscriptLoading && <div className="webapp-muted">Загружаем субтитры...</div>}
                      {youtubeTranscriptError && <div className="webapp-error">{youtubeTranscriptError}</div>}

                      {youtubeTranscript.length > 0 ? (
                        <div className="webapp-subtitles-list" onMouseUp={handleSelection}>
                          {youtubeTranscript.map((item, index) => (
                            <p key={`${item.start}-${index}`}>
                              {renderClickableText(item.text)}
                            </p>
                          ))}
                        </div>
                      ) : (
                        <div className="webapp-subtitles-fallback">
                          <p className="webapp-muted">
                            Если авто-субтитры недоступны, вставьте текст субтитров ниже.
                          </p>
                          <textarea
                            rows={5}
                            value={manualTranscript}
                            onChange={(event) => setManualTranscript(event.target.value)}
                            placeholder="Вставьте .srt/.vtt или обычный текст субтитров"
                          />
                        </div>
                      )}
                    </div>
                    */}
                  </section>
                )}

                {isSectionVisible('dictionary') && (
                  <section className="webapp-dictionary" ref={dictionaryRef}>
                    <h3>Словарь</h3>
                    <div className="folder-panel">
                      <div className="folder-row">
                        <label className="webapp-field">
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

                    <form className="webapp-dictionary-form" onSubmit={handleDictionaryLookup}>
                      <label className="webapp-field">
                        <span>Слово или фраза (русский)</span>
                        <input
                          type="text"
                          value={dictionaryWord}
                          onChange={(event) => setDictionaryWord(event.target.value)}
                          placeholder="Например: отказаться, уважение, несмотря на"
                        />
                      </label>
                      <div className="dictionary-actions">
                        <button className="secondary-button dictionary-button" type="submit" disabled={dictionaryLoading}>
                          {dictionaryLoading ? 'Ищем...' : 'Перевести'}
                        </button>
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
                          <strong>Перевод:</strong> {dictionaryResult.translation_de || '—'}
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
                    <div className="dictionary-flashcards">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          setFlashcardsVisible(true);
                          setFlashcardsOnly(true);
                          ensureSectionVisible('flashcards');
                          scrollToFlashcards();
                        }}
                      >
                        Повторить слова
                      </button>
                    </div>
                  </section>
                )}
              </div>
            )}

            {isSectionVisible('flashcards') && (
              <section className="webapp-flashcards" ref={flashcardsRef}>
                <h3>Карточки</h3>
                {!flashcardsVisible && (
                  <div className="webapp-muted">
                    Нажмите «Повторить слова», чтобы начать тренировку.
                  </div>
                )}
                {flashcardsVisible && (
                  <div className="flashcards-panel">
                    {flashcardsOnly && (
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          setFlashcardsOnly(false);
                          setFlashcardsVisible(false);
                        }}
                      >
                        Закончить повтор
                      </button>
                    )}
                    <div className="flashcards-filter">
                      <label className="webapp-field">
                        <span>Папка для тренировки</span>
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

                    {flashcardsLoading && <div className="webapp-muted">Загружаем карточки...</div>}
                    {flashcardsError && <div className="webapp-error">{flashcardsError}</div>}
                    {!flashcardsLoading && !flashcardsError && flashcards.length === 0 && (
                      <div className="webapp-muted">Словарь пуст. Сначала добавьте слова.</div>
                    )}
                    {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && (
                      <>
                        {flashcardSetComplete ? (
                          <div className="flashcard-summary">
                            <h4>Сет завершён</h4>
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
                            const correct = entry.translation_de || responseJson.translation_de || '—';
                            const context = Array.isArray(responseJson.usage_examples)
                              ? responseJson.usage_examples[0]
                              : '';

                            return (
                              <div className="flashcard">
                                <div className="flashcard-header">
                                  <span className="flashcard-counter">
                                    {flashcardIndex + 1} / {flashcards.length}
                                  </span>
                                  <div className="flashcard-actions-row">
                                    <button
                                      type="button"
                                      className={`flashcard-pause ${autoAdvancePaused ? 'is-paused' : ''}`}
                                      onClick={() => {
                                        if (autoAdvancePaused) {
                                          setAutoAdvancePaused(false);
                                          if (flashcardSelection !== null) {
                                            advanceFlashcard();
                                          }
                                        } else {
                                          setAutoAdvancePaused(true);
                                        }
                                      }}
                                    >
                                      {autoAdvancePaused ? 'Продолжить' : 'Пауза'}
                                    </button>
                                    <button
                                      type="button"
                                      className="flashcard-refresh"
                                      onClick={loadFlashcards}
                                    >
                                      Обновить
                                    </button>
                                  </div>
                                </div>
                                <div className="flashcard-word">{entry.word_ru || '—'}</div>
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
                                      <button
                                        key={`${option}-${idx}`}
                                        type="button"
                                        className={className}
                                        onClick={() => {
                                          if (flashcardSelection !== null) return;
                                          setFlashcardSelection(idx);
                                          recordFlashcardAnswer(entry.id, option === correct);
                                          setFlashcardStats((prev) => ({
                                            ...prev,
                                            correct: prev.correct + (option === correct ? 1 : 0),
                                            wrong: prev.wrong + (option === correct ? 0 : 1),
                                          }));
                                        }}
                                      >
                                        {option}
                                      </button>
                                    );
                                  })}
                                </div>
                                {flashcardSelection !== null && (
                                  <div className="flashcard-hint">
                                    Следующая карточка через 10 секунд
                                  </div>
                                )}
                              </div>
                            );
                          })()
                        )}
                      </>
                    )}
                  </div>
                )}
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
              <svg viewBox="0 0 320 320" role="img">
                <defs>
                  <linearGradient id="bookGlow" x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0%" stopColor="#ffb347" />
                    <stop offset="100%" stopColor="#ff7e5f" />
                  </linearGradient>
                </defs>
                <circle cx="160" cy="160" r="120" fill="#fff1d6" />
                <path d="M95 110c0-12 10-22 22-22h58c12 0 22 10 22 22v100c0 8-6 15-14 16-20 2-44 2-66 0-12-1-22-10-22-22z" fill="url(#bookGlow)" />
                <path d="M185 88h32c12 0 22 10 22 22v100c0 12-10 22-22 22h-32" fill="#ffd7aa" />
                <path d="M120 135h60M120 165h60M120 195h50" stroke="#6b3a1a" strokeWidth="6" strokeLinecap="round" />
                <circle cx="210" cy="90" r="26" fill="#6b3a1a" />
                <path d="M198 86h24v8h-24zM210 72v32" fill="#fff1d6" />
              </svg>
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
