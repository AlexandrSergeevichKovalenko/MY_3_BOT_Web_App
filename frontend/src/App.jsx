import { useEffect, useMemo, useState } from 'react';
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

function App() {
  const telegramApp = useMemo(() => window.Telegram?.WebApp, []);
  const isWebAppMode = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const isWebappPath = window.location.pathname === '/webapp';
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
  const [finishStatus, setFinishStatus] = useState('idle');
  const [explanations, setExplanations] = useState({});
  const [explanationLoading, setExplanationLoading] = useState({});

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
      const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
      safeStorageRemove(storageKey);
      setTranslationDrafts({});
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

  if (isWebAppMode) {
    return (
      <div className="webapp-page">
        <div className="webapp-card">
          <header className="webapp-header">
            <span className="pill">Telegram Web App</span>
            <h1>Проверка переводов</h1>
            <p>Введите оригинальное предложение и свой перевод, чтобы получить оценку.</p>
          </header>

          <section className="webapp-meta">
            <div>
              <strong>Пользователь:</strong> {webappUser?.first_name || 'Гость'}
            </div>
            <div>
              <strong>Session ID:</strong> {sessionId || '—'}
            </div>
          </section>

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

          <form className="webapp-form" onSubmit={handleWebappSubmit}>
            <section className="webapp-translation-list">
              <div className="webapp-history-head">
                <h3>Ваши переводы</h3>
              </div>
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
                        rows={3}
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
                        <pre className="webapp-result-text">{item.feedback}</pre>
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
                          <pre className="webapp-explanation">
                            {explanations[String(item.sentence_number ?? item.original_text)]}
                          </pre>
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
              className={`secondary-button ${finishStatus === 'done' ? 'status-done' : ''}`}
              disabled={webappLoading}
            >
              {finishStatus === 'done' ? 'Завершено 🙂' : 'Завершить перевод'}
            </button>
          </div>

          <section className="webapp-dictionary">
            <h3>Словарь</h3>
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
          </section>
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

export default App;
