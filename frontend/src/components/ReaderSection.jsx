/* eslint-disable react/jsx-key */
/**
 * ReaderSection — презентационный компонент раздела «Читалка».
 *
 * Что делает: рендерит ВСЮ Reader-секцию (библиотека, чтение, scrubber, TOC,
 * settings sheet, page-jump, аудио-панель). Никакой бизнес-логики. Всё state,
 * хелперы, async-вызовы остаются в App.jsx и передаются как props.
 *
 * Включает встроенные фичи Фазы 2:
 *   • Hero-карточка «Продолжаешь читать» в библиотеке
 *   • Сегментированный контрол тем (dark / sepia / cream) в Settings sheet
 *
 * Совместим с reader-redesign.css (Phase 1). Без него — будет старый дизайн,
 * но всё работает.
 *
 * См. READER_INTEGRATION.md → раздел «Полный JSX-вынос (Phase B)».
 */
import React from 'react';
import { ReaderLibraryProvider } from '../providers/ReaderLibraryProvider.jsx';
import ReaderLibrarySection from './ReaderLibrarySection.jsx';

export default function ReaderSection(props) {
  const {
    // ── i18n ─────────────────────────────────────────────────────
    tr,

    // ── billing (для апгрейда при лимите) ────────────────────────
    handleBillingUpgrade,
    billingActionLoading,

    // ── refs ─────────────────────────────────────────────────────
    readerRef,
    readerArticleRef,
    readerPageInnerRef,
    readerMeasureInnerRef,
    readerFileInputRef,

    // ── library state ────────────────────────────────────────────
    readerDocuments,
    readerLibrarySearch, setReaderLibrarySearch,
    readerIncludeArchived, setReaderIncludeArchived,
    readerLibraryLoading, readerLibraryError,
    loadReaderLibrary,
    readerAddOpen, setReaderAddOpen,
    readerOpeningDocumentId,
    openReaderDocument, renameReaderDocument, archiveReaderDocument, deleteReaderDocument,

    // ── add form ─────────────────────────────────────────────────
    readerInput, setReaderInput,
    readerSelectedFile,
    handleReaderFileSelect, handleReaderIngest,
    readerLoading, readerError, readerErrorCode,

    // ── reading state ────────────────────────────────────────────
    readerDocumentId, readerTitle, readerContent,
    readerPages, readerDisplayPages, readerPageCount,
    readerCurrentPage, setReaderCurrentPage,
    readerProgressPercent,
    applyReaderProgressPercent = () => {},
    readerBookmarkPercent, setReaderBookmarkPercent, readerBookmarkPage,
    persistReaderExactBookmark = () => {},
    isCurrentReaderPageBookmarked,
    readerCanUseOriginalLayout,
    readerUsesOriginalEpubLayout = false,
    readerOriginalTocHref = '',
    readerResolvedOriginalTocTitle = '',
    readerOriginalCoverUrl = '',
    readerOriginalCoverVisible = false,
    dismissReaderOriginalCover = () => {},
    readerLayoutMode,
    readerReadingMode, setReaderReadingMode,
    readerFontSize, setReaderFontSize,
    readerFontWeight, setReaderFontWeight,
    readerSwipeSensitivity, setReaderSwipeSensitivity,
    readerImmersive, setReaderImmersive,
    readerTopbarCollapsed, setReaderTopbarCollapsed,
    readerSettingsOpen, setReaderSettingsOpen,
    readerArchiveOpen, setReaderArchiveOpen,
    readerHasContent,
    readerOriginalEpubLoading = false,
    readerOriginalEpubError = '',

    // ── reading event handlers ───────────────────────────────────
    handleReaderStructuredClick,
    handleReaderArticleMouseUp,
    handleReaderPageWheel,
    handleReaderPageTouchStart,
    handleReaderArticleTouchMove,
    handleReaderArticleTouchEnd,
    handleReaderArticleTouchCancel,
    renderReaderStructuredText,

    // ── timer ────────────────────────────────────────────────────
    readerTimerPaused,
    readerElapsedTotalSeconds,
    formatReaderTimer,
    toggleReaderTimerPause,
    computeReaderProgressPercent,
    syncReaderState,

    // ── TOC ──────────────────────────────────────────────────────
    readerShowToc, setReaderShowToc,
    readerTocItems,
    loadReaderToc,

    // ── page jump ────────────────────────────────────────────────
    readerShowPageJump, setReaderShowPageJump,
    readerPageJumpInput, setReaderPageJumpInput,

    // ── audio (оффлайн-аудио документа) ──────────────────────────
    readerAudioLoading, readerAudioError,
    readerAudioPreviewUrl, readerAudioPreviewName,
    downloadReaderAudio, closeReaderAudioPreview,
    readerAudioPremiumEnabled = true,
    readerAudioPremiumKnown = false,
    onReaderAudioUpgrade = () => {},

    // ── cover/meta helpers ───────────────────────────────────────
    getReaderCoverUrl,
    getReaderCoverInitials,
    getReaderCoverGradient,
    buildReaderArchiveMeta,

    // ── constants ────────────────────────────────────────────────
    READER_DEFAULT_FONT_SIZE,
    READER_DEFAULT_FONT_WEIGHT,

    // ── Phase 2.2: theme switcher ─────────────────────────────────
    readerColorTheme = 'dark',
    applyReaderColorTheme = () => {},

    // ── Phase 2.4: audio-sync player ──────────────────────────────
    audioElementRef,
    readerAudioPreloadElementRef,
    readerEpubViewportRef,
    readerAudioPlayActive = false,
    readerAudioPlayLoading = false,
    readerAudioPlayError = '',
    readerAudioPlayData = null,
    readerAudioPlayPosition = 0,
    readerAudioPaused = false,
    readerAudioVoice = '',
    setReaderAudioVoice = () => {},
    readerAudioRate = 1.0,
    setReaderAudioRate = () => {},
    readerAudioStartWid = null,
    readerAudioAwaitingWordTap = false,
    onReaderAudioPlayBtn = () => {},
    playReaderAudioPage = () => {},
    pauseReaderAudioPlay = () => {},
    resumeReaderAudioPlay = () => {},
    stopReaderAudioPlay = () => {},
    jumpReaderTocItem = () => {},
    switchReaderLayoutMode = () => {},
  } = props;

  const sectionClass = [
    'webapp-section',
    'webapp-reader',
    readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : '',
    readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : '',
  ].filter(Boolean).join(' ');
  const showLibraryMode = !readerHasContent || readerArchiveOpen || !readerImmersive;
  const effectiveReaderTheme = showLibraryMode ? 'dark' : readerColorTheme;
  const readerUsesCustomLayout = !readerCanUseOriginalLayout || readerLayoutMode === 'custom';
  const readerShowsLazyOriginalPage = !readerUsesCustomLayout
    && Array.isArray(readerPages)
    && readerPages[readerCurrentPage - 1] === null;
  const readerAudioPremiumLocked = readerAudioPremiumKnown && !readerAudioPremiumEnabled;
  const readerAudioPremiumHint = tr(
    'Аудио в книге доступно только по премиум подписке.',
    'Audio im Reader ist nur mit Premium verfuegbar.'
  );

  return (
    <section
      className={sectionClass}
      data-reader-theme={effectiveReaderTheme}
      ref={readerRef}
    >
      {(() => {
        if (showLibraryMode) {
          return (
            <ReaderLibraryProvider
              value={{
                tr,
                handleBillingUpgrade,
                billingActionLoading,
                readerFileInputRef,
                readerDocuments,
                readerLibrarySearch,
                setReaderLibrarySearch,
                readerIncludeArchived,
                setReaderIncludeArchived,
                readerLibraryLoading,
                readerLibraryError,
                loadReaderLibrary,
                readerAddOpen,
                setReaderAddOpen,
                readerOpeningDocumentId,
                openReaderDocument,
                renameReaderDocument,
                archiveReaderDocument,
                deleteReaderDocument,
                readerInput,
                setReaderInput,
                readerSelectedFile,
                handleReaderFileSelect,
                handleReaderIngest,
                readerLoading,
                readerError,
                readerErrorCode,
                readerDocumentId,
                readerAudioLoading,
                readerAudioError,
                readerAudioPreviewUrl,
                readerAudioPreviewName,
                downloadReaderAudio,
                closeReaderAudioPreview,
                readerAudioPremiumLocked,
                readerAudioPremiumHint,
                onReaderAudioUpgrade,
                getReaderCoverUrl,
                getReaderCoverInitials,
                getReaderCoverGradient,
                buildReaderArchiveMeta,
              }}
            >
              <ReaderLibrarySection />
            </ReaderLibraryProvider>
          );
        }

        // ════════════════════════════════════════════════════════════════
        //  READING MODE
        // ════════════════════════════════════════════════════════════════
        return (
          <>
            {/* ── Topbar peek (when chrome collapsed) ──────────────── */}
            {readerImmersive && readerTopbarCollapsed && (
              <div className="reader-topbar-peek">
                <button
                  type="button"
                  className={`secondary-button reader-toolbar-btn reader-toolbar-btn-icon-only ${readerShowToc ? 'is-active' : ''}`}
                  onClick={() => {
                    if (!readerShowToc && readerTocItems.length === 0) void loadReaderToc();
                    setReaderShowToc((v) => !v);
                  }}
                  disabled={!readerContent}
                  title={tr('Оглавление', 'Inhaltsverzeichnis')}
                  aria-label={tr('Оглавление', 'Inhaltsverzeichnis')}
                >
                  <span className="reader-toolbar-btn-icon" aria-hidden="true">
                    <svg viewBox="0 0 18 18" fill="none">
                      <path d="M4 5h10M4 9h10M4 13h6.5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                    </svg>
                  </span>
                </button>
                <button
                  type="button"
                  className={`reader-bookmark-btn reader-toolbar-btn reader-toolbar-btn-icon-only ${isCurrentReaderPageBookmarked ? 'is-active' : ''}`}
                  onClick={() => {
                    const mark = computeReaderProgressPercent();
                    setReaderBookmarkPercent(mark);
                    persistReaderExactBookmark(readerCurrentPage);
                    if (readerDocumentId) {
                      syncReaderState({ bookmark_percent: Number(mark.toFixed(2)) });
                    }
                  }}
                  disabled={!readerContent || !readerDocumentId}
                  aria-label={tr('Поставить закладку', 'Lesezeichen setzen')}
                  title={tr('Поставить закладку', 'Lesezeichen setzen')}
                >
                  <span className="reader-toolbar-btn-icon" aria-hidden="true">
                    <svg viewBox="0 0 18 18" fill="none">
                      <path d="M5.25 3.75h7.5a.75.75 0 0 1 .75.75v9.75L9 11.55l-4.5 2.7V4.5a.75.75 0 0 1 .75-.75Z" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
                    </svg>
                  </span>
                </button>
                {/* ── Audio play button (peek bar) ── */}
                <button
                  type="button"
                  className={`secondary-button reader-toolbar-btn reader-toolbar-btn-icon-only reader-audio-play-btn${readerAudioPlayActive ? ' is-playing' : ''}${readerAudioAwaitingWordTap ? ' is-awaiting' : ''}`}
                  onClick={readerAudioPremiumLocked ? onReaderAudioUpgrade : onReaderAudioPlayBtn}
                  disabled={!readerHasContent || readerAudioPlayLoading || billingActionLoading}
                  title={readerAudioPremiumLocked
                    ? readerAudioPremiumHint
                    : (readerAudioPlayActive
                      ? (readerAudioPaused ? tr('Продолжить', 'Fortsetzen') : tr('Пауза', 'Pause'))
                      : (readerAudioAwaitingWordTap ? tr('Нажми слово…', 'Wort antippen…') : tr('Аудио', 'Audio')))}
                  aria-label={tr('Аудиовоспроизведение', 'Audio')}
                >
                  <span className="reader-toolbar-btn-icon" aria-hidden="true">
                    {readerAudioPlayLoading ? (
                      <svg viewBox="0 0 18 18" fill="none">
                        <circle cx="9" cy="9" r="6" stroke="currentColor" strokeWidth="1.6" strokeDasharray="28" strokeDashoffset="10" strokeLinecap="round">
                          <animateTransform attributeName="transform" type="rotate" from="0 9 9" to="360 9 9" dur="0.9s" repeatCount="indefinite"/>
                        </circle>
                      </svg>
                    ) : readerAudioPlayActive && !readerAudioPaused ? (
                      <svg viewBox="0 0 18 18" fill="none">
                        <rect x="5" y="4.5" width="2.8" height="9" rx="1" fill="currentColor"/>
                        <rect x="10.2" y="4.5" width="2.8" height="9" rx="1" fill="currentColor"/>
                      </svg>
                    ) : (
                      <svg viewBox="0 0 18 18" fill="none">
                        <path d="M6 4.5l8 4.5-8 4.5V4.5z" fill="currentColor"/>
                      </svg>
                    )}
                  </span>
                </button>
                <div className="reader-topbar-peek-spacer" />
                <button
                  type="button"
                  className="secondary-button reader-topbar-toggle-chip reader-toolbar-btn"
                  onClick={() => setReaderTopbarCollapsed(false)}
                  title={tr('Развернуть панель', 'Leiste aufklappen')}
                >
                  <span className="reader-toolbar-btn-icon" aria-hidden="true">
                    <svg viewBox="0 0 18 18" fill="none">
                      <path d="M4.5 6.75 9 11.25l4.5-4.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </span>
                  <span className="reader-toolbar-btn-label">{tr('Развернуть', 'Aufklappen')}</span>
                </button>
              </div>
            )}

            {/* ── Topbar (expanded) ───────────────────────────────── */}
            {!readerTopbarCollapsed && (
              <div className="reader-topbar reader-immersive-topbar">
                <div className="reader-immersive-head">
                  <div className="reader-immersive-title-wrap">
                    <div className="reader-topbar-title">
                      {readerTitle || tr('Читалка', 'Leser')}
                    </div>
                    <div className="webapp-muted reader-topbar-meta">
                      {tr('Прогресс', 'Fortschritt')}: {Math.round(readerProgressPercent)}%
                      {!readerUsesOriginalEpubLayout && readerPageCount > 0 ? ` • ${tr('Страница', 'Seite')} ${readerCurrentPage}/${readerPageCount}` : ''}
                      {readerUsesOriginalEpubLayout ? ` • EPUB Original` : ''}
                    </div>
                    {readerUsesOriginalEpubLayout && readerResolvedOriginalTocTitle && (
                      <div className="webapp-muted reader-topbar-meta reader-topbar-meta-chapter">
                        {readerResolvedOriginalTocTitle}
                      </div>
                    )}
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
                    className="section-home-back reader-toolbar-btn reader-toolbar-btn-back"
                    onClick={() => {
                      setReaderArchiveOpen(true);
                      setReaderImmersive(false);
                      setReaderTopbarCollapsed(false);
                      setReaderSettingsOpen(false);
                    }}
                  >
                    <span className="reader-toolbar-btn-icon" aria-hidden="true">
                      <svg viewBox="0 0 18 18" fill="none">
                        <path
                          d="M10.75 4.25 6 9l4.75 4.75M6.6 9h6.15"
                          stroke="currentColor"
                          strokeWidth="1.75"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </svg>
                    </span>
                    <span className="reader-toolbar-btn-label">
                      {tr('Архив', 'Archiv')}
                    </span>
                  </button>
                  <button
                    type="button"
                    className={`secondary-button reader-toolbar-btn reader-toolbar-btn-icon-only ${readerReadingMode === 'horizontal' ? 'is-active' : ''}`}
                    onClick={() => {
                      const nextMode = readerReadingMode === 'vertical' ? 'horizontal' : 'vertical';
                      setReaderReadingMode(nextMode);
                      if (readerDocumentId) {
                        syncReaderState({ reading_mode: nextMode });
                      }
                    }}
                    disabled={!readerContent}
                    title={tr('Направление прокрутки', 'Scroll-Richtung')}
                    aria-label={tr('Направление прокрутки', 'Scroll-Richtung')}
                  >
                    <span className="reader-toolbar-btn-icon" aria-hidden="true">
                      {readerReadingMode === 'vertical' ? (
                        <svg viewBox="0 0 18 18" fill="none">
                          <path
                            d="M9 3.5v11M9 3.5 6.9 5.6M9 3.5l2.1 2.1M9 14.5l-2.1-2.1M9 14.5l2.1-2.1"
                            stroke="currentColor"
                            strokeWidth="1.7"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      ) : (
                        <svg viewBox="0 0 18 18" fill="none">
                          <path
                            d="M3.5 9h11M3.5 9l2.1-2.1M3.5 9l2.1 2.1M14.5 9l-2.1-2.1M14.5 9l-2.1 2.1"
                            stroke="currentColor"
                            strokeWidth="1.7"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      )}
                    </span>
                  </button>
                  <button
                    type="button"
                    className="secondary-button reader-toolbar-btn reader-toolbar-btn-icon-only"
                    onClick={() => setReaderSettingsOpen(true)}
                    title={tr('Настройки чтения', 'Leseeinstellungen')}
                    aria-label={tr('Настройки чтения', 'Leseeinstellungen')}
                  >
                    <span className="reader-toolbar-btn-icon" aria-hidden="true">
                      <svg viewBox="0 0 18 18" fill="none">
                        <path
                          d="M4.25 5.25h9.5M6.5 5.25a.75.75 0 1 1-1.5 0 .75.75 0 0 1 1.5 0Zm6.5 3.75h-8M10.75 9a.75.75 0 1 1-1.5 0 .75.75 0 0 1 1.5 0Zm3 3.75h-9.5M8.75 12.75a.75.75 0 1 1-1.5 0 .75.75 0 0 1 1.5 0Z"
                          stroke="currentColor"
                          strokeWidth="1.55"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </svg>
                    </span>
                  </button>
                  {readerCanUseOriginalLayout && (
                    <button
                      type="button"
                      className={`secondary-button reader-toolbar-btn ${readerLayoutMode === 'original' ? 'is-active' : ''}`}
                      onClick={() => switchReaderLayoutMode(readerLayoutMode === 'original' ? 'custom' : 'original', { resetTypography: readerLayoutMode !== 'original' })}
                      title={readerLayoutMode === 'original'
                        ? tr('Переключить в текстовый режим', 'In Textmodus wechseln')
                        : tr('Переключить в оригинальную вёрстку', 'Zum Originallayout wechseln')}
                    >
                      <span className="reader-toolbar-btn-label">
                        {readerLayoutMode === 'original'
                          ? tr('Textmodus', 'Textmodus')
                          : tr('Original', 'Original')}
                      </span>
                    </button>
                  )}
                  <button
                    type="button"
                    className="secondary-button reader-topbar-collapse-btn reader-topbar-toggle-chip reader-toolbar-btn"
                    onClick={() => {
                      const next = !readerTopbarCollapsed;
                      setReaderTopbarCollapsed(next);
                      if (next) setReaderSettingsOpen(false);
                    }}
                    title={readerTopbarCollapsed
                      ? tr('Развернуть панель', 'Leiste aufklappen')
                      : tr('Свернуть панель', 'Leiste einklappen')}
                  >
                    <span className="reader-toolbar-btn-icon" aria-hidden="true">
                      <svg viewBox="0 0 18 18" fill="none">
                        <path
                          d="M4.5 11.25 9 6.75l4.5 4.5"
                          stroke="currentColor"
                          strokeWidth="1.7"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </svg>
                    </span>
                    <span className="reader-toolbar-btn-label">
                      {tr('Свернуть', 'Einklappen')}
                    </span>
                  </button>
                </div>
              </div>
            )}

            {/* ── Audio awaiting-word hint ─────────────────────────── */}
            {readerAudioAwaitingWordTap && !readerUsesOriginalEpubLayout && (
              <div className="reader-audio-word-hint">
                {tr('Нажми на слово — аудио начнётся с него', 'Tippe ein Wort an — Audio startet dort')}
              </div>
            )}

            {/* ── Article ─────────────────────────────────────────── */}
            {readerContent && (
              <article
                ref={readerArticleRef}
                className={`reader-article ${readerReadingMode === 'horizontal' ? 'is-horizontal' : 'is-vertical'} ${readerPageCount > 0 ? 'has-pages' : ''}${readerUsesOriginalEpubLayout ? ' is-epub-original' : ''}`}
                onClick={readerUsesOriginalEpubLayout ? undefined : handleReaderStructuredClick}
                onMouseUp={readerUsesOriginalEpubLayout ? undefined : handleReaderArticleMouseUp}
                onWheel={readerUsesOriginalEpubLayout ? undefined : handleReaderPageWheel}
                onTouchStart={readerUsesOriginalEpubLayout ? undefined : handleReaderPageTouchStart}
                onTouchMove={readerUsesOriginalEpubLayout ? undefined : handleReaderArticleTouchMove}
                onTouchEnd={readerUsesOriginalEpubLayout ? undefined : handleReaderArticleTouchEnd}
                onTouchCancel={readerUsesOriginalEpubLayout ? undefined : handleReaderArticleTouchCancel}
              >
                {readerUsesOriginalEpubLayout ? (
                  <div className="reader-epub-original-shell">
                    {readerOriginalEpubLoading && (
                      <div className="reader-epub-original-status webapp-muted">
                        {tr('Загружаем оригинальный EPUB…', 'Original-EPUB wird geladen…')}
                      </div>
                    )}
                    {readerOriginalEpubError && (
                      <div className="reader-epub-original-error">
                        {readerOriginalEpubError}
                      </div>
                    )}
                    {readerOriginalCoverVisible && readerOriginalCoverUrl && (
                      <button
                        type="button"
                        className="reader-epub-original-cover"
                        onClick={dismissReaderOriginalCover}
                        title={tr(
                          'Показана настоящая обложка EPUB. Нажми, чтобы перейти к содержимому книги.',
                          'Hier siehst du das echte EPUB-Cover. Tippe, um zum Buchinhalt zu wechseln.'
                        )}
                      >
                        <img
                          src={readerOriginalCoverUrl}
                          alt={tr('Обложка книги', 'Buchcover')}
                          className="reader-epub-original-cover-image"
                        />
                        <span className="reader-epub-original-cover-caption">
                          {tr('Это оригинальная обложка EPUB. Нажми, чтобы открыть текст книги.', 'Das ist das originale EPUB-Cover. Tippe, um den Buchtext zu oeffnen.')}
                        </span>
                      </button>
                    )}
                    <div ref={readerEpubViewportRef} className="reader-epub-original-viewport" />
                  </div>
                ) : readerPageCount > 0 ? (
                  <div className="reader-pages-layout">
                  <div
                      key={`reader-page-${readerLayoutMode}-${readerCurrentPage}`}
                      className="reader-page-sheet"
                      style={{
                        '--reader-font-size': `${readerFontSize}px`,
                        '--reader-font-weight': readerFontWeight,
                      }}
                    >
                      {isCurrentReaderPageBookmarked && (
                        <span className="reader-page-bookmark-indicator" aria-hidden="true" />
                      )}
                      <div ref={readerPageInnerRef} className="reader-page-sheet-inner">
                        {readerShowsLazyOriginalPage
                          ? (
                            <div className="reader-page-loading">
                              <svg className="reader-lib-spinner" viewBox="0 0 24 24" fill="none" style={{ width: 36, height: 36, color: 'rgba(148,163,184,0.6)' }}>
                                <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                              </svg>
                            </div>
                          )
                          : renderReaderStructuredText()
                        }
                      </div>
                      <div className="reader-page-num">
                        {tr('Стр.', 'S.')}{' '}{readerCurrentPage}{readerPageCount > 0 ? ` / ${readerPageCount}` : ''}
                      </div>
                    </div>
                    <div
                      className="reader-page-sheet reader-page-sheet-measure"
                      aria-hidden="true"
                      style={{
                        '--reader-font-size': `${readerFontSize}px`,
                        '--reader-font-weight': readerFontWeight,
                      }}
                    >
                      <div ref={readerMeasureInnerRef} className="reader-page-sheet-inner" />
                      <div className="reader-page-num">{tr('Стр.', 'S.')} 999 / 999</div>
                    </div>
                  </div>
                ) : (
                  renderReaderStructuredText()
                )}
              </article>
            )}

            {/* ── Scrubber bar — hidden while audio player is active ── */}
            {(readerUsesOriginalEpubLayout || readerPageCount > 0) && !readerAudioPlayActive && (
              <div className="reader-scrubber-bar">
                <button
                  type="button"
                  className="reader-scrubber-page-btn"
                  onClick={() => {
                    if (readerUsesOriginalEpubLayout) return;
                    setReaderPageJumpInput(String(readerCurrentPage));
                    setReaderShowPageJump(true);
                  }}
                  title={readerUsesOriginalEpubLayout
                    ? tr('В оригинальном EPUB число страниц не фиксировано и зависит от движка рендера.', 'Im originalen EPUB ist die Seitenzahl nicht fest und haengt vom Rendering ab.')
                    : tr('Перейти к странице', 'Zur Seite springen')}
                >
                  {readerUsesOriginalEpubLayout
                    ? `${Math.round(readerProgressPercent)}%`
                    : `${readerCurrentPage} / ${readerPageCount}`}
                </button>
                <div className="reader-scrubber-track-wrap">
                  <input
                    type="range"
                    className="reader-scrubber-input"
                    min={readerUsesOriginalEpubLayout ? 0 : 1}
                    max={readerUsesOriginalEpubLayout ? 100 : readerPageCount}
                    value={readerUsesOriginalEpubLayout ? Math.round(readerProgressPercent) : readerCurrentPage}
                    onChange={(e) => {
                      if (readerUsesOriginalEpubLayout) {
                        applyReaderProgressPercent(Number(e.target.value));
                        return;
                      }
                      const page = Math.max(1, Math.min(readerPageCount, Number(e.target.value)));
                      setReaderCurrentPage(page);
                    }}
                  />
                </div>
                {readerBookmarkPercent > 0 && !isCurrentReaderPageBookmarked ? (
                  <button
                    type="button"
                    className="reader-scrubber-bookmark-btn"
                    onClick={() => {
                      if (readerUsesOriginalEpubLayout) {
                        applyReaderProgressPercent(readerBookmarkPercent);
                        return;
                      }
                      setReaderCurrentPage(readerBookmarkPage);
                    }}
                    title={readerUsesOriginalEpubLayout
                      ? tr('Перейти к сохранённому прогрессу', 'Zum gespeicherten Fortschritt springen')
                      : tr('Перейти к закладке', 'Zur Lesezeiche springen')}
                  >
                    <svg viewBox="0 0 18 18" fill="none" width="16" height="16">
                      <path d="M5.25 3.75h7.5a.75.75 0 0 1 .75.75v9.75L9 11.55l-4.5 2.7V4.5a.75.75 0 0 1 .75-.75Z" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
                    </svg>
                  </button>
                ) : (
                  <div className="reader-scrubber-pct webapp-muted">{Math.round(readerProgressPercent)}%</div>
                )}
              </div>
            )}

            {/* ── Hidden audio element ────────────────────────────── */}
            <audio ref={audioElementRef} preload="metadata" playsInline style={{ display: 'none' }} />
            <audio ref={readerAudioPreloadElementRef} preload="none" playsInline style={{ display: 'none' }} />

            {/* ── Audio mini-player bar ────────────────────────────── */}
            {readerAudioPlayActive && (
              <div className="reader-audio-mini-player">
                <button
                  type="button"
                  className="reader-audio-mini-btn is-primary"
                  onClick={readerAudioPaused ? resumeReaderAudioPlay : pauseReaderAudioPlay}
                  aria-label={readerAudioPaused ? tr('Продолжить', 'Fortsetzen') : tr('Пауза', 'Pause')}
                >
                  {readerAudioPaused ? (
                    <svg viewBox="0 0 18 18" fill="none" width="18" height="18">
                      <path d="M4 3.5a1 1 0 0 1 1.5-.87l9 5.18a1 1 0 0 1 0 1.74l-9 5.18A1 1 0 0 1 4 13.82V3.5Z" fill="currentColor" />
                    </svg>
                  ) : (
                    <svg viewBox="0 0 18 18" fill="none" width="18" height="18">
                      <rect x="4" y="4" width="3.5" height="10" rx="1" fill="currentColor" />
                      <rect x="10.5" y="4" width="3.5" height="10" rx="1" fill="currentColor" />
                    </svg>
                  )}
                </button>
                <div className="reader-audio-mini-progress">
                  <div
                    className="reader-audio-mini-fill"
                    style={{
                      width: readerAudioPlayData?.duration_ms
                        ? `${Math.min(100, (readerAudioPlayPosition / readerAudioPlayData.duration_ms) * 100)}%`
                        : '0%',
                    }}
                  />
                </div>
                <div className="reader-audio-mini-time webapp-muted">
                  {(() => {
                    const fmt = (ms) => {
                      const s = Math.floor(ms / 1000);
                      return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
                    };
                    return `${fmt(readerAudioPlayPosition)} / ${fmt(readerAudioPlayData?.duration_ms || 0)}`;
                  })()}
                </div>
                <select
                  className="reader-audio-mini-rate"
                  value={readerAudioRate}
                  onChange={(e) => {
                    const newRate = parseFloat(e.target.value);
                    setReaderAudioRate(newRate);
                    if (audioElementRef?.current) {
                      audioElementRef.current.playbackRate = newRate;
                    }
                  }}
                  aria-label={tr('Скорость', 'Geschwindigkeit')}
                >
                  <option value="0.75">0.75×</option>
                  <option value="1">1×</option>
                  <option value="1.25">1.25×</option>
                  <option value="1.5">1.5×</option>
                </select>
                <button
                  type="button"
                  className="reader-audio-mini-btn reader-audio-mini-close"
                  onClick={stopReaderAudioPlay}
                  aria-label={tr('Закрыть плеер', 'Player schließen')}
                >
                  <svg viewBox="0 0 18 18" fill="none" width="16" height="16">
                    <path d="M4.5 4.5 13.5 13.5M13.5 4.5 4.5 13.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
                  </svg>
                </button>
              </div>
            )}

            {/* ── Audio error (shown even when player not yet active) ── */}
            {readerAudioPlayError && !readerAudioPlayActive && (
              <div className="reader-audio-error-bar">
                {readerAudioPlayError === 'reader_audio_monthly_limit_exceeded'
                  ? tr('Лимит аудио на этот месяц исчерпан. Попробуй в следующем месяце или улучши план.',
                       'Monatliches Audio-Limit erreicht. Nächsten Monat oder Plan upgraden.')
                  : readerAudioPlayError}
              </div>
            )}

            {/* ── Page jump dialog ────────────────────────────────── */}
            {readerShowPageJump && (
              <div
                className="reader-page-jump-overlay"
                role="dialog"
                aria-modal="true"
                onClick={() => setReaderShowPageJump(false)}
              >
                <div className="reader-page-jump-dialog" onClick={(e) => e.stopPropagation()}>
                  <div className="reader-page-jump-title">
                    {tr('Перейти к странице', 'Zur Seite springen')}
                  </div>
                  <div className="reader-page-jump-body">
                    <input
                      type="number"
                      inputMode="numeric"
                      className="reader-page-jump-input"
                      value={readerPageJumpInput}
                      min={1}
                      max={readerPageCount}
                      onChange={(e) => setReaderPageJumpInput(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                          const page = Math.max(1, Math.min(readerPageCount, Number(readerPageJumpInput)));
                          if (!Number.isNaN(page)) setReaderCurrentPage(page);
                          setReaderShowPageJump(false);
                        }
                      }}
                      autoFocus
                    />
                    <span className="reader-page-jump-total webapp-muted"> / {readerPageCount}</span>
                  </div>
                  <div className="reader-page-jump-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setReaderShowPageJump(false)}
                    >
                      {tr('Отмена', 'Abbrechen')}
                    </button>
                    <button
                      type="button"
                      className="primary-button"
                      onClick={() => {
                        const page = Math.max(1, Math.min(readerPageCount, Number(readerPageJumpInput)));
                        if (!Number.isNaN(page)) setReaderCurrentPage(page);
                        setReaderShowPageJump(false);
                      }}
                    >
                      {tr('Перейти', 'Springen')} →
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* ── TOC drawer ──────────────────────────────────────── */}
            {readerShowToc && (
              <div
                className="reader-toc-overlay"
                onClick={() => setReaderShowToc(false)}
              >
                <div className="reader-toc-drawer" onClick={(e) => e.stopPropagation()}>
                  <div className="reader-toc-head">
                    <strong>{tr('Оглавление', 'Inhaltsverzeichnis')}</strong>
                    <button
                      type="button"
                      className="secondary-button reader-toc-close-btn"
                      onClick={() => setReaderShowToc(false)}
                      aria-label={tr('Закрыть', 'Schliessen')}
                    >×</button>
                  </div>
                  <div className="reader-toc-list">
                    {readerTocItems.length === 0 ? (
                      <div className="reader-toc-empty webapp-muted">
                        {tr('Оглавление недоступно', 'Keine Gliederung verfügbar')}
                      </div>
                    ) : (
                      readerTocItems.map((item, idx) => (
                        <button
                          key={idx}
                          type="button"
                          className={`reader-toc-item ${(
                            readerUsesOriginalEpubLayout
                              ? (String(item?.href_normalized || '').trim() !== '' && String(item?.href_normalized || '').trim() === String(readerOriginalTocHref || '').trim())
                              : item.page_number === readerCurrentPage
                          ) ? 'is-active' : ''}`}
                          onClick={() => {
                            jumpReaderTocItem(item);
                            setReaderShowToc(false);
                          }}
                        >
                          <span className="reader-toc-item-title">{item.title}</span>
                          {!readerUsesOriginalEpubLayout && (
                            <span className="reader-toc-item-page webapp-muted">{item.page_number}</span>
                          )}
                        </button>
                      ))
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* ── Settings sheet ──────────────────────────────────── */}
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
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {readerCanUseOriginalLayout && (
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => switchReaderLayoutMode(readerLayoutMode === 'original' ? 'custom' : 'original', { resetTypography: readerLayoutMode !== 'original' })}
                        >
                          {readerLayoutMode === 'original'
                            ? tr('Текстовый режим', 'Textmodus')
                            : tr('Оригинал', 'Original')}
                        </button>
                      )}
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setReaderSettingsOpen(false)}
                      >
                        ×
                      </button>
                    </div>
                  </div>
                  {readerCanUseOriginalLayout && (
                    <div className="webapp-muted" style={{ fontSize: 12 }}>
                      {readerLayoutMode === 'original'
                        ? tr(
                          'Сейчас открыт оригинальный режим: читалка показывает исходную EPUB/PDF-вёрстку книги. Любое изменение шрифта переключит книгу в адаптивный текстовый режим.',
                          'Aktuell ist der Originalmodus aktiv: Der Reader zeigt das urspruengliche EPUB/PDF-Layout des Buches. Jede Schriftanpassung schaltet in den adaptiven Textmodus um.'
                        )
                        : tr(
                          'Сейчас открыт адаптивный режим: текст пересчитан под ваш шрифт и экран. Кнопка "Оригинал" вернёт исходную вёрстку книги.',
                          'Aktuell ist der adaptive Modus aktiv: Der Text wurde fuer deine Schrift und deinen Bildschirm neu berechnet. Mit "Original" kehrst du zum urspruenglichen Buchlayout zurueck.'
                        )}
                    </div>
                  )}
                  {readerCanUseOriginalLayout && (
                    <label className="webapp-field">
                      <span>{tr('Режим отображения', 'Anzeigemodus')}</span>
                      <div className="reader-theme-seg reader-layout-seg">
                        {[
                          {
                            key: 'original',
                            label: tr('Original', 'Original'),
                            hint: tr('Исходная EPUB/PDF-вёрстка', 'Urspruengliches EPUB/PDF-Layout'),
                          },
                          {
                            key: 'custom',
                            label: tr('Textmodus', 'Textmodus'),
                            hint: tr('Нужен для word-tap и аудио', 'Fuer Wort-Tap und Audio'),
                          },
                        ].map((option) => (
                          <button
                            key={option.key}
                            type="button"
                            className={`reader-theme-seg-btn ${readerLayoutMode === option.key ? 'is-active' : ''}`}
                            onClick={() => switchReaderLayoutMode(option.key, { resetTypography: option.key === 'original' })}
                          >
                            <span className="reader-layout-seg-label">{option.label}</span>
                            <span className="reader-layout-seg-hint">{option.hint}</span>
                          </button>
                        ))}
                      </div>
                    </label>
                  )}

                  {/* Phase 2.2: Theme switcher */}
                  <label className="webapp-field">
                    <span>{tr('Тема страницы', 'Hintergrund')}</span>
                    <div className="reader-theme-seg">
                      {[
                        { k: 'dark',  l: tr('Тёмная', 'Dunkel') },
                        { k: 'sepia', l: tr('Сепия',  'Sepia')  },
                        { k: 'cream', l: tr('Бумага', 'Papier') },
                      ].map((opt) => (
                        <button
                          key={opt.k}
                          type="button"
                          className={`reader-theme-seg-btn ${readerColorTheme === opt.k ? 'is-active' : ''}`}
                          onClick={() => applyReaderColorTheme(opt.k)}
                        >
                          <span className={`reader-theme-swatch reader-theme-swatch-${opt.k}`} aria-hidden="true">Aa</span>
                          {opt.l}
                        </button>
                      ))}
                    </div>
                  </label>

                  <label className="webapp-field">
                    <span>{tr('Размер шрифта', 'Schriftgroesse')}</span>
                    <input
                      type="range"
                      min="14"
                      max="28"
                      step="1"
                      value={readerFontSize}
                      onChange={(event) => {
                        switchReaderLayoutMode('custom');
                        setReaderFontSize(Number(event.target.value));
                      }}
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
                      onChange={(event) => {
                        switchReaderLayoutMode('custom');
                        setReaderFontWeight(Number(event.target.value));
                      }}
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
  );
}
