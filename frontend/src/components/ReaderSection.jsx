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
    readerBookmarkPercent, setReaderBookmarkPercent, readerBookmarkPage,
    isCurrentReaderPageBookmarked,
    readerCanUseOriginalLayout,
    readerLayoutMode, setReaderLayoutMode,
    readerReadingMode, setReaderReadingMode,
    readerFontSize, setReaderFontSize,
    readerFontWeight, setReaderFontWeight,
    readerSwipeSensitivity, setReaderSwipeSensitivity,
    readerImmersive, setReaderImmersive,
    readerTopbarCollapsed, setReaderTopbarCollapsed,
    readerSettingsOpen, setReaderSettingsOpen,
    readerArchiveOpen, setReaderArchiveOpen,
    readerHasContent,

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
    playReaderAudioPage = () => {},
    pauseReaderAudioPlay = () => {},
    resumeReaderAudioPlay = () => {},
    stopReaderAudioPlay = () => {},
  } = props;

  const sectionClass = [
    'webapp-section',
    'webapp-reader',
    readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : '',
    readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : '',
  ].filter(Boolean).join(' ');

  return (
    <section
      className={sectionClass}
      data-reader-theme={readerColorTheme}
      ref={readerRef}
    >
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
          // ════════════════════════════════════════════════════════════════
          //  LIBRARY MODE
          // ════════════════════════════════════════════════════════════════
          return (
            <div className="reader-library-mode">

              {/* ── Library header ─────────────────────────────────── */}
              <div className="reader-lib-header">
                <h2 className="reader-lib-header-title">
                  {readerArchiveOpen ? tr('Архив', 'Archiv') : tr('Моя библиотека', 'Meine Bibliothek')}
                </h2>
                <div className="reader-lib-header-actions">
                  <button
                    type="button"
                    className="reader-lib-icon-btn"
                    onClick={() => loadReaderLibrary()}
                    title={tr('Обновить', 'Aktualisieren')}
                  >
                    <svg
                      width="16" height="16" viewBox="0 0 24 24" fill="none"
                      stroke="currentColor" strokeWidth="2.2"
                      strokeLinecap="round" strokeLinejoin="round"
                      className={readerLibraryLoading ? 'is-spinning' : ''}
                    >
                      <polyline points="23 4 23 10 17 10" />
                      <polyline points="1 20 1 14 7 14" />
                      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
                    </svg>
                  </button>
                  <button
                    type="button"
                    className={`reader-lib-add-btn${readerAddOpen ? ' is-open' : ''}`}
                    onClick={() => setReaderAddOpen((prev) => !prev)}
                  >
                    {readerAddOpen ? tr('✕ Скрыть', '✕ Schließen') : tr('+ Добавить', '+ Hinzufügen')}
                  </button>
                </div>
              </div>

              {/* ── Add form (URL / file / submit) ─────────────────── */}
              {readerAddOpen && (
                <div className="reader-add-form-wrap">
                  <form className="webapp-reader-form" onSubmit={handleReaderIngest}>
                    <label className="webapp-field">
                      <span>{tr('URL или текст', 'URL oder Text')}</span>
                      <textarea
                        rows={2}
                        value={readerInput}
                        onChange={(event) => setReaderInput(event.target.value)}
                        placeholder={tr(
                          'Вставьте URL статьи/книги (включая PDF) или сам текст.',
                          'Füge URL eines Artikels/Buchs (auch PDF) oder den Text selbst ein.'
                        )}
                      />
                    </label>
                    <label className="webapp-field">
                      <span>{tr('Файл с телефона', 'Datei vom Telefon')}</span>
                      <input
                        ref={readerFileInputRef}
                        type="file"
                        accept=".txt,.md,.pdf,.epub,text/plain,application/pdf,application/epub+zip"
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
                </div>
              )}

              {/* ── Phase 2.1: Hero-карточка «Продолжаешь читать» ──── */}
              {(() => {
                let candidate = null;
                if (readerDocumentId) {
                  candidate = readerDocuments.find(
                    (d) => Number(d?.id) === Number(readerDocumentId) && !d?.is_archived
                  );
                }
                if (!candidate) {
                  const inProgress = readerDocuments
                    .filter((d) =>
                      !d?.is_archived &&
                      Number(d?.progress_percent || 0) > 0 &&
                      Number(d?.progress_percent || 0) < 100
                    )
                    .sort((a, b) => Number(b?.progress_percent || 0) - Number(a?.progress_percent || 0));
                  candidate = inProgress[0] || null;
                }
                if (!candidate) return null;

                const progress = Math.max(0, Math.min(100, Number(candidate?.progress_percent || 0)));
                const coverUrl = getReaderCoverUrl(candidate);
                const initials = getReaderCoverInitials(candidate?.title);
                const gradient = getReaderCoverGradient(candidate);
                const meta = buildReaderArchiveMeta(candidate);
                const isOpening = Number(readerOpeningDocumentId) === Number(candidate.id);

                return (
                  <div
                    className="reader-hero-card"
                    onClick={() => openReaderDocument(candidate.id)}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(candidate.id)}
                  >
                    <div
                      className="reader-hero-cover"
                      style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
                    >
                      {coverUrl
                        ? <img src={coverUrl} alt="" loading="lazy" />
                        : <span className="reader-hero-cover-initials">{initials}</span>}
                    </div>
                    <div className="reader-hero-body">
                      <div className="reader-hero-kicker">
                        <span className="reader-hero-dot" aria-hidden="true" />
                        {tr('Продолжаешь читать', 'Du liest gerade')}
                      </div>
                      <div className="reader-hero-title">
                        {candidate.title || tr('Без названия', 'Ohne Titel')}
                      </div>
                      <div className="reader-hero-meta">
                        <span>{meta || ''}</span>
                        <span className="reader-hero-pct">{Math.round(progress)}%</span>
                      </div>
                      <div className="reader-hero-progress">
                        <div className="reader-hero-progress-fill" style={{ width: `${progress}%` }} />
                      </div>
                      <button
                        type="button"
                        className="reader-hero-continue"
                        onClick={(e) => { e.stopPropagation(); openReaderDocument(candidate.id); }}
                        disabled={isOpening}
                      >
                        {isOpening
                          ? tr('Открываем…', 'Oeffnen…')
                          : `▶  ${tr('Продолжить', 'Weiterlesen')}`}
                      </button>
                    </div>
                  </div>
                );
              })()}

              {/* ── Library section ────────────────────────────────── */}
              <section className="reader-library">
                <div className="reader-lib-controls">
                  <input
                    type="text"
                    className="reader-lib-search"
                    value={readerLibrarySearch}
                    onChange={(event) => setReaderLibrarySearch(event.target.value)}
                    placeholder={tr('Поиск по библиотеке…', 'Suche in Bibliothek…')}
                  />
                  <label className="reader-lib-archive-toggle">
                    <input
                      type="checkbox"
                      checked={readerIncludeArchived}
                      onChange={(event) => setReaderIncludeArchived(event.target.checked)}
                    />
                    <span>{tr('Архив', 'Archiv')}</span>
                  </label>
                </div>

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
                      const gradient = getReaderCoverGradient(item);
                      const meta = buildReaderArchiveMeta(item);
                      const isOpening = Number(readerOpeningDocumentId) === Number(item.id);
                      return (
                        <div
                          key={`reader-doc-${item.id}`}
                          className={`reader-library-card${Number(readerDocumentId) === Number(item.id) ? ' is-active' : ''}${isOpening ? ' is-opening' : ''}`}
                        >
                          <div
                            className="reader-library-cover"
                            style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
                            onClick={() => openReaderDocument(item.id)}
                            role="button"
                            tabIndex={0}
                            onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(item.id)}
                          >
                            {coverUrl ? (
                              <img src={coverUrl} alt="" loading="lazy" className="reader-archive-cover-img" />
                            ) : (
                              <span className="reader-archive-cover-fallback">{initials}</span>
                            )}
                            <div className="reader-library-cover-progress" style={{ width: `${progress}%` }} />
                            {isOpening && (
                              <div className="reader-library-cover-loading">
                                <svg className="reader-lib-spinner" viewBox="0 0 24 24" fill="none">
                                  <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                                </svg>
                              </div>
                            )}
                          </div>
                          <div
                            className="reader-library-card-body"
                            onClick={() => openReaderDocument(item.id)}
                            role="button"
                            tabIndex={-1}
                            style={{ cursor: 'pointer' }}
                          >
                            <div className="reader-library-title">{item.title || tr('Без названия', 'Ohne Titel')}</div>
                            <div className="reader-library-meta">
                              <span>{Math.round(progress)}%</span>
                              {meta && <span>{meta}</span>}
                            </div>
                          </div>
                          <div className="reader-library-actions">
                            <button
                              type="button"
                              className="reader-lib-action reader-lib-action-open"
                              onClick={() => openReaderDocument(item.id)}
                              disabled={isOpening}
                              title={tr('Открыть книгу', 'Buch oeffnen')}
                            >
                              {isOpening ? (
                                <svg className="reader-lib-spinner reader-lib-action-icon" viewBox="0 0 24 24" fill="none">
                                  <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                                </svg>
                              ) : (
                                <span className="reader-lib-action-icon" aria-hidden="true">
                                  <svg viewBox="0 0 18 18" fill="none">
                                    <path d="M7.5 4.5 12 9l-4.5 4.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                </span>
                              )}
                              <span className="reader-lib-action-label">
                                {isOpening ? tr('Загрузка…', 'Laden…') : tr('Открыть', 'Lesen')}
                              </span>
                            </button>
                            <button
                              type="button"
                              className="reader-lib-action"
                              onClick={(e) => { e.stopPropagation(); renameReaderDocument(item.id, item.title); }}
                              title={tr('Переименовать', 'Umbenennen')}
                            >
                              <span className="reader-lib-action-icon" aria-hidden="true">
                                <svg viewBox="0 0 18 18" fill="none">
                                  <path
                                    d="M12.9 3.6a1.5 1.5 0 0 1 2.12 2.12L7.2 13.5 4.5 14.1l.6-2.7 7.8-7.8Z"
                                    stroke="currentColor"
                                    strokeWidth="1.5"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                  />
                                  <path
                                    d="M11.4 5.1 13.5 7.2"
                                    stroke="currentColor"
                                    strokeWidth="1.5"
                                    strokeLinecap="round"
                                  />
                                </svg>
                              </span>
                              <span className="reader-lib-action-label">
                                {tr('Название', 'Titel')}
                              </span>
                            </button>
                            <button
                              type="button"
                              className="reader-lib-action"
                              onClick={() => archiveReaderDocument(item.id, !Boolean(item?.is_archived))}
                              title={Boolean(item?.is_archived) ? tr('Разархивировать', 'Wiederherstellen') : tr('В архив', 'Archivieren')}
                            >
                              <span className="reader-lib-action-icon" aria-hidden="true">
                                {Boolean(item?.is_archived) ? (
                                  <svg viewBox="0 0 18 18" fill="none">
                                    <path d="M14.25 6.75V3.75h-3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                    <path d="M13.9 8.25a5.25 5.25 0 1 1-1.1-3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                ) : (
                                  <svg viewBox="0 0 18 18" fill="none">
                                    <path d="M3.75 5.25h10.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                    <path d="M5.25 5.25 6 13.5h6l.75-8.25" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                    <path d="M7.5 8.25h3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                    <path d="M9 3.75v4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                  </svg>
                                )}
                              </span>
                              <span className="reader-lib-action-label">
                                {Boolean(item?.is_archived) ? tr('Вернуть', 'Zurueck') : tr('Скрыть', 'Ausblenden')}
                              </span>
                            </button>
                            <button
                              type="button"
                              className="reader-lib-action is-danger"
                              onClick={() => deleteReaderDocument(item.id)}
                              title={tr('Удалить', 'Loeschen')}
                            >
                              <span className="reader-lib-action-icon" aria-hidden="true">
                                <svg viewBox="0 0 18 18" fill="none">
                                  <path d="M5.25 5.25 12.75 12.75" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                  <path d="M12.75 5.25 5.25 12.75" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                </svg>
                              </span>
                              <span className="reader-lib-action-label">
                                {tr('Удалить', 'Loeschen')}
                              </span>
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>

              {/* ── Audio panel (offline whole-doc) ─────────────────── */}
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
                    if (readerDocumentId) {
                      syncReaderState({ bookmark_percent: Number(mark.toFixed(2)), progress_percent: Number(mark.toFixed(2)) });
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
                <div className="reader-topbar-peek-spacer" />
                <button
                  type="button"
                  className={`secondary-button reader-toolbar-btn reader-toolbar-btn-icon-only${readerAudioPlayActive ? ' is-active' : ''}`}
                  onClick={() => {
                    if (readerAudioPlayActive) {
                      stopReaderAudioPlay();
                    } else {
                      playReaderAudioPage(readerCurrentPage);
                    }
                  }}
                  disabled={!readerContent || readerAudioPlayLoading}
                  title={readerAudioPlayActive ? tr('Остановить аудио', 'Audio stoppen') : tr('Слушать страницу', 'Seite vorlesen')}
                  aria-label={readerAudioPlayActive ? tr('Остановить аудио', 'Audio stoppen') : tr('Слушать страницу', 'Seite vorlesen')}
                >
                  <span className="reader-toolbar-btn-icon" aria-hidden="true">
                    {readerAudioPlayLoading ? (
                      <svg className="reader-lib-spinner" viewBox="0 0 24 24" fill="none">
                        <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                      </svg>
                    ) : readerAudioPlayActive ? (
                      <svg viewBox="0 0 18 18" fill="none">
                        <rect x="4" y="4" width="3.5" height="10" rx="1" fill="currentColor" />
                        <rect x="10.5" y="4" width="3.5" height="10" rx="1" fill="currentColor" />
                      </svg>
                    ) : (
                      <svg viewBox="0 0 18 18" fill="none">
                        <path d="M4 3.5a1 1 0 0 1 1.5-.87l9 5.18a1 1 0 0 1 0 1.74l-9 5.18A1 1 0 0 1 4 13.82V3.5Z" fill="currentColor" />
                      </svg>
                    )}
                  </span>
                </button>
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

            {/* ── Article ─────────────────────────────────────────── */}
            {readerContent && (
              <article
                ref={readerArticleRef}
                className={`reader-article ${readerReadingMode === 'horizontal' ? 'is-horizontal' : 'is-vertical'} ${readerPageCount > 0 ? 'has-pages' : ''}`}
                onClick={handleReaderStructuredClick}
                onMouseUp={handleReaderArticleMouseUp}
                onWheel={handleReaderPageWheel}
                onTouchStart={handleReaderPageTouchStart}
                onTouchMove={handleReaderArticleTouchMove}
                onTouchEnd={handleReaderArticleTouchEnd}
                onTouchCancel={handleReaderArticleTouchCancel}
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
                      <div ref={readerPageInnerRef} className="reader-page-sheet-inner">
                        {Array.isArray(readerPages) && readerPages[readerCurrentPage - 1] === null
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

            {/* ── Audio hint — always rendered (fixed height) to prevent layout shift ── */}
            <div className={`reader-audio-hint${(readerAudioPlayActive || readerAudioStartWid) ? ' is-active' : ''}`}>
              {readerAudioStartWid
                ? tr('▶ начнёт со слова, которое ты выбрал', '▶ startet beim gewählten Wort')
                : tr('тапни слово в тексте — ▶ заиграет с него', 'tippe ein Wort an — ▶ startet von dort')}
            </div>

            {/* ── Scrubber bar ────────────────────────────────────── */}
            {readerPageCount > 0 && (
              <div className="reader-scrubber-bar">
                <button
                  type="button"
                  className="reader-scrubber-page-btn"
                  onClick={() => {
                    setReaderPageJumpInput(String(readerCurrentPage));
                    setReaderShowPageJump(true);
                  }}
                  title={tr('Перейти к странице', 'Zur Seite springen')}
                >
                  {readerCurrentPage} / {readerPageCount}
                </button>
                <div className="reader-scrubber-track-wrap">
                  <input
                    type="range"
                    className="reader-scrubber-input"
                    min={1}
                    max={readerPageCount}
                    value={readerCurrentPage}
                    onChange={(e) => {
                      const page = Math.max(1, Math.min(readerPageCount, Number(e.target.value)));
                      setReaderCurrentPage(page);
                    }}
                  />
                </div>
                {readerBookmarkPercent > 0 && !isCurrentReaderPageBookmarked ? (
                  <button
                    type="button"
                    className="reader-scrubber-bookmark-btn"
                    onClick={() => setReaderCurrentPage(readerBookmarkPage)}
                    title={tr('Перейти к закладке', 'Zur Lesezeiche springen')}
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
            <audio ref={audioElementRef} preload="metadata" style={{ display: 'none' }} />

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
              <div className="reader-audio-error-bar">{readerAudioPlayError}</div>
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
                          className={`reader-toc-item ${item.page_number === readerCurrentPage ? 'is-active' : ''}`}
                          onClick={() => {
                            setReaderCurrentPage(item.page_number);
                            setReaderShowToc(false);
                          }}
                        >
                          <span className="reader-toc-item-title">{item.title}</span>
                          <span className="reader-toc-item-page webapp-muted">{item.page_number}</span>
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
                          onClick={() => {
                            setReaderFontSize(READER_DEFAULT_FONT_SIZE);
                            setReaderFontWeight(READER_DEFAULT_FONT_WEIGHT);
                            setReaderLayoutMode('original');
                          }}
                        >
                          {tr('Оригинал', 'Original')}
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
                          'Сейчас открыт оригинальный режим: исходная разбивка страниц книги. Любое изменение шрифта переключит книгу в адаптивный режим.',
                          'Aktuell ist der Originalmodus aktiv: die urspruengliche Seitenteilung des Buches. Jede Schriftanpassung schaltet in den adaptiven Modus um.'
                        )
                        : tr(
                          'Сейчас открыт адаптивный режим: страницы пересчитаны под ваш шрифт и экран. Кнопка "Оригинал" вернёт исходную разбивку.',
                          'Aktuell ist der adaptive Modus aktiv: die Seiten wurden fuer deine Schrift und deinen Bildschirm neu berechnet. Mit "Original" kehrst du zur urspruenglichen Seitenteilung zurueck.'
                        )}
                    </div>
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
                        setReaderLayoutMode('custom');
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
                        setReaderLayoutMode('custom');
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
