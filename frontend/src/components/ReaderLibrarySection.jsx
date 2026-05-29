import React from 'react';
import { useReaderLibraryContext } from '../providers/ReaderLibraryProvider.jsx';

export default function ReaderLibrarySection() {
  const {
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
    readerArchiveOpen,
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
  } = useReaderLibraryContext();

  const searchRaw = String(readerLibrarySearch || '').trim().toLowerCase();
  const visibleLibraryItems = readerDocuments.filter((item) => {
    const isArchived = Boolean(item?.is_archived);
    if (!readerIncludeArchived && isArchived) return false;
    if (!searchRaw) return true;
    const haystack = `${item?.title || ''} ${item?.source_type || ''} ${item?.target_lang || ''}`.toLowerCase();
    return haystack.includes(searchRaw);
  });

  let continueCandidate = null;
  if (readerDocumentId) {
    continueCandidate = readerDocuments.find(
      (d) => Number(d?.id) === Number(readerDocumentId) && !d?.is_archived
    );
  }
  if (!continueCandidate) {
    const notFinished = readerDocuments
      .filter((d) => !d?.is_archived && Number(d?.progress_percent || 0) < 100)
      .sort((a, b) => {
        const ta = new Date(a?.last_opened_at || a?.updated_at || a?.created_at || 0).getTime();
        const tb = new Date(b?.last_opened_at || b?.updated_at || b?.created_at || 0).getTime();
        return tb - ta;
      });
    continueCandidate = notFinished[0] || null;
  }

  const renderCover = (item, className, fallbackClassName) => {
    const coverUrl = getReaderCoverUrl(item);
    const initials = getReaderCoverInitials(item?.title);
    return coverUrl
      ? <img src={coverUrl} alt="" loading="lazy" className={className || undefined} />
      : <span className={fallbackClassName}>{initials}</span>;
  };

  return (
    <div className="reader-library-mode">
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

      {continueCandidate && (() => {
        const progress = Math.max(0, Math.min(100, Number(continueCandidate?.progress_percent || 0)));
        const gradient = getReaderCoverGradient(continueCandidate);
        const meta = buildReaderArchiveMeta(continueCandidate);
        const isOpening = Number(readerOpeningDocumentId) === Number(continueCandidate.id);
        return (
          <div
            className="reader-hero-card"
            onClick={() => openReaderDocument(continueCandidate.id)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(continueCandidate.id)}
          >
            <div
              className="reader-hero-cover"
              style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
            >
              {renderCover(continueCandidate, '', 'reader-hero-cover-initials')}
            </div>
            <div className="reader-hero-body">
              <div className="reader-hero-kicker">
                <span className="reader-hero-dot" aria-hidden="true" />
                {tr('Продолжаешь читать', 'Du liest gerade')}
              </div>
              <div className="reader-hero-title">
                {continueCandidate.title || tr('Без названия', 'Ohne Titel')}
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
                onClick={(e) => { e.stopPropagation(); openReaderDocument(continueCandidate.id); }}
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
              const gradient = getReaderCoverGradient(item);
              const meta = buildReaderArchiveMeta(item);
              const isOpening = Number(readerOpeningDocumentId) === Number(item.id);
              return (
                <div
                  key={`reader-doc-${item.id}`}
                  className={`reader-library-card${Number(readerDocumentId) === Number(item.id) ? ' is-active' : ''}${isOpening ? ' is-opening' : ''}`}
                  onClick={() => openReaderDocument(item.id)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(item.id)}
                >
                  <div
                    className="reader-library-cover"
                    style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
                  >
                    {renderCover(item, 'reader-archive-cover-img', 'reader-archive-cover-fallback')}
                    <div className="reader-library-cover-progress" style={{ width: `${progress}%` }} />
                    {isOpening && (
                      <div className="reader-library-cover-loading">
                        <svg className="reader-lib-spinner" viewBox="0 0 24 24" fill="none">
                          <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                        </svg>
                      </div>
                    )}
                  </div>
                  <div className="reader-library-card-body" style={{ cursor: 'pointer' }}>
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
                      onClick={(e) => { e.stopPropagation(); openReaderDocument(item.id); }}
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
                          <path d="M11.4 5.1 13.5 7.2" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                        </svg>
                      </span>
                      <span className="reader-lib-action-label">
                        {tr('Название', 'Titel')}
                      </span>
                    </button>
                    <button
                      type="button"
                      className="reader-lib-action"
                      onClick={(e) => { e.stopPropagation(); archiveReaderDocument(item.id, !Boolean(item?.is_archived)); }}
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
                      onClick={(e) => { e.stopPropagation(); deleteReaderDocument(item.id); }}
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

      {readerDocumentId && (
        <section className="reader-audio-panel">
          <div className="reader-audio-head">
            <strong>{tr('Оффлайн-аудио документа', 'Offline-Audio des Dokuments')}</strong>
          </div>
          {readerAudioPremiumLocked && (
            <div className="webapp-muted">{readerAudioPremiumHint}</div>
          )}
          <div className="reader-audio-actions">
            <button
              type="button"
              className="secondary-button"
              onClick={() => (readerAudioPremiumLocked ? onReaderAudioUpgrade() : downloadReaderAudio(true))}
              disabled={readerAudioLoading || billingActionLoading}
            >
              {readerAudioPremiumLocked
                ? tr('Открыть Premium', 'Premium öffnen')
                : (readerAudioLoading ? tr('Готовим...', 'Erstellen...') : tr('Скачать весь документ', 'Ganzes Dokument herunterladen'))}
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
