import React, { createContext, useContext, useEffect, useMemo } from 'react';

export const ReaderLibraryContext = createContext(null);

export const READER_LIBRARY_PROVIDER_EXTRACTION_METRICS = {
  provider_name: 'reader_library',
  state_removed: 0,
  refs_removed: 0,
  effects_removed: 0,
  callbacks_removed: 0,
  rendering_lines_removed: 381,
};

const REQUIRED_READER_LIBRARY_KEYS = [
  'tr',
  'handleBillingUpgrade',
  'billingActionLoading',
  'readerFileInputRef',
  'readerDocuments',
  'readerLibrarySearch',
  'setReaderLibrarySearch',
  'readerIncludeArchived',
  'setReaderIncludeArchived',
  'readerLibraryLoading',
  'readerLibraryError',
  'loadReaderLibrary',
  'readerArchiveOpen',
  'readerDocumentId',
  'readerOpeningDocumentId',
  'openReaderDocument',
  'renameReaderDocument',
  'archiveReaderDocument',
  'deleteReaderDocument',
  'readerInput',
  'setReaderInput',
  'readerSelectedFile',
  'handleReaderFileSelect',
  'handleReaderIngest',
  'readerLoading',
  'readerError',
  'readerErrorCode',
  'readerAudioLoading',
  'readerAudioError',
  'readerAudioPreviewUrl',
  'readerAudioPreviewName',
  'downloadReaderAudio',
  'closeReaderAudioPreview',
  'readerAudioPremiumLocked',
  'readerAudioPremiumHint',
  'onReaderAudioUpgrade',
  'getReaderCoverUrl',
  'getReaderCoverInitials',
  'getReaderCoverGradient',
  'buildReaderArchiveMeta',
];

function validateReaderLibraryValue(value) {
  if (!value || typeof value !== 'object') {
    throw new Error('ReaderLibraryProvider requires value');
  }
  for (const key of REQUIRED_READER_LIBRARY_KEYS) {
    if (value[key] === undefined) {
      throw new Error(`ReaderLibraryProvider requires ${key}`);
    }
  }
  if (!Array.isArray(value.readerDocuments)) {
    throw new Error('ReaderLibraryProvider requires readerDocuments array');
  }
  return value;
}

export function ReaderLibraryProvider({ value, children }) {
  const validatedValue = validateReaderLibraryValue(value);
  const contextValue = useMemo(() => validatedValue, [validatedValue]);

  useEffect(() => {
    console.info('reader_library_provider_mount', {
      provider_name: 'reader_library',
      document_count: validatedValue.readerDocuments.length,
      include_archived: Boolean(validatedValue.readerIncludeArchived),
    });
    console.info('frontend_provider_extracted', READER_LIBRARY_PROVIDER_EXTRACTION_METRICS);
    return () => {
      console.info('reader_library_provider_unmount', {
        provider_name: 'reader_library',
      });
    };
  }, []);

  return (
    <ReaderLibraryContext.Provider value={contextValue}>
      {children}
    </ReaderLibraryContext.Provider>
  );
}

export function useReaderLibraryContext() {
  const context = useContext(ReaderLibraryContext);
  if (!context) {
    throw new Error('ReaderLibraryContext is required');
  }
  return context;
}

export default ReaderLibraryProvider;
