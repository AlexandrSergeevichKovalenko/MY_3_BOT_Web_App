import React, { createContext, useContext, useEffect, useMemo, useState, useCallback } from 'react';

export const ReaderDocumentContext = createContext(null);

export const READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS = {
  provider_name: 'reader_document',
  state_removed: 35,
  memo_removed: 9,
  callback_removed: 1,
};

function requireReaderDocumentInput(name, value) {
  if (value === undefined || value === null) {
    throw new Error(`ReaderDocumentProvider requires ${name}`);
  }
  return value;
}

function validateReaderDocumentContext(value) {
  if (!value || typeof value !== 'object') {
    throw new Error('ReaderDocumentProvider requires value');
  }
  for (const key of [
    'readerContent',
    'readerTitle',
    'readerDocumentId',
    'readerPages',
    'readerDisplayPages',
    'readerPageCount',
    'readerVisibleText',
    'readerSentencesModel',
    'setReaderContent',
    'setReaderCurrentPage',
  ]) {
    if (value[key] === undefined) {
      throw new Error(`ReaderDocumentProvider requires ${key}`);
    }
  }
  return value;
}

export function useReaderDocumentController({
  defaultFontSize,
  defaultFontWeight,
  normalizeLangCode,
  normalizeReaderPaginationText,
  normalizeReaderVisiblePageText,
  normalizeReaderEpubHref,
  segmentText,
}) {
  const normalizeLanguageCode = requireReaderDocumentInput('normalizeLangCode', normalizeLangCode);
  const normalizePaginationText = requireReaderDocumentInput('normalizeReaderPaginationText', normalizeReaderPaginationText);
  const normalizeVisibleText = requireReaderDocumentInput('normalizeReaderVisiblePageText', normalizeReaderVisiblePageText);
  const normalizeEpubHref = requireReaderDocumentInput('normalizeReaderEpubHref', normalizeReaderEpubHref);
  const segmentReaderText = requireReaderDocumentInput('segmentText', segmentText);

  const [readerLoading, setReaderLoading] = useState(false);
  const [readerOpeningDocumentId, setReaderOpeningDocumentId] = useState(0);
  const [readerError, setReaderError] = useState('');
  const [readerErrorCode, setReaderErrorCode] = useState('');
  const [readerContent, setReaderContent] = useState('');
  const [readerTitle, setReaderTitle] = useState('');
  const [readerSourceType, setReaderSourceType] = useState('');
  const [readerSourceUrl, setReaderSourceUrl] = useState('');
  const [readerDetectedLanguage, setReaderDetectedLanguage] = useState('');
  const [readerDocumentId, setReaderDocumentId] = useState(null);
  const [readerPages, setReaderPages] = useState([]);
  const [readerDynamicPages, setReaderDynamicPages] = useState([]);
  const [readerCurrentPage, setReaderCurrentPage] = useState(1);
  const [readerOriginalEpubLoading, setReaderOriginalEpubLoading] = useState(false);
  const [readerOriginalEpubError, setReaderOriginalEpubError] = useState('');
  const [readerOriginalTocHref, setReaderOriginalTocHref] = useState('');
  const [readerOriginalTocTitle, setReaderOriginalTocTitle] = useState('');
  const [readerOriginalCoverUrl, setReaderOriginalCoverUrl] = useState('');
  const [readerOriginalCoverVisible, setReaderOriginalCoverVisible] = useState(false);
  const [readerShowToc, setReaderShowToc] = useState(false);
  const [readerTocItems, setReaderTocItems] = useState([]);
  const [readerShowPageJump, setReaderShowPageJump] = useState(false);
  const [readerPageJumpInput, setReaderPageJumpInput] = useState('');
  const [readerProgressPercent, setReaderProgressPercent] = useState(0);
  const [readerBookmarkPercent, setReaderBookmarkPercent] = useState(0);
  const [readerReadingMode, setReaderReadingMode] = useState('vertical');
  const [readerSwipeSensitivity, setReaderSwipeSensitivity] = useState('medium');
  const [readerImmersive, setReaderImmersive] = useState(false);
  const [readerSettingsOpen, setReaderSettingsOpen] = useState(false);
  const [readerTopbarCollapsed, setReaderTopbarCollapsed] = useState(false);
  const [readerPaginationLayoutTick, setReaderPaginationLayoutTick] = useState(0);
  const [readerFontSize, setReaderFontSize] = useState(defaultFontSize);
  const [readerFontWeight, setReaderFontWeight] = useState(defaultFontWeight);
  const [readerColorTheme, setReaderColorTheme] = useState(() => {
    try {
      const value = localStorage.getItem('reader_color_theme');
      return (value === 'sepia' || value === 'cream') ? value : 'dark';
    } catch (_error) {
      return 'dark';
    }
  });
  const [readerLayoutMode, setReaderLayoutMode] = useState('custom');

  useEffect(() => {
    console.info('reader_document_state_initialized', {
      provider_name: 'reader_document',
      state_count: READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS.state_removed,
      memo_count: READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS.memo_removed,
    });
  }, []);

  const readerHasContent = Boolean(String(readerContent || '').trim());
  const readerCanonicalText = useMemo(
    () => normalizePaginationText(readerContent),
    [normalizePaginationText, readerContent]
  );
  const readerUsesOriginalEpubLayout = readerSourceType === 'epub' && readerLayoutMode === 'original';
  const readerCanUseOriginalLayout = readerSourceType === 'epub'
    || (readerSourceType === 'pdf' && Array.isArray(readerPages) && readerPages.length > 0);
  const readerUsesCustomLayout = !readerCanUseOriginalLayout || readerLayoutMode === 'custom';
  const readerDisplayPages = useMemo(() => {
    if (readerUsesOriginalEpubLayout) {
      return [];
    }
    if (readerUsesCustomLayout && Array.isArray(readerDynamicPages) && readerDynamicPages.length > 0) {
      return readerDynamicPages.map((item, index) => ({
        page_number: Number(item?.page_number || index + 1),
        text: String(item?.text || '').trim(),
      }));
    }
    if (Array.isArray(readerPages) && readerPages.length > 0) {
      return readerPages.map((item, index) => ({
        page_number: Number(item?.page_number || index + 1),
        text: item ? String(item?.text || '').trim() : '',
      }));
    }
    if (!readerCanonicalText) return [];
    return [{ page_number: 1, text: readerCanonicalText }];
  }, [readerCanonicalText, readerDynamicPages, readerPages, readerUsesCustomLayout, readerUsesOriginalEpubLayout]);
  const getReaderDisplayPageText = useCallback((page) => {
    const pageIndex = Math.max(0, Number(page || 1) - 1);
    return normalizeVisibleText(String(readerDisplayPages[pageIndex]?.text || ''));
  }, [normalizeVisibleText, readerDisplayPages]);
  const readerPageCount = readerDisplayPages.length;
  const readerVisibleText = useMemo(() => {
    if (readerUsesOriginalEpubLayout) {
      return '';
    }
    if (readerPageCount > 0) {
      return getReaderDisplayPageText(readerCurrentPage);
    }
    return normalizeVisibleText(String(readerContent || ''));
  }, [getReaderDisplayPageText, normalizeVisibleText, readerPageCount, readerCurrentPage, readerContent, readerUsesOriginalEpubLayout]);
  const readerResolvedOriginalTocTitle = useMemo(() => {
    const currentHref = normalizeEpubHref(readerOriginalTocHref);
    if (!currentHref) return String(readerOriginalTocTitle || '').trim();
    const matched = (Array.isArray(readerTocItems) ? readerTocItems : []).find((item) => (
      normalizeEpubHref(item?.href || item?.cfi || '') === currentHref
    ));
    return String(matched?.title || readerOriginalTocTitle || '').trim();
  }, [normalizeEpubHref, readerOriginalTocHref, readerOriginalTocTitle, readerTocItems]);
  const readerSegmentationHash = useMemo(() => {
    const value = String(readerVisibleText || '');
    let hash = 0;
    for (let index = 0; index < value.length; index += 1) {
      hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
    }
    return `${String(readerDocumentId || 'no-doc')}:${value.length}:${hash}`;
  }, [readerVisibleText, readerDocumentId]);
  const readerSegmentationLang = useMemo(
    () => normalizeLanguageCode(readerDetectedLanguage || '') || 'de',
    [normalizeLanguageCode, readerDetectedLanguage]
  );
  const readerSentencesModel = useMemo(
    () => segmentReaderText(readerVisibleText, readerSegmentationLang),
    [segmentReaderText, readerVisibleText, readerSegmentationLang, readerSegmentationHash]
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
  const readerBookmarkPage = readerPageCount > 0
    ? Math.max(1, Math.min(readerPageCount, Math.round((Math.max(0, Math.min(100, Number(readerBookmarkPercent || 0))) / 100) * readerPageCount) || 1))
    : 0;
  const isCurrentReaderPageBookmarked = readerPageCount > 0
    && readerBookmarkPage === Math.max(1, Math.min(readerPageCount, Number(readerCurrentPage || 1)));

  return {
    readerLoading, setReaderLoading,
    readerOpeningDocumentId, setReaderOpeningDocumentId,
    readerError, setReaderError,
    readerErrorCode, setReaderErrorCode,
    readerContent, setReaderContent,
    readerTitle, setReaderTitle,
    readerSourceType, setReaderSourceType,
    readerSourceUrl, setReaderSourceUrl,
    readerDetectedLanguage, setReaderDetectedLanguage,
    readerDocumentId, setReaderDocumentId,
    readerPages, setReaderPages,
    readerDynamicPages, setReaderDynamicPages,
    readerCurrentPage, setReaderCurrentPage,
    readerOriginalEpubLoading, setReaderOriginalEpubLoading,
    readerOriginalEpubError, setReaderOriginalEpubError,
    readerOriginalTocHref, setReaderOriginalTocHref,
    readerOriginalTocTitle, setReaderOriginalTocTitle,
    readerOriginalCoverUrl, setReaderOriginalCoverUrl,
    readerOriginalCoverVisible, setReaderOriginalCoverVisible,
    readerShowToc, setReaderShowToc,
    readerTocItems, setReaderTocItems,
    readerShowPageJump, setReaderShowPageJump,
    readerPageJumpInput, setReaderPageJumpInput,
    readerProgressPercent, setReaderProgressPercent,
    readerBookmarkPercent, setReaderBookmarkPercent,
    readerReadingMode, setReaderReadingMode,
    readerSwipeSensitivity, setReaderSwipeSensitivity,
    readerImmersive, setReaderImmersive,
    readerSettingsOpen, setReaderSettingsOpen,
    readerTopbarCollapsed, setReaderTopbarCollapsed,
    readerPaginationLayoutTick, setReaderPaginationLayoutTick,
    readerFontSize, setReaderFontSize,
    readerFontWeight, setReaderFontWeight,
    readerColorTheme, setReaderColorTheme,
    readerLayoutMode, setReaderLayoutMode,
    readerHasContent,
    readerCanonicalText,
    readerUsesOriginalEpubLayout,
    readerCanUseOriginalLayout,
    readerUsesCustomLayout,
    readerDisplayPages,
    getReaderDisplayPageText,
    readerPageCount,
    readerVisibleText,
    readerResolvedOriginalTocTitle,
    readerSegmentationHash,
    readerSegmentationLang,
    readerSentencesModel,
    readerSentenceMap,
    readerWordMap,
    readerBookmarkPage,
    isCurrentReaderPageBookmarked,
  };
}

export function ReaderDocumentProvider({ value, children }) {
  const contextValue = validateReaderDocumentContext(value);
  useEffect(() => {
    console.info('reader_document_provider_mount', {
      provider_name: 'reader_document',
      document_id_present: Boolean(contextValue.readerDocumentId),
    });
    console.info('frontend_provider_extracted', READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS);
    return () => {
      console.info('reader_document_provider_unmount', {
        provider_name: 'reader_document',
      });
    };
  }, []);

  return (
    <ReaderDocumentContext.Provider value={contextValue}>
      {children}
    </ReaderDocumentContext.Provider>
  );
}

export function useReaderDocumentContext() {
  const context = useContext(ReaderDocumentContext);
  if (!context) {
    throw new Error('ReaderDocumentContext is required');
  }
  return context;
}

export default ReaderDocumentProvider;
