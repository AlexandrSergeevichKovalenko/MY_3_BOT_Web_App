import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import ReaderLibrarySection from '../components/ReaderLibrarySection.jsx';
import {
  ReaderLibraryProvider,
  READER_LIBRARY_PROVIDER_EXTRACTION_METRICS,
  useReaderLibraryContext,
} from '../providers/ReaderLibraryProvider.jsx';

function libraryValue(overrides = {}) {
  return {
    tr: (ru) => ru,
    handleBillingUpgrade: vi.fn(),
    billingActionLoading: false,
    readerFileInputRef: { current: null },
    readerDocuments: [
      {
        id: 7,
        title: 'Das Herz von Dresden',
        source_type: 'pdf',
        target_lang: 'de',
        progress_percent: 12,
        updated_at: '2026-05-29T12:00:00Z',
      },
    ],
    readerLibrarySearch: '',
    setReaderLibrarySearch: vi.fn(),
    readerIncludeArchived: false,
    setReaderIncludeArchived: vi.fn(),
    readerLibraryLoading: false,
    readerLibraryError: '',
    loadReaderLibrary: vi.fn(),
    readerArchiveOpen: false,
    readerAddOpen: false,
    setReaderAddOpen: vi.fn(),
    readerOpeningDocumentId: 0,
    openReaderDocument: vi.fn(),
    renameReaderDocument: vi.fn(),
    archiveReaderDocument: vi.fn(),
    deleteReaderDocument: vi.fn(),
    readerInput: '',
    setReaderInput: vi.fn(),
    readerSelectedFile: null,
    handleReaderFileSelect: vi.fn(),
    handleReaderIngest: vi.fn(),
    readerLoading: false,
    readerError: '',
    readerErrorCode: '',
    readerDocumentId: null,
    readerAudioLoading: false,
    readerAudioError: '',
    readerAudioPreviewUrl: '',
    readerAudioPreviewName: '',
    downloadReaderAudio: vi.fn(),
    closeReaderAudioPreview: vi.fn(),
    readerAudioPremiumLocked: false,
    readerAudioPremiumHint: 'audio premium',
    onReaderAudioUpgrade: vi.fn(),
    getReaderCoverUrl: () => '',
    getReaderCoverInitials: (title) => String(title || 'BK').slice(0, 2).toUpperCase(),
    getReaderCoverGradient: () => ['#1d4ed8', '#0f172a'],
    buildReaderArchiveMeta: (item) => String(item?.source_type || '').toUpperCase(),
    ...overrides,
  };
}

function ContextProbe() {
  const context = useReaderLibraryContext();
  return React.createElement('span', null, [
    String(context.readerDocuments.length),
    String(context.readerLibraryLoading),
    String(typeof context.openReaderDocument),
    String(typeof context.renameReaderDocument),
  ].join('|'));
}

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('ReaderLibraryProvider', () => {
  it('mounts and exposes explicit library context', () => {
    const html = renderToStaticMarkup(React.createElement(
      ReaderLibraryProvider,
      { value: libraryValue() },
      React.createElement(ContextProbe),
    ));
    expect(html).toContain('1|false|function|function');
  });

  it('fails explicitly when required context values are missing', () => {
    const value = libraryValue();
    delete value.loadReaderLibrary;
    expect(() => renderToStaticMarkup(React.createElement(
      ReaderLibraryProvider,
      { value },
      React.createElement(ContextProbe),
    ))).toThrow('ReaderLibraryProvider requires loadReaderLibrary');
  });

  it('renders library search and document cards through ReaderLibrarySection', () => {
    const html = renderToStaticMarkup(React.createElement(
      ReaderLibraryProvider,
      { value: libraryValue() },
      React.createElement(ReaderLibrarySection),
    ));
    expect(html).toContain('Моя библиотека');
    expect(html).toContain('Das Herz von Dresden');
    expect(html).toContain('reader-library-grid');
  });

  it('documents extraction metrics', () => {
    expect(READER_LIBRARY_PROVIDER_EXTRACTION_METRICS).toEqual(expect.objectContaining({
      provider_name: 'reader_library',
      rendering_lines_removed: 381,
    }));
  });

  it('ReaderSection delegates library rendering to ReaderLibrarySection', () => {
    const readerSectionSource = fs.readFileSync(path.resolve(process.cwd(), 'src/components/ReaderSection.jsx'), 'utf8');
    expect(readerSectionSource).toContain('<ReaderLibrarySection />');
    expect(readerSectionSource).toContain('<ReaderLibraryProvider');
    expect(readerSectionSource).not.toContain('className="reader-library-grid"');
    expect(readerSectionSource).not.toContain('readerDocuments.filter((item)');
  });

  it('App.jsx keeps document rendering, audio, and timer ownership unchanged for this slice', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).toContain('async function openReaderDocument(documentId)');
    expect(appSource).toContain('const [readerAudioPlayActive, setReaderAudioPlayActive] = useState');
    expect(appSource).toContain('const [readerLiveSeconds, setReaderLiveSeconds] = useState');
  });

  // ── Regression: readerArchiveOpen must be forwarded into provider value ──
  // Commit c3223141 introduced ReaderLibraryProvider but omitted readerArchiveOpen
  // from the value object, causing validateReaderLibraryValue() to throw on every
  // library open.  This test pins the fix so it can never silently regress.
  it('ReaderSection.jsx value object includes readerArchiveOpen (regression guard)', () => {
    const src = fs.readFileSync(path.resolve(process.cwd(), 'src/components/ReaderSection.jsx'), 'utf8');
    // The value object passed to <ReaderLibraryProvider value={{...}}> must include readerArchiveOpen.
    // A simple substring check is intentionally broad — it would catch any form of the assignment.
    expect(src).toContain('readerArchiveOpen,');
  });

  it('renders archive title when readerArchiveOpen is true', () => {
    const html = renderToStaticMarkup(React.createElement(
      ReaderLibraryProvider,
      { value: libraryValue({ readerArchiveOpen: true }) },
      React.createElement(ReaderLibrarySection),
    ));
    expect(html).toContain('Архив');
    expect(html).not.toContain('Моя библиотека');
  });

  it('renders library title when readerArchiveOpen is false', () => {
    const html = renderToStaticMarkup(React.createElement(
      ReaderLibraryProvider,
      { value: libraryValue({ readerArchiveOpen: false }) },
      React.createElement(ReaderLibrarySection),
    ));
    expect(html).toContain('Моя библиотека');
  });
});
