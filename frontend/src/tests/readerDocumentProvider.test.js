import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  ReaderDocumentProvider,
  READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS,
  useReaderDocumentContext,
  useReaderDocumentController,
} from '../providers/ReaderDocumentProvider.jsx';

const normalizePaginationText = (value) => String(value || '').trim();
const normalizeVisibleText = (value) => String(value || '').trim();
const normalizeEpubHref = (value) => String(value || '').trim().toLowerCase();
const normalizeLangCode = (value) => String(value || '').trim().toLowerCase();
const segmentText = (value) => String(value || '').trim()
  ? [{ sid: 's1', tokens: [{ kind: 'word', wid: 'w1', value: String(value).trim(), start: 0 }] }]
  : [];

function ContextProbe() {
  const context = useReaderDocumentContext();
  return React.createElement('span', null, [
    String(context.readerDocumentId ?? 'none'),
    String(context.readerPageCount),
    String(context.readerSentencesModel.length),
    String(typeof context.setReaderContent),
  ].join('|'));
}

function ControllerProbe() {
  const controller = useReaderDocumentController({
    defaultFontSize: 18,
    defaultFontWeight: 500,
    normalizeLangCode,
    normalizeReaderPaginationText: normalizePaginationText,
    normalizeReaderVisiblePageText: normalizeVisibleText,
    normalizeReaderEpubHref: normalizeEpubHref,
    segmentText,
  });
  return React.createElement(
    ReaderDocumentProvider,
    { value: controller },
    React.createElement(ContextProbe),
  );
}

function providerValue(overrides = {}) {
  return {
    readerContent: 'Hallo',
    readerTitle: 'Buch',
    readerDocumentId: 10,
    readerPages: [{ page_number: 1, text: 'Hallo' }],
    readerDisplayPages: [{ page_number: 1, text: 'Hallo' }],
    readerPageCount: 1,
    readerVisibleText: 'Hallo',
    readerSentencesModel: [{ sid: 's1', tokens: [] }],
    setReaderContent: () => {},
    setReaderCurrentPage: () => {},
    ...overrides,
  };
}

describe('ReaderDocumentProvider', () => {
  it('mounts and exposes explicit document context', () => {
    const html = renderToStaticMarkup(React.createElement(
      ReaderDocumentProvider,
      { value: providerValue() },
      React.createElement(ContextProbe),
    ));
    expect(html).toContain('10|1|1|function');
  });

  it('fails explicitly when required context is missing', () => {
    const value = providerValue();
    delete value.readerDisplayPages;
    expect(() => renderToStaticMarkup(React.createElement(
      ReaderDocumentProvider,
      { value },
      React.createElement(ContextProbe),
    ))).toThrow('ReaderDocumentProvider requires readerDisplayPages');
  });

  it('controller owns document state and memo surface', () => {
    const html = renderToStaticMarkup(React.createElement(ControllerProbe));
    expect(html).toContain('none|0|0|function');
  });

  it('documents extraction metrics', () => {
    expect(READER_DOCUMENT_PROVIDER_EXTRACTION_METRICS).toEqual(expect.objectContaining({
      provider_name: 'reader_document',
      state_removed: 35,
      memo_removed: 9,
    }));
  });

  it('App.jsx no longer owns moved reader document state or memos directly', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).not.toContain('const [readerContent, setReaderContent] = useState');
    expect(appSource).not.toContain('const [readerPages, setReaderPages] = useState');
    expect(appSource).not.toContain('const readerDisplayPages = useMemo');
    expect(appSource).not.toContain('const readerSentencesModel = useMemo');
    expect(appSource).toContain('const readerDocument = useReaderDocumentController');
  });

  it('keeps openReaderDocument App-owned and wraps ReaderSection with provider', () => {
    const appSource = fs.readFileSync(path.resolve(process.cwd(), 'src/App.jsx'), 'utf8');
    expect(appSource).toContain('async function openReaderDocument(documentId)');
    expect(appSource).toContain('<ReaderDocumentProvider value={readerDocument}>');
  });
});
