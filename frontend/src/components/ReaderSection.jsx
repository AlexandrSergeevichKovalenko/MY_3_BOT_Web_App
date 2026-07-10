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
    applyReaderProgressPercent = () => {},
    readerBookmarkPercent, setReaderBookmarkPercent, readerBookmarkPage,
    persistReaderExactBookmark = () => {},
    isCurrentReaderPageBookmarked,
    readerCanUseOriginalLayout,
    readerUsesCustomLayout: readerUsesCustomLayoutProp,
    readerWindowModel = null,
    loadReaderPageRange = () => {},
    onReaderVisualProgress = () => {},
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
    readerAudioEngineAdmin = false,
    readerAudioEnginePref = 'legacy',
    applyReaderAudioEnginePref = () => {},
    readerAudioEngineSupported = false,
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

  // Local UI-only state: the top-right "···" overflow menu that holds the
  // less-used chrome controls (scroll direction, original/text layout, timer,
  // collapse). Pure presentation — no business logic lives here.
  const [readerOverflowOpen, setReaderOverflowOpen] = React.useState(false);
  React.useEffect(() => {
    if (!readerOverflowOpen) return undefined;
    // Dismiss on any tap outside the menu (a fixed-position scrim can't be used:
    // the topbar's backdrop-filter traps position:fixed to the bar itself).
    const onDown = (event) => {
      const t = event.target;
      if (t && t.closest && (t.closest('.reader-overflow-menu') || t.closest('.reader-topbar-more'))) return;
      setReaderOverflowOpen(false);
    };
    const close = () => setReaderOverflowOpen(false);
    document.addEventListener('pointerdown', onDown, true);
    window.addEventListener('resize', close);
    return () => {
      document.removeEventListener('pointerdown', onDown, true);
      window.removeEventListener('resize', close);
    };
  }, [readerOverflowOpen]);

  // ── Phase 1: Apple-Books book-page engine (state/refs) ───────────────
  // The current server page is laid out in CSS columns so it always fills the
  // screen with NO vertical scroll; horizontal swipe turns columns and, at the
  // last column, advances to the next server page. Tap a word → translate;
  // tap centre → toggle chrome. Local UI state only.
  const readerColViewportRef = React.useRef(null);
  const readerColTrackRef = React.useRef(null);
  const [readerColIndex, setReaderColIndex] = React.useState(0);
  const [readerColCount, setReaderColCount] = React.useState(1);
  const [readerChromeHidden, setReaderChromeHidden] = React.useState(false);
  const readerColIndexRef = React.useRef(0);
  const readerColCountRef = React.useRef(1);
  const readerColGoLastRef = React.useRef(false);
  const readerColAnchorCharRef = React.useRef(0);
  // Real, measured column geometry — the CSS column pitch does NOT equal the
  // viewport width (the browser expands columns to fill), so we measure it from
  // the laid-out word spans and page by exactly that. Works on any screen.
  const readerColPitchRef = React.useRef(0);
  const readerColOriginRef = React.useRef(0);
  const readerGestureRef = React.useRef({ down: false, x: 0, y: 0, moved: false, base: 0 });

  const sectionClass = [
    'webapp-section',
    'webapp-reader',
    readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : '',
    readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : '',
    readerChromeHidden ? 'is-chrome-hidden' : '',
  ].filter(Boolean).join(' ');
  const showLibraryMode = !readerHasContent || readerArchiveOpen || !readerImmersive;
  const effectiveReaderTheme = showLibraryMode ? 'dark' : readerColorTheme;
  // Prefer the authoritative value computed in App.jsx (which forces server-paged
  // PDFs to never reflow); fall back to the local derivation only if not passed.
  const readerUsesCustomLayout = typeof readerUsesCustomLayoutProp === 'boolean'
    ? readerUsesCustomLayoutProp
    : (!readerCanUseOriginalLayout || readerLayoutMode === 'custom');
  const readerShowsLazyOriginalPage = !readerUsesCustomLayout
    && Array.isArray(readerPages)
    && readerPages[readerCurrentPage - 1] === null;
  const readerAudioPremiumLocked = readerAudioPremiumKnown && !readerAudioPremiumEnabled;
  const readerAudioPremiumHint = tr(
    'Аудио в книге доступно только по премиум подписке.',
    'Audio im Reader ist nur mit Premium verfügbar.'
  );

  // Engine runs only for server-paged content (PDF / EPUB text mode), never for
  // client-reflow text sources or the original-EPUB renderer.
  const readerColUsesEngine = readerPageCount > 0 && !readerUsesOriginalEpubLayout && !readerUsesCustomLayout;
  React.useEffect(() => { readerColIndexRef.current = readerColIndex; }, [readerColIndex]);
  React.useEffect(() => { readerColCountRef.current = readerColCount; }, [readerColCount]);

  const readerColStep = () => readerColPitchRef.current || (readerColViewportRef.current?.clientWidth || 1);
  const applyReaderColTransform = (px, animate) => {
    const track = readerColTrackRef.current;
    if (!track) return;
    track.style.transition = animate ? 'transform .3s cubic-bezier(.4,0,.2,1)' : 'none';
    track.style.transform = `translateX(${px}px)`;
  };
  // Apply column styles, then MEASURE the true pitch/origin/column-count from the
  // rendered spans (never assume pitch === viewport width). Returns { n }.
  const measureReaderColGeometry = () => {
    const vp = readerColViewportRef.current;
    const track = readerColTrackRef.current;
    if (!vp || !track) return { n: 1 };
    const w = vp.clientWidth, h = vp.clientHeight;
    if (w <= 0 || h <= 0) return { n: readerColCountRef.current || 1 };
    const M = 22;
    // Padding (not margin) → the single column exactly fills the content box
    // (w-2M) so the column pitch is the viewport width; symmetric M margins.
    track.style.height = `${h}px`;
    track.style.marginLeft = '0px';
    track.style.paddingLeft = `${M}px`;
    track.style.paddingRight = `${M}px`;
    track.style.columnWidth = `${Math.max(1, w - 2 * M)}px`;
    track.style.columnGap = `${2 * M}px`;
    // With the padding layout the single column exactly fills the content box,
    // so the column pitch is EXACTLY the viewport width (integer, no drift).
    const spans = track.querySelectorAll('[data-start]');
    let x0 = Infinity, maxX = 0;
    for (let i = 0; i < spans.length; i += 1) {
      const x = spans[i].offsetLeft;
      if (x < x0) x0 = x;
      if (x > maxX) maxX = x;
    }
    if (x0 === Infinity) x0 = 0;
    const pitch = w;
    readerColPitchRef.current = pitch;
    readerColOriginRef.current = x0;
    const n = Math.max(1, Math.round((maxX - x0) / pitch) + 1);
    return { n };
  };

  // Content signature: in window mode this changes ONLY when the loaded window
  // itself changes (extension / new doc / external jump), NOT on every page turn,
  // so turning columns inside the window never re-measures or fights the engine.
  const readerColContentSig = readerWindowModel
    ? `w:${readerWindowModel.lo}:${readerWindowModel.hi}:${readerWindowModel.totalChars}`
    : `p:${readerCurrentPage}:${readerShowsLazyOriginalPage ? 1 : 0}`;

  const readerColOffsetOf = (el) => Math.max(0, (el?.offsetLeft || 0) - readerColOriginRef.current);
  const readerColFindColOfChar = (charOffset) => {
    const track = readerColTrackRef.current;
    const step = readerColStep();
    if (!track || step <= 1) return 0;
    const spans = track.querySelectorAll('[data-start]');
    let el = null;
    for (let i = 0; i < spans.length; i += 1) {
      if (Number(spans[i].getAttribute('data-start')) >= charOffset - 1) { el = spans[i]; break; }
    }
    if (!el) el = spans[spans.length - 1];
    if (!el) return 0;
    return Math.max(0, Math.floor((readerColOffsetOf(el) + step * 0.5) / step));
  };
  const readerColVisibleCharAt = (colIndex) => {
    const track = readerColTrackRef.current;
    const step = readerColStep();
    if (!track || step <= 1) return readerColAnchorCharRef.current;
    const lo = colIndex * step - 2, hi = (colIndex + 1) * step;
    const spans = track.querySelectorAll('[data-start]');
    let bestStart = Infinity;
    for (let i = 0; i < spans.length; i += 1) {
      const x = readerColOffsetOf(spans[i]);
      if (x >= lo && x < hi) {
        const s = Number(spans[i].getAttribute('data-start'));
        if (s < bestStart) bestStart = s;
      }
    }
    return bestStart === Infinity ? readerColAnchorCharRef.current : bestStart;
  };
  const readerColPageOfChar = (charOffset) => {
    const offs = readerWindowModel?.offsets;
    if (!Array.isArray(offs) || !offs.length) return null;
    let page = offs[0].page;
    for (let i = 0; i < offs.length; i += 1) {
      if (offs[i].charStart <= charOffset) page = offs[i].page; else break;
    }
    return page;
  };
  // Record the reading position (char) and sync the server page for progress /
  // bookmark / prefetch — WITHOUT re-measuring (window text is unchanged).
  const readerColSyncPosition = () => {
    if (!readerWindowModel) return;
    const ch = readerColVisibleCharAt(readerColIndexRef.current);
    readerColAnchorCharRef.current = ch;
    const p = readerColPageOfChar(ch);
    if (p && p !== readerCurrentPage) setReaderCurrentPage(p);
    // Single-server-page sources (text/URL): progress is visual (char position in
    // the whole text), since there are no server pages to derive it from.
    if (readerPageCount <= 1 && readerWindowModel.totalChars > 0) {
      onReaderVisualProgress((ch / readerWindowModel.totalChars) * 100);
    }
  };

  // Measure the window into screen-columns; land on the column holding the
  // current reading position (anchor), so turning + window growth stay put.
  React.useLayoutEffect(() => {
    if (!readerColUsesEngine) return undefined;
    const vp = readerColViewportRef.current;
    const track = readerColTrackRef.current;
    if (!vp || !track) return undefined;
    const raf = window.requestAnimationFrame(() => {
      const { n } = measureReaderColGeometry();
      let target;
      if (readerWindowModel) {
        let anchor = readerColAnchorCharRef.current;
        const ap = readerColPageOfChar(anchor);
        if (ap == null || Math.abs(ap - readerCurrentPage) > 2) {
          anchor = readerWindowModel.curCharStart;
          readerColAnchorCharRef.current = anchor;
        }
        target = readerColFindColOfChar(anchor);
      } else {
        target = readerColGoLastRef.current ? n - 1 : 0;
      }
      readerColGoLastRef.current = false;
      target = Math.max(0, Math.min(n - 1, target));
      setReaderColCount(n);
      setReaderColIndex(target);
      readerColIndexRef.current = target;
      readerColCountRef.current = n;
      applyReaderColTransform(-target * readerColStep(), false);
    });
    return () => window.cancelAnimationFrame(raf);
  }, [readerColUsesEngine, readerColContentSig, readerFontSize, readerFontWeight]); // eslint-disable-line react-hooks/exhaustive-deps

  // Animate the turn + record the new reading position.
  React.useEffect(() => {
    if (!readerColUsesEngine) return;
    applyReaderColTransform(-readerColIndex * readerColStep(), true);
    readerColSyncPosition();
  }, [readerColIndex]); // eslint-disable-line react-hooks/exhaustive-deps

  // Re-measure on viewport resize (rotation / chrome toggle / keyboard).
  React.useEffect(() => {
    if (!readerColUsesEngine) return undefined;
    const vp = readerColViewportRef.current;
    if (!vp || typeof ResizeObserver === 'undefined') return undefined;
    let raf = 0;
    const ro = new ResizeObserver(() => {
      window.cancelAnimationFrame(raf);
      raf = window.requestAnimationFrame(() => {
        const track = readerColTrackRef.current;
        if (!track || vp.clientWidth <= 0 || vp.clientHeight <= 0) return;
        const { n } = measureReaderColGeometry();
        readerColCountRef.current = n;
        setReaderColCount(n);
        const i = Math.max(0, Math.min(readerColFindColOfChar(readerColAnchorCharRef.current), n - 1));
        readerColIndexRef.current = i;
        setReaderColIndex(i);
        applyReaderColTransform(-i * readerColStep(), false);
      });
    });
    ro.observe(vp);
    return () => { ro.disconnect(); window.cancelAnimationFrame(raf); };
  }, [readerColUsesEngine]); // eslint-disable-line react-hooks/exhaustive-deps

  const readerTurnNext = () => {
    if (readerColIndexRef.current < readerColCountRef.current - 1) {
      setReaderColIndex((i) => i + 1);
    } else if (readerWindowModel && readerWindowModel.hi < readerPageCount) {
      // End of loaded window but the book continues — load ahead; when the window
      // grows, the measure re-runs and keeps us anchored, adding columns to turn.
      loadReaderPageRange(readerWindowModel.hi + 1);
      applyReaderColTransform(-readerColIndexRef.current * readerColStep(), true);
    } else if (!readerWindowModel && readerCurrentPage < readerPageCount) {
      readerColGoLastRef.current = false;
      setReaderCurrentPage(readerCurrentPage + 1);
    } else {
      applyReaderColTransform(-readerColIndexRef.current * readerColStep(), true);
    }
  };
  const readerTurnPrev = () => {
    if (readerColIndexRef.current > 0) {
      setReaderColIndex((i) => i - 1);
    } else if (readerWindowModel && readerWindowModel.lo > 1) {
      loadReaderPageRange(readerWindowModel.lo - 1);
      applyReaderColTransform(0, true);
    } else if (!readerWindowModel && readerCurrentPage > 1) {
      readerColGoLastRef.current = true;
      setReaderCurrentPage(readerCurrentPage - 1);
    } else {
      applyReaderColTransform(0, true);
    }
  };

  const onReaderColPointerDown = (e) => {
    if (!readerColUsesEngine) return;
    const g = readerGestureRef.current;
    g.down = true; g.x = e.clientX; g.y = e.clientY; g.moved = false;
    g.base = -readerColIndexRef.current * readerColStep();
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch (_err) { /* ignore */ }
    applyReaderColTransform(g.base, false);
  };
  const onReaderColPointerMove = (e) => {
    const g = readerGestureRef.current;
    if (!g.down) return;
    const dx = e.clientX - g.x, dy = e.clientY - g.y;
    // Only real drags count as a swipe; small finger jitter (esp. the 2nd tap of a
    // double-tap) stays a TAP so the sentence-translation isn't lost.
    if (Math.abs(dx) > 12 || Math.abs(dy) > 12) g.moved = true;
    if (Math.abs(dx) >= Math.abs(dy)) {
      const atStart = readerColIndexRef.current === 0 && readerCurrentPage <= 1;
      const atEnd = readerColIndexRef.current >= readerColCountRef.current - 1 && readerCurrentPage >= readerPageCount;
      let d = dx;
      if ((atStart && dx > 0) || (atEnd && dx < 0)) d = dx * 0.3;
      applyReaderColTransform(g.base + d, false);
    }
  };
  const onReaderColPointerEnd = (e) => {
    const g = readerGestureRef.current;
    if (!g.down) return;
    g.down = false;
    const dx = e.clientX - g.x, dy = e.clientY - g.y;
    if (!g.moved) { handleReaderColTap(e); return; }
    if (Math.abs(dx) >= Math.abs(dy) && Math.abs(dx) > 40) {
      if (dx < 0) readerTurnNext(); else readerTurnPrev();
    } else {
      applyReaderColTransform(-readerColIndexRef.current * readerColStep(), true);
    }
  };
  const handleReaderColTap = (e) => {
    // Tap ON a word → translate (our core feature). Tap on any NON-text area
    // (margins, gaps, blank) → toggle chrome; only the thin outer edges turn.
    const wordEl = e.target && e.target.closest ? e.target.closest('[data-wid]') : null;
    if (wordEl) { handleReaderStructuredClick(e); return; }
    const vp = readerColViewportRef.current;
    if (!vp) { setReaderChromeHidden((v) => !v); return; }
    const rect = vp.getBoundingClientRect();
    const rx = e.clientX - rect.left;
    if (rx < rect.width * 0.14) readerTurnPrev();
    else if (rx > rect.width * 0.86) readerTurnNext();
    else setReaderChromeHidden((v) => !v);
  };

  // Single-server-page sources (text/URL): page number/total is visual (columns).
  const readerSinglePageDoc = readerPageCount <= 1 && !readerUsesOriginalEpubLayout;
  const readerDockPageNum = readerSinglePageDoc ? (readerColIndex + 1) : readerCurrentPage;
  const readerDockPageTotal = readerSinglePageDoc ? readerColCount : readerPageCount;

  // "N pages left in chapter" from the table of contents (server-page based).
  const readerChapterPagesLeft = React.useMemo(() => {
    if (readerUsesOriginalEpubLayout || !readerPageCount || readerPageCount <= 1) return null;
    const items = (Array.isArray(readerTocItems) ? readerTocItems : [])
      .map((it) => Number(it?.page_number))
      .filter((p) => Number.isFinite(p) && p > 0)
      .sort((a, b) => a - b);
    if (!items.length) return null;
    let nextChapterPage = null;
    for (let i = 0; i < items.length; i += 1) {
      if (items[i] > readerCurrentPage) { nextChapterPage = items[i]; break; }
    }
    const boundary = nextChapterPage != null ? nextChapterPage : (readerPageCount + 1);
    return Math.max(0, boundary - readerCurrentPage);
  }, [readerTocItems, readerCurrentPage, readerPageCount, readerUsesOriginalEpubLayout]);

  return (
    <section
      className={sectionClass}
      data-reader-theme={effectiveReaderTheme}
      ref={readerRef}
    >
      {(() => {
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
                          {tr('Выбран файл', 'Datei gewählt')}: {readerSelectedFile.name}
                        </small>
                      )}
                    </label>
                    <div className="webapp-actions">
                      <button type="submit" className="primary-button" disabled={readerLoading}>
                        {readerLoading ? tr('Загружаем...', 'Laden...') : tr('Открыть в читалке', 'Im Leser öffnen')}
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
                            {billingActionLoading ? tr('Открываем...', 'Öffnen...') : tr('Upgrade', 'Upgrade')}
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
                  const notFinished = readerDocuments
                    .filter((d) =>
                      !d?.is_archived &&
                      Number(d?.progress_percent || 0) < 100
                    )
                    .sort((a, b) => {
                      const ta = new Date(a?.last_opened_at || a?.updated_at || a?.created_at || 0).getTime();
                      const tb = new Date(b?.last_opened_at || b?.updated_at || b?.created_at || 0).getTime();
                      return tb - ta; // descending: most recently opened first
                    });
                  candidate = notFinished[0] || null;
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
                          ? tr('Открываем…', 'Öffnen…')
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
                          onClick={() => openReaderDocument(item.id)}
                          role="button"
                          tabIndex={0}
                          onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(item.id)}
                        >
                          <div
                            className="reader-library-cover"
                            style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
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
                              onClick={(e) => { e.stopPropagation(); openReaderDocument(item.id); }}
                              disabled={isOpening}
                              title={tr('Открыть книгу', 'Buch öffnen')}
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
                                {Boolean(item?.is_archived) ? tr('Вернуть', 'Zurück') : tr('Скрыть', 'Ausblenden')}
                              </span>
                            </button>
                            <button
                              type="button"
                              className="reader-lib-action is-danger"
                              onClick={(e) => { e.stopPropagation(); deleteReaderDocument(item.id); }}
                              title={tr('Удалить', 'Löschen')}
                            >
                              <span className="reader-lib-action-icon" aria-hidden="true">
                                <svg viewBox="0 0 18 18" fill="none">
                                  <path d="M5.25 5.25 12.75 12.75" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                  <path d="M12.75 5.25 5.25 12.75" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                </svg>
                              </span>
                              <span className="reader-lib-action-label">
                                {tr('Удалить', 'Löschen')}
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
                          {tr('Назад', 'Zurück')}
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
            {/* ── Topbar peek (collapsed): compact title pill → expand.
                 TOC / bookmark / audio now live in the bottom dock. ─────── */}
            {readerImmersive && readerTopbarCollapsed && (
              <div className="reader-topbar-peek reader-topbar-peek-flag">
                <button
                  type="button"
                  className="reader-peek-pill"
                  onClick={() => setReaderTopbarCollapsed(false)}
                  title={tr('Развернуть панель', 'Leiste aufklappen')}
                  aria-label={tr('Развернуть панель', 'Leiste aufklappen')}
                >
                  <span className="reader-peek-title">{readerTitle || tr('Читалка', 'Leser')}</span>
                  <span className="reader-peek-meta">
                    {!readerUsesOriginalEpubLayout && readerPageCount > 0
                      ? `${readerCurrentPage} / ${readerPageCount}`
                      : `${Math.round(readerProgressPercent)}%`}
                  </span>
                  <svg className="reader-peek-chevron" viewBox="0 0 18 18" fill="none" aria-hidden="true">
                    <path d="M4.5 6.75 9 11.25l4.5-4.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </button>
              </div>
            )}

            {/* ── Topbar (expanded, redesigned: back · title · ···) ─ */}
            {!readerTopbarCollapsed && (
              <div className="reader-topbar reader-immersive-topbar reader-topbar-flag">
                <div className="reader-topbar-row">
                  <button
                    type="button"
                    className="reader-topbar-icbtn reader-topbar-back"
                    onClick={() => {
                      setReaderArchiveOpen(true);
                      setReaderImmersive(false);
                      setReaderTopbarCollapsed(false);
                      setReaderSettingsOpen(false);
                    }}
                    title={tr('К библиотеке', 'Zur Bibliothek')}
                    aria-label={tr('К библиотеке', 'Zur Bibliothek')}
                  >
                    <svg viewBox="0 0 18 18" fill="none">
                      <path d="M10.75 4.25 6 9l4.75 4.75" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </button>
                  <div className="reader-topbar-center">
                    {/* Title + progress on ONE thin line (page number lives at the
                        bottom). Chapter title for EPUB instead of %. */}
                    <span className="reader-topbar-title">{readerTitle || tr('Читалка', 'Leser')}</span>
                    <span className="reader-topbar-pct">
                      {readerUsesOriginalEpubLayout && readerResolvedOriginalTocTitle
                        ? readerResolvedOriginalTocTitle
                        : (readerChapterPagesLeft != null && readerChapterPagesLeft > 0
                          ? `${Math.round(readerProgressPercent)}% · ${readerChapterPagesLeft} ${tr('стр. до главы', 'S. bis Kapitel')}`
                          : `${Math.round(readerProgressPercent)}%`)}
                    </span>
                  </div>
                  <button
                    type="button"
                    className="reader-topbar-icbtn reader-topbar-collapse"
                    onClick={() => {
                      setReaderTopbarCollapsed(true);
                      setReaderSettingsOpen(false);
                      setReaderOverflowOpen(false);
                    }}
                    title={tr('Свернуть панель', 'Leiste einklappen')}
                    aria-label={tr('Свернуть панель', 'Leiste einklappen')}
                  >
                    {/* Expanded → chevron points UP (collapse upward). */}
                    <svg viewBox="0 0 18 18" fill="none">
                      <path d="M4.5 11.25 9 6.75l4.5 4.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </button>
                  <button
                    type="button"
                    className={`reader-topbar-icbtn reader-topbar-more ${readerOverflowOpen ? 'is-active' : ''}`}
                    onClick={() => setReaderOverflowOpen((v) => !v)}
                    title={tr('Ещё', 'Mehr')}
                    aria-label={tr('Ещё', 'Mehr')}
                    aria-expanded={readerOverflowOpen}
                  >
                    <svg viewBox="0 0 18 18" fill="none">
                      <circle cx="9" cy="4.5" r="1.35" fill="currentColor" />
                      <circle cx="9" cy="9" r="1.35" fill="currentColor" />
                      <circle cx="9" cy="13.5" r="1.35" fill="currentColor" />
                    </svg>
                  </button>
                </div>

                {readerOverflowOpen && (
                  <>
                    <div className="reader-overflow-menu" role="menu">
                      <button
                        type="button"
                        className="reader-overflow-item"
                        role="menuitem"
                        onClick={() => {
                          const nextMode = readerReadingMode === 'vertical' ? 'horizontal' : 'vertical';
                          setReaderReadingMode(nextMode);
                          if (readerDocumentId) syncReaderState({ reading_mode: nextMode });
                          setReaderOverflowOpen(false);
                        }}
                        disabled={!readerContent}
                      >
                        <span className="reader-overflow-ic" aria-hidden="true">
                          {readerReadingMode === 'vertical' ? (
                            <svg viewBox="0 0 18 18" fill="none"><path d="M9 3.5v11M9 3.5 6.9 5.6M9 3.5l2.1 2.1M9 14.5l-2.1-2.1M9 14.5l2.1-2.1" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
                          ) : (
                            <svg viewBox="0 0 18 18" fill="none"><path d="M3.5 9h11M3.5 9l2.1-2.1M3.5 9l2.1 2.1M14.5 9l-2.1-2.1M14.5 9l-2.1 2.1" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
                          )}
                        </span>
                        <span className="reader-overflow-label">{tr('Прокрутка', 'Scrollen')}</span>
                        <span className="reader-overflow-value">
                          {readerReadingMode === 'vertical' ? tr('вертикаль', 'vertikal') : tr('горизонталь', 'horizontal')}
                        </span>
                      </button>

                      <button
                        type="button"
                        className={`reader-overflow-item ${readerTimerPaused ? 'is-paused' : ''}`}
                        role="menuitem"
                        onClick={toggleReaderTimerPause}
                        disabled={!readerHasContent}
                      >
                        <span className="reader-overflow-ic" aria-hidden="true">
                          <svg viewBox="0 0 18 18" fill="none"><circle cx="9" cy="10" r="5.5" stroke="currentColor" strokeWidth="1.4" /><path d="M9 7.2V10M7 2.5h4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
                        </span>
                        <span className="reader-overflow-label">
                          {readerTimerPaused ? tr('Таймер на паузе', 'Timer pausiert') : tr('Таймер чтения', 'Lese-Timer')}
                        </span>
                        <span className="reader-overflow-value reader-overflow-timer">
                          {formatReaderTimer(readerElapsedTotalSeconds)}
                        </span>
                      </button>
                    </div>
                  </>
                )}

                <div className="reader-progline" aria-hidden="true">
                  <i style={{ width: `${Math.max(0, Math.min(100, readerProgressPercent))}%` }} />
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
                className={`reader-article ${readerReadingMode === 'horizontal' ? 'is-horizontal' : 'is-vertical'} ${readerPageCount > 0 ? 'has-pages' : ''}${readerUsesOriginalEpubLayout ? ' is-epub-original' : ''}${readerColUsesEngine ? ' is-book-engine' : ''}`}
                onClick={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderStructuredClick}
                onMouseUp={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderArticleMouseUp}
                onWheel={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderPageWheel}
                onTouchStart={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderPageTouchStart}
                onTouchMove={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderArticleTouchMove}
                onTouchEnd={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderArticleTouchEnd}
                onTouchCancel={(readerUsesOriginalEpubLayout || readerColUsesEngine) ? undefined : handleReaderArticleTouchCancel}
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
                          {tr('Это оригинальная обложка EPUB. Нажми, чтобы открыть текст книги.', 'Das ist das originale EPUB-Cover. Tippe, um den Buchtext zu öffnen.')}
                        </span>
                      </button>
                    )}
                    <div ref={readerEpubViewportRef} className="reader-epub-original-viewport" />
                  </div>
                ) : readerColUsesEngine ? (
                  <div
                    className="reader-col-viewport"
                    ref={readerColViewportRef}
                    onPointerDown={onReaderColPointerDown}
                    onPointerMove={onReaderColPointerMove}
                    onPointerUp={onReaderColPointerEnd}
                    onPointerCancel={onReaderColPointerEnd}
                    style={{
                      '--reader-font-size': `${readerFontSize}px`,
                      '--reader-font-weight': readerFontWeight,
                    }}
                  >
                    {isCurrentReaderPageBookmarked && (
                      <span className="reader-page-bookmark-indicator" aria-hidden="true" />
                    )}
                    <div ref={readerColTrackRef} className="reader-col-track">
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

            {/* ── Bottom dock: actions + scrubber, OR the audio player in the
                 SAME reserved space when audio is active (no overlap, no page
                 shrink — the dock footprint is already accounted for). ─ */}
            {readerContent && (
              <div className={`reader-bottom-dock${readerAudioPlayActive ? ' is-audio' : ''}`}>
                {readerAudioPlayActive ? (
                  <div className="reader-dock-audio">
                    <button
                      type="button"
                      className="reader-dock-play reader-dock-audio-toggle"
                      onClick={readerAudioPaused ? resumeReaderAudioPlay : pauseReaderAudioPlay}
                      aria-label={readerAudioPaused ? tr('Продолжить', 'Fortsetzen') : tr('Пауза', 'Pause')}
                    >
                      {readerAudioPaused ? (
                        <svg viewBox="0 0 18 18" fill="none"><path d="M6 4.5l8 4.5-8 4.5V4.5z" fill="currentColor"/></svg>
                      ) : (
                        <svg viewBox="0 0 18 18" fill="none"><rect x="5" y="4.5" width="2.8" height="9" rx="1" fill="currentColor"/><rect x="10.2" y="4.5" width="2.8" height="9" rx="1" fill="currentColor"/></svg>
                      )}
                    </button>
                    <div className="reader-dock-audio-progress">
                      <div
                        className="reader-dock-audio-fill"
                        style={{ width: readerAudioPlayData?.duration_ms ? `${Math.min(100, (readerAudioPlayPosition / readerAudioPlayData.duration_ms) * 100)}%` : '0%' }}
                      />
                    </div>
                    <div className="reader-dock-audio-time">
                      {(() => {
                        const fmt = (ms) => { const s = Math.floor(ms / 1000); return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`; };
                        return `${fmt(readerAudioPlayPosition)} / ${fmt(readerAudioPlayData?.duration_ms || 0)}`;
                      })()}
                    </div>
                    <select
                      className="reader-dock-audio-rate"
                      value={readerAudioRate}
                      onChange={(e) => { const newRate = parseFloat(e.target.value); setReaderAudioRate(newRate); if (audioElementRef?.current) audioElementRef.current.playbackRate = newRate; }}
                      aria-label={tr('Скорость', 'Geschwindigkeit')}
                    >
                      <option value="0.75">0.75×</option>
                      <option value="1">1×</option>
                      <option value="1.25">1.25×</option>
                      <option value="1.5">1.5×</option>
                    </select>
                    <button
                      type="button"
                      className="reader-dock-audio-close"
                      onClick={stopReaderAudioPlay}
                      aria-label={tr('Закрыть плеер', 'Player schließen')}
                    >
                      <svg viewBox="0 0 18 18" fill="none"><path d="M4.5 4.5 13.5 13.5M13.5 4.5 4.5 13.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" /></svg>
                    </button>
                  </div>
                ) : (
                <>
                <div className="reader-dock-slim">
                  <button
                    type="button"
                    className="reader-dock-btn reader-dock-aa"
                    onClick={() => setReaderSettingsOpen(true)}
                    disabled={!readerContent}
                    title={tr('Шрифт и тема', 'Schrift & Thema')}
                    aria-label={tr('Шрифт и тема', 'Schrift & Thema')}
                  >Aa</button>
                  <button
                    type="button"
                    className={`reader-dock-btn ${readerShowToc ? 'is-active' : ''}`}
                    onClick={() => {
                      if (!readerShowToc && readerTocItems.length === 0) void loadReaderToc();
                      setReaderShowToc((v) => !v);
                    }}
                    disabled={!readerContent}
                    title={tr('Оглавление', 'Inhaltsverzeichnis')}
                    aria-label={tr('Оглавление', 'Inhaltsverzeichnis')}
                  >
                    <svg viewBox="0 0 18 18" fill="none"><path d="M4 5h10M4 9h10M4 13h6.5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" /></svg>
                  </button>
                  <button
                    type="button"
                    className={`reader-dock-btn ${isCurrentReaderPageBookmarked ? 'is-active' : ''}`}
                    onClick={() => {
                      const mark = computeReaderProgressPercent();
                      setReaderBookmarkPercent(mark);
                      persistReaderExactBookmark(readerCurrentPage);
                      if (readerDocumentId) syncReaderState({ bookmark_percent: Number(mark.toFixed(2)) });
                    }}
                    disabled={!readerContent || !readerDocumentId}
                    title={tr('Поставить закладку', 'Lesezeichen setzen')}
                    aria-label={tr('Поставить закладку', 'Lesezeichen setzen')}
                  >
                    <svg viewBox="0 0 18 18" fill="none"><path d="M5.25 3.75h7.5a.75.75 0 0 1 .75.75v9.75L9 11.55l-4.5 2.7V4.5a.75.75 0 0 1 .75-.75Z" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" /></svg>
                  </button>
                  {readerBookmarkPercent > 0 && !isCurrentReaderPageBookmarked && (
                    <button
                      type="button"
                      className="reader-dock-btn reader-dock-bmjump"
                      onClick={() => {
                        if (readerUsesOriginalEpubLayout) { applyReaderProgressPercent(readerBookmarkPercent); return; }
                        setReaderCurrentPage(readerBookmarkPage);
                      }}
                      title={tr('Перейти к закладке', 'Zur Lesezeiche springen')}
                      aria-label={tr('Перейти к закладке', 'Zur Lesezeiche springen')}
                    >
                      <svg viewBox="0 0 18 18" fill="none"><path d="M9 3.5v8M9 11.5 6.4 8.9M9 11.5l2.6-2.6M4.5 14h9" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
                    </button>
                  )}
                  <button
                    type="button"
                    className={`reader-dock-play-flat${readerAudioAwaitingWordTap ? ' is-awaiting' : ''}`}
                    onClick={readerAudioPremiumLocked ? onReaderAudioUpgrade : onReaderAudioPlayBtn}
                    disabled={!readerHasContent || readerAudioPlayLoading || billingActionLoading}
                    title={readerAudioPremiumLocked
                      ? readerAudioPremiumHint
                      : (readerAudioAwaitingWordTap ? tr('Нажми слово…', 'Wort antippen…') : tr('Слушать', 'Hören'))}
                    aria-label={tr('Слушать книгу', 'Buch hören')}
                  >
                    {readerAudioPlayLoading ? (
                      <svg viewBox="0 0 18 18" fill="none"><circle cx="9" cy="9" r="6" stroke="currentColor" strokeWidth="1.7" strokeDasharray="28" strokeDashoffset="10" strokeLinecap="round"><animateTransform attributeName="transform" type="rotate" from="0 9 9" to="360 9 9" dur="0.9s" repeatCount="indefinite"/></circle></svg>
                    ) : (
                      <>
                        <svg viewBox="0 0 18 18" fill="none"><path d="M6 4.5l8 4.5-8 4.5V4.5z" fill="currentColor"/></svg>
                        <span className="reader-dock-play-label">{tr('Слушать', 'Hören')}</span>
                      </>
                    )}
                  </button>
                  {(readerUsesOriginalEpubLayout || readerPageCount > 0) && (
                    <button
                      type="button"
                      className="reader-dock-page"
                      onClick={() => {
                        if (readerUsesOriginalEpubLayout || readerSinglePageDoc) return;
                        setReaderPageJumpInput(String(readerCurrentPage));
                        setReaderShowPageJump(true);
                      }}
                      title={tr('Перейти к странице', 'Zur Seite springen')}
                    >
                      {readerUsesOriginalEpubLayout
                        ? `${Math.round(readerProgressPercent)}%`
                        : `${readerDockPageNum} / ${readerDockPageTotal}`}
                    </button>
                  )}
                </div>
                </>
                )}
              </div>
            )}

            {/* ── Hidden audio element ────────────────────────────── */}
            <audio ref={audioElementRef} preload="metadata" playsInline style={{ display: 'none' }} />
            <audio ref={readerAudioPreloadElementRef} preload="auto" playsInline style={{ display: 'none' }} />

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
                      aria-label={tr('Закрыть', 'Schließen')}
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
                  aria-label={tr('Закрыть', 'Schließen')}
                  onClick={() => setReaderSettingsOpen(false)}
                />
                <div className="reader-settings-sheet">
                  <div className="reader-settings-sheet-head">
                    <strong>{tr('Настройки чтения', 'Leseeinstellungen')}</strong>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setReaderSettingsOpen(false)}
                      >
                        ×
                      </button>
                    </div>
                  </div>

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

                  {/* Admin-only (test): audio playback engine. Hidden for everyone else. */}
                  {readerAudioEngineAdmin && (
                    <label className="webapp-field">
                      <span>{tr('Движок аудио (тест)', 'Audio-Engine (Test)')}</span>
                      <div className="reader-theme-seg">
                        {[
                          { k: 'legacy', l: tr('Классический', 'Klassisch') },
                          { k: 'webaudio', l: tr('Gapless', 'Gapless') },
                        ].map((opt) => (
                          <button
                            key={opt.k}
                            type="button"
                            className={`reader-theme-seg-btn ${readerAudioEnginePref === opt.k ? 'is-active' : ''}`}
                            disabled={opt.k === 'webaudio' && !readerAudioEngineSupported}
                            onClick={() => applyReaderAudioEnginePref(opt.k)}
                          >
                            {opt.l}
                          </button>
                        ))}
                      </div>
                      <span className="webapp-muted" style={{ fontSize: 11, marginTop: 4 }}>
                        {readerAudioEngineSupported
                          ? tr('Gapless убирает паузу при перелистывании. Применяется со следующего запуска аудио.',
                               'Gapless entfernt die Pause beim Seitenwechsel. Wirkt ab dem nächsten Audiostart.')
                          : tr('Web Audio не поддерживается в этом браузере.', 'Web Audio wird in diesem Browser nicht unterstützt.')}
                      </span>
                    </label>
                  )}

                  <label className="webapp-field">
                    <span>{tr('Размер шрифта', 'Schriftgröße')}</span>
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
                    <span>{tr('Жирность текста', 'Schriftstärke')}</span>
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
