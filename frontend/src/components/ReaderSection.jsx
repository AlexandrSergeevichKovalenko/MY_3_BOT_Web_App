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
import ReaderGuideModal from './ReaderGuideModal';

// Persist the open library shelf across brief ReaderSection remounts. The section is
// mounted only while isSectionVisible('reader') is true; an async event (poll /
// visibilitychange) can flip it for a frame → remount → local state would reset and
// bounce the user from a shelf back to the overview mid-scroll. Module-scoped so it
// survives the remount (single ReaderSection instance).
let _readerLibraryShelfMemo = null;

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
    readerPublicDocuments = [],
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
    openReaderArticleSearch = () => {},
    readerArticleClipUrl = '',
    openReaderArticleClipUrl = () => {},
    dismissReaderArticleClip = () => {},
    pasteReaderClipboardUrl = () => {},
    // ── "Источники": curated in-app article feed ─────────────────
    readerSourcesOpen = false,
    openReaderSourcesPanel = () => {},
    readerSources = [],
    readerSourceId = '',
    selectReaderSource = () => {},
    readerSourceArticles = [],
    readerSourcesLoading = false,
    readerSourcesError = '',
    openReaderSourceArticle = () => {},
    readerLoading, readerError, readerErrorCode,

    // ── reading state ────────────────────────────────────────────
    readerOpenNonce,
    readerDocumentId, readerTitle, readerContent,
    readerPages, readerDisplayPages, readerPageCount,
    readerCurrentPage, setReaderCurrentPage,
    readerProgressPercent,
    applyReaderProgressPercent = () => {},
    readerBookmarkPercent, setReaderBookmarkPercent, readerBookmarkPage,
    readerBookmarkAnchor = null,
    setReaderBookmarkAnchorSaved = () => {},
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
    readerCanUseOriginal = false,
    onOpenReaderOriginal = () => {},
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
    // Выделение фразы удержанием (long-press → тянем по словам). Живёт в App.jsx,
    // сюда приходит как готовый жест: begin/move/end + отмена.
    beginReaderPhraseDrag = () => false,
    moveReaderPhraseDrag = () => false,
    endReaderPhraseDrag = () => false,
    cancelReaderPhraseDrag = () => {},
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
    dismissReaderAudioPlayError = () => {},
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
    readerAudioPlayingWid = null,
    readerAudioAwaitingWordTap = false,
    onReaderAudioPlayBtn = () => {},
    readerIsArticle = false, // web articles are read-only — no narration button
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
  // Which library book has its action menu (⋯) expanded — Apple-Books style, one
  // quiet corner button instead of a permanent 4-button row on every cover.
  const [readerLibActionsOpenId, setReaderLibActionsOpenId] = React.useState(null);
  // Library overview vs a single shelf detail: null | 'mine' | 'classics' | 'articles'
  const [readerLibraryShelf, _setReaderLibraryShelf] = React.useState(_readerLibraryShelfMemo);
  const setReaderLibraryShelf = React.useCallback((v) => {
    _setReaderLibraryShelf((prev) => {
      const next = typeof v === 'function' ? v(prev) : v;
      _readerLibraryShelfMemo = next;
      return next;
    });
  }, []);
  // «Как читать и слушать» grandma-proof explainer.
  const [readerGuideOpen, setReaderGuideOpen] = React.useState(false);
  const readerColIndexRef = React.useRef(0);
  const readerColCountRef = React.useRef(1);
  const readerColGoLastRef = React.useRef(false);
  const readerColAnchorCharRef = React.useRef(0);
  // Tracks the doc whose saved bookmark has already been restored (one-shot per
  // open) so single-page/pageless sources land on the saved position, not char 0.
  const readerColRestoredDocRef = React.useRef(null);
  // Which screen column the bookmark sits on (null = not on any column of the loaded
  // window). Recomputed only where the geometry is actually known — inside place().
  const [readerBookmarkCol, setReaderBookmarkCol] = React.useState(null);
  // "Put the view exactly here" — {page, char-in-page}. Consumed by the next place()
  // once that page is inside the window: used to reopen a book on its bookmark and
  // to jump to the bookmark from the dock.
  const readerColPendingAnchorRef = React.useRef(null);
  // Real, measured column geometry — the CSS column pitch does NOT equal the
  // viewport width (the browser expands columns to fill), so we measure it from
  // the laid-out word spans and page by exactly that. Works on any screen.
  const readerColPitchRef = React.useRef(0);
  const readerColOriginRef = React.useRef(0);
  const readerColWillChangeTimer = React.useRef(0);
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
  // Engine runs only for server-paged content (PDF / EPUB text mode), never for
  // client-reflow text sources or the original-EPUB renderer.
  const readerColUsesEngine = readerPageCount > 0 && !readerUsesOriginalEpubLayout && !readerUsesCustomLayout;
  React.useEffect(() => { readerColIndexRef.current = readerColIndex; }, [readerColIndex]);
  React.useEffect(() => { readerColCountRef.current = readerColCount; }, [readerColCount]);

  const readerColStep = () => readerColPitchRef.current || (readerColViewportRef.current?.clientWidth || 1);
  const applyReaderColTransform = (px, animate) => {
    const track = readerColTrackRef.current;
    if (!track) return;
    // Snappier page-turn: shorter with a decel (ease-out) curve so the page arrives
    // crisply and settles — reads more like a physical page flip than the old linear-ish glide.
    track.style.transition = animate ? 'transform .26s cubic-bezier(.2,.68,.32,1)' : 'none';
    // will-change ONLY during the turn animation. The track is very wide (whole doc =
    // thousands of px); a PERMANENT will-change:transform keeps it as one composited
    // layer whose far tiles iOS WebKit leaves UNPAINTED after a jump/reopen → blank
    // pages at a correct transform. So promote for the animation, then drop the layer
    // (will-change:auto) so WebKit repaints the visible columns fresh on every jump.
    window.clearTimeout(readerColWillChangeTimer.current);
    if (animate) {
      track.style.willChange = 'transform';
      readerColWillChangeTimer.current = window.setTimeout(() => {
        const t = readerColTrackRef.current;
        if (t) t.style.willChange = 'auto';
      }, 340);
    } else {
      track.style.willChange = 'auto';
    }
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
    // Columns-per-screen scale with the viewport so a TABLET uses the whole screen
    // for the book text (Apple-Books style), while the phone is unchanged:
    //   • landscape tablet (≥900px) → TWO-page spread (2 columns fill the screen)
    //   • portrait tablet (620–900) → ONE wide column capped to a comfortable
    //     measure and centred
    //   • phone (<620) → ONE full-width column (original)
    // Invariant that keeps paging aligned: gap = 2·M and pitch = viewport width, so
    // exactly `cols` columns fit per screen and the next screen's first column lands
    // on the left margin (no drift). Column pattern per screen: [M · col · 2M · col · M].
    let cols = 1;
    let M = 22;
    if (w >= 900) {
      cols = 2;
      M = Math.min(60, Math.max(28, Math.round(w * 0.03)));
    } else if (w >= 620) {
      cols = 1;
      // Portrait tablet: one wide column that uses more of the screen for the book
      // text (Apple-Books-like) — cap ~760px, centred, with a sensible min margin.
      M = Math.max(32, Math.round((w - 760) / 2));
    }
    const gap = 2 * M;
    const colWidth = cols === 2 ? Math.max(1, (w - 4 * M) / 2) : Math.max(1, w - 2 * M);
    track.style.height = `${h}px`;
    track.style.marginLeft = '0px';
    track.style.paddingLeft = `${M}px`;
    track.style.paddingRight = `${M}px`;
    track.style.columnWidth = `${colWidth}px`;
    track.style.columnGap = `${gap}px`;
    // MEASURE the true pitch from where the columns actually landed — do NOT assume
    // pitch === viewport width. The browser's used column width can differ from the
    // requested (w − 2M) by a sub-pixel amount; that tiny error ACCUMULATES across
    // columns, so by the 7th–9th screen the transform (index × w) lands in a column
    // GAP and the page renders blank («first page fine, later pages empty»).
    //
    // Method: bin every span into its nominal column k = floor((x − x0)/nominalStep).
    // FLOOR (not round) matters: a column's spans span [colLeft, colLeft+colWidth] and
    // colWidth is nearly a whole step, so round() would push mid-line words into the next
    // column and corrupt its edge — floor keeps every span in its own column. Accumulated
    // drift stays far below a column over a page range, so binning is unambiguous.
    // Each column's TRUE left edge is the min offset of the spans binned into it (every
    // text line starts at the column's left margin). The per-column step for column k is
    // (leftEdge_k − x0)/k; the MEDIAN of these over all populated columns is the true
    // step — the long lever arm of far columns averages out the sub-pixel noise, and the
    // median is immune to the odd column whose first line happens to be indented. This is
    // robust to sparse/short-line columns (nav-menu junk) that broke gap-based detection.
    const nominalStep = (colWidth + gap) || w;
    const spans = track.querySelectorAll('[data-start]');
    let x0 = Infinity;
    let maxX = 0;
    const colMin = new Map(); // nominal column index → its leftmost span offset
    for (let i = 0; i < spans.length; i += 1) {
      const x = spans[i].offsetLeft;
      if (!Number.isFinite(x)) continue;
      if (x < x0) x0 = x;
      if (x > maxX) maxX = x;
    }
    if (x0 === Infinity) x0 = 0;
    for (let i = 0; i < spans.length; i += 1) {
      const x = spans[i].offsetLeft;
      if (!Number.isFinite(x)) continue;
      const k = Math.floor((x - x0) / nominalStep);
      if (k > 0 && (!colMin.has(k) || x < colMin.get(k))) colMin.set(k, x);
    }
    let pitch = w; // safe fallback (too little content to measure)
    let maxK = 0;
    const steps = [];
    colMin.forEach((edge, k) => { steps.push((edge - x0) / k); if (k > maxK) maxK = k; });
    if (steps.length) {
      steps.sort((a, b) => a - b);
      const colStep = steps[Math.floor(steps.length / 2)];
      const screenStep = colStep * cols;
      if (screenStep > w * 0.5 && screenStep < w * 1.5) pitch = screenStep;
    }
    readerColPitchRef.current = pitch;
    readerColOriginRef.current = x0;
    // Page count from the farthest populated column (exact), falling back to the pitch
    // estimate when nothing could be binned.
    const n = maxK > 0
      ? Math.max(1, Math.floor(maxK / cols) + 1)
      : Math.max(1, Math.round((maxX - x0) / pitch) + 1);
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
  // The bookmark, expressed the way it SURVIVES a font or screen change: the server
  // page under the first visible character of a screen column, plus that character's
  // offset INSIDE the page. Absolute doc offsets are unusable — pages load lazily, so
  // the text before the current one may not be in memory at all.
  const readerColBookmarkAnchorNow = () => {
    if (!readerWindowModel) return null;
    const ch = readerColVisibleCharAt(readerColIndexRef.current);
    const page = readerColPageOfChar(ch);
    if (!page) return null;
    const off = readerWindowModel.offsets.find((o) => Number(o.page) === Number(page));
    if (!off) return null;
    return { page: Number(page), char: Math.max(0, ch - Number(off.charStart || 0)) };
  };
  // Same anchor mapped back into the CURRENT window's coordinates, or null when the
  // bookmarked page is not inside the loaded window (then there is nothing to show).
  const readerBookmarkWindowChar = () => {
    if (!readerBookmarkAnchor || !readerWindowModel) return null;
    const off = readerWindowModel.offsets.find((o) => Number(o.page) === Number(readerBookmarkAnchor.page));
    if (!off) return null;
    return Number(off.charStart || 0) + Math.max(0, Number(readerBookmarkAnchor.char || 0));
  };

  // Record the reading position (char) and sync the server page for progress /
  // bookmark / prefetch — WITHOUT re-measuring (window text is unchanged).
  const readerColSyncPosition = () => {
    if (!readerWindowModel) return;
    const ch = readerColVisibleCharAt(readerColIndexRef.current);
    readerColAnchorCharRef.current = ch;
    const p = readerColPageOfChar(ch);
    // During audio, the audio page-sync OWNS readerCurrentPage. If the word-follow
    // (which moves the column) also set it here, changing readerCurrentPage would
    // recompute the window → recompute the highlighted word → re-fire the follow →
    // endless page bouncing. So only update the page from column moves when NOT
    // playing; the anchor is still recorded above so window edges stay aligned.
    if (p && p !== readerCurrentPage && !readerAudioPlayActive) setReaderCurrentPage(p);
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
    let cancelled = false;
    const rafs = [];
    const timers = [];

    // Measure the columns and land on the anchor column. Safe to run repeatedly —
    // it just re-measures + re-positions to the same reading anchor.
    const place = () => {
      if (cancelled) return;
      const v = readerColViewportRef.current;
      if (!v || v.clientWidth <= 0) return;
      const { n } = measureReaderColGeometry();
      let target;
      if (readerWindowModel) {
        const openKey = `${readerDocumentId}:${readerOpenNonce}`;
        // ── One-shot restore of the reading position, once per open ──────────────
        // Best source first: the EXACT anchor (page + char inside it). A ~1100-char
        // server page is 2-3 screens wide, so landing on "the right page" would still
        // be the wrong screen. Only armed once the bookmarked page is really inside
        // the loaded window — otherwise we wait for the window that holds it.
        if (readerColRestoredDocRef.current !== openKey) {
          if (readerBookmarkAnchor) {
            // Wait for the window that actually holds the bookmarked page: staying
            // unmarked means the next measurement tries again.
            if (readerBookmarkWindowChar() != null) {
              readerColPendingAnchorRef.current = { ...readerBookmarkAnchor };
              readerColRestoredDocRef.current = openKey;
            }
          } else {
            if (readerSinglePageDoc && readerWindowModel.totalChars > 0) {
              // No exact anchor and no server pages to aim at: the saved percent IS
              // the character position for these sources (see onReaderVisualProgress),
              // so it is a real coordinate here, not a guess.
              const pct = Math.max(0, Math.min(100, Number(readerBookmarkPercent || 0)));
              if (pct > 0.3) {
                readerColAnchorCharRef.current = Math.round((pct / 100) * readerWindowModel.totalChars);
              }
            }
            // Nothing to restore (no bookmark) — but this open is DONE restoring, so
            // setting a bookmark later never reads as "restore me to it" and yanks
            // the reader away from where they are.
            readerColRestoredDocRef.current = openKey;
          }
        }
        // An explicit "go exactly here" request (reopen-on-bookmark, dock jump) wins
        // over the running reading anchor for this one measurement.
        const pending = readerColPendingAnchorRef.current;
        if (pending) {
          const off = readerWindowModel.offsets.find((o) => Number(o.page) === Number(pending.page));
          if (off) {
            readerColAnchorCharRef.current = Number(off.charStart || 0) + Math.max(0, Number(pending.char || 0));
            readerColPendingAnchorRef.current = null;
          }
        }
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
      // The ribbon belongs to ONE screen column. Measured here because only here is
      // the laid-out geometry known; turning pages inside the window does not move it.
      const bmWindowChar = readerBookmarkWindowChar();
      setReaderBookmarkCol(bmWindowChar == null ? null : readerColFindColOfChar(bmWindowChar));
      readerColGoLastRef.current = false;
      target = Math.max(0, Math.min(n - 1, target));
      setReaderColCount(n);
      setReaderColIndex(target);
      readerColIndexRef.current = target;
      readerColCountRef.current = n;
      applyReaderColTransform(-target * readerColStep(), false);
    };

    rafs.push(window.requestAnimationFrame(place));
    // Web fonts change text metrics → column count/positions. If the first pass
    // measured against fallback-font geometry (before the reading font loaded),
    // the track paginates wrong and later pages render blank until a re-open.
    // Re-place once fonts settle, plus a short safety-net pass for late layout.
    if (typeof document !== 'undefined' && document.fonts && document.fonts.ready) {
      document.fonts.ready
        .then(() => { if (!cancelled) rafs.push(window.requestAnimationFrame(place)); })
        .catch(() => {});
    }
    timers.push(window.setTimeout(() => {
      if (!cancelled) rafs.push(window.requestAnimationFrame(place));
    }, 240));
    // GLOBAL blank-after-reopen fix: on some iOS WebKit states the re-mounted track has
    // the content laid out and the transform correct (proven by diagnostics: wantTx ==
    // realTx == dom) yet paints nothing at all. Force a hard repaint of the track once
    // the layout has settled — an off→on display flip makes WebKit re-rasterize the
    // whole element. The inline transform survives the toggle, so the page stays put.
    timers.push(window.setTimeout(() => {
      if (cancelled) return;
      const t = readerColTrackRef.current;
      if (!t) return;
      const prevDisplay = t.style.display;
      t.style.display = 'none';
      void t.offsetHeight; // force reflow
      t.style.display = prevDisplay || '';
      rafs.push(window.requestAnimationFrame(place));
    }, 380));

    return () => {
      cancelled = true;
      rafs.forEach((id) => window.cancelAnimationFrame(id));
      timers.forEach((id) => window.clearTimeout(id));
    };
  }, [readerColUsesEngine, readerColContentSig, readerFontSize, readerFontWeight, readerDocumentId, readerOpenNonce, readerBookmarkAnchor]); // eslint-disable-line react-hooks/exhaustive-deps

  // Animate the turn + record the new reading position.
  React.useEffect(() => {
    if (!readerColUsesEngine) return;
    applyReaderColTransform(-readerColIndex * readerColStep(), true);
    readerColSyncPosition();
  }, [readerColIndex]); // eslint-disable-line react-hooks/exhaustive-deps

  // Re-measure on viewport resize (rotation / chrome toggle / keyboard) AND on
  // TRACK growth. The track grows when lazily-loaded pages arrive or late assets
  // reflow the text — without re-measuring then, the extra columns stay blank
  // (the "first page full, rest empty until re-open" bug). Observing the track
  // makes the pagination self-heal the moment more content lands.
  React.useEffect(() => {
    if (!readerColUsesEngine) return undefined;
    const vp = readerColViewportRef.current;
    if (!vp || typeof ResizeObserver === 'undefined') return undefined;
    let raf = 0;
    let lastW = 0;
    const remeasure = () => {
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
    };
    const ro = new ResizeObserver((entries) => {
      // Ignore no-op fires (the track reports its own size on observe start).
      let changed = false;
      for (const e of entries) {
        const w = e.contentRect ? Math.round(e.contentRect.width) : 0;
        if (e.target === vp || Math.abs(w - lastW) > 1) { changed = true; lastW = w; }
      }
      if (changed) remeasure();
    });
    ro.observe(vp);
    const track = readerColTrackRef.current;
    if (track) ro.observe(track);
    return () => { ro.disconnect(); window.cancelAnimationFrame(raf); };
  }, [readerColUsesEngine]); // eslint-disable-line react-hooks/exhaustive-deps

  // SELF-HEAL blank pages. The geometry (pitch/origin) is measured once against the
  // rendered spans; if that measure landed on an unsettled layout (the Telegram sheet
  // still resizing, late reflow, a stale pitch carried over from the previous document),
  // the transform step no longer matches the real column pitch and the visible column
  // falls into the gap → «one page shows, the rest are blank until a full re-open».
  // The ResizeObserver only fires on a size CHANGE, so a stale-but-static layout never
  // heals itself. This poll runs for a few seconds after the content changes: if the
  // current column holds NO text span while the track clearly has content, it re-measures
  // from the live span offsets (which converges to the true pitch) and re-anchors to the
  // reading position. On a correctly-paginated page the check is a no-op.
  React.useEffect(() => {
    if (!readerColUsesEngine) return undefined;
    let stopped = false;
    let ticks = 0;
    let timer = 0;
    const tick = () => {
      if (stopped) return;
      const vp = readerColViewportRef.current;
      const track = readerColTrackRef.current;
      const step = readerColStep();
      if (vp && track && vp.clientWidth > 0 && step > 1) {
        const spans = track.querySelectorAll('[data-start]');
        if (spans.length > 0) {
          const cur = readerColIndexRef.current;
          const lo = cur * step - 2;
          const hi = (cur + 1) * step;
          let visible = false;
          for (let i = 0; i < spans.length; i += 1) {
            const x = readerColOffsetOf(spans[i]);
            if (x >= lo && x < hi) { visible = true; break; }
          }
          if (!visible) {
            const { n } = measureReaderColGeometry();
            readerColCountRef.current = n;
            setReaderColCount(n);
            const idx = Math.max(0, Math.min(readerColFindColOfChar(readerColAnchorCharRef.current), n - 1));
            readerColIndexRef.current = idx;
            setReaderColIndex(idx);
            applyReaderColTransform(-idx * readerColStep(), false);
          }
        }
      }
      ticks += 1;
      if (ticks < 12) timer = window.setTimeout(tick, 300); // ~3.6s watchdog
    };
    timer = window.setTimeout(tick, 300);
    return () => { stopped = true; window.clearTimeout(timer); };
  }, [readerColUsesEngine, readerColContentSig, readerOpenNonce]); // eslint-disable-line react-hooks/exhaustive-deps

  // Follow the spoken word during audio: scroll to the column holding the
  // currently-highlighted word. Uses the LIVE DOM span, so it's immune to the
  // window-coordinate shifts that made the view jump to a page start when audio
  // started on a tapped word (or crossed a page). Keeps the read word on screen.
  React.useEffect(() => {
    if (!readerColUsesEngine || !readerAudioPlayActive || !readerAudioPlayingWid) return undefined;
    const raf = window.requestAnimationFrame(() => {
      const track = readerColTrackRef.current;
      if (!track) return;
      let el = null;
      try {
        const sel = (typeof window !== 'undefined' && window.CSS && CSS.escape)
          ? CSS.escape(readerAudioPlayingWid) : readerAudioPlayingWid;
        el = track.querySelector(`[data-wid="${sel}"]`);
      } catch (_e) { el = null; }
      if (!el) return;
      const step = readerColStep();
      if (step <= 1) return;
      const x = readerColOffsetOf(el);
      const cur = readerColIndexRef.current;
      // If the spoken word is already on the visible column, do NOTHING. Following
      // every word (even visible ones) fought the re-anchor and bounced the page.
      // Only scroll when the word has genuinely moved off the current column.
      if (x >= cur * step - 6 && x < (cur + 1) * step - 6) return;
      const col = Math.max(0, Math.floor((x + step * 0.5) / step));
      if (Number.isFinite(col) && col !== cur) {
        setReaderColIndex(col);
      }
    });
    return () => window.cancelAnimationFrame(raf);
  }, [readerAudioPlayingWid, readerAudioPlayActive, readerColUsesEngine]); // eslint-disable-line react-hooks/exhaustive-deps

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
    g.t = (typeof performance !== 'undefined' && performance.now) ? performance.now() : 0;
    g.base = -readerColIndexRef.current * readerColStep();
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch (_err) { /* ignore */ }
    applyReaderColTransform(g.base, false);
    // Палец на слове — заводим удержание. Пока оно не сработало, жест обычный:
    // тап переводит слово, движение листает страницу.
    beginReaderPhraseDrag({ clientX: e.clientX, clientY: e.clientY, target: e.target });
  };
  const onReaderColPointerMove = (e) => {
    const g = readerGestureRef.current;
    if (!g.down) return;
    // Слово придержали → палец тянет выделение по словам, страница стоит на месте.
    if (moveReaderPhraseDrag({ clientX: e.clientX, clientY: e.clientY })) {
      applyReaderColTransform(g.base, false);
      return;
    }
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
    // Прерванный системой жест (звонок, шторка) выделение не подтверждает.
    if (e?.type === 'pointercancel') {
      cancelReaderPhraseDrag();
      applyReaderColTransform(-readerColIndexRef.current * readerColStep(), true);
      return;
    }
    // Отпустили после удержания → открываем перевод выделенного, страницу не листаем.
    if (endReaderPhraseDrag({ clientX: e.clientX, clientY: e.clientY })) {
      applyReaderColTransform(-readerColIndexRef.current * readerColStep(), true);
      return;
    }
    const dx = e.clientX - g.x, dy = e.clientY - g.y;
    if (!g.moved) { handleReaderColTap(e); return; }
    // Turn on either a committed DRAG (>40px) OR a quick FLICK (high velocity, even
    // if short) — the flick makes page-turns feel instant/light like Apple Books,
    // instead of "dead" when a fast swipe travels under 40px.
    const now = (typeof performance !== 'undefined' && performance.now) ? performance.now() : 0;
    const dt = Math.max(1, now - (g.t || now));
    const vx = dx / dt; // px per ms
    const isFlick = Math.abs(vx) > 0.45 && Math.abs(dx) > 8;
    if (Math.abs(dx) >= Math.abs(dy) && (Math.abs(dx) > 40 || isFlick)) {
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
  // ┌─ FIXED 27.08.2026. НЕ ПОДНИМАТЬ КАК НОВУЮ НАХОДКУ. ─────────────────────────┐
  // │ The ribbon used to key off isCurrentReaderPageBookmarked — the SERVER page.  │
  // │ The column engine cuts text by real screen width, so one ~1100-char server   │
  // │ page covers 2-3 screens, and the ribbon (a child of the fixed viewport, not  │
  // │ of the sliding track) stayed pinned to all of them: "закладка на 2 страницах"│
  // │ reported 27.08.2026. It now keys off the measured bookmark COLUMN, so it     │
  // │ shows on exactly one screen at any font size. isCurrentReaderPageBookmarked  │
  // │ remains only for the non-engine page-sheet renderer, where page == screen.   │
  // └─────────────────────────────────────────────────────────────────────────────┘
  const readerBookmarkOnThisScreen = readerColUsesEngine
    ? (readerBookmarkCol !== null && readerBookmarkCol === readerColIndex)
    : isCurrentReaderPageBookmarked;
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
        // Books stay in the permanent library; web articles are read-once and shown
        // in a separate auto-cleaning «Недавние статьи» group so they don't clutter it.
        const libraryBooks = visibleLibraryItems.filter((it) => String(it?.source_type || '') !== 'html');
        const libraryArticles = visibleLibraryItems.filter((it) => String(it?.source_type || '') === 'html');
        const renderReaderLibCard = (item) => {
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
                <button
                  type="button"
                  className={`reader-lib-more${readerLibActionsOpenId === item.id ? ' is-open' : ''}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    setReaderLibActionsOpenId((cur) => (cur === item.id ? null : item.id));
                  }}
                  title={readerLibActionsOpenId === item.id ? tr('Закрыть', 'Schließen') : tr('Ещё', 'Mehr')}
                  aria-label={readerLibActionsOpenId === item.id ? tr('Закрыть', 'Schließen') : tr('Действия', 'Aktionen')}
                >
                  {readerLibActionsOpenId === item.id ? (
                    <svg viewBox="0 0 18 18" fill="none" aria-hidden="true">
                      <path d="M5 5l8 8M13 5l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                    </svg>
                  ) : (
                    <svg viewBox="0 0 18 18" fill="currentColor" aria-hidden="true">
                      <circle cx="9" cy="4" r="1.45" /><circle cx="9" cy="9" r="1.45" /><circle cx="9" cy="14" r="1.45" />
                    </svg>
                  )}
                </button>
                <div className="reader-library-cover-progress" style={{ width: `${progress}%` }} />
                {isOpening && (
                  <div className="reader-library-cover-loading">
                    <svg className="reader-lib-spinner" viewBox="0 0 24 24" fill="none">
                      <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="42 14" />
                    </svg>
                  </div>
                )}
                {readerLibActionsOpenId === item.id && (
                  <div
                    className="reader-lib-cover-actions"
                    onClick={(e) => { e.stopPropagation(); if (e.target === e.currentTarget) setReaderLibActionsOpenId(null); }}
                  >
                    <button
                      type="button"
                      className="reader-lib-cover-act"
                      disabled={isOpening}
                      onClick={(e) => { e.stopPropagation(); openReaderDocument(item.id); }}
                      title={tr('Открыть книгу', 'Buch öffnen')}
                    >
                      <svg viewBox="0 0 24 24" fill="none"><path d="M5 12h12.5M12 6l6 6-6 6" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" /></svg>
                      <span>{tr('Открыть', 'Lesen')}</span>
                    </button>
                    <button
                      type="button"
                      className="reader-lib-cover-act"
                      onClick={(e) => { e.stopPropagation(); renameReaderDocument(item.id, item.title); }}
                      title={tr('Переименовать', 'Umbenennen')}
                    >
                      <svg viewBox="0 0 24 24" fill="none"><path d="M16.4 4.6a2 2 0 0 1 2.83 2.83L9.5 17.16l-3.9.9.9-3.9 9.9-9.56Z" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /><path d="M14.5 6.5 17.5 9.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" /></svg>
                      <span>{tr('Название', 'Titel')}</span>
                    </button>
                    <button
                      type="button"
                      className="reader-lib-cover-act"
                      onClick={(e) => { e.stopPropagation(); archiveReaderDocument(item.id, !Boolean(item?.is_archived)); }}
                      title={Boolean(item?.is_archived) ? tr('Разархивировать', 'Wiederherstellen') : tr('В архив', 'Archivieren')}
                    >
                      {Boolean(item?.is_archived) ? (
                        <svg viewBox="0 0 24 24" fill="none"><path d="M19.5 8.5V4.5h-4" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /><path d="M19 11a7 7 0 1 1-1.7-4.2" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /></svg>
                      ) : (
                        <svg viewBox="0 0 24 24" fill="none"><rect x="4" y="5" width="16" height="4.2" rx="1.2" stroke="currentColor" strokeWidth="1.7" /><path d="M5.5 9.2 6.2 19h11.6l.7-9.8" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /><path d="M10 13h4" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" /></svg>
                      )}
                      <span>{Boolean(item?.is_archived) ? tr('Вернуть', 'Zurück') : tr('Скрыть', 'Ausblenden')}</span>
                    </button>
                    <button
                      type="button"
                      className="reader-lib-cover-act is-danger"
                      onClick={(e) => { e.stopPropagation(); deleteReaderDocument(item.id); }}
                      title={tr('Удалить', 'Löschen')}
                    >
                      <svg viewBox="0 0 24 24" fill="none"><path d="M5 7h14M9 7V5.6A1.6 1.6 0 0 1 10.6 4h2.8A1.6 1.6 0 0 1 15 5.6V7M6.6 7 7.2 19h9.6L17.4 7" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /><path d="M10 10.5v5M14 10.5v5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" /></svg>
                      <span>{tr('Удалить', 'Löschen')}</span>
                    </button>
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
            </div>
          );
        };

        if (showLibraryMode) {
          // ════════════════════════════════════════════════════════════════
          //  LIBRARY MODE — 3 shelves overview → tap → category grid
          // ════════════════════════════════════════════════════════════════
          const notArchivedBase = readerDocuments.filter((item) => {
            if (!readerIncludeArchived && Boolean(item?.is_archived)) return false;
            return true;
          });
          const allBooks = notArchivedBase.filter((it) => String(it?.source_type || '') !== 'html');
          const allArticles = notArchivedBase.filter((it) => String(it?.source_type || '') === 'html');
          const publicItems = Array.isArray(readerPublicDocuments) ? readerPublicDocuments : [];
          const shelfDefs = [
            {
              key: 'mine', accent: 'mine', emoji: '📚',
              name: tr('Мои книги', 'Meine Bücher'),
              desc: tr('Загруженные книги и PDF', 'Hochgeladene Bücher & PDFs'),
              items: allBooks,
            },
            {
              key: 'classics', accent: 'classics', emoji: '🏛️',
              name: tr('Классика', 'Klassiker'),
              desc: tr('Бесплатно, с озвучкой', 'Kostenlos, mit Audio'),
              items: publicItems,
            },
            {
              key: 'articles', accent: 'articles', emoji: '📰',
              name: tr('Статьи', 'Artikel'),
              desc: tr('Из интернета, очищаются сами', 'Aus dem Web, selbstreinigend'),
              items: allArticles,
            },
          ];
          const activeShelf = readerLibraryShelf
            ? (shelfDefs.find((s) => s.key === readerLibraryShelf) || null)
            : null;
          const backToShelves = () => { setReaderLibraryShelf(null); setReaderLibActionsOpenId(null); };

          // Public-domain «Классика» card (read-only, level chip, no ⋯ actions).
          const renderReaderPublicCard = (item) => {
            const coverUrl = getReaderCoverUrl(item);
            const initials = getReaderCoverInitials(item?.title);
            const gradient = getReaderCoverGradient(item);
            const isOpening = Number(readerOpeningDocumentId) === Number(item.id);
            const pubProgress = Math.max(0, Math.min(100, Number(item?.progress_percent || 0)));
            return (
              <div
                key={`reader-public-${item.id}`}
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
                  {item.level && <span className="reader-library-level-chip">{item.level}</span>}
                  {pubProgress > 0 && (
                    <div className="reader-library-cover-progress" style={{ width: `${pubProgress}%` }} />
                  )}
                  {isOpening && (
                    <div className="reader-library-cover-loading">
                      <span className="reader-library-cover-spinner" />
                    </div>
                  )}
                </div>
                <div className="reader-library-card-body" style={{ cursor: 'pointer' }}>
                  <div className="reader-library-title">{item.title || tr('Без названия', 'Ohne Titel')}</div>
                  <div className="reader-library-meta">
                    {pubProgress > 0 && <span>{Math.round(pubProgress)}%</span>}
                    {item.public_author && <span>{item.public_author}</span>}
                  </div>
                </div>
              </div>
            );
          };

          // One wide tappable shelf card with a fanned preview of real covers.
          const renderShelfCard = (shelf, idx) => {
            const covers = (shelf.items || []).slice(0, 3);
            const count = (shelf.items || []).length;
            return (
              <button
                key={shelf.key}
                type="button"
                className={`reader-shelf-card is-${shelf.accent}`}
                style={{ animationDelay: `${idx * 70}ms` }}
                onClick={() => { setReaderLibraryShelf(shelf.key); setReaderLibActionsOpenId(null); }}
              >
                <span className="reader-shelf-stack" aria-hidden="true">
                  {covers.length ? covers.map((it, i) => {
                    const url = getReaderCoverUrl(it);
                    const grad = getReaderCoverGradient(it);
                    const initials = getReaderCoverInitials(it?.title);
                    return (
                      <span
                        key={it.id}
                        className="reader-shelf-mini"
                        style={{ background: `linear-gradient(150deg, ${grad[0]} 0%, ${grad[1]} 100%)`, zIndex: 5 - i }}
                      >
                        {url ? <img src={url} alt="" loading="lazy" /> : <span className="reader-shelf-mini-ini">{initials}</span>}
                      </span>
                    );
                  }) : (
                    <span className="reader-shelf-emoji">{shelf.emoji}</span>
                  )}
                </span>
                <span className="reader-shelf-main">
                  <span className="reader-shelf-name">
                    <span className="reader-shelf-badge" aria-hidden="true">{shelf.emoji}</span>
                    {shelf.name}
                    <span className="reader-shelf-count">{count}</span>
                  </span>
                  <span className="reader-shelf-desc">{shelf.desc}</span>
                </span>
                <svg className="reader-shelf-chev" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                  <path d="M9 6l6 6-6 6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>
            );
          };

          return (
            <div className="reader-library-mode" data-shelf={activeShelf ? activeShelf.key : 'overview'}>

              {/* ── Library header — shelf-aware (back + dynamic title) ── */}
              <div className="reader-lib-header">
                <div className="reader-lib-header-lead">
                  {(activeShelf || readerArchiveOpen) && (
                    <button
                      type="button"
                      className="reader-shelf-back"
                      onClick={() => {
                        if (readerArchiveOpen) {
                          setReaderArchiveOpen(false);
                          setReaderIncludeArchived(false);
                          loadReaderLibrary();
                        } else {
                          backToShelves();
                        }
                      }}
                      aria-label={tr('К полкам', 'Zu den Regalen')}
                      title={tr('К полкам', 'Zu den Regalen')}
                    >
                      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
                        <path d="M15 6l-6 6 6 6" stroke="currentColor" strokeWidth="2.1" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </button>
                  )}
                  <h2 className="reader-lib-header-title">
                    {activeShelf
                      ? activeShelf.name
                      : (readerArchiveOpen ? tr('Архив', 'Archiv') : tr('Моя библиотека', 'Meine Bibliothek'))}
                  </h2>
                  {activeShelf && <span className="reader-shelf-title-count">{(activeShelf.items || []).length}</span>}
                </div>
                <div className="reader-lib-header-actions">
                  <button
                    type="button"
                    className="reader-lib-icon-btn reader-lib-help-btn"
                    onClick={() => setReaderGuideOpen(true)}
                    title={tr('Как читать и слушать', 'Lesen & Hören')}
                    aria-label={tr('Как читать и слушать', 'Lesen & Hören')}
                  >
                    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="9.2" />
                      <path d="M9.4 9.2a2.7 2.7 0 0 1 5.2.9c0 1.8-2.6 2.4-2.6 3.9" />
                      <circle cx="12" cy="17.4" r="0.15" fill="currentColor" />
                    </svg>
                  </button>
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
                  {activeShelf?.key !== 'classics' && (
                    <button
                      type="button"
                      className={`reader-lib-add-btn${readerAddOpen ? ' is-open' : ''}`}
                      onClick={() => setReaderAddOpen((prev) => !prev)}
                      aria-label={readerAddOpen ? tr('Скрыть', 'Schließen') : tr('Добавить', 'Hinzufügen')}
                    >
                      <span className="reader-lib-add-ico" aria-hidden="true">{readerAddOpen ? '✕' : '+'}</span>
                      <span className="reader-lib-add-label">{readerAddOpen ? tr('Скрыть', 'Schließen') : tr('Добавить', 'Hinzufügen')}</span>
                    </button>
                  )}
                </div>
              </div>

              {/* ── Add form: paste URL/text → Открыть, OR pick a file → opens instantly ── */}
              {readerAddOpen && activeShelf?.key !== 'classics' && (
                <div className="reader-add-form-wrap">
                  <form className="reader-add-form" onSubmit={handleReaderIngest}>
                    <textarea
                      className="reader-add-textarea"
                      rows={2}
                      value={readerInput}
                      onChange={(event) => setReaderInput(event.target.value)}
                      placeholder={tr(
                        'Вставь ссылку на статью или книгу (PDF), либо сам текст',
                        'Link zu einem Artikel/Buch (PDF) oder den Text einfügen'
                      )}
                    />
                    <div className="reader-add-row">
                      <button
                        type="button"
                        className="reader-add-file-btn"
                        onClick={() => readerFileInputRef.current?.click()}
                        disabled={readerLoading}
                      >
                        <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
                          <path d="M10 4.5v11M4.5 10h11" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
                        </svg>
                        {readerLoading ? tr('Открываем…', 'Öffnen…') : tr('Файл с телефона', 'Datei vom Telefon')}
                      </button>
                      <button
                        type="submit"
                        className="reader-add-submit"
                        disabled={readerLoading || !String(readerInput || '').trim()}
                      >
                        {tr('Открыть', 'Öffnen')}
                      </button>
                    </div>
                    <button
                      type="button"
                      className={`reader-add-sources${readerSourcesOpen ? ' is-open' : ''}`}
                      onClick={openReaderSourcesPanel}
                    >
                      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
                        <circle cx="10" cy="10" r="7.2" stroke="currentColor" strokeWidth="1.5" />
                        <path d="M2.8 10h14.4M10 2.8c2 2 2 12.4 0 14.4M10 2.8c-2 2-2 12.4 0 14.4" stroke="currentColor" strokeWidth="1.3" />
                      </svg>
                      <span className="reader-add-sources-label">
                        {tr('Источники', 'Quellen')}
                        <span className="reader-add-sources-sub">{tr('DW, Tagesschau — открыть статью сразу', 'DW, Tagesschau — Artikel direkt öffnen')}</span>
                      </span>
                      <svg className="reader-add-sources-chev" viewBox="0 0 20 20" fill="none" aria-hidden="true">
                        <path d="M6 8l4 4 4-4" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </button>

                    {readerSourcesOpen && (
                      <div className="reader-sources-panel">
                        <div className="reader-sources-tabs">
                          {readerSources.map((s) => (
                            <button
                              key={s.id}
                              type="button"
                              className={`reader-sources-tab${s.id === readerSourceId ? ' is-active' : ''}`}
                              onClick={() => selectReaderSource(s.id)}
                            >
                              <span className="reader-sources-tab-name">{s.name}</span>
                              {s.level ? <span className="reader-sources-tab-level">{s.level}</span> : null}
                            </button>
                          ))}
                        </div>
                        {readerSourcesError ? (
                          <div className="reader-sources-empty">{readerSourcesError}</div>
                        ) : readerSourcesLoading ? (
                          <div className="reader-sources-empty">{tr('Загружаем свежие статьи…', 'Frische Artikel werden geladen…')}</div>
                        ) : readerSourceArticles.length ? (
                          <ul className="reader-sources-list">
                            {readerSourceArticles.map((a) => {
                              const d = a.published_ts ? new Date(a.published_ts * 1000) : null;
                              const when = d
                                ? `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}`
                                : '';
                              const activeName = (readerSources.find((s) => s.id === readerSourceId) || {}).name || '';
                              return (
                                <li key={a.url}>
                                  <button
                                    type="button"
                                    className="reader-source-item"
                                    onClick={() => openReaderSourceArticle(a)}
                                    disabled={readerLoading}
                                  >
                                    {a.image ? (
                                      <img className="reader-source-thumb" src={a.image} alt="" loading="lazy" />
                                    ) : (
                                      <span className="reader-source-thumb reader-source-thumb-letter">{(activeName || '?').slice(0, 1)}</span>
                                    )}
                                    <span className="reader-source-text">
                                      <span className="reader-source-title">{a.title}</span>
                                      <span className="reader-source-meta">
                                        {activeName}{when ? ` · ${when}` : ''}
                                      </span>
                                      {a.summary ? <span className="reader-source-summary">{a.summary}</span> : null}
                                    </span>
                                  </button>
                                </li>
                              );
                            })}
                          </ul>
                        ) : (
                          <div className="reader-sources-empty">
                            {tr('Пока нет статей из этого источника. Попробуйте другой.',
                                'Aktuell keine Artikel aus dieser Quelle. Versuche eine andere.')}
                          </div>
                        )}
                      </div>
                    )}

                    <button
                      type="button"
                      className="reader-add-search"
                      onClick={openReaderArticleSearch}
                    >
                      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
                        <circle cx="10" cy="10" r="7.2" stroke="currentColor" strokeWidth="1.5" />
                        <path d="M2.8 10h14.4M10 2.8c2 2 2 12.4 0 14.4M10 2.8c-2 2-2 12.4 0 14.4" stroke="currentColor" strokeWidth="1.3" />
                      </svg>
                      {tr('Другой сайт — найти в интернете', 'Andere Seite — im Internet finden')}
                    </button>
                    {readerArticleClipUrl && (
                      <div className="reader-add-clip">
                        <div className="reader-add-clip-text">
                          <span className="reader-add-clip-label">{tr('Скопированная ссылка', 'Kopierter Link')}</span>
                          <span className="reader-add-clip-url">{readerArticleClipUrl}</span>
                        </div>
                        <button type="button" className="reader-add-clip-open" onClick={openReaderArticleClipUrl}>
                          {tr('Открыть', 'Öffnen')}
                        </button>
                        <button
                          type="button"
                          className="reader-add-clip-x"
                          onClick={dismissReaderArticleClip}
                          aria-label={tr('Закрыть', 'Schließen')}
                        >✕</button>
                      </div>
                    )}
                    <input
                      ref={readerFileInputRef}
                      type="file"
                      className="reader-add-file-input"
                      accept=".txt,.md,.pdf,.epub,text/plain,application/pdf,application/epub+zip"
                      onChange={handleReaderFileSelect}
                    />
                    <div className="reader-add-hint">
                      {tr('Файл откроется сразу. Из браузера — скопируй ссылку и вернись, мы её подхватим.',
                          'Die Datei öffnet sofort. Aus dem Browser den Link kopieren und zurückkehren — wir übernehmen ihn.')}
                      {' '}
                      <button type="button" className="reader-add-paste" onClick={pasteReaderClipboardUrl}>
                        {tr('Вставить из буфера', 'Aus Zwischenablage')}
                      </button>
                    </div>
                  </form>
                  {readerError && (
                    (readerErrorCode === 'LIMIT_FREE_PLAN_1_BOOK' || readerErrorCode === 'LIMIT_FREE_PLAN_1_ARTICLE') ? (
                      <div className="reader-upsell">
                        <div className="reader-upsell-head">
                          <div className="reader-upsell-ico" aria-hidden="true">
                            {readerErrorCode === 'LIMIT_FREE_PLAN_1_ARTICLE' ? '📰' : '📚'}
                          </div>
                          <div className="reader-upsell-titles">
                            <span className="reader-upsell-badge">👑 {tr('Полный доступ', 'Voller Zugang')}</span>
                            <div className="reader-upsell-title">
                              {readerErrorCode === 'LIMIT_FREE_PLAN_1_ARTICLE'
                                ? tr('Статьи без лимита', 'Artikel ohne Limit')
                                : tr('Свои книги — в Полном доступе', 'Eigene Bücher — mit vollem Zugang')}
                            </div>
                          </div>
                        </div>
                        <p className="reader-upsell-body">{readerError}</p>
                        <button
                          type="button"
                          className="reader-upsell-cta"
                          onClick={() => handleBillingUpgrade('pro')}
                          disabled={billingActionLoading}
                        >
                          {billingActionLoading
                            ? tr('Открываем…', 'Öffnen…')
                            : <>✨ {tr('Оформить полный доступ', 'Vollen Zugang holen')}</>}
                        </button>
                      </div>
                    ) : (
                      <div className="webapp-error"><span>{readerError}</span></div>
                    )
                  )}
                </div>
              )}

              {/* ── Overview only: Hero «Продолжаешь читать» ──── */}
              {!activeShelf && !readerArchiveOpen && (() => {
                let candidate = null;
                // Classics count too — but only once the user actually STARTED one
                // (progress > 0), so a never-opened classic can't hijack the hero.
                const startedPublic = publicItems.filter((d) =>
                  Number(d?.progress_percent || 0) > 0 && Number(d?.progress_percent || 0) < 100
                );
                if (readerDocumentId) {
                  candidate = [...readerDocuments, ...publicItems].find(
                    (d) => Number(d?.id) === Number(readerDocumentId) && !d?.is_archived
                  );
                }
                if (!candidate) {
                  const notFinished = [
                    ...readerDocuments.filter((d) =>
                      !d?.is_archived &&
                      Number(d?.progress_percent || 0) < 100
                    ),
                    ...startedPublic,
                  ]
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

              {/* ── Overview: three tappable shelves ──────────────── */}
              {!activeShelf && !readerArchiveOpen && (
                <div className="reader-shelves">
                  {shelfDefs.map(renderShelfCard)}
                  <button
                    type="button"
                    className="reader-shelf-guide-link"
                    onClick={() => setReaderGuideOpen(true)}
                  >
                    <span className="reader-shelf-guide-emoji" aria-hidden="true">💡</span>
                    {tr('Как читать и слушать?', 'Wie lese & höre ich?')}
                  </button>
                </div>
              )}

              {/* ── Archive view: flat grid (opened via «Архив») ──── */}
              {readerArchiveOpen && (
                <section className="reader-library reader-shelf-detail">
                  <div className="reader-lib-controls">
                    <div className="reader-lib-search-wrap">
                      <svg className="reader-lib-search-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                        <circle cx="11" cy="11" r="7" stroke="currentColor" strokeWidth="1.8" />
                        <path d="M21 21l-4.3-4.3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                      </svg>
                      <input
                        type="text"
                        className="reader-lib-search"
                        value={readerLibrarySearch}
                        onChange={(event) => setReaderLibrarySearch(event.target.value)}
                        placeholder={tr('Поиск по библиотеке…', 'Suche in Bibliothek…')}
                      />
                    </div>
                  </div>
                  {readerLibraryError && <div className="webapp-error">{readerLibraryError}</div>}
                  {!readerLibraryError && visibleLibraryItems.length === 0 && (
                    <div className="webapp-muted">{tr('Архив пуст.', 'Archiv ist leer.')}</div>
                  )}
                  {visibleLibraryItems.length > 0 && (
                    <div className="reader-library-grid">
                      {visibleLibraryItems.map(renderReaderLibCard)}
                    </div>
                  )}
                </section>
              )}

              {/* ── Shelf detail: Мои книги ───────────────────────── */}
              {activeShelf?.key === 'mine' && (
                <section className="reader-library reader-shelf-detail">
                  <div className="reader-lib-controls">
                    <div className="reader-lib-search-wrap">
                      <svg className="reader-lib-search-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                        <circle cx="11" cy="11" r="7" stroke="currentColor" strokeWidth="1.8" />
                        <path d="M21 21l-4.3-4.3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                      </svg>
                      <input
                        type="text"
                        className="reader-lib-search"
                        value={readerLibrarySearch}
                        onChange={(event) => setReaderLibrarySearch(event.target.value)}
                        placeholder={tr('Поиск по книгам…', 'Bücher durchsuchen…')}
                      />
                    </div>
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
                  {!readerLibraryError && libraryBooks.length === 0 && (
                    <div className="reader-shelf-empty">
                      <span className="reader-shelf-empty-emoji" aria-hidden="true">📚</span>
                      <span>{tr('Здесь появятся ваши книги. Нажмите «+ Добавить».',
                              'Hier erscheinen deine Bücher. Tippe auf «+ Hinzufügen».')}</span>
                    </div>
                  )}
                  {libraryBooks.length > 0 && (
                    <div className="reader-library-grid">
                      {libraryBooks.map(renderReaderLibCard)}
                    </div>
                  )}
                </section>
              )}

              {/* ── Shelf detail: Классика (public-domain, free) ──── */}
              {activeShelf?.key === 'classics' && (
                <section className="reader-library reader-shelf-detail reader-library-public">
                  <div className="reader-shelf-hint">
                    <span className="reader-shelf-hint-badge is-classics" aria-hidden="true">🏛️</span>
                    <span>{tr('Классика в свободном доступе — читай и слушай бесплатно.',
                            'Gemeinfreie Klassiker — kostenlos lesen und hören.')}</span>
                  </div>
                  {publicItems.length === 0 ? (
                    <div className="reader-shelf-empty">
                      <span className="reader-shelf-empty-emoji" aria-hidden="true">🏛️</span>
                      <span>{tr('Коллекция скоро появится.', 'Die Sammlung erscheint bald.')}</span>
                    </div>
                  ) : (
                    <div className="reader-library-grid">
                      {publicItems.map(renderReaderPublicCard)}
                    </div>
                  )}
                </section>
              )}

              {/* ── Shelf detail: Статьи (web, auto-cleaning) ─────── */}
              {activeShelf?.key === 'articles' && (
                <section className="reader-library reader-shelf-detail">
                  <div className="reader-shelf-hint">
                    <span className="reader-shelf-hint-badge is-articles" aria-hidden="true">📰</span>
                    <span>{tr('Статьи из интернета. Хранятся недолго и очищаются автоматически.',
                            'Web-Artikel. Werden nur kurz gespeichert und automatisch geleert.')}</span>
                  </div>
                  {libraryArticles.length === 0 ? (
                    <div className="reader-shelf-empty">
                      <span className="reader-shelf-empty-emoji" aria-hidden="true">📰</span>
                      <span>{tr('Откройте статью из «Источников» — она появится здесь.',
                              'Öffne einen Artikel aus «Quellen» — er erscheint hier.')}</span>
                    </div>
                  ) : (
                    <div className="reader-library-grid">
                      {libraryArticles.map(renderReaderLibCard)}
                    </div>
                  )}
                </section>
              )}

              {/* Offline whole-document audio panel removed — we don't offer that. */}

              <ReaderGuideModal
                isOpen={readerGuideOpen}
                onClose={() => setReaderGuideOpen(false)}
                tr={tr}
              />
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
                  {readerCanUseOriginal && (
                    <button
                      type="button"
                      className="reader-topbar-icbtn reader-topbar-original"
                      onClick={onOpenReaderOriginal}
                      title={tr('Оригинал страницы', 'Originalseite')}
                      aria-label={tr('Оригинал страницы', 'Originalseite')}
                    >
                      <svg viewBox="0 0 18 18" fill="none">
                        <rect x="2.75" y="3" width="12.5" height="12" rx="1.7" stroke="currentColor" strokeWidth="1.5" />
                        <circle cx="6.5" cy="7" r="1.15" fill="currentColor" />
                        <path d="M3 12.5l3.4-3.2 2.3 2.1 2.9-2.7 3.4 3.3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </button>
                  )}
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
                    {readerBookmarkOnThisScreen && (
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
                    className={`reader-dock-btn ${readerBookmarkOnThisScreen ? 'is-active' : ''}`}
                    onClick={() => {
                      const mark = computeReaderProgressPercent();
                      // Exact anchor for the column engine. The epub.js renderer has no
                      // server pages to anchor to, so it saves page/char 0 — which the
                      // reader reads as "page precision only", never as position 0.
                      const anchor = readerColUsesEngine ? readerColBookmarkAnchorNow() : null;
                      setReaderBookmarkPercent(mark);
                      persistReaderExactBookmark(readerCurrentPage);
                      setReaderBookmarkAnchorSaved(anchor);
                      setReaderBookmarkCol(anchor ? readerColIndexRef.current : null);
                      if (readerDocumentId) {
                        syncReaderState({
                          bookmark_percent: Number(mark.toFixed(2)),
                          // Always sent: 0/0 CLEARS a stale anchor left by an earlier
                          // bookmark, so percent and anchor can never disagree.
                          bookmark_page: anchor ? anchor.page : 0,
                          bookmark_char: anchor ? anchor.char : 0,
                        });
                      }
                    }}
                    disabled={!readerContent || !readerDocumentId}
                    title={tr('Поставить закладку', 'Lesezeichen setzen')}
                    aria-label={tr('Поставить закладку', 'Lesezeichen setzen')}
                  >
                    <svg viewBox="0 0 18 18" fill="none"><path d="M5.25 3.75h7.5a.75.75 0 0 1 .75.75v9.75L9 11.55l-4.5 2.7V4.5a.75.75 0 0 1 .75-.75Z" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" /></svg>
                  </button>
                  {readerBookmarkPercent > 0 && !readerBookmarkOnThisScreen && (
                    <button
                      type="button"
                      className="reader-dock-btn reader-dock-bmjump"
                      onClick={() => {
                        if (readerUsesOriginalEpubLayout) { applyReaderProgressPercent(readerBookmarkPercent); return; }
                        // Bookmark already measured inside the loaded window → turn
                        // straight to its column. Needed on its own: the bookmark is
                        // often on the SAME server page, and setReaderCurrentPage to
                        // the page we are already on changes nothing and would leave
                        // the button dead.
                        if (readerBookmarkCol !== null) { setReaderColIndex(readerBookmarkCol); return; }
                        // Bookmark lives outside the loaded window: move the window to
                        // its page, and let the next measurement land on the character.
                        if (readerBookmarkAnchor) readerColPendingAnchorRef.current = { ...readerBookmarkAnchor };
                        setReaderCurrentPage(readerBookmarkAnchor ? readerBookmarkAnchor.page : readerBookmarkPage);
                      }}
                      title={tr('Перейти к закладке', 'Zur Lesezeiche springen')}
                      aria-label={tr('Перейти к закладке', 'Zur Lesezeiche springen')}
                    >
                      <svg viewBox="0 0 18 18" fill="none"><path d="M9 3.5v8M9 11.5 6.4 8.9M9 11.5l2.6-2.6M4.5 14h9" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
                    </button>
                  )}
                  {!readerIsArticle && (
                  <button
                    type="button"
                    className={`reader-dock-play-flat${readerAudioAwaitingWordTap ? ' is-awaiting' : ''}`}
                    // No blanket Pro pre-gate: «Классика» (public books) is voiced free
                    // for everyone by design — the play handler streams it via
                    // /reader/audio/page, and a personal book resolves to its per-book
                    // Stars unlock plaque via the backend 402 (audio_unlock_required).
                    // Gating here blocked classics too, contradicting the free-classic promise.
                    onClick={onReaderAudioPlayBtn}
                    disabled={!readerHasContent || readerAudioPlayLoading || billingActionLoading}
                    title={readerAudioAwaitingWordTap ? tr('Нажми слово…', 'Wort antippen…') : tr('Слушать', 'Hören')}
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
                  )}
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

            {/* ── Audio error — soft, closable banner (our style) ── */}
            {readerAudioPlayError && !readerAudioPlayActive && !readerIsArticle && (() => {
              const isLimit = readerAudioPlayError === 'reader_audio_monthly_limit_exceeded';
              const title = isLimit
                ? tr('Лимит озвучки', 'Vertonungslimit')
                : tr('Не удалось озвучить', 'Vertonung fehlgeschlagen');
              const text = isLimit
                ? tr('Бесплатная озвучка на этот месяц закончилась. Попробуй в следующем месяце.',
                     'Die kostenlose Vertonung für diesen Monat ist aufgebraucht. Versuch es nächsten Monat.')
                : readerAudioPlayError;
              return (
                <div className={`reader-audio-banner${isLimit ? ' is-limit' : ''}`} role="alert">
                  <span className="reader-audio-banner-icon" aria-hidden="true">{isLimit ? '🎧' : '⚠️'}</span>
                  <span className="reader-audio-banner-body">
                    <span className="reader-audio-banner-title">{title}</span>
                    <span className="reader-audio-banner-text">{text}</span>
                  </span>
                  {!isLimit && typeof onReaderAudioPlayBtn === 'function' && (
                    <button
                      type="button"
                      className="reader-audio-banner-retry"
                      onClick={() => { dismissReaderAudioPlayError(); onReaderAudioPlayBtn(); }}
                    >
                      {tr('Ещё раз', 'Nochmal')}
                    </button>
                  )}
                  <button
                    type="button"
                    className="reader-audio-banner-close"
                    onClick={dismissReaderAudioPlayError}
                    aria-label={tr('Закрыть', 'Schließen')}
                  >×</button>
                </div>
              );
            })()}

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
