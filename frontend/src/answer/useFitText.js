import { useCallback, useLayoutEffect, useRef } from 'react';

// Shrink a one-line text element so it always fits its container's width on ONE
// line (no ugly mid-phrase wrap), on any screen. Attach the returned ref to an
// inline-block element with `white-space: nowrap` (e.g. class "fit-line"); it is
// re-fit whenever `dep` changes and on window resize.
export default function useFitText(dep, { max = 40, min = 14, padding = 28 } = {}) {
  const ref = useRef(null);
  const fit = useCallback(() => {
    const el = ref.current;
    const box = el?.parentElement;
    if (!el || !box) return;
    let size = max;
    el.style.fontSize = `${size}px`;
    const avail = box.clientWidth - padding;
    let guard = 0;
    while (el.scrollWidth > avail && size > min && guard < 80) {
      size -= 1;
      el.style.fontSize = `${size}px`;
      guard += 1;
    }
  }, [max, min, padding]);

  // re-fit on content change (next item) — layout effect avoids a flash
  useLayoutEffect(() => { fit(); }, [dep, fit]);
  // re-fit on viewport resize / rotation AND whenever the container's own width
  // settles (Telegram WebApp sheet animates in → the box width isn't final on first
  // layout; a plain window-resize listener doesn't catch that). ResizeObserver fires
  // an initial callback on observe, so this also covers the post-mount width.
  useLayoutEffect(() => {
    window.addEventListener('resize', fit);
    let ro;
    const box = ref.current?.parentElement;
    if (box && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(() => fit());
      ro.observe(box);
    }
    // Re-fit once web fonts finish loading. The first measurement runs with a
    // fallback font (narrower) → text "fits" and isn't shrunk; when Manrope/Onest
    // load the text gets wider and overflows, but nothing else re-triggers fit.
    let cancelled = false;
    const reFit = () => { if (!cancelled) fit(); };
    const fontSet = typeof document !== 'undefined' ? document.fonts : null;
    if (fontSet) {
      try { fontSet.ready.then(reFit); } catch (_e) { /* ignore */ }
      try { fontSet.addEventListener('loadingdone', reFit); } catch (_e) { /* ignore */ }
    }
    return () => {
      cancelled = true;
      window.removeEventListener('resize', fit);
      ro?.disconnect();
      if (fontSet) {
        try { fontSet.removeEventListener('loadingdone', reFit); } catch (_e) { /* ignore */ }
      }
    };
  }, [fit]);

  return ref;
}
