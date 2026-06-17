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
    return () => { window.removeEventListener('resize', fit); ro?.disconnect(); };
  }, [fit]);

  return ref;
}
