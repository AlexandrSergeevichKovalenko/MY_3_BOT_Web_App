import { useCallback, useLayoutEffect, useRef } from 'react';

// Один общий canvas на весь роут — только для замера ширины текста, в DOM не попадает.
let measureCanvas = null;
function fitCanvas() {
  if (!measureCanvas) measureCanvas = document.createElement('canvas');
  return measureCanvas;
}

// Shrink a one-line text element so it always fits its container's width on ONE
// line (no ugly mid-phrase wrap), on any screen. Attach the returned ref to an
// inline-block element with `white-space: nowrap` (e.g. class "fit-line"); it is
// re-fit whenever `dep` changes and on window resize.
// `max: 'css'` — потолок берём из самой вёрстки: размер НИКОГДА не больше того, что задал
// CSS (а он у нас считается от экрана и на планшете свой), хук только уменьшает, если слово
// не влезло. Так подгонка кегля не ломает согласованную типографику — она её страхует.
// `fitBy: 'word'` — влезать должно САМОЕ ДЛИННОЕ СЛОВО, а не вся фраза в одну строку.
// Тогда «die Zustimmung» в узкой колонке планшета спокойно переносится на две строки
// крупным кеглем, а «die Geschwindigkeitsbegrenzung» ужимается — ровно настолько, чтобы
// не разрезаться посередине. Режим по умолчанию (вся фраза в одну строку) не тронут.
export default function useFitText(dep, { max = 40, min = 14, padding = 28, fitBy = 'line' } = {}) {
  const ref = useRef(null);
  const fit = useCallback(() => {
    const el = ref.current;
    const box = el?.parentElement;
    if (!el || !box) return;
    el.style.whiteSpace = 'nowrap'; // measure/shrink on one line first
    let size = max;
    if (max === 'css') {
      el.style.fontSize = '';                                  // вернуть размер из CSS…
      size = parseFloat(getComputedStyle(el).fontSize) || 24;   // …и стартовать от него
    }
    el.style.fontSize = `${size}px`;
    const avail = box.clientWidth - padding;
    let guard = 0;
    if (fitBy === 'word') {
      // Ширину самого длинного слова меряем на canvas: без вставки узлов в DOM, которым
      // владеет React, и без зависимости от текущего переноса строк.
      const words = String(el.textContent || '').split(/\s+/).filter(Boolean);
      const longest = words.sort((a, b) => b.length - a.length)[0] || '';
      const cs = getComputedStyle(el);
      const ctx = fitCanvas().getContext('2d');
      const widthAt = (px) => {
        ctx.font = `${cs.fontStyle} ${cs.fontWeight} ${px}px ${cs.fontFamily}`;
        return ctx.measureText(longest).width;
      };
      while (longest && widthAt(size) > avail && size > min && guard < 80) { size -= 1; guard += 1; }
      el.style.fontSize = `${size}px`;
    } else {
      while (el.scrollWidth > avail && size > min && guard < 80) {
        size -= 1;
        el.style.fontSize = `${size}px`;
        guard += 1;
      }
    }
    // Resting state: always allow normal word-wrapping. If the phrase fits at the
    // size we just settled on, it stays on one line anyway; but if a later change
    // we can't always re-measure in time (web font finishing, the Telegram sheet
    // width settling) makes it overflow, the browser wraps the overflowing word
    // onto the next line instead of clipping it. Combined with `overflow-wrap:
    // break-word` in the CSS, whole words wrap at the spaces — a long word like
    // "Kaffeemaschine" drops to the next line intact rather than being cut off.
    el.style.whiteSpace = 'normal';
  }, [max, min, padding, fitBy]);

  // Подбор кегля — ПОСЛЕ КАЖДОЙ ОТРИСОВКИ, без списка зависимостей. И это не небрежность.
  //
  // Раньше здесь стояло `[dep, fit]`, и первое слово спринта оставалось неподогнанным.
  // Порядок такой: игра монтируется на экране отсчёта, элемента со словом ещё нет, эффект
  // отрабатывает вхолостую (ref пустой). Потом отсчёт заканчивается, слово появляется — но
  // `dep` (индекс слова) всё ещё 0 и `fit` тот же, значит эффект НЕ перезапускается. Кегль
  // так и остаётся тот, что дал CSS от ширины экрана, и длинное слово вроде «Aschenbecher»
  // браузер переносит посередине. Заодно не создавался и ResizeObserver — он тоже искал
  // элемент, которого ещё не было, поэтому и последующие изменения ширины ничего не чинили.
  //
  // Стоимость: цикл подбора ограничен и почти всегда завершается на первом сравнении, а эти
  // экраны перерисовываются по нажатию, а не постоянно.
  useLayoutEffect(() => { fit(); });
  // re-fit on viewport resize / rotation AND whenever the container's own width
  // settles (Telegram WebApp sheet animates in → the box width isn't final on first
  // layout; a plain window-resize listener doesn't catch that). ResizeObserver fires
  // an initial callback on observe, so this also covers the post-mount width.
  // Тот же список зависимостей заменён на «после каждой отрисовки» по той же причине:
  // наблюдатель за шириной нужно ставить тогда, когда элемент уже есть в разметке.
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
  });

  return ref;
}
