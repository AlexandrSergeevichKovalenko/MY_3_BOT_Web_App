// React-обёртка над ядром подгонки (`modalFit.js`): когда пересчитывать.
//
// Само решение «какой кегль и какая высота» живёт в ядре без React — так стенд проверки
// (матрица телефонов) гоняет тот же код, что и приложение.
import { useEffect } from 'react';
import fitModal from './modalFit';

/**
 * @param {object} overlayRef ref на корень оверлея (ему задаётся видимая высота)
 * @param {object} cardRef    ref на саму карточку окна
 * @param {object} bodyRef    ref на прокручиваемое тело окна
 * @param {boolean} active    окно открыто
 * @param {string|number} contentKey меняется вместе с содержимым — повод пересчитать
 */
export default function useModalFit(overlayRef, cardRef, bodyRef, active, contentKey) {
  useEffect(() => {
    if (!active) return undefined;
    if (typeof window === 'undefined') return undefined;

    let raf = 0;
    const fit = () => fitModal(overlayRef.current, cardRef.current, bodyRef.current);
    const schedule = () => {
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => { raf = 0; fit(); });
    };

    fit();

    window.addEventListener('resize', schedule);
    window.addEventListener('orientationchange', schedule);
    // Видимая область меняется без `resize`: шторка Telegram, панели браузера, клавиатура.
    // Ровно для этого и существует visualViewport — штатный сигнал, а не наша выдумка.
    try { window.visualViewport?.addEventListener('resize', schedule); } catch (_e) { /* noop */ }
    let offTelegram = null;
    try {
      const tg = window.Telegram?.WebApp;
      if (tg?.onEvent) {
        tg.onEvent('viewportChanged', schedule);
        offTelegram = () => { try { tg.offEvent?.('viewportChanged', schedule); } catch (_e) { /* noop */ } };
      }
    } catch (_e) { /* noop */ }
    // Шрифты догружаются после первого layout и меняют высоту текста.
    try { document.fonts?.ready?.then(schedule); } catch (_e) { /* noop */ }
    // Шторка Telegram разворачивается с анимацией: первый расчёт может попасть на ещё не
    // доехавшую высоту. Два поздних пересчёта дешевле застрявшего маленького окна.
    const t1 = setTimeout(schedule, 400);
    const t2 = setTimeout(schedule, 1200);

    return () => {
      if (raf) cancelAnimationFrame(raf);
      clearTimeout(t1);
      clearTimeout(t2);
      window.removeEventListener('resize', schedule);
      window.removeEventListener('orientationchange', schedule);
      try { window.visualViewport?.removeEventListener('resize', schedule); } catch (_e) { /* noop */ }
      if (offTelegram) offTelegram();
    };
  }, [active, contentKey, overlayRef, cardRef, bodyRef]);
}
