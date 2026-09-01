import { useCallback, useEffect, useRef, useState } from 'react';

/**
 * Клавиатура на каркасе `.pinw` (админские экраны приёмки).
 *
 * ┌─ ИЗМЕРЕНО 01.09.2026 по видео владельца (iPhone, Telegram, экран «Спорные фразы») ─┐
 * │ Дефект: тап в поле → на правку остаётся полоска в две строки текста.               │
 * │ Замер по кадрам записи: окно приложения с открытой клавиатурой ≈ 423 px, из них    │
 * │ нижняя панель `.pinw-bar` ≈ 335 px (79%), область правки `.frv-scroll` ≈ 73 px.    │
 * │ Причина: `.pinw-bar` — flex:0 0 auto, свою высоту не отдаёт НИКОГДА, поэтому всё   │
 * │ сжатие окна под клавиатуру целиком достаётся области правки.                       │
 * │ Механизм «идёт набор» на каркасе был, но прятал только заголовок экрана.           │
 * └────────────────────────────────────────────────────────────────────────────────────┘
 *
 * Решение владельца 01.09.2026: пока курсор стоит в поле, в нижней панели остаётся
 * ТОЛЬКО главное действие (класс `pinw-typing-keep`), всё остальное убирается; поле,
 * в которое ткнули, подкручивается к середине освободившейся области.
 *
 * Правило разметки живёт в каркасе — `.pinw.typing .pinw-bar > *` в `answer.css`,
 * — а не в отдельном экране, чтобы следующий экран на этом каркасе не сломался заново.
 */
export function useTypingChrome() {
  const [typing, setTyping] = useState(false);
  const timers = useRef([]);

  const clearTimers = useCallback(() => {
    timers.current.forEach((id) => clearTimeout(id));
    timers.current = [];
  }, []);

  useEffect(() => clearTimers, [clearTimers]);

  const onFocus = useCallback((event) => {
    const el = event.currentTarget;
    setTyping(true);
    clearTimers();
    // Три замера, а не один: клавиатура в WKWebView встаёт с анимацией, а панель
    // схлопывается на следующей отрисовке React — одиночная прокрутка попадает в
    // ещё старую геометрию и оставляет поле у самого края.
    [0, 150, 400].forEach((delay) => {
      timers.current.push(setTimeout(() => {
        if (typeof document === 'undefined' || document.activeElement !== el) return;
        // try/catch здесь не глушит данные: это прокрутка экрана, наружу ничего не
        // уходит. Объектный аргумент scrollIntoView не понимают только очень старые
        // движки, и там поле просто останется на месте — как было до этой правки.
        try {
          el.scrollIntoView({ block: 'center', behavior: delay ? 'smooth' : 'auto' });
        } catch (e) {
          console.warn('[pinw] scrollIntoView не поддержан движком', e);
        }
      }, delay));
    });
  }, [clearTimers]);

  const onBlur = useCallback(() => {
    clearTimers();
    setTyping(false);
  }, [clearTimers]);

  return { typing, onFocus, onBlur };
}

export default useTypingChrome;
