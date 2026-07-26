import React, { useCallback, useLayoutEffect, useRef } from 'react';

/**
 * Авто-подгон размера текста под ширину своего блока.
 *
 * Зачем: длинные немецкие слова (Verschwiegenheitspflicht) больше НЕ рвутся
 * посреди букв («…pflic|ht»). Слово остаётся целым, а шрифт уменьшается, пока
 * самое широкое слово не влезет в строку. Одно слово → одна строка; фраза
 * переносится только по пробелам.
 *
 * Реализация: элемент display:block; width:100%; overflow:hidden, поэтому его
 * clientWidth = внутренняя ширина блока (с учётом паддингов), а scrollWidth =
 * ширина содержимого. Уменьшаем font-size, пока содержимое перестанет вылазить
 * за границы. Инлайновый font-size перебивает clamp() из CSS.
 */
export default function FitText({
  as: Tag = 'div',
  text,
  children,
  className = '',
  min = 15,
  max = 48,
  step = 1,
  refitKey,
  ...rest
}) {
  const ref = useRef(null);

  const fit = useCallback(() => {
    const el = ref.current;
    if (!el) return;
    const avail = el.clientWidth;
    if (!avail) return; // блок ещё не измерен — подождём следующего прохода
    let size = max;
    el.style.fontSize = `${size}px`;
    let guard = 0;
    while (size > min && el.scrollWidth > el.clientWidth + 0.5 && guard < 240) {
      size -= step;
      if (size < min) size = min;
      el.style.setProperty('font-size', `${size}px`, 'important');
      guard += 1;
    }
  }, [min, max, step]);

  useLayoutEffect(() => {
    fit();
    // Повторная подгонка при догрузке шрифтов / первом измерении нулевой ширины.
    const raf = requestAnimationFrame(fit);

    // Реагируем на изменение ширины КОНТЕЙНЕРА (поворот, клавиатура, ресайз).
    // Наблюдаем за родителем, а не за собой — смена font-size не меняет ширину
    // родителя, поэтому цикла обновлений не будет.
    let ro;
    const parent = ref.current && ref.current.parentElement;
    if (typeof ResizeObserver !== 'undefined' && parent) {
      ro = new ResizeObserver(() => fit());
      ro.observe(parent);
    }
    window.addEventListener('resize', fit);
    return () => {
      cancelAnimationFrame(raf);
      if (ro) ro.disconnect();
      window.removeEventListener('resize', fit);
    };
  }, [fit, text, children, refitKey]);

  return (
    <Tag ref={ref} className={`fit-text ${className}`.trim()} {...rest}>
      {children != null ? children : text}
    </Tag>
  );
}
