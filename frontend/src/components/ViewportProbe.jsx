// ВРЕМЕННАЯ ИЗМЕРИТЕЛЬНАЯ СТРОКА — ставится 28.08.2026, снимается вместе с починкой.
//
// Зачем: в приложении с иконки (standalone PWA) клавиатура уносит карточку слова с
// экрана, а в Telegram — нет. Первая правка была сделана по рассуждению о числах
// visualViewport, и она промахнулась: карточка уехала в другую сторону. Значит, числам
// нельзя верить «по документации» — их надо УВИДЕТЬ на этом телефоне.
//
// Строка живая: обновляется на каждое событие вьюпорта и раз в 250 мс, чтобы её можно
// было снять на видео с открытой клавиатурой. Показывается ТОЛЬКО владельцу (признак
// админа из словаря), обычный пользователь её не видит.
//
// Что означают поля:
//   ih  — window.innerHeight, высота РАЗМЕТОЧНОГО вьюпорта (полный экран)
//   ch  — clientHeight корня, то же самое с другой стороны
//   vh  — visualViewport.height, высота ВИДИМОЙ части (должна ужаться клавиатурой)
//   vt  — visualViewport.offsetTop, на сколько видимая часть сдвинута вниз по разметке
//   pt  — visualViewport.pageTop, та же величина, но от начала документа
//   sy  — window.scrollY, прокрутился ли сам документ
//   app — текущее значение --app-height (под него подогнана высота карточки)
//   ovl — верх/низ оверлея по замеру getBoundingClientRect
//   crd — верх/низ самой карточки по тому же замеру
//   act — верх/низ поля, в котором сейчас курсор
import { useEffect, useState } from 'react';

const num = (value) => (Number.isFinite(value) ? Math.round(value) : '—');

function readNumbers() {
  if (typeof window === 'undefined') return '';
  const vv = window.visualViewport || null;
  const root = document.documentElement;
  const rect = (selector) => {
    const el = typeof selector === 'string' ? document.querySelector(selector) : selector;
    if (!el || typeof el.getBoundingClientRect !== 'function') return '—/—';
    const r = el.getBoundingClientRect();
    return `${num(r.top)}/${num(r.bottom)}`;
  };
  const appHeight = (() => {
    try { return getComputedStyle(root).getPropertyValue('--app-height').trim() || '—'; } catch (_e) { return '—'; }
  })();
  const active = document.activeElement;
  const isField = active && /^(input|textarea)$/i.test(active.tagName || '');
  return [
    `ih${num(window.innerHeight)}`,
    `ch${num(root?.clientHeight)}`,
    `vh${num(vv?.height)}`,
    `vt${num(vv?.offsetTop)}`,
    `pt${num(vv?.pageTop)}`,
    `sy${num(window.scrollY)}`,
    `app${appHeight}`,
    `ovl${rect('.vocab-word-fullscreen-overlay')}`,
    `crd${rect('.vocab-word-fullscreen-card')}`,
    `act${isField ? rect(active) : '—/—'}`,
  ].join(' ');
}

export default function ViewportProbe() {
  const [line, setLine] = useState(() => readNumbers());

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    const update = () => setLine(readNumbers());
    update();
    const vv = window.visualViewport || null;
    vv?.addEventListener('resize', update);
    vv?.addEventListener('scroll', update);
    window.addEventListener('resize', update);
    window.addEventListener('scroll', update, { passive: true });
    document.addEventListener('focusin', update);
    document.addEventListener('focusout', update);
    // Клавиатура выезжает анимацией: событий может не хватить, чтобы поймать конечные
    // числа. Тик раз в 250 мс делает строку пригодной для съёмки на видео.
    const tick = window.setInterval(update, 250);
    return () => {
      window.clearInterval(tick);
      vv?.removeEventListener('resize', update);
      vv?.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
      window.removeEventListener('scroll', update);
      document.removeEventListener('focusin', update);
      document.removeEventListener('focusout', update);
    };
  }, []);

  return (
    <div
      style={{
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
        fontSize: '9px',
        lineHeight: 1.35,
        color: '#0f172a',
        background: '#fde68a',
        border: '1px solid #f59e0b',
        borderRadius: '6px',
        padding: '3px 5px',
        margin: '6px 0',
        wordBreak: 'break-all',
      }}
    >
      {line}
    </div>
  );
}
