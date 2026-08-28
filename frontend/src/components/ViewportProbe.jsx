// ВРЕМЕННАЯ ИЗМЕРИТЕЛЬНАЯ СТРОКА — ставится 28.08.2026, снимается вместе с починкой.
//
// Зачем: в приложении с иконки (standalone PWA) клавиатура уносит карточку слова с
// экрана, а в Telegram — нет. Первая правка была сделана по рассуждению о числах
// visualViewport, и она промахнулась: карточка уехала в другую сторону. Значит, числам
// нельзя верить «по документации» — их надо УВИДЕТЬ на этом телефоне.
//
// Строка живая, НО снять её с открытой клавиатурой нельзя: карточку как раз и уносит за
// экран вместе с ней (видео 28.08.2026 — на экране остаётся только нижний край карточки).
// Поэтому прибор ВЕДЁТ ЗАПИСЬ: пока поле в фокусе, он снимает показания 4 раза в секунду
// и складывает их в журнал, а когда клавиатура закрылась — печатает журнал прямо в
// карточке. Достаточно ткнуть в поле, закрыть клавиатуру и прислать один скриншот.
// Показывается ТОЛЬКО владельцу (признак админа из словаря).
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
  const [log, setLog] = useState([]);
  const [focused, setFocused] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    let sinceFocus = 0;
    const fieldFocused = () => {
      const el = document.activeElement;
      return !!el && /^(input|textarea)$/i.test(el.tagName || '');
    };
    const update = () => {
      const next = readNumbers();
      setLine(next);
      const isFocused = fieldFocused();
      setFocused(isFocused);
      // Пишем журнал, пока поле в фокусе, и ещё пару тактов после — чтобы поймать, как
      // числа возвращаются обратно. Повторы подряд не пишем: журнал должен читаться.
      if (isFocused) sinceFocus = 0; else sinceFocus += 1;
      if (isFocused || sinceFocus <= 2) {
        setLog((prev) => {
          const mark = `${isFocused ? 'KB' : '..'} ${next}`;
          if (prev.length && prev[prev.length - 1] === mark) return prev;
          return [...prev, mark].slice(-10);
        });
      }
    };
    update();
    const vv = window.visualViewport || null;
    vv?.addEventListener('resize', update);
    vv?.addEventListener('scroll', update);
    window.addEventListener('resize', update);
    window.addEventListener('scroll', update, { passive: true });
    document.addEventListener('focusin', update);
    document.addEventListener('focusout', update);
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

  const box = {
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
  };

  return (
    <div style={box}>
      <div>{line}</div>
      {!focused && log.length > 0 && (
        <>
          <div style={{ marginTop: 3, fontWeight: 700 }}>журнал клавиатуры ({log.length}):</div>
          {log.map((entry, index) => (
            <div key={`probe-log-${index}`} style={{ opacity: 0.95 }}>{index + 1}. {entry}</div>
          ))}
          <button
            type="button"
            onClick={() => setLog([])}
            style={{ marginTop: 3, fontSize: '9px', padding: '2px 6px' }}
          >
            очистить журнал
          </button>
        </>
      )}
    </div>
  );
}
