// Fit every interactive card into the screen the user actually has.
//
// The games are one card (`.ans-root > .ans-card`) whose height is whatever the
// content needs. On a big phone that fits; on a small one the last option and the
// action button fall below the fold — you scroll down to press "Weiter" and lose
// sight of the question. This module makes the card adapt to the viewport instead:
//
//   1. it first spends PADDING — `.ans-root.is-tight` (answer.css) trims the outer
//      and inner gaps, which is free and costs no legibility;
//   2. if that is still not enough it scales the card down with `zoom`, so the whole
//      thing (question + options + explanation + buttons) lands on one screen;
//   3. it never scales below MIN_ZOOM — if a card is so long that fitting it would
//      make the text unreadable (a 10-item review list), it is left at full size and
//      scrolls normally.
//
// `zoom` (not `transform: scale`) on purpose: it re-flows, so the card keeps the full
// width of the screen and does NOT become a containing block for the `position: fixed`
// pop-ups some games render inside it (ask-popup, word popup, toast).
//
// It runs as one document-level controller instead of a hook in ~20 game files: every
// screen of every game is covered, including ones added later.

const MIN_ZOOM = 0.78;      // ниже — текст становится мелким, лучше обычная прокрутка
const SLACK_GROW = 24;      // px пустоты, после которых пробуем вернуть размер
const SLACK_UNTIGHT = 48;   // px пустоты, после которых возвращаем обычные отступы
const MAX_PASSES = 10;      // предохранитель от бесконечной подстройки внутри одного экрана

const cards = new WeakMap(); // card element → { k, kFail, passes }
const observedCards = new WeakSet();

let installed = false;
let typing = false;          // пока открыта клавиатура, подгонку снимаем
let rafId = 0;
let timerId = 0;
let ro = null;

function supportsZoom() {
  try { return !!(window.CSS && CSS.supports && CSS.supports('zoom', '0.9')); } catch (_e) { return false; }
}

// The height we may actually use. Inside Telegram the webview can be taller than the
// visible sheet, so trust `viewportStableHeight` (stable = without the keyboard) too.
function viewportHeight() {
  const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
  const stable = Number(tg?.viewportStableHeight) || 0;
  const inner = Number(window.innerHeight) || 0;
  if (stable > 200 && inner > 200) return Math.min(stable, inner);
  return Math.max(stable, inner);
}

function stateOf(card) {
  let st = cards.get(card);
  if (!st) { st = { k: 1, kFail: Infinity, passes: 0, tightLocked: false }; cards.set(card, st); }
  return st;
}

function setZoom(card, st, k) {
  const next = k >= 0.999 ? 1 : Math.round(k * 1000) / 1000;
  if (next === st.k) return false;
  st.k = next;
  st.passes += 1;
  card.style.zoom = next === 1 ? '' : String(next);
  return true;
}

// Одна итерация подгонки. Возвращает true, если что-то поменяли — тогда нужен ещё
// проход (ResizeObserver на смену zoom не срабатывает: собственный layout-бокс
// карточки в её же координатах не меняется).
function fitOne(root) {
  if (root.classList.contains('ans-root--cw')) return false; // кроссворд считает свою раскладку сам
  const card = root.querySelector(':scope > .ans-card');
  if (!card) return false;
  if (ro && !observedCards.has(card)) { observedCards.add(card); ro.observe(card); }

  const st = stateOf(card);
  // Клавиатура: при zoom < 1 поле ввода мельче 16px, и iOS сам зумит страницу.
  // Пока пользователь печатает — полный размер, обычная прокрутка.
  if (typing) return setZoom(card, st, 1);
  if (st.passes > MAX_PASSES) return false;

  const cs = getComputedStyle(root);
  const avail = viewportHeight() - (parseFloat(cs.paddingTop) || 0) - (parseFloat(cs.paddingBottom) || 0);
  if (!(avail > 160)) return false;
  const vis = card.getBoundingClientRect().height; // уже с учётом текущего zoom
  if (!(vis > 0)) return false;

  if (vis > avail + 1) {
    // Сначала отдаём отступы — это не стоит читаемости.
    if (!root.classList.contains('is-tight')) {
      root.classList.add('is-tight');
      st.tightLocked = true; // отступы этому экрану нужны — не отдавать их обратно в том же кадре
      st.passes += 1;
      return true;
    }
    const want = (st.k * (avail - 2)) / vis;
    if (want < MIN_ZOOM) return setZoom(card, st, 1); // не влезет по-человечески — скроллим
    st.kFail = Math.min(st.kFail, st.k);
    return setZoom(card, st, want);
  }

  const slack = avail - vis;
  if (st.k < 1 && slack > SLACK_GROW) {
    // Контент стал короче (ушёл разбор, следующий вопрос компактнее) — возвращаем размер,
    // но не до масштаба, который на этом же экране уже не влезал.
    const want = Math.min(1, (st.k * (avail - 6)) / vis, st.kFail - 0.01);
    if (want > st.k + 0.012) return setZoom(card, st, want);
    return false;
  }
  if (st.k >= 1 && !st.tightLocked && slack > SLACK_UNTIGHT && root.classList.contains('is-tight')) {
    root.classList.remove('is-tight');
    st.passes += 1;
    return true;
  }
  return false;
}

function run() {
  let changed = false;
  try {
    document.querySelectorAll('.ans-root').forEach((root) => { if (fitOne(root)) changed = true; });
  } catch (_e) { /* noop */ }
  if (changed) schedule(); // досчитываем, пока состояние не устоится
}

// rAF, но со страховкой таймером: в фоновой/подтормаживающей вкладке кадры могут
// не приходить, а подгонка должна досчитаться — иначе карточка застынет на полпути.
function schedule() {
  if (rafId || timerId) return;
  rafId = requestAnimationFrame(() => {
    rafId = 0;
    if (timerId) { clearTimeout(timerId); timerId = 0; }
    run();
  });
  timerId = setTimeout(() => {
    timerId = 0;
    if (rafId) { cancelAnimationFrame(rafId); rafId = 0; }
    run();
  }, 120);
}

// Новый экран/новый вопрос — считаем подгонку заново (сняв предохранители).
function freshStart() {
  try {
    document.querySelectorAll('.ans-root > .ans-card').forEach((card) => {
      const st = stateOf(card);
      st.passes = 0;
      st.kFail = Infinity;
      st.tightLocked = false;
    });
  } catch (_e) { /* noop */ }
  schedule();
}

export default function installCardAutoFit() {
  if (installed || typeof window === 'undefined' || typeof document === 'undefined') return;
  if (!supportsZoom() || typeof ResizeObserver === 'undefined') return; // без zoom — оставляем как было
  installed = true;

  ro = new ResizeObserver(schedule);

  // Смена содержимого (появился разбор, следующий вопрос) снимает предохранители:
  // только childList — по characterData тикает секундомер спринтов, и высота от него не меняется.
  if (typeof MutationObserver !== 'undefined') {
    const mo = new MutationObserver((records) => {
      let touched = false;
      for (const r of records) {
        const node = r.target?.nodeType === 1 ? r.target : r.target?.parentElement;
        const card = node?.closest?.('.ans-card');
        if (!card) continue;
        const st = stateOf(card);
        st.passes = 0;
        st.kFail = Infinity;
        st.tightLocked = false;
        touched = true;
      }
      if (touched) schedule();
    });
    mo.observe(document.body, { childList: true, subtree: true });
  }

  window.addEventListener('resize', freshStart);
  window.addEventListener('orientationchange', freshStart);
  try { window.Telegram?.WebApp?.onEvent?.('viewportChanged', freshStart); } catch (_e) { /* noop */ }

  const isField = (el) => !!el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable);
  document.addEventListener('focusin', (e) => { if (isField(e.target)) { typing = true; schedule(); } });
  document.addEventListener('focusout', (e) => {
    if (!isField(e.target)) return;
    typing = false;
    // Клавиатура закрывается не мгновенно — пересчитываем, когда viewport устоялся.
    setTimeout(freshStart, 120);
  });

  // Шрифты догружаются после первого layout и меняют высоту текста.
  try { document.fonts?.ready?.then(freshStart); } catch (_e) { /* noop */ }
  schedule();
}
