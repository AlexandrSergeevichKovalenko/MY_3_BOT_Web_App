// Fit every interactive card into the screen the user actually has.
//
// A game is one card (`.ans-root > .ans-card`) as tall as its content. On a big phone
// that fits; on a small one the last option and the action button fall below the fold —
// you scroll to press "Weiter" and lose sight of the question. So the card adapts:
//
//   1. it first spends PADDING — `.ans-root.is-tight` (answer.css) trims the outer and
//      inner gaps, which costs no legibility;
//   2. if that is not enough it scales the card down with `zoom`, so the whole screen
//      (question + options + explanation + buttons) lands above the fold;
//   3. below MILD_ZOOM scaling alone would make the text too small, so the screen is
//      re-laid-out instead: the card takes exactly the viewport height and its longest
//      block (the word list, the explanation) gets its own scroll — the header, the score
//      and the action button stay visible whatever the content length;
//   4. only if even that fails (a giant block that is itself the whole screen) it falls
//      back to MIN_ZOOM + normal page scroll, with the final button pinned to the bottom
//      (`.ans-root.is-scroll`) so the way out is always in reach.
//
// The whole decision is ONE SYNCHRONOUS PASS: reset → measure → apply, all before the
// browser paints. Nothing is applied in steps and nothing is "grown back" later — an
// earlier multi-frame version made the card visibly jump and flicker on every answer.
// The pass always starts from the natural size, so it can never get stuck small.
//
// `zoom` (not `transform: scale`) on purpose: it re-flows, so the card keeps the full
// width of the screen and does NOT become a containing block for the `position: fixed`
// pop-ups some games render inside it (ask-popup, word popup, toast).
//
// One document-level controller instead of a hook in ~20 game files: every screen of
// every game is covered, including ones added later.

const MILD_ZOOM = 0.82;  // до этого масштаба просто ужимаем карточку целиком
const MIN_ZOOM = 0.72;   // ниже не опускаемся никогда — дальше текст не читается
const PANEL_MIN = 96;    // сколько px экрана минимум оставляем прокручиваемому блоку
const EPS = 1.5;         // допуск в px, чтобы не дёргаться из-за долей пикселя

const cards = new WeakMap(); // card → { k, avail, vis } — состояние последнего расчёта
const observedCards = new WeakSet();

let installed = false;
let typing = false;      // пока открыта клавиатура, подгонку снимаем
let rafId = 0;
let timerId = 0;
let ro = null;

function supportsZoom() {
  try { return !!(window.CSS && CSS.supports && CSS.supports('zoom', '0.9')); } catch (_e) { return false; }
}

// Высота, которой мы реально располагаем. В Telegram webview может быть выше видимой
// шторки, поэтому учитываем и viewportStableHeight (stable = без клавиатуры).
function viewportHeight() {
  const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
  const stable = Number(tg?.viewportStableHeight) || 0;
  const inner = Number(window.innerHeight) || 0;
  if (stable > 200 && inner > 200) return Math.min(stable, inner);
  return Math.max(stable, inner);
}

function availHeight(root) {
  const cs = getComputedStyle(root);
  return viewportHeight() - (parseFloat(cs.paddingTop) || 0) - (parseFloat(cs.paddingBottom) || 0);
}

function stateOf(card) {
  let st = cards.get(card);
  if (!st) { st = { k: 1, avail: 0, vis: 0 }; cards.set(card, st); }
  return st;
}

function setZoom(card, k) {
  card.style.zoom = k >= 0.999 ? '' : String(Math.round(k * 1000) / 1000);
}

// Снять всё, что подгонка применяла раньше: считаем каждый раз от натурального вида,
// иначе карточка может «залипнуть» мелкой после длинного экрана.
function resetCard(root, card) {
  setZoom(card, 1);
  root.style.minHeight = '';
  root.classList.remove('is-tight', 'is-scroll');
  card.classList.remove('is-panelled');
  card.style.maxHeight = '';
  const panel = card.querySelector(':scope > .is-fit-panel');
  if (panel) panel.classList.remove('is-fit-panel');
}

// Один синхронный расчёт для одной карточки. Между чтениями и записями браузер не
// рисует, поэтому пользователь видит только итоговое состояние.
function fitOne(root) {
  if (root.classList.contains('ans-root--cw')) return; // кроссворд считает свою раскладку сам
  const card = root.querySelector(':scope > .ans-card');
  if (!card) return;
  if (ro && !observedCards.has(card)) { observedCards.add(card); ro.observe(card); }
  const st = stateOf(card);

  if (typing) {
    // При zoom < 1 поле ввода мельче 16px — iOS начинает зумить страницу сам.
    // Пока печатают, отдаём полный размер и обычную прокрутку.
    if (st.k !== 1) { resetCard(root, card); st.k = 1; st.vis = 0; }
    return;
  }

  // Ничего не изменилось с прошлого расчёта — не трогаем (иначе лишние reflow).
  const availNow = availHeight(root);
  if (st.vis && Math.abs(card.getBoundingClientRect().height - st.vis) < EPS
      && Math.abs(availNow - st.avail) < EPS) return;

  // 1. Натуральный размер: снимаем всё, что применяли раньше.
  resetCard(root, card);
  let avail = availHeight(root);
  let h = card.getBoundingClientRect().height;
  if (!(avail > 160) || !(h > 0)) return;

  // Подгонка сработала → фиксируем высоту экрана за корнем, иначе `min-height: 100vh`
  // в iOS/Telegram бывает выше видимой области и остаётся паразитная прокрутка пустоты.
  const remember = (k, fitted) => {
    if (fitted) root.style.minHeight = `${Math.round(viewportHeight())}px`;
    st.k = k;
    st.avail = availHeight(root);
    st.vis = card.getBoundingClientRect().height;
  };

  if (h <= avail) { remember(1, false); return; }        // помещается как есть

  // 2. Отдаём отступы — это бесплатно.
  root.classList.add('is-tight');
  avail = availHeight(root);
  h = card.getBoundingClientRect().height;
  if (h <= avail) { remember(1, true); return; }

  // 3. Ужимаем карточку целиком — пока это не бьёт по читаемости.
  const k = (avail - 2) / h;
  if (k >= MILD_ZOOM) {
    setZoom(card, k);
    // Поправка: при меньшем шрифте текст переносится иначе, высота уходит от расчётной.
    const h2 = card.getBoundingClientRect().height;
    if (h2 > avail) { const k2 = Math.max(MIN_ZOOM, (k * (avail - 2)) / h2); setZoom(card, k2); remember(k2, true); return; }
    if (h2 < avail - 16) {
      const k2 = Math.min(1, (k * (avail - 4)) / h2);
      setZoom(card, k2);
      if (card.getBoundingClientRect().height > avail) setZoom(card, k); // откат, если перебрали
      else { remember(k2, true); return; }
    }
    remember(k, true);
    return;
  }

  // 4. Ужимать сильнее нельзя — текст станет нечитаемым. Тогда экран собирается иначе:
  //    карточка ровно по высоте экрана, а самый длинный блок внутри (список слов, разбор)
  //    получает свою прокрутку. Заголовок, счёт и кнопка при этом видны всегда.
  const panel = pickPanel(card);
  if (panel) {
    const rest = h - panel.getBoundingClientRect().height; // всё, кроме длинного блока
    const kp = Math.max(MIN_ZOOM, Math.min(1, (avail - PANEL_MIN) / Math.max(1, rest)));
    setZoom(card, kp);
    card.classList.add('is-panelled');
    panel.classList.add('is-fit-panel');
    card.style.maxHeight = `${Math.round(avail / kp)}px`; // px внутри карточки — уже в её масштабе
    if (card.getBoundingClientRect().height <= avail + EPS) { remember(kp, true); return; }
    // не помогло — откатываем к обычной прокрутке
    card.classList.remove('is-panelled');
    panel.classList.remove('is-fit-panel');
    card.style.maxHeight = '';
  }

  // 5. Последний вариант: ужимаем до предела и оставляем прокрутку, но кнопку выхода
  //    прижимаем к низу экрана — до неё не придётся долистывать.
  setZoom(card, MIN_ZOOM);
  root.classList.add('is-scroll');
  remember(MIN_ZOOM, false);
}

// Блок внутри карточки, который не жалко прокручивать: самый высокий из тех, что не
// содержат кнопку действия (иначе «Дальше» уедет под фолд собственного скролла).
function pickPanel(card) {
  const kids = Array.from(card.children);
  const cardH = card.getBoundingClientRect().height;
  let best = null;
  for (const el of kids) {
    // Кнопки-«фишки» (сохранить слово) прокручивать можно, кнопку действия — нет.
    if (el.tagName === 'BUTTON' || el.querySelector('.ans-btn, .ans-btn-ghost')) continue;
    const h = el.getBoundingClientRect().height;
    if (h < cardH * 0.28) continue;      // мелкие блоки прокручивать бессмысленно
    if (!best || h > best.h) best = { el, h };
  }
  return best ? best.el : null;
}

function run() {
  rafId = 0;
  if (timerId) { clearTimeout(timerId); timerId = 0; }
  try { document.querySelectorAll('.ans-root').forEach(fitOne); } catch (_e) { /* noop */ }
}

// rAF, но со страховкой таймером: в подтормаживающей вкладке кадры могут не приходить,
// а подгонка должна досчитаться.
function schedule() {
  if (rafId || timerId) return;
  rafId = requestAnimationFrame(run);
  timerId = setTimeout(() => {
    timerId = 0;
    if (rafId) { cancelAnimationFrame(rafId); rafId = 0; }
    run();
  }, 150);
}

// Внешние обстоятельства поменялись (поворот, шторка Telegram) — забываем кеш и считаем заново.
function freshStart() {
  try {
    document.querySelectorAll('.ans-root > .ans-card').forEach((card) => {
      const st = stateOf(card);
      st.vis = 0;
      st.avail = 0;
    });
  } catch (_e) { /* noop */ }
  schedule();
}

export default function installCardAutoFit() {
  if (installed || typeof window === 'undefined' || typeof document === 'undefined') return;
  if (!supportsZoom() || typeof ResizeObserver === 'undefined') return; // без zoom — оставляем как было
  installed = true;

  ro = new ResizeObserver(schedule);

  if (typeof MutationObserver !== 'undefined') {
    // Синхронно, прямо в колбэке: он выполняется ПОСЛЕ правки DOM, но ДО отрисовки —
    // значит подгонка попадёт в тот же кадр и пользователь не увидит промежуточный,
    // не влезающий вариант. Через rAF был бы лишний мелькающий кадр.
    const mo = new MutationObserver(run);
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
    setTimeout(freshStart, 120); // клавиатура закрывается не мгновенно
  });

  // Шрифты догружаются после первого layout и меняют высоту текста.
  try { document.fonts?.ready?.then(freshStart); } catch (_e) { /* noop */ }
  schedule();
}
