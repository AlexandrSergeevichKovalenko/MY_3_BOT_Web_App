// Подгонка интерактива под экран пользователя — по высоте И по ширине, на любом телефоне.
//
// Игра — это одна карточка (`.ans-root > .ans-card`) высотой во столько, сколько занял
// контент. На большом телефоне это влезало, на маленьком последний вариант и кнопка
// уезжали под сгиб. Плюс обратная беда: если внутри карточки есть свой прокручиваемый
// список, карточка получалась НИЖЕ экрана — сверху и снизу пустота, а список зажат в
// окошко на две строки. И то и другое — плохо использованный экран.
//
// Что делает этот модуль (всё — в одном синхронном проходе, см. ниже):
//   1. РАСТЯГИВАЕТ: если внутри есть блок со своей прокруткой, он забирает всё свободное
//      место — карточка занимает экран целиком, читать видно максимум; если прокручивать
//      нечего, а место осталось — карточка УВЕЛИЧИВАЕТСЯ (до MAX_ZOOM), чтобы занять экран
//      и дать более крупный текст;
//   2. ПОДЖИМАЕТ ОТСТУПЫ: `.ans-root.is-tight` (answer.css) — бесплатно, читаемость не
//      страдает;
//   3. УЖИМАЕТ пропорционально (`zoom`) — пока это не бьёт по читаемости (до MILD_ZOOM);
//   4. ПЕРЕСОБИРАЕТ ЭКРАН: карточка ровно по высоте экрана, самый длинный блок внутри
//      получает свою прокрутку. Заголовок, счёт и кнопка действия видны всегда;
//   5. крайний случай — MIN_ZOOM и обычная прокрутка страницы, но кнопка выхода прижата
//      к низу экрана (`.ans-root.is-scroll`), долистывать до неё не нужно.
//
// ВСЁ РЕШЕНИЕ — ОДИН СИНХРОННЫЙ ПРОХОД: сброс → замер → применение, до отрисовки кадра.
// Ничего не применяется «по шагам» и ничего не отыгрывается назад позже: ранняя версия
// делала это за несколько кадров, и карточка на глазах прыгала и мигала при каждом ответе.
// Проход всегда стартует от натурального вида, поэтому «залипнуть мелким» невозможно.
//
// `zoom`, а не `transform: scale`, намеренно: он делает реальный перенос текста, карточка
// сохраняет полную ширину экрана и НЕ становится containing block для `position: fixed`
// попапов, которые некоторые игры рисуют внутри карточки.
//
// Один контроллер на весь роут вместо хука в ~20 файлах игр: покрыты все экраны всех игр,
// включая те, что появятся позже.

const MAX_ZOOM = 1.18;   // на большом экране карточку не только можно, но и НУЖНО увеличить
const MILD_ZOOM = 0.82;  // до этого масштаба просто ужимаем карточку целиком
const MIN_ZOOM = 0.72;   // ниже не опускаемся никогда — дальше текст не читается
const PANEL_MIN = 96;    // сколько px экрана минимум оставляем прокручиваемому блоку
const EPS = 6;           // допуск в px: мелкие расхождения не должны запускать пересчёт

const cards = new WeakMap(); // card → { k, avail, vis, stretched }
const naturalMaxWidth = new WeakMap(); // card → max-width в натуральную величину, px
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
  // 3 px запаса: округления в разных браузерах не должны давать лишний пиксель прокрутки
  return viewportHeight() - (parseFloat(cs.paddingTop) || 0) - (parseFloat(cs.paddingBottom) || 0) - 3;
}

function stateOf(card) {
  let st = cards.get(card);
  if (!st) { st = { k: 1, avail: 0, vis: 0, stretched: null, runs: 0 }; cards.set(card, st); }
  return st;
}

function setZoom(card, k) {
  // ровно 1 — снимаем свойство совсем; и уменьшение, и УВЕЛИЧЕНИЕ пишем как есть
  const one = k > 0.999 && k < 1.001;
  card.style.zoom = one ? '' : String(Math.round(k * 1000) / 1000);
  // max-width карточки задан в её собственных px, а они масштабируются zoom'ом — без
  // поправки ужатая карточка становится ещё и уже, и по бокам появляется пустота.
  if (one) {
    card.style.maxWidth = '';
  } else {
    if (!naturalMaxWidth.has(card)) {
      const raw = parseFloat(getComputedStyle(card).maxWidth);
      naturalMaxWidth.set(card, Number.isFinite(raw) ? raw : 0);
    }
    const base = naturalMaxWidth.get(card) || 0;
    card.style.maxWidth = base > 0 ? `${Math.round(base / k)}px` : '';
  }
}

// Блок внутри карточки со СВОЕЙ прокруткой (список разборов, список слов). Именно ему
// отдаём всё свободное место — иначе он остаётся окошком в две строки посреди пустого экрана.
function findScroller(card) {
  let best = null;
  for (const el of card.querySelectorAll('*')) {
    const cs = getComputedStyle(el);
    if (!/auto|scroll/.test(cs.overflowY)) continue;
    if (el.scrollHeight - el.clientHeight < 4) continue;   // прокручивать нечего
    const h = el.getBoundingClientRect().height;
    if (!best || h > best.h) best = { el, h };
  }
  return best ? best.el : null;
}

// Блок, который не жалко прокручивать, если экран пересобирается целиком.
//
// Сначала спрашиваем саму вёрстку: игра помечает своё «тело» классом `ans-body` (список
// слов, разбор). Это правильный порядок — решение принимает разметка, а не догадка по
// высоте. Эвристика ниже осталась для экранов, которые ещё не размечены.
//
// Кнопка действия внутри такого блока не теряется — CSS прижимает её к низу блока
// (`.is-fit-panel > .ans-btn`), так что «Дальше» видно всегда.
function pickPanel(card) {
  const marked = card.querySelector(':scope > .ans-body');
  if (marked) return marked;
  const cardH = card.getBoundingClientRect().height;
  let best = null;
  for (const el of Array.from(card.children)) {
    if (el.tagName === 'BUTTON') continue;          // сама кнопка — не блок для прокрутки
    const h = el.getBoundingClientRect().height;
    if (h < cardH * 0.2) continue;                  // мелкие блоки прокручивать бессмысленно
    if (!best || h > best.h) best = { el, h };
  }
  return best ? best.el : null;
}

// Снять всё, что подгонка применяла раньше: считаем каждый раз от натурального вида,
// иначе карточка может «залипнуть» мелкой после длинного экрана.
function resetCard(root, card, st) {
  setZoom(card, 1);
  card.style.maxWidth = '';
  root.style.minHeight = '';
  root.classList.remove('is-tight', 'is-scroll');
  card.classList.remove('is-panelled');
  card.style.maxHeight = '';
  const panel = card.querySelector(':scope > .is-fit-panel');
  if (panel) panel.classList.remove('is-fit-panel');
  if (st && st.stretched) { st.stretched.style.maxHeight = ''; st.stretched = null; }
}

// Один синхронный расчёт для одной карточки. Между чтениями и записями браузер не рисует,
// поэтому пользователь видит только итоговое состояние.
function fitOne(root) {
  if (root.classList.contains('ans-root--cw')) return; // кроссворд считает свою раскладку сам
  const card = root.querySelector(':scope > .ans-card');
  if (!card) return;
  if (ro && !observedCards.has(card)) { observedCards.add(card); ro.observe(card); }
  const st = stateOf(card);

  if (typing) {
    // При zoom < 1 поле ввода мельче 16px — iOS начинает зумить страницу сам.
    // Пока печатают, отдаём полный размер и обычную прокрутку.
    if (st.k !== 1 || st.stretched) { resetCard(root, card, st); st.k = 1; st.vis = 0; }
    return;
  }

  // Ничего не изменилось с прошлого расчёта — не трогаем (иначе лишние reflow).
  const availNow = availHeight(root);
  if (st.vis && Math.abs(card.getBoundingClientRect().height - st.vis) < EPS
      && Math.abs(availNow - st.avail) < EPS) return;
  // Предохранитель от «подрастания»: на одно и то же содержимое хватает пары расчётов.
  // Дальше замолкаем до следующей смены контента (её ловит MutationObserver) или размера
  // окна — иначе редкие расхождения в пару пикселей гоняли бы карточку кадр за кадром.
  if (st.runs >= 2) return;
  st.runs += 1;

  // 1. Натуральный размер: снимаем всё, что применяли раньше. Корень сразу прижимаем к
  //    видимой высоте — иначе центрирование и замеры считаются по разной мере.
  resetCard(root, card, st);
  root.style.minHeight = `${Math.round(viewportHeight())}px`;
  let avail = availHeight(root);
  let h = card.getBoundingClientRect().height;
  if (!(avail > 160) || !(h > 0)) return;

  const remember = (k) => {
    st.k = k;
    st.avail = availHeight(root);
    st.vis = card.getBoundingClientRect().height;
  };

  // Отдать внутреннему прокручиваемому блоку всю оставшуюся высоту экрана. Цикл — внутри
  // ОДНОГО прохода: блок может упереться в собственный контент, и остаток надо добрать
  // сразу, а не по кадру за проход (иначе карточка на глазах подрастает несколько раз).
  const stretch = (k) => {
    const sc = findScroller(card);
    if (!sc) return;
    // Экран на вес золота: раз внутри есть прокрутка, значит контента больше, чем видно —
    // отдаём отступы содержимому.
    if (!root.classList.contains('is-tight')) {
      root.classList.add('is-tight');
      avail = availHeight(root);
    }
    st.stretched = sc;
    let prev = -1;
    for (let i = 0; i < 8; i += 1) {
      const cardH = card.getBoundingClientRect().height;
      const slack = avail - cardH;
      if (slack < 6) break;
      if (prev > 0 && cardH - prev < 2) break;            // перестал расти — упёрся в контент
      if (sc.scrollHeight - sc.clientHeight < 2) break;   // прокручивать уже нечего
      prev = cardH;
      const cur = sc.getBoundingClientRect().height;
      sc.style.maxHeight = `${Math.round((cur + slack - 4) / k)}px`;
    }
    for (let i = 0; i < 2; i += 1) {
      const over = card.getBoundingClientRect().height - avail;
      if (over <= 0) break;
      const cur = sc.getBoundingClientRect().height;
      sc.style.maxHeight = `${Math.round((cur - over - 3) / k)}px`;
    }
  };

  // Финальная проверка ПО ФАКТУ: страница не должна прокручиваться. Замер высоты карточки
  // при zoom отдаёт родителю чуть другую величину, поэтому доводим по самому документу.
  // Всё в том же синхронном проходе — промежуточных состояний пользователь не видит.
  const settle = (k0) => {
    let k = k0;
    stretch(k);
    for (let i = 0; i < 3; i += 1) {
      // высота КОРНЯ интерактива против видимой высоты — не документа: документ выше
      // видимой области (см. комментарий к viewportHeight), и по нему получалось бы
      // вечное «не влезает».
      const over = Math.round(root.getBoundingClientRect().height - viewportHeight());
      if (over <= 1) break;
      const vis = card.getBoundingClientRect().height;
      if (!(vis > 0)) break;
      if (st.stretched) {
        // сначала отдаём лишнее из растянутого блока — это не стоит читаемости
        const cur = st.stretched.getBoundingClientRect().height;
        if (cur - over > 80) { st.stretched.style.maxHeight = `${Math.round((cur - over - 2) / k)}px`; continue; }
      }
      const nk = Math.max(MIN_ZOOM, k * Math.max(0.5, (vis - over - 2) / vis));
      if (nk >= k - 0.002) break;
      k = nk;
      setZoom(card, k);
      if (card.classList.contains('is-panelled')) card.style.maxHeight = `${Math.round((avail - 2) / k)}px`;
    }
    remember(k);
  };

  // Помещается ли карточка при масштабе kk (замер, а не расчёт: текст переносится иначе).
  const fitsAt = (kk) => { setZoom(card, kk); return card.getBoundingClientRect().height <= avail; };
  // Максимальный масштаб, при котором ещё помещается: lo помещается, hi — уже нет.
  const bisect = (lo, hi) => {
    let best = lo;
    for (let i = 0; i < 5; i += 1) {
      const mid = (lo + hi) / 2;
      if (fitsAt(mid)) { best = mid; lo = mid; } else hi = mid;
    }
    setZoom(card, best);
    return best;
  };

  if (h <= avail) {
    // Есть запас. Сначала отдаём его внутреннему прокручиваемому блоку.
    stretch(1);
    const left = avail - card.getBoundingClientRect().height;
    // Место всё равно осталось (прокручивать нечего) — увеличиваем карточку, пока она не
    // упрётся в края: экран должен быть занят, а текст крупным. Короткие экраны (заставка,
    // обратный отсчёт) не раздуваем — им воздух идёт на пользу.
    if (left >= 10 && h >= avail * 0.6) {
      const kUp = fitsAt(MAX_ZOOM) ? MAX_ZOOM : bisect(1, MAX_ZOOM);
      settle(kUp);
      return;
    }
    settle(1);
    return;
  }

  // 2. Отдаём отступы — это бесплатно.
  root.classList.add('is-tight');
  avail = availHeight(root);
  h = card.getBoundingClientRect().height;
  if (h <= avail) {
    stretch(1);
    const left = avail - card.getBoundingClientRect().height;
    if (left >= 10 && h >= avail * 0.6) {
      const kUp = fitsAt(MAX_ZOOM) ? MAX_ZOOM : bisect(1, MAX_ZOOM);
      settle(kUp);
      return;
    }
    settle(1);
    return;
  }

  // 3. Ужимаем карточку целиком — пока это не бьёт по читаемости. Масштаб подбираем
  //    замером, а не формулой: при другом кегле текст переносится иначе.
  const k0 = (avail - 2) / h;
  if (k0 >= MILD_ZOOM) {
    const k = fitsAt(k0) ? bisect(k0, Math.min(1, k0 * 1.35)) : bisect(Math.max(MIN_ZOOM, k0 * 0.85), k0);
    if (card.getBoundingClientRect().height <= avail) { settle(k); return; }
  }

  // 4. Ужимать сильнее нельзя — текст станет нечитаемым. Тогда экран собирается иначе:
  //    карточка ровно по высоте экрана, а самый длинный блок внутри (список слов, разбор)
  //    получает свою прокрутку. Заголовок, счёт и кнопка при этом видны всегда.
  const panel = pickPanel(card);
  if (panel) {
    const rest = h - panel.getBoundingClientRect().height; // всё, кроме длинного блока
    // Если внутри блока живёт кнопка действия — она прилипнет к его низу, значит на текст
    // должно остаться место СВЕРХ её высоты, иначе от разбора видно две строки.
    const btnInside = panel.querySelector('.ans-btn, .ans-btn-ghost');
    const needPanel = PANEL_MIN + (btnInside ? btnInside.getBoundingClientRect().height : 0);
    const kp = Math.max(MIN_ZOOM, Math.min(1, (avail - needPanel) / Math.max(1, rest)));
    setZoom(card, kp);
    card.classList.add('is-panelled');
    panel.classList.add('is-fit-panel');
    card.style.maxHeight = `${Math.round((avail - 2) / kp)}px`; // px внутри карточки — уже в её масштабе; −2 на рамку
    if (card.getBoundingClientRect().height <= avail + EPS) { settle(kp); return; }
    // не помогло — откатываем к обычной прокрутке
    card.classList.remove('is-panelled');
    panel.classList.remove('is-fit-panel');
    card.style.maxHeight = '';
  }

  // 5. Последний вариант: ужимаем до предела и оставляем прокрутку, но кнопку выхода
  //    прижимаем к низу экрана — до неё не придётся долистывать.
  setZoom(card, MIN_ZOOM);
  root.classList.add('is-scroll');
  st.k = MIN_ZOOM;
  st.avail = availHeight(root);
  st.vis = card.getBoundingClientRect().height;
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
      st.runs = 0;
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
    const mo = new MutationObserver(() => {
      // содержимое поменялось — предохранитель снимаем, считаем заново
      try {
        document.querySelectorAll('.ans-root > .ans-card').forEach((card) => { stateOf(card).runs = 0; });
      } catch (_e) { /* noop */ }
      run();
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
    setTimeout(freshStart, 120); // клавиатура закрывается не мгновенно
  });

  // Шрифты догружаются после первого layout и меняют высоту текста.
  try { document.fonts?.ready?.then(freshStart); } catch (_e) { /* noop */ }
  // Шторка Telegram разворачивается с анимацией: первый расчёт может попасть на ещё не
  // доехавшую высоту. Два поздних пересчёта дешевле, чем застрявшая маленькая карточка.
  setTimeout(freshStart, 400);
  setTimeout(freshStart, 1200);
  schedule();
}
