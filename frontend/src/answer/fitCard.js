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

// Потолок увеличения у телефона и у планшета РАЗНЫЙ, и это не вкусовщина.
// Кегли внутри карточки заданы в её собственных px, а zoom масштабирует и их, и ширину
// карточки — значит слово занимает ту же долю строки только до тех пор, пока запас по
// ширине есть. На телефоне его нет: колонка узкая, немецкие слова длинные, и уже на +35%
// «Ladegerät» разрезалось посередине, а кнопки уезжали под сгиб. 1.18 — проверенное на
// матрице телефонов значение, его не трогаем. Планшету +35% можно: там колонка вдвое шире.
const MAX_ZOOM = 1.18;        // телефон
const WIDE_MAX_ZOOM = 1.35;   // планшет / большой браузер
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

// Высота, которой мы реально располагаем. ОДИН ИСТОЧНИК — БРАУЗЕР.
//
// `visualViewport.height` — это то же самое число, что стоит за единицей `dvh`, и именно по
// нему браузер рисует страницу. Оно не может разойтись с тем, что видит пользователь.
//
// Числа Telegram (`viewportStableHeight`, `isExpanded`) здесь НЕТ, и это выстраданное
// решение, а не небрежность. Замер на живом телефоне: мини-апп развёрнут на весь экран,
// `dvh` даёт 723, а Telegram отдаёт ~566 и держит `isExpanded` не равным true. По его числу
// карточка подгоняется под экран на треть меньше настоящего — те самые 69-71% и пустые
// полосы сверху и снизу, на которые жаловались трое суток. Проверить «свежесть» этого числа
// изнутри страницы нельзя, поэтому мы на него не опираемся вовсе.
//
// А чтобы мини-апп не оставался шторкой (единственный случай, где webview выше видимой
// части), при старте зовётся `expand()` — см. tgReady() в main.jsx. Это штатный способ, а не
// подгонка задним числом.
function viewportHeight() {
  const vv = typeof window !== 'undefined' ? window.visualViewport : null;
  const visual = Number(vv?.height) || 0;
  if (visual > 200) return visual;
  return Number(window.innerHeight) || 0;
}

// Планшет / большой браузер. Порог тот же, что у медиазапроса в answer.css (ширина от
// 700 px И высота от 560 px), чтобы движок и вёрстка считали «широким» одно и то же.
// Телефон в горизонтали сюда намеренно не попадает.
function isWide(root) {
  return root.getBoundingClientRect().width >= 700 && viewportHeight() >= 560;
}

function availHeight(root) {
  const cs = getComputedStyle(root);
  // 3 px запаса: округления в разных браузерах не должны давать лишний пиксель прокрутки
  return viewportHeight() - (parseFloat(cs.paddingTop) || 0) - (parseFloat(cs.paddingBottom) || 0) - 3;
}

function stateOf(card) {
  let st = cards.get(card);
  if (!st) { st = { k: 1, avail: 0, vis: 0, stretched: null, runs: 0, hard: 0 }; cards.set(card, st); }
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
  card.classList.remove('is-panelled', 'is-filled');
  card.style.maxHeight = '';
  card.style.minHeight = '';
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
  const visNow = card.getBoundingClientRect().height;
  if (st.vis && Math.abs(visNow - st.vis) < EPS && Math.abs(availNow - st.avail) < EPS) return;

  // Предохранитель от бесконечного пересчёта. Он ОБЯЗАН пропускать настоящие изменения.
  //
  // Раньше он их не пропускал, и это была причина «карточка ниже экрана, а сверху и снизу
  // пустые поля». После ответа карточка перерисовывается НЕ ОДИН РАЗ: React дорисовывает
  // разбор, доезжает веб-шрифт, подгружается картинка. Счётчик «хватит двух проходов»
  // сгорал на промежуточных состояниях, и последний, правильный расчёт уже не запускался —
  // на экране навсегда оставался промежуточный результат. Экран-вопрос в это не попадал,
  // потому что рисуется за один заход, — отсюда и «одна карточка нормально, другая нет».
  //
  // Правило теперь такое: если высота отличается от той, которую мы САМИ выставили, — это
  // новая вводная, а не дребезг, и счётчик сбрасывается. От настоящих качелей (мы ставим
  // X, вёрстка отвечает Y, и так по кругу) страхует ЖЁСТКИЙ потолок проходов; он снимается
  // только сменой содержимого или размера экрана.
  if (st.vis && Math.abs(visNow - st.vis) > EPS) st.runs = 0;
  if (Math.abs(availNow - st.avail) > EPS) { st.runs = 0; st.hard = 0; }
  if (st.runs >= 2 || st.hard >= 8) { debugLine(root, card, `stop r${st.runs}h${st.hard}`); return; }
  st.runs += 1;
  st.hard += 1;
  let branch = '?';

  // 1. Натуральный размер: снимаем всё, что применяли раньше. Корень сразу прижимаем к
  //    видимой высоте — иначе центрирование и замеры считаются по разной мере.
  resetCard(root, card, st);
  root.style.minHeight = `${Math.round(viewportHeight())}px`;
  let avail = availHeight(root);
  let h = card.getBoundingClientRect().height;
  if (!(avail > 160) || !(h > 0)) return;

  // Растягивание карточки на всю высоту (fill/rescue ниже) — ТОЛЬКО планшет. На телефоне
  // вёрстка выверена по матрице телефонов и подгонка там делает ровно одно: ужимает то,
  // что не влезло, и умеренно (до MAX_ZOOM) растит то, что почти влезло. Попытка занять
  // телефонный экран «целиком» ломала его: текст раздувался, длинные слова разрезались,
  // кнопки уходили под сгиб.
  const wide = isWide(root);
  const zoomCeil = wide ? WIDE_MAX_ZOOM : MAX_ZOOM;

  const remember = (k) => {
    st.k = k;
    st.avail = availHeight(root);
    st.vis = card.getBoundingClientRect().height;
    debugLine(root, card, branch);
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

  // Место всё равно осталось — добираем его ВЫСОТОЙ САМОЙ КАРТОЧКИ, содержимое встаёт по
  // центру (.ans-card.is-filled). Масштаб при этом НЕ трогаем.
  //
  // Почему именно так. Карточку, которая ниже экрана, увеличить можно только масштабом, а у
  // него есть потолок: дальше текст становится неприлично крупным и длинные немецкие слова
  // начинают разрезаться. Поэтому карточка, которой до экрана не хватило даже потолка, так
  // и висела полоской посреди пустоты — сверху и снизу мёртвые поля. Растянуть карточку —
  // ровно то, что нужно: экран занят целиком, а кегль остаётся выверенным.
  //
  // Прокрутки это добавить не может: тянем строго до availHeight, и только когда место
  // ЕСТЬ. Двух режимов, где высота уже задана точно, не касаемся.
  // ИНВАРИАНТ: карточка занимает ровно видимую высоту экрана. ВСЕГДА, а не «если повезло с
  // веткой». Пустых полос сверху и снизу не может быть по построению — остаток высоты
  // забирает сама карточка, содержимое встаёт по центру (.ans-card.is-filled).
  //
  // Это замена целой россыпи условий («растить, если запас больше 10 px», «не раздувать
  // короткий экран», «в режиме панели не трогать»). Каждое из них было отдельной дырой, в
  // которую проваливался очередной экран и оставался полоской посреди пустоты. Условие
  // осталось ровно одно: если карточка и так не влезла (обычная прокрутка страницы) —
  // растягивать нечего.
  //
  // Прокрутки это добавить не может: тянем строго до availHeight.
  const fillToScreen = (k) => {
    if (root.classList.contains('is-scroll')) return;
    const room = availHeight(root);
    if (room - card.getBoundingClientRect().height < 4) return;
    card.classList.add('is-filled');
    card.style.minHeight = `${Math.round((room - 2) / k)}px`;
  };

  // ГЛАВНАЯ проверка, важнее любой ветки выше: если после всех расчётов на экране осталось
  // пустое место — значит расчёт ошибся, а не экран такой. Ветка могла ошибиться (не
  // доехавшая шторка Telegram, замер до подгрузки шрифта, откат из режима панели), замер
  // по факту — нет. Тогда возвращаем масштаб, который реально помещается, и добираем
  // остаток высотой самой карточки. Ровно этим лечится «карточка скукожилась в центре
  // экрана, а сверху и снизу пусто».
  //
  // Только планшет. На телефоне эта же добавка и творила беду: она срабатывала ПОСЛЕ
  // финальной проверки «страница не прокручивается» и поднимала масштаб мимо неё — отсюда
  // и не влезающие кнопки, и дёрганье карточки между расчётами.
  const rescue = (k) => {
    let cur = k;
    if (!wide) return cur;
    if (card.classList.contains('is-panelled')) return cur;   // там высота задана точно
    if (availHeight(root) - card.getBoundingClientRect().height < 12) return cur;
    root.classList.remove('is-scroll');
    if (cur < WIDE_MAX_ZOOM - 0.002) cur = fitsAt(WIDE_MAX_ZOOM) ? WIDE_MAX_ZOOM : bisect(cur, WIDE_MAX_ZOOM);
    return cur;
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
    // Планшет может ещё поднять масштаб (у него запас по ширине), телефон — нет: масштаб
    // уже утверждён проверкой на прокрутку, и пересматривать его после неё нельзя, именно
    // это и выносило кнопки под сгиб. А высоту до экрана добирают оба — всегда.
    const kFinal = rescue(k);
    fillToScreen(kFinal);
    remember(kFinal);
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

  // Растим карточку, только если она уже занимает БОЛЬШУЮ ЧАСТЬ экрана. Короткий экран
  // (заставка, обратный отсчёт, слово и три кнопки) не раздуваем — ему воздух идёт на
  // пользу, а раздутый он выглядит именно так, как выглядел: гигантские буквы и мишени
  // во весь экран. На планшете исключения нет: там раскладка двухколоночная и содержимое
  // по определению ниже экрана.
  const grow = () => {
    stretch(1);
    const left = avail - card.getBoundingClientRect().height;
    if (left >= 10 && (wide || h >= avail * 0.6)) {
      settle(fitsAt(zoomCeil) ? zoomCeil : bisect(1, zoomCeil));
      return;
    }
    settle(1);
  };

  if (h <= avail) { branch = '1grow'; grow(); return; }   // есть запас — сначала внутреннему блоку, потом рост

  // 2. Отдаём отступы — это бесплатно.
  root.classList.add('is-tight');
  avail = availHeight(root);
  h = card.getBoundingClientRect().height;
  if (h <= avail) { branch = '2tight'; grow(); return; }

  // 3. Ужимаем карточку целиком — пока это не бьёт по читаемости. Масштаб подбираем
  //    замером, а не формулой: при другом кегле текст переносится иначе.
  const k0 = (avail - 2) / h;
  if (k0 >= MILD_ZOOM) {
    const k = fitsAt(k0) ? bisect(k0, Math.min(1, k0 * 1.35)) : bisect(Math.max(MIN_ZOOM, k0 * 0.85), k0);
    if (card.getBoundingClientRect().height <= avail) { branch = '3shrink'; settle(k); return; }
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
    if (card.getBoundingClientRect().height <= avail + EPS) { branch = '4panel'; settle(kp); return; }
    // не помогло — откатываем к обычной прокрутке
    card.classList.remove('is-panelled');
    panel.classList.remove('is-fit-panel');
    card.style.maxHeight = '';
  }

  // 5. Последний вариант: ужимаем до предела и оставляем прокрутку, но кнопку выхода
  //    прижимаем к низу экрана — до неё не придётся долистывать.
  setZoom(card, MIN_ZOOM);
  root.classList.add('is-scroll');

  // ПРОВЕРКА ПО ФАКТУ, и она обязательна. Ужатие делает карточку не только ниже, но и ШИРЕ
  // (max-width задан в её собственных px, а их масштабирует zoom) — текст перетекает в
  // меньшее число строк, и высота падает СКАЧКОМ, а не плавно. Из-за этого сюда регулярно
  // попадала карточка, которая после ужатия занимает ~70% экрана: прокручивать уже нечего,
  // а растянуть её мы себе запретили (в режиме прокрутки растягивание отключено). Ровно так
  // и выглядела жалоба: мелкий текст и пустые полосы сверху и снизу.
  //
  // Раз по факту всё помещается — никакой прокрутки не нужно. Снимаем её и подбираем самый
  // КРУПНЫЙ масштаб, который реально влезает; потолок — 1, выше поднимать нельзя, это уже
  // проходили. settle() заново проверит, что страница не прокручивается, и дотянет карточку
  // до высоты экрана.
  if (card.getBoundingClientRect().height <= avail - 4) {
    root.classList.remove('is-scroll');
    branch = '5back';
    settle(fitsAt(1) ? 1 : bisect(MIN_ZOOM, 1));
    return;
  }

  branch = '5scroll';
  const kLast = rescue(MIN_ZOOM);
  fillToScreen(kLast);
  remember(kLast);
}

// ВРЕМЕННО. Строка с числами расчёта поверх экрана — видна ТОЛЬКО владельцу (его telegram
// id), остальным её нет. Нужна, чтобы один скриншот показывал, что движок считает экраном и
// какой ветке он отдал карточку: три дня правок вслепую упирались в то, что стенд считает
// одно, а телефон ведёт себя иначе. Снять сразу, как только причина будет найдена.
const DEBUG_OWNER_ID = 117649764;
function debugOn() {
  try { return Number(window.Telegram?.WebApp?.initDataUnsafe?.user?.id) === DEBUG_OWNER_ID; }
  catch (_e) { return false; }
}
function debugLine(root, card, branch) {
  if (!debugOn()) return;
  let el = document.getElementById('fitdbg');
  if (!el) {
    el = document.createElement('div');
    el.id = 'fitdbg';
    el.style.cssText = 'position:fixed;left:0;right:0;top:0;z-index:99999;font:10px/1.25 ui-monospace,monospace;'
      + 'background:rgba(0,0,0,.82);color:#7CFC00;padding:2px 4px;white-space:pre-wrap;pointer-events:none';
    document.body.appendChild(el);
  }
  const tg = window.Telegram?.WebApp;
  const probe = document.getElementById('fitdbgprobe') || (() => {
    const p = document.createElement('div');
    p.id = 'fitdbgprobe';
    p.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:100dvh;pointer-events:none;opacity:0';
    document.body.appendChild(p);
    return p;
  })();
  const r = card.getBoundingClientRect();
  const vh = viewportHeight();
  el.textContent = `dvh=${Math.round(probe.getBoundingClientRect().height)} vv=${Math.round(window.visualViewport?.height || 0)} `
    + `iH=${window.innerHeight} tg=${Math.round(Number(tg?.viewportStableHeight) || 0)}/${tg?.isExpanded} `
    + `vh=${Math.round(vh)} avail=${Math.round(availHeight(root))} k=${card.style.zoom || '1'} `
    + `card=${Math.round(r.height)}(${Math.round(r.height / vh * 100)}%) top=${Math.round(r.top)} `
    + `br=${branch} cls=${card.className.replace(/ans-card|as-card|al-card|\s+/g, ' ').trim()}|${root.className.replace('ans-root', '').trim()}`;
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
      st.hard = 0;
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
      // Содержимое поменялось — считаем заново. Сбрасываем и запомненную высоту: карточка,
      // растянутая на весь экран, имеет ЗАКРЕПЛЁННУЮ высоту, и по ней смену содержимого не
      // видно — сравнение «высота такая же» приняло бы новый экран за старый и оставило
      // ему чужой масштаб.
      try {
        document.querySelectorAll('.ans-root > .ans-card').forEach((card) => {
          const st = stateOf(card);
          st.runs = 0;
          st.hard = 0;
          st.vis = 0;
        });
      } catch (_e) { /* noop */ }
      run();
    });
    mo.observe(document.body, { childList: true, subtree: true });
  }

  window.addEventListener('resize', freshStart);
  window.addEventListener('orientationchange', freshStart);
  // Видимая область меняется без `resize`: шторка Telegram, панели браузера, клавиатура.
  // Ровно для этого и существует visualViewport — штатный сигнал, а не наша выдумка.
  // `scroll` здесь НЕ слушаем намеренно: на iOS он приходит на каждый кадр инерционной
  // прокрутки, и полный пересчёт на каждом кадре — это и есть то самое «дёрганье» карточки.
  // Размер видимой области от прокрутки не меняется, так что сигнал нам ничего не даёт.
  try {
    window.visualViewport?.addEventListener('resize', freshStart);
  } catch (_e) { /* noop */ }
  // Событие Telegram — тоже повод пересчитать (величину берём у браузера, см. viewportHeight).
  try { window.Telegram?.WebApp?.onEvent?.('viewportChanged', freshStart); } catch (_e) { /* noop */ }

  // Прогрев: несколько пересчётов в первые пару секунд.
  //
  // Разворот шторки — это АНИМАЦИЯ, и её событие может прийти раньше, чем мы подписались:
  // тогда мы навсегда останемся с высотой, снятой в середине анимации, и карточка окажется
  // подогнана под экран, которого уже нет. Пять дешёвых пересчётов закрывают этот разрыв
  // надёжнее любой подписки — и на этом заканчиваются, дальше работают события.
  [120, 320, 700, 1400, 2500].forEach((ms) => { setTimeout(freshStart, ms); });

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
