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
let typing = false;      // пока печатают В КАРТОЧКЕ, подгонку снимаем
let askTyping = false;   // печатают в окне «Спросить» — карточку не трогаем вовсе
let settling = false;    // клавиатура ещё едет — любой пересчёт сейчас будет по неверной высоте
let rafId = 0;
let timerId = 0;
let ro = null;

function supportsZoom() {
  try { return !!(window.CSS && CSS.supports && CSS.supports('zoom', '0.9')); } catch (_e) { return false; }
}

// Высота, которой мы реально располагаем. ОДИН ИСТОЧНИК — БРАУЗЕР.
//
// `visualViewport.height` — то же число, что стоит за единицей `dvh`, и именно по нему
// браузер рисует страницу; разойтись с тем, что видит пользователь, оно не может.
//
// Числа Telegram (`viewportStableHeight`) здесь НЕТ намеренно. Замер по кадру с живого
// телефона: мини-апп развёрнут на весь экран, `dvh` даёт 723, а Telegram отдаёт ~566. По
// его числу карточка подгонялась под экран на треть меньше настоящего — 69% высоты и
// пустые полосы сверху и снизу. Проверить свежесть этого числа изнутри страницы нельзя,
// поэтому мы на него не опираемся.
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

  // Печатают в окне «Спросить» — оно к заданию отношения не имеет. Ничего не считаем и
  // ничего не меняем: карточка остаётся ровно такой, какой была.
  if (askTyping) return;

  if (typing) {
    // Интерактивы, которые ПОД КЛАВИАТУРУ НЕ ПЕРЕСТРАИВАЮТСЯ (класс `ans-root--keepkbd`).
    //
    // Там, где в самой карточке печатать нечего, клавиатура выезжает ровно за одним —
    // задать вопрос в окне «Спросить». Само окно живёт отдельно от карточки и само встаёт
    // над клавиатурой, поэтому перекраивать под неё задание незачем: пользователь видит,
    // как экран без причины дёргается и ужимается. Замораживаем всё как есть — клавиатура
    // просто ложится поверх.
    if (root.classList.contains('ans-root--keepkbd')) return;
    // Остальные (диктант чисел, работа над ошибками, перевод) — поле ввода ВНУТРИ карточки,
    // и его надо видеть. Плюс при zoom < 1 поле мельче 16px, и iOS начинает зумить страницу
    // сам. Пока печатают, отдаём полный размер и обычную прокрутку.
    if (st.k !== 1 || st.stretched) { resetCard(root, card, st); st.k = 1; st.vis = 0; }
    return;
  }

  // Ничего не изменилось с прошлого расчёта — не трогаем (иначе лишние reflow).
  const availNow = availHeight(root);
  const visNow = card.getBoundingClientRect().height;
  if (st.vis && Math.abs(visNow - st.vis) < EPS && Math.abs(availNow - st.avail) < EPS) return;
  // Предохранитель от «подрастания»: на одно и то же содержимое хватает пары расчётов.
  // Дальше замолкаем до следующей смены контента (её ловит MutationObserver) или размера
  // окна — иначе редкие расхождения в пару пикселей гоняли бы карточку кадр за кадром.
  // Но если ИЗМЕНИЛАСЬ САМА ВЫСОТА ЭКРАНА — это не дребезг, а новая вводная (шторка
  // Telegram доехала), и предохранитель снимаем: иначе карточка навсегда останется
  // подогнанной под старую, меньшую высоту — «скукоженной» посреди пустого экрана.
  //
  // И ОБЯЗАН ПРОПУСКАТЬ НАСТОЯЩИЕ ИЗМЕНЕНИЯ. Это и есть причина «после ответа карточка
  // короче, снизу серая полоса»: разбор дорисовывается НЕ ЗА ОДИН ЗАХОД (React дорисовал
  // блок, доехал шрифт, встали отступы). Два прохода сгорали на промежуточных состояниях, а
  // последний, правильный, уже блокировался — и на экране навсегда оставался промежуточный
  // результат. Экран-вопрос в это не попадал: он рисуется за один заход. Отсюда и «одна
  // карточка нормально, соседняя нет».
  //
  // Правило: если высота отличается от той, которую мы САМИ выставили, — это новая вводная,
  // а не дребезг, и счётчик сбрасывается. От настоящих качелей страхует жёсткий потолок
  // проходов; он снимается только сменой содержимого или размера экрана.
  if (st.vis && Math.abs(visNow - st.vis) > EPS) st.runs = 0;
  if (Math.abs(availNow - st.avail) > EPS) { st.runs = 0; st.hard = 0; }
  if (st.runs >= 2 || st.hard >= 8) return;
  st.runs += 1;
  st.hard += 1;

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

  // Место всё равно осталось (упёрлись в предел масштаба — крупнее текст делать уже
  // некрасиво): карточка растягивается на всю высоту экрана, а остаток уходит внутрь неё.
  // Иначе получалось то, на что жалуются: карточка «скукожилась» посреди экрана, а сверху
  // и снизу пустые полосы.
  //
  // Работает и на телефоне, и на планшете. Масштаб при этом НЕ ТРОГАЕТСЯ — меняется только
  // высота самой карточки, и тянется она строго до видимой высоты. Это принципиально: беду
  // на телефоне творил не добор высоты, а подъём МАСШТАБА после финальной проверки «страница
  // не прокручивается» — он шёл мимо неё и выносил кнопки под сгиб. Здесь масштаб уже
  // утверждён и остаётся как есть, поэтому под сгиб уйти нечему.
  const fill = (k) => {
    if (root.classList.contains('is-scroll')) return;   // и так не влезло — растягивать нечего
    const cardH = card.getBoundingClientRect().height;
    const slack = availHeight(root) - cardH;
    if (slack < 4) return;
    card.classList.add('is-filled');
    card.style.minHeight = `${Math.round((cardH + slack - 2) / k)}px`;
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
    return cur;   // высоту добирает fill() в settle — общий путь для телефона и планшета
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
    // Планшет может ещё поднять масштаб (у него запас по ширине), телефон — нет. А остаток
    // высоты до края экрана добирают оба: пустых полос сверху и снизу быть не должно.
    const kFinal = rescue(k);
    fill(kFinal);
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

  if (h <= avail) { grow(); return; }   // есть запас — сначала внутреннему блоку, потом рост

  // 2. Отдаём отступы — это бесплатно.
  root.classList.add('is-tight');
  avail = availHeight(root);
  h = card.getBoundingClientRect().height;
  if (h <= avail) { grow(); return; }

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
  // И здесь проверяем по факту: если после ужатия на экране всё-таки осталось место —
  // расчёт выше ошибся, масштаб возвращаем (сюда попадала «скукоженная» карточка).
  const kLast = rescue(MIN_ZOOM);
  fill(kLast);
  remember(kLast);
}

function run() {
  rafId = 0;
  if (timerId) { clearTimeout(timerId); timerId = 0; }
  // Клавиатура ещё едет — считать нельзя: высота экрана в этот момент промежуточная.
  if (settling) return;
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
    const mo = new MutationObserver((records) => {
      // Окно «Спросить» рисуется в <body> порталом. Для наблюдателя это правка DOM — и он
      // честно запускал полный пересчёт карточки: она дёргалась в момент открытия окна и
      // ещё раз при его закрытии. Но к заданию окно отношения не имеет.
      // Реагируем только на правки ВНУТРИ самого интерактива.
      // ОКНО «СПРОСИТЬ» ЗАКРЫЛИ КРЕСТИКОМ — снимаем всё, что держали ради него.
      //
      // Флаг «печатают в окне» снимался ТОЛЬКО по потере фокуса. Но крестик убирает окно
      // вместе с полем ввода, а при удалении сфокусированного элемента браузер событие
      // потери фокуса не шлёт. Флаг оставался поднятым: замок прокрутки не снят, базовый
      // кегль заморожен, пересчёт запрещён. Карточка стояла в том виде, в каком её застала
      // клавиатура, и «дотягивалась» только когда расчёт случайно будило что-то другое.
      // Ровно это пользователь и видит как задержку в полсекунды и рывок.
      if (askTyping && !document.querySelector('.ask-pop')) {
        askTyping = false;
        unlockScroll();
        thawCardBox();
        thawBase();
        settleAfterKeyboard();   // клавиатура ещё уезжает — считаем, когда высота устоится
        return;
      }

      const touchesCard = records.some((r) => {
        const t = r.target;
        const el = t && t.nodeType === 1 ? t : t?.parentElement;
        return !!el && !!el.closest?.('.ans-root') && !el.closest?.('.ask-pop');
      });
      if (!touchesCard) return;
      // Содержимое поменялось — считаем заново. Сбрасываем и запомненную высоту: карточка,
      // растянутая на весь экран, имеет ЗАКРЕПЛЁННУЮ высоту, и по ней смену содержимого не
      // видно — сравнение «высота такая же» приняло бы новый экран за старый и оставило
      // ему чужой масштаб.
      try {
        document.querySelectorAll('.ans-root > .ans-card').forEach((card) => {
          const st = stateOf(card);
          st.runs = 0;
          st.hard = 0;   // смена содержимого снимает и жёсткий потолок проходов
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

  const isField = (el) => !!el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable);
  // ЗАМОРОЗКА БАЗОВОГО КЕГЛЯ, пока открыта клавиатура на интерактиве с `ans-root--keepkbd`.
  //
  // Это второй — и главный — источник «экран сам ужимается». В вёрстке базовый размер шрифта
  // всего приложения привязан к высоте экрана:
  //     html { font-size: clamp(13px, calc(1.42vh + 0.4vw + 4.8px), 18.5px); }
  // Клавиатура забирает высоту, `1.42vh` падает, базовый кегль уменьшается — а в `rem` от
  // него заданы ВСЕ размеры внутри карточки. Поэтому карточка ужимается сама, без всякого
  // пересчёта: движок в это время уже молчит (см. ветку `typing` выше), а картинка всё равно
  // едет. Отключить это в движке было нельзя — источник в CSS.
  //
  // Поэтому на время клавиатуры прибиваем базовый кегль к тому значению, что было ДО её
  // появления, и заодно закрепляем высоту корня. После закрытия — снимаем, и всё снова
  // считается от экрана как обычно.
  // Кроссворд сюда входит наравне с остальными: у него свой корень (`ans-root--cw`) и своя
  // раскладка, но базовый кегль у него ТОТ ЖЕ — общий для всего приложения. Пока эта строка
  // искала только `keepkbd`, кроссворд оставался без заморозки, и его масштабировало
  // целиком: сначала при выезде клавиатуры, потом обратно при её закрытии.
  const freezeBase = () => {
    const root = document.querySelector('.ans-root--keepkbd, .ans-root--cw');
    if (!root) return;
    const html = document.documentElement;
    if (!html.style.fontSize) {
      html.style.fontSize = getComputedStyle(html).fontSize;   // текущий, ещё «доклавиатурный»
    }
    // Высоту корня закрепляем только у обычных интерактивов. У кроссворда высота задана
    // свойством `height` и им управляет сам кроссворд (CrosswordGrid) — сюда не лезем.
    if (!root.classList.contains('ans-root--cw') && !root.style.minHeight) {
      const h = viewportHeight();
      if (h > 200) root.style.minHeight = `${Math.round(h)}px`;
    }
  };
  const thawBase = () => { document.documentElement.style.fontSize = ''; };

  // ГЛАВНОЕ ПРАВИЛО: поле ввода ВНЕ карточки (а это всегда окно «Спросить») не имеет к
  // заданию никакого отношения — значит и трогать задание не должно. Никак. Ни на каком
  // интерактиве. Карточка просто стоит, клавиатура ложится поверх.
  //
  // Раньше связь была двойная и обе стороны портили картинку: во-первых, базовый кегль всего
  // приложения считается от высоты экрана, и клавиатура его роняла; во-вторых, наблюдатель за
  // DOM видел появление окна и запускал полный пересчёт. Отсюда качели: карточка ужималась
  // при открытии окна и разжималась при закрытии.
  const inCard = (el) => !!el?.closest?.('.ans-card');

  // ЗАМОК ПРОКРУТКИ, пока печатают в окне «Спросить».
  //
  // Последняя — и самая заметная — половина «качелей»: карточка не ужимается, а СТРАНИЦА
  // ПРОКРУЧИВАЕТСЯ. iOS сам подкручивает страницу к полю ввода, а поле у нас в окне
  // «Спросить». Верх задания уезжает за экран, кнопка «Дальше» подскакивает к клавиатуре,
  // и при закрытии всё возвращается назад — со стороны это выглядит как масштабирование.
  //
  // Окно «Спросить» и так само встаёт над клавиатурой, подкручивать под него ничего не надо.
  // Поэтому на время ввода запоминаем положение прокрутки и возвращаем его: и запретом
  // прокрутки, и возвратом по факту — iOS уважает не каждый запрет.
  let lockedY = 0;
  const onLockedScroll = () => {
    if (!askTyping) return;
    if (Math.abs((window.scrollY || 0) - lockedY) > 1) window.scrollTo(0, lockedY);
  };
  // ЖЁСТКОЕ ЗАКРЕПЛЕНИЕ КАРТОЧКИ.
  //
  // Все предыдущие правки затыкали причины по одной: базовый кегль от высоты экрана,
  // прокрутка к полю ввода, события Telegram на каждом шаге клавиатуры, залипший флаг,
  // лишний подбор кегля строки. Каждая была настоящей — и каждый раз оставалась следующая.
  //
  // Здесь другой подход: пока пользователь печатает в окне «Спросить», интерактив
  // ФИКСИРУЕТСЯ В ПИКСЕЛЯХ на своём месте — `position: fixed` и точные размеры, снятые ДО
  // появления клавиатуры. Дальше он просто не может измениться: ни от единиц, считающих
  // высоту экрана, ни от изменения размера окна, ни от прокрутки, ни от любого пересчёта.
  // `position: fixed` привязан к разметочной области, а её клавиатура на iOS не двигает.
  //
  // Окно «Спросить» это не задевает: оно рисуется отдельно, в <body>.
  const frozenBoxes = [];
  const freezeCardBox = () => {
    document.querySelectorAll('.ans-root').forEach((root) => {
      if (root.dataset.frozen === '1') return;
      const r = root.getBoundingClientRect();
      if (!(r.height > 100)) return;
      frozenBoxes.push({ root, style: root.getAttribute('style') || '' });
      root.dataset.frozen = '1';
      root.style.position = 'fixed';
      root.style.top = `${Math.round(r.top)}px`;
      root.style.left = `${Math.round(r.left)}px`;
      root.style.width = `${Math.round(r.width)}px`;
      root.style.height = `${Math.round(r.height)}px`;
      root.style.minHeight = `${Math.round(r.height)}px`;
      root.style.overflow = 'hidden';
    });
  };
  const thawCardBox = () => {
    while (frozenBoxes.length) {
      const { root, style } = frozenBoxes.pop();
      delete root.dataset.frozen;
      if (style) root.setAttribute('style', style); else root.removeAttribute('style');
    }
  };

  const lockScroll = () => {
    lockedY = window.scrollY || 0;
    document.documentElement.style.overflow = 'hidden';
    document.body.style.overflow = 'hidden';
    window.addEventListener('scroll', onLockedScroll, { passive: true });
  };
  // ПОКА КЛАВИАТУРА УЕЗЖАЕТ — НЕ СЧИТАЕМ НИЧЕГО.
  //
  // Telegram сам шлёт `viewportChanged`, а браузер — `visualViewport.resize`, и не один раз, а
  // на каждом шаге ухода клавиатуры. Мы на них подписаны и запускаем полный пересчёт. Один
  // такой пересчёт попадает на момент, когда экран ещё НЕ вернулся: карточка считается под
  // урезанную высоту, встаёт короткой — снизу видна серая полоса, — и только следующим
  // событием дотягивается. Со стороны это ровно то, на что жалуется пользователь: «сначала
  // урезанный, потом масштабируется до конца».
  //
  // Поэтому от потери фокуса и до тех пор, пока высота экрана не перестанет меняться, все
  // пересчёты откладываются. Высота устоялась — один расчёт, сразу правильный.
  const settleAfterKeyboard = () => {
    settling = true;
    let last = viewportHeight();
    let stable = 0;
    const tick = () => {
      const now = viewportHeight();
      stable = Math.abs(now - last) < 2 ? stable + 1 : 0;
      last = now;
      if (stable >= 3) { settling = false; freshStart(); return; }   // ~180 мс без изменений
      setTimeout(tick, 60);
    };
    setTimeout(tick, 60);
  };

  const unlockScroll = () => {
    window.removeEventListener('scroll', onLockedScroll);
    document.documentElement.style.overflow = '';
    document.body.style.overflow = '';
    try { window.scrollTo(0, lockedY); } catch (_e) { /* noop */ }
  };

  document.addEventListener('focusin', (e) => {
    if (!isField(e.target)) return;
    freezeBase();          // ДО того, как клавиатура успеет изменить высоту
    if (!inCard(e.target)) { askTyping = true; freezeCardBox(); lockScroll(); return; }
    typing = true;
    schedule();
  });
  document.addEventListener('focusout', (e) => {
    if (!isField(e.target)) return;
    const wasAsk = askTyping && !inCard(e.target);
    askTyping = false;
    if (wasAsk) unlockScroll();
    typing = false;
    // Отпускаем кегль НЕ СРАЗУ: клавиатура закрывается с анимацией, и если снять
    // заморозку раньше, чем вернётся высота, карточка на эти доли секунды снова ужмётся —
    // это и есть вторая половина «качелей». Ждём, пока высота реально вернётся.
    const startedAt = viewportHeight();
    let tries = 0;
    const release = () => {
      tries += 1;
      if (viewportHeight() > startedAt + 40 || tries > 20) { thawBase(); freshStart(); return; }
      setTimeout(release, 60);
    };
    if (wasAsk) { thawCardBox(); release(); settleAfterKeyboard(); return; }   // ждём, пока экран устоится
    thawBase();
    setTimeout(freshStart, 120);
  });

  // Шрифты догружаются после первого layout и меняют высоту текста.
  try { document.fonts?.ready?.then(freshStart); } catch (_e) { /* noop */ }
  // Шторка Telegram разворачивается с анимацией: первый расчёт может попасть на ещё не
  // доехавшую высоту. Два поздних пересчёта дешевле, чем застрявшая маленькая карточка.
  setTimeout(freshStart, 400);
  setTimeout(freshStart, 1200);
  schedule();
}
