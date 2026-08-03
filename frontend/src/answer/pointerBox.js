// Куда именно ткнул палец внутри карточки интерактива.
//
// Карточка подгоняется под экран через CSS `zoom` (fitCard.js). И вот тут движки
// расходятся — замер 03.08.2026, одна и та же вёрстка (блок внутри карточки с
// `zoom: 1.18` и с `zoom: 0.82`), настоящее место блока найдено попаданиями
// `elementFromPoint` — это ровно та система координат, в которой приходят события:
//
//   Safari 18.6 (WebKit — движок мини-аппа в Telegram на iPhone)
//       getBoundingClientRect() → 640×220, top=172
//       на самом деле блок      → 757×260, top=203      ← ровно в 1.18 раза больше
//       то есть WebKit отдаёт прямоугольник в СОБСТВЕННЫХ координатах масштабированного
//       поддерева, БЕЗ учёта zoom.
//   Chrome 148
//       getBoundingClientRect() → 755×260 при настоящих 756×261 — уже с учётом zoom.
//
// Отсюда и «синий кружок не там, куда ткнул» в «Finde im Bild»: доля считалась как
// (clientX − rect.left) / rect.width, а в WebKit это деление величин из разных систем
// координат. Тап в середину картинки уезжал вниз и вправо, а на высокой картинке ещё и
// упирался в нижний край.
//
// Движки различаем ЗАМЕРОМ, а не по User-Agent: `offsetWidth` в обоих — собственная,
// немасштабированная ширина элемента, и этого достаточно, чтобы понять, в какой системе
// координат нам отдали прямоугольник.

// Накопленный `zoom` элемента и всех его предков.
export function zoomOf(el) {
  let z = 1;
  for (let n = el; n && n.nodeType === 1; n = n.parentElement) {
    const v = parseFloat(getComputedStyle(n).zoom);
    if (Number.isFinite(v) && v > 0) z *= v;
  }
  return z;
}

// Прямоугольник элемента В КООРДИНАТАХ УКАЗАТЕЛЯ — тех же, в которых приходят
// clientX/clientY. Считать по нему можно в любом браузере.
export function pointerRect(el) {
  const r = el.getBoundingClientRect();
  const z = zoomOf(el);
  if (!(z > 0) || (z > 0.999 && z < 1.001)) return r;
  const own = el.offsetWidth || 0;
  // Chrome: rect уже с учётом zoom (rect.width ≈ own × z) — берём как есть.
  if (own > 0 && Math.abs(r.width - own * z) <= Math.abs(r.width - own)) return r;
  // WebKit: rect без учёта zoom — переводим весь прямоугольник целиком.
  return {
    left: r.left * z, top: r.top * z,
    right: r.right * z, bottom: r.bottom * z,
    width: r.width * z, height: r.height * z,
  };
}

// Доля 0…1 по горизонтали и вертикали — то, что сохраняется как ответ и чем рисуется
// метка (метка стоит в процентах от блока, поэтому доли всегда совпадают с картинкой).
export function pointerFraction(el, clientX, clientY) {
  const r = pointerRect(el);
  const w = r.width || 1;
  const h = r.height || 1;
  return {
    x: Math.min(1, Math.max(0, (clientX - r.left) / w)),
    y: Math.min(1, Math.max(0, (clientY - r.top) / h)),
  };
}
