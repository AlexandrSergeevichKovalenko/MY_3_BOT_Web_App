// Ядро подгонки модального окна под экран — чистые замеры DOM, без React.
//
// Вынесено отдельно от хука намеренно: так стенд проверки (матрица телефонов) гоняет
// РОВНО ТОТ ЖЕ код, что и приложение, а не его копию. Копия расходится с оригиналом на
// первой же правке, и матрица начинает подтверждать то, чего в продукте нет.
//
// Принцип тот же, что у интерактивов (`src/answer/fitCard.js`): экран пользователя должен
// использоваться целиком на любом телефоне. Отличие одно — там масштабируется карточка
// целиком (`zoom`), здесь только КЕГЛЬ (`--wh-k`): модалка обязана оставаться во всю
// ширину экрана, а `zoom` ужал бы и её.

// Потолок роста кегля. Телефон: колонка узкая, немецкие слова длинные — выше 1.18 на
// матрице телефонов начинает разрезать «Zusammenarbeit». Планшет: колонка вдвое шире.
// Те же числа, что в fitCard.js.
export const PHONE_MAX_K = 1.18;
export const WIDE_MAX_K = 1.35;
const SLACK = 12; // меньше этого запаса растягивать нечего

// Высота, которой мы реально располагаем. МИНИМУМ ИЗ ДВУХ ЧИСЕЛ, и это не перестраховка:
// в Telegram мини-апп живёт в шторке, но сам вебвью выложен на всю высоту экрана — шторка
// просто закрывает его низ. И `visualViewport.height`, и `innerHeight` про шторку не знают
// и возвращают полную высоту; видимую часть знает только `viewportStableHeight` Telegram.
// Без него окно растянется под несуществующую высоту и кнопка «Понятно» уедет под сгиб.
// Обратная беда (число Telegram отстаёт во время анимации разворота) лечится не
// выбрасыванием числа, а пересчётом — подписки живут в useModalFit.
// Дословно как в fitCard.js: расхождение означало бы, что интерактив и модалка считают
// экран по-разному.
export function viewportHeight() {
  const vv = typeof window !== 'undefined' ? window.visualViewport : null;
  const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;
  const browser = Number(vv?.height) || Number(window.innerHeight) || 0;
  const stable = Number(tg?.viewportStableHeight) || 0;
  if (stable > 200 && browser > 200) return Math.min(stable, browser);
  return Math.max(stable, browser);
}

// Порог «широкого» экрана тот же, что у медиазапроса вёрстки (ширина от 700 И высота от
// 560), чтобы движок и CSS считали планшетом одно и то же. Телефон в горизонтали сюда
// намеренно не попадает: высоты там нет.
function isWide() {
  return window.innerWidth >= 700 && viewportHeight() >= 560;
}

function overflows(el) {
  return el.scrollHeight - el.clientHeight > 2;
}

/**
 * Один синхронный проход: сброс → замер → применение. Ничего не применяется «по шагам»
 * и не отыгрывается назад позже — иначе окно на глазах прыгает.
 *
 * @returns {{k:number, filled:boolean, used:number}|null} масштаб, растянуто ли окно и
 *          какую долю видимой высоты оно заняло. Возвращается для стенда проверки.
 */
export default function fitModal(overlay, card, body) {
  if (!overlay || !card || !body) return null;

  // 1. Видимая высота — оверлею. `inset: 0` дал бы высоту вебвью вместе с частью,
  //    закрытой шторкой Telegram, и окно ушло бы вниз за край видимого.
  const vh = viewportHeight();
  if (!(vh > 200)) return null;
  overlay.style.setProperty('--wh-vh', `${Math.round(vh)}px`);

  // Всегда считаем от натурального вида: иначе окно «залипнет» на масштабе прошлого
  // слова — у длинной фразы и у короткого слова он разный.
  card.style.setProperty('--wh-k', '1');
  card.style.height = '';
  card.classList.remove('is-filled');

  const pad = (parseFloat(getComputedStyle(overlay).paddingTop) || 0) * 2;
  const avail = vh - pad;
  if (!(avail > 120)) return null;

  // Высоту берём у РАСКЛАДКИ (offsetHeight), а не у getBoundingClientRect: окно
  // появляется анимацией `scale(0.96)`, а прямоугольник учитывает трансформацию — в
  // первом кадре он меньше правды на 4%, и окно выросло бы больше, чем помещается.
  const cardHeight = () => card.offsetHeight;

  const report = (k, filled) => ({ k, filled, used: cardHeight() / avail });

  // 2. Текста больше, чем экран: тело окна прокручивается внутри, шапка со словом и
  //    кнопка «Понятно» видны всегда. Кегль не уменьшаем — читаемость дороже.
  if (overflows(body)) return report(1, false);

  // 3. Растим кегль ЗАМЕРОМ, а не формулой: при другом кегле текст переносится иначе.
  //    lo — заведомо помещается, hi — уже нет.
  const ceil = isWide() ? WIDE_MAX_K : PHONE_MAX_K;
  const fitsAt = (k) => {
    card.style.setProperty('--wh-k', String(Math.round(k * 1000) / 1000));
    return !overflows(body) && cardHeight() <= avail + 1;
  };
  let lo = 1;
  let hi = ceil;
  if (fitsAt(ceil)) {
    lo = ceil;
  } else {
    for (let i = 0; i < 5; i += 1) {
      const mid = (lo + hi) / 2;
      if (fitsAt(mid)) lo = mid; else hi = mid;
    }
  }
  card.style.setProperty('--wh-k', String(Math.round(lo * 1000) / 1000));

  // 4. Место осталось даже на потолке кегля — добираем высотой самой карточки,
  //    содержимое встаёт по центру. Прокрутки это добавить не может: тянем ровно до
  //    доступной высоты и только когда запас есть.
  let filled = false;
  if (avail - cardHeight() >= SLACK) {
    card.style.height = `${Math.round(avail)}px`;
    card.classList.add('is-filled');
    filled = true;
  }
  return report(lo, filled);
}
