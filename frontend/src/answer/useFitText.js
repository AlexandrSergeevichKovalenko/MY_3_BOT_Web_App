import { useCallback, useLayoutEffect, useRef } from 'react';

// Один общий canvas на весь роут — только для замера ширины текста, в DOM не попадает.
let measureCanvas = null;
function fitCanvas() {
  if (!measureCanvas) measureCanvas = document.createElement('canvas');
  return measureCanvas;
}

// Shrink a one-line text element so it always fits its container's width on ONE
// line (no ugly mid-phrase wrap), on any screen. Attach the returned ref to an
// inline-block element with `white-space: nowrap` (e.g. class "fit-line"); it is
// re-fit whenever `dep` changes and on window resize.
// `max: 'css'` — потолок берём из самой вёрстки: размер НИКОГДА не больше того, что задал
// CSS (а он у нас считается от экрана и на планшете свой), хук только уменьшает, если слово
// не влезло. Так подгонка кегля не ломает согласованную типографику — она её страхует.
// `fitBy: 'word'` — влезать должно САМОЕ ДЛИННОЕ СЛОВО, а не вся фраза в одну строку.
// Тогда «die Zustimmung» в узкой колонке планшета спокойно переносится на две строки
// крупным кеглем, а «die Geschwindigkeitsbegrenzung» ужимается — ровно настолько, чтобы
// не разрезаться посередине. Режим по умолчанию (вся фраза в одну строку) не тронут.
export default function useFitText(dep, { max = 40, min = 14, padding = 28, fitBy = 'line' } = {}) {
  const ref = useRef(null);
  // Отпечаток последнего подбора: текст + ширина поля + настройки.
  //
  // Подбор идёт после КАЖДОЙ отрисовки — иначе первое слово спринта остаётся неподогнанным
  // (эффект отрабатывал, когда элемента ещё нет на экране). Но сам подбор не бесплатный для
  // глаза: он на миг переводит строку в режим «без переносов», меряет и возвращает обратно.
  // Пока это случалось только при смене слова, никто не замечал. А теперь карточка
  // перерисовывается и от чужих причин — например, когда открывается окно «Спросить»: React
  // перерисовывает игру, подбор запускается снова, фраза мигает и перекладывается. Именно это
  // и выглядит как «основной экран дёргается, хотя я в нём ничего не делаю».
  //
  // Поэтому: те же входные данные — ничего не трогаем вообще.
  const lastFit = useRef({ key: '', w: -1 });
  const fit = useCallback(() => {
    const el = ref.current;
    const box = el?.parentElement;
    if (!el || !box) return;
    // Ширину сравниваем С ДОПУСКОМ: заморозка базового кегля меняет её на доли пикселя
    // (max-width карточки задан в rem), и без допуска строка перекладывалась бы от каждого
    // такого дрожания — то самое дёрганье при работе с окном «Спросить».
    const w = box.clientWidth;
    // Строки меряем ДИАПАЗОНОМ ПО ТЕКСТУ, а не прямоугольниками элемента: элемент со словом
    // — `inline-block`, у него всегда РОВНО ОДИН прямоугольник, сколько бы строк ни легло
    // внутри. По нему разрыв слова не увидеть вообще (на этом я и попался в первой версии
    // этой проверки). Диапазон отдаёт по прямоугольнику на каждую строку текста.
    const lineRects = () => {
      try {
        const rng = document.createRange();
        rng.selectNodeContents(el);
        return Array.from(rng.getClientRects()).filter((r) => r.width > 0 && r.height > 0);
      } catch (_e) { return []; }
    };
    // «Сирота» — строка-огрызок короче четверти колонки. Это всегда разрыв ВНУТРИ слова
    // («anwesen» + одинокая «d»). Перенос по пробелу («die» / «Zustimmung») сиротой не
    // считается: это нормальный перенос, кегль трогать незачем.
    // РАЗОРВАНО ЛИ СЛОВО — считаем по строкам, а не по ширинам. Перенос по пробелу законный:
    // фраза из двух слов имеет право лечь в две строки. А вот строк БОЛЬШЕ, чем слов, бывает
    // только в одном случае — какое-то слово разрезали посередине. Это и есть «anwesen» плюс
    // одинокая «d». Правило не зависит ни от единиц, ни от масштаба карточки.
    const wordsAll = String(el.textContent || '').split(/\s+/).filter(Boolean);
    const splitWord = () => lineRects().length > Math.max(1, wordsAll.length);
    const key = `${el.textContent || ''}|${max}|${min}|${padding}|${fitBy}`;
    // Предохранитель от лишних пересчётов — но он НЕ ДОЛЖЕН молчать над уже сломанным
    // словом. Если слово прямо сейчас разорвано, считаем заново при любых входных данных:
    // расчёт мог промахнуться по причине, которой в его входных данных нет (шрифт доехал
    // позже, ширина колонки поменялась в тот же кадр, поворот экрана).
    if (lastFit.current.key === key && Math.abs(lastFit.current.w - w) <= 2 && !splitWord()) return;
    lastFit.current = { key, w };
    el.style.whiteSpace = 'nowrap'; // measure/shrink on one line first
    let size = max;
    if (max === 'css') {
      el.style.fontSize = '';                                  // вернуть размер из CSS…
      size = parseFloat(getComputedStyle(el).fontSize) || 24;   // …и стартовать от него
    }
    el.style.fontSize = `${size}px`;
    const avail = box.clientWidth - padding;
    let guard = 0;
    if (fitBy === 'word') {
      // Ширину самого длинного слова меряем на canvas: без вставки узлов в DOM, которым
      // владеет React, и без зависимости от текущего переноса строк.
      const words = String(el.textContent || '').split(/\s+/).filter(Boolean);
      const longest = words.sort((a, b) => b.length - a.length)[0] || '';
      const cs = getComputedStyle(el);
      const ctx = fitCanvas().getContext('2d');
      const widthAt = (px) => {
        ctx.font = `${cs.fontStyle} ${cs.fontWeight} ${px}px ${cs.fontFamily}`;
        return ctx.measureText(longest).width;
      };
      // Кегль считаем ОДНОЙ формулой, а не подбором по одному пикселю: ширина текста растёт
      // ровно пропорционально кеглю, поэтому нужный размер = текущий × (сколько влезает /
      // сколько занимает). Подбор давал тот же ответ, но за десятки промежуточных раскладок,
      // и каждый промежуточный шаг менял высоту карточки — а её масштаб, в свою очередь,
      // менял ширину колонки. Эта петля и выдавала то верный кегль, то вдвое меньший.
      const need = longest ? widthAt(size) : 0;
      if (need > avail && avail > 0) size = Math.max(min, Math.floor(size * avail / need));
      el.style.fontSize = `${size}px`;
      guard += 1;
    } else {
      while (el.scrollWidth > avail && size > min && guard < 80) {
        size -= 1;
        el.style.fontSize = `${size}px`;
        guard += 1;
      }
    }
    // Resting state: always allow normal word-wrapping. If the phrase fits at the
    // size we just settled on, it stays on one line anyway; but if a later change
    // we can't always re-measure in time (web font finishing, the Telegram sheet
    // width settling) makes it overflow, the browser wraps the overflowing word
    // onto the next line instead of clipping it. Combined with `overflow-wrap:
    // break-word` in the CSS, whole words wrap at the spaces — a long word like
    // "Kaffeemaschine" drops to the next line intact rather than being cut off.
    el.style.whiteSpace = 'normal';

    // ── ПРОВЕРКА ПО ФАКТУ ───────────────────────────────────────────────────────────
    //
    // Всё выше — РАСЧЁТ: сколько места займёт самое длинное слово при таком кегле. Расчёт
    // может промахнуться (шрифт доехал позже, ширина колонки поменялась в том же кадре,
    // округления масштаба карточки, поворот экрана) — и тогда пользователь видит ровно то,
    // на что жалуется: «anwesen» на одной строке и одинокая «d» на следующей. Замер по
    // факту не промахивается: спрашиваем у браузера, на сколько строк ЛЁГ текст.
    //
    // Порядок ровно тот, что нужен: сначала уменьшаем кегль, пока слово не перестанет
    // делиться; и только упершись в нижний предел, разрешаем перенос ПО ПРАВИЛАМ ЯЗЫКА
    // (со знаком переноса) — это и включает класс ниже вместе с `hyphens: auto` в CSS.
    // У страховки есть ПОЛ: расчёт по ширине слова надёжен (проверено — канвас и вёрстка
    // дают одно и то же), ей нужно лишь добрать его промах. Без пола единичный замер,
    // пойманный на неустоявшейся раскладке, уводил кегль вдвое вниз и там и оставлял.
    const floor = Math.max(min, Math.round(size * 0.85));
    let guard2 = 0;
    while (splitWord() && size > floor && guard2 < 60) {
      size -= 1;
      el.style.fontSize = `${size}px`;
      guard2 += 1;
    }
    // Уменьшать больше нельзя, а слово всё равно не помещается — значит оно и правда длиннее
    // строки (немецкие сложносоставные бывают). Тогда перенос ПО ПРАВИЛАМ ЯЗЫКА, со знаком
    // переноса: включает CSS (`hyphens: auto` + `lang="de"`), здесь только помечаем случай.
    el.classList.toggle('fit-hyphen', splitWord());
  }, [max, min, padding, fitBy]);

  // Подбор кегля — ПОСЛЕ КАЖДОЙ ОТРИСОВКИ, без списка зависимостей. И это не небрежность.
  //
  // Раньше здесь стояло `[dep, fit]`, и первое слово спринта оставалось неподогнанным.
  // Порядок такой: игра монтируется на экране отсчёта, элемента со словом ещё нет, эффект
  // отрабатывает вхолостую (ref пустой). Потом отсчёт заканчивается, слово появляется — но
  // `dep` (индекс слова) всё ещё 0 и `fit` тот же, значит эффект НЕ перезапускается. Кегль
  // так и остаётся тот, что дал CSS от ширины экрана, и длинное слово вроде «Aschenbecher»
  // браузер переносит посередине. Заодно не создавался и ResizeObserver — он тоже искал
  // элемент, которого ещё не было, поэтому и последующие изменения ширины ничего не чинили.
  //
  // Стоимость: цикл подбора ограничен и почти всегда завершается на первом сравнении, а эти
  // экраны перерисовываются по нажатию, а не постоянно.
  useLayoutEffect(() => { fit(); });
  // ДОВОДОЧНЫЕ ПРОХОДЫ. Первый расчёт нередко попадает на ещё не устоявшуюся раскладку:
  // карточка в этот же кадр получает свой масштаб, планшетная сетка переставляет колонки,
  // шторка Telegram доезжает. Тогда кегль подбирается под ширину, которой уже нет, и
  // остаётся таким навсегда — предохранитель выше считает, что входные данные не менялись.
  // Два поздних пересчёта дешевле застрявшего размера; тот же приём, что в подгонке карточки.
  useLayoutEffect(() => {
    const again = () => { lastFit.current = { key: '', w: -1 }; fit(); };
    // Позже, чем доводочные проходы самой карточки (400 и 1200 мс в fitCard): ширина колонки
    // задана в rem и меняется вместе с масштабом карточки, поэтому считать кегль слова имеет
    // смысл только после того, как карточка свой масштаб утвердила. Иначе получается гонка:
    // одно слово подгоняется под одну ширину, соседнее — под другую.
    const t1 = setTimeout(again, 550);
    const t2 = setTimeout(again, 1600);
    return () => { clearTimeout(t1); clearTimeout(t2); };
  }, [fit]);
  // re-fit on viewport resize / rotation AND whenever the container's own width
  // settles (Telegram WebApp sheet animates in → the box width isn't final on first
  // layout; a plain window-resize listener doesn't catch that). ResizeObserver fires
  // an initial callback on observe, so this also covers the post-mount width.
  // Тот же список зависимостей заменён на «после каждой отрисовки» по той же причине:
  // наблюдатель за шириной нужно ставить тогда, когда элемент уже есть в разметке.
  useLayoutEffect(() => {
    // ПОСЛЕ КАЖДОГО ИЗМЕНЕНИЯ РАЗМЕРА — не только пересчёт сразу, но и доводочный пересчёт
    // погодя. Поворот планшета меняет ширину колонки не мгновенно: карточка в это же время
    // пересчитывает свой масштаб, а ширина колонки задана в rem и от масштаба зависит.
    // Первый пересчёт попадает в середину этого перехода и берёт ширину, которой уже нет —
    // отсюда «слово вдруг стало вдвое мельче» после поворота. Доводочный проход считает по
    // устоявшейся раскладке. Раньше такие проходы были только при открытии экрана.
    let settle = 0;
    const refit = () => {
      fit();
      clearTimeout(settle);
      settle = setTimeout(() => { lastFit.current = { key: '', w: -1 }; fit(); }, 600);
    };
    window.addEventListener('resize', refit);
    let ro;
    const box = ref.current?.parentElement;
    if (box && typeof ResizeObserver !== 'undefined') {
      // Реагируем только на смену ШИРИНЫ. Высота блока меняется от самой подгонки (кегль
      // стал меньше — строка стала ниже), и наблюдатель тут же будил бы новый пересчёт:
      // получалась карусель, в которой итоговый размер зависел от того, где её оборвали.
      let seenW = box.clientWidth;
      ro = new ResizeObserver(() => {
        const w2 = box.clientWidth;
        if (Math.abs(w2 - seenW) <= 1) return;
        seenW = w2;
        refit();
      });
      ro.observe(box);
    }
    // Re-fit once web fonts finish loading. The first measurement runs with a
    // fallback font (narrower) → text "fits" and isn't shrunk; when Manrope/Onest
    // load the text gets wider and overflows, but nothing else re-triggers fit.
    let cancelled = false;
    const reFit = () => { if (!cancelled) fit(); };
    const fontSet = typeof document !== 'undefined' ? document.fonts : null;
    if (fontSet) {
      try { fontSet.ready.then(reFit); } catch (_e) { /* ignore */ }
      try { fontSet.addEventListener('loadingdone', reFit); } catch (_e) { /* ignore */ }
    }
    return () => {
      cancelled = true;
      clearTimeout(settle);
      window.removeEventListener('resize', refit);
      ro?.disconnect();
      if (fontSet) {
        try { fontSet.removeEventListener('loadingdone', reFit); } catch (_e) { /* ignore */ }
      }
    };
  });

  return ref;
}
