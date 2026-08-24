// Пара языков «откуда → куда» — один источник правды для обоих словарей.
//
// Зачем этот файл появился. До него направление перевода было переодетым ВЫКЛЮЧАТЕЛЕМ:
// по коду разбросано `source === 'de' ? 'de-ru' : 'ru-de'`, а определялся язык одной
// строчкой «есть кириллица → русский, иначе немецкий». В такую конструкцию третий язык
// не помещается физически: английский и немецкий пишутся одним алфавитом, и правило
// «иначе немецкий» назовёт «table» немецким словом.
//
// Поэтому языки здесь — ДАННЫЕ, а не код. Новый язык добавляется строкой в таблице
// LANGUAGES, а не новой веткой `if`. Но добавить строку — не значит подключить язык:
// по решению владельца от 07.08.2026 каждый язык входит вместе со своим правилом
// алфавита в базе, профилем грамматики и заслоном в очередях тренажёров. Здесь только
// первая из трёх частей.
//
// Главная честность этого модуля — он умеет отвечать «не знаю». Латиница сама по себе
// не отличает немецкий от английского; когда различить нельзя, мы не гадаем молча,
// а возвращаем confident:false, чтобы спросить человека.

// Порядок в таблице значим: при прочих равных выигрывает тот, кто выше.
export const LANGUAGES = {
  ru: { code: 'ru', name: 'Русский', script: 'cyrillic', letters: /[А-Яа-яЁё]/ },
  de: { code: 'de', name: 'Deutsch', script: 'latin', letters: /[A-Za-zÄÖÜäöüß]/ },
  en: { code: 'en', name: 'English', script: 'latin', letters: /[A-Za-z]/ },
};

// Языки, которые сегодня показываем человеку. Остальное в LANGUAGES — задел, который
// уже понимает код, но которого ещё нет в интерфейсе (см. решение «устройство
// мультиязычное, витрина по одному языку»).
export const ACTIVE_LANGUAGES = ['ru', 'de'];

export const DEFAULT_PAIR = { source: 'ru', target: 'de' };

export function languageName(code) {
  return LANGUAGES[code]?.name || String(code || '').toUpperCase();
}

export function isKnownLanguage(code) {
  return Object.prototype.hasOwnProperty.call(LANGUAGES, String(code || ''));
}

/** Каким алфавитом набран текст. null — если букв нет вовсе (цифры, знаки, пусто). */
export function detectScript(text) {
  const s = String(text || '');
  if (LANGUAGES.ru.letters.test(s)) return 'cyrillic';
  if (LANGUAGES.en.letters.test(s)) return 'latin';
  return null;
}

/** Языки, которые ФИЗИЧЕСКИ могут быть написаны так. Одного алфавита мало для ответа. */
export function candidatesByScript(text, allowed = ACTIVE_LANGUAGES) {
  const script = detectScript(text);
  if (!script) return [];
  return allowed.filter((code) => LANGUAGES[code]?.script === script);
}

export function pairCode(pair) {
  return `${pair.source}-${pair.target}`;
}

export function parsePairCode(code) {
  const [source, target] = String(code || '').split('-');
  if (!isKnownLanguage(source) || !isKnownLanguage(target) || source === target) return null;
  return { source, target };
}

export function flipPair(pair) {
  return { source: pair.target, target: pair.source };
}

export function samePair(a, b) {
  return Boolean(a && b && a.source === b.source && a.target === b.target);
}

/**
 * Определить пару «откуда → куда» для набранного текста.
 *
 * @param {string}  text     что набрал человек
 * @param {object}  profile  { learningLanguage, nativeLanguage } — что человек учит
 * @param {object}  override пара, выбранная человеком руками; она сильнее всего
 * @param {array}   allowed  какие языки сейчас включены
 *
 * Возвращает { source, target, confident, reason }.
 * confident:false означает «текст подходит нескольким языкам сразу» — тогда пара
 * выбрана по профилю, но интерфейс вправе предложить человеку поправить.
 */
export function resolvePair(text, { profile, override, allowed = ACTIVE_LANGUAGES } = {}) {
  const learning = isKnownLanguage(profile?.learningLanguage) ? profile.learningLanguage : DEFAULT_PAIR.target;
  const native = isKnownLanguage(profile?.nativeLanguage) ? profile.nativeLanguage : DEFAULT_PAIR.source;

  if (override && isKnownLanguage(override.source) && isKnownLanguage(override.target)
      && override.source !== override.target) {
    return { ...override, confident: true, reason: 'выбор человека' };
  }

  const candidates = candidatesByScript(text, allowed);

  // Букв нет вовсе — переводим «со своего на изучаемый», это самый частый случай.
  if (!candidates.length) {
    return { source: native, target: learning, confident: true, reason: 'по умолчанию' };
  }

  // Алфавит назвал ровно один язык — сомнений нет.
  if (candidates.length === 1) {
    const source = candidates[0];
    const target = source === learning ? native : learning;
    return { source, target, confident: true, reason: 'по алфавиту' };
  }

  // Несколько языков делят алфавит (немецкий и английский — латиница).
  // Молча гадать нельзя: «table» — английское слово, а не немецкое. Берём изучаемый
  // язык, если он среди подходящих, и честно говорим, что уверенности нет.
  const source = candidates.includes(learning) ? learning : candidates[0];
  const target = source === learning ? native : learning;
  return { source, target, confident: false, reason: 'алфавит делят несколько языков' };
}

/**
 * Пара, ПРИВЯЗАННАЯ К ТЕКСТУ: {text, pair}. Отвечает парой, только если спрашивают
 * ровно про тот текст, для которого она записана, — иначе null.
 *
 * Зачем понадобилось. Быстрый словарь закреплял направление после КАЖДОГО перевода,
 * а закреплённое сильнее алфавита (ветка «выбор человека» в resolvePair). То есть
 * первый же перевод выключал автоопределение навсегда: панель показывала прошлую
 * пару, и на сервер уходил прошлый source_lang. Набрал «Hund», потом «собака» — и
 * русское слово уезжало в переводчик с пометкой «это немецкий», а в своём словаре
 * искалось в немецкой половине. Проверено прогоном 24.08.2026: панель отставала на
 * один перевод. Привязка к тексту и есть лечение: сменил слово — привязка отпала.
 */
export function pairBoundToText(text, bound) {
  if (!bound || typeof bound !== 'object') return null;
  if (String(bound.text ?? '').trim() !== String(text ?? '').trim()) return null;
  const pair = bound.pair;
  if (!pair || !isKnownLanguage(pair.source) || !isKnownLanguage(pair.target)) return null;
  if (pair.source === pair.target) return null;
  return { source: pair.source, target: pair.target };
}

/**
 * Что показать в языковой панели словаря.
 *
 * Два разных момента, и путать их нельзя:
 *   1. На экране лежит ответ РОВНО на этот текст — панель обязана показывать, в
 *      какую сторону его вправду перевели. Это факт, и знает его сервер
 *      (detected_source_lang), а не алфавит. Иначе панель будет спорить с
 *      карточкой под ней.
 *   2. Человек набирает — панель показывает, куда уйдёт запрос, то есть алфавит.
 *
 * Ручного переключателя направления здесь НЕТ и быть не должно (решение владельца
 * 24.08.2026). Кириллица и латиница не пересекаются, поэтому на паре ru↔de алфавит
 * не ошибается, а кнопка «поменять языки» могла только испортить ответ: сказать
 * переводчику «Krieg — это русский» и получить «Krieg» обратно. Так же устроено у
 * лидеров: у dict.cc и Linguee направления как настройки нет вовсе, у LEO его
 * выбирают данные, у Google Translate переключатель недоступен, пока работает
 * автоопределение (патент US9524293B2 прямо запрещает разворот при автоопределении
 * и во время набора). Кнопка ⇄ у нас теперь переворачивает СОДЕРЖИМОЕ — прошлый
 * перевод становится новым запросом, — и направление для него снова решает алфавит.
 *
 * Третий язык этого не отменяет: когда латиницу поделят немецкий с английским,
 * resolvePair вернёт confident:false, и спрашивать надо будет ЯЗЫК, а не сторону.
 *
 * @param {string} text   что сейчас в поле
 * @param {object} result {text, pair} — что и в какую сторону уже переведено
 */
export function displayPair(text, { result = null, profile, allowed = ACTIVE_LANGUAGES } = {}) {
  const clean = String(text ?? '').trim();
  const done = clean ? pairBoundToText(clean, result) : null;
  if (done) return { ...done, confident: true, reason: 'так и перевели' };
  return resolvePair(clean, { profile, allowed });
}
