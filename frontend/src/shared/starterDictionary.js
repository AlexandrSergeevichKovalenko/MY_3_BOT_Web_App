// Базовый словарь: ОДНО место, где решается, что человеку показать и какими словами.
//
// ┌─ ПОВОД, 15.09.2026. ТРИ ДВЕРИ, И ТОЛЬКО ОДНА ЗНАЛА ПРО ПОЛНЫЙ СЛОВАРЬ. ─────────┐
// │ Подключить базовый словарь можно было из трёх мест, и они разъехались:          │
// │   A — окно при первом входе в приложение: ОДНА кнопка «Да, подключить» →        │
// │       всегда быстрый старт (1000 слов). Про полный словарь ни слова.            │
// │   B — шаг 4 тура: три кнопки, включая «🔓 Весь словарь — 17 539 слов».          │
// │   C — настройки: «Переподключить» → тоже всегда быстрый старт.                  │
// │ Двери не спрашивают дважды (сработала одна — остальные молчат), поэтому всё      │
// │ решала гонка: кто первый встретился, тот и определил, что человеку достанется.   │
// │ Замер на живой базе 15.09.2026: из шести «быстрых стартов» двое (uid 362151600,  │
// │ 5126120959) тур не открывали ни разу — им о полном словаре не сказал никто и     │
// │ негде было узнать. Это и есть «решили за пользователя».                          │
// │                                                                                 │
// │ Решение владельца 15.09.2026: свести двери в один источник. Скин у каждого       │
// │ экрана свой (модалка, шаг тура, строка настроек) — а СПИСОК вариантов, их        │
// │ подписи и состояние «что уже подключено» берутся отсюда. Добавляете вариант или  │
// │ меняете текст — правьте здесь, иначе двери разъедутся снова.                     │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// Модуль намеренно без React и без JSX: его импортирует тест напрямую
// (frontend/tests/starter_dictionary_choice.test.mjs).

const TXT = {
  quick:       { ru: '📚 Быстрый старт',              de: '📚 Schnellstart' },
  full:        { ru: '🔓 Весь словарь',               de: '🔓 Ganzes Wörterbuch' },
  upgrade:     { ru: '🔓 Подключить весь словарь',    de: '🔓 Ganzes Wörterbuch verbinden' },
  skip:        { ru: 'Пропустить',                    de: 'Überspringen' },
  keep:        { ru: 'Оставить быстрый старт',        de: 'Schnellstart behalten' },
  disconnect:  { ru: 'Отключить базовый словарь',     de: 'Basiswörterbuch trennen' },
  words:       { ru: 'слов',                          de: 'Wörter' },
  badgeFull:   { ru: '✅ Весь словарь подключён',      de: '✅ Ganzes Wörterbuch verbunden' },
  badgeQuick:  { ru: '✅ Быстрый старт подключён',     de: '✅ Schnellstart verbunden' },
  busy:        { ru: 'Подключаю…',                    de: 'Verbinde…' },
};

function say(key, lang) {
  const row = TXT[key];
  return row ? (lang === 'de' ? row.de : row.ru) : '';
}

function fmt(n, lang) {
  const value = Math.max(0, Number(n || 0));
  try { return value.toLocaleString(lang === 'de' ? 'de-DE' : 'ru-RU'); }
  catch (_e) { return String(value); }
}

/** Что сервер знает про базовый словарь этого человека. Только чтение, без догадок. */
export function starterDictionaryFacts(offer) {
  const state = (offer && offer.state) || {};
  const subscribed = !!state.live_subscription;
  // Потолок лежит и в state, и на верхнем уровне ответа. Нормализация в App.jsx роняла
  // его из state (тот же класс, что однажды уже случился с live_subscription), поэтому
  // читаем оба места: пустое значение здесь означало бы «весь словарь» и соврало бы.
  const rawLimit = state.subscription_limit != null ? state.subscription_limit
    : (offer ? offer.subscription_limit : null);
  const subLimit = (rawLimit == null || Number(rawLimit) <= 0) ? null : Number(rawLimit);
  const have = Math.max(0, Number((offer && offer.starter_pair_total) || 0));
  const total = Math.max(0, Number((offer && offer.template_total) || 0));
  const suggested = Math.max(0, Number((offer && (offer.suggested_count || offer.import_limit)) || 0));
  const decision = String(state.decision_status || 'pending').trim().toLowerCase() || 'pending';
  // Весь словарь — это подписка БЕЗ потолка (копирования больше нет, have остаётся 0)
  // либо старая копия, добравшая весь шаблон.
  const hasFull = (subscribed && subLimit == null) || (total > 0 && have >= total);
  const hasQuick = !hasFull && (subscribed || have > 0);
  const connected = hasFull ? 'full' : (hasQuick ? 'quick' : 'none');
  const canUpgrade = connected === 'quick' && total > Math.max(have, subLimit || 0);
  return { decision, subscribed, subLimit, have, total, suggested, connected, canUpgrade };
}

/**
 * Размер, который человек УЖЕ подключил: 'quick' | 'full' | null.
 *
 * null — когда подтверждать нечего: он отказался, или «согласился», но не подключилось
 * ничего (сорванный импорт). В обоих случаях кнопки выбора обязаны остаться на экране,
 * а плашка «подключён» соврала бы.
 */
export function starterDictionaryDecided(offer) {
  const facts = starterDictionaryFacts(offer);
  if (facts.decision !== 'accepted') return null;
  return facts.connected === 'none' ? null : facts.connected;
}

/** Подпись состояния или null, если ничего не подключено. */
export function starterDictionaryBadge(offer, lang = 'ru') {
  const f = starterDictionaryFacts(offer);
  if (f.connected === 'full') {
    return f.total ? `${say('badgeFull', lang)} — ${fmt(f.total, lang)} ${say('words', lang)}`
                   : say('badgeFull', lang);
  }
  if (f.connected === 'quick') {
    const size = f.subLimit || f.have;
    return size ? `${say('badgeQuick', lang)} — ${fmt(size, lang)} ${say('words', lang)}`
                : say('badgeQuick', lang);
  }
  return null;
}

/**
 * Какие кнопки показать. Один список на все три двери — окно первого входа, шаг тура
 * и настройки. Каждый экран рисует их своим скином, но состав и подписи здесь.
 *
 * Возвращает [{ key, action, full, kind, label }]:
 *   action/full — ровно то, что уходит в /api/webapp/starter-dictionary/apply;
 *   key         — что показывать «в работе» (совпадает с ключом busy);
 *   kind        — 'primary' | 'alt' | 'skip' | 'danger', подсказка скину.
 * Особый action 'keep' запросов не шлёт: человек остаётся с тем, что уже подключено.
 */
export function starterDictionaryOptions(offer, lang = 'ru', opts = {}) {
  const allowSkip = opts.allowSkip !== false;
  const allowDisconnect = !!opts.allowDisconnect;
  const f = starterDictionaryFacts(offer);
  const out = [];
  if (f.connected === 'none') {
    out.push({
      key: 'quick', action: 'accept', full: false, kind: 'primary',
      label: `${say('quick', lang)}${f.suggested ? ` — ~${fmt(f.suggested, lang)} ${say('words', lang)}` : ''}`,
    });
    // Полный вариант показываем всегда, когда он вправду больше быстрого старта. Если
    // размеров нет (гостю сервер их не отдаёт), показываем обе кнопки без чисел — иначе
    // выбор исчезает совсем, а это и есть «решили за человека».
    if (!f.total || f.total > f.suggested) {
      out.push({
        key: 'full', action: 'accept', full: true, kind: 'alt',
        label: `${say('full', lang)}${f.total ? ` — ${fmt(f.total, lang)} ${say('words', lang)}` : ''}`,
      });
    }
    if (allowSkip) out.push({ key: 'decline', action: 'decline', full: false, kind: 'skip', label: say('skip', lang) });
  } else if (f.connected === 'quick' && f.canUpgrade) {
    out.push({
      key: 'full', action: 'accept', full: true, kind: 'alt',
      label: `${say('upgrade', lang)} — ${fmt(f.total, lang)} ${say('words', lang)}`,
    });
    if (allowSkip) out.push({ key: 'keep', action: 'keep', full: false, kind: 'skip', label: say('keep', lang) });
  }
  if (allowDisconnect && f.connected !== 'none') {
    out.push({ key: 'disconnect', action: 'disconnect', full: false, kind: 'danger', label: say('disconnect', lang) });
  }
  return out;
}

/** Подпись кнопки, пока запрос в полёте. */
export function starterDictionaryBusyLabel(lang = 'ru') {
  return say('busy', lang);
}
