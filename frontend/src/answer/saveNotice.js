// Что сказать человеку, когда сохранение слова не прошло.
//
// Раньше любая неудача превращалась в «Не удалось сохранить слово. Нажми на него ещё
// раз» — и это было неправдой в самом частом случае: на бесплатном тарифе 20 сохранений
// в день, сервер отвечает 429 `free_limit_exceeded`, и повторное нажатие не поможет
// никогда. Человек жал по кругу и не понимал, что происходит.
//
// Здесь одно место, которое переводит ответ сервера в человеческую фразу. Все игры
// показывают её всплывающей плашкой (Toast) — она не занимает место на экране.

const LIMIT_CODES = new Set(['free_limit_exceeded', 'limit_exceeded', 'daily_limit_exceeded']);

// Тариф живёт в основном мини-приложении, раздел «Подписка». Открываем его в том же окне
// Telegram: адрес свой, домен тот же, так что это обычный переход, а не новая вкладка.
export function openFullAccess() {
  try { window.Telegram?.WebApp?.HapticFeedback?.impactOccurred?.('light'); } catch (_e) { /* noop */ }
  try {
    window.location.assign('/webapp?startapp=billing');
  } catch (_e) {
    window.location.href = '/webapp?startapp=billing';
  }
}

// err — Error из api(): у него есть .status и .payload (тело ответа сервера).
export function describeSaveError(err) {
  const status = Number(err?.status || 0);
  const payload = (err && typeof err.payload === 'object' && err.payload) || {};
  const code = String(payload.error || err?.message || '').trim();

  if (status === 429 || LIMIT_CODES.has(code)) {
    const limit = Number(payload.limit);
    const used = Number(payload.used);
    const count = Number.isFinite(limit) && limit > 0 ? limit : null;
    return {
      kind: 'limit',
      title: 'Дневной лимит словаря',
      text: count
        ? `Сегодня сохранено ${Number.isFinite(used) && used > 0 ? used : count} слов — это дневной лимит бесплатного тарифа. Завтра снова будет ${count}.`
        : 'На сегодня бесплатные сохранения в словарь закончились. Завтра лимит обновится.',
      hint: 'На «Полном доступе» лимита нет.',
      action: { label: 'Открыть «Полный доступ»', onClick: openFullAccess },
    };
  }
  if (status === 401 || status === 403) {
    return {
      kind: 'auth',
      title: 'Не получилось сохранить',
      text: 'Открой игру заново из чата с ботом — ссылка устарела.',
    };
  }
  if (!status || status >= 500) {
    return {
      kind: 'retry',
      title: 'Слово не сохранилось',
      text: 'Похоже, пропала связь. Нажми на слово ещё раз.',
    };
  }
  return {
    kind: 'retry',
    title: 'Слово не сохранилось',
    text: 'Попробуй нажать на слово ещё раз.',
  };
}

// Короткая строка для всплывающей плашки.
export function saveErrorToast(err) {
  const d = describeSaveError(err);
  return {
    kind: d.kind === 'limit' ? 'limit' : 'bad',
    text: d.text,
    hint: d.hint || '',
    action: d.action || null,
  };
}
