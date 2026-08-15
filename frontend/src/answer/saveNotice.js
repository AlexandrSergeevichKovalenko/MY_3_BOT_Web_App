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

// Услуга, про которую честно говорить «сохранено N слов». Сервер в любом отказе 429
// присылает поле feature с названием того, что кончилось (backend/database.py,
// build_free_limit_error), но раньше мы это поле выбрасывали и подписывали ЛЮБОЙ лимит
// как лимит сохранений. 15.08.2026 человек упёрся в «Разбор новых слов» (1 в день) и
// прочитал «Сегодня сохранено 1 слов — это дневной лимит», имея 11 сохранений из 20.
const SAVE_LIMIT_FEATURE = 'dictionary_lookup_save_daily';

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

  // Дневной потолок расходов — не лимит услуги: числа «сколько осталось» у него нет,
  // и называть его лимитом словаря было прямой неправдой.
  if (code === 'cost_cap_exceeded') {
    return {
      kind: 'limit',
      title: 'На сегодня хватит',
      text: 'Бесплатные возможности на сегодня закончились. Завтра всё снова откроется.',
      hint: 'На «Полном доступе» этой границы нет.',
      action: { label: 'Открыть «Полный доступ»', onClick: openFullAccess },
    };
  }
  if (status === 429 || LIMIT_CODES.has(code)) {
    const limit = Number(payload.limit);
    const used = Number(payload.used);
    const count = Number.isFinite(limit) && limit > 0 ? limit : null;
    const feature = String(payload.feature || '').trim();
    const featureTitle = String(payload.feature_title || '').trim();
    if (feature && feature !== SAVE_LIMIT_FEATURE) {
      return {
        kind: 'limit',
        title: featureTitle || 'Дневной лимит',
        text: count
          ? `Бесплатно это доступно ${count} раз(а) в день, на сегодня уже израсходовано. Завтра лимит обновится.`
          : 'На сегодня бесплатный лимит исчерпан. Завтра обновится.',
        hint: 'На «Полном доступе» лимита нет.',
        action: { label: 'Открыть «Полный доступ»', onClick: openFullAccess },
      };
    }
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
