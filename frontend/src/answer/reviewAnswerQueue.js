// Отправка ответа на экране «работа над ошибками» — с повтором и без потерь.
//
// Зачем этот файл существует
// ──────────────────────────
// Раньше ответ уходил так: `api(...).catch(() => {})` — «не дошло, ну и ладно».
// Человек видел галочку и растущий счётчик, а на сервер ответ мог не доехать: моргнул
// интернет, свернули окно, шёл деплой. На экране задание решено, в базе — нет, и оно
// возвращается снова. Замер 15.08.2026: из 363 заданий очереди 149 не двигались ни разу,
// и понять, сколько из них — потерянные ответы, было невозможно: ни клиент, ни сервер
// такого не писали.
//
// Здесь ответ повторяется, а то, что так и не ушло, складывается в память браузера и
// досылается при следующем открытии экрана. Терять ответ человека молча нельзя.

const STORAGE_KEY = 'review_unsent_answers_v1';
const RETRIES = [400, 1500, 4000];   // паузы между попытками, мс

function readPending() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const list = raw ? JSON.parse(raw) : [];
    return Array.isArray(list) ? list : [];
  } catch (_e) { return []; }
}

function writePending(list) {
  try {
    // Держим последние 200: это заведомо больше любой сессии, а расти без предела
    // хранилищу нельзя.
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list.slice(-200)));
  } catch (_e) { /* приватный режим — тогда просто без запаса */ }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Отправить один ответ, при неудаче — отложить, а не потерять. */
export async function sendReviewAnswer(api, path, payload) {
  for (let i = 0; i <= RETRIES.length; i += 1) {
    try {
      const res = await api(path, payload);
      if (res && res.ok !== false) return true;
    } catch (_e) { /* пробуем ещё */ }
    if (i < RETRIES.length) await sleep(RETRIES[i]);
  }
  const pending = readPending();
  pending.push({ path, payload, at: Date.now() });
  writePending(pending);
  return false;
}

/** Досылка отложенного. Зовётся при открытии экрана повторов. */
export async function flushPendingReviewAnswers(api) {
  const pending = readPending();
  if (!pending.length) return 0;
  writePending([]);                     // забираем себе, чтобы не досылать дважды
  const stillFailing = [];
  let sent = 0;
  for (const item of pending) {
    try {
      const res = await api(item.path, item.payload);
      if (res && res.ok !== false) { sent += 1; continue; }
      stillFailing.push(item);
    } catch (_e) { stillFailing.push(item); }
  }
  if (stillFailing.length) writePending(stillFailing);
  return sent;
}

/** Сколько ответов ждут отправки — для надписи человеку. */
export function pendingReviewAnswersCount() {
  return readPending().length;
}
