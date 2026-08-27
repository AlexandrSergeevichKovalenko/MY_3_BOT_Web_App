import { api } from './WordBreakdown';

/**
 * История поиска в словаре — одна на все устройства.
 *
 * До 27.08.2026 она жила ТОЛЬКО в памяти браузера (localStorage, ключ dq_recents_v1).
 * У Telegram, у приложения с рабочего стола и у Safari память разная: человек искал
 * слова в Telegram, открывал приложение с иконки — и видел пустую историю, хотя слов
 * у него сотни. Владелец 27.08.2026.
 *
 * Кто здесь источник истины: СЕРВЕР (bt_3_dictionary_search_history). Локальная память
 * остаётся ЗЕРКАЛОМ и нужна ровно для одного — словарь работает без сети, и отнимать
 * у него историю вместе с сетью нельзя. Зеркало не выдумывает записей: в нём лежит
 * ровно то, что мы последний раз получили от сервера или сами туда положили.
 *
 * Ненаписанное на сервер НЕ ТЕРЯЕТСЯ: слово, которое не удалось записать (нет сети),
 * ложится в очередь ожидания и уходит на сервер с первым же удавшимся обращением.
 * Без этой очереди офлайновый поиск исчезал бы навсегда, а зеркало показывало бы
 * человеку список, которого на сервере нет, — то есть тихо врало.
 *
 * Модуль ОДИН на оба словаря (внутренний в App.jsx и быстрый DictionaryOverlay):
 * копия экрана рано или поздно разъезжается с оригиналом, копия логики — тем более.
 */

const KEY = 'dq_recents_v1';
const KEEP = 60;                              // сколько держим в зеркале
const MERGED_FLAG = 'dq_recents_merged_v1';   // устройство уже отдало накопленное до переезда
const PENDING_KEY = 'dq_recents_pending_v1';  // что ещё не доехало до сервера
const PENDING_MAX = 30;

function readList(key, cap) {
  try {
    const raw = JSON.parse(localStorage.getItem(key) || '[]');
    return Array.isArray(raw) ? raw.filter((x) => typeof x === 'string').slice(0, cap) : [];
  } catch (_e) { return []; }
}

function writeList(key, list, cap) {
  const next = (Array.isArray(list) ? list : []).filter((x) => typeof x === 'string').slice(0, cap);
  try { localStorage.setItem(key, JSON.stringify(next)); } catch (_e) { /* зеркало не критично */ }
  return next;
}

export function readLocalHistory() { return readList(KEY, KEEP); }
function writeLocalHistory(list) { return writeList(KEY, list, KEEP); }

function withWordOnTop(word, list) {
  const w = String(word || '').trim();
  if (!w) return list;
  return [w, ...list.filter((x) => x.toLowerCase() !== w.toLowerCase())].slice(0, KEEP);
}

async function sendRecord(word, lookupLang) {
  const data = await api('/api/webapp/dictionary/history/record', {
    word: String(word || '').trim(), lookup_lang: lookupLang || '', limit: KEEP,
  });
  return Array.isArray(data?.items) ? data.items.filter((x) => typeof x === 'string') : [];
}

/** Дослать то, что не доехало. Порядок от старого к новому — свежее окажется сверху. */
async function flushPending() {
  const pending = readList(PENDING_KEY, PENDING_MAX);
  if (!pending.length) return null;
  let items = null;
  const stuck = [];
  for (const word of pending.slice().reverse()) {
    try { items = await sendRecord(word, ''); } catch (_e) { stuck.push(word); }
  }
  writeList(PENDING_KEY, stuck.reverse(), PENDING_MAX);
  return items;
}

function rememberPending(word) {
  const w = String(word || '').trim();
  if (!w) return;
  const next = [w, ...readList(PENDING_KEY, PENDING_MAX).filter((x) => x.toLowerCase() !== w.toLowerCase())];
  writeList(PENDING_KEY, next, PENDING_MAX);
}

/**
 * Забрать историю с сервера. Первым заходом с этого устройства отдаём наверх то, что
 * успело накопиться в его памяти до переезда, — иначе человек потерял бы свою историю
 * ровно в тот день, когда мы её чинили.
 *
 * Сервер не ответил (нет сети, человека не узнали) — возвращаем зеркало и говорим об
 * этом вторым полем. Молча выдавать зеркало за ответ сервера нельзя: пустая история от
 * «сети нет» и пустая история от «человек ничего не искал» — разные вещи.
 */
export async function loadSearchHistory() {
  const local = readLocalHistory();
  let merged = false;
  try { merged = localStorage.getItem(MERGED_FLAG) === '1'; } catch (_e) { /* ignore */ }
  const body = { limit: KEEP };
  if (!merged && local.length) body.merge = local;
  try {
    const data = await api('/api/webapp/dictionary/history', body);
    const items = Array.isArray(data?.items) ? data.items.filter((x) => typeof x === 'string') : [];
    if (!merged) { try { localStorage.setItem(MERGED_FLAG, '1'); } catch (_e) { /* ignore */ } }
    const flushed = await flushPending();
    return { items: writeLocalHistory(flushed || items), fromServer: true };
  } catch (_e) {
    return { items: local, fromServer: false };
  }
}

/**
 * Записать поиск. Зовётся оттуда же, где раньше писалось в localStorage, — то есть в
 * тот момент, когда слово нашёл ЧЕЛОВЕК с экрана поиска. Служебные обращения к словарю
 * (сохранение, чипы синонимов, разбор) сюда не заходят и историю не засоряют.
 *
 * Зеркало обновляем сразу, чтобы список не ждал сети; сервер отвечает своим порядком,
 * и он побеждает.
 */
export async function recordSearch(word, lookupLang = '') {
  const w = String(word || '').trim();
  if (!w) return readLocalHistory();
  const optimistic = writeLocalHistory(withWordOnTop(w, readLocalHistory()));
  try {
    await flushPending();
    const items = await sendRecord(w, lookupLang);
    return items.length ? writeLocalHistory(items) : optimistic;
  } catch (_e) {
    // Сети нет или человека не узнали. Слово встаёт в очередь и уедет на сервер с
    // первым же удавшимся обращением — терять его из-за одного неудачного запроса
    // незачем. Ошибку наружу не выносим: это список, а не ответ на вопрос человека.
    rememberPending(w);
    return optimistic;
  }
}
