// Единый клиент потокового разбора — `/api/webapp/dictionary/stream`.
//
// ЗАЧЕМ ОТДЕЛЬНЫМ ФАЙЛОМ. Потоковый разбор читают ДВА места: быстрый словарь
// (`DictionaryOverlay`) и попап выделенного слова в видео и читалке (`App.jsx`).
// До 30.08.2026 у попапа был СВОЙ путь к модели — отдельный урезанный промпт
// `dictionary_assistant_multilang_reader` и своя сборка текста на бэкенде. Два
// разбора одного слова разъезжались молча: в словаре у слова были часть речи,
// состав слова и управление, а в видео — самодельная простыня с механически
// нарезанными «типичными сочетаниями». Промпт убран, оба места ходят сюда.
//
// Здесь ТОЛЬКО транспорт: запрос, разбор SSE-кадров, отдача секций наружу.
// Ни решений о содержимом, ни подстановок: чего сервер не прислал — того нет.
import { getInitData, getDictToken } from './WordBreakdown';

/**
 * Открыть поток разбора и скармливать секции по мере готовности.
 *
 * @param {string}   word        что разбираем (слово, оборот, предложение)
 * @param {string}   lookupLang  подсказка о языке запроса ('de' | 'ru' | ...), необязательна
 * @param {AbortSignal} signal   отмена запроса
 * @param {Function} isStale     () => boolean: запрос устарел, читать дальше незачем
 * @param {Function} onStreamStart вызывается, когда пошёл именно ПОТОК (а не готовый JSON)
 * @param {Function} onSection    ({ name, fields }) на каждую приехавшую секцию
 * @param {Function} onDone       (payload) на финальный собранный ответ; его результат вернётся
 *
 * @returns {Promise<{ streamed: boolean, sawSection: boolean, result: * }>}
 *
 * Бросает: ошибку с полями `.status` и `.payload` на 4xx/5xx (лимит дня и прочее —
 * это ОТВЕТ сервера, его надо показать человеку, а не глушить), 'stream unsupported'
 * и 'empty stream' — на них вызывающий уходит на обычный `/api/webapp/dictionary`.
 * Это запасной ТРАНСПОРТ того же самого разбора, а не запасные данные.
 */
export async function streamDictionaryLookup({
  word,
  lookupLang,
  signal,
  isStale,
  onStreamStart,
  onSection,
  onDone,
}) {
  const dictToken = getDictToken();
  const headers = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
  if (dictToken) headers['X-Dict-Token'] = dictToken;
  const resp = await fetch('/api/webapp/dictionary/stream', {
    method: 'POST',
    headers,
    body: JSON.stringify({
      initData: getInitData(),
      ...(dictToken ? { dqt: dictToken } : {}),
      word,
      lookup_lang: lookupLang,
    }),
    signal,
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({}));
    const err = new Error(data?.error || 'Fehler');
    err.status = resp.status;
    err.payload = data;
    throw err;
  }
  // Попадание в кеш или в общий пул приходит обычным JSON, а не потоком: слово у нас
  // уже есть, ждать нечего и платить не за что.
  if ((resp.headers.get('Content-Type') || '').includes('application/json')) {
    const data = await resp.json().catch(() => ({}));
    return { streamed: false, sawSection: false, result: onDone ? onDone(data) : data };
  }
  if (!resp.body || typeof resp.body.getReader !== 'function') {
    throw new Error('stream unsupported');
  }

  if (onStreamStart) onStreamStart();
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let sawSection = false;
  let result = null;

  const handleFrame = (block) => {
    let ev = 'message';
    const dataLines = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('event:')) ev = line.slice(6).trim();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
    }
    if (!dataLines.length) return;
    let payload;
    try { payload = JSON.parse(dataLines.join('\n')); } catch (_e) { return; }
    if (ev === 'section') {
      sawSection = true;
      if (onSection) onSection({ name: String(payload?.name || ''), fields: (payload && payload.fields) || {} });
    } else if (ev === 'done') {
      result = onDone ? onDone(payload) : payload;
    } else if (ev === 'error') {
      throw new Error(payload?.error || 'stream error');
    }
  };

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (isStale && isStale()) return { streamed: true, sawSection, result };
    buffer += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buffer.indexOf('\n\n')) >= 0) {
      const frame = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      handleFrame(frame);
    }
  }
  if (buffer.trim()) handleFrame(buffer);
  // Ни одной секции и ни одного финального ответа — это НЕ «пустой разбор», это
  // оборванный поток. Молчать нельзя: вызывающий уйдёт на обычный запрос.
  if (!result && !sawSection) throw new Error('empty stream');
  return { streamed: true, sawSection, result };
}
