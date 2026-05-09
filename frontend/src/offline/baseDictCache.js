/**
 * Offline base dictionary cache — separate IndexedDB database.
 *
 * Stores entries from bt_base_dictionary for offline lookups.
 *   Store "base_dict"
 *     keyPath: "k"  (lemma_key — normalized lowercase)
 *     Fields : k, w (lemma), p (pos), a (article), ru (string[]), en (string[])
 *
 *   Store "base_dict_meta"
 *     keyPath: "key"
 *     Fields : key="pack_version", value (ISO timestamp of last pack download)
 */

const BD_DB_NAME    = 'DeutschBaseDictionary';
const BD_DB_VERSION = 1;
const STORE_BD      = 'base_dict';
const STORE_BD_META = 'base_dict_meta';
const PACK_VERSION_KEY = 'pack_version';
const PACK_MAX_AGE_MS  = 7 * 24 * 60 * 60 * 1000; // re-download after 7 days

let _bdDbPromise = null;

function _openBD() {
  if (_bdDbPromise) return _bdDbPromise;
  _bdDbPromise = new Promise((resolve, reject) => {
    if (!('indexedDB' in globalThis)) {
      reject(new Error('IndexedDB not available'));
      return;
    }
    const req = indexedDB.open(BD_DB_NAME, BD_DB_VERSION);
    req.onupgradeneeded = (evt) => {
      const db = evt.target.result;
      if (!db.objectStoreNames.contains(STORE_BD)) {
        db.createObjectStore(STORE_BD, { keyPath: 'k' });
      }
      if (!db.objectStoreNames.contains(STORE_BD_META)) {
        db.createObjectStore(STORE_BD_META, { keyPath: 'key' });
      }
    };
    req.onsuccess  = (e) => resolve(e.target.result);
    req.onerror    = (e) => { _bdDbPromise = null; reject(e.target.error); };
    req.onblocked  = () => console.warn('[baseDictCache] DB upgrade blocked');
  });
  return _bdDbPromise;
}

function _pr(req) {
  return new Promise((res, rej) => {
    req.onsuccess = (e) => res(e.target.result);
    req.onerror   = (e) => rej(e.target.error);
  });
}

function _normKey(word) {
  let w = String(word || '').trim().toLowerCase();
  for (const pfx of ['der ', 'die ', 'das ', 'ein ', 'eine ', 'einem ', 'einen ', 'einer ', 'eines ']) {
    if (w.startsWith(pfx)) { w = w.slice(pfx.length); break; }
  }
  return w;
}

export async function getBaseDictEntry(word) {
  const key = _normKey(word);
  if (!key) return null;
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD, 'readonly');
    return await _pr(tx.objectStore(STORE_BD).get(key)) || null;
  } catch {
    return null;
  }
}

export async function saveBaseDictEntry(entry) {
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD, 'readwrite');
    await _pr(tx.objectStore(STORE_BD).put(entry));
  } catch {
    // non-fatal
  }
}

export async function saveBaseDictEntryFromServerResult(word, serverItem) {
  if (!serverItem || !serverItem.is_base_dict) return;
  const key = _normKey(word);
  if (!key) return;
  const entry = {
    k:  key,
    w:  serverItem.word_de  || serverItem.translation_de || word,
    p:  serverItem.part_of_speech || '',
    a:  serverItem.article || '',
    ru: serverItem.dictionary_senses
          ? serverItem.dictionary_senses.map((s) => s.translation_ru).filter(Boolean)
          : [serverItem.translation_ru || ''].filter(Boolean),
    en: serverItem.dictionary_senses
          ? serverItem.dictionary_senses.map((s) => s.value).filter(Boolean)
          : [],
    senses: serverItem.dictionary_senses || [],
    forms:  serverItem.forms || {},
  };
  await saveBaseDictEntry(entry);
}

function _entryToResult(entry, word) {
  const ruList = Array.isArray(entry.ru) ? entry.ru : [];
  const enList = Array.isArray(entry.en) ? entry.en : [];
  const senses = Array.isArray(entry.senses)
    ? entry.senses
    : ruList.map((ru, i) => ({ value: enList[i] || '', translation_ru: ru }));
  const translationRu = ruList.slice(0, 5).join(', ');
  const displayWord = entry.a ? `${entry.a} ${entry.w}` : (entry.w || word);
  return {
    word_de: displayWord,
    word_ru: translationRu,
    translation_de: entry.w || word,
    translation_ru: translationRu,
    source_text: displayWord,
    target_text: translationRu,
    source_lang: 'de',
    target_lang: 'ru',
    part_of_speech: entry.p || '',
    article: entry.a || '',
    forms: entry.forms || {},
    usage_examples: [],
    dictionary_senses: senses,
    provider: 'base_dict_offline',
    is_base_dict: true,
    wikt_fetched: true,
    quick_mode: false,
  };
}

export async function lookupOfflineBaseDictEntry(word) {
  const entry = await getBaseDictEntry(word);
  if (!entry || !entry.ru || entry.ru.length === 0) return null;
  return _entryToResult(entry, word);
}

async function _getPackVersion() {
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD_META, 'readonly');
    const rec = await _pr(tx.objectStore(STORE_BD_META).get(PACK_VERSION_KEY));
    return rec ? rec.value : null;
  } catch {
    return null;
  }
}

async function _setPackVersion(isoTs) {
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD_META, 'readwrite');
    await _pr(tx.objectStore(STORE_BD_META).put({ key: PACK_VERSION_KEY, value: isoTs }));
  } catch {
    // non-fatal
  }
}

export async function isOfflinePackFresh() {
  const ts = await _getPackVersion();
  if (!ts) return false;
  return (Date.now() - new Date(ts).getTime()) < PACK_MAX_AGE_MS;
}

export async function downloadOfflinePack(lang = 'de', limit = 10000) {
  try {
    const resp = await fetch(`/api/webapp/dictionary/offline-pack?lang=${lang}&limit=${limit}`);
    if (!resp.ok) return false;
    const data = await resp.json();
    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (entries.length === 0) return false;

    const db = await _openBD();
    const tx = db.transaction(STORE_BD, 'readwrite');
    const store = tx.objectStore(STORE_BD);
    for (const e of entries) {
      store.put(e);
    }
    await new Promise((res, rej) => {
      tx.oncomplete = res;
      tx.onerror = (ev) => rej(ev.target.error);
    });
    await _setPackVersion(new Date().toISOString());
    return true;
  } catch {
    return false;
  }
}
