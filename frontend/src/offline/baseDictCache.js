/**
 * Offline base dictionary cache — separate IndexedDB database.
 *
 * Stores entries from bt_base_dictionary for offline lookups.
 *   Store "base_dict"
 *     keyPath: "k"  (lemma_key — normalized lowercase German)
 *     Fields : k, w (lemma), p (pos), a (article), ru (string[]), ru_lc (string[]), en (string[])
 *     Index  : by_ru_lc on ru_lc (multiEntry) — fast reverse ru→de lookups
 *
 *   Store "base_dict_meta"
 *     keyPath: "key"
 *     Fields : key="pack_version", value (ISO timestamp of last pack download)
 *
 * v2: added by_ru_lc multi-entry index for O(log n) Russian → German lookups.
 *     Previously the reverse lookup did getAll() + JS scan = O(n), very slow.
 */

const BD_DB_NAME    = 'DeutschBaseDictionary';
const BD_DB_VERSION = 2;
const STORE_BD      = 'base_dict';
const STORE_BD_META = 'base_dict_meta';
const PACK_VERSION_KEY = 'pack_version';
const PACK_MAX_AGE_MS  = 7 * 24 * 60 * 60 * 1000; // re-download after 7 days
// localStorage key mirrors the IndexedDB timestamp so pack freshness survives
// sessions even when the browser clears IndexedDB between app launches (iOS).
const LS_PACK_TS_KEY = 'bd_pack_ts';

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
      const upgradeTx = evt.currentTarget.transaction;

      let store;
      if (!db.objectStoreNames.contains(STORE_BD)) {
        store = db.createObjectStore(STORE_BD, { keyPath: 'k' });
      } else {
        store = upgradeTx.objectStore(STORE_BD);
      }

      // Add multi-entry index for fast Russian → German lookups (v2)
      if (!store.indexNames.contains('by_ru_lc')) {
        store.createIndex('by_ru_lc', 'ru_lc', { multiEntry: true });
        // Backfill existing entries: add ru_lc to records that don't have it yet
        const cursorReq = store.openCursor();
        cursorReq.onsuccess = (e) => {
          const cursor = e.target.result;
          if (!cursor) return;
          const entry = cursor.value;
          if (Array.isArray(entry.ru) && !entry.ru_lc) {
            entry.ru_lc = entry.ru.map((r) => String(r || '').trim().toLowerCase()).filter(Boolean);
            cursor.update(entry);
          }
          cursor.continue();
        };
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

function _addRuLc(entry) {
  if (Array.isArray(entry.ru) && !entry.ru_lc) {
    entry.ru_lc = entry.ru.map((r) => String(r || '').trim().toLowerCase()).filter(Boolean);
  }
  return entry;
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
  _addRuLc(entry);
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
  const key = _normKey(serverItem.translation_de || serverItem.word_de || word);
  if (!key) return;
  const ruList = serverItem.dictionary_senses
    ? serverItem.dictionary_senses.map((s) => s.translation_ru).filter(Boolean)
    : [serverItem.translation_ru || ''].filter(Boolean);
  const entry = {
    k:     key,
    w:     serverItem.word_de  || serverItem.translation_de || word,
    p:     serverItem.part_of_speech || '',
    a:     serverItem.article || '',
    ru:    ruList,
    ru_lc: ruList.map((r) => String(r || '').trim().toLowerCase()).filter(Boolean),
    en:    serverItem.dictionary_senses
             ? serverItem.dictionary_senses.map((s) => s.value).filter(Boolean)
             : [],
    senses: serverItem.dictionary_senses || [],
    forms:  serverItem.forms || {},
  };
  await saveBaseDictEntry(entry);
}

function _entryToResult(entry, word, queryLang = 'de') {
  const ruList = Array.isArray(entry.ru) ? entry.ru : [];
  const enList = Array.isArray(entry.en) ? entry.en : [];
  const senses = Array.isArray(entry.senses)
    ? entry.senses
    : ruList.map((ru, i) => ({ value: enList[i] || '', translation_ru: ru }));
  const translationRu = ruList.slice(0, 5).join(', ');
  const displayWord = entry.a ? `${entry.a} ${entry.w}` : (entry.w || word);
  const normalizedQueryLang = String(queryLang || 'de').trim().toLowerCase() || 'de';
  const sourceText = normalizedQueryLang === 'ru' ? word : displayWord;
  const targetText = normalizedQueryLang === 'ru' ? displayWord : translationRu;
  return {
    word_de: displayWord,
    word_ru: translationRu,
    translation_de: entry.w || word,
    translation_ru: translationRu,
    source_text: sourceText,
    target_text: targetText,
    source_lang: normalizedQueryLang === 'ru' ? 'ru' : 'de',
    target_lang: normalizedQueryLang === 'ru' ? 'de' : 'ru',
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

/**
 * Fast offline lookup: O(1) for German (key lookup), O(log n) for Russian (index).
 * No more O(n) getAll() scan.
 */
export async function lookupOfflineBaseDictEntry(word) {
  const isCyrillic = /[А-Яа-яЁё]/.test(word);
  const query = String(word || '').trim().toLowerCase();
  if (!query) return null;

  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD, 'readonly');
    const store = tx.objectStore(STORE_BD);

    if (!isCyrillic) {
      // German → fast key lookup by lemma_key
      const entry = await _pr(store.get(_normKey(word)));
      if (entry && Array.isArray(entry.ru) && entry.ru.length > 0) {
        return _entryToResult(entry, word, 'de');
      }
      return null;
    }

    // Russian → use by_ru_lc multi-entry index (O(log n))
    try {
      const hit = await _pr(store.index('by_ru_lc').get(query));
      if (hit && Array.isArray(hit.ru) && hit.ru.length > 0) {
        return _entryToResult(hit, word, 'ru');
      }
    } catch {
      // Index not yet available (old DB without upgrade) — return null
    }
    return null;
  } catch {
    return null;
  }
}

function _lsGetPackTs() {
  try { return localStorage.getItem(LS_PACK_TS_KEY) || null; } catch { return null; }
}

function _lsSetPackTs(isoTs) {
  try { localStorage.setItem(LS_PACK_TS_KEY, isoTs); } catch { /* non-fatal */ }
}

async function _setPackVersion(isoTs) {
  // Write to localStorage first — it survives session clearing on iOS WebView.
  _lsSetPackTs(isoTs);
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD_META, 'readwrite');
    await _pr(tx.objectStore(STORE_BD_META).put({ key: PACK_VERSION_KEY, value: isoTs }));
  } catch {
    // non-fatal — localStorage copy is the reliable one
  }
}

function _isTs_fresh(isoTs) {
  if (!isoTs) return false;
  try { return (Date.now() - new Date(isoTs).getTime()) < PACK_MAX_AGE_MS; } catch { return false; }
}

export async function isOfflinePackFresh() {
  // Check localStorage first — survives even when IndexedDB is cleared by the OS.
  if (_isTs_fresh(_lsGetPackTs())) return true;
  // Fallback: read from IndexedDB (covers migrated sessions without localStorage entry).
  try {
    const db = await _openBD();
    const tx = db.transaction(STORE_BD_META, 'readonly');
    const rec = await _pr(tx.objectStore(STORE_BD_META).get(PACK_VERSION_KEY));
    const ts = rec ? rec.value : null;
    if (_isTs_fresh(ts)) {
      _lsSetPackTs(ts); // backfill localStorage so next check is instant
      return true;
    }
  } catch { /* ignore */ }
  return false;
}

export async function downloadOfflinePack(lang = 'de', limit = 10000) {
  try {
    const resp = await fetch(`/api/webapp/dictionary/offline-pack?lang=${lang}&limit=${limit}`);
    if (!resp.ok) return false;
    const data = await resp.json();
    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (entries.length === 0) return false;

    const db = await _openBD();
    // Write in small batches so readonly transactions (lookups) can run between batches.
    // A single readwrite transaction for 10 000 entries blocks all reads for 30–60 s on mobile.
    const BATCH = 500;
    for (let i = 0; i < entries.length; i += BATCH) {
      const batch = entries.slice(i, i + BATCH);
      const tx = db.transaction(STORE_BD, 'readwrite');
      const store = tx.objectStore(STORE_BD);
      for (const e of batch) {
        if (Array.isArray(e.ru) && !e.ru_lc) {
          e.ru_lc = e.ru.map((r) => String(r || '').trim().toLowerCase()).filter(Boolean);
        }
        store.put(e);
      }
      await new Promise((res, rej) => {
        tx.oncomplete = res;
        tx.onerror = (ev) => rej(ev.target.error);
      });
      // Yield the event loop so pending readonly transactions can start between batches.
      await new Promise((res) => setTimeout(res, 0));
    }
    await _setPackVersion(new Date().toISOString());
    return true;
  } catch {
    return false;
  }
}
