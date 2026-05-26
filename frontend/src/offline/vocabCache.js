/**
 * Offline cache — IndexedDB layer.
 *
 * Schema (DB_VERSION 3):
 *   Store "vocab"
 *     keyPath : "id"
 *     indexes : user_id, folder_id, created_at, user_created (compound)
 *
 *   Store "vocab_meta"
 *     keyPath : "key"   — e.g. "u_<userId>"
 *     Fields  : last_updated (ISO), total_count
 *
 *   Store "srs_queue"         — prefetched SRS cards for offline review
 *     keyPath : "_id" (autoIncrement)
 *     Fields  : _id, user_id, _pos, ...card data from server
 *     indexes : user_id, user_pos (compound ['user_id','_pos'])
 *
 *   Store "srs_pending"       — reviews recorded offline, waiting for sync
 *     keyPath : "_id" (autoIncrement)
 *     Fields  : _id, user_id, card_id, rating, response_ms, queue_source, recorded_at
 *     indexes : user_id
 *
 *   Store "vocab_mutations"   — edit/delete operations queued while offline
 *     keyPath : "_id" (autoIncrement)
 *     Fields  : _id, user_id, type ('delete'|'edit'), entry_id, payload (edit only), recorded_at
 *     indexes : user_id
 */

const DB_NAME    = 'DeutschLernApp';
const DB_VERSION = 3;
const STORE_VOCAB            = 'vocab';
const STORE_META             = 'vocab_meta';
const STORE_SRS_QUEUE        = 'srs_queue';
const STORE_SRS_PENDING      = 'srs_pending';
const STORE_VOCAB_MUTATIONS  = 'vocab_mutations';

// ─── DB open ────────────────────────────────────────────────────────────────

let _dbPromise = null;

function _openDB() {
  if (_dbPromise) return _dbPromise;
  _dbPromise = new Promise((resolve, reject) => {
    if (!('indexedDB' in globalThis)) {
      reject(new Error('IndexedDB not available'));
      return;
    }
    const req = indexedDB.open(DB_NAME, DB_VERSION);

    req.onupgradeneeded = (evt) => {
      const db = evt.target.result;

      if (!db.objectStoreNames.contains(STORE_VOCAB)) {
        const s = db.createObjectStore(STORE_VOCAB, { keyPath: 'id' });
        s.createIndex('user_id',    'user_id',    { unique: false });
        s.createIndex('folder_id',  'folder_id',  { unique: false });
        s.createIndex('created_at', 'created_at', { unique: false });
        s.createIndex('user_created', ['user_id', 'created_at'], { unique: false });
      }

      if (!db.objectStoreNames.contains(STORE_META)) {
        db.createObjectStore(STORE_META, { keyPath: 'key' });
      }

      if (!db.objectStoreNames.contains(STORE_SRS_QUEUE)) {
        const s = db.createObjectStore(STORE_SRS_QUEUE, { keyPath: '_id', autoIncrement: true });
        s.createIndex('user_id',  'user_id',            { unique: false });
        s.createIndex('user_pos', ['user_id', '_pos'],  { unique: false });
      }

      if (!db.objectStoreNames.contains(STORE_SRS_PENDING)) {
        const s = db.createObjectStore(STORE_SRS_PENDING, { keyPath: '_id', autoIncrement: true });
        s.createIndex('user_id', 'user_id', { unique: false });
      }

      if (!db.objectStoreNames.contains(STORE_VOCAB_MUTATIONS)) {
        const s = db.createObjectStore(STORE_VOCAB_MUTATIONS, { keyPath: '_id', autoIncrement: true });
        s.createIndex('user_id', 'user_id', { unique: false });
      }
    };

    req.onsuccess  = (evt) => resolve(evt.target.result);
    req.onerror    = (evt) => {
      _dbPromise = null;
      reject(evt.target.error);
    };
    req.onblocked  = () => {
      console.warn('[vocabCache] DB upgrade blocked by another tab.');
    };
  });
  return _dbPromise;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function _metaKey(userId) {
  return `u_${userId}`;
}

function _tx(db, stores, mode) {
  return db.transaction(stores, mode);
}

function _promisifyRequest(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror   = (e) => reject(e.target.error);
  });
}

function _promisifyTx(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror    = (e) => reject(e.target.error);
    tx.onabort    = (e) => reject(e.target.error ?? new Error('Transaction aborted'));
  });
}

// ─── Public API — Vocabulary ─────────────────────────────────────────────────

/**
 * Returns true if IndexedDB is usable in this environment.
 */
export function isOfflineCacheAvailable() {
  return 'indexedDB' in globalThis;
}

/**
 * Save a batch of vocabulary items for a user.
 * Uses put() — existing entries are overwritten with fresh data.
 *
 * @param {number} userId
 * @param {Array}  items       — from /api/webapp/vocabulary/list
 * @param {number} serverTotal — total word count on server (for meta)
 */
export async function saveVocabBatch(userId, items, serverTotal) {
  if (!items.length) return;
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB, STORE_META], 'readwrite');
  const vocabStore = tx.objectStore(STORE_VOCAB);
  const metaStore  = tx.objectStore(STORE_META);

  for (const item of items) {
    vocabStore.put({ ...item, user_id: userId });
  }

  metaStore.put({
    key:          _metaKey(userId),
    last_updated: new Date().toISOString(),
    total_count:  serverTotal,
  });

  await _promisifyTx(tx);
}

/**
 * Return cached vocabulary for a user with optional filtering.
 *
 * @param {number} userId
 * @param {object} opts
 *   folder_id : number | -1 (no folder) | null (all)
 *   search    : string | null
 *   sort      : 'date_desc' | 'date_asc' | 'alpha_asc' | 'alpha_desc' | 'srs_status'
 *   limit     : number
 *   offset    : number
 * @returns {{ items: Array, total: number, meta: object|null }}
 */
export async function getCachedVocab(userId, opts = {}) {
  const { folder_id = null, search = null, sort = 'date_desc', limit = 40, offset = 0 } = opts;
  const db = await _openDB();

  const tx       = _tx(db, [STORE_VOCAB, STORE_META], 'readonly');
  const vocabIdx = tx.objectStore(STORE_VOCAB).index('user_id');
  const metaKey  = IDBKeyRange.only(userId);

  const [allItems, meta] = await Promise.all([
    _promisifyRequest(vocabIdx.getAll(metaKey)),
    _promisifyRequest(tx.objectStore(STORE_META).get(_metaKey(userId))),
  ]);

  let filtered = allItems;

  if (folder_id === -1) {
    filtered = filtered.filter((it) => it.folder_id == null);
  } else if (folder_id != null) {
    filtered = filtered.filter((it) => it.folder_id === folder_id);
  }

  if (search) {
    const needle = search.trim().toLowerCase();
    filtered = filtered.filter((it) => {
      if ((it.word_de        || '').toLowerCase().includes(needle)) return true;
      if ((it.word_ru        || '').toLowerCase().includes(needle)) return true;
      if ((it.translation_ru || '').toLowerCase().includes(needle)) return true;
      if ((it.translation_de || '').toLowerCase().includes(needle)) return true;
      // Also check display fields which pull from response_json (richer than raw columns).
      if ((it.display_word        || '').toLowerCase().includes(needle)) return true;
      if ((it.display_translation || '').toLowerCase().includes(needle)) return true;
      // Deep-search response_json for source_text / target_text / translation fields.
      const rj = it.response_json && typeof it.response_json === 'object' ? it.response_json : null;
      if (rj) {
        if ((rj.source_text    || '').toLowerCase().includes(needle)) return true;
        if ((rj.target_text    || '').toLowerCase().includes(needle)) return true;
        if ((rj.word_de        || '').toLowerCase().includes(needle)) return true;
        if ((rj.translation_ru || '').toLowerCase().includes(needle)) return true;
        const senses = Array.isArray(rj.dictionary_senses) ? rj.dictionary_senses : [];
        if (senses.some((s) => (s?.value || '').toLowerCase().includes(needle))) return true;
      }
      return false;
    });
  }

  const sorters = {
    date_desc:  (a, b) => (b.created_at || '').localeCompare(a.created_at || ''),
    date_asc:   (a, b) => (a.created_at || '').localeCompare(b.created_at || ''),
    alpha_asc:  (a, b) => (a.display_word || '').localeCompare(b.display_word || '', 'de'),
    alpha_desc: (a, b) => (b.display_word || '').localeCompare(a.display_word || '', 'de'),
    srs_status: (a, b) => {
      const order = { due: 0, new: 1, ok: 2, none: 3 };
      return (order[a.srs_label] ?? 3) - (order[b.srs_label] ?? 3);
    },
  };
  filtered.sort(sorters[sort] ?? sorters.date_desc);

  const total = filtered.length;
  const items = filtered.slice(offset, offset + limit);

  return { items, total, meta: meta ?? null };
}

/**
 * Return cache metadata for a user (last_updated, total_count).
 */
export async function getVocabCacheMeta(userId) {
  const db   = await _openDB();
  const tx   = _tx(db, [STORE_META], 'readonly');
  const meta = await _promisifyRequest(tx.objectStore(STORE_META).get(_metaKey(userId)));
  return meta ?? null;
}

/**
 * Count cached entries for a user.
 */
export async function countCachedVocab(userId) {
  const db  = await _openDB();
  const tx  = _tx(db, [STORE_VOCAB], 'readonly');
  const idx = tx.objectStore(STORE_VOCAB).index('user_id');
  return _promisifyRequest(idx.count(IDBKeyRange.only(userId)));
}

/**
 * Return sync stats from the local cache for a user.
 *
 * @param {number} userId
 * @param {number|null} folderId
 * @returns {{ totalCount: number, folderCount: number, latestUpdatedAt: string|null }}
 */
export async function getCachedVocabSyncStats(userId, folderId = null) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB], 'readonly');
  const idx = tx.objectStore(STORE_VOCAB).index('user_id');
  const allItems = await _promisifyRequest(idx.getAll(IDBKeyRange.only(userId)));

  let folderCount = 0;
  let latestUpdatedAt = null;

  for (const item of allItems) {
    if (folderId != null && Number(item?.folder_id) === Number(folderId)) {
      folderCount += 1;
    }
    const updatedAt = String(item?.updated_at || '').trim();
    if (updatedAt && (!latestUpdatedAt || updatedAt > latestUpdatedAt)) {
      latestUpdatedAt = updatedAt;
    }
  }

  return {
    totalCount: allItems.length,
    folderCount,
    latestUpdatedAt,
  };
}

/**
 * Reconcile one folder in local cache against the authoritative server ID set.
 * Removes entries that still appear to belong to the folder locally but are not
 * present on the server anymore.
 *
 * @param {number} userId
 * @param {number} folderId
 * @param {Array<number>} keepEntryIds
 */
export async function reconcileCachedFolderEntries(userId, folderId, keepEntryIds = []) {
  const db = await _openDB();
  const keepSet = new Set(
    (Array.isArray(keepEntryIds) ? keepEntryIds : [])
      .map((id) => Number(id))
      .filter((id) => Number.isFinite(id) && id > 0)
  );
  const tx = _tx(db, [STORE_VOCAB], 'readwrite');
  const idx = tx.objectStore(STORE_VOCAB).index('user_id');
  const allItems = await _promisifyRequest(idx.getAll(IDBKeyRange.only(userId)));
  const store = tx.objectStore(STORE_VOCAB);

  for (const item of allItems) {
    if (Number(item?.folder_id) !== Number(folderId)) continue;
    const entryId = Number(item?.id);
    if (!keepSet.has(entryId)) {
      store.delete(entryId);
    }
  }

  await _promisifyTx(tx);
}

/**
 * Delete a single entry from cache (mirrors a server-side delete).
 */
export async function deleteCachedVocabEntry(entryId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB], 'readwrite');
  tx.objectStore(STORE_VOCAB).delete(entryId);
  await _promisifyTx(tx);
}

/**
 * Update a single cached entry (mirrors a server-side edit).
 */
export async function updateCachedVocabEntry(userId, updatedItem) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB], 'readwrite');
  tx.objectStore(STORE_VOCAB).put({ ...updatedItem, user_id: userId });
  await _promisifyTx(tx);
}

/**
 * Compute folder stats from cached data.
 */
export async function getCachedFolderStats(userId) {
  const db  = await _openDB();
  const tx  = _tx(db, [STORE_VOCAB], 'readonly');
  const idx = tx.objectStore(STORE_VOCAB).index('user_id');
  const all = await _promisifyRequest(idx.getAll(IDBKeyRange.only(userId)));

  const folderMap = {};
  let noFolderCount = 0;

  for (const item of all) {
    if (item.folder_id == null) {
      noFolderCount++;
    } else {
      if (!folderMap[item.folder_id]) {
        folderMap[item.folder_id] = {
          id:         item.folder_id,
          name:       item.folder_name || String(item.folder_id),
          icon:       item.folder_icon || 'book',
          color:      item.folder_color || '#5ddcff',
          word_count: 0,
        };
      }
      folderMap[item.folder_id].word_count++;
    }
  }

  return {
    folders:         Object.values(folderMap),
    no_folder_count: noFolderCount,
    total_count:     all.length,
  };
}

// ─── Public API — SRS Queue ──────────────────────────────────────────────────

/**
 * Replace the prefetched SRS card queue for a user with a fresh batch.
 * Clears existing cards for the user, then inserts the new ones in order.
 *
 * @param {number} userId
 * @param {Array}  cards  — card objects from /api/cards/prefetch
 */
export async function saveSrsQueue(userId, cards) {
  if (!cards.length) return;
  const db = await _openDB();
  return new Promise((resolve, reject) => {
    const tx    = db.transaction([STORE_SRS_QUEUE], 'readwrite');
    const store = tx.objectStore(STORE_SRS_QUEUE);
    const idx   = store.index('user_id');

    // Delete all existing cards for this user, then insert fresh batch.
    const clearReq = idx.openCursor(IDBKeyRange.only(userId));
    clearReq.onsuccess = (e) => {
      const cursor = e.target.result;
      if (cursor) {
        cursor.delete();
        cursor.continue();
        return;
      }
      // All old records deleted — insert fresh batch.
      cards.forEach((card, pos) => {
        store.add({ ...card, user_id: userId, _pos: pos });
      });
    };
    clearReq.onerror = (e) => reject(e.target.error);
    tx.oncomplete = () => resolve();
    tx.onerror    = (e) => reject(e.target.error);
    tx.onabort    = (e) => reject(e.target.error ?? new Error('Transaction aborted'));
  });
}

/**
 * Pop and return the next card from the offline SRS queue (lowest _pos for user).
 * Returns null if the queue is empty.
 *
 * @param {number} userId
 * @returns {object|null}
 */
export async function takeNextSrsCard(userId) {
  const db = await _openDB();
  return new Promise((resolve, reject) => {
    const tx    = db.transaction([STORE_SRS_QUEUE], 'readwrite');
    const idx   = tx.objectStore(STORE_SRS_QUEUE).index('user_pos');
    const range = IDBKeyRange.bound([userId, 0], [userId, Number.MAX_SAFE_INTEGER]);
    const req   = idx.openCursor(range);
    let result  = null;

    req.onsuccess = (e) => {
      const cursor = e.target.result;
      if (!cursor) return;
      const record = { ...cursor.value };
      delete record._id;
      delete record._pos;
      result = record;
      cursor.delete();
    };
    req.onerror   = (e) => reject(e.target.error);
    tx.oncomplete = () => resolve(result);
    tx.onerror    = (e) => reject(e.target.error);
    tx.onabort    = (e) => reject(e.target.error ?? new Error('Transaction aborted'));
  });
}

/**
 * Count how many SRS cards are queued for a user.
 *
 * @param {number} userId
 * @returns {number}
 */
export async function countSrsQueue(userId) {
  const db  = await _openDB();
  const tx  = _tx(db, [STORE_SRS_QUEUE], 'readonly');
  return _promisifyRequest(tx.objectStore(STORE_SRS_QUEUE).index('user_id').count(IDBKeyRange.only(userId)));
}

// ─── Public API — SRS Pending Reviews ────────────────────────────────────────

/**
 * Record a review that was made offline (to be synced later).
 *
 * @param {number} userId
 * @param {{ card_id, rating, response_ms, queue_source }} review
 */
export async function addPendingReview(userId, { card_id, rating, response_ms, queue_source }) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_SRS_PENDING], 'readwrite');
  tx.objectStore(STORE_SRS_PENDING).add({
    user_id:      userId,
    card_id:      String(card_id),
    rating,
    response_ms:  Number(response_ms) || 0,
    queue_source: queue_source || 'system',
    recorded_at:  new Date().toISOString(),
  });
  await _promisifyTx(tx);
}

/**
 * Return all pending (unsynced) reviews for a user, ordered by insertion.
 *
 * @param {number} userId
 * @returns {Array}
 */
export async function getPendingReviews(userId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_SRS_PENDING], 'readonly');
  return _promisifyRequest(
    tx.objectStore(STORE_SRS_PENDING).index('user_id').getAll(IDBKeyRange.only(userId))
  );
}

/**
 * Remove a pending review by its IDB auto-incremented key.
 *
 * @param {number} pendingId  — the _id field returned by getPendingReviews
 */
export async function clearPendingReview(pendingId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_SRS_PENDING], 'readwrite');
  tx.objectStore(STORE_SRS_PENDING).delete(pendingId);
  await _promisifyTx(tx);
}

/**
 * Count pending (unsynced) reviews for a user.
 *
 * @param {number} userId
 * @returns {number}
 */
export async function countPendingReviews(userId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_SRS_PENDING], 'readonly');
  return _promisifyRequest(
    tx.objectStore(STORE_SRS_PENDING).index('user_id').count(IDBKeyRange.only(userId))
  );
}

// ─── Public API — Vocab Mutations ─────────────────────────────────────────────

/**
 * Queue a vocabulary mutation (edit or delete) recorded while offline.
 *
 * @param {number} userId
 * @param {{ type: 'delete'|'edit', entry_id: number, payload?: object }} mutation
 *   payload is only required for type='edit' and contains the API request fields
 *   (word_de, translation_ru, dictionary_senses, folder_id, clear_folder).
 */
export async function addVocabMutation(userId, { type, entry_id, payload = null }) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB_MUTATIONS], 'readwrite');
  tx.objectStore(STORE_VOCAB_MUTATIONS).add({
    user_id:     userId,
    type,
    entry_id:    Number(entry_id),
    payload:     payload ?? null,
    recorded_at: new Date().toISOString(),
  });
  await _promisifyTx(tx);
}

/**
 * Return all queued vocab mutations for a user, in insertion order.
 *
 * @param {number} userId
 * @returns {Array}
 */
export async function getVocabMutations(userId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB_MUTATIONS], 'readonly');
  return _promisifyRequest(
    tx.objectStore(STORE_VOCAB_MUTATIONS).index('user_id').getAll(IDBKeyRange.only(userId))
  );
}

/**
 * Remove a mutation by its IDB auto-incremented key.
 *
 * @param {number} mutationId  — the _id field from getVocabMutations
 */
export async function clearVocabMutation(mutationId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB_MUTATIONS], 'readwrite');
  tx.objectStore(STORE_VOCAB_MUTATIONS).delete(mutationId);
  await _promisifyTx(tx);
}

/**
 * Count pending vocab mutations for a user.
 *
 * @param {number} userId
 * @returns {number}
 */
export async function countVocabMutations(userId) {
  const db = await _openDB();
  const tx = _tx(db, [STORE_VOCAB_MUTATIONS], 'readonly');
  return _promisifyRequest(
    tx.objectStore(STORE_VOCAB_MUTATIONS).index('user_id').count(IDBKeyRange.only(userId))
  );
}
